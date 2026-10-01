//! Authorization (design §11): who may perform which S3 action on which
//! resource.
//!
//! Every request the gateway routes is authorized before its operation
//! runs, and before `s3s` reads its input:
//!
//! 1. Right after authentication, the gateway refuses an unsigned request
//!    with `403 AccessDenied` unless `anonymous_access` is on, so an
//!    anonymous caller learns nothing else about the request.
//! 2. Once `s3s` has routed the request to an operation, [`s3_actions`]
//!    names the IAM actions it needs, and the request's path names the
//!    resource: `arn:aws:s3:::bucket`, `arn:aws:s3:::bucket/key`, or
//!    `arn:aws:s3:::*` for ListBuckets. The caller's [`Permissions`] must
//!    allow every action, or the answer is `403 AccessDenied`. Operations
//!    with no IAM action are denied to everyone.
//!
//! The caller is the [`Principal`] in the request's
//! [`crate::Authenticated`] extension, or, for an unsigned
//! request, the anonymous principal whose permissions come from
//! `anonymous_policy`.
//!
//! Operations whose authorization depends on their input are checked again
//! in the matching `S3Access` method, once `s3s` has parsed the input:
//!
//! - CopyObject also needs its [`source_actions`], `s3:GetObject`, on the
//!   object `x-amz-copy-source` names, so a copy reads nothing its caller
//!   could not GET. UploadPartCopy will need the same (plan M4-05).
//! - DeleteObjects names a bucket in its path but deletes the keys its body
//!   lists, so it is authorized key by key ([`is_per_key`]): each key needs
//!   `s3:DeleteObject` on `arn:aws:s3:::bucket/key`. A key its caller may
//!   not delete gets an `AccessDenied` entry in the answer, as in S3, and
//!   the other keys are deleted. The decisions reach the operation in a
//!   [`KeyDecisions`] request extension.

use std::fmt;
use std::sync::Arc;

use s3s::access::{S3Access, S3AccessContext};
use s3s::auth::{S3Auth, SecretKey};
use s3s::dto::{CopyObjectInput, CopySource, DeleteObjectsInput};
use s3s::path::S3Path;
use s3s::{S3Error, S3Request, S3Result, s3_error};
use skys3_types::policy::{Decision, Policy, RequestContext, S3_ARN_PREFIX};

use crate::sigv4::Authenticated;

/// The policies that decide what a principal may do.
///
/// A request is allowed when no policy denies it explicitly, some identity
/// policy allows it, and the session policy, if there is one, allows it
/// too. A session policy only narrows: it never grants what the identity
/// policies do not (design §11).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Permissions {
    identity: Arc<[Arc<Policy>]>,
    session: Option<Arc<Policy>>,
}

impl Permissions {
    /// The permissions that `policies` grant together: a static
    /// credential's policy, or a role's policies.
    pub fn new(policies: impl IntoIterator<Item = Arc<Policy>>) -> Self {
        Self {
            identity: policies.into_iter().collect(),
            session: None,
        }
    }

    /// The permissions of a single policy.
    #[must_use]
    pub fn from_policy(policy: Policy) -> Self {
        Self::new([Arc::new(policy)])
    }

    /// Permissions that allow everything, for tests and fuzzing.
    #[cfg(any(test, feature = "test-util"))]
    #[must_use]
    pub fn allow_all() -> Self {
        Self::from_policy(Policy::allow_all())
    }

    /// These permissions, narrowed by a session's policy (plan M1-24).
    #[must_use]
    pub fn with_session_policy(mut self, policy: Arc<Policy>) -> Self {
        self.session = Some(policy);
        self
    }

    /// The decision for one action on one resource.
    #[must_use]
    pub fn evaluate(&self, request: &RequestContext) -> Decision {
        let mut identity = Decision::ImplicitDeny;
        for policy in self.identity.iter() {
            match policy.evaluate(request) {
                Decision::ExplicitDeny => return Decision::ExplicitDeny,
                Decision::Allow => identity = Decision::Allow,
                Decision::ImplicitDeny => {}
            }
        }
        match self.session.as_ref().map(|policy| policy.evaluate(request)) {
            Some(Decision::ExplicitDeny) => Decision::ExplicitDeny,
            Some(Decision::ImplicitDeny) => Decision::ImplicitDeny,
            Some(Decision::Allow) | None => identity,
        }
    }
}

/// Whom a credential identifies: a name for logs, and its permissions.
#[derive(Clone, PartialEq, Eq)]
pub struct Principal {
    name: Arc<str>,
    permissions: Permissions,
}

impl Principal {
    /// A principal called `name`, such as a static credential's name.
    pub fn new(name: impl Into<Arc<str>>, permissions: Permissions) -> Self {
        Self {
            name: name.into(),
            permissions,
        }
    }

    /// The principal's name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// What the principal may do.
    #[must_use]
    pub fn permissions(&self) -> &Permissions {
        &self.permissions
    }
}

impl fmt::Debug for Principal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Principal").field(&self.name).finish()
    }
}

/// The IAM actions each S3 operation needs, by `s3s` operation name,
/// sorted by name. The names are the ones AWS documents for each operation;
/// an operation that needs several actions needs all of them. HeadBucket
/// needs `s3:ListBucket`, as in S3.
const ACTIONS: &[(&str, &[&str])] = &[
    ("AbortMultipartUpload", &["s3:AbortMultipartUpload"]),
    ("CompleteMultipartUpload", &["s3:PutObject"]),
    ("CopyObject", &["s3:PutObject"]),
    ("CreateBucket", &["s3:CreateBucket"]),
    ("CreateMultipartUpload", &["s3:PutObject"]),
    ("DeleteBucket", &["s3:DeleteBucket"]),
    (
        "DeleteBucketAnalyticsConfiguration",
        &["s3:PutAnalyticsConfiguration"],
    ),
    ("DeleteBucketCors", &["s3:PutBucketCORS"]),
    ("DeleteBucketEncryption", &["s3:PutEncryptionConfiguration"]),
    (
        "DeleteBucketIntelligentTieringConfiguration",
        &["s3:PutIntelligentTieringConfiguration"],
    ),
    (
        "DeleteBucketInventoryConfiguration",
        &["s3:PutInventoryConfiguration"],
    ),
    ("DeleteBucketLifecycle", &["s3:PutLifecycleConfiguration"]),
    (
        "DeleteBucketMetricsConfiguration",
        &["s3:PutMetricsConfiguration"],
    ),
    (
        "DeleteBucketOwnershipControls",
        &["s3:PutBucketOwnershipControls"],
    ),
    ("DeleteBucketPolicy", &["s3:DeleteBucketPolicy"]),
    (
        "DeleteBucketReplication",
        &["s3:PutReplicationConfiguration"],
    ),
    ("DeleteBucketTagging", &["s3:PutBucketTagging"]),
    ("DeleteBucketWebsite", &["s3:DeleteBucketWebsite"]),
    ("DeleteObject", &["s3:DeleteObject"]),
    ("DeleteObjectTagging", &["s3:DeleteObjectTagging"]),
    ("DeleteObjects", &["s3:DeleteObject"]),
    (
        "DeletePublicAccessBlock",
        &["s3:PutBucketPublicAccessBlock"],
    ),
    (
        "GetBucketAccelerateConfiguration",
        &["s3:GetAccelerateConfiguration"],
    ),
    ("GetBucketAcl", &["s3:GetBucketAcl"]),
    (
        "GetBucketAnalyticsConfiguration",
        &["s3:GetAnalyticsConfiguration"],
    ),
    ("GetBucketCors", &["s3:GetBucketCORS"]),
    ("GetBucketEncryption", &["s3:GetEncryptionConfiguration"]),
    (
        "GetBucketIntelligentTieringConfiguration",
        &["s3:GetIntelligentTieringConfiguration"],
    ),
    (
        "GetBucketInventoryConfiguration",
        &["s3:GetInventoryConfiguration"],
    ),
    (
        "GetBucketLifecycleConfiguration",
        &["s3:GetLifecycleConfiguration"],
    ),
    ("GetBucketLocation", &["s3:GetBucketLocation"]),
    ("GetBucketLogging", &["s3:GetBucketLogging"]),
    (
        "GetBucketMetricsConfiguration",
        &["s3:GetMetricsConfiguration"],
    ),
    (
        "GetBucketNotificationConfiguration",
        &["s3:GetBucketNotification"],
    ),
    (
        "GetBucketOwnershipControls",
        &["s3:GetBucketOwnershipControls"],
    ),
    ("GetBucketPolicy", &["s3:GetBucketPolicy"]),
    ("GetBucketPolicyStatus", &["s3:GetBucketPolicyStatus"]),
    ("GetBucketReplication", &["s3:GetReplicationConfiguration"]),
    ("GetBucketRequestPayment", &["s3:GetBucketRequestPayment"]),
    ("GetBucketTagging", &["s3:GetBucketTagging"]),
    ("GetBucketVersioning", &["s3:GetBucketVersioning"]),
    ("GetBucketWebsite", &["s3:GetBucketWebsite"]),
    ("GetObject", &["s3:GetObject"]),
    ("GetObjectAcl", &["s3:GetObjectAcl"]),
    (
        "GetObjectAttributes",
        &["s3:GetObject", "s3:GetObjectAttributes"],
    ),
    ("GetObjectLegalHold", &["s3:GetObjectLegalHold"]),
    (
        "GetObjectLockConfiguration",
        &["s3:GetBucketObjectLockConfiguration"],
    ),
    ("GetObjectRetention", &["s3:GetObjectRetention"]),
    ("GetObjectTagging", &["s3:GetObjectTagging"]),
    ("GetPublicAccessBlock", &["s3:GetBucketPublicAccessBlock"]),
    ("HeadBucket", &["s3:ListBucket"]),
    ("HeadObject", &["s3:GetObject"]),
    (
        "ListBucketAnalyticsConfigurations",
        &["s3:GetAnalyticsConfiguration"],
    ),
    (
        "ListBucketIntelligentTieringConfigurations",
        &["s3:GetIntelligentTieringConfiguration"],
    ),
    (
        "ListBucketInventoryConfigurations",
        &["s3:GetInventoryConfiguration"],
    ),
    (
        "ListBucketMetricsConfigurations",
        &["s3:GetMetricsConfiguration"],
    ),
    ("ListBuckets", &["s3:ListAllMyBuckets"]),
    ("ListMultipartUploads", &["s3:ListBucketMultipartUploads"]),
    ("ListObjectVersions", &["s3:ListBucketVersions"]),
    ("ListObjects", &["s3:ListBucket"]),
    ("ListObjectsV2", &["s3:ListBucket"]),
    ("ListParts", &["s3:ListMultipartUploadParts"]),
    (
        "PutBucketAccelerateConfiguration",
        &["s3:PutAccelerateConfiguration"],
    ),
    ("PutBucketAcl", &["s3:PutBucketAcl"]),
    (
        "PutBucketAnalyticsConfiguration",
        &["s3:PutAnalyticsConfiguration"],
    ),
    ("PutBucketCors", &["s3:PutBucketCORS"]),
    ("PutBucketEncryption", &["s3:PutEncryptionConfiguration"]),
    (
        "PutBucketIntelligentTieringConfiguration",
        &["s3:PutIntelligentTieringConfiguration"],
    ),
    (
        "PutBucketInventoryConfiguration",
        &["s3:PutInventoryConfiguration"],
    ),
    (
        "PutBucketLifecycleConfiguration",
        &["s3:PutLifecycleConfiguration"],
    ),
    ("PutBucketLogging", &["s3:PutBucketLogging"]),
    (
        "PutBucketMetricsConfiguration",
        &["s3:PutMetricsConfiguration"],
    ),
    (
        "PutBucketNotificationConfiguration",
        &["s3:PutBucketNotification"],
    ),
    (
        "PutBucketOwnershipControls",
        &["s3:PutBucketOwnershipControls"],
    ),
    ("PutBucketPolicy", &["s3:PutBucketPolicy"]),
    ("PutBucketReplication", &["s3:PutReplicationConfiguration"]),
    ("PutBucketRequestPayment", &["s3:PutBucketRequestPayment"]),
    ("PutBucketTagging", &["s3:PutBucketTagging"]),
    ("PutBucketVersioning", &["s3:PutBucketVersioning"]),
    ("PutBucketWebsite", &["s3:PutBucketWebsite"]),
    ("PutObject", &["s3:PutObject"]),
    ("PutObjectAcl", &["s3:PutObjectAcl"]),
    ("PutObjectLegalHold", &["s3:PutObjectLegalHold"]),
    (
        "PutObjectLockConfiguration",
        &["s3:PutBucketObjectLockConfiguration"],
    ),
    ("PutObjectRetention", &["s3:PutObjectRetention"]),
    ("PutObjectTagging", &["s3:PutObjectTagging"]),
    ("PutPublicAccessBlock", &["s3:PutBucketPublicAccessBlock"]),
    ("RestoreObject", &["s3:RestoreObject"]),
    ("SelectObjectContent", &["s3:GetObject"]),
    ("UploadPart", &["s3:PutObject"]),
    ("UploadPartCopy", &["s3:PutObject"]),
];

/// Operations whose [`s3_actions`] apply to each key of their input rather
/// than to the resource their path names.
const PER_KEY: &[&str] = &["DeleteObjects"];

/// The IAM actions an S3 operation needs, by its `s3s` operation name, or
/// `None` for an operation no policy can allow. For an operation that
/// [`is_per_key`], they apply to each key it names; otherwise to the
/// resource its path names.
#[must_use]
pub fn s3_actions(operation: &str) -> Option<&'static [&'static str]> {
    ACTIONS
        .binary_search_by(|(name, _)| (*name).cmp(operation))
        .ok()
        .map(|index| ACTIONS[index].1)
}

/// Whether an operation is authorized key by key (DeleteObjects), rather
/// than on the resource its path names.
#[must_use]
pub fn is_per_key(operation: &str) -> bool {
    PER_KEY.contains(&operation)
}

/// The IAM actions an operation needs on the object it copies from, besides
/// its [`s3_actions`] on its own resource: `s3:GetObject` for CopyObject.
#[must_use]
pub fn source_actions(operation: &str) -> Option<&'static [&'static str]> {
    (operation == "CopyObject").then_some(&["s3:GetObject"])
}

/// Which keys of a DeleteObjects request its caller may delete, in the
/// order of the request's `<Object>` elements. The access hook sets it; an
/// operation that finds none deletes nothing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KeyDecisions(Vec<bool>);

impl KeyDecisions {
    /// Whether the caller may delete the key at `index`.
    #[must_use]
    pub fn allows(&self, index: usize) -> bool {
        self.0.get(index).copied().unwrap_or(false)
    }
}

/// The ARN of the resource a request's path names.
#[must_use]
pub fn resource_arn(path: &S3Path) -> String {
    match path {
        S3Path::Root => format!("{S3_ARN_PREFIX}*"),
        S3Path::Bucket { bucket } => format!("{S3_ARN_PREFIX}{bucket}"),
        S3Path::Object { bucket, key } => format!("{S3_ARN_PREFIX}{bucket}/{key}"),
    }
}

/// The answer to a request its caller may not make. It says no more than
/// S3 does.
pub(crate) fn access_denied() -> S3Error {
    s3_error!(AccessDenied, "Access Denied")
}

/// Decides whether `principal`, or the anonymous caller when it is `None`,
/// may perform `operation` on `resource`.
pub(crate) fn authorize(
    principal: Option<&Principal>,
    anonymous: Option<&Permissions>,
    operation: &str,
    resource: &str,
) -> S3Result<()> {
    let actions = s3_actions(operation);
    authorize_actions(principal, anonymous, operation, actions, resource)
}

/// Decides whether `principal`, or the anonymous caller when it is `None`,
/// may perform every one of `actions` on `resource` for `operation`. No
/// actions means that no policy can allow it.
fn authorize_actions(
    principal: Option<&Principal>,
    anonymous: Option<&Permissions>,
    operation: &str,
    actions: Option<&[&str]>,
    resource: &str,
) -> S3Result<()> {
    let permissions = match principal {
        Some(principal) => principal.permissions(),
        None => anonymous.ok_or_else(access_denied)?,
    };
    let decision = match actions {
        None => Decision::ImplicitDeny,
        Some(actions) => actions
            .iter()
            .map(|action| permissions.evaluate(&RequestContext::new(action, resource)))
            .find(|decision| !decision.is_allowed())
            .unwrap_or(Decision::Allow),
    };
    if decision.is_allowed() {
        return Ok(());
    }
    tracing::debug!(
        principal = principal.map_or("anonymous", Principal::name),
        operation,
        resource,
        ?decision,
        "request denied"
    );
    Err(access_denied())
}

/// The `s3s` access hook, which authorizes every routed request.
pub(crate) struct Access {
    anonymous: Option<Permissions>,
}

impl Access {
    pub(crate) fn new(anonymous: Option<Permissions>) -> Self {
        Self { anonymous }
    }
}

#[async_trait::async_trait]
impl S3Access for Access {
    async fn check(&self, cx: &mut S3AccessContext<'_>) -> S3Result<()> {
        let operation = cx.s3_op().name().to_owned();
        let resource = resource_arn(cx.s3_path());
        let principal = cx
            .extensions_mut()
            .get::<Authenticated>()
            .map(|caller| &caller.principal);
        if is_per_key(&operation) {
            // Each key is checked once the input is parsed. Here the caller
            // only has to be one that policies apply to.
            return match (principal, &self.anonymous) {
                (None, None) => Err(access_denied()),
                _ => Ok(()),
            };
        }
        authorize(principal, self.anonymous.as_ref(), &operation, &resource)
    }

    async fn copy_object(&self, req: &mut S3Request<CopyObjectInput>) -> S3Result<()> {
        let CopySource::Bucket { bucket, key, .. } = &req.input.copy_source else {
            return Err(s3_error!(
                NotImplemented,
                "x-amz-copy-source must name a bucket and a key; \
                 access points and outposts are not supported"
            ));
        };
        let resource = format!("{S3_ARN_PREFIX}{bucket}/{key}");
        let principal = req
            .extensions
            .get::<Authenticated>()
            .map(|caller| &caller.principal);
        let operation = "CopyObject";
        let actions = source_actions(operation);
        authorize_actions(
            principal,
            self.anonymous.as_ref(),
            operation,
            actions,
            &resource,
        )
    }

    async fn delete_objects(&self, req: &mut S3Request<DeleteObjectsInput>) -> S3Result<()> {
        let principal = req
            .extensions
            .get::<Authenticated>()
            .map(|caller| &caller.principal);
        let bucket = &req.input.bucket;
        let decisions = req
            .input
            .delete
            .objects
            .iter()
            .map(|object| {
                let resource = format!("{S3_ARN_PREFIX}{bucket}/{}", object.key);
                let operation = "DeleteObjects";
                authorize(principal, self.anonymous.as_ref(), operation, &resource).is_ok()
            })
            .collect();
        req.extensions.insert(KeyDecisions(decisions));
        Ok(())
    }
}

/// The `s3s` authentication provider, which never finds a key.
///
/// The gateway verifies signatures itself and removes them before `s3s`
/// sees a request (design §11), so `s3s` never needs a secret. It calls its
/// access hook only when a provider is set, so this one exists to enable
/// the hook, and refuses anything `s3s` still takes as signed.
pub(crate) struct NoSignatures;

#[async_trait::async_trait]
impl S3Auth for NoSignatures {
    async fn get_secret_key(&self, _access_key: &str) -> S3Result<SecretKey> {
        Err(access_denied())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(json: &str) -> Arc<Policy> {
        Arc::new(json.parse().unwrap())
    }

    fn statement(effect: &str, action: &str, resource: &str) -> Arc<Policy> {
        policy(&format!(
            r#"{{"Version":"2012-10-17","Statement":{{"Effect":"{effect}","Action":"{action}","Resource":"{resource}"}}}}"#
        ))
    }

    #[test]
    fn session_policies_only_narrow() {
        let get = RequestContext::new("s3:GetObject", "arn:aws:s3:::b/k");
        let put = get.with_action("s3:PutObject");
        let delete = get.with_action("s3:DeleteObject");
        let role = Permissions::new([
            statement("Allow", "s3:Get*", "*"),
            statement("Allow", "s3:PutObject", "*"),
        ]);
        assert_eq!(role.evaluate(&get), Decision::Allow);
        assert_eq!(role.evaluate(&delete), Decision::ImplicitDeny);

        let session = role
            .clone()
            .with_session_policy(statement("Allow", "s3:*Object", "*"));
        assert_eq!(session.evaluate(&get), Decision::Allow);
        assert_eq!(
            session.evaluate(&delete),
            Decision::ImplicitDeny,
            "the session cannot grant what the role does not"
        );
        let narrowed = role
            .clone()
            .with_session_policy(statement("Allow", "s3:GetObject", "*"));
        assert_eq!(narrowed.evaluate(&put), Decision::ImplicitDeny);
        let denied = role.with_session_policy(statement("Deny", "s3:GetObject", "*"));
        assert_eq!(denied.evaluate(&get), Decision::ExplicitDeny);
    }

    #[test]
    fn a_deny_in_any_identity_policy_wins() {
        let permissions = Permissions::new([
            statement("Allow", "*", "*"),
            statement("Deny", "s3:DeleteBucket", "arn:aws:s3:::keep"),
            statement("Allow", "s3:DeleteBucket", "*"),
        ]);
        let delete = |bucket: &str| {
            RequestContext::new("s3:DeleteBucket", &format!("arn:aws:s3:::{bucket}"))
        };
        assert_eq!(
            permissions.evaluate(&delete("keep")),
            Decision::ExplicitDeny
        );
        assert_eq!(permissions.evaluate(&delete("other")), Decision::Allow);
        assert_eq!(
            Permissions::new([]).evaluate(&delete("other")),
            Decision::ImplicitDeny
        );
    }

    #[test]
    fn resources_follow_the_path() {
        assert_eq!(resource_arn(&S3Path::Root), "arn:aws:s3:::*");
        assert_eq!(resource_arn(&S3Path::bucket("b")), "arn:aws:s3:::b");
        assert_eq!(
            resource_arn(&S3Path::object("b", "a/ké y")),
            "arn:aws:s3:::b/a/ké y"
        );
    }

    #[test]
    fn operations_need_every_action_they_name() {
        let path = resource_arn(&S3Path::object("b", "k"));
        let get_only = Principal::new(
            "reader",
            Permissions::new([statement("Allow", "s3:GetObject", "*")]),
        );
        authorize(Some(&get_only), None, "GetObject", &path).unwrap();
        let error = authorize(Some(&get_only), None, "GetObjectAttributes", &path).unwrap_err();
        assert_eq!(error.code(), &s3s::S3ErrorCode::AccessDenied);

        let admin = Principal::new("admin", Permissions::allow_all());
        authorize(Some(&admin), None, "GetObjectAttributes", &path).unwrap();
        assert!(
            authorize(Some(&admin), None, "WriteGetObjectResponse", &path).is_err(),
            "an operation without actions is denied to everyone"
        );
        assert!(authorize(None, None, "GetObject", &path).is_err());
        authorize(None, Some(&Permissions::allow_all()), "GetObject", &path).unwrap();
        assert_eq!(format!("{admin:?}"), "Principal(\"admin\")");
        assert_eq!(admin.name(), "admin");
    }

    #[test]
    fn the_action_table_is_sorted_and_valid() {
        assert!(
            ACTIONS.windows(2).all(|pair| pair[0].0 < pair[1].0),
            "sorted and unique, for binary search"
        );
        for (operation, actions) in ACTIONS {
            assert_eq!(s3_actions(operation), Some(*actions));
            assert!(!actions.is_empty(), "{operation}");
            for action in *actions {
                // Each action is a valid policy action that names itself.
                let only = statement("Allow", action, "*");
                let request = RequestContext::new(action, "arn:aws:s3:::b");
                assert!(only.evaluate(&request).is_allowed(), "{operation}");
            }
        }
        assert!(is_per_key("DeleteObjects"));
        assert!(!is_per_key("DeleteObject"));
        assert_eq!(source_actions("CopyObject"), Some(&["s3:GetObject"][..]));
        assert_eq!(source_actions("PutObject"), None);
        assert_eq!(s3_actions("PostObject"), None);
    }

    #[tokio::test]
    async fn s3s_never_finds_a_key() {
        let error = NoSignatures.get_secret_key("AKID").await.unwrap_err();
        assert_eq!(error.code(), &s3s::S3ErrorCode::AccessDenied);
    }
}
