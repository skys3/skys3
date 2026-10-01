//! Authorization of every S3 operation the gateway supports (design §11).
//!
//! [`CASES`] holds one request per supported operation, the IAM actions it
//! needs, and the resource it names, and for a copy the actions it needs on
//! its source. Each case runs against a fresh gateway that authenticates
//! with SigV4 and static credentials, and checks that anonymous requests
//! and requests outside the caller's policy are denied, and that a policy
//! granting exactly those actions on exactly those resources is enough.
//! DeleteObjects is authorized key by key: a caller outside its policy gets
//! `200` with an `AccessDenied` entry for the key.
//!
//! A pull request that adds an S3 operation adds its row here.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::signing::{HOST, NOW, Signing, request, sdk_signed};
use common::{Answer, answer, config, gateway_with};
use http::{Method, Request};
use s3s::Body;
use serde_json::json;
use skys3_gateway::authz::{is_per_key, s3_actions, source_actions};
use skys3_gateway::sigv4::AuthMethod;
use skys3_gateway::{
    Authenticated, Gateway, GatewayConfig, MODE_HEADER, Permissions, Principal, SecretAccessKey,
    SigV4Authenticator, StaticCredentials,
};
use skys3_io::ManualWallClock;
use skys3_types::policy::Policy;

/// One supported S3 operation.
struct Case {
    /// The `s3s` operation name.
    operation: &'static str,
    method: Method,
    uri: &'static str,
    headers: &'static [(&'static str, &'static str)],
    body: &'static [u8],
    /// The IAM actions the operation needs, all of them.
    actions: &'static [&'static str],
    /// The resource the request names.
    resource: &'static str,
    /// For a copy, the actions it needs on its source, and the source.
    source: Option<(&'static [&'static str], &'static str)>,
    /// The answer to a caller allowed exactly `actions` on `resource`.
    allowed: (u16, Option<&'static str>),
}

impl Case {
    /// Every resource the case names, with the actions it needs there.
    fn grants(&self) -> Vec<(&'static [&'static str], &'static str)> {
        let mut grants = vec![(self.actions, self.resource)];
        grants.extend(self.source);
        grants
    }

    /// Every action the case needs.
    fn all_actions(&self) -> Vec<&'static str> {
        self.grants()
            .into_iter()
            .flat_map(|(actions, _)| actions.iter().copied())
            .collect()
    }
}

const VERSIONING: &[u8] = b"<VersioningConfiguration><Status>Enabled</Status>\
    </VersioningConfiguration>";
const ENCRYPTION: &[u8] = b"<ServerSideEncryptionConfiguration><Rule>\
    <ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm>\
    </ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>";
const OBJECT_LOCK: &[u8] = b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled\
    </ObjectLockEnabled></ObjectLockConfiguration>";
const RETENTION: &[u8] = b"<Retention><Mode>GOVERNANCE</Mode>\
    <RetainUntilDate>2030-01-01T00:00:00Z</RetainUntilDate></Retention>";
const COMPLETE: &[u8] = b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>\
    <ETag>\"8d777f385d3dfec8815d20f7496026dc\"</ETag></Part></CompleteMultipartUpload>";
const LEGAL_HOLD: &[u8] = b"<LegalHold><Status>ON</Status></LegalHold>";
const OWNERSHIP: &[u8] = b"<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced\
    </ObjectOwnership></Rule></OwnershipControls>";

const TAGGING: &[u8] = b"<Tagging><TagSet><Tag><Key>a</Key><Value>b</Value></Tag>\
    </TagSet></Tagging>";
const DELETE: &[u8] = b"<Delete><Object><Key>k</Key></Object></Delete>";
/// The base64 MD5 of [`DELETE`].
const DELETE_MD5: &str = "5DKh5iefM5MSKvRILIuFwQ==";

const BUCKET: &str = "arn:aws:s3:::bucket";
const OBJECT: &str = "arn:aws:s3:::bucket/k";
const SOURCE: &str = "arn:aws:s3:::bucket/source";

/// Every S3 operation the gateway supports. Bucket `bucket` exists when each
/// request is sent.
const CASES: &[Case] = &[
    Case {
        operation: "CreateBucket",
        method: Method::PUT,
        uri: "/new-bucket",
        headers: &[(MODE_HEADER, "local")],
        body: b"",
        actions: &["s3:CreateBucket"],
        resource: "arn:aws:s3:::new-bucket",
        source: None,
        allowed: (200, None),
    },
    Case {
        operation: "DeleteBucket",
        method: Method::DELETE,
        uri: "/bucket",
        headers: &[],
        body: b"",
        actions: &["s3:DeleteBucket"],
        resource: BUCKET,
        source: None,
        allowed: (204, None),
    },
    Case {
        operation: "HeadBucket",
        method: Method::HEAD,
        uri: "/bucket",
        headers: &[],
        body: b"",
        actions: &["s3:ListBucket"],
        resource: BUCKET,
        source: None,
        allowed: (200, None),
    },
    Case {
        operation: "ListBuckets",
        method: Method::GET,
        uri: "/",
        headers: &[],
        body: b"",
        actions: &["s3:ListAllMyBuckets"],
        resource: "arn:aws:s3:::*",
        source: None,
        allowed: (200, None),
    },
    Case {
        operation: "GetBucketLocation",
        method: Method::GET,
        uri: "/bucket?location",
        headers: &[],
        body: b"",
        actions: &["s3:GetBucketLocation"],
        resource: BUCKET,
        source: None,
        allowed: (200, None),
    },
    Case {
        operation: "GetBucketVersioning",
        method: Method::GET,
        uri: "/bucket?versioning",
        headers: &[],
        body: b"",
        actions: &["s3:GetBucketVersioning"],
        resource: BUCKET,
        source: None,
        allowed: (200, None),
    },
    Case {
        operation: "PutBucketVersioning",
        method: Method::PUT,
        uri: "/bucket?versioning",
        headers: &[],
        body: VERSIONING,
        actions: &["s3:PutBucketVersioning"],
        resource: BUCKET,
        source: None,
        allowed: (501, Some("NotImplemented")),
    },
    Case {
        operation: "ListObjectVersions",
        method: Method::GET,
        uri: "/bucket?versions",
        headers: &[],
        body: b"",
        actions: &["s3:ListBucketVersions"],
        resource: BUCKET,
        source: None,
        allowed: (501, Some("NotImplemented")),
    },
    Case {
        operation: "PutBucketEncryption",
        method: Method::PUT,
        uri: "/bucket?encryption",
        headers: &[],
        body: ENCRYPTION,
        actions: &["s3:PutEncryptionConfiguration"],
        resource: BUCKET,
        source: None,
        allowed: (501, Some("NotImplemented")),
    },
    Case {
        operation: "GetBucketEncryption",
        method: Method::GET,
        uri: "/bucket?encryption",
        headers: &[],
        body: b"",
        actions: &["s3:GetEncryptionConfiguration"],
        resource: BUCKET,
        source: None,
        allowed: (400, Some("ServerSideEncryptionConfigurationNotFoundError")),
    },
    Case {
        operation: "PutObjectLockConfiguration",
        method: Method::PUT,
        uri: "/bucket?object-lock",
        headers: &[],
        body: OBJECT_LOCK,
        actions: &["s3:PutBucketObjectLockConfiguration"],
        resource: BUCKET,
        source: None,
        allowed: (501, Some("NotImplemented")),
    },
    Case {
        operation: "GetObjectLockConfiguration",
        method: Method::GET,
        uri: "/bucket?object-lock",
        headers: &[],
        body: b"",
        actions: &["s3:GetBucketObjectLockConfiguration"],
        resource: BUCKET,
        source: None,
        allowed: (404, Some("ObjectLockConfigurationNotFoundError")),
    },
    Case {
        operation: "PutObjectRetention",
        method: Method::PUT,
        uri: "/bucket/k?retention",
        headers: &[],
        body: RETENTION,
        actions: &["s3:PutObjectRetention"],
        resource: OBJECT,
        source: None,
        allowed: (400, Some("InvalidRequest")),
    },
    Case {
        operation: "GetObjectRetention",
        method: Method::GET,
        uri: "/bucket/k?retention",
        headers: &[],
        body: b"",
        actions: &["s3:GetObjectRetention"],
        resource: OBJECT,
        source: None,
        allowed: (400, Some("InvalidRequest")),
    },
    Case {
        operation: "PutObjectLegalHold",
        method: Method::PUT,
        uri: "/bucket/k?legal-hold",
        headers: &[],
        body: LEGAL_HOLD,
        actions: &["s3:PutObjectLegalHold"],
        resource: OBJECT,
        source: None,
        allowed: (400, Some("InvalidRequest")),
    },
    Case {
        operation: "GetObjectLegalHold",
        method: Method::GET,
        uri: "/bucket/k?legal-hold",
        headers: &[],
        body: b"",
        actions: &["s3:GetObjectLegalHold"],
        resource: OBJECT,
        source: None,
        allowed: (400, Some("InvalidRequest")),
    },
    Case {
        operation: "PutBucketAcl",
        method: Method::PUT,
        uri: "/bucket?acl",
        headers: &[("x-amz-acl", "private")],
        body: b"",
        actions: &["s3:PutBucketAcl"],
        resource: BUCKET,
        source: None,
        allowed: (200, None),
    },
    Case {
        operation: "PutObjectAcl",
        method: Method::PUT,
        uri: "/bucket/k?acl",
        headers: &[("x-amz-acl", "private")],
        body: b"",
        actions: &["s3:PutObjectAcl"],
        resource: OBJECT,
        source: None,
        allowed: (501, Some("NotImplemented")),
    },
    Case {
        operation: "PutBucketOwnershipControls",
        method: Method::PUT,
        uri: "/bucket?ownershipControls",
        headers: &[],
        body: OWNERSHIP,
        actions: &["s3:PutBucketOwnershipControls"],
        resource: BUCKET,
        source: None,
        allowed: (200, None),
    },
    Case {
        operation: "GetBucketOwnershipControls",
        method: Method::GET,
        uri: "/bucket?ownershipControls",
        headers: &[],
        body: b"",
        actions: &["s3:GetBucketOwnershipControls"],
        resource: BUCKET,
        source: None,
        allowed: (200, None),
    },
    Case {
        operation: "PutObject",
        method: Method::PUT,
        uri: "/bucket/k",
        headers: &[],
        body: b"data",
        actions: &["s3:PutObject"],
        resource: OBJECT,
        source: None,
        allowed: (200, None),
    },
    Case {
        operation: "GetObject",
        method: Method::GET,
        uri: "/bucket/k",
        headers: &[],
        body: b"",
        actions: &["s3:GetObject"],
        resource: OBJECT,
        source: None,
        allowed: (404, Some("NoSuchKey")),
    },
    Case {
        operation: "HeadObject",
        method: Method::HEAD,
        uri: "/bucket/k",
        headers: &[],
        body: b"",
        actions: &["s3:GetObject"],
        resource: OBJECT,
        source: None,
        allowed: (404, Some("NoSuchKey")),
    },
    Case {
        operation: "DeleteObject",
        method: Method::DELETE,
        uri: "/bucket/k",
        headers: &[],
        body: b"",
        actions: &["s3:DeleteObject"],
        resource: OBJECT,
        source: None,
        allowed: (204, None),
    },
    Case {
        operation: "CreateMultipartUpload",
        method: Method::POST,
        uri: "/bucket/k?uploads",
        headers: &[],
        body: b"",
        actions: &["s3:PutObject"],
        resource: OBJECT,
        source: None,
        allowed: (200, None),
    },
    Case {
        operation: "UploadPart",
        method: Method::PUT,
        uri: "/bucket/k?partNumber=1&uploadId=00000000000000010000000000000001",
        headers: &[],
        body: b"data",
        actions: &["s3:PutObject"],
        resource: OBJECT,
        source: None,
        allowed: (404, Some("NoSuchUpload")),
    },
    Case {
        operation: "CompleteMultipartUpload",
        method: Method::POST,
        uri: "/bucket/k?uploadId=00000000000000010000000000000001",
        headers: &[],
        body: COMPLETE,
        actions: &["s3:PutObject"],
        resource: OBJECT,
        source: None,
        allowed: (404, Some("NoSuchUpload")),
    },
    Case {
        operation: "AbortMultipartUpload",
        method: Method::DELETE,
        uri: "/bucket/k?uploadId=00000000000000010000000000000001",
        headers: &[],
        body: b"",
        actions: &["s3:AbortMultipartUpload"],
        resource: OBJECT,
        source: None,
        allowed: (404, Some("NoSuchUpload")),
    },
    Case {
        operation: "ListParts",
        method: Method::GET,
        uri: "/bucket/k?uploadId=00000000000000010000000000000001",
        headers: &[],
        body: b"",
        actions: &["s3:ListMultipartUploadParts"],
        resource: OBJECT,
        source: None,
        allowed: (404, Some("NoSuchUpload")),
    },
    Case {
        operation: "ListMultipartUploads",
        method: Method::GET,
        uri: "/bucket?uploads",
        headers: &[],
        body: b"",
        actions: &["s3:ListBucketMultipartUploads"],
        resource: BUCKET,
        source: None,
        allowed: (200, None),
    },
    Case {
        operation: "ListObjectsV2",
        method: Method::GET,
        uri: "/bucket?list-type=2&prefix=k&delimiter=%2F",
        headers: &[],
        body: b"",
        actions: &["s3:ListBucket"],
        resource: BUCKET,
        source: None,
        allowed: (200, None),
    },
    Case {
        operation: "ListObjects",
        method: Method::GET,
        uri: "/bucket?marker=k",
        headers: &[],
        body: b"",
        actions: &["s3:ListBucket"],
        resource: BUCKET,
        source: None,
        allowed: (200, None),
    },
    Case {
        operation: "DeleteObjects",
        method: Method::POST,
        uri: "/bucket?delete",
        headers: &[("content-md5", DELETE_MD5)],
        body: DELETE,
        actions: &["s3:DeleteObject"],
        resource: OBJECT,
        source: None,
        allowed: (200, None),
    },
    Case {
        operation: "CopyObject",
        method: Method::PUT,
        uri: "/bucket/k",
        headers: &[("x-amz-copy-source", "bucket/source")],
        body: b"",
        actions: &["s3:PutObject"],
        resource: OBJECT,
        source: Some((&["s3:GetObject"], SOURCE)),
        allowed: (404, Some("NoSuchKey")),
    },
    Case {
        operation: "GetObjectTagging",
        method: Method::GET,
        uri: "/bucket/k?tagging",
        headers: &[],
        body: b"",
        actions: &["s3:GetObjectTagging"],
        resource: OBJECT,
        source: None,
        allowed: (404, Some("NoSuchKey")),
    },
    Case {
        operation: "PutObjectTagging",
        method: Method::PUT,
        uri: "/bucket/k?tagging",
        headers: &[],
        body: TAGGING,
        actions: &["s3:PutObjectTagging"],
        resource: OBJECT,
        source: None,
        allowed: (404, Some("NoSuchKey")),
    },
    Case {
        operation: "DeleteObjectTagging",
        method: Method::DELETE,
        uri: "/bucket/k?tagging",
        headers: &[],
        body: b"",
        actions: &["s3:DeleteObjectTagging"],
        resource: OBJECT,
        source: None,
        allowed: (404, Some("NoSuchKey")),
    },
];

/// The callers each case uses, by access key ID.
const ADMIN: &str = "AKIAADMIN";
const EXACT: &str = "AKIAEXACT";
const OTHER_ACTIONS: &str = "AKIAOTHERACTIONS";
const OTHER_RESOURCES: &str = "AKIAOTHERRESOURCES";
const DENIED: &str = "AKIADENIED";
const DENIED_SOURCE: &str = "AKIADENIEDSOURCE";
const SECRET: &str = "authorization-test-secret";

fn policy(statements: &[serde_json::Value]) -> Permissions {
    let document = json!({"Version": "2012-10-17", "Statement": statements}).to_string();
    Permissions::from_policy(Policy::parse(&document).unwrap())
}

/// Static credentials for `case`'s callers:
///
/// - `EXACT` may perform the case's actions on its resources, and nothing
///   else;
/// - `OTHER_ACTIONS` may perform every other action on every resource;
/// - `OTHER_RESOURCES` may perform the case's actions on every other
///   resource;
/// - `DENIED` may do anything, except that one statement denies the case's
///   last action on its resource;
/// - `DENIED_SOURCE`, for a copy, may do anything except read its source.
fn credentials(case: &Case) -> StaticCredentials {
    let key = |credentials: StaticCredentials, id: &str, permissions| {
        credentials.with_key(
            id,
            SecretAccessKey::new(SECRET),
            Principal::new(id, permissions),
        )
    };
    let credentials = key(
        StaticCredentials::new(),
        ADMIN,
        policy(&[json!(
            {"Effect": "Allow", "Action": "*", "Resource": "*"}
        )]),
    );
    let grants = case.grants();
    let statements = |effect: &str, resource: &str| -> Vec<serde_json::Value> {
        grants
            .iter()
            .map(|(actions, target)| json!({"Effect": effect, "Action": actions, resource: target}))
            .collect()
    };
    let credentials = key(credentials, EXACT, policy(&statements("Allow", "Resource")));
    let credentials = key(
        credentials,
        OTHER_ACTIONS,
        policy(&[json!(
            {"Effect": "Allow", "NotAction": case.all_actions(), "Resource": "*"}
        )]),
    );
    let credentials = key(
        credentials,
        OTHER_RESOURCES,
        policy(&statements("Allow", "NotResource")),
    );
    let credentials = key(
        credentials,
        DENIED,
        policy(&[
            json!({"Effect": "Allow", "Action": "s3:*", "Resource": "*"}),
            json!({"Effect": "Deny", "Action": case.actions.last(), "Resource": case.resource}),
        ]),
    );
    let (source_actions, source) = case.source.unwrap_or((case.actions, case.resource));
    key(
        credentials,
        DENIED_SOURCE,
        policy(&[
            json!({"Effect": "Allow", "Action": "s3:*", "Resource": "*"}),
            json!({"Effect": "Deny", "Action": source_actions, "Resource": source}),
        ]),
    )
}

async fn gateway(
    config: GatewayConfig,
    credentials: StaticCredentials,
) -> Gateway<SigV4Authenticator<StaticCredentials>> {
    let clock = Arc::new(ManualWallClock::new(Duration::from_secs(NOW)));
    gateway_with(config, SigV4Authenticator::new(credentials, clock)).await
}

fn signed(case: &Case, key: &str) -> Request<Body> {
    sdk_signed(
        case.method.clone(),
        case.uri,
        case.headers,
        case.body,
        Signing {
            key,
            secret: SECRET,
            ..Signing::default()
        },
    )
}

fn unsigned(case: &Case) -> Request<Body> {
    let mut headers = vec![("host", HOST)];
    headers.extend_from_slice(case.headers);
    request(
        case.method.clone(),
        case.uri,
        &headers,
        bytes::Bytes::from_static(case.body),
    )
}

#[tokio::test]
async fn requests_outside_the_policy_are_denied_for_every_operation() {
    for case in CASES {
        let name = case.operation;
        assert_eq!(
            s3_actions(name),
            Some(case.actions),
            "{name} needs the actions the table gives"
        );
        assert_eq!(
            source_actions(name),
            case.source.map(|(actions, _)| actions),
            "{name} needs the source actions the table gives"
        );
        let gateway = gateway(config(""), credentials(case)).await;
        let send = async |request| answer(gateway.handle(request).await).await;
        let create = sdk_signed(
            Method::PUT,
            "/bucket",
            &[(MODE_HEADER, "local")],
            b"",
            Signing {
                key: ADMIN,
                secret: SECRET,
                ..Signing::default()
            },
        );
        send(create).await.assert(200, None);

        // The status and error code; an answer to HEAD has no body, so no
        // code.
        let head = case.method == Method::HEAD;
        let outcome = |answer: &Answer| {
            let code = answer.code().filter(|_| !head).map(str::to_owned);
            (answer.status.as_u16(), code)
        };
        let denied = (403, (!head).then(|| "AccessDenied".to_owned()));
        let anonymous = send(unsigned(case)).await;
        assert_eq!(
            outcome(&anonymous),
            denied,
            "{name} anonymous: {anonymous:?}"
        );
        // A key-by-key operation answers for each key.
        let denied = if is_per_key(name) {
            (200, Some("AccessDenied".to_owned()))
        } else {
            denied
        };
        for key in [OTHER_ACTIONS, OTHER_RESOURCES, DENIED, DENIED_SOURCE] {
            let answer = send(signed(case, key)).await;
            assert_eq!(outcome(&answer), denied, "{name} by {key}: {answer:?}");
        }

        let answer = send(signed(case, EXACT)).await;
        let (status, code) = case.allowed;
        assert_eq!(
            outcome(&answer),
            (status, code.filter(|_| !head).map(str::to_owned)),
            "{name} by {EXACT}: {answer:?}"
        );
    }
}

#[tokio::test]
async fn operations_without_an_action_are_denied_to_everyone() {
    let gateway = gateway(config(""), credentials(&CASES[0])).await;
    let torrent = sdk_signed(
        Method::GET,
        "/bucket/k?torrent",
        &[],
        b"",
        Signing {
            key: ADMIN,
            secret: SECRET,
            ..Signing::default()
        },
    );
    let answer = answer(gateway.handle(torrent).await).await;
    answer.assert(403, Some("AccessDenied"));
}

/// DeleteObjects deletes the keys its caller may delete and reports the
/// others, in one answer.
#[tokio::test]
async fn deletes_are_authorized_key_by_key() {
    let case = CASES
        .iter()
        .find(|case| case.operation == "DeleteObjects")
        .unwrap();
    let gateway = gateway(config(""), credentials(case)).await;
    let send = async |method, uri: &str, headers: &[(&str, &str)], body: &'static [u8], key| {
        let signing = Signing {
            key,
            secret: SECRET,
            ..Signing::default()
        };
        answer(
            gateway
                .handle(sdk_signed(method, uri, headers, body, signing))
                .await,
        )
        .await
    };
    send(
        Method::PUT,
        "/bucket",
        &[(MODE_HEADER, "local")],
        b"",
        ADMIN,
    )
    .await
    .assert(200, None);
    for key in ["k", "other"] {
        send(Method::PUT, &format!("/bucket/{key}"), &[], b"data", ADMIN)
            .await
            .assert(200, None);
    }
    let body = b"<Delete><Object><Key>k</Key></Object><Object><Key>other</Key></Object></Delete>";
    let md5 = {
        use base64::Engine as _;
        use md5::Digest as _;
        base64::engine::general_purpose::STANDARD.encode(md5::Md5::digest(body))
    };
    let answer = send(
        Method::POST,
        "/bucket?delete",
        &[("content-md5", &md5)],
        body,
        EXACT,
    )
    .await;
    assert_eq!(answer.status, 200, "{answer:?}");
    assert!(
        answer.body.contains("<Deleted><Key>k</Key></Deleted>"),
        "{answer:?}"
    );
    assert!(
        answer
            .body
            .contains("<Error><Code>AccessDenied</Code><Key>other</Key>"),
        "{answer:?}"
    );
    send(Method::HEAD, "/bucket/k", &[], b"", ADMIN)
        .await
        .assert(404, Some("NoSuchKey"));
    send(Method::HEAD, "/bucket/other", &[], b"", ADMIN)
        .await
        .assert(200, None);
}

/// Authentication answers before authorization: a signed request that
/// repeats a query parameter is refused as malformed (plan M1-07a) whoever
/// signed it, while an unsigned one is refused as anonymous.
#[tokio::test]
async fn authentication_failures_come_before_authorization() {
    let gateway = gateway(config(""), credentials(&CASES[0])).await;
    let uri = "/bucket?location&x=1&x=2";
    for key in [ADMIN, OTHER_ACTIONS] {
        let signing = Signing {
            key,
            secret: SECRET,
            ..Signing::default()
        };
        let repeated = sdk_signed(Method::GET, uri, &[], b"", signing);
        let answer = answer(gateway.handle(repeated).await).await;
        answer.assert(400, Some("InvalidArgument"));
    }
    let anonymous = request(Method::GET, uri, &[("host", HOST)], bytes::Bytes::new());
    let answer = answer(gateway.handle(anonymous).await).await;
    answer.assert(403, Some("AccessDenied"));
}

/// Only the gateway's own authenticator may say who a caller is: an
/// `Authenticated` extension that arrives with the request is discarded.
#[tokio::test]
async fn forged_principals_are_ignored() {
    let gateway = gateway(config(""), credentials(&CASES[0])).await;
    let forged = || {
        let mut forged = request(
            Method::PUT,
            "/forged",
            &[("host", HOST)],
            bytes::Bytes::new(),
        );
        forged.extensions_mut().insert(Authenticated {
            principal: Principal::new("intruder", Permissions::allow_all()),
            access_key_id: ADMIN.to_owned(),
            method: AuthMethod::Header,
        });
        forged
    };
    let answer = answer(gateway.handle(forged()).await).await;
    answer.assert(403, Some("AccessDenied"));

    // With anonymous access on, the request is anonymous, not the forger's.
    let read_only = r#"{"Version": "2012-10-17", "Statement": {"Effect": "Allow", "Action": "s3:ListAllMyBuckets", "Resource": "*"}}"#;
    let config = config(&format!(
        "[identity]\nanonymous_access = true\nanonymous_policy = '{read_only}'\n"
    ));
    let gateway = self::gateway(config, StaticCredentials::new()).await;
    let answer = common::answer(gateway.handle(forged()).await).await;
    answer.assert(403, Some("AccessDenied"));
}

#[tokio::test]
async fn unimplemented_operations_are_authorized_first() {
    let gateway = gateway(config(""), credentials(&CASES[0])).await;
    let tagging = |key| {
        sdk_signed(
            Method::GET,
            "/bucket?tagging",
            &[],
            b"",
            Signing {
                key,
                secret: SECRET,
                ..Signing::default()
            },
        )
    };
    let denied = answer(gateway.handle(tagging(EXACT)).await).await;
    denied.assert(403, Some("AccessDenied"));
    let allowed = answer(gateway.handle(tagging(ADMIN)).await).await;
    allowed.assert(501, Some("NotImplemented"));
}

#[tokio::test]
async fn anonymous_requests_follow_the_anonymous_policy() {
    let read_only = r#"{"Version": "2012-10-17", "Statement": {"Effect": "Allow", "Action": ["s3:ListAllMyBuckets", "s3:ListBucket"], "Resource": "*"}}"#;
    let config = config(&format!(
        "[identity]\nanonymous_access = true\nanonymous_policy = '{read_only}'\n"
    ));
    let gateway = gateway(config, StaticCredentials::new()).await;
    let send = async |method, uri: &str, headers: &[(&str, &str)]| {
        let request = request(method, uri, headers, bytes::Bytes::new());
        answer(gateway.handle(request).await).await
    };
    send(Method::GET, "/", &[]).await.assert(200, None);
    send(Method::PUT, "/new-bucket", &[])
        .await
        .assert(403, Some("AccessDenied"));
    send(Method::HEAD, "/new-bucket", &[])
        .await
        .assert(404, Some("NoSuchBucket"));
}

#[tokio::test]
async fn static_credentials_load_from_the_configuration() {
    let dir = tempfile::tempdir().unwrap();
    let admin_secret = dir.path().join("admin.secret");
    let reader_secret = dir.path().join("reader.secret");
    let admin_key = "0123456789abcdef0123456789abcdef";
    let reader_key = "fedcba9876543210fedcba9876543210";
    std::fs::write(&admin_secret, format!("{admin_key}\n")).unwrap();
    std::fs::write(&reader_secret, reader_key).unwrap();
    let identity = format!(
        r#"
[identity.static_credentials.bootstrap]
access_key_id = "AKIASKYS3BOOTSTRAP"
secret_access_key_file = "{}"
policy = '{{"Version": "2012-10-17", "Statement": {{"Effect": "Allow", "Action": "*", "Resource": "*"}}}}'

[identity.static_credentials.reader]
access_key_id = "AKIASKYS3READER000"
secret_access_key_file = "{}"
policy = '{{"Version": "2012-10-17", "Statement": {{"Effect": "Allow", "Action": "s3:List*", "Resource": "*"}}}}'
"#,
        admin_secret.display(),
        reader_secret.display()
    );
    let text = format!(
        "[cluster]\ncluster_id = \"test\"\n[control_store]\n\
         etcd_endpoints = [\"https://etcd.invalid:2379\"]\n{identity}"
    );
    let node: skys3_config::Config = text.parse().unwrap();
    let credentials = StaticCredentials::load(node.identity()).unwrap();
    assert_eq!(credentials.len(), 2);
    let debug = format!("{credentials:?}");
    assert!(!debug.contains(admin_key), "{debug}");
    let gateway = gateway(config(&identity), credentials).await;
    let send = async |method, uri: &str, key, secret| {
        let signing = Signing {
            key,
            secret,
            ..Signing::default()
        };
        let request = sdk_signed(method, uri, &[(MODE_HEADER, "local")], b"", signing);
        answer(gateway.handle(request).await).await
    };
    send(Method::PUT, "/bucket", "AKIASKYS3BOOTSTRAP", admin_key)
        .await
        .assert(200, None);
    send(Method::GET, "/", "AKIASKYS3READER000", reader_key)
        .await
        .assert(200, None);
    send(
        Method::PUT,
        "/other-bucket",
        "AKIASKYS3READER000",
        reader_key,
    )
    .await
    .assert(403, Some("AccessDenied"));
    send(Method::GET, "/", "AKIASKYS3READER000", admin_key)
        .await
        .assert(403, Some("SignatureDoesNotMatch"));

    // A missing file and a short secret are refused when loading.
    std::fs::write(&reader_secret, "short").unwrap();
    let error = StaticCredentials::load(node.identity()).unwrap_err();
    assert!(error.to_string().contains("reader"), "{error}");
    std::fs::remove_file(&admin_secret).unwrap();
    let error = StaticCredentials::load(node.identity()).unwrap_err();
    assert!(error.to_string().contains("bootstrap"), "{error}");
}
