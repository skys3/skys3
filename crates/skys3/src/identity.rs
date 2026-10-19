//! The operator's management of the identity registers (design §6.1, §11):
//! `skys3 control identity put` and `skys3 control identity delete`.
//!
//! OIDC providers and roles live in the control store under `identity/`,
//! and every node follows them into its local copy (§6.2). An operator
//! writes them with the node binary itself, which reaches the control store
//! with the configuration and credentials the nodes use, rather than
//! through a node's admin API: the registers are cluster state, a write
//! needs no node to be running, and the store's own access control is the
//! only one that guards them (design §6.1; plan section 14 assigns the
//! decision to M8-01).
//!
//! `put` reads an [`IdentityFile`], a JSON document of named providers and
//! roles:
//!
//! ```json
//! {
//!   "providers": {
//!     "ci": {"issuer": "https://token.example", "audiences": ["sts.amazonaws.com"]}
//!   },
//!   "roles": {
//!     "builder": {
//!       "trust_policy": {"Version": "2012-10-17", "Statement": "..."},
//!       "policies": [{"Version": "2012-10-17", "Statement": "..."}]
//!     }
//!   }
//! }
//! ```
//!
//! A provider has the fields of its register without `proposal_id`
//! (`issuer`, `audiences`, and optionally `authorized_parties` and
//! `algorithms`). A role's policies are JSON objects, or JSON documents in
//! strings as the registers hold them. Every entry is checked before
//! anything is written, and each named register is then created or
//! replaced; a register whose content is already the same is left alone,
//! and registers the file does not name are kept. `delete` removes the
//! named registers. Either one then increments the generation in
//! `cluster.json` once, which announces the change to every node (§6.2).
//!
//! Writes use the control store's conditional writes
//! ([`propose`](skys3_control::propose)), so a concurrent writer is never
//! overwritten blindly: a register that changed between its read and the
//! write is read again and written again.
//!
//! **Backends.** [`put`] and [`delete`] run on any [`ControlStore`]. The
//! command opens the store the configuration names; this build opens the
//! file store only, the one its nodes run with. That store is locked by
//! the node it serves and read when the node starts, so the command runs
//! while the node is stopped, after its first start created the store,
//! and the node follows the change when it starts again.

use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;
use skys3_config::{Config, ControlStoreBackend};
use skys3_control::{
    ControlError, ControlStore, DeletionOutcome, Expected, KeyError, KeyPrefix, ProposalIds,
    ProposalOutcome, RegisterKey, RetryPolicy, TypedKey, get_with_retries, propose_delete,
    propose_document, read_cluster,
};
use skys3_io::BlockingPool;
use skys3_sts::identity::{PROVIDERS_PREFIX, provider_key, role_key};
use skys3_sts::{OidcProvider, ProviderDocument};
use skys3_types::policy::trust::TrustPolicy;
use skys3_types::policy::{Policy, PolicyDocument};
use skys3_types::{ClusterId, Generation, ProposalId, RegisterDocument, RoleDocument};

use crate::control::{OpenStoreError, StoreOwner, open_file_store};
use crate::datadir::{self, DataDir, DataDirError};

/// The largest identity file `put` reads.
pub const MAX_FILE_BYTES: u64 = 1 << 20;

/// Why an identity command did not complete.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum IdentityError {
    /// The identity file is not valid JSON of the expected shape.
    #[error("the identity file: {0}")]
    File(String),
    /// An entry of the file breaks a rule of its register.
    #[error("{kind} {name:?}: {reason}")]
    Invalid {
        /// `provider` or `role`.
        kind: &'static str,
        /// The entry's name.
        name: String,
        /// The rule it breaks.
        reason: String,
    },
    /// A provider the file does not name already allowlists one of its
    /// issuers. Nodes ignore an issuer several providers name, so writing
    /// it would disable both.
    #[error(
        "issuer {issuer:?} is already allowlisted by provider register {key}; delete or \
         rename one of them"
    )]
    IssuerTaken {
        /// The issuer.
        issuer: String,
        /// The register that names it.
        key: RegisterKey,
    },
    /// A register kept changing under the command's writes.
    #[error("{0} kept changing while it was written; try again")]
    Contended(RegisterKey),
    /// The control store failed or refused a request.
    #[error("the control store: {0}")]
    Store(#[from] ControlError),
    /// The control store could not be opened.
    #[error("the control store: {0}")]
    Open(#[from] OpenStoreError),
    /// The node's data directory could not be opened.
    #[error(transparent)]
    DataDir(#[from] DataDirError),
    /// The command does not apply to this configuration or node.
    #[error("{0}")]
    Refused(String),
}

/// The named providers and roles of an identity file.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IdentityFile {
    /// Providers by register name.
    pub providers: BTreeMap<String, OidcProvider>,
    /// Roles by name.
    pub roles: BTreeMap<String, RoleSpec>,
}

/// A role as an identity file gives it: its register without the
/// `proposal_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleSpec {
    /// Who may assume the role.
    pub trust_policy: PolicyDocument<TrustPolicy>,
    /// What its sessions may do.
    pub policies: Vec<PolicyDocument<Policy>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFile {
    #[serde(default)]
    providers: BTreeMap<String, OidcProvider>,
    #[serde(default)]
    roles: BTreeMap<String, RawRole>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRole {
    trust_policy: serde_json::Value,
    #[serde(default)]
    policies: Vec<serde_json::Value>,
}

/// The text of a policy given as a JSON object or as a JSON string that
/// holds one.
fn policy_text(value: serde_json::Value) -> String {
    match value {
        serde_json::Value::String(text) => text,
        other => other.to_string(),
    }
}

impl IdentityFile {
    /// Parses and checks an identity file: every provider and role must be
    /// a valid register, and no two providers may name one issuer.
    ///
    /// # Errors
    ///
    /// [`IdentityError::File`] for a file that does not parse, and
    /// [`IdentityError::Invalid`] for the first entry that breaks a rule.
    pub fn parse(bytes: &[u8]) -> Result<Self, IdentityError> {
        let raw: RawFile = serde_json::from_slice(bytes)
            .map_err(|error| IdentityError::File(error.to_string()))?;
        let invalid = |kind, name: &str, reason: String| IdentityError::Invalid {
            kind,
            name: name.to_owned(),
            reason,
        };
        let mut issuers = BTreeMap::new();
        for (name, provider) in &raw.providers {
            provider_key(name).map_err(|error| invalid("provider", name, error.to_string()))?;
            provider
                .check()
                .map_err(|error| invalid("provider", name, error.to_string()))?;
            if let Some(other) = issuers.insert(provider.issuer.as_str(), name) {
                return Err(invalid(
                    "provider",
                    name,
                    format!("provider {other:?} names the same issuer"),
                ));
            }
        }
        let mut roles = BTreeMap::new();
        for (name, role) in raw.roles {
            if !RoleDocument::valid_name(&name) {
                return Err(invalid(
                    "role",
                    &name,
                    "a role name is 1 to 64 ASCII letters, digits, `_`, `-`, and `.`, not \
                     starting with `.`"
                        .to_owned(),
                ));
            }
            let trust_policy = PolicyDocument::parse(&policy_text(role.trust_policy))
                .map_err(|error| invalid("role", &name, format!("trust policy: {error}")))?;
            let policies = role
                .policies
                .into_iter()
                .enumerate()
                .map(|(i, policy)| {
                    PolicyDocument::parse(&policy_text(policy))
                        .map_err(|error| invalid("role", &name, format!("policy {i}: {error}")))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let spec = RoleSpec {
                trust_policy,
                policies,
            };
            spec.document(ProposalId::from_u128(0))
                .validate()
                .map_err(|error| invalid("role", &name, error.to_string()))?;
            roles.insert(name, spec);
        }
        Ok(Self {
            providers: raw.providers,
            roles,
        })
    }

    /// Whether the file names nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.providers.is_empty() && self.roles.is_empty()
    }
}

impl RoleSpec {
    /// The role's register document, written under `proposal_id`.
    #[must_use]
    pub fn document(&self, proposal_id: ProposalId) -> RoleDocument {
        RoleDocument {
            trust_policy: self.trust_policy.clone(),
            policies: self.policies.clone(),
            proposal_id,
        }
    }
}

/// What a command did to one register.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Change {
    /// The register was written; it did not exist.
    Created,
    /// The register was written over another value.
    Replaced,
    /// The register already held the same content and was left alone.
    Unchanged,
    /// The register was deleted.
    Deleted,
    /// The register to delete did not exist.
    Absent,
}

impl Change {
    /// The change in words, for the operator.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Change::Created => "created",
            Change::Replaced => "replaced",
            Change::Unchanged => "unchanged",
            Change::Deleted => "deleted",
            Change::Absent => "absent",
        }
    }

    fn wrote(self) -> bool {
        matches!(self, Change::Created | Change::Replaced | Change::Deleted)
    }
}

/// What [`put`] or [`delete`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Applied {
    /// Each register the command named, in the order it was handled.
    pub changes: Vec<(RegisterKey, Change)>,
    /// The generation that announces the changes, or `None` if nothing
    /// changed.
    pub generation: Option<Generation>,
}

/// A register document whose content can be compared without its
/// `proposal_id`.
trait Content: RegisterDocument + Sized {
    fn same_content(&self, other: &Self) -> bool;
}

impl Content for ProviderDocument {
    fn same_content(&self, other: &Self) -> bool {
        self.provider() == other.provider()
    }
}

impl Content for RoleDocument {
    fn same_content(&self, other: &Self) -> bool {
        let texts = |role: &RoleDocument| {
            (
                role.trust_policy.text().to_owned(),
                role.policies
                    .iter()
                    .map(|policy| policy.text().to_owned())
                    .collect::<Vec<_>>(),
            )
        };
        texts(self) == texts(other)
    }
}

/// Writes every provider and role of `file` into `store`, the control
/// store of `cluster`, and announces the change (see the [module
/// documentation](self)).
///
/// # Errors
///
/// [`IdentityError::IssuerTaken`] if a provider register the file does not
/// name allowlists one of its issuers, [`IdentityError::Contended`] if a
/// register keeps changing, and [`IdentityError::Store`] if the store
/// fails or is not `cluster`'s. Registers written before an error stay
/// written; running the command again finishes the job.
pub async fn put<C: ControlStore>(
    store: &C,
    cluster: &ClusterId,
    file: &IdentityFile,
    ids: &mut ProposalIds,
    retry: &RetryPolicy,
) -> Result<Applied, IdentityError> {
    read_cluster(store, cluster, retry).await?;
    check_issuers(store, file, retry).await?;
    let mut changes = Vec::new();
    for (name, provider) in &file.providers {
        let key = provider_key(name).map_err(|error| invalid_key("provider", name, &error))?;
        let change = write(store, &key, ids, retry, |id| {
            ProviderDocument::new(provider.clone(), id)
        })
        .await?;
        changes.push((key.key().clone(), change));
    }
    for (name, role) in &file.roles {
        let key = role_key(name).map_err(|error| invalid_key("role", name, &error))?;
        let change = write(store, &key, ids, retry, |id| role.document(id)).await?;
        changes.push((key.key().clone(), change));
    }
    announce(store, cluster, ids, retry, changes).await
}

/// Deletes the provider registers `providers` and the roles `roles` from
/// `store`, the control store of `cluster`, and announces the change.
///
/// # Errors
///
/// [`IdentityError::Invalid`] for a name that cannot be a register's,
/// [`IdentityError::Contended`] if a register keeps changing, and
/// [`IdentityError::Store`] if the store fails or is not `cluster`'s.
pub async fn delete<C: ControlStore>(
    store: &C,
    cluster: &ClusterId,
    providers: &[String],
    roles: &[String],
    ids: &mut ProposalIds,
    retry: &RetryPolicy,
) -> Result<Applied, IdentityError> {
    let mut keys = Vec::new();
    for name in providers {
        let key = provider_key(name).map_err(|error| invalid_key("provider", name, &error))?;
        keys.push(key.key().clone());
    }
    for name in roles {
        let key = role_key(name).map_err(|error| invalid_key("role", name, &error))?;
        keys.push(key.key().clone());
    }
    read_cluster(store, cluster, retry).await?;
    let mut changes = Vec::new();
    for key in keys {
        let change = remove(store, &key, retry).await?;
        changes.push((key, change));
    }
    announce(store, cluster, ids, retry, changes).await
}

/// The error for a `kind` entry whose `name` cannot be part of a register
/// key.
fn invalid_key(kind: &'static str, name: &str, error: &KeyError) -> IdentityError {
    IdentityError::Invalid {
        kind,
        name: name.to_owned(),
        reason: error.to_string(),
    }
}

/// Refuses a file whose issuer a provider register it does not name
/// already allowlists. Registers that do not parse name no issuer a node
/// would use.
async fn check_issuers<C: ControlStore>(
    store: &C,
    file: &IdentityFile,
    retry: &RetryPolicy,
) -> Result<(), IdentityError> {
    let issuers: BTreeSet<&str> = file
        .providers
        .values()
        .map(|provider| provider.issuer.as_str())
        .collect();
    let named: BTreeSet<RegisterKey> = file
        .providers
        .keys()
        .filter_map(|name| provider_key(name).ok().map(|key| key.key().clone()))
        .collect();
    for (key, _) in store.list(&KeyPrefix::identity()).await? {
        if !key.as_str().starts_with(PROVIDERS_PREFIX) || named.contains(&key) {
            continue;
        }
        let Some(current) = get_with_retries(store, &key, retry).await? else {
            continue;
        };
        if let Ok(document) = ProviderDocument::from_json(&current.value)
            && issuers.contains(document.issuer.as_str())
        {
            return Err(IdentityError::IssuerTaken {
                issuer: document.issuer,
                key,
            });
        }
    }
    Ok(())
}

/// Creates or replaces the register at `key` with the document `build`
/// makes for a fresh proposal ID, unless it already holds the same
/// content. A register that changes between the read and the write is
/// read again, at most `retry.max_attempts` times.
async fn write<D: Content, C: ControlStore>(
    store: &C,
    key: &TypedKey<D>,
    ids: &mut ProposalIds,
    retry: &RetryPolicy,
    build: impl Fn(ProposalId) -> D,
) -> Result<Change, IdentityError> {
    for _ in 0..retry.max_attempts {
        let document = build(ids.next_id());
        let (expected, change) = match get_with_retries(store, key.key(), retry).await? {
            None => (Expected::Absent, Change::Created),
            Some(current) => {
                // A register that does not parse is replaced like any other.
                if D::from_json(&current.value)
                    .is_ok_and(|existing| existing.same_content(&document))
                {
                    return Ok(Change::Unchanged);
                }
                (Expected::Version(current.version), Change::Replaced)
            }
        };
        match propose_document(store, key, expected, &document, retry).await? {
            ProposalOutcome::Accepted(_) => return Ok(change),
            ProposalOutcome::Rejected => {}
        }
    }
    Err(IdentityError::Contended(key.key().clone()))
}

/// Deletes the register at `key`, whatever it holds.
async fn remove<C: ControlStore>(
    store: &C,
    key: &RegisterKey,
    retry: &RetryPolicy,
) -> Result<Change, IdentityError> {
    for _ in 0..retry.max_attempts {
        let Some(current) = get_with_retries(store, key, retry).await? else {
            return Ok(Change::Absent);
        };
        match propose_delete(store, key, &current.version, retry).await? {
            DeletionOutcome::Deleted => return Ok(Change::Deleted),
            DeletionOutcome::Rejected => {}
        }
    }
    Err(IdentityError::Contended(key.clone()))
}

/// Increments the generation once if any of `changes` wrote something.
async fn announce<C: ControlStore>(
    store: &C,
    cluster: &ClusterId,
    ids: &mut ProposalIds,
    retry: &RetryPolicy,
    changes: Vec<(RegisterKey, Change)>,
) -> Result<Applied, IdentityError> {
    let generation = if changes.iter().any(|(_, change)| change.wrote()) {
        Some(skys3_control::bump_generation(store, cluster, ids, retry).await?)
    } else {
        None
    };
    Ok(Applied {
        changes,
        generation,
    })
}

/// What the command asks of the identity registers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    /// Create or replace what the file names.
    Put(IdentityFile),
    /// Delete these providers and roles.
    Delete {
        /// Provider register names.
        providers: Vec<String>,
        /// Role names.
        roles: Vec<String>,
    },
}

/// Runs `request` against the control store `config` names
/// (`skys3 control identity`).
///
/// # Errors
///
/// [`IdentityError::Refused`] for a backend this build does not open or a
/// data directory that holds no node yet, [`IdentityError::DataDir`] if
/// the node is running, and the errors of [`put`] and [`delete`].
pub async fn run(config: &Config, request: &Request) -> Result<Applied, IdentityError> {
    let ControlStoreBackend::File { directory } = &config.control_store().backend else {
        return Err(IdentityError::Refused(
            "this build opens only the file control store, the only one its nodes run with"
                .to_owned(),
        ));
    };
    let node_file = config.node().data_dir.join(datadir::NODE_FILE);
    if !node_file.exists() {
        return Err(IdentityError::Refused(format!(
            "{} holds no node: start the node once, so that it creates its control store, \
             then stop it and run this again",
            config.node().data_dir.display()
        )));
    }
    let cluster = config.cluster().cluster_id.clone();
    let pool = BlockingPool::new("control", std::num::NonZeroUsize::MIN)
        .map_err(|error| IdentityError::Store(ControlError::Io(error)))?;
    let result = async {
        let data_dir = {
            let (node, cluster) = (config.node().clone(), cluster.clone());
            let boot_id = datadir::boot_id();
            pool.run(move || DataDir::open(&node, &cluster, boot_id.as_deref()))
                .await
                .map_err(|error| ControlError::Io(error.into()))??
        };
        let owner = StoreOwner::new(
            cluster.clone(),
            data_dir.node_id().clone(),
            data_dir.instance_id().to_owned(),
        );
        let store = open_file_store(directory, &owner, true, &pool).await?.store;
        let (mut ids, retry) = (ProposalIds::from_os_rng(), RetryPolicy::default());
        let applied = match request {
            Request::Put(file) => put(&store, &cluster, file, &mut ids, &retry).await,
            Request::Delete { providers, roles } => {
                delete(&store, &cluster, providers, roles, &mut ids, &retry).await
            }
        };
        drop(store);
        drop(data_dir);
        applied
    }
    .await;
    pool.shutdown();
    result
}

/// Reads an identity file of at most [`MAX_FILE_BYTES`] bytes.
///
/// # Errors
///
/// [`IdentityError::File`] if it cannot be read, is too large, or is not a
/// valid identity file, and [`IdentityError::Invalid`] for an invalid
/// entry.
pub fn read_file(path: &std::path::Path) -> Result<IdentityFile, IdentityError> {
    use std::io::Read;
    let unreadable =
        |error: std::io::Error| IdentityError::File(format!("{}: {error}", path.display()));
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .map_err(unreadable)?
        .take(MAX_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(unreadable)?;
    if bytes.len() as u64 > MAX_FILE_BYTES {
        return Err(IdentityError::File(format!(
            "{} is larger than {MAX_FILE_BYTES} bytes",
            path.display()
        )));
    }
    IdentityFile::parse(&bytes)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use serde_json::json;
    use skys3_control::{MemoryControlStore, PutOutcome, bootstrap};
    use skys3_io::SystemWallClock;
    use skys3_sts::IdentityCopy;

    use super::*;

    const ISSUER: &str = "https://token.example";

    fn trust() -> serde_json::Value {
        json!({
            "Version": "2012-10-17",
            "Statement": {
                "Effect": "Allow",
                "Principal": {"Federated": ISSUER},
                "Action": "sts:AssumeRoleWithWebIdentity",
                "Condition": {"StringEquals": {"token.example:aud": "sts.amazonaws.com"}}
            }
        })
    }

    fn allow(resource: &str) -> serde_json::Value {
        json!({
            "Version": "2012-10-17",
            "Statement": {"Effect": "Allow", "Action": "s3:*", "Resource": resource}
        })
    }

    /// An identity file with one provider and two roles, one of whose
    /// policies is given as a string.
    fn file_json() -> serde_json::Value {
        json!({
            "providers": {"ci": {"issuer": ISSUER, "audiences": ["sts.amazonaws.com"]}},
            "roles": {
                "builder": {"trust_policy": trust(), "policies": [allow("arn:aws:s3:::b/*")]},
                "reader": {
                    "trust_policy": trust().to_string(),
                    "policies": [allow("arn:aws:s3:::r/*").to_string()]
                }
            }
        })
    }

    fn parse(value: &serde_json::Value) -> Result<IdentityFile, IdentityError> {
        IdentityFile::parse(value.to_string().as_bytes())
    }

    async fn bootstrapped() -> (MemoryControlStore, ClusterId, ProposalIds) {
        let store = MemoryControlStore::new();
        let cluster = ClusterId::new("c").unwrap();
        let mut ids = ProposalIds::seeded(5);
        bootstrap(&store, &cluster, ids.next_id(), &RetryPolicy::default())
            .await
            .unwrap();
        (store, cluster, ids)
    }

    fn summary(applied: &Applied) -> Vec<(String, Change)> {
        applied
            .changes
            .iter()
            .map(|(key, change)| (key.as_str().to_owned(), *change))
            .collect()
    }

    #[test]
    fn files_are_parsed_and_checked() {
        let file = parse(&file_json()).unwrap();
        assert_eq!(file.providers["ci"].issuer, ISSUER);
        assert_eq!(file.roles.len(), 2);
        // A policy given as an object and one given as a string are the
        // same document.
        assert_eq!(
            file.roles["builder"].trust_policy.text(),
            file.roles["reader"].trust_policy.text()
        );
        assert!(!file.is_empty());
        assert!(parse(&json!({})).unwrap().is_empty());

        let refused = |value: serde_json::Value| match parse(&value) {
            Err(error) => error.to_string(),
            Ok(file) => panic!("{value} parsed: {file:?}"),
        };
        assert!(refused(json!({"roles": {}, "extra": 1})).starts_with("the identity file"));
        assert!(
            refused(json!({"providers": {"a": {"issuer": "ftp://x", "audiences": ["a"]}}}))
                .starts_with("provider \"a\"")
        );
        assert!(
            refused(json!({"providers": {"a/../b": {"issuer": ISSUER, "audiences": ["a"]}}}))
                .starts_with("provider \"a/../b\"")
        );
        let twice = json!({"providers": {
            "a": {"issuer": ISSUER, "audiences": ["x"]},
            "b": {"issuer": ISSUER, "audiences": ["y"]},
        }});
        assert!(refused(twice).contains("names the same issuer"));
        assert!(refused(json!({"roles": {".r": {"trust_policy": trust()}}})).contains("role name"));
        assert!(
            refused(json!({"roles": {"r": {"trust_policy": {"Version": 1}}}}))
                .contains("trust policy")
        );
        assert!(
            refused(json!({"roles": {"r": {"trust_policy": trust(), "policies": [1]}}}))
                .contains("policy 0")
        );
        let many: Vec<_> = (0..=RoleDocument::MAX_POLICIES)
            .map(|_| allow("*"))
            .collect();
        assert!(
            refused(json!({"roles": {"r": {"trust_policy": trust(), "policies": many}}}))
                .starts_with("role \"r\"")
        );
    }

    #[tokio::test]
    async fn put_creates_replaces_and_announces_once() {
        let (store, cluster, mut ids) = bootstrapped().await;
        let retry = RetryPolicy::default();
        let file = parse(&file_json()).unwrap();
        let applied = put(&store, &cluster, &file, &mut ids, &retry)
            .await
            .unwrap();
        assert_eq!(
            summary(&applied),
            [
                ("identity/providers/ci.json".to_owned(), Change::Created),
                ("identity/roles/builder.json".to_owned(), Change::Created),
                ("identity/roles/reader.json".to_owned(), Change::Created),
            ]
        );
        assert_eq!(applied.generation, Some(Generation::new(2)));

        // What a node's copy reads is what the file says.
        let copy = IdentityCopy::new(Arc::new(SystemWallClock), Duration::from_secs(60));
        let snapshot = copy.sync(&store, &retry).await.unwrap();
        assert_eq!(snapshot.providers(), [file.providers["ci"].clone()]);
        assert!(snapshot.role("builder").is_some() && snapshot.role("reader").is_some());

        // The same file again changes nothing and announces nothing.
        let again = put(&store, &cluster, &file, &mut ids, &retry)
            .await
            .unwrap();
        assert!(again.changes.iter().all(|(_, c)| *c == Change::Unchanged));
        assert_eq!(again.generation, None);

        // A changed role is replaced; the others are left alone.
        let mut changed = file.clone();
        changed.roles.get_mut("reader").unwrap().policies.clear();
        changed.providers.clear();
        let applied = put(&store, &cluster, &changed, &mut ids, &retry)
            .await
            .unwrap();
        assert_eq!(
            summary(&applied),
            [
                ("identity/roles/builder.json".to_owned(), Change::Unchanged),
                ("identity/roles/reader.json".to_owned(), Change::Replaced),
            ]
        );
        assert_eq!(applied.generation, Some(Generation::new(3)));
        let snapshot = copy.sync(&store, &retry).await.unwrap();
        assert_eq!(
            snapshot.providers().len(),
            1,
            "providers not named are kept"
        );
    }

    #[tokio::test]
    async fn an_invalid_register_is_replaced() {
        let (store, cluster, mut ids) = bootstrapped().await;
        let key = RegisterKey::new("identity/roles/builder.json").unwrap();
        let written = store
            .put_if(&key, Expected::Absent, Bytes::from_static(b"not json"))
            .await
            .unwrap();
        assert!(matches!(written, PutOutcome::Written(_)));
        let file = parse(&file_json()).unwrap();
        let applied = put(&store, &cluster, &file, &mut ids, &RetryPolicy::default())
            .await
            .unwrap();
        assert!(applied.changes.contains(&(key, Change::Replaced)));
    }

    #[tokio::test]
    async fn an_issuer_another_provider_names_is_refused() {
        let (store, cluster, mut ids) = bootstrapped().await;
        let retry = RetryPolicy::default();
        let file = parse(&file_json()).unwrap();
        put(&store, &cluster, &file, &mut ids, &retry)
            .await
            .unwrap();
        // An invalid provider register names no issuer and is ignored.
        let junk = RegisterKey::new("identity/providers/junk.json").unwrap();
        let written = store
            .put_if(&junk, Expected::Absent, Bytes::from_static(b"{}"))
            .await
            .unwrap();
        assert!(matches!(written, PutOutcome::Written(_)));
        let renamed = parse(&json!({
            "providers": {"other": {"issuer": ISSUER, "audiences": ["x"]}}
        }))
        .unwrap();
        let error = put(&store, &cluster, &renamed, &mut ids, &retry)
            .await
            .unwrap_err();
        assert!(
            matches!(&error, IdentityError::IssuerTaken { key, .. }
                if key.as_str() == "identity/providers/ci.json"),
            "{error}"
        );
        // Replacing the provider under its own name is fine.
        let same_name = parse(&json!({
            "providers": {"ci": {"issuer": ISSUER, "audiences": ["x"]}}
        }))
        .unwrap();
        let applied = put(&store, &cluster, &same_name, &mut ids, &retry)
            .await
            .unwrap();
        assert_eq!(applied.changes[0].1, Change::Replaced);
    }

    #[tokio::test]
    async fn delete_removes_named_registers() {
        let (store, cluster, mut ids) = bootstrapped().await;
        let retry = RetryPolicy::default();
        let file = parse(&file_json()).unwrap();
        put(&store, &cluster, &file, &mut ids, &retry)
            .await
            .unwrap();
        let applied = delete(
            &store,
            &cluster,
            &["ci".to_owned()],
            &["reader".to_owned(), "gone".to_owned()],
            &mut ids,
            &retry,
        )
        .await
        .unwrap();
        assert_eq!(
            summary(&applied),
            [
                ("identity/providers/ci.json".to_owned(), Change::Deleted),
                ("identity/roles/reader.json".to_owned(), Change::Deleted),
                ("identity/roles/gone.json".to_owned(), Change::Absent),
            ]
        );
        assert!(applied.generation.is_some());
        let left: Vec<_> = store
            .list(&KeyPrefix::identity())
            .await
            .unwrap()
            .into_iter()
            .map(|(key, _)| key.as_str().to_owned())
            .collect();
        assert_eq!(left, ["identity/roles/builder.json"]);

        let nothing = delete(
            &store,
            &cluster,
            &[],
            &["gone".to_owned()],
            &mut ids,
            &retry,
        )
        .await
        .unwrap();
        assert_eq!(nothing.generation, None);
        let error = delete(
            &store,
            &cluster,
            &["a/../b".to_owned()],
            &[],
            &mut ids,
            &retry,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            IdentityError::Invalid {
                kind: "provider",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn another_cluster_s_store_is_refused() {
        let (store, _, mut ids) = bootstrapped().await;
        let other = ClusterId::new("other").unwrap();
        let file = parse(&file_json()).unwrap();
        let error = put(&store, &other, &file, &mut ids, &RetryPolicy::default())
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            IdentityError::Store(ControlError::ClusterMismatch { .. })
        ));
        let identity = store.list(&KeyPrefix::identity()).await.unwrap();
        assert!(identity.is_empty());
    }

    #[test]
    fn files_are_read_within_their_bound() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("identity.json");
        std::fs::write(&path, file_json().to_string()).unwrap();
        assert_eq!(read_file(&path).unwrap().roles.len(), 2);
        let large = vec![b' '; usize::try_from(MAX_FILE_BYTES).unwrap() + 1];
        std::fs::write(&path, large).unwrap();
        assert!(read_file(&path).unwrap_err().to_string().contains("larger"));
        let missing = dir.path().join("missing.json");
        assert!(matches!(read_file(&missing), Err(IdentityError::File(_))));
    }

    #[test]
    fn changes_have_names() {
        let names = [
            Change::Created,
            Change::Replaced,
            Change::Unchanged,
            Change::Deleted,
            Change::Absent,
        ]
        .map(Change::as_str);
        assert_eq!(
            names,
            ["created", "replaced", "unchanged", "deleted", "absent"]
        );
    }
}
