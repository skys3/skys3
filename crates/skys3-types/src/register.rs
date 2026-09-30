//! Control-store register documents (§6.1).
//!
//! Every backend stores the same registers:
//!
//! | Register | Document |
//! |---|---|
//! | `cluster.json` | [`ClusterDocument`] |
//! | `coordinator.lease` | [`CoordinatorLease`] |
//! | `nodes/<node-id>.json` | [`NodeRegistration`] |
//! | `buckets/<bucket>.json` | [`BucketDocument`] |
//! | `shards/<bucket>/<n>.json` | [`ShardConfig`] |
//!
//! Identity registers (`identity/`) are defined with the authorization
//! model.
//!
//! Each document is a JSON object whose fields are written in declaration
//! order. Readers reject unknown fields: the format version in
//! `cluster.json` covers every register, so a new field comes with a new
//! format version. Every document carries the [`ProposalId`] of the write
//! that stored it, for the lost-response rule (§6.1).
//!
//! [`RegisterDocument::from_json`] and [`RegisterDocument::to_json`] check a
//! document's invariants, so a document that breaks one is never stored or
//! acted on.

use std::collections::BTreeSet;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::{
    BucketId, BucketName, ClusterId, Epoch, Generation, Label, NodeAddress, NodeId, ProposalId,
    ShardCount, ShardId,
};

/// A control-store register document.
pub trait RegisterDocument: Serialize + DeserializeOwned {
    /// What the document is, as error messages name it.
    const KIND: &'static str;

    /// The proposal ID of the write that stored this document.
    fn proposal_id(&self) -> &ProposalId;

    /// Checks the invariants that the field types alone do not.
    ///
    /// # Errors
    ///
    /// Returns the first [`InvalidRegister`] problem found.
    fn validate(&self) -> Result<(), InvalidRegister>;

    /// Parses and validates a register value.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterError::Json`] if `bytes` is not a well-formed
    /// document, and [`RegisterError::Invalid`] if it breaks an invariant.
    fn from_json(bytes: &[u8]) -> Result<Self, RegisterError> {
        let document: Self =
            serde_json::from_slice(bytes).map_err(|source| RegisterError::Json {
                kind: Self::KIND,
                source,
            })?;
        document
            .validate()
            .map_err(|source| RegisterError::Invalid {
                kind: Self::KIND,
                source,
            })?;
        Ok(document)
    }

    /// Validates the document and serializes it as compact JSON.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterError::Invalid`] if the document breaks an
    /// invariant.
    fn to_json(&self) -> Result<Vec<u8>, RegisterError> {
        self.validate().map_err(|source| RegisterError::Invalid {
            kind: Self::KIND,
            source,
        })?;
        serde_json::to_vec(self).map_err(|source| RegisterError::Json {
            kind: Self::KIND,
            source,
        })
    }
}

/// A register value that could not be read or written.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RegisterError {
    /// The value is not well-formed JSON for the document, or a field is
    /// missing, unknown, or of the wrong type or format.
    #[error("malformed {kind} document: {source}")]
    Json {
        /// The document kind.
        kind: &'static str,
        /// The parser's error.
        #[source]
        source: serde_json::Error,
    },
    /// The document breaks an invariant.
    #[error("invalid {kind} document: {source}")]
    Invalid {
        /// The document kind.
        kind: &'static str,
        /// The invariant broken.
        #[source]
        source: InvalidRegister,
    },
}

/// An invariant a register document breaks.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum InvalidRegister {
    /// `cluster.json` has a format version this build does not support.
    #[error("format version {found} is not supported; this build supports version {supported}")]
    UnsupportedFormatVersion {
        /// The version found.
        found: u32,
        /// The version this build reads and writes.
        supported: u32,
    },
    /// The replication settings are inconsistent.
    #[error(transparent)]
    Replication(#[from] ReplicationError),
    /// A shard configuration has no members.
    #[error("the configuration has no members")]
    NoMembers,
    /// A shard's primary is not one of its members.
    #[error("primary {0} is not a member")]
    PrimaryNotMember(NodeId),
    /// A node is listed twice among a shard's members or learners.
    #[error("node {0} is listed twice")]
    DuplicateNode(NodeId),
    /// A node is both a member and a learner of a shard.
    #[error("node {0} is both a member and a learner")]
    LearnerIsMember(NodeId),
    /// A bucket mode that needs a remote target has none.
    #[error("a {0} bucket needs a target")]
    TargetRequired(BucketMode),
    /// A bucket mode that has no remote target has one.
    #[error("a {0} bucket has no target")]
    TargetNotAllowed(BucketMode),
    /// A remote target field is empty or malformed.
    #[error("target {0} is empty or contains whitespace or control characters")]
    InvalidTarget(&'static str),
    /// A disk ID is listed twice in a node registration.
    #[error("disk {0} is listed twice")]
    DuplicateDisk(Label),
}

/// Inconsistent replication settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ReplicationError {
    /// `replicas` is 0.
    #[error("replicas is 0; it must be at least 1")]
    NoReplicas,
    /// `min_write_replicas` is 0 or above `replicas`.
    #[error(
        "min_write_replicas is {min_write_replicas}; it must be from 1 to replicas ({replicas})"
    )]
    MinWriteReplicas {
        /// The `min_write_replicas` value.
        min_write_replicas: u8,
        /// The `replicas` value.
        replicas: u8,
    },
    /// `clean_copies` is above `replicas`.
    #[error("clean_copies is {clean_copies}; it must be from 0 to replicas ({replicas})")]
    CleanCopies {
        /// The `clean_copies` value.
        clean_copies: u8,
        /// The `replicas` value.
        replicas: u8,
    },
}

fn validate_write_replicas(replicas: u8, min_write_replicas: u8) -> Result<(), ReplicationError> {
    if replicas == 0 {
        return Err(ReplicationError::NoReplicas);
    }
    if min_write_replicas == 0 || min_write_replicas > replicas {
        return Err(ReplicationError::MinWriteReplicas {
            min_write_replicas,
            replicas,
        });
    }
    Ok(())
}

/// A bucket's replication settings (§4.1).
///
/// Bucket documents store these as top-level fields. Configuration loading
/// validates the `[buckets.*]` settings with the same rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReplicationSettings {
    /// The number of shard members, and so of copies of every write until
    /// it reaches its durable home.
    pub replicas: u8,
    /// The fewest copies every new write must reach, counting members and
    /// learners already acknowledging (§6.4).
    pub min_write_replicas: u8,
    /// For `write_back` buckets, how many members keep a flushed object as
    /// cache.
    pub clean_copies: u8,
}

impl ReplicationSettings {
    /// Checks `1 ≤ min_write_replicas ≤ replicas` and
    /// `clean_copies ≤ replicas`.
    ///
    /// # Errors
    ///
    /// Returns the first [`ReplicationError`] found.
    pub fn validate(&self) -> Result<(), ReplicationError> {
        validate_write_replicas(self.replicas, self.min_write_replicas)?;
        if self.clean_copies > self.replicas {
            return Err(ReplicationError::CleanCopies {
                clean_copies: self.clean_copies,
                replicas: self.replicas,
            });
        }
        Ok(())
    }
}

/// `cluster.json`: the cluster's identity, register format version, and
/// configuration generation (§6.1, §6.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterDocument {
    /// The cluster's ID.
    pub cluster_id: ClusterId,
    /// The format version of every register under the cluster prefix.
    pub format_version: u32,
    /// Incremented by every control-store change the coordinator makes.
    pub generation: Generation,
    /// The proposal ID of the write that stored this document.
    pub proposal_id: ProposalId,
}

impl ClusterDocument {
    /// The register format version this build reads and writes.
    pub const FORMAT_VERSION: u32 = 1;
}

impl RegisterDocument for ClusterDocument {
    const KIND: &'static str = "cluster";

    fn proposal_id(&self) -> &ProposalId {
        &self.proposal_id
    }

    fn validate(&self) -> Result<(), InvalidRegister> {
        if self.format_version != Self::FORMAT_VERSION {
            return Err(InvalidRegister::UnsupportedFormatVersion {
                found: self.format_version,
                supported: Self::FORMAT_VERSION,
            });
        }
        Ok(())
    }
}

/// `coordinator.lease`: the node that holds the coordinator lease (§6.7).
///
/// The lease has no expiry time. The holder renews it by rewriting it with
/// `If-Match`, and a candidate takes over only after the register's ETag has
/// stayed the same for longer than `coordinator_lease × (1+ρ)` on its own
/// clock. Each renewal carries a fresh proposal ID, which changes the stored
/// bytes and so the ETag even though the holder is unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoordinatorLease {
    /// The node holding the lease.
    pub holder: NodeId,
    /// The proposal ID of the write that stored this document.
    pub proposal_id: ProposalId,
}

impl RegisterDocument for CoordinatorLease {
    const KIND: &'static str = "coordinator lease";

    fn proposal_id(&self) -> &ProposalId {
        &self.proposal_id
    }

    fn validate(&self) -> Result<(), InvalidRegister> {
        Ok(())
    }
}

/// A disk a node offers for storage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiskInfo {
    /// The disk's ID, unique within the node.
    pub disk_id: Label,
    /// The bytes the disk offers to SkyS3.
    pub capacity_bytes: u64,
}

/// `nodes/<node-id>.json`: a node's registration (§6.1, §6.7).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeRegistration {
    /// The node's ID.
    pub node_id: NodeId,
    /// The `host:port` other nodes reach it at.
    pub address: NodeAddress,
    /// The node's zone label, if it has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zone: Option<Label>,
    /// The node's rack label, if it has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rack: Option<Label>,
    /// The node's disks.
    pub disks: Vec<DiskInfo>,
    /// The proposal ID of the write that stored this document.
    pub proposal_id: ProposalId,
}

impl RegisterDocument for NodeRegistration {
    const KIND: &'static str = "node registration";

    fn proposal_id(&self) -> &ProposalId {
        &self.proposal_id
    }

    fn validate(&self) -> Result<(), InvalidRegister> {
        let mut seen = BTreeSet::new();
        for disk in &self.disks {
            if !seen.insert(&disk.disk_id) {
                return Err(InvalidRegister::DuplicateDisk(disk.disk_id.clone()));
            }
        }
        Ok(())
    }
}

/// A bucket's mode, fixed at creation (§4.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BucketMode {
    /// A remote S3 target is the system of record; writes are flushed to it.
    WriteBack,
    /// The cluster is the system of record (§8).
    Local,
    /// An external origin that SkyS3 does not own; writes are rejected
    /// (§9.5).
    ReadOnly,
}

impl BucketMode {
    /// The mode's configuration and register spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WriteBack => "write_back",
            Self::Local => "local",
            Self::ReadOnly => "read_only",
        }
    }

    /// Whether buckets of this mode are bound to a remote target: the system
    /// of record of a `write_back` bucket, or the origin of a `read_only`
    /// one.
    #[must_use]
    pub const fn has_target(self) -> bool {
        matches!(self, Self::WriteBack | Self::ReadOnly)
    }
}

impl std::fmt::Display for BucketMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The remote a bucket is bound to: an S3 endpoint, a bucket there, and an
/// optional key prefix (§4.1). Credentials are not stored in the register.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteTarget {
    /// The S3 endpoint URL.
    pub endpoint: String,
    /// The remote bucket's name.
    pub bucket: String,
    /// The key prefix SkyS3 owns within the remote bucket, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
}

impl RemoteTarget {
    fn validate(&self) -> Result<(), InvalidRegister> {
        let visible = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_graphic());
        if !visible(&self.endpoint) {
            return Err(InvalidRegister::InvalidTarget("endpoint"));
        }
        if !visible(&self.bucket) {
            return Err(InvalidRegister::InvalidTarget("bucket"));
        }
        Ok(())
    }
}

/// `buckets/<bucket>.json`: a bucket's mode, target binding, shard count,
/// and replication settings (§6.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BucketDocument {
    /// The bucket's ID, which keys its shards and write identities.
    pub bucket_id: BucketId,
    /// The S3 name clients address the bucket by.
    pub name: BucketName,
    /// The bucket's mode.
    pub mode: BucketMode,
    /// The number of shards, fixed at creation.
    pub shards: ShardCount,
    /// See [`ReplicationSettings::replicas`].
    pub replicas: u8,
    /// See [`ReplicationSettings::min_write_replicas`].
    pub min_write_replicas: u8,
    /// See [`ReplicationSettings::clean_copies`].
    pub clean_copies: u8,
    /// The remote target: required for `write_back` and `read_only`
    /// buckets, absent for `local` ones.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<RemoteTarget>,
    /// The proposal ID of the write that stored this document.
    pub proposal_id: ProposalId,
}

impl BucketDocument {
    /// The bucket's replication settings.
    #[must_use]
    pub const fn replication(&self) -> ReplicationSettings {
        ReplicationSettings {
            replicas: self.replicas,
            min_write_replicas: self.min_write_replicas,
            clean_copies: self.clean_copies,
        }
    }
}

impl RegisterDocument for BucketDocument {
    const KIND: &'static str = "bucket";

    fn proposal_id(&self) -> &ProposalId {
        &self.proposal_id
    }

    fn validate(&self) -> Result<(), InvalidRegister> {
        self.replication().validate()?;
        match (&self.target, self.mode.has_target()) {
            (None, true) => Err(InvalidRegister::TargetRequired(self.mode)),
            (Some(_), false) => Err(InvalidRegister::TargetNotAllowed(self.mode)),
            (Some(target), true) => target.validate(),
            (None, false) => Ok(()),
        }
    }
}

/// `shards/<bucket>/<n>.json`: a shard's configuration (§4.1, §6.1, §6.3).
///
/// Every change writes a configuration with the next epoch, by
/// compare-and-swap over the current one. The same configuration is the
/// body of a `CONFIG` log record, each replica's local copy (§10.1).
///
/// A configuration may have more members than `replicas` for a while: a
/// rebalance promotes the new member before removing the old one (§6.7).
///
/// ```
/// use skys3_types::{RegisterDocument, ShardConfig};
///
/// let config = ShardConfig::from_json(br#"{
///   "bucket_id": "b-7f3a", "shard": 5, "epoch": 42, "primary": "node-3",
///   "members": ["node-3", "node-7", "node-9"], "learners": [],
///   "min_write_replicas": 2, "replicas": 3, "proposal_id": "01J8Z6K3V2Q4"
/// }"#)?;
/// assert_eq!(config.epoch.get(), 42);
/// assert!(config.is_member(&"node-7".parse()?));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardConfig {
    /// The shard's bucket.
    pub bucket_id: BucketId,
    /// The shard's number within the bucket.
    pub shard: ShardId,
    /// The configuration's epoch.
    pub epoch: Epoch,
    /// The primary, which is one of the members.
    pub primary: NodeId,
    /// The members: every one acknowledges every write (§5.1).
    pub members: Vec<NodeId>,
    /// The learners: they receive the log while catching up and do not
    /// count toward the commit rule.
    pub learners: Vec<NodeId>,
    /// The fewest copies every new write must reach (§6.4).
    pub min_write_replicas: u8,
    /// The number of members the shard aims for.
    pub replicas: u8,
    /// The proposal ID of the write that stored this document.
    pub proposal_id: ProposalId,
}

impl ShardConfig {
    /// Whether `node` is a member.
    #[must_use]
    pub fn is_member(&self, node: &NodeId) -> bool {
        self.members.contains(node)
    }

    /// Whether `node` is a learner.
    #[must_use]
    pub fn is_learner(&self, node: &NodeId) -> bool {
        self.learners.contains(node)
    }
}

impl RegisterDocument for ShardConfig {
    const KIND: &'static str = "shard configuration";

    fn proposal_id(&self) -> &ProposalId {
        &self.proposal_id
    }

    fn validate(&self) -> Result<(), InvalidRegister> {
        validate_write_replicas(self.replicas, self.min_write_replicas)?;
        if self.members.is_empty() {
            return Err(InvalidRegister::NoMembers);
        }
        if !self.is_member(&self.primary) {
            return Err(InvalidRegister::PrimaryNotMember(self.primary.clone()));
        }
        let mut seen = BTreeSet::new();
        for node in &self.members {
            if !seen.insert(node) {
                return Err(InvalidRegister::DuplicateNode(node.clone()));
            }
        }
        let mut learners = BTreeSet::new();
        for node in &self.learners {
            if seen.contains(node) {
                return Err(InvalidRegister::LearnerIsMember(node.clone()));
            }
            if !learners.insert(node) {
                return Err(InvalidRegister::DuplicateNode(node.clone()));
            }
        }
        Ok(())
    }
}
