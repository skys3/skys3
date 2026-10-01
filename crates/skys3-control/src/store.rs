//! The [`ControlStore`] trait and the types it exchanges.

use std::fmt;
use std::future::Future;
use std::io;
use std::sync::Arc;

use bytes::Bytes;
use skys3_types::{ClusterId, Generation, NodeId, RegisterError};

use crate::key::{KeyError, KeyPrefix, RegisterKey};

/// The version of a register's value: an S3 ETag, an etcd `mod_revision`,
/// or a backend's own counter or content hash.
///
/// Versions are opaque and compared only for equality. Two different values
/// of a register never have the same version. A backend may give equal
/// values equal versions (an S3 ETag is a content hash), which is harmless
/// because every value carries a fresh [`ProposalId`](skys3_types::ProposalId).
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Version(Arc<str>);

impl Version {
    /// Wraps a backend's version text.
    #[must_use]
    pub fn new(version: impl Into<Arc<str>>) -> Self {
        Self(version.into())
    }

    /// The version text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Version({})", self.0)
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A register's value and its version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Versioned<T = Bytes> {
    /// The value.
    pub value: T,
    /// The version the store holds the value at.
    pub version: Version,
}

/// The precondition of a [`ControlStore::put_if`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expected {
    /// The register must not exist (`If-None-Match: *`).
    Absent,
    /// The register must be at this version (`If-Match: <etag>`).
    Version(Version),
}

/// What a [`ControlStore::put_if`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub enum PutOutcome {
    /// The value was stored, at this version.
    Written(Version),
    /// The precondition did not hold, and nothing was written: another
    /// writer won (`412 Precondition Failed`).
    PreconditionFailed,
}

/// What a [`ControlStore::delete_if`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum DeleteOutcome {
    /// The register was at the expected version and is gone.
    Deleted,
    /// The register was not at the expected version, or did not exist,
    /// and nothing was deleted (`412 Precondition Failed`).
    PreconditionFailed,
}

/// One report of a [`ChangeStream`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    /// The generation in `cluster.json` when the registers were listed.
    pub generation: Generation,
    /// Whether `registers` lists every register the feed reports: a copy of
    /// any other register is gone. The first report of a feed is a
    /// snapshot.
    pub snapshot: bool,
    /// The registers whose version differs from the feed's previous report,
    /// in key order, with the current version, or `None` for a register
    /// that no longer exists.
    pub registers: Vec<(RegisterKey, Option<Version>)>,
}

/// A stream of [`Change`]s from [`ControlStore::changes`].
///
/// Delivery is driven by the generation in `cluster.json` (design §6.2):
///
/// - The stream reports whenever it observes a generation other than the
///   one it last reported (at first, the `after` it was opened with). It
///   lists the registers under `nodes/`, `buckets/`, `shards/`, and
///   `identity/`; `cluster.json` and `coordinator.lease` are not reported.
/// - The first report is a snapshot of every register. Later reports hold
///   the registers whose version changed since the previous report.
/// - A register written before a generation increment is reported, at that
///   version or a later one, no later than the first report at or after
///   that generation. A write that is never followed by an increment may
///   be reported early, or never.
/// - Reports are coalesced: a register written several times between
///   reports is reported once, at its latest version. A reader acts on the
///   version it then reads, not on the reported one.
///
/// Backends differ only in when they observe: in-process backends on every
/// write, polling backends every `config_poll_interval`, and native watches
/// when notified.
pub trait ChangeStream: Send {
    /// Waits for the next report.
    ///
    /// # Errors
    ///
    /// A [`ControlError`] from reading the store, including
    /// [`ControlError::NotBootstrapped`]. The stream keeps its state, and
    /// the next call observes again.
    fn next(&mut self) -> impl Future<Output = Result<Change, ControlError>> + Send;
}

/// A store of small linearizable registers: the cluster's control state
/// (design §6.1).
///
/// `put_if` is linearizable per key; nothing else is required. Every value
/// is a register document with a fresh `proposal_id`, so the lost-response
/// rule in [`propose`](crate::propose) can tell whether an unanswered write
/// landed.
///
/// Implementations answer each call once and do not retry: retries,
/// conflicts, and lost responses are handled by [`propose`](crate::propose),
/// the same way for every backend.
///
/// Like `skys3-remote`'s `ObjectStore`, the methods return
/// `impl Future + Send`, so the trait is used through generics.
pub trait ControlStore: fmt::Debug + Clone + Send + Sync + 'static {
    /// The stream [`ControlStore::changes`] returns.
    type Changes: ChangeStream;

    /// Reads a register, or `None` if it does not exist.
    ///
    /// # Errors
    ///
    /// [`ControlError::Unavailable`] or [`ControlError::Indeterminate`] if
    /// the store did not answer.
    fn get(
        &self,
        key: &RegisterKey,
    ) -> impl Future<Output = Result<Option<Versioned>, ControlError>> + Send;

    /// Writes `value` only if the register is still at `expected`.
    ///
    /// # Errors
    ///
    /// - [`ControlError::Conflict`]: a concurrent conditional write was in
    ///   progress (`409 ConditionalRequestConflict`); nothing was written.
    /// - [`ControlError::Unavailable`]: nothing was written.
    /// - [`ControlError::Indeterminate`]: no answer; the value may or may
    ///   not have been written, and may still be written later.
    fn put_if(
        &self,
        key: &RegisterKey,
        expected: Expected,
        value: Bytes,
    ) -> impl Future<Output = Result<PutOutcome, ControlError>> + Send;

    /// Deletes the register only if it is still at `expected`.
    ///
    /// A deleted register carries no `proposal_id`, so
    /// [`propose_delete`](crate::propose_delete) resolves lost answers by
    /// re-reading: an absent register counts as deleted.
    ///
    /// # Errors
    ///
    /// As [`ControlStore::put_if`]: a [`ControlError::Indeterminate`] delete
    /// may still be applied later, but only while the register is at
    /// `expected`, so it never removes a newer value.
    fn delete_if(
        &self,
        key: &RegisterKey,
        expected: &Version,
    ) -> impl Future<Output = Result<DeleteOutcome, ControlError>> + Send;

    /// Lists the registers under `prefix` with their versions, in key
    /// order.
    ///
    /// # Errors
    ///
    /// As [`ControlStore::get`].
    fn list(
        &self,
        prefix: &KeyPrefix,
    ) -> impl Future<Output = Result<Vec<(RegisterKey, Version)>, ControlError>> + Send;

    /// Opens a stream of changes after generation `after`; see
    /// [`ChangeStream`] for the delivery rules. Pass [`Generation::ZERO`]
    /// to start from nothing: every bootstrapped store is at generation 1
    /// or later.
    ///
    /// # Errors
    ///
    /// As [`ControlStore::get`].
    fn changes(
        &self,
        after: Generation,
    ) -> impl Future<Output = Result<Self::Changes, ControlError>> + Send;
}

/// A control-store operation that failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ControlError {
    /// No answer arrived: a timeout, a dropped connection, or a lost
    /// response. A write may or may not have been applied.
    #[error("no answer from the control store: {0}")]
    Indeterminate(String),
    /// A concurrent conditional write to the key was in progress (S3's
    /// `409 ConditionalRequestConflict`). Nothing was written.
    #[error("a concurrent conditional write to {0} was in progress")]
    Conflict(RegisterKey),
    /// The store refused the request or could not be reached, and applied
    /// nothing.
    #[error("the control store is unavailable: {0}")]
    Unavailable(String),
    /// The store rejected the request for a reason a retry does not fix,
    /// such as `403 AccessDenied`, a missing bucket, or a header it does
    /// not implement, and applied nothing.
    #[error("the control store rejected the request: {0}")]
    Rejected(String),
    /// A register holds a value that is not a valid document.
    #[error("register {key} is invalid: {source}")]
    InvalidRegister {
        /// The register.
        key: RegisterKey,
        /// Why its value was rejected.
        #[source]
        source: RegisterError,
    },
    /// The store holds a key outside the register-key grammar.
    #[error(transparent)]
    InvalidKey(#[from] KeyError),
    /// `cluster.json` does not exist.
    #[error("the control store has no cluster.json; the cluster is not bootstrapped")]
    NotBootstrapped,
    /// `cluster.json` names another cluster.
    #[error("the control store belongs to cluster {found}, not {expected}")]
    ClusterMismatch {
        /// This node's cluster.
        expected: ClusterId,
        /// The cluster `cluster.json` names.
        found: ClusterId,
    },
    /// The file backend serves one node, and another node is registered
    /// or tried to register.
    #[error(
        "the file control store serves only node {serving}; node {other} cannot register in it"
    )]
    SecondNode {
        /// The node the store serves.
        serving: NodeId,
        /// The other node.
        other: NodeId,
    },
    /// Local storage failed. The file backend then refuses every request
    /// until it is reopened.
    #[error("control store I/O failed: {0}")]
    Io(#[from] io::Error),
    /// [`propose`](crate::propose) ran out of attempts.
    #[error("gave up on {key} after {attempts} attempts: {last}")]
    RetriesExhausted {
        /// The register.
        key: RegisterKey,
        /// The attempts made.
        attempts: u32,
        /// Whether an attempt may have been applied.
        may_have_applied: bool,
        /// The last error.
        #[source]
        last: Box<ControlError>,
    },
}

impl ControlError {
    /// Whether the same request may succeed if sent again: conflicts,
    /// unavailability, and missing answers.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Indeterminate(_) | Self::Conflict(_) | Self::Unavailable(_)
        )
    }

    /// Whether a failed write may still have been applied.
    #[must_use]
    pub fn may_have_applied(&self) -> bool {
        match self {
            Self::Indeterminate(_) | Self::Io(_) => true,
            Self::RetriesExhausted {
                may_have_applied, ..
            } => *may_have_applied,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_print_their_text() {
        let version = Version::new("\"9b2cf535f27731c974343645a3985328\"");
        assert_eq!(version.as_str(), "\"9b2cf535f27731c974343645a3985328\"");
        assert_eq!(version.to_string(), version.as_str());
        assert_eq!(
            format!("{:?}", Version::new("42")),
            "Version(42)".to_owned()
        );
        assert_eq!(Version::new("1"), Version::new(String::from("1")));
    }

    #[test]
    fn errors_classify_retries_and_uncertain_writes() {
        let key = RegisterKey::cluster();
        let retryable = [
            ControlError::Indeterminate("timeout".into()),
            ControlError::Conflict(key.clone()),
            ControlError::Unavailable("down".into()),
        ];
        assert!(retryable.iter().all(ControlError::is_retryable));
        assert!(retryable[0].may_have_applied());
        assert!(!retryable[1].may_have_applied());
        assert!(!retryable[2].may_have_applied());
        let rejected = ControlError::Rejected("403 AccessDenied".into());
        assert!(!rejected.is_retryable() && !rejected.may_have_applied());
        let io = ControlError::from(io::Error::other("disk"));
        assert!(!io.is_retryable() && io.may_have_applied());
        assert!(!ControlError::NotBootstrapped.may_have_applied());
        let exhausted = ControlError::RetriesExhausted {
            key,
            attempts: 3,
            may_have_applied: true,
            last: Box::new(ControlError::Unavailable("down".into())),
        };
        assert!(exhausted.may_have_applied() && !exhausted.is_retryable());
        assert_eq!(
            exhausted.to_string(),
            "gave up on cluster.json after 3 attempts: \
             the control store is unavailable: down"
        );
    }
}
