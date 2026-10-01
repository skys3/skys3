//! The shard runtime's error type.

use skys3_log::ShardRef;
use skys3_types::{Epoch, EpochSeq, NodeId};

/// Why a shard request failed.
///
/// A failed write is **not acknowledged**, which is not the same as not
/// applied (§5.2): a record that failed after it was appended may be
/// durable, and replay after a restart then applies it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ShardError {
    /// The shard is not open on this node.
    #[error("shard {0} is not open")]
    NotFound(ShardRef),
    /// The shard is sealed while its bucket is deleted, and refuses client
    /// writes (§4.1).
    #[error("shard {0} is sealed while its bucket is deleted")]
    Sealed(ShardRef),
    /// The record was refused before it got a position: it cannot be
    /// encoded, or it breaks a rule of the write path. Nothing was
    /// appended.
    #[error("shard {shard} refused the record: {reason}")]
    InvalidRecord {
        /// The shard.
        shard: ShardRef,
        /// Why.
        reason: String,
    },
    /// The shard's configuration cannot be served by this build or this
    /// replica.
    #[error("shard {shard} cannot open: {reason}")]
    Configuration {
        /// The shard.
        shard: ShardRef,
        /// Why.
        reason: String,
    },
    /// This replica is a member, not the shard's primary, so it serves no
    /// client request (§5.1, step 1). The primary and epoch it knows are a
    /// redirect hint.
    #[error("shard {shard} is served by its primary {primary} in epoch {epoch}")]
    NotPrimary {
        /// The shard.
        shard: ShardRef,
        /// The primary this replica knows.
        primary: NodeId,
        /// The epoch of the configuration that names it.
        epoch: Epoch,
    },
    /// A replicated shard did not acknowledge the write within its
    /// [`AckTimeout`](crate::AckTimeout), or refused it at once because an
    /// earlier write is late in fail-fast mode (§5.2). Clients get `503
    /// SlowDown`. A write that timed out keeps its position and may still
    /// be applied, but never over a write sequenced after it.
    #[error("shard {shard} did not acknowledge the write: {reason}")]
    NotAcknowledged {
        /// The shard.
        shard: ShardRef,
        /// The position the write's record took, if it got one: the record
        /// may still commit there.
        position: Option<EpochSeq>,
        /// Why.
        reason: String,
    },
    /// The shard stopped: its disk is out of service, its index failed, or
    /// it was removed. Nothing is acknowledged on it until the node reopens
    /// it, after replaying its log.
    #[error("shard {shard} is unavailable: {reason}")]
    Unavailable {
        /// The shard.
        shard: ShardRef,
        /// Why.
        reason: String,
    },
}

impl ShardError {
    pub(crate) fn unavailable(shard: &ShardRef, reason: impl ToString) -> Self {
        Self::Unavailable {
            shard: shard.clone(),
            reason: reason.to_string(),
        }
    }

    pub(crate) fn not_acknowledged(shard: &ShardRef, reason: impl ToString) -> Self {
        Self::NotAcknowledged {
            shard: shard.clone(),
            position: None,
            reason: reason.to_string(),
        }
    }

    /// Names `at` as the position of the write that was not acknowledged,
    /// unless the error names one already or is another error.
    pub(crate) fn sequenced_at(self, at: EpochSeq) -> Self {
        match self {
            Self::NotAcknowledged {
                shard,
                position: None,
                reason,
            } => Self::NotAcknowledged {
                shard,
                position: Some(at),
                reason,
            },
            other => other,
        }
    }

    pub(crate) fn invalid(shard: &ShardRef, reason: impl ToString) -> Self {
        Self::InvalidRecord {
            shard: shard.clone(),
            reason: reason.to_string(),
        }
    }

    pub(crate) fn configuration(shard: &ShardRef, reason: impl ToString) -> Self {
        Self::Configuration {
            shard: shard.clone(),
            reason: reason.to_string(),
        }
    }
}
