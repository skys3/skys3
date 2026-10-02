//! How long a replicated shard waits for its members (§5.2).
//!
//! Every write needs every current member's acknowledgement, so a member
//! that stops acknowledging stalls every write of its shards, and every
//! barrier behind them: seals, conditional checks that wait for an earlier
//! write of their key, and closing the shard. [`AckTimeout`] bounds those
//! waits on a replicated shard ([`Shard::set_ack_timeout`]). A shard alone
//! waits only for its own disk, which either syncs or fails, and has no
//! timeout.
//!
//! A write that times out fails with [`ShardError::NotAcknowledged`]:
//! **not acknowledged**, which is not the same as not applied. Its record
//! keeps its position, and commits once the member catches up (or, from
//! plan M2-11, once the member is removed), so it may become visible after
//! its writer was told it failed. Every record sequenced after it commits
//! after it, so it never takes effect over a write that was sequenced
//! later, such as any write sent after the failure was answered.
//!
//! [`Shard::set_ack_timeout`]: crate::Shard::set_ack_timeout
//! [`ShardError::NotAcknowledged`]: crate::ShardError::NotAcknowledged

use std::time::Duration;

/// What clients see while a member is late (§5.2,
/// `replica_ack_timeout_mode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum AckMode {
    /// Requests wait up to the timeout, which outlasts the removal of a
    /// failed member (plan M2-11), so they complete once the remaining
    /// members acknowledge: added latency rather than errors.
    #[default]
    WaitThrough,
    /// Requests wait up to the timeout, and once one has timed out the
    /// shard refuses new writes at once, without sequencing them, until
    /// every record sequenced before that timeout is applied.
    FailFast,
}

/// How long a replicated shard's requests wait for its members
/// (`replica_ack_timeout`, §5.2), and in which [`AckMode`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AckTimeout {
    /// How long a request waits, from when the shard takes it until its
    /// record is committed and applied.
    pub timeout: Duration,
    /// What a late member does to later requests.
    pub mode: AckMode,
}

impl AckTimeout {
    /// The design's default: wait through a member's removal, for up to 5
    /// seconds.
    pub const DEFAULT: Self = Self {
        timeout: Duration::from_secs(5),
        mode: AckMode::WaitThrough,
    };

    /// A timeout of `timeout` in [`AckMode::WaitThrough`].
    #[must_use]
    pub const fn wait_through(timeout: Duration) -> Self {
        Self {
            timeout,
            mode: AckMode::WaitThrough,
        }
    }

    /// A timeout of `timeout` in [`AckMode::FailFast`].
    #[must_use]
    pub const fn fail_fast(timeout: Duration) -> Self {
        Self {
            timeout,
            mode: AckMode::FailFast,
        }
    }
}

impl Default for AckTimeout {
    fn default() -> Self {
        Self::DEFAULT
    }
}
