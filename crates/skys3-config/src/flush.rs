//! `[flush]`: write-back flushing (§7).

use serde::Deserialize;

use crate::error::Checker;
use crate::storage::{GIB, MIB, TIB};

/// When a write is acknowledged relative to its flush (§7.5, §8.9).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AckPolicy {
    /// After the local commit on every member.
    #[default]
    Local,
    /// After the local commit and the remote flush.
    WriteThrough,
}

/// What happens when a conditional flush finds the remote changed out of
/// band (§7.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictPolicy {
    /// Keep the key dirty and report the conflict.
    #[default]
    Hold,
    /// Flush unconditionally; local writes win.
    Overwrite,
    /// Adopt the remote version and drop the local one. This loses
    /// acknowledged writes, so only a `[buckets.<name>]` table may choose it.
    DiscardLocal,
}

/// `[flush]`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FlushConfig {
    /// `ack_policy`: the default for every bucket (§7.5).
    pub ack_policy: AckPolicy,
    /// `flush_min_concurrency_per_shard`: the floor of adaptive flush
    /// concurrency (§7.7).
    pub flush_min_concurrency_per_shard: u32,
    /// `flush_max_concurrency_per_shard`: its ceiling.
    pub flush_max_concurrency_per_shard: u32,
    /// `flush_max_inflight_bytes_per_target`: the bound on bytes in flight
    /// to one target (§7.7).
    pub flush_max_inflight_bytes_per_target: u64,
    /// `streaming_flush_min_bytes`: single PUTs at least this large are
    /// streamed to the remote while they arrive (§7.3).
    pub streaming_flush_min_bytes: u64,
    /// `flush_part_bytes`: the part size of remote multipart uploads for
    /// streamed single PUTs (§7.3).
    pub flush_part_bytes: u64,
    /// `flush_conflict_policy`: the default for every bucket (§7.2).
    pub flush_conflict_policy: ConflictPolicy,
    /// `max_dirty_bytes`: the cluster's dirty-data budget (§7.6).
    pub max_dirty_bytes: u64,
}

impl Default for FlushConfig {
    fn default() -> Self {
        Self {
            ack_policy: AckPolicy::Local,
            flush_min_concurrency_per_shard: 4,
            flush_max_concurrency_per_shard: 64,
            flush_max_inflight_bytes_per_target: GIB,
            streaming_flush_min_bytes: 64 * MIB,
            flush_part_bytes: 64 * MIB,
            flush_conflict_policy: ConflictPolicy::Hold,
            max_dirty_bytes: 2 * TIB,
        }
    }
}

impl FlushConfig {
    /// The smallest part S3 accepts in a multipart upload, except the last.
    pub const MIN_PART_BYTES: u64 = 5 * MIB;
    /// The largest part S3 accepts.
    pub const MAX_PART_BYTES: u64 = 5 * GIB;

    pub(crate) fn check(&self, checker: &mut Checker) {
        checker.nonzero(
            "flush.flush_min_concurrency_per_shard",
            self.flush_min_concurrency_per_shard.into(),
        );
        checker.require(
            self.flush_max_concurrency_per_shard >= self.flush_min_concurrency_per_shard,
            "flush.flush_max_concurrency_per_shard",
            || {
                format!(
                    "is {}; it must be at least flush_min_concurrency_per_shard ({})",
                    self.flush_max_concurrency_per_shard, self.flush_min_concurrency_per_shard
                )
            },
        );
        checker.nonzero(
            "flush.flush_max_inflight_bytes_per_target",
            self.flush_max_inflight_bytes_per_target,
        );
        checker.nonzero(
            "flush.streaming_flush_min_bytes",
            self.streaming_flush_min_bytes,
        );
        checker.require(
            (Self::MIN_PART_BYTES..=Self::MAX_PART_BYTES).contains(&self.flush_part_bytes),
            "flush.flush_part_bytes",
            || {
                format!(
                    "is {}; S3 parts must be from {} (5 MiB) to {} (5 GiB) bytes",
                    self.flush_part_bytes,
                    Self::MIN_PART_BYTES,
                    Self::MAX_PART_BYTES
                )
            },
        );
        checker.require(
            self.flush_conflict_policy != ConflictPolicy::DiscardLocal,
            "flush.flush_conflict_policy",
            || {
                "\"discard_local\" loses acknowledged writes, so it must be opted into per \
                 bucket in a [buckets.<name>] table (§7.2)"
                    .to_owned()
            },
        );
        checker.nonzero("flush.max_dirty_bytes", self.max_dirty_bytes);
    }
}
