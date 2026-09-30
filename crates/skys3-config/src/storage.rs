//! `[storage]` and `[cache]`: the node storage engine (§10) and cache
//! budgets (§9.2, §9.3).

use std::time::Duration;

use serde::Deserialize;

use crate::error::Checker;

/// `[storage]`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    /// `inline_max_bytes`: the largest payload stored inline in its log
    /// record; larger bodies are streamed as extent records (§5.1, §10.1).
    pub inline_max_bytes: u64,
    /// `extent_bytes`: the size of the extent records large bodies are
    /// streamed in (§5.1).
    pub extent_bytes: u64,
    /// `segment_bytes`: the size at which a log segment file is closed
    /// (§10.1).
    pub segment_bytes: u64,
    /// `group_commit_max_delay_us`: how long a group commit waits for more
    /// records before it syncs (§10.4).
    pub group_commit_max_delay_us: u64,
    /// `group_commit_max_bytes`: the bytes after which a group commit syncs
    /// without waiting longer.
    pub group_commit_max_bytes: u64,
    /// `index_checkpoint_interval_seconds`: how often the index is made
    /// durable (§10.2).
    pub index_checkpoint_interval_seconds: u64,
    /// `compaction_live_threshold`: the live ratio below which a segment is
    /// reclaimed (§10.3).
    pub compaction_live_threshold: f64,
    /// `read_registration_ttl_seconds`: when a vanished gateway's read
    /// registrations expire (§8.7).
    pub read_registration_ttl_seconds: u64,
    /// `read_registration_renew_interval_seconds`: how often a streaming
    /// gateway renews its read registrations (§8.7).
    pub read_registration_renew_interval_seconds: u64,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            inline_max_bytes: 128 * KIB,
            extent_bytes: MIB,
            segment_bytes: 256 * MIB,
            group_commit_max_delay_us: 500,
            group_commit_max_bytes: 4 * MIB,
            index_checkpoint_interval_seconds: 10,
            compaction_live_threshold: 0.5,
            read_registration_ttl_seconds: 30,
            read_registration_renew_interval_seconds: 10,
        }
    }
}

crate::durations! {
    StorageConfig {
        /// `group_commit_max_delay_us`.
        group_commit_max_delay => group_commit_max_delay_us, Duration::from_micros;
        /// `index_checkpoint_interval_seconds`.
        index_checkpoint_interval => index_checkpoint_interval_seconds, Duration::from_secs;
        /// `read_registration_ttl_seconds`.
        read_registration_ttl => read_registration_ttl_seconds, Duration::from_secs;
        /// `read_registration_renew_interval_seconds`.
        read_registration_renew_interval => read_registration_renew_interval_seconds, Duration::from_secs;
    }
}

impl StorageConfig {
    pub(crate) fn check(&self, checker: &mut Checker) {
        for (key, value) in [
            ("storage.extent_bytes", self.extent_bytes),
            ("storage.segment_bytes", self.segment_bytes),
            (
                "storage.group_commit_max_bytes",
                self.group_commit_max_bytes,
            ),
            (
                "storage.index_checkpoint_interval_seconds",
                self.index_checkpoint_interval_seconds,
            ),
            (
                "storage.read_registration_ttl_seconds",
                self.read_registration_ttl_seconds,
            ),
            (
                "storage.read_registration_renew_interval_seconds",
                self.read_registration_renew_interval_seconds,
            ),
        ] {
            checker.nonzero(key, value);
        }
        let largest_record = self.extent_bytes.max(self.inline_max_bytes);
        checker.require(
            self.segment_bytes > largest_record,
            "storage.segment_bytes",
            || {
                format!(
                    "is {}; it must be greater than extent_bytes ({}) and inline_max_bytes ({}), \
                     so a segment holds at least one record",
                    self.segment_bytes, self.extent_bytes, self.inline_max_bytes
                )
            },
        );
        let threshold = self.compaction_live_threshold;
        checker.require(
            threshold > 0.0 && threshold < 1.0,
            "storage.compaction_live_threshold",
            || format!("is {threshold}; it must be greater than 0 and less than 1"),
        );
        checker.require(
            self.read_registration_renew_interval_seconds < self.read_registration_ttl_seconds,
            "storage.read_registration_renew_interval_seconds",
            || {
                format!(
                    "is {}; it must be less than read_registration_ttl_seconds ({}), or \
                     registrations expire while a gateway still streams (§8.7)",
                    self.read_registration_renew_interval_seconds,
                    self.read_registration_ttl_seconds
                )
            },
        );
    }
}

/// `[cache]`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CacheConfig {
    /// `hot_cache_bytes_per_node`: the node-local cache of recently read
    /// objects (§9.2).
    pub hot_cache_bytes_per_node: u64,
    /// `cache_max_bytes_per_node`: the bound on clean cached payload on a
    /// node, enforced by LRU eviction (§9.3).
    pub cache_max_bytes_per_node: u64,
    /// `reserve_fraction`: the share of each disk kept free for learner
    /// catch-up and filesystem overhead (§9.3).
    pub reserve_fraction: f64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            hot_cache_bytes_per_node: 64 * GIB,
            cache_max_bytes_per_node: 1024 * GIB,
            reserve_fraction: 0.10,
        }
    }
}

impl CacheConfig {
    pub(crate) fn check(&self, checker: &mut Checker) {
        checker.fraction("cache.reserve_fraction", self.reserve_fraction, 1.0, false);
    }
}

pub(crate) const KIB: u64 = 1024;
pub(crate) const MIB: u64 = 1024 * KIB;
pub(crate) const GIB: u64 = 1024 * MIB;
pub(crate) const TIB: u64 = 1024 * GIB;
