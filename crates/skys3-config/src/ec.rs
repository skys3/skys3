//! `[ec]`: erasure coding of `local` buckets (§8).

use std::time::Duration;

use serde::Deserialize;

use crate::error::Checker;
use crate::storage::MIB;

/// `[ec]`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EcConfig {
    /// `parity_fragments`: `m`, the parity fragments per stripe (§8.3).
    pub parity_fragments: u32,
    /// `max_data_fragments`: the widest `k` the coordinator picks (§8.3).
    pub max_data_fragments: u32,
    /// `min_eligible_nodes`: the fewest eligible nodes for which objects
    /// are encoded at all (§8.2).
    pub min_eligible_nodes: u32,
    /// `fragment_release_delay_seconds`: how long released data stays
    /// readable for a read plan not yet registered (§8.7).
    pub fragment_release_delay_seconds: u64,
    /// `fragment_orphan_after_seconds`: when unreferenced fragments may be
    /// reclaimed (§8.4).
    pub fragment_orphan_after_seconds: u64,
    /// `repair_bytes_per_second_per_node`: the repair bandwidth cap (§8.6).
    pub repair_bytes_per_second_per_node: u64,
}

impl Default for EcConfig {
    fn default() -> Self {
        Self {
            parity_fragments: 2,
            max_data_fragments: 8,
            min_eligible_nodes: 5,
            fragment_release_delay_seconds: 60,
            fragment_orphan_after_seconds: 3600,
            repair_bytes_per_second_per_node: 100 * MIB,
        }
    }
}

crate::durations! {
    EcConfig {
        /// `fragment_release_delay_seconds`.
        fragment_release_delay => fragment_release_delay_seconds, Duration::from_secs;
        /// `fragment_orphan_after_seconds`.
        fragment_orphan_after => fragment_orphan_after_seconds, Duration::from_secs;
    }
}

impl EcConfig {
    pub(crate) fn check(&self, checker: &mut Checker) {
        checker.nonzero("ec.parity_fragments", self.parity_fragments.into());
        checker.nonzero("ec.max_data_fragments", self.max_data_fragments.into());
        checker.require(
            self.min_eligible_nodes > self.parity_fragments,
            "ec.min_eligible_nodes",
            || {
                format!(
                    "is {}; a stripe puts at most one fragment on a node, so it must be greater \
                     than parity_fragments ({}) to leave room for data fragments (§8.3)",
                    self.min_eligible_nodes, self.parity_fragments
                )
            },
        );
        checker.nonzero(
            "ec.fragment_orphan_after_seconds",
            self.fragment_orphan_after_seconds,
        );
        checker.nonzero(
            "ec.repair_bytes_per_second_per_node",
            self.repair_bytes_per_second_per_node,
        );
    }
}
