//! `[ec]`: erasure coding of `local` buckets (§8).

use std::time::Duration;

use serde::Deserialize;
use skys3_types::Geometry;

use crate::error::Checker;
use crate::storage::MIB;

/// The longest fragment a fragment store keeps (§8.4): a stripe's data
/// fragments, each `⌈ec_stripe_data_bytes / k⌉` bytes rounded up to a
/// multiple of 64, must fit it in the narrowest geometry.
pub const MAX_FRAGMENT_BYTES: u64 = 256 * MIB;

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
    /// `k` of the narrowest geometry (§8.3): `min_eligible_nodes −
    /// parity_fragments`, at least 1 and at most `max_data_fragments`.
    #[must_use]
    pub fn narrowest_data_fragments(&self) -> u64 {
        u64::from(self.min_eligible_nodes)
            .saturating_sub(self.parity_fragments.into())
            .clamp(1, u64::from(self.max_data_fragments).max(1))
    }

    /// The length of each fragment of a stripe of `stripe_data_bytes` in
    /// the narrowest geometry, the longest any stripe of that size has.
    #[must_use]
    pub fn fragment_bytes(&self, stripe_data_bytes: u64) -> u64 {
        stripe_data_bytes
            .div_ceil(self.narrowest_data_fragments())
            .div_ceil(64)
            .saturating_mul(64)
    }

    /// Checks that a stripe of a bucket's `ec_stripe_data_bytes` fits the
    /// fragment store in the narrowest geometry; `key` names the setting.
    pub(crate) fn check_stripe(&self, key: &str, stripe_data_bytes: u64, checker: &mut Checker) {
        let fragment = self.fragment_bytes(stripe_data_bytes);
        checker.require(fragment <= MAX_FRAGMENT_BYTES, key, || {
            format!(
                "is {stripe_data_bytes}; in the narrowest geometry ({} data fragments, from \
                 ec.min_eligible_nodes and ec.parity_fragments) each fragment would be \
                 {fragment} bytes, and a fragment holds at most {MAX_FRAGMENT_BYTES} (§8.4)",
                self.narrowest_data_fragments(),
            )
        });
    }

    pub(crate) fn check(&self, checker: &mut Checker) {
        checker.nonzero("ec.parity_fragments", self.parity_fragments.into());
        checker.nonzero("ec.max_data_fragments", self.max_data_fragments.into());
        let widest = u64::from(self.max_data_fragments) + u64::from(self.parity_fragments);
        checker.require(
            widest <= Geometry::MAX_FRAGMENTS as u64,
            "ec.max_data_fragments",
            || {
                format!(
                    "is {}; with parity_fragments ({}) the widest stripe would have {widest} \
                     fragments, and a stripe has at most {} (§8.4)",
                    self.max_data_fragments,
                    self.parity_fragments,
                    Geometry::MAX_FRAGMENTS,
                )
            },
        );
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
