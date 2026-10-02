//! Exposure after member loss (§6.4): how much data has fewer copies than
//! its shard's `replicas`, and for how long, as the metrics
//! `under_replicated_bytes` and `oldest_under_replicated_age` report it.

use std::sync::atomic::AtomicU64;
use std::time::Duration;

use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Unit;
use skys3_obs::MetricsRegistry;

/// The data of a node's under-replicated shards at one moment.
///
/// A shard is under-replicated while its configuration has fewer members
/// than its `replicas`, after a member was removed: everything it holds
/// then has fewer copies, old data and new writes alike. Each shard is
/// counted on its primary only, so summing every node's bytes counts each
/// shard once.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Exposure {
    /// The shards counted.
    pub shards: usize,
    /// The bytes of the object versions they hold
    /// (`under_replicated_bytes`).
    pub bytes: u64,
    /// How long the longest of them has been under-replicated, or zero
    /// (`oldest_under_replicated_age`). A node counts from when it saw the
    /// shard lose a member, or opened it with fewer members after a
    /// restart: a restart makes the age look younger than it is.
    pub oldest: Duration,
}

/// The replication metrics of a node: `skys3_under_replicated_bytes` and
/// `skys3_oldest_under_replicated_age_seconds` (`docs/skys3-metrics.md`).
#[derive(Debug, Clone, Default)]
pub struct ReplicationMetrics {
    under_replicated: Gauge,
    oldest_under_replicated_age: Gauge<f64, AtomicU64>,
}

impl ReplicationMetrics {
    /// Registers the replication metrics in `registry`.
    #[must_use]
    pub fn register(registry: &MetricsRegistry) -> Self {
        let metrics = Self::default();
        registry.register_with_unit(
            "under_replicated",
            "Bytes of the object versions held by shards whose primary is on this node and \
             that have fewer members than their replicas setting.",
            Unit::Bytes,
            metrics.under_replicated.clone(),
        );
        registry.register_with_unit(
            "oldest_under_replicated_age",
            "How long the longest under-replicated shard whose primary is on this node has had \
             fewer members than its replicas setting.",
            Unit::Seconds,
            metrics.oldest_under_replicated_age.clone(),
        );
        metrics
    }

    /// Sets the gauges to `exposure`.
    pub fn set(&self, exposure: Exposure) {
        self.under_replicated
            .set(i64::try_from(exposure.bytes).unwrap_or(i64::MAX));
        self.oldest_under_replicated_age
            .set(exposure.oldest.as_secs_f64());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposure_is_exported_under_the_design_names() {
        let registry = MetricsRegistry::new();
        let metrics = ReplicationMetrics::register(&registry);
        metrics.set(Exposure {
            shards: 2,
            bytes: 4096,
            oldest: Duration::from_millis(2500),
        });
        let text = registry.encode().unwrap();
        assert!(
            text.contains("skys3_under_replicated_bytes 4096\n"),
            "{text}"
        );
        assert!(
            text.contains("skys3_oldest_under_replicated_age_seconds 2.5\n"),
            "{text}"
        );
    }
}
