//! The repair metrics of a node (§8.6, §16.3), over the shards it leads.
//! The metrics reference, `docs/skys3-metrics.md`, describes each.

use std::collections::BTreeMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Unit;
use skys3_log::ShardRef;
use skys3_obs::MetricsRegistry;
use tokio::time::Instant;

/// The repair metrics of one node. Clones share them; the default ones
/// belong to no registry.
#[derive(Debug, Clone)]
pub struct RepairMetrics {
    repaired: Counter,
    moved: Counter,
    bytes: Counter,
    duration: Histogram,
    unrepaired: Gauge,
    oldest: Gauge<f64, AtomicU64>,
    /// What each shard this node leads has left to repair: how many
    /// fragments, and since when the oldest of them is known lost.
    backlog: Arc<Mutex<BTreeMap<ShardRef, (u64, Instant)>>>,
}

impl Default for RepairMetrics {
    fn default() -> Self {
        Self {
            repaired: Counter::default(),
            moved: Counter::default(),
            bytes: Counter::default(),
            // From a second to about three days.
            duration: Histogram::new(exponential_buckets(1.0, 2.0, 19)),
            unrepaired: Gauge::default(),
            oldest: Gauge::default(),
            backlog: Arc::default(),
        }
    }
}

impl RepairMetrics {
    /// Registers the repair metrics in `registry`.
    #[must_use]
    pub fn register(registry: &MetricsRegistry) -> Self {
        let metrics = Self::default();
        registry.register(
            "repaired_fragments",
            "Fragments this node's shard primaries rebuilt on another node and relocated with \
             a committed EC_RELOCATE.",
            metrics.repaired.clone(),
        );
        registry.register(
            "moved_fragments",
            "Fragments this node's shard primaries moved to another node, to drain a node, \
             respect a failure domain's cap, or balance the nodes, with a committed \
             EC_RELOCATE.",
            metrics.moved.clone(),
        );
        registry.register_with_unit(
            "repair",
            "Bytes this node's repairs and fragment moves read and wrote.",
            Unit::Bytes,
            metrics.bytes.clone(),
        );
        registry.register_with_unit(
            "repair_duration",
            "For each fragment repaired, the time from when its shard primary found it lost to \
             when its EC_RELOCATE committed.",
            Unit::Seconds,
            metrics.duration.clone(),
        );
        registry.register(
            "unrepaired_fragments",
            "Fragments the shards this node leads know lost and have not repaired yet.",
            metrics.unrepaired.clone(),
        );
        registry.register_with_unit(
            "oldest_unrepaired_age",
            "How long the oldest of those fragments has been known lost.",
            Unit::Seconds,
            metrics.oldest.clone(),
        );
        metrics
    }

    /// The fragments repaired so far.
    #[must_use]
    pub fn repaired(&self) -> u64 {
        self.repaired.get()
    }

    /// The fragments moved so far.
    #[must_use]
    pub fn moved(&self) -> u64 {
        self.moved.get()
    }

    /// The bytes repairs and moves read and wrote so far.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.bytes.get()
    }

    /// The fragments known lost and not repaired yet.
    #[must_use]
    pub fn unrepaired(&self) -> i64 {
        self.unrepaired.get()
    }

    /// Counts a fragment moved.
    pub(crate) fn moved_fragment(&self) {
        self.moved.inc();
    }

    /// Counts `bytes` a repair or a move read or wrote.
    pub(crate) fn transferred(&self, bytes: u64) {
        self.bytes.inc_by(bytes);
    }

    /// Counts a fragment repaired `took` after it was found lost.
    pub(crate) fn repaired_after(&self, took: Duration) {
        self.repaired.inc();
        self.duration.observe(took.as_secs_f64());
    }

    /// Records that `shard` knows `lost` fragments lost, the oldest of
    /// them since `since`, or none if `lost` is 0.
    pub(crate) fn backlog(&self, shard: &ShardRef, lost: u64, since: Option<Instant>) {
        let mut backlog = self.backlog.lock().unwrap_or_else(PoisonError::into_inner);
        match since {
            Some(since) if lost > 0 => backlog.insert(shard.clone(), (lost, since)),
            _ => backlog.remove(shard),
        };
        let total: u64 = backlog.values().map(|(lost, _)| lost).sum();
        self.unrepaired
            .set(i64::try_from(total).unwrap_or(i64::MAX));
        let oldest = backlog.values().map(|(_, since)| *since).min();
        self.oldest
            .set(oldest.map_or(0.0, |since| since.elapsed().as_secs_f64()));
    }
}

#[cfg(test)]
mod tests {
    use skys3_types::{BucketId, ShardId};

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn the_backlog_sums_over_shards() {
        let registry = MetricsRegistry::new();
        let metrics = RepairMetrics::register(&registry);
        let shard = |n| ShardRef::new(BucketId::new("b-1").unwrap(), ShardId::new(n));
        let start = Instant::now();
        metrics.backlog(&shard(0), 3, Some(start));
        tokio::time::sleep(Duration::from_secs(2)).await;
        metrics.backlog(&shard(1), 2, Some(Instant::now()));
        assert_eq!(metrics.unrepaired(), 5);
        assert!((metrics.oldest.get() - 2.0).abs() < 1e-9);
        metrics.backlog(&shard(0), 0, None);
        assert_eq!(metrics.unrepaired(), 2);
        metrics.backlog(&shard(1), 0, Some(start));
        assert_eq!(metrics.unrepaired(), 0);
        assert!(metrics.oldest.get().abs() < 1e-9);

        metrics.transferred(10);
        metrics.repaired_after(Duration::from_secs(3));
        assert_eq!((metrics.repaired(), metrics.bytes()), (1, 10));
        metrics.moved_fragment();
        assert_eq!(metrics.moved(), 1);
        let text = registry.encode().unwrap();
        for name in [
            "skys3_repaired_fragments_total",
            "skys3_moved_fragments_total",
            "skys3_repair_bytes_total",
            "skys3_repair_duration_seconds_bucket",
            "skys3_unrepaired_fragments",
            "skys3_oldest_unrepaired_age_seconds",
        ] {
            assert!(text.contains(name), "{name} missing from {text}");
        }
    }
}
