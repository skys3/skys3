//! The flush metrics (§7.6, §13), labeled by bucket name. The metrics
//! reference, `docs/skys3-metrics.md`, describes each.

use std::sync::atomic::AtomicU64;

use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Unit;
use skys3_obs::MetricsRegistry;

/// The label set of every flush metric: `bucket`, the bucket's name.
type Labels = Vec<(String, String)>;

/// The counters one target's flushers count events in. The default
/// counters belong to no registry.
#[derive(Debug, Clone, Default)]
pub struct Counters {
    /// Versions flushed: the remote accepted them, or already held them.
    pub flushes: Counter,
    /// Flush attempts that failed and will be retried.
    pub retries: Counter,
    /// Keys put in conflict.
    pub conflicts: Counter,
}

/// A bucket's flush gauges at one moment.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Gauges {
    /// Bytes of versions not yet at the remote, conflicts included.
    pub dirty_bytes: u64,
    /// Age of the oldest change not yet at the remote, in seconds.
    pub oldest_dirty_age: f64,
    /// Age of the oldest change the flushers are still working on, held
    /// conflicts excluded, in seconds.
    pub flush_lag: f64,
    /// Keys held in conflict.
    pub conflicted_keys: u64,
}

/// The flush metrics of a node.
#[derive(Debug, Clone, Default)]
pub struct FlushMetrics {
    dirty_bytes: Family<Labels, Gauge>,
    oldest_dirty_age: Family<Labels, Gauge<f64, AtomicU64>>,
    flush_lag: Family<Labels, Gauge<f64, AtomicU64>>,
    conflicted_keys: Family<Labels, Gauge>,
    flushes: Family<Labels, Counter>,
    retries: Family<Labels, Counter>,
    conflicts: Family<Labels, Counter>,
}

impl FlushMetrics {
    /// Registers the flush metrics in `registry`.
    #[must_use]
    pub fn register(registry: &MetricsRegistry) -> Self {
        let metrics = Self::default();
        registry.register_with_unit(
            "dirty",
            "Bytes of committed versions not yet flushed to the remote target, conflicts \
             included.",
            Unit::Bytes,
            metrics.dirty_bytes.clone(),
        );
        registry.register_with_unit(
            "oldest_dirty_age",
            "Age of the oldest committed change not yet flushed to the remote target.",
            Unit::Seconds,
            metrics.oldest_dirty_age.clone(),
        );
        registry.register_with_unit(
            "flush_lag",
            "Age of the oldest committed change the flushers are still working on, held \
             conflicts excluded: how far flushing trails ingest.",
            Unit::Seconds,
            metrics.flush_lag.clone(),
        );
        registry.register(
            "conflicted_keys",
            "Keys whose flush found an out-of-band remote write, held under the hold policy.",
            metrics.conflicted_keys.clone(),
        );
        registry.register(
            "flushes",
            "Versions flushed to the remote target.",
            metrics.flushes.clone(),
        );
        registry.register(
            "flush_retries",
            "Flush attempts that failed and are retried after a backoff.",
            metrics.retries.clone(),
        );
        registry.register(
            "flush_conflicts",
            "Flushes that found an out-of-band remote write and put their key in conflict.",
            metrics.conflicts.clone(),
        );
        metrics
    }

    /// The counters of `bucket`'s flushers.
    #[must_use]
    pub fn counters(&self, bucket: &str) -> Counters {
        let labels = labels(bucket);
        Counters {
            flushes: self.flushes.get_or_create_owned(&labels),
            retries: self.retries.get_or_create_owned(&labels),
            conflicts: self.conflicts.get_or_create_owned(&labels),
        }
    }

    /// Sets `bucket`'s gauges.
    pub fn set(&self, bucket: &str, gauges: Gauges) {
        let labels = labels(bucket);
        let saturate = |n: u64| i64::try_from(n).unwrap_or(i64::MAX);
        self.dirty_bytes
            .get_or_create(&labels)
            .set(saturate(gauges.dirty_bytes));
        self.oldest_dirty_age
            .get_or_create(&labels)
            .set(gauges.oldest_dirty_age);
        self.flush_lag.get_or_create(&labels).set(gauges.flush_lag);
        self.conflicted_keys
            .get_or_create(&labels)
            .set(saturate(gauges.conflicted_keys));
    }

    /// Forgets `bucket`'s series, once it is no longer flushed here.
    pub fn remove(&self, bucket: &str) {
        let labels = labels(bucket);
        self.dirty_bytes.remove(&labels);
        self.oldest_dirty_age.remove(&labels);
        self.flush_lag.remove(&labels);
        self.conflicted_keys.remove(&labels);
    }
}

fn labels(bucket: &str) -> Labels {
    vec![("bucket".to_owned(), bucket.to_owned())]
}
