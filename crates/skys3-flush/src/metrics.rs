//! The flush metrics (§7.6, §13), labeled by bucket name. The metrics
//! reference, `docs/skys3-metrics.md`, describes each.

use std::sync::atomic::AtomicU64;

use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Unit;
use skys3_obs::MetricsRegistry;

/// The label set of every flush metric: `bucket`, the bucket's name.
type Labels = Vec<(String, String)>;

/// The buckets of the streaming-overlap histogram: tenths of an object.
const OVERLAP_BUCKETS: [f64; 10] = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0];

/// A streaming-overlap histogram.
fn overlap_histogram() -> Histogram {
    Histogram::new(OVERLAP_BUCKETS)
}

/// The counters one target's flushers count events in. The default
/// counters belong to no registry.
#[derive(Debug, Clone)]
pub struct Counters {
    /// Versions flushed: the remote accepted them, or already held them.
    pub flushes: Counter,
    /// Flush attempts that failed and will be retried.
    pub retries: Counter,
    /// Keys put in conflict.
    pub conflicts: Counter,
    /// Conflicts resolved by sending the local version unconditionally,
    /// over the out-of-band write (`overwrite`, §7.2).
    pub overwritten: Counter,
    /// Conflicts resolved by adopting the out-of-band write and dropping
    /// the local version (`discard_local`, §7.2).
    pub discarded: Counter,
    /// Copies flushed as a server-side `CopyObject` (§7.2, §11).
    pub copies: Counter,
    /// Copies meant for a server-side `CopyObject` that were sent as
    /// regular uploads: the remote source had changed, or the target
    /// refused the copy.
    pub copy_fallbacks: Counter,
    /// Evicted versions filled from the remote into the clean cache.
    pub fills: Counter,
    /// Fills that found the remote changed out of band (§9.2).
    pub fill_conflicts: Counter,
    /// For each streamed multipart object, the fraction of its bytes the
    /// remote held when the client completed it (§7.3, §16.3).
    pub streaming_overlap: Histogram,
    /// Requests the target answered with a throttle, such as `503
    /// SlowDown`, which shrinks the window of requests in flight (§7.7).
    pub throttles: Counter,
}

impl Default for Counters {
    fn default() -> Self {
        Self {
            flushes: Counter::default(),
            retries: Counter::default(),
            conflicts: Counter::default(),
            overwritten: Counter::default(),
            discarded: Counter::default(),
            copies: Counter::default(),
            copy_fallbacks: Counter::default(),
            fills: Counter::default(),
            fill_conflicts: Counter::default(),
            streaming_overlap: overlap_histogram(),
            throttles: Counter::default(),
        }
    }
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
    /// Remote multipart uploads flushes left open that wait to be aborted.
    pub orphaned_uploads: u64,
    /// This node's share of the bucket's dirty-data budget, in bytes.
    pub dirty_budget: u64,
    /// The requests the flushers may have in flight to the target (§7.7).
    pub concurrency: u64,
    /// The bytes the flushers hold for requests to the target.
    pub inflight_bytes: u64,
    /// The base round trip of the target, in seconds; 0 until measured.
    pub base_round_trip: f64,
}

/// The flush metrics of a node.
#[derive(Debug, Clone)]
pub struct FlushMetrics {
    dirty_bytes: Family<Labels, Gauge>,
    dirty_budget: Family<Labels, Gauge>,
    oldest_dirty_age: Family<Labels, Gauge<f64, AtomicU64>>,
    flush_lag: Family<Labels, Gauge<f64, AtomicU64>>,
    conflicted_keys: Family<Labels, Gauge>,
    orphaned_uploads: Family<Labels, Gauge>,
    concurrency: Family<Labels, Gauge>,
    inflight_bytes: Family<Labels, Gauge>,
    base_round_trip: Family<Labels, Gauge<f64, AtomicU64>>,
    throttles: Family<Labels, Counter>,
    flushes: Family<Labels, Counter>,
    retries: Family<Labels, Counter>,
    conflicts: Family<Labels, Counter>,
    overwritten: Family<Labels, Counter>,
    discarded: Family<Labels, Counter>,
    copies: Family<Labels, Counter>,
    copy_fallbacks: Family<Labels, Counter>,
    fills: Family<Labels, Counter>,
    fill_conflicts: Family<Labels, Counter>,
    streaming_overlap: Family<Labels, Histogram, fn() -> Histogram>,
}

impl Default for FlushMetrics {
    fn default() -> Self {
        Self {
            dirty_bytes: Family::default(),
            dirty_budget: Family::default(),
            oldest_dirty_age: Family::default(),
            flush_lag: Family::default(),
            conflicted_keys: Family::default(),
            orphaned_uploads: Family::default(),
            concurrency: Family::default(),
            inflight_bytes: Family::default(),
            base_round_trip: Family::default(),
            throttles: Family::default(),
            flushes: Family::default(),
            retries: Family::default(),
            conflicts: Family::default(),
            overwritten: Family::default(),
            discarded: Family::default(),
            copies: Family::default(),
            copy_fallbacks: Family::default(),
            fills: Family::default(),
            fill_conflicts: Family::default(),
            streaming_overlap: Family::new_with_constructor(overlap_histogram),
        }
    }
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
            "dirty_budget",
            "This node's share of the bucket's dirty-data budget (max_dirty_bytes): new writes \
             get 503 SlowDown while dirty bytes are at or above it.",
            Unit::Bytes,
            metrics.dirty_budget.clone(),
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
            "flush_orphaned_uploads",
            "Remote multipart uploads that flushes left open, because an abort failed or a \
             flusher stopped, and that wait to be aborted.",
            metrics.orphaned_uploads.clone(),
        );
        registry.register(
            "flush_concurrency",
            "Requests the flushers may have in flight to the remote target: the adaptive \
             window, between flush_min_concurrency_per_shard and \
             flush_max_concurrency_per_shard times the flushing shards.",
            metrics.concurrency.clone(),
        );
        registry.register_with_unit(
            "flush_inflight",
            "Bytes the flushers hold in memory for requests in flight to the remote target, \
             bounded by flush_max_inflight_bytes_per_target.",
            Unit::Bytes,
            metrics.inflight_bytes.clone(),
        );
        registry.register_with_unit(
            "flush_base_round_trip",
            "The remote target's base round trip, which adaptive flush concurrency compares \
             latency with: the smallest mean latency of recent rounds.",
            Unit::Seconds,
            metrics.base_round_trip.clone(),
        );
        registry.register(
            "flush_throttles",
            "Flush requests the remote target answered with a throttle (503 SlowDown, 429, or \
             a throttling code), each of which may shrink the flush concurrency.",
            metrics.throttles.clone(),
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
        registry.register(
            "flush_conflicts_overwritten",
            "Conflicts resolved by flushing the local version unconditionally over the \
             out-of-band write (the overwrite policy).",
            metrics.overwritten.clone(),
        );
        registry.register(
            "flush_conflicts_discarded",
            "Conflicts resolved by adopting the out-of-band write and dropping the local \
             version, an acknowledged write (the discard_local policy).",
            metrics.discarded.clone(),
        );
        registry.register(
            "flush_copies",
            "Copies flushed as a server-side CopyObject from a clean source in the same \
             remote bucket.",
            metrics.copies.clone(),
        );
        registry.register(
            "flush_copy_fallbacks",
            "Copies meant for a server-side CopyObject that were sent as regular uploads, \
             because the remote source had changed or the target refused the copy.",
            metrics.copy_fallbacks.clone(),
        );
        registry.register(
            "fills",
            "Evicted versions filled from the remote target into the clean cache.",
            metrics.fills.clone(),
        );
        registry.register(
            "fill_conflicts",
            "Read-through fills that found the remote changed out of band, whether the \
             remote version was adopted or a local write came first.",
            metrics.fill_conflicts.clone(),
        );
        registry.register_with_unit(
            "flush_streaming_overlap",
            "For each streamed multipart object, the fraction of its bytes already at the \
             remote target when the client completed the upload.",
            Unit::Other("ratio".to_owned()),
            metrics.streaming_overlap.clone(),
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
            overwritten: self.overwritten.get_or_create_owned(&labels),
            discarded: self.discarded.get_or_create_owned(&labels),
            copies: self.copies.get_or_create_owned(&labels),
            copy_fallbacks: self.copy_fallbacks.get_or_create_owned(&labels),
            fills: self.fills.get_or_create_owned(&labels),
            fill_conflicts: self.fill_conflicts.get_or_create_owned(&labels),
            streaming_overlap: self.streaming_overlap.get_or_create_owned(&labels),
            throttles: self.throttles.get_or_create_owned(&labels),
        }
    }

    /// Sets `bucket`'s gauges.
    pub fn set(&self, bucket: &str, gauges: Gauges) {
        let labels = labels(bucket);
        let saturate = |n: u64| i64::try_from(n).unwrap_or(i64::MAX);
        self.dirty_bytes
            .get_or_create(&labels)
            .set(saturate(gauges.dirty_bytes));
        self.dirty_budget
            .get_or_create(&labels)
            .set(saturate(gauges.dirty_budget));
        self.oldest_dirty_age
            .get_or_create(&labels)
            .set(gauges.oldest_dirty_age);
        self.flush_lag.get_or_create(&labels).set(gauges.flush_lag);
        self.conflicted_keys
            .get_or_create(&labels)
            .set(saturate(gauges.conflicted_keys));
        self.orphaned_uploads
            .get_or_create(&labels)
            .set(saturate(gauges.orphaned_uploads));
        self.concurrency
            .get_or_create(&labels)
            .set(saturate(gauges.concurrency));
        self.inflight_bytes
            .get_or_create(&labels)
            .set(saturate(gauges.inflight_bytes));
        self.base_round_trip
            .get_or_create(&labels)
            .set(gauges.base_round_trip);
    }

    /// Forgets `bucket`'s series, once it is no longer flushed here.
    pub fn remove(&self, bucket: &str) {
        let labels = labels(bucket);
        self.dirty_bytes.remove(&labels);
        self.dirty_budget.remove(&labels);
        self.oldest_dirty_age.remove(&labels);
        self.flush_lag.remove(&labels);
        self.conflicted_keys.remove(&labels);
        self.orphaned_uploads.remove(&labels);
        self.concurrency.remove(&labels);
        self.inflight_bytes.remove(&labels);
        self.base_round_trip.remove(&labels);
    }
}

fn labels(bucket: &str) -> Labels {
    vec![("bucket".to_owned(), bucket.to_owned())]
}
