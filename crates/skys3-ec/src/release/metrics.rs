//! The fragment release metrics of a node (§8.7, §10.3), over its fragment
//! stores. The metrics reference, `docs/skys3-metrics.md`, describes each.

use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Unit;
use skys3_obs::MetricsRegistry;

use crate::CompactionReport;

/// The fragment release metrics of one node. Clones share them; the
/// default ones belong to no registry.
#[derive(Debug, Clone, Default)]
pub struct ReleaseMetrics {
    released: Counter,
    releasing: Gauge,
    reclaimed: Counter,
    segments: Counter,
    removed: Counter,
    copied: Counter,
}

impl ReleaseMetrics {
    /// Registers the fragment release metrics in `registry`.
    #[must_use]
    pub fn register(registry: &MetricsRegistry) -> Self {
        let metrics = Self::default();
        registry.register_with_unit(
            "fragment_released",
            "Bytes of fragments released on this node: fragments of versions that writes \
             replaced and fragments that relocations moved away from, as their shard \
             primaries told it, and orphans their primaries judged.",
            Unit::Bytes,
            metrics.released.clone(),
        );
        registry.register_with_unit(
            "fragment_releasing",
            "Bytes of released fragments this node still keeps, for the release delay or \
             while reads hold them.",
            Unit::Bytes,
            metrics.releasing.clone(),
        );
        registry.register_with_unit(
            "fragment_reclaimed",
            "Bytes of released fragments this node reclaimed: dropped once the release delay \
             passed and no read held them, for fragment-segment compaction to remove.",
            Unit::Bytes,
            metrics.reclaimed.clone(),
        );
        registry.register(
            "fragment_compaction_segments",
            "Fragment segments that compaction removed on this node.",
            metrics.segments.clone(),
        );
        registry.register_with_unit(
            "fragment_compaction_removed",
            "Bytes of the fragment segments compaction removed: the size of their files.",
            Unit::Bytes,
            metrics.removed.clone(),
        );
        registry.register_with_unit(
            "fragment_compaction_copied",
            "Bytes of live fragments compaction copied out of the segments it removed.",
            Unit::Bytes,
            metrics.copied.clone(),
        );
        metrics
    }

    /// The bytes released so far.
    #[must_use]
    pub fn released(&self) -> u64 {
        self.released.get()
    }

    /// The released bytes still kept.
    #[must_use]
    pub fn releasing(&self) -> i64 {
        self.releasing.get()
    }

    /// The released bytes reclaimed so far.
    #[must_use]
    pub fn reclaimed(&self) -> u64 {
        self.reclaimed.get()
    }

    /// Counts a fragment of `bytes` released.
    pub(crate) fn release(&self, bytes: u64) {
        self.released.inc_by(bytes);
        self.releasing.inc_by(gauge(bytes));
    }

    /// Counts a released fragment of `bytes` reclaimed, or forgotten
    /// because its store no longer held it.
    pub(crate) fn reclaim(&self, bytes: u64, reclaimed: bool) {
        self.releasing.dec_by(gauge(bytes));
        if reclaimed {
            self.reclaimed.inc_by(bytes);
        }
    }

    /// Counts what a compaction pass did.
    pub(crate) fn compacted(&self, report: &CompactionReport) {
        self.segments.inc_by(report.segments as u64);
        self.removed.inc_by(report.removed_bytes);
        self.copied.inc_by(report.copied_bytes);
    }
}

fn gauge(bytes: u64) -> i64 {
    i64::try_from(bytes).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_metrics_count_and_register() {
        let registry = MetricsRegistry::new();
        let metrics = ReleaseMetrics::register(&registry);
        metrics.release(100);
        metrics.release(50);
        metrics.reclaim(100, true);
        metrics.reclaim(50, false);
        assert_eq!(
            (metrics.released(), metrics.releasing(), metrics.reclaimed()),
            (150, 0, 100)
        );
        metrics.compacted(&CompactionReport {
            segments: 2,
            copied: 1,
            copied_bytes: 10,
            damaged: 0,
            removed_bytes: 300,
        });
        let text = registry.encode().unwrap();
        for name in [
            "skys3_fragment_released_bytes_total",
            "skys3_fragment_releasing_bytes",
            "skys3_fragment_reclaimed_bytes_total",
            "skys3_fragment_compaction_segments_total",
            "skys3_fragment_compaction_removed_bytes_total",
            "skys3_fragment_compaction_copied_bytes_total",
        ] {
            assert!(text.contains(name), "{name} missing from {text}");
        }
        assert!(text.contains("skys3_fragment_compaction_removed_bytes_total 300"));
    }
}
