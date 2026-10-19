//! The coordinator's metrics (`docs/skys3-metrics.md`): whether this node
//! is coordinator, and the placement health it judges (§6.7).

use prometheus_client::metrics::gauge::Gauge;
use skys3_obs::MetricsRegistry;

use crate::policy::PolicyReport;

/// The coordinator metrics of a node: `skys3_coordinator`,
/// `skys3_placement_unsatisfied_buckets`, and
/// `skys3_placement_short_shards`.
///
/// One set serves both the [`Coordinator`](crate::Coordinator), which
/// reports its tenure ([`Coordinator::with_metrics`]), and the
/// [`PolicyWatch`](crate::PolicyWatch) it runs, which reports what it
/// judged ([`PolicyWatch::with_metrics`]). The placement gauges are
/// nonzero only on the node that is coordinator now: they are cleared
/// when a tenure begins and when it ends, so summing every node's series
/// counts each unsatisfied bucket once. The default metrics belong to no
/// registry.
///
/// [`Coordinator::with_metrics`]: crate::Coordinator::with_metrics
/// [`PolicyWatch::with_metrics`]: crate::PolicyWatch::with_metrics
#[derive(Debug, Clone, Default)]
pub struct CoordinatorMetrics {
    coordinator: Gauge,
    unsatisfied_buckets: Gauge,
    short_shards: Gauge,
}

impl CoordinatorMetrics {
    /// Registers the coordinator metrics in `registry`.
    #[must_use]
    pub fn register(registry: &MetricsRegistry) -> Self {
        let metrics = Self::default();
        registry.register(
            "coordinator",
            "1 while this node is coordinator and serving its tenure, otherwise 0.",
            metrics.coordinator.clone(),
        );
        registry.register(
            "placement_unsatisfied_buckets",
            "Buckets whose placement policy the cluster does not satisfy now, as judged by \
             the coordinator; 0 on every other node.",
            metrics.unsatisfied_buckets.clone(),
        );
        registry.register(
            "placement_short_shards",
            "Shards with fewer members in separate failure domains than their replicas \
             setting, as judged by the coordinator; 0 on every other node.",
            metrics.short_shards.clone(),
        );
        metrics
    }

    /// Records whether this node serves a coordinator tenure. Either way
    /// the placement gauges are cleared: a report from an earlier tenure
    /// is stale, and only the coordinator judges placement.
    pub(crate) fn set_coordinator(&self, serving: bool) {
        self.coordinator.set(i64::from(serving));
        self.set_placement(None);
    }

    /// Records the latest placement judgement, or none.
    pub(crate) fn set_placement(&self, report: Option<&PolicyReport>) {
        let (buckets, shards) = report.map_or((0, 0), |report| {
            let shards = report.unsatisfied.iter().map(|b| b.short.len()).sum();
            (report.unsatisfied.len(), shards)
        });
        self.unsatisfied_buckets
            .set(i64::try_from(buckets).unwrap_or(i64::MAX));
        self.short_shards
            .set(i64::try_from(shards).unwrap_or(i64::MAX));
    }

    #[cfg(test)]
    pub(crate) fn values(&self) -> (i64, i64, i64) {
        (
            self.coordinator.get(),
            self.unsatisfied_buckets.get(),
            self.short_shards.get(),
        )
    }
}

#[cfg(test)]
mod tests {
    use skys3_config::FailureDomain;
    use skys3_types::{BucketId, BucketName, ShardId};

    use super::*;
    use crate::policy::{BucketPolicy, ShortShard};

    fn report(short: &[u8]) -> PolicyReport {
        PolicyReport {
            failure_domain: FailureDomain::Rack,
            eligible_nodes: 2,
            domains: 2,
            unlabeled: Vec::new(),
            unsatisfied: short
                .iter()
                .enumerate()
                .map(|(b, &shards)| BucketPolicy {
                    bucket_id: BucketId::new(format!("b-{b}")).unwrap(),
                    name: BucketName::new(format!("bucket-{b}")).unwrap(),
                    replicas: 3,
                    placeable: false,
                    short: (0..shards)
                        .map(|s| ShortShard {
                            shard: ShardId::new(s),
                            members: Vec::new(),
                            domains: 2,
                        })
                        .collect(),
                    co_located: Vec::new(),
                })
                .collect(),
        }
    }

    #[test]
    fn the_gauges_follow_tenures_and_reports() {
        let registry = MetricsRegistry::new();
        let metrics = CoordinatorMetrics::register(&registry);
        let text = registry.encode().unwrap();
        for line in [
            "skys3_coordinator 0\n",
            "skys3_placement_unsatisfied_buckets 0\n",
            "skys3_placement_short_shards 0\n",
        ] {
            assert!(text.contains(line), "{text}");
        }

        metrics.set_coordinator(true);
        metrics.set_placement(Some(&report(&[2, 1])));
        assert_eq!(metrics.values(), (1, 2, 3));
        metrics.set_placement(Some(&report(&[])));
        assert_eq!(metrics.values(), (1, 0, 0));
        metrics.set_placement(Some(&report(&[4])));
        // A tenure that ends clears what it judged.
        metrics.set_coordinator(false);
        assert_eq!(metrics.values(), (0, 0, 0));
    }
}
