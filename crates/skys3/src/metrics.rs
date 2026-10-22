//! The node's own metrics (`docs/skys3-metrics.md` section 3.1): its
//! disks, how current its copy of control state is (§6.2, §6.10), and the
//! TLS handshakes of its listeners (§12).
//!
//! The components a node runs register their own metrics in the same
//! registry. The tests check the metrics reference against every metric
//! the code registers, and the alerting rules and the dashboard under
//! `deploy/` against the reference.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use prometheus_client::encoding::{EncodeMetric, MetricEncoder, NoLabelSet};
use prometheus_client::metrics::MetricType;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Unit;
use skys3_log::LogStats;
use skys3_obs::{HandshakeMetrics, MetricsRegistry};
use skys3_remote::aws::BucketVersioning;

/// A gauge of seconds, such as a Unix time.
type Seconds = Gauge<f64, AtomicU64>;

/// The node's metrics beyond the admin listener's and its components'.
pub(crate) struct NodeMetrics {
    pub(crate) registry: MetricsRegistry,
    pub(crate) disks_out_of_service: Gauge,
    pub(crate) control: ControlMetrics,
    /// The handshakes of the gateway, the transport, and the peer
    /// endpoint.
    pub(crate) handshakes: HandshakeMetrics,
}

impl NodeMetrics {
    /// A registry holding the node's metrics. `identity_max_staleness` is
    /// `[identity] identity_max_staleness_hours`, exported so that alerts
    /// can compare the identity copy's age with it.
    pub(crate) fn new(identity_max_staleness: Duration) -> Self {
        let registry = MetricsRegistry::new();
        let disks_out_of_service = Gauge::default();
        registry.register(
            "disks_out_of_service",
            "Disks taken out of service after an I/O error; used again after a host restart.",
            disks_out_of_service.clone(),
        );
        let control = ControlMetrics::register(&registry);
        control
            .identity_max_staleness
            .set(identity_max_staleness.as_secs_f64());
        let handshakes = HandshakeMetrics::register(&registry);
        Self {
            registry,
            disks_out_of_service,
            control,
            handshakes,
        }
    }
}

/// How current the node's copy of control state is (§6.2): whether the
/// node reaches the control store, and the age of its identity copy,
/// which STS refuses to issue sessions from once it is older than
/// `identity_max_staleness` (§6.10).
#[derive(Debug, Clone, Default)]
pub(crate) struct ControlMetrics {
    live: Gauge,
    last_success: Seconds,
    identity_synced: Seconds,
    identity_max_staleness: Seconds,
    versioning: Gauge,
}

impl ControlMetrics {
    fn register(registry: &MetricsRegistry) -> Self {
        let metrics = Self::default();
        metrics.set_versioning(None);
        registry.register(
            "control_store_live",
            "1 once the control store answered since startup; 0 while the node serves its \
             local copy of control state.",
            metrics.live.clone(),
        );
        registry.register(
            "control_store_versioning",
            "1 if the S3 control bucket keeps versions, 0 if its versioning is suspended or \
             was never enabled, -1 if it is not known: another backend, or a credential \
             without s3:GetBucketVersioning.",
            metrics.versioning.clone(),
        );
        registry.register_with_unit(
            "control_store_last_success_timestamp",
            "When the node last read the control store successfully, as Unix time; 0 if it \
             has not since it started.",
            Unit::Seconds,
            metrics.last_success.clone(),
        );
        registry.register_with_unit(
            "identity_synced_timestamp",
            "When the sync that produced the node's identity copy started, as Unix time; 0 \
             before the first sync.",
            Unit::Seconds,
            metrics.identity_synced.clone(),
        );
        registry.register_with_unit(
            "identity_max_staleness",
            "identity_max_staleness: the identity copy's age past which STS issues no new \
             sessions.",
            Unit::Seconds,
            metrics.identity_max_staleness.clone(),
        );
        metrics
    }

    /// Records whether the node serves from the control store rather than
    /// its local copy.
    pub(crate) fn set_live(&self, live: bool) {
        self.live.set(i64::from(live));
    }

    /// Records a successful read of the control store at `now`, time since
    /// the Unix epoch.
    pub(crate) fn store_answered(&self, now: Duration) {
        self.last_success.set(now.as_secs_f64());
    }

    /// Records that the identity copy now comes from a sync that started at
    /// `started`, time since the Unix epoch.
    pub(crate) fn identity_synced(&self, started: Duration) {
        self.identity_synced.set(started.as_secs_f64());
    }

    /// Records the S3 control bucket's versioning, or `None` if it is not
    /// known (§12).
    pub(crate) fn set_versioning(&self, versioning: Option<BucketVersioning>) {
        self.versioning.set(match versioning {
            Some(BucketVersioning::Enabled) => 1,
            Some(BucketVersioning::Suspended | BucketVersioning::Disabled) => 0,
            None => -1,
        });
    }
}

/// What [`LogMetrics`] reads at each scrape: the summed [`LogStats`] of
/// the node's logs.
type StatsSource = Arc<dyn Fn() -> LogStats + Send + Sync>;

/// The group commits of the node's logs (§5.3, §10.4), summed over its
/// disks: `skys3_log_group_commits_total`, `skys3_log_records_total`,
/// `skys3_log_syncs_total`, `skys3_log_written_bytes_total`, and
/// `skys3_log_commit_seconds_total`. Each is read from the logs' own
/// counters when the registry is encoded, so the commit path does no
/// extra work for them. The performance suite divides them by the PUTs
/// it had acknowledged, to compare with §5.3's table.
pub(crate) struct LogMetrics;

impl LogMetrics {
    /// Registers the log metrics in `registry`, reading `stats` at each
    /// scrape.
    pub(crate) fn register(
        registry: &MetricsRegistry,
        stats: impl Fn() -> LogStats + Send + Sync + 'static,
    ) {
        let stats: StatsSource = Arc::new(stats);
        let counter = |read: fn(&LogStats) -> LogCounterValue| LogCounter {
            stats: Arc::clone(&stats),
            read,
        };
        registry.register(
            "log_group_commits",
            "Group commits of the node's logs that succeeded, each one serial durable round.",
            counter(|s| LogCounterValue::Count(s.group_commits)),
        );
        registry.register(
            "log_records",
            "Records those group commits made durable.",
            counter(|s| LogCounterValue::Count(s.records)),
        );
        registry.register(
            "log_syncs",
            "File syncs those group commits made: one per segment written, and one of the \
             directory after a segment was created.",
            counter(|s| LogCounterValue::Count(s.syncs)),
        );
        registry.register_with_unit(
            "log_written",
            "Bytes those group commits wrote.",
            Unit::Bytes,
            counter(|s| LogCounterValue::Count(s.bytes)),
        );
        registry.register_with_unit(
            "log_commit",
            "Time those group commits took to write and sync.",
            Unit::Seconds,
            counter(|s| LogCounterValue::Seconds(s.commit_time.as_secs_f64())),
        );
    }

    /// The sum of the logs' counts.
    pub(crate) fn sum<'a>(stats: impl IntoIterator<Item = &'a LogStats>) -> LogStats {
        stats
            .into_iter()
            .fold(LogStats::default(), |total, s| LogStats {
                group_commits: total.group_commits + s.group_commits,
                records: total.records + s.records,
                bytes: total.bytes + s.bytes,
                syncs: total.syncs + s.syncs,
                commit_time: total.commit_time + s.commit_time,
            })
    }
}

/// A counter's value as one of the log counters reads it.
enum LogCounterValue {
    Count(u64),
    Seconds(f64),
}

/// One of the [`LogMetrics`] counters.
struct LogCounter {
    stats: StatsSource,
    read: fn(&LogStats) -> LogCounterValue,
}

impl fmt::Debug for LogCounter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LogCounter").finish_non_exhaustive()
    }
}

impl EncodeMetric for LogCounter {
    fn encode(&self, mut encoder: MetricEncoder) -> fmt::Result {
        match (self.read)(&(self.stats)()) {
            LogCounterValue::Count(value) => {
                encoder.encode_counter::<NoLabelSet, _, u64>(&value, None)
            }
            LogCounterValue::Seconds(value) => {
                encoder.encode_counter::<NoLabelSet, _, f64>(&value, None)
            }
        }
    }

    fn metric_type(&self) -> MetricType {
        MetricType::Counter
    }
}

#[cfg(test)]
mod reference;

#[cfg(test)]
mod runbooks;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_log_counters_sum_the_logs_at_each_scrape() {
        let one = LogStats {
            group_commits: 2,
            records: 5,
            bytes: 700,
            syncs: 3,
            commit_time: Duration::from_millis(250),
        };
        let two = LogStats {
            group_commits: 1,
            records: 1,
            bytes: 100,
            syncs: 2,
            commit_time: Duration::from_millis(500),
        };
        let total = LogMetrics::sum([&one, &two]);
        assert_eq!(total.group_commits, 3);
        assert_eq!(total.records, 6);
        assert_eq!(total.bytes, 800);
        assert_eq!(total.syncs, 5);
        assert_eq!(total.commit_time, Duration::from_millis(750));

        let registry = MetricsRegistry::new();
        let commits = Arc::new(AtomicU64::new(3));
        let source = Arc::clone(&commits);
        LogMetrics::register(&registry, move || LogStats {
            group_commits: source.load(std::sync::atomic::Ordering::Relaxed),
            ..total
        });
        let text = registry.encode().unwrap();
        for line in [
            "skys3_log_group_commits_total 3\n",
            "skys3_log_records_total 6\n",
            "skys3_log_syncs_total 5\n",
            "skys3_log_written_bytes_total 800\n",
            "skys3_log_commit_seconds_total 0.75\n",
            "# TYPE skys3_log_syncs counter\n",
        ] {
            assert!(text.contains(line), "{line} in {text}");
        }
        commits.store(10, std::sync::atomic::Ordering::Relaxed);
        let text = registry.encode().unwrap();
        assert!(
            text.contains("skys3_log_group_commits_total 10\n"),
            "{text}"
        );
        assert!(
            format!(
                "{:?}",
                LogCounter {
                    stats: Arc::new(LogStats::default),
                    read: |s| LogCounterValue::Count(s.records),
                }
            )
            .contains("LogCounter")
        );
    }

    #[test]
    fn the_control_gauges_follow_the_store_and_the_identity_copy() {
        let metrics = NodeMetrics::new(Duration::from_secs(24 * 3600));
        let text = metrics.registry.encode().unwrap();
        for line in [
            "skys3_control_store_live 0\n",
            "skys3_control_store_last_success_timestamp_seconds 0.0\n",
            "skys3_identity_synced_timestamp_seconds 0.0\n",
            "skys3_identity_max_staleness_seconds 86400.0\n",
            "skys3_disks_out_of_service 0\n",
            "skys3_control_store_versioning -1\n",
        ] {
            assert!(text.contains(line), "{line} in {text}");
        }

        let control = metrics.control.clone();
        control.set_live(true);
        control.store_answered(Duration::from_millis(1_700_000_000_500));
        control.identity_synced(Duration::from_secs(1_699_999_000));
        control.set_versioning(Some(BucketVersioning::Enabled));
        let text = metrics.registry.encode().unwrap();
        assert!(
            text.contains("skys3_control_store_versioning 1\n"),
            "{text}"
        );
        control.set_versioning(Some(BucketVersioning::Suspended));
        let text = metrics.registry.encode().unwrap();
        for line in [
            "skys3_control_store_versioning 0\n",
            "skys3_control_store_live 1\n",
            "skys3_control_store_last_success_timestamp_seconds 1700000000.5\n",
            "skys3_identity_synced_timestamp_seconds 1699999000.0\n",
        ] {
            assert!(text.contains(line), "{line} in {text}");
        }
    }
}
