//! The node's own metrics (`docs/skys3-metrics.md` section 3.1): its
//! disks, and how current its copy of control state is (§6.2, §6.10).
//!
//! The components a node runs register their own metrics in the same
//! registry. The tests check the metrics reference against every metric
//! the code registers, and the alerting rules and the dashboard under
//! `deploy/` against the reference.

use std::sync::atomic::AtomicU64;
use std::time::Duration;

use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Unit;
use skys3_obs::MetricsRegistry;

/// A gauge of seconds, such as a Unix time.
type Seconds = Gauge<f64, AtomicU64>;

/// The node's metrics beyond the admin listener's and its components'.
pub(crate) struct NodeMetrics {
    pub(crate) registry: MetricsRegistry,
    pub(crate) disks_out_of_service: Gauge,
    pub(crate) control: ControlMetrics,
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
        Self {
            registry,
            disks_out_of_service,
            control,
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
}

impl ControlMetrics {
    fn register(registry: &MetricsRegistry) -> Self {
        let metrics = Self::default();
        registry.register(
            "control_store_live",
            "1 once the control store answered since startup; 0 while the node serves its \
             local copy of control state.",
            metrics.live.clone(),
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
}

#[cfg(test)]
mod reference;

#[cfg(test)]
mod runbooks;

#[cfg(test)]
mod tests {
    use super::*;

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
        ] {
            assert!(text.contains(line), "{line} in {text}");
        }

        let control = metrics.control.clone();
        control.set_live(true);
        control.store_answered(Duration::from_millis(1_700_000_000_500));
        control.identity_synced(Duration::from_secs(1_699_999_000));
        let text = metrics.registry.encode().unwrap();
        for line in [
            "skys3_control_store_live 1\n",
            "skys3_control_store_last_success_timestamp_seconds 1700000000.5\n",
            "skys3_identity_synced_timestamp_seconds 1699999000.0\n",
        ] {
            assert!(text.contains(line), "{line} in {text}");
        }
    }
}
