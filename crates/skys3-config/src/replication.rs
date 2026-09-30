//! `[replication]`: acknowledgement timeouts, member failure detection,
//! leases, and node bookkeeping (§5.2, §5.4, §6.3–§6.5, §6.7).

use std::time::Duration;

use serde::Deserialize;

use crate::error::Checker;

/// How requests behave while a member is failing (§5.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AckTimeoutMode {
    /// Requests in flight wait while the primary removes the member, then
    /// commit with the remaining members. `replica_ack_timeout` must exceed
    /// `member_suspect_after` plus [`ReplicationConfig::CAS_ALLOWANCE`].
    #[default]
    WaitThrough,
    /// Requests fail with `503 SlowDown` as soon as a member is late.
    FailFast,
}

/// `[replication]`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReplicationConfig {
    /// `replica_ack_timeout_ms`: how long a write waits for every member's
    /// acknowledgement before it fails with `503 SlowDown` (§5.2).
    pub replica_ack_timeout_ms: u64,
    /// `replica_ack_timeout_mode`: whether that timeout waits through a
    /// member's removal or fails fast (§5.2).
    pub replica_ack_timeout_mode: AckTimeoutMode,
    /// `member_suspect_after_ms`: how long a member may stay unresponsive
    /// before it is removed (§6.3, §6.4).
    pub member_suspect_after_ms: u64,
    /// `lease_renew_interval_ms`: how often the primary sends lease beacons
    /// (§5.4).
    pub lease_renew_interval_ms: u64,
    /// `primary_lease_ms`: how long a member's acknowledgement of a beacon
    /// lets the primary serve reads, on the primary's clock (§5.4).
    pub primary_lease_ms: u64,
    /// `primary_grace_ms`: how long a member waits after the last beacon it
    /// acknowledged before it proposes itself as primary (§5.4, §6.5).
    pub primary_grace_ms: u64,
    /// `assumed_clock_drift`: the bound `ρ` on clock rate drift between
    /// nodes (§5.4).
    pub assumed_clock_drift: f64,
    /// `node_forget_after_hours`: how long a node may stay unreachable
    /// before the coordinator forgets it (§6.7).
    pub node_forget_after_hours: u64,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            replica_ack_timeout_ms: 5000,
            replica_ack_timeout_mode: AckTimeoutMode::WaitThrough,
            member_suspect_after_ms: 3000,
            lease_renew_interval_ms: 1000,
            primary_lease_ms: 4000,
            primary_grace_ms: 6000,
            assumed_clock_drift: 0.01,
            node_forget_after_hours: 24,
        }
    }
}

crate::durations! {
    ReplicationConfig {
        /// `replica_ack_timeout_ms`.
        replica_ack_timeout => replica_ack_timeout_ms, Duration::from_millis;
        /// `member_suspect_after_ms`.
        member_suspect_after => member_suspect_after_ms, Duration::from_millis;
        /// `lease_renew_interval_ms`.
        lease_renew_interval => lease_renew_interval_ms, Duration::from_millis;
        /// `primary_lease_ms`.
        primary_lease => primary_lease_ms, Duration::from_millis;
        /// `primary_grace_ms`.
        primary_grace => primary_grace_ms, Duration::from_millis;
        /// `node_forget_after_hours`.
        node_forget_after => node_forget_after_hours, crate::hours;
    }
}

impl ReplicationConfig {
    /// The margin `primary_grace` keeps above the drift-adjusted lease
    /// (§5.4). It covers the time between a primary's lease check and the
    /// index read it admits, and timer granularity.
    pub const LEASE_MARGIN: Duration = Duration::from_millis(500);

    /// The time allowed for the control-store CAS that removes a member
    /// (§5.2). In wait-through mode, `replica_ack_timeout` must exceed
    /// `member_suspect_after` plus this allowance, or requests in flight
    /// time out before the removal lets them commit.
    pub const CAS_ALLOWANCE: Duration = Duration::from_secs(1);

    /// The smallest `primary_grace` in milliseconds that keeps reads
    /// linearizable: `primary_lease × (1+ρ)/(1−ρ) + margin`, rounded up
    /// (§5.4). Meaningful only for `0 ≤ ρ < 1`.
    #[must_use]
    pub fn min_primary_grace_ms(&self) -> u64 {
        let rho = self.assumed_clock_drift;
        // Lease durations are far below 2^52 ms, so the conversion is exact;
        // the conversion back saturates.
        let drift_adjusted =
            (self.primary_lease_ms as f64 * (1.0 + rho) / (1.0 - rho)).ceil() as u64;
        drift_adjusted.saturating_add(duration_ms(Self::LEASE_MARGIN))
    }

    pub(crate) fn check(&self, checker: &mut Checker) {
        let durations = [
            (
                "replication.replica_ack_timeout_ms",
                self.replica_ack_timeout_ms,
            ),
            (
                "replication.member_suspect_after_ms",
                self.member_suspect_after_ms,
            ),
            (
                "replication.lease_renew_interval_ms",
                self.lease_renew_interval_ms,
            ),
            ("replication.primary_lease_ms", self.primary_lease_ms),
            ("replication.primary_grace_ms", self.primary_grace_ms),
            (
                "replication.node_forget_after_hours",
                self.node_forget_after_hours,
            ),
        ];
        for (key, value) in durations {
            checker.nonzero(key, value);
        }

        let rho = self.assumed_clock_drift;
        checker.fraction("replication.assumed_clock_drift", rho, 1.0, false);
        if (0.0..1.0).contains(&rho) {
            let min = self.min_primary_grace_ms();
            checker.require(
                self.primary_grace_ms >= min,
                "replication.primary_grace_ms",
                || {
                    format!(
                        "is {}; with primary_lease_ms = {} and assumed_clock_drift = {rho} it \
                         must be at least {min} (primary_lease × (1+ρ)/(1−ρ) + {} ms, §5.4)",
                        self.primary_grace_ms,
                        self.primary_lease_ms,
                        duration_ms(Self::LEASE_MARGIN),
                    )
                },
            );
        }

        checker.require(
            self.lease_renew_interval_ms < self.primary_lease_ms,
            "replication.lease_renew_interval_ms",
            || {
                format!(
                    "is {}; it must be less than primary_lease_ms ({}), or leases lapse between \
                     beacons",
                    self.lease_renew_interval_ms, self.primary_lease_ms
                )
            },
        );
        checker.require(
            self.member_suspect_after_ms > self.lease_renew_interval_ms,
            "replication.member_suspect_after_ms",
            || {
                format!(
                    "is {}; it must be greater than lease_renew_interval_ms ({}), or healthy \
                     members are suspected between heartbeats",
                    self.member_suspect_after_ms, self.lease_renew_interval_ms
                )
            },
        );

        if self.replica_ack_timeout_mode == AckTimeoutMode::WaitThrough {
            let allowance = duration_ms(Self::CAS_ALLOWANCE);
            let min_exclusive = self.member_suspect_after_ms.saturating_add(allowance);
            checker.require(
                self.replica_ack_timeout_ms > min_exclusive,
                "replication.replica_ack_timeout_ms",
                || {
                    format!(
                        "is {}; in wait_through mode it must be greater than \
                         member_suspect_after_ms ({}) plus {allowance} ms for the removal CAS \
                         (§5.2), or set replica_ack_timeout_mode = \"fail_fast\"",
                        self.replica_ack_timeout_ms, self.member_suspect_after_ms
                    )
                },
            );
        }
    }
}

/// Whole milliseconds of a constant duration.
fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid_and_match_the_design() {
        let config = ReplicationConfig::default();
        let mut checker = Checker::default();
        config.check(&mut checker);
        assert!(checker.finish().is_ok());
        assert_eq!(config.primary_lease(), Duration::from_secs(4));
        assert_eq!(config.primary_grace(), Duration::from_secs(6));
        assert_eq!(config.node_forget_after(), Duration::from_secs(24 * 3600));
        // 4000 × 1.01 / 0.99 = 4080.8, rounded up, plus the 500 ms margin.
        assert_eq!(config.min_primary_grace_ms(), 4581);
    }

    #[test]
    fn zero_drift_needs_only_the_margin() {
        let config = ReplicationConfig {
            assumed_clock_drift: 0.0,
            ..ReplicationConfig::default()
        };
        assert_eq!(config.min_primary_grace_ms(), 4500);
    }
}
