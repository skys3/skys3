//! The coordinator lease (design §6.7): one register, `coordinator.lease`,
//! that names the node acting as coordinator, built from `put_if` and local
//! timers alone.
//!
//! - The holder renews the lease every `coordinator_lease / 3` with
//!   `If-Match` on the version it last wrote. Each renewal carries a fresh
//!   `proposal_id`, so the version changes even though the holder does not.
//! - A candidate takes over only after it has observed the same version for
//!   longer than `coordinator_lease × (1+ρ)` on its own monotonic clock, and
//!   then only by `If-Match` on that version. No clock readings are
//!   compared between nodes.
//! - The holder acts as coordinator only until `coordinator_lease × (1−ρ)`
//!   has passed on its own clock since it sent the write that last renewed
//!   the lease. While clocks drift within `ρ`, its belief ends before any
//!   candidate's wait does, so two nodes never both believe they are
//!   coordinator. Beyond `ρ`, or after a long pause, they can, and every
//!   change being a compare-and-swap keeps that safe ([`crate::apply`]).

use std::sync::Arc;
use std::time::Duration;

use skys3_control::{
    ControlError, ControlStore, Expected, ProposalIds, ProposalOutcome, RetryPolicy, TypedKey,
    Version, propose_document, read,
};
use skys3_io::{Clock, MonoTime};
use skys3_types::{CoordinatorLease, NodeId};
use tokio::sync::watch;

/// The timings of the coordinator lease.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LeaseConfig {
    lease: Duration,
    drift: f64,
    /// How each write and read of the lease is retried. A renewal must
    /// fit well inside the lease, so the policy's total backoff should be
    /// a fraction of [`LeaseConfig::renew_interval`].
    pub retry: RetryPolicy,
}

/// Why lease timings are unusable.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
pub enum LeaseConfigError {
    /// The lease is zero.
    #[error("the coordinator lease must be longer than zero")]
    ZeroLease,
    /// The drift bound is not in `[0, 1)`.
    #[error("the clock drift bound must be at least 0 and less than 1, not {0}")]
    Drift(f64),
}

impl LeaseConfig {
    /// Timings for a lease of `lease` (`coordinator_lease`) under the
    /// clock drift bound `drift` (`ρ`, `assumed_clock_drift`), with the
    /// default retry policy.
    ///
    /// # Errors
    ///
    /// [`LeaseConfigError`] for a zero lease or a drift bound outside
    /// `[0, 1)`.
    pub fn new(lease: Duration, drift: f64) -> Result<Self, LeaseConfigError> {
        if lease.is_zero() {
            return Err(LeaseConfigError::ZeroLease);
        }
        if !(0.0..1.0).contains(&drift) {
            return Err(LeaseConfigError::Drift(drift));
        }
        Ok(Self {
            lease,
            drift,
            retry: RetryPolicy::default(),
        })
    }

    /// The timings a node's configuration sets:
    /// `control_store.coordinator_lease_seconds` and
    /// `replication.assumed_clock_drift`.
    ///
    /// # Errors
    ///
    /// As [`LeaseConfig::new`]; configuration loading already refuses both
    /// cases.
    pub fn from_config(config: &skys3_config::Config) -> Result<Self, LeaseConfigError> {
        Self::new(
            config.control_store().coordinator_lease(),
            config.replication().assumed_clock_drift,
        )
    }

    /// Sets the retry policy of the lease's reads and writes.
    #[must_use]
    pub fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// `coordinator_lease`.
    #[must_use]
    pub fn lease(&self) -> Duration {
        self.lease
    }

    /// The clock drift bound `ρ`.
    #[must_use]
    pub fn drift(&self) -> f64 {
        self.drift
    }

    /// How often the holder renews: `coordinator_lease / 3`. Candidates
    /// read the lease as often.
    #[must_use]
    pub fn renew_interval(&self) -> Duration {
        self.lease / 3
    }

    /// How long a candidate must observe an unchanged lease before it
    /// takes over: `coordinator_lease × (1+ρ)`.
    #[must_use]
    pub fn takeover_after(&self) -> Duration {
        self.lease.mul_f64(1.0 + self.drift)
    }

    /// How long the holder acts as coordinator after sending the write
    /// that last renewed the lease: `coordinator_lease × (1−ρ)`. On a
    /// clock that runs slow by `ρ` this lasts at most `coordinator_lease`
    /// of real time, and a candidate's wait on a clock fast by `ρ` at
    /// least as long, counted from when it first read the write.
    #[must_use]
    pub fn tenure(&self) -> Duration {
        self.lease.mul_f64(1.0 - self.drift)
    }

    /// The pause before another attempt after a failed read or write.
    fn retry_delay(&self) -> Duration {
        self.renew_interval() / 4
    }
}

/// Whether this node acts as coordinator, as its [`Elector`] last decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Leadership {
    /// Another node holds the lease, or none is known to.
    Follower {
        /// The holder the lease named when this node last read it.
        holder: Option<NodeId>,
    },
    /// This node holds the lease at `version`, and acts as coordinator
    /// until `until` on its own clock unless a renewal extends it.
    Coordinator {
        /// The lease's version as this node last wrote it.
        version: Version,
        /// When the tenure ends without a renewal.
        until: MonoTime,
    },
}

impl Leadership {
    /// Whether this node acts as coordinator at `now` on its own clock.
    #[must_use]
    pub fn is_coordinator_at(&self, now: MonoTime) -> bool {
        matches!(self, Self::Coordinator { until, .. } if now < *until)
    }

    /// When the current tenure ends, for a coordinator.
    #[must_use]
    pub fn until(&self) -> Option<MonoTime> {
        match self {
            Self::Coordinator { until, .. } => Some(*until),
            Self::Follower { .. } => None,
        }
    }
}

/// The lease this node holds.
#[derive(Debug)]
struct Held {
    version: Version,
    until: MonoTime,
    renew_at: MonoTime,
}

/// A lease version this node has seen unchanged since `since`.
#[derive(Debug)]
struct Observed {
    version: Version,
    since: MonoTime,
}

/// The lease write this node last proposed. It is kept while an attempt
/// of it may have landed unseen: a later read that finds its
/// `proposal_id` adopts it, and a new attempt under the same precondition
/// sends it again rather than a new value, so at most one of them lands.
#[derive(Debug)]
struct Pending {
    lease: CoordinatorLease,
    expected: Expected,
    /// When the first attempt was sent: a tenure counts from here.
    sent: MonoTime,
}

/// Contends for the coordinator lease and keeps it while it can: one per
/// node.
///
/// [`Elector::run`] runs the protocol; [`Elector::subscribe`] tells the
/// node's [`Coordinator`](crate::Coordinator) whether it may act.
#[derive(Debug)]
pub struct Elector<S> {
    store: S,
    node: NodeId,
    clock: Arc<dyn Clock>,
    config: LeaseConfig,
    proposals: ProposalIds,
    leadership: watch::Sender<Leadership>,
    held: Option<Held>,
    observed: Option<Observed>,
    pending: Option<Pending>,
}

impl<S: ControlStore> Elector<S> {
    /// An elector for `node`, timing the lease on `clock`, which must be
    /// the node's own monotonic clock.
    pub fn new(
        store: S,
        node: NodeId,
        clock: Arc<dyn Clock>,
        config: LeaseConfig,
        proposals: ProposalIds,
    ) -> Self {
        let (leadership, _) = watch::channel(Leadership::Follower { holder: None });
        Self {
            store,
            node,
            clock,
            config,
            proposals,
            leadership,
            held: None,
            observed: None,
            pending: None,
        }
    }

    /// Follows this node's leadership as the elector decides it. A
    /// [`Leadership::Coordinator`] ends at its `until` even if no update
    /// follows, for example while a renewal is still being retried.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<Leadership> {
        self.leadership.subscribe()
    }

    /// The leadership as last decided.
    #[must_use]
    pub fn leadership(&self) -> Leadership {
        self.leadership.borrow().clone()
    }

    /// Runs the protocol until the task is dropped.
    pub async fn run(mut self) {
        loop {
            let wake = self.round().await;
            self.clock.sleep_until(wake).await;
        }
    }

    /// Runs one round: renews a lease that is due, or reads the lease and
    /// takes it over if it is free or has gone unchanged for long enough.
    /// Returns when the next round is due, on this node's clock.
    pub async fn round(&mut self) -> MonoTime {
        match self.held.take() {
            Some(held) => self.renew(held).await,
            None => self.contend().await,
        }
    }

    async fn renew(&mut self, held: Held) -> MonoTime {
        if self.clock.now() < held.renew_at {
            let renew_at = held.renew_at;
            self.held = Some(held);
            return renew_at;
        }
        if self.clock.now() >= held.until {
            // The tenure lapsed unrenewed. The lease may still be this
            // node's, so it keeps renewing, but acts again only once a
            // renewal lands.
            let lapsed = self.publish(Leadership::Follower {
                holder: Some(self.node.clone()),
            });
            if lapsed {
                tracing::warn!(node = %self.node, "the coordinator tenure ended unrenewed");
            }
        }
        match self.propose(Expected::Version(held.version.clone())).await {
            Ok(Some((version, sent))) => self.hold(version, sent),
            Ok(None) => {
                tracing::info!(node = %self.node, "another node took the coordinator lease");
                self.step_down();
                self.clock.now()
            }
            Err(error) => {
                tracing::warn!(node = %self.node, %error, "cannot renew the coordinator lease");
                let now = self.clock.now();
                let mut renew_at = now.saturating_add(self.config.retry_delay());
                if now < held.until {
                    // Try again, at the latest when the tenure ends.
                    renew_at = renew_at.min(held.until);
                }
                self.held = Some(Held { renew_at, ..held });
                renew_at
            }
        }
    }

    async fn contend(&mut self) -> MonoTime {
        let current = match read(&self.store, &TypedKey::coordinator_lease()).await {
            Ok(current) => current,
            Err(error) => {
                tracing::warn!(node = %self.node, %error, "cannot read the coordinator lease");
                return self.clock.now().saturating_add(self.config.retry_delay());
            }
        };
        // The read's answer arrived after the version it returns was
        // written, so a wait counted from now is never longer than the
        // time since that write.
        let now = self.clock.now();
        let Some(current) = current else {
            self.observed = None;
            return self.take_over(Expected::Absent).await;
        };
        if let Some(pending) = self
            .pending
            .take_if(|pending| pending.lease.proposal_id == current.value.proposal_id)
        {
            // A write of ours landed although its answer was lost.
            return self.hold(current.version, pending.sent);
        }
        self.publish(Leadership::Follower {
            holder: Some(current.value.holder),
        });
        let since = match &self.observed {
            Some(observed) if observed.version == current.version => observed.since,
            _ => {
                self.observed = Some(Observed {
                    version: current.version.clone(),
                    since: now,
                });
                now
            }
        };
        let due = since.saturating_add(self.config.takeover_after());
        if now > due {
            return self.take_over(Expected::Version(current.version)).await;
        }
        // "Longer than": the first reading strictly past `due`.
        let next = now.saturating_add(self.config.renew_interval());
        next.min(due.saturating_add(Duration::from_nanos(1)))
    }

    async fn take_over(&mut self, expected: Expected) -> MonoTime {
        match self.propose(expected).await {
            Ok(Some((version, sent))) => {
                tracing::info!(node = %self.node, "took the coordinator lease");
                self.hold(version, sent)
            }
            // Another node got there first: read what it wrote.
            Ok(None) => {
                self.observed = None;
                self.clock.now()
            }
            Err(error) => {
                tracing::warn!(node = %self.node, %error, "cannot take the coordinator lease");
                self.clock.now().saturating_add(self.config.retry_delay())
            }
        }
    }

    /// Writes a lease naming this node under `expected`: the version it
    /// accepted and when the write was first sent, or `None` if the
    /// precondition failed.
    async fn propose(
        &mut self,
        expected: Expected,
    ) -> Result<Option<(Version, MonoTime)>, ControlError> {
        let pending = match self.pending.take() {
            Some(pending) if pending.expected == expected => pending,
            _ => Pending {
                lease: CoordinatorLease {
                    holder: self.node.clone(),
                    proposal_id: self.proposals.next_id(),
                },
                expected: expected.clone(),
                sent: self.clock.now(),
            },
        };
        let key = TypedKey::coordinator_lease();
        let outcome =
            propose_document(&self.store, &key, expected, &pending.lease, &self.config.retry).await;
        match outcome {
            Ok(ProposalOutcome::Accepted(version)) => Ok(Some((version, pending.sent))),
            Ok(ProposalOutcome::Rejected) => {
                self.pending = Some(pending);
                Ok(None)
            }
            Err(error) => {
                self.pending = Some(pending);
                Err(error)
            }
        }
    }

    /// Holds the lease at `version`, written by a request first sent at
    /// `sent`. Returns when to renew it.
    fn hold(&mut self, version: Version, sent: MonoTime) -> MonoTime {
        let until = sent.saturating_add(self.config.tenure());
        let now = self.clock.now();
        // A write adopted late may already be past its tenure: renew at
        // once, and act only once the renewal lands.
        let renew_at = sent
            .saturating_add(self.config.renew_interval())
            .min(until.max(now));
        self.observed = None;
        self.publish(if now < until {
            Leadership::Coordinator {
                version: version.clone(),
                until,
            }
        } else {
            Leadership::Follower {
                holder: Some(self.node.clone()),
            }
        });
        self.held = Some(Held {
            version,
            until,
            renew_at,
        });
        renew_at
    }

    fn step_down(&mut self) {
        self.held = None;
        self.observed = None;
        self.publish(Leadership::Follower { holder: None });
    }

    /// Publishes `leadership`, and returns whether it changed.
    fn publish(&self, leadership: Leadership) -> bool {
        self.leadership.send_if_modified(|current| {
            let changed = *current != leadership;
            *current = leadership;
            changed
        })
    }
}

#[cfg(test)]
mod tests;
