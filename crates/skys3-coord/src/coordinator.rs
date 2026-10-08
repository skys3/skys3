//! The coordinator's work loop (design §6.7): while this node holds the
//! coordinator lease, it asks its [`Placement`] for changes, applies each
//! as compare-and-swaps announced by a new generation, and pushes that
//! generation to every node.
//!
//! What to change is the placement's decision: the node lifecycle
//! ([`Lifecycle`](crate::Lifecycle)) wraps the placement, replacement, and
//! rebalancing of plan tasks M3-03 to M3-06. This loop owns only the rules
//! every change follows.

use std::sync::Arc;
use std::time::Duration;

use skys3_control::{ControlError, ControlStore, ProposalIds, RetryPolicy};
use skys3_io::{Clock, MonoTime};
use skys3_types::ClusterId;
use tokio::sync::{Notify, watch};

use crate::change::{Applied, ChangeSet, Pending, apply, settle};
use crate::lease::Leadership;
use crate::metrics::CoordinatorMetrics;
use crate::push::Announce;

/// The placement work a coordinator does: the extension point for the
/// node registry, placement, replacement, and rebalancing.
///
/// A placement plans from what it reads in the control store, and every
/// write it plans is conditional on the version it read ([`ChangeSet`]).
/// Its view may be stale, and another node may briefly act as coordinator
/// too; either way its change is rejected, and it plans again from what
/// the store holds then.
pub trait Placement: Send + 'static {
    /// Plans the next change, or `None` if there is nothing to do now.
    /// Documents take their `proposal_id`s from `proposals`.
    ///
    /// # Errors
    ///
    /// A [`ControlError`] from reading the store. The coordinator retries
    /// after a pause.
    fn plan<S: ControlStore>(
        &mut self,
        store: &S,
        proposals: &mut ProposalIds,
    ) -> impl Future<Output = Result<Option<ChangeSet>, ControlError>> + Send;

    /// Learns what [`apply`] did with a planned change, also when it
    /// failed part of the way ([`Applied::is_complete`]).
    fn applied(&mut self, change: &ChangeSet, applied: &Applied) {
        let _ = (change, applied);
    }

    /// Learns that this node has just become coordinator, before the
    /// tenure's first plan. What it observed as coordinator before, such
    /// as heartbeats, may be stale: other nodes reported to another
    /// coordinator meanwhile.
    fn begin_tenure(&mut self) {}
}

/// A placement with nothing to do: the coordinator holds the lease and
/// changes nothing.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoPlacement;

impl Placement for NoPlacement {
    async fn plan<S: ControlStore>(
        &mut self,
        _store: &S,
        _proposals: &mut ProposalIds,
    ) -> Result<Option<ChangeSet>, ControlError> {
        Ok(None)
    }
}

/// How a [`Coordinator`] works.
#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    /// The cluster whose `cluster.json` announces changes.
    pub cluster: ClusterId,
    /// How each write of a change is retried.
    pub retry: RetryPolicy,
    /// How long the coordinator waits after the placement had nothing to
    /// do, or failed, before it asks again, unless woken
    /// ([`Coordinator::waker`]).
    pub idle: Duration,
}

/// Does placement work while this node holds the coordinator lease.
///
/// Leadership comes from the node's [`Elector`](crate::Elector), and the
/// coordinator checks it on its own clock before every change: a change
/// planned under a tenure that has since ended is dropped.
///
/// A change that failed part of the way still has what it wrote pushed. A
/// write of it that got no answer, and so may land later, is announced
/// only once [`settle`] has learned its outcome; until then the
/// coordinator settles it before it plans anything else, also after its
/// tenure ends.
pub struct Coordinator<S, P, A> {
    store: S,
    clock: Arc<dyn Clock>,
    leadership: watch::Receiver<Leadership>,
    placement: P,
    announce: A,
    config: CoordinatorConfig,
    proposals: ProposalIds,
    wake: Arc<Notify>,
    /// Changes whose announcement is still owed.
    pending: Vec<Pending>,
    metrics: CoordinatorMetrics,
}

impl<S, P, A> std::fmt::Debug for Coordinator<S, P, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Coordinator")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl<S: ControlStore, P: Placement, A: Announce> Coordinator<S, P, A> {
    /// A coordinator that writes to `store`, acts while `leadership` says
    /// so on `clock`, asks `placement` what to change, and tells nodes
    /// through `announce`.
    pub fn new(
        store: S,
        clock: Arc<dyn Clock>,
        leadership: watch::Receiver<Leadership>,
        placement: P,
        announce: A,
        config: CoordinatorConfig,
        proposals: ProposalIds,
    ) -> Self {
        Self {
            store,
            clock,
            leadership,
            placement,
            announce,
            config,
            proposals,
            wake: Arc::new(Notify::new()),
            pending: Vec::new(),
            metrics: CoordinatorMetrics::default(),
        }
    }

    /// Reports each tenure in `metrics` (`skys3_coordinator`), and clears
    /// its placement gauges when a tenure begins or ends. Pass the same
    /// metrics to the [`PolicyWatch`](crate::PolicyWatch) the placement
    /// runs, if any.
    #[must_use]
    pub fn with_metrics(mut self, metrics: CoordinatorMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    /// Wakes the coordinator to plan before its idle wait ends, for
    /// example when a node registers or misses heartbeats.
    #[must_use]
    pub fn waker(&self) -> Arc<Notify> {
        Arc::clone(&self.wake)
    }

    /// Works whenever this node is coordinator, until the elector is
    /// dropped and nothing is left to settle.
    pub async fn run(mut self) {
        loop {
            self.settle().await;
            if self.tenure().is_some() {
                // `serve_tenure` returns only once the tenure has ended,
                // so each call serves a new tenure.
                // Cleared when the tenure ends, also if this task is
                // dropped during it.
                let _tenure = TenureGauge::begin(self.metrics.clone());
                self.placement.begin_tenure();
                self.serve_tenure().await;
            } else if !self.pending.is_empty() {
                self.clock.sleep(self.config.idle).await;
            } else if self.leadership.changed().await.is_err() {
                return;
            }
        }
    }

    /// Settles every change whose announcement is owed, and announces
    /// what landed. Returns whether none is left.
    async fn settle(&mut self) -> bool {
        let mut left = Vec::new();
        for pending in std::mem::take(&mut self.pending) {
            let settled = settle(
                &self.store,
                &self.config.cluster,
                &pending,
                &mut self.proposals,
                &self.config.retry,
            )
            .await;
            match settled {
                Ok(settled) => {
                    if let Some(generation) = settled.generation {
                        self.announce.announce(generation).await;
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, register = ?pending.unsettled(),
                        "cannot settle a coordinator change yet");
                    left.push(pending);
                }
            }
        }
        self.pending = left;
        self.pending.is_empty()
    }

    /// When the current tenure ends, if this node is coordinator now.
    fn tenure(&self) -> Option<MonoTime> {
        let leadership = self.leadership.borrow();
        let now = self.clock.now();
        leadership
            .is_coordinator_at(now)
            .then(|| leadership.until())
            .flatten()
    }

    /// Plans and applies changes until the tenure ends.
    async fn serve_tenure(&mut self) {
        while let Some(until) = self.tenure() {
            // A change planned while an earlier one may still land could
            // be announced before it.
            if !self.settle().await {
                self.idle(until).await;
                continue;
            }
            let planned = self.placement.plan(&self.store, &mut self.proposals).await;
            match planned {
                Ok(Some(change)) if !change.is_empty() => {
                    if self.tenure().is_none() {
                        tracing::info!("the coordinator tenure ended while planning a change");
                        return;
                    }
                    self.make(change).await;
                }
                Ok(_) => self.idle(until).await,
                Err(error) => {
                    tracing::warn!(%error, "cannot plan a coordinator change");
                    self.idle(until).await;
                }
            }
        }
    }

    /// Applies `change`, announces what it wrote, keeps what is still to
    /// be settled, and reports it to the placement.
    async fn make(&mut self, change: ChangeSet) {
        let applied = apply(
            &self.store,
            &self.config.cluster,
            &change,
            &mut self.proposals,
            &self.config.retry,
        )
        .await;
        let (applied, failed) = match applied {
            Ok(applied) => (applied, false),
            Err(failed) => {
                tracing::warn!(error = %failed.source, "a coordinator change failed");
                (failed.applied, true)
            }
        };
        if let Some(generation) = applied.generation {
            self.announce.announce(generation).await;
        }
        if let Some(pending) = applied.pending.clone() {
            self.pending.push(pending);
        }
        self.placement.applied(&change, &applied);
        if failed {
            let until = self.tenure().unwrap_or_else(|| self.clock.now());
            self.idle(until).await;
        }
    }

    /// Waits for the idle interval, a wake-up, or the end of the tenure,
    /// whichever comes first.
    async fn idle(&self, until: MonoTime) {
        let deadline = self.clock.now().saturating_add(self.config.idle).min(until);
        // A wake-up and a deadline ready at once are taken in this order,
        // not at random, so a simulation seed replays exactly.
        tokio::select! {
            biased;
            () = self.wake.notified() => {}
            () = self.clock.sleep_until(deadline) => {}
        }
    }
}

/// Reports a tenure in the coordinator metrics while it lives.
struct TenureGauge(CoordinatorMetrics);

impl TenureGauge {
    fn begin(metrics: CoordinatorMetrics) -> Self {
        metrics.set_coordinator(true);
        Self(metrics)
    }
}

impl Drop for TenureGauge {
    fn drop(&mut self) {
        self.0.set_coordinator(false);
    }
}

#[cfg(test)]
mod tests;
