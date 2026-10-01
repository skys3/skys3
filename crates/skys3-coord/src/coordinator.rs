//! The coordinator's work loop (design §6.7): while this node holds the
//! coordinator lease, it asks its [`Placement`] for changes, applies each
//! as compare-and-swaps announced by a new generation, and pushes that
//! generation to every node.
//!
//! What to change is the placement's decision; plan tasks M3-02 to M3-06
//! add the node registry, placement, replacement, and rebalancing as
//! placements. This loop owns only the rules every change follows.

use std::sync::Arc;
use std::time::Duration;

use skys3_control::{ControlError, ControlStore, ProposalIds, RetryPolicy};
use skys3_io::{Clock, MonoTime};
use skys3_types::ClusterId;
use tokio::sync::{Notify, watch};

use crate::change::{Applied, ChangeSet, apply};
use crate::lease::Leadership;
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

    /// Learns what [`apply`] did with a planned change.
    fn applied(&mut self, change: &ChangeSet, applied: &Applied) {
        let _ = (change, applied);
    }
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
pub struct Coordinator<S, P, A> {
    store: S,
    clock: Arc<dyn Clock>,
    leadership: watch::Receiver<Leadership>,
    placement: P,
    announce: A,
    config: CoordinatorConfig,
    proposals: ProposalIds,
    wake: Arc<Notify>,
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
        }
    }

    /// Wakes the coordinator to plan before its idle wait ends, for
    /// example when a node registers or misses heartbeats.
    #[must_use]
    pub fn waker(&self) -> Arc<Notify> {
        Arc::clone(&self.wake)
    }

    /// Works whenever this node is coordinator, until the elector is
    /// dropped.
    pub async fn run(mut self) {
        while self.wait_for_tenure().await {
            self.serve_tenure().await;
        }
    }

    /// Waits until this node is coordinator. Returns `false` once the
    /// elector is gone.
    async fn wait_for_tenure(&mut self) -> bool {
        loop {
            if self.tenure().is_some() {
                return true;
            }
            if self.leadership.changed().await.is_err() {
                return false;
            }
        }
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

    /// Applies `change`, announces it, and reports it to the placement.
    async fn make(&mut self, change: ChangeSet) {
        let applied = apply(
            &self.store,
            &self.config.cluster,
            &change,
            &mut self.proposals,
            &self.config.retry,
        )
        .await;
        match applied {
            Ok(applied) => {
                if let Some(generation) = applied.generation {
                    self.announce.announce(generation).await;
                }
                self.placement.applied(&change, &applied);
            }
            Err(error) => {
                tracing::warn!(%error, "a coordinator change failed");
                let until = self.tenure().unwrap_or_else(|| self.clock.now());
                self.idle(until).await;
            }
        }
    }

    /// Waits for the idle interval, a wake-up, or the end of the tenure,
    /// whichever comes first.
    async fn idle(&self, until: MonoTime) {
        let deadline = self.clock.now().saturating_add(self.config.idle).min(until);
        tokio::select! {
            () = self.wake.notified() => {}
            () = self.clock.sleep_until(deadline) => {}
        }
    }
}

#[cfg(test)]
mod tests;
