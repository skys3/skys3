//! Following the control store (design §6.2): the node's copy of bucket
//! bindings and identity configuration kept current from the store's change
//! stream, with `config_poll_interval` as the backstop, and the store
//! detached and opened again when it fails.

use std::sync::{Arc, PoisonError};
use std::time::Duration;

use skys3_control::{ChangeStream, ControlError, ControlStore, read_cluster};
use skys3_types::Generation;
use tokio::time::timeout;

use crate::admin::ControlState;
use crate::control::backend::Backend;
use crate::control::{self, ControlCopy, OpenStoreError};
use crate::node::{Shared, StartError, no_store};

impl Shared {
    /// Syncs the identity copy from the store, with its age running from
    /// `started`, and allowlists its providers for STS.
    pub(crate) async fn sync_identity(&self, started: Duration) -> Result<(), ControlError> {
        match &self.sts {
            Some(sts) => {
                sts.sync_identity_as_of(&self.store, &self.retry, started)
                    .await?;
            }
            None => {
                self.identity
                    .sync_as_of(&self.store, &self.retry, started)
                    .await?;
            }
        }
        self.control_metrics.identity_synced(started);
        Ok(())
    }

    /// Reads a fresh copy from the store and keeps it in the index.
    pub(crate) async fn refresh_copy(&self) -> Result<ControlCopy, StartError> {
        let cluster = &self.config.cluster().cluster_id;
        let store = self.store.store().ok_or_else(no_store)?;
        let (pool, started) = (&self.pools.index, self.wall.now());
        control::refresh(&store, cluster, &self.retry, &self.index, pool, started).await
    }

    /// The generation of the node's copy.
    pub(crate) fn generation(&self) -> Option<Generation> {
        self.control
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .generation
    }

    /// Whether the copy should be synced although no generation moved, so
    /// that the identity copy, whose age STS measures from its last sync,
    /// never goes stale while the store answers (§6.2).
    pub(crate) fn sync_due(&self) -> bool {
        let synced_at = self
            .control
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .synced_at;
        let max_staleness = self.config.identity().identity_max_staleness();
        control::sync_due(synced_at, self.wall.now(), max_staleness)
    }

    /// Reads a fresh copy from the store, keeps it, and serves from the
    /// store from now on: buckets, identity, and the shards of new
    /// buckets.
    pub(crate) async fn sync(&self) -> Result<(), StartError> {
        let copy = self.refresh_copy().await?;
        let started = copy.synced_at;
        self.control_metrics.store_answered(self.wall.now());
        self.store.go_live();
        self.control_metrics.set_live(true);
        self.gateway.reload_buckets().await?;
        self.sync_identity(started).await?;
        self.open_bucket_shards().await?;
        *self.control.lock().unwrap_or_else(PoisonError::into_inner) = ControlState {
            live: true,
            generation: Some(copy.generation),
            synced_at: Some(copy.synced_at),
        };
        Ok(())
    }

    /// Keeps the node's copy current (§6.2): it waits for the store's
    /// change stream to report a new generation, or at most
    /// `config_poll_interval`, then syncs if the generation in
    /// `cluster.json` moved, reading only the registers that changed, or
    /// if half of `identity_max_staleness` passed since the last sync.
    /// While the node runs from its copy, it tries a sync every interval.
    /// A generation another node pushes (`ControlChanged`, recorded in the
    /// node's hints) ends the wait early, once per pushed generation, so a
    /// push costs at most a read of `cluster.json` even if it is forged. A
    /// store that refuses every request until it is reopened (a file
    /// store after a failed write) is detached, and opened again.
    pub(crate) async fn follow_control_store(self: Arc<Self>) {
        let interval = self.config.control_store().config_poll_interval();
        let cluster = self.config.cluster().cluster_id.clone();
        let mut hinted = Generation::ZERO;
        loop {
            let known = self.generation();
            let after = known.unwrap_or(Generation::ZERO);
            let wait = async {
                if !self.store.is_live() {
                    tokio::time::sleep(interval).await;
                    return;
                }
                match self.store.changes(after).await {
                    Ok(mut changes) => {
                        if let Ok(Err(error)) = timeout(interval, changes.next()).await {
                            tracing::warn!(%error, "the control store's change stream failed");
                            tokio::time::sleep(interval).await;
                        }
                    }
                    Err(error) => {
                        tracing::warn!(%error, "cannot follow the control store");
                        tokio::time::sleep(interval).await;
                    }
                }
            };
            tokio::select! {
                () = wait => {}
                pushed = self.hints.newer_than(after.max(hinted)) => {
                    tracing::debug!(%pushed, "a pushed generation wakes the sync");
                    hinted = pushed;
                }
            }
            if self.store.store().is_some_and(|store| store.failed()) {
                self.detach().await;
            }
            let store = match self.store.store() {
                Some(store) => store,
                None => match self.reopen().await {
                    Some(store) => store,
                    None => continue,
                },
            };
            let moved = match read_cluster(&store, &cluster, &self.retry).await {
                Ok(current) => {
                    self.control_metrics.store_answered(self.wall.now());
                    Some(current.value.generation) != known
                }
                Err(error) => {
                    tracing::warn!(%error, "cannot read the control store's generation");
                    continue;
                }
            };
            if (moved || !self.store.is_live() || self.sync_due())
                && let Err(error) = self.sync().await
            {
                tracing::warn!(%error, "cannot sync the control-state copy");
                // The stream would report the same generation at once.
                tokio::time::sleep(interval).await;
            }
        }
    }

    /// Drops a store that refuses every request until it is reopened, and
    /// serves the node's latest copy until [`Shared::reopen`] opens it
    /// again and a sync succeeds (§6.2).
    pub(crate) async fn detach(&self) {
        match control::kept_copy(&self.index, &self.pools.index).await {
            Ok(Some(copy)) => {
                tracing::error!(
                    generation = %copy.generation,
                    "the control store failed; serving the local copy until it is reopened"
                );
                self.store.detach(copy);
                self.control_metrics.set_live(false);
                self.control
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .live = false;
            }
            // A running node always keeps a copy: it opened from one or
            // saved one.
            Ok(None) => tracing::error!("the control store failed, and the node has no copy"),
            Err(error) => {
                tracing::error!(%error, "the control store failed; cannot read the local copy");
            }
        }
    }

    /// Opens the control store for a node that runs from its copy without
    /// one, and gives it to the node store.
    pub(crate) async fn reopen(&self) -> Option<Backend> {
        match self.opener.open(true).await {
            Ok(opened) => {
                tracing::info!("opened the control store");
                self.store.attach(opened.store.clone());
                Some(opened.store)
            }
            Err(OpenStoreError::Unavailable(error)) => {
                tracing::warn!(%error, "cannot open the control store; serving the local copy");
                None
            }
            Err(error) => {
                tracing::error!(%error, "refusing the control store; serving the local copy");
                None
            }
        }
    }
}
