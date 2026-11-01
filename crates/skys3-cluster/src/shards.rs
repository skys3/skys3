//! [`ReplicatedShards`]: the gateway's shards on a node of a replicated
//! cluster.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use skys3_gateway::{
    ConditionFailed, FlushState, LocalShards, Precondition, ReadId, ReadPlan, Registered,
    ShardError, ShardRef, ShardSummary, Shards, StreamedBody, UploadParts, WriteOutcome,
};
use skys3_index::{Entry, ImportRanges, IndexError, ListPage, ListQuery, Part, Upload};
use skys3_io::Disk;
use skys3_log::RecordBody;
use skys3_log::record::{Extent, ExtentRef};
use skys3_net::Network;
use skys3_shard::Shard;
use skys3_shard::replication::Replication;
use skys3_types::{BucketDocument, EpochSeq, NodeId, ShardConfig};

use crate::follow::{RegisterFollower, ShardConfigs};
use crate::handoff::Handoffs;
use crate::observe::{ReplicaObserver, Unobserved};

/// Where [`ReplicatedShards`] finds the configuration a shard opens in.
#[derive(Clone, Default)]
pub enum Opening {
    /// As a node that starts does (§6.2): in the configuration the shard's
    /// register holds, read through the registers the replication was
    /// given ([`Replication::with_removal`]), or, while it cannot be read,
    /// in the configuration of the latest `CONFIG` record the node applied
    /// ([`Replication::resume`]). The node binary opens shards this way.
    #[default]
    Resume,
    /// In the configuration `configs` gives, as it is now: for a node whose
    /// configurations come from somewhere other than its replication's
    /// registers, such as a fixed placement in a simulation. A shard whose
    /// configuration cannot be read fails to open.
    Configs(Arc<dyn ShardConfigs>),
}

impl fmt::Debug for Opening {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Resume => f.write_str("Resume"),
            Self::Configs(_) => f.write_str("Configs"),
        }
    }
}

/// The gateway's shards on a replicated node: the node's replica of each
/// shard, opened in its shard's configuration, so that writes on a primary
/// commit once every member holds them durably (§5.1), and reads are
/// served only by a primary that holds its leases (§5.4).
///
/// [`Shards::open`] opens the replica only if the shard's configuration
/// names the node, as member or learner, and otherwise succeeds without
/// opening anything: the shard lives on other nodes, which the gateway's
/// routing reaches. Every other call goes to the node's replica of the
/// shard ([`LocalShards`]), which serves client requests only as the
/// shard's primary and otherwise refuses them with a redirect hint. A
/// primary removes unresponsive members, and members take over from a
/// silent primary, as the [`Replication`] it was given is set up to.
///
/// Clones share the replicas.
pub struct ReplicatedShards<N: Network, D: Disk> {
    local: LocalShards<D>,
    replication: Replication<N, D>,
    opening: Opening,
    observer: Arc<dyn ReplicaObserver<D>>,
}

impl<N: Network, D: Disk> Clone for ReplicatedShards<N, D> {
    fn clone(&self) -> Self {
        Self {
            local: self.local.clone(),
            replication: self.replication.clone(),
            opening: self.opening.clone(),
            observer: Arc::clone(&self.observer),
        }
    }
}

impl<N: Network, D: Disk> fmt::Debug for ReplicatedShards<N, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplicatedShards")
            .field("node", self.replication.node())
            .field("opening", &self.opening)
            .finish_non_exhaustive()
    }
}

impl<N: Network, D: Disk> ReplicatedShards<N, D> {
    /// The shards of `replication`, whose replicas `local` serves: the
    /// same node's shard set. Shards open as [`Opening::Resume`] says.
    #[must_use]
    pub fn new(local: LocalShards<D>, replication: Replication<N, D>) -> Self {
        Self {
            local,
            replication,
            opening: Opening::Resume,
            observer: Arc::new(Unobserved),
        }
    }

    /// The same shards, opening shards as `opening` says.
    #[must_use]
    pub fn with_opening(self, opening: Opening) -> Self {
        Self { opening, ..self }
    }

    /// The same shards, reporting what they open to `observer`, as do the
    /// [`RegisterFollower`] and the [`Handoffs`] made from them.
    #[must_use]
    pub fn with_observer(self, observer: Arc<dyn ReplicaObserver<D>>) -> Self {
        Self { observer, ..self }
    }

    /// The node's replicas, as a single node serves them.
    #[must_use]
    pub fn local(&self) -> &LocalShards<D> {
        &self.local
    }

    /// The node's replication.
    #[must_use]
    pub fn replication(&self) -> &Replication<N, D> {
        &self.replication
    }

    /// The node these shards are on.
    #[must_use]
    pub fn node_id(&self) -> &NodeId {
        self.replication.node()
    }

    /// What follows the shard registers `configs` gives for these shards.
    #[must_use]
    pub fn follower(&self, configs: Arc<dyn ShardConfigs>) -> RegisterFollower<N, D> {
        RegisterFollower::new(self.clone(), configs)
    }

    /// What starts the planned handoffs a coordinator asks this node for,
    /// on these shards' replicas.
    #[must_use]
    pub fn handoffs(&self) -> Handoffs<N, D> {
        Handoffs::new(self.replication.clone()).with_observer(Arc::clone(&self.observer))
    }

    /// Opens this node's replica of `shard` in `config`, or brings the
    /// open one up to it ([`Replication::open`]), and reports it.
    ///
    /// # Errors
    ///
    /// [`ShardError::Unavailable`] if the replica cannot be opened in
    /// `config`, as when `config` does not name the node.
    pub async fn open_config(
        &self,
        shard: &ShardRef,
        config: &ShardConfig,
    ) -> Result<Shard<D>, ShardError> {
        let replica = self
            .replication
            .open(config)
            .await
            .map_err(|error| unavailable(shard, &error))?;
        self.observer.opened(&replica);
        Ok(replica)
    }

    /// Reopens every replica whose configuration the node keeps, as a node
    /// that starts does ([`Replication::resume_kept`]): each in its
    /// register's configuration, or in its kept one while the register
    /// cannot be read, with the registers read concurrently. A replica
    /// that fails to reopen is logged and left closed; the gateway's next
    /// [`Shards::open`] of its shard tries again. Returns the replicas
    /// reopened, in shard order.
    ///
    /// # Errors
    ///
    /// An [`IndexError`] if the node's index cannot list the kept
    /// configurations.
    pub async fn resume_kept(&self) -> Result<Vec<Shard<D>>, IndexError> {
        let mut reopened = Vec::new();
        for (shard, resumed) in self.replication.resume_kept().await? {
            match resumed {
                Ok(Some(replica)) => {
                    self.observer.opened(&replica);
                    reopened.push(replica);
                }
                Ok(None) => {}
                Err(error) => tracing::warn!(%shard, %error, "a replica did not resume"),
            }
        }
        Ok(reopened)
    }

    /// The observer these shards report to.
    pub(crate) fn observer(&self) -> &Arc<dyn ReplicaObserver<D>> {
        &self.observer
    }
}

/// `error`, which opening a replica of `shard` ended with, as the gateway
/// sees it.
fn unavailable(shard: &ShardRef, error: &skys3_shard::ShardError) -> ShardError {
    ShardError::Unavailable {
        shard: shard.clone(),
        reason: error.to_string(),
    }
}

impl<N: Network, D: Disk> Shards for ReplicatedShards<N, D> {
    async fn open(&self, shard: &ShardRef, _bucket: &BucketDocument) -> Result<(), ShardError> {
        if self.replication.set().get(&shard.into()).await.is_some() {
            return Ok(());
        }
        match &self.opening {
            Opening::Resume => {
                let resumed = self.replication.resume(&shard.into()).await;
                if let Some(replica) = resumed.map_err(|error| unavailable(shard, &error))? {
                    self.observer.opened(&replica);
                }
                Ok(())
            }
            Opening::Configs(configs) => {
                let config =
                    configs
                        .config(shard)
                        .await
                        .map_err(|error| ShardError::Unavailable {
                            shard: shard.clone(),
                            reason: error.to_string(),
                        })?;
                let node = self.node_id();
                let Some(config) = config.filter(|c| c.is_member(node) || c.is_learner(node))
                else {
                    // Another node's shard, or one this node was removed
                    // from.
                    return Ok(());
                };
                self.open_config(shard, &config).await.map(drop)
            }
        }
    }

    async fn seal(&self, shard: &ShardRef) -> Result<ShardSummary, ShardError> {
        self.local.seal(shard).await
    }

    async fn unseal(&self, shard: &ShardRef) -> Result<(), ShardError> {
        self.local.unseal(shard).await
    }

    async fn remove(&self, shard: &ShardRef) -> Result<(), ShardError> {
        self.local.remove(shard).await
    }

    async fn entry(&self, shard: &ShardRef, key: &str) -> Result<Option<Entry>, ShardError> {
        self.local.entry(shard, key).await
    }

    async fn import_progress(
        &self,
        shard: &ShardRef,
    ) -> Result<Option<(u64, ImportRanges)>, ShardError> {
        self.local.import_progress(shard).await
    }

    async fn list(&self, shard: &ShardRef, query: &ListQuery) -> Result<ListPage, ShardError> {
        self.local.list(shard, query).await
    }

    async fn upload(
        &self,
        shard: &ShardRef,
        key: &str,
        upload: EpochSeq,
        after: u16,
        limit: usize,
    ) -> Result<Option<UploadParts>, ShardError> {
        self.local.upload(shard, key, upload, after, limit).await
    }

    async fn uploads(
        &self,
        shard: &ShardRef,
        prefix: &str,
        after: Option<(String, Option<EpochSeq>)>,
        limit: usize,
    ) -> Result<Vec<(String, EpochSeq, Upload)>, ShardError> {
        self.local.uploads(shard, prefix, after, limit).await
    }

    async fn parts(
        &self,
        shard: &ShardRef,
        upload: EpochSeq,
        after: u16,
        limit: usize,
    ) -> Result<Vec<(u16, Part)>, ShardError> {
        self.local.parts(shard, upload, after, limit).await
    }

    async fn payload(&self, shard: &ShardRef, position: EpochSeq) -> Result<Bytes, ShardError> {
        self.local.payload(shard, position).await
    }

    async fn plan(&self, shard: &ShardRef, key: &str) -> Result<ReadPlan, ShardError> {
        self.local.plan(shard, key).await
    }

    async fn register(
        &self,
        shard: &ShardRef,
        holder: &NodeId,
        key: &str,
        version: EpochSeq,
        layout: Vec<ExtentRef>,
    ) -> Result<Option<Registered>, ShardError> {
        self.local
            .register(shard, holder, key, version, layout)
            .await
    }

    async fn renew(
        &self,
        shard: &ShardRef,
        holder: &NodeId,
        read: ReadId,
    ) -> Result<bool, ShardError> {
        self.local.renew(shard, holder, read).await
    }

    async fn release(
        &self,
        shard: &ShardRef,
        holder: &NodeId,
        read: ReadId,
    ) -> Result<(), ShardError> {
        self.local.release(shard, holder, read).await
    }

    async fn fetch(
        &self,
        shard: &ShardRef,
        holder: &NodeId,
        read: ReadId,
        position: EpochSeq,
    ) -> Result<Bytes, ShardError> {
        self.local.fetch(shard, holder, read, position).await
    }

    async fn append_extent(
        &self,
        shard: &ShardRef,
        extent: Extent,
    ) -> Result<ExtentRef, ShardError> {
        self.local.append_extent(shard, extent).await
    }

    async fn announce(&self, shard: &ShardRef, body: StreamedBody) -> Result<(), ShardError> {
        self.local.announce(shard, body).await
    }

    async fn flushed(
        &self,
        shard: &ShardRef,
        key: &str,
        version: EpochSeq,
        wait: Duration,
    ) -> Result<FlushState, ShardError> {
        self.local.flushed(shard, key, version, wait).await
    }

    async fn write(
        &self,
        shard: &ShardRef,
        body: RecordBody,
        condition: Precondition,
    ) -> Result<Result<EpochSeq, ConditionFailed>, ShardError> {
        self.local.write(shard, body, condition).await
    }

    fn node(&self) -> Option<NodeId> {
        Some(self.node_id().clone())
    }

    async fn write_all(
        &self,
        shard: &ShardRef,
        writes: Vec<(RecordBody, Precondition)>,
    ) -> Vec<WriteOutcome> {
        self.local.write_all(shard, writes).await
    }
}
