//! Starting, running, and stopping a node (design §3, §6.2, §10).

use std::collections::BTreeSet;
use std::io;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use prometheus_client::metrics::gauge::Gauge;
use skys3_config::{Config, ControlStoreBackend, LogFormat};
use skys3_control::{ControlError, ProposalIds, RetryPolicy};
use skys3_coord::{ControlHints, HeartbeatStatus};
use skys3_flush::snapshot::SnapshotService;
use skys3_flush::{Commit, DirtyBudget, FlushMetrics, FlushService, FlushSettings, WholeFill};
use skys3_gateway::{
    BucketLookup, CredentialError, FillBody, FillError, FillProgress, Fills, Gateway,
    GatewayConfig, GatewayListener, HotCache, HotCacheMetrics, IdSource, LocalShards, PeerRefusals,
    Precondition, ShardPlacement, ShardRef, Shards, SigV4Authenticator, StaticCredentials,
    StsService,
};
use skys3_index::{Checkpointer, Index, IndexConfig, IndexError};
use skys3_io::{BlockingPool, MonotonicClock, RealDisk, SystemWallClock, WallClock};
use skys3_log::{LogConfig, SegmentLog};
use skys3_obs::{
    AdminConfig, AdminError, AdminListener, AdminToken, HandshakeLimit, HandshakeMetrics, Health,
};
use skys3_remote::aws::{AwsS3, default_credentials, profile_credentials};
use skys3_shard::lifecycle::{self, LifecycleMetrics};
use skys3_shard::{
    CacheMetrics, CacheSettings, CleanCache, CompactionMetrics, CompactionSettings, Compactor,
    ReadSettings,
};
use skys3_sts::{
    HttpsFetcher, HttpsFetcherOptions, IdentityCopy, NodeCredentials, OidcValidator, SessionStore,
    StsEndpoint, StsSettings, ValidatorSettings,
};
use skys3_types::{BucketDocument, BucketId, BucketMode, BucketName, Label, NodeId};
use tokio::sync::{oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::timeout;

use crate::admin::{BucketList, ControlState, NodeAdmin};
use crate::admission::{DiskSpace, NodeAdmission, Place, Watched, watch_space};
use crate::cluster::ClusterServices;
use crate::cluster::coding::NodeCoding;
use crate::cluster::peering::NodePeering;
use crate::cluster::replication::{ClusterShards, Routed};
use crate::control::backend::{self, Backend, ConnectError, Opener};
use crate::control::{self, ControlCopy, NodeStore, OpenStoreError, StoreOwner};
use crate::datadir::{self, DataDir, DataDirError, DiskDir};
use crate::metrics::{ControlMetrics, NodeMetrics};
use crate::remote::NodeRemote;
use crate::sessions::{SESSIONS_BUCKET_ID, SystemSessions};
use crate::storage::{self, on_pool};

/// Worker threads per disk for log I/O (§10.4).
const DISK_THREADS: usize = 4;
/// Worker threads for index I/O.
const INDEX_THREADS: usize = 4;
/// How often expired sessions are removed.
const SESSION_SWEEP_INTERVAL: Duration = Duration::from_secs(300);
/// How often the node checks whether a disk went out of service, and the
/// free space of its disks.
const DISK_WATCH_INTERVAL: Duration = Duration::from_secs(1);
/// How often the flushers follow the node's buckets and shards, and their
/// gauges are refreshed.
const FLUSH_FOLLOW_INTERVAL: Duration = Duration::from_secs(1);
/// The longest one request to a remote target may take, a whole PUT body
/// included, before the flusher gives up on it and retries.
const FLUSH_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(600);

/// The shards of a running node.
type NodeShards = LocalShards<RealDisk>;
/// Session records.
type Sessions = SystemSessions<RealDisk>;
/// The control store as the gateway uses it.
type Store = NodeStore<Backend>;
/// The STS endpoint.
type Sts = StsEndpoint<HttpsFetcher, Sessions>;
/// The gateway's authenticator.
type NodeAuth = SigV4Authenticator<NodeCredentials<Sessions>>;
/// The gateway.
type NodeGateway = Gateway<NodeAuth>;
/// The flushers of `write_back` buckets.
pub(crate) type NodeFlush = FlushService<AwsS3, RealDisk>;

/// The node's index snapshots (§8.9).
type NodeSnapshots = SnapshotService<AwsS3, RealDisk>;

/// Read-through fill on this node (§9.2): the gateway's [`Fills`] over
/// each `write_back` bucket's [`skys3_flush::Filler`], into the bucket's
/// local shards.
///
/// Filled bytes are clean cache, so they never count against the dirty
/// budget, and the budget does not hold fills back. They do take disk
/// space, so a shard whose disk (or the data directory) is low on space
/// is not filled: the read answers `503` until space is freed, as a write
/// would be.
///
/// Only a shard's serving primary fills (§9.2). With `routed`, the
/// gateway's fills of a `write_back` bucket's shard this node does not
/// lead ask the primary to fill the version, and answer
/// [`FillError::Changed`] once it has, so that the gateway reads it from
/// the primary as a holder. The fills the transport serves for other
/// nodes have no `routed`, so they never forward in turn.
#[derive(Debug, Clone)]
struct NodeFills {
    shards: NodeShards,
    flush: Arc<NodeFlush>,
    space: Arc<DiskSpace>,
    routed: Option<Routed>,
}

impl Fills for NodeFills {
    fn read(
        &self,
        shard: &ShardRef,
        key: &str,
        version: skys3_types::EpochSeq,
        range: std::ops::Range<u64>,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<FillBody, FillError>> + Send + '_>> {
        let (shard, key) = (shard.clone(), key.to_owned());
        Box::pin(async move {
            let unavailable = |reason: &str| FillError::Unavailable(reason.to_owned());
            let local = self.shards.set().get(&(&shard).into()).await;
            let serving = local.as_ref().is_some_and(|l| l.check_readable().is_ok());
            // A read_only bucket's versions are read straight from its
            // origin elsewhere than on the primary (§9.5).
            if let Some(routed) = self.routed.as_ref().filter(|_| !serving)
                && self.flush.origin(&shard.bucket).is_none()
            {
                return match routed.fill(&shard, &key, version).await {
                    Ok(Ok(()) | Err(FillError::Changed)) => Err(FillError::Changed),
                    Ok(Err(error)) => Err(error),
                    Err(error) => Err(FillError::Unavailable(error.to_string())),
                };
            }
            if self
                .space
                .is_low(self.shards.set().disk_of(&(&shard).into()))
            {
                return Err(unavailable("the node is low on disk space; retry later"));
            }
            let local = local.ok_or_else(|| unavailable("the shard is not open on this node"))?;
            let filler = self
                .flush
                .filler(&shard.bucket)
                .ok_or_else(|| unavailable("the bucket's target is not attached yet"))?;
            filler
                .read(&local, &key, version, range)
                .await
                .map_err(|error| match error {
                    skys3_flush::FillError::Changed => FillError::Changed,
                    other => FillError::Unavailable(other.to_string()),
                })
        })
    }

    fn fill(
        &self,
        shard: &ShardRef,
        key: &str,
        version: skys3_types::EpochSeq,
        wait: Duration,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<FillProgress, FillError>> + Send + '_>> {
        let (shard, key) = (shard.clone(), key.to_owned());
        Box::pin(async move {
            let unavailable = |reason: &str| FillError::Unavailable(reason.to_owned());
            if self
                .space
                .is_low(self.shards.set().disk_of(&(&shard).into()))
            {
                return Err(unavailable("the node is low on disk space; retry later"));
            }
            let local = self
                .shards
                .set()
                .get(&(&shard).into())
                .await
                .ok_or_else(|| unavailable("the shard is not open on this node"))?;
            let filler = self
                .flush
                .filler(&shard.bucket)
                .ok_or_else(|| unavailable("the bucket's target is not attached yet"))?;
            filler
                .fill_whole(&local, &key, version, wait)
                .await
                .map(|filled| match filled {
                    WholeFill::Filled => FillProgress::Filled,
                    WholeFill::Filling { filled } => FillProgress::Filling { filled },
                })
                .map_err(|error| match error {
                    skys3_flush::FillError::Changed => FillError::Changed,
                    other => FillError::Unavailable(other.to_string()),
                })
        })
    }
}

/// Why a node could not start.
#[derive(Debug, thiserror::Error)]
pub enum StartError {
    /// The configuration asks for something this build does not run.
    #[error("{0}")]
    Unsupported(String),
    /// The data directory or a disk cannot be used.
    #[error(transparent)]
    DataDir(#[from] DataDirError),
    /// A disk's log could not be recovered.
    #[error("cannot recover the log of disk {disk}: {source}")]
    Recovery {
        /// The disk.
        disk: Label,
        /// Why.
        #[source]
        source: skys3_log::RecoveryError,
    },
    /// A disk's fragment store could not be recovered (a node of a cluster).
    #[error("cannot recover the fragment store of disk {disk}: {source}")]
    Fragments {
        /// The disk.
        disk: Label,
        /// Why.
        #[source]
        source: skys3_ec::RecoveryError,
    },
    /// The index could not be opened or replayed.
    #[error("the index: {0}")]
    Index(#[from] IndexError),
    /// The control store failed, and the node has no local copy to run
    /// from.
    #[error("the control store: {0}")]
    Control(#[from] ControlError),
    /// The control store belongs to another node or data directory, or
    /// looks reset to a node with no copy to run from.
    #[error("the control store: {0}")]
    ControlStore(String),
    /// A static credential's secret could not be loaded.
    #[error(transparent)]
    Credentials(#[from] CredentialError),
    /// The gateway's TLS configuration is unusable.
    #[error(transparent)]
    Tls(#[from] crate::tls::TlsError),
    /// The admin listener could not start.
    #[error(transparent)]
    Admin(#[from] AdminError),
    /// The admin token file could not be read.
    #[error(transparent)]
    AdminToken(#[from] skys3_obs::AdminTokenError),
    /// STS could not be set up.
    #[error("STS: {0}")]
    Sts(String),
    /// The cluster services of a node of a cluster cannot start:
    /// a certificate that does not name this node, or a registration of
    /// this node's ID from another data directory.
    #[error("the cluster: {0}")]
    Cluster(String),
    /// A shard could not be opened.
    #[error("shard {shard}: {reason}")]
    Shard {
        /// The shard.
        shard: String,
        /// Why.
        reason: String,
    },
    /// A listener could not be bound, or a pool started.
    #[error("{what}: {source}")]
    Io {
        /// What failed.
        what: String,
        /// The error.
        #[source]
        source: io::Error,
    },
}

fn io_error(what: impl Into<String>) -> impl FnOnce(io::Error) -> StartError {
    let what = what.into();
    move |source| StartError::Io { what, source }
}

/// Why a running node stopped with an error.
#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    /// A checkpoint failed; the node must stop and replay from the last
    /// durable one (§10.2).
    #[error("a checkpoint failed: {0}")]
    Checkpoint(#[from] IndexError),
    /// A background task ended unexpectedly.
    #[error("a background task failed: {0}")]
    Task(String),
    /// On a node of a cluster, the node's registration turned
    /// out to belong to another data directory after it had started; it
    /// stops rather than serve under another node's ID (§6.7).
    #[error("the cluster: {0}")]
    Cluster(String),
}

/// Pools the node runs its blocking work on.
#[derive(Debug, Clone)]
pub(crate) struct Pools {
    pub(crate) index: BlockingPool,
    hashing: BlockingPool,
    control: BlockingPool,
    disks: Vec<BlockingPool>,
}

impl Pools {
    fn shutdown(&self) {
        for pool in [&self.index, &self.hashing, &self.control]
            .into_iter()
            .chain(&self.disks)
        {
            pool.shutdown();
        }
    }
}

fn pool(name: &str, threads: usize) -> Result<BlockingPool, StartError> {
    let threads = NonZeroUsize::new(threads).unwrap_or(NonZeroUsize::MIN);
    BlockingPool::new(name, threads).map_err(io_error(format!("starting the {name} pool")))
}

/// What the background tasks share.
pub(crate) struct Shared {
    pub(crate) config: Config,
    pub(crate) retry: RetryPolicy,
    pub(crate) wall: Arc<dyn WallClock>,
    pub(crate) index: Arc<Index>,
    pub(crate) pools: Pools,
    pub(crate) store: Store,
    /// Opens the control store again while the node has none.
    pub(crate) opener: Opener,
    pub(crate) shards: NodeShards,
    pub(crate) gateway: NodeGateway,
    pub(crate) identity: Arc<IdentityCopy>,
    pub(crate) sts: Option<Arc<Sts>>,
    pub(crate) control: Arc<Mutex<ControlState>>,
    pub(crate) control_metrics: ControlMetrics,
    pub(crate) flush: Arc<NodeFlush>,
    pub(crate) snapshots: NodeSnapshots,
    /// The generations other nodes push, which wake the sync of the
    /// node's copy; none arrive on a single node.
    pub(crate) hints: ControlHints,
    /// On a node of a cluster, the shards replicated across the
    /// nodes their registers name, which the gateway reaches by routing
    /// (plan M8-05b); `None` for a single node, whose shards are all its
    /// own ([`Shared::shards`]).
    pub(crate) cluster: Option<Arc<ClusterShards>>,
    /// On a node of a cluster, erasure coding on the node's replicas of
    /// `local` buckets' shards (plan M8-07a).
    pub(crate) coding: Option<Arc<NodeCoding>>,
    /// The fills [`Shared::cluster`]'s transport serves to other nodes'
    /// gateways, which its server holds only weakly.
    _served_fills: Option<Arc<dyn Fills>>,
}

impl Shared {
    /// Opens the shards of every bucket the gateway knows. A node of
    /// several opens instead the replicas of the shards whose registers
    /// name it (§6.3), as its periodic pass does.
    pub(crate) async fn open_bucket_shards(&self) -> Result<(), StartError> {
        if let Some(cluster) = &self.cluster {
            cluster.follow().await;
            return Ok(());
        }
        for bucket in self.gateway.buckets() {
            for shard in ShardRef::all(&bucket) {
                self.shards
                    .open(&shard, &bucket)
                    .await
                    .map_err(|error| StartError::Shard {
                        shard: format!("{}/{}", shard.bucket, shard.shard),
                        reason: error.to_string(),
                    })?;
            }
        }
        Ok(())
    }

    /// Keeps a flusher on every `write_back` bucket shard open on the node
    /// (§7.1) and an index snapshot writer on every shard it leads of a
    /// bucket with a snapshot target (§8.9), refreshes the flush gauges, and
    /// gives the clean cache each bucket's `clean_copies` (§9.3).
    async fn follow_flushes(self: Arc<Self>) {
        loop {
            let buckets = self.gateway.buckets();
            // Before the reconcile awaits: a bucket made since the last
            // round may already have clean entries.
            self.install_clean_copies(&buckets);
            self.flush.reconcile(&buckets, self.shards.set()).await;
            self.snapshots.reconcile(&buckets, self.shards.set()).await;
            self.flush.refresh_metrics();
            tokio::time::sleep(FLUSH_FOLLOW_INTERVAL).await;
        }
    }

    /// Gives the clean cache each bucket's `clean_copies` (§9.3). The
    /// cache keeps every copy of a bucket until it is told, and is never
    /// told of a `local` bucket, whose replicas are its durable home even
    /// once a backup target made its entries clean (§8.9).
    fn install_clean_copies(&self, buckets: &[BucketDocument]) {
        if let Some(cache) = self.shards.set().cache() {
            cache.set_clean_copies(
                buckets
                    .iter()
                    .filter(|bucket| bucket.mode != BucketMode::Local)
                    .map(|bucket| (bucket.bucket_id.clone(), bucket.clean_copies)),
            );
        }
    }

    /// Runs a lifecycle pass every `lifecycle_interval_seconds` over the
    /// shards this node leads of each `local` bucket with lifecycle rules
    /// (§8.7). Each pass starts from the shards' indexes, so a shard this
    /// node took over is evaluated from what its log holds.
    async fn follow_lifecycle(self: Arc<Self>, metrics: LifecycleMetrics) {
        let interval = self.config.storage().lifecycle_interval();
        loop {
            tokio::time::sleep(interval).await;
            let buckets = self.gateway.buckets();
            let now = u64::try_from(self.wall.now().as_millis()).unwrap_or(u64::MAX);
            // A backup target's flusher removes the tombstones (§8.9).
            let tombstones = |bucket: &BucketDocument| match self
                .config
                .buckets()
                .get(&bucket.name)
                .backup_target
            {
                Some(_) => lifecycle::Tombstones::Keep,
                None => lifecycle::Tombstones::Remove,
            };
            let report =
                lifecycle::run_pass(self.shards.set(), &buckets, now, &metrics, tombstones).await;
            if report != lifecycle::LifecycleReport::default() {
                tracing::debug!(
                    expired = report.expired,
                    aborted = report.aborted,
                    failed = report.failed,
                    "ran a lifecycle pass"
                );
            }
        }
    }

    /// Removes expired sessions every [`SESSION_SWEEP_INTERVAL`].
    async fn sweep_sessions(self: Arc<Self>, sessions: Sessions) {
        loop {
            tokio::time::sleep(SESSION_SWEEP_INTERVAL).await;
            match sessions.remove_expired(self.wall.now()).await {
                Ok(0) => {}
                Ok(removed) => tracing::debug!(removed, "removed expired sessions"),
                Err(error) => tracing::warn!(%error, "cannot remove expired sessions"),
            }
        }
    }
}

/// Fences each disk that goes out of service, so it is not used again
/// before the host restarts (§10.4).
async fn watch_disks(
    disks: Vec<(DiskDir, SegmentLog<RealDisk>)>,
    pool: BlockingPool,
    boot_id: Option<String>,
    out_of_service: Gauge,
    storage: skys3_obs::Readiness,
) {
    let mut fenced = BTreeSet::new();
    loop {
        for (disk, log) in &disks {
            let Some(error) = log.failure() else {
                continue;
            };
            if !fenced.insert(disk.label.clone()) {
                continue;
            }
            tracing::error!(disk = %disk.label, %error, "a disk was taken out of service");
            out_of_service.inc();
            storage.set_ready(false);
            let (disk, boot_id, error) = (disk.clone(), boot_id.clone(), error.to_string());
            let written = pool
                .run(move || datadir::fence(&disk, boot_id.as_deref(), &error))
                .await;
            if !matches!(written, Ok(Ok(()))) {
                tracing::error!(
                    "cannot fence the disk; it stays out of service only until the node exits"
                );
            }
        }
        tokio::time::sleep(DISK_WATCH_INTERVAL).await;
    }
}

/// The storage engine after recovery.
struct Storage {
    disks: Vec<(DiskDir, SegmentLog<RealDisk>)>,
    /// Each disk by label, which the fragment stores share with the logs.
    mounts: Vec<(Label, RealDisk)>,
    index: Arc<Index>,
    checkpointer: Arc<Checkpointer<RealDisk>>,
    shards: NodeShards,
}

/// Recovers each disk's log, opens the index, and replays the logs into
/// Opens each disk and the index, recovers each disk's log, and replays the
/// logs into the index (§10.1, §10.2). The index is kept in `opened` as
/// soon as it is open.
async fn open_storage(
    config: &Config,
    data_dir: &DataDir,
    pools: &mut Pools,
    opened: &mut Option<Arc<Index>>,
) -> Result<Storage, StartError> {
    let mut dirs = Vec::new();
    let mut disks = Vec::new();
    for disk in data_dir.disks() {
        let disk_pool = pool(&format!("disk-{}", disk.label), DISK_THREADS)?;
        pools.disks.push(disk_pool.clone());
        let real = RealDisk::open(&disk.path, disk_pool)
            .await
            .map_err(io_error(format!("opening {}", disk.path.display())))?;
        dirs.push(disk.clone());
        disks.push((disk.label.clone(), real));
    }
    let mounts = disks.clone();
    let path = data_dir.path().join("index.redb");
    let index_config = IndexConfig::from_storage(config.storage());
    let index = on_pool(&pools.index, move || Index::open(&path, &index_config)).await?;
    let index = Arc::clone(opened.insert(Arc::new(index)));
    let recovered = storage::recover(
        disks,
        LogConfig::from_storage(config.storage()),
        Arc::new(MonotonicClock::new()),
        index,
        pools.index.clone(),
        data_dir.node_id().clone(),
    )
    .await?;
    // Read registrations of this node's replicas as holders (§8.7).
    recovered.shards.set().reads().configure(ReadSettings {
        ttl: config.storage().read_registration_ttl(),
        release_delay: config.ec().fragment_release_delay(),
    });
    let disks = dirs
        .into_iter()
        .map(|dir| {
            let log = recovered.logs[&dir.label].clone();
            (dir, log)
        })
        .collect();
    Ok(Storage {
        disks,
        mounts,
        index: recovered.index,
        checkpointer: recovered.checkpointer,
        shards: recovered.shards,
    })
}

/// What [`open_control`] gives the node.
struct OpenedControl {
    store: Store,
    copy: ControlCopy,
    /// Whether the buckets the copy names were read from the store at this
    /// start, from a store this start did not create, so a shard no
    /// register names is an orphan (§4.1).
    reclaim: bool,
}

/// Opens the control store with `opener` (the startup probe of an S3
/// store), bootstraps `cluster.json` if this node never synced, and syncs
/// the copy kept in the index. If the store cannot be opened or does not
/// answer, or looks reset, a node with a copy runs from it (§6.2); a file
/// store that belongs to another node, or a store that fails the probe, is
/// refused.
async fn open_control(
    config: &Config,
    opener: &Opener,
    index: &Arc<Index>,
    pools: &Pools,
    started: Duration,
) -> Result<OpenedControl, StartError> {
    let cluster = &config.cluster().cluster_id;
    let retry = RetryPolicy::default();
    let kept = control::kept_copy(index, &pools.index).await?;
    let initialized = kept.is_some();
    let opened = match opener.open(initialized).await {
        Ok(opened) => opened,
        Err(OpenStoreError::Foreign(reason) | OpenStoreError::Refused(reason)) => {
            return Err(StartError::ControlStore(reason));
        }
        Err(error) => {
            let Some(kept) = kept else {
                return Err(match error {
                    OpenStoreError::Unavailable(error) => StartError::Control(error),
                    other => StartError::ControlStore(other.to_string()),
                });
            };
            if matches!(error, OpenStoreError::Reset(_)) {
                tracing::error!(
                    %error,
                    generation = %kept.generation,
                    "the control store looks reset; running from the local copy"
                );
            } else {
                tracing::warn!(
                    %error,
                    generation = %kept.generation,
                    "cannot open the control store; running from the local copy"
                );
            }
            return Ok(OpenedControl {
                store: NodeStore::from_copy(None, kept.clone()),
                copy: kept,
                reclaim: false,
            });
        }
    };
    let proposal = ProposalIds::from_os_rng().next_id();
    let (store, copy) = control::open(
        opened.store,
        kept,
        cluster,
        proposal,
        &retry,
        index,
        &pools.index,
        started,
    )
    .await?;
    Ok(OpenedControl {
        reclaim: store.is_live() && !opened.claimed,
        store,
        copy,
    })
}

/// The error a node without a control store gives for what needs one.
pub(crate) fn no_store() -> ControlError {
    ControlError::Unavailable("the control store has not been opened".to_owned())
}

/// The STS endpoint, if `sts_web_identity` is on. It validates tokens
/// against the issuers of the identity copy and stores sessions in the
/// system bucket.
fn sts_endpoint(
    config: &Config,
    identity: &Arc<IdentityCopy>,
    sessions: &Sessions,
    wall: &Arc<dyn WallClock>,
) -> Result<Option<Arc<Sts>>, StartError> {
    let Some(settings) = StsSettings::from_config(config.identity()) else {
        return Ok(None);
    };
    let fetcher = HttpsFetcher::new(HttpsFetcherOptions::default())
        .map_err(|error| StartError::Sts(error.to_string()))?;
    let validator_settings = ValidatorSettings {
        clock_skew: config.identity().oidc_clock_skew(),
        ..ValidatorSettings::default()
    };
    let validator = OidcValidator::new(fetcher, Arc::clone(wall), validator_settings);
    Ok(Some(Arc::new(StsEndpoint::new(
        validator,
        Arc::clone(identity),
        sessions.clone(),
        Arc::clone(wall),
        settings,
    ))))
}

/// Binds the gateway's listener, with TLS when `[gateway]` names a
/// certificate, and at most `max_inflight_handshakes` handshakes at once,
/// exported in `handshakes` (§12).
async fn bind_gateway(
    config: &Config,
    gateway: NodeGateway,
    handshakes: &HandshakeMetrics,
) -> Result<GatewayListener<NodeAuth>, StartError> {
    let settings = config.gateway();
    let listener = GatewayListener::bind(settings.listen, gateway)
        .await
        .map_err(io_error(format!(
            "binding the gateway to {}",
            settings.listen
        )))?;
    if let Some((cert, key)) = settings.tls() {
        let limit = HandshakeLimit::new(settings.max_inflight_handshakes)
            .with_metrics(handshakes, "gateway");
        return Ok(listener
            .with_tls(crate::tls::server_config(cert, key)?)
            .with_handshake_limit(limit));
    }
    if !settings.listen.ip().is_loopback() {
        tracing::warn!(
            listen = %settings.listen,
            "the gateway serves plain HTTP on a non-loopback address; set [gateway] \
             tls_cert_file and tls_key_file"
        );
    }
    Ok(listener)
}

/// What a start opened, which a failed start closes in order: the index,
/// then the data directory's lock.
#[derive(Default)]
struct Opened {
    data_dir: Option<DataDir>,
    index: Option<Arc<Index>>,
}

/// A running node.
///
/// [`Node::start`] brings a node up in this order:
///
/// 1. Open the data directory and find the node's disks
///    ([`DataDir`](crate::datadir::DataDir)), refusing a disk fenced in
///    this boot.
/// 2. Recover each disk's log (torn tails cut, damage refused), open the
///    index, and replay every record after its shard's checkpoint.
/// 3. Open the control store `[control_store]` names, probing an S3 store
///    first ([`crate::control::backend`]); bootstrap `cluster.json`; and sync the
///    local copy of bucket bindings and identity configuration. If the
///    store does not answer, run from the copy kept in the index
///    ([`crate::control`]).
/// 4. Build the gateway with SigV4 authentication over static credentials
///    and STS sessions, which live in the system bucket
///    ([`crate::sessions`]), STS if `sts_web_identity` is on, and admission
///    control: writes that add data get `503 SlowDown` while a dirty-data
///    budget is used up or a disk is low on space (§7.6, §13).
/// 5. Open every bucket's shards, and drop shards whose bucket no register
///    names: those a creation or deletion of unknown outcome left behind
///    (§4.1). This needs a copy read from the store itself.
/// 6. Serve the gateway (HTTPS when `[gateway]` names a certificate) and
///    the admin listener, and start the background work: checkpoints,
///    control-store polling, session expiry, disk fencing, free-space
///    checks, the flushers of `write_back` buckets, which track their
///    dirty keys at once and flush them once each target's probe succeeds
///    (`skys3_flush`), the index snapshots of the shards it leads
///    (`skys3_flush::snapshot`), and the lifecycle passes of `local`
///    buckets (`skys3_shard::lifecycle`).
///
/// [`Node::shutdown`] stops accepting requests, drains those in flight,
/// stops the flushers and snapshot writers, stops every shard once its sequenced records are
/// applied, and takes a final checkpoint, so the next start replays
/// little.
pub struct Node {
    node_id: NodeId,
    gateway_addr: SocketAddr,
    admin_addr: SocketAddr,
    serving: skys3_obs::Readiness,
    stop_gateway: oneshot::Sender<()>,
    gateway_task: JoinHandle<()>,
    stop_admin: oneshot::Sender<()>,
    admin_task: JoinHandle<()>,
    checkpoints: JoinHandle<IndexError>,
    checkpointer: Arc<Checkpointer<RealDisk>>,
    background: JoinSet<()>,
    /// What the heartbeater reports, on a node of a cluster.
    registration: Option<watch::Receiver<HeartbeatStatus>>,
    shared: Arc<Shared>,
    /// The QUIC endpoint of native replication, if the node peers.
    peer_endpoint: Option<skys3_peer::PeerEndpoint>,
    data_dir: DataDir,
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("node_id", &self.node_id)
            .field("gateway_addr", &self.gateway_addr)
            .field("admin_addr", &self.admin_addr)
            .finish_non_exhaustive()
    }
}

impl Node {
    /// Starts a node with `config`, in the order [`Node`] describes.
    ///
    /// # Errors
    ///
    /// [`StartError`] for whatever stopped the node from starting. Nothing
    /// is served then.
    pub async fn start(config: Config) -> Result<Self, StartError> {
        let threads = std::thread::available_parallelism().map_or(4, NonZeroUsize::get);
        let index_pool = pool("index", INDEX_THREADS)?;
        let pools = Pools {
            hashing: pool("hashing", threads)?,
            control: pool("control", 1)?,
            disks: Vec::new(),
            index: index_pool,
        };
        let mut opened = Opened::default();
        let result = Self::start_with(config, pools.clone(), &mut opened).await;
        if result.is_err() {
            // Close the index before another process may take the data
            // directory.
            if let Some(index) = opened.index.take() {
                close_index(index).await;
            }
            drop(opened);
            pools.shutdown();
        }
        result
    }

    async fn start_with(
        config: Config,
        mut pools: Pools,
        opened: &mut Opened,
    ) -> Result<Self, StartError> {
        let cluster = config.cluster().cluster_id.clone();
        let boot_id = datadir::boot_id();
        let data_dir = {
            let (node, cluster, boot_id) =
                (config.node().clone(), cluster.clone(), boot_id.clone());
            on_pool(&pools.index, move || {
                DataDir::open(&node, &cluster, boot_id.as_deref())
            })
            .await?
        };
        let data_dir = opened.data_dir.insert(data_dir);
        let node_id = data_dir.node_id().clone();
        tracing::info!(%node_id, %cluster, "starting");
        // A certificate that does not name this node stops the start before
        // anything is recovered.
        let node_credentials = crate::cluster::credentials(&config, &node_id)?;
        let storage = open_storage(&config, data_dir, &mut pools, &mut opened.index).await?;
        check_mode(&config, data_dir, &storage.index, &pools.index).await?;

        let metrics = NodeMetrics::new(config.identity().identity_max_staleness());
        let health = Health::new();
        let serving = health.register("gateway");
        let storage_ready = health.register("storage");
        storage_ready.set_ready(true);

        let wall: Arc<dyn WallClock> = Arc::new(SystemWallClock);
        let opener = match &config.control_store().backend {
            ControlStoreBackend::File { directory } => {
                let owner = StoreOwner::new(
                    cluster.clone(),
                    node_id.clone(),
                    data_dir.instance_id().to_owned(),
                );
                Opener::file(directory.clone(), owner, pools.control.clone())
            }
            _ => {
                let store = backend::connect(config.control_store())
                    .await
                    .map_err(|error| match error {
                        ConnectError::Tls(error) => StartError::Tls(error),
                        other => StartError::ControlStore(other.to_string()),
                    })?;
                Opener::shared(store)
            }
        };
        let OpenedControl {
            store,
            copy,
            reclaim,
        } = open_control(&config, &opener, &storage.index, &pools, wall.now()).await?;
        metrics.control.set_live(store.is_live());
        if store.is_live() {
            metrics.control.store_answered(wall.now());
        }
        let control = Arc::new(Mutex::new(ControlState {
            live: store.is_live(),
            generation: Some(copy.generation),
            synced_at: Some(copy.synced_at),
        }));
        // On a node of a cluster, the shards replicated across the nodes their
        // registers name, and the gateway's routing to their primaries;
        // shard registers change through the store's own client, never the
        // copy (plan M8-05b).
        let cluster_shards = match (&node_credentials, opener.shared_store()) {
            (Some(credentials), Some(shared_store)) => Some(Arc::new(
                ClusterShards::new(
                    &config,
                    node_id.clone(),
                    &storage.shards,
                    (Arc::clone(&storage.index), pools.index.clone()),
                    credentials,
                    shared_store.clone(),
                )
                .await?,
            )),
            _ => None,
        };
        // On a node of a cluster, the fragment stores, recovered now, and
        // what codes and reads coded objects (plan M8-07a); whether an
        // object is coded is the [ec] policy's, by the eligible nodes.
        let coding = match &cluster_shards {
            Some(cluster) => Some(Arc::new(
                NodeCoding::open(
                    &config,
                    &node_id,
                    &storage.mounts,
                    cluster,
                    (Arc::clone(&wall), pools.index.clone()),
                    &metrics.registry,
                )
                .await?,
            )),
            None => None,
        };

        // Identity, sessions, and the gateway.
        let retry = RetryPolicy::default();
        let identity = Arc::new(IdentityCopy::new(
            Arc::clone(&wall),
            config.identity().identity_max_staleness(),
        ));
        let sessions = SystemSessions::open(
            storage.shards.clone(),
            pools.index.clone(),
            config.storage().inline_max_bytes,
        )
        .await
        .map_err(|error| StartError::Shard {
            shard: SESSIONS_BUCKET_ID.to_owned(),
            reason: error.to_string(),
        })?;
        let sts = sts_endpoint(&config, &identity, &sessions, &wall)?;
        let static_keys = {
            let identity_config = config.identity().clone();
            on_pool(&pools.index, move || {
                StaticCredentials::load(&identity_config)
            })
            .await?
        };
        let lookup = NodeCredentials::new(
            static_keys,
            sessions.clone(),
            Arc::clone(&identity),
            Arc::clone(&wall),
        );
        // Peers' requests the S3 endpoint refuses for their configuration
        // (§7.8): counted where authentication refuses unsigned SkyS3
        // headers, and where the gateway refuses a peer's writes.
        let peer_refusals = PeerRefusals::register(&metrics.registry);
        let auth = SigV4Authenticator::new(lookup, Arc::clone(&wall))
            .with_hashing_pool(pools.hashing.clone())
            .with_peer_refusals(peer_refusals.clone());
        // Admission control (§7.6, §13): the dirty budgets, which the
        // flushers count against, and free disk space, read once now and
        // then every second.
        let budget = Arc::new(
            DirtyBudget::new(config.flush().max_dirty_bytes).with_buckets(config.buckets().clone()),
        );
        // The clean cache (§9.3): every replica reports to it, and it
        // evicts within `[cache]` and each disk's room, which the space
        // watcher reads.
        let cache = CleanCache::new(
            CacheSettings {
                max_bytes: config.cache().cache_max_bytes_per_node,
                reserve_fraction: config.cache().reserve_fraction,
            },
            CacheMetrics::register(&metrics.registry),
        );
        storage.shards.set().use_cache(&cache).await;
        // Segment compaction (§10.3) after every checkpoint interval: it
        // reclaims released segments and tells the cache what it evicted.
        let compactor = Compactor::new(
            storage.shards.set().clone(),
            CompactionSettings {
                live_threshold: config.storage().compaction_live_threshold,
                unreferenced_ttl: config.peering().peer_staging_ttl(),
            },
            CompactionMetrics::register(&metrics.registry),
        );
        // Lifecycle passes (§8.7) on the shards this node leads.
        let lifecycle_metrics = LifecycleMetrics::register(&metrics.registry);
        let space = Arc::new(
            DiskSpace::new(config.storage().disk_min_free_bytes).with_cache(cache.clone()),
        );
        let watched: Vec<Watched> = storage
            .disks
            .iter()
            .zip(&pools.disks)
            .map(|((disk, _), pool)| {
                let place = Place::Disk(disk.label.clone());
                (place, disk.path.clone(), pool.clone())
            })
            .chain([(
                Place::DataDir,
                data_dir.path().to_owned(),
                pools.index.clone(),
            )])
            .collect();
        space.refresh(&watched).await;
        let disk_of = {
            let shards = storage.shards.clone();
            Box::new(move |shard: &ShardRef| shards.set().disk_of(&shard.into()).clone())
        };
        let admission = NodeAdmission::new(
            Arc::clone(&budget),
            Arc::clone(&space),
            disk_of,
            &metrics.registry,
        );
        let region = config.flush().target_region.clone();
        let credentials = default_credentials(&region).await;
        let connect = {
            let (region, credentials) = (region.clone(), credentials.clone());
            move |target: &skys3_types::RemoteTarget| {
                AwsS3::builder(target, region.clone(), credentials.clone())
                    .attempt_timeout(FLUSH_ATTEMPT_TIMEOUT)
                    .build()
            }
        };
        // A `read_only` bucket's origin is read with its `origin_profile`'s
        // credentials, or the default chain's (§9.5).
        let connect_origin = move |target: &skys3_types::RemoteTarget, profile: Option<&str>| {
            let credentials = profile.map_or_else(
                || credentials.clone(),
                |profile| profile_credentials(profile, &region),
            );
            AwsS3::builder(target, region.clone(), credentials)
                .attempt_timeout(FLUSH_ATTEMPT_TIMEOUT)
                .build()
        };
        let snapshots = SnapshotService::new(Box::new(connect.clone()), config.buckets().clone())
            .with_wall_clock(Arc::clone(&wall));
        // Native replication with peer clusters (§7.8), if any is
        // configured.
        let peering = NodePeering::open(&config, Arc::clone(&wall), &metrics.handshakes)?;
        let flush = {
            let settings = FlushSettings {
                extent_bytes: config.storage().extent_bytes,
                // A body the gateway streams for at most half the TTL.
                body_timeout: config.peering().peer_staging_ttl(),
                ..FlushSettings::from_config(config.flush())
            };
            let service = FlushService::new(
                cluster.clone(),
                settings,
                Box::new(connect),
                FlushMetrics::register(&metrics.registry),
            )
            .with_budget(budget)
            .with_buckets(config.buckets().clone())
            .with_origin_connect(Box::new(connect_origin));
            // An import's records go to each shard's primary, wherever it
            // is (§9.1).
            let service = match &cluster_shards {
                Some(cluster) => service.with_commit(commit_through(cluster.routed())),
                None => service,
            };
            Arc::new(match &peering {
                Some(peering) => service.with_peer_transport(peering.transport(&config)),
                None => service,
            })
        };
        let mut gateway_config = GatewayConfig::new(&config);
        gateway_config.retry = retry;
        gateway_config.hashing_pool = Some(pools.hashing.clone());
        gateway_config.admission = Arc::new(admission);
        let fills = NodeFills {
            shards: storage.shards.clone(),
            flush: Arc::clone(&flush),
            space: Arc::clone(&space),
            routed: None,
        };
        // Other nodes' gateways ask this one to fill the shards it leads
        // (§9.2). The node keeps these fills; the server holds them weakly.
        let served_fills = cluster_shards.as_ref().map(|cluster| {
            let served: Arc<dyn Fills> = Arc::new(fills.clone());
            cluster.server().fill_with(&served);
            served
        });
        gateway_config.fills = Some(Arc::new(NodeFills {
            routed: cluster_shards.as_ref().map(|c| c.routed().clone()),
            ..fills
        }));
        gateway_config.remote = Some(Arc::new(NodeRemote::new(Arc::clone(&flush))));
        gateway_config.fragments = coding.as_ref().map(|coding| coding.source());
        gateway_config.hot_cache = HotCache::with_metrics(
            config.cache().hot_cache_bytes_per_node,
            HotCacheMetrics::register(&metrics.registry),
        );
        gateway_config.descriptors = peering.as_ref().and_then(NodePeering::descriptors);
        gateway_config.peer_access = gateway_config.peer_access.with_refusals(peer_refusals);
        if cluster_shards.is_some() {
            // New buckets' shards are placed on the registered nodes, and
            // their registers created with the bucket's (§4.1, §6.7).
            gateway_config.placement = ShardPlacement::Cluster(config.cluster().failure_domain);
        }
        // The destination side applies peers' commits as the gateway's
        // writes are applied.
        let peer_gateway_config = peering.as_ref().map(|_| gateway_config.clone());
        let ids = IdSource::from_os_rng();
        let mut gateway = match &cluster_shards {
            Some(cluster) => {
                let shards = cluster.routed().clone();
                Gateway::new(gateway_config, store.clone(), shards, ids, auth).await?
            }
            None => {
                let shards = storage.shards.clone();
                Gateway::new(gateway_config, store.clone(), shards, ids, auth).await?
            }
        };
        if let Some(sts) = &sts {
            gateway = gateway.with_sts(Arc::clone(sts) as Arc<dyn StsService>);
        }
        let shared = Arc::new(Shared {
            config,
            retry,
            wall,
            index: Arc::clone(&storage.index),
            pools: pools.clone(),
            store,
            opener,
            shards: storage.shards.clone(),
            gateway,
            identity,
            sts,
            control: Arc::clone(&control),
            control_metrics: metrics.control.clone(),
            flush: Arc::clone(&flush),
            snapshots,
            hints: ControlHints::new(),
            cluster: cluster_shards,
            coding,
            _served_fills: served_fills,
        });
        shared.sync_identity(copy.synced_at).await?;
        let mut background = JoinSet::new();
        let mut operations = None;
        let (cluster_view, registration) = match (
            shared.opener.shared_store(),
            &shared.cluster,
            &shared.coding,
        ) {
            (Some(store), Some(cluster), Some(coding)) => {
                // The transport serves, and the node is registered, before
                // its replicas open: other nodes' primaries link to them
                // as they do.
                let services = ClusterServices::start(
                    &shared.config,
                    data_dir,
                    store.clone(),
                    shared.hints.clone(),
                    (&metrics.handshakes, &metrics.registry),
                    &pools.index,
                    (cluster, coding.services()),
                )
                .await?;
                let (view, status) = (services.view(), services.status());
                operations = Some(services.operations() as Arc<dyn crate::admin::ClusterApi>);
                services.spawn(&mut background);
                cluster.resume().await?;
                (Some(view), Some(status))
            }
            _ => {
                shared.open_bucket_shards().await?;
                (None, None)
            }
        };
        if reclaim {
            reclaim_orphans(&shared).await?;
        }
        // Registered now, before the admin listener takes the registry.
        let exposure = shared
            .cluster
            .as_ref()
            .map(|cluster| cluster.report_exposure(&metrics.registry));

        // Listeners.
        let listener =
            bind_gateway(&shared.config, shared.gateway.clone(), &metrics.handshakes).await?;
        let gateway_addr = listener
            .local_addr()
            .map_err(io_error("reading the gateway's address"))?;
        let admin_config = AdminConfig {
            listen: shared.config.admin().listen,
            token: match &shared.config.admin().token_file {
                Some(path) => Some(AdminToken::from_file(path)?),
                None => None,
            },
        };
        let buckets: BucketList = {
            let gateway = shared.gateway.clone();
            Arc::new(move || gateway.buckets())
        };
        let admin_api = NodeAdmin {
            node_id: node_id.clone(),
            cluster_id: cluster,
            buckets,
            shards: storage.shards.clone(),
            disks: storage.disks.clone(),
            control,
            health: health.clone(),
            flush,
            cluster: cluster_view,
            replicated: shared
                .cluster
                .clone()
                .map(|cluster| cluster as Arc<dyn crate::admin::ShardsReport>),
            operations,
        };
        let admin = AdminListener::bind(admin_config, metrics.registry, health)
            .await?
            .with_api(Arc::new(admin_api));
        let admin_addr = admin
            .local_addr()
            .map_err(io_error("reading the admin listener's address"))?;

        // Serving and background work.
        let (stop_gateway, gateway_stopped) = oneshot::channel::<()>();
        let gateway_task = tokio::spawn(listener.serve(async {
            let _ = gateway_stopped.await;
        }));
        let (stop_admin, admin_stopped) = oneshot::channel::<()>();
        let admin_task = tokio::spawn(admin.serve(async {
            let _ = admin_stopped.await;
        }));
        let interval = shared.config.storage().index_checkpoint_interval();
        let checkpoints = {
            let checkpointer = Arc::clone(&storage.checkpointer);
            tokio::spawn(async move { checkpointer.run(interval).await })
        };
        if let Some(store) = shared.opener.shared_store().cloned() {
            // The S3 control bucket's versioning (§12), read once.
            let control_metrics = shared.control_metrics.clone();
            background.spawn(async move {
                control_metrics.set_versioning(backend::read_versioning(&store).await);
            });
        }
        background.spawn(Arc::clone(&shared).follow_control_store());
        if let Some(cluster) = &shared.cluster {
            let interval = shared.config.control_store().config_poll_interval();
            background.spawn(Arc::clone(cluster).run(interval));
        }
        if let Some(exposure) = exposure {
            background.spawn(exposure);
        }
        background.spawn(Arc::clone(&shared).sweep_sessions(sessions));
        background.spawn(Arc::clone(&shared).follow_flushes());
        background.spawn(Arc::clone(&shared).follow_lifecycle(lifecycle_metrics));
        if let Some(coding) = &shared.coding {
            coding.spawn(&mut background);
            let gateway = shared.gateway.clone();
            let set = storage.shards.set().clone();
            background.spawn(Arc::clone(coding).follow(set, move || gateway.buckets()));
        }
        let peer_endpoint = peering.as_ref().map(NodePeering::closer);
        if let (Some(peering), Some(gateway_config)) = (peering, peer_gateway_config) {
            let gateway = shared.gateway.clone();
            let buckets: BucketLookup = Arc::new(move |name: &BucketName| gateway.bucket(name));
            let shared = Arc::clone(&shared);
            background.spawn(peering.adapt_pool());
            // Peers' commits reach each shard's primary as the gateway's
            // writes do.
            background.spawn(async move {
                let config = &shared.config;
                match shared.cluster.clone() {
                    Some(cluster) => {
                        let shards = cluster.routed().clone();
                        peering
                            .serve(config, &gateway_config, shards, buckets)
                            .await;
                    }
                    None => {
                        let shards = shared.shards.clone();
                        peering
                            .serve(config, &gateway_config, shards, buckets)
                            .await;
                    }
                }
            });
        }
        background.spawn(watch_space(space, watched, DISK_WATCH_INTERVAL));
        {
            // The policies go in before the cache scans the replicas open
            // already, so that the scans apply them at once.
            shared.install_clean_copies(&shared.gateway.buckets());
            let set = storage.shards.set().clone();
            background.spawn(async move { cache.run(set).await });
        }
        background.spawn(async move { compactor.run(interval).await });
        background.spawn(watch_disks(
            storage.disks,
            pools.index.clone(),
            boot_id,
            metrics.disks_out_of_service,
            storage_ready,
        ));
        serving.set_ready(true);
        tracing::info!(%gateway_addr, %admin_addr, "serving");
        Ok(Self {
            node_id,
            gateway_addr,
            admin_addr,
            serving,
            stop_gateway,
            gateway_task,
            stop_admin,
            admin_task,
            checkpoints,
            checkpointer: storage.checkpointer,
            background,
            registration,
            shared,
            peer_endpoint,
            data_dir: opened.data_dir.take().expect("the data directory is open"),
        })
    }

    /// The node's ID.
    #[must_use]
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// The address the gateway serves on.
    #[must_use]
    pub fn gateway_addr(&self) -> SocketAddr {
        self.gateway_addr
    }

    /// The address the admin listener serves on.
    #[must_use]
    pub fn admin_addr(&self) -> SocketAddr {
        self.admin_addr
    }

    /// Serves until `stop` completes, a checkpoint fails, or, with
    /// on a node of a cluster, the node's registration turns out to
    /// belong to another data directory, then shuts down.
    ///
    /// # Errors
    ///
    /// [`NodeError`] if a checkpoint failed, while serving or at shutdown,
    /// or [`NodeError::Cluster`] for a registration another data directory
    /// holds.
    pub async fn run_until(mut self, stop: impl Future<Output = ()>) -> Result<(), NodeError> {
        let claimed = crate::cluster::claimed(
            self.registration.take(),
            self.node_id.clone(),
            self.data_dir.instance_id().to_owned(),
        );
        let failed = tokio::select! {
            () = stop => None,
            ended = &mut self.checkpoints => Some(match ended {
                Ok(error) => NodeError::Checkpoint(error),
                Err(error) => NodeError::Task(error.to_string()),
            }),
            reason = claimed => Some(NodeError::Cluster(reason)),
        };
        let shutdown = self.shutdown().await;
        match failed {
            Some(error) => {
                tracing::error!(%error, "stopping after a failure");
                Err(error)
            }
            None => shutdown,
        }
    }

    /// Stops the node: no new requests, in-flight requests drained, every
    /// shard stopped once its records are applied, and a final checkpoint.
    ///
    /// # Errors
    ///
    /// [`NodeError::Checkpoint`] if the final checkpoint fails; the next
    /// start then replays from the last durable one.
    pub async fn shutdown(self) -> Result<(), NodeError> {
        let Self {
            serving,
            stop_gateway,
            gateway_task,
            stop_admin,
            admin_task,
            checkpoints,
            checkpointer,
            mut background,
            shared,
            peer_endpoint,
            data_dir,
            ..
        } = self;
        tracing::info!("shutting down");
        serving.set_ready(false);
        let _ = stop_gateway.send(());
        let _ = gateway_task.await;
        background.shutdown().await;
        if let Some(endpoint) = peer_endpoint {
            endpoint.close();
        }
        checkpoints.abort();
        let _ = checkpoints.await;
        shared.flush.shutdown().await;
        shared.snapshots.shutdown().await;
        if let Some(coding) = &shared.coding {
            coding.stop();
        }
        if let Err(error) = shared.shards.set().close_all().await {
            tracing::warn!(%error, "a shard stopped with an error");
        }
        // The next start may find the store unreachable, so the copy it
        // runs from should be as recent as possible.
        if shared.store.is_live()
            && let Err(error) = shared.refresh_copy().await
        {
            tracing::warn!(%error, "cannot refresh the control-state copy");
        }
        let checkpoint = checkpointer.checkpoint().await;
        let _ = stop_admin.send(());
        let _ = admin_task.await;
        let index = Arc::clone(&shared.index);
        let pools = shared.pools.clone();
        drop((shared, checkpointer));
        close_index(index).await;
        pools.shutdown();
        // Only now may another process open the data directory.
        drop(data_dir);
        checkpoint?;
        tracing::info!("stopped");
        Ok(())
    }
}

/// A stopped node's data directory and its storage, recovered as a start
/// recovers them but without serving, for an offline command such as
/// `skys3 control export`. The data directory's lock keeps the node from
/// starting meanwhile, and a running node keeps the command out.
pub(crate) struct Offline {
    /// The data directory, locked.
    pub(crate) data_dir: DataDir,
    /// The index, with every log replayed into it.
    pub(crate) index: Arc<Index>,
    /// The pool index work runs on.
    pub(crate) pool: BlockingPool,
    pools: Pools,
}

impl Offline {
    /// Opens the data directory of `config`, which must hold a node, and
    /// recovers its logs into its index.
    ///
    /// # Errors
    ///
    /// [`StartError::Unsupported`] if the data directory holds no node, and
    /// otherwise what stops a start from recovering its storage, such as a
    /// data directory in use by a running node.
    pub(crate) async fn open(config: &Config) -> Result<Self, StartError> {
        let node_file = config.node().data_dir.join(datadir::NODE_FILE);
        if !node_file.exists() {
            return Err(StartError::Unsupported(format!(
                "{} holds no node",
                config.node().data_dir.display()
            )));
        }
        let mut pools = Pools {
            index: pool("index", INDEX_THREADS)?,
            hashing: pool("hashing", 1)?,
            control: pool("control", 1)?,
            disks: Vec::new(),
        };
        let mut opened = Opened::default();
        let recovered = async {
            let (node, cluster) = (config.node().clone(), config.cluster().cluster_id.clone());
            let boot_id = datadir::boot_id();
            let data_dir = on_pool(&pools.index, move || {
                DataDir::open(&node, &cluster, boot_id.as_deref())
            })
            .await?;
            let data_dir = opened.data_dir.insert(data_dir);
            open_storage(config, data_dir, &mut pools, &mut opened.index).await
        }
        .await;
        match recovered {
            Ok(storage) => {
                let index = Arc::clone(&storage.index);
                drop(storage);
                Ok(Self {
                    data_dir: opened.data_dir.take().expect("the data directory is open"),
                    index,
                    pool: pools.index.clone(),
                    pools,
                })
            }
            Err(error) => {
                if let Some(index) = opened.index.take() {
                    close_index(index).await;
                }
                drop(opened);
                pools.shutdown();
                Err(error)
            }
        }
    }

    /// Closes the index, then the data directory.
    pub(crate) async fn close(self) {
        let Self {
            data_dir,
            index,
            pool,
            pools,
        } = self;
        drop(pool);
        close_index(index).await;
        pools.shutdown();
        drop(data_dir);
    }
}

/// Waits until `index` is its last handle and closes it. Each shard's
/// pipeline task holds the index until it notices that its shard was
/// dropped, which happens on another task shortly after.
async fn close_index(index: Arc<Index>) {
    const WAIT: Duration = Duration::from_secs(10);
    let closed = timeout(WAIT, async {
        while Arc::strong_count(&index) > 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    if closed.is_err() {
        tracing::warn!("the index is still in use; it closes when the process exits");
    }
}

/// Checks that the data directory serves as the control store says (plan
/// M8-05b; [`Config::multi_node`]), and records the mode if the directory
/// holds no bucket's shards yet. A single node serves each shard alone in
/// epoch 1, and a node of a cluster opens the replicas the shard registers
/// name: a replica opened one way cannot be served the other, so a data
/// directory that holds a bucket's shards keeps the mode it served them in.
/// Moving a single node's buckets into a cluster goes through the buckets'
/// targets or an S3 copy (runbook "Moving a single node into a cluster").
///
/// # Errors
///
/// [`StartError::DataDir`] for a data directory that holds a bucket's
/// shards in the other mode; [`StartError::Index`] if the index cannot be
/// read.
async fn check_mode(
    config: &Config,
    data_dir: &DataDir,
    index: &Arc<Index>,
    pool: &BlockingPool,
) -> Result<(), StartError> {
    let multi_node = config.multi_node();
    if data_dir.multi_node() == multi_node {
        return Ok(());
    }
    let held = {
        let index = Arc::clone(index);
        on_pool(pool, move || {
            let read = index.read()?;
            let mut held: BTreeSet<BucketId> = read
                .applied_positions()?
                .into_keys()
                .map(|shard| shard.bucket)
                .collect();
            held.extend(read.configs()?.into_keys().map(|shard| shard.bucket));
            Ok::<_, IndexError>(held)
        })
        .await?
    };
    // Every node keeps STS sessions in a system bucket of its own.
    let held: Vec<String> = held
        .into_iter()
        .filter(|bucket| bucket.as_str() != SESSIONS_BUCKET_ID)
        .map(|bucket| bucket.as_str().to_owned())
        .collect();
    if !held.is_empty() {
        let (was, is, store) = if multi_node {
            (
                "a single node",
                "one of a cluster's nodes",
                "an etcd or S3 control store",
            )
        } else {
            (
                "one of a cluster's nodes",
                "a single node",
                "the file control store",
            )
        };
        return Err(DataDirError::Mismatch(format!(
            "{} holds the shards of {} bucket(s) ({}) that it served as {was}; a node over {store} \
             serves as {is} and cannot take them over: start it with a new data directory, or \
             with the control store it served them with (runbook \"Moving a single node into a \
             cluster\")",
            data_dir.path().display(),
            held.len(),
            held.join(", "),
        ))
        .into());
    }
    let path = data_dir.path().to_owned();
    on_pool(pool, move || datadir::record_multi_node(&path, multi_node)).await?;
    tracing::info!(multi_node, "recorded how the data directory serves");
    Ok(())
}

/// Commits the records of an import through `shards`, the gateway's, which
/// reach each shard's primary wherever it is (§9.1).
fn commit_through(shards: &Routed) -> Commit {
    let shards = shards.clone();
    Arc::new(move |shard, body| {
        let shards = shards.clone();
        Box::pin(async move {
            let shard = ShardRef {
                bucket: shard.bucket,
                shard: shard.shard,
            };
            match shards.write(&shard, body, Precondition::None).await {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(failed)) => Err(failed.to_string()),
                Err(error) => Err(error.to_string()),
            }
        })
    })
}

/// Drops shards whose bucket no register names (§4.1): left by a bucket
/// creation or deletion whose outcome was unknown. Only a copy read from
/// the store itself may decide that a bucket is gone.
async fn reclaim_orphans(shared: &Shared) -> Result<(), StartError> {
    let mut known: BTreeSet<BucketId> = shared
        .gateway
        .buckets()
        .iter()
        .map(|bucket: &BucketDocument| bucket.bucket_id.clone())
        .collect();
    known.insert(BucketId::new(SESSIONS_BUCKET_ID).expect("the ID is valid"));
    let index = Arc::clone(&shared.index);
    let applied = on_pool(&shared.pools.index, move || {
        index.read()?.applied_positions()
    })
    .await?;
    for shard in applied.into_keys() {
        if known.contains(&shard.bucket) {
            continue;
        }
        tracing::info!(%shard, "dropping a shard whose bucket no longer exists");
        shared
            .shards
            .set()
            .remove(&shard)
            .await
            .map_err(|error| StartError::Shard {
                shard: shard.to_string(),
                reason: error.to_string(),
            })?;
    }
    Ok(())
}

/// The observability crate's logging settings for `[logging]`.
#[must_use]
pub fn log_config(config: &Config) -> skys3_obs::LogConfig {
    skys3_obs::LogConfig {
        filter: config.logging().filter.clone(),
        format: match config.logging().format {
            LogFormat::Text => skys3_obs::LogFormat::Text,
            LogFormat::Json => skys3_obs::LogFormat::Json,
        },
    }
}
