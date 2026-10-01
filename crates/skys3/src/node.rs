//! Starting, running, and stopping a node (design §3, §6.2, §10).

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use prometheus_client::metrics::gauge::Gauge;
use skys3_config::{Config, ControlStoreBackend, LogFormat};
use skys3_control::{ChangeStream, ControlStore};
use skys3_control::{
    ControlError, FileControlStore, ProposalIds, RetryPolicy, bootstrap, read_cluster,
};
use skys3_gateway::{
    CredentialError, Gateway, GatewayConfig, GatewayListener, IdSource, LocalShards, ShardRef,
    Shards, SigV4Authenticator, StaticCredentials, StsService,
};
use skys3_index::{Checkpointer, Index, IndexConfig, IndexError};
use skys3_io::{BlockingPool, MonotonicClock, RealDisk, SystemWallClock, WallClock};
use skys3_log::{LogConfig, SegmentLog};
use skys3_obs::{AdminConfig, AdminError, AdminListener, AdminToken, Health, MetricsRegistry};
use skys3_shard::{ShardSet, StateMachine};
use skys3_sts::{
    HttpsFetcher, HttpsFetcherOptions, IdentityCopy, NodeCredentials, OidcValidator, SessionStore,
    StsEndpoint, StsSettings, ValidatorSettings,
};
use skys3_types::Generation;
use skys3_types::{BucketDocument, BucketId, Label, NodeId};
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::timeout;

use crate::admin::{BucketList, ControlState, NodeAdmin};
use crate::control::{ControlCopy, NodeStore, OpenStoreError, StoreOwner, open_file_store};
use crate::datadir::{self, DataDir, DataDirError, DiskDir};
use crate::sessions::{SESSIONS_BUCKET_ID, SystemSessions};

/// Worker threads per disk for log I/O (§10.4).
const DISK_THREADS: usize = 4;
/// Worker threads for index I/O.
const INDEX_THREADS: usize = 4;
/// How often expired sessions are removed.
const SESSION_SWEEP_INTERVAL: Duration = Duration::from_secs(300);
/// How often the node checks whether a disk went out of service.
const DISK_WATCH_INTERVAL: Duration = Duration::from_secs(1);

/// The shards of a running node.
type NodeShards = LocalShards<RealDisk>;
/// Session records.
type Sessions = SystemSessions<RealDisk>;
/// The control store as the gateway uses it.
type Store = NodeStore<FileControlStore>;
/// The STS endpoint.
type Sts = StsEndpoint<HttpsFetcher, Sessions>;
/// The gateway's authenticator.
type NodeAuth = SigV4Authenticator<NodeCredentials<Sessions>>;
/// The gateway.
type NodeGateway = Gateway<NodeAuth>;

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
}

/// Pools the node runs its blocking work on.
#[derive(Debug, Clone)]
struct Pools {
    index: BlockingPool,
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
struct Shared {
    config: Config,
    retry: RetryPolicy,
    wall: Arc<dyn WallClock>,
    index: Arc<Index>,
    pools: Pools,
    store: Store,
    /// The file control store's directory and owner, for reopening it.
    control_dir: PathBuf,
    owner: StoreOwner,
    shards: NodeShards,
    gateway: NodeGateway,
    identity: Arc<IdentityCopy>,
    sts: Option<Arc<Sts>>,
    control: Arc<Mutex<ControlState>>,
    control_live: Gauge,
}

impl Shared {
    /// Syncs the identity copy from the store, with its age running from
    /// `started`, and allowlists its providers for STS.
    async fn sync_identity(&self, started: Duration) -> Result<(), ControlError> {
        match &self.sts {
            Some(sts) => {
                sts.sync_identity_as_of(&self.store, &self.retry, started)
                    .await
            }
            None => self
                .identity
                .sync_as_of(&self.store, &self.retry, started)
                .await
                .map(drop),
        }
    }

    /// Opens the shards of every bucket the gateway knows.
    async fn open_bucket_shards(&self) -> Result<(), StartError> {
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

    /// Reads a fresh copy from the store and keeps it in the index.
    async fn refresh_copy(&self) -> Result<ControlCopy, StartError> {
        let started = self.wall.now();
        let cluster = &self.config.cluster().cluster_id;
        let store = self.store.store().ok_or_else(no_store)?;
        let copy = ControlCopy::fetch(&store, cluster, &self.retry, started).await?;
        let (index, kept) = (Arc::clone(&self.index), copy.clone());
        on_pool(&self.pools.index, move || kept.save(&index)).await?;
        Ok(copy)
    }

    /// The generation of the node's copy.
    fn generation(&self) -> Option<Generation> {
        self.control
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .generation
    }

    /// Reads a fresh copy from the store, keeps it, and serves from the
    /// store from now on: buckets, identity, and the shards of new
    /// buckets.
    async fn sync(&self) -> Result<(), StartError> {
        let copy = self.refresh_copy().await?;
        let started = copy.synced_at;
        self.store.go_live();
        self.control_live.set(1);
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
    /// `cluster.json` moved. While the node runs from its copy, it tries a
    /// sync every interval.
    async fn follow_control_store(self: Arc<Self>) {
        let interval = self.config.control_store().config_poll_interval();
        let cluster = self.config.cluster().cluster_id.clone();
        loop {
            let known = self.generation();
            if self.store.is_live() {
                let after = known.unwrap_or(Generation::ZERO);
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
            } else {
                tokio::time::sleep(interval).await;
            }
            let store = match self.store.store() {
                Some(store) => store,
                None => match self.reopen().await {
                    Some(store) => store,
                    None => continue,
                },
            };
            let moved = match read_cluster(&store, &cluster, &self.retry).await {
                Ok(current) => Some(current.value.generation) != known,
                Err(error) => {
                    tracing::warn!(%error, "cannot read the control store's generation");
                    continue;
                }
            };
            if (moved || !self.store.is_live())
                && let Err(error) = self.sync().await
            {
                tracing::warn!(%error, "cannot sync the control-state copy");
                // The stream would report the same generation at once.
                tokio::time::sleep(interval).await;
            }
        }
    }

    /// Opens the control store for a node that runs from its copy without
    /// one, and gives it to the node store.
    async fn reopen(&self) -> Option<FileControlStore> {
        match open_file_store(&self.control_dir, &self.owner, true, &self.pools.control).await {
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

/// Runs blocking `job` on `pool`.
async fn on_pool<T, E>(
    pool: &BlockingPool,
    job: impl FnOnce() -> Result<T, E> + Send + 'static,
) -> Result<T, StartError>
where
    T: Send + 'static,
    E: Send + 'static,
    StartError: From<E>,
{
    match pool.run(job).await {
        Ok(result) => result.map_err(StartError::from),
        Err(closed) => Err(io_error(format!("the {} pool", pool.name()))(closed.into())),
    }
}

/// The node's metrics beyond the admin listener's own.
struct NodeMetrics {
    registry: MetricsRegistry,
    disks_out_of_service: Gauge,
    control_store_live: Gauge,
}

impl NodeMetrics {
    fn new() -> Self {
        let registry = MetricsRegistry::new();
        let disks_out_of_service = Gauge::default();
        registry.register(
            "disks_out_of_service",
            "Disks taken out of service after an I/O error; used again after a host restart.",
            disks_out_of_service.clone(),
        );
        let control_store_live = Gauge::default();
        registry.register(
            "control_store_live",
            "1 once the control store answered since startup; 0 while the node serves its \
             local copy of control state.",
            control_store_live.clone(),
        );
        Self {
            registry,
            disks_out_of_service,
            control_store_live,
        }
    }
}

/// The storage engine after recovery.
struct Storage {
    disks: Vec<(DiskDir, SegmentLog<RealDisk>)>,
    index: Arc<Index>,
    checkpointer: Arc<Checkpointer<RealDisk>>,
    shards: NodeShards,
}

/// Recovers each disk's log, opens the index, and replays the logs into
/// it (§10.1, §10.2). The index is kept in `opened` as soon as it is open.
async fn open_storage(
    config: &Config,
    data_dir: &DataDir,
    pools: &mut Pools,
    opened: &mut Option<Arc<Index>>,
) -> Result<Storage, StartError> {
    let clock = Arc::new(MonotonicClock::new());
    let mut logs = BTreeMap::new();
    let mut disks = Vec::new();
    for disk in data_dir.disks() {
        let disk_pool = pool(&format!("disk-{}", disk.label), DISK_THREADS)?;
        pools.disks.push(disk_pool.clone());
        let real = RealDisk::open(&disk.path, disk_pool)
            .await
            .map_err(io_error(format!("opening {}", disk.path.display())))?;
        let log_config = LogConfig::from_storage(config.storage());
        let (log, report) = SegmentLog::open(real, log_config, clock.clone())
            .await
            .map_err(|source| StartError::Recovery {
                disk: disk.label.clone(),
                source,
            })?;
        tracing::info!(disk = %disk.label, ?report, "recovered the log");
        logs.insert(disk.label.clone(), log.clone());
        disks.push((disk.clone(), log));
    }
    let path = data_dir.path().join("index.redb");
    let index_config = IndexConfig::from_storage(config.storage());
    let index = on_pool(&pools.index, move || Index::open(&path, &index_config)).await?;
    let index = Arc::clone(opened.insert(Arc::new(index)));
    let checkpointer = Arc::new(Checkpointer::new(
        Arc::clone(&index),
        logs.clone(),
        pools.index.clone(),
    ));
    let replayed = checkpointer.replay(Arc::new(StateMachine)).await?;
    tracing::info!(records = replayed.applied, "replayed the logs");
    let set = ShardSet::with_disks(Arc::clone(&index), logs, pools.index.clone());
    Ok(Storage {
        disks,
        index,
        checkpointer,
        shards: LocalShards::new(set, data_dir.node_id().clone()),
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

/// Opens the file control store, bootstraps `cluster.json` if this node
/// never synced, and syncs the copy kept in the index. If the store cannot
/// be opened or does not answer, or looks reset, a node with a copy runs
/// from it (§6.2); a store that belongs to another node is refused.
async fn open_control(
    config: &Config,
    owner: &StoreOwner,
    directory: &Path,
    index: &Arc<Index>,
    pools: &Pools,
    started: Duration,
) -> Result<OpenedControl, StartError> {
    let cluster = &config.cluster().cluster_id;
    let retry = RetryPolicy::default();
    let kept = {
        let index = Arc::clone(index);
        on_pool(&pools.index, move || ControlCopy::load(&index)).await?
    };
    let initialized = kept.is_some();
    let opened = match open_file_store(directory, owner, initialized, &pools.control).await {
        Ok(opened) => opened,
        Err(OpenStoreError::Foreign(reason)) => return Err(StartError::ControlStore(reason)),
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
    let store = opened.store;
    let fetched = async {
        // A node that synced before never bootstraps: its store must
        // already hold cluster.json (open_file_store checked).
        if !initialized {
            let proposal = ProposalIds::from_os_rng().next_id();
            bootstrap(&store, cluster, proposal, &retry).await?;
        }
        ControlCopy::fetch(&store, cluster, &retry, started).await
    }
    .await;
    match (fetched, kept) {
        (Ok(copy), _) => {
            let (index, saved) = (Arc::clone(index), copy.clone());
            on_pool(&pools.index, move || saved.save(&index)).await?;
            Ok(OpenedControl {
                store: NodeStore::live(store),
                copy,
                reclaim: !opened.claimed,
            })
        }
        (Err(error), Some(kept)) => {
            tracing::warn!(
                %error,
                generation = %kept.generation,
                "the control store does not answer; running from the local copy"
            );
            Ok(OpenedControl {
                store: NodeStore::from_copy(Some(store), kept.clone()),
                copy: kept,
                reclaim: false,
            })
        }
        (Err(error), None) => Err(error.into()),
    }
}

/// The error a node without a control store gives for what needs one.
fn no_store() -> ControlError {
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
/// certificate.
async fn bind_gateway(
    config: &Config,
    gateway: NodeGateway,
) -> Result<GatewayListener<NodeAuth>, StartError> {
    let settings = config.gateway();
    let listener = GatewayListener::bind(settings.listen, gateway)
        .await
        .map_err(io_error(format!(
            "binding the gateway to {}",
            settings.listen
        )))?;
    if let Some((cert, key)) = settings.tls() {
        return Ok(listener.with_tls(crate::tls::server_config(cert, key)?));
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
/// 3. Open the control store, bootstrap `cluster.json`, and sync the local
///    copy of bucket bindings and identity configuration. If the store does
///    not answer, run from the copy kept in the index ([`crate::control`]).
/// 4. Build the gateway with SigV4 authentication over static credentials
///    and STS sessions, which live in the system bucket
///    ([`crate::sessions`]), and STS if `sts_web_identity` is on.
/// 5. Open every bucket's shards, and drop shards whose bucket no register
///    names: those a creation or deletion of unknown outcome left behind
///    (§4.1). This needs a copy read from the store itself.
/// 6. Serve the gateway (HTTPS when `[gateway]` names a certificate) and
///    the admin listener, and start the background work: checkpoints,
///    control-store polling, session expiry, and disk fencing.
///
/// [`Node::shutdown`] stops accepting requests, drains those in flight,
/// stops every shard once its sequenced records are applied, and takes a
/// final checkpoint, so the next start replays little.
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
    shared: Arc<Shared>,
    _data_dir: DataDir,
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
        let ControlStoreBackend::File { directory } = &config.control_store().backend else {
            return Err(StartError::Unsupported(
                "this build runs only with [control_store] backend = \"file\"; the etcd and s3 \
                 backends arrive with replication (plan M2-04, M2-05)"
                    .to_owned(),
            ));
        };
        let control_dir = directory.clone();
        let threads = std::thread::available_parallelism().map_or(4, NonZeroUsize::get);
        let index_pool = pool("index", INDEX_THREADS)?;
        let pools = Pools {
            hashing: pool("hashing", threads)?,
            control: pool("control", 1)?,
            disks: Vec::new(),
            index: index_pool,
        };
        let mut opened = Opened::default();
        let result = Self::start_with(config, control_dir, pools.clone(), &mut opened).await;
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
        control_dir: PathBuf,
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
        let storage = open_storage(&config, data_dir, &mut pools, &mut opened.index).await?;

        let metrics = NodeMetrics::new();
        let health = Health::new();
        let serving = health.register("gateway");
        let storage_ready = health.register("storage");
        storage_ready.set_ready(true);

        let wall: Arc<dyn WallClock> = Arc::new(SystemWallClock);
        let owner = StoreOwner::new(
            cluster.clone(),
            node_id.clone(),
            data_dir.instance_id().to_owned(),
        );
        let OpenedControl {
            store,
            copy,
            reclaim,
        } = open_control(
            &config,
            &owner,
            &control_dir,
            &storage.index,
            &pools,
            wall.now(),
        )
        .await?;
        metrics.control_store_live.set(i64::from(store.is_live()));
        let control = Arc::new(Mutex::new(ControlState {
            live: store.is_live(),
            generation: Some(copy.generation),
            synced_at: Some(copy.synced_at),
        }));

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
        let auth = SigV4Authenticator::new(lookup, Arc::clone(&wall))
            .with_hashing_pool(pools.hashing.clone());
        let mut gateway_config = GatewayConfig::new(&config);
        gateway_config.retry = retry;
        gateway_config.hashing_pool = Some(pools.hashing.clone());
        let ids = IdSource::from_os_rng();
        let mut gateway = Gateway::new(
            gateway_config,
            store.clone(),
            storage.shards.clone(),
            ids,
            auth,
        )
        .await?;
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
            control_dir,
            owner,
            shards: storage.shards.clone(),
            gateway,
            identity,
            sts,
            control: Arc::clone(&control),
            control_live: metrics.control_store_live.clone(),
        });
        shared.sync_identity(copy.synced_at).await?;
        shared.open_bucket_shards().await?;
        if reclaim {
            reclaim_orphans(&shared).await?;
        }

        // Listeners.
        let listener = bind_gateway(&shared.config, shared.gateway.clone()).await?;
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
        let mut background = JoinSet::new();
        background.spawn(Arc::clone(&shared).follow_control_store());
        background.spawn(Arc::clone(&shared).sweep_sessions(sessions));
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
            shared,
            _data_dir: opened.data_dir.take().expect("the data directory is open"),
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

    /// Serves until `stop` completes or a checkpoint fails, then shuts
    /// down.
    ///
    /// # Errors
    ///
    /// [`NodeError`] if a checkpoint failed, while serving or at shutdown.
    pub async fn run_until(mut self, stop: impl Future<Output = ()>) -> Result<(), NodeError> {
        let failed = tokio::select! {
            () = stop => None,
            ended = &mut self.checkpoints => Some(match ended {
                Ok(error) => NodeError::Checkpoint(error),
                Err(error) => NodeError::Task(error.to_string()),
            }),
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
            _data_dir: data_dir,
            ..
        } = self;
        tracing::info!("shutting down");
        serving.set_ready(false);
        let _ = stop_gateway.send(());
        let _ = gateway_task.await;
        background.shutdown().await;
        checkpoints.abort();
        let _ = checkpoints.await;
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
