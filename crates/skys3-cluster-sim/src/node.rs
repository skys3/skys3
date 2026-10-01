//! One simulated node: its disks, its handle on the control store, its
//! credentials, and the software one life of it runs.
//!
//! Each life starts the way the node binary does, through the same
//! functions: it recovers the logs and replays them into the index
//! ([`skys3::storage::recover`]), opens the control store or falls back to
//! the copy the index kept ([`skys3::control::open`]), builds the gateway,
//! and opens every bucket's shards. Then it serves S3 on the simulated
//! network, takes checkpoints, flushes `write_back` buckets to the remote
//! store ([`FlushService`]), and keeps trying the control store while it
//! runs from its copy. What differs from the binary is the environment:
//! simulated disks, a drifting clock, a fault-injecting control store, an
//! inline blocking pool, and an authenticator that trusts every request.

use std::collections::BTreeMap;
use std::error::Error;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use skys3::control::{self, NodeStore};
use skys3::storage;
use skys3_control::faults::FaultyStore;
use skys3_control::{ProposalIds, RetryPolicy, S3ControlStore, read_cluster};
use skys3_flush::{FlushMetrics, FlushService, FlushSettings};
use skys3_gateway::{Gateway, GatewayConfig, IdSource, LocalShards, ShardRef, Shards, TrustAll};
use skys3_index::{Index, IndexConfig};
use skys3_io::{
    BlockingPool, Clock, Drift, MonotonicClock, SimDisk, SimMount, SimPower, WallClock,
};
use skys3_log::LogConfig;
use skys3_net::{Credentials, Transport, TurmoilNetwork};
use skys3_obs::MetricsRegistry;
use skys3_sim::{NodeClock, SimS3};
use skys3_types::{BucketDocument, ClusterId, Generation, Label, NodeAddress, NodeId, ShardConfig};

use crate::s3;

/// A node's handle on the shared control store: the S3 backend over the
/// simulated control bucket, behind the node's own fault injection.
pub type ControlHandle = FaultyStore<S3ControlStore<SimS3>>;

/// The error type of node software.
pub type BoxError = Box<dyn Error + Send + Sync>;

/// The port the intra-cluster transport listens on.
pub const TRANSPORT_PORT: u16 = 7400;

/// What a node's services get each time the node starts: everything the
/// node recovered, and the cluster around it.
pub struct NodeEnv {
    /// The node's ID.
    pub node: NodeId,
    /// The node's position in the cluster, from 0.
    pub position: usize,
    /// How many times the node started before this life.
    pub life: u64,
    /// The node's clock for this life, drifting within the configured
    /// bound. Leases and timers must use it.
    pub clock: Arc<MonotonicClock>,
    /// The node's shards, recovered from its logs. None is open yet.
    pub shards: LocalShards<SimMount>,
    /// The node's index.
    pub index: Arc<Index>,
    /// The control store, read through the node's local copy until it
    /// answers.
    pub control: NodeStore<ControlHandle>,
    /// The intra-cluster transport, with this node's credentials.
    pub transport: Transport<TurmoilNetwork>,
    /// Every node's transport address, this one's included.
    pub peers: BTreeMap<NodeId, NodeAddress>,
    /// The labels of the node's disks.
    pub disks: Vec<Label>,
    /// The static shard placement: each shard's configuration.
    pub placement: Arc<BTreeMap<ShardRef, ShardConfig>>,
    /// The remote S3 store that `write_back` buckets flush to.
    pub remote: SimS3,
    /// A seed for the services' own random choices in this life.
    pub seed: u64,
}

/// The protocols a node runs on top of its storage: the extension point
/// for replication (plan M2-07) and every later protocol.
///
/// The harness calls [`NodeServices::start`] in each life of each node,
/// inside the node's simulated host, after recovery and before the gateway
/// serves. The returned shards are what the gateway calls. Services keep
/// whatever state checks need in the value itself (it is shared by every
/// node and by [`Invariant`](crate::Invariant)s), for example behind an
/// `Arc<Mutex<_>>`.
pub trait NodeServices: Clone + 'static {
    /// The shards the gateway calls.
    type Shards: Shards;

    /// Starts the services of one life of a node. Tasks it spawns end
    /// when the node crashes.
    fn start(&self, env: NodeEnv) -> impl Future<Output = Result<Self::Shards, BoxError>>;

    /// Whether the services are ready for clients once every node serves
    /// S3, such as replicated shards whose primaries have brought their
    /// members up to date. The workload starts once they are.
    fn ready(&self) -> bool {
        true
    }
}

/// The single-node services of M1: every shard is local, and the node is
/// the only member of each shard it serves.
#[derive(Debug, Clone, Copy, Default)]
pub struct LocalServices;

impl NodeServices for LocalServices {
    type Shards = LocalShards<SimMount>;

    async fn start(&self, env: NodeEnv) -> Result<Self::Shards, BoxError> {
        Ok(env.shards)
    }
}

/// The settings every node runs with.
#[derive(Clone, Debug)]
pub(crate) struct NodeSettings {
    pub cluster: ClusterId,
    pub gateway: GatewayConfig,
    pub log: LogConfig,
    pub checkpoint_interval: Duration,
    pub drift: Drift,
    pub retry: RetryPolicy,
    pub poll_interval: Duration,
    pub flush: FlushSettings,
}

/// One node of the cluster, shared by the driver and the node's host.
pub(crate) struct NodeSlot {
    pub id: NodeId,
    pub position: usize,
    pub host: String,
    pub disks: Vec<(Label, SimDisk)>,
    /// The power supply of the node's disks, which numbers their syncs.
    pub power: SimPower,
    pub control: ControlHandle,
    pub credentials: Credentials,
    /// The node's clock drift in every life, if it is fixed.
    drift: Option<Drift>,
    /// Draws for each life: its clock and seeds.
    rng: Mutex<SmallRng>,
    lives: Mutex<u64>,
    /// Whether the node served S3 at least once.
    pub ready: AtomicBool,
}

impl NodeSlot {
    pub(crate) fn new(
        id: NodeId,
        position: usize,
        disks: Vec<(Label, SimDisk)>,
        control: ControlHandle,
        credentials: Credentials,
        drift: Option<Drift>,
        seed: u64,
    ) -> Self {
        let power = SimPower::new();
        for (_, disk) in &disks {
            disk.set_power(&power);
        }
        Self {
            host: id.to_string(),
            id,
            position,
            disks,
            power,
            control,
            credentials,
            drift,
            rng: Mutex::new(SmallRng::seed_from_u64(seed)),
            lives: Mutex::new(0),
            ready: AtomicBool::new(false),
        }
    }

    /// The number of lives started so far.
    pub(crate) fn lives(&self) -> u64 {
        *self.lives.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Makes the node's disks unusable by the life that ran on them: a
    /// power loss drops what was not synced, a process kill keeps it.
    pub(crate) fn stop_disks(&self, power_loss: bool) {
        for (_, disk) in &self.disks {
            if power_loss {
                disk.crash();
            } else {
                disk.kill();
            }
        }
    }
}

/// Everything a node's software needs, shared by every node.
pub(crate) struct Shared<S> {
    pub settings: NodeSettings,
    pub services: S,
    pub peers: BTreeMap<NodeId, NodeAddress>,
    pub placement: Arc<BTreeMap<ShardRef, ShardConfig>>,
    pub remote: SimS3,
}

/// The index settings of a simulated node: a small page cache, so redb
/// writes pages out between checkpoints.
pub(crate) fn index_config(settings: &NodeSettings) -> IndexConfig {
    IndexConfig {
        checkpoint_interval: settings.checkpoint_interval,
        cache_bytes: 256 * 1024,
    }
}

/// The name of the index's block file on the node's first disk.
pub(crate) const INDEX_FILE: &str = "index.redb";

/// Runs one life of `slot`'s node until it crashes, or until a checkpoint
/// fails, which stops a real node too.
pub(crate) async fn run<S: NodeServices>(
    shared: Arc<Shared<S>>,
    slot: Arc<NodeSlot>,
) -> Result<(), BoxError> {
    let (life, clock, seed) = {
        let mut lives = slot.lives.lock().unwrap_or_else(PoisonError::into_inner);
        let life = *lives;
        *lives += 1;
        let mut rng = slot.rng.lock().unwrap_or_else(PoisonError::into_inner);
        // The drift is drawn even when it is fixed, so fixing it changes no
        // other draw of the seed.
        let drawn = Drift::random_within(&mut *rng, shared.settings.drift);
        let clock = NodeClock {
            drift: slot.drift.unwrap_or(drawn),
            start: skys3_io::MonoTime::from_nanos(rng.random_range(0..1 << 50)),
        };
        (life, clock, rng.random::<u64>())
    };
    let settings = &shared.settings;
    let clock = Arc::new(clock.start());
    let pool = BlockingPool::inline("index");
    let mounts: Vec<(Label, SimMount)> = slot
        .disks
        .iter()
        .map(|(label, disk)| (label.clone(), disk.mount()))
        .collect();
    let index = Index::open_sim(&mounts[0].1, INDEX_FILE, &index_config(settings))?;
    let recovered = storage::recover(
        mounts,
        settings.log.clone(),
        Arc::clone(&clock) as Arc<dyn Clock>,
        Arc::new(index),
        pool.clone(),
        slot.id.clone(),
    )
    .await?;

    let kept = control::kept_copy(&recovered.index, &pool).await?;
    let (store, copy) = control::open(
        slot.control.clone(),
        kept,
        &settings.cluster,
        ProposalIds::seeded(seed).next_id(),
        &settings.retry,
        &recovered.index,
        &pool,
        wall_now(),
    )
    .await?;

    let env = NodeEnv {
        node: slot.id.clone(),
        position: slot.position,
        life,
        clock,
        shards: recovered.shards.clone(),
        index: Arc::clone(&recovered.index),
        control: store.clone(),
        transport: Transport::new(TurmoilNetwork, &slot.credentials),
        peers: shared.peers.clone(),
        disks: slot.disks.iter().map(|(label, _)| label.clone()).collect(),
        placement: Arc::clone(&shared.placement),
        remote: shared.remote.clone(),
        seed: seed.rotate_left(17),
    };
    let shards = shared.services.start(env).await?;
    let gateway = Gateway::new(
        settings.gateway.clone(),
        store.clone(),
        shards.clone(),
        IdSource::seeded(seed),
        TrustAll,
    )
    .await?;
    open_shards(&shards, &gateway.buckets()).await?;
    {
        let flush = flush_service(settings, &shared.remote);
        let (gateway, local) = (gateway.clone(), recovered.shards.clone());
        tokio::spawn(async move { follow_flushes(&flush, &gateway, &local).await });
    }

    let checkpointer = Arc::clone(&recovered.checkpointer);
    let interval = settings.checkpoint_interval;
    let checkpoints = tokio::spawn(async move { checkpointer.run(interval).await });
    {
        let (settings, index, gateway, shards) = (
            shared.settings.clone(),
            Arc::clone(&recovered.index),
            gateway.clone(),
            shards.clone(),
        );
        let known = store.is_live().then_some(copy.generation);
        tokio::spawn(async move {
            follow_control_store(&settings, &store, &index, &pool, &gateway, &shards, known).await;
        });
    }
    let listener = s3::bind().await?;
    slot.ready.store(true, Ordering::SeqCst);
    tokio::select! {
        served = s3::serve(listener, gateway) => served?,
        failed = checkpoints => {
            // A failed checkpoint stops the node; the harness restarts it.
            let error = failed.map_or_else(|e| e.to_string(), |e| e.to_string());
            tracing::warn!(node = %slot.id, %error, "a checkpoint failed; the node stops");
        }
    }
    Ok(())
}

/// Opens the shards of every bucket in `buckets`.
async fn open_shards<H: Shards>(shards: &H, buckets: &[BucketDocument]) -> Result<(), BoxError> {
    for bucket in buckets {
        for shard in ShardRef::all(bucket) {
            shards.open(&shard, bucket).await?;
        }
    }
    Ok(())
}

/// The node's flushers: every `write_back` bucket flushes to `remote`, the
/// harness's one remote store, whatever its target names; bucket prefixes
/// keep their keys apart.
fn flush_service(settings: &NodeSettings, remote: &SimS3) -> FlushService<SimS3, SimMount> {
    let remote = remote.clone();
    FlushService::new(
        settings.cluster.clone(),
        settings.flush.clone(),
        Box::new(move |_| remote.clone()),
        FlushMetrics::register(&MetricsRegistry::new()),
    )
    .with_wall_clock(Arc::new(SimWallClock))
}

/// Keeps a flusher on every `write_back` bucket shard open on the node, as
/// the node binary does (§7.1).
async fn follow_flushes(
    flush: &FlushService<SimS3, SimMount>,
    gateway: &Gateway<TrustAll>,
    shards: &LocalShards<SimMount>,
) {
    loop {
        flush.reconcile(&gateway.buckets(), shards.set()).await;
        tokio::time::sleep(FLUSH_FOLLOW_INTERVAL).await;
    }
}

/// How often a node checks which shards it flushes: more often than the
/// node binary's second, so that flushes start within a short workload.
const FLUSH_FOLLOW_INTERVAL: Duration = Duration::from_millis(200);

/// Wall-clock time in the simulation, for dirty ages.
#[derive(Debug, Clone, Copy)]
struct SimWallClock;

impl WallClock for SimWallClock {
    fn now(&self) -> Duration {
        wall_now()
    }
}

/// Keeps the node's copy of control state current, as the node binary's
/// poll does (§6.2): every poll interval it reads the generation in
/// `cluster.json`, and when it moved, or while the node still serves from
/// its local copy, it keeps a fresh copy, serves from the store, reloads
/// the buckets, and opens the shards of new ones.
async fn follow_control_store<H: Shards>(
    settings: &NodeSettings,
    store: &NodeStore<ControlHandle>,
    index: &Arc<Index>,
    pool: &BlockingPool,
    gateway: &Gateway<TrustAll>,
    shards: &H,
    mut known: Option<Generation>,
) {
    loop {
        tokio::time::sleep(settings.poll_interval).await;
        let Some(handle) = store.store() else {
            continue;
        };
        let Ok(current) = read_cluster(&handle, &settings.cluster, &settings.retry).await else {
            continue;
        };
        if store.is_live() && Some(current.value.generation) == known {
            continue;
        }
        let refreshed = control::refresh(
            &handle,
            &settings.cluster,
            &settings.retry,
            index,
            pool,
            wall_now(),
        )
        .await;
        let Ok(copy) = refreshed else {
            continue;
        };
        store.go_live();
        if gateway.reload_buckets().await.is_ok()
            && open_shards(shards, &gateway.buckets()).await.is_ok()
        {
            known = Some(copy.generation);
        }
    }
}

/// The simulated wall-clock time, as time since the Unix epoch.
fn wall_now() -> Duration {
    turmoil::since_epoch().unwrap_or_default()
}
