//! Erasure coding under crashes (plan M5-04, design §8.4): a shard
//! primary encodes its objects while the driver crashes, kills, or cuts
//! the power of the primary, a member, or a fragment holder at a chosen
//! step of an attempt, and the object must stay readable in full from its
//! replicas or from `k` fragments of each stripe throughout.
//!
//! # The cluster
//!
//! Six nodes, `n0` to `n5`, each with a log disk and a fragment disk on
//! one power supply. One shard has the members `n0` (its primary), `n1`,
//! and `n2`, under a static configuration, replicated as in M2-07. Every
//! node keeps a fragment store and serves fragment writes on its transport
//! port beside replication ([`FragmentServer`]); with the default `[ec]`
//! policy six nodes code stripes as 3+2. Each life of a node recovers its
//! log and index ([`storage::recover`]), takes checkpoints, and compacts
//! its log with a short release delay, so members drop the replicas of
//! coded objects within the run. The primary writes the objects through
//! its shard, once, and runs an [`Encoder`] whose steps the driver
//! watches.
//!
//! # What is checked
//!
//! Every 10 ms of simulated time, for every object, over the nodes that
//! are up (a node that is down counts as holding whatever it held):
//!
//! - some member locates every byte of its version, or some coded layout
//!   a member applied has at least `k` fragments of each stripe in the
//!   holders' stores; and
//! - a member that applied an `EC_PUBLISH` and no longer locates the
//!   version's bytes dropped them only once the record committed: some
//!   replica learned a commit watermark at or past it.
//!
//! Once every object is coded on the primary and no member locates its
//! replicated bytes, every node loses power and is recovered outside the
//! simulation. Then each member's entry of each object must read back the
//! last bytes written: from its own log, or decoded from the fragments
//! its coded layout names.

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use rand::Rng;
use skys3::storage::{self, Storage};
use skys3_config::{EcConfig, FailureDomain};
use skys3_coord::{Candidate, FragmentPlanner, GeometryPolicy, NodeState, Topology};
use skys3_ec::{
    EncodeEvent, EncodeStep, Encoder, EncoderSettings, FragmentClient, FragmentServer,
    FragmentStore, FragmentStoreConfig, SeededBug as StoreBug, codec,
};
use skys3_index::{Entry, Index, IndexConfig, IndexReader};
use skys3_io::{
    BlockingPool, Clock, MonotonicClock, SimDisk, SimMount, SimPower, SyncCut, WallClock,
};
use skys3_log::record::{Extent, ExtentRef, Put, PutData};
use skys3_log::{LogConfig, RecordBody, ShardRef};
use skys3_net::{Credentials, Listener, MessageKind, Transport, TurmoilNetwork};
use skys3_shard::replication::{Replication, ReplicationConfig};
use skys3_shard::{
    CompactionMetrics, CompactionSettings, Compactor, ReadSettings, SeededBug, Shard,
};
use skys3_sim::SimContext;
use skys3_types::{
    BucketId, ClusterId, ETag, Epoch, Label, NodeAddress, NodeId, ProposalId, ShardConfig, ShardId,
};

use crate::node::{BoxError, TRANSPORT_PORT};
use crate::pki::Pki;

/// Where in an attempt the driver crashes nodes: at the first time the
/// encoder reports the step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashPoint {
    /// A stripe is planned and encoded, and no fragment of it is durable.
    StripeStarted,
    /// Two fragments of a stripe are durable, and the rest are not yet.
    MidStripe,
    /// Every fragment of a stripe is durable, and others remain.
    StripeWritten,
    /// The `EC_PUBLISH` record is sequenced, not yet committed.
    Appended,
    /// The record committed, and no member has dropped its replica yet.
    Committed,
    /// The last stripe of an object is planned and encoded: once its
    /// fragments are acknowledged, the attempt publishes.
    LastStripeStarted,
}

impl CrashPoint {
    /// Every point, in the order an attempt reaches them.
    pub const ALL: [CrashPoint; 6] = [
        CrashPoint::StripeStarted,
        CrashPoint::MidStripe,
        CrashPoint::StripeWritten,
        CrashPoint::LastStripeStarted,
        CrashPoint::Appended,
        CrashPoint::Committed,
    ];

    /// Whether `events` reached the point at their last event.
    fn reached(self, events: &[EncodeEvent]) -> bool {
        let Some(last) = events.last() else {
            return false;
        };
        match (self, &last.step) {
            (CrashPoint::LastStripeStarted, EncodeStep::StripeStarted { stripe, .. }) => {
                let stripes = events.iter().rev().find_map(|event| match event.step {
                    EncodeStep::Started { stripes } if event.attempt == last.attempt => {
                        Some(stripes)
                    }
                    _ => None,
                });
                stripes == Some(stripe + 1)
            }
            (CrashPoint::StripeStarted, EncodeStep::StripeStarted { .. })
            | (CrashPoint::StripeWritten, EncodeStep::StripeWritten { .. })
            | (CrashPoint::Appended, EncodeStep::Appended)
            | (CrashPoint::Committed, EncodeStep::Committed { .. }) => true,
            (CrashPoint::MidStripe, EncodeStep::FragmentWritten { written, .. }) => *written == 2,
            _ => false,
        }
    }
}

/// Which node a crash hits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashTarget {
    /// The shard's primary, `n0`, which also holds fragments.
    Primary,
    /// The shard's member `n1` (1) or `n2` (2).
    Member(usize),
    /// A node that holds a fragment of the stripe the encoder last
    /// started and is not a member.
    Holder,
    /// Every node that holds a fragment of that stripe, but the primary.
    Holders,
}

/// How a crash hits its node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashKind {
    /// The process dies; what it wrote stays in the page cache.
    Kill,
    /// The power fails at once.
    PowerLoss,
    /// The power fails at the node's next sync, before it takes effect.
    PowerAtNextSync,
}

/// One crash: `delay` after its point is reached, `kind` hits `target`,
/// which restarts `downtime` later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Crash {
    /// How long after the point.
    pub delay: Duration,
    /// The node.
    pub target: CrashTarget,
    /// How.
    pub kind: CrashKind,
    /// How long the node stays down.
    pub downtime: Duration,
}

/// What a run encodes, and how it is hurt.
#[derive(Debug, Clone)]
pub struct CodingConfig {
    /// How many objects the primary writes.
    pub objects: usize,
    /// The range of their sizes.
    pub object_bytes: Range<usize>,
    /// `ec_stripe_data_bytes`.
    pub stripe_data_bytes: u64,
    /// The step the crashes fall at, if any, and the crashes.
    pub crashes: Option<(CrashPoint, Vec<Crash>)>,
    /// Whether the primary overwrites the first object, with as many
    /// bytes, once its encoding has started.
    pub overwrite: bool,
    /// A fragment-store bug seeded into every node but the primary.
    pub store_bug: Option<StoreBug>,
}

impl Default for CodingConfig {
    fn default() -> Self {
        Self {
            objects: 4,
            object_bytes: 12 * 1024..30 * 1024,
            stripe_data_bytes: 8 * 1024,
            crashes: None,
            overwrite: false,
            store_bug: None,
        }
    }
}

/// What a run saw.
#[derive(Debug, Clone, Default)]
pub struct CodingReport {
    /// Attempts whose `EC_PUBLISH` committed and was applied.
    pub published: usize,
    /// Attempts whose `EC_PUBLISH` was rejected as superseded.
    pub rejected: usize,
    /// Attempts abandoned before they appended their record.
    pub abandoned: usize,
    /// Stripes planned, re-plans included.
    pub stripes: usize,
    /// The crashes the driver made, as `(node, kind)`.
    pub crashes: Vec<(String, CrashKind)>,
    /// When every object was coded and its replicas dropped.
    pub settled_at: Duration,
}

const NODES: usize = 6;
const MEMBERS: usize = 3;
const CHECK_INTERVAL: Duration = Duration::from_millis(10);
const RELEASE_DELAY: Duration = Duration::from_millis(300);
const CHECKPOINT_INTERVAL: Duration = Duration::from_millis(250);
const COMPACTION_INTERVAL: Duration = Duration::from_millis(200);
const SCAN_INTERVAL: Duration = Duration::from_millis(100);
const SUPERVISOR_DELAY: Duration = Duration::from_millis(500);
/// How long a host runs on after its disks lost power at a sync.
const CUT_GRACE: Duration = Duration::from_millis(3);
/// When a run that has not settled fails.
const SETTLE_LIMIT: Duration = Duration::from_secs(120);

/// One node: its disks, power, and credentials, and its current life's
/// handles, which the driver inspects.
struct NodeSlot {
    id: NodeId,
    log_disk: SimDisk,
    fragment_disk: SimDisk,
    power: SimPower,
    credentials: Credentials,
    view: Mutex<Option<View>>,
}

/// What the driver reads of a running node.
#[derive(Clone)]
struct View {
    index: Arc<Index>,
    shard: Option<Shard<SimMount>>,
    fragments: FragmentServer<SimMount>,
}

/// Everything the nodes and the driver share.
struct World {
    nodes: Vec<Arc<NodeSlot>>,
    peers: BTreeMap<NodeId, NodeAddress>,
    shard: ShardRef,
    config: ShardConfig,
    run: CodingConfig,
    sizes: Vec<usize>,
    seed: u64,
    events: Mutex<Vec<EncodeEvent>>,
    /// The bytes last acknowledged for each key.
    expected: Mutex<BTreeMap<String, Vec<u8>>>,
    written: AtomicBool,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Runs one simulation of `config`.
///
/// # Errors
///
/// The first check that fails, or a failure of the simulation.
pub fn run(context: &mut SimContext, config: &CodingConfig) -> Result<CodingReport, String> {
    let world = Arc::new(build(context, config).map_err(|e| e.to_string())?);
    let mut sim = context
        .builder()
        .simulation_duration(SETTLE_LIMIT + Duration::from_secs(10))
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(4))
        .build();
    for (index, slot) in world.nodes.iter().enumerate() {
        let world = Arc::clone(&world);
        sim.host(slot.id.as_str(), move || {
            let world = Arc::clone(&world);
            async move {
                if let Err(error) = life(&world, index).await {
                    tracing::warn!(node = index, %error, "the node stopped");
                }
                Ok(())
            }
        });
    }
    let mut driver = Driver {
        world: &world,
        down: vec![None; NODES],
        cut: vec![false; NODES],
        crashes: config.crashes.clone(),
        seen: 0,
        due: Vec::new(),
        committed: 0,
        report: CodingReport::default(),
    };
    let mut next_check = CHECK_INTERVAL;
    loop {
        sim.step()
            .map_err(|e| format!("the simulation failed: {e}"))?;
        let now = sim.elapsed();
        driver.advance(&mut sim, now);
        if now >= next_check {
            next_check = now + CHECK_INTERVAL;
            driver.check()?;
            if driver.settled() {
                driver.report.settled_at = now;
                break;
            }
        }
        if now > SETTLE_LIMIT {
            return Err(format!(
                "the objects were not all coded with their replicas dropped by {now:?}"
            ));
        }
    }
    for slot in &world.nodes {
        slot.log_disk.crash();
        slot.fragment_disk.crash();
        *lock(&slot.view) = None;
        sim.crash(slot.id.as_str());
    }
    let events = lock(&world.events);
    for event in events.iter() {
        let report = &mut driver.report;
        match event.step {
            EncodeStep::Committed {
                published: true, ..
            } => report.published += 1,
            EncodeStep::Committed { .. } => report.rejected += 1,
            EncodeStep::Abandoned => report.abandoned += 1,
            EncodeStep::StripeStarted { .. } => report.stripes += 1,
            _ => {}
        }
    }
    drop(events);
    let report = driver.report.clone();
    drop(sim);
    final_check(&world)?;
    Ok(report)
}

fn build(context: &mut SimContext, config: &CodingConfig) -> Result<World, BoxError> {
    let cluster = ClusterId::new("coding")?;
    let pki = Pki::new()?;
    let mut nodes = Vec::new();
    for n in 0..NODES {
        let id: NodeId = format!("n{n}").parse()?;
        let power = SimPower::new();
        let (log_disk, fragment_disk) = (context.disk(), context.disk());
        log_disk.set_power(&power);
        fragment_disk.set_power(&power);
        nodes.push(Arc::new(NodeSlot {
            credentials: pki.node(&cluster, &id)?,
            id,
            log_disk,
            fragment_disk,
            power,
            view: Mutex::new(None),
        }));
    }
    let peers = nodes
        .iter()
        .map(|slot| {
            Ok((
                slot.id.clone(),
                format!("{}:{TRANSPORT_PORT}", slot.id).parse()?,
            ))
        })
        .collect::<Result<_, BoxError>>()?;
    let shard = ShardRef::new(BucketId::new("b-coded")?, ShardId::new(0));
    let members: Vec<NodeId> = nodes[..MEMBERS].iter().map(|s| s.id.clone()).collect();
    let shard_config = ShardConfig {
        bucket_id: shard.bucket.clone(),
        shard: shard.shard,
        epoch: Epoch::new(1),
        primary: members[0].clone(),
        members,
        learners: Vec::new(),
        min_write_replicas: 1,
        replicas: MEMBERS as u8,
        proposal_id: ProposalId::new("p-coding")?,
    };
    let sizes = (0..config.objects)
        .map(|_| context.rng().random_range(config.object_bytes.clone()))
        .collect();
    Ok(World {
        nodes,
        peers,
        shard,
        config: shard_config,
        run: config.clone(),
        sizes,
        seed: context.fork_seed(),
        events: Mutex::new(Vec::new()),
        expected: Mutex::new(BTreeMap::new()),
        written: AtomicBool::new(false),
    })
}

fn log_config() -> LogConfig {
    LogConfig {
        inline_max_bytes: 512,
        segment_bytes: 16 * 1024,
        group_commit_max_delay: Duration::from_millis(1),
        group_commit_max_bytes: 16 * 1024,
    }
}

fn store_config() -> FragmentStoreConfig {
    FragmentStoreConfig {
        disk: 0,
        segment_bytes: 128 * 1024,
        group_commit_max_bytes: 64 * 1024,
        max_fragment_bytes: 1 << 20,
    }
}

fn index_config() -> IndexConfig {
    IndexConfig {
        checkpoint_interval: CHECKPOINT_INTERVAL,
        cache_bytes: 256 * 1024,
    }
}

fn replication_config() -> ReplicationConfig {
    ReplicationConfig::default()
}

/// The fixed topology of the six nodes, each a domain of its own, under
/// the default `[ec]` policy.
fn planner() -> FragmentPlanner {
    let candidates = (0..NODES).map(|n| Candidate {
        node: format!("n{n}").parse().expect("a valid node ID"),
        zone: None,
        rack: None,
        capacity_bytes: 1 << 40,
        state: NodeState::Live,
        shards: 0,
        primaries: 0,
    });
    let policy = GeometryPolicy::from_config(&EcConfig::default()).expect("the default policy");
    FragmentPlanner::new(Topology::new(FailureDomain::Node, candidates), policy)
}

/// The simulated time of day.
#[derive(Debug, Clone, Copy)]
struct SimWallClock;

impl WallClock for SimWallClock {
    fn now(&self) -> Duration {
        turmoil::since_epoch().unwrap_or_default()
    }
}

/// Opens one node's storage as its software does: the log and index of
/// its log disk, and the fragment store of its fragment disk.
async fn open_storage(
    slot: &NodeSlot,
    bug: Option<StoreBug>,
) -> Result<(Storage<SimMount>, FragmentStore<SimMount>), BoxError> {
    let clock = Arc::new(MonotonicClock::new());
    let pool = BlockingPool::inline("index");
    let mount = slot.log_disk.mount();
    let index = Index::open_sim(&mount, "index.redb", &index_config())?;
    let storage = storage::recover(
        vec![(Label::new("log")?, mount)],
        log_config(),
        clock as Arc<dyn Clock>,
        Arc::new(index),
        pool,
        slot.id.clone(),
    )
    .await?;
    let fragments = slot.fragment_disk.mount();
    let (store, _) = match bug {
        Some(bug) => FragmentStore::open_with_bug(fragments, store_config(), bug).await?,
        None => FragmentStore::open(fragments, store_config()).await?,
    };
    Ok((storage, store))
}

/// One life of node `index`, until it crashes or a checkpoint fails.
async fn life(world: &Arc<World>, index: usize) -> Result<(), BoxError> {
    let slot = &world.nodes[index];
    *lock(&slot.view) = None;
    let bug = world.run.store_bug.filter(|_| index != 0);
    let (storage, store) = open_storage(slot, bug).await?;
    let set = storage.shards.set().clone();
    set.reads().configure(ReadSettings {
        ttl: Duration::from_secs(30),
        release_delay: RELEASE_DELAY,
    });
    let fragments = FragmentServer::new(vec![store]);
    let transport = Transport::new(TurmoilNetwork, &slot.credentials);
    let clock = Arc::new(MonotonicClock::new());
    let replication = Replication::new(
        slot.id.clone(),
        set.clone(),
        transport.clone(),
        world.peers.clone(),
        clock as Arc<dyn Clock>,
        replication_config(),
    );
    let listener = transport
        .bind((std::net::Ipv4Addr::UNSPECIFIED, TRANSPORT_PORT).into())
        .await?;
    {
        let (replication, fragments) = (replication.clone(), fragments.clone());
        tokio::spawn(async move { serve(listener, replication, fragments).await });
    }
    let shard = if index < MEMBERS {
        Some(replication.open(&world.config).await?)
    } else {
        None
    };
    *lock(&slot.view) = Some(View {
        index: Arc::clone(&storage.index),
        shard: shard.clone(),
        fragments: fragments.clone(),
    });
    let checkpointer = Arc::clone(&storage.checkpointer);
    let checkpoints = tokio::spawn(async move { checkpointer.run(CHECKPOINT_INTERVAL).await });
    let settings = CompactionSettings {
        live_threshold: 0.5,
        unreferenced_ttl: Duration::from_secs(1),
    };
    let compactor = Compactor::new(set, settings, CompactionMetrics::default());
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(COMPACTION_INTERVAL).await;
            if let Err(error) = compactor.compact().await {
                tracing::debug!(%error, "compaction failed");
            }
        }
    });
    if let (0, Some(shard)) = (index, shard) {
        let client = FragmentClient::new(transport, world.peers.clone())
            .with_local(slot.id.clone(), fragments);
        tokio::spawn(primary(Arc::clone(world), shard, client));
    }
    let error = checkpoints.await;
    tracing::warn!(node = index, ?error, "a checkpoint failed; the node stops");
    Ok(())
}

/// Accepts links on the node's transport port: fragment writes go to the
/// fragment server, everything else to replication.
async fn serve(
    listener: Listener<TurmoilNetwork>,
    replication: Replication<TurmoilNetwork, SimMount>,
    fragments: FragmentServer<SimMount>,
) {
    loop {
        let Ok(incoming) = listener.accept().await else {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        };
        let (replication, fragments) = (replication.clone(), fragments.clone());
        tokio::spawn(async move {
            let Ok(connection) = incoming.handshake().await else {
                return;
            };
            let (mut receiver, sender) = connection.into_split();
            let first = tokio::time::timeout(Duration::from_secs(5), receiver.recv()).await;
            let Ok(Ok(Some(first))) = first else {
                return;
            };
            if first.header.kind == MessageKind::FragmentWrite {
                fragments.serve((receiver, sender), first).await;
            } else {
                replication.follow((receiver, sender), first).await;
            }
        });
    }
}

/// The primary's work: write the objects once, overwrite the first while
/// it is encoded if asked, encode, and write unreferenced extents once
/// every object is coded, so the segments holding their replicas stop
/// being the last.
async fn primary(
    world: Arc<World>,
    shard: Shard<SimMount>,
    writer: FragmentClient<TurmoilNetwork, SimMount>,
) {
    if !world.written.load(Ordering::SeqCst) {
        for (n, size) in world.sizes.iter().enumerate() {
            let data = bytes(world.seed, n as u64, *size);
            put(&world, &shard, &key(n), data).await;
        }
        world.written.store(true, Ordering::SeqCst);
    }
    let events = Arc::clone(&world);
    let encoder = Encoder::new(
        shard.clone(),
        Arc::new(writer),
        Arc::new(planner),
        Arc::new(SimWallClock),
        EncoderSettings {
            min_object_bytes: 1,
            stripe_data_bytes: world.run.stripe_data_bytes,
            after: Duration::ZERO,
            replans: 3,
            after_backup: false,
        },
    )
    .with_observer(Arc::new(move |event: &EncodeEvent| {
        lock(&events.events).push(event.clone());
    }));
    tokio::spawn(encoder.run(SCAN_INTERVAL));
    if world.run.overwrite {
        overwrite(&world, &shard).await;
    }
    // Once every object is coded on the primary, roll the bulk segment.
    loop {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let mut coded = true;
        for n in 0..world.sizes.len() {
            let entry = shard.entry(&key(n)).await.ok().flatten();
            coded &= entry
                .and_then(|e| e.object)
                .is_some_and(|object| object.coded.is_some());
        }
        if coded {
            let filler = Extent {
                key: "filler".to_owned(),
                offset: 0,
                data: Bytes::from(vec![0; 4000]),
            };
            for _ in 0..8 {
                let _ = shard.append_extent(filler.clone()).await;
            }
            return;
        }
    }
}

/// Overwrites the first object with as many new bytes once its encoding
/// has started a stripe.
async fn overwrite(world: &World, shard: &Shard<SimMount>) {
    loop {
        let started = lock(&world.events).iter().any(|event| {
            event.key == key(0) && matches!(event.step, EncodeStep::StripeStarted { .. })
        });
        if started {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let data = bytes(world.seed ^ 0xffff, 0, world.sizes[0]);
    put(world, shard, &key(0), data).await;
}

fn key(n: usize) -> String {
    format!("object-{n}")
}

/// Deterministic bytes for object `n`.
fn bytes(seed: u64, n: u64, len: usize) -> Vec<u8> {
    let mut state = (seed ^ n.wrapping_mul(0x9e37_79b9_7f4a_7c15)) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

/// Writes `data` as `key` in extents, retrying until the write commits,
/// and records it as the key's expected bytes.
async fn put(world: &World, shard: &Shard<SimMount>, key: &str, data: Vec<u8>) {
    loop {
        if let Ok(()) = try_put(shard, key, &data).await {
            lock(&world.expected).insert(key.to_owned(), data);
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn try_put(shard: &Shard<SimMount>, key: &str, data: &[u8]) -> Result<(), BoxError> {
    // The extents go out together, as a streamed body's do.
    let mut appends = tokio::task::JoinSet::new();
    for (n, chunk) in data.chunks(4000).enumerate() {
        let extent = Extent {
            key: key.to_owned(),
            offset: n as u64 * 4000,
            data: Bytes::copy_from_slice(chunk),
        };
        let shard = shard.clone();
        appends.spawn(async move { (n, shard.append_extent(extent).await) });
    }
    let mut extents = BTreeMap::new();
    while let Some(appended) = appends.join_next().await {
        let (n, extent) = appended?;
        extents.insert(n, extent?);
    }
    let extents: Vec<ExtentRef> = extents.into_values().collect();
    let now = u64::try_from(SimWallClock.now().as_millis()).unwrap_or(0);
    let digest = data
        .iter()
        .fold(0u64, |h, b| h.rotate_left(5) ^ u64::from(*b));
    let body = RecordBody::Put(Put {
        key: key.to_owned(),
        size: data.len() as u64,
        last_modified_ms: now,
        etag: ETag::new(format!("{digest:032x}"))?,
        inherited_identity: None,
        metadata: BTreeMap::new(),
        tags: BTreeMap::new(),
        checksums: BTreeMap::new(),
        copy_source: None,
        data: PutData::Extents(extents),
    });
    shard.commit(body).await?;
    Ok(())
}

/// The driver's state between steps.
struct Driver<'a> {
    world: &'a World,
    /// When each node that is down restarts.
    down: Vec<Option<Duration>>,
    /// Whether each node's planned power cut was handled.
    cut: Vec<bool>,
    crashes: Option<(CrashPoint, Vec<Crash>)>,
    /// How many events the crash point was looked for in.
    seen: usize,
    /// Crashes due at a time, with the node they hit.
    due: Vec<(Duration, usize, Crash)>,
    /// The highest commit watermark any replica has learned.
    committed: u64,
    report: CodingReport,
}

impl Driver<'_> {
    /// Restarts nodes, cuts power, and starts the crashes that are due.
    fn advance(&mut self, sim: &mut turmoil::Sim<'_>, now: Duration) {
        if let Some((point, _)) = &self.crashes {
            let point = *point;
            let events = lock(&self.world.events);
            let reached = (self.seen..events.len())
                .find(|end| point.reached(&events[..=*end]))
                .map(|end| events[end].clone());
            self.seen = events.len();
            drop(events);
            if let Some(event) = reached {
                let (_, crashes) = self.crashes.take().expect("crashes are planned");
                for crash in crashes {
                    for node in self.targets(crash.target) {
                        self.due.push((now + crash.delay, node, crash));
                    }
                }
                tracing::debug!(?event, "the crash point is reached");
            }
        }
        let mut due = Vec::new();
        self.due.retain(|(at, node, crash)| {
            let ready = *at <= now;
            if ready {
                due.push((*node, *crash));
            }
            !ready
        });
        for (node, crash) in due {
            let slot = &self.world.nodes[node];
            self.report.crashes.push((slot.id.to_string(), crash.kind));
            match crash.kind {
                CrashKind::Kill => self.crash(sim, now, node, false, crash.downtime),
                CrashKind::PowerLoss => self.crash(sim, now, node, true, crash.downtime),
                CrashKind::PowerAtNextSync => {
                    slot.power.cut_at_sync(slot.power.syncs(), SyncCut::Before);
                    self.cut[node] = false;
                }
            }
        }
        for node in 0..NODES {
            let slot = &self.world.nodes[node];
            if !self.cut[node] && slot.power.is_cut() {
                // The disks lost power inside a sync. The host stops a
                // moment later, so what it sent before the sync, such as
                // an early acknowledgement, still leaves; nothing it does
                // meanwhile reaches its disks.
                self.cut[node] = true;
                let stop = Crash {
                    delay: CUT_GRACE,
                    target: CrashTarget::Primary,
                    kind: CrashKind::PowerLoss,
                    downtime: SUPERVISOR_DELAY,
                };
                self.due.push((now + CUT_GRACE, node, stop));
            }
            match self.down[node] {
                Some(at) if at <= now => {
                    self.down[node] = None;
                    sim.bounce(slot.id.as_str());
                }
                Some(_) => {}
                None if !sim.is_host_running(slot.id.as_str()) => {
                    // The node stopped by itself: a supervisor restarts it.
                    self.crash(sim, now, node, true, SUPERVISOR_DELAY);
                }
                None => {}
            }
        }
    }

    /// The nodes a crash of `target` hits now.
    fn targets(&self, target: CrashTarget) -> Vec<usize> {
        let stripe_nodes = || {
            lock(&self.world.events)
                .iter()
                .rev()
                .find_map(|event| match &event.step {
                    EncodeStep::StripeStarted { nodes, .. } => Some(nodes.clone()),
                    _ => None,
                })
                .unwrap_or_default()
        };
        let position = |node: &NodeId| {
            self.world
                .nodes
                .iter()
                .position(|slot| slot.id == *node)
                .expect("a node of the cluster")
        };
        match target {
            CrashTarget::Primary => vec![0],
            CrashTarget::Member(n) => vec![n],
            CrashTarget::Holder => stripe_nodes()
                .iter()
                .map(position)
                .find(|n| *n >= MEMBERS)
                .into_iter()
                .collect(),
            CrashTarget::Holders => stripe_nodes()
                .iter()
                .map(position)
                .filter(|n| *n != 0)
                .collect(),
        }
    }

    fn crash(
        &mut self,
        sim: &mut turmoil::Sim<'_>,
        now: Duration,
        node: usize,
        power_loss: bool,
        downtime: Duration,
    ) {
        if self.down[node].is_some() {
            return;
        }
        let slot = &self.world.nodes[node];
        if power_loss {
            slot.log_disk.crash();
            slot.fragment_disk.crash();
        } else {
            slot.log_disk.kill();
            slot.fragment_disk.kill();
        }
        *lock(&slot.view) = None;
        sim.crash(slot.id.as_str());
        self.down[node] = Some(now + downtime);
    }

    /// The views of the nodes that are up and have recovered.
    fn views(&self) -> Vec<Option<View>> {
        self.world
            .nodes
            .iter()
            .zip(&self.down)
            .map(|(slot, down)| match down {
                Some(_) => None,
                None => lock(&slot.view).clone(),
            })
            .collect()
    }

    /// Checks every object, as the module documentation says.
    fn check(&mut self) -> Result<(), String> {
        let views = self.views();
        for view in views.iter().flatten() {
            if let Some(through) = view.shard.as_ref().and_then(Shard::replicated_through) {
                self.committed = self.committed.max(through.get());
            }
        }
        let keys: Vec<String> = lock(&self.world.expected).keys().cloned().collect();
        for key in keys {
            self.check_key(&views, &key)
                .map_err(|e| format!("{key}: {e}"))?;
        }
        Ok(())
    }

    fn check_key(&self, views: &[Option<View>], key: &str) -> Result<(), String> {
        let shard = &self.world.shard;
        let mut replica = false;
        let mut layouts = Vec::new();
        for (member, view) in views.iter().enumerate().take(MEMBERS) {
            let Some(view) = view else {
                // Down, or not yet recovered: it holds what it held.
                replica = true;
                continue;
            };
            let reader = view.index.read().map_err(|e| e.to_string())?;
            let Some(entry) = reader.entry(shard, key).map_err(|e| e.to_string())? else {
                continue;
            };
            let located = locates(&reader, shard, &entry)?;
            replica |= located;
            if let Some(coded) = entry.object.and_then(|object| object.coded) {
                let at = coded.publish.seq.get();
                if !located && at > self.committed {
                    return Err(format!(
                        "member n{member} dropped the replica before the EC_PUBLISH at {} \
                         committed (watermark {})",
                        coded.publish, self.committed
                    ));
                }
                layouts.push(coded.stripes);
            }
        }
        if replica {
            return Ok(());
        }
        let present = |node: &NodeId, id| {
            let n = self.world.nodes.iter().position(|slot| slot.id == *node);
            match n.map(|n| &views[n]) {
                Some(Some(view)) => view.fragments.stores()[0].len(id).is_some(),
                // Down: it holds what it held.
                Some(None) => true,
                None => false,
            }
        };
        let readable = layouts.iter().any(|stripes| {
            stripes.iter().all(|stripe| {
                let held = stripe
                    .fragments()
                    .iter()
                    .filter(|location| present(&location.node, location.fragment))
                    .count();
                held >= stripe.geometry().data_fragments()
            })
        });
        if readable {
            Ok(())
        } else {
            Err("readable neither from a replica nor from k fragments of each stripe".to_owned())
        }
    }

    /// Whether every object is coded on the primary and no member that is
    /// up locates its replicated bytes, with every node up.
    fn settled(&self) -> bool {
        if !self.world.written.load(Ordering::SeqCst)
            || self.down.iter().any(Option::is_some)
            || self.crashes.is_some()
            || !self.due.is_empty()
        {
            return false;
        }
        let views = self.views();
        if views.iter().any(Option::is_none) {
            return false;
        }
        let shard = &self.world.shard;
        let keys: Vec<String> = lock(&self.world.expected).keys().cloned().collect();
        views.iter().take(MEMBERS).flatten().all(|view| {
            let Ok(reader) = view.index.read() else {
                return false;
            };
            keys.iter().all(|key| {
                let Ok(Some(entry)) = reader.entry(shard, key) else {
                    return false;
                };
                let coded = entry.object.as_ref().is_some_and(|o| o.coded.is_some());
                coded && locates(&reader, shard, &entry) == Ok(false)
            })
        })
    }
}

/// Whether this replica locates every byte of `entry`'s version.
fn locates(reader: &IndexReader, shard: &ShardRef, entry: &Entry) -> Result<bool, String> {
    let Some(object) = &entry.object else {
        return Ok(false);
    };
    let Some(layout) =
        skys3_shard::reads::layout(reader, shard, object).map_err(|e| e.to_string())?
    else {
        return Ok(false);
    };
    for extent in &layout {
        if reader
            .location(shard, extent.position)
            .map_err(|e| e.to_string())?
            .is_none()
        {
            return Ok(false);
        }
    }
    Ok(!layout.is_empty())
}

/// Recovers every node outside the simulation and reads every object
/// back through each member's entry.
fn final_check(world: &World) -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .map_err(|e| e.to_string())?;
    runtime.block_on(async {
        let mut storages = Vec::new();
        let mut stores = BTreeMap::new();
        for slot in &world.nodes {
            let (storage, store) = open_storage(slot, None)
                .await
                .map_err(|e| format!("{} does not recover: {e}", slot.id))?;
            storages.push(storage);
            stores.insert(slot.id.clone(), store);
        }
        let expected = lock(&world.expected).clone();
        for (key, data) in &expected {
            for (member, storage) in storages.iter().enumerate().take(MEMBERS) {
                let read = read_back(world, storage, &stores, key)
                    .await
                    .map_err(|e| format!("{key} on n{member}: {e}"))?;
                if read != *data {
                    return Err(format!(
                        "{key} on n{member} reads {} bytes that are not the {} written",
                        read.len(),
                        data.len()
                    ));
                }
            }
        }
        Ok(())
    })
}

/// The bytes of `key` as `storage`'s entry gives them.
async fn read_back(
    world: &World,
    storage: &Storage<SimMount>,
    stores: &BTreeMap<NodeId, FragmentStore<SimMount>>,
    key: &str,
) -> Result<Vec<u8>, String> {
    let shard = &world.shard;
    let reader = storage.index.read().map_err(|e| e.to_string())?;
    let entry = reader
        .entry(shard, key)
        .map_err(|e| e.to_string())?
        .ok_or("no entry")?;
    let object = entry.object.clone().ok_or("deleted")?;
    if let Some(coded) = &object.coded {
        let mut data = Vec::new();
        for stripe in &coded.stripes {
            let mut slots: Vec<Option<Bytes>> = Vec::new();
            for (index, location) in stripe.fragments().iter().enumerate() {
                // A fragment counts only if it is the one the layout names:
                // readable, verified, and with a header of this stripe.
                let store = &stores[&location.node];
                let len = store.len(location.fragment).unwrap_or(0);
                let read = store.read(location.fragment, 0..len).await.ok();
                slots.push(
                    read.filter(|read| {
                        let header = &read.header;
                        header.key == key
                            && header.version == entry.version
                            && header.stripe.number == stripe.number()
                            && usize::from(header.index) == index
                    })
                    .map(|read| read.data),
                );
            }
            let slots: Vec<Option<&[u8]>> = slots.iter().map(|s| s.as_deref()).collect();
            let codec = codec(stripe.codec()).map_err(|e| e.to_string())?;
            let bytes = codec
                .decode(stripe.geometry(), stripe.data_len(), &slots)
                .map_err(|e| format!("stripe {} does not decode: {e}", stripe.number()))?;
            data.extend_from_slice(&bytes);
        }
        return Ok(data);
    }
    let layout = skys3_shard::reads::layout(&reader, shard, &object)
        .map_err(|e| e.to_string())?
        .ok_or("no local bytes")?;
    drop(reader);
    let log = storage.logs.values().next().ok_or("no log")?;
    let mut data = Vec::new();
    for ExtentRef { position, .. } in layout {
        let location = storage
            .index
            .read()
            .and_then(|reader| reader.location(shard, position))
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("the payload at {position} is gone"))?;
        let record = log.read(location).await.map_err(|e| e.to_string())?;
        match record.body {
            RecordBody::Extent(extent) => data.extend_from_slice(&extent.data),
            RecordBody::Put(Put {
                data: PutData::Inline(bytes),
                ..
            }) => data.extend_from_slice(&bytes),
            _ => return Err(format!("the record at {position} holds no payload")),
        }
    }
    Ok(data)
}

/// Seeds a shard bug for the rest of this thread's runs, or removes it.
pub fn seed_shard_bug(bug: Option<SeededBug>) {
    skys3_shard::seed_bug(bug);
}
