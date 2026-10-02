//! Building a simulated cluster, driving it through a workload and a
//! fault plan, and checking the outcome.

use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use skys3_control::faults::{FaultRates, FaultyStore};
use skys3_control::{
    Expected, ProposalIds, ProposalOutcome, RetryPolicy, S3ControlStore, S3StoreConfig, TypedKey,
    bootstrap, bump_generation, propose_document,
};
use skys3_gateway::{GatewayConfig, ShardRef};
use skys3_index::Index;
use skys3_io::{Drift, MonotonicClock, SimDiskFaults};
use skys3_log::LogConfig;
use skys3_sim::check::{Survivors, Violation, check_durable, check_linearizable};
use skys3_sim::history::{History, Operation, Outcome};
use skys3_sim::s3::{SimS3Config, SimS3Faults};
use skys3_sim::{SimContext, SimS3};
use skys3_types::{
    BucketDocument, BucketId, BucketMode, BucketName, ClusterId, Epoch, Label, NodeAddress, NodeId,
    ShardConfig, ShardCount,
};

use crate::faults::{Endpoint, Fault, FaultPlan};
use crate::node::{
    self, BoxError, ControlHandle, INDEX_FILE, LocalServices, NodeServices, NodeSettings, NodeSlot,
    Shared, TRANSPORT_PORT,
};
use crate::pki::Pki;
use crate::workload::{Client, Routes, Workload};

/// The shape of a simulated cluster.
#[derive(Clone, Debug, PartialEq)]
pub struct ClusterConfig {
    /// Nodes.
    pub nodes: usize,
    /// Disks per node. The first also holds the index.
    pub disks_per_node: usize,
    /// Buckets, all `local`.
    pub buckets: usize,
    /// Shards per bucket.
    pub shards_per_bucket: u32,
    /// Members of each shard's configuration in the static placement. The
    /// M1 node keeps one copy, on the primary, whatever this says.
    pub replicas: usize,
    /// The bound `ρ` on each node's clock drift; each life of a node
    /// draws its drift within it.
    pub drift: Drift,
    /// Faults every disk injects throughout, such as torn writes.
    pub disk_faults: SimDiskFaults,
    /// Faults of every node's control-store requests throughout.
    pub control_rates: FaultRates,
    /// The shortest and longest latency of each network message.
    pub latency: (Duration, Duration),
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            nodes: 3,
            disks_per_node: 2,
            buckets: 2,
            shards_per_bucket: 4,
            replicas: 1,
            drift: Drift::from_ppm(1_000).expect("the drift is valid"),
            disk_faults: SimDiskFaults {
                torn_write_probability: 0.5,
                ..SimDiskFaults::default()
            },
            control_rates: FaultRates {
                lose_response: 0.02,
                conflict: 0.02,
                unavailable: 0.02,
                max_delay: Duration::from_millis(5),
                ..FaultRates::default()
            },
            latency: (Duration::from_millis(1), Duration::from_millis(5)),
        }
    }
}

/// What the cluster looks like to an [`Invariant`] after each step.
#[derive(Debug)]
pub struct View<'a, S> {
    /// Simulated time since the start.
    pub elapsed: Duration,
    /// The node services, which hold whatever state they expose.
    pub services: &'a S,
    /// Whether each node's software is running.
    pub up: &'a [bool],
    /// The history so far.
    pub history: &'a History,
}

/// A property checked after every step of the simulation: the extension
/// point for protocol invariants, such as one committing primary per epoch
/// (plan M2-07). Closures of the right shape are invariants.
pub trait Invariant<S>: 'static {
    /// Checks the property, or says what is wrong.
    ///
    /// # Errors
    ///
    /// The violation, which fails the run.
    fn check(&mut self, view: &View<'_, S>) -> Result<(), String>;
}

impl<S, F> Invariant<S> for F
where
    F: FnMut(&View<'_, S>) -> Result<(), String> + 'static,
{
    fn check(&mut self, view: &View<'_, S>) -> Result<(), String> {
        self(view)
    }
}

/// Counts from one run.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Report {
    /// Every operation, in the order they were called.
    pub history: Vec<Operation>,
    /// Faults that started.
    pub faults: usize,
    /// Node lives started, restarts included.
    pub lives: u64,
    /// Power-loss restarts the driver forced on nodes whose sync failed
    /// and that had not been restarted otherwise.
    pub fences: usize,
}

impl Report {
    /// The operations that ended with `outcome`'s kind.
    #[must_use]
    pub fn count(&self, matches: impl Fn(&Outcome) -> bool) -> usize {
        self.history
            .iter()
            .filter(|op| matches(&op.outcome))
            .count()
    }
}

/// Why a run failed.
#[derive(Debug)]
pub enum RunError {
    /// The simulation itself failed: a client saw an answer it does not
    /// expect, or an invariant broke.
    Simulation(String),
    /// A checker found the history wrong.
    Check(Violation),
    /// The cluster could not be set up or recovered.
    Setup(BoxError),
}

impl std::fmt::Display for RunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunError::Simulation(error) => write!(f, "the simulation failed: {error}"),
            RunError::Check(violation) => write!(f, "a checker failed: {violation}"),
            RunError::Setup(error) => write!(f, "the cluster failed outside the workload: {error}"),
        }
    }
}

impl Error for RunError {}

impl From<BoxError> for RunError {
    fn from(error: BoxError) -> Self {
        RunError::Setup(error)
    }
}

/// A simulated cluster: nodes running `S` on simulated disks, a shared
/// control store and remote store, all in one `turmoil` simulation.
pub struct Cluster<S: NodeServices = LocalServices> {
    config: ClusterConfig,
    services: S,
    invariants: Vec<Box<dyn Invariant<S>>>,
}

impl Cluster<LocalServices> {
    /// A cluster of M1 nodes.
    #[must_use]
    pub fn new(config: ClusterConfig) -> Self {
        Self::with_services(config, LocalServices)
    }
}

/// The cluster ID of every simulated cluster.
const CLUSTER: &str = "sim";
/// Simulated time limit of a run.
const TIME_LIMIT: Duration = Duration::from_secs(3600);
/// How long the nodes may take to start serving.
const STARTUP_LIMIT: Duration = Duration::from_secs(60);
/// How long a node that stopped by itself stays down before it restarts.
const SUPERVISOR_DELAY: Duration = Duration::from_millis(500);
/// How long after a failed sync the node is restarted with a power loss.
const FENCE_DELAY: Duration = Duration::from_secs(1);
/// Attempts of each final read.
const FINAL_READ_ATTEMPTS: usize = 120;

/// A change the driver makes when a fault heals.
#[derive(Debug, Clone)]
enum Heal {
    Repair(Endpoint, Endpoint),
    Release(Endpoint, Endpoint),
    /// The end of a window of [`InForce`].
    End(Window),
    /// Not a heal: the power-loss restart a failed sync needs, for the
    /// node's process `life` (see `Driver::life`).
    Fence {
        node: usize,
        life: u64,
    },
}

/// A window of a fault whose effect overlaps others of its kind, which
/// [`InForce`] combines.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Window {
    Loss(u64),
    Outage(u64),
    Latency(u64),
    LostResponses(u64),
}

/// The faults in force whose windows may overlap: message loss and the
/// control store's. Each window ends on its own; what is in force is
/// recomputed from the windows still open.
#[derive(Debug, Default)]
struct InForce {
    next: u64,
    loss: Vec<(u64, f64)>,
    outages: Vec<u64>,
    latencies: Vec<(u64, Duration)>,
    lost: Vec<(u64, f64)>,
}

impl InForce {
    /// Opens the window `fault` starts, if it has one.
    fn open(&mut self, fault: &Fault) -> Option<Window> {
        let id = self.next;
        let window = match *fault {
            Fault::MessageLoss { rate, .. } => {
                self.loss.push((id, rate));
                Window::Loss(id)
            }
            Fault::ControlOutage { .. } => {
                self.outages.push(id);
                Window::Outage(id)
            }
            Fault::ControlLatency { min, .. } => {
                self.latencies.push((id, min));
                Window::Latency(id)
            }
            Fault::LostCasResponses { probability, .. } => {
                self.lost.push((id, probability));
                Window::LostResponses(id)
            }
            _ => return None,
        };
        self.next += 1;
        Some(window)
    }

    /// Closes `window`, and leaves every other open.
    fn close(&mut self, window: Window) {
        match window {
            Window::Loss(id) => self.loss.retain(|(open, _)| *open != id),
            Window::Outage(id) => self.outages.retain(|open| *open != id),
            Window::Latency(id) => self.latencies.retain(|(open, _)| *open != id),
            Window::LostResponses(id) => self.lost.retain(|(open, _)| *open != id),
        }
    }

    /// The message loss rate in force: the highest of the open windows'.
    fn loss_rate(&self) -> f64 {
        self.loss.iter().map(|(_, rate)| *rate).fold(0.0, f64::max)
    }

    /// The control store's fault rates in force over `base`.
    fn control_rates(&self, base: FaultRates) -> FaultRates {
        let mut rates = base;
        if !self.outages.is_empty() {
            rates.unavailable = 1.0;
        }
        if let Some(lost) = self.lost.iter().map(|(_, p)| *p).reduce(f64::max) {
            rates.lose_response = lost;
        }
        rates
    }

    /// The control bucket's faults in force over `base`.
    fn control_faults(&self, base: &SimS3Faults) -> SimS3Faults {
        let mut faults = base.clone();
        if let Some(min) = self.latencies.iter().map(|(_, min)| *min).max() {
            faults.min_delay = min;
            faults.max_delay = min * 4;
        }
        faults
    }
}

impl<S: NodeServices> Cluster<S> {
    /// A cluster whose nodes run `services`.
    #[must_use]
    pub fn with_services(config: ClusterConfig, services: S) -> Self {
        Self {
            config,
            services,
            invariants: Vec::new(),
        }
    }

    /// Adds an invariant, checked after every step.
    #[must_use]
    pub fn invariant(mut self, invariant: impl Invariant<S>) -> Self {
        self.invariants.push(Box::new(invariant));
        self
    }

    /// Runs `workload` under `plan` with everything random drawn from
    /// `context`, heals every fault, reads every key back, cuts power to
    /// every node, and checks the history: linearizable per key, and every
    /// acknowledged write present on a surviving member after recovery.
    ///
    /// # Errors
    ///
    /// [`RunError`] for a failed simulation, setup, or check.
    pub fn run(
        mut self,
        context: &mut SimContext,
        workload: &Workload,
        plan: &FaultPlan,
    ) -> Result<Report, RunError> {
        let world = World::build(context, &self.config, self.services.clone())?;
        let history = History::new();
        let mut builder = context.builder();
        builder
            .simulation_duration(TIME_LIMIT)
            .min_message_latency(self.config.latency.0)
            .max_message_latency(self.config.latency.1)
            .repair_rate(0.05);
        let mut sim = builder.build();
        for slot in &world.slots {
            let (shared, slot) = (Arc::clone(&world.shared), Arc::clone(slot));
            sim.host(slot.host.clone(), move || {
                let (shared, slot) = (Arc::clone(&shared), Arc::clone(&slot));
                async move {
                    let node = slot.id.clone();
                    if let Err(error) = node::run(shared, slot).await {
                        tracing::warn!(%node, %error, "the node stopped")
                    }
                    Ok(())
                }
            });
        }
        let mut driver = Driver {
            world: &world,
            plan: plan.faults().iter().cloned().collect(),
            origin: None,
            heals: Vec::new(),
            down: vec![None; world.slots.len()],
            fenced: vec![false; world.slots.len()],
            life: vec![0; world.slots.len()],
            in_force: InForce::default(),
            faults: 0,
            fences: 0,
        };
        // The clients start once every node serves, and the faults with
        // them.
        while !world
            .slots
            .iter()
            .all(|slot| slot.ready.load(Ordering::SeqCst))
        {
            if sim.elapsed() > STARTUP_LIMIT {
                return Err(RunError::Simulation("the nodes did not start".to_owned()));
            }
            driver.advance(&mut sim, false);
            sim.step().map_err(failed)?;
        }
        driver.origin = Some(sim.elapsed());
        for index in 0..workload.clients {
            let client = Client::new(
                client_host(index),
                world.routes.clone(),
                history.clone(),
                workload.clone(),
                context.fork_seed(),
            );
            sim.client(client_host(index), client.run());
        }
        self.drive(&mut sim, &mut driver, &history, false)?;

        // Heal everything, and read every key back.
        driver.advance(&mut sim, true);
        let reader = Client::new(
            "verifier".to_owned(),
            world.routes.clone(),
            history.clone(),
            workload.clone(),
            context.fork_seed(),
        );
        sim.client("verifier", reader.read_all(FINAL_READ_ATTEMPTS));
        self.drive(&mut sim, &mut driver, &history, true)?;

        // Power loss everywhere: what survives is what the checkers see.
        for slot in &world.slots {
            slot.stop_disks(true);
            sim.crash(slot.host.as_str());
        }
        let operations = history.operations();
        let survivors = world.survivors(workload.keys)?;
        check_linearizable(&operations).map_err(RunError::Check)?;
        check_durable(&operations, &survivors).map_err(RunError::Check)?;
        Ok(Report {
            history: operations,
            faults: driver.faults,
            lives: world.slots.iter().map(|slot| slot.lives()).sum(),
            fences: driver.fences,
        })
    }

    /// Steps the simulation, with faults and heals, until every client is
    /// done, checking the invariants after every step.
    fn drive(
        &mut self,
        sim: &mut turmoil::Sim<'_>,
        driver: &mut Driver<'_, S>,
        history: &History,
        heal_all: bool,
    ) -> Result<(), RunError> {
        loop {
            driver.advance(sim, heal_all);
            self.check_invariants(sim, driver, history)?;
            if sim.step().map_err(failed)? {
                return Ok(());
            }
        }
    }

    fn check_invariants(
        &mut self,
        sim: &mut turmoil::Sim<'_>,
        driver: &Driver<'_, S>,
        history: &History,
    ) -> Result<(), RunError> {
        if self.invariants.is_empty() {
            return Ok(());
        }
        let up: Vec<bool> = driver
            .world
            .slots
            .iter()
            .map(|slot| sim.is_host_running(slot.host.as_str()))
            .collect();
        let view = View {
            elapsed: sim.elapsed(),
            services: &self.services,
            up: &up,
            history,
        };
        for invariant in &mut self.invariants {
            invariant
                .check(&view)
                .map_err(|error| RunError::Simulation(format!("an invariant failed: {error}")))?;
        }
        Ok(())
    }
}

/// A failed simulation: a host's error, or `turmoil`'s own.
fn failed(error: Box<dyn Error>) -> RunError {
    RunError::Simulation(error.to_string())
}

fn client_host(index: usize) -> String {
    format!("client-{index}")
}

/// Everything a run shares between the driver and the hosts.
struct World<S> {
    slots: Vec<Arc<NodeSlot>>,
    shared: Arc<Shared<S>>,
    routes: Routes,
    control: SimS3,
    base_control: SimS3Faults,
    rates: FaultRates,
}

impl<S: NodeServices> World<S> {
    /// Creates the stores, the disks, and the credentials, and writes the
    /// cluster's registers: `cluster.json`, every bucket, and the static
    /// placement of every shard.
    fn build(
        context: &mut SimContext,
        config: &ClusterConfig,
        services: S,
    ) -> Result<Self, BoxError> {
        let cluster = ClusterId::new(CLUSTER)?;
        let control = context.s3(SimS3Config::default());
        let remote = context.s3(SimS3Config::default());
        let settings = settings(&cluster)?;
        let store = S3ControlStore::new(
            control.clone(),
            S3StoreConfig {
                prefix: "control/".to_owned(),
                poll_interval: settings.poll_interval,
            },
        )?;
        let node_ids: Vec<NodeId> = (1..=config.nodes)
            .map(|n| NodeId::new(format!("node-{n}")))
            .collect::<Result<_, _>>()?;
        let mut ids = ProposalIds::seeded(context.fork_seed());
        let (buckets, placement) = registers(config, &node_ids, &mut ids)?;
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()?
            .block_on(write_registers(
                &store, &cluster, &buckets, &placement, &mut ids,
            ))?;

        let pki = Pki::new()?;
        let mut slots = Vec::new();
        for (index, id) in node_ids.iter().enumerate() {
            let disks = (0..config.disks_per_node)
                .map(|n| {
                    let disk = context.disk_with_faults(config.disk_faults.clone());
                    Ok((Label::new(format!("disk-{n}"))?, disk))
                })
                .collect::<Result<Vec<_>, BoxError>>()?;
            let handle: ControlHandle =
                FaultyStore::seeded(store.clone(), context.fork_seed(), config.control_rates);
            let credentials = pki.node(&cluster, id)?;
            let seed = context.fork_seed();
            slots.push(Arc::new(NodeSlot::new(
                id.clone(),
                index,
                disks,
                handle,
                credentials,
                seed,
            )));
        }
        let peers = node_ids
            .iter()
            .map(|id| Ok((id.clone(), format!("{id}:{TRANSPORT_PORT}").parse()?)))
            .collect::<Result<BTreeMap<NodeId, NodeAddress>, BoxError>>()?;
        let placement = Arc::new(placement);
        let settings = NodeSettings {
            drift: config.drift,
            ..settings
        };
        Ok(Self {
            slots,
            shared: Arc::new(Shared {
                settings,
                services,
                peers,
                placement: Arc::clone(&placement),
                remote,
            }),
            routes: Routes { buckets, placement },
            base_control: control.faults(),
            control,
            rates: config.control_rates,
        })
    }

    /// Recovers every node from its disks, outside the simulation, and
    /// reads each key's value on every member of its shard.
    fn survivors(&self, keys: usize) -> Result<BTreeMap<String, Survivors>, BoxError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()?;
        let settings = &self.shared.settings;
        let mut indexes = BTreeMap::new();
        for slot in &self.slots {
            let mounts: Vec<_> = slot
                .disks
                .iter()
                .map(|(label, disk)| (label.clone(), disk.mount()))
                .collect();
            let index = Index::open_sim(&mounts[0].1, INDEX_FILE, &node::index_config(settings))?;
            let storage = runtime.block_on(skys3::storage::recover(
                mounts,
                settings.log.clone(),
                Arc::new(MonotonicClock::new()),
                Arc::new(index),
                skys3_io::BlockingPool::inline("recovery"),
                slot.id.clone(),
            ))?;
            indexes.insert(slot.id.clone(), storage.index);
        }
        let mut survivors = BTreeMap::new();
        for (name, bucket, key) in self.routes.keys(keys) {
            let shard = ShardRef::for_key(&bucket, &key);
            let members = &self.routes.placement[&shard].members;
            let mut copies = Vec::new();
            for member in members {
                let entry = indexes[member].read()?.entry(&(&shard).into(), &key)?;
                copies.push(
                    entry
                        .and_then(|entry| entry.object)
                        .map(|o| o.local_etag.to_string()),
                );
            }
            let survivor = Survivors {
                copies,
                ..Survivors::default()
            };
            survivors.insert(name, survivor);
        }
        Ok(survivors)
    }
}

/// The settings every simulated node runs with: small log segments and
/// inline bodies, so a short workload exercises extents and several
/// segments, and frequent checkpoints.
fn settings(cluster: &ClusterId) -> Result<NodeSettings, BoxError> {
    let config: skys3_config::Config = format!(
        "[cluster]\ncluster_id = \"{cluster}\"\n\
         [control_store]\netcd_endpoints = [\"https://etcd.sim.internal:2379\"]\n"
    )
    .parse()?;
    let mut gateway = GatewayConfig::new(&config);
    gateway.inline_max_bytes = 512;
    gateway.extent_bytes = 512;
    gateway.retry = RetryPolicy {
        max_attempts: 5,
        initial_backoff: Duration::from_millis(20),
        max_backoff: Duration::from_millis(200),
    };
    Ok(NodeSettings {
        cluster: cluster.clone(),
        log: LogConfig {
            inline_max_bytes: 512,
            segment_bytes: 16 * 1024,
            group_commit_max_delay: Duration::from_millis(1),
            group_commit_max_bytes: 16 * 1024,
        },
        retry: gateway.retry,
        gateway,
        checkpoint_interval: Duration::from_secs(1),
        drift: Drift::NONE,
        poll_interval: Duration::from_millis(500),
    })
}

/// The bucket documents and the static placement: shard `s` of bucket `b`
/// has its primary on node `(b × shards + s) mod nodes`, followed by the
/// next nodes as members.
fn registers(
    config: &ClusterConfig,
    nodes: &[NodeId],
    ids: &mut ProposalIds,
) -> Result<(Vec<BucketDocument>, BTreeMap<ShardRef, ShardConfig>), BoxError> {
    let shards = ShardCount::new(config.shards_per_bucket)?;
    let replicas = config.replicas.clamp(1, nodes.len());
    let mut buckets = Vec::new();
    let mut placement = BTreeMap::new();
    for b in 0..config.buckets {
        let bucket = BucketDocument {
            bucket_id: BucketId::new(format!("b-sim{b}"))?,
            name: BucketName::new(format!("bucket-{b}"))?,
            mode: BucketMode::Local,
            shards,
            replicas: u8::try_from(replicas)?,
            min_write_replicas: 1,
            clean_copies: 1,
            target: None,
            created_unix_ms: 0,
            proposal_id: ids.next_id(),
        };
        for shard in ShardRef::all(&bucket) {
            let first =
                b * usize::try_from(config.shards_per_bucket)? + usize::from(shard.shard.get());
            let members: Vec<NodeId> = (0..replicas)
                .map(|n| nodes[(first + n) % nodes.len()].clone())
                .collect();
            let shard_config = ShardConfig {
                bucket_id: bucket.bucket_id.clone(),
                shard: shard.shard,
                epoch: Epoch::new(1),
                primary: members[0].clone(),
                members,
                learners: Vec::new(),
                min_write_replicas: 1,
                replicas: u8::try_from(replicas)?,
                proposal_id: ids.next_id(),
            };
            placement.insert(shard, shard_config);
        }
        buckets.push(bucket);
    }
    Ok((buckets, placement))
}

/// Writes `cluster.json`, every bucket register, and every shard register,
/// and announces them.
async fn write_registers(
    store: &S3ControlStore<SimS3>,
    cluster: &ClusterId,
    buckets: &[BucketDocument],
    placement: &BTreeMap<ShardRef, ShardConfig>,
    ids: &mut ProposalIds,
) -> Result<(), BoxError> {
    let retry = RetryPolicy::default();
    bootstrap(store, cluster, ids.next_id(), &retry).await?;
    for bucket in buckets {
        let key = TypedKey::bucket(&bucket.name);
        accepted(propose_document(store, &key, Expected::Absent, bucket, &retry).await?)?;
    }
    for (shard, config) in placement {
        let key = TypedKey::shard(&shard.bucket, shard.shard);
        accepted(propose_document(store, &key, Expected::Absent, config, &retry).await?)?;
    }
    bump_generation(store, cluster, ids, &retry).await?;
    Ok(())
}

fn accepted(outcome: ProposalOutcome) -> Result<(), BoxError> {
    match outcome {
        ProposalOutcome::Accepted(_) => Ok(()),
        ProposalOutcome::Rejected => Err("a register exists in a fresh control store".into()),
    }
}

/// Applies faults and heals as simulated time passes, and restarts nodes
/// that stopped.
struct Driver<'a, S> {
    world: &'a World<S>,
    plan: std::collections::VecDeque<crate::faults::ScheduledFault>,
    /// When the workload started, which the plan's times count from.
    origin: Option<Duration>,
    heals: Vec<(Duration, Heal)>,
    /// When each node that is down restarts.
    down: Vec<Option<Duration>>,
    /// Nodes with a failed sync, whose next restart must be a power loss.
    fenced: Vec<bool>,
    /// Which process of each node runs, or runs next if the node is down:
    /// bumped each time a process ends, by a crash or by itself.
    life: Vec<u64>,
    in_force: InForce,
    faults: usize,
    fences: usize,
}

impl<S: NodeServices> Driver<'_, S> {
    fn host(&self, endpoint: Endpoint) -> String {
        match endpoint {
            Endpoint::Node(index) => self.world.slots[index].host.clone(),
            Endpoint::Client(index) => client_host(index),
        }
    }

    /// Starts due faults and heals due ones; with `heal_all`, starts no
    /// more faults and heals everything now.
    fn advance(&mut self, sim: &mut turmoil::Sim<'_>, heal_all: bool) {
        let now = sim.elapsed();
        if heal_all {
            self.plan.clear();
        }
        let origin = self.origin.unwrap_or(Duration::MAX);
        while self
            .plan
            .front()
            .is_some_and(|next| origin.saturating_add(next.at) <= now)
        {
            let scheduled = self.plan.pop_front().expect("a fault is due");
            self.start(sim, now, scheduled.fault);
        }
        let mut due = Vec::new();
        self.heals.retain(|(at, heal)| {
            let ready = heal_all || *at <= now;
            if ready {
                due.push(heal.clone());
            }
            !ready
        });
        for heal in due {
            self.heal(sim, now, heal, heal_all);
        }
        for index in 0..self.world.slots.len() {
            let host = self.world.slots[index].host.as_str();
            match self.down[index] {
                Some(at) if heal_all || at <= now => {
                    self.down[index] = None;
                    sim.bounce(host);
                }
                Some(_) => {}
                None if !sim.is_host_running(host) => {
                    // The node stopped by itself, as a failed start or
                    // checkpoint stops it: a supervisor restarts it, after
                    // a power loss in case a disk went out of service.
                    self.world.slots[index].stop_disks(true);
                    self.fenced[index] = false;
                    self.life[index] += 1;
                    self.down[index] = Some(now + SUPERVISOR_DELAY);
                }
                None => {}
            }
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
        let power_loss = power_loss || std::mem::take(&mut self.fenced[node]);
        let slot = &self.world.slots[node];
        slot.stop_disks(power_loss);
        sim.crash(slot.host.as_str());
        self.life[node] += 1;
        self.down[node] = Some(now + downtime);
    }

    fn start(&mut self, sim: &mut turmoil::Sim<'_>, now: Duration, fault: Fault) {
        self.faults += 1;
        let until = now + fault.duration().unwrap_or_default();
        if let Some(window) = self.in_force.open(&fault) {
            self.heals.push((until, Heal::End(window)));
            self.apply_in_force(sim);
            return;
        }
        match fault {
            Fault::Crash {
                node,
                power_loss,
                downtime,
            } => self.crash(sim, now, node, power_loss, downtime),
            Fault::Partition { a, b, .. } => {
                sim.partition(self.host(a).as_str(), self.host(b).as_str());
                self.heals.push((until, Heal::Repair(a, b)));
            }
            Fault::Hold { a, b, .. } => {
                sim.hold(self.host(a).as_str(), self.host(b).as_str());
                self.heals.push((until, Heal::Release(a, b)));
            }
            Fault::FailSync { node, disk } => {
                let disks = &self.world.slots[node].disks;
                disks[disk % disks.len()].1.fail_next_syncs(1);
                self.fenced[node] = true;
                let life = self.life[node];
                self.heals
                    .push((now + FENCE_DELAY, Heal::Fence { node, life }));
            }
            Fault::MessageLoss { .. }
            | Fault::ControlOutage { .. }
            | Fault::ControlLatency { .. }
            | Fault::LostCasResponses { .. } => unreachable!("a window opened"),
        }
    }

    fn heal(&mut self, sim: &mut turmoil::Sim<'_>, now: Duration, heal: Heal, heal_all: bool) {
        match heal {
            Heal::Repair(a, b) => sim.repair(self.host(a).as_str(), self.host(b).as_str()),
            Heal::Release(a, b) => sim.release(self.host(a).as_str(), self.host(b).as_str()),
            Heal::End(window) => {
                self.in_force.close(window);
                self.apply_in_force(sim);
            }
            // The process whose sync failed has already ended, and its
            // end was a power loss.
            Heal::Fence { node, life } if life != self.life[node] => {}
            // The sync failed while the node was down: the process that
            // starts next inherits the failure, and the fence.
            Heal::Fence { node, life } if self.down[node].is_some() => {
                let at = if heal_all {
                    now
                } else {
                    self.down[node].unwrap_or(now) + FENCE_DELAY
                };
                self.heals.push((at, Heal::Fence { node, life }));
            }
            Heal::Fence { node, .. } => {
                // At the end of the run, the node restarts at once.
                let downtime = if heal_all {
                    Duration::ZERO
                } else {
                    Duration::from_millis(200)
                };
                self.fences += 1;
                self.crash(sim, now, node, true, downtime);
            }
        }
    }

    /// Sets the message loss rate, every node's control-store faults, and
    /// the control bucket's, from the faults in force.
    fn apply_in_force(&self, sim: &mut turmoil::Sim<'_>) {
        let world = self.world;
        sim.set_fail_rate(self.in_force.loss_rate());
        let rates = self.in_force.control_rates(world.rates);
        for slot in &world.slots {
            slot.control.set_rates(rates);
        }
        world
            .control
            .set_faults(self.in_force.control_faults(&world.base_control));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Opens the window of each fault of `plan`, in start order.
    fn open_all(in_force: &mut InForce, plan: &FaultPlan) -> Vec<Window> {
        plan.faults()
            .iter()
            .map(|scheduled| in_force.open(&scheduled.fault).unwrap())
            .collect()
    }

    fn ms(ms: u64) -> Duration {
        Duration::from_millis(ms)
    }

    #[test]
    fn overlapping_message_loss_windows_end_on_their_own() {
        let duration = Duration::from_secs(1);
        let plan = FaultPlan::none()
            .with(
                ms(0),
                Fault::MessageLoss {
                    rate: 0.2,
                    duration,
                },
            )
            .with(
                ms(100),
                Fault::MessageLoss {
                    rate: 0.5,
                    duration,
                },
            );
        let mut in_force = InForce::default();
        let windows = open_all(&mut in_force, &plan);
        assert_eq!(in_force.loss_rate(), 0.5);
        // Either window ending leaves the other's loss in force.
        in_force.close(windows[1]);
        assert_eq!(in_force.loss_rate(), 0.2);
        in_force.close(windows[0]);
        assert_eq!(in_force.loss_rate(), 0.0);

        let mut in_force = InForce::default();
        let windows = open_all(&mut in_force, &plan);
        in_force.close(windows[0]);
        assert_eq!(in_force.loss_rate(), 0.5);
        in_force.close(windows[1]);
        assert_eq!(in_force.loss_rate(), 0.0);
    }

    #[test]
    fn control_windows_that_end_out_of_start_order_remove_their_own_effect() {
        let (long, short) = (Duration::from_secs(4), Duration::from_secs(1));
        // Each later window ends first.
        let plan = FaultPlan::none()
            .with(
                ms(0),
                Fault::ControlLatency {
                    min: ms(100),
                    duration: long,
                },
            )
            .with(
                ms(10),
                Fault::LostCasResponses {
                    probability: 0.1,
                    duration: long,
                },
            )
            .with(ms(20), Fault::ControlOutage { duration: long })
            .with(
                ms(30),
                Fault::ControlLatency {
                    min: ms(300),
                    duration: short,
                },
            )
            .with(
                ms(40),
                Fault::LostCasResponses {
                    probability: 0.6,
                    duration: short,
                },
            )
            .with(ms(50), Fault::ControlOutage { duration: short });
        let base_rates = FaultRates::default();
        let base_faults = SimS3Faults::default();
        let mut in_force = InForce::default();
        let windows = open_all(&mut in_force, &plan);
        assert_eq!(in_force.control_faults(&base_faults).min_delay, ms(300));
        assert_eq!(in_force.control_rates(base_rates).lose_response, 0.6);
        assert_eq!(in_force.control_rates(base_rates).unavailable, 1.0);

        // The short windows end: the long ones' effects stay.
        for window in &windows[3..] {
            in_force.close(*window);
        }
        let faults = in_force.control_faults(&base_faults);
        assert_eq!((faults.min_delay, faults.max_delay), (ms(100), ms(400)));
        assert_eq!(in_force.control_rates(base_rates).lose_response, 0.1);
        assert_eq!(in_force.control_rates(base_rates).unavailable, 1.0);

        // Then the long ones: back to the base.
        for window in &windows[..3] {
            in_force.close(*window);
        }
        assert_eq!(in_force.control_faults(&base_faults), base_faults);
        assert_eq!(in_force.control_rates(base_rates), base_rates);
    }

    #[test]
    fn only_overlapping_kinds_open_windows() {
        let mut in_force = InForce::default();
        let crash = Fault::Crash {
            node: 0,
            power_loss: false,
            downtime: ms(1),
        };
        assert_eq!(in_force.open(&crash), None);
        assert_eq!(in_force.open(&Fault::FailSync { node: 0, disk: 0 }), None);
    }

    #[test]
    fn placement_spreads_primaries() {
        let config = ClusterConfig {
            nodes: 3,
            buckets: 2,
            shards_per_bucket: 2,
            replicas: 2,
            ..ClusterConfig::default()
        };
        let nodes: Vec<NodeId> = (1..=3)
            .map(|n| NodeId::new(format!("node-{n}")).unwrap())
            .collect();
        let mut ids = ProposalIds::seeded(1);
        let (buckets, placement) = registers(&config, &nodes, &mut ids).unwrap();
        assert_eq!(buckets.len(), 2);
        let primaries: Vec<String> = placement.values().map(|c| c.primary.to_string()).collect();
        assert_eq!(primaries, ["node-1", "node-2", "node-3", "node-1"]);
        for config in placement.values() {
            assert_eq!(config.members.len(), 2);
            assert_eq!(config.members[0], config.primary);
        }
    }
}
