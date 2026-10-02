//! Building a simulated cluster, driving it through a workload and a
//! fault plan, and checking the outcome.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use skys3::storage::Storage;
use skys3_config::FailureDomain;
use skys3_control::faults::{FaultRates, FaultyStore};
use skys3_control::{
    Applied, ControlExport, ControlStore, Expected, KeyPrefix, ProposalIds, ProposalOutcome,
    RebuildOptions, RebuildPlan, RetryPolicy, S3ControlStore, S3StoreConfig, TypedKey, bootstrap,
    bump_generation, propose_document,
};
use skys3_flush::FlushSettings;
use skys3_gateway::{GatewayConfig, ShardPlacement, ShardRef};
use skys3_index::{Entry, EntryState, Index, IndexReader, Payload};
use skys3_io::{BlockingPool, Drift, MonotonicClock, SimDiskFaults, SimMount, SyncCut};
use skys3_log::{LogConfig, RecordBody, RecordKind, SegmentId};
use skys3_shard::{CacheSettings, CompactionSettings};
use skys3_sim::check::{Survivors, Violation, check_durable, check_linearizable};
use skys3_sim::history::{History, Operation, Outcome};
use skys3_sim::s3::{SimS3Config, SimS3Faults};
use skys3_sim::{SimContext, SimS3};
use skys3_types::{
    BucketDocument, BucketId, BucketMode, BucketName, ClusterId, Epoch, Label, NodeAddress, NodeId,
    RegisterDocument, RemoteTarget, ShardConfig, ShardCount,
};

use crate::creation;
use crate::faults::{Endpoint, Fault, FaultPlan};
use crate::node::{
    self, BoxError, ControlHandle, INDEX_FILE, LocalServices, NodeServices, NodeSettings, NodeSlot,
    Shared, TRANSPORT_PORT,
};
use crate::pki::Pki;
use crate::replication::register;
use crate::workload::{Client, Routes, Timings, Workload};

/// The shape of a simulated cluster.
#[derive(Clone, Debug, PartialEq)]
pub struct ClusterConfig {
    /// Nodes.
    pub nodes: usize,
    /// Disks per node. The first also holds the index.
    pub disks_per_node: usize,
    /// Buckets.
    pub buckets: usize,
    /// How many of the buckets, the last ones, are `write_back` buckets
    /// whose nodes flush them to the remote store. The others are `local`.
    pub write_back_buckets: usize,
    /// Shards per bucket.
    pub shards_per_bucket: u32,
    /// Members of each shard's configuration in the static placement. The
    /// M1 node keeps one copy, on the primary, whatever this says;
    /// [`ReplicatedServices`](crate::ReplicatedServices) keep one on every
    /// member.
    pub replicas: usize,
    /// The epoch of every shard's configuration in the static placement.
    /// Above 1, gateways can be given maps of older epochs.
    pub placement_epoch: u64,
    /// Whether every member of a shard, not only one, must hold each
    /// acknowledged write after recovery, as the all-member commit rule
    /// promises (§5.1).
    pub every_member_durable: bool,
    /// The bound `ρ` on each node's clock drift; each life of a node
    /// draws its drift within it.
    pub drift: Drift,
    /// Fixed drifts of the first nodes, by position, in every life, in
    /// place of one drawn within `drift`: for scenarios that set chosen
    /// nodes' clocks fast or slow, beyond `ρ` too. Empty by default.
    pub node_drifts: Vec<Drift>,
    /// Faults every disk injects throughout, such as torn writes.
    pub disk_faults: SimDiskFaults,
    /// Faults of every node's control-store requests throughout.
    pub control_rates: FaultRates,
    /// The shortest and longest latency of each network message.
    pub latency: (Duration, Duration),
    /// Faults of the remote store that `write_back` buckets flush to.
    pub remote_faults: SimS3Faults,
    /// Whether the buckets are created through the S3 API, each by a
    /// different node's gateway, before the workload starts, with their
    /// shards placed on the registered nodes, kept apart at the `node`
    /// level (plan M3-04), instead of written by the harness with a static
    /// placement. The nodes must register themselves
    /// ([`CoordinatedServices`](crate::CoordinatedServices)).
    pub create_buckets: bool,
    /// How many of the nodes, the last ones, join the cluster later: they
    /// hold no shard in the static placement, and their hosts stay down,
    /// with empty disks, until a [`Fault::Join`] starts them. Placement
    /// then gives them their share (plan M3-06). The workload starts
    /// without them.
    pub joining: usize,
    /// The clean cache every node keeps (§9.3), with read-through fills of
    /// what it evicts (§9.2); `None` for no cache, in which clean payload
    /// stays. With a cache, a dirty copy whose bytes its node does not
    /// hold counts as no copy when the checkers look for acknowledged
    /// writes: dirty payload must never be evicted.
    pub clean_cache: Option<CacheSettings>,
    /// Segment compaction every node runs after each checkpoint interval
    /// (§10.3); `None` for none. With compaction, a dirty copy counts as
    /// held only if each record its payload names is in a segment its
    /// node still has, and every node must keep each shard's latest
    /// `CONFIG` record in its log.
    pub compaction: Option<CompactionSettings>,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            nodes: 3,
            disks_per_node: 2,
            buckets: 2,
            write_back_buckets: 0,
            shards_per_bucket: 4,
            replicas: 1,
            placement_epoch: 1,
            every_member_durable: false,
            drift: Drift::from_ppm(1_000).expect("the drift is valid"),
            node_drifts: Vec::new(),
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
            remote_faults: SimS3Faults::default(),
            create_buckets: false,
            joining: 0,
            clean_cache: None,
            compaction: None,
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
    /// The syncs each node's disks performed before the final power loss,
    /// by node: the sync boundaries [`Cluster::power_loss_at_sync`] can
    /// pick from on another run of the same seed.
    pub syncs: Vec<u64>,
    /// Power losses planned with [`Cluster::power_loss_at_sync`] that
    /// happened.
    pub power_cuts: usize,
    /// Keys of `write_back` buckets that the remote store holds an object
    /// for at the end.
    pub flushed: usize,
    /// Power-loss restarts the driver forced on nodes whose sync failed
    /// and that had not been restarted otherwise.
    pub fences: usize,
    /// When the clients started, in simulated time since the run began:
    /// the time the fault plan counts from.
    pub started: Duration,
    /// Every write a client sent and got an answer to, or gave up on, in
    /// the order they ended: how long each took, end to end.
    pub writes: Vec<WriteTiming>,
    /// The rebuilds of the control store ([`Fault::RebuildControlStore`]),
    /// in order.
    pub rebuilds: Vec<Rebuild>,
    /// What each shard's register holds at the end, after every fault
    /// healed; a shard without one is left out.
    pub registers: BTreeMap<ShardRef, ShardConfig>,
    /// Copies of keys whose payload their member evicted, at the end, over
    /// every member of every key's shard.
    pub evicted: usize,
    /// Log segments compaction reclaimed, over every node and life.
    pub compacted: u64,
}

/// An operator's rebuild of the control store from the nodes' exports
/// ([`Fault::RebuildControlStore`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rebuild {
    /// When the nodes stopped for it, in simulated time since the run
    /// began.
    pub at: Duration,
    /// The registers it wrote.
    pub plan: RebuildPlan,
    /// How many it wrote, and how many an attempt whose answer was lost
    /// had written.
    pub applied: Applied,
}

/// One client write, timed end to end in simulated time: a `PUT`, the
/// completion of a multipart upload, or a `DELETE`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteTiming {
    /// The shard of the key written.
    pub shard: ShardRef,
    /// When the client sent it, in simulated time since the run began.
    pub sent: Duration,
    /// How long until its answer arrived, or until the client gave up,
    /// connecting included: a write that never connected is recorded
    /// with the time the client spent trying.
    pub took: Duration,
    /// Whether it was acknowledged, or refused for its condition: whether
    /// the shard answered it definitely.
    pub answered: bool,
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
    cut: Option<SyncPowerLoss>,
}

/// A power loss planned at one sync of one node.
#[derive(Clone, Copy, Debug)]
struct SyncPowerLoss {
    node: usize,
    sync: u64,
    cut: SyncCut,
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
/// How long a node stays down after a planned power loss at a sync.
const CUT_DOWNTIME: Duration = Duration::from_millis(300);
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
            cut: None,
        }
    }

    /// Adds an invariant, checked after every step.
    #[must_use]
    pub fn invariant(mut self, invariant: impl Invariant<S>) -> Self {
        self.invariants.push(Box::new(invariant));
        self
    }

    /// Cuts the power of node `node` at its sync numbered `sync`, counting
    /// from 0 over every sync of the node's disks since the run began
    /// (startup included), just before or just after the sync takes
    /// effect. The node then restarts, as after a [`Fault::Crash`] with a
    /// power loss. [`Report::syncs`] gives the number of syncs of a run of
    /// the same seed without the cut; since a seed replays exactly, each
    /// of them is a boundary this run reaches.
    #[must_use]
    pub fn power_loss_at_sync(mut self, node: usize, sync: u64, cut: SyncCut) -> Self {
        self.cut = Some(SyncPowerLoss { node, sync, cut });
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
        if let Some(planned) = self.cut {
            world.slots[planned.node]
                .power
                .cut_at_sync(planned.sync, planned.cut);
        }
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
        let joining = self.config.nodes.saturating_sub(self.config.joining).max(1);
        let mut down = vec![None; world.slots.len()];
        for (index, slot) in world.slots.iter().enumerate().skip(joining) {
            // Held back until it joins: its software never ran.
            sim.crash(slot.host.as_str());
            down[index] = Some(Duration::MAX);
        }
        let mut driver = Driver {
            world: &world,
            history: history.clone(),
            cut: vec![false; world.slots.len()],
            plan: plan.faults().iter().cloned().collect(),
            origin: None,
            heals: Vec::new(),
            down,
            fenced: vec![false; world.slots.len()],
            life: vec![0; world.slots.len()],
            in_force: InForce::default(),
            faults: 0,
            fences: 0,
            rebuilds: Vec::new(),
            failure: None,
        };
        // The clients start once every node serves, and the faults with
        // them.
        while !(world
            .slots
            .iter()
            .zip(&driver.down)
            .all(|(slot, down)| slot.ready.load(Ordering::SeqCst) || *down == Some(Duration::MAX))
            && self.services.ready())
        {
            if sim.elapsed() > STARTUP_LIMIT {
                return Err(RunError::Simulation("the nodes did not start".to_owned()));
            }
            driver.advance(&mut sim, false);
            driver.check()?;
            sim.step().map_err(failed)?;
        }
        let routes = if self.config.create_buckets {
            self.create_buckets(&mut sim, &mut driver, &history, workload.timeout)?
        } else {
            world.routes.clone()
        };
        driver.origin = Some(sim.elapsed());
        let writes = Timings::default();
        for index in 0..workload.clients {
            let client = Client::new(
                client_host(index),
                routes.clone(),
                history.clone(),
                workload.clone(),
                context.fork_seed(),
            )
            .with_timings(writes.clone());
            sim.client(client_host(index), client.run());
        }
        self.drive(&mut sim, &mut driver, &history, false)?;

        // Heal everything, and read every key back.
        driver.advance(&mut sim, true);
        let reader = Client::new(
            "verifier".to_owned(),
            routes.clone(),
            history.clone(),
            workload.clone(),
            context.fork_seed(),
        );
        sim.client("verifier", reader.read_all(FINAL_READ_ATTEMPTS));
        self.drive(&mut sim, &mut driver, &history, true)?;

        let registers = routes
            .placement
            .keys()
            .filter_map(|shard| Some((shard.clone(), register(&world.registers, shard)?)))
            .collect();
        // Power loss everywhere: what survives is what the checkers see.
        let syncs = world.slots.iter().map(|slot| slot.power.syncs()).collect();
        for slot in &world.slots {
            slot.stop_disks(true);
            sim.crash(slot.host.as_str());
        }
        let operations = history.operations();
        let (survivors, evicted) = world.survivors(&routes, workload.keys)?;
        check_linearizable(&operations).map_err(RunError::Check)?;
        check_durable(&operations, &survivors).map_err(RunError::Check)?;
        if self.config.every_member_durable {
            let members = survivors.values().map(|s| s.copies.len()).max();
            for member in 0..members.unwrap_or(0) {
                let one: BTreeMap<String, Survivors> = survivors
                    .iter()
                    .map(|(key, survivor)| {
                        // A shard that lost members (§6.4) has fewer:
                        // its last one stands in for the missing ones.
                        let copy = survivor.copies.get(member).or(survivor.copies.last());
                        let copies = copy.cloned().into_iter().collect();
                        let one = Survivors {
                            copies,
                            ..survivor.clone()
                        };
                        (key.clone(), one)
                    })
                    .collect();
                check_durable(&operations, &one).map_err(RunError::Check)?;
            }
        }
        Ok(Report {
            history: operations,
            faults: driver.faults,
            lives: world.slots.iter().map(|slot| slot.lives()).sum(),
            syncs,
            power_cuts: driver.cut.iter().filter(|cut| **cut).count(),
            flushed: survivors
                .values()
                .filter(|survivor| matches!(survivor.flushed, Some(Some(_))))
                .count(),
            fences: driver.fences,
            started: driver.origin.unwrap_or_default(),
            writes: writes.take(),
            rebuilds: driver.rebuilds,
            registers,
            evicted,
            compacted: world.shared.compacted.load(Ordering::Relaxed),
        })
    }

    /// Creates the buckets through the gateways (plan M3-04): a client
    /// sends CreateBucket for each planned bucket to a different node, and
    /// waits until every node's gateway knows every bucket. Then the
    /// driver waits until every shard register exists and the services are
    /// ready. Returns the routes to the created buckets, read from their
    /// registers.
    fn create_buckets(
        &mut self,
        sim: &mut turmoil::Sim<'_>,
        driver: &mut Driver<'_, S>,
        history: &History,
        timeout: Duration,
    ) -> Result<Routes, RunError> {
        let world = driver.world;
        let planned = world.routes.buckets.clone();
        let nodes = world.routes.nodes.clone();
        sim.client("creator", creation::create(planned.clone(), nodes, timeout));
        self.drive(sim, driver, history, false)?;
        let started = sim.elapsed();
        loop {
            if let Some(routes) = world.created(&planned)
                && self.services.ready()
            {
                return Ok(routes);
            }
            if sim.elapsed() > started + STARTUP_LIMIT {
                return Err(RunError::Simulation(
                    "the created buckets did not become ready".to_owned(),
                ));
            }
            driver.advance(sim, false);
            driver.check()?;
            self.check_invariants(sim, driver, history)?;
            sim.step().map_err(failed)?;
        }
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
            driver.check()?;
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
    /// The control store over `control`, without faults, for reading the
    /// registers at the end.
    registers: S3ControlStore<SimS3>,
    base_control: SimS3Faults,
    rates: FaultRates,
    remote: SimS3,
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
        remote.set_faults(config.remote_faults.clone());
        let settings = settings(&cluster, config)?;
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
        // Nodes that join later hold nothing at first.
        let initial = &node_ids[..config.nodes.saturating_sub(config.joining).max(1)];
        let (buckets, mut placement) = registers(config, initial, &mut ids)?;
        if config.create_buckets {
            // The gateways create the buckets, and their placement.
            placement.clear();
        }
        let written = if config.create_buckets {
            &[][..]
        } else {
            &buckets[..]
        };
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()?
            .block_on(write_registers(
                &store, &cluster, written, &placement, &mut ids,
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
                config.node_drifts.get(index).copied(),
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
                remote: remote.clone(),
                compacted: Arc::new(AtomicU64::new(0)),
            }),
            routes: Routes {
                buckets,
                placement,
                // Clients and the bucket creator address only the nodes in
                // the cluster from the start: one that joins later is down
                // until then, and no client knows of it.
                nodes: node_ids[..config.nodes.saturating_sub(config.joining).max(1)].to_vec(),
            },
            base_control: control.faults(),
            control,
            registers: store,
            rates: config.control_rates,
            remote,
        })
    }

    /// The routes to the `planned` buckets as the gateways created them,
    /// once every bucket register and every shard register exists.
    fn created(&self, planned: &[BucketDocument]) -> Option<Routes> {
        let mut buckets = Vec::new();
        let mut placement = BTreeMap::new();
        for bucket in planned {
            let key = self
                .registers
                .object_key(TypedKey::bucket(&bucket.name).key());
            let object = self.control.object(&key)?;
            let bucket = BucketDocument::from_json(&object.body).ok()?;
            for shard in ShardRef::all(&bucket) {
                let config = register(&self.registers, &shard)?;
                placement.insert(shard, config);
            }
            buckets.push(bucket);
        }
        Some(Routes {
            buckets,
            placement: Arc::new(placement),
            nodes: self.routes.nodes.clone(),
        })
    }

    /// Recovers every node from its disks, outside the simulation, and
    /// reads each key's value on every member of its shard and, for a
    /// `write_back` bucket, whether the member's entry is clean, and the
    /// key's value in the remote store. Also returns how many of those
    /// copies are evicted.
    fn survivors(
        &self,
        routes: &Routes,
        keys: usize,
    ) -> Result<(BTreeMap<String, Survivors>, usize), BoxError> {
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
            let segments = if settings.compaction.is_some() {
                runtime.block_on(check_configs(&storage))?;
                Some(segments_of(&storage))
            } else {
                None
            };
            indexes.insert(slot.id.clone(), (storage.index, storage.shards, segments));
        }
        let mut survivors = BTreeMap::new();
        let mut evicted = 0;
        for (name, bucket, key) in routes.keys(keys) {
            let shard = ShardRef::for_key(&bucket, &key);
            // The members now: a member removed (§6.4) misses the writes
            // committed after its removal.
            let members = register(&self.registers, &shard)
                .map_or_else(|| routes.placement[&shard].members.clone(), |c| c.members);

            let write_back = bucket.mode == BucketMode::WriteBack;
            let mut survivor = Survivors::default();
            for member in &members {
                let (index, shards, segments) = &indexes[member];
                let read = index.read()?;
                // With compaction, the bytes must be in a segment the node
                // still has.
                let segments = segments
                    .as_ref()
                    .map(|segments| &segments[shards.set().disk_of(&(&shard).into())]);
                let entry = read.entry(&(&shard).into(), &key)?;
                evicted += usize::from(
                    entry
                        .as_ref()
                        .is_some_and(|e| e.state == EntryState::Evicted),
                );
                // No entry claims what a clean one does: the remote store
                // matches, here by holding nothing.
                let clean = entry.as_ref().is_none_or(|entry| {
                    matches!(entry.state, EntryState::Clean | EntryState::Evicted)
                });
                // With a cache, a dirty copy must still hold its bytes.
                let held = clean
                    || (settings.clean_cache.is_none() && settings.compaction.is_none())
                    || entry
                        .as_ref()
                        .is_some_and(|entry| holds_bytes(&read, &shard, entry, segments));
                survivor.copies.push(
                    entry
                        .filter(|_| held)
                        .and_then(|entry| entry.object)
                        .map(|o| o.local_etag.to_string()),
                );
                if write_back {
                    survivor.clean.push(clean);
                }
            }
            if write_back {
                let remote_key = format!("{}{key}", remote_prefix(&bucket));
                let object = self.remote.object(&remote_key);
                survivor.flushed = Some(object.map(|o| o.info.etag.to_string()));
            }
            survivors.insert(name, survivor);
        }
        Ok((survivors, evicted))
    }
}

/// The segments each disk of a recovered node holds.
fn segments_of(storage: &Storage<SimMount>) -> BTreeMap<Label, BTreeSet<SegmentId>> {
    storage
        .logs
        .iter()
        .map(|(disk, log)| {
            let ids = log.segments().iter().map(|segment| segment.id).collect();
            (disk.clone(), ids)
        })
        .collect()
}

/// Checks that each shard's latest `CONFIG` record, whose configuration
/// the index of a recovered node keeps, is in the log of the shard's disk:
/// compaction keeps it (§10.3).
async fn check_configs(storage: &Storage<SimMount>) -> Result<(), BoxError> {
    let configs = storage.index.read()?.configs()?;
    let set = storage.shards.set();
    let mut found = Vec::new();
    for (_, log) in set.logs() {
        for segment in log.segments() {
            let mut scanner = log.scan(segment.id)?;
            while let Some(scanned) = scanner.next().await? {
                if scanned.header.kind == RecordKind::Config
                    && let RecordBody::Config(config) = scanned.decode()?.body
                {
                    found.push(config);
                }
            }
        }
    }
    match configs.values().find(|config| !found.contains(*config)) {
        Some(lost) => Err(format!(
            "the latest CONFIG record of {}/{} is gone from the log",
            lost.bucket_id, lost.shard
        )
        .into()),
        None => Ok(()),
    }
}

/// Whether the node that `read` is of holds the bytes of `entry`, a dirty
/// entry of `shard`: every record its payload names is located there, in
/// one of `segments` if they are given.
fn holds_bytes(
    read: &IndexReader,
    shard: &ShardRef,
    entry: &Entry,
    segments: Option<&BTreeSet<SegmentId>>,
) -> bool {
    let shard = &shard.into();
    let located = |position| match read.location(shard, position) {
        Ok(Some(location)) => segments.is_none_or(|ids| ids.contains(&location.segment)),
        _ => false,
    };
    let Some(object) = &entry.object else {
        return true;
    };
    match &object.payload {
        Payload::None => object.size == 0,
        Payload::Inline(position) => located(*position),
        Payload::Extents(extents) => extents.iter().all(|e| located(e.position)),
        Payload::Parts { upload, parts } => {
            read.parts(shard, *upload, 0, parts.len())
                .is_ok_and(|rows| {
                    rows.len() == parts.len()
                        && rows.iter().all(|(_, part)| match &part.payload {
                            Payload::Inline(position) => located(*position),
                            Payload::Extents(extents) => {
                                extents.iter().all(|e| located(e.position))
                            }
                            _ => false,
                        })
                })
        }
    }
}

/// The faults of the control store's requests while an operator rebuilds
/// it: the answers an operator's tool sees from any store.
const REBUILD_FAULTS: FaultRates = FaultRates {
    lose_request: 0.1,
    lose_response: 0.2,
    late_request: 0.0,
    conflict: 0.1,
    unavailable: 0.1,
    max_delay: Duration::ZERO,
};

/// How the rebuild retries: often, and without waiting long, since the
/// harness runs it outside simulated time.
const REBUILD_RETRY: RetryPolicy = RetryPolicy {
    max_attempts: 30,
    initial_backoff: Duration::from_millis(1),
    max_backoff: Duration::from_millis(2),
};

impl<S: NodeServices> World<S> {
    /// Deletes every register of the control store, as when its bucket is
    /// lost ([`Fault::LoseControlStore`]).
    fn lose_control_store(&self) -> Result<(), BoxError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()?;
        runtime.block_on(async {
            for (key, version) in self.registers.list(&KeyPrefix::root()).await? {
                let _ = self.registers.delete_if(&key, &version).await?;
            }
            Ok(())
        })
    }

    /// Rebuilds the control store from every node's export, which the
    /// nodes, all down, give as `skys3 control export` does: their logs
    /// recovered into their indexes first ([`Fault::RebuildControlStore`]).
    /// The store's answers are lost now and then (`seed`).
    fn rebuild_control_store(&self, seed: u64) -> Result<(RebuildPlan, Applied), BoxError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()?;
        let settings = &self.shared.settings;
        let mut exports: Vec<ControlExport> = Vec::new();
        for slot in &self.slots {
            let mounts: Vec<_> = slot
                .disks
                .iter()
                .map(|(label, disk)| (label.clone(), disk.mount()))
                .collect();
            let index = Index::open_sim(&mounts[0].1, INDEX_FILE, &node::index_config(settings))?;
            let pool = BlockingPool::inline("export");
            let storage = runtime.block_on(skys3::storage::recover(
                mounts,
                settings.log.clone(),
                Arc::new(MonotonicClock::new()),
                Arc::new(index),
                pool.clone(),
                slot.id.clone(),
            ))?;
            let export = runtime.block_on(skys3::rebuild::export(
                &storage.index,
                &pool,
                &settings.cluster,
                &slot.id,
                slot.id.as_str(),
            ))?;
            drop(storage);
            // The export's process ends.
            slot.stop_disks(false);
            exports.push(export);
        }
        let plan = RebuildPlan::new(&settings.cluster, &exports, &RebuildOptions::default())?;
        let store = FaultyStore::seeded(self.registers.clone(), seed, REBUILD_FAULTS);
        let applied = runtime.block_on(plan.apply(&store, &REBUILD_RETRY))?;
        Ok((plan, applied))
    }
}

/// The settings every simulated node runs with: small log segments and
/// inline bodies, so a short workload exercises extents and several
/// segments, and frequent checkpoints.
fn settings(cluster: &ClusterId, shape: &ClusterConfig) -> Result<NodeSettings, BoxError> {
    // Buckets the gateways create get the shape's shards and replicas.
    let defaults = format!(
        "[buckets.defaults]\nshards_per_bucket = {}\nreplicas = {}\nmin_write_replicas = 1\n",
        shape.shards_per_bucket,
        shape.replicas.clamp(1, shape.nodes.max(1)),
    );
    let config: skys3_config::Config = format!(
        "[cluster]\ncluster_id = \"{cluster}\"\n\
         [control_store]\netcd_endpoints = [\"https://etcd.sim.internal:2379\"]\n{defaults}"
    )
    .parse()?;
    let mut gateway = GatewayConfig::new(&config);
    if shape.create_buckets {
        gateway.placement = ShardPlacement::Cluster(FailureDomain::Node);
    }
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
        flush: FlushSettings {
            // Fills commit extents as small as a PUT's.
            extent_bytes: 512,
            concurrency: 2,
            min_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(1),
            ..FlushSettings::default()
        },
        checkpoint_interval: Duration::from_secs(1),
        drift: Drift::NONE,
        poll_interval: Duration::from_millis(500),
        clean_cache: shape.clean_cache,
        compaction: shape.compaction,
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
    let local_buckets = config.buckets.saturating_sub(config.write_back_buckets);
    for b in 0..config.buckets {
        let name = BucketName::new(format!("bucket-{b}"))?;
        let (mode, target) = if b < local_buckets {
            (BucketMode::Local, None)
        } else {
            let target = RemoteTarget {
                endpoint: "https://remote.sim.internal".to_owned(),
                bucket: "remote".to_owned(),
                prefix: Some(format!("{name}/")),
            };
            (BucketMode::WriteBack, Some(target))
        };
        let bucket = BucketDocument {
            bucket_id: BucketId::new(format!("b-sim{b}"))?,
            name,
            mode,
            shards,
            replicas: u8::try_from(replicas)?,
            min_write_replicas: 1,
            clean_copies: 1,
            target,
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
                epoch: Epoch::new(config.placement_epoch),
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

/// The key prefix of a `write_back` bucket in the remote store.
fn remote_prefix(bucket: &BucketDocument) -> &str {
    bucket
        .target
        .as_ref()
        .and_then(|target| target.prefix.as_deref())
        .unwrap_or_default()
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
    /// The history, whose operations a node's crash ends.
    history: History,
    /// Nodes whose planned power loss at a sync happened.
    cut: Vec<bool>,
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
    /// The rebuilds of the control store so far.
    rebuilds: Vec<Rebuild>,
    /// Why the harness itself failed while applying a fault, which fails
    /// the run.
    failure: Option<String>,
}

impl<S: NodeServices> Driver<'_, S> {
    /// Fails the run if the harness failed to apply a fault.
    fn check(&mut self) -> Result<(), RunError> {
        match self.failure.take() {
            Some(failure) => Err(RunError::Simulation(failure)),
            None => Ok(()),
        }
    }

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
            if !self.cut[index] && self.world.slots[index].power.is_cut() {
                // The disks lost power inside a sync; the host goes with
                // them now.
                self.cut[index] = true;
                self.crash(sim, now, index, true, CUT_DOWNTIME);
            }
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
                    self.history.crashed(host);
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
        self.history.crashed(&slot.host);
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
            Fault::Join { node } => {
                // It starts at the next advance, once.
                if self.down[node] == Some(Duration::MAX) {
                    self.down[node] = Some(now);
                }
            }
            Fault::LoseControlStore => {
                if let Err(error) = self.world.lose_control_store() {
                    self.failure = Some(format!("the control store could not be lost: {error}"));
                }
            }
            Fault::RebuildControlStore {
                power_loss,
                downtime,
            } => {
                for node in 0..self.world.slots.len() {
                    self.crash(sim, now, node, power_loss, downtime);
                }
                let seed = u64::try_from(now.as_nanos()).unwrap_or(u64::MAX);
                match self.world.rebuild_control_store(seed) {
                    Ok((plan, applied)) => self.rebuilds.push(Rebuild {
                        at: now,
                        plan,
                        applied,
                    }),
                    Err(error) => {
                        self.failure = Some(format!("the control-store rebuild failed: {error}"));
                    }
                }
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
