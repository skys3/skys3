//! Node services with a coordinator (plan M3-01, design §6.2, §6.7): beside
//! the services they wrap, every node contends for the coordinator lease,
//! runs a coordinator, and serves pushed generations. Placement proper
//! comes with later plans, so the coordinator's work is a stand-in: it
//! moves a few shard registers of a bucket no gateway serves through their
//! epochs, as membership changes would.
//!
//! Every node also registers itself and sends the coordinator heartbeats
//! (plan M3-02, design §6.7). The coordinator's pushes start with no node
//! to go to: they reach the nodes its registry lists, so a node that is
//! pushed to has joined with nothing but its credentials. The coordinator
//! forgets a node silent for `node_forget_after` once no shard register
//! names it.
//!
//! The audits check what the scenarios need:
//!
//! - every accepted change wrote an epoch nobody else wrote, so two nodes
//!   that both act as coordinator lose no change and overwrite none
//!   unseen ([`CoordinatedServices::check_changes`]);
//! - while clocks drift within `ρ`, no two nodes' tenures overlap in
//!   simulated time ([`CoordinatedServices::check_tenures`]);
//! - every announced generation reached every node by push
//!   ([`CoordinatedServices::check_pushes`]);
//! - placement work pauses only while the lease moves
//!   ([`CoordinatedServices::longest_pause`]).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::num::NonZeroU16;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use skys3_control::faults::FaultRates;
use skys3_control::{
    ControlError, ControlStore, ProposalIds, RegisterKind, RetryPolicy, TypedKey, Version, read,
};
use skys3_coord::{
    AdminEndpoint, Announce, Applied, ChangeSet, ControlHints, Coordinator, CoordinatorConfig,
    Elector, HeartbeatConfig, HeartbeatStatus, Heartbeater, Leadership, LeaseConfig, Lifecycle,
    NodeEntry, NodeProfile, NodeRegistry, PeerSink, Placement, Pusher, RegistryConfig,
};
use skys3_io::{Clock, MonoTime, MonotonicClock};
use skys3_net::TurmoilNetwork;
use skys3_types::{
    BucketId, ClusterId, DiskInfo, Epoch, Generation, Label, NodeAddress, NodeId, ShardConfig,
    ShardId,
};
use tokio::sync::watch;

use crate::cluster::View;
use crate::node::{BoxError, ControlHandle, NodeEnv, NodeServices};

/// The port pushes of new generations arrive on. Replication holds the
/// transport port, and the harness gives pushes a port of their own; a node
/// binary that serves both on one port dispatches by message class.
pub const PUSH_PORT: u16 = 7401;

/// How long a push may take to reach a node.
const PUSH_TIMEOUT: Duration = Duration::from_secs(1);

/// The bucket whose shard registers the stand-in placement changes.
fn bucket() -> BucketId {
    BucketId::new("b-coordination").expect("a valid bucket ID")
}

/// How the coordinator of [`CoordinatedServices`] works.
#[derive(Clone, Debug)]
pub struct CoordinationConfig {
    /// The lease timings.
    pub lease: LeaseConfig,
    /// The shard registers placement cycles through, each change moving
    /// one to its next epoch.
    pub registers: u8,
    /// The pause between two changes of a coordinator.
    pub interval: Duration,
    /// A node that acts as coordinator throughout without the lease, as
    /// one that wrongly believes it holds it would: after a long pause, or
    /// with a clock drifting far beyond `ρ`.
    pub stale: Option<usize>,
    /// A seeded bug: a node whose lease timings are a quarter of the
    /// others', so it takes over while the holder still acts.
    pub hasty: Option<usize>,
    /// Cuts the node that is coordinator at this simulated time off from
    /// the control store for this long, and leaves its data path alone.
    pub isolate: Option<(Duration, Duration)>,
    /// The control-store fault rates the isolated node returns to.
    pub rates: FaultRates,
    /// How the coordinator judges node health, and when it forgets nodes.
    pub registry: RegistryConfig,
    /// How nodes register and send heartbeats.
    pub heartbeat: HeartbeatConfig,
}

impl Default for CoordinationConfig {
    /// A 1.2 s lease under `ρ` = 1%, three registers, and a change every
    /// 100 ms.
    fn default() -> Self {
        let lease = LeaseConfig::new(Duration::from_millis(1200), 0.01)
            .expect("valid lease timings")
            .with_retry(RetryPolicy {
                max_attempts: 4,
                initial_backoff: Duration::from_millis(20),
                max_backoff: Duration::from_millis(100),
            });
        Self {
            lease,
            registers: 3,
            interval: Duration::from_millis(100),
            stale: None,
            hasty: None,
            isolate: None,
            rates: FaultRates::default(),
            // Nodes are suspected after 600 ms of silence, and forgotten
            // after an hour: never, in most runs.
            registry: RegistryConfig {
                suspect_after: Duration::from_millis(600),
                forget_after: Duration::from_secs(3600),
            },
            heartbeat: HeartbeatConfig {
                interval: Duration::from_millis(150),
                timeout: Duration::from_millis(500),
                resolve_interval: Duration::from_millis(300),
                retry: lease.retry,
            },
        }
    }
}

/// An accepted change of the stand-in placement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Change {
    /// When the change was accepted, in simulated time.
    pub at: Duration,
    /// The node that made it.
    pub node: NodeId,
    /// Whether that node acted without the lease.
    pub stale: bool,
    /// The shard register.
    pub register: u8,
    /// The epoch it wrote.
    pub epoch: u64,
}

/// How pushes reached nodes, as [`CoordinatedServices::check_pushes`]
/// measured them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PushDelays {
    /// The pushes checked: one per node and announcement.
    pub checked: usize,
    /// The longest a node took to hear of an announced generation.
    pub slowest: Duration,
}

/// A span of simulated time in which a node believed it was coordinator.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Tenure {
    node: NodeId,
    life: u64,
    start: Duration,
    end: Duration,
    open: bool,
}

#[derive(Default)]
struct Audit {
    changes: Vec<Change>,
    rejected: usize,
    tenures: Vec<Tenure>,
    /// When each node announced each generation.
    announced: Vec<(Duration, NodeId, Generation)>,
    /// When each node first heard of each generation pushed to it.
    pushed: BTreeMap<NodeId, Vec<(Duration, Generation)>>,
    /// The node cut off from the control store, and when.
    isolated: Option<(NodeId, Duration)>,
    /// The nodes, by position.
    nodes: BTreeMap<usize, NodeId>,
    /// When the harness first showed the services which nodes run: once
    /// the clients start.
    watched: Option<Duration>,
    /// When it last did.
    observed: Option<Duration>,
    /// When each node, by position, was seen down: spans of simulated
    /// time, the last one growing while the node stays down.
    down: BTreeMap<usize, Vec<(Duration, Duration)>>,
    /// What the registry of the latest coordinator to plan listed.
    view: Option<RegistryView>,
    /// The nodes coordinators forgot.
    forgotten: Vec<Forgotten>,
    /// When each node registered, in each of its lives and again whenever
    /// a coordinator told it that it was not registered.
    registered: BTreeMap<NodeId, Vec<Duration>>,
}

/// Node services that add the coordinator of plan M3-01 to `S`.
#[derive(Clone)]
pub struct CoordinatedServices<S> {
    inner: S,
    config: CoordinationConfig,
    audit: Arc<Mutex<Audit>>,
}

impl<S> fmt::Debug for CoordinatedServices<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CoordinatedServices")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// The simulated time since the run began.
fn elapsed() -> Duration {
    turmoil::sim_elapsed().unwrap_or_default()
}

/// The simulated time at which `clock`, a node's clock in this life,
/// reads `local`. Call it in the node's host.
fn simulated(clock: &MonotonicClock, local: MonoTime) -> Duration {
    elapsed()
        + clock
            .runtime_deadline(local)
            .saturating_duration_since(tokio::time::Instant::now())
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl<S: NodeServices> CoordinatedServices<S> {
    /// `inner`, with a coordinator configured by `config`.
    #[must_use]
    pub fn new(inner: S, config: CoordinationConfig) -> Self {
        Self {
            inner,
            config,
            audit: Arc::default(),
        }
    }

    /// The wrapped services.
    #[must_use]
    pub fn inner(&self) -> &S {
        &self.inner
    }

    fn audit(&self) -> MutexGuard<'_, Audit> {
        lock(&self.audit)
    }

    /// Every accepted change so far, in the order they were accepted.
    #[must_use]
    pub fn changes(&self) -> Vec<Change> {
        self.audit().changes.clone()
    }

    /// How many planned changes lost their compare-and-swap.
    #[must_use]
    pub fn rejected(&self) -> usize {
        self.audit().rejected
    }

    /// The node cut off from the control store, and when, if any was.
    #[must_use]
    pub fn isolated(&self) -> Option<(NodeId, Duration)> {
        self.audit().isolated.clone()
    }

    /// Checks that no two accepted changes wrote the same epoch of a
    /// register: each was a compare-and-swap over the epoch it read, so
    /// none was lost or overwritten unseen, whoever made it.
    ///
    /// # Errors
    ///
    /// The first epoch written twice.
    pub fn check_changes(&self) -> Result<(), String> {
        let audit = self.audit();
        let mut seen: BTreeMap<(u8, u64), &Change> = BTreeMap::new();
        for change in &audit.changes {
            if let Some(first) = seen.insert((change.register, change.epoch), change) {
                return Err(format!(
                    "epoch {} of register {} was written twice: by {} at {:?} and by {} at {:?}",
                    change.epoch, change.register, first.node, first.at, change.node, change.at
                ));
            }
        }
        Ok(())
    }

    /// Checks that no two nodes believed they were coordinator at the same
    /// simulated time, leaving out the stale node. That holds while clocks
    /// drift within `ρ`.
    ///
    /// # Errors
    ///
    /// The first two tenures that overlap.
    pub fn check_tenures(&self) -> Result<(), String> {
        let audit = self.audit();
        for (n, a) in audit.tenures.iter().enumerate() {
            for b in &audit.tenures[n + 1..] {
                if a.node != b.node && a.start < b.end && b.start < a.end {
                    return Err(format!(
                        "{} was coordinator from {:?} to {:?}, and {} from {:?} to {:?}",
                        a.node, a.start, a.end, b.node, b.start, b.end
                    ));
                }
            }
        }
        Ok(())
    }

    /// The nodes that held the lease at some point.
    #[must_use]
    pub fn coordinators(&self) -> BTreeSet<NodeId> {
        self.audit()
            .tenures
            .iter()
            .map(|tenure| tenure.node.clone())
            .collect()
    }

    /// Records which nodes run, for
    /// [`CoordinatedServices::check_pushes`]: an
    /// [`Invariant`](crate::Invariant) that always holds.
    ///
    /// # Errors
    ///
    /// None.
    pub fn observe(&self, view: &View<'_, Self>) -> Result<(), String> {
        let mut audit = self.audit();
        let now = view.elapsed;
        audit.watched.get_or_insert(now);
        let previous = audit.observed.replace(now);
        for (position, up) in view.up.iter().enumerate() {
            if *up {
                continue;
            }
            let spans = audit.down.entry(position).or_default();
            match spans.last_mut() {
                // Still down since the previous step.
                Some(span) if previous == Some(span.1) => span.1 = now,
                _ => spans.push((now, now)),
            }
        }
        Ok(())
    }

    /// Checks that every generation announced while every node ran
    /// reached every node by push within `bound`, and returns how fast. A node that is down, or `restart` after it came back, misses
    /// pushes and learns the generation from its change stream instead, so
    /// announcements then are not checked, nor those outside the steps the
    /// harness checks invariants at: from when the clients start until the
    /// final restart. [`CoordinatedServices::observe`] must run as an
    /// invariant.
    ///
    /// # Errors
    ///
    /// The first announcement a running node did not hear of in time.
    pub fn check_pushes(&self, bound: Duration, restart: Duration) -> Result<PushDelays, String> {
        let audit = self.audit();
        let (Some(first), Some(last)) = (audit.watched, audit.observed) else {
            return Err("observe did not run as an invariant".to_owned());
        };
        let disturbed = |at: Duration| {
            at < first
                || at + bound > last
                || audit
                    .down
                    .values()
                    .flatten()
                    .any(|(start, end)| at + bound >= *start && at <= *end + restart)
        };
        let mut delays = PushDelays::default();
        for (at, from, generation) in &audit.announced {
            if disturbed(*at) {
                continue;
            }
            for node in audit.nodes.values() {
                let heard = audit
                    .pushed
                    .get(node)
                    // Another coordinator may have pushed a newer
                    // generation first.
                    .and_then(|heard| heard.iter().find(|(_, got)| got >= generation))
                    .map(|(when, _)| when.saturating_sub(*at));
                match heard {
                    Some(delay) if delay <= bound => {
                        delays.checked += 1;
                        delays.slowest = delays.slowest.max(delay);
                    }
                    _ => {
                        return Err(format!(
                            "{node} did not hear of generation {generation}, announced by {from} \
                             at {at:?}, within {bound:?}"
                        ));
                    }
                }
            }
        }
        Ok(delays)
    }

    /// What the registry of the latest coordinator to plan listed, if one
    /// planned.
    #[must_use]
    pub fn registry_view(&self) -> Option<RegistryView> {
        self.audit().view.clone()
    }

    /// The spans of simulated time in which the node at `position` was
    /// seen down, as [`CoordinatedServices::observe`] recorded them.
    #[must_use]
    pub fn downtime(&self, position: usize) -> Vec<(Duration, Duration)> {
        self.audit()
            .down
            .get(&position)
            .cloned()
            .unwrap_or_default()
    }

    /// The nodes coordinators forgot, in the order they forgot them.
    #[must_use]
    pub fn forgotten(&self) -> Vec<Forgotten> {
        self.audit().forgotten.clone()
    }

    /// When `node` registered: in each life, and again whenever a
    /// coordinator told it that it was not registered.
    #[must_use]
    pub fn registrations(&self, node: &NodeId) -> Vec<Duration> {
        self.audit()
            .registered
            .get(node)
            .cloned()
            .unwrap_or_default()
    }

    /// How many generations were announced.
    #[must_use]
    pub fn announcements(&self) -> usize {
        self.audit().announced.len()
    }

    /// The longest simulated time between two consecutive changes made
    /// under the lease, from `from` on.
    #[must_use]
    pub fn longest_pause(&self, from: Duration) -> Duration {
        let audit = self.audit();
        let times: Vec<Duration> = audit
            .changes
            .iter()
            .filter(|change| !change.stale && change.at >= from)
            .map(|change| change.at)
            .collect();
        times
            .windows(2)
            .map(|pair| pair[1] - pair[0])
            .max()
            .unwrap_or_default()
    }
}

impl<S: NodeServices> NodeServices for CoordinatedServices<S> {
    type Shards = S::Shards;

    fn ready(&self) -> bool {
        self.inner.ready()
    }

    async fn start(&self, env: NodeEnv) -> Result<S::Shards, BoxError> {
        let node = env.node.clone();
        let (position, life, clock) = (env.position, env.life, Arc::clone(&env.clock));
        let store = env.control.store();
        let transport = env.transport.clone();
        // What the node registers: its transport address, a rack label,
        // and its disks. Nothing else tells the coordinator about it.
        let profile = NodeProfile {
            node: node.clone(),
            address: env.peers[&node].clone(),
            zone: None,
            rack: Some(rack(position)),
            disks: env
                .disks
                .iter()
                .map(|disk| DiskInfo {
                    disk_id: disk.clone(),
                    capacity_bytes: DISK_BYTES,
                })
                .collect(),
        };
        let seed = env.seed;
        let shards = self.inner.start(env).await?;

        let hints = ControlHints::new();
        let listener = transport
            .bind((std::net::Ipv4Addr::UNSPECIFIED, PUSH_PORT).into())
            .await?;
        self.audit().nodes.insert(position, node.clone());
        self.follow_pushes(node.clone(), &hints);

        let Some(store) = store else {
            tracing::warn!(%node, "no control store: this life runs no coordinator");
            let endpoint = AdminEndpoint::new(hints);
            tokio::spawn(async move { endpoint.serve(listener).await });
            return Ok(shards);
        };
        let stale = self.config.stale == Some(position);
        let lease = match self.config.hasty {
            Some(hasty) if hasty == position => {
                let config = self.config.lease;
                LeaseConfig::new(config.lease() / 4, config.drift())?.with_retry(config.retry)
            }
            _ => self.config.lease,
        };
        let leadership = if stale {
            // A belief that never ends, and that no elector updates.
            let (_, believer) = watch::channel(Leadership::Coordinator {
                version: Version::new("believed"),
                until: MonoTime::MAX,
            });
            believer
        } else {
            let elector = Elector::new(
                store.clone(),
                node.clone(),
                Arc::clone(&clock) as Arc<dyn Clock>,
                lease,
                ProposalIds::seeded(seed.wrapping_add(1)),
            );
            let leadership = elector.subscribe();
            self.follow_tenures(node.clone(), life, Arc::clone(&clock), leadership.clone());
            if let Some((at, duration)) = self.config.isolate {
                let isolation = Isolation {
                    store: store.clone(),
                    node: node.clone(),
                    at,
                    duration,
                    rates: self.config.rates,
                    leadership: leadership.clone(),
                    clock: Arc::clone(&clock),
                    audit: Arc::clone(&self.audit),
                };
                tokio::spawn(isolation.run());
            }
            tokio::spawn(elector.run());
            leadership
        };
        let placement = EpochPlacement {
            node: node.clone(),
            stale,
            registers: self.config.registers,
            next: 0,
            rest: false,
            planned: None,
            audit: Arc::clone(&self.audit),
        };
        let clock = clock as Arc<dyn Clock>;
        let registry = NodeRegistry::new(Arc::clone(&clock), self.config.registry);
        let endpoint = AdminEndpoint::new(hints.clone()).with_heartbeats(
            registry.clone(),
            leadership.clone(),
            Arc::clone(&clock),
        );
        tokio::spawn(async move { endpoint.serve(listener).await });

        // Pushes go only to the nodes the registry lists.
        let pusher =
            Pusher::new(transport.clone(), BTreeMap::new(), PUSH_TIMEOUT).with_local(hints);
        let placement = Observed {
            node: node.clone(),
            stale,
            lifecycle: Lifecycle::new(registry.clone(), placement)
                .with_peers(PushPeers(pusher.clone())),
            audit: Arc::clone(&self.audit),
        };
        let announce = AuditedPush {
            node: node.clone(),
            pusher,
            audit: Arc::clone(&self.audit),
        };
        let cluster = ClusterId::new("sim")?;
        // The node serves once it is registered, as the binary would.
        let mut heartbeater = Heartbeater::new(
            store.clone(),
            transport,
            cluster.clone(),
            profile,
            Arc::clone(&clock),
            self.config.heartbeat,
            ProposalIds::seeded(seed.wrapping_add(3)),
        )
        .with_local(registry.clone(), leadership.clone())
        .with_admin_port(NonZeroU16::new(PUSH_PORT).expect("a nonzero port"));
        self.follow_registrations(node.clone(), heartbeater.subscribe());
        heartbeater.register().await?;
        tokio::spawn(heartbeater.run());
        let coordinator = Coordinator::new(
            store,
            clock,
            leadership,
            placement,
            announce,
            CoordinatorConfig {
                cluster,
                retry: self.config.lease.retry,
                idle: self.config.interval,
            },
            ProposalIds::seeded(seed.wrapping_add(2)),
        );
        registry.set_waker(coordinator.waker());
        tokio::spawn(coordinator.run());
        Ok(shards)
    }
}

/// The rack label of the node at `position`: two racks, alternating.
fn rack(position: usize) -> Label {
    Label::new(format!("rack-{}", position % 2)).expect("a valid label")
}

/// The capacity each simulated disk registers.
const DISK_BYTES: u64 = 1 << 30;

/// Hands the registry's nodes to a [`Pusher`], at the port pushes arrive
/// on in the harness.
struct PushPeers(Pusher<TurmoilNetwork>);

impl PeerSink for PushPeers {
    fn set_peers(&self, peers: BTreeMap<NodeId, NodeAddress>) {
        let port = NonZeroU16::new(PUSH_PORT).expect("a nonzero port");
        self.0.set_peers(
            peers
                .into_iter()
                .map(|(node, address)| (node, NodeAddress::new(address.host().clone(), port)))
                .collect(),
        );
    }
}

/// A node the coordinator forgot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Forgotten {
    /// When, in simulated time.
    pub at: Duration,
    /// The coordinator that forgot it.
    pub by: NodeId,
    /// The node forgotten.
    pub node: NodeId,
}

/// What a coordinator's registry listed when it last planned.
#[derive(Clone, Debug)]
pub struct RegistryView {
    /// When, in simulated time.
    pub at: Duration,
    /// The coordinator.
    pub by: NodeId,
    /// Every registered node.
    pub nodes: Vec<NodeEntry>,
}

/// The node lifecycle around the stand-in placement, recording what the
/// registry lists and which nodes it forgets.
struct Observed<P> {
    node: NodeId,
    stale: bool,
    lifecycle: Lifecycle<P>,
    audit: Arc<Mutex<Audit>>,
}

impl<P: Placement> Placement for Observed<P> {
    fn begin_tenure(&mut self) {
        self.lifecycle.begin_tenure();
    }

    async fn plan<C: ControlStore>(
        &mut self,
        store: &C,
        proposals: &mut ProposalIds,
    ) -> Result<Option<ChangeSet>, ControlError> {
        let planned = self.lifecycle.plan(store, proposals).await;
        if !self.stale {
            lock(&self.audit).view = Some(RegistryView {
                at: elapsed(),
                by: self.node.clone(),
                nodes: self.lifecycle.registry().entries(),
            });
        }
        planned
    }

    fn applied(&mut self, change: &ChangeSet, applied: &Applied) {
        for (key, version) in &applied.written {
            if let (RegisterKind::Node(node), None) = (key.kind(), version) {
                lock(&self.audit).forgotten.push(Forgotten {
                    at: elapsed(),
                    by: self.node.clone(),
                    node,
                });
            }
        }
        self.lifecycle.applied(change, applied);
    }
}

impl<S> CoordinatedServices<S> {
    /// Records when `node` registers.
    fn follow_registrations(&self, node: NodeId, mut status: watch::Receiver<HeartbeatStatus>) {
        let audit = Arc::clone(&self.audit);
        tokio::spawn(async move {
            let mut seen = 0;
            while status.changed().await.is_ok() {
                let registrations = status.borrow_and_update().registrations;
                if registrations > seen {
                    seen = registrations;
                    lock(&audit)
                        .registered
                        .entry(node.clone())
                        .or_default()
                        .push(elapsed());
                }
            }
        });
    }

    /// Records when `node` hears of each new generation pushed to it.
    fn follow_pushes(&self, node: NodeId, hints: &ControlHints) {
        let (hints, audit) = (hints.clone(), Arc::clone(&self.audit));
        tokio::spawn(async move {
            let mut known = Generation::ZERO;
            loop {
                known = hints.newer_than(known).await;
                lock(&audit)
                    .pushed
                    .entry(node.clone())
                    .or_default()
                    .push((elapsed(), known));
            }
        });
    }

    /// Records the spans of simulated time in which `node`, in `life`,
    /// believes it is coordinator.
    fn follow_tenures(
        &self,
        node: NodeId,
        life: u64,
        clock: Arc<MonotonicClock>,
        mut leadership: watch::Receiver<Leadership>,
    ) {
        let audit = Arc::clone(&self.audit);
        tokio::spawn(async move {
            loop {
                let until = leadership.borrow_and_update().until();
                let until = until.map(|until| simulated(&clock, until));
                lock(&audit).tenure(&node, life, elapsed(), until);
                if leadership.changed().await.is_err() {
                    return;
                }
            }
        });
    }
}

impl Audit {
    /// Records that at `now`, `node` in `life` believes it is coordinator
    /// until `until`, or no longer believes it.
    fn tenure(&mut self, node: &NodeId, life: u64, now: Duration, until: Option<Duration>) {
        let open = self
            .tenures
            .iter_mut()
            .find(|tenure| tenure.node == *node && tenure.life == life && tenure.open);
        match (open, until) {
            (Some(tenure), Some(until)) => tenure.end = until,
            (Some(tenure), None) => {
                tenure.end = tenure.end.min(now);
                tenure.open = false;
            }
            (None, Some(until)) => self.tenures.push(Tenure {
                node: node.clone(),
                life,
                start: now,
                end: until,
                open: true,
            }),
            (None, None) => {}
        }
    }
}

/// Cuts a node off from the control store while it is coordinator.
struct Isolation<C> {
    store: ControlHandle,
    node: NodeId,
    at: Duration,
    duration: Duration,
    rates: FaultRates,
    leadership: watch::Receiver<Leadership>,
    clock: Arc<C>,
    audit: Arc<Mutex<Audit>>,
}

impl<C: Clock> Isolation<C> {
    /// Waits for the isolation's time, and cuts the node off if it is
    /// coordinator then. Only one node of a run is cut off, once.
    async fn run(self) {
        let Some(wait) = self.at.checked_sub(elapsed()) else {
            return;
        };
        tokio::time::sleep(wait).await;
        if !self.leadership.borrow().is_coordinator_at(self.clock.now()) {
            return;
        }
        {
            let mut audit = lock(&self.audit);
            if audit.isolated.is_some() {
                return;
            }
            audit.isolated = Some((self.node.clone(), elapsed()));
        }
        tracing::info!(node = %self.node, "the coordinator loses the control store");
        let _restore = Restore {
            store: self.store.clone(),
            rates: self.rates,
        };
        self.store.set_rates(FaultRates {
            unavailable: 1.0,
            ..self.rates
        });
        tokio::time::sleep(self.duration).await;
    }
}

/// Gives a node its control store back when an isolation ends, also when
/// the node crashes first.
struct Restore {
    store: ControlHandle,
    rates: FaultRates,
}

impl Drop for Restore {
    fn drop(&mut self) {
        self.store.set_rates(self.rates);
    }
}

/// The stand-in placement: every other plan moves the next register to its
/// next epoch, naming this node as primary.
struct EpochPlacement {
    node: NodeId,
    stale: bool,
    registers: u8,
    next: u8,
    rest: bool,
    /// The register and epoch of the change being made.
    planned: Option<(u8, u64)>,
    audit: Arc<Mutex<Audit>>,
}

impl Placement for EpochPlacement {
    async fn plan<C: ControlStore>(
        &mut self,
        store: &C,
        proposals: &mut ProposalIds,
    ) -> Result<Option<ChangeSet>, ControlError> {
        self.rest = !self.rest;
        if !self.rest {
            return Ok(None);
        }
        let register = self.next;
        self.next = (self.next + 1) % self.registers.max(1);
        let key = TypedKey::shard(&bucket(), ShardId::new(register));
        let current = read(store, &key).await?;
        let epoch = current.as_ref().map_or(1, |c| c.value.epoch.get() + 1);
        let document = ShardConfig {
            bucket_id: bucket(),
            shard: ShardId::new(register),
            epoch: Epoch::new(epoch),
            primary: self.node.clone(),
            members: vec![self.node.clone()],
            learners: Vec::new(),
            min_write_replicas: 1,
            replicas: 1,
            proposal_id: proposals.next_id(),
        };
        let change = match current {
            None => ChangeSet::new().create(&key, &document),
            Some(current) => ChangeSet::new().update(&key, &current.version, &document),
        };
        self.planned = Some((register, epoch));
        Ok(Some(change.map_err(|error| {
            ControlError::Rejected(format!("an invalid stand-in change: {error}"))
        })?))
    }

    fn applied(&mut self, _change: &ChangeSet, applied: &Applied) {
        let mut audit = lock(&self.audit);
        match self.planned.take() {
            Some((register, epoch)) if applied.is_complete() => audit.changes.push(Change {
                at: elapsed(),
                node: self.node.clone(),
                stale: self.stale,
                register,
                epoch,
            }),
            _ => audit.rejected += 1,
        }
    }
}

/// Pushes through a [`Pusher`], recording each announcement.
struct AuditedPush {
    node: NodeId,
    pusher: Pusher<TurmoilNetwork>,
    audit: Arc<Mutex<Audit>>,
}

impl Announce for AuditedPush {
    async fn announce(&self, generation: Generation) {
        lock(&self.audit)
            .announced
            .push((elapsed(), self.node.clone(), generation));
        self.pusher.announce(generation).await;
    }
}
