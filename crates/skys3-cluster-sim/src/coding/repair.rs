//! Repair after fragment holders are lost (plan M5-08, design §8.6): once
//! every object is coded and its replicas are dropped, one or two holders
//! are lost, for good or by losing their fragment disk, while clients may
//! keep reading, and the shard primary's [`Repairer`] must make every
//! stripe whole again.
//!
//! Every node that may lead the shard runs a repairer whose steps the
//! driver watches; crashes may strike the primary or a rebuilt fragment's
//! new holder at a chosen step. On top of the coding checks, the driver
//! checks:
//!
//! - **Durability.** No layout a serving primary applied names a fragment
//!   that its node, up and on the disk the fragment was written to, no
//!   longer holds, or whose ID now names another fragment.
//! - **Priority.** Within each pass of each repairer, stripes are started
//!   in order of fragments lost, most first.
//! - **Bandwidth.** Each repairer's transfers start no sooner than the cap
//!   allows: between the starts of two transfers of one repairer, at least
//!   the bytes of those between them at `bytes_per_second`, give or take a
//!   tick.
//! - **Time.** Every stripe of every member's layout names `k + m`
//!   fragments held by nodes that are not lost, within `bound` of the
//!   losses: the repair time the report gives.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use skys3_ec::read::ReadFuture;
use skys3_ec::repair::RepairBug;
use skys3_ec::{
    Attempts, FragmentReadClient, FragmentRequest, FragmentSource, RepairEvent, RepairSettings,
    RepairStep, Repairer,
};
use skys3_io::{BlockingPool, SimMount};
use skys3_net::TurmoilNetwork;
use skys3_shard::Shard;

use super::{Crash, CrashKind, CrashTarget, Driver, MEMBERS, NODES, World, Writer, lock};

/// How long a node lost for good, or still to join, stays down: past the
/// end of any run.
pub(super) const FOREVER: Duration = Duration::from_secs(1 << 40);

/// How a run loses holders and repairs them.
#[derive(Debug, Clone)]
pub struct RepairConfig {
    /// The losses, each of a distinct holder: lost for good ones are drawn
    /// from the nodes outside the shard, `n3` to `n5`, and lost disks from
    /// `n1` to `n5`.
    pub losses: Vec<Loss>,
    /// A crash during the repairs, if any.
    pub crash: Option<RepairCrash>,
    /// `repair_bytes_per_second_per_node`.
    pub bytes_per_second: u64,
    /// `fragment_repair_after_seconds`.
    pub lost_after: Duration,
    /// How long after the losses every stripe must be whole again.
    pub bound: Duration,
    /// How long each rebuilt fragment's write waits once its node made it
    /// durable, before the repairer learns its ID: what makes repairs
    /// outlast `fragment_orphan_after_seconds`.
    pub ack_delay: Duration,
    /// A bug seeded into every repairer.
    pub bug: Option<RepairBug>,
    /// How many objects, the first in key order, are retagged through
    /// `n0`'s gateway once every object is coded and before the losses: a
    /// retag moves the entry's version past its coded layout's, which the
    /// fragments' headers keep naming. Only a run with reads has gateways.
    pub retag: usize,
}

impl Default for RepairConfig {
    fn default() -> Self {
        Self {
            losses: vec![Loss::ForGood],
            crash: None,
            bytes_per_second: 64 * 1024,
            lost_after: Duration::from_secs(1),
            bound: Duration::from_secs(15),
            ack_delay: Duration::ZERO,
            bug: None,
            retag: 0,
        }
    }
}

/// How a holder is lost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Loss {
    /// The node loses power and never comes back.
    ForGood,
    /// The node's fragment disk fails; the node restarts at once with an
    /// empty one.
    Disk,
}

/// Where in a stripe's repair a crash strikes: the first time a repairer
/// reports the step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairPoint {
    /// The repair started.
    Started,
    /// The rebuilt fragments were placed, and none is written yet.
    Placed,
    /// A rebuilt fragment is durable.
    Written,
    /// The `EC_RELOCATE` is sequenced, not yet committed.
    Appended,
    /// The `EC_RELOCATE` committed.
    Committed,
}

impl RepairPoint {
    /// Every point, in the order a repair reaches them.
    pub const ALL: [RepairPoint; 5] = [
        RepairPoint::Started,
        RepairPoint::Placed,
        RepairPoint::Written,
        RepairPoint::Appended,
        RepairPoint::Committed,
    ];

    fn reached(self, step: &RepairStep) -> bool {
        matches!(
            (self, step),
            (RepairPoint::Started, RepairStep::Started { .. })
                | (RepairPoint::Placed, RepairStep::Placed { .. })
                | (RepairPoint::Written, RepairStep::Written { .. })
                | (RepairPoint::Appended, RepairStep::Appended)
                | (RepairPoint::Committed, RepairStep::Committed { .. })
        )
    }
}

/// Whom a crash during the repairs strikes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairTarget {
    /// The repairing primary.
    Primary,
    /// The new holder of the fragment the repair placed first. A power cut
    /// at its next sync is armed as the next repair write to it leaves.
    NewHolder,
}

/// A crash during the repairs: `kind` strikes `target` at `point`, which
/// restarts `downtime` later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepairCrash {
    /// The step.
    pub point: RepairPoint,
    /// The node.
    pub target: RepairTarget,
    /// How.
    pub kind: CrashKind,
    /// How long the node stays down.
    pub downtime: Duration,
}

/// What the repairs did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepairOutcome {
    /// The nodes lost, and how.
    pub lost: Vec<(String, Loss)>,
    /// `EC_RELOCATE`s that committed and were applied.
    pub relocated: usize,
    /// Repairs started.
    pub started: usize,
    /// The most fragments one repair started for.
    pub most_lost: usize,
    /// Bytes the repairers read and wrote.
    pub bytes: u64,
    /// From the losses until every stripe was whole again.
    pub repair_time: Duration,
    /// Objects retagged before the losses.
    pub retagged: usize,
    /// `EC_RELOCATE`s that committed and were applied for those objects.
    pub retagged_relocated: usize,
}

/// What the repairers did, as the driver sees it.
#[derive(Default)]
pub(super) struct RepairLog {
    /// Every repair step, a move's too, with the node and life of its
    /// repairer.
    pub(super) events: std::sync::Mutex<Vec<(usize, u64, RepairEvent)>>,
    /// Every transfer's start and bytes, by the node and life of its
    /// repairer.
    transfers: std::sync::Mutex<Transfers>,
    /// The objects retagged before the losses, once every retag is done.
    retagged: std::sync::Mutex<Option<Vec<String>>>,
}

/// The start and bytes of each transfer, by the node and life of its
/// repairer.
type Transfers = BTreeMap<(usize, u64), Vec<(Duration, u64)>>;

impl RepairLog {
    /// Records a transfer of `bytes` by node `node`'s repairer of life
    /// `life`, starting now.
    pub(super) fn transfer(&self, node: usize, life: u64, bytes: u64) {
        let now = turmoil::since_epoch().unwrap_or_default();
        lock(&self.transfers)
            .entry((node, life))
            .or_default()
            .push((now, bytes));
    }

    /// Whether the attempt of a repair or a move committed and relocated.
    pub(super) fn relocated(&self, attempt: skys3_types::AttemptId) -> bool {
        lock(&self.events).iter().any(|(_, _, event)| {
            event.attempt == attempt
                && matches!(
                    event.step,
                    RepairStep::Committed {
                        relocated: true,
                        ..
                    }
                )
        })
    }
}

/// A repairer's fragment reads, counted as its transfers, but for the
/// one-byte checks.
struct Counted {
    client: FragmentReadClient<TurmoilNetwork, SimMount>,
    world: Arc<World>,
    node: usize,
    life: u64,
}

impl std::fmt::Debug for Counted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Counted")
            .field("node", &self.node)
            .field("life", &self.life)
            .finish_non_exhaustive()
    }
}

impl FragmentSource for Counted {
    fn read(&self, request: FragmentRequest) -> ReadFuture<'_> {
        let len = request.range.end - request.range.start;
        if len > 1 {
            self.world.repair.transfer(self.node, self.life, len);
        }
        self.client.read(request)
    }
}

/// Starts node `index`'s repairer of life `life` on its replica `shard`,
/// sharing the node's attempt tracker `attempts`.
pub(super) fn start(
    world: &Arc<World>,
    index: usize,
    life: u64,
    shard: Shard<SimMount>,
    client: FragmentReadClient<TurmoilNetwork, SimMount>,
    writer: Writer,
    attempts: Attempts,
) {
    let Some(config) = &world.run.repair else {
        return;
    };
    let source = Counted {
        client,
        world: Arc::clone(world),
        node: index,
        life,
    };
    let settings = RepairSettings {
        interval: Duration::from_millis(100),
        lost_after: config.lost_after,
        checks_per_node: 4,
        replans: 3,
        bytes_per_second: config.bytes_per_second,
        // Moves only in a run that rebalances.
        moves_per_pass: world.run.rebalance.as_ref().map_or(0, |r| r.moves_per_pass),
    };
    let observed = Arc::clone(world);
    let planned = Arc::clone(world);
    let repairer = Repairer::new(
        shard,
        Arc::new(source),
        Arc::new(writer),
        super::moves::planner_source(planned),
        BlockingPool::inline("repair"),
        settings,
    )
    .with_attempts(attempts)
    .with_bug(config.bug)
    .with_observer(Arc::new(move |event: &RepairEvent| {
        super::moves::observe(&observed, event);
        lock(&observed.repair.events).push((index, life, event.clone()));
    }));
    tokio::spawn(repairer.run());
}

/// Retags the first `count` objects in key order through `n0`'s gateway,
/// each until it is done, and records them once all are.
fn start_retags(sim: &mut turmoil::Sim<'_>, world: &Arc<World>, count: usize) {
    let world = Arc::clone(world);
    sim.client("retagger", async move {
        let keys: Vec<String> = lock(&world.expected).keys().take(count).cloned().collect();
        let mut done = Vec::new();
        for key in keys {
            for _ in 0..40 {
                match super::reads::retag("n0", &key, "before-the-losses").await {
                    Ok(true) => {
                        done.push(key.clone());
                        break;
                    }
                    Ok(false) => tokio::time::sleep(Duration::from_millis(100)).await,
                    Err(error) => return Err(format!("a retag of {key}: {error}").into()),
                }
            }
        }
        *lock(&world.repair.retagged) = Some(done);
        Ok(())
    });
}

/// The repair phase of the driver.
#[derive(Debug, Default)]
pub(super) struct Phase {
    /// When the holders were lost.
    lost_at: Option<Duration>,
    /// When every stripe was whole again.
    whole_at: Option<Duration>,
    /// The crash to strike, until it does.
    crash: Option<RepairCrash>,
    /// How many repair events the crash point was looked for in.
    seen: usize,
    /// The nodes lost, and how.
    lost: Vec<(usize, Loss)>,
    /// Whether the retags before the losses started.
    retagging: bool,
    /// Why the repairs had not ended at the last check.
    unfinished: String,
}

impl Driver<'_> {
    /// Whether the repairs keep the run going: once the objects settle it
    /// retags the objects the run asks for, then loses the holders once
    /// the retags are done, and the run goes on until every stripe is whole
    /// again, or the bound passes.
    pub(super) fn repair_pending(
        &mut self,
        sim: &mut turmoil::Sim<'_>,
        world: &Arc<World>,
        now: Duration,
    ) -> bool {
        let Some(config) = self.world.run.repair.clone() else {
            return false;
        };
        if self.repair.lost_at.is_none() {
            if config.retag > 0 && self.world.reads.is_some() {
                if !self.repair.retagging {
                    self.repair.retagging = true;
                    start_retags(sim, world, config.retag);
                }
                if lock(&self.world.repair.retagged).is_none() {
                    return true;
                }
            }
            self.lose_holders(sim, now, &config);
            self.start_moves(now);
            return true;
        }
        // Repairs that do not end within the bound end the run, which the
        // outcome then reports.
        let lost_at = self.repair.lost_at.unwrap_or_default();
        self.repair.whole_at.is_none() && now <= lost_at + config.bound
    }

    /// Notes when every stripe is whole again after the losses, and, in a
    /// run that rebalances, the drains and the balance are done.
    pub(super) fn repair_progress(&mut self, now: Duration) {
        if self.repair.lost_at.is_none() || self.repair.whole_at.is_some() {
            return;
        }
        match self.whole().and_then(|()| self.rebalanced()) {
            Ok(()) => self.repair.whole_at = Some(now),
            Err(why) => self.repair.unfinished = why,
        }
    }

    /// The nodes that hold each stripe of each object, by a member's
    /// layouts.
    fn stripe_holders(&self) -> Vec<BTreeSet<usize>> {
        let views = self.views();
        let keys: Vec<String> = lock(&self.world.expected).keys().cloned().collect();
        let Some(view) = self
            .world
            .members()
            .into_iter()
            .find_map(|n| views[n].clone())
        else {
            return Vec::new();
        };
        let Ok(reader) = view.index.read() else {
            return Vec::new();
        };
        let mut holders = Vec::new();
        for key in &keys {
            let Ok(Some(entry)) = reader.entry(&self.world.shard, key) else {
                continue;
            };
            let Some(coded) = entry.object.and_then(|object| object.coded) else {
                continue;
            };
            for stripe in &coded.stripes {
                let nodes = stripe.fragments().iter();
                holders.push(
                    nodes
                        .filter_map(|location| self.world.position(&location.node))
                        .collect(),
                );
            }
        }
        holders
    }

    /// The holders to lose, one for each of `losses`, drawn from the seed:
    /// lost for good ones from the nodes outside the shard, lost disks from
    /// any node but the first. Of two, the first pair that leaves some
    /// stripes missing two fragments and others one, else two, if any.
    fn holders_to_lose(&self, losses: &[Loss]) -> Vec<(usize, Loss)> {
        let mut rng = StdRng::seed_from_u64(self.world.seed ^ 0x5245_5041_4952);
        let mut outside: Vec<usize> = (MEMBERS..NODES).collect();
        let mut any: Vec<usize> = (1..NODES).collect();
        outside.shuffle(&mut rng);
        any.shuffle(&mut rng);
        let pool = |loss: &Loss| match loss {
            Loss::ForGood => &outside,
            Loss::Disk => &any,
        };
        if let [first, second] = losses {
            let holders = self.stripe_holders();
            let lost = |a: usize, b: usize, count: usize| {
                holders
                    .iter()
                    .filter(|nodes| {
                        usize::from(nodes.contains(&a)) + usize::from(nodes.contains(&b)) == count
                    })
                    .count()
            };
            let pairs = pool(first)
                .iter()
                .flat_map(|&a| pool(second).iter().map(move |&b| (a, b)))
                .filter(|(a, b)| a != b);
            let mut best: Option<((bool, bool), (usize, usize))> = None;
            for (a, b) in pairs {
                let score = (lost(a, b, 2) > 0 && lost(a, b, 1) > 0, lost(a, b, 2) > 0);
                if best.is_none_or(|(top, _)| score > top) {
                    best = Some((score, (a, b)));
                }
            }
            if let Some((_, (a, b))) = best {
                return vec![(a, *first), (b, *second)];
            }
        }
        let mut taken: Vec<(usize, Loss)> = Vec::new();
        for loss in losses {
            if let Some(&node) = pool(loss)
                .iter()
                .find(|node| taken.iter().all(|(taken, _)| taken != *node))
            {
                taken.push((node, *loss));
            }
        }
        taken
    }

    /// Loses the run's holders.
    fn lose_holders(&mut self, sim: &mut turmoil::Sim<'_>, now: Duration, config: &RepairConfig) {
        let mut rng = StdRng::seed_from_u64(self.world.seed ^ 0x444f_574e);
        for (node, loss) in self.holders_to_lose(&config.losses) {
            self.repair.lost.push((node, loss));
            let slot = &self.world.nodes[node];
            match &loss {
                Loss::ForGood => {
                    self.world.lost[node].store(true, Ordering::SeqCst);
                    self.crash(sim, now, node, true, FOREVER);
                }
                Loss::Disk => {
                    self.lose_disk(node);
                    let down = Duration::from_millis(rng.random_range(20..300));
                    self.crash(sim, now, node, true, down);
                }
            }
            self.report
                .crashes
                .push((slot.id.to_string(), CrashKind::PowerLoss));
        }
        self.repair.lost_at = Some(now);
        self.repair.crash = config.crash;
        self.repair.seen = lock(&self.world.repair.events).len();
    }

    /// Strikes the repair crash once its point is reached.
    pub(super) fn repair_crash(&mut self, now: Duration) {
        let Some(crash) = self.repair.crash else {
            return;
        };
        let events = lock(&self.world.repair.events);
        let reached = events[self.repair.seen..]
            .iter()
            .find(|(_, _, event)| crash.point.reached(&event.step))
            .cloned();
        self.repair.seen = events.len();
        let placed = events
            .iter()
            .rev()
            .find_map(|(_, _, event)| match &event.step {
                RepairStep::Placed { nodes } => nodes.first().map(|(_, node)| node.clone()),
                _ => None,
            });
        drop(events);
        let Some((primary, _, event)) = reached else {
            return;
        };
        let node = match crash.target {
            RepairTarget::Primary => Some(primary),
            RepairTarget::NewHolder => placed.and_then(|node| self.world.position(&node)),
        };
        self.repair.crash = None;
        let Some(node) = node.filter(|node| !self.world.lost[*node].load(Ordering::SeqCst)) else {
            return;
        };
        tracing::debug!(?event, node, "the repair crash point is reached");
        if crash.target == RepairTarget::NewHolder && crash.kind == CrashKind::PowerAtNextSync {
            // The new holder's next sync may be another's than the rebuilt
            // fragment's, which waits for the bandwidth cap first: the cut
            // is armed as the fragment's write leaves.
            let slot = &self.world.nodes[node];
            self.world.cut_at_repair_write[node].store(true, Ordering::SeqCst);
            self.cut[node] = false;
            self.report
                .crashes
                .push((slot.id.to_string(), CrashKind::PowerAtNextSync));
            return;
        }
        let crash = Crash {
            delay: Duration::ZERO,
            target: CrashTarget::Primary,
            kind: crash.kind,
            downtime: crash.downtime,
        };
        self.due.push((now, node, crash));
    }

    /// Checks that every member's layout of every object names `k + m`
    /// fragments its nodes hold and that are not lost; or says why not.
    pub(super) fn whole(&self) -> Result<(), String> {
        let views = self.views();
        let shard = &self.world.shard;
        let keys: Vec<String> = lock(&self.world.expected).keys().cloned().collect();
        let written = lock(&self.world.fragments);
        for member in self.world.members() {
            let Some(view) = &views[member] else {
                return Err(format!("member n{member} is down"));
            };
            let reader = view.index.read().map_err(|e| e.to_string())?;
            for key in &keys {
                let entry = reader.entry(shard, key).map_err(|e| e.to_string())?;
                let Some(coded) = entry.and_then(|e| e.object).and_then(|o| o.coded) else {
                    return Err(format!("member n{member} has not coded {key}"));
                };
                for stripe in &coded.stripes {
                    for (index, location) in stripe.fragments().iter().enumerate() {
                        let named = (key.as_str(), coded.version, stripe.number(), index);
                        let current = self.world.generation_of(&location.node);
                        let on_disk =
                            self.world.named_generation(&written, location, named) == Some(current);
                        let held = on_disk
                            && self.world.position(&location.node).is_some_and(|n| {
                                !self.world.lost[n].load(Ordering::SeqCst)
                                    && views[n].as_ref().is_some_and(|view| {
                                        view.fragments.stores()[0].len(location.fragment).is_some()
                                    })
                            });
                        if !held {
                            return Err(format!(
                                "{key} stripe {} names {} on {}, which is not held",
                                stripe.number(),
                                location.fragment,
                                location.node
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Checks that no layout a serving primary applied names a fragment
    /// its node lost while on the disk it was written to.
    pub(super) fn check_held(&self) -> Result<(), String> {
        if self.world.run.repair.is_none() {
            return Ok(());
        }
        let views = self.views();
        let shard = &self.world.shard;
        let keys: Vec<String> = lock(&self.world.expected).keys().cloned().collect();
        let written = lock(&self.world.fragments);
        let reclaimed = lock(&self.world.reclaimed);
        for view in views.iter().flatten() {
            let leads = view.shard.as_ref().is_some_and(|shard| {
                matches!(
                    shard.role(),
                    skys3_shard::Role::Primary | skys3_shard::Role::Alone
                ) && shard.is_serving()
            });
            if !leads {
                continue;
            }
            let reader = view.index.read().map_err(|e| e.to_string())?;
            for key in &keys {
                let entry = reader.entry(shard, key).map_err(|e| e.to_string())?;
                let Some(coded) = entry.and_then(|e| e.object).and_then(|o| o.coded) else {
                    continue;
                };
                for stripe in &coded.stripes {
                    for (index, location) in stripe.fragments().iter().enumerate() {
                        let Some(n) = self.world.position(&location.node) else {
                            continue;
                        };
                        // A fragment on a disk its node lost since is the
                        // repairs' to rebuild.
                        let named = (key.as_str(), coded.version, stripe.number(), index);
                        let current = self.world.generation[n].load(Ordering::SeqCst);
                        let generation = self.world.named_generation(&written, location, named);
                        let other = (location.node.clone(), 0, location.fragment);
                        if generation.is_none() && current == 0 && written.contains_key(&other) {
                            return Err(format!(
                                "the committed layout of {key} names fragment {} of stripe {} \
                                 on {}, which holds another fragment",
                                location.fragment,
                                stripe.number(),
                                location.node
                            ));
                        }
                        if generation != Some(current) {
                            continue;
                        }
                        let fragment = (location.node.clone(), current, location.fragment);
                        let node_view = match &views[n] {
                            Some(view) if !reclaimed.contains(&fragment) => view,
                            _ => continue,
                        };
                        let holds = node_view.fragments.stores()[0]
                            .len(location.fragment)
                            .is_some();
                        if !holds {
                            return Err(format!(
                                "the committed layout of {key} names fragment {} of stripe {} \
                                 on {}, which lost it",
                                location.fragment,
                                stripe.number(),
                                location.node
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// The outcome of the repairs, once every stripe is whole again within
    /// the bound, after checking the order and the pace of the repairs.
    pub(super) fn repair_outcome(&self) -> Result<Option<RepairOutcome>, String> {
        let Some(config) = &self.world.run.repair else {
            return Ok(None);
        };
        let (Some(lost_at), Some(whole_at)) = (self.repair.lost_at, self.repair.whole_at) else {
            return Err(format!(
                "the repairs did not end within {:?}: {}",
                config.bound, self.repair.unfinished
            ));
        };
        let repair_time = whole_at - lost_at;
        if repair_time > config.bound {
            return Err(format!(
                "the stripes were whole again {repair_time:?} after the losses, past {:?}",
                config.bound
            ));
        }
        let retagged = lock(&self.world.repair.retagged)
            .clone()
            .unwrap_or_default();
        let events = lock(&self.world.repair.events);
        let moves = super::moves::move_attempts(&events);
        let mut outcome = RepairOutcome {
            lost: self
                .repair
                .lost
                .iter()
                .map(|(node, loss)| (self.world.nodes[*node].id.to_string(), *loss))
                .collect(),
            repair_time,
            retagged: retagged.len(),
            ..RepairOutcome::default()
        };
        // Within a pass, stripes that lost more fragments start first.
        let mut last: BTreeMap<(usize, u64, u64), (usize, String)> = BTreeMap::new();
        for (node, life, event) in events.iter() {
            match &event.step {
                RepairStep::Started { pass, lost } => {
                    outcome.started += 1;
                    outcome.most_lost = outcome.most_lost.max(lost.len());
                    let what = format!("{} stripe {}", event.key, event.stripe);
                    if let Some((before, earlier)) = last.get(&(*node, *life, *pass))
                        && *before < lost.len()
                    {
                        return Err(format!(
                            "n{node} repaired {earlier}, which lost {before} fragments, before \
                             {what}, which lost {}",
                            lost.len()
                        ));
                    }
                    last.insert((*node, *life, *pass), (lost.len(), what));
                }
                RepairStep::Committed {
                    relocated: true, ..
                } if !moves.contains(&event.attempt) => {
                    outcome.relocated += 1;
                    if retagged.contains(&event.key) {
                        outcome.retagged_relocated += 1;
                    }
                }
                _ => {}
            }
        }
        drop(events);
        // Each repairer's transfers keep to the cap: between the starts of
        // two transfers, at least the bytes of those started before the
        // second at the cap, give or take a tick.
        let transfers = lock(&self.world.repair.transfers);
        let rate = u128::from(config.bytes_per_second.max(1));
        let tick = Duration::from_millis(2);
        for ((node, _), starts) in transfers.iter() {
            for (i, &(from, _)) in starts.iter().enumerate() {
                let mut bytes: u64 = 0;
                for j in i + 1..starts.len() {
                    bytes += starts[j - 1].1;
                    let nanos = u128::from(bytes) * 1_000_000_000 / rate;
                    let least = Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX));
                    let at = starts[j].0;
                    if at + tick < from + least {
                        return Err(format!(
                            "n{node}'s repairs started {bytes} bytes of transfers within {:?},                              past the bandwidth cap of {} bytes a second",
                            at - from,
                            config.bytes_per_second
                        ));
                    }
                }
            }
            outcome.bytes += starts.iter().map(|(_, len)| len).sum::<u64>();
        }
        Ok(Some(outcome))
    }
}

/// Whether node `node` is known to hold nothing of what it held before
/// it went down: lost for good, or restarting on a new fragment disk.
pub(super) fn holds_nothing(world: &World, node: usize) -> bool {
    world.lost[node].load(Ordering::SeqCst) || world.disk_gone[node].load(Ordering::SeqCst)
}

impl Driver<'_> {
    /// Fails node `node`'s fragment disk under it; it starts on an empty
    /// one at its next life.
    pub(super) fn lose_disk(&mut self, node: usize) {
        let slot = &self.world.nodes[node];
        slot.fragment_disk().crash();
        self.world.lose_disk[node].store(true, Ordering::SeqCst);
        self.world.disk_gone[node].store(true, Ordering::SeqCst);
        self.world.generation[node].fetch_add(1, Ordering::SeqCst);
    }
}
