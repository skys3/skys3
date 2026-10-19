//! Fragment moves (plan M5-09, design §8.3): once every object is coded,
//! nodes join the cluster or are drained, at the moment the run's holders
//! are lost (see [`RepairConfig`](super::RepairConfig)) and while clients
//! may keep reading, and the shard primary's repairer moves fragments onto
//! the new nodes and off the drained ones, after its repairs.
//!
//! A node that joins starts on empty disks, and placement lists it once
//! its software is up. A drained node keeps running, its registration
//! marked departing. Crashes may strike the primary, or the old or the new
//! holder of a moved fragment, at a chosen step of a move. On top of the
//! coding and repair checks, the driver checks:
//!
//! - **Failure domains.** Throughout, no stripe of a member's layout holds
//!   more than `m` fragments in one failure domain of the run's level.
//! - **Priority.** No pass of a repairer both repairs and moves.
//! - **The end.** Within the repair bound, besides every stripe being
//!   whole again: no layout names a fragment on a drained node, every node
//!   that joined holds fragments, and the live nodes' shares of fragment
//!   bytes are within two fragments of each other.
//!
//! The checks that every stripe keeps `k` readable fragments, that no
//! committed layout names a reclaimed fragment, and that no layout names a
//! fragment its node lost are the coding and repair checks, which every
//! move must keep too.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use skys3_config::FailureDomain;
use skys3_coord::{Candidate, Domain, FragmentPlanner, GeometryPolicy, NodeState, Topology};
use skys3_ec::{PlannerSource, RepairEvent, RepairStep};
use skys3_index::IndexReader;
use skys3_log::ShardRef;
use skys3_types::{AttemptId, Label, NodeId};

use super::{CodingConfig, Crash, CrashKind, CrashTarget, Driver, NODES, World, lock, planner};

/// Nodes that join or are drained, and how the moves are hurt.
#[derive(Debug, Clone)]
pub struct RebalanceConfig {
    /// How many nodes join, `n6` on, on empty disks.
    pub joins: usize,
    /// How many of `n1` to `n5`, drawn from the seed, are drained.
    pub drains: usize,
    /// Whether placement keeps fragments apart by rack (`failure_domain =
    /// "rack"`): the six first nodes in three racks of two, the first node
    /// to join in the first rack, and the others each in a new one.
    pub racks: bool,
    /// The most fragments a repairer's pass moves.
    pub moves_per_pass: usize,
    /// A crash during the moves, if any.
    pub crash: Option<MoveCrash>,
    /// A bug seeded into the cluster around the moves.
    pub bug: Option<RebalanceBug>,
}

impl Default for RebalanceConfig {
    fn default() -> Self {
        Self {
            joins: 0,
            drains: 0,
            racks: false,
            moves_per_pass: 8,
            crash: None,
            bug: None,
        }
    }
}

/// A bug of the cluster around its moves, seeded to show that the checks
/// catch it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebalanceBug {
    /// A moved fragment's old holder drops it as soon as the mover has read
    /// it, before the move commits: retirement before publication.
    RetireEarly,
    /// The repairers plan at the `node` level in a run that keeps fragments
    /// apart by rack: a wiring mistake.
    NodeLevel,
}

/// Where in a move a crash strikes: the first time a repairer reports the
/// step of a move.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MovePoint {
    /// The move started.
    Started,
    /// The fragment was read whole from its old holder.
    Read,
    /// The fragment is about to be written to its new holder.
    Placed,
    /// The copy is durable on the new holder.
    Written,
    /// The `EC_RELOCATE` is sequenced, not yet committed.
    Appended,
    /// The `EC_RELOCATE` committed.
    Committed,
}

impl MovePoint {
    /// Every point, in the order a move reaches them.
    pub const ALL: [MovePoint; 6] = [
        MovePoint::Started,
        MovePoint::Read,
        MovePoint::Placed,
        MovePoint::Written,
        MovePoint::Appended,
        MovePoint::Committed,
    ];

    fn reached(self, step: &RepairStep) -> bool {
        matches!(
            (self, step),
            (MovePoint::Started, RepairStep::MoveStarted { .. })
                | (MovePoint::Read, RepairStep::Read { .. })
                | (MovePoint::Placed, RepairStep::Placed { .. })
                | (MovePoint::Written, RepairStep::Written { .. })
                | (MovePoint::Appended, RepairStep::Appended)
                | (MovePoint::Committed, RepairStep::Committed { .. })
        )
    }
}

/// Whom a crash during the moves strikes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoveTarget {
    /// The moving primary.
    Primary,
    /// The node the fragment moves from.
    OldHolder,
    /// The node the fragment moves to. A power cut at its next sync is
    /// armed as the next repairer's write to it leaves.
    NewHolder,
}

/// A crash during the moves: `kind` strikes `target` at `point` of the
/// first move to reach it, which restarts `downtime` later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MoveCrash {
    /// The step.
    pub point: MovePoint,
    /// The node.
    pub target: MoveTarget,
    /// How.
    pub kind: CrashKind,
    /// How long the node stays down.
    pub downtime: Duration,
}

/// What the moves did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MoveOutcome {
    /// The nodes that joined.
    pub joined: Vec<String>,
    /// The nodes drained.
    pub drained: Vec<String>,
    /// Moves started.
    pub started: usize,
    /// Moves whose `EC_RELOCATE` committed and was applied.
    pub moved: usize,
    /// Fragments the old holder dropped early, under
    /// [`RebalanceBug::RetireEarly`].
    pub retired: usize,
    /// How many fragments the layouts place on each node at the end.
    pub spread: Vec<(String, usize)>,
}

/// What the moves did, as the nodes see it.
#[derive(Debug, Default)]
pub(super) struct MoveLog {
    /// Fragments dropped early under [`RebalanceBug::RetireEarly`].
    retired: std::sync::atomic::AtomicUsize,
}

/// The rebalancing phase of the driver.
#[derive(Debug, Default)]
pub(super) struct Phase {
    /// Whether the joins and drains started.
    started: bool,
    /// The nodes that join, once each is up and listed.
    joining: Vec<usize>,
    /// The nodes that joined.
    joined: Vec<usize>,
    /// The nodes drained.
    drained: Vec<usize>,
    /// The crash to strike, until it does.
    crash: Option<MoveCrash>,
    /// How many repair events the crash point was looked for in.
    seen: usize,
}

/// The failure-domain level of a run.
pub(super) fn level(run: &CodingConfig) -> FailureDomain {
    if run.rebalance.as_ref().is_some_and(|r| r.racks) {
        FailureDomain::Rack
    } else {
        FailureDomain::Node
    }
}

/// Node `n` as placement first sees it: live, with a terabyte, and in its
/// rack if the run has racks.
fn candidate(run: &CodingConfig, n: usize) -> Candidate {
    let rack = (level(run) == FailureDomain::Rack).then(|| {
        let rack = match n.checked_sub(NODES) {
            None => n / 2,
            Some(0) => 0,
            Some(joined) => NODES / 2 + joined - 1,
        };
        Label::new(format!("rack-{rack}")).expect("a valid label")
    });
    Candidate {
        node: format!("n{n}").parse().expect("a valid node ID"),
        zone: None,
        rack,
        capacity_bytes: 1 << 40,
        state: NodeState::Live,
        shards: 0,
        primaries: 0,
    }
}

/// The nodes placement knows when a run starts: the six first.
pub(super) fn initial_topology(run: &CodingConfig) -> Vec<Candidate> {
    (0..NODES).map(|n| candidate(run, n)).collect()
}

/// The planner a repairer takes: the cluster's, or one at the `node`
/// level under [`RebalanceBug::NodeLevel`].
pub(super) fn planner_source(world: Arc<World>) -> PlannerSource {
    let bug = world.run.rebalance.as_ref().and_then(|r| r.bug);
    Arc::new(move || {
        let planner = planner(&world);
        if bug != Some(RebalanceBug::NodeLevel) {
            return planner;
        }
        let candidates = planner.topology().candidates().cloned();
        let policy = GeometryPolicy::from_config(&world.run.ec).expect("a valid policy");
        FragmentPlanner::new(Topology::new(FailureDomain::Node, candidates), policy)
    })
}

/// Every key of `shard` with a coded layout in `reader`: the objects the
/// run wrote, and the copies its readers made, which are coded too.
fn coded_keys(reader: &IndexReader, shard: &ShardRef) -> Result<BTreeSet<String>, String> {
    let mut keys = BTreeSet::new();
    for node in reader.fragment_nodes(shard).map_err(|e| e.to_string())? {
        let rows = reader
            .fragments_on(shard, &node)
            .map_err(|e| e.to_string())?;
        keys.extend(rows.into_iter().map(|(row, _)| row.key));
    }
    Ok(keys)
}

/// The attempts of `events` that started as moves.
pub(super) fn move_attempts(events: &[(usize, u64, RepairEvent)]) -> BTreeSet<AttemptId> {
    events
        .iter()
        .filter(|(_, _, event)| matches!(event.step, RepairStep::MoveStarted { .. }))
        .map(|(_, _, event)| event.attempt)
        .collect()
}

/// Sees a repairer's step before the driver does: under
/// [`RebalanceBug::RetireEarly`], the old holder of a moved fragment drops
/// it once the mover has read it.
pub(super) fn observe(world: &Arc<World>, event: &RepairEvent) {
    let early = world
        .run
        .rebalance
        .as_ref()
        .is_some_and(|r| r.bug == Some(RebalanceBug::RetireEarly));
    let RepairStep::Read { index, node } = &event.step else {
        return;
    };
    let moving = lock(&world.repair.events).iter().any(|(_, _, started)| {
        started.attempt == event.attempt && matches!(started.step, RepairStep::MoveStarted { .. })
    });
    if !early || !moving {
        return;
    }
    let Some(n) = world.position(node) else {
        return;
    };
    // The fragment the layout locates there, which the move copies.
    let Some(fragment) = world.members().into_iter().find_map(|member| {
        let view = lock(&world.nodes[member].view).clone()?;
        let reader = view.index.read().ok()?;
        let entry = reader.entry(&world.shard, &event.key).ok()??;
        let coded = entry.object?.coded?;
        let stripe = coded.stripes.get(usize::try_from(event.stripe).ok()?)?;
        stripe
            .fragments()
            .get(usize::from(*index))
            .filter(|location| location.node == *node)
            .map(|location| location.fragment)
    }) else {
        return;
    };
    let Some(view) = lock(&world.nodes[n].view).clone() else {
        return;
    };
    let generation = world.generation_of(node);
    lock(&world.reclaimed).insert((node.clone(), generation, fragment));
    world.moves.retired.fetch_add(1, Ordering::SeqCst);
    let store = view.fragments.stores()[0].clone();
    tokio::spawn(async move {
        let _ = store.reclaim(fragment).await;
    });
}

impl Driver<'_> {
    /// Starts the joins and drains, with the losses of the repairs.
    pub(super) fn start_moves(&mut self, now: Duration) {
        let Some(config) = self.world.run.rebalance.clone() else {
            return;
        };
        self.moves.started = true;
        self.moves.crash = config.crash;
        self.moves.seen = lock(&self.world.repair.events).len();
        let mut rng = StdRng::seed_from_u64(self.world.seed ^ 0x0044_5241_494e);
        let mut pool: Vec<usize> = (1..NODES).collect();
        pool.shuffle(&mut rng);
        let mut topology = lock(&self.world.topology);
        for &n in pool.iter().take(config.drains) {
            topology[n].state = NodeState::Departing;
            self.moves.drained.push(n);
        }
        drop(topology);
        for n in NODES..self.world.nodes.len() {
            // It starts at the next advance, and is listed once it is up.
            self.world.lost[n].store(false, Ordering::SeqCst);
            self.down[n] = Some(now);
            self.moves.joining.push(n);
        }
    }

    /// Lists the nodes that joined once they are up, and strikes the move
    /// crash once its point is reached.
    pub(super) fn advance_moves(&mut self, now: Duration) {
        let views = self.views();
        let up: Vec<usize> = self
            .moves
            .joining
            .iter()
            .copied()
            .filter(|&n| views[n].is_some())
            .collect();
        for n in up {
            self.moves.joining.retain(|joining| *joining != n);
            self.moves.joined.push(n);
            lock(&self.world.topology).push(candidate(&self.world.run, n));
        }
        self.move_crash(now);
    }

    fn move_crash(&mut self, now: Duration) {
        let Some(crash) = self.moves.crash else {
            return;
        };
        let events = lock(&self.world.repair.events);
        let moves = move_attempts(&events);
        let reached = events[self.moves.seen..]
            .iter()
            .find(|(_, _, event)| {
                moves.contains(&event.attempt) && crash.point.reached(&event.step)
            })
            .cloned();
        self.moves.seen = events.len();
        let Some((primary, _, event)) = reached else {
            return;
        };
        let ends = events
            .iter()
            .find_map(|(_, _, started)| match &started.step {
                RepairStep::MoveStarted { from, to, .. } if started.attempt == event.attempt => {
                    Some((from.clone(), to.clone()))
                }
                _ => None,
            });
        drop(events);
        self.moves.crash = None;
        let Some((from, to)) = ends else {
            return;
        };
        let node = match crash.target {
            MoveTarget::Primary => Some(primary),
            MoveTarget::OldHolder => self.world.position(&from),
            MoveTarget::NewHolder => self.world.position(&to),
        };
        let Some(node) = node.filter(|node| !self.world.lost[*node].load(Ordering::SeqCst)) else {
            return;
        };
        tracing::debug!(?event, node, "the move crash point is reached");
        let slot = &self.world.nodes[node];
        if crash.target == MoveTarget::NewHolder && crash.kind == CrashKind::PowerAtNextSync {
            // As for a repair: the cut is armed as the copy's write leaves.
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

    /// The stripes of the coded layouts of the first member that is up:
    /// each as its fragment bytes, `m`, and its fragments' nodes.
    fn stripes(&self) -> Result<Vec<(u64, usize, Vec<NodeId>)>, String> {
        let views = self.views();
        let Some(view) = self
            .world
            .members()
            .into_iter()
            .find_map(|n| views[n].clone())
        else {
            return Ok(Vec::new());
        };
        let reader = view.index.read().map_err(|e| e.to_string())?;
        let keys = coded_keys(&reader, &self.world.shard)?;
        let mut stripes = Vec::new();
        for key in &keys {
            let entry = reader
                .entry(&self.world.shard, key)
                .map_err(|e| e.to_string())?;
            let Some(coded) = entry.and_then(|e| e.object).and_then(|o| o.coded) else {
                continue;
            };
            for stripe in &coded.stripes {
                let geometry = stripe.geometry();
                let bytes = stripe.data_len().div_ceil(geometry.data_fragments() as u64);
                let nodes = stripe.fragments().iter().map(|l| l.node.clone()).collect();
                stripes.push((bytes, geometry.parity_fragments(), nodes));
            }
        }
        Ok(stripes)
    }

    /// Checks that no stripe of any member's layout holds more than `m`
    /// fragments in one failure domain of the run's level.
    pub(super) fn check_domains(&self) -> Result<(), String> {
        if self.world.run.rebalance.is_none() {
            return Ok(());
        }
        let topology = Topology::new(level(&self.world.run), lock(&self.world.topology).clone());
        let views = self.views();
        for member in self.world.members() {
            let Some(view) = &views[member] else {
                continue;
            };
            let reader = view.index.read().map_err(|e| e.to_string())?;
            for key in &coded_keys(&reader, &self.world.shard)? {
                let entry = reader
                    .entry(&self.world.shard, key)
                    .map_err(|e| e.to_string())?;
                let Some(coded) = entry.and_then(|e| e.object).and_then(|o| o.coded) else {
                    continue;
                };
                for stripe in &coded.stripes {
                    let mut per: BTreeMap<Domain, usize> = BTreeMap::new();
                    for location in stripe.fragments() {
                        if let Some(domain) = topology.domain(&location.node) {
                            *per.entry(domain).or_default() += 1;
                        }
                    }
                    let cap = stripe.geometry().parity_fragments();
                    if let Some((domain, count)) = per.into_iter().find(|(_, n)| *n > cap) {
                        return Err(format!(
                            "member n{member}'s layout of {key} puts {count} fragments of \
                             stripe {} in {domain}, over its cap of {cap}",
                            stripe.number()
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// Checks that the drains and the balancing are done: no layout names a
    /// fragment on a drained node, every node that joined holds fragments,
    /// and the live nodes' fragment bytes are within two fragments of each
    /// other; or says why not.
    pub(super) fn rebalanced(&self) -> Result<(), String> {
        if self.world.run.rebalance.is_none() {
            return Ok(());
        }
        if !self.moves.joining.is_empty() {
            return Err("a node has not joined yet".to_owned());
        }
        let stripes = self.stripes()?;
        let mut held: BTreeMap<NodeId, u64> = BTreeMap::new();
        for (bytes, _, nodes) in &stripes {
            for node in nodes {
                *held.entry(node.clone()).or_default() += bytes;
            }
        }
        for &n in &self.moves.drained {
            let node = &self.world.nodes[n].id;
            if held.contains_key(node) {
                return Err(format!("the drained {node} still holds fragments"));
            }
        }
        let live: Vec<NodeId> = lock(&self.world.topology)
            .iter()
            .filter(|c| c.state == NodeState::Live)
            .filter(|c| {
                self.world
                    .position(&c.node)
                    .is_some_and(|n| !self.world.lost[n].load(Ordering::SeqCst))
            })
            .map(|c| c.node.clone())
            .collect();
        for &n in &self.moves.joined {
            let node = &self.world.nodes[n].id;
            if live.contains(node) && !held.contains_key(node) {
                return Err(format!("{node} joined and holds no fragment"));
            }
        }
        let largest = stripes
            .iter()
            .map(|(bytes, _, _)| *bytes)
            .max()
            .unwrap_or(0);
        let shares = live.iter().map(|node| held.get(node).copied().unwrap_or(0));
        let (least, most) = (shares.clone().min(), shares.max());
        if let (Some(least), Some(most)) = (least, most)
            && most - least > 2 * largest
        {
            return Err(format!(
                "the live nodes hold from {least} to {most} bytes of fragments, more than two \
                 fragments of {largest} bytes apart"
            ));
        }
        Ok(())
    }

    /// The outcome of the moves, after checking that no pass both repaired
    /// and moved.
    pub(super) fn move_outcome(&self) -> Result<Option<MoveOutcome>, String> {
        if self.world.run.rebalance.is_none() {
            return Ok(None);
        }
        let events = lock(&self.world.repair.events);
        let moves = move_attempts(&events);
        let mut passes: BTreeMap<(usize, u64, u64), (bool, bool)> = BTreeMap::new();
        let mut outcome = MoveOutcome {
            retired: self.world.moves.retired.load(Ordering::SeqCst),
            ..MoveOutcome::default()
        };
        for (node, life, event) in events.iter() {
            match &event.step {
                RepairStep::Started { pass, .. } => {
                    passes.entry((*node, *life, *pass)).or_default().0 = true;
                }
                RepairStep::MoveStarted { pass, .. } => {
                    outcome.started += 1;
                    passes.entry((*node, *life, *pass)).or_default().1 = true;
                }
                RepairStep::Committed {
                    relocated: true, ..
                } if moves.contains(&event.attempt) => outcome.moved += 1,
                _ => {}
            }
        }
        drop(events);
        if let Some(((node, _, pass), _)) = passes
            .iter()
            .find(|(_, (repaired, moved))| *repaired && *moved)
        {
            return Err(format!(
                "n{node} moved fragments in pass {pass}, which repaired stripes: repairs come first"
            ));
        }
        let name = |n: &usize| self.world.nodes[*n].id.to_string();
        outcome.joined = self.moves.joined.iter().map(name).collect();
        outcome.drained = self.moves.drained.iter().map(name).collect();
        let mut spread: BTreeMap<String, usize> = BTreeMap::new();
        for (_, _, nodes) in self.stripes()? {
            for node in nodes {
                *spread.entry(node.to_string()).or_default() += 1;
            }
        }
        outcome.spread = spread.into_iter().collect();
        Ok(Some(outcome))
    }
}
