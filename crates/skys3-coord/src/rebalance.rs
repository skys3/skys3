//! Rebalancing (design §6.7): the coordinator moves shard members and
//! primaries so that every node holds its share, newly joined nodes
//! included.
//!
//! **Shares.** Among the nodes placement may give members to that are
//! live (eligible, and heard from within `suspect_after`), a node's share
//! of the shards' members and learners is proportional to the capacity
//! its disks offer, and its share of primaries is an equal part. The
//! cluster is balanced when no move from one node to another would bring
//! both closer to their shares: for shards, when no node `a` holds `s_a`
//! members and another `t` holds `s_t` with `(s_a − 1) / c_a ≥ (s_t + 1) /
//! c_t` (capacities `c`), so at equal capacity every node holds within one
//! shard of every other; for primaries, when no node leads two more than
//! another.
//!
//! **Moving a shard** from `a` to `t` takes the steps of replacement
//! (plan M3-05), so a shard never has fewer than `replicas` members:
//!
//! 1. the coordinator adds `t` as a learner, by a compare-and-swap of the
//!    shard's register;
//! 2. the primary backfills it and promotes it once it holds every
//!    committed record (rule R3);
//! 3. the shard now has a member too many, and the coordinator removes
//!    `a` by a compare-and-swap; if `a` is the primary, it asks `a` to
//!    hand the shard off instead ([`Handoff`]), which leaves `a` out of the
//!    next configuration (§5.4).
//!
//! A move whose source leads the shard moves the primary with it, to `t`,
//! when that brings both nodes closer to their share of primaries. Which
//! member leaves is decided again from the register in every round: the
//! one this coordinator's move named, or, after a coordinator failover,
//! the member on the node furthest over its share, so a move a previous
//! coordinator started completes too.
//!
//! **Moving a primary** alone is a planned handoff to another member: the
//! primary steps down and the member proposes itself (§5.4, rule R1). The
//! old primary leaves the configuration, so the shard is short one member
//! until replacement adds a learner, the old primary as likely as any
//! other node; rebalancing moves primaries alone only once no shard needs
//! to move.
//!
//! **A primary that does not count** (on a departing or unlabeled node) is
//! handed off once the members that count other than it reach `replicas`,
//! which replacement brings about by adding learners; replacement never
//! moves a primary (rule R1).
//!
//! **Composing with replacement.** [`Rebalancing`] is the placement that
//! [`Replacement`](crate::Replacement) wraps, so it plans only in rounds
//! in which no shard needs repair. It starts moves only when the cluster is
//! settled: no shard names a learner or more members than `replicas`, no
//! handoff it asked for is outstanding, and no node is suspect. Then it
//! plans one batch of at most [`RebalanceConfig::max_moves`] moves, each
//! on a different shard, counting each as done when it picks the next, so
//! a batch never overshoots. Replacement counts a rebalancing learner as
//! one of the shard's own and never removes a member that counts, so it
//! has nothing to undo; rebalancing never touches a shard with a member
//! that does not count, which is replacement's to remove.
//!
//! **Pace.** At most [`RebalanceConfig::max_moves`] shard moves at once,
//! which bounds the backfills rebalancing adds; a new batch only after the
//! last one completed; and at most one handoff per
//! [`RebalanceConfig::handoff_interval`] across the cluster, so at most one
//! shard's writes wait for a handoff at a time.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use skys3_config::FailureDomain;
use skys3_control::{ControlError, ControlStore, ProposalIds, RegisterKey, TypedKey, Version};
use skys3_io::{Clock, MonoTime};
use skys3_types::{Epoch, NodeId, ProposalId, ShardConfig};

use crate::change::{Applied, ChangeSet};
use crate::coordinator::{NoPlacement, Placement};
use crate::handoff::{Handoff, RequestHandoff};
use crate::place::Topology;
use crate::policy::ClusterScan;
use crate::registry::{NodeRegistry, NodeState};
use crate::replace::Census;

/// How fast [`Rebalancing`] moves shards and primaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RebalanceConfig {
    /// The most shard moves one batch starts. The next batch starts once
    /// every shard has completed its moves, so this bounds the backfills
    /// rebalancing runs at once.
    pub max_moves: usize,
    /// The least time between two handoffs this coordinator asks for,
    /// across the cluster. Each stalls one shard's writes for the time a
    /// handoff takes.
    pub handoff_interval: Duration,
    /// How long a handoff asked for in an epoch is given to land before
    /// the coordinator asks again.
    pub handoff_retry: Duration,
}

impl Default for RebalanceConfig {
    /// Four moves at once, a handoff every 2 s at most, and one asked for
    /// again after 10 s.
    fn default() -> Self {
        Self {
            max_moves: 4,
            handoff_interval: Duration::from_secs(2),
            handoff_retry: Duration::from_secs(10),
        }
    }
}

/// A shard move this coordinator started: a learner on `to`, which
/// replaces `from` once the primary promotes it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Move {
    from: NodeId,
    to: NodeId,
    /// Whether `to` takes over as primary, if `from` leads the shard.
    lead: bool,
}

/// A shard register, as one round reads it.
struct Shard<'a> {
    key: &'a RegisterKey,
    version: &'a Version,
    config: &'a ShardConfig,
    census: Census,
}

impl Shard<'_> {
    fn replicas(&self) -> usize {
        usize::from(self.config.replicas)
    }

    /// Whether the primary counts, as it does unless its node is departing
    /// or lacks the label the level needs.
    fn primary_counts(&self) -> bool {
        self.census.counted.first() == Some(&self.config.primary)
    }

    /// The members that count, the primary first if it does.
    fn counted_members(&self) -> &[NodeId] {
        &self.census.counted[..self.census.members]
    }

    /// The members that count, other than the primary: those a handoff
    /// may go to.
    fn successors(&self) -> impl Iterator<Item = &NodeId> {
        self.counted_members()
            .iter()
            .filter(|node| **node != self.config.primary)
    }

    /// Whether a move or a repair is under way: the shard names a learner,
    /// or more members than `replicas`.
    fn is_moving(&self) -> bool {
        !self.config.learners.is_empty() || self.config.members.len() > self.replicas()
    }

    /// Whether a new move may start on the shard: nothing is under way,
    /// and its `replicas` members all count.
    fn is_settled(&self) -> bool {
        !self.is_moving()
            && self.config.members.len() == self.replicas()
            && self.census.members == self.replicas()
            && self.primary_counts()
    }
}

/// A node's load as rebalancing weighs it.
#[derive(Debug, Clone)]
struct Load {
    node: NodeId,
    /// Members and learners it holds.
    shards: u64,
    /// Shards it leads.
    primaries: u64,
    /// The bytes its disks offer, at least 1.
    capacity: u128,
}

impl Load {
    /// Compares `self` holding `extra` more shards with `other` holding
    /// `other_extra` more, relative to their capacities.
    fn cmp_shards(&self, extra: i64, other: &Load, other_extra: i64) -> Ordering {
        let shards = |load: &Load, extra: i64| {
            u128::try_from(i128::from(load.shards) + i128::from(extra)).unwrap_or(0)
        };
        (shards(self, extra) * other.capacity).cmp(&(shards(other, other_extra) * self.capacity))
    }

    /// Whether moving a shard from `self` to `target` leaves `self` at
    /// least as loaded as `target`, for their capacities: the move then
    /// brings both closer to their shares.
    fn may_give_to(&self, target: &Load) -> bool {
        self.shards > 0 && self.cmp_shards(-1, target, 1) != Ordering::Less
    }

    /// Whether moving a primary from `self` to `target` brings both closer
    /// to their equal shares of primaries.
    fn may_lead_less_than(&self, target: &Load) -> bool {
        self.primaries >= target.primaries + 2
    }
}

/// What one round decided for one shard register.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Step {
    /// A learner, to replace a member.
    Learner(NodeId),
    /// A member that leaves, now that its replacement was promoted.
    Remove(NodeId),
}

/// The coordinator's rebalancing, as a [`Placement`] (design §6.7): it
/// moves shard members and primaries so that every live node holds its
/// share, a newly joined one included, and moves primaries off nodes that
/// no longer count.
///
/// A shard moves by add-learner, promote, and remove steps, each a
/// compare-and-swap of its register (the promotion is the primary's), and
/// a primary by a planned handoff that the coordinator asks the primary's
/// node for through `H` ([`RequestHandoff`]).
///
/// - **Shares.** Among the live nodes placement may give members to, a
///   node's share of members and learners follows its disks' capacity,
///   and its share of primaries is an equal part. A shard moves from `a`
///   to `t` while `(s_a − 1) / c_a ≥ (s_t + 1) / c_t` (members `s`,
///   capacity `c`), and a primary alone while `a` leads at least two
///   more shards than `t`.
/// - **Completing a move.** Once the learner is promoted, the member the
///   move named leaves (or, after a coordinator failover, the one on the
///   node furthest over its share): by a compare-and-swap, or, for the
///   primary, by a handoff. A primary that does not count is handed off
///   once the other members that count reach `replicas`.
/// - **Pace.** New moves start only in a settled cluster (no learner, no
///   surplus member, no handoff outstanding, no suspect node), in batches
///   of at most [`RebalanceConfig::max_moves`], and handoffs at most one
///   per [`RebalanceConfig::handoff_interval`].
///
/// It plans nothing until the registry has listed the nodes in this
/// tenure, and plans for the placement it wraps whenever it has nothing
/// to do. In a full coordinator it is the placement
/// [`Replacement`](crate::Replacement) wraps, so that repairs come first:
/// `Replacement::new(..).with_placement(Rebalancing::new(..))`.
pub struct Rebalancing<H, P = NoPlacement> {
    registry: NodeRegistry,
    level: FailureDomain,
    clock: Arc<dyn Clock>,
    config: RebalanceConfig,
    handoffs: Arc<H>,
    scan: ClusterScan,
    /// The moves this coordinator started, by shard register, while the
    /// register names both nodes.
    moves: BTreeMap<RegisterKey, Move>,
    /// The handoffs this coordinator asked for, by shard register: the
    /// epoch, and when.
    requested: BTreeMap<RegisterKey, (Epoch, MonoTime)>,
    /// When it last asked for a handoff.
    handed_off_at: Option<MonoTime>,
    /// The steps of the change being made, if the change is this
    /// placement's.
    planned: Option<BTreeMap<RegisterKey, Step>>,
    placement: P,
}

impl<H, P> std::fmt::Debug for Rebalancing<H, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Rebalancing")
            .field("level", &self.level)
            .field("config", &self.config)
            .field("moves", &self.moves)
            .field("requested", &self.requested)
            .finish_non_exhaustive()
    }
}

impl<H: RequestHandoff> Rebalancing<H> {
    /// Rebalances at `level` the nodes `registry` lists, at the pace
    /// `config` sets, timing handoffs on `clock` and asking for them
    /// through `handoffs`.
    #[must_use]
    pub fn new(
        registry: NodeRegistry,
        level: FailureDomain,
        clock: Arc<dyn Clock>,
        config: RebalanceConfig,
        handoffs: H,
    ) -> Self {
        Self {
            registry,
            level,
            clock,
            config,
            handoffs: Arc::new(handoffs),
            scan: ClusterScan::new(),
            moves: BTreeMap::new(),
            requested: BTreeMap::new(),
            handed_off_at: None,
            planned: None,
            placement: NoPlacement,
        }
    }
}

impl<H: RequestHandoff, P> Rebalancing<H, P> {
    /// Wraps `placement`, which plans whenever rebalancing has nothing to
    /// change.
    pub fn with_placement<Q: Placement>(self, placement: Q) -> Rebalancing<H, Q> {
        Rebalancing {
            registry: self.registry,
            level: self.level,
            clock: self.clock,
            config: self.config,
            handoffs: self.handoffs,
            scan: self.scan,
            moves: self.moves,
            requested: self.requested,
            handed_off_at: self.handed_off_at,
            planned: None,
            placement,
        }
    }

    /// Plans the round from the scanned registers: the steps that complete
    /// moves under way, then, in a settled cluster, a new batch of moves
    /// or one primary's move. Asks for the round's handoff, if any, itself.
    fn plan_scanned(
        &mut self,
        proposals: &mut ProposalIds,
    ) -> (ChangeSet, BTreeMap<RegisterKey, Step>) {
        // The registers stay borrowed while the round updates its moves.
        let scan = std::mem::take(&mut self.scan);
        let planned = self.plan_with(&scan, proposals);
        self.scan = scan;
        planned
    }

    fn plan_with(
        &mut self,
        scan: &ClusterScan,
        proposals: &mut ProposalIds,
    ) -> (ChangeSet, BTreeMap<RegisterKey, Step>) {
        let mut topology = Topology::from_registry(self.level, &self.registry);
        for shard in scan.shards() {
            topology.record(shard);
        }
        let shards: Vec<Shard<'_>> = scan
            .shard_registers()
            .map(|(key, version, config)| Shard {
                key,
                version,
                config,
                census: Census::of(&topology, config),
            })
            .collect();
        let now = self.clock.now();
        self.forget_settled(&shards, now);

        let mut steps = BTreeMap::new();
        let mut handoff = None;
        for shard in &shards {
            match self.finish(&topology, shard) {
                Some(Finish::Remove(member)) => {
                    steps.insert(shard.key.clone(), Step::Remove(member));
                }
                // The first shard whose handoff was not asked for already.
                Some(Finish::HandOff(to))
                    if handoff.is_none() && !self.requested.contains_key(shard.key) =>
                {
                    handoff = Some((shard, to));
                }
                Some(Finish::HandOff(_)) | None => {}
            }
        }
        let settled = steps.is_empty()
            && handoff.is_none()
            && !shards.iter().any(Shard::is_moving)
            && !self.awaits_handoff(&shards, now)
            && !topology
                .candidates()
                .any(|node| node.state == NodeState::Suspect);
        if settled {
            for (key, step) in self.start_moves(&topology, &shards) {
                steps.insert(key, step);
            }
            if steps.is_empty() {
                handoff = primary_move(&topology, &shards);
            }
        }
        if let Some((shard, to)) = handoff {
            self.hand_off(shard, to, now);
        }

        let mut change = ChangeSet::new();
        let mut planned = BTreeMap::new();
        for (key, step) in steps {
            let Some(shard) = shards.iter().find(|shard| *shard.key == key) else {
                continue;
            };
            let Some(next) = next_config(shard.config, &step, proposals.next_id()) else {
                continue;
            };
            change = match change
                .clone()
                .update(&TypedKey::new(key.clone()), shard.version, &next)
            {
                Ok(change) => change,
                Err(error) => {
                    tracing::warn!(%error, "skipping an invalid shard change");
                    continue;
                }
            };
            tracing::info!(register = %key, epoch = %next.epoch, ?step, "rebalancing a shard");
            planned.insert(key, step);
        }
        (change, planned)
    }

    /// Forgets the moves whose register no longer names both nodes, as
    /// once the source left, and the handoffs whose shard moved past the
    /// epoch they were asked in or that are due to be asked again.
    fn forget_settled(&mut self, shards: &[Shard<'_>], now: MonoTime) {
        let register = |key: &RegisterKey| {
            shards
                .iter()
                .find(|shard| shard.key == key)
                .map(|shard| shard.config)
        };
        self.moves.retain(|key, step| {
            register(key).is_some_and(|config| {
                config.is_member(&step.from)
                    && (config.is_member(&step.to) || config.is_learner(&step.to))
            })
        });
        let retry = self.config.handoff_retry;
        self.requested.retain(|key, (epoch, at)| {
            register(key).is_some_and(|config| config.epoch == *epoch)
                && now.saturating_duration_since(*at) < retry
        });
    }

    /// Whether a handoff this coordinator asked for may still land.
    fn awaits_handoff(&self, shards: &[Shard<'_>], now: MonoTime) -> bool {
        self.requested.iter().any(|(key, (epoch, at))| {
            now.saturating_duration_since(*at) < self.config.handoff_retry
                && shards
                    .iter()
                    .any(|shard| shard.key == key && shard.config.epoch == *epoch)
        })
    }

    /// The step that completes a move under way on `shard`, if one is due:
    /// a surplus member removed, or the primary handed off.
    fn finish(&self, topology: &Topology, shard: &Shard<'_>) -> Option<Finish> {
        // A learner is still to be promoted, or dropped; a member that
        // does not count is replacement's to remove.
        if !shard.config.learners.is_empty() || !shard.census.stray_members.is_empty() {
            return None;
        }
        let planned = self.moves.get(shard.key);
        if !shard.primary_counts() {
            // Replacement brings the other members up to `replicas`; then
            // the primary, which only a handoff moves, leaves.
            return (shard.successors().count() >= shard.replicas())
                .then(|| successor(topology, shard, planned.map(|m| &m.to)))
                .flatten()
                .map(Finish::HandOff);
        }
        if shard.census.members <= shard.replicas() {
            return None;
        }
        let leaving = planned
            .filter(|step| {
                shard.counted_members().contains(&step.from)
                    && shard.counted_members().contains(&step.to)
            })
            .map(|step| step.from.clone())
            .or_else(|| most_loaded(topology, shard))?;
        if leaving == shard.config.primary {
            let lead = planned.filter(|step| step.lead).map(|step| &step.to);
            successor(topology, shard, lead).map(Finish::HandOff)
        } else {
            Some(Finish::Remove(leaving))
        }
    }

    /// Plans a batch of at most [`RebalanceConfig::max_moves`] shard moves
    /// in a settled cluster: each from the most loaded node that holds a
    /// shard the least loaded node may take, while that brings both closer
    /// to their shares. Records each move, and returns the learners to add.
    fn start_moves(
        &mut self,
        topology: &Topology,
        shards: &[Shard<'_>],
    ) -> Vec<(RegisterKey, Step)> {
        let mut loads = live_loads(topology);
        let mut taken: Vec<&RegisterKey> = Vec::new();
        let mut started = Vec::new();
        while started.len() < self.config.max_moves {
            let Some((source, target, shard, lead)) = next_move(topology, shards, &loads, &taken)
            else {
                break;
            };
            let led = shard.config.primary == loads[source].node;
            loads[source].shards -= 1;
            loads[target].shards += 1;
            if led {
                loads[source].primaries -= 1;
                if lead {
                    loads[target].primaries += 1;
                }
            }
            let step = Move {
                from: loads[source].node.clone(),
                to: loads[target].node.clone(),
                lead,
            };
            tracing::info!(register = %shard.key, from = %step.from, to = %step.to, lead,
                "moving a shard");
            taken.push(shard.key);
            started.push((shard.key.clone(), Step::Learner(step.to.clone())));
            self.moves.insert(shard.key.clone(), step);
        }
        started
    }

    /// Asks the primary of `shard` to hand it off to `to`, unless a handoff
    /// was asked for within [`RebalanceConfig::handoff_interval`], or for
    /// this shard in its epoch already.
    fn hand_off(&mut self, shard: &Shard<'_>, to: NodeId, now: MonoTime) {
        let interval = self.config.handoff_interval;
        if self
            .handed_off_at
            .is_some_and(|at| now.saturating_duration_since(at) < interval)
            || self.requested.contains_key(shard.key)
        {
            return;
        }
        let primary = shard.config.primary.clone();
        let Some(entry) = self.registry.get(&primary) else {
            return;
        };
        let handoff = Handoff {
            bucket: shard.config.bucket_id.clone(),
            shard: shard.config.shard,
            epoch: shard.config.epoch,
            to,
        };
        tracing::info!(register = %shard.key, %primary, to = %handoff.to, epoch = %handoff.epoch,
            "asking the primary to hand the shard off");
        self.handed_off_at = Some(now);
        self.requested
            .insert(shard.key.clone(), (shard.config.epoch, now));
        let request = self
            .handoffs
            .request(primary.clone(), entry.registration.address, handoff);
        tokio::spawn(async move {
            if let Err(error) = request.await {
                tracing::info!(%primary, %error, "a handoff did not start");
            }
        });
    }
}

/// How a move under way completes on a shard.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Finish {
    /// The member leaves by a compare-and-swap.
    Remove(NodeId),
    /// The primary hands the shard off to the member.
    HandOff(NodeId),
}

/// The live, eligible nodes, by node ID.
fn live_loads(topology: &Topology) -> Vec<Load> {
    topology
        .eligible()
        .filter(|node| node.state == NodeState::Live)
        .map(|node| Load {
            node: node.node.clone(),
            shards: u64::from(node.shards),
            primaries: u64::from(node.primaries),
            capacity: u128::from(node.capacity_bytes.max(1)),
        })
        .collect()
}

/// The next move of a batch: from the most loaded node that may give the
/// least loaded node it can a shard, the shard, and whether the primary
/// moves with it. Returns the source and target by index into `loads`.
fn next_move<'a, 's>(
    topology: &Topology,
    shards: &'a [Shard<'s>],
    loads: &[Load],
    taken: &[&RegisterKey],
) -> Option<(usize, usize, &'a Shard<'s>, bool)> {
    let mut by_load: Vec<usize> = (0..loads.len()).collect();
    by_load.sort_by(|a, b| {
        loads[*a]
            .cmp_shards(0, &loads[*b], 0)
            .then_with(|| loads[*a].node.cmp(&loads[*b].node))
    });
    for &target in &by_load {
        for &source in by_load.iter().rev() {
            if source == target {
                continue;
            }
            if !loads[source].may_give_to(&loads[target]) {
                // Every later source holds less.
                break;
            }
            if let Some((shard, lead)) =
                movable(topology, shards, taken, &loads[source], &loads[target])
            {
                return Some((source, target, shard, lead));
            }
        }
    }
    None
}

/// A settled shard that `source` holds and `target` may take, not yet in
/// the batch, and whether its primary moves to `target` with it. A shard
/// `source` leads comes first when the primary should move too, and last
/// otherwise, since moving it needs a handoff.
fn movable<'a, 's>(
    topology: &Topology,
    shards: &'a [Shard<'s>],
    taken: &[&RegisterKey],
    source: &Load,
    target: &Load,
) -> Option<(&'a Shard<'s>, bool)> {
    let lead = source.may_lead_less_than(target);
    let domain = topology.domain(&target.node)?;
    // A learner in the source's own domain counts in its place, and the
    // source is then removed as a member that does not count; but the
    // primary keeps its domain, so it moves only to another one.
    let within = topology.domain(&source.node).as_ref() == Some(&domain);
    let candidates = shards.iter().filter(|shard| {
        shard.is_settled()
            && !taken.contains(&shard.key)
            && shard.counted_members().contains(&source.node)
            && !shard.config.is_member(&target.node)
            && !shard.config.is_learner(&target.node)
            && !(within && shard.config.primary == source.node)
            // The target's domain is free once the source leaves.
            && shard
                .counted_members()
                .iter()
                .filter(|node| **node != source.node)
                .all(|node| topology.domain(node).as_ref() != Some(&domain))
    });
    let (led, other): (Vec<&Shard<'s>>, Vec<&Shard<'s>>) =
        candidates.partition(|shard| shard.config.primary == source.node);
    if lead && let Some(shard) = led.first() {
        return Some((shard, true));
    }
    other
        .first()
        .map(|shard| (*shard, false))
        .or_else(|| led.first().map(|shard| (*shard, false)))
}

/// One primary's move by handoff in a balanced, settled cluster: from the
/// node that leads the most shards to a member that leads at least two
/// fewer, if any.
fn primary_move<'a, 's>(
    topology: &Topology,
    shards: &'a [Shard<'s>],
) -> Option<(&'a Shard<'s>, NodeId)> {
    let loads = live_loads(topology);
    let load = |node: &NodeId| loads.iter().find(|load| load.node == *node);
    shards
        .iter()
        .filter(|shard| shard.is_settled())
        .filter_map(|shard| {
            let primary = load(&shard.config.primary)?;
            let to = shard
                .successors()
                .filter_map(&load)
                .filter(|member| primary.may_lead_less_than(member))
                .min_by(|a, b| {
                    a.primaries
                        .cmp(&b.primaries)
                        .then_with(|| a.node.cmp(&b.node))
                })?;
            Some((shard, primary.primaries, to.node.clone()))
        })
        .max_by(|a, b| a.1.cmp(&b.1).then_with(|| b.0.key.cmp(a.0.key)))
        .map(|(shard, _, to)| (shard, to))
}

/// The member of `shard` to hand it off to: `preferred` if it is a member
/// that counts, otherwise the one that counts on a live node leading the
/// fewest shards.
fn successor(topology: &Topology, shard: &Shard<'_>, preferred: Option<&NodeId>) -> Option<NodeId> {
    if let Some(preferred) = preferred.filter(|node| shard.successors().any(|s| s == *node)) {
        return Some(preferred.clone());
    }
    shard
        .successors()
        .filter_map(|node| topology.get(node))
        .min_by(|a, b| {
            a.state
                .cmp(&b.state)
                .then(a.primaries.cmp(&b.primaries))
                .then_with(|| a.node.cmp(&b.node))
        })
        .map(|candidate| candidate.node.clone())
}

/// The member of `shard` that leaves when it has one too many: the one on
/// the registered node most loaded for its capacity, a member other than
/// the primary first among equals, since the primary leaves only by a
/// handoff.
fn most_loaded(topology: &Topology, shard: &Shard<'_>) -> Option<NodeId> {
    let loads: Vec<(Load, bool)> = shard
        .counted_members()
        .iter()
        .filter_map(|node| topology.get(node))
        .map(|node| {
            let load = Load {
                node: node.node.clone(),
                shards: u64::from(node.shards),
                primaries: u64::from(node.primaries),
                capacity: u128::from(node.capacity_bytes.max(1)),
            };
            (load, node.node == shard.config.primary)
        })
        .collect();
    loads
        .iter()
        .max_by(|(a, a_leads), (b, b_leads)| {
            a.cmp_shards(0, b, 0)
                .then(b_leads.cmp(a_leads))
                .then_with(|| b.node.cmp(&a.node))
        })
        .map(|(load, _)| load.node.clone())
}

/// The configuration after `step`: epoch `e+1`, the same primary, with the
/// learner added or the member removed. `None` if the epoch is exhausted.
fn next_config(config: &ShardConfig, step: &Step, proposal: ProposalId) -> Option<ShardConfig> {
    let epoch = config.epoch.checked_next()?;
    let (members, learners) = match step {
        Step::Learner(node) => (
            config.members.clone(),
            config.learners.iter().chain([node]).cloned().collect(),
        ),
        Step::Remove(node) => (
            config
                .members
                .iter()
                .filter(|m| *m != node)
                .cloned()
                .collect(),
            config.learners.clone(),
        ),
    };
    Some(ShardConfig {
        epoch,
        members,
        learners,
        proposal_id: proposal,
        ..config.clone()
    })
}

impl<H: RequestHandoff, P: Placement> Placement for Rebalancing<H, P> {
    fn begin_tenure(&mut self) {
        self.placement.begin_tenure();
    }

    async fn plan<S: ControlStore>(
        &mut self,
        store: &S,
        proposals: &mut ProposalIds,
    ) -> Result<Option<ChangeSet>, ControlError> {
        self.planned = None;
        if self.registry.is_listed() {
            self.scan.refresh(store).await?;
            let (change, steps) = self.plan_scanned(proposals);
            if !change.is_empty() {
                self.planned = Some(steps);
                return Ok(Some(change));
            }
        }
        self.placement.plan(store, proposals).await
    }

    fn applied(&mut self, change: &ChangeSet, applied: &Applied) {
        let Some(steps) = self.planned.take() else {
            self.placement.applied(change, applied);
            return;
        };
        if let Some(rejected) = &applied.rejected {
            tracing::info!(register = %rejected, "a shard changed under its rebalancing; planning again");
        }
        // A learner whose compare-and-swap did not land started no move.
        for (key, step) in &steps {
            if matches!(step, Step::Learner(_)) && !applied.written.iter().any(|(k, _)| k == key) {
                self.moves.remove(key);
            }
        }
    }
}

#[cfg(test)]
mod tests;
