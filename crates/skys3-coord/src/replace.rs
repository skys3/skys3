//! Replacement (design §6.4, §6.7): the coordinator brings every shard
//! back to `replicas` members after it loses one, and re-homes the members
//! placement no longer counts.
//!
//! A shard's members and learners **count** when placement can show them
//! apart: each on a registered node that is not departing, with the label
//! the `failure_domain` level needs, and in a domain no other member or
//! learner that counts is in (the primary first, then the other members,
//! then the learners). This is the measure cluster health reports shards
//! short by ([`report`](crate::report)), with one exception: a member on a
//! node the registry does not list counts, in no domain. Such a node may
//! not have registered yet, and nothing shows it lost; if it is, its
//! primary removes it (design §6.4), and the shard is then short.
//!
//! Each round, [`Replacement`] reads the shard registers ([`ClusterScan`])
//! and changes each shard by one compare-and-swap of its register, from
//! epoch `e` to `e+1` under `If-Match` on the version it read, keeping the
//! primary and the members:
//!
//! - **Adding learners.** A shard whose members and learners that count are
//!   fewer than `replicas` gets learners on the nodes [`Topology::place`]
//!   chooses: eligible, in domains the shard does not use, live before
//!   suspect, least loaded first. The primary streams the log to them,
//!   backfills them, and promotes each once it holds every committed
//!   record (rule R3, plan M2-14, M2-15). The coordinator never makes a
//!   member itself.
//! - **Dropping learners that cannot help.** A learner that does not count
//!   (its node departing, unregistered, unlabeled, or in a domain the
//!   shard already uses) is dropped, and one on a suspect node is swapped
//!   for a live node when placement has one: a learner is never counted
//!   by the commit rule, so dropping it costs nothing but its backfill.
//! - **Removing members that do not count** (never the primary, which only
//!   a handoff moves, rule R1): once the members that count reach
//!   `replicas` without them, that is, once their replacements have been
//!   promoted. So a removal never leaves a shard with fewer than `replicas`
//!   members, nor with fewer that count. Removals are rate-limited to one
//!   per [`ReplacementConfig::removal_interval`] across the cluster.
//!
//! A change is a compare-and-swap like the primary's own removals and
//! promotions, and neither side yields to the other: whichever lands first
//! wins. A primary whose change loses reads the register and adopts the
//! coordinator's, since it keeps the primary and adds no member, and
//! proposes again over it. A coordinator change that loses is dropped, and
//! the next round plans from what the register holds then.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use skys3_config::FailureDomain;
use skys3_control::{ControlError, ControlStore, ProposalIds, RegisterKey, TypedKey, Version};
use skys3_io::{Clock, MonoTime};
use skys3_types::{NodeId, ProposalId, ShardConfig};

use crate::change::{Applied, ChangeSet};
use crate::coordinator::{NoPlacement, Placement};
use crate::place::{ShardRequest, Topology};
use crate::policy::ClusterScan;
use crate::registry::{NodeRegistry, NodeState};

/// How fast [`Replacement`] changes shards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplacementConfig {
    /// The most learners the shard registers may name at once, across the
    /// cluster: it bounds how many backfills run together. A shard that
    /// would exceed it gets its learners in a later round.
    pub max_learners: usize,
    /// The most shard registers one change writes. Each change is
    /// announced by one generation increment.
    pub batch: usize,
    /// The least time between two member removals, across the cluster.
    pub removal_interval: Duration,
}

impl Default for ReplacementConfig {
    /// 64 learners at once, 32 registers per change, and a removal every
    /// 10 s.
    fn default() -> Self {
        Self {
            max_learners: 64,
            batch: 32,
            removal_interval: Duration::from_secs(10),
        }
    }
}

/// A shard's members and learners, as placement counts them.
#[derive(Debug, Default)]
struct Census {
    /// The members and learners that count, at most one per domain (a
    /// member on an unregistered node in none): the primary first, then the
    /// other members, then the learners.
    counted: Vec<NodeId>,
    /// How many of `counted` are members.
    members: usize,
    /// The members other than the primary that do not count.
    stray_members: Vec<NodeId>,
    /// The learners that do not count.
    stray_learners: Vec<NodeId>,
    /// The learners that count, on nodes the registry suspects.
    suspect_learners: Vec<NodeId>,
}

impl Census {
    fn of(topology: &Topology, config: &ShardConfig) -> Self {
        let mut census = Self::default();
        let mut domains = BTreeSet::new();
        let members = std::iter::once(&config.primary)
            .chain(config.members.iter().filter(|m| **m != config.primary));
        for (node, member) in members
            .map(|node| (node, true))
            .chain(config.learners.iter().map(|node| (node, false)))
        {
            let counts = match topology.get(node) {
                // A member on a node the registry does not list may not
                // have registered yet, and cannot be shown lost: it counts,
                // in no domain, and is left to its primary, which removes
                // it if it does not respond (§6.4).
                None => member,
                Some(candidate) => {
                    candidate.state != NodeState::Departing
                        && candidate
                            .domain(topology.level())
                            .is_some_and(|domain| domains.insert(domain))
                }
            };
            match (counts, member) {
                (true, true) => {
                    census.counted.push(node.clone());
                    census.members += 1;
                }
                (true, false) => {
                    census.counted.push(node.clone());
                    let suspect = topology
                        .get(node)
                        .is_some_and(|candidate| candidate.state == NodeState::Suspect);
                    if suspect {
                        census.suspect_learners.push(node.clone());
                    }
                }
                (false, true) if *node != config.primary => census.stray_members.push(node.clone()),
                (false, true) => {}
                (false, false) => census.stray_learners.push(node.clone()),
            }
        }
        census
    }
}

/// What one round decided for one shard.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Repair {
    /// New learners, and learners dropped.
    Learners {
        added: Vec<NodeId>,
        dropped: Vec<NodeId>,
    },
    /// A member that does not count, removed.
    Remove(NodeId),
}

/// The coordinator's replacement of lost members, as a [`Placement`]
/// (design §6.4, §6.7).
///
/// Each round it reads the shard registers and counts each shard's members
/// and learners as cluster health does ([`report`](crate::report)): on a
/// registered node that is not departing, with the label the level needs,
/// and in a domain no other counted node of the shard is in (a member on
/// a node the registry does not list counts too, in no domain). It then
/// changes each shard that needs it by one compare-and-swap of its
/// register to the next epoch, keeping the primary and the members:
///
/// - a shard whose counted members and learners are fewer than `replicas`
///   gets learners on the nodes [`Topology::place`] chooses, which its
///   primary backfills and promotes (rule R3);
/// - a learner that does not count is dropped, and one on a suspect node
///   is swapped for a live node when placement has one;
/// - a member other than the primary that does not count is removed once
///   the counted members reach `replicas` without it, at most one removal
///   per [`ReplacementConfig::removal_interval`] across the cluster.
///
/// Its compare-and-swaps and the primary's own removals and promotions
/// are equals: the first to land wins, and the loser plans again from the
/// register.
///
/// It plans before the placement it wraps, which plans whenever no shard
/// needs a change. It reads node health from the [`NodeRegistry`] that the
/// node [`Lifecycle`](crate::Lifecycle) keeps current, and plans nothing
/// until the registry has listed the nodes in this tenure: a node missing
/// from an empty registry would look unregistered, and its members
/// removable.
///
/// In a full coordinator it sits inside the bucket and shard creation:
/// `Lifecycle::new(registry.clone(), BucketShards::new(registry.clone(),
/// level).with_placement(Replacement::new(registry, level, clock,
/// ReplacementConfig::default())))`.
pub struct Replacement<P = NoPlacement> {
    registry: NodeRegistry,
    level: FailureDomain,
    clock: Arc<dyn Clock>,
    config: ReplacementConfig,
    scan: ClusterScan,
    /// When this coordinator last planned a member removal.
    removed_at: Option<MonoTime>,
    /// The repairs of the change being made, by register, if the change is
    /// this placement's.
    planned: Option<BTreeMap<RegisterKey, Repair>>,
    placement: P,
}

impl<P> std::fmt::Debug for Replacement<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Replacement")
            .field("level", &self.level)
            .field("config", &self.config)
            .field("removed_at", &self.removed_at)
            .finish_non_exhaustive()
    }
}

impl Replacement {
    /// Replaces lost members at `level`, on the nodes `registry` lists, at
    /// the pace `config` sets, timing removals on `clock`.
    #[must_use]
    pub fn new(
        registry: NodeRegistry,
        level: FailureDomain,
        clock: Arc<dyn Clock>,
        config: ReplacementConfig,
    ) -> Self {
        Self {
            registry,
            level,
            clock,
            config,
            scan: ClusterScan::new(),
            removed_at: None,
            planned: None,
            placement: NoPlacement,
        }
    }
}

impl<P> Replacement<P> {
    /// Wraps `placement`, which plans whenever no shard needs a change.
    pub fn with_placement<Q: Placement>(self, placement: Q) -> Replacement<Q> {
        Replacement {
            registry: self.registry,
            level: self.level,
            clock: self.clock,
            config: self.config,
            scan: self.scan,
            removed_at: self.removed_at,
            planned: None,
            placement,
        }
    }

    /// Whether a member removal may be planned now.
    fn may_remove(&self, now: MonoTime) -> bool {
        self.removed_at
            .is_none_or(|at| now.saturating_duration_since(at) >= self.config.removal_interval)
    }

    /// Plans the round's change from the scanned registers: the shards
    /// with the fewest members that count first, at most
    /// [`ReplacementConfig::batch`] of them. Returns the change and the
    /// repair of each register it writes.
    fn plan_scanned(
        &mut self,
        proposals: &mut ProposalIds,
    ) -> (ChangeSet, BTreeMap<RegisterKey, Repair>) {
        let mut topology = Topology::from_registry(self.level, &self.registry);
        for shard in self.scan.shards() {
            topology.record(shard);
        }
        let mut learners: usize = self.scan.shards().map(|s| s.learners.len()).sum();
        let mut shards: Vec<(&RegisterKey, &Version, &ShardConfig, Census)> = self
            .scan
            .shard_registers()
            .map(|(key, version, config)| (key, version, config, Census::of(&topology, config)))
            .collect();
        shards.sort_by(|a, b| {
            let urgency = |(_, _, config, census): &(_, _, &ShardConfig, Census)| {
                census.members.min(usize::from(config.replicas))
            };
            urgency(a).cmp(&urgency(b)).then_with(|| a.0.cmp(b.0))
        });
        let now = self.clock.now();
        let mut may_remove = self.may_remove(now);
        let mut change = ChangeSet::new();
        let mut repairs = BTreeMap::new();
        for (key, version, config, census) in shards {
            if repairs.len() >= self.config.batch {
                break;
            }
            let room = self.config.max_learners.saturating_sub(learners);
            let repair = match learners_for(&mut topology, config, &census, room) {
                Some(repair) => repair,
                None if may_remove && census.members >= usize::from(config.replicas) => {
                    match census.stray_members.last() {
                        Some(member) => Repair::Remove(member.clone()),
                        None => continue,
                    }
                }
                None => continue,
            };
            let Some(next) = next_config(config, &repair, proposals.next_id()) else {
                continue;
            };
            let key = TypedKey::new(key.clone());
            change = match change.clone().update(&key, version, &next) {
                Ok(change) => change,
                Err(error) => {
                    tracing::warn!(%error, "skipping an invalid shard change");
                    continue;
                }
            };
            match &repair {
                Repair::Learners { added, dropped } => {
                    learners = (learners + added.len()).saturating_sub(dropped.len());
                }
                Repair::Remove(_) => {
                    may_remove = false;
                    self.removed_at = Some(now);
                }
            }
            tracing::info!(
                register = %key.key(),
                epoch = %next.epoch,
                ?repair,
                "replacing shard members"
            );
            repairs.insert(key.key().clone(), repair);
        }
        (change, repairs)
    }
}

/// The learners `config`'s shard gains and loses this round, if any, with
/// room for `room` more learners in the cluster. Counts each new learner
/// into `topology`'s loads.
fn learners_for(
    topology: &mut Topology,
    config: &ShardConfig,
    census: &Census,
    room: usize,
) -> Option<Repair> {
    let replicas = usize::from(config.replicas);
    let named: Vec<NodeId> = config
        .members
        .iter()
        .chain(&config.learners)
        .cloned()
        .collect();
    let mut added = Vec::new();
    let missing = replicas.saturating_sub(census.counted.len()).min(room);
    if missing > 0 {
        let wanted = census.counted.len() + missing;
        let request = ShardRequest::new(&config.bucket_id, config.shard, clamp(wanted))
            .keep(&census.counted)
            .avoid(&named);
        added = topology.place(&request).added;
    }
    let mut dropped = census.stray_learners.clone();
    // Swap one learner on a suspect node for a live node, if placement
    // has one in a domain the shard does not use otherwise.
    if let Some(suspect) = census.suspect_learners.first() {
        let keep: Vec<NodeId> = census
            .counted
            .iter()
            .chain(&added)
            .filter(|node| *node != suspect)
            .cloned()
            .collect();
        let avoid: Vec<NodeId> = named.iter().chain(&added).cloned().collect();
        let request = ShardRequest::new(&config.bucket_id, config.shard, clamp(keep.len() + 1))
            .keep(&keep)
            .avoid(&avoid);
        let live = topology.place(&request).added.into_iter().find(|node| {
            topology
                .get(node)
                .is_some_and(|c| c.state == NodeState::Live)
        });
        if let Some(live) = live {
            added.push(live);
            dropped.push(suspect.clone());
        }
    }
    for node in &added {
        topology.assign(node);
    }
    (!added.is_empty() || !dropped.is_empty()).then_some(Repair::Learners { added, dropped })
}

fn clamp(count: usize) -> u8 {
    u8::try_from(count).unwrap_or(u8::MAX)
}

/// The configuration after `repair`: epoch `e+1`, the same primary, and
/// the members and learners `repair` leaves. `None` if the epoch is
/// exhausted.
fn next_config(config: &ShardConfig, repair: &Repair, proposal: ProposalId) -> Option<ShardConfig> {
    let epoch = config.epoch.checked_next()?;
    let (members, learners) = match repair {
        Repair::Learners { added, dropped } => (
            config.members.clone(),
            config
                .learners
                .iter()
                .filter(|learner| !dropped.contains(learner))
                .chain(added)
                .cloned()
                .collect(),
        ),
        Repair::Remove(member) => (
            config
                .members
                .iter()
                .filter(|m| *m != member)
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

impl<P: Placement> Placement for Replacement<P> {
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
            let (change, repairs) = self.plan_scanned(proposals);
            if !change.is_empty() {
                self.planned = Some(repairs);
                return Ok(Some(change));
            }
        }
        self.placement.plan(store, proposals).await
    }

    fn applied(&mut self, change: &ChangeSet, applied: &Applied) {
        let Some(repairs) = self.planned.take() else {
            self.placement.applied(change, applied);
            return;
        };
        if let Some(rejected) = &applied.rejected {
            tracing::info!(register = %rejected, "a shard changed under its replacement; planning again");
        }
        let done = applied
            .written
            .iter()
            .filter(|(key, _)| repairs.contains_key(key))
            .count();
        tracing::debug!(done, planned = repairs.len(), "replacement changes applied");
    }
}

#[cfg(test)]
mod tests;
