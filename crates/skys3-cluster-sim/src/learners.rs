//! Learners in the replicated services (plans M2-14, M2-15): a test driver
//! adds a spare node to each shard as a learner by a compare-and-swap of
//! the shard's register, as the coordinator will (plan M3-05), or, to
//! replace a lost member, a node outside the shard, a removed one
//! included; every node follows the registers that name it, and the
//! audits check rule R3, time the promotions, and measure how long shards
//! that lost a member had fewer than `replicas` copies (§16.3).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use skys3_control::ProposalIds;
use skys3_control::{ControlError, S3ControlStore};
use skys3_gateway::ShardRef;
use skys3_shard::replication::{BoxFuture, ControlRegisters, Replaced, ShardRegisters};
use skys3_sim::SimS3;
use skys3_types::{NodeId, Seq, ShardConfig};

use crate::node::ControlHandle;
use crate::replication::{
    Audit, REGISTER_RETRY, ReplicatedServices, ReplicatedShards, lock, register,
};

/// When the driver adds learners ([`ReplicatedServices::with_learners`]).
#[derive(Clone, Copy, Debug)]
pub(crate) struct LearnerPlan {
    /// From when, in simulated time.
    pub(crate) at: Duration,
    /// A seeded bug: the driver promotes each learner itself as soon as
    /// it adds it, before the learner holds anything.
    pub(crate) early: bool,
    /// Whether the driver replaces lost members only
    /// ([`ReplicatedServices::replacing_lost_members`]).
    pub(crate) replace: bool,
}

/// How often a node reads the registers that name it.
const FOLLOW_INTERVAL: Duration = Duration::from_millis(100);

/// Learners over a run, as the services counted them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LearnerCounts {
    /// Learners the driver added.
    pub added: usize,
    /// Promotions a primary proposed, counting each attempt.
    pub proposed: usize,
    /// Promotions the register accepted.
    pub promoted: usize,
    /// The longest a promotion's compare-and-swap took.
    pub slowest_promotion: Duration,
    /// The longest an acknowledged write took, among those in flight while
    /// a promotion's compare-and-swap was.
    pub slowest_write_during: Duration,
}

/// How long shards had fewer than `replicas` copies after losing a member
/// (§6.4, §16.3), as the audit sampled them after every step: from the
/// compare-and-swap that removed the member, until new writes had
/// `replicas` copies again, as a learner joined the acknowledgement set,
/// and until all data had them, as its backfill completed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DurabilityWindows {
    /// How many times a shard's register came to have fewer members than
    /// `replicas`.
    pub opened: usize,
    /// For each window that closed with a promotion: how long until new
    /// writes had `replicas` copies again.
    pub new_writes: Vec<Duration>,
    /// For each window that closed with a promotion: how long until all
    /// data had them again.
    pub all_data: Vec<Duration>,
}

impl DurabilityWindows {
    /// The longest of `times`.
    #[must_use]
    pub fn longest(times: &[Duration]) -> Duration {
        times.iter().copied().max().unwrap_or_default()
    }
}

/// A shard with fewer members than `replicas`: since when, and when new
/// writes and all data had `replicas` copies again, if they did.
#[derive(Debug, Clone, Copy)]
struct Window {
    since: Duration,
    new_writes: Option<Duration>,
    all_data: Option<Duration>,
}

/// What the learner audit keeps.
#[derive(Debug, Default)]
pub(crate) struct LearnerAudit {
    pub(crate) added: usize,
    /// Each promotion attempt: when it started, how long it took, and
    /// whether the register accepted it.
    promotions: Vec<(Duration, Duration, bool)>,
    /// Each acknowledged write: when it started, and how long it took.
    pub(crate) writes: Vec<(Duration, Duration)>,
    /// The windows of the shards with fewer members than `replicas` now.
    open: BTreeMap<ShardRef, Window>,
    windows: DurabilityWindows,
}

impl LearnerAudit {
    fn counts(&self) -> LearnerCounts {
        let during = |(start, took): &(Duration, Duration)| {
            self.promotions
                .iter()
                .any(|(at, cas, _)| *start < *at + *cas && *at < *start + *took)
        };
        LearnerCounts {
            added: self.added,
            proposed: self.promotions.len(),
            promoted: self.promotions.iter().filter(|p| p.2).count(),
            slowest_promotion: self
                .promotions
                .iter()
                .map(|p| p.1)
                .max()
                .unwrap_or_default(),
            slowest_write_during: self
                .writes
                .iter()
                .filter(|write| during(write))
                .map(|write| write.1)
                .max()
                .unwrap_or_default(),
        }
    }
}

fn now() -> Duration {
    turmoil::sim_elapsed().unwrap_or_default()
}

/// Whether `next` makes a learner of `current` a member.
fn promotes(current: &ShardConfig, next: &ShardConfig) -> bool {
    next.members.iter().any(|member| current.is_learner(member))
}

/// A node's shard registers, timing the promotions its primaries propose.
pub(crate) struct Audited {
    pub(crate) registers: ControlRegisters<ControlHandle>,
    pub(crate) audit: Arc<Mutex<Audit>>,
}

impl ShardRegisters for Audited {
    fn replace<'a>(
        &'a self,
        current: &'a ShardConfig,
        next: &'a ShardConfig,
    ) -> BoxFuture<'a, Result<Replaced, ControlError>> {
        Box::pin(async move {
            if !promotes(current, next) {
                return self.registers.replace(current, next).await;
            }
            let started = now();
            let replaced = self.registers.replace(current, next).await;
            let accepted = matches!(replaced, Ok(Replaced::Accepted));
            let promotion = (started, now() - started, accepted);
            lock(&self.audit).learners.promotions.push(promotion);
            replaced
        })
    }

    fn read<'a>(
        &'a self,
        shard: &'a skys3_log::ShardRef,
    ) -> BoxFuture<'a, Result<Option<ShardConfig>, ControlError>> {
        self.registers.read(shard)
    }
}

impl ReplicatedServices {
    /// The same services, whose nodes follow the shard registers that name
    /// them, as the coordinator's change propagation will have them do
    /// (plan M3-05), and whose primaries add a spare node to each shard
    /// they serve as a learner from `at` on, by a compare-and-swap of its
    /// register through the node's faulty control store: the test driver
    /// of §6.7's add-learner step. A spare is a node outside the shard's
    /// placement, so the learner starts with nothing; a cluster needs more
    /// nodes than `replicas` for one. The primaries promote their learners
    /// once they have caught up (§6.7).
    #[must_use]
    pub fn with_learners(mut self, at: Duration) -> Self {
        self.learners = Some(LearnerPlan {
            at,
            early: false,
            replace: false,
        });
        self
    }

    /// The same services, whose nodes follow the shard registers that name
    /// them, and whose primaries, from `at` on, add a learner to each shard
    /// they serve that has fewer members than `replicas` and no learner:
    /// a node outside the shard's placement if one is left, and otherwise
    /// a node removed from the shard, which rejoins as a learner with
    /// whatever its log still holds (§6.7). The primaries backfill and
    /// promote their learners; [`ReplicatedServices::durability_windows`]
    /// says how long that took.
    #[must_use]
    pub fn replacing_lost_members(mut self, at: Duration) -> Self {
        self.learners = Some(LearnerPlan {
            at,
            early: false,
            replace: true,
        });
        self
    }

    /// The same services with a seeded bug for the R3 audit to catch: the
    /// driver promotes each learner itself as soon as it adds it, before
    /// the learner holds a single record.
    #[must_use]
    pub fn promoting_early(mut self) -> Self {
        let at = self.learners.map_or(Duration::ZERO, |plan| plan.at);
        self.learners = Some(LearnerPlan {
            at,
            early: true,
            replace: false,
        });
        self
    }

    /// The learners added and promoted so far, and how long promotions and
    /// the writes during them took.
    #[must_use]
    pub fn learners(&self) -> LearnerCounts {
        lock(&self.audit).learners.counts()
    }

    /// How long shards that lost a member had fewer than `replicas` copies,
    /// as [`ReplicatedServices::sample_durability`] measured it.
    #[must_use]
    pub fn durability_windows(&self) -> DurabilityWindows {
        lock(&self.audit).learners.windows.clone()
    }

    /// Samples, at `now` in simulated time, which shards have fewer
    /// members than `replicas` in their register, and whether new writes,
    /// and all data, have `replicas` copies again: see
    /// [`DurabilityWindows`]. Call it after every step, as an invariant
    /// does; it never fails.
    ///
    /// # Errors
    ///
    /// None.
    pub fn sample_durability(&self, now: Duration) -> Result<(), String> {
        let mut audit = lock(&self.audit);
        let Audit {
            replicas,
            learners,
            store,
            ..
        } = &mut *audit;
        let Some(store) = store.as_ref() else {
            return Ok(());
        };
        let shards: BTreeSet<ShardRef> = replicas
            .iter()
            .map(|(_, config, _)| ShardRef {
                bucket: config.bucket_id.clone(),
                shard: config.shard,
            })
            .collect();
        for shard in shards {
            let Some(config) = register(store, &shard) else {
                continue;
            };
            let wanted = usize::from(config.replicas);
            let under = config.members.len() < wanted;
            let Some(window) = learners.open.get_mut(&shard) else {
                if under {
                    let window = Window {
                        since: now,
                        new_writes: None,
                        all_data: None,
                    };
                    learners.open.insert(shard, window);
                    learners.windows.opened += 1;
                }
                continue;
            };
            if !under {
                let window = *window;
                learners.open.remove(&shard);
                let windows = &mut learners.windows;
                let new_writes = window.new_writes.unwrap_or(now);
                windows.new_writes.push(new_writes - window.since);
                windows
                    .all_data
                    .push(window.all_data.unwrap_or(now) - window.since);
                continue;
            }
            // The primary of the register's configuration, as it serves now.
            let leader = replicas
                .iter()
                .rev()
                .filter(|(node, _, replica)| {
                    *node == config.primary
                        && replica.config().bucket_id == shard.bucket
                        && replica.config().shard == shard.shard
                        && !replica.is_stopped()
                })
                .find_map(|(_, _, replica)| replica.leader().cloned());
            let Some(leader) = leader else {
                continue;
            };
            let acking = leader.acking();
            if window.new_writes.is_none() && config.members.len() + acking.len() >= wanted {
                window.new_writes = Some(now);
            }
            let filled = acking.iter().any(|learner| leader.is_backfilled(learner));
            if window.new_writes.is_some() && window.all_data.is_none() && filled {
                window.all_data = Some(now);
            }
        }
        Ok(())
    }

    /// Checks rule R3 and that committed records survive (§6.3, §6.8):
    /// every member the register of a shard names now holds durably every
    /// record any primary of the shard has committed, a learner promoted to
    /// member included.
    ///
    /// # Errors
    ///
    /// The first member that misses a committed record.
    pub fn check_members_hold_commits(&self) -> Result<(), String> {
        let audit = lock(&self.audit);
        let Some(store) = &audit.store else {
            return Ok(());
        };
        let mut commits: BTreeMap<ShardRef, (Seq, NodeId)> = BTreeMap::new();
        for (node, config, replica) in &audit.replicas {
            let Some(leader) = replica.leader() else {
                continue;
            };
            let shard = ShardRef {
                bucket: config.bucket_id.clone(),
                shard: config.shard,
            };
            let commit = leader.commit();
            let highest = commits.entry(shard).or_insert((Seq::ZERO, node.clone()));
            if commit > highest.0 {
                *highest = (commit, node.clone());
            }
        }
        for (shard, (commit, by)) in commits {
            let Some(config) = register(store, &shard) else {
                continue;
            };
            for member in &config.members {
                let durable = audit.durable(member, &config);
                if durable < commit {
                    return Err(format!(
                        "{member} is a member of shard {shard} in epoch {}, but holds only seq \
                         {durable} durably, while {by} committed through seq {commit}",
                        config.epoch
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Opens, or brings up to date, the replica `shards` hold of each shard
/// whose register in `registers` names their node.
async fn follow(shards: &ReplicatedShards, registers: &S3ControlStore<SimS3>) {
    let node = &shards.node;
    for shard in shards.placement.keys() {
        let Some(config) = register(registers, shard) else {
            continue;
        };
        if !config.is_member(node) && !config.is_learner(node) {
            continue;
        }
        let open = shards.replication.set().get(&shard.into()).await;
        if let Some(replica) = &open {
            // A learner opens its replica again once it installs a
            // snapshot (§6.7).
            shards.track(replica);
        }
        if open.is_some_and(|replica| replica.config().epoch >= config.epoch) {
            continue;
        }
        if let Err(error) = shards.open_config(shard, &config).await {
            tracing::debug!(%shard, %error, "following the shard's register failed");
        }
    }
}

/// One life of a node following the shard registers that name it, and
/// adding learners to the shards it serves as primary
/// ([`ReplicatedServices::with_learners`]).
pub(crate) struct LearnerDriver {
    pub(crate) shards: ReplicatedShards,
    /// The node's control store, with its faults, for the driver's
    /// compare-and-swaps.
    pub(crate) store: ControlHandle,
    /// The raw store, for reading registers without faults.
    pub(crate) registers: S3ControlStore<SimS3>,
    /// Every node of the cluster.
    pub(crate) nodes: Vec<NodeId>,
    pub(crate) plan: LearnerPlan,
    pub(crate) ids: ProposalIds,
}

impl LearnerDriver {
    /// Follows the registers, and adds learners from the plan's time on,
    /// each in its own loop: a slow compare-and-swap holds up only the
    /// other additions.
    pub(crate) async fn run(mut self) {
        let (shards, registers) = (self.shards.clone(), self.registers.clone());
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(FOLLOW_INTERVAL).await;
                follow(&shards, &registers).await;
            }
        });
        let mut added = BTreeSet::new();
        loop {
            tokio::time::sleep(FOLLOW_INTERVAL).await;
            if now() >= self.plan.at {
                self.add(&mut added).await;
            }
        }
    }

    /// Adds a learner to each shard this node serves as primary that has
    /// none yet, once per shard and life, or, if the driver replaces lost
    /// members, whenever the shard has fewer members than `replicas`.
    async fn add(&mut self, added: &mut BTreeSet<ShardRef>) {
        let node = self.shards.node.clone();
        let placement = Arc::clone(&self.shards.placement);
        let replace = self.plan.replace;
        for (shard, placed) in placement.iter() {
            if added.contains(shard) && !replace {
                continue;
            }
            let Some(replica) = self.shards.replication.set().get(&shard.into()).await else {
                continue;
            };
            let current = replica.config();
            let serving = current.primary == node && replica.is_serving();
            if !serving || !current.learners.is_empty() {
                continue;
            }
            if replace && current.members.len() >= usize::from(current.replicas) {
                continue;
            }
            if register(&self.registers, shard).as_ref() != Some(&current) {
                continue;
            }
            let outside = |candidate: &&NodeId| {
                !current.is_member(candidate) && !current.is_learner(candidate)
            };
            let spare = self
                .nodes
                .iter()
                .filter(outside)
                .find(|candidate| !placed.is_member(candidate))
                .or_else(|| self.nodes.iter().find(|c| replace && outside(c)));
            let Some(spare) = spare.cloned() else {
                continue;
            };
            let Some(epoch) = current.epoch.checked_next() else {
                continue;
            };
            let next = ShardConfig {
                epoch,
                learners: vec![spare.clone()],
                proposal_id: self.ids.next_id(),
                ..current.clone()
            };
            let registers = ControlRegisters::new(self.store.clone(), REGISTER_RETRY);
            if !matches!(
                registers.replace(&current, &next).await,
                Ok(Replaced::Accepted)
            ) {
                continue;
            }
            tracing::info!(%shard, %spare, %epoch, "the test driver added a learner");
            added.insert(shard.clone());
            lock(&self.shards.audit).learners.added += 1;
            if self.plan.early {
                let Some(epoch) = epoch.checked_next() else {
                    continue;
                };
                let promoted = ShardConfig {
                    epoch,
                    members: next.members.iter().chain([&spare]).cloned().collect(),
                    learners: Vec::new(),
                    proposal_id: self.ids.next_id(),
                    ..next.clone()
                };
                let _ = registers.replace(&next, &promoted).await;
            }
        }
    }
}
