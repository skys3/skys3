//! Learners in the replicated services (plan M2-14): a test driver adds a
//! spare node to each shard as a learner by a compare-and-swap of the
//! shard's register, as the coordinator will (plan M3-05), every node
//! follows the registers that name it, and the audits check rule R3 and
//! time the promotions.

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

/// What the learner audit keeps.
#[derive(Debug, Default)]
pub(crate) struct LearnerAudit {
    pub(crate) added: usize,
    /// Each promotion attempt: when it started, how long it took, and
    /// whether the register accepted it.
    promotions: Vec<(Duration, Duration, bool)>,
    /// Each acknowledged write: when it started, and how long it took.
    pub(crate) writes: Vec<(Duration, Duration)>,
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
        self.learners = Some(LearnerPlan { at, early: false });
        self
    }

    /// The same services with a seeded bug for the R3 audit to catch: the
    /// driver promotes each learner itself as soon as it adds it, before
    /// the learner holds a single record.
    #[must_use]
    pub fn promoting_early(mut self) -> Self {
        let at = self.learners.map_or(Duration::ZERO, |plan| plan.at);
        self.learners = Some(LearnerPlan { at, early: true });
        self
    }

    /// The learners added and promoted so far, and how long promotions and
    /// the writes during them took.
    #[must_use]
    pub fn learners(&self) -> LearnerCounts {
        lock(&self.audit).learners.counts()
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
    /// none yet, once per shard and life.
    async fn add(&mut self, added: &mut BTreeSet<ShardRef>) {
        let node = self.shards.node.clone();
        let placement = Arc::clone(&self.shards.placement);
        for (shard, placed) in placement.iter() {
            if added.contains(shard) {
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
            if register(&self.registers, shard).as_ref() != Some(&current) {
                continue;
            }
            let spare = self.nodes.iter().find(|candidate| {
                !placed.is_member(candidate)
                    && !current.is_member(candidate)
                    && !current.is_learner(candidate)
            });
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
