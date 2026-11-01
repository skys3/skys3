//! Node services that replicate every shard to the members of its
//! placement (plan M2-07), with the audits the replication scenarios check:
//! commits (M2-07) and reads under leases (M2-09). Writes wait for the
//! members at most the configured acknowledgement timeout (M2-10), and
//! primaries remove members that stop responding (M2-11). Members can take
//! over from a primary that stays silent (M2-12), primaries hand
//! their shards off to members now and then (M2-13), and a test driver adds
//! learners that primaries promote (M2-14, in [`crate::learners`]). With
//! local copies (M2-16), nodes open their replicas the way a restarted
//! node does: from the register through the node's faulty control store,
//! or from the configuration of the latest `CONFIG` record while it cannot
//! be read.
//!
//! The replicated shards, register following, and the handoffs a
//! coordinator asks for are the production code of `skys3-cluster`
//! (plan M8-05a); this module adds the seeded bugs, the audits, which
//! the library's [`ReplicaObserver`] feeds, the stand-in for the shard
//! registers ([`SimRegisters`]), and the driver of periodic handoffs.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use skys3_cluster::{
    BoxFuture, Handoffs, Opening, ReplicaObserver, ReplicatedShards, ShardConfigs,
};
use skys3_control::{
    ControlError, ProposalIds, RegisterKey, RegisterKind, RetryPolicy, S3ControlStore, TypedKey,
};
use skys3_coord::HandoffSink;
use skys3_gateway::{
    ConditionFailed, LocalShards, Precondition, ReadId, ReadPlan, Registered, ShardError, ShardRef,
    ShardSummary, Shards, UploadParts,
};
use skys3_index::{Entry, ImportRanges, ListPage, ListQuery, Part, Upload};
use skys3_io::{Clock, MonoTime, MonotonicClock, SimMount};
use skys3_log::record::{Extent, ExtentRef};
use skys3_log::{LogStats, RecordBody, SegmentLog};
use skys3_net::{Listener, TurmoilNetwork};
use skys3_shard::replication::{ControlRegisters, HandedOff, Replication, ReplicationConfig};
use skys3_shard::{Grace, Role, Shard};
use skys3_sim::SimS3;
use skys3_types::{BucketDocument, Epoch, EpochSeq, NodeId, RegisterDocument, Seq, ShardConfig};

use crate::holders::{HolderAudit, HolderCounts, HolderFaults, Holders, own_plan};
use crate::learners::{Audited, LearnerAudit, LearnerDriver, LearnerPlan};
use crate::node::{BoxError, NodeEnv, NodeServices, TRANSPORT_PORT};

/// A node's replicated shards, as the library runs them.
pub(crate) type Replicas = ReplicatedShards<TurmoilNetwork, SimMount>;

/// Replicated node services: every node opens its replica of each shard
/// whose register names it, as primary or member, and the gateway's
/// writes on a primary commit once every member holds them durably
/// (§5.1). A primary removes a member that stays unresponsive for
/// `member_suspect_after` by a compare-and-swap of the shard's register,
/// through the node's control store and its faults (§6.4).
///
/// Nodes read the registers straight from the control bucket, where the
/// harness wrote the static placement, when they open a shard: a stand-in
/// for the shard map (plan M2-08) and the local copies of control state
/// (M2-16).
///
/// The services remember every replica and log of every life, so checks
/// can compare what primaries committed with what members made durable
/// ([`ReplicatedServices::check_commits`]) and count the I/O the writes
/// cost ([`ReplicatedServices::io`]).
///
/// They also audit reads against leases (§5.4). Each member's grace for
/// each shard is followed in simulated time, converted from the member's
/// own drifting clock, and every read a primary serves is checked against
/// it: a read served once some member's `primary_grace` had passed could be
/// stale, since that member could have taken over and acknowledged writes
/// the read misses ([`ReplicatedServices::check_reads`]). Within the drift
/// bound `ρ` no such read exists; beyond it, some do
/// ([`ReplicatedServices::leases`]). A planned handoff lets a member
/// propose without waiting for its grace (§5.4), so a read is also checked
/// against the step-downs members received: one served after a member of
/// its shard learned that the primary of the read's epoch stepped down
/// could miss writes of the new primary.
#[derive(Clone, Default)]
pub struct ReplicatedServices {
    config: ReplicationConfig,
    /// A seeded bug: primaries commit alone.
    alone: bool,
    /// A seeded bug: writes that were not acknowledged are sent again.
    resubmit: bool,
    /// A seeded bug: members answer reads from their own index.
    members_read: bool,
    /// Whether members take over from a silent primary (plan M2-12).
    takeover: bool,
    /// How often primaries hand a shard off, and until when (plan M2-13).
    handoffs: Option<HandoffPlan>,
    /// A seeded bug: a primary that stepped down still answers reads.
    stepped_down_read: bool,
    /// When the test driver adds learners, if it does (plan M2-14).
    pub(crate) learners: Option<LearnerPlan>,
    /// Whether nodes open replicas from their local copies (plan M2-16).
    local_copies: bool,
    /// How replicas behave as holders of read payload (plan M2-18).
    holders: HolderFaults,
    pub(crate) audit: Arc<Mutex<Audit>>,
    holder_audit: HolderAudit,
}

/// When primaries hand their shards off ([`ReplicatedServices::with_handoffs`]).
#[derive(Clone, Copy, Debug)]
struct HandoffPlan {
    every: Duration,
    until: Duration,
}

/// How the services' register writes retry.
pub(crate) const REGISTER_RETRY: RetryPolicy = RetryPolicy {
    max_attempts: 5,
    initial_backoff: Duration::from_millis(20),
    max_backoff: Duration::from_millis(200),
};

/// The configuration the register of `shard` holds now, read without a
/// request or a fault, or `None` if it holds none.
pub(crate) fn register(store: &S3ControlStore<SimS3>, shard: &ShardRef) -> Option<ShardConfig> {
    let key = TypedKey::shard(&shard.bucket, shard.shard);
    let object = store.objects().object(&store.object_key(key.key()))?;
    ShardConfig::from_json(&object.body).ok()
}

/// The shard configurations the harness gives the library's replicated
/// shards: the registers, read straight from the control bucket without a
/// request or a fault, as a stand-in for the shard map (plan M2-08) and
/// the local copies of control state (M2-16).
pub(crate) struct SimRegisters {
    /// The raw control store, if the cluster has one.
    pub(crate) store: Option<S3ControlStore<SimS3>>,
    /// The static placement the harness wrote, epoch 1 of every shard.
    pub(crate) placement: Arc<BTreeMap<ShardRef, ShardConfig>>,
    /// Whether a shard whose register cannot be read has its placement's
    /// configuration, as opening a shard takes it, or none, as following
    /// the registers does.
    pub(crate) placed: bool,
}

impl SimRegisters {
    /// The configuration of `shard`, as [`SimRegisters::placed`] says.
    pub(crate) fn current(&self, shard: &ShardRef) -> Option<ShardConfig> {
        let held = self.store.as_ref().and_then(|store| register(store, shard));
        if self.placed {
            held.or_else(|| self.placement.get(shard).cloned())
        } else {
            held
        }
    }
}

impl ShardConfigs for SimRegisters {
    /// The shards of the static placement, and those whose registers
    /// exist, which includes the shards of buckets the gateways created
    /// (plan M3-04), unknown to the static placement.
    fn shards(&self) -> BoxFuture<'_, Result<BTreeSet<ShardRef>, ControlError>> {
        let created: Vec<ShardRef> = self.store.as_ref().map_or_else(Vec::new, |store| {
            let prefix = store.config().prefix.as_str();
            store
                .objects()
                .keys()
                .into_iter()
                .filter_map(|key| {
                    let key = RegisterKey::new(key.strip_prefix(prefix)?).ok()?;
                    match key.kind() {
                        RegisterKind::Shard(bucket, shard) => Some(ShardRef { bucket, shard }),
                        _ => None,
                    }
                })
                .collect()
        });
        let shards = self.placement.keys().cloned().chain(created).collect();
        Box::pin(async move { Ok(shards) })
    }

    fn config<'a>(
        &'a self,
        shard: &'a ShardRef,
    ) -> BoxFuture<'a, Result<Option<ShardConfig>, ControlError>> {
        let config = self.current(shard);
        Box::pin(async move { Ok(config) })
    }
}

/// How long the seeded bug of
/// [`ReplicatedServices::resubmitting_failed_writes`] waits before it
/// sends a failed write again.
const RESUBMIT_DELAY: Duration = Duration::from_secs(1);

impl fmt::Debug for ReplicatedServices {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplicatedServices")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// Every replica and log of every life of every node.
#[derive(Default)]
pub(crate) struct Audit {
    pub(crate) replicas: Vec<(NodeId, ShardConfig, Shard<SimMount>)>,
    logs: Vec<SegmentLog<SimMount>>,
    /// Acknowledged writes that some member did not hold durably.
    early: Vec<String>,
    /// The I/O before the workload started.
    baseline: Option<IoCounts>,
    /// When the grace of each member of each shard passes, in simulated
    /// time since the run began, as of its latest grant (§5.4).
    graces: BTreeMap<(NodeId, ShardRef), Duration>,
    /// Reads under leases.
    leases: LeaseCounts,
    /// Reads served after a member's grace had passed.
    stale: Vec<String>,
    /// Writes that were not acknowledged in time after they got a
    /// position, by shard.
    late: Vec<(ShardRef, EpochSeq)>,
    /// The longest a write took to be acknowledged, in simulated time.
    slowest: Duration,
    /// The node that acknowledged writes in each epoch of each shard.
    committers: BTreeMap<(ShardRef, Epoch), NodeId>,
    /// Writes acknowledged in an epoch in which another node acknowledged
    /// writes.
    rivals: Vec<String>,
    /// The newest step-down each member of each shard received, with the
    /// epoch it names, in simulated time since the run began (§5.4).
    step_downs: BTreeMap<(NodeId, ShardRef), (Epoch, Duration)>,
    /// Planned handoffs over the run.
    handoffs: HandoffCounts,
    /// Learners and promotions over the run (plan M2-14).
    pub(crate) learners: LearnerAudit,
    /// The control store without faults, for reading registers.
    pub(crate) store: Option<S3ControlStore<SimS3>>,
    /// When a write to each shard was last acknowledged, in simulated time
    /// since the run began, and in which epoch.
    acknowledged: BTreeMap<ShardRef, (Duration, Epoch)>,
    /// The replica each node opened last of each shard, in its current
    /// life.
    current: BTreeMap<(NodeId, ShardRef), Shard<SimMount>>,
    /// The control store without faults, for the checks of the replicas
    /// against their registers.
    registers: Option<S3ControlStore<SimS3>>,
    /// What starts the handoffs a coordinator asks each node for, in the
    /// node's current life (plan M3-06).
    asked_handoffs: BTreeMap<NodeId, Handoffs<TurmoilNetwork, SimMount>>,
    /// The handoffs coordinators asked for and primaries began: the shard,
    /// the epoch the primary stepped down in, and when, in simulated time.
    asked: Vec<(ShardRef, Epoch, Duration)>,
    /// When a primary first served each epoch of each shard, in simulated
    /// time, as sampled after every step.
    serving: BTreeMap<(ShardRef, Epoch), Duration>,
}

/// A planned handoff a coordinator asked for (plan M3-06), and how long
/// the shard took to serve again under its new primary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HandoffTime {
    /// The shard.
    pub shard: ShardRef,
    /// The epoch the old primary stepped down in.
    pub epoch: Epoch,
    /// When it began stepping down, in simulated time since the run began.
    pub began: Duration,
    /// How long until a primary served a later epoch, if one did.
    pub took: Option<Duration>,
}

/// A node's replica of a shard in its current life, against the shard's
/// register ([`ReplicatedServices::replicas_now`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaState {
    /// The node.
    pub node: NodeId,
    /// The shard.
    pub shard: ShardRef,
    /// The replica's configuration.
    pub config: ShardConfig,
    /// The configuration the shard's register holds, if it holds one.
    pub register: Option<ShardConfig>,
    /// The replica's role.
    pub role: Role,
    /// Whether it serves as the shard's primary: it reconciled its members
    /// and has not stopped.
    pub serving: bool,
}

/// Planned handoffs over a run (§5.4), as the services counted them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HandoffCounts {
    /// Handoffs a primary began.
    pub begun: usize,
    /// Those whose step-down message the primary sent. The others, and
    /// those whose message was lost on the way, fall back to the members'
    /// grace.
    pub sent: usize,
    /// Step-downs a member received and acted on.
    pub received: usize,
}

/// Writes that were not acknowledged in time over a run (§5.2), as the
/// acknowledgement audit counted them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LateWrites {
    /// Writes answered `503 SlowDown` after their record got a position.
    pub sequenced: usize,
    /// Those whose record a primary of their shard committed later: not
    /// acknowledged, but applied.
    pub committed: usize,
}

/// Reads at primaries of replicated shards over a run, as the lease audit
/// counted them (§5.4).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LeaseCounts {
    /// Reads (`GET`, `HEAD`, and listings) a primary served.
    pub served: u64,
    /// Reads a serving primary refused because the lease from some member
    /// had lapsed.
    pub refused: u64,
    /// Reads a primary served after some member's `primary_grace` had
    /// passed: reads that would be stale had that member taken over.
    pub stale: u64,
}

/// The I/O of every replica's log over a run.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IoCounts {
    /// Group commits: one sync of each file written, on every node.
    pub group_commits: u64,
    /// Records those group commits made durable.
    pub records: u64,
}

impl ReplicatedServices {
    /// Services whose links use `config`.
    #[must_use]
    pub fn new(config: ReplicationConfig) -> Self {
        Self {
            config,
            ..Self::default()
        }
    }

    /// Services with a seeded bug for the routing audit to catch: a member
    /// answers the entry of a key from its own index instead of refusing,
    /// so a gateway with a stale map is served by a non-primary.
    #[must_use]
    pub fn members_serving_reads() -> Self {
        Self {
            members_read: true,
            ..Self::default()
        }
    }

    /// Services with a seeded bug for the checks to catch: each primary
    /// opens its shards as their only member, so it acknowledges writes
    /// that no other member holds.
    #[must_use]
    pub fn committing_alone() -> Self {
        Self {
            alone: true,
            ..Self::default()
        }
    }

    /// Services whose links use `config`, with a seeded bug for the checks
    /// to catch: a write the members did not acknowledge in time is sent
    /// again a second after its failure was answered, as a gateway that
    /// retried it on its own would. It then takes a new position, and can
    /// take effect over a write sent after the failure (§5.2).
    #[must_use]
    pub fn resubmitting_failed_writes(config: ReplicationConfig) -> Self {
        Self {
            resubmit: true,
            ..Self::new(config)
        }
    }

    /// The same services, whose members take over from a primary they
    /// have not heard from for `primary_grace` (§6.5). Takeovers need the
    /// control store.
    #[must_use]
    pub fn with_takeover(self) -> Self {
        Self {
            takeover: true,
            ..self
        }
    }

    /// The same services, whose primaries hand a shard off to one of its
    /// members every `every` of simulated time, until `until` (§5.4): each
    /// node's primaries take turns, one shard per round, and the member
    /// each hands off to rotates. A handoff leaves the old primary out of
    /// the next configuration, so a shard of `n` members is handed off at
    /// most `n − 1` times. Members take over too
    /// ([`ReplicatedServices::with_takeover`]), which falls back to the
    /// grace when a step-down is lost.
    #[must_use]
    pub fn with_handoffs(self, every: Duration, until: Duration) -> Self {
        Self {
            handoffs: Some(HandoffPlan { every, until }),
            ..self.with_takeover()
        }
    }

    /// The same services with a seeded bug for the lease audit to catch: a
    /// primary that stepped down for a planned handoff still answers reads
    /// from its own index, though the member it stepped down to may have
    /// taken over and acknowledged writes since.
    #[must_use]
    pub fn stepped_down_serving_reads(self) -> Self {
        Self {
            stepped_down_read: true,
            ..self
        }
    }

    /// The same services, whose nodes open their replicas as a restarted
    /// node does (§6.2): each in its shard's register, read through the
    /// node's control store and its faults, or, while the register cannot
    /// be read, in the configuration of the latest `CONFIG` record the node
    /// applied ([`Replication::resume`]). Without it, nodes read the
    /// registers without faults, as a stand-in for the local copies.
    #[must_use]
    pub fn with_local_copies(self) -> Self {
        Self {
            local_copies: true,
            ..self
        }
    }

    /// The same services, whose replicas behave as `faults` says when they
    /// hold the payload a gateway reads (plan M2-18).
    #[must_use]
    pub fn with_holders(self, faults: HolderFaults) -> Self {
        Self {
            holders: faults,
            ..self
        }
    }

    /// What the replicas did as holders of read payload, over every life.
    #[must_use]
    pub fn holder_counts(&self) -> HolderCounts {
        self.holder_audit.counts()
    }

    /// Checks that no holder served a fetch under a registration that had
    /// lapsed (§8.7).
    ///
    /// # Errors
    ///
    /// The first such fetch.
    pub fn check_holders(&self) -> Result<(), String> {
        self.holder_audit.check()
    }

    /// The reads each node's replicas served as their holder, over every
    /// life: the registrations they accepted. Nodes that served none are
    /// left out.
    #[must_use]
    pub fn holder_reads(&self) -> BTreeMap<NodeId, u64> {
        self.holder_audit.by_holder()
    }

    /// Each node's replica of each shard in the node's current life, with
    /// the register's configuration, in node and shard order. A node that
    /// is down, or has not opened the shard in this life, has none.
    #[must_use]
    pub fn replicas_now(&self, up: &[bool]) -> Vec<ReplicaState> {
        let audit = self.audit();
        audit
            .current
            .iter()
            .filter(|((node, _), _)| node_position(node).is_some_and(|n| up.get(n) == Some(&true)))
            .map(|((node, shard), replica)| ReplicaState {
                node: node.clone(),
                shard: shard.clone(),
                config: replica.config(),
                register: audit
                    .registers
                    .as_ref()
                    .and_then(|store| register(store, shard)),
                role: replica.role(),
                serving: replica.role() != Role::Member
                    && replica.role() != Role::Learner
                    && replica.is_serving()
                    && !replica.is_stopped(),
            })
            .collect()
    }

    /// When each shard last acknowledged a write, in simulated time since
    /// the run began, and in which epoch.
    #[must_use]
    pub fn acknowledged(&self) -> BTreeMap<ShardRef, (Duration, Epoch)> {
        self.audit().acknowledged.clone()
    }

    /// The planned handoffs so far.
    #[must_use]
    pub fn handoffs(&self) -> HandoffCounts {
        self.audit().handoffs
    }

    /// The shards that a node other than their first primary serves as
    /// primary now: those a member took over.
    #[must_use]
    pub fn taken_over(&self) -> BTreeSet<ShardRef> {
        let audit = self.audit();
        let mut first = BTreeMap::new();
        for (_, config, replica) in &audit.replicas {
            first
                .entry(replica.shard().clone())
                .or_insert_with(|| config.primary.clone());
        }
        audit
            .replicas
            .iter()
            .filter(|(node, _, replica)| {
                first.get(replica.shard()) != Some(node)
                    && replica.role() == Role::Primary
                    && replica.is_serving()
                    && !replica.is_stopped()
            })
            .map(|(_, _, replica)| ShardRef {
                bucket: replica.shard().bucket.clone(),
                shard: replica.shard().shard,
            })
            .collect()
    }

    /// Checks the commit rule against what members really hold: no primary
    /// has committed a record, nor acknowledged a write, that some member
    /// of its shard did not hold durably then. A replica's durable run only
    /// grows across restarts, and shrinks only where a takeover truncates
    /// records that were never committed (§6.6), so comparing with the
    /// longest of every life's runs misses nothing committed. It also
    /// checks that no two nodes acknowledged writes in the same epoch of a
    /// shard: one committing primary per epoch (§6.8).
    ///
    /// # Errors
    ///
    /// What was committed or acknowledged too early, or by a second node
    /// in an epoch.
    pub fn check_commits(&self) -> Result<(), String> {
        let audit = self.audit();
        if let Some(early) = audit.early.first().or(audit.rivals.first()) {
            return Err(early.clone());
        }
        for (node, config, replica) in &audit.replicas {
            let Some(leader) = replica.leader() else {
                continue;
            };
            let commit = leader.commit();
            for member in leader.members() {
                let durable = audit.durable(&member, config);
                if commit > durable {
                    return Err(format!(
                        "{node} committed shard {} through seq {commit}, but {member} holds \
                         only seq {durable} durably",
                        replica.shard()
                    ));
                }
            }
        }
        Ok(())
    }

    /// The group commits and records of every log of every life since the
    /// workload started: what the clients' writes cost, without opening
    /// the shards.
    #[must_use]
    pub fn io(&self) -> IoCounts {
        let audit = self.audit();
        let total = audit.io();
        let baseline = audit.baseline.unwrap_or_default();
        IoCounts {
            group_commits: total.group_commits - baseline.group_commits,
            records: total.records - baseline.records,
        }
    }

    /// Checks every read a primary served against its members' grace: none
    /// was served once some member's `primary_grace` had passed since it
    /// last granted the primary a lease, nor once some member had received
    /// the step-down of the primary of the read's epoch. Such a member
    /// could have taken over, and the read could then miss a write the new
    /// primary acknowledged (§5.4, §6.8).
    ///
    /// # Errors
    ///
    /// The first read that could be stale.
    pub fn check_reads(&self) -> Result<(), String> {
        match self.audit().stale.first() {
            Some(stale) => Err(stale.clone()),
            None => Ok(()),
        }
    }

    /// What the lease audit counted so far.
    #[must_use]
    pub fn leases(&self) -> LeaseCounts {
        self.audit().leases
    }

    /// The longest an acknowledged write took, in simulated time: a write
    /// waits for a member until its removal (§6.4).
    #[must_use]
    pub fn slowest_write(&self) -> Duration {
        self.audit().slowest
    }

    /// How many shards a primary has reconfigured since the placement,
    /// by removing members (§6.4).
    #[must_use]
    pub fn reconfigured(&self) -> usize {
        let audit = self.audit();
        let shards: BTreeSet<_> = audit
            .replicas
            .iter()
            .filter(|(_, config, replica)| {
                replica.leader().is_some() && replica.config().epoch > config.epoch
            })
            .map(|(_, _, replica)| replica.shard().clone())
            .collect();
        shards.len()
    }

    /// The writes that were not acknowledged in time after they got a
    /// position, and how many of them a primary has committed since.
    #[must_use]
    pub fn late_writes(&self) -> LateWrites {
        let audit = self.audit();
        let committed = audit
            .late
            .iter()
            .filter(|(shard, position)| {
                audit.replicas.iter().any(|(_, config, replica)| {
                    config.bucket_id == shard.bucket
                        && config.shard == shard.shard
                        && replica
                            .leader()
                            .is_some_and(|leader| leader.commit() >= position.seq)
                })
            })
            .count();
        LateWrites {
            sequenced: audit.late.len(),
            committed,
        }
    }

    /// The configuration each shard's register holds now, for every shard
    /// a node opened, read without faults.
    #[must_use]
    pub fn shard_registers(&self) -> BTreeMap<ShardRef, ShardConfig> {
        let audit = self.audit();
        let Some(store) = audit.registers.as_ref() else {
            return BTreeMap::new();
        };
        audit
            .replicas
            .iter()
            .map(|(_, _, replica)| gateway_ref(replica.shard()))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter_map(|shard| register(store, &shard).map(|config| (shard, config)))
            .collect()
    }

    /// What starts the handoffs a coordinator asks `node` for, on its
    /// replicas in its current life (plan M3-06).
    #[must_use]
    pub fn handoff_sink(&self, node: &NodeId) -> Option<Arc<dyn HandoffSink>> {
        let handoffs = self.audit().asked_handoffs.get(node)?.clone();
        Some(Arc::new(handoffs))
    }

    /// Records, at `now` in simulated time, which epochs of which shards a
    /// primary serves, for [`ReplicatedServices::handoff_times`]. Call it
    /// after every step, as an invariant does; it never fails.
    ///
    /// # Errors
    ///
    /// None.
    pub fn sample_serving(&self, now: Duration) -> Result<(), String> {
        let mut audit = self.audit();
        let serving: Vec<(ShardRef, Epoch)> = audit
            .current
            .iter()
            .filter(|(_, replica)| {
                replica.leader().is_some() && replica.is_serving() && !replica.is_stopped()
            })
            .map(|((_, shard), replica)| (shard.clone(), replica.config().epoch))
            .collect();
        for key in serving {
            audit.serving.entry(key).or_insert(now);
        }
        Ok(())
    }

    /// The handoffs coordinators asked for and primaries began, in the
    /// order they began, with how long each shard took to serve again, as
    /// [`ReplicatedServices::sample_serving`] saw it.
    #[must_use]
    pub fn handoff_times(&self) -> Vec<HandoffTime> {
        let audit = self.audit();
        audit
            .asked
            .iter()
            .map(|(shard, epoch, began)| {
                let served = audit
                    .serving
                    .iter()
                    .filter(|((of, served), at)| of == shard && served > epoch && *at >= began)
                    .map(|(_, at)| *at)
                    .min();
                HandoffTime {
                    shard: shard.clone(),
                    epoch: *epoch,
                    began: *began,
                    took: served.map(|at| at - *began),
                }
            })
            .collect()
    }

    fn audit(&self) -> MutexGuard<'_, Audit> {
        lock(&self.audit)
    }
}

/// What one life of a node's replicated shards report, into the audit:
/// the replicas they open and find, whose grace it follows, and the
/// handoffs coordinators ask for (plan M3-06).
struct Auditor {
    node: NodeId,
    /// The node's clock in this life.
    clock: Arc<MonotonicClock>,
    replication: Replication<TurmoilNetwork, SimMount>,
    audit: Arc<Mutex<Audit>>,
    /// The shards whose grace this life follows.
    followed: Mutex<BTreeSet<ShardRef>>,
}

impl ReplicaObserver<SimMount> for Auditor {
    /// Audits `replica`, which this node opened, unless it is audited
    /// already, and follows its grace.
    fn opened(&self, replica: &Shard<SimMount>) {
        let shard = &gateway_ref(replica.shard());
        if let Some(grace) = self.replication.grace(replica.shard()) {
            self.follow_grace(shard, grace);
        }
        let mut audit = lock(&self.audit);
        audit
            .current
            .insert((self.node.clone(), shard.clone()), replica.clone());
        if !audit
            .replicas
            .iter()
            .any(|(_, _, open)| open.durable().same_channel(&replica.durable()))
        {
            audit
                .replicas
                .push((self.node.clone(), replica.config(), replica.clone()));
        }
    }

    /// Audits `replica`, this node's replica of its shard, unless it is
    /// audited already: a learner that installed a snapshot opened it
    /// again (§6.7). A learner's earlier replicas of the shard leave the
    /// audit then, since the snapshot discarded what they held.
    fn found(&self, replica: &Shard<SimMount>) {
        let mut audit = lock(&self.audit);
        let durable = replica.durable();
        if audit
            .replicas
            .iter()
            .any(|(_, _, open)| open.durable().same_channel(&durable))
        {
            return;
        }
        if replica.role() == Role::Learner {
            audit
                .replicas
                .retain(|(node, _, old)| *node != self.node || old.shard() != replica.shard());
        }
        let key = (self.node.clone(), gateway_ref(replica.shard()));
        audit.current.insert(key, replica.clone());
        audit
            .replicas
            .push((self.node.clone(), replica.config(), replica.clone()));
    }

    fn handoff_began(&self, shard: &ShardRef, epoch: Epoch) {
        let at = turmoil::sim_elapsed().unwrap_or_default();
        let mut audit = lock(&self.audit);
        audit.handoffs.begun += 1;
        audit.asked.push((shard.clone(), epoch, at));
    }

    fn handoff_ended(
        &self,
        _shard: &ShardRef,
        result: &Result<HandedOff, skys3_shard::ShardError>,
    ) {
        if matches!(result, Ok(handed) if handed.sent) {
            lock(&self.audit).handoffs.sent += 1;
        }
    }
}

impl Auditor {
    /// Follows the grace of this node's replica of `shard` into the audit:
    /// in simulated time, when it passes as of each grant. Each life
    /// follows a shard once.
    fn follow_grace(&self, shard: &ShardRef, grace: Arc<Grace>) {
        if !lock(&self.followed).insert(shard.clone()) {
            return;
        }
        let (clock, audit) = (Arc::clone(&self.clock), Arc::clone(&self.audit));
        let key = (self.node.clone(), shard.clone());
        let mut granted = grace.subscribe();
        let mut step_downs = grace.subscribe_step_downs();
        let released = (Arc::clone(&audit), key.clone());
        tokio::spawn(async move {
            let (audit, key) = released;
            loop {
                let stepped = *step_downs.borrow_and_update();
                if let Some(epoch) = stepped {
                    let now = turmoil::sim_elapsed().unwrap_or_default();
                    let mut audit = lock(&audit);
                    audit.handoffs.received += 1;
                    audit.step_downs.insert(key.clone(), (epoch, now));
                }
                if step_downs.changed().await.is_err() {
                    return;
                }
            }
        });
        tokio::spawn(async move {
            loop {
                granted.borrow_and_update();
                let passes = simulated(&clock, grace.passes_at());
                lock(&audit).graces.insert(key.clone(), passes);
                if granted.changed().await.is_err() {
                    return;
                }
            }
        });
    }
}

/// The gateway's name of a replica's shard.
fn gateway_ref(shard: &skys3_log::ShardRef) -> ShardRef {
    ShardRef {
        bucket: shard.bucket.clone(),
        shard: shard.shard,
    }
}

/// The position of `node` in the cluster, from 0, from its ID
/// (`node-<n>`, from 1).
fn node_position(node: &NodeId) -> Option<usize> {
    let n: usize = node.as_str().strip_prefix("node-")?.parse().ok()?;
    n.checked_sub(1)
}

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The simulated time since the run began at which `clock`, a node's clock
/// in this life, reads `local`. Call it in the node's host.
fn simulated(clock: &MonotonicClock, local: MonoTime) -> Duration {
    let now = turmoil::sim_elapsed().unwrap_or_default();
    now + clock
        .runtime_deadline(local)
        .saturating_duration_since(tokio::time::Instant::now())
}

impl Audit {
    fn io(&self) -> IoCounts {
        self.logs.iter().map(SegmentLog::stats).fold(
            IoCounts::default(),
            |counts, stats: LogStats| IoCounts {
                group_commits: counts.group_commits + stats.group_commits,
                records: counts.records + stats.records,
            },
        )
    }

    /// The last `seq` `node` ever held durably in the shard of `config`.
    pub(crate) fn durable(&self, node: &NodeId, config: &ShardConfig) -> Seq {
        self.replicas
            .iter()
            .filter(|(n, c, _)| {
                n == node && c.bucket_id == config.bucket_id && c.shard == config.shard
            })
            .map(|(_, _, replica)| *replica.durable().borrow())
            .max()
            .unwrap_or(Seq::ZERO)
    }
}

impl NodeServices for ReplicatedServices {
    type Shards = AuditedShards;

    fn handoff_sink(&self, node: &NodeId) -> Option<Arc<dyn HandoffSink>> {
        ReplicatedServices::handoff_sink(self, node)
    }

    /// Ready once every primary opened so far serves, and every member
    /// granted it a lease, unless it stopped for good, as a primary that
    /// handed its shard off does. The I/O counts start then.
    fn ready(&self) -> bool {
        let mut audit = self.audit();
        let ready = audit.replicas.iter().all(|(_, _, replica)| {
            replica.stepped_down().is_some()
                || replica.leader().is_none_or(|leader| {
                    replica.is_serving()
                        && leader
                            .members()
                            .iter()
                            .all(|member| leader.lease(member).is_some())
                })
        });
        if ready && audit.baseline.is_none() {
            audit.baseline = Some(audit.io());
        }
        ready
    }

    async fn start(&self, env: NodeEnv) -> Result<AuditedShards, BoxError> {
        let (shards, listener) = self.start_replicas(&env).await?;
        let serving = shards.replication().clone();
        tokio::spawn(async move { serving.serve(listener).await });
        Ok(shards)
    }
}

impl ReplicatedServices {
    /// The replicated shards of one life of a node, and its bound
    /// transport listener, which the caller serves.
    pub(crate) async fn start_replicas(
        &self,
        env: &NodeEnv,
    ) -> Result<(AuditedShards, Listener<TurmoilNetwork>), BoxError> {
        let set = env.shards.set().clone();
        {
            let mut audit = self.audit();
            audit.logs.extend(set.logs().map(|(_, log)| log.clone()));
            // A new life: the replicas of the last one are gone.
            audit.current.retain(|(node, _), _| *node != env.node);
        }
        let mut replication = Replication::new(
            env.node.clone(),
            set,
            env.transport.clone(),
            env.peers.clone(),
            Arc::clone(&env.clock) as Arc<dyn Clock>,
            self.config,
        );
        let store = env.control.store();
        if let Some(store) = &store {
            self.audit().registers = Some(store.inner().clone());
            let registers = ControlRegisters::new(store.clone(), REGISTER_RETRY);
            let ids = ProposalIds::seeded(env.seed);
            replication = if self.learners.is_some() {
                let audit = Arc::clone(&self.audit);
                lock(&audit).store = Some(store.inner().clone());
                replication.with_removal(Audited { registers, audit }, ids)
            } else {
                replication.with_removal(registers, ids)
            };
            if self.takeover {
                replication = replication.with_takeover();
            }
        }
        let listener = env
            .transport
            .bind((std::net::Ipv4Addr::UNSPECIFIED, TRANSPORT_PORT).into())
            .await?;
        let auditor = Auditor {
            node: env.node.clone(),
            clock: Arc::clone(&env.clock),
            replication: replication.clone(),
            audit: Arc::clone(&self.audit),
            followed: Mutex::default(),
        };
        // Nodes read the registers straight from the control bucket, as a
        // stand-in for the local copies, unless they open their replicas
        // as a restarted node does.
        let configs = Arc::new(SimRegisters {
            store: store.as_ref().map(|store| store.inner().clone()),
            placement: Arc::clone(&env.placement),
            placed: true,
        });
        let opening = if self.local_copies {
            Opening::Resume
        } else {
            Opening::Configs(Arc::clone(&configs) as Arc<dyn ShardConfigs>)
        };
        let replicas = Replicas::new(env.shards.clone(), replication.clone())
            .with_opening(opening)
            .with_observer(Arc::new(auditor));
        self.audit()
            .asked_handoffs
            .insert(env.node.clone(), replicas.handoffs());
        if let Some(handoffs) = self.handoffs {
            let driver = HandoffDriver {
                replication,
                node: env.node.clone(),
                position: env.position,
                handoffs,
                audit: Arc::clone(&self.audit),
            };
            tokio::spawn(driver.run());
        }
        let shards = AuditedShards {
            replicas,
            configs,
            alone: self.alone,
            resubmit: self.resubmit,
            members_read: self.members_read,
            stepped_down_read: self.stepped_down_read,
            audit: Arc::clone(&self.audit),
            holders: Holders::new(
                self.holders,
                env.seed ^ 0x484f_4c44,
                self.holder_audit.clone(),
            ),
        };
        if self.local_copies {
            // Every replica the node keeps a configuration of, with the
            // registers read concurrently, as a restarted node does.
            shards.replicas.resume_kept().await?;
        }
        if let (Some(plan), Some(store)) = (self.learners, store) {
            let mut nodes: Vec<NodeId> = env.peers.keys().cloned().collect();
            nodes.push(env.node.clone());
            nodes.sort();
            let driver = LearnerDriver {
                shards: shards.clone(),
                registers: store.inner().clone(),
                store,
                nodes,
                plan,
                ids: ProposalIds::seeded(env.seed ^ 0x4c45_4152),
            };
            tokio::spawn(driver.run());
        }
        Ok((shards, listener))
    }
}

/// A node's primaries handing their shards off now and then, in one life
/// ([`ReplicatedServices::with_handoffs`]).
struct HandoffDriver {
    replication: Replication<TurmoilNetwork, SimMount>,
    node: NodeId,
    position: usize,
    handoffs: HandoffPlan,
    audit: Arc<Mutex<Audit>>,
}

impl HandoffDriver {
    async fn run(self) {
        let every = self.handoffs.every;
        // Nodes take turns within each round.
        let offset = every / 4 * u32::try_from(self.position % 4).unwrap_or(0);
        tokio::time::sleep(offset).await;
        for round in 0.. {
            tokio::time::sleep(every).await;
            if turmoil::sim_elapsed().unwrap_or_default() >= self.handoffs.until {
                return;
            }
            self.hand_off_one(round).await;
        }
    }

    /// Hands off the first shard this node serves as primary that has
    /// another member, to the member `round` picks.
    async fn hand_off_one(&self, round: usize) {
        let set = self.replication.set();
        for shard in set.shards().await {
            let Some(replica) = set.get(&shard).await else {
                continue;
            };
            if replica.role() != Role::Primary || !replica.is_serving() {
                continue;
            }
            let config = replica.config();
            let others: Vec<&NodeId> = config.members.iter().filter(|m| **m != self.node).collect();
            let Some(&to) = others.get(round % others.len().max(1)) else {
                continue;
            };
            lock(&self.audit).handoffs.begun += 1;
            match self.replication.hand_off(&shard, to).await {
                Ok(handed) if handed.sent => lock(&self.audit).handoffs.sent += 1,
                Ok(_) => {}
                Err(error) => tracing::debug!(%shard, %error, "a handoff failed"),
            }
            return;
        }
    }
}

/// The gateway's shards on a replicated node: the library's
/// [`ReplicatedShards`], with replicas open in their register's
/// configuration (or their placement's), with the seeded bugs of the
/// services, and with reads and acknowledged writes checked against the
/// audit.
#[derive(Clone)]
pub struct AuditedShards {
    pub(crate) replicas: Replicas,
    /// The registers, read without the node's faults, and the placement.
    pub(crate) configs: Arc<SimRegisters>,
    alone: bool,
    resubmit: bool,
    members_read: bool,
    stepped_down_read: bool,
    pub(crate) audit: Arc<Mutex<Audit>>,
    /// The node's replicas as holders of read payload.
    holders: Holders,
}

impl fmt::Debug for AuditedShards {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuditedShards")
            .field("node", self.node_id())
            .finish_non_exhaustive()
    }
}

impl AuditedShards {
    /// The node's replicas, as a single node serves them.
    fn local(&self) -> &LocalShards<SimMount> {
        self.replicas.local()
    }

    /// The node's replication.
    pub(crate) fn replication(&self) -> &Replication<TurmoilNetwork, SimMount> {
        self.replicas.replication()
    }

    /// The node.
    pub(crate) fn node_id(&self) -> &NodeId {
        self.replicas.node_id()
    }

    /// The placement the harness wrote, epoch 1 of every shard.
    pub(crate) fn placement(&self) -> &Arc<BTreeMap<ShardRef, ShardConfig>> {
        &self.configs.placement
    }

    /// The shard's configuration now: its register's, or the placement's
    /// if the register cannot be read.
    pub(crate) fn current(&self, shard: &ShardRef) -> Option<ShardConfig> {
        self.configs.current(shard)
    }

    /// Whether this node's replica of `shard` has led it as primary in
    /// this life or an earlier one.
    pub(crate) fn has_led(&self, shard: &ShardRef) -> bool {
        let node = self.node_id();
        lock(&self.audit)
            .replicas
            .iter()
            .any(|(of, config, replica)| {
                of == node
                    && config.bucket_id == shard.bucket
                    && config.shard == shard.shard
                    && replica.leader().is_some()
            })
    }

    /// Records an acknowledgement of `position` that some member of the
    /// shard did not hold durably. The members are those of the register
    /// now: a removal takes effect only once its compare-and-swap landed,
    /// and only removes members, so every member it names must hold every
    /// write acknowledged so far.
    fn audit_acknowledged(&self, shard: &ShardRef, position: EpochSeq) {
        let mut audit = lock(&self.audit);
        let now = turmoil::sim_elapsed().unwrap_or_default();
        audit
            .acknowledged
            .insert(shard.clone(), (now, position.epoch));
        let key = (shard.clone(), position.epoch);
        match audit.committers.get(&key) {
            Some(other) if *other != *self.node_id() => {
                let rival = format!(
                    "{} acknowledged the write at {position} of shard {shard}, but {other} \
                     acknowledged writes in epoch {} too",
                    self.node_id(),
                    position.epoch
                );
                audit.rivals.push(rival);
            }
            Some(_) => {}
            None => {
                audit.committers.insert(key, self.node_id().clone());
            }
        }
        drop(audit);
        let Some(config) = self.current(shard) else {
            return;
        };
        let config = &config;
        let mut audit = lock(&self.audit);
        for member in &config.members {
            let durable = audit.durable(member, config);
            if durable < position.seq {
                let early = format!(
                    "{} acknowledged the write at {position} of shard {shard} while {member} \
                     held only seq {durable} durably",
                    self.node_id()
                );
                audit.early.push(early);
            }
        }
    }

    /// Counts a read of `shard` that ended with `result`, and checks a
    /// served one against the grace of the shard's other members.
    async fn audit_read<T>(&self, shard: &ShardRef, result: &Result<T, ShardError>) {
        let Some(replica) = self.replication().set().get(&shard.into()).await else {
            return;
        };
        let Some(leader) = replica.leader() else {
            return;
        };
        let now = turmoil::sim_elapsed().unwrap_or_default();
        let mut audit = lock(&self.audit);
        if result.is_err() {
            if replica.is_serving() && !leader.holds_leases() {
                audit.leases.refused += 1;
            }
            return;
        }
        audit.leases.served += 1;
        // A member that received the step-down of the primary of the
        // epoch the read's index state is in may have taken over since.
        let epoch = replica.applied().epoch;
        let stale: Vec<String> = audit
            .step_downs
            .iter()
            .filter(|((member, of), (stepped, at))| {
                of == shard && *member != *self.node_id() && *stepped >= epoch && *at <= now
            })
            .map(|((member, _), (stepped, at))| {
                format!(
                    "{} served a read of shard {shard} in epoch {epoch} at {now:?}, but its \
                     primary had stepped down to {member} in epoch {stepped} at {at:?}",
                    self.node_id()
                )
            })
            .collect();
        audit.leases.stale += stale.len() as u64;
        audit.stale.extend(stale);
        for member in leader.members() {
            let Some(&passed) = audit.graces.get(&(member.clone(), shard.clone())) else {
                continue;
            };
            if passed <= now {
                audit.leases.stale += 1;
                let stale = format!(
                    "{} served a read of shard {shard} at {now:?}, but the primary_grace of \
                     {member} had passed at {passed:?}",
                    self.node_id()
                );
                audit.stale.push(stale);
            }
        }
    }
}

impl Shards for AuditedShards {
    async fn open(&self, shard: &ShardRef, bucket: &BucketDocument) -> Result<(), ShardError> {
        if self.alone {
            return self.local().open(shard, bucket).await;
        }
        self.replicas.open(shard, bucket).await
    }

    async fn seal(&self, shard: &ShardRef) -> Result<ShardSummary, ShardError> {
        self.replicas.seal(shard).await
    }

    async fn unseal(&self, shard: &ShardRef) -> Result<(), ShardError> {
        self.replicas.unseal(shard).await
    }

    async fn remove(&self, shard: &ShardRef) -> Result<(), ShardError> {
        self.replicas.remove(shard).await
    }

    async fn entry(&self, shard: &ShardRef, key: &str) -> Result<Option<Entry>, ShardError> {
        if self.members_read
            && let Some(replica) = self.local().set().get(&shard.into()).await
            && replica.role() == Role::Member
        {
            // The seeded bug: read the member's index, which may lag its
            // primary's.
            let read = replica
                .index()
                .read()
                .and_then(|r| r.entry(&shard.into(), key));
            return read.map_err(|error| ShardError::Unavailable {
                shard: shard.clone(),
                reason: error.to_string(),
            });
        }
        if self.stepped_down_read
            && let Some(replica) = self.local().set().get(&shard.into()).await
            && replica.stepped_down().is_some()
        {
            // The seeded bug: a primary that stepped down reads its index.
            let read = replica
                .index()
                .read()
                .and_then(|r| r.entry(&shard.into(), key))
                .map_err(|error| ShardError::Unavailable {
                    shard: shard.clone(),
                    reason: error.to_string(),
                });
            self.audit_read(shard, &read).await;
            return read;
        }
        let entry = self.replicas.entry(shard, key).await;
        self.audit_read(shard, &entry).await;
        entry
    }

    async fn import_progress(
        &self,
        shard: &ShardRef,
    ) -> Result<Option<(u64, ImportRanges)>, ShardError> {
        self.replicas.import_progress(shard).await
    }

    async fn list(&self, shard: &ShardRef, query: &ListQuery) -> Result<ListPage, ShardError> {
        let page = self.replicas.list(shard, query).await;
        self.audit_read(shard, &page).await;
        page
    }

    async fn upload(
        &self,
        shard: &ShardRef,
        key: &str,
        upload: EpochSeq,
        after: u16,
        limit: usize,
    ) -> Result<Option<UploadParts>, ShardError> {
        self.replicas.upload(shard, key, upload, after, limit).await
    }

    async fn uploads(
        &self,
        shard: &ShardRef,
        prefix: &str,
        after: Option<(String, Option<EpochSeq>)>,
        limit: usize,
    ) -> Result<Vec<(String, EpochSeq, Upload)>, ShardError> {
        self.replicas.uploads(shard, prefix, after, limit).await
    }

    async fn parts(
        &self,
        shard: &ShardRef,
        upload: EpochSeq,
        after: u16,
        limit: usize,
    ) -> Result<Vec<(u16, Part)>, ShardError> {
        self.replicas.parts(shard, upload, after, limit).await
    }

    async fn payload(&self, shard: &ShardRef, position: EpochSeq) -> Result<Bytes, ShardError> {
        self.replicas.payload(shard, position).await
    }

    fn node(&self) -> Option<NodeId> {
        self.replicas.node()
    }

    async fn plan(&self, shard: &ShardRef, key: &str) -> Result<ReadPlan, ShardError> {
        if let Some(replica) = self.local().set().get(&shard.into()).await
            && ((self.members_read && replica.role() == Role::Member)
                || (self.stepped_down_read && replica.stepped_down().is_some()))
        {
            // The seeded bugs of `entry`, for the plans of GETs.
            let plan = own_plan(&replica, shard, key, self.node_id());
            if replica.stepped_down().is_some() {
                self.audit_read(shard, &plan).await;
            }
            return plan;
        }
        let plan = self.replicas.plan(shard, key).await;
        self.audit_read(shard, &plan).await;
        plan
    }

    async fn register(
        &self,
        shard: &ShardRef,
        holder: &NodeId,
        key: &str,
        version: EpochSeq,
        layout: Vec<ExtentRef>,
    ) -> Result<Option<Registered>, ShardError> {
        self.holders
            .register(self.local(), shard, holder, key, version, layout)
            .await
    }

    async fn renew(
        &self,
        shard: &ShardRef,
        holder: &NodeId,
        read: ReadId,
    ) -> Result<bool, ShardError> {
        self.replicas.renew(shard, holder, read).await
    }

    async fn release(
        &self,
        shard: &ShardRef,
        holder: &NodeId,
        read: ReadId,
    ) -> Result<(), ShardError> {
        self.replicas.release(shard, holder, read).await
    }

    async fn fetch(
        &self,
        shard: &ShardRef,
        holder: &NodeId,
        read: ReadId,
        position: EpochSeq,
    ) -> Result<Bytes, ShardError> {
        self.holders
            .fetch(self.local(), shard, holder, read, position)
            .await
    }

    async fn append_extent(
        &self,
        shard: &ShardRef,
        extent: Extent,
    ) -> Result<ExtentRef, ShardError> {
        let extent = self.replicas.append_extent(shard, extent).await?;
        self.audit_acknowledged(shard, extent.position);
        Ok(extent)
    }

    async fn announce(
        &self,
        shard: &ShardRef,
        body: skys3_gateway::StreamedBody,
    ) -> Result<(), ShardError> {
        self.replicas.announce(shard, body).await
    }

    async fn flushed(
        &self,
        shard: &ShardRef,
        key: &str,
        version: EpochSeq,
        wait: Duration,
    ) -> Result<skys3_gateway::FlushState, ShardError> {
        self.replicas.flushed(shard, key, version, wait).await
    }

    async fn write(
        &self,
        shard: &ShardRef,
        body: RecordBody,
        condition: Precondition,
    ) -> Result<Result<EpochSeq, ConditionFailed>, ShardError> {
        let again = self.resubmit.then(|| (body.clone(), condition.clone()));
        let started = turmoil::sim_elapsed().unwrap_or_default();
        let written = self.replicas.write(shard, body, condition).await;
        let took = turmoil::sim_elapsed()
            .unwrap_or_default()
            .saturating_sub(started);
        if let (Err(ShardError::NotAcknowledged { .. }), Some((body, condition))) =
            (&written, again)
        {
            let (local, shard) = (self.local().clone(), shard.clone());
            tokio::spawn(async move {
                tokio::time::sleep(RESUBMIT_DELAY).await;
                let _ = local.write(&shard, body, condition).await;
            });
        }
        if let Err(ShardError::NotAcknowledged {
            position: Some(position),
            ..
        }) = &written
        {
            lock(&self.audit).late.push((shard.clone(), *position));
        }
        let written = written?;
        if let Ok(position) = written {
            let mut audit = lock(&self.audit);
            audit.slowest = audit.slowest.max(took);
            if audit.store.is_some() {
                audit.learners.writes.push((started, took));
            }
            drop(audit);

            self.audit_acknowledged(shard, position);
        }
        Ok(written)
    }
}
