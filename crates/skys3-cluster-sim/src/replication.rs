//! Node services that replicate every shard to the members of its static
//! placement (plan M2-07), with the audits the replication scenarios check:
//! commits (M2-07) and reads under leases (M2-09). Writes wait for the
//! members at most the configured acknowledgement timeout (M2-10).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use skys3_gateway::{
    ConditionFailed, LocalShards, Precondition, ShardError, ShardRef, ShardSummary, Shards,
    UploadParts,
};
use skys3_index::{Entry, ListPage, ListQuery, Part, Upload};
use skys3_io::{Clock, MonoTime, MonotonicClock, SimMount};
use skys3_log::record::{Extent, ExtentRef};
use skys3_log::{LogStats, RecordBody, SegmentLog};
use skys3_net::TurmoilNetwork;
use skys3_shard::replication::{Replication, ReplicationConfig};
use skys3_shard::{Grace, Shard};
use skys3_types::{BucketDocument, EpochSeq, NodeId, Seq, ShardConfig};

use crate::node::{BoxError, NodeEnv, NodeServices, TRANSPORT_PORT};

/// Replicated node services: every node opens its replica of each shard
/// whose static placement names it, as primary or member, and the
/// gateway's writes on a primary commit once every member holds them
/// durably (§5.1).
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
/// ([`ReplicatedServices::leases`]).
#[derive(Clone, Default)]
pub struct ReplicatedServices {
    config: ReplicationConfig,
    /// A seeded bug: primaries commit alone.
    alone: bool,
    /// A seeded bug: writes that were not acknowledged are sent again.
    resubmit: bool,
    audit: Arc<Mutex<Audit>>,
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
struct Audit {
    replicas: Vec<(NodeId, ShardConfig, Shard<SimMount>)>,
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
            alone: false,
            resubmit: false,
            audit: Arc::default(),
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

    /// Checks the commit rule against what members really hold: no primary
    /// has committed a record, nor acknowledged a write, that some member
    /// of its shard did not hold durably then. A replica's durable run only
    /// grows, also across restarts, so comparing with every life's replicas
    /// is exact.
    ///
    /// # Errors
    ///
    /// What was committed or acknowledged too early.
    pub fn check_commits(&self) -> Result<(), String> {
        let audit = self.audit();
        if let Some(early) = audit.early.first() {
            return Err(early.clone());
        }
        for (node, config, replica) in &audit.replicas {
            let Some(leader) = replica.leader() else {
                continue;
            };
            let commit = leader.commit();
            for member in leader.members() {
                let durable = audit.durable(member, config);
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
    /// last granted the primary a lease. Such a member could have taken
    /// over, and the read could then miss a write the new primary
    /// acknowledged (§5.4, §6.8).
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

    fn audit(&self) -> MutexGuard<'_, Audit> {
        lock(&self.audit)
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
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
    fn durable(&self, node: &NodeId, config: &ShardConfig) -> Seq {
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
    type Shards = ReplicatedShards;

    /// Ready once every primary opened so far serves, and every member
    /// granted it a lease. The I/O counts start then.
    fn ready(&self) -> bool {
        let mut audit = self.audit();
        let ready = audit.replicas.iter().all(|(_, _, replica)| {
            replica.leader().is_none_or(|leader| {
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

    async fn start(&self, env: NodeEnv) -> Result<ReplicatedShards, BoxError> {
        let set = env.shards.set().clone();
        self.audit()
            .logs
            .extend(set.logs().map(|(_, log)| log.clone()));
        let replication = Replication::new(
            env.node.clone(),
            set,
            env.transport.clone(),
            env.peers.clone(),
            Arc::clone(&env.clock) as Arc<dyn Clock>,
            self.config,
        );
        let listener = env
            .transport
            .bind((std::net::Ipv4Addr::UNSPECIFIED, TRANSPORT_PORT).into())
            .await?;
        let serving = replication.clone();
        tokio::spawn(async move { serving.serve(listener).await });
        Ok(ReplicatedShards {
            local: env.shards,
            replication,
            placement: env.placement,
            node: env.node,
            clock: env.clock,
            alone: self.alone,
            resubmit: self.resubmit,
            audit: Arc::clone(&self.audit),
            followed: Arc::default(),
        })
    }
}

/// The gateway's shards on a replicated node: replicas open in their
/// placement's configuration, and writes are checked against what the
/// members hold when they are acknowledged.
#[derive(Clone)]
pub struct ReplicatedShards {
    local: LocalShards<SimMount>,
    replication: Replication<TurmoilNetwork, SimMount>,
    placement: Arc<BTreeMap<ShardRef, ShardConfig>>,
    node: NodeId,
    /// The node's clock in this life.
    clock: Arc<MonotonicClock>,
    alone: bool,
    resubmit: bool,
    audit: Arc<Mutex<Audit>>,
    /// The shards whose grace this life follows.
    followed: Arc<Mutex<BTreeSet<ShardRef>>>,
}

impl fmt::Debug for ReplicatedShards {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplicatedShards")
            .field("node", &self.node)
            .finish_non_exhaustive()
    }
}

impl ReplicatedShards {
    /// Records an acknowledgement of `position` that some member of the
    /// shard did not hold durably.
    fn audit_acknowledged(&self, shard: &ShardRef, position: EpochSeq) {
        let Some(config) = self.placement.get(shard) else {
            return;
        };
        let mut audit = lock(&self.audit);
        for member in &config.members {
            let durable = audit.durable(member, config);
            if durable < position.seq {
                let early = format!(
                    "{} acknowledged the write at {position} of shard {shard} while {member} \
                     held only seq {durable} durably",
                    self.node
                );
                audit.early.push(early);
            }
        }
    }

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

    /// Counts a read of `shard` that ended with `result`, and checks a
    /// served one against the grace of the shard's other members.
    async fn audit_read<T>(&self, shard: &ShardRef, result: &Result<T, ShardError>) {
        let Some(replica) = self.replication.set().get(&shard.into()).await else {
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
        for member in leader.members() {
            let Some(&passed) = audit.graces.get(&(member.clone(), shard.clone())) else {
                continue;
            };
            if passed <= now {
                audit.leases.stale += 1;
                let stale = format!(
                    "{} served a read of shard {shard} at {now:?}, but the primary_grace of \
                     {member} had passed at {passed:?}",
                    self.node
                );
                audit.stale.push(stale);
            }
        }
    }
}

impl Shards for ReplicatedShards {
    async fn open(&self, shard: &ShardRef, bucket: &BucketDocument) -> Result<(), ShardError> {
        if self.alone {
            return self.local.open(shard, bucket).await;
        }
        let Some(config) = self
            .placement
            .get(shard)
            .filter(|c| c.is_member(&self.node))
        else {
            // Another node's shard.
            return Ok(());
        };
        let replica =
            self.replication
                .open(config)
                .await
                .map_err(|error| ShardError::Unavailable {
                    shard: shard.clone(),
                    reason: error.to_string(),
                })?;
        if let Some(grace) = self.replication.grace(replica.shard()) {
            self.follow_grace(shard, grace);
        }
        let mut audit = lock(&self.audit);
        if !audit
            .replicas
            .iter()
            .any(|(_, _, open)| open.durable().same_channel(&replica.durable()))
        {
            audit
                .replicas
                .push((self.node.clone(), config.clone(), replica));
        }
        Ok(())
    }

    async fn seal(&self, shard: &ShardRef) -> Result<ShardSummary, ShardError> {
        self.local.seal(shard).await
    }

    async fn unseal(&self, shard: &ShardRef) -> Result<(), ShardError> {
        self.local.unseal(shard).await
    }

    async fn remove(&self, shard: &ShardRef) -> Result<(), ShardError> {
        self.local.remove(shard).await
    }

    async fn entry(&self, shard: &ShardRef, key: &str) -> Result<Option<Entry>, ShardError> {
        let entry = self.local.entry(shard, key).await;
        self.audit_read(shard, &entry).await;
        entry
    }

    async fn list(&self, shard: &ShardRef, query: &ListQuery) -> Result<ListPage, ShardError> {
        let page = self.local.list(shard, query).await;
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
        self.local.upload(shard, key, upload, after, limit).await
    }

    async fn uploads(
        &self,
        shard: &ShardRef,
        prefix: &str,
        after: Option<(String, Option<EpochSeq>)>,
        limit: usize,
    ) -> Result<Vec<(String, EpochSeq, Upload)>, ShardError> {
        self.local.uploads(shard, prefix, after, limit).await
    }

    async fn parts(
        &self,
        shard: &ShardRef,
        upload: EpochSeq,
        after: u16,
        limit: usize,
    ) -> Result<Vec<(u16, Part)>, ShardError> {
        self.local.parts(shard, upload, after, limit).await
    }

    async fn payload(&self, shard: &ShardRef, position: EpochSeq) -> Result<Bytes, ShardError> {
        self.local.payload(shard, position).await
    }

    async fn append_extent(
        &self,
        shard: &ShardRef,
        extent: Extent,
    ) -> Result<ExtentRef, ShardError> {
        let extent = self.local.append_extent(shard, extent).await?;
        self.audit_acknowledged(shard, extent.position);
        Ok(extent)
    }

    async fn write(
        &self,
        shard: &ShardRef,
        body: RecordBody,
        condition: Precondition,
    ) -> Result<Result<EpochSeq, ConditionFailed>, ShardError> {
        let again = self.resubmit.then(|| (body.clone(), condition.clone()));
        let written = self.local.write(shard, body, condition).await;
        if let (Err(ShardError::NotAcknowledged { .. }), Some((body, condition))) =
            (&written, again)
        {
            let (local, shard) = (self.local.clone(), shard.clone());
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
            self.audit_acknowledged(shard, position);
        }
        Ok(written)
    }
}
