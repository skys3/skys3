//! Node services that replicate every shard to the members of its static
//! placement (plan M2-07), with the audit the replication scenarios check.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::Bytes;
use skys3_gateway::{
    ConditionFailed, LocalShards, Precondition, ShardError, ShardRef, ShardSummary, Shards,
    UploadParts,
};
use skys3_index::{Entry, ListPage, ListQuery, Part, Upload};
use skys3_io::SimMount;
use skys3_log::record::{Extent, ExtentRef};
use skys3_log::{LogStats, RecordBody, SegmentLog};
use skys3_net::TurmoilNetwork;
use skys3_shard::Shard;
use skys3_shard::replication::{Replication, ReplicationConfig};
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
#[derive(Clone, Default)]
pub struct ReplicatedServices {
    config: ReplicationConfig,
    /// A seeded bug: primaries commit alone.
    alone: bool,
    audit: Arc<Mutex<Audit>>,
}

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

    fn audit(&self) -> MutexGuard<'_, Audit> {
        self.audit.lock().unwrap_or_else(PoisonError::into_inner)
    }
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

    /// Ready once every primary opened so far serves. The I/O counts start
    /// then.
    fn ready(&self) -> bool {
        let mut audit = self.audit();
        let ready = audit
            .replicas
            .iter()
            .all(|(_, _, replica)| replica.is_serving() || replica.leader().is_none());
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
            alone: self.alone,
            audit: Arc::clone(&self.audit),
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
    alone: bool,
    audit: Arc<Mutex<Audit>>,
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
        let mut audit = self.audit.lock().unwrap_or_else(PoisonError::into_inner);
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
        let mut audit = self.audit.lock().unwrap_or_else(PoisonError::into_inner);
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
        self.local.entry(shard, key).await
    }

    async fn list(&self, shard: &ShardRef, query: &ListQuery) -> Result<ListPage, ShardError> {
        self.local.list(shard, query).await
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
        let written = self.local.write(shard, body, condition).await?;
        if let Ok(position) = written {
            self.audit_acknowledged(shard, position);
        }
        Ok(written)
    }
}
