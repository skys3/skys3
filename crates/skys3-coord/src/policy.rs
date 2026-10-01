//! Placement policy health (design §6.7): which buckets the cluster does
//! not currently satisfy, and why.
//!
//! A bucket whose policy the cluster cannot satisfy is rejected when it is
//! created ([`Topology::check_bucket`]). The cluster can stop satisfying a
//! bucket later, for example by losing a rack. The coordinator then never
//! co-locates members to make up the difference: the shards run with fewer
//! members, and cluster health reports the policy as unsatisfied until
//! capacity returns. [`report`] is that judgement, a pure function of the
//! [`Topology`] and what the bucket and shard registers say;
//! [`PolicyWatch`] runs it on the coordinator before each placement round
//! and publishes the result through a [`PlacementHealth`] handle, which the
//! admin API reads.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use skys3_config::FailureDomain;
use skys3_control::{
    ControlError, ControlStore, KeyPrefix, ProposalIds, RegisterKey, RegisterKind, Version,
};
use skys3_types::{
    BucketDocument, BucketId, BucketName, NodeId, RegisterDocument, ShardConfig, ShardId,
};

use crate::change::{Applied, ChangeSet};
use crate::coordinator::{NoPlacement, Placement};
use crate::place::{Domain, Topology};
use crate::registry::{NodeRegistry, NodeState};

/// The cluster's placement health: every bucket whose policy it does not
/// satisfy now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyReport {
    /// The level members are kept apart at.
    pub failure_domain: FailureDomain,
    /// The nodes that may receive new members.
    pub eligible_nodes: usize,
    /// The domains those nodes are in: the most members any shard can
    /// have.
    pub domains: usize,
    /// Nodes that lack the label `failure_domain` needs, and so never
    /// receive members.
    pub unlabeled: Vec<NodeId>,
    /// The buckets whose policy is not satisfied, by bucket ID.
    pub unsatisfied: Vec<BucketPolicy>,
}

impl PolicyReport {
    /// Whether every bucket's policy is satisfied.
    #[must_use]
    pub fn is_satisfied(&self) -> bool {
        self.unsatisfied.is_empty()
    }
}

/// A bucket whose policy the cluster does not satisfy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketPolicy {
    /// The bucket's ID.
    pub bucket_id: BucketId,
    /// The bucket's S3 name.
    pub name: BucketName,
    /// The members each of its shards needs.
    pub replicas: u8,
    /// Whether the cluster has enough eligible domains for `replicas`.
    /// When it has not, no shard can be made whole until capacity returns.
    pub placeable: bool,
    /// The shards with fewer than `replicas` members in separate domains,
    /// counting no member on a departing node.
    pub short: Vec<ShortShard>,
    /// The shards with two or more members in one domain, which placement
    /// never does but relabeled nodes can cause.
    pub co_located: Vec<CoLocated>,
}

/// A shard short of members.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShortShard {
    /// The shard.
    pub shard: ShardId,
    /// Its members, by its register; empty if it has none.
    pub members: Vec<NodeId>,
    /// The separate domains its members not departing are in.
    pub domains: usize,
}

/// Members of one shard that share a domain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoLocated {
    /// The shard.
    pub shard: ShardId,
    /// The domain they share.
    pub domain: Domain,
    /// The members in it.
    pub members: Vec<NodeId>,
}

/// Judges every bucket in `buckets` against `topology`, from its shards'
/// registers in `shards`. A shard of a bucket with no register counts as
/// a shard with no members; shards of buckets not in `buckets` are
/// ignored.
#[must_use]
pub fn report<'a>(
    topology: &Topology,
    buckets: impl IntoIterator<Item = &'a BucketDocument>,
    shards: impl IntoIterator<Item = &'a ShardConfig>,
) -> PolicyReport {
    let mut by_bucket: BTreeMap<&BucketId, BTreeMap<ShardId, &ShardConfig>> = BTreeMap::new();
    for config in shards {
        by_bucket
            .entry(&config.bucket_id)
            .or_default()
            .insert(config.shard, config);
    }
    let domains = topology.eligible_domains().len();
    let mut unsatisfied: Vec<BucketPolicy> = buckets
        .into_iter()
        .filter_map(|bucket| {
            let configs = by_bucket.remove(&bucket.bucket_id).unwrap_or_default();
            judge(topology, bucket, &configs)
        })
        .collect();
    unsatisfied.sort_by(|a, b| a.bucket_id.cmp(&b.bucket_id));
    PolicyReport {
        failure_domain: topology.level(),
        eligible_nodes: topology.eligible().count(),
        domains,
        unlabeled: topology.unlabeled(),
        unsatisfied,
    }
}

/// The bucket's policy status, if it is not satisfied.
fn judge(
    topology: &Topology,
    bucket: &BucketDocument,
    configs: &BTreeMap<ShardId, &ShardConfig>,
) -> Option<BucketPolicy> {
    let mut policy = BucketPolicy {
        bucket_id: bucket.bucket_id.clone(),
        name: bucket.name.clone(),
        replicas: bucket.replicas,
        placeable: topology.check(bucket.replicas).is_ok(),
        short: Vec::new(),
        co_located: Vec::new(),
    };
    for shard in bucket.shards.shards() {
        let members = configs
            .get(&shard)
            .map(|config| config.members.clone())
            .unwrap_or_default();
        let mut by_domain: BTreeMap<Domain, Vec<NodeId>> = BTreeMap::new();
        let mut healthy = BTreeSet::new();
        // A member whose domain is unknown (unregistered, or unlabeled)
        // takes a domain of its own.
        let mut unknown = 0_usize;
        for node in &members {
            let Some(domain) = topology.domain(node) else {
                unknown += 1;
                continue;
            };
            let departing = topology
                .get(node)
                .is_some_and(|candidate| candidate.state == NodeState::Departing);
            if !departing {
                healthy.insert(domain.clone());
            }
            by_domain.entry(domain).or_default().push(node.clone());
        }
        let domains = healthy.len() + unknown;
        if domains < usize::from(bucket.replicas) {
            policy.short.push(ShortShard {
                shard,
                members: members.clone(),
                domains,
            });
        }
        policy.co_located.extend(
            by_domain
                .into_iter()
                .filter(|(_, nodes)| nodes.len() > 1)
                .map(|(domain, members)| CoLocated {
                    shard,
                    domain,
                    members,
                }),
        );
    }
    let satisfied = policy.placeable && policy.short.is_empty() && policy.co_located.is_empty();
    (!satisfied).then_some(policy)
}

/// The latest [`PolicyReport`], shared between the coordinator that
/// computes it and the admin API that serves it. Clones share it.
#[derive(Debug, Clone, Default)]
pub struct PlacementHealth {
    latest: Arc<Mutex<Option<PolicyReport>>>,
}

impl PlacementHealth {
    /// A handle with no report yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, Option<PolicyReport>> {
        self.latest.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The latest report, or `None` before the first placement round of
    /// this node's latest tenure as coordinator. Only the coordinator
    /// judges policy, and a node whose tenure ended keeps its last report,
    /// so a reader serves it only while the node's
    /// [`Leadership`](crate::Leadership) says it is coordinator.
    #[must_use]
    pub fn latest(&self) -> Option<PolicyReport> {
        self.lock().clone()
    }

    /// Replaces the report, returning the previous one.
    pub fn publish(&self, report: Option<PolicyReport>) -> Option<PolicyReport> {
        std::mem::replace(&mut *self.lock(), report)
    }
}

/// The bucket and shard registers, read from the control store and kept
/// current: each scan lists `buckets/` and `shards/` and reads only the
/// registers whose version changed. A register that does not parse is
/// left out, and logged.
#[derive(Debug, Default)]
pub struct ClusterScan {
    buckets: BTreeMap<RegisterKey, (Version, BucketDocument)>,
    shards: BTreeMap<RegisterKey, (Version, ShardConfig)>,
    /// How many registers the last scan could not parse.
    unreadable: usize,
}

impl ClusterScan {
    /// An empty scan.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Lists and reads the registers again.
    ///
    /// # Errors
    ///
    /// The store's errors. The scan keeps what it read before.
    pub async fn refresh<S: ControlStore>(&mut self, store: &S) -> Result<(), ControlError> {
        let (buckets, unreadable) = sync(store, &KeyPrefix::buckets(), &self.buckets, |kind| {
            matches!(kind, RegisterKind::Bucket(_))
        })
        .await?;
        let (shards, more) = sync(store, &KeyPrefix::shards(), &self.shards, |kind| {
            matches!(kind, RegisterKind::Shard(..))
        })
        .await?;
        self.buckets = buckets;
        self.shards = shards;
        self.unreadable = unreadable + more;
        Ok(())
    }

    /// Every bucket register, with its key and version.
    pub fn bucket_registers(
        &self,
    ) -> impl Iterator<Item = (&RegisterKey, &Version, &BucketDocument)> {
        self.buckets
            .iter()
            .map(|(key, (version, bucket))| (key, version, bucket))
    }

    /// Every shard register, with its key and version.
    pub fn shard_registers(&self) -> impl Iterator<Item = (&RegisterKey, &Version, &ShardConfig)> {
        self.shards
            .iter()
            .map(|(key, (version, shard))| (key, version, shard))
    }

    /// How many registers the last scan listed but could not parse, such
    /// as ones of a newer format: what they hold is unknown.
    #[must_use]
    pub fn unreadable(&self) -> usize {
        self.unreadable
    }

    /// Every bucket register.
    pub fn buckets(&self) -> impl Iterator<Item = &BucketDocument> {
        self.buckets.values().map(|(_, bucket)| bucket)
    }

    /// Every shard register.
    pub fn shards(&self) -> impl Iterator<Item = &ShardConfig> {
        self.shards.values().map(|(_, shard)| shard)
    }
}

/// Lists `prefix` and reads every register of a kind `wanted` accepts
/// whose version is not the one `cached` holds, and counts those that do
/// not parse.
async fn sync<D, S>(
    store: &S,
    prefix: &KeyPrefix,
    cached: &BTreeMap<RegisterKey, (Version, D)>,
    wanted: fn(&RegisterKind) -> bool,
) -> Result<(BTreeMap<RegisterKey, (Version, D)>, usize), ControlError>
where
    D: RegisterDocument + Clone,
    S: ControlStore,
{
    let mut current = BTreeMap::new();
    let mut unreadable = 0;
    for (key, version) in store.list(prefix).await? {
        if !wanted(&key.kind()) {
            continue;
        }
        if let Some(entry) = cached.get(&key).filter(|(seen, _)| *seen == version) {
            current.insert(key, entry.clone());
            continue;
        }
        // Absent: deleted since the listing.
        let Some(read) = store.get(&key).await? else {
            continue;
        };
        match D::from_json(&read.value) {
            Ok(document) => {
                current.insert(key, (read.version, document));
            }
            Err(error) => {
                unreadable += 1;
                tracing::warn!(%key, %error, "ignoring a register that does not parse");
            }
        }
    }
    Ok((current, unreadable))
}

/// Judges placement policy on the coordinator: before each plan of the
/// placement it wraps, it scans the bucket and shard registers
/// ([`ClusterScan`]), judges them against the nodes its [`NodeRegistry`]
/// lists ([`report`]), and publishes the result to its
/// [`PlacementHealth`]. It changes nothing itself.
///
/// It runs inside the node [`Lifecycle`](crate::Lifecycle), which keeps
/// the registry current: `Lifecycle::new(registry.clone(),
/// PolicyWatch::new(registry, failure_domain))`. Creating buckets and
/// replacing members (plan M3-04, M3-05) are the placement it wraps.
pub struct PolicyWatch<P = NoPlacement> {
    registry: NodeRegistry,
    level: FailureDomain,
    scan: ClusterScan,
    health: PlacementHealth,
    placement: P,
}

impl<P> std::fmt::Debug for PolicyWatch<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PolicyWatch")
            .field("level", &self.level)
            .field("scan", &self.scan)
            .field("health", &self.health)
            .finish_non_exhaustive()
    }
}

impl PolicyWatch {
    /// Judges policy at `level` against the nodes `registry` lists.
    #[must_use]
    pub fn new(registry: NodeRegistry, level: FailureDomain) -> Self {
        Self {
            registry,
            level,
            scan: ClusterScan::new(),
            health: PlacementHealth::new(),
            placement: NoPlacement,
        }
    }
}

impl<P> PolicyWatch<P> {
    /// Wraps `placement`, which plans after each judgement.
    pub fn with_placement<Q: Placement>(self, placement: Q) -> PolicyWatch<Q> {
        PolicyWatch {
            registry: self.registry,
            level: self.level,
            scan: self.scan,
            health: self.health,
            placement,
        }
    }

    /// Publishes to `health` instead of a handle of its own.
    #[must_use]
    pub fn with_health(mut self, health: PlacementHealth) -> Self {
        self.health = health;
        self
    }

    /// The handle the reports are published to.
    #[must_use]
    pub fn health(&self) -> PlacementHealth {
        self.health.clone()
    }

    /// The topology of the registry's nodes, loaded with the scanned
    /// shards.
    fn topology(&self) -> Topology {
        let mut topology = Topology::from_registry(self.level, &self.registry);
        for shard in self.scan.shards() {
            topology.record(shard);
        }
        topology
    }

    /// Judges the scanned registers, publishes the report, and logs each
    /// bucket that changed between satisfied and not.
    fn judge(&self) {
        let topology = self.topology();
        let report = report(&topology, self.scan.buckets(), self.scan.shards());
        let names = |report: &PolicyReport| -> BTreeSet<BucketId> {
            report
                .unsatisfied
                .iter()
                .map(|bucket| bucket.bucket_id.clone())
                .collect()
        };
        let now = names(&report);
        let before = self
            .health
            .publish(Some(report.clone()))
            .as_ref()
            .map(names)
            .unwrap_or_default();
        for bucket in report
            .unsatisfied
            .iter()
            .filter(|b| !before.contains(&b.bucket_id))
        {
            tracing::warn!(
                bucket = %bucket.name,
                replicas = bucket.replicas,
                domains = report.domains,
                failure_domain = crate::place::level_name(report.failure_domain),
                short_shards = bucket.short.len(),
                co_located_shards = bucket.co_located.len(),
                "the cluster does not satisfy a bucket's placement policy"
            );
        }
        for bucket in before.difference(&now) {
            tracing::info!(%bucket, "the cluster satisfies a bucket's placement policy again");
        }
    }
}

impl<P: Placement> Placement for PolicyWatch<P> {
    fn begin_tenure(&mut self) {
        // A report from an earlier tenure is stale.
        self.health.publish(None);
        self.placement.begin_tenure();
    }

    async fn plan<S: ControlStore>(
        &mut self,
        store: &S,
        proposals: &mut ProposalIds,
    ) -> Result<Option<ChangeSet>, ControlError> {
        self.scan.refresh(store).await?;
        self.judge();
        self.placement.plan(store, proposals).await
    }

    fn applied(&mut self, change: &ChangeSet, applied: &Applied) {
        self.placement.applied(change, applied);
    }
}

#[cfg(test)]
mod tests;
