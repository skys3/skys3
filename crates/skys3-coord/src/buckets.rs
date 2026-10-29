//! Creating a bucket's shards on a multi-node cluster (design §4.1, §6.1,
//! §6.7), and keeping every bucket register matched by its shard
//! registers.
//!
//! A gateway creates a bucket with [`create_bucket`]: it reads the node
//! registrations, places the bucket's shards on them ([`Topology`]), and
//! makes one change ([`apply`]) that creates the bucket register and then
//! every shard register, each with `If-None-Match: *`, announced by one
//! generation increment. The bucket register comes first, so a creation
//! that loses the name to another writes nothing, and every shard register
//! is written after its bucket's: a shard register whose bucket no register
//! names is left over from a bucket that existed, never a creation still in
//! progress.
//!
//! The coordinator finishes what a creation leaves undone and drops what a
//! deletion leaves behind ([`BucketShards`]): it creates the missing shard
//! registers of a bucket whose creation was cut short, and deletes the
//! shard registers of a bucket that no longer exists. Each acts only on
//! what two consecutive scans showed, so a creation in progress is never
//! mistaken for either; a race with it is settled by compare-and-swap,
//! like any other.

use std::collections::{BTreeMap, BTreeSet};

use skys3_config::FailureDomain;
use skys3_control::{
    ControlError, ControlStore, KeyPrefix, ProposalIds, RegisterKey, RegisterKind, RetryPolicy,
    TypedKey, Version, read_with_retries,
};
use skys3_types::{
    BucketDocument, BucketId, ClusterId, Epoch, NodeRegistration, ProposalId, ShardConfig, ShardId,
};

use crate::change::{Applied, ChangeError, ChangeFailed, ChangeSet, Pending, apply, settle};
use crate::coordinator::{NoPlacement, Placement};
use crate::place::{Candidate, NewShard, ShardRequest, Topology, Unsatisfiable};
use crate::policy::ClusterScan;
use crate::registry::NodeRegistry;

/// What [`create_bucket`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum Creation {
    /// The bucket register was created. `complete` says whether every
    /// shard register is known to exist too; if not, the coordinator
    /// creates the missing ones ([`BucketShards`]).
    Created {
        /// Whether every shard register exists.
        complete: bool,
    },
    /// A bucket register of that name exists: nothing was written.
    Taken,
}

/// Why [`create_bucket`] could not create the bucket.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CreationError {
    /// The registered nodes cannot satisfy the bucket's policy: the bucket
    /// is rejected (design §6.7).
    #[error(transparent)]
    Unsatisfiable(#[from] Unsatisfiable),
    /// A register would be invalid.
    #[error(transparent)]
    Invalid(#[from] ChangeError),
    /// The control store failed before the bucket register was written, or
    /// the bucket register's write got no answer that settles it: it may
    /// still land, and the coordinator then creates its shard registers.
    #[error(transparent)]
    Control(#[from] ControlError),
    /// A write of the creation got no answer, or the generation increment
    /// that announces it failed, and settling it failed too. The bucket
    /// may exist. Its announcement is owed until `pending` is settled
    /// ([`settle`]): the caller keeps settling it, and does not report the
    /// creation as done (design §6.7).
    #[error("a write of a new bucket is unsettled: {source}")]
    Unsettled {
        /// What is still to be settled and announced.
        pending: Box<Pending>,
        /// Why settling it failed.
        #[source]
        source: ControlError,
    },
}

/// The registration of every node under `nodes/`. A registration that
/// does not parse is left out.
///
/// # Errors
///
/// The store's errors.
pub async fn registrations<S: ControlStore>(
    store: &S,
    policy: &RetryPolicy,
) -> Result<Vec<NodeRegistration>, ControlError> {
    let mut nodes = Vec::new();
    for (key, _) in store.list(&KeyPrefix::nodes()).await? {
        let RegisterKind::Node(node) = key.kind() else {
            continue;
        };
        match read_with_retries(store, &TypedKey::node(&node), policy).await {
            Ok(Some(current)) => nodes.push(current.value),
            Ok(None) => {}
            Err(error @ ControlError::InvalidRegister { .. }) => {
                tracing::warn!(%error, "ignoring a node register that does not parse");
            }
            Err(error) => return Err(error),
        }
    }
    Ok(nodes)
}

/// The first configuration of a new shard of `bucket`: epoch 1, with the
/// members and primary placement chose.
#[must_use]
pub fn first_config(bucket: &BucketDocument, shard: NewShard, proposal: ProposalId) -> ShardConfig {
    ShardConfig {
        bucket_id: bucket.bucket_id.clone(),
        shard: shard.shard,
        epoch: Epoch::new(1),
        primary: shard.primary,
        members: shard.members,
        learners: Vec::new(),
        min_write_replicas: bucket.min_write_replicas,
        replicas: bucket.replicas,
        proposal_id: proposal,
    }
}

/// Creates `bucket` on the nodes registered under `nodes/`: places its
/// shards at `level` and writes, in one change announced by one
/// generation increment, the bucket register and then every shard
/// register, each with `If-None-Match: *`.
///
/// A shard register another writer created first, the coordinator
/// finishing what it took for a creation cut short, is kept, and the
/// writes after it are sent again. A write that got no answer is settled
/// ([`settle`]) before this returns.
///
/// # Errors
///
/// [`CreationError`]. Once the bucket register is known to exist and every
/// write that landed is announced, a failed shard register write is not an
/// error: the coordinator creates what is missing, and
/// [`Creation::Created`] says the creation is incomplete. A write or an
/// announcement still owed is [`CreationError::Unsettled`].
pub async fn create_bucket<S: ControlStore>(
    store: &S,
    cluster: &ClusterId,
    bucket: &BucketDocument,
    level: FailureDomain,
    proposals: &mut ProposalIds,
    policy: &RetryPolicy,
) -> Result<Creation, CreationError> {
    let nodes = registrations(store, policy).await?;
    let topology = Topology::new(level, nodes.iter().map(Candidate::from_registration));
    let placed = topology.place_bucket(&bucket.bucket_id, bucket.shards, bucket.replicas)?;
    let bucket_key = TypedKey::bucket(&bucket.name);
    let shards: Vec<(TypedKey<ShardConfig>, ShardConfig)> = placed
        .into_iter()
        .map(|shard| {
            let key = TypedKey::shard(&bucket.bucket_id, shard.shard);
            (key, first_config(bucket, shard, proposals.next_id()))
        })
        .collect();
    let mut change = ChangeSet::new().create(&bucket_key, bucket)?;
    let mut rest = 0;
    loop {
        for (key, config) in &shards[rest..] {
            change = change.create(key, config)?;
        }
        let applied = match apply(store, cluster, &change, proposals, policy).await {
            Ok(applied) => applied,
            Err(failed) => {
                return settled(store, cluster, &bucket_key, failed, proposals, policy).await;
            }
        };
        if applied.rejected.as_ref() == Some(bucket_key.key()) {
            return Ok(Creation::Taken);
        }
        let Some(rejected) = &applied.rejected else {
            return Ok(Creation::Created { complete: true });
        };
        // Another writer created this shard register, after the bucket's:
        // keep it, and send the rest again.
        tracing::info!(register = %rejected, "a shard register of a new bucket was created by another writer");
        rest += shards[rest..]
            .iter()
            .position(|(key, _)| key.key() == rejected)
            .map_or(shards.len() - rest, |at| at + 1);
        if rest == shards.len() {
            return Ok(Creation::Created { complete: true });
        }
        change = ChangeSet::new();
    }
}

/// What a creation that failed part of the way did: whether its bucket
/// register is known to exist, once the write that got no answer, if any,
/// is settled and what landed is announced. If settling fails, the
/// creation is [`CreationError::Unsettled`], whatever it wrote: reporting
/// it done would drop the only handle on an announcement still owed.
async fn settled<S: ControlStore>(
    store: &S,
    cluster: &ClusterId,
    bucket_key: &TypedKey<BucketDocument>,
    failed: Box<ChangeFailed>,
    proposals: &mut ProposalIds,
    policy: &RetryPolicy,
) -> Result<Creation, CreationError> {
    let ChangeFailed { source, applied } = *failed;
    let wrote = |key: &RegisterKey| key == bucket_key.key();
    let mut created = applied.written.iter().any(|(key, _)| wrote(key));
    if let Some(pending) = applied.pending {
        match settle(store, cluster, &pending, proposals, policy).await {
            Ok(settled) => created |= settled.written.is_some_and(|(key, _)| wrote(&key)),
            Err(error) => {
                tracing::warn!(%error, register = ?pending.unsettled(), "a write of a new bucket is unsettled");
                return Err(CreationError::Unsettled {
                    pending: Box::new(pending),
                    source: error,
                });
            }
        }
    }
    if created {
        tracing::warn!(error = %source, bucket = %bucket_key.key(), "a new bucket's shard registers were not all written; the coordinator creates the rest");
        Ok(Creation::Created { complete: false })
    } else {
        Err(CreationError::Control(source))
    }
}

/// Keeps every bucket register matched by its shard registers, as part of
/// the coordinator's placement (design §4.1, §6.7): it creates the shard
/// registers missing from a bucket whose creation was cut short, placed
/// like a new bucket's, and deletes the shard registers of a bucket no
/// register names, which a deletion leaves behind.
///
/// Before each plan of the placement it wraps it scans the bucket and
/// shard registers ([`ClusterScan`]), and it acts only on a bucket or a
/// shard register that the previous scan found in the same state. A
/// creation writes its bucket register before its shard registers, and a
/// scan lists `buckets/` before `shards/`, so a shard register seen in one
/// scan was written after its bucket register, which the next scan's
/// listing would show unless the bucket was deleted. A creation in
/// progress may still look incomplete twice; then both write the same
/// shard registers with `If-None-Match: *`, and one of each pair wins.
///
/// It runs inside the node [`Lifecycle`](crate::Lifecycle), which keeps
/// the registry current, and wraps the placement that plans when it has
/// nothing to do: `Lifecycle::new(registry.clone(), BucketShards::new(registry,
/// failure_domain).with_placement(placement))`.
pub struct BucketShards<P = NoPlacement> {
    registry: NodeRegistry,
    level: FailureDomain,
    scan: ClusterScan,
    /// The buckets the last scan found missing shard registers, by register
    /// version.
    incomplete: BTreeMap<RegisterKey, Version>,
    /// The shard registers the last scan found without a bucket, by
    /// version.
    orphans: BTreeMap<RegisterKey, Version>,
    /// Whether the change being made is this placement's own.
    planned: bool,
    placement: P,
}

impl<P> std::fmt::Debug for BucketShards<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BucketShards")
            .field("level", &self.level)
            .field("incomplete", &self.incomplete)
            .field("orphans", &self.orphans)
            .finish_non_exhaustive()
    }
}

impl BucketShards {
    /// Places missing shards at `level` on the nodes `registry` lists.
    #[must_use]
    pub fn new(registry: NodeRegistry, level: FailureDomain) -> Self {
        Self {
            registry,
            level,
            scan: ClusterScan::new(),
            incomplete: BTreeMap::new(),
            orphans: BTreeMap::new(),
            planned: false,
            placement: NoPlacement,
        }
    }
}

impl<P> BucketShards<P> {
    /// Wraps `placement`, which plans whenever no shard register is
    /// missing or left over.
    pub fn with_placement<Q: Placement>(self, placement: Q) -> BucketShards<Q> {
        BucketShards {
            registry: self.registry,
            level: self.level,
            scan: self.scan,
            incomplete: self.incomplete,
            orphans: self.orphans,
            planned: false,
            placement,
        }
    }

    /// Judges the scan, remembers what it found, and plans the change for
    /// what the previous scan found too: the missing shard registers of
    /// one bucket, or else the deletion of every leftover one.
    fn plan_scanned(
        &mut self,
        proposals: &mut ProposalIds,
    ) -> Result<Option<ChangeSet>, ChangeError> {
        let present: BTreeMap<&BucketId, BTreeSet<ShardId>> =
            self.scan
                .shard_registers()
                .fold(BTreeMap::new(), |mut present, (_, _, config)| {
                    present
                        .entry(&config.bucket_id)
                        .or_default()
                        .insert(config.shard);
                    present
                });
        let mut incomplete = BTreeMap::new();
        let mut repair = None;
        for (key, version, bucket) in self.scan.bucket_registers() {
            let have = present.get(&bucket.bucket_id);
            let missing: Vec<ShardId> = bucket
                .shards
                .shards()
                .filter(|shard| have.is_none_or(|have| !have.contains(shard)))
                .collect();
            if missing.is_empty() {
                continue;
            }
            if repair.is_none() && self.incomplete.get(key) == Some(version) {
                repair = Some((bucket, missing));
            }
            incomplete.insert(key.clone(), version.clone());
        }
        let buckets: BTreeSet<&BucketId> = self.scan.buckets().map(|b| &b.bucket_id).collect();
        let mut orphans = BTreeMap::new();
        let mut leftover = Vec::new();
        for (key, version, config) in self.scan.shard_registers() {
            if buckets.contains(&config.bucket_id) {
                continue;
            }
            if self.orphans.get(key) == Some(version) {
                leftover.push((TypedKey::<ShardConfig>::new(key.clone()), version.clone()));
            }
            orphans.insert(key.clone(), version.clone());
        }
        // A bucket register that does not parse may name any of them.
        let unreadable = self.scan.unreadable() > 0;
        if unreadable && !leftover.is_empty() {
            tracing::warn!("a register does not parse; deleting no shard register");
        }
        let change = match repair {
            Some((bucket, missing)) => Some(self.complete(bucket, &missing, proposals)?),
            None if !leftover.is_empty() && !unreadable => {
                tracing::info!(
                    registers = leftover.len(),
                    "deleting the shard registers of deleted buckets"
                );
                let mut change = ChangeSet::new();
                for (key, version) in &leftover {
                    change = change.delete(key, version)?;
                }
                Some(change)
            }
            None => None,
        };
        self.incomplete = incomplete;
        self.orphans = orphans;
        Ok(change)
    }

    /// The creates of `bucket`'s `missing` shard registers, placed on the
    /// registry's nodes, loaded with every scanned shard. A shard placement
    /// cannot fill gets fewer members; one it cannot place at all is left
    /// for a later round.
    fn complete(
        &self,
        bucket: &BucketDocument,
        missing: &[ShardId],
        proposals: &mut ProposalIds,
    ) -> Result<ChangeSet, ChangeError> {
        let mut topology = Topology::from_registry(self.level, &self.registry);
        for shard in self.scan.shards() {
            topology.record(shard);
        }
        let mut change = ChangeSet::new();
        for &shard in missing {
            let placed = topology.place(&ShardRequest::new(
                &bucket.bucket_id,
                shard,
                bucket.replicas,
            ));
            let Some(primary) = topology.choose_primary(&bucket.bucket_id, shard, &placed.members)
            else {
                continue;
            };
            let new = NewShard {
                shard,
                primary,
                members: placed.members,
            };
            let config = first_config(bucket, new, proposals.next_id());
            topology.record(&config);
            change = change.create(&TypedKey::shard(&bucket.bucket_id, shard), &config)?;
        }
        tracing::info!(bucket = %bucket.name, shards = change.writes().len(), "creating the missing shard registers of a bucket");
        Ok(change)
    }
}

impl<P: Placement> Placement for BucketShards<P> {
    fn begin_tenure(&mut self) {
        self.placement.begin_tenure();
    }

    async fn plan<S: ControlStore>(
        &mut self,
        store: &S,
        proposals: &mut ProposalIds,
    ) -> Result<Option<ChangeSet>, ControlError> {
        self.planned = false;
        self.scan.refresh(store).await?;
        let change = self
            .plan_scanned(proposals)
            .map_err(|error| ControlError::Rejected(format!("an invalid shard change: {error}")))?;
        if let Some(change) = change.filter(|change| !change.is_empty()) {
            self.planned = true;
            return Ok(Some(change));
        }
        self.placement.plan(store, proposals).await
    }

    fn applied(&mut self, change: &ChangeSet, applied: &Applied) {
        if !std::mem::take(&mut self.planned) {
            self.placement.applied(change, applied);
        }
    }
}

#[cfg(test)]
mod tests;
