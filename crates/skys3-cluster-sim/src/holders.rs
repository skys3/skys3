//! Holders of read payload (plan M2-18): what a node's replicas do when a
//! gateway registers a read with them and fetches under it (§8.7, §9.2),
//! with delays that widen the gap between a read plan and its
//! registration, seeded bugs, and an audit of what the holders served.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use skys3_gateway::{LocalShards, ReadId, ReadPlan, Registered, ShardError, ShardRef, Shards};
use skys3_index::EntryState;
use skys3_io::SimMount;
use skys3_log::record::ExtentRef;
use skys3_shard::{Role, Shard};
use skys3_types::{EpochSeq, NodeId};

/// How holders behave ([`ReplicatedServices::with_holders`](crate::ReplicatedServices::with_holders)).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HolderFaults {
    /// The longest a holder waits before it registers a read, drawn at
    /// random for each: a write or an eviction may land in between, after
    /// the primary planned the read.
    pub register_delay: Duration,
    /// The longest a holder waits before it serves a fetch, drawn at
    /// random for each: a registration may lapse in between.
    pub fetch_delay: Duration,
    /// A seeded bug: a holder registers the copy of the key it holds now,
    /// whatever version the plan named.
    pub ignore_version: bool,
    /// A seeded bug: a holder serves fetches under registrations that
    /// lapsed.
    pub ignore_lapse: bool,
}

/// What holders did over a run.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HolderCounts {
    /// Reads a shard's primary registered as their holder.
    pub by_primary: u64,
    /// Reads a member registered as their holder.
    pub by_member: u64,
    /// Registrations refused because the replica no longer held the
    /// version, nor located its plan's payload.
    pub not_held: u64,
    /// Registrations of a version the replica's entry no longer named
    /// with its bytes, replaced or evicted since the plan: served from the
    /// payload the replica still located.
    pub superseded: u64,
    /// Fetches served.
    pub fetched: u64,
    /// Fetches refused because their registration had lapsed.
    pub lapsed: u64,
}

#[derive(Debug, Default)]
struct Audit {
    counts: HolderCounts,
    /// Fetches served under a registration that had lapsed.
    violations: Vec<String>,
}

/// The holder audit of every node, which clones share.
#[derive(Clone, Debug, Default)]
pub(crate) struct HolderAudit(Arc<Mutex<Audit>>);

impl HolderAudit {
    pub(crate) fn counts(&self) -> HolderCounts {
        self.lock().counts
    }

    pub(crate) fn check(&self) -> Result<(), String> {
        self.lock().violations.first().cloned().map_or(Ok(()), Err)
    }

    fn lock(&self) -> MutexGuard<'_, Audit> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// One node's replicas as holders, in one life.
#[derive(Clone, Debug)]
pub(crate) struct Holders {
    faults: HolderFaults,
    rng: Arc<Mutex<SmallRng>>,
    audit: HolderAudit,
}

impl Holders {
    pub(crate) fn new(faults: HolderFaults, seed: u64, audit: HolderAudit) -> Self {
        Self {
            faults,
            rng: Arc::new(Mutex::new(SmallRng::seed_from_u64(seed))),
            audit,
        }
    }

    /// Registers a read with the node's replica of `shard`, after the
    /// drawn delay; see [`Shards::register`].
    pub(crate) async fn register(
        &self,
        local: &LocalShards<SimMount>,
        shard: &ShardRef,
        holder: &NodeId,
        key: &str,
        mut version: EpochSeq,
        mut layout: Vec<ExtentRef>,
    ) -> Result<Option<Registered>, ShardError> {
        self.delay(self.faults.register_delay).await;
        let replica = local.set().get(&shard.into()).await;
        if self.faults.ignore_version
            && let Some(copy) = replica.as_ref().and_then(|r| current_copy(r, shard, key))
        {
            // The seeded bug: the copy held now stands for any version.
            (version, layout) = copy;
        }
        let current = replica.as_ref().and_then(|r| current_copy(r, shard, key));
        let superseded = current.is_none_or(|(current, _)| current != version);
        let registered = local.register(shard, holder, key, version, layout).await;
        let mut audit = self.audit.lock();
        if superseded && matches!(registered, Ok(Some(_))) {
            audit.counts.superseded += 1;
        }
        match &registered {
            Ok(Some(_)) if replica.as_ref().is_some_and(|r| r.role() == Role::Member) => {
                audit.counts.by_member += 1;
            }
            Ok(Some(_)) => audit.counts.by_primary += 1,
            Ok(None) => audit.counts.not_held += 1,
            Err(_) => {}
        }
        registered
    }

    /// Serves a fetch under the read `read`, after the drawn delay; see
    /// [`Shards::fetch`]. A fetch served although its registration had
    /// lapsed when it was asked is a violation.
    pub(crate) async fn fetch(
        &self,
        local: &LocalShards<SimMount>,
        shard: &ShardRef,
        holder: &NodeId,
        read: ReadId,
        position: EpochSeq,
    ) -> Result<Bytes, ShardError> {
        self.delay(self.faults.fetch_delay).await;
        let live = local.set().reads().is_live(read);
        let fetched = if self.faults.ignore_lapse {
            // The seeded bug: serve whatever the registration says.
            match local.set().get(&shard.into()).await {
                Some(replica) => {
                    replica
                        .payload(position)
                        .await
                        .map_err(|error| ShardError::Unavailable {
                            shard: shard.clone(),
                            reason: error.to_string(),
                        })
                }
                None => Err(ShardError::NotFound(shard.clone())),
            }
        } else {
            local.fetch(shard, holder, read, position).await
        };
        let mut audit = self.audit.lock();
        match (&fetched, live) {
            (Ok(_), true) => audit.counts.fetched += 1,
            (Ok(_), false) => audit.violations.push(format!(
                "{holder} served {position} of shard {shard} under read {read}, whose \
                 registration had lapsed"
            )),
            (Err(_), false) => audit.counts.lapsed += 1,
            (Err(_), true) => {}
        }
        fetched
    }

    async fn delay(&self, longest: Duration) {
        if longest.is_zero() {
            return;
        }
        let delay = {
            let mut rng = self.rng.lock().unwrap_or_else(PoisonError::into_inner);
            rng.random_range(Duration::ZERO..=longest)
        };
        tokio::time::sleep(delay).await;
    }
}

/// A plan of `key` read from `replica`'s own index, naming `node` as its
/// only holder: what a replica that serves reads it should refuse plans.
pub(crate) fn own_plan(
    replica: &Shard<SimMount>,
    shard: &ShardRef,
    key: &str,
    node: &NodeId,
) -> Result<ReadPlan, ShardError> {
    let unavailable = |error: skys3_index::IndexError| ShardError::Unavailable {
        shard: shard.clone(),
        reason: error.to_string(),
    };
    let entry = replica
        .index()
        .read()
        .and_then(|r| r.entry(&shard.into(), key))
        .map_err(unavailable)?;
    let copy = current_copy(replica, shard, key).filter(|(version, _)| {
        entry.as_ref().is_some_and(|entry| entry.version == *version)
    });
    let (layout, holders) = match copy {
        Some((_, layout)) => (layout, vec![node.clone()]),
        None => (Vec::new(), Vec::new()),
    };
    Ok(ReadPlan {
        entry,
        layout,
        holders,
    })
}

/// The version of `key` that `replica` holds bytes of now, with their
/// layout.
fn current_copy(
    replica: &Shard<SimMount>,
    shard: &ShardRef,
    key: &str,
) -> Option<(EpochSeq, Vec<ExtentRef>)> {
    let shard = shard.into();
    let reader = replica.index().read().ok()?;
    let entry = reader.entry(&shard, key).ok()??;
    if entry.state == EntryState::Evicted {
        return None;
    }
    let object = entry.object?;
    let layout = skys3_shard::reads::layout(&reader, &shard, &object).ok()??;
    Some((entry.version, layout))
}
