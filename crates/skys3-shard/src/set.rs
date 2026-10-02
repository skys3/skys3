//! The shards open on a node: what the gateway's shard interface needs to
//! open, seal, and drop real shards (§4.1).

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use skys3_index::{ImportRanges, Index, IndexError};
use skys3_io::{BlockingPool, Disk};
use skys3_log::{SegmentLog, ShardRef};
use skys3_types::{BucketId, KeyHash, Label, NodeId, ShardConfig};
use tokio::sync::{Mutex, MutexGuard, watch};
use tokio::task::JoinSet;

use crate::error::ShardError;
use crate::shard::{Role, Shard, ShardSummary};

/// A shard's place in the set.
enum Slot<D: Disk> {
    /// The shard is open.
    Open(Shard<D>),
    /// The shard is being removed: it is not open, and cannot open again
    /// until the removal finished. The receiver turns `true` then, and
    /// closes if the removal task died first.
    Removing(watch::Receiver<bool>),
}

type Slots<D> = BTreeMap<ShardRef, Slot<D>>;

/// The shard replicas open on one node, over the logs of the node's disks.
///
/// Each shard replica keeps all its records on one disk: the location map
/// names no disk, so a replica reads its payload from the log it appends
/// to (§10.2). The disk is chosen by the hash of the shard's bucket ID and
/// number among the disks in label order ([`ShardSet::disk_of`]), so it
/// stays the same across restarts as long as the node keeps the same
/// disks. A node therefore never starts with a disk added or removed.
pub struct ShardSet<D: Disk> {
    index: Arc<Index>,
    /// Each disk's log, in label order; never empty.
    logs: Vec<(Label, SegmentLog<D>)>,
    pool: BlockingPool,
    shards: Arc<Mutex<Slots<D>>>,
}

impl<D: Disk> Clone for ShardSet<D> {
    /// Another handle to the same set.
    fn clone(&self) -> Self {
        Self {
            index: Arc::clone(&self.index),
            logs: self.logs.clone(),
            pool: self.pool.clone(),
            shards: Arc::clone(&self.shards),
        }
    }
}

impl<D: Disk> fmt::Debug for ShardSet<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardSet").finish_non_exhaustive()
    }
}

impl<D: Disk> ShardSet<D> {
    /// The label [`ShardSet::new`] gives its one disk.
    pub const SINGLE_DISK: &str = "disk-0";

    /// No shards yet, over `index` and the log of the node's one disk,
    /// which is labelled [`ShardSet::SINGLE_DISK`]. Index I/O runs on
    /// `pool`.
    #[must_use]
    pub fn new(index: Arc<Index>, log: SegmentLog<D>, pool: BlockingPool) -> Self {
        let label = Label::new(Self::SINGLE_DISK).expect("the label is valid");
        Self::with_disks(index, BTreeMap::from([(label, log)]), pool)
    }

    /// No shards yet, over `index` and each disk's log, by disk label.
    ///
    /// # Panics
    ///
    /// If `logs` is empty.
    #[must_use]
    pub fn with_disks(
        index: Arc<Index>,
        logs: BTreeMap<Label, SegmentLog<D>>,
        pool: BlockingPool,
    ) -> Self {
        assert!(!logs.is_empty(), "a shard set needs at least one disk");
        Self {
            index,
            logs: logs.into_iter().collect(),
            pool,
            shards: Arc::default(),
        }
    }

    /// The label of the disk that holds `shard`'s records: the shard's
    /// hash ([`KeyHash`] of its bucket ID and its number as one byte)
    /// modulo the number of disks, among the disks in label order.
    #[must_use]
    pub fn disk_of(&self, shard: &ShardRef) -> &Label {
        &self.logs[self.placement(shard)].0
    }

    /// Each disk's log, in label order.
    pub fn logs(&self) -> impl Iterator<Item = (&Label, &SegmentLog<D>)> {
        self.logs.iter().map(|(label, log)| (label, log))
    }

    fn placement(&self, shard: &ShardRef) -> usize {
        let hash = KeyHash::of(&shard.bucket, &[shard.shard.get()]).get();
        // The remainder is below the disk count, which is a `usize`.
        (hash % self.logs.len() as u64) as usize
    }

    /// Opens the shard of `config`, or returns it if it is open already.
    ///
    /// An open shard is compared with `config` by epoch (§5.1). In the same
    /// configuration it is returned as it is. In an older epoch it first
    /// adopts `config` in order with its writes (see
    /// [`Shard::reconfigure`]). A `config` older than the shard's epoch, or
    /// another configuration in the same epoch, is refused. A shard that is
    /// being removed opens only once its removal finished.
    ///
    /// # Errors
    ///
    /// As [`Shard::open`] and [`Shard::reconfigure`];
    /// [`ShardError::Unavailable`] if the shard's removal did not finish.
    pub async fn open(&self, config: &ShardConfig) -> Result<Shard<D>, ShardError> {
        self.open_as(config, None).await
    }

    /// Opens node `node`'s replica in the shard of `config`, in the role the
    /// configuration gives it, or returns it if it is open already; see
    /// [`ShardSet::open`] and [`Shard::open_replica`]. A replica open as
    /// the shard's only member that `config` gives learners (§6.7) is
    /// closed once its records are applied, and a new one opens as their
    /// primary: requests that still hold the closed one fail as
    /// unavailable.
    ///
    /// # Errors
    ///
    /// As [`ShardSet::open`] and [`Shard::open_replica`].
    pub async fn open_replica(
        &self,
        config: &ShardConfig,
        node: &NodeId,
    ) -> Result<Shard<D>, ShardError> {
        self.open_as(config, Some(node)).await
    }

    async fn open_as(
        &self,
        config: &ShardConfig,
        node: Option<&NodeId>,
    ) -> Result<Shard<D>, ShardError> {
        let key = ShardRef::new(config.bucket_id.clone(), config.shard);
        // Held across the open and any epoch change, so a shard never gets
        // two sequencers, and two opens in a newer epoch append one CONFIG.
        let mut shards = self.settled(&key).await?;
        if let Some(Slot::Open(shard)) = shards.get(&key) {
            // A replica opened as the shard's only member runs for one
            // member. Given learners (§6.7), it closes once its records are
            // applied, and opens again as their primary.
            let current = shard.config();
            let gains_learners = node.is_some()
                && shard.role() == Role::Alone
                && !shard.is_stopped()
                && config.epoch > current.epoch
                && config.members == current.members
                && !config.learners.is_empty();
            // A node removed from the shard rejoins only as a learner
            // (§6.7): a replica that is not a learner of the same primary,
            // such as a member removed while it was cut off, or a learner
            // that a takeover left out and the coordinator added again,
            // cannot follow the new configuration. It stops, and opens
            // again as a learner, as a restart would open it; its records
            // seed catch-up only once the primary verifies them.
            let rejoins = node.is_some_and(|node| {
                config.epoch > current.epoch
                    && config.is_learner(node)
                    && (shard.role() != Role::Learner || config.primary != current.primary)
            });
            if rejoins {
                shard
                    .abandon("the node rejoins the shard as a learner")
                    .await;
            } else if gains_learners {
                shard.close().await?;
            } else {
                shard.reconfigure(config).await?;
                return Ok(shard.clone());
            }
        }
        let log = self.logs[self.placement(&key)].1.clone();
        let (index, pool) = (Arc::clone(&self.index), self.pool.clone());
        let shard = match node {
            Some(node) => Shard::open_replica(config, node, log, index, pool).await?,
            None => Shard::open(config, log, index, pool).await?,
        };
        shards.insert(key, Slot::Open(shard.clone()));
        Ok(shard)
    }

    /// Stops `replica`, node `node`'s replica in the shard of `config`,
    /// keeps the shard closed while `work` runs, and then opens the
    /// replica again in `config`, from what the index holds then, whatever
    /// `work` returned. A learner installs a snapshot this way (§6.7).
    /// Meanwhile the shard is not open: [`ShardSet::get`] does not find it,
    /// and opening it waits.
    ///
    /// # Errors
    ///
    /// As [`Shard::open_replica`], if the replica does not open again; it
    /// is then not open, and opening it later tries again.
    pub(crate) async fn reopen_after<T>(
        &self,
        replica: &Shard<D>,
        config: &ShardConfig,
        node: &NodeId,
        work: impl Future<Output = T>,
    ) -> Result<(Shard<D>, T), ShardError> {
        let key = replica.shard().clone();
        let (done, closed) = watch::channel(false);
        {
            let mut shards = self.settled(&key).await?;
            let open = match shards.get(&key) {
                Some(Slot::Open(open)) => open.durable().same_channel(&replica.durable()),
                _ => false,
            };
            if !open {
                return Err(ShardError::unavailable(&key, "the replica is not open"));
            }
            shards.insert(key.clone(), Slot::Removing(closed));
        }
        replica.abandon("the learner installs a snapshot").await;
        let output = work.await;
        let log = self.logs[self.placement(&key)].1.clone();
        let (index, pool) = (Arc::clone(&self.index), self.pool.clone());
        let mut shards = self.shards.lock().await;
        let opened = Shard::open_replica(config, node, log, index, pool).await;
        match &opened {
            Ok(shard) => shards.insert(key, Slot::Open(shard.clone())),
            Err(_) => shards.remove(&key),
        };
        drop(shards);
        // Nobody may be waiting.
        let _ = done.send(true);
        opened.map(|shard| (shard, output))
    }

    /// Returns the open shard `shard`.
    pub async fn get(&self, shard: &ShardRef) -> Option<Shard<D>> {
        match self.shards.lock().await.get(shard) {
            Some(Slot::Open(open)) => Some(open.clone()),
            Some(Slot::Removing(_)) | None => None,
        }
    }

    /// The namespace import ranges of `bucket` on this node and their
    /// checkpoints (§9.1), read on the index's pool.
    ///
    /// # Errors
    ///
    /// An [`IndexError`] if the index fails.
    pub async fn import_ranges(
        &self,
        bucket: &BucketId,
    ) -> Result<Option<ImportRanges>, IndexError> {
        let (index, bucket) = (Arc::clone(&self.index), bucket.clone());
        self.pool.run(move || index.import_ranges(&bucket)).await?
    }

    /// Stores the namespace import ranges of `bucket` durably, or removes
    /// them, on the index's pool ([`Index::set_import_ranges`]).
    ///
    /// # Errors
    ///
    /// An [`IndexError`] if the index fails; nothing changes then.
    pub async fn set_import_ranges(
        &self,
        bucket: &BucketId,
        import: Option<ImportRanges>,
    ) -> Result<(), IndexError> {
        let (index, bucket) = (Arc::clone(&self.index), bucket.clone());
        self.pool
            .run(move || index.set_import_ranges(&bucket, import.as_ref()))
            .await?
    }

    /// The open shards, in order.
    pub async fn shards(&self) -> Vec<ShardRef> {
        self.shards
            .lock()
            .await
            .iter()
            .filter(|(_, slot)| matches!(slot, Slot::Open(_)))
            .map(|(shard, _)| shard.clone())
            .collect()
    }

    /// Closes every open shard ([`Shard::close`]): each stops once the
    /// records it sequenced are applied, so nothing is in flight when the
    /// node checkpoints and exits, or, on a replicated shard, once its
    /// [`AckTimeout`](crate::AckTimeout) gave up on its members. The shards
    /// stay in the set, closed. The shards close concurrently, so the whole
    /// call waits about one acknowledgement timeout, not one per shard
    /// whose members are gone, and returns once every shard is closed.
    ///
    /// # Errors
    ///
    /// The error of the first shard, in shard order, that failed to close;
    /// every shard is closed regardless.
    pub async fn close_all(&self) -> Result<(), ShardError> {
        let open: Vec<_> = self
            .shards
            .lock()
            .await
            .values()
            .filter_map(|slot| match slot {
                Slot::Open(shard) => Some(shard.clone()),
                Slot::Removing(_) => None,
            })
            .collect();
        let mut closing = JoinSet::new();
        for (n, shard) in open.into_iter().enumerate() {
            closing.spawn(async move { (n, shard.close().await) });
        }
        let mut errors = BTreeMap::new();
        while let Some(closed) = closing.join_next().await {
            match closed {
                Ok((n, Err(error))) => {
                    errors.insert(n, error);
                }
                Ok((_, Ok(()))) => {}
                Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
                // Only the runtime shutting down cancels a close.
                Err(_) => {}
            }
        }
        errors.into_values().next().map_or(Ok(()), Err)
    }

    /// Seals `shard`; see [`Shard::seal`].
    ///
    /// # Errors
    ///
    /// [`ShardError::NotFound`] if the shard is not open, and otherwise as
    /// [`Shard::seal`].
    pub async fn seal(&self, shard: &ShardRef) -> Result<ShardSummary, ShardError> {
        self.find(shard).await?.seal().await
    }

    /// Lifts one seal of `shard`; see [`Shard::unseal`].
    ///
    /// # Errors
    ///
    /// [`ShardError::NotFound`] if the shard is not open.
    pub async fn unseal(&self, shard: &ShardRef) -> Result<(), ShardError> {
        self.find(shard).await?.unseal();
        Ok(())
    }

    /// Closes `shard` and removes everything the index holds for it. A
    /// shard that is not open has its index state removed all the same.
    ///
    /// The shard cannot open again until its removal finished, so a shard
    /// opened afterwards starts from an index that holds nothing of it. The
    /// removal runs to the end even if the returned future is dropped.
    ///
    /// # Errors
    ///
    /// [`ShardError::Unavailable`] if the index fails; startup recovery
    /// then reclaims the shard (§4.1).
    pub async fn remove(&self, shard: &ShardRef) -> Result<(), ShardError> {
        let (done, removing) = watch::channel(false);
        let removed = match self
            .settled(shard)
            .await?
            .insert(shard.clone(), Slot::Removing(removing))
        {
            Some(Slot::Open(removed)) => Some(removed),
            Some(Slot::Removing(_)) | None => None,
        };
        let shards = Arc::clone(&self.shards);
        let (index, pool, key) = (Arc::clone(&self.index), self.pool.clone(), shard.clone());
        let removal = tokio::spawn(async move {
            if let Some(removed) = removed {
                // A shard that stopped already has nothing left to apply.
                let _ = removed.close().await;
            }
            let removed = {
                let key = key.clone();
                pool.run(move || index.remove_shard(&key)).await
            };
            shards.lock().await.remove(&key);
            // Nobody may be waiting.
            let _ = done.send(true);
            match removed {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(ShardError::unavailable(&key, error)),
                Err(error) => Err(ShardError::unavailable(&key, error)),
            }
        });
        removal
            .await
            .unwrap_or_else(|error| Err(ShardError::unavailable(shard, error)))
    }

    /// Locks the set once `shard` is not being removed.
    async fn settled(&self, shard: &ShardRef) -> Result<MutexGuard<'_, Slots<D>>, ShardError> {
        loop {
            let shards = self.shards.lock().await;
            let Some(Slot::Removing(removing)) = shards.get(shard) else {
                return Ok(shards);
            };
            let mut removing = removing.clone();
            drop(shards);
            if removing.wait_for(|done| *done).await.is_err() {
                return Err(ShardError::unavailable(
                    shard,
                    "the shard's removal did not finish",
                ));
            }
        }
    }

    async fn find(&self, shard: &ShardRef) -> Result<Shard<D>, ShardError> {
        self.get(shard)
            .await
            .ok_or_else(|| ShardError::NotFound(shard.clone()))
    }
}
