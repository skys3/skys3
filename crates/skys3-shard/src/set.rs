//! The shards open on a node: what the gateway's shard interface needs to
//! open, seal, and drop real shards (§4.1).

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use skys3_index::Index;
use skys3_io::{BlockingPool, Disk};
use skys3_log::{SegmentLog, ShardRef};
use skys3_types::ShardConfig;
use tokio::sync::{Mutex, MutexGuard, watch};

use crate::error::ShardError;
use crate::shard::{Shard, ShardSummary};

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

/// The shard replicas open on one node, all on one disk's log.
///
/// A shard's records must all be in one disk's log, because replay applies
/// each disk's records in position order separately. Choosing a disk for
/// each shard on a node with several is left to the node's startup (plan
/// M1-13).
pub struct ShardSet<D: Disk> {
    index: Arc<Index>,
    log: SegmentLog<D>,
    pool: BlockingPool,
    shards: Arc<Mutex<Slots<D>>>,
}

impl<D: Disk> fmt::Debug for ShardSet<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardSet").finish_non_exhaustive()
    }
}

impl<D: Disk> ShardSet<D> {
    /// No shards yet, over `index` and the log of the disk that holds them.
    /// Index I/O runs on `pool`.
    #[must_use]
    pub fn new(index: Arc<Index>, log: SegmentLog<D>, pool: BlockingPool) -> Self {
        Self {
            index,
            log,
            pool,
            shards: Arc::default(),
        }
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
        let key = ShardRef::new(config.bucket_id.clone(), config.shard);
        // Held across the open and any epoch change, so a shard never gets
        // two sequencers, and two opens in a newer epoch append one CONFIG.
        let mut shards = self.settled(&key).await?;
        if let Some(Slot::Open(shard)) = shards.get(&key) {
            shard.reconfigure(config).await?;
            return Ok(shard.clone());
        }
        let shard = Shard::open(
            config,
            self.log.clone(),
            Arc::clone(&self.index),
            self.pool.clone(),
        )
        .await?;
        shards.insert(key, Slot::Open(shard.clone()));
        Ok(shard)
    }

    /// Returns the open shard `shard`.
    pub async fn get(&self, shard: &ShardRef) -> Option<Shard<D>> {
        match self.shards.lock().await.get(shard) {
            Some(Slot::Open(open)) => Some(open.clone()),
            Some(Slot::Removing(_)) | None => None,
        }
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
