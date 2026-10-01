//! The shards open on a node: what the gateway's shard interface needs to
//! open, seal, and drop real shards (§4.1).

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use skys3_index::Index;
use skys3_io::{BlockingPool, Disk};
use skys3_log::{SegmentLog, ShardRef};
use skys3_types::ShardConfig;
use tokio::sync::Mutex;

use crate::error::ShardError;
use crate::shard::{Shard, ShardSummary};

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
    shards: Mutex<BTreeMap<ShardRef, Shard<D>>>,
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
            shards: Mutex::default(),
        }
    }

    /// Opens the shard of `config`, or returns it if it is open already.
    ///
    /// # Errors
    ///
    /// As [`Shard::open`].
    pub async fn open(&self, config: &ShardConfig) -> Result<Shard<D>, ShardError> {
        let key = ShardRef::new(config.bucket_id.clone(), config.shard);
        // Held across the open, so a shard never gets two sequencers.
        let mut shards = self.shards.lock().await;
        if let Some(shard) = shards.get(&key) {
            return Ok(shard.clone());
        }
        let shard = Shard::open(
            config,
            self.log.clone(),
            Arc::clone(&self.index),
            self.pool.clone(),
        )
        .await?;
        shards.insert(key, shard.clone());
        Ok(shard)
    }

    /// Returns the open shard `shard`.
    pub async fn get(&self, shard: &ShardRef) -> Option<Shard<D>> {
        self.shards.lock().await.get(shard).cloned()
    }

    /// The open shards, in order.
    pub async fn shards(&self) -> Vec<ShardRef> {
        self.shards.lock().await.keys().cloned().collect()
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
    /// # Errors
    ///
    /// [`ShardError::Unavailable`] if the index fails; startup recovery
    /// then reclaims the shard (§4.1).
    pub async fn remove(&self, shard: &ShardRef) -> Result<(), ShardError> {
        let removed = self.shards.lock().await.remove(shard);
        if let Some(removed) = removed {
            // A shard that stopped already has nothing left to apply.
            let _ = removed.close().await;
        }
        let (index, key) = (Arc::clone(&self.index), shard.clone());
        match self.pool.run(move || index.remove_shard(&key)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(ShardError::unavailable(shard, error)),
            Err(error) => Err(ShardError::unavailable(shard, error)),
        }
    }

    async fn find(&self, shard: &ShardRef) -> Result<Shard<D>, ShardError> {
        self.get(shard)
            .await
            .ok_or_else(|| ShardError::NotFound(shard.clone()))
    }
}
