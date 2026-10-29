//! The gateway's shard map (§6.2).

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use skys3_index::{Index, IndexError};
use skys3_io::BlockingPool;
use skys3_types::ShardConfig;

use crate::shard::ShardRef;

/// The configuration of every shard the gateway routes to, by shard, kept
/// in the node-local index.
///
/// The map only moves forward: a configuration replaces the shard's only
/// if its epoch is newer, so a late or repeated redirect hint never undoes
/// a newer one. It is a cache. Every request carries the epoch it names,
/// and replicas refuse to serve one they are not the primary of, so a
/// stale entry costs a redirect, never a wrong answer. Each change is
/// written to the index durably before it is used, so a restarted node
/// routes as it did, and it learns what changed since from redirects.
#[derive(Clone)]
pub struct ShardMap {
    inner: Arc<Inner>,
}

struct Inner {
    routes: Mutex<BTreeMap<ShardRef, ShardConfig>>,
    /// Where the map is kept, and the pool its I/O runs on.
    kept: Option<(Arc<Index>, BlockingPool)>,
    /// Orders changes, so they reach the index in the order they apply.
    changes: tokio::sync::Mutex<()>,
}

impl fmt::Debug for ShardMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardMap")
            .field("routes", &self.routes().len())
            .finish_non_exhaustive()
    }
}

impl ShardMap {
    /// An empty map that is not kept anywhere.
    #[must_use]
    pub fn in_memory() -> Self {
        Self::with(BTreeMap::new(), None)
    }

    /// The map `index` keeps, which it keeps from now on. Index I/O runs on
    /// `pool`.
    ///
    /// # Errors
    ///
    /// The index's error.
    pub async fn load(index: Arc<Index>, pool: BlockingPool) -> Result<Self, IndexError> {
        let kept = {
            let index = Arc::clone(&index);
            pool.run(move || index.read()?.shard_map()).await??
        };
        let routes = kept
            .into_iter()
            .map(|(shard, config)| {
                let shard = ShardRef {
                    bucket: shard.bucket,
                    shard: shard.shard,
                };
                (shard, config)
            })
            .collect();
        Ok(Self::with(routes, Some((index, pool))))
    }

    fn with(
        routes: BTreeMap<ShardRef, ShardConfig>,
        kept: Option<(Arc<Index>, BlockingPool)>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                routes: Mutex::new(routes),
                kept,
                changes: tokio::sync::Mutex::new(()),
            }),
        }
    }

    /// The configuration the map holds for `shard`.
    #[must_use]
    pub fn get(&self, shard: &ShardRef) -> Option<ShardConfig> {
        self.routes().get(shard).cloned()
    }

    /// Every configuration the map holds, by shard.
    #[must_use]
    pub fn all(&self) -> BTreeMap<ShardRef, ShardConfig> {
        self.routes().clone()
    }

    /// Keeps `config` for its shard if the map holds none, or one of an
    /// older epoch, and returns whether it did. A configuration the index
    /// could not keep is still used, and a restart forgets it.
    pub async fn learn(&self, config: ShardConfig) -> bool {
        let shard = ShardRef {
            bucket: config.bucket_id.clone(),
            shard: config.shard,
        };
        let _ordered = self.inner.changes.lock().await;
        if self
            .routes()
            .get(&shard)
            .is_some_and(|known| known.epoch >= config.epoch)
        {
            return false;
        }
        if let Some((index, pool)) = &self.inner.kept {
            let (index, kept) = (Arc::clone(index), config.clone());
            let stored = pool.run(move || index.store_route(&kept)).await;
            if !matches!(stored, Ok(Ok(()))) {
                tracing::warn!(%shard, epoch = %config.epoch, "the shard map could not be kept");
            }
        }
        tracing::debug!(%shard, epoch = %config.epoch, primary = %config.primary, "a shard's configuration");
        self.routes().insert(shard, config);
        true
    }

    /// Forgets `shard`, once its bucket is gone.
    pub async fn forget(&self, shard: &ShardRef) {
        let _ordered = self.inner.changes.lock().await;
        if self.routes().remove(shard).is_none() {
            return;
        }
        if let Some((index, pool)) = &self.inner.kept {
            let (index, key) = (Arc::clone(index), shard.into());
            let removed = pool.run(move || index.forget_route(&key)).await;
            if !matches!(removed, Ok(Ok(()))) {
                tracing::warn!(%shard, "the shard map could not forget a shard");
            }
        }
    }

    fn routes(&self) -> MutexGuard<'_, BTreeMap<ShardRef, ShardConfig>> {
        // Every update leaves the map consistent.
        self.inner
            .routes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}
