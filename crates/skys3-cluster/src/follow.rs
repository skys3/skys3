//! Following the shard registers (§6.3, §6.7): [`RegisterFollower`] opens
//! the replica of every shard whose register names the node, and brings an
//! open one up to a newer configuration, from the configurations a
//! [`ShardConfigs`] gives.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use skys3_control::{
    ControlError, ControlStore, KeyPrefix, RegisterKind, RetryPolicy, TypedKey, Version,
    read_with_retries,
};
use skys3_gateway::ShardRef;
use skys3_io::Disk;
use skys3_net::Network;
use skys3_shard::replication::BoxFuture;
use skys3_types::ShardConfig;

use crate::shards::ReplicatedShards;

/// Where a node reads shard configurations: the shard registers of §6.1,
/// or a stand-in for them.
pub trait ShardConfigs: Send + Sync + 'static {
    /// The shards that have a configuration: every shard register.
    ///
    /// # Errors
    ///
    /// A [`ControlError`] if they cannot be listed.
    fn shards(&self) -> BoxFuture<'_, Result<BTreeSet<ShardRef>, ControlError>>;

    /// The configuration of `shard` now, or `None` if it has none: its
    /// register is gone with its bucket, or not written yet.
    ///
    /// # Errors
    ///
    /// A [`ControlError`] if it cannot be read.
    fn config<'a>(
        &'a self,
        shard: &'a ShardRef,
    ) -> BoxFuture<'a, Result<Option<ShardConfig>, ControlError>>;
}

/// [`ShardConfigs`] in a [`ControlStore`], under the register layout of
/// §6.1 (`shards/<bucket-id>/<n>.json`).
///
/// A listing names each register's version, and a version names one value
/// (§6.1), so a configuration read once is not read again while the next
/// listing names the same version: following the registers costs one
/// listing, plus one read for each register that changed since. Any other
/// read of a configuration goes to the store.
pub struct ControlShardConfigs<S> {
    store: S,
    policy: RetryPolicy,
    cache: Mutex<Cache>,
}

/// What [`ControlShardConfigs`] remembers between calls.
#[derive(Default)]
struct Cache {
    /// The version of each shard register that the latest listing named
    /// and no read has used yet.
    listed: BTreeMap<ShardRef, Version>,
    /// The configuration last read of each listed shard, and its version.
    read: BTreeMap<ShardRef, (Version, Option<ShardConfig>)>,
}

impl<S> fmt::Debug for ControlShardConfigs<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlShardConfigs")
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl<S: ControlStore> ControlShardConfigs<S> {
    /// The shard registers of `store`, whose reads retry under `policy`.
    #[must_use]
    pub fn new(store: S, policy: RetryPolicy) -> Self {
        Self {
            store,
            policy,
            cache: Mutex::default(),
        }
    }

    fn cache(&self) -> std::sync::MutexGuard<'_, Cache> {
        self.cache.lock().unwrap_or_else(PoisonError::into_inner)
    }

    async fn list(&self) -> Result<BTreeSet<ShardRef>, ControlError> {
        let listed = self.store.list(&KeyPrefix::shards()).await?;
        let listed: BTreeMap<ShardRef, Version> = listed
            .into_iter()
            .filter_map(|(key, version)| match key.kind() {
                RegisterKind::Shard(bucket, shard) => Some((ShardRef { bucket, shard }, version)),
                _ => None,
            })
            .collect();
        let shards = listed.keys().cloned().collect();
        let mut cache = self.cache();
        cache.read.retain(|shard, _| listed.contains_key(shard));
        cache.listed = listed;
        Ok(shards)
    }

    async fn read(&self, shard: &ShardRef) -> Result<Option<ShardConfig>, ControlError> {
        {
            let mut cache = self.cache();
            if let Some(listed) = cache.listed.remove(shard)
                && let Some((version, config)) = cache.read.get(shard)
                && *version == listed
            {
                return Ok(config.clone());
            }
        }
        let key = TypedKey::shard(&shard.bucket, shard.shard);
        let Some(held) = read_with_retries(&self.store, &key, &self.policy).await? else {
            self.cache().read.remove(shard);
            return Ok(None);
        };
        self.cache()
            .read
            .insert(shard.clone(), (held.version, Some(held.value.clone())));
        Ok(Some(held.value))
    }
}

impl<S: ControlStore> ShardConfigs for ControlShardConfigs<S> {
    fn shards(&self) -> BoxFuture<'_, Result<BTreeSet<ShardRef>, ControlError>> {
        Box::pin(self.list())
    }

    fn config<'a>(
        &'a self,
        shard: &'a ShardRef,
    ) -> BoxFuture<'a, Result<Option<ShardConfig>, ControlError>> {
        Box::pin(self.read(shard))
    }
}

/// Follows the shard registers that name a node (§6.3, §6.7): for every
/// shard whose configuration names the node as member or learner, it opens
/// the node's replica, or brings the open one up to the configuration if
/// it is newer. That is how a node learns that it was added to a shard as
/// a learner, that its learner was promoted, or that a member took over
/// from its primary, without a restart.
///
/// The coordinator's pushes and the node's change stream say when the
/// registers changed (§6.2); [`RegisterFollower::follow`] is one pass,
/// for a caller that follows them, and [`RegisterFollower::run`] passes on
/// a fixed interval. A configuration that does not name the node is left
/// to the replica: one that is open stops once it learns of it.
pub struct RegisterFollower<N: Network, D: Disk> {
    shards: ReplicatedShards<N, D>,
    configs: Arc<dyn ShardConfigs>,
}

impl<N: Network, D: Disk> fmt::Debug for RegisterFollower<N, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegisterFollower")
            .field("shards", &self.shards)
            .finish_non_exhaustive()
    }
}

impl<N: Network, D: Disk> RegisterFollower<N, D> {
    /// Follows the configurations `configs` gives, opening replicas in
    /// `shards`.
    #[must_use]
    pub fn new(shards: ReplicatedShards<N, D>, configs: Arc<dyn ShardConfigs>) -> Self {
        Self { shards, configs }
    }

    /// One pass over every shard: opens or brings up to date the node's
    /// replica of each shard whose configuration names it, one shard at a
    /// time, in shard order. A shard whose configuration cannot be read,
    /// or whose replica fails to open, is logged and tried again on the
    /// next pass. Returns how many replicas the pass opened or brought up
    /// to date.
    pub async fn follow(&self) -> usize {
        let shards = match self.configs.shards().await {
            Ok(shards) => shards,
            Err(error) => {
                tracing::debug!(%error, "the shard registers cannot be listed");
                return 0;
            }
        };
        let node = self.shards.node_id();
        let set = self.shards.replication().set();
        let mut opened = 0;
        for shard in &shards {
            let config = match self.configs.config(shard).await {
                Ok(Some(config)) => config,
                Ok(None) => continue,
                Err(error) => {
                    tracing::debug!(%shard, %error, "the shard's register cannot be read");
                    continue;
                }
            };
            if !config.is_member(node) && !config.is_learner(node) {
                continue;
            }
            let open = set.get(&shard.into()).await;
            if let Some(replica) = &open {
                self.shards.observer().found(replica);
            }
            if open.is_some_and(|replica| replica.config().epoch >= config.epoch) {
                continue;
            }
            match self.shards.open_config(shard, &config).await {
                Ok(_) => opened += 1,
                Err(error) => {
                    tracing::debug!(%shard, %error, "following the shard's register failed");
                }
            }
        }
        opened
    }

    /// Passes every `interval`, starting one `interval` from now, until
    /// the task is dropped.
    pub async fn run(self, interval: Duration) {
        loop {
            tokio::time::sleep(interval).await;
            self.follow().await;
        }
    }
}
