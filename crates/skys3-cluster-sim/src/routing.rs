//! Node services whose gateways route every request to its shard's primary
//! by their shard maps (plan M2-08), with stale maps to start from and the
//! audit the routing scenarios check.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use skys3::control::NodeStore;
use skys3_gateway::ShardRef;
use skys3_gateway::routing::{RoutedShards, RoutingConfig, RoutingStats, ShardMap, serve_peers};
use skys3_io::{BlockingPool, SimMount};
use skys3_net::TurmoilNetwork;
use skys3_types::{Epoch, NodeId, ShardConfig};

use crate::node::{BoxError, ControlHandle, NodeEnv, NodeServices};
use crate::replication::{ReplicatedServices, ReplicatedShards};

/// The shards a routing node's gateway calls.
pub type RoutingShards =
    RoutedShards<ReplicatedShards, SimMount, TurmoilNetwork, NodeStore<ControlHandle>>;

/// Replicated node services whose gateways serve every key: each gateway
/// sends a request to the primary its shard map names, in process or to
/// another node, and follows redirect hints (§5.1, §6.2). Clients may then
/// send any request to any node ([`Workload::any_gateway`](crate::Workload::any_gateway)).
///
/// Each node's map starts stale where it holds nothing yet, as after
/// configurations changed while the node was away, unless
/// [`RoutedServices::fresh`]: with the static placement in epoch `e > 1`,
/// a third of the shards are known in epoch `e − 1` with the next member
/// as their primary, which a member's redirect corrects; a third in epoch
/// `e − 1` with only a node that does not exist as their member, so no
/// member answers and the gateway reads the register; and a third not at
/// all, which also needs the register.
///
/// [`RoutedServices::members_first`] instead starts every shard known in
/// epoch `e − 1` with the next member as its primary, so that a gateway
/// asks a member before the primary for every shard.
///
/// An audit records every request a replica served, and
/// [`RoutedServices::check_served`] finds any that the shard's primary in
/// the current configuration did not serve. The current configuration is
/// the register's: a primary that removed a member (plan M2-11) serves in
/// a later epoch than the placement's.
#[derive(Clone)]
pub struct RoutedServices {
    replicated: ReplicatedServices,
    config: RoutingConfig,
    stale: Stale,
    audit: Arc<Mutex<Audit>>,
}

/// What a node's map starts from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stale {
    /// Nothing.
    No,
    /// A mix of stale configurations, and none for some shards.
    Mixed,
    /// The next member as the primary of every shard.
    MembersFirst,
}

#[derive(Default)]
struct Audit {
    /// Requests served by something other than the current primary.
    wrong: Vec<String>,
    /// Requests served, in all.
    served: u64,
    /// Every gateway of every life, for its counts.
    gateways: Vec<RoutingShards>,
}

impl fmt::Debug for RoutedServices {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RoutedServices")
            .field("replicated", &self.replicated)
            .field("stale", &self.stale)
            .finish_non_exhaustive()
    }
}

impl RoutedServices {
    /// Routing over `replicated`, with gateways that time out requests
    /// after `config`, starting from stale maps.
    #[must_use]
    pub fn new(replicated: ReplicatedServices, config: RoutingConfig) -> Self {
        Self {
            replicated,
            config,
            stale: Stale::Mixed,
            audit: Arc::default(),
        }
    }

    /// The same services with empty maps to start from.
    #[must_use]
    pub fn fresh(self) -> Self {
        Self {
            stale: Stale::No,
            ..self
        }
    }

    /// The same services with maps that name a member as the primary of
    /// every shard, one epoch back.
    #[must_use]
    pub fn members_first(self) -> Self {
        Self {
            stale: Stale::MembersFirst,
            ..self
        }
    }

    /// The replication services underneath.
    #[must_use]
    pub fn replicated(&self) -> &ReplicatedServices {
        &self.replicated
    }

    /// Checks that every request a gateway accepted was served by the
    /// primary of its shard's current configuration, in that
    /// configuration's epoch or, while a removal's compare-and-swap has
    /// landed unseen, an earlier one since the placement's: removals keep
    /// the primary.
    ///
    /// # Errors
    ///
    /// The first request another replica served.
    pub fn check_served(&self) -> Result<(), String> {
        match self.audit().wrong.first() {
            Some(wrong) => Err(wrong.clone()),
            None => Ok(()),
        }
    }

    /// How many requests replicas served, and how every gateway of every
    /// life routed its calls, summed.
    #[must_use]
    pub fn stats(&self) -> (u64, RoutingStats) {
        let audit = self.audit();
        let stats = audit.gateways.iter().map(RoutedShards::stats).fold(
            RoutingStats::default(),
            |sum, one| RoutingStats {
                forwarded: sum.forwarded + one.forwarded,
                redirects: sum.redirects + one.redirects,
                register_reads: sum.register_reads + one.register_reads,
            },
        );
        (audit.served, stats)
    }

    fn audit(&self) -> MutexGuard<'_, Audit> {
        self.audit.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The stale configurations a node's map starts from.
    fn stale_maps(&self, placement: &BTreeMap<ShardRef, ShardConfig>) -> Vec<ShardConfig> {
        let mut stale = Vec::new();
        if self.stale == Stale::No {
            return stale;
        }
        for (n, config) in placement.values().enumerate() {
            let Some(older) = config.epoch.get().checked_sub(1).filter(|e| *e > 0) else {
                continue;
            };
            let mut old = config.clone();
            old.epoch = Epoch::new(older);
            let kind = if self.stale == Stale::MembersFirst {
                0
            } else {
                n % 3
            };
            match kind {
                0 => {
                    old.members.rotate_left(1);
                    old.primary = old.members[0].clone();
                }
                1 => {
                    let ghost: NodeId = "node-gone".parse().expect("a valid node ID");
                    old.members = vec![ghost.clone()];
                    old.primary = ghost;
                    old.replicas = 1;
                    old.min_write_replicas = 1;
                }
                _ => continue,
            }
            stale.push(old);
        }
        stale
    }
}

impl NodeServices for RoutedServices {
    type Shards = RoutingShards;

    fn ready(&self) -> bool {
        self.replicated.ready()
    }

    async fn start(&self, env: NodeEnv) -> Result<RoutingShards, BoxError> {
        let (local, listener) = self.replicated.start_replicas(&env).await?;
        let map = ShardMap::load(Arc::clone(&env.index), BlockingPool::inline("routes")).await?;
        for config in self.stale_maps(&env.placement) {
            map.learn(config).await;
        }
        let replication = local.replication.clone();
        let (audit, placement, node, registers) = (
            Arc::clone(&self.audit),
            Arc::clone(&env.placement),
            env.node.clone(),
            local.clone(),
        );
        let shards = RoutedShards::new(
            env.node,
            local,
            env.shards.set().clone(),
            map,
            env.transport,
            env.peers,
            env.control,
        )
        .with_config(self.config, skys3_control::RetryPolicy::default())
        .with_observer(move |served| {
            let mut audit = audit.lock().unwrap_or_else(PoisonError::into_inner);
            audit.served += 1;
            let placed = &placement[&served.shard];
            let current = registers
                .current(&served.shard)
                .unwrap_or_else(|| placed.clone());
            if served.node != current.primary
                || served.epoch > current.epoch
                || served.epoch < placed.epoch
            {
                audit.wrong.push(format!(
                    "{node}'s gateway was served on shard {} by {} in epoch {}, but its primary \
                     is {} in epoch {}",
                    served.shard, served.node, served.epoch, current.primary, current.epoch
                ));
            }
        });
        self.audit().gateways.push(shards.clone());
        let server = shards.server().clone();
        tokio::spawn(async move { serve_peers(listener, replication, server).await });
        Ok(shards)
    }
}
