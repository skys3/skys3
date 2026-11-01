//! Replicated shards and routing (design §5, §6.3–§6.6, §9.2; plan
//! M8-05b): every node of a cluster serves the shards
//! whose registers name it as the cluster simulation's nodes do, through
//! the same library code.
//!
//! - **Replicas.** Each shard opens in the configuration its register
//!   holds, on the nodes it names ([`ReplicatedShards`]): writes on a
//!   primary commit once every member holds them durably, reads are served
//!   only under the members' leases, a primary removes a member that stays
//!   unresponsive, and members take over from a silent primary
//!   ([`Replication::with_removal`], [`Replication::with_takeover`]). A
//!   primary backfills and promotes the learners a register names.
//! - **Following the registers.** A node reopens its replicas from their
//!   registers or local copies when it starts, and then follows the shard
//!   registers ([`RegisterFollower`]) on every sync of its control-state
//!   copy and every `config_poll_interval`: that is how it opens the
//!   replicas of a new bucket, and learns that it was added as a learner.
//! - **Routing.** The gateway's shards are [`RoutedShards`]: each call
//!   goes to the shard's primary, in process or over the transport, and a
//!   GET's bytes come from a holder the read plan names. The transport
//!   serves the other nodes' replication links, leases, and forwarded
//!   requests ([`ClusterShards::replication`], [`ClusterShards::server`]).
//! - **Addresses.** Other nodes are reached at the addresses their
//!   registrations under `nodes/` name ([`Peers`]), read when the node
//!   starts and on every pass, re-reading only the registrations whose
//!   version changed.
//!
//! - **Deleted buckets.** The gateway that deletes a bucket drops its own
//!   node's replicas at once. Every other node drops its replicas of the
//!   bucket once a sync of its copy from the store finds the bucket
//!   register gone ([`ClusterShards::drop_deleted`]), and opens them no
//!   more, although the shard registers outlive the bucket until the
//!   coordinator deletes them (design §6.7).
//!
//! What a coordinator does runs on the node that holds the coordinator
//! lease ([`super::coordinator`]): adding learners,
//! re-admitting a returning node, finishing a bucket creation cut short,
//! dropping the shard registers of a deleted bucket, and asking for
//! handoffs. Replication itself needs none of it: placement happens when
//! a gateway creates a bucket (`skys3_coord::create_bucket`), and leases,
//! member removal, takeover, promotion, and backfill run on the shards'
//! own nodes.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use serde_json::{Value, json};
use skys3_cluster::{
    BoxFuture, ControlShardConfigs, Handoffs, RegisterFollower, ReplicatedShards, ShardConfigs,
};
use skys3_config::Config;
use skys3_control::{
    ControlError, ControlStore, KeyPrefix, ProposalIds, RegisterKind, RetryPolicy, TypedKey,
    Version, read_with_retries,
};
use skys3_gateway::routing::{ForwardServer, RoutedShards, RoutingConfig, ShardMap};
use skys3_gateway::{LocalShards, ShardRef, Shards};
use skys3_index::{Index, IndexError};
use skys3_io::{BlockingPool, Clock, MonotonicClock, RealDisk};
use skys3_net::{Credentials, Peers, TokioNetwork, Transport};
use skys3_obs::MetricsRegistry;
use skys3_shard::AckTimeout;
use skys3_shard::replication::{
    ControlRegisters, Replication, ReplicationConfig, ReplicationMetrics,
};
use skys3_types::{BucketDocument, BucketId, NodeId, NodeRegistration, ShardConfig};

use crate::admin::{ReportFuture, ShardsReport, shard_entry};
use crate::control::backend::Backend;

/// The node's replication service.
pub(crate) type NodeReplication = Replication<TokioNetwork, RealDisk>;
/// The node's replicas of the shards whose registers name it.
pub(crate) type Replicas = ReplicatedShards<TokioNetwork, RealDisk>;
/// The shards the gateway calls on a node of several.
pub(crate) type Routed = RoutedShards<Replicas, RealDisk, TokioNetwork, Backend>;
/// The node's end of the requests other gateways forward to it.
pub(crate) type Forwarded = ForwardServer<Replicas, RealDisk>;

/// How often the node reports its replication metrics.
const EXPOSURE_INTERVAL: Duration = Duration::from_secs(1);

/// The replication timing of `[replication]` (§5.2, §5.4, §6.4). Beacons,
/// link timeouts, reconnects, and the takeover delay keep the library's
/// defaults, which no key names.
pub(crate) fn replication_config(config: &skys3_config::ReplicationConfig) -> ReplicationConfig {
    let timeout = config.replica_ack_timeout();
    ReplicationConfig {
        lease_renew_interval: config.lease_renew_interval(),
        primary_lease: config.primary_lease(),
        primary_grace: config.primary_grace(),
        member_suspect_after: config.member_suspect_after(),
        ack_timeout: match config.replica_ack_timeout_mode {
            skys3_config::AckTimeoutMode::WaitThrough => AckTimeout::wait_through(timeout),
            skys3_config::AckTimeoutMode::FailFast => AckTimeout::fail_fast(timeout),
        },
        ..ReplicationConfig::default()
    }
}

/// The node registrations under `nodes/` as a node read them last, with
/// the version of each: the nodes erasure coding places fragments on and
/// releases them to (§8.3, §8.7). Clones share them.
pub(crate) type Registrations = Arc<Mutex<BTreeMap<NodeId, (Version, NodeRegistration)>>>;

/// The buckets a node saw deleted. Bucket IDs are never reused (§4.1), so
/// the set only grows, by the buckets deleted while the node runs.
type Deleted = Arc<Mutex<BTreeSet<BucketId>>>;

/// The shard registers, without those of the buckets the node saw
/// deleted: they outlive their bucket until the coordinator deletes them
/// (§6.7), and the node must not open its replicas of them again.
struct LiveShards<C> {
    registers: C,
    deleted: Deleted,
}

impl<C: ShardConfigs> ShardConfigs for LiveShards<C> {
    fn shards(&self) -> BoxFuture<'_, Result<BTreeSet<ShardRef>, ControlError>> {
        Box::pin(async move {
            let mut shards = self.registers.shards().await?;
            let deleted = self.deleted.lock().unwrap_or_else(PoisonError::into_inner);
            shards.retain(|shard| !deleted.contains(&shard.bucket));
            Ok(shards)
        })
    }

    fn config<'a>(
        &'a self,
        shard: &'a ShardRef,
    ) -> BoxFuture<'a, Result<Option<ShardConfig>, ControlError>> {
        self.registers.config(shard)
    }
}

/// The replicated shards of a node of a cluster,
/// the gateway's routed shards over them, and what keeps them following
/// the registers (see the [module](self) docs).
pub(crate) struct ClusterShards {
    transport: Transport<TokioNetwork>,
    replicas: Replicas,
    routed: Routed,
    follower: RegisterFollower<TokioNetwork, RealDisk>,
    peers: Peers,
    store: Backend,
    retry: RetryPolicy,
    /// Each registration read last, with its version.
    registrations: Registrations,
    /// The buckets this node saw deleted.
    deleted: Deleted,
}

impl std::fmt::Debug for ClusterShards {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterShards")
            .field("node", self.replicas.node_id())
            .field("peers", &self.peers)
            .finish_non_exhaustive()
    }
}

impl ClusterShards {
    /// The replicated shards of node `node`, whose replicas `local` holds,
    /// connecting to other nodes with `credentials`, changing shard
    /// registers in `store`, and keeping its shard map in `index`.
    ///
    /// # Errors
    ///
    /// An [`IndexError`] if the shard map cannot be read from the index.
    pub(crate) async fn new(
        config: &Config,
        node: NodeId,
        local: &LocalShards<RealDisk>,
        (index, pool): (Arc<Index>, BlockingPool),
        credentials: &Credentials,
        store: Backend,
    ) -> Result<Self, IndexError> {
        let transport = Transport::new(TokioNetwork, credentials);
        let peers = Peers::new();
        let retry = RetryPolicy::default();
        let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::new());
        let replication = Replication::new(
            node.clone(),
            local.set().clone(),
            transport.clone(),
            peers.clone(),
            clock,
            replication_config(config.replication()),
        )
        .with_removal(
            ControlRegisters::new(store.clone(), retry),
            ProposalIds::from_os_rng(),
        )
        .with_takeover();
        let replicas = ReplicatedShards::new(local.clone(), replication);
        let map = ShardMap::load(index, pool).await?;
        let routed = RoutedShards::new(
            node,
            replicas.clone(),
            local.set().clone(),
            map,
            transport.clone(),
            peers.clone(),
            store.clone(),
        )
        .with_config(RoutingConfig::default(), retry);
        let deleted = Deleted::default();
        let follower = replicas.follower(Arc::new(LiveShards {
            registers: ControlShardConfigs::new(store.clone(), retry),
            deleted: Arc::clone(&deleted),
        }));
        Ok(Self {
            transport,
            replicas,
            routed,
            follower,
            peers,
            store,
            retry,
            registrations: Registrations::default(),
            deleted,
        })
    }

    /// The node's intra-cluster transport, with its credentials.
    pub(crate) fn transport(&self) -> &Transport<TokioNetwork> {
        &self.transport
    }

    /// The shards the gateway calls.
    pub(crate) fn routed(&self) -> &Routed {
        &self.routed
    }

    /// The node's replication, which serves the links of other nodes'
    /// primaries.
    pub(crate) fn replication(&self) -> &NodeReplication {
        self.replicas.replication()
    }

    /// The addresses of the registered nodes, which the node's clients
    /// reach them at.
    pub(crate) fn peers(&self) -> &Peers {
        &self.peers
    }

    /// The node registrations as the node read them last, which every
    /// pass refreshes.
    pub(crate) fn registrations(&self) -> Registrations {
        Arc::clone(&self.registrations)
    }

    /// The node's end of forwarded requests.
    pub(crate) fn server(&self) -> &Forwarded {
        self.routed.server()
    }

    /// What starts the planned handoffs of the shards this node leads.
    pub(crate) fn handoffs(&self) -> Handoffs<TokioNetwork, RealDisk> {
        self.replicas.handoffs()
    }

    /// Reopens the replicas the node keeps a configuration of, as a node
    /// that starts does (§6.2), then opens those the registers name
    /// ([`ClusterShards::follow`]).
    ///
    /// # Errors
    ///
    /// An [`IndexError`] if the kept configurations cannot be listed.
    pub(crate) async fn resume(&self) -> Result<(), IndexError> {
        if let Err(error) = self.refresh_peers().await {
            tracing::warn!(%error, "cannot read the node registrations yet");
        }
        let resumed = self.replicas.resume_kept().await?;
        tracing::info!(
            replicas = resumed.len(),
            "resumed the replicas the node keeps"
        );
        self.follow().await;
        Ok(())
    }

    /// One pass: reads the registrations that changed, and opens or brings
    /// up to date every replica whose register names the node. Returns how
    /// many replicas it opened or brought up to date.
    pub(crate) async fn follow(&self) -> usize {
        if let Err(error) = self.refresh_peers().await {
            tracing::debug!(%error, "cannot read the node registrations");
        }
        self.follower.follow().await
    }

    /// Drops the replicas of `open`, those open before a sync of the node's
    /// copy began, whose bucket the copy's `buckets` no longer name, and
    /// opens them no more: the bucket was deleted through another node's
    /// gateway (§4.1). `keep` names the buckets without a register, which
    /// are never dropped. Only a copy read from the store itself, at a
    /// generation past the one before, may decide it, as at startup (§6.2):
    /// a bucket register is written before the shard registers that open
    /// a replica, so a listing that starts after the replica opened lists
    /// the bucket unless it was deleted, and bucket IDs are never reused.
    /// Returns how many replicas it dropped.
    pub(crate) async fn drop_deleted(
        &self,
        open: &[skys3_log::ShardRef],
        buckets: &[BucketDocument],
        keep: &BucketId,
    ) -> usize {
        let known: BTreeSet<&BucketId> = buckets
            .iter()
            .map(|bucket| &bucket.bucket_id)
            .chain([keep])
            .collect();
        let mut dropped = 0;
        for shard in open.iter().filter(|shard| !known.contains(&shard.bucket)) {
            self.deleted
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(shard.bucket.clone());
            let named = ShardRef {
                bucket: shard.bucket.clone(),
                shard: shard.shard,
            };
            match self.routed.remove(&named).await {
                Ok(()) => {
                    tracing::info!(%shard, "dropped the replica of a deleted bucket");
                    dropped += 1;
                }
                Err(error) => {
                    tracing::warn!(%shard, %error, "cannot drop the replica of a deleted bucket");
                }
            }
        }
        dropped
    }

    /// Passes every `interval`, starting one `interval` from now, until the
    /// task is dropped.
    pub(crate) async fn run(self: Arc<Self>, interval: Duration) {
        loop {
            tokio::time::sleep(interval).await;
            self.follow().await;
        }
    }

    /// Reports the shards this node leads that have fewer members than
    /// `replicas` in `registry` (§6.4), every second, until the task is
    /// dropped.
    pub(crate) fn report_exposure(
        &self,
        registry: &MetricsRegistry,
    ) -> impl Future<Output = ()> + Send + 'static {
        let metrics = ReplicationMetrics::register(registry);
        let replication = self.replication().clone();
        async move {
            replication
                .report_exposure(&metrics, EXPOSURE_INTERVAL)
                .await;
        }
    }

    /// Reads the node registrations under `nodes/` whose version changed
    /// since the last read, and gives replication and routing the
    /// addresses they name. Returns whether an address changed.
    ///
    /// # Errors
    ///
    /// The store's error; the addresses known stay as they were.
    pub(crate) async fn refresh_peers(&self) -> Result<bool, ControlError> {
        let listed = self.store.list(&KeyPrefix::nodes()).await?;
        let known = self
            .registrations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let mut read = BTreeMap::new();
        for (key, version) in listed {
            let RegisterKind::Node(node) = key.kind() else {
                continue;
            };
            if let Some((seen, registration)) = known.get(&node)
                && *seen == version
            {
                read.insert(node, (version, registration.clone()));
                continue;
            }
            match read_with_retries(&self.store, &TypedKey::node(&node), &self.retry).await {
                Ok(Some(current)) => {
                    read.insert(node, (current.version, current.value));
                }
                Ok(None) => {}
                Err(error @ ControlError::InvalidRegister { .. }) => {
                    tracing::warn!(%node, %error, "ignoring a node registration that does not parse");
                }
                Err(error) => return Err(error),
            }
        }
        let addresses = read
            .iter()
            .map(|(node, (_, registration))| (node.clone(), registration.address.clone()))
            .collect();
        *self
            .registrations
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = read;
        let changed = self.peers.replace(addresses);
        if changed {
            tracing::info!(peers = ?self.peers, "the nodes' addresses changed");
        }
        Ok(changed)
    }

    /// The answer to `GET /v1/shards` (see [`crate::admin`]): every shard
    /// of `buckets`, as this node's replica or its shard map knows it.
    async fn shards(&self, buckets: &[BucketDocument]) -> Value {
        let set = self.replication().set();
        let map = self.routed.map();
        let node = self.replicas.node_id();
        let mut shards = Vec::new();
        for bucket in buckets {
            for shard in ShardRef::all(bucket) {
                shards.push(shard_entry(bucket, &shard, set, map.get(&shard), node).await);
            }
        }
        let stats = self.routed.stats();
        json!({
            "node_id": node.as_str(),
            "shards": shards,
            "peers": self
                .peers
                .all()
                .iter()
                .map(|(node, address)| (node.as_str().to_owned(), json!(address.to_string())))
                .collect::<serde_json::Map<_, _>>(),
            "routing": {
                "forwarded": stats.forwarded,
                "redirects": stats.redirects,
                "register_reads": stats.register_reads,
            },
        })
    }
}

impl ShardsReport for ClusterShards {
    fn report<'a>(&'a self, buckets: &'a [BucketDocument]) -> ReportFuture<'a> {
        Box::pin(self.shards(buckets))
    }
}

#[cfg(test)]
mod tests {
    use skys3_shard::AckMode;

    use super::*;

    #[test]
    fn replication_timing_comes_from_the_configuration() {
        let mut config = skys3_config::ReplicationConfig {
            replica_ack_timeout_ms: 9000,
            member_suspect_after_ms: 2000,
            lease_renew_interval_ms: 500,
            primary_lease_ms: 3000,
            primary_grace_ms: 4000,
            ..skys3_config::ReplicationConfig::default()
        };
        let timing = replication_config(&config);
        assert_eq!(timing.ack_timeout.timeout, Duration::from_secs(9));
        assert_eq!(timing.ack_timeout.mode, AckMode::WaitThrough);
        assert_eq!(timing.member_suspect_after, Duration::from_secs(2));
        assert_eq!(timing.lease_renew_interval, Duration::from_millis(500));
        assert_eq!(timing.primary_lease, Duration::from_secs(3));
        assert_eq!(timing.primary_grace, Duration::from_secs(4));
        let defaults = ReplicationConfig::default();
        assert_eq!(timing.beacon_interval, defaults.beacon_interval);
        assert_eq!(timing.link_timeout, defaults.link_timeout);
        assert_eq!(timing.takeover_delay, defaults.takeover_delay);

        config.replica_ack_timeout_mode = skys3_config::AckTimeoutMode::FailFast;
        assert_eq!(
            replication_config(&config).ack_timeout.mode,
            AckMode::FailFast
        );
        // The defaults are the design's, which are the library's.
        let defaults = replication_config(&skys3_config::ReplicationConfig::default());
        assert_eq!(defaults, ReplicationConfig::default());
    }
}
