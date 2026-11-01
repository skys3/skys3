//! Replicated shards and routing (design §5, §6.3–§6.6, §9.2; plan
//! M8-05b): with `[cluster] multi_node = true`, a node serves the shards
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
//! What a coordinator does is not run here (plan M8-06): no node adds
//! learners, re-admits a returning node, finishes a bucket creation cut
//! short, drops the shard registers of a deleted bucket, or asks for a
//! handoff. Replication itself needs none of it: placement happens when a
//! gateway creates a bucket (`skys3_coord::create_bucket`), and leases,
//! member removal, takeover, promotion, and backfill run on the shards'
//! own nodes.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use serde_json::{Value, json};
use skys3_cluster::{ControlShardConfigs, RegisterFollower, ReplicatedShards};
use skys3_config::Config;
use skys3_control::{
    ControlError, ControlStore, KeyPrefix, ProposalIds, RegisterKind, RetryPolicy, TypedKey,
    Version, read_with_retries,
};
use skys3_gateway::routing::{ForwardServer, RoutedShards, RoutingConfig, ShardMap};
use skys3_gateway::{LocalShards, ShardRef};
use skys3_index::{Index, IndexError};
use skys3_io::{BlockingPool, Clock, MonotonicClock, RealDisk};
use skys3_net::{Credentials, Peers, TokioNetwork, Transport};
use skys3_obs::MetricsRegistry;
use skys3_shard::AckTimeout;
use skys3_shard::replication::{
    ControlRegisters, Replication, ReplicationConfig, ReplicationMetrics,
};
use skys3_types::{BucketDocument, NodeAddress, NodeId};

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

/// The replicated shards of a node with `[cluster] multi_node = true`,
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
    /// Each registration read last: its version and the address it names.
    registrations: Mutex<BTreeMap<NodeId, (Version, NodeAddress)>>,
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
        let follower = replicas.follower(Arc::new(ControlShardConfigs::new(store.clone(), retry)));
        Ok(Self {
            transport,
            replicas,
            routed,
            follower,
            peers,
            store,
            retry,
            registrations: Mutex::default(),
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

    /// The node's end of forwarded requests.
    pub(crate) fn server(&self) -> &Forwarded {
        self.routed.server()
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
            if let Some((seen, address)) = known.get(&node)
                && *seen == version
            {
                read.insert(node, (version, address.clone()));
                continue;
            }
            match read_with_retries(&self.store, &TypedKey::node(&node), &self.retry).await {
                Ok(Some(current)) => {
                    read.insert(node, (current.version, current.value.address));
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
            .map(|(node, (_, address))| (node.clone(), address.clone()))
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
