//! The coordinator (design §6.7; plan M8-06): with `[cluster] coordinator
//! = true`, the node contends for `coordinator.lease` through an
//! [`Elector`], and while it holds the lease, its [`Coordinator`] plans
//! and applies every placement change and its [`Pusher`] pushes the
//! generation that announces each one to the nodes the registry lists.
//!
//! Placement is the composition the cluster simulation runs, innermost
//! last, each planning only in rounds the one around it leaves alone:
//!
//! - [`Lifecycle`]: lists `nodes/` into the [`NodeRegistry`], hands the
//!   nodes' addresses to the pusher, and forgets nodes silent for
//!   `node_forget_after` once no shard names them;
//! - [`PolicyWatch`]: judges every bucket's placement policy and publishes
//!   the judgement ([`PlacementHealth`]) for the admin API and the
//!   coordinator's metrics;
//! - [`BucketShards`]: finishes bucket creations cut short and deletes the
//!   shard registers of deleted buckets;
//! - [`Replacement`]: adds learners for lost members, the node a takeover
//!   left out included when it is the only candidate, which re-admits it,
//!   and removes the members placement no longer counts once replaced;
//! - [`Rebalancing`]: moves shards onto nodes below their share, a node
//!   that joins included, empties drained nodes, and asks primaries for
//!   handoffs over the transport ([`HandoffClient`]).
//!
//! A node without the flag runs none of this and never contends: it only
//! follows whichever node holds the lease (its heartbeats go there), so
//! the gate keeps the coordinator unreachable until an operator turns it
//! on, and a cluster where no node has it runs as plan M8-05b left it.
//! The registry also answers the heartbeats other nodes send this node
//! ([`hints`](super::hints)), and the elector's leadership tells them
//! whether it is coordinator.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use skys3_config::{Config, ReplicationConfig};
use skys3_control::{ProposalIds, RetryPolicy};
use skys3_coord::{
    BucketShards, ControlHints, Coordinator, CoordinatorConfig, CoordinatorMetrics, Elector,
    HandoffClient, Leadership, LeaseConfig, Lifecycle, NodeRegistry, PlacementHealth, PolicyWatch,
    Pusher, RebalanceConfig, Rebalancing, RegistryConfig, Replacement, ReplacementConfig,
};
use skys3_io::Clock;
use skys3_net::{TokioNetwork, Transport};
use skys3_obs::MetricsRegistry;
use skys3_types::NodeId;
use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::control::backend::Backend;
use crate::node::StartError;

/// How long a push of a new generation may take to reach a node.
const PUSH_TIMEOUT: Duration = Duration::from_secs(2);

/// How long a primary may take to answer a handoff request.
pub(crate) const HANDOFF_TIMEOUT: Duration = Duration::from_secs(5);

/// The coordinator's placement: the lifecycle, around the policy
/// judgement, around bucket and shard creation, around replacement, around
/// rebalancing (see the [module](self) docs).
type NodePlacement =
    Lifecycle<PolicyWatch<BucketShards<Replacement<Rebalancing<HandoffClient<TokioNetwork>>>>>>;

/// The pace of replacement and rebalancing that `[replication]` sets
/// (§6.7): the removal interval and the rebalancing keys, with the
/// library's bounds on concurrent learners and on the registers one change
/// writes.
fn pace(config: &ReplicationConfig) -> (ReplacementConfig, RebalanceConfig) {
    let replacement = ReplacementConfig {
        removal_interval: config.removal_interval(),
        ..ReplacementConfig::default()
    };
    let rebalancing = RebalanceConfig {
        max_moves: usize::try_from(config.rebalance_max_moves).unwrap_or(usize::MAX),
        handoff_interval: config.rebalance_handoff_interval(),
        handoff_retry: config.rebalance_handoff_retry(),
    };
    (replacement, rebalancing)
}

/// What the node's other services share with its coordinator: the
/// registry heartbeats are recorded in, whether the node is coordinator,
/// and the latest placement judgement.
#[derive(Debug, Clone)]
pub(crate) struct CoordinatorView {
    /// Whether the node contends for the lease (`[cluster] coordinator`).
    pub(crate) candidate: bool,
    /// The registry heartbeats are recorded in, which the coordinator
    /// refreshes from `nodes/` while it holds the lease.
    pub(crate) registry: NodeRegistry,
    /// Whether this node is coordinator, as its elector decides; always a
    /// follower without one.
    pub(crate) leadership: watch::Receiver<Leadership>,
    /// The latest placement judgement of this node's tenure.
    pub(crate) placement: PlacementHealth,
    /// The node's monotonic clock, which the leadership is timed on.
    pub(crate) clock: Arc<dyn Clock>,
}

impl CoordinatorView {
    /// Whether this node serves a tenure as coordinator now.
    pub(crate) fn is_coordinator(&self) -> bool {
        self.leadership.borrow().is_coordinator_at(self.clock.now())
    }
}

/// The elector and the coordinator of a node with `[cluster] coordinator =
/// true`, ready to run ([`NodeCoordinator::spawn`]).
pub(crate) struct NodeCoordinator {
    elector: Elector<Backend>,
    coordinator: Coordinator<Backend, NodePlacement, Pusher<TokioNetwork>>,
}

impl std::fmt::Debug for NodeCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeCoordinator")
            .field("coordinator", &self.coordinator)
            .finish_non_exhaustive()
    }
}

/// The coordinator services of node `node`: with `[cluster] coordinator =
/// true`, an elector and a coordinator over `store` (the control store's
/// own client) that reach other nodes through `transport` and record
/// pushes to this node in `hints`, with their metrics in `metrics`; a view
/// of them either way.
///
/// # Errors
///
/// [`StartError::Cluster`] for lease timings the elector cannot use, which
/// configuration loading already refuses.
pub(crate) fn start(
    config: &Config,
    node: &NodeId,
    store: &Backend,
    transport: &Transport<TokioNetwork>,
    hints: &ControlHints,
    clock: &Arc<dyn Clock>,
    metrics: &MetricsRegistry,
) -> Result<(Option<NodeCoordinator>, CoordinatorView), StartError> {
    let registry = NodeRegistry::new(Arc::clone(clock), RegistryConfig::from_config(config));
    let placement = PlacementHealth::new();
    if !config.cluster().coordinator {
        // Never coordinator: no elector decides otherwise.
        let (_, leadership) = watch::channel(Leadership::Follower { holder: None });
        let view = CoordinatorView {
            candidate: false,
            registry,
            leadership,
            placement,
            clock: Arc::clone(clock),
        };
        return Ok((None, view));
    }
    let lease = LeaseConfig::from_config(config)
        .map_err(|error| StartError::Cluster(format!("the coordinator lease: {error}")))?;
    let elector = Elector::new(
        store.clone(),
        node.clone(),
        Arc::clone(clock),
        lease,
        ProposalIds::from_os_rng(),
    );
    let leadership = elector.subscribe();
    let level = config.cluster().failure_domain;
    let coordinator_metrics = CoordinatorMetrics::register(metrics);
    let handoffs = HandoffClient::new(transport.clone(), HANDOFF_TIMEOUT);
    let (replacing, rebalancing) = pace(config.replication());
    let rebalancing = Rebalancing::new(
        registry.clone(),
        level,
        Arc::clone(clock),
        rebalancing,
        handoffs,
    );
    let replacement = Replacement::new(registry.clone(), level, Arc::clone(clock), replacing)
        .with_placement(rebalancing);
    let watch = PolicyWatch::new(registry.clone(), level)
        .with_health(placement.clone())
        .with_metrics(coordinator_metrics.clone())
        .with_placement(BucketShards::new(registry.clone(), level).with_placement(replacement));
    // Pushes go to the nodes the registry lists, at their transport
    // addresses, which serve admin messages too.
    let pusher =
        Pusher::new(transport.clone(), BTreeMap::new(), PUSH_TIMEOUT).with_local(hints.clone());
    let lifecycle = Lifecycle::new(registry.clone(), watch).with_peers(pusher.clone());
    let coordinator = Coordinator::new(
        store.clone(),
        Arc::clone(clock),
        leadership.clone(),
        lifecycle,
        pusher,
        CoordinatorConfig {
            cluster: config.cluster().cluster_id.clone(),
            retry: RetryPolicy::default(),
            // A placement round at least every poll interval, and at once
            // when a node the registry does not list sends a heartbeat.
            idle: config.control_store().config_poll_interval(),
        },
        ProposalIds::from_os_rng(),
    )
    .with_metrics(coordinator_metrics);
    registry.set_waker(coordinator.waker());
    let view = CoordinatorView {
        candidate: true,
        registry,
        leadership,
        placement,
        clock: Arc::clone(clock),
    };
    Ok((
        Some(NodeCoordinator {
            elector,
            coordinator,
        }),
        view,
    ))
}

impl NodeCoordinator {
    /// Contends for the lease, and coordinates while it holds it, for as
    /// long as the node's background tasks run.
    pub(crate) fn spawn(self, background: &mut JoinSet<()>) {
        let Self {
            elector,
            coordinator,
        } = self;
        tracing::info!("contending for the coordinator lease");
        background.spawn(elector.run());
        background.spawn(coordinator.run());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_pace_comes_from_the_configuration() {
        let config = ReplicationConfig {
            removal_interval_seconds: 3,
            rebalance_max_moves: 7,
            rebalance_handoff_interval_seconds: 4,
            rebalance_handoff_retry_seconds: 30,
            ..ReplicationConfig::default()
        };
        let (replacing, rebalancing) = pace(&config);
        assert_eq!(replacing.removal_interval, Duration::from_secs(3));
        assert_eq!(
            replacing.max_learners,
            ReplacementConfig::default().max_learners
        );
        assert_eq!(replacing.batch, ReplacementConfig::default().batch);
        assert_eq!(rebalancing.max_moves, 7);
        assert_eq!(rebalancing.handoff_interval, Duration::from_secs(4));
        assert_eq!(rebalancing.handoff_retry, Duration::from_secs(30));
        // The defaults are the design's, which are the library's.
        let (replacing, rebalancing) = pace(&ReplicationConfig::default());
        assert_eq!(replacing, ReplacementConfig::default());
        assert_eq!(rebalancing, RebalanceConfig::default());
    }
}
