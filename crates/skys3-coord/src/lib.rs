#![forbid(unsafe_code)]
//! The SkyS3 coordinator (design §6.7): the node that makes placement
//! decisions, elected by a lease in the control store, and the path every
//! one of its changes takes to the control store and to the nodes
//! (design §6.2).
//!
//! Section numbers (§) refer to the [SkyS3 design].
//!
//! [SkyS3 design]: https://github.com/skys3/skys3/blob/main/docs/skys3-design.md
//!
//! - [`Elector`]: contends for `coordinator.lease` and renews it every
//!   `coordinator_lease / 3` with `If-Match`. A candidate takes over only
//!   after observing the same lease version for longer than
//!   `coordinator_lease × (1+ρ)` on its own monotonic clock. Its
//!   [`Leadership`] says whether the node may act as coordinator.
//! - [`ChangeSet`] and [`apply`]: a change is a sequence of
//!   compare-and-swaps, and every applied change increments the
//!   generation in `cluster.json`. Two nodes that both believe they are
//!   coordinator can only compete: each write is conditional on the
//!   version its planner read.
//! - [`Pusher`] and [`ControlHints`]: the generation that announces a
//!   change is pushed to every node over the intra-cluster transport as a
//!   [`ControlChanged`] frame. Nodes that miss a push learn the generation
//!   from their change streams (a native watch or polling).
//! - [`Coordinator`]: the work loop that, while the node holds the lease,
//!   asks a [`Placement`] what to change. Placement itself is the work of
//!   later tasks; [`NoPlacement`] changes nothing.
//! - [`register`] and [`Heartbeater`]: every node registers itself in
//!   `nodes/<node-id>.json` when it starts and sends the coordinator
//!   heartbeats, so a node with valid credentials joins with no other
//!   action.
//! - [`NodeRegistry`] and [`Lifecycle`]: the coordinator's view of the
//!   nodes, with health from heartbeats as advice only. It keeps the
//!   [`Pusher`]'s nodes current, and forgets a node silent for
//!   `node_forget_after` once its [`Rehoming`] says no shard names it.
//! - [`Topology`]: the placement engine, a pure function from the nodes,
//!   their labels and capacity, and a bucket's policy to shard members,
//!   never two in one domain at the `failure_domain` level. A policy the
//!   cluster cannot satisfy is [`Unsatisfiable`].
//! - [`GeometryPolicy`] and [`FragmentPlanner`]: the geometry of new
//!   erasure-coded stripes, the widest the counts of eligible nodes and
//!   domains allow, and the node of each fragment, at most one per node
//!   and `m` per domain (§8.3). A [`StripePlan`] becomes the stripe layout
//!   an `EC_PUBLISH` record holds, which is never recomputed.
//! - [`PolicyWatch`] and [`report`]: the coordinator judges which buckets
//!   the cluster does not satisfy now, and publishes a [`PolicyReport`]
//!   through [`PlacementHealth`] for cluster health.
//! - [`create_bucket`] and [`BucketShards`]: a gateway creates a bucket's
//!   register and its placed shard registers in one change, and the
//!   coordinator finishes a creation cut short and drops the shard
//!   registers of deleted buckets.
//! - [`Replacement`]: the coordinator adds learners to the shards with
//!   fewer than `replicas` members that placement counts, on nodes
//!   placement chooses, for their primaries to backfill and promote, and
//!   removes the members placement no longer counts once they are
//!   replaced, at a pace [`ReplacementConfig`] bounds.
//! - [`Rebalancing`]: the coordinator moves shard members and primaries so
//!   that every live node holds its share, a newly joined one included:
//!   shards by add-learner, promote, and remove steps, primaries by a
//!   planned handoff it asks the primary for ([`Handoff`],
//!   [`HandoffClient`]), at a pace [`RebalanceConfig`] bounds.
//! - [`AdminEndpoint`]: serves pushes, heartbeats, and handoff requests on
//!   a node, the last through a [`HandoffSink`].
//!
//! ```
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! use skys3_control::{MemoryControlStore, ProposalIds};
//! use skys3_coord::{Elector, LeaseConfig, Leadership};
//! use skys3_io::{Clock, MonotonicClock};
//!
//! # tokio::runtime::Builder::new_current_thread().enable_time().build()?.block_on(async {
//! let store = MemoryControlStore::new();
//! let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::new());
//! let config = LeaseConfig::new(Duration::from_secs(10), 0.01)?;
//! let mut elector = Elector::new(
//!     store,
//!     "node-1".parse()?,
//!     Arc::clone(&clock),
//!     config,
//!     ProposalIds::seeded(1),
//! );
//! // Nobody holds the lease yet, so the first round takes it.
//! let renew_at = elector.round().await;
//! assert!(elector.leadership().is_coordinator_at(clock.now()));
//! assert!(matches!(elector.leadership(), Leadership::Coordinator { .. }));
//! assert!(renew_at <= clock.now().saturating_add(config.renew_interval()));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # })?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod admin;
mod buckets;
mod change;
mod coordinator;
mod fragments;
mod handoff;
mod heartbeat;
mod join;
mod lease;
mod place;
mod policy;
mod push;
mod rebalance;
mod registry;
mod replace;
#[cfg(test)]
mod testing;

pub use admin::AdminEndpoint;
pub use buckets::{
    BucketShards, Creation, CreationError, create_bucket, first_config, registrations,
};
pub use change::{
    Applied, ChangeError, ChangeFailed, ChangeSet, Pending, Settled, Write, apply, settle,
};
pub use coordinator::{Coordinator, CoordinatorConfig, NoPlacement, Placement};
pub use fragments::{
    FragmentPlanner, GeometryPolicy, NoGeometry, StripePlan, StripeRequest, StripeRoom,
};
pub use handoff::{
    Handoff, HandoffAck, HandoffClient, HandoffError, HandoffFuture, HandoffSink, RequestHandoff,
};
pub use heartbeat::{
    Heartbeat, HeartbeatAck, HeartbeatConfig, HeartbeatError, HeartbeatStatus, Heartbeater,
};
pub use join::{NodeProfile, Registered, Registration, RegistrationError, register};
pub use lease::{Elector, Leadership, LeaseConfig, LeaseConfigError};
pub use place::{Candidate, Domain, NewShard, Placed, ShardRequest, Topology, Unsatisfiable};
pub use policy::{
    BucketPolicy, ClusterScan, CoLocated, PlacementHealth, PolicyReport, PolicyWatch, ShortShard,
    report,
};
pub use push::{
    Announce, ControlChanged, ControlHints, HintError, PUSH_IDLE_TIMEOUT, PushError, Pushed, Pusher,
};
pub use rebalance::{RebalanceConfig, Rebalancing};
pub use registry::{
    Lifecycle, NodeEntry, NodeRegistry, NodeState, PeerSink, RegistryConfig, Rehoming, ShardScan,
};
pub use replace::{Replacement, ReplacementConfig};
