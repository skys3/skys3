#![forbid(unsafe_code)]
//! The SkyS3 cluster simulation harness (design §16.1): several real nodes
//! in one deterministic `turmoil` simulation, driven by a client workload
//! under faults, with the history checked at the end.
//!
//! Section numbers (§) refer to the [SkyS3 design].
//!
//! [SkyS3 design]: https://github.com/skys3/skys3/blob/main/docs/skys3-design.md
//!
//! # What runs
//!
//! - **Nodes.** Each node is a simulated host that starts the way the node
//!   binary does, through the same functions: log recovery and index
//!   replay ([`skys3::storage::recover`]), the control store or the local
//!   copy of control state ([`skys3::control::open`]), the gateway, and
//!   every bucket's shards. It serves S3 over the simulated network, takes
//!   checkpoints, and flushes `write_back` buckets to the remote store
//!   with the flusher (`skys3-flush`). Its disks are `skys3-io` simulated
//!   disks with torn writes, its clock drifts within `ρ`, and each node
//!   has intra-cluster credentials from a throwaway PKI. See [`NodeEnv`].
//! - **Stores.** One simulated S3 bucket holds the control store (the S3
//!   backend), which every node reaches through its own fault injection;
//!   another, with its own faults, is the remote store for `write_back`
//!   buckets ([`ClusterConfig::write_back_buckets`]).
//! - **Clients.** The [`Workload`] of M1: concurrent `PUT`s, inline and as
//!   extents, conditional `PUT`s, one-part multipart uploads, `GET`, `HEAD`,
//!   and `DELETE`, each sent to the primary of its key's shard under the
//!   static placement the harness writes to the control store (`shards/`
//!   registers), or to any node, whose gateway routes it
//!   ([`Workload::any_gateway`]). With [`ClusterConfig::create_buckets`],
//!   a client first creates the buckets through the nodes' gateways, which
//!   place their shards on the registered nodes (plan M3-04).
//! - **Faults.** A [`FaultPlan`]: crashes with or without power loss,
//!   partitions, held links (delay and reordering), random message loss,
//!   failed syncs, control-store outages, control-store round trips of
//!   100 ms and more, lost control-store answers, and a lost control
//!   store, which an operator rebuilds from the nodes' exports (plan
//!   M3-07), plus a new clock
//!   drift for every life of a node, or a fixed one, even beyond `ρ`
//!   ([`ClusterConfig::node_drifts`]). Separately,
//!   [`Cluster::power_loss_at_sync`] cuts a node's power at one numbered
//!   sync of its disks, so a scenario can visit every sync boundary of a
//!   seed, one per run.
//!
//! # What is checked
//!
//! After the workload the driver heals every fault, restarts every node,
//! and reads every key back from its primary. Then every node loses power
//! and is recovered outside the simulation. The history must be
//! linearizable per key ([`skys3_sim::check::check_linearizable`]), and
//! every acknowledged write must be flushed, present on a surviving member
//! of its shard (in a `write_back` bucket, one whose entry is still
//! dirty), or reported lost ([`skys3_sim::check::check_durable`]).
//! Clients record an operation once a node accepted its connection, and
//! a node's crash ends the operations it had not answered
//! ([`skys3_sim::history::History::crashed`]), so a write cut off by a
//! crash cannot resurface over a later acknowledged one unnoticed.
//! [`Invariant`]s run after every step.
//!
//! # Extending it
//!
//! Protocols plug in as [`NodeServices`]: started in each life of each
//! node with its recovered storage, clock, transport, and the placement,
//! they return the shards the gateway calls. [`ReplicatedServices`] is
//! replication (plan M2-07), with learners and their promotion
//! ([`ReplicatedServices::with_learners`], plan M2-14), [`RoutedServices`] adds gateways that route
//! by their shard maps (plan M2-08), and later protocols extend them.
//! [`CoordinatedServices`] wraps any services with the coordinator lease
//! and change propagation (plan M3-01). Their failure
//! cases become seeded scenarios: a
//! [`FaultPlan`] built by hand or drawn from a [`FaultProfile`], and
//! invariants over the state the services expose. Scenario tests live in
//! this crate's `simulation` test target, which CI runs with a fixed seed
//! set and the nightly job with random seeds and longer runs.
//!
//! ```no_run
//! use skys3_cluster_sim::{Cluster, ClusterConfig, FaultPlan, FaultProfile, Workload};
//! use skys3_sim::Runner;
//!
//! Runner::new().run(|context| {
//!     let config = ClusterConfig::default();
//!     let workload = Workload::default();
//!     let plan = FaultPlan::random(context.rng(), &FaultProfile::default(), 3, 2, 4);
//!     let report = Cluster::new(config).run(context, &workload, &plan)?;
//!     assert!(!report.history.is_empty());
//!     Ok(())
//! });
//! ```

mod backup;
mod cluster;
pub mod coding;
mod conflicts;
mod coordination;
mod creation;
mod faults;
mod holders;
mod learners;
mod lifecycle;
mod node;
mod origin;
mod pki;
mod replication;
mod routing;
mod s3;
mod snapshots;
mod workload;
mod write_through;

pub use backup::Backup;
pub use cluster::{
    Cluster, ClusterConfig, HotCaches, Invariant, ReadRegistration, Rebuild, Report, RunError,
    View, WriteTiming,
};
pub use conflicts::{ConflictAudit, Conflicts};
pub use coordination::{
    Change, ChangeStage, CoordinatedServices, CoordinationConfig, CoordinatorLoss, Forgotten,
    LostCoordinator, PUSH_PORT, PushDelays, RegistryView,
};
pub use faults::{Endpoint, Fault, FaultPlan, FaultProfile, ScheduledFault};
pub use holders::{HolderCounts, HolderFaults};
pub use learners::{DurabilityWindows, LearnerCounts};
pub use lifecycle::{Lifecycle, LifecycleAudit, LifecycleObject, LifecycleStart, ORIGIN_DAY};
pub use node::{BoxError, ControlHandle, LocalServices, NodeEnv, NodeServices, TRANSPORT_PORT};
pub use origin::{FULL, GUEST, Origin, OriginAudit, OriginBug, OriginFreshness};
pub use replication::{
    HandoffCounts, HandoffTime, IoCounts, LateWrites, LeaseCounts, ReplicaState,
    ReplicatedServices, ReplicatedShards,
};
pub use routing::{RoutedServices, RoutingShards};
pub use s3::S3_PORT;
/// The seeded bugs of conflict handling ([`Conflicts::bug`]).
pub use skys3_flush::test_hooks::ConflictBug;
/// The seeded bugs of index snapshots ([`Snapshots::bug`]).
pub use skys3_flush::test_hooks::SnapshotBug;
pub use snapshots::{SnapshotAudit, Snapshots};
pub use workload::Workload;
pub use write_through::WriteThrough;

/// The control-store fault rates of [`ClusterConfig::control_rates`].
pub use skys3_control::faults::FaultRates;
/// The drift bound and disk faults of [`ClusterConfig`], and where a power
/// loss falls at a sync ([`Cluster::power_loss_at_sync`]).
pub use skys3_io::{Drift, SimDiskFaults, SyncCut};
