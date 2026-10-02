//! Cluster simulation scenarios, run by CI's simulation job with a larger
//! fixed seed set and nightly with random seeds and longer runs.
//!
//! - [`crash`]: power losses at every sync boundary of a node under the
//!   M1 workload, with a `write_back` bucket (plan M1-14).
//! - [`workload`]: the M1 workload on a cluster of real nodes, without
//!   faults, under random faults, and through a whole-cluster restart
//!   while the control store is unreachable.
//! - [`harness`]: the harness itself: replay of a seed, the checkers
//!   catching seeded bugs, invariants, and node services.
//! - [`replication`]: shards with three members, under crashes, power
//!   loss, partitions, and message loss.
//! - [`leases`]: reads under leases, with partitions and clock drift within
//!   and beyond the bound `ρ`.
//! - [`acks`]: writes that time out while a member is cut off, in both
//!   acknowledgement timeout modes.
//! - [`coordinator`]: the coordinator lease, with a node that wrongly
//!   believes it is coordinator and with a coordinator cut off from the
//!   control store, and pushes of every change.
//! - [`removal`]: primaries removing a member that stops responding, in
//!   both modes, without the control store, and across a restart.
//! - [`routing`]: gateways on every node with stale shard maps, routing
//!   each request to its primary under crashes and partitions.
//! - [`takeover`]: members taking over from a primary that crashed, with
//!   competing candidates, partitions, crashes during reconciliation, and
//!   a single survivor.
//! - [`handoff`]: primaries handing their shards off to members while
//!   gateways with stale shard maps read through them, with lost
//!   step-downs, crashes, and drift.
//! - [`learners`]: learners added to every shard and promoted to member
//!   while writes go on, under slow and faulty control stores, crashes,
//!   and partitions, with rule R3 checked after every step.
//! - [`backfill`]: learners that get a snapshot or keep a verified log,
//!   backfill their payload, and are promoted, with the durability windows
//!   of a member loss measured.
//! - [`replacement`]: the coordinator replacing every member a lost node
//!   held, and forgetting the node once no shard names it.
//! - [`rebalancing`]: nodes that join a loaded cluster receive their share
//!   of shards and primaries, with writes waiting only for handoffs.
//! - [`registry`]: nodes that join with nothing but their credentials, and
//!   a coordinator that forgets a silent node only once no shard names it.
//! - [`restart`]: a whole-cluster restart while the control store is
//!   unreachable, with shards whose membership changed while a node was
//!   down: unchanged shards serve again, stale configurations stay fenced.
//! - [`rebuild`]: the drill of a lost control store: the nodes serve on
//!   from their local copies, an operator rebuilds the store from their
//!   exports, and membership changes resume on it.
//! - [`buckets`]: buckets created through a gateway, with their shards
//!   placed on the registered nodes, serving reads and writes through
//!   every node.
//! - [`cache`]: replicated `write_back` buckets under a clean cache smaller
//!   than the workload: copies dropped beyond `clean_copies`, LRU
//!   eviction, fills of what was evicted, and no dirty byte lost under
//!   crashes.
//! - [`reads`]: `GET`s through any node read their bytes from the holders
//!   their read plans name while overwrites and evictions race them, and
//!   fail mid-stream when their registration lapses.

mod acks;
mod backfill;
mod buckets;
mod cache;
mod coordinator;
mod crash;
mod handoff;
mod harness;
mod learners;
mod leases;
mod reads;
mod rebalancing;
mod rebuild;
mod registry;
mod removal;
mod replacement;
mod replication;
mod restart;
mod routing;
mod takeover;
mod workload;

/// What one seed of a cluster scenario costs, in seeds of a typical
/// scenario: CI's fixed seed set runs `SKYS3_SIM_SEEDS / COST` seeds of
/// each (`Runner::with_cost`), and the random-fault scenario twice that.
const COST: u64 = 32;
