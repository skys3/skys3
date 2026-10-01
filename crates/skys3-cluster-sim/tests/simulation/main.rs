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
//! - [`removal`]: primaries removing a member that stops responding, in
//!   both modes, without the control store, and across a restart.

mod acks;
mod crash;
mod harness;
mod leases;
mod removal;
mod replication;
mod workload;

/// What one seed of a cluster scenario costs, in seeds of a typical
/// scenario: CI's fixed seed set runs `SKYS3_SIM_SEEDS / COST` seeds of
/// each (`Runner::with_cost`), and the random-fault scenario twice that.
const COST: u64 = 32;
