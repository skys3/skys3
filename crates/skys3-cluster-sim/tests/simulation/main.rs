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
//! - [`heal`]: the M3 exit criterion: a node lost, replaced, forgotten,
//!   and succeeded by a new one, a rack lost under `failure_domain =
//!   "rack"`, and a coordinator lost in the middle of a change, each
//!   healed with no operator action.
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
//! - [`coding`]: a primary encoding its objects while it, a member, or
//!   fragment holders crash or lose power at each step of an attempt, with
//!   every object readable from its replicas or `k` fragments of each
//!   stripe throughout, and seeded bugs of the publish steps caught.
//! - [`coded_reads`]: `GET`s of coded objects through every node's
//!   gateway, decoding stripes while fragment holders crash, lose their
//!   fragment disk, or corrupt what they send, with seeded read bugs
//!   caught.
//! - [`repair`]: holders of coded stripes lost for good or losing their
//!   fragment disk while reads go on, and every stripe made whole again
//!   within a bound, most damaged first and within the bandwidth cap,
//!   through crashes of the primary and the new holders, with seeded
//!   repair bugs caught.
//! - [`fragment_moves`]: nodes joining or drained while holders are lost
//!   and reads go on, with fragments moved after repairs, publish before
//!   retire, never leaving a stripe below `k` readable fragments or a
//!   failure domain over its cap, through crashes at each step of a move,
//!   with seeded move bugs caught.
//! - [`compaction`]: segment compaction on replicated shards under
//!   crashes, message loss, and power losses at sync boundaries, with
//!   every member's dirty bytes and every shard's latest `CONFIG` record
//!   kept.
//! - [`reads`]: `GET`s through any node read their bytes from the holders
//!   their read plans name while overwrites and evictions race them, and
//!   fail mid-stream when their registration lapses.
//! - [`hot_cache`]: every gateway serves `GET`s of hot objects from its
//!   hot cache, only for the version the read plan names, while
//!   overwrites, evictions, compaction, crashes, and message loss race
//!   them.
//! - [`lifecycle`]: lifecycle rules expiring objects and aborting
//!   uploads while primaries crash, lose power, and are taken over: each
//!   version is expired by exactly one committed `DELETE`.
//! - [`write_through`]: writes to a `write_through` bucket acknowledged
//!   only once the remote store holds them, so that losing every node
//!   after an acknowledgement loses none, under takeovers and remote
//!   faults.
//! - [`backup`]: `local` buckets backed up to the remote store: every
//!   committed change reaches the backup and no member drops a backed-up
//!   payload, and with `backup_ack = "write_through"` losing every node
//!   after an acknowledgement loses no write, under takeovers and remote
//!   faults, with seeded bugs caught.
//! - [`origin`]: `read_only` buckets over an origin that an out-of-band
//!   writer changes while clients read through every node: every answer is
//!   one the origin gave, as the reader's credentials see it, within the
//!   freshness mode's bound, and seeded bugs of revalidation and of the
//!   credential scope are caught.
//! - [`conflicts`]: an out-of-band writer at the remote store of a
//!   `write_back` bucket under takeovers and remote faults, with the final
//!   remote state audited against each conflict policy, held conflicts
//!   resolved by an operator, and seeded bugs caught.
//! - [`snapshots`]: index snapshots of `local` and `write_back` buckets
//!   under takeovers and faults, and the restore drill of a shard whose
//!   members are all lost: its lost-key report matches the clients'
//!   history, and seeded bugs of snapshots and restores are caught.
//! - [`drills`]: the restore drill of a shard whose member, with up to two
//!   other nodes, is lost while coded and replicated objects are written,
//!   retagged, overwritten, deleted, and repaired: coded objects, those
//!   written after the latest snapshot included, are re-indexed from
//!   fragment headers and read back, the lost-key report matches the
//!   history, and seeded bugs of re-indexing are caught.

mod acks;
mod backfill;
mod backup;
mod buckets;
mod cache;
mod coded_reads;
mod coding;
mod compaction;
mod conflicts;
mod coordinator;
mod crash;
mod drills;
mod fragment_moves;
mod handoff;
mod harness;
mod heal;
mod hot_cache;
mod learners;
mod leases;
mod lifecycle;
mod origin;
mod reads;
mod rebalancing;
mod rebuild;
mod registry;
mod removal;
mod repair;
mod replacement;
mod replication;
mod restart;
mod routing;
mod snapshots;
mod takeover;
mod workload;
mod write_through;

/// What one seed of a cluster scenario costs, in seeds of a typical
/// scenario: CI's fixed seed set runs `SKYS3_SIM_SEEDS / COST` seeds of
/// each (`Runner::with_cost`), and the random-fault scenario twice that.
const COST: u64 = 32;
