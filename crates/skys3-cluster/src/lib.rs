#![forbid(unsafe_code)]
//! SkyS3 replicated node services (design §5, §6.3–§6.6): what a node of
//! a multi-node cluster runs between its gateway and its replication.
//!
//! Section numbers (§) refer to the [SkyS3 design].
//!
//! [SkyS3 design]: https://github.com/skys3/skys3/blob/main/docs/skys3-design.md
//!
//! - [`ReplicatedShards`]: the gateway's [`Shards`](skys3_gateway::Shards)
//!   over a node's [`Replication`](skys3_shard::replication::Replication).
//!   A shard opens in the configuration its register holds, never in one
//!   the gateway makes up, and only on a node the configuration names
//!   ([`Opening`]); every other call goes to the node's replica, which
//!   serves it only as the shard's primary (§5.1). [`ReplicatedShards::resume_kept`]
//!   reopens a restarted node's replicas from their registers or, while a
//!   register cannot be read, from the node's local copy (§6.2).
//! - [`RegisterFollower`]: follows the shard registers ([`ShardConfigs`],
//!   in a control store [`ControlShardConfigs`]), opening the replica of
//!   every shard whose register names the node as member or learner, and
//!   bringing an open one up to a newer configuration (§6.3, §6.7).
//! - [`Handoffs`]: the [`HandoffSink`](skys3_coord::HandoffSink) that
//!   starts the planned handoffs a coordinator asks a node for (§5.4,
//!   §6.7), only on the node's serving primary of the shard, in the epoch
//!   the request names, to another member.
//! - [`ReplicaObserver`]: what these report as they act, for metrics and
//!   for the audits of the cluster simulation.
//!
//! Everything is generic over the disk ([`Disk`](skys3_io::Disk)) and the
//! network ([`Network`](skys3_net::Network)), so the node binary runs it
//! on real disks and sockets and the cluster simulation on simulated ones.
//! Time is the Tokio runtime's, and nothing here draws randomness, so a
//! simulation stays deterministic.

mod follow;
mod handoff;
mod observe;
mod shards;
#[cfg(test)]
mod tests;

pub use follow::{ControlShardConfigs, RegisterFollower, ShardConfigs};
pub use handoff::Handoffs;
pub use observe::ReplicaObserver;
pub use shards::{Opening, ReplicatedShards};
/// The boxed future [`ShardConfigs`] returns.
pub use skys3_shard::replication::BoxFuture;
