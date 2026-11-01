//! [`ReplicaObserver`]: what the replicated services report as they act.

use skys3_gateway::ShardRef;
use skys3_io::Disk;
use skys3_shard::Shard;
use skys3_shard::replication::HandedOff;
use skys3_types::Epoch;

/// What a node's replicated services report as they act: the replicas
/// they open and find, and the handoffs they start. Every method does
/// nothing by default.
///
/// The calls come in the order the services act, from the task that acts,
/// and before it awaits anything more, so an observer sees each event at
/// the point in the node's history it happened. They must not block.
pub trait ReplicaObserver<D: Disk>: Send + Sync + 'static {
    /// [`ReplicatedShards`](crate::ReplicatedShards) opened `replica`, or
    /// brought the open one up to a newer configuration: through
    /// [`Shards::open`](skys3_gateway::Shards::open),
    /// [`ReplicatedShards::open_config`](crate::ReplicatedShards::open_config),
    /// or [`ReplicatedShards::resume_kept`](crate::ReplicatedShards::resume_kept).
    fn opened(&self, replica: &Shard<D>) {
        let _ = replica;
    }

    /// [`RegisterFollower`](crate::RegisterFollower) found `replica` open
    /// for a shard whose register names the node. The replica may be one
    /// the observer has not seen: a learner that installs a snapshot opens
    /// its replica again by itself (§6.7).
    fn found(&self, replica: &Shard<D>) {
        let _ = replica;
    }

    /// [`Handoffs`](crate::Handoffs) began a planned handoff of `shard`,
    /// whose primary steps down in `epoch`.
    fn handoff_began(&self, shard: &ShardRef, epoch: Epoch) {
        let _ = (shard, epoch);
    }

    /// The handoff of `shard` that [`ReplicaObserver::handoff_began`]
    /// reported ended with `result`.
    fn handoff_ended(&self, shard: &ShardRef, result: &Result<HandedOff, skys3_shard::ShardError>) {
        let _ = (shard, result);
    }
}

/// The observer of services that report to nobody.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Unobserved;

impl<D: Disk> ReplicaObserver<D> for Unobserved {}
