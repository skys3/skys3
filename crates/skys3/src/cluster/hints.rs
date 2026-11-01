//! The `ControlHints` receiver (design §6.2, §6.7): the admin messages
//! other nodes send this node, served by [`AdminEndpoint`].
//!
//! - A pushed generation (`ControlChanged`) is recorded in the node's
//!   [`ControlHints`], which wakes its sync of control state ahead of the
//!   next poll ([`Shared::follow_control_store`](crate::node::Shared)). A
//!   push is only a hint: the node reads `cluster.json` and the registers
//!   from the store, so a lost, late, or forged push costs at most a read.
//! - A heartbeat (`NodeHeartbeat`) is recorded in the node's registry and
//!   answered with whether this node is coordinator, as its elector
//!   decides ([`CoordinatorView`]); a node without `[cluster] coordinator`
//!   answers that it is not, and the sender looks the coordinator up
//!   again.
//! - A handoff request (`Handoff`) starts the handoff on the node's
//!   serving primary of the shard (`skys3_cluster::Handoffs`), only if it
//!   comes from the holder of `coordinator.lease` or from an `admin`
//!   certificate ([`CoordinatorOrAdmin`], threat T6). Any other node of
//!   the cluster is refused, its certificate notwithstanding.

use std::sync::Arc;

use skys3_cluster::Handoffs;
use skys3_coord::{AdminEndpoint, ControlHints, CoordinatorOrAdmin, HandoffSink};
use skys3_io::RealDisk;
use skys3_net::TokioNetwork;

use super::coordinator::CoordinatorView;
use crate::control::backend::Backend;

/// The endpoint that serves admin messages on the node's transport:
/// pushed generations go to `hints`, heartbeats to the registry of
/// `coordinator`, and the handoffs that the coordinator in `store` or an
/// operator tool asks for to `handoffs`.
pub(crate) fn endpoint(
    hints: ControlHints,
    coordinator: &CoordinatorView,
    handoffs: Handoffs<TokioNetwork, RealDisk>,
    store: Backend,
) -> AdminEndpoint {
    let authority = CoordinatorOrAdmin::new(store, Arc::clone(&coordinator.clock));
    let sink: Arc<dyn HandoffSink> = Arc::new(handoffs);
    AdminEndpoint::new(hints)
        .with_heartbeats(
            coordinator.registry.clone(),
            coordinator.leadership.clone(),
            Arc::clone(&coordinator.clock),
        )
        .with_handoffs_from(sink, Arc::new(authority))
}
