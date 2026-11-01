//! The `ControlHints` receiver (design §6.2, §6.7): the admin messages
//! other nodes send this node, served by [`AdminEndpoint`].
//!
//! - A pushed generation (`ControlChanged`) is recorded in the node's
//!   [`ControlHints`], which wakes its sync of control state ahead of the
//!   next poll ([`Shared::follow_control_store`](crate::node::Shared)). A
//!   push is only a hint: the node reads `cluster.json` and the registers
//!   from the store, so a lost, late, or forged push costs at most a read.
//! - A heartbeat (`NodeHeartbeat`) is answered from the node's
//!   [`NodeRegistry`], saying whether this node is coordinator. This build
//!   runs no elector, so it answers that it is not, and the sender looks
//!   the coordinator up again; plan M8-06 gives the endpoint the
//!   elector's leadership.
//! - A handoff request (`Handoff`) is refused. The node serves shards
//!   across nodes, and `skys3_cluster::Handoffs` could start the handoff,
//!   but any node of the cluster may send the message, and threat T6 asks
//!   that only the coordinator or an `admin` certificate may: plan M8-06
//!   gives the endpoint its sink once it can tell them apart.

use std::sync::Arc;

use skys3_config::Config;
use skys3_coord::{AdminEndpoint, ControlHints, Leadership, NodeRegistry, RegistryConfig};
use skys3_io::Clock;
use tokio::sync::watch;

/// The endpoint that serves admin messages on the node's transport,
/// recording pushed generations in `hints`.
pub(crate) fn endpoint(
    config: &Config,
    hints: ControlHints,
    clock: Arc<dyn Clock>,
) -> AdminEndpoint {
    let registry = NodeRegistry::new(Arc::clone(&clock), RegistryConfig::from_config(config));
    // No elector runs, so the node never coordinates.
    let (_, leadership) = watch::channel(Leadership::Follower { holder: None });
    AdminEndpoint::new(hints).with_heartbeats(registry, leadership, clock)
}
