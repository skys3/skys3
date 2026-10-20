//! Node registration and heartbeats (design §6.7): the node registers
//! itself in `nodes/<node-id>.json` when it starts, and then sends the
//! coordinator heartbeats, through a [`Heartbeater`].
//!
//! The registration describes the node as its configuration and data
//! directory do: its ID, the address other nodes reach its transport at
//! (`[transport] advertise`), its `zone` and `rack` labels, its disks with
//! their capacity, and the instance ID of its data directory. A node does
//! not start while the register holds its node ID for another data
//! directory ([`RegistrationError::Claimed`]), which is what is left of the
//! single-node guard of plan M8-03 once nodes share a store. Any other
//! failure to register, such as a store that does not answer, does not
//! stop the start: the node runs from its copy of control state, and the
//! heartbeater keeps trying.

use std::path::PathBuf;
use std::sync::Arc;

use skys3_config::Config;
use skys3_control::ProposalIds;
use skys3_coord::{HeartbeatConfig, Heartbeater, NodeProfile, RegistrationError};
use skys3_io::{BlockingPool, Clock};
use skys3_net::{TokioNetwork, Transport};
use skys3_types::{DiskInfo, Label, NodeAddress};

use crate::control::backend::Backend;
use crate::datadir::DataDir;
use crate::node::StartError;

/// The heartbeater of a node, over its control store and its transport.
pub(crate) type NodeHeartbeater = Heartbeater<Backend, TokioNetwork>;

/// What the node registers about itself: `advertise` as its address, and
/// each disk's capacity read on `pool`.
///
/// # Errors
///
/// [`StartError::Io`] if a disk's capacity cannot be read.
pub(crate) async fn profile(
    config: &Config,
    data_dir: &DataDir,
    advertise: NodeAddress,
    pool: &BlockingPool,
) -> Result<NodeProfile, StartError> {
    let mut disks = Vec::new();
    for disk in data_dir.disks() {
        let path: PathBuf = disk.path.clone();
        let space = pool
            .run(move || skys3_io::disk::space(&path))
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))
            .and_then(|space| space)
            .map_err(|source| StartError::Io {
                what: format!("reading the capacity of {}", disk.path.display()),
                source,
            })?;
        disks.push(DiskInfo {
            disk_id: disk.label.clone(),
            capacity_bytes: space.total,
        });
    }
    // Data directories made by this build have an instance ID that is a
    // label; any other registers without one.
    let instance = Label::new(data_dir.instance_id()).ok();
    Ok(NodeProfile {
        node: data_dir.node_id().clone(),
        address: advertise,
        zone: config.node().zone.clone(),
        rack: config.node().rack.clone(),
        disks,
        instance,
    })
}

/// The node's heartbeater: it registers `profile` in `store` and sends the
/// coordinator heartbeats over `transport`.
pub(crate) fn heartbeater(
    config: &Config,
    store: Backend,
    transport: Transport<TokioNetwork>,
    profile: NodeProfile,
    clock: Arc<dyn Clock>,
) -> NodeHeartbeater {
    Heartbeater::new(
        store,
        transport,
        config.cluster().cluster_id.clone(),
        profile,
        clock,
        HeartbeatConfig::default(),
        ProposalIds::from_os_rng(),
    )
}

/// Registers the node before it serves.
///
/// # Errors
///
/// [`StartError::Cluster`] if the register holds the node's ID for another
/// data directory. Other failures are logged, and the heartbeater tries
/// again once it runs.
pub(crate) async fn register_at_start(heartbeater: &mut NodeHeartbeater) -> Result<(), StartError> {
    match heartbeater.register().await {
        Ok(registered) => {
            tracing::info!(
                registration = ?registered.registration,
                generation = ?registered.generation.map(|generation| generation.get()),
                "the node is registered"
            );
            Ok(())
        }
        Err(error @ RegistrationError::Claimed { .. }) => {
            Err(StartError::Cluster(error.to_string()))
        }
        Err(error) => {
            tracing::warn!(%error, "cannot register the node yet; trying again in the background");
            Ok(())
        }
    }
}
