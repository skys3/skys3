//! The replication data path between a shard's primary and its members
//! (§5.1, §6.6), over the intra-cluster transport.
//!
//! [`Replication`] runs on every node. It opens the node's shard replicas
//! in the roles their configurations give it, and:
//!
//! - **As a primary**, it keeps one link per member and shard. A link
//!   connects, opens a session with a `Sync` (which also rolls forward the
//!   records the member holds beyond the primary's log), and then sends the
//!   member every record the primary holds after the member's last, as the
//!   primary appends it, with the commit watermark. While there is nothing
//!   to send it sends beacons that carry the watermark. Appends and
//!   beacons carry lease stamps, and a beacon goes out at least every
//!   [`ReplicationConfig::lease_renew_interval`] even while records flow.
//!   The member's acknowledgements feed the commit rule and the primary's
//!   leases ([`Leader`](crate::Leader)). A link that fails,
//!   or hears nothing for [`ReplicationConfig::link_timeout`], reconnects,
//!   and resumes from what the member then holds.
//! - **As a member**, it accepts links from the shard's primary only. A
//!   `Sync` opens a new session, which refuses appends of every earlier one
//!   (such as appends of a primary's earlier life still in the network),
//!   waits until the records already queued are durable, and reports the
//!   last one. Appends must come from the shard's epoch (rule R2 refuses
//!   older ones) and follow the member's last record. The member
//!   acknowledges the run of records it holds durably as it grows, and
//!   applies records up to the commit watermark. Each acknowledgement
//!   echoes the latest lease stamp, which grants the primary a lease, and
//!   restarts the member's [`Grace`] for the shard
//!   ([`Replication::grace`], §5.4).
//!
//! Configurations are static here: a configuration change (plan M2-08
//! onward) needs the `CONFIG` handling of a newer epoch on both ends.

mod member;
mod primary;
#[cfg(test)]
mod tests;
pub mod wire;

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use skys3_io::{Clock, Disk};
use skys3_log::ShardRef;
use skys3_net::{Frame, Listener, Network, Receiver, Transport, TransportError};
use skys3_types::{NodeAddress, NodeId, ShardConfig};

use crate::error::ShardError;
use crate::lease::Grace;
use crate::set::ShardSet;
use crate::shard::{Role, Shard};

/// Timing of the replication links. Configuration keys for them come with
/// the PR that runs replication in the node binary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicationConfig {
    /// How often a primary sends a beacon with the commit watermark while
    /// it has no record to send a member.
    pub beacon_interval: Duration,
    /// How long either end of a link waits for the next frame before it
    /// drops the link. Members answer every beacon, so a live link hears
    /// something at least every `beacon_interval`.
    pub link_timeout: Duration,
    /// How long a primary waits before it reconnects a link that dropped.
    pub reconnect_delay: Duration,
    /// `lease_renew_interval`: the longest a primary goes without sending a
    /// member a beacon, even while it sends records (§5.4). At least
    /// `beacon_interval`.
    pub lease_renew_interval: Duration,
    /// `primary_lease`: how long a member's acknowledgement lets the
    /// primary serve reads, from the stamp it echoes, on the primary's
    /// clock (§5.4).
    pub primary_lease: Duration,
    /// `primary_grace`: how long after it last granted a lease a member
    /// waits, on its own clock, before it may propose a new primary
    /// (§5.4). Configuration loading checks it against `primary_lease`
    /// and the drift bound `ρ`.
    pub primary_grace: Duration,
}

impl Default for ReplicationConfig {
    /// The design's defaults, with beacons every 100 ms.
    fn default() -> Self {
        Self {
            beacon_interval: Duration::from_millis(100),
            link_timeout: Duration::from_secs(1),
            reconnect_delay: Duration::from_millis(200),
            lease_renew_interval: Duration::from_secs(1),
            primary_lease: Duration::from_secs(4),
            primary_grace: Duration::from_secs(6),
        }
    }
}

/// The replication service of one node: see the [module](self) docs.
pub struct Replication<N: Network, D: Disk> {
    inner: Arc<Inner<N, D>>,
}

struct Inner<N: Network, D: Disk> {
    node: NodeId,
    set: ShardSet<D>,
    transport: Transport<N>,
    peers: BTreeMap<NodeId, NodeAddress>,
    clock: Arc<dyn Clock>,
    config: ReplicationConfig,
    /// The grace of each shard this node is a member of, in this life.
    graces: Mutex<BTreeMap<ShardRef, Arc<Grace>>>,
}

impl<N: Network, D: Disk> Inner<N, D> {
    /// The grace of `shard`, started now if the shard has none yet.
    fn grace_of(&self, shard: &ShardRef) -> Arc<Grace> {
        let mut graces = self.graces.lock().unwrap_or_else(PoisonError::into_inner);
        let grace = graces.entry(shard.clone()).or_insert_with(|| {
            Arc::new(Grace::new(
                Arc::clone(&self.clock),
                self.config.primary_grace,
            ))
        });
        Arc::clone(grace)
    }
}

impl<N: Network, D: Disk> Clone for Replication<N, D> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<N: Network, D: Disk> fmt::Debug for Replication<N, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Replication")
            .field("node", &self.inner.node)
            .finish_non_exhaustive()
    }
}

impl<N: Network, D: Disk> Replication<N, D> {
    /// The replication service of node `node`, whose replicas are in `set`,
    /// reaching the other nodes at `peers` through `transport`, and timing
    /// leases and grace on `clock`.
    #[must_use]
    pub fn new(
        node: NodeId,
        set: ShardSet<D>,
        transport: Transport<N>,
        peers: BTreeMap<NodeId, NodeAddress>,
        clock: Arc<dyn Clock>,
        config: ReplicationConfig,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                node,
                set,
                transport,
                peers,
                clock,
                config,
                graces: Mutex::default(),
            }),
        }
    }

    /// The node's shard replicas.
    #[must_use]
    pub fn set(&self) -> &ShardSet<D> {
        &self.inner.set
    }

    /// The grace of this node's replica of `shard`, if it is a member of
    /// it: the time since it last granted the primary a lease (§5.4).
    #[must_use]
    pub fn grace(&self, shard: &ShardRef) -> Option<Arc<Grace>> {
        let graces = self.inner.graces.lock();
        graces
            .unwrap_or_else(PoisonError::into_inner)
            .get(shard)
            .cloned()
    }

    /// Opens this node's replica in the shard of `config`, or returns it if
    /// it is open. As the primary of a replicated shard, it starts its
    /// links to the members on the current Tokio runtime, which run until
    /// the shard stops, and its leases. As a member, it starts the shard's
    /// grace, unless it is running: opening counts as granting a lease,
    /// since this life does not know when the node last granted one.
    ///
    /// # Errors
    ///
    /// As [`ShardSet::open_replica`].
    pub async fn open(&self, config: &ShardConfig) -> Result<Shard<D>, ShardError> {
        let inner = &self.inner;
        let shard = inner.set.open_replica(config, &inner.node).await?;
        if shard.role() == Role::Member {
            inner.grace_of(shard.shard());
        }
        if let Some(leader) = shard.leader().filter(|leader| leader.claim_links()) {
            leader.start_leases(Arc::clone(&inner.clock), inner.config.primary_lease);
            for member in leader.members() {
                let Some(address) = inner.peers.get(member) else {
                    tracing::warn!(shard = %leader.shard(), %member, "no address for a member");
                    continue;
                };
                let link = primary::Link {
                    shard: shard.clone(),
                    leader: Arc::clone(leader),
                    member: member.clone(),
                    address: address.clone(),
                    transport: inner.transport.clone(),
                    config: inner.config,
                };
                tokio::spawn(link.run());
            }
        }
        Ok(shard)
    }

    /// Accepts links from primaries on `listener` until it fails, serving
    /// each on a task of its own.
    pub async fn serve(&self, listener: Listener<N>) {
        loop {
            let incoming = match listener.accept().await {
                Ok(incoming) => incoming,
                Err(error) => {
                    tracing::warn!(%error, "accepting a replication link failed");
                    tokio::time::sleep(self.inner.config.reconnect_delay).await;
                    continue;
                }
            };
            let inner = Arc::clone(&self.inner);
            tokio::spawn(async move {
                let connection = match incoming.handshake().await {
                    Ok(connection) => connection,
                    Err(error) => {
                        tracing::debug!(%error, "a replication link failed its handshake");
                        return;
                    }
                };
                if let Err(error) = member::serve(connection, &inner).await {
                    tracing::debug!(%error, "a replication link to a primary ended");
                }
            });
        }
    }
}

/// Why a link ended.
#[derive(Debug, thiserror::Error)]
enum LinkError {
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error("no frame came within {0:?}")]
    Timeout(Duration),
    #[error("the peer closed the link")]
    Closed,
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error(transparent)]
    Shard(#[from] ShardError),
}

/// Receives the next frame within `timeout`.
async fn recv<S>(receiver: &mut Receiver<S>, timeout: Duration) -> Result<Frame, LinkError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    match tokio::time::timeout(timeout, receiver.recv()).await {
        Err(_) => Err(LinkError::Timeout(timeout)),
        Ok(Ok(Some(frame))) => Ok(frame),
        Ok(Ok(None)) => Err(LinkError::Closed),
        Ok(Err(error)) => Err(error.into()),
    }
}
