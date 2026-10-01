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
//!   to send it sends beacons that carry the watermark. The member's
//!   acknowledgements feed the commit rule ([`Leader`](crate::Leader)). A link that fails,
//!   or hears nothing for [`ReplicationConfig::link_timeout`], reconnects,
//!   and resumes from what the member then holds.
//! - **As a member**, it accepts links from the shard's primary only. A
//!   `Sync` opens a new session, which refuses appends of every earlier one
//!   (such as appends of a primary's earlier life still in the network),
//!   waits until the records already queued are durable, and reports the
//!   last one. Appends must come from the shard's epoch (rule R2 refuses
//!   older ones) and follow the member's last record. The member
//!   acknowledges the run of records it holds durably as it grows, and
//!   applies records up to the commit watermark.
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
use std::sync::Arc;
use std::time::Duration;

use skys3_io::Disk;
use skys3_net::{Frame, Listener, Network, Receiver, Sender, Transport, TransportError};
use skys3_types::{NodeAddress, NodeId, ShardConfig};

use crate::error::ShardError;
use crate::set::ShardSet;
use crate::shard::Shard;

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
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            beacon_interval: Duration::from_millis(100),
            link_timeout: Duration::from_secs(1),
            reconnect_delay: Duration::from_millis(200),
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
    config: ReplicationConfig,
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
    /// reaching the other nodes at `peers` through `transport`.
    #[must_use]
    pub fn new(
        node: NodeId,
        set: ShardSet<D>,
        transport: Transport<N>,
        peers: BTreeMap<NodeId, NodeAddress>,
        config: ReplicationConfig,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                node,
                set,
                transport,
                peers,
                config,
            }),
        }
    }

    /// The node's shard replicas.
    #[must_use]
    pub fn set(&self) -> &ShardSet<D> {
        &self.inner.set
    }

    /// Opens this node's replica in the shard of `config`, or returns it if
    /// it is open, and, if it is the primary of a replicated shard, starts
    /// its links to the members on the current Tokio runtime. The links run
    /// until the shard stops.
    ///
    /// # Errors
    ///
    /// As [`ShardSet::open_replica`].
    pub async fn open(&self, config: &ShardConfig) -> Result<Shard<D>, ShardError> {
        let inner = &self.inner;
        let shard = inner.set.open_replica(config, &inner.node).await?;
        if let Some(leader) = shard.leader().filter(|leader| leader.claim_links()) {
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
            let replication = self.clone();
            tokio::spawn(async move {
                let connection = match incoming.handshake().await {
                    Ok(connection) => connection,
                    Err(error) => {
                        tracing::debug!(%error, "a replication link failed its handshake");
                        return;
                    }
                };
                let (mut receiver, sender) = connection.into_split();
                let timeout = replication.inner.config.link_timeout;
                match recv(&mut receiver, timeout).await {
                    Ok(first) => replication.follow((receiver, sender), first).await,
                    Err(error) => tracing::debug!(%error, "a replication link sent nothing"),
                }
            });
        }
    }

    /// Serves a link from a primary, whose first frame `first` arrived on
    /// `link`, as a member, until the link fails or a later session
    /// replaces it. A node whose transport also carries other messages
    /// accepts connections itself and hands replication links here.
    pub async fn follow<S>(&self, link: (Receiver<S>, Sender<S>), first: Frame)
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let inner = &self.inner;
        if let Err(error) = member::serve(link, first, &inner.set, inner.config).await {
            tracing::debug!(%error, "a replication link to a primary ended");
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
