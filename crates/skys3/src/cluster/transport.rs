//! The intra-cluster transport listener (design §12): TCP with mutual TLS
//! on `[transport] listen`, at most `max_inflight_handshakes` handshakes
//! at once, and each authenticated connection handed, by the class of its
//! first frame, to the service that serves it.
//!
//! A connection gets a task only once a handshake slot is free, and the
//! listener accepts no other meanwhile, so a flood of connections that
//! never finish a handshake waits in the kernel's accept queue: the node
//! holds at most the cap of handshake tasks and TLS states, each for at
//! most `skys3_net::HANDSHAKE_TIMEOUT`, whatever the rate of the flood.
//!
//! It serves admin messages (pushed generations, heartbeats, and handoff
//! requests, `skys3_coord::AdminEndpoint`); replication links, backfills,
//! and lease messages from other nodes' primaries, through the node's
//! replication; and the requests other gateways forward to this node's
//! replicas (plan M8-05b). A connection whose first frame is of erasure
//! coding (fragment writes, reads, and releases, plan M8-07a) is closed.

use std::time::Duration;

use skys3_coord::AdminEndpoint;
use skys3_net::{Connection, Frame, Listener, MessageClass, MessageKind, Network, TransportError};
use skys3_obs::{HandshakeLimit, HandshakeOutcome};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::task::JoinSet;

use super::replication::{Forwarded, NodeReplication};

/// How long an authenticated connection may take to send its first frame.
const FIRST_FRAME_TIMEOUT: Duration = Duration::from_secs(10);

/// The pause after a failed `accept`, such as running out of file
/// descriptors.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

/// The services authenticated connections are handed to. Clones share
/// them.
#[derive(Debug, Clone)]
pub(crate) struct Services {
    /// Admin messages from other nodes and operator tools.
    pub(crate) admin: AdminEndpoint,
    /// Replication links, backfills, and lease messages from the primaries
    /// of the shards this node holds a replica of.
    pub(crate) replication: NodeReplication,
    /// Requests other gateways forward to this node's replicas.
    pub(crate) forwarded: Forwarded,
}

/// Whether `kind` opens a connection of erasure coding, which this build
/// does not serve yet (plan M8-07a).
fn is_coding(kind: MessageKind) -> bool {
    matches!(
        kind,
        MessageKind::FragmentWrite
            | MessageKind::FragmentWritten
            | MessageKind::OrphanQuery
            | MessageKind::OrphanVerdicts
            | MessageKind::FragmentRead
            | MessageKind::FragmentData
            | MessageKind::FragmentRelease
            | MessageKind::FragmentHold
            | MessageKind::FragmentNoted
    )
}

impl Services {
    /// Serves an authenticated connection by the class of its first frame.
    async fn serve<S>(&self, connection: Connection<S>)
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let peer = connection.peer().clone();
        let (mut receiver, sender) = connection.into_split();
        let first: Frame = match tokio::time::timeout(FIRST_FRAME_TIMEOUT, receiver.recv()).await {
            Ok(Ok(Some(frame))) => frame,
            Ok(Ok(None)) => return,
            Ok(Err(error)) => {
                tracing::debug!(%peer, %error, "an intra-cluster connection failed");
                return;
            }
            Err(_) => {
                tracing::debug!(%peer, "an intra-cluster connection sent nothing");
                return;
            }
        };
        let kind = first.header.kind;
        match kind.class() {
            Some(MessageClass::Admin) => {
                if let Err(error) = self.admin.serve_link((receiver, sender), first).await {
                    tracing::debug!(%peer, %error, "an admin connection ended");
                }
            }
            Some(MessageClass::Replication | MessageClass::Lease) if !is_coding(kind) => {
                self.replication.follow((receiver, sender), first).await;
            }
            Some(MessageClass::Request) if kind == MessageKind::Forward => {
                self.forwarded.serve((receiver, sender), first).await;
            }
            class => tracing::info!(
                %peer,
                kind = ?first.header.kind,
                ?class,
                "closing an intra-cluster connection this node serves nothing on yet"
            ),
        }
    }
}

/// Accepts connections on `listener` until the task is dropped, with at
/// most `limit` handshakes at once, and serves each authenticated one with
/// `services` on a task of its own. The connections' tasks end with this
/// one: a node that stops serves no other node's link or forwarded request
/// from then on, and those nodes see their connections end at once, rather
/// than answers from replicas that are closing.
pub(crate) async fn serve<N: Network>(
    listener: Listener<N>,
    limit: HandshakeLimit,
    services: Services,
) {
    let mut connections = JoinSet::new();
    loop {
        // Forget the connections that ended.
        while connections.try_join_next().is_some() {}
        let incoming = match listener.accept().await {
            Ok(incoming) => incoming,
            Err(error) => {
                tracing::warn!(%error, "the transport listener cannot accept a connection");
                tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
                continue;
            }
        };
        // Until a slot is free, further connections stay in the kernel's
        // accept queue.
        let permit = limit.acquire().await;
        let services = services.clone();
        connections.spawn(async move {
            let remote = incoming.remote_addr();
            let connection = match incoming.handshake().await {
                Ok(connection) => {
                    permit.finish(HandshakeOutcome::Completed);
                    connection
                }
                Err(TransportError::HandshakeTimeout) => {
                    permit.finish(HandshakeOutcome::TimedOut);
                    tracing::debug!(%remote, "a transport handshake timed out");
                    return;
                }
                Err(error) => {
                    permit.finish(HandshakeOutcome::Failed);
                    tracing::info!(%remote, %error, "refused a connection to the transport");
                    return;
                }
            };
            services.serve(connection).await;
        });
    }
}
