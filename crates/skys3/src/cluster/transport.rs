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
//! This build serves admin messages (pushed generations, heartbeats, and
//! handoff requests, `skys3_coord::AdminEndpoint`). A connection of
//! another class is closed: replication and forwarded requests join when
//! the binary serves shards across nodes (plan M8-05b).

use std::time::Duration;

use skys3_coord::AdminEndpoint;
use skys3_net::{Connection, Frame, Listener, MessageClass, Network, TransportError};
use skys3_obs::{HandshakeLimit, HandshakeOutcome};
use tokio::io::{AsyncRead, AsyncWrite};

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
        match first.header.kind.class() {
            Some(MessageClass::Admin) => {
                if let Err(error) = self.admin.serve_link((receiver, sender), first).await {
                    tracing::debug!(%peer, %error, "an admin connection ended");
                }
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
/// `services` on a task of its own.
pub(crate) async fn serve<N: Network>(
    listener: Listener<N>,
    limit: HandshakeLimit,
    services: Services,
) {
    loop {
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
        tokio::spawn(async move {
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
