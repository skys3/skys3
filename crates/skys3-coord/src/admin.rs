//! The node's endpoint for admin messages from other nodes (design §12):
//! pushed generations ([`ControlChanged`]) on every node, heartbeats
//! ([`Heartbeat`]) on a node that may become coordinator, and requests to
//! hand a shard off ([`Handoff`]) on a node that leads shards, from the
//! peers a [`HandoffAuthority`] admits (threat T6).

use std::sync::Arc;
use std::time::Duration;

use skys3_io::Clock;
use skys3_net::{
    Connection, Frame, Listener, MessageKind, Network, PeerIdentity, Receiver, Sender,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::watch;

use crate::authority::HandoffAuthority;
use crate::handoff::{Handoff, HandoffAck, HandoffSink};
use crate::heartbeat::{Heartbeat, HeartbeatAck};
use crate::lease::Leadership;
use crate::push::{ControlChanged, ControlHints, PUSH_IDLE_TIMEOUT, PushError, reply};
use crate::registry::NodeRegistry;

/// Serves the admin messages a node receives from other nodes: records
/// pushed generations in its [`ControlHints`], answers heartbeats given a
/// registry, and starts the handoffs it is asked for given a
/// [`HandoffSink`].
///
/// Clones share the hints, the registry, and the sink.
#[derive(Clone)]
pub struct AdminEndpoint {
    hints: ControlHints,
    heartbeats: Option<Heartbeats>,
    handoffs: Option<Handoffs>,
}

/// What starts handoffs, and who may ask for them.
#[derive(Clone)]
struct Handoffs {
    sink: Arc<dyn HandoffSink>,
    /// `None` admits every authenticated peer.
    authority: Option<Arc<dyn HandoffAuthority>>,
}

/// What answers heartbeats: the registry they are recorded in, and
/// whether this node is coordinator.
#[derive(Clone)]
struct Heartbeats {
    registry: NodeRegistry,
    leadership: watch::Receiver<Leadership>,
    clock: Arc<dyn Clock>,
}

impl std::fmt::Debug for AdminEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdminEndpoint")
            .field("hints", &self.hints)
            .field("heartbeats", &self.heartbeats.is_some())
            .field("handoffs", &self.handoffs.is_some())
            .field(
                "handoff_authority",
                &self
                    .handoffs
                    .as_ref()
                    .is_some_and(|handoffs| handoffs.authority.is_some()),
            )
            .finish()
    }
}

impl AdminEndpoint {
    /// An endpoint that records pushes in `hints` and refuses heartbeats.
    #[must_use]
    pub fn new(hints: ControlHints) -> Self {
        Self {
            hints,
            heartbeats: None,
            handoffs: None,
        }
    }

    /// Also starts the planned handoffs any authenticated peer asks for
    /// ([`Handoff`]), through `sink`, as in a harness whose every peer is
    /// trusted. Without it, it refuses them. A node uses
    /// [`AdminEndpoint::with_handoffs_from`].
    #[must_use]
    pub fn with_handoffs(mut self, sink: Arc<dyn HandoffSink>) -> Self {
        self.handoffs = Some(Handoffs {
            sink,
            authority: None,
        });
        self
    }

    /// Also starts, through `sink`, the planned handoffs that the peers
    /// `authority` admits ask for, and refuses the others' (threat T6).
    #[must_use]
    pub fn with_handoffs_from(
        mut self,
        sink: Arc<dyn HandoffSink>,
        authority: Arc<dyn HandoffAuthority>,
    ) -> Self {
        self.handoffs = Some(Handoffs {
            sink,
            authority: Some(authority),
        });
        self
    }

    /// Also answers heartbeats, recording them in `registry` and telling
    /// the sender whether this node is coordinator, as `leadership` says
    /// on `clock`.
    #[must_use]
    pub fn with_heartbeats(
        mut self,
        registry: NodeRegistry,
        leadership: watch::Receiver<Leadership>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        self.heartbeats = Some(Heartbeats {
            registry,
            leadership,
            clock,
        });
        self
    }

    /// Serves connections arriving on `listener`, each on a task of its
    /// own, until the task is dropped.
    pub async fn serve<N: Network>(&self, listener: Listener<N>) {
        loop {
            let incoming = match listener.accept().await {
                Ok(incoming) => incoming,
                Err(error) => {
                    tracing::warn!(%error, "accepting an admin connection failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            let endpoint = self.clone();
            tokio::spawn(async move {
                let result = match incoming.handshake().await {
                    Ok(connection) => endpoint.serve_connection(connection).await,
                    Err(error) => Err(error.into()),
                };
                if let Err(error) = result {
                    tracing::debug!(%error, "an admin connection ended");
                }
            });
        }
    }

    /// Serves one connection until the peer closes it.
    ///
    /// # Errors
    ///
    /// [`PushError`] if the connection fails, stays idle for
    /// [`PUSH_IDLE_TIMEOUT`], or carries a malformed frame, a frame of a
    /// kind this endpoint does not serve, or a heartbeat from a peer that
    /// is not a node. A handoff request it cannot serve is answered with a
    /// refusal.
    pub async fn serve_connection<S>(&self, connection: Connection<S>) -> Result<(), PushError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let (mut receiver, sender) = connection.into_split();
        match next_frame(&mut receiver).await? {
            Some(first) => self.serve_link((receiver, sender), first).await,
            None => Ok(()),
        }
    }

    /// Serves a connection whose first frame, `first`, the caller already
    /// received, until the peer closes it: for a node whose transport
    /// carries other messages too, and that hands a connection here by the
    /// class of its first frame.
    ///
    /// # Errors
    ///
    /// As [`AdminEndpoint::serve_connection`].
    pub async fn serve_link<S>(
        &self,
        (mut receiver, mut sender): (Receiver<S>, Sender<S>),
        first: Frame,
    ) -> Result<(), PushError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let mut next = Some(first);
        while let Some(frame) = next {
            let request = frame.header.request_id;
            let answer = match (frame.header.kind, &self.heartbeats) {
                (MessageKind::ControlChanged, _) => {
                    let generation = ControlChanged::from_frame(&frame)?;
                    if self.hints.hint(generation) {
                        tracing::debug!(%generation, peer = %receiver.peer(), "pushed a new generation");
                    }
                    (request != 0).then(|| reply(request))
                }
                (MessageKind::NodeHeartbeat, Some(heartbeats)) => {
                    Heartbeat::from_frame(&frame)?;
                    let node = receiver
                        .peer()
                        .node_id()
                        .ok_or_else(|| PushError::NotANode(receiver.peer().clone()))?;
                    let ack = HeartbeatAck {
                        coordinator: heartbeats
                            .leadership
                            .borrow()
                            .is_coordinator_at(heartbeats.clock.now()),
                        unregistered: !heartbeats.registry.heard(node),
                    };
                    Some(ack.frame(request))
                }
                (MessageKind::Handoff, _) => {
                    let handoff = Handoff::from_frame(&frame)?;
                    let ack = match &self.handoffs {
                        None => HandoffAck::refused("this node starts no handoffs"),
                        Some(handoffs) => handoffs.serve(receiver.peer(), handoff).await,
                    };
                    Some(ack.frame(request))
                }
                (kind, _) => return Err(PushError::Unexpected(kind)),
            };
            if let Some(answer) = answer {
                sender.send(&answer).await?;
            }
            next = next_frame(&mut receiver).await?;
        }
        Ok(())
    }
}

impl Handoffs {
    /// Starts `handoff`, which `peer` asks for, if the authority admits
    /// the peer.
    async fn serve(&self, peer: &PeerIdentity, handoff: Handoff) -> HandoffAck {
        if let Some(authority) = &self.authority
            && let Err(reason) = authority.permits(peer).await
        {
            tracing::warn!(%peer, ?handoff, %reason, "refused a handoff request");
            return HandoffAck::refused(reason);
        }
        tracing::info!(%peer, ?handoff, "asked to hand a shard off");
        match self.sink.hand_off(handoff).await {
            Ok(()) => HandoffAck::started(),
            Err(reason) => HandoffAck::refused(reason),
        }
    }
}

/// The next frame of an admin connection, or `None` once the peer closed
/// it.
///
/// # Errors
///
/// [`PushError::Idle`] if none comes within [`PUSH_IDLE_TIMEOUT`], and the
/// transport's errors.
async fn next_frame<S>(receiver: &mut Receiver<S>) -> Result<Option<Frame>, PushError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match tokio::time::timeout(PUSH_IDLE_TIMEOUT, receiver.recv()).await {
        Err(_) => Err(PushError::Idle),
        Ok(frame) => Ok(frame?),
    }
}
