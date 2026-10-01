//! Pushing new generations to every node (design §6.2): after each change,
//! the coordinator sends every node a [`MessageKind::ControlChanged`] frame
//! naming the generation that announces it, and each node records it in its
//! [`ControlHints`].
//!
//! A push is a hint, not state. A node acts on it by reading
//! `cluster.json` and the registers from the control store, so a lost,
//! late, or forged push costs at most a read, and a node that misses one
//! still learns the generation from its change stream: a native watch, or
//! polling every `config_poll_interval`.
//!
//! ```text
//! coordinator                            node
//!   ControlChanged(generation), id n   ->
//!                                      <- AdminReply, id n
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use bytes::Bytes;
use prost::Message;
use skys3_net::{
    Connection, Frame, Header, Listener, MessageKind, Network, PeerIdentity, Transport,
    TransportError,
};
use skys3_types::{Generation, NodeAddress, NodeId};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::admin::AdminEndpoint;
use crate::heartbeat::HeartbeatError;

/// The body of a [`MessageKind::ControlChanged`] frame. Its payload is
/// empty.
#[derive(Clone, PartialEq, Eq, Message)]
pub struct ControlChanged {
    /// The generation in `cluster.json` that announces the change; at
    /// least 1.
    #[prost(uint64, tag = "1")]
    pub generation: u64,
}

/// Why a frame is not a valid [`ControlChanged`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum HintError {
    /// The frame is of another kind.
    #[error("expected a ControlChanged frame, not {0:?}")]
    Kind(MessageKind),
    /// The frame carries a payload.
    #[error("a ControlChanged frame carries no payload, but this one has {0} bytes")]
    Payload(usize),
    /// The body is not a valid protobuf message.
    #[error("malformed ControlChanged body: {0}")]
    Malformed(String),
    /// The body names generation 0, which no bootstrapped store has.
    #[error("a ControlChanged frame must name a generation of at least 1")]
    ZeroGeneration,
}

impl ControlChanged {
    /// A frame announcing `generation`, with `request_id` (nonzero for a
    /// frame that expects a reply).
    #[must_use]
    pub fn frame(generation: Generation, request_id: u64) -> Frame {
        let body = Self {
            generation: generation.get(),
        };
        let header = Header::new(MessageKind::ControlChanged)
            .with_request_id(request_id)
            .with_body(body.encode_to_vec());
        Frame::new(header, Bytes::new())
    }

    /// The generation a frame from a peer announces.
    ///
    /// # Errors
    ///
    /// [`HintError`] if the frame is not a well-formed `ControlChanged`.
    pub fn from_frame(frame: &Frame) -> Result<Generation, HintError> {
        if frame.header.kind != MessageKind::ControlChanged {
            return Err(HintError::Kind(frame.header.kind));
        }
        if !frame.payload.is_empty() {
            return Err(HintError::Payload(frame.payload.len()));
        }
        let body = Self::decode(frame.header.body.clone())
            .map_err(|error| HintError::Malformed(error.to_string()))?;
        if body.generation == 0 {
            return Err(HintError::ZeroGeneration);
        }
        Ok(Generation::new(body.generation))
    }
}

/// The reply to a [`ControlChanged`] frame that carries a request ID.
pub(crate) fn reply(request_id: u64) -> Frame {
    Frame::new(
        Header::new(MessageKind::AdminReply).with_request_id(request_id),
        Bytes::new(),
    )
}

/// How long a node keeps a push connection open without a frame.
pub const PUSH_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// A node's record of the newest generation pushed to it: the wake-up for
/// its sync of control state, ahead of its next poll.
///
/// Clones share the record.
#[derive(Debug, Clone)]
pub struct ControlHints {
    latest: Arc<watch::Sender<Generation>>,
}

impl Default for ControlHints {
    fn default() -> Self {
        Self::new()
    }
}

/// Why a push or admin connection ended.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PushError {
    /// The transport failed.
    #[error(transparent)]
    Transport(#[from] TransportError),
    /// The peer sent a malformed push.
    #[error(transparent)]
    Hint(#[from] HintError),
    /// The peer sent a malformed heartbeat.
    #[error(transparent)]
    Heartbeat(#[from] HeartbeatError),
    /// The peer sent a frame of a kind the endpoint does not serve.
    #[error("unexpected {0:?} frame on an admin connection")]
    Unexpected(MessageKind),
    /// A peer that is not a node sent a heartbeat.
    #[error("{0} is not a node, so it sends no heartbeats")]
    NotANode(PeerIdentity),
    /// The peer sent nothing for [`PUSH_IDLE_TIMEOUT`].
    #[error("no frame within {PUSH_IDLE_TIMEOUT:?}")]
    Idle,
}

impl ControlHints {
    /// A record at generation 0: nothing pushed yet.
    #[must_use]
    pub fn new() -> Self {
        let (latest, _) = watch::channel(Generation::ZERO);
        Self {
            latest: Arc::new(latest),
        }
    }

    /// Records a pushed `generation`. Returns whether it is newer than
    /// every one recorded before.
    pub fn hint(&self, generation: Generation) -> bool {
        self.latest.send_if_modified(|latest| {
            let newer = generation > *latest;
            if newer {
                *latest = generation;
            }
            newer
        })
    }

    /// The newest generation pushed so far.
    #[must_use]
    pub fn latest(&self) -> Generation {
        *self.latest.borrow()
    }

    /// Waits until a generation newer than `known` has been pushed, and
    /// returns the newest one. A node's sync loop selects on this beside
    /// its change stream.
    pub async fn newer_than(&self, known: Generation) -> Generation {
        let mut latest = self.latest.subscribe();
        let newer = latest.wait_for(|latest| *latest > known).await.map(|g| *g);
        match newer {
            Ok(newer) => newer,
            // The sender lives in `self`, so it is never dropped first.
            Err(_) => std::future::pending().await,
        }
    }

    /// Serves pushes arriving on `listener`, each connection on a task of
    /// its own, until the task is dropped. An [`AdminEndpoint`] that also
    /// answers heartbeats serves the same pushes.
    pub async fn serve<N: Network>(&self, listener: Listener<N>) {
        AdminEndpoint::new(self.clone()).serve(listener).await;
    }

    /// Serves pushes on one connection until the peer closes it.
    ///
    /// # Errors
    ///
    /// [`PushError`] if the connection fails, stays idle, or carries
    /// anything but valid pushes.
    pub async fn serve_connection<S>(&self, connection: Connection<S>) -> Result<(), PushError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        AdminEndpoint::new(self.clone())
            .serve_connection(connection)
            .await
    }
}

/// Delivers the generation that announces a change to the nodes: the
/// extension point between the coordinator and how nodes are reached.
pub trait Announce: Send + Sync + 'static {
    /// Tells the nodes that `generation` announces a change. Delivery is
    /// best effort: nodes that miss it learn it from their change streams.
    fn announce(&self, generation: Generation) -> impl Future<Output = ()> + Send;
}

/// Records the generation on this node only, for a single node and for
/// tests.
impl Announce for ControlHints {
    async fn announce(&self, generation: Generation) {
        self.hint(generation);
    }
}

/// What one [`Pusher::push`] achieved.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Pushed {
    /// The nodes that acknowledged the push, this one included when it
    /// has local hints.
    pub delivered: BTreeSet<NodeId>,
    /// The nodes that did not, and why.
    pub failed: BTreeMap<NodeId, String>,
}

/// Pushes generations over the intra-cluster transport to every node it
/// knows, in parallel, each push bounded by a timeout.
///
/// The node set is replaceable ([`Pusher::set_peers`]), so the node
/// registry (plan M3-02) can keep it current. Clones share it.
#[derive(Clone)]
pub struct Pusher<N> {
    transport: Transport<N>,
    peers: Arc<RwLock<BTreeMap<NodeId, NodeAddress>>>,
    local: Option<ControlHints>,
    timeout: Duration,
    requests: Arc<AtomicU64>,
}

impl<N> std::fmt::Debug for Pusher<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pusher")
            .field("peers", &self.peers)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl<N: Network> Pusher<N> {
    /// A pusher that reaches `peers` through `transport`, giving each push
    /// `timeout` to connect, send, and be acknowledged. A peer that is
    /// this node is skipped, unless [`Pusher::with_local`] gives it hints.
    #[must_use]
    pub fn new(
        transport: Transport<N>,
        peers: BTreeMap<NodeId, NodeAddress>,
        timeout: Duration,
    ) -> Self {
        Self {
            transport,
            peers: Arc::new(RwLock::new(peers)),
            local: None,
            timeout,
            requests: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Records pushes to this node in `hints` directly, without the
    /// network.
    #[must_use]
    pub fn with_local(mut self, hints: ControlHints) -> Self {
        self.local = Some(hints);
        self
    }

    /// Replaces the nodes pushes go to.
    pub fn set_peers(&self, peers: BTreeMap<NodeId, NodeAddress>) {
        *self.peers.write().unwrap_or_else(PoisonError::into_inner) = peers;
    }

    /// Pushes `generation` to every node and waits for each to acknowledge
    /// it or time out.
    pub async fn push(&self, generation: Generation) -> Pushed {
        let me = match self.transport.identity() {
            PeerIdentity::Node(node) => Some(node.clone()),
            PeerIdentity::Admin(_) => None,
        };
        let peers = self
            .peers
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let mut pushed = Pushed::default();
        let mut sends = JoinSet::new();
        for (node, address) in peers {
            if Some(&node) == me.as_ref() {
                if let Some(local) = &self.local {
                    local.hint(generation);
                    pushed.delivered.insert(node);
                }
                continue;
            }
            let request = self.requests.fetch_add(1, Ordering::Relaxed);
            let (transport, timeout) = (self.transport.clone(), self.timeout);
            sends.spawn(async move {
                let frame = ControlChanged::frame(generation, request);
                let sent = tokio::time::timeout(timeout, send(&transport, &node, &address, &frame))
                    .await
                    .unwrap_or_else(|_| Err(format!("no acknowledgement within {timeout:?}")));
                (node, sent)
            });
        }
        while let Some(joined) = sends.join_next().await {
            match joined {
                Ok((node, Ok(()))) => {
                    pushed.delivered.insert(node);
                }
                Ok((node, Err(error))) => {
                    tracing::debug!(%node, %generation, %error, "a push failed");
                    pushed.failed.insert(node, error);
                }
                Err(error) => tracing::warn!(%error, "a push task failed"),
            }
        }
        pushed
    }
}

/// Sends one push and waits for its acknowledgement.
async fn send<N: Network>(
    transport: &Transport<N>,
    node: &NodeId,
    address: &NodeAddress,
    frame: &Frame,
) -> Result<(), String> {
    let mut connection = transport
        .connect(node, address)
        .await
        .map_err(|error| error.to_string())?;
    connection
        .send(frame)
        .await
        .map_err(|error| error.to_string())?;
    let answer = connection.recv().await.map_err(|error| error.to_string())?;
    // The peer closes when it sees the end of the stream; its close is
    // not ours to wait for.
    let _ = connection.close().await;
    match answer {
        Some(answer)
            if answer.header.kind == MessageKind::AdminReply
                && answer.header.request_id == frame.header.request_id =>
        {
            Ok(())
        }
        Some(answer) => Err(format!(
            "unexpected answer {:?} to request {}",
            answer.header.kind, frame.header.request_id
        )),
        None => Err("the peer closed the connection".to_owned()),
    }
}

impl<N: Network> Announce for Pusher<N> {
    async fn announce(&self, generation: Generation) {
        let pushed = self.push(generation).await;
        if !pushed.failed.is_empty() {
            tracing::info!(
                %generation,
                failed = pushed.failed.len(),
                "some nodes missed a push; they learn the generation from their change streams"
            );
        }
    }
}

#[cfg(test)]
mod tests;
