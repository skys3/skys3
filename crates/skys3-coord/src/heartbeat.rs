//! Node registration and heartbeats to the coordinator (design §6.7,
//! §12): what a node does to join the cluster and stay known to it.
//!
//! A [`Heartbeater`] runs on every node. It registers the node
//! ([`register`]), then sends the coordinator a
//! [`MessageKind::NodeHeartbeat`] frame every interval over the
//! intra-cluster transport. The coordinator records it in its
//! [`NodeRegistry`] and answers with a [`HeartbeatAck`]:
//!
//! ```text
//! node                                   coordinator
//!   NodeHeartbeat, id n                ->
//!                                      <- AdminReply(HeartbeatAck), id n
//! ```
//!
//! The node finds the coordinator by reading `coordinator.lease` and the
//! holder's registration, and keeps its connection while the holder
//! answers as coordinator. So heartbeats cost the control store nothing
//! while the coordinator stays put, and two reads, at most every
//! `resolve_interval`, after it moves. A node whose registration the
//! coordinator does not list, because the coordinator forgot it during a
//! long partition for example, registers again.

use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use prost::Message;
use skys3_control::{ControlStore, ProposalIds, RetryPolicy, TypedKey, read};
use skys3_io::{Clock, MonoTime};
use skys3_net::{Connection, Frame, Header, MessageKind, Network, Transport};
use skys3_types::{ClusterId, NodeAddress, NodeId};
use tokio::sync::watch;

use crate::change::{Pending, settle};
use crate::join::{NodeProfile, Registered, RegistrationError, register};
use crate::lease::Leadership;
use crate::registry::NodeRegistry;

/// The body of a [`MessageKind::NodeHeartbeat`] frame: empty, since the
/// sender's certificate says which node it is. Its payload is empty.
#[derive(Clone, PartialEq, Eq, Message)]
pub struct Heartbeat {}

/// The body of the [`MessageKind::AdminReply`] that answers a heartbeat.
#[derive(Clone, PartialEq, Eq, Message)]
pub struct HeartbeatAck {
    /// Whether the receiver acts as coordinator. A node told otherwise
    /// looks the coordinator up again.
    #[prost(bool, tag = "1")]
    pub coordinator: bool,
    /// Whether the coordinator's registry lists no registration for the
    /// sender, which then registers again.
    #[prost(bool, tag = "2")]
    pub unregistered: bool,
}

/// Why a frame is not a valid heartbeat or heartbeat answer.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum HeartbeatError {
    /// The frame is of another kind.
    #[error("expected a {expected:?} frame, not {found:?}")]
    Kind {
        /// The kind expected.
        expected: MessageKind,
        /// The kind found.
        found: MessageKind,
    },
    /// The frame carries a payload.
    #[error("a heartbeat frame carries no payload, but this one has {0} bytes")]
    Payload(usize),
    /// The body is not a valid protobuf message.
    #[error("malformed heartbeat body: {0}")]
    Malformed(String),
    /// An answer to another request.
    #[error("an answer to request {found}, not {expected}")]
    RequestId {
        /// The request sent.
        expected: u64,
        /// The request answered.
        found: u64,
    },
}

/// Checks a frame's kind and payload, and decodes its body.
fn decode<M: Message + Default>(frame: &Frame, expected: MessageKind) -> Result<M, HeartbeatError> {
    if frame.header.kind != expected {
        return Err(HeartbeatError::Kind {
            expected,
            found: frame.header.kind,
        });
    }
    if !frame.payload.is_empty() {
        return Err(HeartbeatError::Payload(frame.payload.len()));
    }
    M::decode(frame.header.body.clone())
        .map_err(|error| HeartbeatError::Malformed(error.to_string()))
}

impl Heartbeat {
    /// A heartbeat frame with `request_id`.
    #[must_use]
    pub fn frame(request_id: u64) -> Frame {
        let header = Header::new(MessageKind::NodeHeartbeat).with_request_id(request_id);
        Frame::new(header, Bytes::new())
    }

    /// Checks that a frame from a peer is a well-formed heartbeat.
    ///
    /// # Errors
    ///
    /// [`HeartbeatError`] if it is not.
    pub fn from_frame(frame: &Frame) -> Result<Self, HeartbeatError> {
        decode(frame, MessageKind::NodeHeartbeat)
    }
}

impl HeartbeatAck {
    /// The answer to the heartbeat with `request_id`.
    #[must_use]
    pub fn frame(&self, request_id: u64) -> Frame {
        let header = Header::new(MessageKind::AdminReply)
            .with_request_id(request_id)
            .with_body(self.encode_to_vec());
        Frame::new(header, Bytes::new())
    }

    /// The answer a frame gives to the heartbeat with `request_id`.
    ///
    /// # Errors
    ///
    /// [`HeartbeatError`] if it is not a well-formed answer to it.
    pub fn from_frame(frame: &Frame, request_id: u64) -> Result<Self, HeartbeatError> {
        let ack = decode(frame, MessageKind::AdminReply)?;
        if frame.header.request_id != request_id {
            return Err(HeartbeatError::RequestId {
                expected: request_id,
                found: frame.header.request_id,
            });
        }
        Ok(ack)
    }
}

/// How a [`Heartbeater`] works.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeartbeatConfig {
    /// The pause between two heartbeats, and between two attempts to
    /// register. Well under the coordinator's
    /// [`RegistryConfig::suspect_after`](crate::RegistryConfig) and the
    /// endpoint's [`PUSH_IDLE_TIMEOUT`](crate::PUSH_IDLE_TIMEOUT).
    pub interval: Duration,
    /// How long a heartbeat may take to connect and be answered.
    pub timeout: Duration,
    /// The least time between two lookups of the coordinator in the
    /// control store.
    pub resolve_interval: Duration,
    /// How registration writes are retried.
    pub retry: RetryPolicy,
}

impl Default for HeartbeatConfig {
    /// A heartbeat every 2 s, each given 2 s, and a lookup at most every
    /// 5 s.
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(2),
            timeout: Duration::from_secs(2),
            resolve_interval: Duration::from_secs(5),
            retry: RetryPolicy::default(),
        }
    }
}

/// What a [`Heartbeater`] has achieved, for health reports and tests.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HeartbeatStatus {
    /// How many times the node registered, an unchanged registration
    /// included.
    pub registrations: u64,
    /// How many heartbeats a coordinator acknowledged.
    pub delivered: u64,
    /// The coordinator that acknowledged the latest one.
    pub coordinator: Option<NodeId>,
}

/// The coordinator a node heartbeats to, and its connection.
struct Target<T> {
    node: NodeId,
    address: NodeAddress,
    connection: Option<Connection<T>>,
}

/// Registers a node and sends heartbeats to the coordinator, for as long
/// as the node runs ([`Heartbeater::run`]).
pub struct Heartbeater<S, N: Network> {
    store: S,
    transport: Transport<N>,
    cluster: ClusterId,
    profile: NodeProfile,
    clock: Arc<dyn Clock>,
    config: HeartbeatConfig,
    proposals: ProposalIds,
    local: Option<(NodeRegistry, watch::Receiver<Leadership>)>,
    admin_port: Option<NonZeroU16>,
    target: Option<Target<N::Stream>>,
    looked_up: Option<MonoTime>,
    registered: bool,
    /// A registration write still to be settled and announced.
    unsettled: Option<Pending>,
    requests: u64,
    status: watch::Sender<HeartbeatStatus>,
}

impl<S, N: Network> std::fmt::Debug for Heartbeater<S, N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Heartbeater")
            .field("profile", &self.profile)
            .field("config", &self.config)
            .field("status", &*self.status.borrow())
            .finish_non_exhaustive()
    }
}

impl<S: ControlStore, N: Network> Heartbeater<S, N> {
    /// A heartbeater for the node `profile` describes, which `transport`'s
    /// credentials must name, registering in `store` for `cluster`.
    #[must_use]
    pub fn new(
        store: S,
        transport: Transport<N>,
        cluster: ClusterId,
        profile: NodeProfile,
        clock: Arc<dyn Clock>,
        config: HeartbeatConfig,
        proposals: ProposalIds,
    ) -> Self {
        Self {
            store,
            transport,
            cluster,
            profile,
            clock,
            config,
            proposals,
            local: None,
            admin_port: None,
            target: None,
            looked_up: None,
            registered: false,
            unsettled: None,
            requests: 1,
            status: watch::channel(HeartbeatStatus::default()).0,
        }
    }

    /// Records heartbeats in `registry` directly, without the network,
    /// while the lease names this node, as long as `leadership` says it is
    /// coordinator.
    #[must_use]
    pub fn with_local(
        mut self,
        registry: NodeRegistry,
        leadership: watch::Receiver<Leadership>,
    ) -> Self {
        self.local = Some((registry, leadership));
        self
    }

    /// Reaches coordinators on `port` of their registered hosts, for
    /// harnesses that serve admin messages apart from replication.
    #[must_use]
    pub fn with_admin_port(mut self, port: NonZeroU16) -> Self {
        self.admin_port = Some(port);
        self
    }

    /// Follows what the heartbeater achieves.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<HeartbeatStatus> {
        self.status.subscribe()
    }

    /// Registers the node, then sends heartbeats every interval, for as
    /// long as the task runs.
    pub async fn run(mut self) {
        loop {
            self.round().await;
            self.clock.sleep(self.config.interval).await;
        }
    }

    /// Registers the node now ([`register`]), for a node that starts
    /// serving only once it is registered. [`Heartbeater::run`] then
    /// registers again only when a coordinator says it must.
    ///
    /// # Errors
    ///
    /// [`RegistrationError`]; [`Heartbeater::run`] keeps trying. A write
    /// left unsettled is kept and settled before the next attempt, so that
    /// a registration that lands late is still announced.
    pub async fn register(&mut self) -> Result<Registered, RegistrationError> {
        if let Some(pending) = &self.unsettled {
            let settled = settle(
                &self.store,
                &self.cluster,
                pending,
                &mut self.proposals,
                &self.config.retry,
            )
            .await;
            if let Err(source) = settled {
                return Err(RegistrationError::Unsettled {
                    pending: Box::new(pending.clone()),
                    source,
                });
            }
            self.unsettled = None;
        }
        let registered = register(
            &self.store,
            &self.cluster,
            &self.profile,
            &mut self.proposals,
            &self.config.retry,
        )
        .await;
        let registered = match registered {
            Err(RegistrationError::Unsettled { pending, source }) => {
                self.unsettled = Some((*pending).clone());
                return Err(RegistrationError::Unsettled { pending, source });
            }
            registered => registered?,
        };
        self.registered = true;
        self.status.send_modify(|status| status.registrations += 1);
        Ok(registered)
    }

    /// Registers the node if it must, and sends one heartbeat if it is
    /// registered.
    async fn round(&mut self) {
        if !self.registered
            && let Err(error) = self.register().await
        {
            tracing::warn!(%error, "cannot register the node; trying again");
            return;
        }
        self.beat().await;
    }

    /// Sends one heartbeat to the coordinator, if one is known.
    async fn beat(&mut self) {
        let Some(coordinator) = self.coordinator().await else {
            return;
        };
        let ack = match &self.local {
            Some((registry, leadership)) if coordinator == self.profile.node => Ok(HeartbeatAck {
                // A node whose tenure ended looks the coordinator up again.
                coordinator: leadership.borrow().is_coordinator_at(self.clock.now()),
                unregistered: !registry.heard(&coordinator),
            }),
            _ => self.exchange().await,
        };
        match ack {
            Ok(ack) => {
                if ack.coordinator {
                    self.status.send_modify(|status| {
                        status.delivered += 1;
                        status.coordinator = Some(coordinator);
                    });
                } else {
                    tracing::debug!(%coordinator, "the lease holder no longer coordinates");
                    self.target = None;
                }
                if ack.unregistered {
                    tracing::info!("the coordinator does not know this node; registering again");
                    self.registered = false;
                }
            }
            Err(error) => {
                tracing::debug!(%coordinator, %error, "a heartbeat failed");
                self.target = None;
            }
        }
    }

    /// The coordinator to send heartbeats to: the one already known, or
    /// the lease holder, looked up at most every `resolve_interval`.
    async fn coordinator(&mut self) -> Option<NodeId> {
        if let Some(target) = &self.target {
            return Some(target.node.clone());
        }
        let now = self.clock.now();
        if self
            .looked_up
            .is_some_and(|at| now.saturating_duration_since(at) < self.config.resolve_interval)
        {
            return None;
        }
        self.looked_up = Some(now);
        let holder = match read(&self.store, &TypedKey::coordinator_lease()).await {
            Ok(lease) => lease?.value.holder,
            Err(error) => {
                tracing::debug!(%error, "cannot read the coordinator lease");
                return None;
            }
        };
        let address = if holder == self.profile.node {
            self.profile.address.clone()
        } else {
            match read(&self.store, &TypedKey::node(&holder)).await {
                Ok(registration) => registration?.value.address,
                Err(error) => {
                    tracing::debug!(%holder, %error, "cannot read the coordinator's registration");
                    return None;
                }
            }
        };
        let address = match self.admin_port {
            Some(port) => NodeAddress::new(address.host().clone(), port),
            None => address,
        };
        self.target = Some(Target {
            node: holder.clone(),
            address,
            connection: None,
        });
        Some(holder)
    }

    /// Sends a heartbeat to the coordinator over its connection, opened
    /// if need be, and waits for the answer.
    async fn exchange(&mut self) -> Result<HeartbeatAck, String> {
        let request = self.requests;
        self.requests += 1;
        let (transport, timeout) = (&self.transport, self.config.timeout);
        let Some(target) = self.target.as_mut() else {
            return Err("no coordinator".to_owned());
        };
        let exchanged = tokio::time::timeout(timeout, async {
            if target.connection.is_none() {
                let connection = transport
                    .connect(&target.node, &target.address)
                    .await
                    .map_err(|error| error.to_string())?;
                target.connection = Some(connection);
            }
            let connection = target
                .connection
                .as_mut()
                .ok_or_else(|| "no connection".to_owned())?;
            connection
                .send(&Heartbeat::frame(request))
                .await
                .map_err(|error| error.to_string())?;
            match connection.recv().await {
                Ok(Some(frame)) => {
                    HeartbeatAck::from_frame(&frame, request).map_err(|error| error.to_string())
                }
                Ok(None) => Err("the coordinator closed the connection".to_owned()),
                Err(error) => Err(error.to_string()),
            }
        })
        .await
        .unwrap_or_else(|_| Err(format!("no answer within {timeout:?}")));
        if exchanged.is_err() {
            target.connection = None;
        }
        exchanged
    }
}

#[cfg(test)]
mod tests;
