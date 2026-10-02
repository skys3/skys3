//! Planned handoffs on the coordinator's request (design §5.4, §6.7): the
//! [`MessageKind::Handoff`] admin message, the node side that starts the
//! handoff ([`HandoffSink`]), and the coordinator side that asks for it
//! ([`HandoffClient`]).
//!
//! Only a shard's primary can hand the shard off: it steps down, and the
//! member it names proposes itself (rule R1). So rebalancing asks the
//! primary's node, over the intra-cluster transport, to hand one shard
//! off to one member, in the epoch the coordinator read:
//!
//! ```text
//! coordinator                            primary's node
//!   Handoff(bucket, shard, epoch, to), id n   ->
//!                                      <- AdminReply(HandoffAck), id n
//! ```
//!
//! The answer says whether the node started the handoff. A node starts it
//! only if its replica is the shard's serving primary in the epoch the
//! request names and `to` is another member, so a request planned from an
//! older register, or by a node that no longer is coordinator, does
//! nothing once the shard has moved on. A request is advice, like a push:
//! the handoff itself checks the register and refuses while a promotion is
//! outstanding (§6.7), and every configuration it leads to is a
//! compare-and-swap, so a lost, late, repeated, or forged request can cost
//! a handoff at most, never safety.

use std::future::Future;
use std::num::NonZeroU16;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use prost::Message;
use skys3_net::{Frame, Header, MessageKind, Network, Transport};
use skys3_types::{BucketId, Epoch, NodeAddress, NodeId, ShardId};

/// A request that a shard's primary hand the shard off to the member `to`,
/// in `epoch`: the typed body of a [`MessageKind::Handoff`] frame.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Handoff {
    /// The shard's bucket.
    pub bucket: BucketId,
    /// The shard.
    pub shard: ShardId,
    /// The epoch of the configuration the request was planned from. A
    /// primary in another epoch refuses it.
    pub epoch: Epoch,
    /// The member that is to propose itself as the next primary.
    pub to: NodeId,
}

/// The wire body of a [`MessageKind::Handoff`] frame. Its payload is empty.
#[derive(Clone, PartialEq, Eq, Message)]
struct HandoffBody {
    #[prost(string, tag = "1")]
    bucket_id: String,
    #[prost(uint32, tag = "2")]
    shard: u32,
    #[prost(uint64, tag = "3")]
    epoch: u64,
    #[prost(string, tag = "4")]
    to: String,
}

/// The body of the [`MessageKind::AdminReply`] that answers a
/// [`Handoff`].
#[derive(Clone, PartialEq, Eq, Message)]
pub struct HandoffAck {
    /// Whether the node started the handoff.
    #[prost(bool, tag = "1")]
    pub started: bool,
    /// Why it did not, for logs.
    #[prost(string, tag = "2")]
    pub reason: String,
}

/// Why a handoff request failed, or a frame is not a valid one.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum HandoffError {
    /// The frame is of another kind.
    #[error("expected a {expected:?} frame, not {found:?}")]
    Kind {
        /// The kind expected.
        expected: MessageKind,
        /// The kind found.
        found: MessageKind,
    },
    /// The frame carries a payload.
    #[error("a handoff frame carries no payload, but this one has {0} bytes")]
    Payload(usize),
    /// The body is not a valid protobuf message.
    #[error("malformed handoff body: {0}")]
    Malformed(String),
    /// A field of the body is not valid.
    #[error("invalid handoff {field}: {reason}")]
    Invalid {
        /// The field.
        field: &'static str,
        /// What is wrong with it.
        reason: String,
    },
    /// An answer to another request.
    #[error("an answer to request {found}, not {expected}")]
    RequestId {
        /// The request sent.
        expected: u64,
        /// The request answered.
        found: u64,
    },
    /// The primary's node did not start the handoff.
    #[error("the handoff was refused: {0}")]
    Refused(String),
    /// The request did not reach the node, or its answer did not arrive.
    #[error("the handoff request failed: {0}")]
    Unanswered(String),
}

fn invalid(field: &'static str, reason: impl ToString) -> HandoffError {
    HandoffError::Invalid {
        field,
        reason: reason.to_string(),
    }
}

/// Checks a frame's kind and payload, and decodes its body.
fn decode<M: Message + Default>(frame: &Frame, expected: MessageKind) -> Result<M, HandoffError> {
    if frame.header.kind != expected {
        return Err(HandoffError::Kind {
            expected,
            found: frame.header.kind,
        });
    }
    if !frame.payload.is_empty() {
        return Err(HandoffError::Payload(frame.payload.len()));
    }
    M::decode(frame.header.body.clone()).map_err(|error| HandoffError::Malformed(error.to_string()))
}

impl Handoff {
    /// The request as a frame with `request_id`.
    #[must_use]
    pub fn frame(&self, request_id: u64) -> Frame {
        let body = HandoffBody {
            bucket_id: self.bucket.to_string(),
            shard: u32::from(self.shard.get()),
            epoch: self.epoch.get(),
            to: self.to.to_string(),
        };
        let header = Header::new(MessageKind::Handoff)
            .with_request_id(request_id)
            .with_body(body.encode_to_vec());
        Frame::new(header, Bytes::new())
    }

    /// The request a frame from a peer carries.
    ///
    /// # Errors
    ///
    /// [`HandoffError`] if the frame is not a well-formed handoff request:
    /// of another kind, with a payload, a malformed body, an invalid
    /// bucket or node ID, a shard number above 255, or epoch 0.
    pub fn from_frame(frame: &Frame) -> Result<Self, HandoffError> {
        let body: HandoffBody = decode(frame, MessageKind::Handoff)?;
        let bucket = BucketId::new(body.bucket_id).map_err(|error| invalid("bucket", error))?;
        let shard = u8::try_from(body.shard)
            .map(ShardId::new)
            .map_err(|_| invalid("shard", format!("{} is above 255", body.shard)))?;
        if body.epoch == 0 {
            return Err(invalid("epoch", "a configuration's epoch is at least 1"));
        }
        let to = NodeId::new(body.to).map_err(|error| invalid("to", error))?;
        Ok(Self {
            bucket,
            shard,
            epoch: Epoch::new(body.epoch),
            to,
        })
    }
}

impl HandoffAck {
    /// An answer that the handoff started.
    #[must_use]
    pub fn started() -> Self {
        Self {
            started: true,
            reason: String::new(),
        }
    }

    /// An answer that the handoff did not start, and why.
    #[must_use]
    pub fn refused(reason: impl Into<String>) -> Self {
        Self {
            started: false,
            reason: reason.into(),
        }
    }

    /// The answer to the request with `request_id`.
    #[must_use]
    pub fn frame(&self, request_id: u64) -> Frame {
        let header = Header::new(MessageKind::AdminReply)
            .with_request_id(request_id)
            .with_body(self.encode_to_vec());
        Frame::new(header, Bytes::new())
    }

    /// The answer a frame gives to the request with `request_id`.
    ///
    /// # Errors
    ///
    /// [`HandoffError`] if it is not a well-formed answer to it.
    pub fn from_frame(frame: &Frame, request_id: u64) -> Result<Self, HandoffError> {
        let ack = decode(frame, MessageKind::AdminReply)?;
        if frame.header.request_id != request_id {
            return Err(HandoffError::RequestId {
                expected: request_id,
                found: frame.header.request_id,
            });
        }
        Ok(ack)
    }
}

/// A boxed future, for the object-safe [`HandoffSink`].
pub type HandoffFuture<'a> = Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

/// What starts a planned handoff on a node, given a [`Handoff`] request:
/// the extension point between the admin endpoint and the node's
/// replication (plan M2-13's `Replication::hand_off`).
pub trait HandoffSink: Send + Sync + 'static {
    /// Starts the handoff `handoff` asks for, if this node's replica of the
    /// shard is its serving primary in `handoff.epoch` and `handoff.to` is
    /// another member. Returns once the handoff has started, without
    /// waiting for it to end, or says why it did not start.
    fn hand_off(&self, handoff: Handoff) -> HandoffFuture<'_>;
}

/// Sends [`Handoff`] requests: the extension point between rebalancing and
/// how primaries are reached.
pub trait RequestHandoff: Send + Sync + 'static {
    /// Asks `primary`, at `address`, to start `handoff`, and waits for its
    /// answer. The future owns what it needs, so the caller can spawn it.
    fn request(
        &self,
        primary: NodeId,
        address: NodeAddress,
        handoff: Handoff,
    ) -> impl Future<Output = Result<(), HandoffError>> + Send + 'static;
}

/// Sends [`Handoff`] requests over the intra-cluster transport, each on a
/// connection of its own, bounded by a timeout.
///
/// Clones share the request IDs.
#[derive(Clone)]
pub struct HandoffClient<N> {
    transport: Transport<N>,
    timeout: Duration,
    admin_port: Option<NonZeroU16>,
    requests: Arc<AtomicU64>,
}

impl<N> std::fmt::Debug for HandoffClient<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HandoffClient")
            .field("timeout", &self.timeout)
            .field("admin_port", &self.admin_port)
            .finish_non_exhaustive()
    }
}

impl<N: Network> HandoffClient<N> {
    /// A client that reaches primaries through `transport`, giving each
    /// request `timeout` to connect, send, and be answered.
    #[must_use]
    pub fn new(transport: Transport<N>, timeout: Duration) -> Self {
        Self {
            transport,
            timeout,
            admin_port: None,
            requests: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Sends requests to `port` on each primary's host rather than to the
    /// port of its registered address, for nodes that serve admin messages
    /// on a port of their own.
    #[must_use]
    pub fn with_admin_port(mut self, port: NonZeroU16) -> Self {
        self.admin_port = Some(port);
        self
    }

    /// Asks `primary` at `address` to start `handoff`.
    ///
    /// # Errors
    ///
    /// [`HandoffError::Refused`] if the node did not start it,
    /// [`HandoffError::Unanswered`] if the request or its answer was lost
    /// or timed out, and the decoding errors of a malformed answer.
    pub async fn send(
        &self,
        primary: &NodeId,
        address: &NodeAddress,
        handoff: &Handoff,
    ) -> Result<(), HandoffError> {
        let address = match self.admin_port {
            Some(port) => NodeAddress::new(address.host().clone(), port),
            None => address.clone(),
        };
        let request = self.requests.fetch_add(1, Ordering::Relaxed);
        let unanswered =
            |error: &dyn std::fmt::Display| HandoffError::Unanswered(error.to_string());
        let exchange = async {
            let mut connection = self
                .transport
                .connect(primary, &address)
                .await
                .map_err(|error| unanswered(&error))?;
            connection
                .send(&handoff.frame(request))
                .await
                .map_err(|error| unanswered(&error))?;
            let answer = connection
                .recv()
                .await
                .map_err(|error| unanswered(&error))?;
            // The peer closes when it sees the end of the stream.
            let _ = connection.close().await;
            let answer = answer.ok_or_else(|| unanswered(&"the peer closed the connection"))?;
            HandoffAck::from_frame(&answer, request)
        };
        let ack = tokio::time::timeout(self.timeout, exchange)
            .await
            .unwrap_or_else(|_| {
                Err(unanswered(&format_args!(
                    "no answer within {:?}",
                    self.timeout
                )))
            })?;
        if ack.started {
            Ok(())
        } else {
            Err(HandoffError::Refused(ack.reason))
        }
    }
}

impl<N: Network> RequestHandoff for HandoffClient<N> {
    fn request(
        &self,
        primary: NodeId,
        address: NodeAddress,
        handoff: Handoff,
    ) -> impl Future<Output = Result<(), HandoffError>> + Send + 'static {
        let client = self.clone();
        async move { client.send(&primary, &address, &handoff).await }
    }
}

#[cfg(test)]
mod tests;
