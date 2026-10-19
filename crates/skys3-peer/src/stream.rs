//! Messages on a QUIC stream, and the destination's authorization of what
//! a source sends on one.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::BytesMut;
use quinn::{ReadExactError, RecvStream, SendStream, VarInt};
use skys3_types::{BucketName, ClusterId, WriteIdentity};

use crate::error::MessageError;
use crate::frame::{PREFIX_LEN, parse_prefix};
use crate::message::{Abort, AbortReason, Applied, ApplyError, Batch, Message, Outcome};
use crate::negotiation::{ProtocolError, Session, Side};
use crate::trust::{PeerTrust, Unauthorized};

/// The application error code a destination stops a stream with when it
/// refuses what the stream carries.
pub const STREAM_REFUSED: VarInt = VarInt::from_u32(1);

/// Why a stream could not be used. After any error except
/// [`StreamError::Unauthorized`] the stream is unusable.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StreamError {
    /// The connection is closed or lost.
    #[error("connection error: {0}")]
    Connection(#[from] quinn::ConnectionError),
    /// Reading failed: the peer reset the stream, or the connection ended.
    #[error("cannot read: {0}")]
    Read(#[from] quinn::ReadError),
    /// Writing failed: the peer stopped the stream, or the connection
    /// ended.
    #[error("cannot write: {0}")]
    Write(#[from] quinn::WriteError),
    /// The stream is already finished or reset on this side.
    #[error("the stream is closed")]
    Closed(#[from] quinn::ClosedStream),
    /// The stream ended inside a frame.
    #[error("the stream ended inside a frame")]
    Truncated,
    /// A frame is malformed, or its message breaks a rule.
    #[error(transparent)]
    Message(#[from] MessageError),
    /// The message is not allowed in the session, or not in this place on
    /// the stream.
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    /// `DATA` arrived on a stream without an accepted `BEGIN`.
    #[error("DATA without a BEGIN on its stream")]
    DataWithoutBegin,
    /// A stream opened in 0-RTT. A server refuses early data, so this
    /// never happens unless that configuration is broken.
    #[error("the stream was opened in 0-RTT, which peer connections refuse")]
    ZeroRtt,
    /// The peer may not write what the message names. The destination has
    /// answered with the protocol's refusal; for a `BEGIN`, the stream is
    /// closed.
    #[error(transparent)]
    Unauthorized(#[from] Unauthorized),
    /// Another transport than QUIC failed, such as the one a simulation
    /// carries the protocol's frames over ([`Inbound`]).
    #[error("transport error: {0}")]
    Transport(String),
}

/// The destination's end of one stream, as
/// [`StagingService::serve`](crate::StagingService::serve) reads it: the
/// source's messages, each authorized before it is returned, and a sending
/// half that tasks share. [`InboundStream`] carries them over QUIC; a
/// simulation may carry the same frames over another transport.
pub trait Inbound: Send {
    /// The sending half.
    type Sender: Outbound;

    /// Receives the next authorized message, or `None` once the source
    /// finished the stream; see [`InboundStream::recv`].
    fn recv(&mut self) -> impl Future<Output = Result<Option<Message>, StreamError>> + Send;

    /// Another handle to the sending half, for a task that answers while
    /// the stream is read.
    fn sender(&self) -> Self::Sender;
}

/// The sending half of an [`Inbound`] stream. Each message is written whole
/// before the next one starts, whichever handle sends it.
pub trait Outbound: Clone + Send + Sync + 'static {
    /// Sends `message` to the source.
    fn send(&self, message: &Message) -> impl Future<Output = Result<(), StreamError>> + Send;

    /// Ends the stream in this direction once every message sent is
    /// delivered.
    fn finish(&self) -> impl Future<Output = Result<(), StreamError>> + Send;
}

/// Counts the open streams of a connection, so a pool can spread new
/// streams across its connections.
#[derive(Debug)]
pub(crate) struct StreamCount(Arc<AtomicUsize>);

impl StreamCount {
    pub(crate) fn new(count: &Arc<AtomicUsize>) -> Self {
        count.fetch_add(1, Ordering::Relaxed);
        Self(count.clone())
    }
}

impl Drop for StreamCount {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// The sending half of a [`MessageStream`].
#[derive(Debug)]
pub struct MessageSender {
    send: SendStream,
    session: Arc<Session>,
    side: Side,
    _count: Arc<StreamCount>,
}

impl MessageSender {
    /// Sends `message` as one frame.
    ///
    /// # Errors
    ///
    /// [`StreamError::Protocol`] for a message this side does not send in
    /// the session, the errors of [`Message::encode`], or a failed write.
    pub async fn send(&mut self, message: &Message) -> Result<(), StreamError> {
        self.session.check(message, self.side)?;
        self.send.write_all(&message.encode()?).await?;
        Ok(())
    }

    /// Ends the stream in this direction once everything sent is
    /// delivered.
    ///
    /// # Errors
    ///
    /// [`StreamError::Closed`] if it is already finished or reset.
    pub fn finish(&mut self) -> Result<(), StreamError> {
        Ok(self.send.finish()?)
    }
}

/// The receiving half of a [`MessageStream`].
#[derive(Debug)]
pub struct MessageReceiver {
    recv: RecvStream,
    session: Arc<Session>,
    peer_side: Side,
    _count: Arc<StreamCount>,
}

impl MessageReceiver {
    /// Receives the next message, or `None` once the peer finished the
    /// stream. Lengths are checked before anything is allocated, and a
    /// payload grows only as its bytes arrive.
    ///
    /// # Errors
    ///
    /// [`StreamError::Truncated`] for a stream that ends inside a frame,
    /// the errors of [`Message::decode_parts`], [`StreamError::Protocol`]
    /// for a message the peer's side does not send in the session, or a
    /// failed read.
    pub async fn recv(&mut self) -> Result<Option<Message>, StreamError> {
        let message = read_message(&mut self.recv).await?;
        if let Some(message) = &message {
            self.session.check(message, self.peer_side)?;
        }
        Ok(message)
    }

    /// Asks the peer to stop sending, with an application error `code`.
    pub fn stop(&mut self, code: VarInt) {
        // A stream already stopped or finished needs nothing more.
        let _ = self.recv.stop(code);
    }
}

/// Reads the next frame of `recv` and decodes its message, or `None` if
/// the stream ends between frames.
pub(crate) async fn read_message(recv: &mut RecvStream) -> Result<Option<Message>, StreamError> {
    let mut prefix = [0; PREFIX_LEN];
    match recv.read_exact(&mut prefix).await {
        Ok(()) => {}
        Err(ReadExactError::FinishedEarly(0)) => return Ok(None),
        Err(ReadExactError::FinishedEarly(_)) => return Err(StreamError::Truncated),
        Err(ReadExactError::ReadError(error)) => return Err(error.into()),
    }
    let (header_len, payload_len) = parse_prefix(&prefix)?;
    let mut header = vec![0; header_len];
    recv.read_exact(&mut header)
        .await
        .map_err(|error| match error {
            ReadExactError::FinishedEarly(_) => StreamError::Truncated,
            ReadExactError::ReadError(error) => error.into(),
        })?;
    let mut payload = BytesMut::new();
    while payload.len() < payload_len {
        let chunk = recv
            .read_chunk(payload_len - payload.len(), true)
            .await?
            .ok_or(StreamError::Truncated)?;
        payload.extend_from_slice(&chunk.bytes);
    }
    Ok(Some(Message::decode_parts(&header, payload.freeze())?))
}

/// A bidirectional stream of peer messages, checked against the session
/// in both directions.
#[derive(Debug)]
pub struct MessageStream {
    sender: MessageSender,
    receiver: MessageReceiver,
}

impl MessageStream {
    pub(crate) fn new(
        (send, recv): (SendStream, RecvStream),
        session: Arc<Session>,
        side: Side,
        count: StreamCount,
    ) -> Self {
        let peer_side = match side {
            Side::Source => Side::Destination,
            Side::Destination => Side::Source,
        };
        let count = Arc::new(count);
        Self {
            sender: MessageSender {
                send,
                session: session.clone(),
                side,
                _count: count.clone(),
            },
            receiver: MessageReceiver {
                recv,
                session,
                peer_side,
                _count: count,
            },
        }
    }

    /// Sends `message`; see [`MessageSender::send`].
    ///
    /// # Errors
    ///
    /// As [`MessageSender::send`].
    pub async fn send(&mut self, message: &Message) -> Result<(), StreamError> {
        self.sender.send(message).await
    }

    /// Receives the next message; see [`MessageReceiver::recv`].
    ///
    /// # Errors
    ///
    /// As [`MessageReceiver::recv`].
    pub async fn recv(&mut self) -> Result<Option<Message>, StreamError> {
        self.receiver.recv().await
    }

    /// Ends the stream in this direction; see [`MessageSender::finish`].
    ///
    /// # Errors
    ///
    /// As [`MessageSender::finish`].
    pub fn finish(&mut self) -> Result<(), StreamError> {
        self.sender.finish()
    }

    /// What the connection's `HELLO`s agreed on, such as whether the
    /// destination accepts `BATCH`.
    #[must_use]
    pub fn session(&self) -> &Session {
        &self.sender.session
    }

    /// Splits the stream, so one task can send while another receives. A
    /// source streams `DATA` while `DURABLE`s arrive, and neither may wait
    /// on the other: QUIC flow control stalls a sender whose peer does not
    /// read. The stream counts as open until both halves are dropped.
    #[must_use]
    pub fn split(self) -> (MessageSender, MessageReceiver) {
        (self.sender, self.receiver)
    }
}

/// A stream a source opened to this destination. Every message is
/// authorized before it is returned: its write identity must be the
/// peer's own, and its bucket pair one the peer may write. A refused
/// message is answered on the stream with the protocol's refusal and never
/// reaches the caller.
#[derive(Debug)]
pub struct InboundStream {
    receiver: MessageReceiver,
    sender: InboundSender,
    peer: ClusterId,
    trust: Arc<PeerTrust>,
    begun: bool,
}

/// The sending half of an [`InboundStream`], which tasks share: a
/// destination answers `DURABLE`s as appends finish while it goes on
/// receiving `DATA`. Each message is written whole before the next one
/// starts. Cloning it gives another handle to the same half.
#[derive(Debug, Clone)]
pub struct InboundSender(Arc<tokio::sync::Mutex<MessageSender>>);

impl InboundSender {
    /// Sends `message`; see [`MessageSender::send`].
    ///
    /// # Errors
    ///
    /// As [`MessageSender::send`].
    pub async fn send(&self, message: &Message) -> Result<(), StreamError> {
        self.0.lock().await.send(message).await
    }

    /// Ends the stream in this direction once every message sent is
    /// delivered; see [`MessageSender::finish`].
    ///
    /// # Errors
    ///
    /// As [`MessageSender::finish`].
    pub async fn finish(&self) -> Result<(), StreamError> {
        self.0.lock().await.finish()
    }
}

impl Outbound for InboundSender {
    async fn send(&self, message: &Message) -> Result<(), StreamError> {
        InboundSender::send(self, message).await
    }

    async fn finish(&self) -> Result<(), StreamError> {
        InboundSender::finish(self).await
    }
}

impl Inbound for InboundStream {
    type Sender = InboundSender;

    async fn recv(&mut self) -> Result<Option<Message>, StreamError> {
        InboundStream::recv(self).await
    }

    fn sender(&self) -> InboundSender {
        InboundStream::sender(self)
    }
}

impl InboundStream {
    pub(crate) fn new(stream: MessageStream, peer: ClusterId, trust: Arc<PeerTrust>) -> Self {
        let (sender, receiver) = stream.split();
        Self {
            receiver,
            sender: InboundSender(Arc::new(tokio::sync::Mutex::new(sender))),
            peer,
            trust,
            begun: false,
        }
    }

    /// Receives the next authorized message, or `None` once the source
    /// finished the stream.
    ///
    /// - A `BEGIN`, `COMMIT`, or `ABORT` of a pair the peer may not write
    ///   is answered with an `ABORT` or an `APPLIED` that says `refused`,
    ///   and returned as [`StreamError::Unauthorized`]. A refused `BEGIN`
    ///   also stops the stream, so the source's `DATA` goes nowhere.
    /// - A `BATCH` loses its refused items, each answered with a refusing
    ///   `APPLIED`; the rest is returned. If no item is left, the batch is
    ///   [`StreamError::Unauthorized`].
    /// - `DATA` is accepted only after an authorized `BEGIN` on the stream.
    ///
    /// # Errors
    ///
    /// [`StreamError::Unauthorized`] as above, after which the stream can
    /// still be read; [`StreamError::DataWithoutBegin`]; and the errors of
    /// [`MessageReceiver::recv`].
    pub async fn recv(&mut self) -> Result<Option<Message>, StreamError> {
        let Some(message) = self.receiver.recv().await? else {
            return Ok(None);
        };
        let refusal = match &message {
            Message::Begin(begin) => self.check(&begin.identity, Some(&begin.bucket)),
            Message::Commit(commit) => self.check(&commit.identity, Some(&commit.bucket)),
            Message::Abort(abort) => self.check(&abort.identity, None),
            Message::Batch(batch) => return self.authorize_batch(batch).await,
            Message::Data(_) if !self.begun => {
                self.receiver.stop(STREAM_REFUSED);
                return Err(StreamError::DataWithoutBegin);
            }
            _ => Ok(()),
        };
        if let Err(unauthorized) = refusal {
            self.refuse(&message, &unauthorized).await?;
            return Err(unauthorized.into());
        }
        if matches!(message, Message::Begin(_)) {
            self.begun = true;
        }
        Ok(Some(message))
    }

    /// Sends a message back to the source; see [`MessageSender::send`].
    ///
    /// # Errors
    ///
    /// As [`MessageSender::send`].
    pub async fn send(&self, message: &Message) -> Result<(), StreamError> {
        self.sender.send(message).await
    }

    /// Ends the stream in this direction; see [`MessageSender::finish`].
    ///
    /// # Errors
    ///
    /// As [`MessageSender::finish`].
    pub async fn finish(&self) -> Result<(), StreamError> {
        self.sender.finish().await
    }

    /// Another handle to the sending half, for a task that answers while
    /// this one receives.
    #[must_use]
    pub fn sender(&self) -> InboundSender {
        self.sender.clone()
    }

    /// The peer cluster the stream belongs to.
    #[must_use]
    pub fn peer(&self) -> &ClusterId {
        &self.peer
    }

    fn check(
        &self,
        identity: &WriteIdentity,
        destination: Option<&BucketName>,
    ) -> Result<(), Unauthorized> {
        self.trust.authorize(&self.peer, identity, destination)
    }

    /// Answers a refused `BEGIN`, `COMMIT`, or `ABORT`.
    async fn refuse(
        &mut self,
        message: &Message,
        unauthorized: &Unauthorized,
    ) -> Result<(), StreamError> {
        let detail = unauthorized.to_string();
        match message {
            Message::Commit(commit) => {
                self.send(&refused_applied(&commit.identity, detail))
                    .await?;
            }
            Message::Begin(begin) => {
                self.send(&Message::Abort(Abort {
                    identity: begin.identity.clone(),
                    reason: AbortReason::Refused,
                    detail,
                }))
                .await?;
                self.sender.finish().await?;
                self.receiver.stop(STREAM_REFUSED);
            }
            Message::Abort(abort) => {
                self.send(&Message::Abort(Abort {
                    identity: abort.identity.clone(),
                    reason: AbortReason::Refused,
                    detail,
                }))
                .await?;
            }
            _ => unreachable!("only BEGIN, COMMIT, and ABORT are refused whole"),
        }
        Ok(())
    }

    /// Answers each refused item of a batch, and returns the others.
    async fn authorize_batch(&mut self, batch: &Batch) -> Result<Option<Message>, StreamError> {
        let mut allowed = Vec::with_capacity(batch.items.len());
        let mut first_refusal = None;
        for item in &batch.items {
            match self.check(&item.identity, Some(&item.bucket)) {
                Ok(()) => allowed.push(item.clone()),
                Err(unauthorized) => {
                    self.send(&refused_applied(&item.identity, unauthorized.to_string()))
                        .await?;
                    first_refusal.get_or_insert(unauthorized);
                }
            }
        }
        match first_refusal {
            Some(unauthorized) if allowed.is_empty() => Err(unauthorized.into()),
            _ => Ok(Some(Message::Batch(Batch { items: allowed }))),
        }
    }
}

fn refused_applied(identity: &WriteIdentity, reason: String) -> Message {
    Message::Applied(Applied {
        identity: identity.clone(),
        outcome: Outcome::Failed {
            error: ApplyError::Refused,
            reason,
        },
    })
}
