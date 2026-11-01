//! How a flusher reaches a SkyS3 destination: streams of peer messages
//! (§7.8), whatever carries them.
//!
//! [`PeerLink`] opens one stream per object or batch. The node's link is a
//! [`ShardLease`] of the QUIC connection pool (plan M6-02); a simulation
//! carries the same frames over its own network. Every wait on a stream is
//! bounded by the target's answer timeout ([`Stream`]), so a link that
//! goes silent fails the flush, which is retried, instead of holding its
//! slot.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use skys3_peer::{Capabilities, Message, MessageReceiver, MessageSender, ShardLease};

/// A boxed future, as the object-safe link traits return.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Why a stream to the destination failed: it could not be opened, a
/// message could not be sent or received, or no answer came in time.
/// Whatever was sent may or may not have been applied.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct LinkError(String);

impl LinkError {
    /// A failure explained by `reason`.
    pub fn new(reason: impl fmt::Display) -> Self {
        Self(reason.to_string())
    }
}

/// The source's end of the peer protocol to one destination cluster.
pub trait PeerLink: Send + Sync + 'static {
    /// Opens a new stream to the destination, for one object or one batch.
    fn open(&self) -> BoxFuture<'_, Result<PeerStream, LinkError>>;
}

/// The sending half of a stream.
pub trait PeerSend: Send {
    /// Sends `message`.
    fn send<'a>(&'a mut self, message: &'a Message) -> BoxFuture<'a, Result<(), LinkError>>;

    /// Ends the stream in this direction once everything sent is
    /// delivered.
    fn finish(&mut self) -> Result<(), LinkError>;
}

/// The receiving half of a stream.
pub trait PeerReceive: Send {
    /// The destination's next message, or `None` once it finished the
    /// stream.
    fn recv(&mut self) -> BoxFuture<'_, Result<Option<Message>, LinkError>>;
}

/// A stream to the destination, split so that one task may send frames
/// while answers arrive.
pub struct PeerStream {
    /// What the source sends.
    pub sender: Box<dyn PeerSend>,
    /// What the destination answers.
    pub receiver: Box<dyn PeerReceive>,
    /// Whether the session accepts `BATCH`; without it, small objects are
    /// staged as large ones are.
    pub batches: bool,
}

impl fmt::Debug for PeerStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerStream")
            .field("batches", &self.batches)
            .finish_non_exhaustive()
    }
}

impl PeerLink for ShardLease {
    fn open(&self) -> BoxFuture<'_, Result<PeerStream, LinkError>> {
        Box::pin(async move {
            let stream = self.open_stream().await.map_err(LinkError::new)?;
            let batches = stream.session().capabilities.contains(Capabilities::BATCH);
            let (sender, receiver) = stream.split();
            Ok(PeerStream {
                sender: Box::new(sender),
                receiver: Box::new(receiver),
                batches,
            })
        })
    }
}

impl PeerSend for MessageSender {
    fn send<'a>(&'a mut self, message: &'a Message) -> BoxFuture<'a, Result<(), LinkError>> {
        Box::pin(async move {
            MessageSender::send(self, message)
                .await
                .map_err(LinkError::new)
        })
    }

    fn finish(&mut self) -> Result<(), LinkError> {
        MessageSender::finish(self).map_err(LinkError::new)
    }
}

impl PeerReceive for MessageReceiver {
    fn recv(&mut self) -> BoxFuture<'_, Result<Option<Message>, LinkError>> {
        Box::pin(async move { MessageReceiver::recv(self).await.map_err(LinkError::new) })
    }
}

/// A stream whose every send and receive must finish within `timeout`.
pub(crate) struct Stream {
    inner: PeerStream,
    timeout: Duration,
}

impl Stream {
    /// Opens a stream on `link`, within `timeout`.
    pub(crate) async fn open(link: &dyn PeerLink, timeout: Duration) -> Result<Self, LinkError> {
        let inner = within(timeout, "opening a stream", link.open()).await?;
        Ok(Self { inner, timeout })
    }

    /// Whether the session accepts `BATCH`.
    pub(crate) fn batches(&self) -> bool {
        self.inner.batches
    }

    pub(crate) async fn send(&mut self, message: &Message) -> Result<(), LinkError> {
        let sending = self.inner.sender.send(message);
        within(self.timeout, message.name(), sending).await
    }

    pub(crate) fn finish(&mut self) -> Result<(), LinkError> {
        self.inner.sender.finish()
    }

    /// The next message, or `None` once the destination finished the
    /// stream.
    pub(crate) async fn recv(&mut self) -> Result<Option<Message>, LinkError> {
        let receiving = self.inner.receiver.recv();
        within(self.timeout, "an answer", receiving).await
    }
}

/// `future`'s result, or a [`LinkError`] if it takes longer than
/// `timeout`.
async fn within<T>(
    timeout: Duration,
    what: &str,
    future: impl Future<Output = Result<T, LinkError>>,
) -> Result<T, LinkError> {
    match tokio::time::timeout(timeout, future).await {
        Ok(result) => result,
        Err(_) => Err(LinkError::new(format!(
            "no progress on {what} within {timeout:?}"
        ))),
    }
}
