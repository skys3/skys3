//! The destination's end of object streams: staging frames on the shard
//! primaries and reporting what is durable (design §7.8).
//!
//! ```text
//! source              destination node (gateway)           shard primary
//!   BEGIN        ->     Staging::begin
//!                <-     RESUME (durable ranges)
//!   DATA         ->     Staging::admit (trim)       ->     EXTENT on every member
//!   DATA         ->     Staging::admit (trim)       ->     EXTENT on every member
//!                                                   <-     durable, applied
//!                <-     DURABLE (cumulative)
//! ```
//!
//! The source never waits for a frame: up to [`MAX_APPENDS_PER_STREAM`]
//! frames of a stream are in flight to the primaries at once, and the
//! `DURABLE`s for every append that finished while the last one was being
//! sent go out as one.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::Bytes;
use skys3_log::record::ExtentRef;
use skys3_types::{BucketName, WriteIdentity};
use tokio::sync::{Semaphore, mpsc};
use tokio::time::Instant;

use crate::endpoint::PeerConnection;
use crate::message::{Abort, AbortReason, Applied, ApplyError, Begin, Data, Message, Outcome};
use crate::staging::{Admitted, Staging};
use crate::stream::{InboundSender, InboundStream, StreamError};

/// The most frames of one stream being staged at once. A stream whose
/// frames are all in flight is not read until one finishes, so QUIC flow
/// control holds the source back.
pub const MAX_APPENDS_PER_STREAM: u32 = 16;

/// Why an extent was not staged.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SinkError {
    /// The destination will never stage it, for example because the bucket
    /// does not exist or is being deleted. The staging is discarded, and
    /// the source is told with `ABORT` (`refused`).
    #[error("refused: {0}")]
    Refused(String),
    /// The primary could not stage it now, or did not acknowledge it in
    /// time. It is not reported durable, so the source sends it again
    /// after its next `RESUME`.
    #[error("unavailable: {0}")]
    Unavailable(String),
}

/// Where staged frames go: the primary of the shard that holds the key.
pub trait ExtentSink: Clone + Send + Sync + 'static {
    /// Commits `data`, the bytes at `offset` of a piece of `key` in the
    /// destination `bucket`, as an `EXTENT` record of the key's shard, and
    /// returns its reference once it is durable on every member of the
    /// shard and applied. No entry references the record, so no client
    /// sees it.
    fn append(
        &self,
        bucket: &BucketName,
        key: &str,
        offset: u64,
        data: Bytes,
    ) -> impl Future<Output = Result<ExtentRef, SinkError>> + Send;
}

/// How a stream's appends report back: to the reporter, which answers the
/// source, and to the stream's loop, which drops the `DATA` of staging an
/// append got refused.
#[derive(Debug, Clone)]
struct Reports {
    events: mpsc::UnboundedSender<Event>,
    refused: Arc<Mutex<BTreeSet<WriteIdentity>>>,
}

impl Reports {
    fn refused(&self) -> MutexGuard<'_, BTreeSet<WriteIdentity>> {
        self.refused.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// What an append tells the stream's reporter.
#[derive(Debug)]
enum Event {
    /// The durable ranges of a piece grew.
    Grew(WriteIdentity, u64),
    /// The destination refused the staging, and discarded it.
    Refused(WriteIdentity, String),
}

/// Serves the object streams of source clusters: stages their frames
/// through an [`ExtentSink`] and keeps the index of what is staged in a
/// [`Staging`] that every stream of the node shares. Cloning it gives
/// another handle to the same service.
pub struct StagingService<S> {
    staging: Arc<Staging>,
    sink: S,
}

impl<S: Clone> Clone for StagingService<S> {
    fn clone(&self) -> Self {
        Self {
            staging: Arc::clone(&self.staging),
            sink: self.sink.clone(),
        }
    }
}

impl<S> fmt::Debug for StagingService<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StagingService")
            .field("staging", &self.staging)
            .finish_non_exhaustive()
    }
}

impl<S: ExtentSink> StagingService<S> {
    /// A service that records staging in `staging` and stages frames
    /// through `sink`.
    #[must_use]
    pub fn new(staging: Arc<Staging>, sink: S) -> Self {
        Self { staging, sink }
    }

    /// The staging the service keeps.
    #[must_use]
    pub fn staging(&self) -> &Arc<Staging> {
        &self.staging
    }

    /// Serves every stream a source opens on `connection`, each on its own
    /// task, until the connection closes.
    pub async fn serve_connection(&self, connection: PeerConnection) {
        loop {
            match connection.accept_stream().await {
                Ok(stream) => {
                    let service = self.clone();
                    tokio::spawn(async move {
                        if let Err(error) = service.serve(stream).await {
                            tracing::debug!(%error, "a peer stream ended with an error");
                        }
                    });
                }
                Err(StreamError::ZeroRtt) => {}
                Err(_) => return,
            }
        }
    }

    /// Serves one stream until the source finishes it, then waits for its
    /// frames in flight, reports them, and finishes the stream.
    ///
    /// - `BEGIN` opens or resumes the staging of its identity and is
    ///   answered with its `RESUME`, or with `ABORT` (`refused`) if the
    ///   identity is staged for another bucket or key.
    /// - `DATA` is staged where the staging lacks it. If it cannot be, the
    ///   staging is gone: the source gets an `ABORT` with the reason, and
    ///   the stream's later `DATA` is dropped until a new `BEGIN`.
    /// - `ABORT` discards the staging it names.
    /// - `COMMIT` and `BATCH` items are answered `unavailable`: this
    ///   destination does not apply them yet.
    ///
    /// Frames in flight when the stream fails still settle into the
    /// staging, so the next `RESUME` reports them.
    ///
    /// # Errors
    ///
    /// The stream's errors, other than refused messages, which
    /// [`InboundStream`] answers itself.
    pub async fn serve(&self, mut stream: InboundStream) -> Result<(), StreamError> {
        let (events, received) = mpsc::unbounded_channel();
        let reports = Reports {
            events,
            refused: Arc::default(),
        };
        let reporter = tokio::spawn(report(Arc::clone(&self.staging), stream.sender(), received));
        let appends = Arc::new(Semaphore::new(MAX_APPENDS_PER_STREAM as usize));
        let mut current: Option<Begin> = None;
        loop {
            let message = match stream.recv().await {
                Ok(Some(message)) => message,
                Ok(None) => break,
                Err(StreamError::Unauthorized(_)) => continue,
                Err(error) => return Err(error),
            };
            match message {
                Message::Begin(begin) => {
                    current = match self.staging.begin(&begin, Instant::now()) {
                        Ok(resume) => {
                            reports.refused().remove(&begin.identity);
                            stream.send(&Message::Resume(resume)).await?;
                            Some(begin)
                        }
                        Err(reason) => {
                            let message = abort(&begin.identity, reason, discarded(reason));
                            stream.send(&message).await?;
                            None
                        }
                    };
                }
                Message::Data(data) => {
                    let Some(begin) = &current else {
                        continue;
                    };
                    // The reporter has aborted it.
                    if reports.refused().remove(&begin.identity) {
                        current = None;
                        continue;
                    }
                    // Nothing waits between admitting a frame and starting its
                    // append, so what the staging has in flight always lands.
                    let permit = Arc::clone(&appends)
                        .acquire_owned()
                        .await
                        .expect("the semaphore is never closed");
                    match self.staging.admit(&begin.identity, &data, Instant::now()) {
                        Ok(admitted) => {
                            let append = self.append(begin, &data, admitted, reports.clone());
                            tokio::spawn(async move {
                                append.await;
                                drop(permit);
                            });
                        }
                        Err(reason) => {
                            let message = abort(&begin.identity, reason, discarded(reason));
                            stream.send(&message).await?;
                            current = None;
                        }
                    }
                }
                Message::Abort(abort) => {
                    self.staging.discard(&abort.identity);
                    if current
                        .as_ref()
                        .is_some_and(|b| b.identity == abort.identity)
                    {
                        current = None;
                    }
                }
                Message::Commit(commit) => stream.send(&not_applied(commit.identity)).await?,
                Message::Batch(batch) => {
                    for item in batch.items {
                        stream.send(&not_applied(item.identity)).await?;
                    }
                }
                // The session refuses every other message from a source
                // before it gets here.
                _ => {}
            }
        }
        let _all = appends
            .acquire_many(MAX_APPENDS_PER_STREAM)
            .await
            .expect("the semaphore is never closed");
        drop(reports);
        // The reporter ends once every append has reported.
        let _ = reporter.await;
        stream.finish().await
    }

    /// Stages the parts of `data` that the staging of `begin` admitted,
    /// one after the other, and tells the reporter what changed.
    fn append(
        &self,
        begin: &Begin,
        data: &Data,
        admitted: Admitted,
        reports: Reports,
    ) -> impl Future<Output = ()> + Send + 'static {
        let (staging, sink) = (Arc::clone(&self.staging), self.sink.clone());
        let Begin {
            identity,
            bucket,
            key,
        } = begin.clone();
        let data = data.clone();
        async move {
            let (piece, generation) = (data.piece, admitted.generation);
            for range in admitted.ranges {
                let start = usize::try_from(range.start - data.offset).expect("within a frame");
                let end = usize::try_from(range.end - data.offset).expect("within a frame");
                let bytes = data.bytes.slice(start..end);
                let result = sink.append(&bucket, &key, range.start, bytes).await;
                let extent = result.as_ref().ok().copied();
                let grew = staging.settle(&identity, generation, piece, range, extent);
                // The reporter is gone only once the stream is.
                match result {
                    Ok(_) if grew => {
                        let _ = reports.events.send(Event::Grew(identity.clone(), piece));
                    }
                    Ok(_) => {}
                    Err(SinkError::Unavailable(reason)) => {
                        tracing::debug!(%identity, piece, %reason, "a staged frame was not acknowledged");
                    }
                    Err(SinkError::Refused(reason)) => {
                        // The staging is gone, and the rest of the frame
                        // with it.
                        if staging.discard(&identity) {
                            reports.refused().insert(identity.clone());
                            let _ = reports.events.send(Event::Refused(identity, reason));
                        }
                        return;
                    }
                }
            }
        }
    }
}

/// Sends a `DURABLE` for the pieces that grew, one per identity for
/// everything that arrived while the last was sent, and an `ABORT` for
/// staging the destination refused, until every append has reported.
/// Once the stream fails, it keeps receiving without sending.
async fn report(
    staging: Arc<Staging>,
    sender: InboundSender,
    mut received: mpsc::UnboundedReceiver<Event>,
) {
    let mut open = true;
    while let Some(first) = received.recv().await {
        let mut grown: BTreeMap<WriteIdentity, BTreeSet<u64>> = BTreeMap::new();
        let mut refused = Vec::new();
        for event in std::iter::once(first).chain(std::iter::from_fn(|| received.try_recv().ok())) {
            match event {
                Event::Grew(identity, piece) => {
                    grown.entry(identity).or_default().insert(piece);
                }
                Event::Refused(identity, reason) => refused.push((identity, reason)),
            }
        }
        let messages = grown
            .into_iter()
            .filter_map(|(identity, pieces)| staging.report(&identity, pieces))
            .map(Message::Durable)
            .chain(
                refused
                    .into_iter()
                    .map(|(identity, reason)| abort(&identity, AbortReason::Refused, &reason)),
            );
        for message in messages {
            if open && sender.send(&message).await.is_err() {
                open = false;
            }
        }
    }
}

/// An `ABORT` from the destination.
fn abort(identity: &WriteIdentity, reason: AbortReason, detail: &str) -> Message {
    let mut detail = detail.to_owned();
    if detail.len() > crate::message::MAX_REASON_LEN {
        let mut end = crate::message::MAX_REASON_LEN;
        while !detail.is_char_boundary(end) {
            end -= 1;
        }
        detail.truncate(end);
    }
    Message::Abort(Abort {
        identity: identity.clone(),
        reason,
        detail,
    })
}

/// Why the staging was discarded, for an `ABORT` that answers `DATA`.
fn discarded(reason: AbortReason) -> &'static str {
    match reason {
        AbortReason::QuotaExceeded => "the source's staged bytes would exceed the quota",
        AbortReason::Refused => {
            "the identity is staged for another bucket or key, or the piece would need more \
             extents than an object references"
        }
        AbortReason::Expired | AbortReason::Cancelled => "the staging expired or was discarded",
    }
}

/// The `APPLIED` of a `COMMIT` this destination does not apply yet.
fn not_applied(identity: WriteIdentity) -> Message {
    Message::Applied(Applied {
        identity,
        outcome: Outcome::Failed {
            error: ApplyError::Unavailable,
            reason: "this destination does not apply COMMIT yet".to_owned(),
        },
    })
}
