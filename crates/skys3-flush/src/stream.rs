//! Streaming flush (§7.3): a remote multipart upload for each local
//! multipart upload and each streamed single PUT, filled while the client
//! uploads, and completed only after the local write commits.
//!
//! A shard's flusher follows the shard's uploads ([`Change`]):
//!
//! - **Open.** When an `MPU_CREATE` applies, the flusher opens a remote
//!   upload whose `CreateMultipartUpload` carries the local upload's
//!   metadata, standard headers, tags, and write identity, the `MPU_CREATE`
//!   position (§7.2), and records its ID with a `PART_FLUSHED` before it
//!   sends any part, so the shard log holds the ID before any data reaches
//!   it. An open that fails is retried while the local upload is open.
//! - **Parts.** When an `MPU_PART` applies on the primary, the flusher sends
//!   the part from the local log, where the body was just written, under the
//!   client's part number and with `Content-MD5`, while the client goes on
//!   uploading. A part that never commits locally, because it failed
//!   validation or its body broke off, is never sent. A part uploaded
//!   again is sent again under its number once any earlier send of that
//!   number ended, so the remote part replaces the earlier one in order.
//!   Each part the remote holds is recorded with a `PART_FLUSHED` naming the
//!   `MPU_PART` it came from. A send that fails is not retried: the
//!   completion sends what is missing.
//! - **Complete.** The remote upload is completed by the key's flush of the
//!   version the local `MPU_COMPLETE` committed, never before: the flush
//!   claims the stream ([`Streams::claim`]), waits for the parts in flight,
//!   sends every kept part the remote does not hold from that very
//!   `MPU_PART`, lists exactly the parts the local completion kept, and
//!   completes with the §7.2 precondition. A part the local completion left
//!   out, or one replaced since it was sent, is never listed.
//! - **Abort.** A remote upload is aborted when its local upload is
//!   aborted, when its remote upload turns out to be gone, and when its key
//!   becomes clean without it: the key's latest version was flushed some
//!   other way, so the remote upload will never complete. The abort ends
//!   with a `PART_FLUSHED` that ends the remote upload, and is retried until
//!   it succeeds.
//!
//! **Streamed single PUTs.** A single PUT whose body reaches
//! `streaming_flush_min_bytes` commits an `UPLOAD_BEGIN` (§7.2), and the
//! gateway receiving it announces the body's applied extents to the
//! primary as it goes ([`Change::Streamed`]). Its stream is named by the
//! `UPLOAD_BEGIN`'s position, as a multipart upload's is by its
//! `MPU_CREATE`'s, and goes through the same steps:
//!
//! - The first announcement opens the remote upload, with the metadata and
//!   tags the `PUT` will store and the `UPLOAD_BEGIN`'s write identity, and
//!   records its ID before any part is sent.
//! - Part *n* is the body from byte `(n−1)·P` to byte `n·P`, where `P` is
//!   `flush_part_bytes`, fixed for the stream when it is first pumped. It
//!   is sent with a `Content-MD5` the flusher computes, once the announced
//!   extents cover it. The body's last part is sent by the completion, as
//!   only the `PUT` says where the body ends.
//! - The `PUT` that inherits the identity is flushed by completing the
//!   remote upload (the `single` module). A part is listed only if it was
//!   sent from exactly the extents the `PUT` names in its range; any other
//!   is sent again from them.
//! - Nothing commits when a body fails, so a body whose `PUT` has not
//!   committed `body_timeout` after its last announcement never will, and
//!   its remote upload is aborted.
//! - A body's parts are not recorded with `PART_FLUSHED`, since the index
//!   would not say which extents a part came from: a flusher that resumes
//!   the stream from the index sends every part again at completion.
//!
//! The streams live in the flusher's memory and in the index, which keeps
//! every remote upload until its end is recorded. A restarted flusher
//! resumes them from the index, and opens remote uploads for open local
//! uploads that have none. A remote upload whose `CreateMultipartUpload`
//! answer was lost, or whose `PART_FLUSHED` was lost in a crash, is left to
//! the remote bucket's abort-incomplete-uploads lifecycle rule (§7.3).
//!
//! **Takeover.** A stream resumed from the index, by a restarted flusher or
//! by a new primary's, knows only what the `PART_FLUSHED` records say. Some
//! may be missing (lost in the crash), and the remote may hold a part other
//! than the one recorded (a send of the old primary that landed late). So
//! the stream reconciles with a remote `ListParts` before it sends a part
//! or completes ([`Listing`]): a recorded part counts only if the remote
//! holds it with the recorded ETag, and a part to send whose MD5 is the
//! ETag of the part the remote holds under its number is taken as sent
//! without sending it. A multipart upload's part has its MD5 in the index;
//! a body's part is read and hashed. A completion that the remote refuses
//! with `400 InvalidPart` lists the remote upload again.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use base64::Engine as _;
use bytes::{Bytes, BytesMut};
use md5::{Digest, Md5};
use skys3_index::{Part, Payload};
use skys3_io::Disk;
use skys3_log::RecordBody;
use skys3_log::record::{ExtentRef, Metadata, PartFlushed, RemoteStep, TagSet};
use skys3_remote::{
    AbortMultipartUpload, ListParts, ObjectStore, S3ErrorKind, UploadId, UploadPart,
};
use skys3_shard::{Shard, ShardError, StreamedBody};
use skys3_types::{ETag, EpochSeq};
use tokio::sync::{Notify, oneshot};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;

use crate::attempt::{Failure, content_md5, read_payload};
use crate::multipart::create_request;
use crate::target::{FlushSettings, Target};

/// How many open uploads the startup scan reads per index transaction.
const SCAN_PAGE: usize = 512;

/// The largest part whose MD5 is computed on the flusher's own task; a
/// larger one is hashed on a blocking thread, off the runtime.
const INLINE_HASH_BYTES: usize = 1 << 20;

/// The extents a part of a streamed body is read from, in body order, each
/// with its offset in the body.
pub(crate) type Span = Vec<(u64, ExtentRef)>;

/// The remote side of a stream.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Remote {
    /// Not opened yet.
    Unopened,
    /// Being opened.
    Opening,
    /// Open, with this ID.
    Open(UploadId),
}

/// The body of a streamed single PUT, as its gateway announced it.
#[derive(Debug)]
struct Body {
    /// The metadata and tags of the `PUT`, to open the remote upload with;
    /// `None` for a stream resumed from the index, which is open.
    headers: Option<(Metadata, TagSet)>,
    /// The part size, fixed when the stream is first pumped.
    part_bytes: Option<u64>,
    /// The extents announced, by offset.
    extents: BTreeMap<u64, ExtentRef>,
    /// The extents of each part queued, being sent, or sent.
    spans: BTreeMap<u16, Span>,
    /// Every part up to this number is queued.
    queued: u16,
    /// When the body was last announced, or the stream resumed.
    announced: Instant,
}

impl Body {
    fn new(headers: Option<(Metadata, TagSet)>) -> Self {
        Self {
            headers,
            part_bytes: None,
            extents: BTreeMap::new(),
            spans: BTreeMap::new(),
            queued: 0,
            announced: Instant::now(),
        }
    }

    /// The parts after the queued ones whose bytes the announced extents
    /// cover, in order, each with its first extent's position.
    fn covered(&mut self) -> Vec<(u16, EpochSeq)> {
        let Some(part_bytes) = self.part_bytes else {
            return Vec::new();
        };
        let mut ready = Vec::new();
        while let Some(number) = self.queued.checked_add(1)
            && let Some(span) = span(&self.extents, part_range(number, part_bytes))
        {
            ready.push((number, span[0].1.position));
            self.spans.insert(number, span);
            self.queued = number;
        }
        ready
    }
}

/// The bytes of part `number` of a body in parts of `part_bytes`.
pub(crate) fn part_range(number: u16, part_bytes: u64) -> (u64, u64) {
    let start = u64::from(number - 1).saturating_mul(part_bytes);
    (start, start.saturating_add(part_bytes))
}

/// Keeps in `flushed` only the parts `remote` holds with the ETag recorded,
/// and returns the others' numbers and positions.
fn confirm(
    flushed: &mut BTreeMap<u16, (EpochSeq, ETag)>,
    remote: &BTreeMap<u16, ETag>,
) -> Vec<(u16, EpochSeq)> {
    let mut stale = Vec::new();
    flushed.retain(|number, (position, etag)| {
        let held = remote.get(number) == Some(etag);
        if !held {
            stale.push((*number, *position));
        }
        held
    });
    stale
}

/// The extents of `extents`, by offset, that hold every byte of `range`,
/// or `None` if they leave a gap.
pub(crate) fn span(extents: &BTreeMap<u64, ExtentRef>, (start, end): (u64, u64)) -> Option<Span> {
    let first = extents
        .range(..=start)
        .next_back()
        .map(|(offset, _)| *offset)?;
    let mut span = Vec::new();
    let mut covered = first;
    for (&offset, &extent) in extents.range(first..) {
        if offset != covered || covered >= end {
            break;
        }
        covered = offset + u64::from(extent.len);
        span.push((offset, extent));
    }
    (covered >= end && !span.is_empty()).then_some(span)
}

/// Whether a stream knows which parts its remote upload holds (§7.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Listing {
    /// It does: the flusher opened the remote upload and saw every send
    /// end, or listed the remote upload since.
    Known,
    /// It was resumed from the index, or a completion found a part listed
    /// that the remote does not hold: `flushed` may name parts the remote
    /// does not hold, and miss some it does. A `ListParts` must tell.
    Due,
    /// The `ListParts` with this number is in flight.
    Running(u64),
}

/// The remote upload of one local upload or streamed body.
#[derive(Debug)]
struct Stream {
    key: String,
    remote: Remote,
    /// Whether the local upload is still open: parts may still come.
    open: bool,
    /// The parts the remote holds: the `MPU_PART` (or a body part's first
    /// `EXTENT`) each came from, and its remote ETag.
    flushed: BTreeMap<u16, (EpochSeq, ETag)>,
    /// Parts to send: the latest `MPU_PART` of each number, or a body
    /// part's first `EXTENT`.
    waiting: BTreeMap<u16, EpochSeq>,
    /// Part numbers being sent.
    sending: BTreeSet<u16>,
    /// A completion is using the stream: no part is started.
    claimed: bool,
    /// The remote upload will never complete: abort it.
    doomed: bool,
    /// Its abort is running.
    aborting: bool,
    /// When the remote acknowledged each part in `flushed`, for the
    /// streaming-overlap metric. A part loaded from the index has none: the
    /// remote held it before this flusher started.
    acked: BTreeMap<u16, Instant>,
    /// When this flusher learned that the local completion committed, until
    /// a completion claims the stream.
    completed_at: Option<Instant>,
    /// For a streamed single PUT, its body; `None` for a multipart upload.
    body: Option<Body>,
    /// Whether the stream knows what its remote upload holds.
    listing: Listing,
    /// The ETags of the parts the last `ListParts` found, but those sent
    /// since: a part to send whose MD5 is the one listed under its number
    /// is not sent again.
    listed: BTreeMap<u16, ETag>,
}

impl Stream {
    fn new(key: String, remote: Remote, open: bool) -> Self {
        Self {
            key,
            remote,
            open,
            listing: Listing::Known,
            listed: BTreeMap::new(),
            flushed: BTreeMap::new(),
            waiting: BTreeMap::new(),
            sending: BTreeSet::new(),
            claimed: false,
            doomed: false,
            aborting: false,
            acked: BTreeMap::new(),
            completed_at: None,
            body: None,
        }
    }

    /// Queues the `MPU_PART` at `position` as part `number`, unless the
    /// remote holds it or a later one of its number is queued.
    fn queue(&mut self, number: u16, position: EpochSeq) {
        if self
            .flushed
            .get(&number)
            .is_some_and(|(p, _)| *p >= position)
        {
            return;
        }
        let waiting = self.waiting.entry(number).or_insert(position);
        *waiting = (*waiting).max(position);
    }

    /// Takes what a `ListParts` found, `remote`: the recorded parts the
    /// remote does not hold as recorded are no longer counted as sent, and
    /// while the local upload is open they are sent again.
    fn reconcile(&mut self, remote: BTreeMap<u16, ETag>) {
        for (number, position) in confirm(&mut self.flushed, &remote) {
            self.acked.remove(&number);
            if self.open {
                self.queue(number, position);
            }
        }
        self.listed = remote;
        self.listing = Listing::Known;
    }

    /// Records that part `number` was sent from `position`, with the remote
    /// ETag `etag`, at `at`.
    fn sent(&mut self, number: u16, position: EpochSeq, etag: ETag, at: Instant) {
        self.flushed.insert(number, (position, etag));
        self.acked.insert(number, at);
        self.listed.remove(&number);
    }

    /// Fixes a body's part size and queues the parts its extents cover; for
    /// an open body past its deadline, gives up: its `PUT` never commits.
    fn advance(&mut self, settings: &FlushSettings, now: Instant) {
        let Some(body) = &mut self.body else {
            return;
        };
        body.part_bytes.get_or_insert(settings.part_bytes.max(1));
        if !self.open {
            return;
        }
        if now >= body.announced + settings.body_timeout {
            self.open = false;
            self.doomed = true;
            self.waiting.clear();
            return;
        }
        for (number, position) in body.covered() {
            self.waiting.insert(number, position);
        }
    }
}

/// How a stream task ended.
#[derive(Debug)]
pub(crate) enum Step {
    /// An open ended: the remote upload's ID and the local upload's parts
    /// then, or `None` if the local upload closed first.
    Opened(EpochSeq, Option<(UploadId, Vec<(u16, EpochSeq)>)>),
    /// A part's send ended.
    Sent {
        upload: EpochSeq,
        number: u16,
        position: EpochSeq,
        sent: Sent,
    },
    /// The remote upload was aborted and its end recorded.
    Ended(EpochSeq),
    /// The `ListParts` numbered `run` ended.
    Listed {
        upload: EpochSeq,
        run: u64,
        listed: Listed,
    },
}

/// How a stream's `ListParts` ended.
#[derive(Debug)]
pub(crate) enum Listed {
    /// The remote upload holds these parts, with these ETags.
    Parts(BTreeMap<u16, ETag>),
    /// The remote upload is gone.
    Gone,
    /// The stream no longer wanted it: a completion claimed it, or it was
    /// doomed.
    Dropped,
}

/// How a part's send ended.
#[derive(Debug)]
pub(crate) enum Sent {
    /// The remote holds it, with this ETag, since this instant: when the
    /// task saw the answer, whenever the flusher gets to the step.
    Done(ETag, Instant),
    /// It failed, or the part was replaced first; the completion sends it.
    Failed,
    /// The remote upload is gone.
    Gone,
}

/// What a completion that claimed a stream ([`Streams::claim`]) leaves of
/// it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// The remote upload completed: its end is recorded.
    Completed,
    /// The remote upload will never complete: abort it.
    Abort,
    /// The completion failed and is retried: keep the stream.
    Keep,
    /// The remote refused a part the completion listed: keep the stream,
    /// and list the remote upload before the retry.
    Relist,
}

/// A stream claimed for a completion.
#[derive(Debug)]
pub(crate) struct Claim {
    /// The remote upload.
    pub(crate) id: UploadId,
    /// The parts the remote holds.
    pub(crate) flushed: BTreeMap<u16, (EpochSeq, ETag)>,
    /// The parts the remote held when the client completed the upload, if
    /// this flusher saw it.
    pub(crate) at_completion: Option<BTreeMap<u16, EpochSeq>>,
    /// For a streamed body, its part size and the extents each part the
    /// stream queued was read from.
    pub(crate) body: Option<(u64, BTreeMap<u16, Span>)>,
    /// The ETags the last `ListParts` found, but for parts sent since, or
    /// `None` if the stream does not know what the remote holds: the
    /// completion lists it first ([`Claim::reconcile`]).
    pub(crate) listed: Option<BTreeMap<u16, ETag>>,
}

impl Claim {
    /// Takes what the completion's `ListParts` found, `remote`.
    pub(crate) fn reconcile(&mut self, remote: BTreeMap<u16, ETag>) {
        confirm(&mut self.flushed, &remote);
        self.listed = Some(remote);
    }
}

/// The streams of one shard flusher, shared by its task and its flushes.
#[derive(Debug, Default)]
pub(crate) struct Streams {
    streams: Mutex<BTreeMap<EpochSeq, Stream>>,
    /// Woken whenever a part's send or an open ends.
    idle: Notify,
    /// The number of the next `ListParts` a stream starts.
    listings: AtomicU64,
    /// The `PART_FLUSHED` commits in flight. They run on their own, so a
    /// flusher that stops leaves none half done, and
    /// [`Streams::recorded`] waits for them.
    records: Mutex<Vec<JoinHandle<()>>>,
}

impl Streams {
    fn lock(&self) -> MutexGuard<'_, BTreeMap<EpochSeq, Stream>> {
        // Every update leaves the map consistent.
        self.streams.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Commits a `PART_FLUSHED` of the remote upload `id` of the local
    /// upload of `key` at `upload`, riding the next group commit (§7.3),
    /// in a task of its own. The returned handle tells when it is applied;
    /// a record that does not commit leaves its remote upload to the
    /// lifecycle rule, or a part to be sent again.
    pub(crate) fn record<D: Disk>(
        &self,
        shard: &Shard<D>,
        key: &str,
        upload: EpochSeq,
        id: &UploadId,
        step: RemoteStep,
    ) -> impl Future<Output = ()> + Send + 'static {
        let shard = shard.clone();
        let body = RecordBody::PartFlushed(PartFlushed {
            key: key.to_owned(),
            upload,
            remote_upload_id: id.0.clone(),
            step,
        });
        let (done, applied) = oneshot::channel();
        let task = tokio::spawn(async move {
            if let Err(error) = shard.commit_lazy(body).await {
                tracing::debug!(%error, "a PART_FLUSHED was not committed");
            }
            let _ = done.send(());
        });
        let mut records = self.records.lock().unwrap_or_else(PoisonError::into_inner);
        records.retain(|task| !task.is_finished());
        records.push(task);
        async move {
            let _ = applied.await;
        }
    }

    /// Waits until every `PART_FLUSHED` commit started so far has ended,
    /// so that a flusher started next finds them applied.
    pub(crate) async fn recorded(&self) {
        let records =
            std::mem::take(&mut *self.records.lock().unwrap_or_else(PoisonError::into_inner));
        for task in records {
            let _ = task.await;
        }
    }

    /// The local upload of `key` at `upload` opened.
    pub(crate) fn opened(&self, key: &str, upload: EpochSeq) {
        self.lock()
            .entry(upload)
            .or_insert_with(|| Stream::new(key.to_owned(), Remote::Unopened, true));
    }

    /// Part `number` of the local upload at `upload` was stored by the
    /// `MPU_PART` at `position`.
    pub(crate) fn part(&self, upload: EpochSeq, number: u16, position: EpochSeq) {
        if let Some(stream) = self.lock().get_mut(&upload) {
            stream.queue(number, position);
        }
    }

    /// The gateway receiving a streamed single PUT announced more of its
    /// body. The first announcement starts the stream; once the `PUT`
    /// committed, more are ignored.
    pub(crate) fn announced(&self, announced: StreamedBody) {
        let StreamedBody {
            key,
            upload,
            metadata,
            tags,
            extents,
        } = announced;
        let mut streams = self.lock();
        let stream = streams.entry(upload).or_insert_with(|| {
            let mut stream = Stream::new(key, Remote::Unopened, true);
            stream.body = Some(Body::new(Some((metadata, tags))));
            stream
        });
        if !stream.open {
            return;
        }
        let Some(body) = &mut stream.body else {
            return;
        };
        body.announced = Instant::now();
        for (offset, extent) in extents {
            body.extents.entry(offset).or_insert(extent);
        }
    }

    /// The local upload at `upload` was aborted.
    pub(crate) fn aborted(&self, upload: EpochSeq) {
        if let Some(stream) = self.lock().get_mut(&upload) {
            stream.open = false;
            stream.doomed = true;
            stream.waiting.clear();
        }
    }

    /// The local upload at `upload` was completed, or the `PUT` of the
    /// streamed body begun there committed: no part comes any more.
    pub(crate) fn completed(&self, upload: EpochSeq) {
        if let Some(stream) = self.lock().get_mut(&upload) {
            stream.open = false;
            stream.completed_at = Some(Instant::now());
        }
    }

    /// `key` became clean: the remote upload of every local upload of it
    /// that is no longer open will never complete.
    pub(crate) fn reap(&self, key: &str) {
        for stream in self.lock().values_mut() {
            if stream.key == key && !stream.open && !stream.claimed {
                stream.doomed = true;
            }
        }
    }

    /// Dooms every stream of a closed local upload whose key is not
    /// `tracked`: nothing will complete it.
    pub(crate) fn reap_untracked(&self, tracked: impl Fn(&str) -> bool) {
        for stream in self.lock().values_mut() {
            if !stream.open && !tracked(&stream.key) {
                stream.doomed = true;
            }
        }
    }

    /// Reads the streams the index holds, and the open uploads, as the
    /// flusher starts. Changes applied meanwhile wait in the subscription.
    /// Each stream read from the index is reconciled with its remote upload
    /// before it sends or completes ([`Listing::Due`]).
    ///
    /// A remote upload without an open local upload belongs to a completed
    /// upload if the key's object is made of its parts, or to a streamed
    /// body whose `PUT` committed if the object inherits its identity.
    /// Otherwise it may belong to a body whose `PUT` is still to come, and
    /// is kept open for `body_timeout` (an aborted upload's is too, and is
    /// aborted then).
    pub(crate) async fn load<D: Disk>(&self, shard: &Shard<D>) -> Result<(), ShardError> {
        for (upload, remote) in shard.remote_uploads().await? {
            let Some((_, flushed)) = shard.remote_upload(upload).await? else {
                continue;
            };
            let open = shard.upload(&remote.key, upload, 0, 0).await?.is_some();
            let mut stream = Stream::new(remote.key, Remote::Open(UploadId(remote.id)), open);
            // The records may be missing parts, or name parts the remote
            // no longer holds: the remote upload is listed first.
            stream.listing = Listing::Due;
            stream.flushed = flushed
                .into_iter()
                .map(|(number, part)| (number, (part.position, part.etag)))
                .collect();
            if open {
                for (number, part) in shard.parts(upload, 0, usize::MAX).await? {
                    stream.queue(number, part.position);
                }
            } else {
                let object = shard.entry(&stream.key).await?.and_then(|e| e.object);
                let completed = object.as_ref().is_some_and(|object| {
                    matches!(object.payload, Payload::Parts { upload: of, .. } if of == upload)
                });
                if !completed {
                    let committed = object.is_some_and(|o| o.write_identity == Some(upload));
                    stream.open = !committed;
                    stream.flushed.clear();
                    stream.body = Some(Body::new(None));
                }
            }
            self.lock().insert(upload, stream);
        }
        let mut after = None;
        loop {
            let page = shard.uploads("", after.take(), SCAN_PAGE).await?;
            for (key, upload, _) in &page {
                self.opened(key, *upload);
            }
            match page.into_iter().last() {
                Some((key, upload, _)) => after = Some((key, Some(upload))),
                None => return Ok(()),
            }
        }
    }

    /// Acts on a finished stream task.
    pub(crate) fn finish(&self, step: Step) {
        let mut streams = self.lock();
        match step {
            Step::Opened(upload, opened) => {
                let Some(stream) = streams.get_mut(&upload) else {
                    return;
                };
                match opened {
                    Some((id, parts)) => {
                        stream.remote = Remote::Open(id);
                        for (number, position) in parts {
                            if stream.open {
                                stream.queue(number, position);
                            }
                        }
                    }
                    // Nothing was opened: the local upload closed first.
                    None => {
                        streams.remove(&upload);
                    }
                }
                self.idle.notify_waiters();
            }
            Step::Sent {
                upload,
                number,
                position,
                sent,
            } => {
                if let Some(stream) = streams.get_mut(&upload) {
                    stream.sending.remove(&number);
                    match sent {
                        Sent::Done(etag, at) => stream.sent(number, position, etag, at),
                        // The send may have landed: what the remote holds
                        // under its number is unknown.
                        Sent::Failed => {
                            stream.listed.remove(&number);
                        }
                        Sent::Gone => stream.doomed = true,
                    }
                }
                self.idle.notify_waiters();
            }
            Step::Ended(upload) => {
                streams.remove(&upload);
            }
            Step::Listed {
                upload,
                run,
                listed,
            } => {
                // A listing a completion claimed the stream from is stale.
                if let Some(stream) = streams.get_mut(&upload)
                    && stream.listing == Listing::Running(run)
                {
                    match listed {
                        Listed::Parts(remote) => stream.reconcile(remote),
                        Listed::Gone if stream.open => {
                            stream.listing = Listing::Known;
                            stream.doomed = true;
                        }
                        // A closed stream's completion lists it again, and
                        // a remote upload that is gone may have been
                        // consumed by an earlier Complete (§7.2).
                        Listed::Gone | Listed::Dropped => stream.listing = Listing::Due,
                    }
                }
            }
        }
    }

    /// When the earliest open body times out, if any stream has one.
    pub(crate) fn next_timeout(&self, settings: &FlushSettings) -> Option<Instant> {
        self.lock()
            .values()
            .filter(|stream| stream.open)
            .filter_map(|stream| stream.body.as_ref())
            .map(|body| body.announced + settings.body_timeout)
            .min()
    }

    /// Starts what the streams need: opens of new uploads, sends of queued
    /// parts while fewer than `concurrency` are in flight, and aborts.
    pub(crate) fn pump<S: ObjectStore, D: Disk>(
        self: &Arc<Self>,
        shard: &Shard<D>,
        target: &Arc<Target<S>>,
        tasks: &mut JoinSet<Step>,
    ) {
        let mut streams = self.lock();
        let streaming = target.settings.streaming;
        let now = Instant::now();
        for stream in streams.values_mut() {
            stream.advance(&target.settings, now);
        }
        streams.retain(|_, stream| {
            stream.remote != Remote::Unopened || (streaming && stream.open && !stream.doomed)
        });
        let mut sending: usize = streams.values().map(|s| s.sending.len()).sum();
        for (&upload, stream) in streams.iter_mut() {
            let remote = match &stream.remote {
                Remote::Unopened => {
                    stream.remote = Remote::Opening;
                    let task = Task::new(self, shard, target, &stream.key, upload);
                    let headers = stream.body.as_mut().and_then(|body| body.headers.take());
                    tasks.spawn(async move { Step::Opened(upload, task.open(headers).await) });
                    continue;
                }
                Remote::Opening => continue,
                Remote::Open(id) => id.clone(),
            };
            if stream.doomed {
                if !stream.aborting && stream.sending.is_empty() && !stream.claimed {
                    stream.aborting = true;
                    let task = Task::new(self, shard, target, &stream.key, upload);
                    tasks.spawn(async move {
                        task.end(remote).await;
                        Step::Ended(upload)
                    });
                }
                continue;
            }
            if stream.claimed {
                continue;
            }
            // A resumed stream sends nothing before it knows what the
            // remote holds. An open one lists it now, so that the parts
            // still to come are sent only if the remote lacks them; a
            // closed one leaves it to its completion.
            match stream.listing {
                Listing::Known => {}
                Listing::Due if stream.open => {
                    let run = self.listings.fetch_add(1, Ordering::Relaxed);
                    stream.listing = Listing::Running(run);
                    let task = Task::new(self, shard, target, &stream.key, upload);
                    tasks.spawn(async move {
                        let listed = task.list(&remote, run).await;
                        Step::Listed {
                            upload,
                            run,
                            listed,
                        }
                    });
                    continue;
                }
                Listing::Due | Listing::Running(_) => continue,
            }
            let ready: Vec<u16> = stream
                .waiting
                .keys()
                .filter(|number| !stream.sending.contains(number))
                .copied()
                .collect();
            for number in ready {
                if sending >= target.settings.concurrency {
                    break;
                }
                let Some(position) = stream.waiting.remove(&number) else {
                    continue;
                };
                // A body's part is read from the extents it was queued
                // with; a multipart upload's from its `MPU_PART`.
                let piece = stream.body.as_ref().and_then(|body| {
                    let span = body.spans.get(&number)?.clone();
                    Some((part_range(number, body.part_bytes?), span))
                });
                stream.sending.insert(number);
                sending += 1;
                let task = Task::new(self, shard, target, &stream.key, upload);
                let id = remote.clone();
                let listed = stream.listed.get(&number).cloned();
                tasks.spawn(async move {
                    let listed = listed.as_ref();
                    let sent = match piece {
                        Some((range, span)) => {
                            task.send_span(&id, number, range, &span, listed).await
                        }
                        None => task.send(&id, number, position, listed).await,
                    };
                    Step::Sent {
                        upload,
                        number,
                        position,
                        sent,
                    }
                });
            }
        }
    }

    /// Claims the stream of the local upload at `upload` for its
    /// completion: no part is started from then on, and once no part of it
    /// is in flight, returns its remote upload and the parts it holds.
    /// Returns `None` if there is no stream with an open remote upload to
    /// complete; a body's stream whose remote upload is being opened is
    /// waited for.
    pub(crate) async fn claim(&self, upload: EpochSeq) -> Option<Claim> {
        loop {
            let idle = self.idle.notified();
            tokio::pin!(idle);
            idle.as_mut().enable();
            {
                let mut streams = self.lock();
                let stream = streams.get_mut(&upload)?;
                if stream.doomed {
                    return None;
                }
                let id = match &stream.remote {
                    Remote::Open(id) => Some(id.clone()),
                    // A body's `PUT` may commit while its remote upload is
                    // being opened: the open's request in flight decides.
                    Remote::Opening if stream.body.is_some() => None,
                    Remote::Opening | Remote::Unopened => return None,
                };
                if id.is_some() {
                    stream.claimed = true;
                    stream.waiting.clear();
                    // The completion lists the remote upload itself rather
                    // than wait for a listing that may be retrying.
                    if matches!(stream.listing, Listing::Running(_)) {
                        stream.listing = Listing::Due;
                    }
                }
                if let Some(id) = id.filter(|_| stream.sending.is_empty()) {
                    // The parts whose acknowledgement came before the
                    // completion, whichever step the flusher handled first.
                    let at_completion = stream.completed_at.take().map(|completed| {
                        stream
                            .flushed
                            .iter()
                            .filter(|(number, _)| {
                                stream.acked.get(number).is_none_or(|at| *at <= completed)
                            })
                            .map(|(number, (position, _))| (*number, *position))
                            .collect()
                    });
                    let body = stream
                        .body
                        .as_ref()
                        .and_then(|body| Some((body.part_bytes?, body.spans.clone())));
                    let listed = (stream.listing == Listing::Known).then(|| stream.listed.clone());
                    return Some(Claim {
                        id,
                        flushed: stream.flushed.clone(),
                        at_completion,
                        body,
                        listed,
                    });
                }
            }
            idle.await;
        }
    }

    /// The completion that claimed the stream of `upload` sent part
    /// `number` from the `MPU_PART` at `position`.
    pub(crate) fn sent(&self, upload: EpochSeq, number: u16, position: EpochSeq, etag: ETag) {
        if let Some(stream) = self.lock().get_mut(&upload) {
            stream.sent(number, position, etag, Instant::now());
        }
    }

    /// The completion that claimed the stream of the body begun at
    /// `upload` sent part `number` from `span`.
    pub(crate) fn sent_span(&self, upload: EpochSeq, number: u16, span: Span, etag: ETag) {
        if let Some(stream) = self.lock().get_mut(&upload)
            && let Some(body) = &mut stream.body
        {
            let position = span[0].1.position;
            body.spans.insert(number, span);
            stream.sent(number, position, etag, Instant::now());
        }
    }

    /// The completion that claimed the stream of `upload` listed its remote
    /// upload, which holds `remote`.
    pub(crate) fn reconciled(&self, upload: EpochSeq, remote: BTreeMap<u16, ETag>) {
        if let Some(stream) = self.lock().get_mut(&upload) {
            stream.reconcile(remote);
        }
    }

    /// Ends the claim of a completion on the stream of `upload`.
    pub(crate) fn release(&self, upload: EpochSeq, verdict: Verdict) {
        let mut streams = self.lock();
        match verdict {
            Verdict::Completed => {
                streams.remove(&upload);
            }
            Verdict::Abort | Verdict::Keep | Verdict::Relist => {
                if let Some(stream) = streams.get_mut(&upload) {
                    stream.claimed = false;
                    stream.doomed |= verdict == Verdict::Abort;
                    if verdict == Verdict::Relist {
                        stream.listing = Listing::Due;
                    }
                }
            }
        }
    }

    /// How many remote uploads the streams keep.
    pub(crate) fn len(&self) -> usize {
        self.lock()
            .values()
            .filter(|stream| matches!(stream.remote, Remote::Open(_)))
            .count()
    }

    /// Whether the stream of `upload` still wants its remote upload opened:
    /// its local upload or body is open, and nothing doomed it.
    fn wants_open(&self, upload: EpochSeq) -> bool {
        self.lock()
            .get(&upload)
            .is_some_and(|stream| stream.open && !stream.doomed)
    }

    /// Whether the stream of `upload` still waits for its `ListParts`
    /// numbered `run`.
    fn wants_listing(&self, upload: EpochSeq, run: u64) -> bool {
        self.lock()
            .get(&upload)
            .is_some_and(|stream| !stream.doomed && stream.listing == Listing::Running(run))
    }
}

/// What a stream task works on: the local upload of `key` at `upload`.
struct Task<S, D: Disk> {
    streams: Arc<Streams>,
    shard: Shard<D>,
    target: Arc<Target<S>>,
    key: String,
    upload: EpochSeq,
}

impl<S: ObjectStore, D: Disk> Task<S, D> {
    fn new(
        streams: &Arc<Streams>,
        shard: &Shard<D>,
        target: &Arc<Target<S>>,
        key: &str,
        upload: EpochSeq,
    ) -> Self {
        Self {
            streams: Arc::clone(streams),
            shard: shard.clone(),
            target: Arc::clone(target),
            key: key.to_owned(),
            upload,
        }
    }

    /// Opens the remote upload and records its ID, trying again after a
    /// backoff while the local upload is open. A streamed body's upload is
    /// opened with its announced `headers`; a multipart upload's with what
    /// its local upload holds. Returns the ID and the local upload's parts,
    /// or `None` if the local upload closed first.
    ///
    /// The parts are read before the remote upload is opened, by the read
    /// that is tried again: once the remote upload and its record exist,
    /// nothing can fail and leave them behind. A part stored after the read
    /// is queued by its change, since the stream exists before this task.
    async fn open(
        &self,
        headers: Option<(Metadata, TagSet)>,
    ) -> Option<(UploadId, Vec<(u16, EpochSeq)>)> {
        let (key, upload) = (self.key.as_str(), self.upload);
        let identity = self.target.identity(self.shard.shard(), upload);
        let remote_key = format!("{}{key}", self.target.prefix);
        for attempt in 1.. {
            let local = match &headers {
                Some(headers) if self.streams.wants_open(upload) => {
                    Ok(Some((headers.clone(), Vec::new())))
                }
                Some(_) => Ok(None),
                None => match self.shard.upload(key, upload, 0, usize::MAX).await {
                    Ok(_) if test_hooks::fail_parts_read() => Err("a test failed it".to_owned()),
                    read => read.map_err(|error| error.to_string()).map(|found| {
                        found.map(|(local, parts)| ((local.metadata, local.tags), parts))
                    }),
                },
            };
            let ((metadata, tags), parts) = match local {
                Ok(None) => return None,
                Ok(Some(local)) => local,
                Err(error) => {
                    tracing::debug!(key, %upload, %error, "a remote upload was not opened");
                    tokio::time::sleep(self.target.settings.backoff(attempt)).await;
                    continue;
                }
            };
            let request = match create_request(remote_key.clone(), &metadata, &tags, &identity) {
                Ok(request) => request,
                // The completion fails the same way, and reports it.
                Err(failure) => {
                    tracing::warn!(key, %upload, %failure, "a remote upload cannot be opened");
                    return None;
                }
            };
            match self.target.store.create_multipart_upload(request).await {
                Ok(id) => {
                    // The ID is in the log before any part is sent.
                    self.record(&id, RemoteStep::Opened).await;
                    let parts = parts.into_iter().map(|(n, p)| (n, p.position)).collect();
                    return Some((id, parts));
                }
                Err(error) => {
                    tracing::debug!(key, %upload, %error, "a remote upload was not opened");
                    // A body whose `PUT` committed meanwhile is sent without
                    // its stream: its flush waits for no backoff.
                    if headers.is_some() && !self.streams.wants_open(upload) {
                        return None;
                    }
                    tokio::time::sleep(self.target.settings.backoff(attempt)).await;
                }
            }
        }
        None
    }

    /// Sends the `MPU_PART` at `position`, part `number`, to the remote
    /// upload `id`, and records it. If the remote holds a part of that
    /// number with the ETag `listed`, the part's own, it is not sent again.
    async fn send(
        &self,
        id: &UploadId,
        number: u16,
        position: EpochSeq,
        listed: Option<&ETag>,
    ) -> Sent {
        let part = match self.shard.parts(self.upload, number - 1, 1).await {
            Ok(parts) => parts
                .into_iter()
                .find(|(n, part)| *n == number && part.position == position),
            Err(_) => None,
        };
        // Replaced, or gone with its upload: a later change says what to
        // send.
        let Some((_, part)) = part else {
            return Sent::Failed;
        };
        let sent = upload_part(
            &self.shard,
            &self.target,
            &self.key,
            id,
            number,
            &part,
            listed,
        );
        match sent.await {
            Ok(etag) => {
                let at = Instant::now();
                let step = RemoteStep::Part {
                    number,
                    position,
                    remote_etag: etag.clone(),
                };
                drop(self.record(id, step));
                Sent::Done(etag, at)
            }
            Err(failure) => self.failed(number, &failure),
        }
    }

    /// Sends the bytes `range` of a streamed body, held by `span`, as part
    /// `number` of the remote upload `id`, unless their MD5 is `listed`.
    async fn send_span(
        &self,
        id: &UploadId,
        number: u16,
        range: (u64, u64),
        span: &Span,
        listed: Option<&ETag>,
    ) -> Sent {
        let part = Piece {
            number,
            range,
            span,
        };
        match upload_span(&self.shard, &self.target, &self.key, id, part, listed).await {
            Ok(etag) => Sent::Done(etag, Instant::now()),
            Err(failure) => self.failed(number, &failure),
        }
    }

    fn failed(&self, number: u16, failure: &Failure) -> Sent {
        match failure {
            Failure::Remote(error) if error.kind() == S3ErrorKind::NoSuchUpload => Sent::Gone,
            failure => {
                tracing::debug!(key = self.key, upload = %self.upload, number, %failure,
                    "a part was not streamed");
                Sent::Failed
            }
        }
    }

    /// Lists the parts of the remote upload `id` for the `ListParts`
    /// numbered `run`, trying again after a backoff while the stream waits
    /// for it.
    async fn list(&self, id: &UploadId, run: u64) -> Listed {
        for attempt in 1.. {
            if !self.streams.wants_listing(self.upload, run) {
                break;
            }
            match list_parts(&self.target, &self.key, id).await {
                Ok(parts) => return Listed::Parts(parts),
                Err(Failure::Remote(error)) if error.kind() == S3ErrorKind::NoSuchUpload => {
                    return Listed::Gone;
                }
                Err(failure) => {
                    tracing::debug!(key = self.key, upload = %self.upload, %failure,
                        "a remote upload was not listed");
                    tokio::time::sleep(self.target.settings.backoff(attempt)).await;
                }
            }
        }
        Listed::Dropped
    }

    /// Aborts the remote upload `id` and records its end, trying again
    /// after a backoff until the remote no longer has it.
    async fn end(&self, id: UploadId) {
        let request = AbortMultipartUpload {
            key: format!("{}{}", self.target.prefix, self.key),
            upload_id: id.clone(),
        };
        for attempt in 1.. {
            match self
                .target
                .store
                .abort_multipart_upload(request.clone())
                .await
            {
                Ok(()) => break,
                Err(error) if error.kind() == S3ErrorKind::NoSuchUpload => break,
                Err(error) => {
                    tracing::debug!(key = self.key, upload = %self.upload, %error,
                        "a remote upload was not aborted");
                    tokio::time::sleep(self.target.settings.backoff(attempt)).await;
                }
            }
        }
        self.record(&id, RemoteStep::Ended).await;
    }

    fn record(&self, id: &UploadId, step: RemoteStep) -> impl Future<Output = ()> + Send + 'static {
        self.streams
            .record(&self.shard, &self.key, self.upload, id, step)
    }
}

/// The parts the remote upload `id` of `key` holds, by number, with their
/// ETags: every page of its `ListParts` (§7.3).
pub(crate) async fn list_parts<S: ObjectStore>(
    target: &Target<S>,
    key: &str,
    id: &UploadId,
) -> Result<BTreeMap<u16, ETag>, Failure> {
    let mut request = ListParts::new(format!("{}{key}", target.prefix), id.clone());
    let mut parts = BTreeMap::new();
    loop {
        let page = target
            .store
            .list_parts(request.clone())
            .await
            .map_err(Failure::Remote)?;
        for part in page.parts {
            // A number outside S3's range is no part SkyS3 sent.
            if let Ok(number) = u16::try_from(part.part_number) {
                parts.insert(number, part.etag);
            }
        }
        if !page.is_truncated {
            return Ok(parts);
        }
        match page.next_part_number_marker {
            Some(next)
                if request
                    .part_number_marker
                    .is_none_or(|marker| next > marker) =>
            {
                request.part_number_marker = Some(next);
            }
            _ => {
                return Err(Failure::Local(
                    "the remote's ListParts pages do not advance".into(),
                ));
            }
        }
    }
}

/// Uploads `part`, part `number` of the remote upload `id` of `key`, from
/// the local log, within the target's in-flight budget, and returns its
/// remote ETag. If the remote holds a part of that number with the ETag
/// `listed` and that is the part's MD5, nothing is sent (§7.3).
pub(crate) async fn upload_part<S: ObjectStore, D: Disk>(
    shard: &Shard<D>,
    target: &Target<S>,
    key: &str,
    id: &UploadId,
    number: u16,
    part: &Part,
    listed: Option<&ETag>,
) -> Result<ETag, Failure> {
    if listed == Some(&part.etag) {
        return Ok(part.etag.clone());
    }
    let _permit = target.reserve(part.size).await;
    let body: Bytes = read_payload(shard, &part.payload, part.size).await?;
    let remote_key = format!("{}{key}", target.prefix);
    let mut request = UploadPart::new(remote_key, id.clone(), u32::from(number), body);
    request.content_md5 = content_md5(&part.etag);
    target
        .store
        .upload_part(request)
        .await
        .map_err(Failure::Remote)
}

/// A part of a streamed body: its number, the bytes it holds, and the
/// extents that hold them.
pub(crate) struct Piece<'a> {
    pub(crate) number: u16,
    pub(crate) range: (u64, u64),
    pub(crate) span: &'a Span,
}

/// Uploads `part` of a body as that part of the remote upload `id` of
/// `key`, from the local log, within the target's in-flight budget, with
/// the `Content-MD5` of its bytes, and returns the part's remote ETag. If
/// the remote holds a part of that number whose ETag, `listed`, is the MD5
/// of those bytes, nothing is sent (§7.3).
pub(crate) async fn upload_span<S: ObjectStore, D: Disk>(
    shard: &Shard<D>,
    target: &Target<S>,
    key: &str,
    id: &UploadId,
    part: Piece<'_>,
    listed: Option<&ETag>,
) -> Result<ETag, Failure> {
    let Piece {
        number,
        range: (start, end),
        span,
    } = part;
    let _permit = target.reserve(end - start).await;
    let mut body = BytesMut::new();
    for (offset, extent) in span {
        let bytes = shard.payload(extent.position).await?;
        if bytes.len() != extent.len as usize {
            return Err(Failure::Local(format!(
                "the extent at {} is {} bytes long, not {}",
                extent.position,
                bytes.len(),
                extent.len
            )));
        }
        // The part's bytes within the extent.
        let within = |at: u64| {
            usize::try_from(at.saturating_sub(*offset))
                .map_or(bytes.len(), |at| at.min(bytes.len()))
        };
        body.extend_from_slice(&bytes[within(start)..within(end)]);
    }
    if body.len() as u64 != end - start {
        return Err(Failure::Local(format!(
            "the extents of bytes {start} to {end} hold {} of them",
            body.len()
        )));
    }
    let body = body.freeze();
    let digest = md5(body.clone()).await?;
    let etag = ETag::new(hex(&digest))
        .map_err(|error| Failure::Local(format!("an MD5 is no ETag: {error}")))?;
    if listed == Some(&etag) {
        return Ok(etag);
    }
    let remote_key = format!("{}{key}", target.prefix);
    let mut request = UploadPart::new(remote_key, id.clone(), u32::from(number), body);
    request.content_md5 = Some(base64::engine::general_purpose::STANDARD.encode(digest));
    target
        .store
        .upload_part(request)
        .await
        .map_err(Failure::Remote)
}

/// The MD5 of `body`, computed off the runtime when it is large.
async fn md5(body: Bytes) -> Result<[u8; 16], Failure> {
    let digest = |body: &[u8]| <[u8; 16]>::from(Md5::digest(body));
    if body.len() <= INLINE_HASH_BYTES {
        return Ok(digest(&body));
    }
    tokio::task::spawn_blocking(move || digest(&body))
        .await
        .map_err(|error| Failure::Local(format!("a part could not be hashed: {error}")))
}

/// `bytes` in lowercase hex, as an MD5 ETag is written.
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut hex, byte| {
        let _ = write!(hex, "{byte:02x}");
        hex
    })
}

/// Failpoints for tests: reads of an upload's parts by a stream's open
/// that fail, as an index read may, and a seeded bug of tombstone flushes.
#[cfg(feature = "test-util")]
pub mod test_hooks {
    use std::cell::Cell;
    use std::sync::atomic::{AtomicU32, Ordering};

    static FAILING: AtomicU32 = AtomicU32::new(0);

    thread_local! {
        static FORGETTING: Cell<bool> = const { Cell::new(false) };
    }

    /// Seeds, or with `false` removes, a bug into every flusher this thread
    /// runs: a tombstone's flush records the key as deleted at the remote
    /// without deleting it there. A deterministic simulation runs every
    /// node on its test's thread, so other tests run the real code.
    #[doc(hidden)]
    pub fn forget_deletes(forget: bool) {
        FORGETTING.with(|forgetting| forgetting.set(forget));
    }

    /// Whether the bug of [`forget_deletes`] is seeded on this thread.
    pub(crate) fn forgets_deletes() -> bool {
        FORGETTING.with(Cell::get)
    }

    /// Makes the next `count` reads of an upload's parts by a stream's
    /// open fail, in every flusher of this process.
    pub fn fail_parts_reads(count: u32) {
        FAILING.store(count, Ordering::SeqCst);
    }

    /// Whether this read fails, counting it.
    pub(crate) fn fail_parts_read() -> bool {
        let mut left = FAILING.load(Ordering::SeqCst);
        while left > 0 {
            match FAILING.compare_exchange(left, left - 1, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => return true,
                Err(now) => left = now,
            }
        }
        false
    }
}

#[cfg(not(feature = "test-util"))]
pub(crate) mod test_hooks {
    /// No read fails without the `test-util` feature.
    pub(crate) fn fail_parts_read() -> bool {
        false
    }

    /// No bug is seeded without the `test-util` feature.
    pub(crate) fn forgets_deletes() -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use skys3_types::{Epoch, Seq};

    use super::*;

    fn position(seq: u64) -> EpochSeq {
        EpochSeq::new(Epoch::new(1), Seq::new(seq))
    }

    /// The overlap counts the parts the remote acknowledged before the
    /// local completion, whichever the flusher handled first: here the
    /// completion's change comes before both parts' steps, one part
    /// acknowledged before it and one after.
    #[test]
    fn overlap_follows_when_parts_were_acknowledged_not_when_they_were_handled() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();
        runtime.block_on(async {
            let streams = Streams::default();
            let upload = position(1);
            streams.opened("k", upload);
            let parts = vec![(1, position(2)), (2, position(3))];
            streams.finish(Step::Opened(
                upload,
                Some((UploadId("r".to_owned()), parts)),
            ));
            {
                let mut map = streams.lock();
                let stream = map.get_mut(&upload).unwrap();
                stream.waiting.clear();
                stream.sending.extend([1, 2]);
            }
            let early = Instant::now();
            tokio::time::advance(Duration::from_millis(5)).await;
            streams.completed(upload);
            tokio::time::advance(Duration::from_millis(5)).await;
            let late = Instant::now();
            let etag = ETag::new("e").unwrap();
            for (number, at) in [(1, early), (2, late)] {
                streams.finish(Step::Sent {
                    upload,
                    number,
                    position: position(u64::from(number) + 1),
                    sent: Sent::Done(etag.clone(), at),
                });
            }
            let claim = streams.claim(upload).await.unwrap();
            assert_eq!(claim.flushed.len(), 2);
            assert_eq!(
                claim.at_completion,
                Some(BTreeMap::from([(1, position(2))]))
            );
        });
    }

    fn extent(seq: u64, len: u32) -> ExtentRef {
        ExtentRef {
            position: EpochSeq::new(Epoch::new(1), Seq::new(seq)),
            len,
        }
    }

    #[test]
    fn a_span_holds_every_byte_of_its_range_or_is_missing() {
        let extents = BTreeMap::from([(0, extent(1, 4)), (4, extent(2, 4)), (12, extent(4, 4))]);
        assert_eq!(
            span(&extents, (0, 8)),
            Some(vec![(0, extent(1, 4)), (4, extent(2, 4))])
        );
        // Ranges need not fall on extent boundaries.
        assert_eq!(
            span(&extents, (2, 6)),
            Some(vec![(0, extent(1, 4)), (4, extent(2, 4))])
        );
        assert_eq!(span(&extents, (12, 16)), Some(vec![(12, extent(4, 4))]));
        // Bytes 8 to 12 were never announced, nor any after 16.
        assert_eq!(span(&extents, (4, 12)), None);
        assert_eq!(span(&extents, (14, 20)), None);
        assert_eq!(span(&BTreeMap::new(), (0, 1)), None);
    }

    #[test]
    fn parts_are_fixed_ranges_of_the_body() {
        assert_eq!(part_range(1, 10), (0, 10));
        assert_eq!(part_range(3, 10), (20, 30));
        assert_eq!(part_range(2, u64::MAX), (u64::MAX, u64::MAX));
    }

    fn etag(value: &str) -> ETag {
        ETag::new(value).unwrap()
    }

    #[test]
    fn a_listing_keeps_only_the_parts_the_remote_holds_as_recorded() {
        let mut flushed = BTreeMap::from([
            (1, (position(2), etag("a"))),
            (2, (position(3), etag("b"))),
            (3, (position(4), etag("c"))),
        ]);
        let remote = BTreeMap::from([(1, etag("a")), (2, etag("x"))]);
        let stale = confirm(&mut flushed, &remote);
        assert_eq!(stale, [(2, position(3)), (3, position(4))]);
        assert_eq!(flushed.keys().copied().collect::<Vec<_>>(), [1]);
    }

    /// A resumed stream that is open: part 1 recorded from the `MPU_PART`
    /// at 2, part 2 waiting to be sent from 3, and the listing numbered 7
    /// in flight.
    fn resumed(streams: &Streams, upload: EpochSeq, open: bool) {
        let mut stream = Stream::new("k".to_owned(), Remote::Open(UploadId("r".into())), open);
        stream.flushed.insert(1, (position(2), etag("a")));
        stream.waiting.insert(2, position(3));
        stream.listing = Listing::Running(7);
        streams.lock().insert(upload, stream);
    }

    fn listed(streams: &Streams, upload: EpochSeq, run: u64, listed: Listed) {
        streams.finish(Step::Listed {
            upload,
            run,
            listed,
        });
    }

    #[test]
    fn a_listing_requeues_what_the_remote_lacks_and_a_stale_one_is_ignored() {
        let streams = Streams::default();
        let upload = position(1);
        resumed(&streams, upload, true);
        assert!(streams.wants_listing(upload, 7));
        assert!(!streams.wants_listing(upload, 6));
        // An earlier listing's answer changes nothing.
        let remote = BTreeMap::from([(1, etag("x")), (2, etag("b"))]);
        listed(&streams, upload, 6, Listed::Parts(remote.clone()));
        assert_eq!(streams.lock()[&upload].listing, Listing::Running(7));

        // The remote holds another part 1 than recorded: it is sent again.
        listed(&streams, upload, 7, Listed::Parts(remote));
        {
            let map = streams.lock();
            let stream = &map[&upload];
            assert_eq!(stream.listing, Listing::Known);
            assert!(stream.flushed.is_empty());
            assert_eq!(
                stream.waiting,
                BTreeMap::from([(1, position(2)), (2, position(3))])
            );
            assert_eq!(stream.listed.len(), 2);
        }
        // What the remote holds under a number sent since, or whose send
        // failed, is no longer known from the listing.
        let at = Instant::now();
        for (number, sent) in [(1, Sent::Done(etag("a"), at)), (2, Sent::Failed)] {
            streams.finish(Step::Sent {
                upload,
                number,
                position: position(u64::from(number) + 1),
                sent,
            });
        }
        assert!(streams.lock()[&upload].listed.is_empty());

        // An open upload whose remote upload is gone ends its stream.
        streams.lock().get_mut(&upload).unwrap().listing = Listing::Running(8);
        listed(&streams, upload, 8, Listed::Gone);
        assert!(streams.lock()[&upload].doomed);
        assert!(!streams.wants_listing(upload, 8));
    }

    #[test]
    fn a_closed_stream_leaves_a_failed_listing_to_its_completion() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            let streams = Streams::default();
            let upload = position(1);
            resumed(&streams, upload, false);
            // A gone remote upload may have been completed: the completion
            // finds out.
            listed(&streams, upload, 7, Listed::Gone);
            let stream = |check: fn(&Stream) -> bool| check(&streams.lock()[&upload]);
            assert!(stream(|s| s.listing == Listing::Due && !s.doomed));

            // A claim does not wait for a listing in flight: it drops it,
            // and the completion lists the remote upload itself.
            streams.lock().get_mut(&upload).unwrap().listing = Listing::Running(9);
            let claim = streams.claim(upload).await.unwrap();
            assert_eq!(claim.listed, None);
            assert!(!streams.wants_listing(upload, 9));
            listed(&streams, upload, 9, Listed::Dropped);
            streams.reconciled(upload, BTreeMap::from([(1, etag("a"))]));
            assert!(stream(
                |s| s.listing == Listing::Known && s.flushed.len() == 1
            ));

            // A part the Complete was refused for makes it list again.
            streams.release(upload, Verdict::Relist);
            assert!(stream(|s| s.listing == Listing::Due && !s.claimed));
            let claim = streams.claim(upload).await.unwrap();
            assert_eq!(claim.listed, None);
            streams.release(upload, Verdict::Keep);
        });
    }

    #[test]
    fn a_body_queues_each_covered_part_once() {
        let mut body = Body::new(None);
        body.extents = BTreeMap::from([(0, extent(1, 6)), (6, extent(2, 6))]);
        // No part size yet: nothing is queued.
        assert!(body.covered().is_empty());
        body.part_bytes = Some(5);
        // Part 2, bytes 5 to 10, starts in the first extent.
        assert_eq!(body.covered(), vec![(1, position(1)), (2, position(1))]);
        assert!(body.covered().is_empty());
        body.extents.insert(12, extent(3, 3));
        assert_eq!(body.covered().len(), 1);
        assert_eq!(body.spans.len(), 3);
    }

    #[tokio::test]
    async fn large_parts_are_hashed_off_the_runtime() {
        let small = Bytes::from_static(b"hello");
        let digest = md5(small).await.ok().unwrap();
        assert_eq!(hex(&digest), "5d41402abc4b2a76b9719d911017c592");
        let large = Bytes::from(vec![0u8; INLINE_HASH_BYTES + 1]);
        let expected = <[u8; 16]>::from(Md5::digest(&large[..]));
        assert_eq!(md5(large).await.ok(), Some(expected));
    }
}
