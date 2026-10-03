//! Streaming multipart flush (§7.3): a remote multipart upload for each
//! local one, filled while the client uploads, and completed only after the
//! local completion commits.
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
//! The streams live in the flusher's memory and in the index, which keeps
//! every remote upload until its end is recorded. A restarted flusher
//! resumes them from the index, and opens remote uploads for open local
//! uploads that have none. A remote upload whose `CreateMultipartUpload`
//! answer was lost, or whose `PART_FLUSHED` was lost in a crash, is left to
//! the remote bucket's abort-incomplete-uploads lifecycle rule (§7.3).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::Bytes;
use skys3_index::Part;
use skys3_io::Disk;
use skys3_log::RecordBody;
use skys3_log::record::{PartFlushed, RemoteStep};
use skys3_remote::{AbortMultipartUpload, ObjectStore, S3ErrorKind, UploadId, UploadPart};
use skys3_shard::{Shard, ShardError};
use skys3_types::{ETag, EpochSeq};
use tokio::sync::{Notify, oneshot};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;

use crate::attempt::{Failure, content_md5, read_payload};
use crate::multipart::create_request;
use crate::target::Target;

/// How many open uploads the startup scan reads per index transaction.
const SCAN_PAGE: usize = 512;

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

/// The remote upload of one local upload.
#[derive(Debug)]
struct Stream {
    key: String,
    remote: Remote,
    /// Whether the local upload is still open: parts may still come.
    open: bool,
    /// The parts the remote holds: the `MPU_PART` each came from, and its
    /// remote ETag.
    flushed: BTreeMap<u16, (EpochSeq, ETag)>,
    /// Parts to send: the latest `MPU_PART` of each number.
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
}

impl Stream {
    fn new(key: String, remote: Remote, open: bool) -> Self {
        Self {
            key,
            remote,
            open,
            flushed: BTreeMap::new(),
            waiting: BTreeMap::new(),
            sending: BTreeSet::new(),
            claimed: false,
            doomed: false,
            aborting: false,
            acked: BTreeMap::new(),
            completed_at: None,
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
}

/// The streams of one shard flusher, shared by its task and its flushes.
#[derive(Debug, Default)]
pub(crate) struct Streams {
    streams: Mutex<HashMap<EpochSeq, Stream>>,
    /// Woken whenever a part's send ends.
    idle: Notify,
    /// The `PART_FLUSHED` commits in flight. They run on their own, so a
    /// flusher that stops leaves none half done, and
    /// [`Streams::recorded`] waits for them.
    records: Mutex<Vec<JoinHandle<()>>>,
}

impl Streams {
    fn lock(&self) -> MutexGuard<'_, HashMap<EpochSeq, Stream>> {
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

    /// The local upload at `upload` was aborted.
    pub(crate) fn aborted(&self, upload: EpochSeq) {
        if let Some(stream) = self.lock().get_mut(&upload) {
            stream.open = false;
            stream.doomed = true;
            stream.waiting.clear();
        }
    }

    /// The local upload at `upload` was completed: no part comes any more.
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
    pub(crate) async fn load<D: Disk>(&self, shard: &Shard<D>) -> Result<(), ShardError> {
        for (upload, remote) in shard.remote_uploads().await? {
            let Some((_, flushed)) = shard.remote_upload(upload).await? else {
                continue;
            };
            let open = shard.upload(&remote.key, upload, 0, 0).await?.is_some();
            let mut stream = Stream::new(remote.key, Remote::Open(UploadId(remote.id)), open);
            stream.flushed = flushed
                .into_iter()
                .map(|(number, part)| (number, (part.position, part.etag)))
                .collect();
            if open {
                for (number, part) in shard.parts(upload, 0, usize::MAX).await? {
                    stream.queue(number, part.position);
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
                        Sent::Done(etag, at) => {
                            stream.flushed.insert(number, (position, etag));
                            stream.acked.insert(number, at);
                        }
                        Sent::Failed => {}
                        Sent::Gone => stream.doomed = true,
                    }
                }
                self.idle.notify_waiters();
            }
            Step::Ended(upload) => {
                streams.remove(&upload);
            }
        }
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
        streams.retain(|_, stream| {
            stream.remote != Remote::Unopened || (streaming && stream.open && !stream.doomed)
        });
        let mut sending: usize = streams.values().map(|s| s.sending.len()).sum();
        for (&upload, stream) in streams.iter_mut() {
            let remote = match &stream.remote {
                Remote::Unopened => {
                    stream.remote = Remote::Opening;
                    let task = Task::new(self, shard, target, &stream.key, upload);
                    tasks.spawn(async move { Step::Opened(upload, task.open().await) });
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
                stream.sending.insert(number);
                sending += 1;
                let task = Task::new(self, shard, target, &stream.key, upload);
                let id = remote.clone();
                tasks.spawn(async move {
                    let sent = task.send(&id, number, position).await;
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
    /// complete.
    pub(crate) async fn claim(&self, upload: EpochSeq) -> Option<Claim> {
        loop {
            let idle = self.idle.notified();
            tokio::pin!(idle);
            idle.as_mut().enable();
            {
                let mut streams = self.lock();
                let stream = streams.get_mut(&upload)?;
                let Remote::Open(id) = &stream.remote else {
                    return None;
                };
                if stream.doomed {
                    return None;
                }
                stream.claimed = true;
                stream.waiting.clear();
                if stream.sending.is_empty() {
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
                    return Some(Claim {
                        id: id.clone(),
                        flushed: stream.flushed.clone(),
                        at_completion,
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
            stream.flushed.insert(number, (position, etag));
            stream.acked.insert(number, Instant::now());
        }
    }

    /// Ends the claim of a completion on the stream of `upload`.
    pub(crate) fn release(&self, upload: EpochSeq, verdict: Verdict) {
        let mut streams = self.lock();
        match verdict {
            Verdict::Completed => {
                streams.remove(&upload);
            }
            Verdict::Abort | Verdict::Keep => {
                if let Some(stream) = streams.get_mut(&upload) {
                    stream.claimed = false;
                    stream.doomed |= verdict == Verdict::Abort;
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
    /// backoff while the local upload is open. Returns the ID and the local
    /// upload's parts, or `None` if the local upload closed first.
    ///
    /// The parts are read before the remote upload is opened, by the read
    /// that is tried again: once the remote upload and its record exist,
    /// nothing can fail and leave them behind. A part stored after the read
    /// is queued by its change, since the stream exists before this task.
    async fn open(&self) -> Option<(UploadId, Vec<(u16, EpochSeq)>)> {
        let (key, upload) = (self.key.as_str(), self.upload);
        let identity = self.target.identity(self.shard.shard(), upload);
        let remote_key = format!("{}{key}", self.target.prefix);
        for attempt in 1.. {
            let read = match self.shard.upload(key, upload, 0, usize::MAX).await {
                Ok(_) if test_hooks::fail_parts_read() => Err("a test failed it".to_owned()),
                read => read.map_err(|error| error.to_string()),
            };
            let (local, parts) = match read {
                Ok(None) => return None,
                Ok(Some(found)) => found,
                Err(error) => {
                    tracing::debug!(key, %upload, %error, "a remote upload was not opened");
                    tokio::time::sleep(self.target.settings.backoff(attempt)).await;
                    continue;
                }
            };
            let request =
                match create_request(remote_key.clone(), &local.metadata, &local.tags, &identity) {
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
                    tokio::time::sleep(self.target.settings.backoff(attempt)).await;
                }
            }
        }
        None
    }

    /// Sends the `MPU_PART` at `position`, part `number`, to the remote
    /// upload `id`, and records it.
    async fn send(&self, id: &UploadId, number: u16, position: EpochSeq) -> Sent {
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
        match upload_part(&self.shard, &self.target, &self.key, id, number, &part).await {
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
            Err(Failure::Remote(error)) if error.kind() == S3ErrorKind::NoSuchUpload => Sent::Gone,
            Err(failure) => {
                tracing::debug!(key = self.key, upload = %self.upload, number, %failure,
                    "a part was not streamed");
                Sent::Failed
            }
        }
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

/// Uploads `part`, part `number` of the remote upload `id` of `key`, from
/// the local log, within the target's in-flight budget, and returns its
/// remote ETag.
pub(crate) async fn upload_part<S: ObjectStore, D: Disk>(
    shard: &Shard<D>,
    target: &Target<S>,
    key: &str,
    id: &UploadId,
    number: u16,
    part: &Part,
) -> Result<ETag, Failure> {
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

/// A failpoint for tests: reads of an upload's parts by a stream's open
/// that fail, as an index read may.
#[cfg(feature = "test-util")]
pub mod test_hooks {
    use std::sync::atomic::{AtomicU32, Ordering};

    static FAILING: AtomicU32 = AtomicU32::new(0);

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
mod test_hooks {
    /// No read fails without the `test-util` feature.
    pub(crate) fn fail_parts_read() -> bool {
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
}
