//! Read-through fill (§9.2): the bytes of an evicted version are read from
//! the remote target into the shard's log, where they become clean cache,
//! and a fill that finds the remote changed out of band commits `ADOPT`.
//!
//! - **The request.** Every remote read of a fill names the version the
//!   entry describes: `If-Match: <remote_etag>`, and `versionId` when the
//!   remote is versioned. A large object is read in chunks of
//!   [`FILL_CHUNK_BYTES`], each with the same conditions, so no fill holds
//!   more than one chunk in memory, and the fills of a target share an
//!   in-flight budget of their own, which also bounds a chunk.
//! - **Committing.** Each chunk is committed as `EXTENT` records of the
//!   shard, `extent_bytes` each, like the body of a PUT (§5.1), but no
//!   larger than a chunk: one remote read never spans more than a chunk,
//!   even where `extent_bytes` is larger. The chunks of an evicted multipart
//!   object end at its part boundaries, so each part gets extents of its
//!   own. Once every
//!   extent is committed, [`Shard::fill`] makes the entry clean with those
//!   extents as its payload (§4.2, Evicted → Clean), unless a write
//!   replaced the version meanwhile. The fill's bytes still serve the reads
//!   that named it then: a version never changes.
//! - **Coalescing.** Concurrent reads of one version join one fill, and
//!   each is served its range as soon as the fill has committed the extents
//!   it covers, while later extents are still being read. The readers of a
//!   fill wake in the order they began waiting ([`Tracker`]), so a
//!   simulation replays from its seed.
//! - **`ADOPT`.** A remote read that fails its precondition, or finds the
//!   object or the version gone, means the remote changed out of band. The
//!   remote is the system of record for clean data, so the fill HEADs the
//!   key and commits `ADOPT` with what the remote holds now, naming the
//!   version it filled. The state machine applies it only if the entry is
//!   still that clean or evicted version; a local write that made it dirty
//!   meanwhile wins, and the flusher's conflict policy owns the key (§7.2).
//!   The conflict is counted either way. The reader then resolves the key
//!   again ([`FillError::Changed`]) and reads the adopted version, or the
//!   local write. A key whose remote object is gone has no version to
//!   adopt: no record removes a clean entry, so its reads fail
//!   ([`FillError::Gone`]) until a write replaces it.

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::ops::Range;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::Bytes;
use skys3_index::{Entry, EntryState, Payload};
use skys3_io::{Disk, WallClock};
use skys3_log::record::{Adopt, Extent, ExtentRef};
use skys3_log::{RecordBody, ShardRef};
use skys3_remote::{
    ByteRange, GetObject, GetOutput, HeadObject, ObjectStore, S3Error, S3ErrorKind, VersionId,
};
use skys3_shard::{Outcome, Shard};
use skys3_types::EpochSeq;
use tokio::sync::{Notify, Semaphore, mpsc};

use crate::import::loaded_metadata;
use crate::metrics::Counters;
use crate::target::FlushSettings;

/// The most bytes one remote read of a fill asks for. A larger object is
/// read in chunks of this size, rounded down to whole extents.
pub const FILL_CHUNK_BYTES: u64 = 8 << 20;

/// How many times a fill sends a remote request that failed with a
/// transient error before it gives up. A fill runs on a client's read, so
/// it does not retry for long; the client retries the read.
const FILL_ATTEMPTS: u32 = 3;

/// The bytes of a range of a filled version, in order, as
/// [`Filler::read`] streams them. An error ends the stream: the fill
/// failed after the read began.
pub type FillBody = mpsc::Receiver<io::Result<Bytes>>;

/// Why a read through a fill could not begin.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum FillError {
    /// The entry no longer is the evicted version the read named: an
    /// `ADOPT` or a local write replaced it, or another fill made it
    /// clean. Resolve the key again.
    #[error("the entry changed; resolve the key again")]
    Changed,
    /// The remote object is gone, deleted out of band.
    #[error("the object is gone from the bucket's remote target")]
    Gone,
    /// The fill failed: a remote error, or the shard refused its extents.
    #[error("the fill failed: {0}")]
    Failed(String),
}

/// What one fill has done so far, as its readers follow it.
#[derive(Debug, Clone, Default)]
struct Progress {
    /// The extents committed so far, in object order.
    extents: Vec<ExtentRef>,
    /// The bytes they hold.
    filled: u64,
    /// How the fill ended, once it has.
    end: Option<Result<(), FillError>>,
}

/// A fill in progress: of a version of a key of a shard.
type FillKey = (ShardRef, String, EpochSeq);

/// One fill's [`Progress`], as its readers follow it.
///
/// Not a `watch` channel: one registers each waiter with one of several
/// notifiers that tokio's thread-local random number generator picks, and
/// wakes them notifier by notifier, so the readers of one fill would wake
/// in an order the simulation's seed does not decide. A [`Notify`] wakes
/// them in the order they began waiting.
#[derive(Debug, Default)]
struct Tracker {
    progress: Mutex<Progress>,
    changed: Notify,
}

impl Tracker {
    /// Waits until `ready` finds what it needs in the progress, and
    /// returns that.
    async fn wait_for<T>(&self, ready: impl Fn(&Progress) -> Option<T>) -> T {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            // Registered before the check, so no change in between is
            // missed.
            changed.as_mut().enable();
            if let Some(found) = ready(&lock(&self.progress)) {
                return found;
            }
            changed.await;
        }
    }
}

/// The fill's side of its [`Tracker`]. A fill dropped before it ends, as
/// when its runtime stops, ends its progress as stopped.
struct Reporter(Arc<Tracker>);

impl Reporter {
    fn update(&self, change: impl FnOnce(&mut Progress)) {
        change(&mut lock(&self.0.progress));
        self.0.changed.notify_waiters();
    }

    fn end(&self, end: Result<(), FillError>) {
        self.update(|progress| progress.end = Some(end));
    }
}

impl Drop for Reporter {
    fn drop(&mut self) {
        if lock(&self.0.progress).end.is_none() {
            self.end(Err(stopped()));
        }
    }
}

/// The read-through fills of one `write_back` bucket's remote target.
///
/// Cloning a filler returns another handle to the same fills.
pub struct Filler<S> {
    inner: Arc<Inner<S>>,
}

impl<S> Clone for Filler<S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<S> fmt::Debug for Filler<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Filler")
            .field("prefix", &self.inner.prefix)
            .field("extent_bytes", &self.inner.extent_bytes)
            .finish_non_exhaustive()
    }
}

struct Inner<S> {
    store: Arc<S>,
    prefix: String,
    settings: FlushSettings,
    counters: Counters,
    wall: Arc<dyn WallClock>,
    /// The size of the `EXTENT` records a fill commits: `extent_bytes`,
    /// but at most [`FILL_CHUNK_BYTES`].
    extent_bytes: u32,
    /// The most bytes one remote read asks for: whole extents, at most
    /// [`FILL_CHUNK_BYTES`] and the in-flight budget, and at least one
    /// extent.
    chunk: u64,
    /// The in-flight budget, in KiB, as a flush target keeps one.
    inflight: Semaphore,
    inflight_kib: u32,
    fills: Mutex<HashMap<FillKey, Arc<Tracker>>>,
}

impl<S: ObjectStore> Filler<S> {
    /// The fills from `store`, under the key prefix `prefix` (empty for a
    /// whole bucket). Fills commit `EXTENT` records of
    /// `settings.extent_bytes`, but at most [`FILL_CHUNK_BYTES`], read
    /// whole extents at a time, hold at most `settings.max_inflight_bytes`
    /// in memory at once (one extent if the budget is smaller than that),
    /// and retry transient errors after `settings`' backoff. They are
    /// counted in `counters`, and `wall` stands in for a `Last-Modified`
    /// the remote does not return.
    pub fn new(
        store: Arc<S>,
        prefix: impl Into<String>,
        settings: FlushSettings,
        counters: Counters,
        wall: Arc<dyn WallClock>,
    ) -> Self {
        let extent = settings.extent_bytes.clamp(1, FILL_CHUNK_BYTES);
        // A read of whole extents, within the chunk and the budget.
        let chunk = (FILL_CHUNK_BYTES.min(settings.max_inflight_bytes) / extent).max(1) * extent;
        // At most `FILL_CHUNK_BYTES`, which fits.
        let extent_bytes = u32::try_from(extent).unwrap_or(u32::MAX);
        let inflight_kib = u32::try_from(settings.max_inflight_bytes.div_ceil(1024))
            .unwrap_or(u32::MAX)
            .clamp(1, u32::try_from(Semaphore::MAX_PERMITS).unwrap_or(u32::MAX));
        Self {
            inner: Arc::new(Inner {
                store,
                prefix: prefix.into(),
                settings,
                counters,
                wall,
                extent_bytes,
                chunk,
                inflight: Semaphore::new(inflight_kib as usize),
                inflight_kib,
                fills: Mutex::default(),
            }),
        }
    }

    /// Reads the bytes `range` of the evicted version `version` of `key`
    /// through a fill: the fill of that version in progress, or a new one.
    ///
    /// It returns once the fill has committed the range's first byte, and
    /// the [`FillBody`] streams the rest as the fill commits it. The fill
    /// runs to its end even if every reader goes away, so the version
    /// becomes clean cache.
    ///
    /// # Errors
    ///
    /// [`FillError::Changed`] if the entry is not that evicted version, or
    /// the fill adopted the remote's; [`FillError::Gone`] and
    /// [`FillError::Failed`] if the fill failed before the range's first
    /// byte.
    pub async fn read<D: Disk>(
        &self,
        shard: &Shard<D>,
        key: &str,
        version: EpochSeq,
        range: Range<u64>,
    ) -> Result<FillBody, FillError> {
        let progress = self.join(shard, key, version);
        let begun = progress
            .wait_for(|p| (p.end.is_some() || p.filled > range.start).then(|| p.end.clone()))
            .await;
        if let Some(Err(error)) = begun {
            return Err(error);
        }
        let (sender, receiver) = mpsc::channel(1);
        tokio::spawn(send_range(shard.clone(), progress, range, sender));
        Ok(receiver)
    }

    /// The progress of the fill of `version` of `key`, started if none is
    /// in progress.
    fn join<D: Disk>(&self, shard: &Shard<D>, key: &str, version: EpochSeq) -> Arc<Tracker> {
        let id = (shard.shard().clone(), key.to_owned(), version);
        let mut fills = lock(&self.inner.fills);
        if let Some(progress) = fills.get(&id) {
            return Arc::clone(progress);
        }
        let tracker = Arc::new(Tracker::default());
        fills.insert(id.clone(), Arc::clone(&tracker));
        let reporter = Reporter(Arc::clone(&tracker));
        let filler = self.clone();
        let shard = shard.clone();
        tokio::spawn(async move {
            let end = filler.fill(&shard, &id.1, version, &reporter).await;
            // Forget the fill before announcing its end, so that a read
            // that comes later starts a new one rather than join a failed
            // one.
            lock(&filler.inner.fills).remove(&id);
            reporter.end(end);
        });
        tracker
    }

    /// Fills the evicted version `version` of `key`, reporting each extent
    /// to `progress`.
    async fn fill<D: Disk>(
        &self,
        shard: &Shard<D>,
        key: &str,
        version: EpochSeq,
        progress: &Reporter,
    ) -> Result<(), FillError> {
        let entry = shard
            .entry(key)
            .await
            .map_err(|error| FillError::Failed(error.to_string()))?
            .filter(|entry| entry.version == version && entry.state == EntryState::Evicted)
            .ok_or(FillError::Changed)?;
        let (Some(object), Some(remote_etag)) = (&entry.object, &entry.remote_etag) else {
            return Err(FillError::Failed(
                "the evicted entry names no remote object".to_owned(),
            ));
        };
        let size = object.size;
        let chunk = self.inner.chunk;
        // A multipart object's extents end at its part boundaries, so that
        // each part gets its own (§9.3).
        let mut boundaries = part_ends(&object.payload).into_iter().peekable();
        let mut extents = Vec::new();
        let mut offset = 0;
        loop {
            while boundaries.next_if(|&end| end <= offset).is_some() {}
            let end = boundaries.peek().map_or(size, |&end| end.min(size));
            let len = (end - offset).min(chunk);
            let mut request =
                GetObject::new(self.remote_key(key)).with_if_match(remote_etag.clone());
            request.version_id = entry.remote_version_id.clone().map(VersionId);
            if len > 0
                && let Some(range) = ByteRange::inclusive(offset, offset + len - 1)
            {
                request = request.with_range(range);
            }
            let _permit = self.reserve(len).await;
            let output = match self.get(request).await {
                Ok(output) => output,
                Err(error) if changed(&error) => return Err(self.adopt(shard, key, &entry).await),
                Err(error) => return Err(FillError::Failed(error.to_string())),
            };
            check_chunk(&output, size, len)?;
            for (at, data) in split(output.body, self.inner.extent_bytes) {
                let extent = Extent {
                    key: key.to_owned(),
                    offset: offset + at,
                    data,
                };
                let extent = shard
                    .append_extent(extent)
                    .await
                    .map_err(|error| FillError::Failed(error.to_string()))?;
                extents.push(extent);
                progress.update(|progress| {
                    progress.extents.push(extent);
                    progress.filled += u64::from(extent.len);
                });
            }
            offset += len;
            if offset >= size {
                break;
            }
        }
        match shard.fill(key, version, Payload::Extents(extents)).await {
            Ok(Ok(())) => {
                self.inner.counters.fills.inc();
            }
            // A write replaced the version while it was read; the bytes
            // still serve the reads that named it.
            Ok(Err(refusal)) => tracing::debug!(key, %refusal, "a filled version is not cached"),
            Err(error) => tracing::warn!(key, %error, "a filled version is not cached"),
        };
        Ok(())
    }

    /// The remote changed out of band under the clean `entry` of `key`:
    /// commits `ADOPT` of what the remote holds now, which the state
    /// machine drops if a local write came first, and returns how the read
    /// goes on.
    async fn adopt<D: Disk>(&self, shard: &Shard<D>, key: &str, entry: &Entry) -> FillError {
        self.inner.counters.fill_conflicts.inc();
        let head = HeadObject::new(self.remote_key(key));
        let info = match self
            .retrying(|| self.inner.store.head_object(head.clone()))
            .await
        {
            Ok(info) => info,
            Err(error) if gone(&error) => {
                tracing::warn!(key, "a clean object is gone from the remote target");
                return FillError::Gone;
            }
            Err(error) => return FillError::Failed(error.to_string()),
        };
        let last_modified_ms = info.last_modified_ms.unwrap_or_else(|| {
            u64::try_from(self.inner.wall.now().as_millis()).unwrap_or(u64::MAX)
        });
        let adopt = Adopt {
            key: key.to_owned(),
            expected_seq: entry.version.seq,
            size: info.size,
            last_modified_ms,
            remote_etag: info.etag.clone(),
            remote_version_id: info.version_id.clone().map(|id| id.0),
            // As lazily loaded metadata (§9.1): an adopted version must not
            // look like an imported stub whose metadata is not loaded yet.
            metadata: loaded_metadata(&info),
            checksums: Default::default(),
        };
        match shard.commit(RecordBody::Adopt(adopt)).await {
            Ok(committed) => {
                match committed.outcome {
                    Outcome::Applied(_) => {
                        tracing::info!(key, etag = %info.etag,
                            "adopted an out-of-band write to the remote target");
                    }
                    Outcome::Rejected(rejection) => {
                        tracing::info!(key, %rejection,
                            "an out-of-band write to the remote target was not adopted");
                    }
                }
                FillError::Changed
            }
            Err(error) => FillError::Failed(error.to_string()),
        }
    }

    /// Sends `request`, retrying transient errors.
    async fn get(&self, request: GetObject) -> Result<GetOutput, S3Error> {
        self.retrying(|| self.inner.store.get_object(request.clone()))
            .await
    }

    /// Runs `send` until it succeeds, fails for good, or has failed
    /// [`FILL_ATTEMPTS`] times.
    async fn retrying<T, F: Future<Output = Result<T, S3Error>>>(
        &self,
        mut send: impl FnMut() -> F,
    ) -> Result<T, S3Error> {
        let mut attempt = 1;
        loop {
            match send().await {
                Err(error) if error.is_transient() && attempt < FILL_ATTEMPTS => {
                    tokio::time::sleep(self.inner.settings.backoff(attempt)).await;
                    attempt += 1;
                }
                result => return result,
            }
        }
    }

    fn remote_key(&self, key: &str) -> String {
        format!("{}{}", self.inner.prefix, key)
    }

    /// Waits until `size` bytes fit the in-flight budget. A read is at most
    /// the budget unless the budget is smaller than one extent; it then
    /// waits for the whole budget.
    async fn reserve(&self, size: u64) -> Option<tokio::sync::SemaphorePermit<'_>> {
        let kib = u32::try_from(size.div_ceil(1024))
            .unwrap_or(u32::MAX)
            .clamp(1, self.inner.inflight_kib);
        self.inner.inflight.acquire_many(kib).await.ok()
    }
}

/// Streams the bytes `range` of the fill that `progress` follows into
/// `sender`, each extent once the fill has committed it, until the range
/// is sent, the receiver is gone, or the fill ends without the extent.
async fn send_range<D: Disk>(
    shard: Shard<D>,
    progress: Arc<Tracker>,
    range: Range<u64>,
    sender: mpsc::Sender<io::Result<Bytes>>,
) {
    let mut start = 0;
    let mut index = 0;
    while start < range.end {
        let next = progress
            .wait_for(|p| {
                (p.extents.len() > index || p.end.is_some())
                    .then(|| (p.extents.get(index).copied(), p.end.clone()))
            })
            .await;
        let extent = match next {
            (Some(extent), _) => extent,
            (None, end) => {
                let error = match end {
                    Some(Err(error)) => error,
                    _ => FillError::Failed("the fill ended short of the range".to_owned()),
                };
                let _ = sender.send(Err(io::Error::other(error))).await;
                return;
            }
        };
        let end = start + u64::from(extent.len);
        if end > range.start {
            let read = shard
                .payload(extent.position)
                .await
                .map_err(io::Error::other)
                .and_then(|data| clip(&data, start, &range));
            let failed = read.is_err();
            if sender.send(read).await.is_err() || failed {
                return;
            }
        }
        start = end;
        index += 1;
    }
}

/// The part of `data`, an extent at body offset `start`, inside `range`.
fn clip(data: &Bytes, start: u64, range: &Range<u64>) -> io::Result<Bytes> {
    let len = data.len() as u64;
    let from = range.start.saturating_sub(start).min(len);
    let to = range.end.saturating_sub(start).min(len);
    // Both are at most the extent's length, which fits in `usize`.
    Ok(data.slice(from as usize..to as usize))
}

/// Checks that a chunk read for a fill is the `len` bytes asked for, of an
/// object of `size` bytes.
fn check_chunk(output: &GetOutput, size: u64, len: u64) -> Result<(), FillError> {
    if output.info.size != size || output.body.len() as u64 != len {
        return Err(FillError::Failed(format!(
            "the remote returned {} bytes of a {}-byte object, expected {len} of {size}",
            output.body.len(),
            output.info.size
        )));
    }
    Ok(())
}

/// Where each part of a multipart object `payload` ends, in object
/// order; none for any other payload.
fn part_ends(payload: &Payload) -> Vec<u64> {
    let Payload::Parts { parts, .. } = payload else {
        return Vec::new();
    };
    parts
        .iter()
        .scan(0, |end, part| {
            *end += part.size;
            Some(*end)
        })
        .collect()
}

/// `body` cut into pieces of at most `extent_bytes`, each with its offset
/// in `body`.
fn split(body: Bytes, extent_bytes: u32) -> impl Iterator<Item = (u64, Bytes)> {
    let step = extent_bytes as usize;
    (0..body.len())
        .step_by(step)
        .map(move |at| (at as u64, body.slice(at..(at + step).min(body.len()))))
}

/// Whether a remote read failed because the version it named is no longer
/// there: the remote changed out of band.
fn changed(error: &S3Error) -> bool {
    matches!(
        error.kind(),
        S3ErrorKind::PreconditionFailed
            | S3ErrorKind::NoSuchKey
            | S3ErrorKind::NoSuchVersion
            | S3ErrorKind::MethodNotAllowed
    )
}

/// Whether a HEAD found no current object.
fn gone(error: &S3Error) -> bool {
    matches!(
        error.kind(),
        S3ErrorKind::NoSuchKey | S3ErrorKind::MethodNotAllowed
    )
}

/// The error of a read whose fill stopped without saying how it ended.
fn stopped() -> FillError {
    FillError::Failed("the fill stopped".to_owned())
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // Every update is a single insert or removal.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bodies_split_into_extents() {
        let body = Bytes::from_static(b"abcdefg");
        let pieces: Vec<_> = split(body, 3).collect();
        assert_eq!(
            pieces,
            [
                (0, Bytes::from_static(b"abc")),
                (3, Bytes::from_static(b"def")),
                (6, Bytes::from_static(b"g")),
            ]
        );
        assert_eq!(split(Bytes::new(), 3).count(), 0);
    }

    #[test]
    fn extents_are_clipped_to_the_range() {
        let data = Bytes::from_static(b"0123456789");
        assert_eq!(clip(&data, 10, &(12..15)).unwrap(), "234");
        assert_eq!(clip(&data, 10, &(0..100)).unwrap(), "0123456789");
        assert_eq!(clip(&data, 10, &(15..100)).unwrap(), "56789");
    }

    #[test]
    fn readers_of_a_fill_wake_in_the_order_they_began_waiting() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime.block_on(async {
            let tracker = Arc::new(Tracker::default());
            let woken = Arc::new(Mutex::new(Vec::new()));
            let mut readers = Vec::new();
            for reader in 0..16 {
                let (tracker, woken) = (Arc::clone(&tracker), Arc::clone(&woken));
                readers.push(tokio::spawn(async move {
                    tracker.wait_for(|p| (p.filled > 0).then_some(())).await;
                    lock(&woken).push(reader);
                }));
            }
            for _ in 0..4 {
                tokio::task::yield_now().await;
            }
            let reporter = Reporter(Arc::clone(&tracker));
            reporter.update(|progress| progress.filled = 1);
            for reader in readers {
                reader.await.unwrap();
            }
            assert_eq!(*lock(&woken), (0..16).collect::<Vec<_>>());
            // A fill that goes away without an end ends as stopped.
            drop(reporter);
            let end = tracker.wait_for(|p| p.end.clone()).await;
            assert!(matches!(end, Err(FillError::Failed(_))), "{end:?}");
        });
    }
}
