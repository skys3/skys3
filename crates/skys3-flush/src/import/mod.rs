//! The namespace import of a `write_back` bucket (§9.1): listing the
//! remote prefix into `IMPORT` records, in key ranges listed in parallel,
//! and reading the remote for keys the import has not reached.
//!
//! The import runs on its **owner**, the node whose replica of the bucket's
//! shard 0 serves as primary. It commits each `IMPORT` to its shard's
//! primary, wherever that is, and its progress, as `IMPORT_PROGRESS`
//! records, to shard 0 on its own replica first and then to every other
//! shard. A new owner resumes from the progress shard 0 holds.

mod split;

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use skys3_index::{
    ImportCheckpoint, ImportRanges, ListItem, MAX_IMPORT_RANGES, ObjectVersion, Payload,
};
use skys3_io::{Disk, WallClock};
use skys3_log::record::{Import, MAX_KEY_LEN, MAX_STORAGE_CLASS_LEN, Metadata};
use skys3_log::{RecordBody, ShardRef};
use skys3_remote::probe::ConditionalProbe;
use skys3_remote::{
    ByteRange, GetObject, HeadObject, ListObjectsV2, ListedObject, ObjectInfo, ObjectStore,
    S3Error, S3ErrorKind,
};
use skys3_shard::{Shard, ShardSet};
use skys3_types::{BucketDocument, ETag, ShardId, WriteIdentity, shard_for_key};
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;

use crate::target::FlushSettings;

/// Commits a record to a shard on whichever node is the shard's primary,
/// and resolves once the primary has applied it: how the import's owner
/// commits `IMPORT` and `IMPORT_PROGRESS` records to shards it is not the
/// primary of. [`FlushService::with_commit`](crate::FlushService::with_commit)
/// sets it; without one, records go to the node's own replicas, which
/// suits a node that is the primary of every shard.
pub type Commit = Arc<
    dyn Fn(ShardRef, RecordBody) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>>
        + Send
        + Sync,
>;

/// A [`Commit`] to the node's own replicas in `set`.
pub(crate) fn local_commit<D: Disk>(set: ShardSet<D>) -> Commit {
    Arc::new(move |shard, body| {
        let set = set.clone();
        Box::pin(async move {
            let Some(shard) = set.get(&shard).await else {
                return Err(format!("shard {shard} is not open"));
            };
            shard
                .commit(body)
                .await
                .map(drop)
                .map_err(|e| e.to_string())
        })
    })
}

/// The most keys one listing request asks for: S3's page size.
pub const IMPORT_PAGE_KEYS: u32 = 1000;

/// The `Content-Type` S3 gives an object stored without one.
pub const DEFAULT_CONTENT_TYPE: &str = "binary/octet-stream";

/// The prefix of user metadata names in stored metadata.
const USER_METADATA_PREFIX: &str = "x-amz-meta-";

/// Where a bucket's import is as this node knows it, for the reads that
/// fall through to the remote ([`RemoteReader`]) and the admin API.
///
/// It shows the newest `IMPORT_PROGRESS` that the import task stored, if
/// it runs here, or that a replica of one of the bucket's shards on the
/// node applied ([`FlushService::reconcile`](crate::FlushService::reconcile)).
/// Each is a bound that holds for the whole bucket, since the owner stores
/// progress only once the `IMPORT` records it covers are applied on every
/// shard. It starts with nothing passed, which is safe: keys fall through
/// to the remote until it shows more. The flushers ask their own shard
/// instead ([`Target::with_import`](crate::Target::with_import)).
#[derive(Debug, Default)]
pub struct ImportState {
    inner: Mutex<StateInner>,
}

#[derive(Debug, Default)]
struct StateInner {
    ranges: ImportRanges,
    /// The number of the `IMPORT_PROGRESS` that `ranges` come from, if any.
    update: Option<u64>,
    imported: u64,
    error: Option<String>,
}

/// A bucket's import, as the admin API shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportStatus {
    /// How far the import has got without a gap
    /// ([`ImportRanges::position`]).
    pub checkpoint: ImportCheckpoint,
    /// The key ranges the import lists in parallel, and how far each has
    /// got.
    pub ranges: ImportRanges,
    /// `IMPORT` records committed since the node started.
    pub imported: u64,
    /// The last error, until the import gets past it.
    pub error: Option<String>,
}

impl ImportState {
    /// How far the import has got without a gap: it has passed every key
    /// up to the checkpoint, and may have passed later ones.
    #[must_use]
    pub fn checkpoint(&self) -> ImportCheckpoint {
        self.lock().ranges.position()
    }

    /// Whether the import has passed `key`: every remote object up to it
    /// in its range has its `IMPORT` record applied.
    #[must_use]
    pub fn passed(&self, key: &str) -> bool {
        self.lock().ranges.passed(key)
    }

    /// The import's status.
    #[must_use]
    pub fn status(&self) -> ImportStatus {
        let inner = self.lock();
        ImportStatus {
            checkpoint: inner.ranges.position(),
            ranges: inner.ranges.clone(),
            imported: inner.imported,
            error: inner.error.clone(),
        }
    }

    /// Shows `ranges`, the progress numbered `update`, unless the state
    /// shows a later one.
    pub(crate) fn observe(&self, update: u64, ranges: ImportRanges) {
        let mut inner = self.lock();
        if inner.update.is_none_or(|shown| shown < update) {
            inner.ranges = ranges;
            inner.update = Some(update);
        }
    }

    /// Shows `progress`, which is committed, if any, and counts
    /// `imported` more records.
    fn advance(&self, progress: Option<(u64, ImportRanges)>, imported: u64) {
        if let Some((update, ranges)) = progress {
            self.observe(update, ranges);
        }
        let mut inner = self.lock();
        inner.imported += imported;
        inner.error = None;
    }

    fn failed(&self, error: String) {
        self.lock().error = Some(error);
    }

    fn lock(&self) -> MutexGuard<'_, StateInner> {
        // Every update is a single assignment.
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// What a bucket's import task runs on.
pub(crate) struct ImportJob<S, D: Disk> {
    /// The bucket's remote target.
    pub(crate) store: Arc<S>,
    /// The target's prefix.
    pub(crate) prefix: String,
    /// The bucket.
    pub(crate) bucket: BucketDocument,
    /// The node's shards.
    pub(crate) set: ShardSet<D>,
    /// The node's replica of the bucket's shard 0, its primary.
    pub(crate) owner: Shard<D>,
    /// How records reach the bucket's other shards.
    pub(crate) commit: Commit,
    /// The import's rate, page size, and backoffs.
    pub(crate) settings: FlushSettings,
    /// How many ranges a new import is split into, one stream each:
    /// `import_parallel_streams`.
    pub(crate) streams: usize,
    /// The clock of a listed object without `Last-Modified`.
    pub(crate) wall: Arc<dyn WallClock>,
    /// Where the import shows its progress.
    pub(crate) state: Arc<ImportState>,
    /// When to stop.
    pub(crate) stop: Stop,
    /// Earlier import tasks of the bucket that were told to stop, which
    /// this one waits for, so that none of their checkpoint writes lands
    /// after this one's.
    pub(crate) previous: Vec<JoinHandle<()>>,
}

/// Tells an import's tasks to stop: on [`FlushService::shutdown`], or once
/// the service no longer follows the bucket.
///
/// They stop between steps, never while a checkpoint is being stored. That
/// write is a job on the index's pool, which finishes it whether or not its
/// caller still waits, so an import that dropped it midway could have a
/// stale checkpoint land after a later import's, and move the bucket's
/// stored progress back. A stopped import's task therefore ends only once
/// none of its writes is pending.
///
/// [`FlushService::shutdown`]: crate::FlushService::shutdown
#[derive(Debug, Clone)]
pub(crate) struct Stop(watch::Receiver<bool>);

impl Stop {
    /// A signal and the sender that raises it, by sending `true` or by
    /// being dropped.
    pub(crate) fn new() -> (watch::Sender<bool>, Self) {
        let (sender, receiver) = watch::channel(false);
        (sender, Self(receiver))
    }

    fn is_raised(&self) -> bool {
        *self.0.borrow() || self.0.has_changed().is_err()
    }

    /// Runs `work`, unless the signal is raised first; `None` then.
    async fn unless<T>(&mut self, work: impl Future<Output = T>) -> Option<T> {
        tokio::select! {
            biased;
            _ = self.0.wait_for(|stopped| *stopped) => None,
            done = work => Some(done),
        }
    }
}

/// Imports the remote namespace of the job's bucket from its target,
/// under the target's prefix, into the bucket's shards, resuming from the
/// progress the owner's shard 0 holds, at most
/// `settings.import_keys_per_second` keys a second over all streams.
///
/// Without progress in shard 0, it resumes from the checkpoint a build
/// that kept it in the node's index stored, if any. An import with no
/// progress, or with one range, which a single-stream import stored, first
/// discovers split points for `streams` streams after its checkpoint
/// ([`split::split_points`]) and stores the ranges as progress. Stored
/// ranges are resumed as they are, whatever `streams` says now.
///
/// Each range is then listed by its own stream, one page at a time, with
/// `StartAfter` its checkpoint, or its start, up to its end: the page's
/// objects become `IMPORT` records, committed to their shards concurrently
/// once the rate allows ([`Throttle`]), and once every one is applied, the
/// ranges with the page's last key as the range's checkpoint are stored as
/// the next progress ([`ImportRun::store`]), and only then shown in the
/// state. A failed request or commit is retried after the flush backoff;
/// the import never gives up, but stops when told to, as when the node is
/// no longer the owner. Keys under the capability probe's scratch prefix
/// (§7.2) are never imported.
pub(crate) async fn run<S: ObjectStore, D: Disk>(mut job: ImportJob<S, D>) {
    let mut stop = job.stop.clone();
    for previous in std::mem::take(&mut job.previous) {
        if stop.unless(previous).await.is_none() {
            return;
        }
    }
    let streams = job.streams.clamp(1, MAX_IMPORT_RANGES);
    let mut retry = Retry::new(&job.bucket, &job.state, &job.settings, &job.stop);
    let stored = loop {
        let Some(read) = stop.unless(resume_point(&job)).await else {
            return;
        };
        match read {
            Ok(stored) => break stored,
            Err(error) => {
                if !retry.wait(error).await {
                    return;
                }
            }
        }
    };
    let (update, stored) = match stored {
        Some((update, ranges)) => (update, Some(ranges)),
        None => (0, None),
    };
    let (ranges, split) = match stored {
        Some(ranges) if ranges.is_done() || ranges.ranges().len() > 1 => (ranges, false),
        stored => {
            let after = match stored.map(|ranges| ranges.position()) {
                Some(ImportCheckpoint::Running { after }) => after,
                _ => None,
            };
            let splits = loop {
                let discovery =
                    split::split_points(&job.store, &job.prefix, after.as_deref(), streams);
                let Some(found) = stop.unless(discovery).await else {
                    return;
                };
                match found {
                    Ok(splits) => break splits,
                    Err(error) => {
                        let error = format!("discovering split points failed: {error}");
                        if !retry.wait(error).await {
                            return;
                        }
                    }
                }
            };
            (ImportRanges::split(after, splits), true)
        }
    };
    drop(retry);
    let import = ImportRun::new(job, update, ranges.clone());
    let mut retry = Retry::new(
        &import.bucket,
        &import.state,
        &import.settings,
        &import.stop,
    );
    if split && ranges.ranges().len() > 1 && !import.checkpoint(None, 0, &mut retry).await {
        return;
    }
    drop(retry);
    import.state.advance(Some((update, ranges.clone())), 0);
    if ranges.is_done() {
        return;
    }
    tracing::info!(
        bucket = %import.bucket.name,
        ranges = ranges.ranges().len(),
        "the namespace import is running"
    );
    let import = Arc::new(import);
    let mut streams = JoinSet::new();
    for (index, range) in ranges.ranges().iter().enumerate() {
        if let ImportCheckpoint::Running { after } = &range.checkpoint {
            let stream = Stream {
                index,
                start: ranges.start(index).map(str::to_owned),
                end: range.end.clone(),
                after: after.clone(),
            };
            streams.spawn(Arc::clone(&import).list(stream));
        }
    }
    while let Some(done) = streams.join_next().await {
        if let Err(error) = done
            && let Ok(panic) = error.try_into_panic()
        {
            std::panic::resume_unwind(panic);
        }
    }
    tracing::info!(bucket = %import.bucket.name, "the namespace import is done");
}

/// Where an import resumes: the progress the owner's shard 0 holds, or
/// else the checkpoint a build that kept it in the node's index stored, as
/// progress number 0.
async fn resume_point<S, D: Disk>(
    job: &ImportJob<S, D>,
) -> Result<Option<(u64, ImportRanges)>, String> {
    match job.owner.import_progress().await {
        Ok(Some(progress)) => Ok(Some(progress)),
        Ok(None) => match job.set.import_ranges(&job.bucket.bucket_id).await {
            Ok(legacy) => Ok(legacy.map(|ranges| (0, ranges))),
            Err(error) => Err(error.to_string()),
        },
        Err(error) => Err(error.to_string()),
    }
}

/// A running import, shared by its streams.
struct ImportRun<S, D: Disk> {
    store: Arc<S>,
    prefix: String,
    bucket: BucketDocument,
    owner: Shard<D>,
    commit: Commit,
    settings: FlushSettings,
    wall: Arc<dyn WallClock>,
    state: Arc<ImportState>,
    /// The rate limit of all streams together.
    throttle: Throttle,
    /// The most keys a listing request asks for.
    page_keys: u32,
    /// Every stream's latest checkpoint, and the progress number it
    /// would be stored as.
    latest: Mutex<(ImportRanges, u64)>,
    /// The number of the latest progress stored. Held while progress is
    /// stored, so that a store never replaces a later one.
    stored: tokio::sync::Mutex<u64>,
    /// When to stop.
    stop: Stop,
}

/// One range's stream.
struct Stream {
    /// The range's number.
    index: usize,
    /// The key the range starts after, if any.
    start: Option<String>,
    /// The range's last key, if any.
    end: Option<String>,
    /// The range's checkpoint.
    after: Option<String>,
}

impl<S: ObjectStore, D: Disk> ImportRun<S, D> {
    /// The import of `job` from `ranges`, whose progress so far is
    /// numbered `update`.
    fn new(job: ImportJob<S, D>, update: u64, ranges: ImportRanges) -> Self {
        let throttle = Throttle::new(job.settings.import_keys_per_second);
        // A page is never larger than a second's keys, so that the
        // throttle can admit it whole.
        let page_keys = job
            .settings
            .import_page_keys
            .clamp(1, IMPORT_PAGE_KEYS)
            .min(u32::try_from(throttle.capacity).unwrap_or(u32::MAX));
        Self {
            store: job.store,
            prefix: job.prefix,
            bucket: job.bucket,
            owner: job.owner,
            commit: job.commit,
            settings: job.settings,
            wall: job.wall,
            state: job.state,
            throttle,
            page_keys,
            latest: Mutex::new((ranges, update)),
            stored: tokio::sync::Mutex::new(update),
            stop: job.stop,
        }
    }

    /// Lists and imports one range, from its checkpoint to its end, or
    /// until the import is stopped.
    async fn list(self: Arc<Self>, mut stream: Stream) {
        let prefix = &self.prefix;
        let mut stop = self.stop.clone();
        let mut retry = Retry::new(&self.bucket, &self.state, &self.settings, &self.stop);
        while !stop.is_raised() {
            let mut request = ListObjectsV2::new(prefix.clone()).with_max_keys(self.page_keys);
            if let Some(from) = stream.after.as_ref().or(stream.start.as_ref()) {
                request = request.with_start_after(format!("{prefix}{from}"));
            }
            let Some(listed) = stop.unless(self.store.list_objects_v2(request)).await else {
                return;
            };
            let page = match listed {
                Ok(page) => page,
                Err(error) => {
                    retry
                        .wait(format!("listing the remote failed: {error}"))
                        .await;
                    continue;
                }
            };
            let mut objects = page.objects;
            // Keys after the range's end are the next range's.
            let within = objects
                .iter()
                .take_while(|object| {
                    let key = object.key.strip_prefix(prefix.as_str());
                    stream
                        .end
                        .as_deref()
                        .is_none_or(|end| key.is_none_or(|key| key <= end))
                })
                .count();
            let ended = !page.is_truncated || within < objects.len();
            objects.truncate(within);
            let last = objects
                .last()
                .and_then(|object| object.key.strip_prefix(prefix.as_str()))
                .map(str::to_owned);
            let listed = objects.len() as u64;
            let imports: Vec<Import> = objects
                .into_iter()
                .filter_map(|object| import_of(object, prefix, &*self.wall))
                .collect();
            let count = imports.len() as u64;
            if stop.unless(self.throttle.admit(listed)).await.is_none() {
                return;
            }
            // A commit dropped midway may still apply: harmless, since an
            // `IMPORT` is conditional, and dropped once the progress of
            // its shard has passed its key.
            let commit = commit(&self.commit, &self.bucket, imports);
            let Some(committed) = stop.unless(commit).await else {
                return;
            };
            if let Err(error) = committed {
                retry.wait(error).await;
                continue;
            }
            let next = match last.filter(|_| !ended) {
                Some(last) => ImportCheckpoint::Running { after: Some(last) },
                None => ImportCheckpoint::Done,
            };
            // Never dropped midway: see `Stop`.
            if !self
                .checkpoint(Some((stream.index, next.clone())), count, &mut retry)
                .await
            {
                return;
            }
            retry.failures = 0;
            match next {
                ImportCheckpoint::Running { after } => stream.after = after,
                ImportCheckpoint::Done => return,
            }
        }
    }

    /// Records `checkpoint` as its range's, if given, stores every
    /// stream's latest checkpoint as the next progress unless a store
    /// since has, and shows it. Streams that finish pages together share
    /// one store. `false` if the import was stopped while a failed store
    /// waited to be retried.
    async fn checkpoint(
        &self,
        checkpoint: Option<(usize, ImportCheckpoint)>,
        imported: u64,
        retry: &mut Retry<'_>,
    ) -> bool {
        // Counted now: the records are committed, and a node may show the
        // progress from its shard before this store returns.
        self.state.advance(None, imported);
        let update = {
            let mut latest = lock(&self.latest);
            if let Some((index, checkpoint)) = checkpoint {
                latest.0.set(index, checkpoint);
            }
            latest.1 += 1;
            latest.1
        };
        let mut stored = self.stored.lock().await;
        if *stored >= update {
            return true;
        }
        let (ranges, update) = lock(&self.latest).clone();
        if !self.store(update, &ranges, retry).await {
            return false;
        }
        *stored = update;
        self.state.advance(Some((update, ranges)), 0);
        true
    }

    /// Commits `ranges` as progress number `update`: to the owner's own
    /// replica of shard 0 first, which fails once another node has taken
    /// the shard over, and then to every other shard of the bucket. A new
    /// owner resumes from shard 0's progress, so no shard holds progress
    /// that shard 0 lacks, and the new owner's progress, numbered after
    /// it, is never behind what a shard holds. `false` if the import was
    /// stopped while a failed commit waited to be retried.
    async fn store(&self, update: u64, ranges: &ImportRanges, retry: &mut Retry<'_>) -> bool {
        let body = RecordBody::ImportProgress(ranges.to_progress(update));
        while let Err(error) = self.owner.commit(body.clone()).await {
            let error = format!("storing the import progress failed: {error}");
            if !retry.wait(error).await {
                return false;
            }
        }
        let mut pending: Vec<ShardId> = self.bucket.shards.shards().skip(1).collect();
        while !pending.is_empty() {
            let mut commits = JoinSet::new();
            for shard in pending.drain(..) {
                let shard_ref = ShardRef::new(self.bucket.bucket_id.clone(), shard);
                let committed = (self.commit)(shard_ref, body.clone());
                commits.spawn(async move { (shard, committed.await) });
            }
            let mut failed = None;
            while let Some(done) = commits.join_next().await {
                match done {
                    Ok((_, Ok(()))) => {}
                    Ok((shard, Err(error))) => {
                        pending.push(shard);
                        failed = Some(error);
                    }
                    Err(error) => failed = Some(error.to_string()),
                }
            }
            if let Some(error) = failed {
                let error = format!("storing the import progress failed: {error}");
                if !retry.wait(error).await {
                    return false;
                }
            }
        }
        true
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A token bucket over listed keys, shared by every stream of an import:
/// it fills at the import's rate, up to a second's keys, and starts empty.
/// Every page reserves its keys before it is committed, the first and the
/// last included, and waits until the bucket has filled for them, so no
/// stretch of `t` seconds commits more than `rate * (t + 1)` keys, however
/// many streams there are and however slow the requests before it were.
#[derive(Debug)]
struct Throttle {
    /// Keys a second.
    rate: f64,
    /// The most keys the bucket holds: a second's, and at least one.
    capacity: u64,
    /// The keys in the bucket, negative while pages wait for theirs, as
    /// of the instant.
    tokens: Mutex<(f64, Instant)>,
}

impl Throttle {
    fn new(keys_per_second: u64) -> Self {
        let capacity = keys_per_second.max(1);
        Self {
            rate: capacity as f64,
            capacity,
            tokens: Mutex::new((0.0, Instant::now())),
        }
    }

    /// Reserves `keys` keys, at most the capacity, and waits until they
    /// may be committed.
    async fn admit(&self, keys: u64) {
        let keys = keys.min(self.capacity) as f64;
        let wait = {
            let mut tokens = lock(&self.tokens);
            let now = Instant::now();
            let filled = tokens.0 + now.duration_since(tokens.1).as_secs_f64() * self.rate;
            *tokens = (filled.min(self.capacity as f64) - keys, now);
            (-tokens.0).max(0.0) / self.rate
        };
        if wait > 0.0 {
            tokio::time::sleep(Duration::from_secs_f64(wait)).await;
        }
    }
}

/// Backs off between failed attempts of an import, recording each error.
struct Retry<'a> {
    bucket: &'a str,
    state: &'a ImportState,
    settings: &'a FlushSettings,
    stop: Stop,
    /// Failures since the last success.
    failures: u32,
}

impl<'a> Retry<'a> {
    fn new(
        bucket: &'a BucketDocument,
        state: &'a ImportState,
        settings: &'a FlushSettings,
        stop: &Stop,
    ) -> Self {
        Self {
            bucket: bucket.name.as_str(),
            state,
            settings,
            stop: stop.clone(),
            failures: 0,
        }
    }

    /// Records `error` and backs off; `false` if the import was stopped
    /// meanwhile.
    async fn wait(&mut self, error: String) -> bool {
        self.failures += 1;
        tracing::warn!(bucket = self.bucket, %error, "the namespace import will retry");
        self.state.failed(error);
        let backoff = tokio::time::sleep(self.settings.backoff(self.failures));
        self.stop.unless(backoff).await.is_some()
    }
}

/// The `IMPORT` record of a listed object, or `None` for a key outside the
/// prefix, the empty key, a key too long, or one under the probe's scratch
/// prefix.
fn import_of(object: ListedObject, prefix: &str, wall: &dyn WallClock) -> Option<Import> {
    let key = object.key.strip_prefix(prefix)?;
    if key.is_empty() || key.len() > MAX_KEY_LEN || key.starts_with(ConditionalProbe::SCRATCH_DIR) {
        return None;
    }
    Some(Import {
        key: key.to_owned(),
        size: object.size,
        last_modified_ms: object
            .last_modified_ms
            .unwrap_or_else(|| millis(wall.now())),
        etag: object.etag,
        storage_class: object
            .storage_class
            .filter(|class| !class.is_empty() && class.len() <= MAX_STORAGE_CLASS_LEN),
    })
}

/// Commits `imports` to their shards concurrently, through `commit`, and
/// returns once every one is applied.
async fn commit(
    commit: &Commit,
    bucket: &BucketDocument,
    imports: Vec<Import>,
) -> Result<(), String> {
    let mut commits = JoinSet::new();
    for import in imports {
        let shard = ShardRef::new(
            bucket.bucket_id.clone(),
            shard_for_key(&bucket.bucket_id, import.key.as_bytes(), bucket.shards),
        );
        commits.spawn(commit(shard, RecordBody::Import(import)));
    }
    while let Some(done) = commits.join_next().await {
        done.map_err(|error| error.to_string())??;
    }
    Ok(())
}

fn millis(since_epoch: Duration) -> u64 {
    u64::try_from(since_epoch.as_millis()).unwrap_or(u64::MAX)
}

/// A `write_back` bucket's remote target, read on behalf of clients:
/// HEAD and GET of keys the import has not reached, listings merged with
/// the index while it runs, and the metadata that an imported stub loads
/// lazily (§9.1).
#[derive(Debug)]
pub struct RemoteReader<S> {
    store: Arc<S>,
    prefix: String,
    import: Arc<ImportState>,
    wall: Arc<dyn WallClock>,
}

impl<S> Clone for RemoteReader<S> {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            prefix: self.prefix.clone(),
            import: Arc::clone(&self.import),
            wall: Arc::clone(&self.wall),
        }
    }
}

/// A remote object as a client sees it: its metadata as an object version
/// without local bytes, and its remote version ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteObject {
    /// Size, ETag, `Last-Modified`, `Content-Type`, and user metadata.
    pub object: ObjectVersion,
    /// The remote version ID, on a versioned remote.
    pub version_id: Option<String>,
}

impl<S: ObjectStore> RemoteReader<S> {
    pub(crate) fn new(
        store: Arc<S>,
        prefix: String,
        import: Arc<ImportState>,
        wall: Arc<dyn WallClock>,
    ) -> Self {
        Self {
            store,
            prefix,
            import,
            wall,
        }
    }

    /// How far the bucket's import has got without a gap
    /// ([`ImportState::checkpoint`]).
    #[must_use]
    pub fn import(&self) -> ImportCheckpoint {
        self.import.checkpoint()
    }

    /// Whether the bucket's import has passed `key`
    /// ([`ImportState::passed`]), which a later range may have done before
    /// [`RemoteReader::import`] gets there.
    #[must_use]
    pub fn passed(&self, key: &str) -> bool {
        self.import.passed(key)
    }

    /// HEADs `key`: `None` if the remote has no object there.
    ///
    /// # Errors
    ///
    /// The store's error.
    pub async fn head(&self, key: &str) -> Result<Option<RemoteObject>, S3Error> {
        let request = HeadObject::new(format!("{}{key}", self.prefix));
        match self.store.head_object(request).await {
            Ok(info) => Ok(Some(self.remote_object(info))),
            Err(error) if error.kind() == S3ErrorKind::NoSuchKey => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// The bytes `range` of `key`, if its ETag is still `etag`; `None` if
    /// the object changed or is gone.
    ///
    /// # Errors
    ///
    /// The store's error.
    pub async fn get(
        &self,
        key: &str,
        etag: &ETag,
        range: std::ops::Range<u64>,
    ) -> Result<Option<Bytes>, S3Error> {
        let mut request =
            GetObject::new(format!("{}{key}", self.prefix)).with_if_match(etag.clone());
        if range.is_empty() {
            return Ok(Some(Bytes::new()));
        }
        if let Some(range) = ByteRange::inclusive(range.start, range.end - 1) {
            request = request.with_range(range);
        }
        match self.store.get_object(request).await {
            Ok(output) => Ok(Some(output.body)),
            Err(error)
                if matches!(
                    error.kind(),
                    S3ErrorKind::NoSuchKey | S3ErrorKind::PreconditionFailed
                ) =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// One page of the remote's listing under `prefix`, rolled up at
    /// `delimiter`, after `start_after`, or continuing the page `token`
    /// ended, with at most `max_items` items. Keys and common prefixes are
    /// the bucket's, without the target's prefix. Returns the page's items
    /// in order, and the token of the next page, if any.
    ///
    /// # Errors
    ///
    /// The store's error.
    pub async fn list(
        &self,
        prefix: &str,
        delimiter: Option<&str>,
        start_after: Option<&str>,
        token: Option<String>,
        max_items: usize,
    ) -> Result<(Vec<ListItem>, Option<String>), S3Error> {
        let max_keys = u32::try_from(max_items)
            .unwrap_or(u32::MAX)
            .clamp(1, IMPORT_PAGE_KEYS);
        let mut request =
            ListObjectsV2::new(format!("{}{prefix}", self.prefix)).with_max_keys(max_keys);
        if let Some(delimiter) = delimiter {
            request = request.with_delimiter(delimiter);
        }
        match (token, start_after) {
            (Some(token), _) => request = request.with_continuation_token(token),
            (None, Some(after)) => {
                request = request.with_start_after(format!("{}{after}", self.prefix))
            }
            (None, None) => {}
        }
        let output = self.store.list_objects_v2(request).await?;
        let mut items: Vec<ListItem> = output
            .common_prefixes
            .into_iter()
            .filter_map(|common| {
                Some(ListItem::Prefix(
                    common.strip_prefix(&self.prefix)?.to_owned(),
                ))
            })
            .collect();
        for object in output.objects {
            if let Some(import) = import_of(object, &self.prefix, &*self.wall) {
                items.push(ListItem::Object {
                    key: import.key.clone(),
                    object: Box::new(stub(import)),
                });
            }
        }
        items.sort_by(|a, b| a.name().cmp(b.name()));
        let next = output
            .next_continuation_token
            .filter(|_| output.is_truncated);
        Ok((items, next))
    }

    fn remote_object(&self, info: ObjectInfo) -> RemoteObject {
        RemoteObject {
            object: ObjectVersion {
                size: info.size,
                last_modified_ms: info
                    .last_modified_ms
                    .unwrap_or_else(|| millis(self.wall.now())),
                local_etag: info.etag.clone(),
                write_identity: None,
                metadata: loaded_metadata(&info),
                tags: Default::default(),
                checksums: Default::default(),
                storage_class: None,
                copy_source: None,
                payload: Payload::None,
            },
            version_id: info.version_id.map(|id| id.0),
        }
    }
}

/// The object version an `IMPORT` record stores: a stub whose metadata is
/// not loaded.
fn stub(import: Import) -> ObjectVersion {
    ObjectVersion {
        size: import.size,
        last_modified_ms: import.last_modified_ms,
        local_etag: import.etag,
        write_identity: None,
        metadata: Metadata::new(),
        tags: Default::default(),
        checksums: Default::default(),
        storage_class: import.storage_class,
        copy_source: None,
        payload: Payload::None,
    }
}

/// The stored metadata of a remote object, as lazily loaded metadata
/// records it (§9.1): its `Content-Type`, S3's default where the remote
/// gives none, so that loaded metadata is never empty, and its user
/// metadata, without SkyS3's write identity.
#[must_use]
pub fn loaded_metadata(info: &ObjectInfo) -> Metadata {
    let content_type = info
        .content_type
        .clone()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_CONTENT_TYPE.to_owned());
    let mut metadata = Metadata::from([("content-type".to_owned(), content_type)]);
    for (name, value) in info.metadata.iter() {
        if !name.eq_ignore_ascii_case(WriteIdentity::METADATA_KEY) {
            metadata.insert(
                format!("{USER_METADATA_PREFIX}{}", name.to_ascii_lowercase()),
                value.to_owned(),
            );
        }
    }
    metadata
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_most_streams_configurable_is_the_most_ranges_stored() {
        let streams = skys3_config::MAX_IMPORT_PARALLEL_STREAMS;
        assert_eq!(usize::try_from(streams).unwrap(), MAX_IMPORT_RANGES);
    }
}
