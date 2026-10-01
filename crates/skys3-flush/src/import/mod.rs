//! The namespace import of a `write_back` bucket (§9.1): listing the
//! remote prefix into `IMPORT` records, in key ranges listed in parallel,
//! and reading the remote for keys the import has not reached.

mod split;

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
use skys3_shard::ShardSet;
use skys3_types::{BucketDocument, ETag, WriteIdentity, shard_for_key};
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::target::{FlushSettings, ImportProgress};

/// The most keys one listing request asks for: S3's page size.
pub const IMPORT_PAGE_KEYS: u32 = 1000;

/// The `Content-Type` S3 gives an object stored without one.
pub const DEFAULT_CONTENT_TYPE: &str = "binary/octet-stream";

/// The prefix of user metadata names in stored metadata.
const USER_METADATA_PREFIX: &str = "x-amz-meta-";

/// Where a bucket's import is, shared by the import task, the bucket's
/// flushers ([`ImportProgress`]), and the reads that fall through to the
/// remote ([`RemoteReader`]).
///
/// It starts with nothing passed, which is safe whatever the stored
/// checkpoints say: until the import task has read them, keys fall through
/// to the remote and tombstones are kept.
#[derive(Debug, Default)]
pub struct ImportState {
    inner: Mutex<StateInner>,
}

#[derive(Debug, Default)]
struct StateInner {
    ranges: ImportRanges,
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

    /// Shows `ranges`, which are durable, if any, and counts `imported`
    /// more records.
    fn advance(&self, ranges: Option<ImportRanges>, imported: u64) {
        let mut inner = self.lock();
        if let Some(ranges) = ranges {
            inner.ranges = ranges;
        }
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

impl ImportProgress for ImportState {
    fn passed(&self, key: &str) -> bool {
        ImportState::passed(self, key)
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
    /// The import's rate, page size, and backoffs.
    pub(crate) settings: FlushSettings,
    /// How many ranges a new import is split into, one stream each:
    /// `import_parallel_streams`.
    pub(crate) streams: usize,
    /// The clock of a listed object without `Last-Modified`.
    pub(crate) wall: Arc<dyn WallClock>,
    /// Where the import shows its progress.
    pub(crate) state: Arc<ImportState>,
}

/// Imports the remote namespace of the job's bucket from its target,
/// under the target's prefix, into the bucket's shards, resuming from the
/// ranges and checkpoints the index holds, at most
/// `settings.import_keys_per_second` keys a second over all streams.
///
/// An import with no stored ranges, or with one, which a single-stream
/// import stored, first discovers split points for `streams` streams
/// after its checkpoint ([`split::split_points`]) and stores the ranges
/// durably. Stored ranges are resumed as they are, whatever `streams` says
/// now.
///
/// Each range is then listed by its own stream, one page at a time, with
/// `StartAfter` its checkpoint, or its start, up to its end: the page's
/// objects become `IMPORT` records, committed to their shards concurrently
/// once the rate allows ([`Throttle`]), and once every one is applied, the
/// page's last key is stored as the range's checkpoint, durably, and only
/// then shown in the state. A failed request or commit is retried after
/// the flush backoff; the import never gives up. Keys under the capability
/// probe's scratch prefix (§7.2) are never imported.
pub(crate) async fn run<S: ObjectStore, D: Disk>(job: ImportJob<S, D>) {
    let streams = job.streams.clamp(1, MAX_IMPORT_RANGES);
    let mut retry = Retry::new(&job.bucket, &job.state, &job.settings);
    let stored = loop {
        match job.set.import_ranges(&job.bucket.bucket_id).await {
            Ok(stored) => break stored,
            Err(error) => retry.wait(error.to_string()).await,
        }
    };
    let ranges = match stored {
        Some(ranges) if ranges.is_done() || ranges.ranges().len() > 1 => ranges,
        stored => {
            let after = match stored.map(|ranges| ranges.position()) {
                Some(ImportCheckpoint::Running { after }) => after,
                _ => None,
            };
            job.state.advance(
                Some(
                    ImportCheckpoint::Running {
                        after: after.clone(),
                    }
                    .into(),
                ),
                0,
            );
            let splits = loop {
                let found =
                    split::split_points(&job.store, &job.prefix, after.as_deref(), streams).await;
                match found {
                    Ok(splits) => break splits,
                    Err(error) => {
                        retry
                            .wait(format!("discovering split points failed: {error}"))
                            .await;
                    }
                }
            };
            let ranges = ImportRanges::split(after, splits);
            while ranges.ranges().len() > 1
                && let Err(error) = job
                    .set
                    .set_import_ranges(&job.bucket.bucket_id, Some(ranges.clone()))
                    .await
            {
                retry
                    .wait(format!("storing the import ranges failed: {error}"))
                    .await;
            }
            ranges
        }
    };
    job.state.advance(Some(ranges.clone()), 0);
    if ranges.is_done() {
        return;
    }
    tracing::info!(
        bucket = %job.bucket.name,
        ranges = ranges.ranges().len(),
        "the namespace import is running"
    );
    let import = Arc::new(ImportRun::new(job, ranges.clone()));
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

/// A running import, shared by its streams.
struct ImportRun<S, D: Disk> {
    store: Arc<S>,
    prefix: String,
    bucket: BucketDocument,
    set: ShardSet<D>,
    settings: FlushSettings,
    wall: Arc<dyn WallClock>,
    state: Arc<ImportState>,
    /// The rate limit of all streams together.
    throttle: Throttle,
    /// The most keys a listing request asks for.
    page_keys: u32,
    /// Every stream's latest checkpoint, and how many updates it holds.
    latest: Mutex<(ImportRanges, u64)>,
    /// How many updates the stored ranges hold. Held while they are
    /// stored, so that a store never replaces a later one.
    stored: tokio::sync::Mutex<u64>,
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
    fn new(job: ImportJob<S, D>, ranges: ImportRanges) -> Self {
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
            set: job.set,
            settings: job.settings,
            wall: job.wall,
            state: job.state,
            throttle,
            page_keys,
            latest: Mutex::new((ranges, 0)),
            stored: tokio::sync::Mutex::new(0),
        }
    }

    /// Lists and imports one range, from its checkpoint to its end.
    async fn list(self: Arc<Self>, mut stream: Stream) {
        let prefix = &self.prefix;
        let mut retry = Retry::new(&self.bucket, &self.state, &self.settings);
        loop {
            let mut request = ListObjectsV2::new(prefix.clone()).with_max_keys(self.page_keys);
            if let Some(from) = stream.after.as_ref().or(stream.start.as_ref()) {
                request = request.with_start_after(format!("{prefix}{from}"));
            }
            let page = match self.store.list_objects_v2(request).await {
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
            self.throttle.admit(listed).await;
            if let Err(error) = commit(&self.set, &self.bucket, imports).await {
                retry.wait(error).await;
                continue;
            }
            let next = match last.filter(|_| !ended) {
                Some(last) => ImportCheckpoint::Running { after: Some(last) },
                None => ImportCheckpoint::Done,
            };
            self.checkpoint(stream.index, next.clone(), count, &mut retry)
                .await;
            retry.failures = 0;
            match next {
                ImportCheckpoint::Running { after } => stream.after = after,
                ImportCheckpoint::Done => return,
            }
        }
    }

    /// Records `checkpoint` as range `index`'s, stores every stream's
    /// latest checkpoint durably unless a store since has, and shows them.
    /// Streams that finish pages together share one store.
    async fn checkpoint(
        &self,
        index: usize,
        checkpoint: ImportCheckpoint,
        imported: u64,
        retry: &mut Retry<'_>,
    ) {
        let update = {
            let mut latest = lock(&self.latest);
            latest.0.set(index, checkpoint);
            latest.1 += 1;
            latest.1
        };
        let mut stored = self.stored.lock().await;
        if *stored >= update {
            self.state.advance(None, imported);
            return;
        }
        let (ranges, updates) = lock(&self.latest).clone();
        while let Err(error) = self
            .set
            .set_import_ranges(&self.bucket.bucket_id, Some(ranges.clone()))
            .await
        {
            retry
                .wait(format!("storing the checkpoint failed: {error}"))
                .await;
        }
        *stored = updates;
        self.state.advance(Some(ranges), imported);
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
    /// Failures since the last success.
    failures: u32,
}

impl<'a> Retry<'a> {
    fn new(
        bucket: &'a BucketDocument,
        state: &'a ImportState,
        settings: &'a FlushSettings,
    ) -> Self {
        Self {
            bucket: bucket.name.as_str(),
            state,
            settings,
            failures: 0,
        }
    }

    async fn wait(&mut self, error: String) {
        self.failures += 1;
        tracing::warn!(bucket = self.bucket, %error, "the namespace import will retry");
        self.state.failed(error);
        tokio::time::sleep(self.settings.backoff(self.failures)).await;
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

/// Commits `imports` to their shards concurrently, and returns once every
/// one is applied.
async fn commit<D: Disk>(
    set: &ShardSet<D>,
    bucket: &BucketDocument,
    imports: Vec<Import>,
) -> Result<(), String> {
    let mut commits = JoinSet::new();
    for import in imports {
        let shard = ShardRef::new(
            bucket.bucket_id.clone(),
            shard_for_key(&bucket.bucket_id, import.key.as_bytes(), bucket.shards),
        );
        let Some(shard) = set.get(&shard).await else {
            return Err(format!("shard {shard} is not open"));
        };
        commits.spawn(async move { shard.commit(RecordBody::Import(import)).await });
    }
    while let Some(done) = commits.join_next().await {
        match done {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => return Err(error.to_string()),
            Err(error) => return Err(error.to_string()),
        }
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
