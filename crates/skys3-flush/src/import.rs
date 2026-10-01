//! The namespace import of a `write_back` bucket (§9.1): listing the
//! remote prefix into `IMPORT` records, and reading the remote for keys
//! the import has not reached.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use skys3_index::{ImportCheckpoint, ListItem, ObjectVersion, Payload};
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
/// checkpoint says: until the import task has read it, keys fall through
/// to the remote and tombstones are kept.
#[derive(Debug)]
pub struct ImportState {
    inner: Mutex<StateInner>,
}

#[derive(Debug)]
struct StateInner {
    checkpoint: ImportCheckpoint,
    imported: u64,
    error: Option<String>,
}

/// A bucket's import, as the admin API shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportStatus {
    /// How far the import has got.
    pub checkpoint: ImportCheckpoint,
    /// `IMPORT` records committed since the node started.
    pub imported: u64,
    /// The last error, until the import gets past it.
    pub error: Option<String>,
}

impl Default for ImportState {
    fn default() -> Self {
        Self {
            inner: Mutex::new(StateInner {
                checkpoint: ImportCheckpoint::Running { after: None },
                imported: 0,
                error: None,
            }),
        }
    }
}

impl ImportState {
    /// How far the import has got: every remote key it has passed has its
    /// `IMPORT` record applied.
    #[must_use]
    pub fn checkpoint(&self) -> ImportCheckpoint {
        self.lock().checkpoint.clone()
    }

    /// The import's status.
    #[must_use]
    pub fn status(&self) -> ImportStatus {
        let inner = self.lock();
        ImportStatus {
            checkpoint: inner.checkpoint.clone(),
            imported: inner.imported,
            error: inner.error.clone(),
        }
    }

    fn advance(&self, checkpoint: ImportCheckpoint, imported: u64) {
        let mut inner = self.lock();
        inner.checkpoint = checkpoint;
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
        self.lock().checkpoint.passed(key)
    }
}

/// Imports the remote namespace of `bucket` from `store`, under `prefix`,
/// into the bucket's shards in `set`, resuming from the checkpoint the
/// index holds, at most `settings.import_keys_per_second` keys a second.
///
/// One page of the listing at a time: the page's objects become `IMPORT`
/// records, committed to their shards concurrently, and once every one is
/// applied, the page's last key is stored as the checkpoint, durably, and
/// only then shown in `state`. A failed request or commit is retried
/// after the flush backoff; the import never gives up. Keys under the
/// capability probe's scratch prefix (§7.2) are never imported.
pub(crate) async fn run<S: ObjectStore, D: Disk>(
    store: Arc<S>,
    prefix: String,
    bucket: BucketDocument,
    set: ShardSet<D>,
    settings: FlushSettings,
    wall: Arc<dyn WallClock>,
    state: Arc<ImportState>,
) {
    let mut retry = Retry {
        bucket: bucket.name.as_str(),
        state: &state,
        settings: &settings,
        failures: 0,
    };
    let mut after = loop {
        match set.import_checkpoint(&bucket.bucket_id).await {
            Ok(None) => break None,
            Ok(Some(ImportCheckpoint::Running { after })) => break after,
            Ok(Some(ImportCheckpoint::Done)) => {
                state.advance(ImportCheckpoint::Done, 0);
                return;
            }
            Err(error) => retry.wait(error.to_string()).await,
        }
    };
    state.advance(
        ImportCheckpoint::Running {
            after: after.clone(),
        },
        0,
    );
    let rate = settings.import_keys_per_second.max(1) as f64;
    let started = Instant::now();
    let mut listed = 0u64;
    loop {
        let page_keys = settings.import_page_keys.clamp(1, IMPORT_PAGE_KEYS);
        let mut request = ListObjectsV2::new(prefix.clone()).with_max_keys(page_keys);
        if let Some(after) = &after {
            request = request.with_start_after(format!("{prefix}{after}"));
        }
        let page = match store.list_objects_v2(request).await {
            Ok(page) => page,
            Err(error) => {
                retry
                    .wait(format!("listing the remote failed: {error}"))
                    .await;
                continue;
            }
        };
        let listed_now = page.objects.len() as u64;
        let last = page
            .objects
            .last()
            .and_then(|object| object.key.strip_prefix(&prefix))
            .map(str::to_owned);
        let imports: Vec<Import> = page
            .objects
            .into_iter()
            .filter_map(|object| import_of(object, &prefix, &*wall))
            .collect();
        let count = imports.len() as u64;
        if let Err(error) = commit(&set, &bucket, imports).await {
            retry.wait(error).await;
            continue;
        }
        let next = match last.filter(|_| page.is_truncated) {
            Some(last) => ImportCheckpoint::Running { after: Some(last) },
            None => ImportCheckpoint::Done,
        };
        while let Err(error) = set
            .set_import_checkpoint(&bucket.bucket_id, Some(next.clone()))
            .await
        {
            retry
                .wait(format!("storing the checkpoint failed: {error}"))
                .await;
        }
        retry.failures = 0;
        state.advance(next.clone(), count);
        let ImportCheckpoint::Running { after: next } = next else {
            tracing::info!(bucket = %bucket.name, "the namespace import is done");
            return;
        };
        after = next;
        listed += listed_now;
        tokio::time::sleep_until(started + Duration::from_secs_f64(listed as f64 / rate)).await;
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

impl Retry<'_> {
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

    /// How far the bucket's import has got.
    #[must_use]
    pub fn import(&self) -> ImportCheckpoint {
        self.import.checkpoint()
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
