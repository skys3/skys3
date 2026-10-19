//! The reconciliation scan of a `write_back` bucket (§9.1): an optional,
//! periodic re-listing of the remote prefix that reports where the remote
//! differs from the index, and changes nothing.
//!
//! Under exclusive ownership (§7.2) a scan finds nothing. A difference is
//! a write outside SkyS3, or a write of SkyS3's that the index does not
//! show. The scan only reads: the remote with `ListObjectsV2` and
//! `HeadObject`, and the index of the shards whose primary is on this node
//! with [`Shard::entries`] and [`Shard::peek`]. It commits no record,
//! writes nothing to the remote, and wakes no flusher; what it finds goes
//! to its report ([`ReconcileStatus`]), the metrics, and the log.
//!
//! **What is compared.** A key is compared when the remote lists an
//! object there or the index holds an entry for it, the key belongs to a
//! shard the node leads and serves, and the index mirrors it (not the
//! probe's scratch keys, the snapshots' directory, or the peer descriptor).
//! Only a clean or evicted entry says what the remote holds, its
//! `remote_etag` and size. A dirty entry, a tombstone, or a key in
//! conflict is not yet at the remote, and its flusher's conditional
//! request finds any out-of-band write to it (§7.2), so it is skipped. A
//! remote object without an entry counts only once the shard's import has
//! passed its key; before that, the import creates its stub.
//!
//! **Confirmation.** A listing page and the index are read at different
//! moments, while flushes go on, so every candidate is confirmed before it
//! is reported: the scan HEADs the key, then reads its entry again, and
//! reports the difference the HEAD shows only if the entry still has the
//! version and the expected remote state it was compared with. A key
//! written or flushed in between is left for the next scan. A remote
//! object carrying a write identity of the key's own shard at a position
//! after what the scan read is a write it raced with, and is not reported
//! either. What neither check catches is a multipart or streamed upload
//! whose identity is older than that read, completed, flushed, deleted,
//! and its delete flushed, all between the confirming HEAD and the second
//! read: reported once, and not by the next scan. Each node scans only the
//! shards it leads, so its index has applied every `FLUSHED` its flushers
//! committed.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use skys3_index::{Entry, EntryState, ImportRanges};
use skys3_io::{Disk, WallClock};
use skys3_log::ShardRef;
use skys3_remote::{
    HeadObject, ListObjectsV2, ListObjectsV2Output, ObjectInfo, ObjectStore, S3ErrorKind,
};
use skys3_shard::{Shard, ShardError};
use skys3_types::{
    BucketDocument, BucketId, ClusterId, ETag, EpochSeq, ShardId, WriteIdentity, shard_for_key,
};

use crate::import::{IMPORT_PAGE_KEYS, Throttle, mirrored};
use crate::metrics::FlushMetrics;
use crate::peer::BoxFuture;
use crate::shard::Ready;
use crate::target::FlushSettings;

/// The most differences one scan confirms and reports. Candidates beyond
/// it are counted as unconfirmed, without a HEAD.
pub const MAX_REPORTED_DIFFERENCES: usize = 1000;

/// The most differences one scan logs a line for; the rest are counted in
/// one line at its end.
pub const MAX_LOGGED_DIFFERENCES: usize = 100;

/// How the remote differs from the index at one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DifferenceKind {
    /// The remote holds an object at a key the import has passed, and the
    /// index has no entry for it.
    RemoteOnly,
    /// The index holds a clean or evicted entry, which says the remote
    /// holds its version, and the remote has no object there.
    IndexOnly,
    /// The remote's object has another ETag than the entry's
    /// `remote_etag`.
    EtagMismatch,
    /// The remote's object has the entry's `remote_etag` but another size.
    SizeMismatch,
}

impl DifferenceKind {
    /// Every kind, in report order.
    pub const ALL: [Self; 4] = [
        Self::RemoteOnly,
        Self::IndexOnly,
        Self::EtagMismatch,
        Self::SizeMismatch,
    ];

    /// The kind's name in the admin API, the metrics, and the log.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RemoteOnly => "remote_only",
            Self::IndexOnly => "index_only",
            Self::EtagMismatch => "etag_mismatch",
            Self::SizeMismatch => "size_mismatch",
        }
    }
}

/// Who wrote a remote object, by its write identity (§7.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteWriter {
    /// No write identity: a writer outside SkyS3.
    Outside,
    /// An identity of this bucket in this cluster: a write of SkyS3's that
    /// the index does not show, such as one a deposed primary sent late.
    Own,
    /// An identity of another cluster or bucket: another SkyS3 bucket
    /// writes the same prefix.
    Foreign,
    /// A `skys3-wid` value that is no write identity.
    Unknown,
}

impl RemoteWriter {
    /// The writer's name in the admin API and the log.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Outside => "outside",
            Self::Own => "own",
            Self::Foreign => "foreign",
            Self::Unknown => "unknown",
        }
    }
}

/// What the index holds at a key that differs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexVersion {
    /// The position of the record that committed the version.
    pub version: EpochSeq,
    /// The entry's state: clean or evicted.
    pub state: EntryState,
    /// The version's size.
    pub size: u64,
    /// The ETag the remote returned when the version was flushed or
    /// imported.
    pub remote_etag: ETag,
    /// The remote version ID, on a versioned remote.
    pub remote_version_id: Option<String>,
}

/// What the remote holds at a key that differs, as the confirming HEAD
/// found it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteVersion {
    /// The object's ETag.
    pub etag: ETag,
    /// The object's size.
    pub size: u64,
    /// The object's version ID, on a versioned remote.
    pub version_id: Option<String>,
    /// The object's `skys3-wid` metadata, if it has one no longer than a
    /// write identity can be.
    pub identity: Option<String>,
    /// Who wrote it.
    pub writer: RemoteWriter,
}

/// One difference a scan confirmed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Difference {
    /// The key, without the target's prefix.
    pub key: String,
    /// The key's shard.
    pub shard: ShardId,
    /// How the remote differs.
    pub kind: DifferenceKind,
    /// The index's entry; `None` for [`DifferenceKind::RemoteOnly`].
    pub index: Option<IndexVersion>,
    /// The remote's object; `None` for [`DifferenceKind::IndexOnly`].
    pub remote: Option<RemoteVersion>,
}

/// What one finished scan found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileReport {
    /// When it started, in milliseconds since the Unix epoch.
    pub started_ms: u64,
    /// When it finished.
    pub finished_ms: u64,
    /// The shards it compared from start to end: those the node led and
    /// served throughout.
    pub shards: Vec<ShardId>,
    /// Remote objects listed under the prefix.
    pub listed: u64,
    /// Keys of those shards compared.
    pub compared: u64,
    /// The differences it confirmed, at most [`MAX_REPORTED_DIFFERENCES`]:
    /// each listing page's in key order, shard by shard.
    pub differences: Vec<Difference>,
    /// Candidates left unconfirmed because the report was full.
    pub unconfirmed: u64,
}

impl ReconcileReport {
    /// How many confirmed differences are of `kind`.
    #[must_use]
    pub fn count(&self, kind: DifferenceKind) -> u64 {
        self.differences.iter().filter(|d| d.kind == kind).count() as u64
    }
}

/// A bucket's reconciliation scans on this node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileStatus {
    /// `reconcile_interval_seconds`: the wait before each scan.
    pub interval: Duration,
    /// Scans finished since the node started following the bucket.
    pub scans: u64,
    /// When the scan in progress started, if one is.
    pub running_since_ms: Option<u64>,
    /// The last key the scan in progress has compared up to.
    pub after: Option<String>,
    /// The last finished scan's report.
    pub last: Option<Arc<ReconcileReport>>,
    /// The scan's last request error, until a request succeeds.
    pub error: Option<String>,
}

/// What a clean or evicted entry says the remote holds: its ETag and size.
/// Any other entry is not at the remote yet, and is not compared.
fn expected(entry: &Entry) -> Option<(&ETag, u64)> {
    match entry.state {
        EntryState::Clean | EntryState::Evicted => {
            Some((entry.remote_etag.as_ref()?, entry.object.as_ref()?.size))
        }
        _ => None,
    }
}

/// How `remote`, an object's ETag and size or `None` for no object,
/// differs from `entry`, if it does and the key is compared. A key without
/// an entry is compared only once the import has `passed` it.
pub(crate) fn compare(
    entry: Option<&Entry>,
    remote: Option<(&ETag, u64)>,
    passed: bool,
) -> Option<DifferenceKind> {
    let Some(entry) = entry else {
        return (remote.is_some() && passed).then_some(DifferenceKind::RemoteOnly);
    };
    let (etag, size) = expected(entry)?;
    match remote {
        None => Some(DifferenceKind::IndexOnly),
        Some((remote, _)) if remote != etag => Some(DifferenceKind::EtagMismatch),
        Some((_, remote)) if remote != size => Some(DifferenceKind::SizeMismatch),
        Some(_) => None,
    }
}

/// Whether two reads of a key's entry say the same about the remote: the
/// same version, expecting the same remote object. Eviction changes
/// neither.
fn unchanged(before: Option<&Entry>, after: Option<&Entry>) -> bool {
    fn view(entry: Option<&Entry>) -> Option<(EpochSeq, Option<(&ETag, u64)>)> {
        entry.map(|e| (e.version, expected(e)))
    }
    view(before) == view(after)
}

/// Who wrote an object whose `skys3-wid` metadata is `identity`, for this
/// cluster's bucket `bucket`, and the identity if it parses.
pub(crate) fn writer_of(
    identity: Option<&str>,
    cluster: &ClusterId,
    bucket: &BucketId,
) -> (RemoteWriter, Option<WriteIdentity>) {
    let Some(value) = identity else {
        return (RemoteWriter::Outside, None);
    };
    match value.parse::<WriteIdentity>() {
        Ok(id) if &id.cluster == cluster && &id.bucket == bucket => (RemoteWriter::Own, Some(id)),
        Ok(id) => (RemoteWriter::Foreign, Some(id)),
        Err(_) => (RemoteWriter::Unknown, None),
    }
}

/// A key whose listing and entry differ, waiting to be confirmed.
#[derive(Debug, Clone)]
pub(crate) struct Candidate {
    pub(crate) key: String,
    pub(crate) shard: ShardId,
    /// The entry it was compared with.
    pub(crate) entry: Option<Entry>,
    /// The shard's applied position before that entry was read: every
    /// write after it may have raced with the scan.
    pub(crate) since: EpochSeq,
    /// Whether the shard's import had passed the key.
    pub(crate) passed: bool,
}

/// The difference `candidate` is, now that a HEAD found `head` at the
/// remote and the index holds `after`; `None` if the key changed in
/// between, or agrees after all.
pub(crate) fn confirm(
    candidate: &Candidate,
    after: Option<&Entry>,
    head: Option<&ObjectInfo>,
    cluster: &ClusterId,
    bucket: &BucketId,
) -> Option<Difference> {
    if !unchanged(candidate.entry.as_ref(), after) {
        return None;
    }
    let kind = compare(
        after,
        head.map(|info| (&info.etag, info.size)),
        candidate.passed,
    )?;
    let remote = match head {
        Some(info) => {
            let value = info.metadata.write_identity();
            // A value too long for an identity does not parse.
            let (writer, identity) = writer_of(value, cluster, bucket);
            // A write of the key's shard after what the scan read raced
            // with it: an entry that came and went in between.
            if writer == RemoteWriter::Own
                && identity
                    .as_ref()
                    .is_some_and(|id| id.shard == candidate.shard && id.position > candidate.since)
            {
                return None;
            }
            Some(RemoteVersion {
                etag: info.etag.clone(),
                size: info.size,
                version_id: info.version_id.as_ref().map(|id| id.0.clone()),
                identity: value
                    .filter(|value| value.len() <= WriteIdentity::MAX_LEN)
                    .map(str::to_owned),
                writer,
            })
        }
        None => None,
    };
    let index = after.and_then(|entry| {
        let (etag, size) = expected(entry)?;
        Some(IndexVersion {
            version: entry.version,
            state: entry.state,
            size,
            remote_etag: etag.clone(),
            remote_version_id: entry.remote_version_id.clone(),
        })
    });
    Some(Difference {
        key: candidate.key.clone(),
        shard: candidate.shard,
        kind,
        index,
        remote,
    })
}

/// The reads a scan makes of a shard's replica: all it can do with one.
pub(crate) trait ShardReader: Send + Sync {
    /// The shard.
    fn shard_ref(&self) -> &ShardRef;
    /// Whether the replica is the shard's primary and serves.
    fn leads(&self) -> bool;
    /// The position of the last record the replica applied.
    fn applied(&self) -> EpochSeq;
    /// Up to `limit` entries in key order after `after` ([`Shard::entries`]).
    fn entries(
        &self,
        after: Option<String>,
        limit: usize,
    ) -> BoxFuture<'_, Result<Vec<(String, Entry)>, ShardError>>;
    /// The entry of `key` ([`Shard::peek`]).
    fn peek<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Entry>, ShardError>>;
    /// The import progress the replica applied ([`Shard::import_progress`]).
    fn import_progress(&self) -> BoxFuture<'_, Result<Option<ImportRanges>, ShardError>>;
}

impl<D: Disk> ShardReader for Shard<D> {
    fn shard_ref(&self) -> &ShardRef {
        self.shard()
    }

    fn leads(&self) -> bool {
        !self.is_stopped() && !self.role().follows() && self.is_serving()
    }

    fn applied(&self) -> EpochSeq {
        Shard::applied(self)
    }

    fn entries(
        &self,
        after: Option<String>,
        limit: usize,
    ) -> BoxFuture<'_, Result<Vec<(String, Entry)>, ShardError>> {
        Box::pin(Shard::entries(self, after, limit))
    }

    fn peek<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Entry>, ShardError>> {
        Box::pin(Shard::peek(self, key))
    }

    fn import_progress(&self) -> BoxFuture<'_, Result<Option<ImportRanges>, ShardError>> {
        Box::pin(async move {
            let progress = Shard::import_progress(self).await?;
            Ok(progress.map(|(_, ranges)| ranges))
        })
    }
}

/// What a bucket's scan reads as the flush service follows the node's
/// shards: the bucket's shards whose primary is here, and its target,
/// whose window of requests in flight the scan yields to.
pub(crate) struct ScanInputs<S> {
    inner: Mutex<Inputs<S>>,
}

struct Inputs<S> {
    shards: Vec<Arc<dyn ShardReader>>,
    target: Option<Arc<Ready<S>>>,
}

impl<S> Default for ScanInputs<S> {
    fn default() -> Self {
        Self {
            inner: Mutex::new(Inputs {
                shards: Vec::new(),
                target: None,
            }),
        }
    }
}

impl<S> ScanInputs<S> {
    /// Replaces the shards and the target.
    pub(crate) fn set(&self, shards: Vec<Arc<dyn ShardReader>>, target: Arc<Ready<S>>) {
        *lock(&self.inner) = Inputs {
            shards,
            target: Some(target),
        };
    }

    /// The shards the node leads and that serve now.
    fn led(&self) -> BTreeMap<ShardId, Arc<dyn ShardReader>> {
        lock(&self.inner)
            .shards
            .iter()
            .filter(|shard| shard.leads())
            .map(|shard| (shard.shard_ref().shard, Arc::clone(shard)))
            .collect()
    }

    /// Whether the node leads and serves `shard` now.
    fn leads(&self, shard: ShardId) -> bool {
        self.led().contains_key(&shard)
    }

    /// Whether the flushers keep the target's window full: every request
    /// they may have in flight is (§7.7).
    fn window_full(&self) -> bool {
        let target = lock(&self.inner)
            .target
            .as_ref()
            .and_then(|ready| ready.get());
        target.is_some_and(|target| {
            let window = target.concurrency();
            window.in_flight >= window.limit
        })
    }
}

/// Where a bucket's scans are, for [`ReconcileStatus`].
#[derive(Debug, Default)]
pub(crate) struct ScanState {
    inner: Mutex<StateInner>,
}

#[derive(Debug, Default)]
struct StateInner {
    scans: u64,
    running_since_ms: Option<u64>,
    after: Option<String>,
    last: Option<Arc<ReconcileReport>>,
    error: Option<String>,
}

impl ScanState {
    /// The scans' status, which waits `interval` before each.
    pub(crate) fn status(&self, interval: Duration) -> ReconcileStatus {
        let inner = lock(&self.inner);
        ReconcileStatus {
            interval,
            scans: inner.scans,
            running_since_ms: inner.running_since_ms,
            after: inner.after.clone(),
            last: inner.last.clone(),
            error: inner.error.clone(),
        }
    }
}

/// What a bucket's scan task runs on.
pub(crate) struct ScanJob<S> {
    pub(crate) bucket: BucketDocument,
    pub(crate) cluster: ClusterId,
    /// The target's store, read outside the flushers' window.
    pub(crate) store: Arc<S>,
    pub(crate) prefix: String,
    pub(crate) inputs: Arc<ScanInputs<S>>,
    /// The wait before each scan.
    pub(crate) interval: Duration,
    pub(crate) settings: FlushSettings,
    pub(crate) wall: Arc<dyn WallClock>,
    pub(crate) state: Arc<ScanState>,
    pub(crate) metrics: FlushMetrics,
}

/// Scans the bucket after every `interval`, over the shards the node leads
/// then, until the task is aborted. A scan only reads, so it may be
/// aborted anywhere.
pub(crate) async fn run<S: ObjectStore>(job: ScanJob<S>) {
    // One rate for every request of the bucket's scans.
    let throttle = Throttle::new(job.settings.reconcile_requests_per_second);
    loop {
        tokio::time::sleep(job.interval).await;
        let shards = job.inputs.led();
        if shards.is_empty() {
            continue;
        }
        let report = Scan::new(&job, &throttle, shards).run().await;
        job.finish(report);
    }
}

impl<S> ScanJob<S> {
    fn now_ms(&self) -> u64 {
        u64::try_from(self.wall.now().as_millis()).unwrap_or(u64::MAX)
    }

    fn state(&self) -> MutexGuard<'_, StateInner> {
        lock(&self.state.inner)
    }

    /// Publishes `report`: the status, the metrics, and a log line.
    fn finish(&self, report: ReconcileReport) {
        let name = self.bucket.name.as_str();
        let left_out = report
            .differences
            .len()
            .saturating_sub(MAX_LOGGED_DIFFERENCES);
        if left_out > 0 {
            tracing::warn!(
                bucket = name,
                left_out,
                "the reconciliation scan logged its first {MAX_LOGGED_DIFFERENCES} differences \
                only; the admin API reports up to {MAX_REPORTED_DIFFERENCES}"
            );
        }
        tracing::info!(
            bucket = name,
            listed = report.listed,
            compared = report.compared,
            differences = report.differences.len(),
            unconfirmed = report.unconfirmed,
            "the reconciliation scan finished"
        );
        self.metrics.set_reconciliation(name, &report);
        let mut state = self.state();
        state.scans += 1;
        state.running_since_ms = None;
        state.after = None;
        state.last = Some(Arc::new(report));
    }
}

/// One scan in progress.
struct Scan<'a, S> {
    job: &'a ScanJob<S>,
    throttle: &'a Throttle,
    cursors: BTreeMap<ShardId, Cursor>,
    report: ReconcileReport,
    /// Failed requests since the last that succeeded.
    failures: u32,
}

impl<'a, S: ObjectStore> Scan<'a, S> {
    fn new(
        job: &'a ScanJob<S>,
        throttle: &'a Throttle,
        shards: BTreeMap<ShardId, Arc<dyn ShardReader>>,
    ) -> Self {
        let started_ms = job.now_ms();
        {
            let mut state = job.state();
            state.running_since_ms = Some(started_ms);
            state.after = None;
        }
        Self {
            job,
            throttle,
            cursors: shards
                .into_iter()
                .map(|(id, shard)| (id, Cursor::new(shard)))
                .collect(),
            report: ReconcileReport {
                started_ms,
                finished_ms: started_ms,
                shards: Vec::new(),
                listed: 0,
                compared: 0,
                differences: Vec::new(),
                unconfirmed: 0,
            },
            failures: 0,
        }
    }

    /// Lists the prefix page by page, comparing each with the index of the
    /// shards it still leads.
    async fn run(mut self) -> ReconcileReport {
        let job = self.job;
        let mut token = None;
        loop {
            let page = self.list(token.take()).await;
            self.report.listed += page.objects.len() as u64;
            let prefix = job.prefix.as_str();
            // The index is read up to the page's last key, or to its end
            // after the last page.
            let bound = match page.objects.last() {
                _ if !page.is_truncated => Some(None),
                Some(last) => last
                    .key
                    .strip_prefix(prefix)
                    .map(|key| Some(key.to_owned())),
                None => None,
            };
            if let Some(bound) = bound {
                self.compare_page(&page, bound.as_deref()).await;
                job.state().after = bound;
            }
            match page.next_continuation_token.filter(|_| page.is_truncated) {
                Some(next) => token = Some(next),
                None => break,
            }
        }
        self.report.shards = self.cursors.keys().copied().collect();
        self.report.finished_ms = job.now_ms();
        self.report
    }

    /// Compares the objects of `page` with the entries up to `bound`,
    /// shard by shard, at most a page of entries of a shard at a time.
    async fn compare_page(&mut self, page: &ListObjectsV2Output, bound: Option<&str>) {
        let job = self.job;
        let led = job.inputs.led();
        self.cursors.retain(|id, _| led.contains_key(id));
        let bucket = &job.bucket;
        // The page's objects of each shard compared, in key order.
        let mut remote: BTreeMap<ShardId, VecDeque<(&str, Listed<'_>)>> = BTreeMap::new();
        for object in &page.objects {
            let Some(key) = object.key.strip_prefix(job.prefix.as_str()) else {
                continue;
            };
            let shard = shard_for_key(&bucket.bucket_id, key.as_bytes(), bucket.shards);
            if mirrored(key) && self.cursors.contains_key(&shard) {
                let listed = (&object.etag, object.size);
                remote.entry(shard).or_default().push_back((key, listed));
            }
        }
        let shards: Vec<ShardId> = self.cursors.keys().copied().collect();
        for id in shards {
            let mut objects = remote.remove(&id).unwrap_or_default();
            while let Some(cursor) = self.cursors.get_mut(&id) {
                let page_keys = job.settings.reconcile_page_keys;
                let chunk = match cursor.chunk(bound, page_keys).await {
                    Ok(chunk) => chunk,
                    Err(error) => {
                        tracing::warn!(bucket = bucket.name.as_str(), shard = %id, %error,
                            "the reconciliation scan stops reading a shard");
                        self.cursors.remove(&id);
                        break;
                    }
                };
                // The chunk holds every entry up to `upto`.
                let upto = match chunk.entries.last() {
                    Some((last, _)) if !chunk.complete => Some(last.clone()),
                    _ => bound.map(str::to_owned),
                };
                let mut listed = BTreeMap::new();
                while let Some(&(key, object)) = objects.front()
                    && upto.as_deref().is_none_or(|upto| key <= upto)
                {
                    listed.insert(key, object);
                    objects.pop_front();
                }
                let candidates = self.candidates(id, &chunk, &listed);
                for candidate in candidates {
                    self.confirm(candidate).await;
                }
                if chunk.complete {
                    break;
                }
            }
        }
    }

    /// The keys of `chunk` and of `listed`, the remote's objects up to the
    /// same key, that differ, counting each key compared.
    fn candidates(
        &mut self,
        shard: ShardId,
        chunk: &Chunk,
        listed: &BTreeMap<&str, Listed<'_>>,
    ) -> Vec<Candidate> {
        let local: BTreeMap<&str, &Entry> = chunk
            .entries
            .iter()
            .filter(|(key, _)| mirrored(key))
            .map(|(key, entry)| (key.as_str(), entry))
            .collect();
        let keys: BTreeSet<&str> = local.keys().chain(listed.keys()).copied().collect();
        let mut candidates = Vec::new();
        for key in keys {
            self.report.compared += 1;
            let entry = local.get(key).copied();
            let passed = chunk.progress.as_ref().is_some_and(|p| p.passed(key));
            if compare(entry, listed.get(key).copied(), passed).is_some() {
                candidates.push(Candidate {
                    key: key.to_owned(),
                    shard,
                    entry: entry.cloned(),
                    since: chunk.since,
                    passed,
                });
            }
        }
        candidates
    }

    /// Confirms `candidate` with a HEAD and a second read of its entry,
    /// and reports it if it stands.
    async fn confirm(&mut self, candidate: Candidate) {
        if self.report.differences.len() >= MAX_REPORTED_DIFFERENCES {
            self.report.unconfirmed += 1;
            return;
        }
        let head = self.head(&candidate.key).await;
        let Some(cursor) = self.cursors.get(&candidate.shard) else {
            return;
        };
        let Ok(after) = cursor.shard.peek(&candidate.key).await else {
            return;
        };
        // Only a primary that serves has applied every `FLUSHED`.
        if !self.job.inputs.leads(candidate.shard) {
            return;
        }
        let (cluster, bucket) = (&self.job.cluster, &self.job.bucket);
        let Some(difference) = confirm(
            &candidate,
            after.as_ref(),
            head.as_ref(),
            cluster,
            &bucket.bucket_id,
        ) else {
            return;
        };
        if self.report.differences.len() < MAX_LOGGED_DIFFERENCES {
            let index = difference.index.as_ref();
            let remote = difference.remote.as_ref();
            tracing::warn!(
                bucket = bucket.name.as_str(),
                key = difference.key,
                shard = %difference.shard,
                kind = difference.kind.as_str(),
                index_version = index.map(|i| i.version.to_string()),
                index_etag = index.map(|i| i.remote_etag.to_string()),
                remote_etag = remote.map(|r| r.etag.to_string()),
                writer = remote.map(|r| r.writer.as_str()),
                identity = remote.and_then(|r| r.identity.clone()),
                "the reconciliation scan found the remote differing from the index"
            );
        }
        self.report.differences.push(difference);
    }

    /// One listing page, after `token` if given, retried until it succeeds.
    async fn list(&mut self, token: Option<String>) -> ListObjectsV2Output {
        let page_keys = self
            .job
            .settings
            .reconcile_page_keys
            .clamp(1, IMPORT_PAGE_KEYS);
        loop {
            let mut request = ListObjectsV2::new(self.job.prefix.clone()).with_max_keys(page_keys);
            if let Some(token) = &token {
                request = request.with_continuation_token(token.clone());
            }
            self.pace().await;
            match self.job.store.list_objects_v2(request).await {
                Ok(page) => {
                    self.succeeded();
                    return page;
                }
                Err(error) => {
                    self.failed(format!("listing the remote failed: {error}"))
                        .await
                }
            }
        }
    }

    /// The remote's object at `key`, `None` if it has none, retried until
    /// the remote answers.
    async fn head(&mut self, key: &str) -> Option<ObjectInfo> {
        loop {
            self.pace().await;
            let request = HeadObject::new(format!("{}{key}", self.job.prefix));
            match self.job.store.head_object(request).await {
                Ok(info) => {
                    self.succeeded();
                    return Some(info);
                }
                Err(error) if error.kind() == S3ErrorKind::NoSuchKey => {
                    self.succeeded();
                    return None;
                }
                Err(error) => {
                    self.failed(format!("a HEAD of {key:?} failed: {error}"))
                        .await
                }
            }
        }
    }

    /// Waits for the rate limit, and while the flushers keep the target's
    /// window full: the scan yields to flushing.
    async fn pace(&self) {
        self.throttle.admit(1).await;
        while self.job.inputs.window_full() {
            tokio::time::sleep(self.job.settings.min_backoff).await;
        }
    }

    fn succeeded(&mut self) {
        if self.failures > 0 {
            self.failures = 0;
            self.job.state().error = None;
        }
    }

    /// Records `error` and backs off before the request is sent again.
    async fn failed(&mut self, error: String) {
        self.failures += 1;
        tracing::warn!(
            bucket = self.job.bucket.name.as_str(),
            %error,
            "the reconciliation scan will retry"
        );
        self.job.state().error = Some(error);
        tokio::time::sleep(self.job.settings.backoff(self.failures)).await;
    }
}

/// A remote object's ETag and size, as a listing gives them.
type Listed<'a> = (&'a ETag, u64);

/// A shard's entries up to some key, after those read before.
struct Chunk {
    entries: Vec<(String, Entry)>,
    /// Whether they reach the bound they were read up to; otherwise they
    /// are a page, and more follow.
    complete: bool,
    /// The shard's applied position before the earliest read they come
    /// from: every write after it may have raced with the scan.
    since: EpochSeq,
    /// The shard's import progress, read before them.
    progress: Option<ImportRanges>,
}

/// A shard's entries, read in key order a page at a time.
struct Cursor {
    shard: Arc<dyn ShardReader>,
    /// The last key read, if any.
    read: Option<String>,
    /// Entries read past the bound of the page that read them.
    ahead: VecDeque<(String, Entry)>,
    /// Whether the last read reached the shard's last entry.
    done: bool,
    /// The shard's applied position before the read that `ahead` comes
    /// from, or the last read.
    since: EpochSeq,
}

impl Cursor {
    fn new(shard: Arc<dyn ShardReader>) -> Self {
        let since = shard.applied();
        Self {
            shard,
            read: None,
            ahead: VecDeque::new(),
            done: false,
            since,
        }
    }

    /// The entries after those returned before, up to `bound`, or to the
    /// end without one, at most `page` of them.
    async fn chunk(&mut self, bound: Option<&str>, page: u32) -> Result<Chunk, ShardError> {
        let progress = self.shard.import_progress().await?;
        let page = usize::try_from(page.max(1)).unwrap_or(usize::MAX);
        let mut chunk = Chunk {
            entries: Vec::new(),
            complete: true,
            since: self.since,
            progress,
        };
        loop {
            while let Some((key, _)) = self.ahead.front() {
                if bound.is_some_and(|bound| key.as_str() > bound) {
                    return Ok(chunk);
                }
                if chunk.entries.len() == page {
                    chunk.complete = false;
                    return Ok(chunk);
                }
                chunk.entries.extend(self.ahead.pop_front());
            }
            if self.done {
                return Ok(chunk);
            }
            if chunk.entries.len() == page {
                chunk.complete = false;
                return Ok(chunk);
            }
            self.since = self.shard.applied();
            let read = self.shard.entries(self.read.clone(), page).await?;
            self.done = read.len() < page;
            if let Some((key, _)) = read.last() {
                self.read = Some(key.clone());
            }
            self.ahead.extend(read);
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // Every update is a single step.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use skys3_index::{ObjectVersion, Payload};
    use skys3_log::record::Metadata;
    use skys3_remote::UserMetadata;
    use skys3_types::{Epoch, Seq};

    use super::*;

    fn etag(value: &str) -> ETag {
        ETag::new(value).unwrap()
    }

    fn position(seq: u64) -> EpochSeq {
        EpochSeq::new(Epoch::new(1), Seq::new(seq))
    }

    fn entry(state: EntryState, seq: u64, remote: Option<&str>, size: u64) -> Entry {
        Entry {
            version: position(seq),
            state,
            object: Some(ObjectVersion {
                size,
                last_modified_ms: 1,
                local_etag: etag("local"),
                write_identity: None,
                metadata: Metadata::new(),
                tags: Default::default(),
                checksums: Default::default(),
                storage_class: None,
                copy_source: None,
                payload: Payload::None,
                coded: None,
            }),
            remote_etag: remote.map(etag),
            remote_version_id: None,
        }
    }

    fn clean(seq: u64, remote: &str, size: u64) -> Entry {
        entry(EntryState::Clean, seq, Some(remote), size)
    }

    fn cluster() -> ClusterId {
        ClusterId::new("c-a").unwrap()
    }

    fn bucket() -> BucketId {
        BucketId::new("b-a").unwrap()
    }

    fn head(etag_value: &str, size: u64, identity: Option<&str>) -> ObjectInfo {
        let mut metadata = UserMetadata::new();
        if let Some(identity) = identity {
            metadata
                .insert(WriteIdentity::METADATA_KEY, identity)
                .unwrap();
        }
        ObjectInfo {
            etag: etag(etag_value),
            size,
            version_id: None,
            metadata,
            content_type: None,
            last_modified_ms: None,
            tag_count: 0,
        }
    }

    fn candidate(entry: Option<Entry>) -> Candidate {
        Candidate {
            key: "k".to_owned(),
            shard: ShardId::new(2),
            entry,
            since: position(10),
            passed: true,
        }
    }

    #[test]
    fn each_kind_is_found() {
        let e = clean(1, "aaa", 4);
        let same = Some((&etag("aaa"), 4));
        assert_eq!(compare(Some(&e), same, true), None);
        assert_eq!(compare(None, same, true), Some(DifferenceKind::RemoteOnly));
        assert_eq!(
            compare(Some(&e), None, true),
            Some(DifferenceKind::IndexOnly)
        );
        assert_eq!(
            compare(Some(&e), Some((&etag("bbb"), 4)), true),
            Some(DifferenceKind::EtagMismatch)
        );
        assert_eq!(
            compare(Some(&e), Some((&etag("aaa"), 5)), true),
            Some(DifferenceKind::SizeMismatch)
        );
        // An evicted stub says the same as a clean entry.
        let evicted = entry(EntryState::Evicted, 1, Some("aaa"), 4);
        assert_eq!(
            compare(Some(&evicted), None, true),
            Some(DifferenceKind::IndexOnly)
        );
        assert_eq!(compare(None, None, true), None);
        let names: Vec<_> = DifferenceKind::ALL.iter().map(|k| k.as_str()).collect();
        assert_eq!(
            names,
            [
                "remote_only",
                "index_only",
                "etag_mismatch",
                "size_mismatch"
            ]
        );
    }

    #[test]
    fn keys_not_yet_at_the_remote_or_not_yet_imported_are_not_compared() {
        for state in [
            EntryState::Dirty,
            EntryState::Flushing,
            EntryState::Conflict,
        ] {
            let e = entry(state, 1, Some("aaa"), 4);
            assert_eq!(compare(Some(&e), None, true), None, "{state:?}");
            assert_eq!(compare(Some(&e), Some((&etag("bbb"), 4)), true), None);
        }
        // A tombstone, and a clean entry without a remote ETag.
        let tombstone = Entry {
            object: None,
            ..entry(EntryState::Dirty, 1, Some("aaa"), 0)
        };
        assert_eq!(
            compare(Some(&tombstone), Some((&etag("aaa"), 4)), true),
            None
        );
        let unknown = entry(EntryState::Clean, 1, None, 4);
        assert_eq!(compare(Some(&unknown), None, true), None);
        // The import creates the stub of a key it has not passed.
        assert_eq!(compare(None, Some((&etag("aaa"), 4)), false), None);
    }

    #[test]
    fn writers_are_told_apart_by_their_identity() {
        let (cluster, bucket) = (cluster(), bucket());
        let writer = |value: Option<&str>| writer_of(value, &cluster, &bucket).0;
        assert_eq!(writer(None), RemoteWriter::Outside);
        assert_eq!(writer(Some("c-a/b-a/2/1.5")), RemoteWriter::Own);
        assert_eq!(writer(Some("c-b/b-a/2/1.5")), RemoteWriter::Foreign);
        assert_eq!(writer(Some("c-a/b-b/2/1.5")), RemoteWriter::Foreign);
        assert_eq!(writer(Some("not an identity")), RemoteWriter::Unknown);
        assert_eq!(writer(Some(&"x".repeat(200))), RemoteWriter::Unknown);
        let names = [
            RemoteWriter::Outside,
            RemoteWriter::Own,
            RemoteWriter::Foreign,
            RemoteWriter::Unknown,
        ]
        .map(RemoteWriter::as_str);
        assert_eq!(names, ["outside", "own", "foreign", "unknown"]);
    }

    #[test]
    fn a_confirmed_difference_carries_both_sides() {
        let (cluster, bucket) = (cluster(), bucket());
        let before = clean(3, "aaa", 4);
        let found = head("bbb", 7, Some("c-z/b-a/2/1.1"));
        let difference = confirm(
            &candidate(Some(before.clone())),
            Some(&before),
            Some(&found),
            &cluster,
            &bucket,
        )
        .unwrap();
        assert_eq!(difference.kind, DifferenceKind::EtagMismatch);
        let index = difference.index.unwrap();
        assert_eq!((index.version, index.size), (position(3), 4));
        assert_eq!(index.remote_etag, etag("aaa"));
        let remote = difference.remote.unwrap();
        assert_eq!((remote.etag, remote.size), (etag("bbb"), 7));
        assert_eq!(remote.writer, RemoteWriter::Foreign);
        assert_eq!(remote.identity.as_deref(), Some("c-z/b-a/2/1.1"));

        // The HEAD decides the kind: an object gone since the listing.
        let gone = confirm(
            &candidate(Some(before.clone())),
            Some(&before),
            None,
            &cluster,
            &bucket,
        )
        .unwrap();
        assert_eq!(gone.kind, DifferenceKind::IndexOnly);
        assert!(gone.remote.is_none());

        // An overlong identity is reported as unknown, without its value.
        let long = "y".repeat(300);
        let odd = confirm(
            &candidate(None),
            None,
            Some(&head("ccc", 1, Some(&long))),
            &cluster,
            &bucket,
        )
        .unwrap();
        assert_eq!(odd.kind, DifferenceKind::RemoteOnly);
        let remote = odd.remote.unwrap();
        assert_eq!(
            (remote.writer, remote.identity),
            (RemoteWriter::Unknown, None)
        );
    }

    #[test]
    fn keys_that_changed_or_agree_by_the_confirmation_are_not_reported() {
        let (cluster, bucket) = (cluster(), bucket());
        let before = clean(3, "aaa", 4);
        let found = head("bbb", 4, None);
        // A new version committed in between, or the key became dirty.
        let newer = clean(4, "bbb", 4);
        let dirty = entry(EntryState::Dirty, 4, Some("aaa"), 4);
        for after in [Some(&newer), Some(&dirty), None] {
            let c = candidate(Some(before.clone()));
            assert_eq!(confirm(&c, after, Some(&found), &cluster, &bucket), None);
        }
        // Eviction alone changes nothing the scan compares.
        let evicted = entry(EntryState::Evicted, 3, Some("aaa"), 4);
        let c = candidate(Some(before.clone()));
        assert!(confirm(&c, Some(&evicted), Some(&found), &cluster, &bucket).is_some());
        // The listing was older than the flush the HEAD sees.
        let flushed = clean(3, "bbb", 4);
        let c = candidate(Some(flushed.clone()));
        assert_eq!(
            confirm(&c, Some(&flushed), Some(&found), &cluster, &bucket),
            None
        );
        // A key created after the index was read shows at the next scan.
        let c = candidate(None);
        assert_eq!(
            confirm(&c, Some(&newer), Some(&found), &cluster, &bucket),
            None
        );
    }

    #[test]
    fn a_write_of_the_shard_after_the_read_raced_with_the_scan() {
        let (cluster, bucket) = (cluster(), bucket());
        let c = candidate(None);
        // Committed after the shard's applied position when the scan read
        // it (1.10): an entry that came and went in between.
        let raced = head("aaa", 1, Some("c-a/b-a/2/1.11"));
        assert_eq!(confirm(&c, None, Some(&raced), &cluster, &bucket), None);
        // An older write of the shard that the index does not show is
        // reported, and so is a later one of another shard.
        for identity in ["c-a/b-a/2/1.10", "c-a/b-a/3/1.11"] {
            let found = head("aaa", 1, Some(identity));
            let difference = confirm(&c, None, Some(&found), &cluster, &bucket).unwrap();
            assert_eq!(difference.remote.unwrap().writer, RemoteWriter::Own);
        }
    }

    #[test]
    fn reports_count_their_differences_by_kind() {
        let difference = |kind| Difference {
            key: "k".to_owned(),
            shard: ShardId::new(0),
            kind,
            index: None,
            remote: None,
        };
        let report = ReconcileReport {
            started_ms: 0,
            finished_ms: 0,
            shards: Vec::new(),
            listed: 0,
            compared: 0,
            differences: vec![
                difference(DifferenceKind::RemoteOnly),
                difference(DifferenceKind::RemoteOnly),
                difference(DifferenceKind::SizeMismatch),
            ],
            unconfirmed: 0,
        };
        assert_eq!(report.count(DifferenceKind::RemoteOnly), 2);
        assert_eq!(report.count(DifferenceKind::IndexOnly), 0);
        assert_eq!(report.count(DifferenceKind::SizeMismatch), 1);
    }
}
