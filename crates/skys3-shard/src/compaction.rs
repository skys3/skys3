//! Segment compaction (§10.3): reclaims the log segments of a node whose
//! live ratio fell below `compaction_live_threshold`.
//!
//! Segment locations are node-local, so each node compacts its own disks
//! with no coordination. A segment is a candidate once the index has
//! released it ([`SegmentLog::release`]): every record in it is at or
//! before its shard's durable applied position, so replay needs none of
//! them. The last segment of each class still takes records and is never
//! one.
//!
//! **What stays.** The index and the shards' logs need only some records
//! of a released segment:
//!
//! - payload that an entry or a part of an open upload names (the
//!   location map locates it in this segment, and
//!   [`IndexReader::holders`] finds what names it): copied, except clean
//!   payload, which is evicted instead unless the clean cache ranks it
//!   among its most recently used and is within its bounds
//!   ([`CleanCache`]). Without a cache, clean payload is copied too.
//! - each shard's latest `CONFIG` record (the configuration the index
//!   keeps), so the log still holds the replica's membership (§6.2):
//!   copied.
//! - `EXTENT` records that nothing names, such as those of a body still
//!   arriving, a failed upload, or a peer's staging (§7.8), until their
//!   segment has taken no records for `peer_staging_ttl_seconds`: the
//!   segment waits until then, and they are dropped.
//! - on a replicated shard, every record after the `seq` through which
//!   every member holds the shard's log ([`Shard::replicated_through`]),
//!   which a member that is behind may be sent: the segment waits. So do
//!   segments with records of a shard not open on the node.
//!
//! - the replicated payload of a coded version (§8.4), until the replica
//!   knows that the version's `EC_PUBLISH` record committed: its commit
//!   watermark covers the record ([`Shard::replicated_through`]), or the
//!   replica is alone. From then on nothing names it, and it is dropped
//!   after the release delay like other unreferenced payload. This is how
//!   a replica drops its copy of an object once the object is coded.
//! - payload a live read registration pins ([`Reads::is_pinned`]), which
//!   a gateway is streaming from this node (§8.7): copied, never evicted.
//! - payload nothing names, until compaction has found it so for the
//!   release delay (`fragment_release_delay_seconds`, [`ReadSettings`]): a
//!   read plan issued just before a write or an eviction left it
//!   unreferenced may still register it here. The segment waits.
//!
//! Everything else, metadata records the index has applied and payload
//! nothing names, is dropped.
//!
//! **Order.** A segment is assessed first: its live bytes, those copied
//! or evicted, against its length. A segment below the threshold is then
//! reclaimed in chunks. For each chunk, clean payload to drop is evicted
//! ([`Shard::evict`]), the records to keep are copied byte for byte into
//! the log's newest segments and made durable, and one durable index
//! commit points their locations at the copies and removes the locations
//! of the records dropped, after checking again that nothing names them
//! and no registration pins them, in one [`Reads::exclusive`] decision.
//! Only once every chunk is through is the segment retired: unlisted, its
//! file removed, and the directory synced ([`SegmentLog::retire`]). A
//! crash at any point leaves either the old segment with every location
//! the index needs in it, or the copies with durable locations: a copy
//! the index does not locate yet is just an unreferenced record, which a
//! later compaction drops. Copies keep their shard's positions, which are
//! at or before its applied position, so replay never applies them.
//!
//! **Write amplification** is the log's bytes written over the bytes
//! written for anything but compaction's copies, since the node started
//! ([`CompactionMetrics`]).

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Unit;
use skys3_index::{EntryState, Holder, Holders, IndexError, IndexReader, IndexWriter};
use skys3_io::{Disk, PoolClosed};
use skys3_log::{
    LogError, RecordBody, RecordKind, RecordLocation, ScanError, ScannedRecord, SegmentClass,
    SegmentInfo, SegmentLog, SegmentScanner, ShardRef,
};
use skys3_obs::MetricsRegistry;
use skys3_types::{EpochSeq, Label, Seq, ShardConfig};
use tokio::time::{Instant, MissedTickBehavior};

use crate::cache::{CleanCache, Hot};
#[cfg(doc)]
use crate::reads::ReadSettings;
use crate::reads::Reads;
use crate::seeded::{SeededBug, is_seeded};
use crate::set::ShardSet;
use crate::shard::Shard;

/// The most bytes of a segment compaction reads, and holds, at a time.
const CHUNK_BYTES: u64 = 32 << 20;

/// The most records compaction handles in one index commit.
const CHUNK_RECORDS: usize = 4096;

/// What compaction needs from the configuration (§14).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CompactionSettings {
    /// `compaction_live_threshold`: a released segment whose live bytes
    /// are a smaller share of its length is reclaimed.
    pub live_threshold: f64,
    /// `peer_staging_ttl_seconds`: how long a segment must have taken no
    /// records before the `EXTENT` records in it that nothing names are
    /// dropped (§7.8, §10.3).
    pub unreferenced_ttl: Duration,
}

impl Default for CompactionSettings {
    /// The configuration's defaults: 0.5 and one day.
    fn default() -> Self {
        Self {
            live_threshold: 0.5,
            unreferenced_ttl: Duration::from_secs(86_400),
        }
    }
}

/// The compaction metrics (`docs/skys3-metrics.md`):
/// `skys3_compaction_segments_total`,
/// `skys3_compaction_reclaimed_bytes_total`,
/// `skys3_compaction_copied_bytes_total`,
/// `skys3_compaction_evictions_total`, and
/// `skys3_compaction_write_amplification`. The default metrics belong to
/// no registry.
#[derive(Debug, Clone, Default)]
pub struct CompactionMetrics {
    segments: Counter,
    reclaimed: Counter,
    copied: Counter,
    evictions: Counter,
    amplification: Gauge<f64, AtomicU64>,
}

impl CompactionMetrics {
    /// Registers the compaction metrics in `registry`.
    #[must_use]
    pub fn register(registry: &MetricsRegistry) -> Self {
        let metrics = Self::default();
        registry.register(
            "compaction_segments",
            "Log segments compaction reclaimed.",
            metrics.segments.clone(),
        );
        registry.register_with_unit(
            "compaction_reclaimed",
            "Bytes of the log segments compaction reclaimed.",
            Unit::Bytes,
            metrics.reclaimed.clone(),
        );
        registry.register_with_unit(
            "compaction_copied",
            "Bytes of records compaction copied out of the segments it reclaimed.",
            Unit::Bytes,
            metrics.copied.clone(),
        );
        registry.register(
            "compaction_evictions",
            "Clean payloads compaction evicted instead of copying them.",
            metrics.evictions.clone(),
        );
        registry.register(
            "compaction_write_amplification",
            "Bytes this node's logs wrote since it started, over those written for anything but \
             compaction's copies: 1 when compaction copied nothing.",
            metrics.amplification.clone(),
        );
        metrics.amplification.set(1.0);
        metrics
    }
}

/// What a compaction pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompactionReport {
    /// Released segments whose live bytes the pass measured.
    pub assessed: u64,
    /// Segments it reclaimed.
    pub segments: u64,
    /// The bytes of those segments.
    pub reclaimed_bytes: u64,
    /// Bytes of records it copied.
    pub copied_bytes: u64,
    /// Clean payloads it evicted instead of copying them.
    pub evictions: u64,
    /// Segments below the threshold it started to reclaim but kept,
    /// because a record became needed meanwhile; a later pass tries again.
    pub kept: u64,
}

impl CompactionReport {
    fn add(&mut self, other: &Self) {
        self.assessed += other.assessed;
        self.segments += other.segments;
        self.reclaimed_bytes += other.reclaimed_bytes;
        self.copied_bytes += other.copied_bytes;
        self.evictions += other.evictions;
        self.kept += other.kept;
    }
}

/// Why compaction stopped. Nothing it did is lost: a later pass starts
/// over from what the log and the index hold.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CompactionError {
    /// The log of `disk` failed, or a record compaction read is damaged.
    #[error("compacting a segment of disk {disk}: {source}")]
    Log {
        /// The disk.
        disk: Label,
        /// The error.
        #[source]
        source: LogError,
    },
    /// A segment could not be scanned to its end.
    #[error("compacting a segment of disk {disk}: {source}")]
    Scan {
        /// The disk.
        disk: Label,
        /// The error.
        #[source]
        source: ScanError,
    },
    /// The index failed.
    #[error("compaction's index work failed: {0}")]
    Index(#[from] IndexError),
    /// The index pool is gone.
    #[error(transparent)]
    Pool(#[from] PoolClosed),
}

/// Reclaims the log segments of a node's disks (see the [module](self)
/// docs). Each pass compacts every disk of the [`ShardSet`] in turn, its
/// oldest candidates first.
///
/// A node runs one compactor, and its passes do not overlap: a pass lists
/// a disk's segments and scans them one by one, however long that takes,
/// so nothing else may retire them meanwhile.
pub struct Compactor<D: Disk> {
    set: ShardSet<D>,
    settings: CompactionSettings,
    metrics: CompactionMetrics,
    /// Bytes copied since the compactor was made.
    copied: AtomicU64,
    /// When compaction first found each payload record of a candidate
    /// segment unreferenced, in the passes since.
    unreferenced: Arc<Mutex<Unreferenced>>,
}

/// When compaction first found payload records unreferenced, by shard and
/// position, with the pass that last found each so. A record a pass does
/// not find unreferenced again is forgotten, and waits the whole delay if
/// it is found so later.
#[derive(Debug, Default)]
struct Unreferenced {
    pass: u64,
    since: HashMap<(ShardRef, EpochSeq), (Instant, u64)>,
}

impl Unreferenced {
    /// Whether the payload at `position` of `shard`, unreferenced now, has
    /// been found so for at least `delay`.
    fn for_at_least(&mut self, shard: &ShardRef, position: EpochSeq, delay: Duration) -> bool {
        let pass = self.pass;
        let now = Instant::now();
        let seen = self
            .since
            .entry((shard.clone(), position))
            .or_insert((now, pass));
        seen.1 = pass;
        now.duration_since(seen.0) >= delay
    }

    /// Starts a pass, forgetting the records the last one did not see.
    fn next_pass(&mut self) {
        let last = self.pass;
        self.since.retain(|_, (_, pass)| *pass == last);
        self.pass += 1;
    }
}

impl<D: Disk> fmt::Debug for Compactor<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Compactor")
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

/// One record of a segment, as compaction needs it.
#[derive(Debug, Clone)]
struct Scanned {
    shard: ShardRef,
    position: EpochSeq,
    location: RecordLocation,
    kind: RecordKind,
    /// The key of a record that may hold payload.
    key: Option<String>,
    /// The configuration of a `CONFIG` record.
    config: Option<ShardConfig>,
    /// The encoded record.
    bytes: Bytes,
}

/// What compaction does with a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fate {
    /// Nothing needs it.
    Drop,
    /// It is copied.
    Copy,
    /// It holds the payload of the clean version `version` of its key,
    /// which is evicted.
    Evict { version: EpochSeq },
    /// It is an `EXTENT` that nothing names, in a segment younger than
    /// the TTL: the segment waits.
    Wait,
}

impl Fate {
    fn is_live(self) -> bool {
        matches!(self, Self::Copy | Self::Evict { .. })
    }
}

/// What classifying a segment's records needs to know besides the index.
#[derive(Clone)]
struct Context {
    /// The node's clean cache, and which of the disk's entries it ranks as
    /// recently used.
    cache: Option<(CleanCache, Hot)>,
    /// Whether the segment has taken no records for the TTL.
    expired: bool,
    /// The node's read registrations.
    reads: Reads,
    /// How long payload must have been unreferenced before it is dropped.
    release_delay: Duration,
    /// When payload was first found unreferenced.
    unreferenced: Arc<Mutex<Unreferenced>>,
}

/// What a chunk's index commit did.
#[derive(Debug, Clone, Copy, Default)]
struct Relocated {
    /// Whether a record compaction did not copy is still needed.
    kept: bool,
    /// Bytes of payload records whose locations went.
    dropped: u64,
}

/// The `seq` through which each shard's records are known to have
/// committed on this replica.
type Committed = HashMap<ShardRef, Seq>;

/// The replicas whose records a segment holds, found as they are needed.
struct Replicas<D: Disk> {
    found: HashMap<ShardRef, Option<Shard<D>>>,
}

impl<D: Disk> Replicas<D> {
    fn new() -> Self {
        Self {
            found: HashMap::new(),
        }
    }

    /// The open replica of `shard`, if any.
    async fn get(&mut self, set: &ShardSet<D>, shard: &ShardRef) -> Option<&Shard<D>> {
        if !self.found.contains_key(shard) {
            let replica = set.get(shard).await;
            self.found.insert(shard.clone(), replica);
        }
        self.found.get(shard).and_then(Option::as_ref)
    }

    /// The `seq` through which each shard of `records` is known to have
    /// committed on this replica: its commit watermark, or every record
    /// for a replica alone. A shard not open here has none.
    async fn committed(&mut self, set: &ShardSet<D>, records: &[Scanned]) -> Committed {
        let mut committed = Committed::new();
        for record in records {
            if committed.contains_key(&record.shard) {
                continue;
            }
            if let Some(replica) = self.get(set, &record.shard).await {
                let through = replica.replicated_through().unwrap_or(Seq::MAX);
                committed.insert(record.shard.clone(), through);
            }
        }
        committed
    }

    /// Whether every record of `records` may leave the log as far as
    /// replication is concerned: its shard is open here, and every member
    /// holds it (see the [module](self) docs).
    async fn release(&mut self, set: &ShardSet<D>, records: &[Scanned]) -> bool {
        for record in records {
            let Some(replica) = self.get(set, &record.shard).await else {
                return false;
            };
            if replica
                .replicated_through()
                .is_some_and(|through| record.position.seq > through)
            {
                return false;
            }
        }
        true
    }
}

impl<D: Disk> Compactor<D> {
    /// A compactor of the logs of `set`, which exports `metrics`.
    #[must_use]
    pub fn new(set: ShardSet<D>, settings: CompactionSettings, metrics: CompactionMetrics) -> Self {
        Self {
            set,
            settings,
            metrics,
            copied: AtomicU64::new(0),
            unreferenced: Arc::default(),
        }
    }

    /// Runs a pass ([`Compactor::compact`]) every `interval`, until the
    /// future is dropped. A pass that fails is logged; the next starts
    /// over.
    pub async fn run(&self, interval: Duration) {
        let mut ticks = tokio::time::interval(interval);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // The first tick completes at once.
        ticks.tick().await;
        loop {
            ticks.tick().await;
            match self.compact().await {
                Ok(report) if report.segments > 0 || report.kept > 0 => {
                    tracing::debug!(?report, "compacted the log");
                }
                Ok(_) => {}
                Err(error) => tracing::warn!(%error, "compaction failed"),
            }
        }
    }

    /// Compacts each disk's log once: reclaims every candidate segment
    /// below the threshold, and returns what it did. A disk out of service
    /// is skipped.
    ///
    /// # Errors
    ///
    /// A [`CompactionError`] if a disk's log, a record, or the index
    /// fails; the pass stops there.
    pub async fn compact(&self) -> Result<CompactionReport, CompactionError> {
        lock(&self.unreferenced).next_pass();
        let mut report = CompactionReport::default();
        let logs: Vec<(Label, SegmentLog<D>)> = self
            .set
            .logs()
            .map(|(label, log)| (label.clone(), log.clone()))
            .collect();
        let result = async {
            for (disk, log) in &logs {
                log.drop_retired();
                if log.is_in_service() {
                    report.add(&self.compact_disk(disk, log).await?);
                }
            }
            Ok(())
        }
        .await;
        self.publish(&logs);
        result.map(|()| report)
    }

    /// Compacts the candidate segments of one disk's log.
    async fn compact_disk(
        &self,
        disk: &Label,
        log: &SegmentLog<D>,
    ) -> Result<CompactionReport, CompactionError> {
        let mut report = CompactionReport::default();
        let segments = log.segments();
        let released = log.released();
        let last = |class: SegmentClass| {
            segments
                .iter()
                .filter(|segment| segment.class == class)
                .map(|segment| segment.id)
                .max()
        };
        let tails = [last(SegmentClass::Hot), last(SegmentClass::Bulk)];
        for segment in segments
            .iter()
            .filter(|segment| released.contains(&segment.id) && !tails.contains(&Some(segment.id)))
        {
            let context = self.context(disk, log, segment);
            report.assessed += 1;
            let Some(live) = self.assess(disk, log, segment, &context).await? else {
                continue;
            };
            // Ratios of byte counts; a float is exact enough to compare.
            if segment.len > 0 && live as f64 >= self.settings.live_threshold * segment.len as f64 {
                continue;
            }
            report.add(&self.reclaim(disk, log, segment, &context).await?);
        }
        Ok(report)
    }

    /// What classifying the records of `segment` needs to know.
    fn context(&self, disk: &Label, log: &SegmentLog<D>, segment: &SegmentInfo) -> Context {
        Context {
            cache: self
                .set
                .cache()
                .map(|cache| (cache.clone(), cache.hot(disk))),
            expired: log
                .sealed_for(segment.id)
                .is_some_and(|sealed| sealed >= self.settings.unreferenced_ttl),
            reads: self.set.reads().clone(),
            release_delay: self.set.reads().settings().release_delay,
            unreferenced: Arc::clone(&self.unreferenced),
        }
    }

    /// Returns the live bytes of `segment`, or `None` if it must wait.
    async fn assess(
        &self,
        disk: &Label,
        log: &SegmentLog<D>,
        segment: &SegmentInfo,
        context: &Context,
    ) -> Result<Option<u64>, CompactionError> {
        let mut scanner = log
            .scan(segment.id)
            .map_err(|source| CompactionError::Log {
                disk: disk.clone(),
                source,
            })?;
        let mut replicas = Replicas::new();
        let mut live = 0;
        loop {
            let chunk = next_chunk(disk, &mut scanner).await?;
            if chunk.is_empty() {
                return Ok(Some(live));
            }
            if !replicas.release(&self.set, &chunk).await {
                return Ok(None);
            }
            let committed = replicas.committed(&self.set, &chunk).await;
            let (chunk, fates) = self.classify(chunk, committed, context).await?;
            if fates.contains(&Fate::Wait) {
                return Ok(None);
            }
            live += chunk
                .iter()
                .zip(&fates)
                .filter(|(_, fate)| fate.is_live())
                .map(|(record, _)| u64::from(record.location.len))
                .sum::<u64>();
        }
    }

    /// Reclaims `segment`, a chunk at a time, and retires it unless a
    /// record it did not copy became needed meanwhile.
    async fn reclaim(
        &self,
        disk: &Label,
        log: &SegmentLog<D>,
        segment: &SegmentInfo,
        context: &Context,
    ) -> Result<CompactionReport, CompactionError> {
        let mut report = CompactionReport::default();
        let result = self
            .reclaim_into(&mut report, disk, log, segment, context)
            .await;
        self.count(&report);
        result.map(|()| report)
    }

    /// The work of [`Compactor::reclaim`], which records what it did in
    /// `report` as it goes, so that a failure still counts the copies made.
    async fn reclaim_into(
        &self,
        report: &mut CompactionReport,
        disk: &Label,
        log: &SegmentLog<D>,
        segment: &SegmentInfo,
        context: &Context,
    ) -> Result<(), CompactionError> {
        let log_error = |source| CompactionError::Log {
            disk: disk.clone(),
            source,
        };
        let mut scanner = log.scan(segment.id).map_err(log_error)?;
        let mut replicas = Replicas::new();
        let mut dropped = 0;
        loop {
            let chunk = next_chunk(disk, &mut scanner).await?;
            if chunk.is_empty() {
                break;
            }
            if !replicas.release(&self.set, &chunk).await {
                report.kept += 1;
                return Ok(());
            }
            let committed = replicas.committed(&self.set, &chunk).await;
            let (chunk, fates) = self.classify(chunk, committed.clone(), context).await?;
            if fates.contains(&Fate::Wait) {
                report.kept += 1;
                return Ok(());
            }
            report.evictions += self.evict(&mut replicas, &chunk, &fates).await;
            // Every copy is queued before any is awaited, so a chunk's
            // copies share group commits.
            let mut queued = Vec::new();
            for (at, (record, fate)) in chunk.iter().zip(&fates).enumerate() {
                if *fate == Fate::Copy {
                    let pending = log.queue_encoded(record.bytes.clone(), false).await;
                    queued.push((at, pending.map_err(log_error)?));
                }
            }
            let mut copies = vec![None; chunk.len()];
            for (at, pending) in queued {
                let copy = pending.durable().await.map_err(log_error)?;
                report.copied_bytes += u64::from(copy.len);
                copies[at] = Some(copy);
            }
            let relocated = {
                let index = Arc::clone(self.set.index());
                let (expired, reads) = (context.expired, context.reads.clone());
                self.set
                    .pool()
                    .run(move || {
                        reads.exclusive(|| {
                            index.update_durable(|writer| {
                                relocate(writer, &chunk, &copies, &committed, expired, &reads)
                            })
                        })
                    })
                    .await??
            };
            dropped += relocated.dropped;
            if relocated.kept {
                report.kept += 1;
                return Ok(());
            }
        }
        let len = log.retire(segment.id).await.map_err(log_error)?;
        if let Some(cache) = self.set.cache() {
            cache.reclaimed(disk, dropped);
        }
        report.segments += 1;
        report.reclaimed_bytes += len;
        tracing::debug!(%disk, segment = %segment.id, len, copied = report.copied_bytes,
            "reclaimed a log segment");
        Ok(())
    }

    /// Decides the fate of each record of `chunk`, on the index pool, and
    /// returns them with the chunk.
    async fn classify(
        &self,
        chunk: Vec<Scanned>,
        committed: Committed,
        context: &Context,
    ) -> Result<(Vec<Scanned>, Vec<Fate>), CompactionError> {
        let index = Arc::clone(self.set.index());
        let context = context.clone();
        let classified = self
            .set
            .pool()
            .run(move || {
                let fates = classify(&index.read()?, &chunk, &committed, &context)?;
                Ok::<_, IndexError>((chunk, fates))
            })
            .await??;
        Ok(classified)
    }

    /// Evicts the clean payloads `fates` drop, and returns how many it
    /// evicted. A refused eviction leaves its record needed, which the
    /// index commit then finds.
    async fn evict(&self, replicas: &mut Replicas<D>, chunk: &[Scanned], fates: &[Fate]) -> u64 {
        let mut evicted = 0;
        let mut tried = HashSet::new();
        for (record, fate) in chunk.iter().zip(fates) {
            let (Fate::Evict { version }, Some(key)) = (fate, &record.key) else {
                continue;
            };
            if !tried.insert((record.shard.clone(), key.clone(), *version)) {
                continue;
            }
            let Some(replica) = replicas.get(&self.set, &record.shard).await else {
                continue;
            };
            if let Ok(Ok(())) = replica.evict(key, *version).await {
                evicted += 1;
            }
        }
        evicted
    }

    /// Adds `report` to the counters.
    fn count(&self, report: &CompactionReport) {
        self.metrics.segments.inc_by(report.segments);
        self.metrics.reclaimed.inc_by(report.reclaimed_bytes);
        self.metrics.copied.inc_by(report.copied_bytes);
        self.metrics.evictions.inc_by(report.evictions);
        self.copied
            .fetch_add(report.copied_bytes, Ordering::Relaxed);
    }

    /// Updates the write amplification from the bytes `logs` wrote.
    fn publish(&self, logs: &[(Label, SegmentLog<D>)]) {
        let written: u64 = logs.iter().map(|(_, log)| log.stats().bytes).sum();
        let copied = self.copied.load(Ordering::Relaxed).min(written);
        let amplification = if written > copied {
            // Byte counts far below 2^52, where the conversion is exact.
            written as f64 / (written - copied) as f64
        } else {
            1.0
        };
        self.metrics.amplification.set(amplification);
    }
}

/// Reads the next chunk of records from `scanner`: at most
/// [`CHUNK_RECORDS`], and no more than [`CHUNK_BYTES`] unless one record
/// is longer. Empty at the end of the segment.
async fn next_chunk<F: skys3_io::SegmentFile>(
    disk: &Label,
    scanner: &mut SegmentScanner<F>,
) -> Result<Vec<Scanned>, CompactionError> {
    let mut chunk = Vec::new();
    let mut bytes = 0;
    while chunk.len() < CHUNK_RECORDS && bytes < CHUNK_BYTES {
        let next = scanner
            .next()
            .await
            .map_err(|source| CompactionError::Scan {
                disk: disk.clone(),
                source,
            })?;
        let Some(record) = next else { break };
        bytes += u64::from(record.location.len);
        chunk.push(scanned(record).map_err(|source| CompactionError::Log {
            disk: disk.clone(),
            source,
        })?);
    }
    Ok(chunk)
}

/// What compaction needs of `record`: its body is decoded only for the
/// kinds that may hold payload, and for `CONFIG`.
fn scanned(record: ScannedRecord) -> Result<Scanned, LogError> {
    let header = &record.header;
    let (key, config) = match header.kind {
        RecordKind::Extent | RecordKind::Put | RecordKind::MpuPart | RecordKind::Config => {
            let decoded = record.decode().map_err(|source| LogError::Damaged {
                location: record.location,
                source,
            })?;
            match decoded.body {
                RecordBody::Config(config) => (None, Some(config)),
                body => (body.key().map(str::to_owned), None),
            }
        }
        _ => (None, None),
    };
    Ok(Scanned {
        shard: header.shard.clone(),
        position: header.position,
        location: record.location,
        kind: header.kind,
        key,
        config,
        bytes: record.bytes,
    })
}

/// The holders of `key` in `shard`, read once per key.
fn holders_of<'a>(
    memo: &'a mut HashMap<(ShardRef, String), Holders>,
    shard: &ShardRef,
    key: &str,
    read: impl FnOnce() -> Result<Holders, IndexError>,
) -> Result<&'a Holders, IndexError> {
    let slot = (shard.clone(), key.to_owned());
    if !memo.contains_key(&slot) {
        let holders = read()?;
        memo.insert(slot.clone(), holders);
    }
    Ok(&memo[&slot])
}

/// Decides the fate of each record of `chunk` (see the [module](self)
/// docs).
fn classify(
    reader: &IndexReader,
    chunk: &[Scanned],
    committed: &Committed,
    context: &Context,
) -> Result<Vec<Fate>, IndexError> {
    let mut memo = HashMap::new();
    let mut fates = Vec::with_capacity(chunk.len());
    for record in chunk {
        let shard = &record.shard;
        let fate = if let Some(config) = &record.config {
            // The configuration the index keeps is the latest applied.
            if reader.config(shard)?.as_ref() == Some(config) {
                Fate::Copy
            } else {
                Fate::Drop
            }
        } else if let Some(key) = &record.key {
            if reader.location(shard, record.position)? != Some(record.location) {
                // Another copy of the record is the one the index locates.
                Fate::Drop
            } else if context.reads.is_pinned(shard, record.position) {
                // A gateway streams it from here (§8.7).
                Fate::Copy
            } else {
                let holders = holders_of(&mut memo, shard, key, || reader.holders(shard, key))?;
                let holder = holders
                    .get(&record.position)
                    .filter(|holder| !dropped_as_coded(holder, shard, committed));
                match holder {
                    None if record.kind == RecordKind::Extent && !context.expired => Fate::Wait,
                    None if !lock(&context.unreferenced).for_at_least(
                        shard,
                        record.position,
                        context.release_delay,
                    ) =>
                    {
                        Fate::Wait
                    }
                    None => Fate::Drop,
                    Some(Holder::Entry {
                        state: EntryState::Clean,
                        version,
                    }) => match &context.cache {
                        Some((cache, hot)) if !cache.is_hot(*hot, shard, key, *version) => {
                            Fate::Evict { version: *version }
                        }
                        _ => Fate::Copy,
                    },
                    Some(_) => Fate::Copy,
                }
            }
        } else {
            Fate::Drop
        };
        fates.push(fate);
    }
    Ok(fates)
}

/// Points the location of each record of `chunk` that was copied at its
/// copy, and removes the locations of those dropped, unless something
/// names one again or a read registration in `reads` pins it: then the
/// segment is kept.
fn relocate(
    writer: &mut IndexWriter<'_>,
    chunk: &[Scanned],
    copies: &[Option<RecordLocation>],
    committed: &Committed,
    expired: bool,
    reads: &Reads,
) -> Result<Relocated, IndexError> {
    let mut memo = HashMap::new();
    let mut relocated = Relocated::default();
    for (record, copy) in chunk.iter().zip(copies) {
        let Some(key) = &record.key else { continue };
        let shard = &record.shard;
        if writer.location(shard, record.position)? != Some(record.location) {
            continue;
        }
        if let Some(copy) = copy {
            writer.put_location(shard, record.position, copy)?;
            continue;
        }
        let holders = holders_of(&mut memo, shard, key, || writer.holders(shard, key))?;
        let named = holders
            .get(&record.position)
            .is_some_and(|holder| !dropped_as_coded(holder, shard, committed));
        if named
            || (record.kind == RecordKind::Extent && !expired)
            || reads.is_pinned(shard, record.position)
        {
            relocated.kept = true;
            continue;
        }
        writer.remove_location(shard, record.position)?;
        relocated.dropped += u64::from(record.location.len);
    }
    Ok(relocated)
}

/// Whether `holder` names replicated bytes this replica drops: those of a
/// coded version whose `EC_PUBLISH` it knows committed in `shard`.
fn dropped_as_coded(holder: &Holder, shard: &ShardRef, committed: &Committed) -> bool {
    let Holder::Coded { publish } = holder else {
        return false;
    };
    is_seeded(SeededBug::DropBeforeCommit)
        || committed
            .get(shard)
            .is_some_and(|through| publish.seq <= *through)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // Every update leaves the value consistent.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use skys3_types::{BucketId, Epoch, Seq, ShardId};

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn unreferenced_payload_waits_out_the_delay_across_passes() {
        let shard = ShardRef::new(BucketId::new("b-1").unwrap(), ShardId::new(0));
        let at = |seq| EpochSeq::new(Epoch::new(1), Seq::new(seq));
        let delay = Duration::from_secs(60);
        let mut seen = Unreferenced::default();
        seen.next_pass();
        assert!(!seen.for_at_least(&shard, at(1), delay));
        assert!(seen.for_at_least(&shard, at(1), Duration::ZERO));
        tokio::time::advance(Duration::from_secs(30)).await;
        seen.next_pass();
        assert!(!seen.for_at_least(&shard, at(1), delay));
        assert!(!seen.for_at_least(&shard, at(2), delay));
        tokio::time::advance(Duration::from_secs(30)).await;
        seen.next_pass();
        assert!(seen.for_at_least(&shard, at(1), delay));
        // Position 2 was not found in that pass, so it starts over.
        seen.next_pass();
        assert!(!seen.for_at_least(&shard, at(2), delay));
        assert_eq!(seen.since.len(), 2);
    }

    #[test]
    fn reports_add_up() {
        let mut total = CompactionReport::default();
        let one = CompactionReport {
            assessed: 1,
            segments: 2,
            reclaimed_bytes: 3,
            copied_bytes: 4,
            evictions: 5,
            kept: 6,
        };
        total.add(&one);
        total.add(&one);
        assert_eq!(total.segments, 4);
        assert_eq!(total.kept, 12);
        assert!(
            Fate::Copy.is_live()
                && Fate::Evict {
                    version: EpochSeq::default()
                }
                .is_live()
        );
        assert!(!Fate::Drop.is_live() && !Fate::Wait.is_live());
    }
}
