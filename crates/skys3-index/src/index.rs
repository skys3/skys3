//! The index database: opening it, applying records, updating control
//! state, and durable checkpoints.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read};
use std::path::Path;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use redb::{Database, Durability, ReadableDatabase, ReadableTable, StorageBackend, TableHandle};
use skys3_config::StorageConfig;
use skys3_io::{Disk, SimBlockFile, SimMount};
use skys3_log::record::ShardRef;
use skys3_log::{
    LogRecord, RecordLocation, SegmentClass, SegmentId, SegmentInfo, SegmentLog, SegmentSummary,
};
use skys3_types::{BucketId, Epoch, EpochSeq, Label, ShardConfig};

use crate::codec;
use crate::error::IndexError;
use crate::import::ImportRanges;
use crate::tables::{
    self, COVERAGE, ControlWriter, FORMAT_VERSION_KEY, IndexReader, IndexWriter, META,
};

/// The index format version this build writes, and the newest it opens. It
/// covers the tables and every encoding in [`codec`]. Version 2 adds the
/// uploads and parts tables; opening a version 1 index creates them and
/// marks it version 2, so a build that does not know them refuses it
/// rather than miss the uploads in it. Version 3 adds the imports table
/// (§9.1) in the same way, and version 4 the step-downs of planned
/// handoffs (§5.4).
pub const FORMAT_VERSION: u64 = 5;

/// The oldest index format version this build opens.
pub const MIN_FORMAT_VERSION: u64 = 1;

/// The index's settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexConfig {
    /// `index_checkpoint_interval`: how often a durable checkpoint runs.
    pub checkpoint_interval: Duration,
    /// The memory redb may use to cache pages, in bytes.
    pub cache_bytes: usize,
}

impl IndexConfig {
    /// The page cache size unless configured otherwise: 256 MiB.
    pub const DEFAULT_CACHE_BYTES: usize = 256 << 20;

    /// Takes the index's settings from the `[storage]` section.
    #[must_use]
    pub fn from_storage(storage: &StorageConfig) -> Self {
        Self {
            checkpoint_interval: storage.index_checkpoint_interval(),
            cache_bytes: Self::DEFAULT_CACHE_BYTES,
        }
    }
}

impl Default for IndexConfig {
    /// The settings of the default configuration.
    fn default() -> Self {
        Self::from_storage(&StorageConfig::default())
    }
}

/// Applies log records to the index: the extension point for the shard
/// state machine (M1-04).
///
/// [`Index::apply`] calls it once per record, in each shard's position
/// order, inside a write transaction, and records the shard's applied
/// position after it. The same code path serves live writes and replay
/// after a crash, so an applier that depends only on the record, its
/// location, and what the index holds reproduces the index exactly.
pub trait Applier {
    /// Applies `record`, stored at `location` on this node.
    ///
    /// # Errors
    ///
    /// An error aborts the whole transaction. A record the state machine
    /// rejects, such as an `IMPORT` of a key that has an entry, is not an
    /// error: it is applied as a no-op.
    fn apply(
        &self,
        index: &mut IndexWriter<'_>,
        record: &LogRecord,
        location: RecordLocation,
    ) -> Result<(), IndexError>;
}

/// What a log holds, as a checkpoint needs it: its segments and their
/// summaries.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogState {
    /// The log's segments, as [`SegmentLog::segments`] returns them.
    pub segments: Vec<SegmentInfo>,
    /// The log's segment summaries, as [`SegmentLog::summaries`] returns
    /// them.
    pub summaries: BTreeMap<SegmentId, SegmentSummary>,
}

impl LogState {
    /// Takes the state of `log`. Segments are read after summaries, so
    /// every summarized segment is listed, at least as long as its summary.
    #[must_use]
    pub fn of<D: Disk>(log: &SegmentLog<D>) -> Self {
        let summaries = log.summaries();
        Self {
            segments: log.segments(),
            summaries,
        }
    }
}

/// The result of a durable checkpoint.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Checkpoint {
    /// Each shard's applied position, now durable.
    pub applied: BTreeMap<ShardRef, EpochSeq>,
    /// Per disk, the segments that replay no longer needs: every record in
    /// them is at or before its shard's durable applied position.
    pub releasable: BTreeMap<Label, Vec<SegmentId>>,
}

/// What the index knows about each segment of each disk: a summary of the
/// segment's records from offset zero (§10.2).
type Coverage = BTreeMap<Label, BTreeMap<SegmentId, SegmentSummary>>;

/// A node's index (§10.2): one redb database.
///
/// It holds each shard's namespace index, the node-local location map,
/// each shard's applied position, the node's copy of control state, and
/// what each log segment holds as of the durable checkpoint. The
/// [`codec`] module documents the tables.
///
/// Records are applied with non-durable commits ([`Index::apply`]); a
/// durable checkpoint ([`Index::checkpoint`]) runs every
/// `index_checkpoint_interval`. After a crash the database reverts to its
/// last durable commit, and [`Checkpointer::replay`](crate::Checkpointer::replay)
/// applies the log records after it.
///
/// Every method blocks on disk I/O; call them on a blocking worker, never
/// on the async reactor (§10.4).
pub struct Index {
    db: Database,
    coverage: Mutex<Coverage>,
}

impl std::fmt::Debug for Index {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Index").finish_non_exhaustive()
    }
}

impl Index {
    /// Opens the index in the file at `path`, creating it if it does not
    /// exist.
    ///
    /// A new file's directory entry is synced, so the file survives a
    /// crash; redb itself syncs only the file. A file whose creation a
    /// crash cut short (it lacks redb's magic number, which redb writes
    /// last) is created again (design §10.2).
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if the file cannot be opened or created,
    /// or is not an index this build can read.
    pub fn open(path: &Path, config: &IndexConfig) -> Result<Self, IndexError> {
        match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
        {
            Ok(_) => sync_parent(path)?,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let mut head = Vec::with_capacity(REDB_MAGIC.len());
        (&file)
            .take(REDB_MAGIC.len() as u64)
            .read_to_end(&mut head)?;
        if unfinished_creation(&head, file.metadata()?.len()) {
            file.set_len(0)?;
            file.sync_all()?;
        }
        Self::init(builder(config).create_file(file)?)
    }

    /// Opens the index in the block file `name` on a simulated disk,
    /// creating it if it does not exist, so that a simulated crash reverts
    /// the index exactly as a power loss would.
    ///
    /// # Errors
    ///
    /// As [`Index::open`].
    pub fn open_sim(
        mount: &SimMount,
        name: &str,
        config: &IndexConfig,
    ) -> Result<Self, IndexError> {
        let file = mount.open_block_file(name)?;
        let len = file.len()?;
        let mut head =
            vec![0; usize::try_from(len).map_or(REDB_MAGIC.len(), |len| len.min(REDB_MAGIC.len()))];
        file.read_at(0, &mut head)?;
        if unfinished_creation(&head, len) {
            file.set_len(0)?;
            file.sync()?;
        }
        Self::init(builder(config).create_with_backend(SimStorage(file))?)
    }

    /// Creates the tables if needed, checks the format version, and loads
    /// the coverage table.
    fn init(db: Database) -> Result<Self, IndexError> {
        let mut txn = db.begin_write()?;
        let mut coverage = Coverage::new();
        let created = {
            // An index of this format may predate the shard map, which
            // needs no new format (see `tables::SHARD_MAP`).
            let had_map = txn
                .list_tables()?
                .any(|table| table.name() == tables::SHARD_MAP.name());
            txn.open_table(tables::SHARD_MAP)?;
            let mut meta = txn.open_table(META)?;
            for table in [
                tables::NAMESPACE,
                tables::LOCATIONS,
                tables::SHARDS,
                tables::UPLOADS,
                tables::PARTS,
            ] {
                txn.open_table(table)?;
            }
            txn.open_table(tables::CONTROL)?;
            txn.open_table(tables::IMPORTS)?;
            txn.open_table(tables::STEP_DOWNS)?;
            txn.open_table(tables::PROMOTIONS)?;
            let format = meta.get(FORMAT_VERSION_KEY)?.map(|v| v.value());
            match format {
                Some(FORMAT_VERSION) => {}
                Some(found) if !(MIN_FORMAT_VERSION..FORMAT_VERSION).contains(&found) => {
                    return Err(IndexError::UnsupportedFormat {
                        found,
                        supported: FORMAT_VERSION,
                    });
                }
                // A new index, or an older one, which the tables just opened
                // bring up to date.
                _ => {
                    meta.insert(FORMAT_VERSION_KEY, FORMAT_VERSION)?;
                }
            }
            for row in txn.open_table(COVERAGE)?.iter()? {
                let (key, value) = row?;
                let (disk, segment) = codec::decode_coverage_key(key.value())
                    .map_err(IndexError::codec("coverage"))?;
                let summary =
                    codec::decode_summary(value.value()).map_err(IndexError::codec("coverage"))?;
                coverage.entry(disk).or_default().insert(segment, summary);
            }
            format != Some(FORMAT_VERSION) || !had_map
        };
        if created {
            txn.set_durability(Durability::Immediate)?;
            txn.commit()?;
        } else {
            txn.abort()?;
        }
        Ok(Self {
            db,
            coverage: Mutex::new(coverage),
        })
    }

    /// Returns a consistent read-only view of the index.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if redb fails.
    pub fn read(&self) -> Result<IndexReader, IndexError> {
        IndexReader::open(&self.db.begin_read()?)
    }

    /// Applies `records` with `applier` in one non-durable commit, and
    /// returns how many it applied.
    ///
    /// Each shard's records must come in position order, each with its
    /// location on this node. A record at or before its shard's applied
    /// position was applied already and is skipped, so replaying a record
    /// twice changes nothing. The commit is visible to readers at once and
    /// durable at the next checkpoint; the log makes the records durable
    /// meanwhile.
    ///
    /// # Errors
    ///
    /// Returns the applier's error, or an [`IndexError`] if redb fails. On
    /// an error nothing is applied.
    pub fn apply<A: Applier + ?Sized>(
        &self,
        applier: &A,
        records: &[(LogRecord, RecordLocation)],
    ) -> Result<usize, IndexError> {
        let mut txn = self.db.begin_write()?;
        txn.set_durability(Durability::None)?;
        let mut applied = 0;
        {
            let mut writer = IndexWriter::open(&txn)?;
            for (record, location) in records {
                let done = writer.applied(&record.shard)?;
                if done.is_some_and(|done| record.position <= done) {
                    continue;
                }
                applier.apply(&mut writer, record, *location)?;
                writer.set_applied(&record.shard, record.position)?;
                applied += 1;
            }
        }
        txn.commit()?;
        Ok(applied)
    }

    /// Removes everything the index holds for `shard`, in one non-durable
    /// commit: its entries, its record locations, its applied position,
    /// its step-down (see [`Index::store_step_down`]), and its promotion
    /// (see [`Index::store_promotion`]). A node drops a shard this way once
    /// its bucket is deleted.
    ///
    /// The shard's records stay in the log until their segments are
    /// reclaimed, so replay after a crash can bring the shard back; startup
    /// recovery reclaims shards that no bucket names (§4.1).
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if redb fails. On an error nothing is
    /// removed.
    pub fn remove_shard(&self, shard: &ShardRef) -> Result<(), IndexError> {
        let mut txn = self.db.begin_write()?;
        txn.set_durability(Durability::None)?;
        IndexWriter::open(&txn)?.remove_shard(shard)?;
        txn.open_table(tables::STEP_DOWNS)?
            .remove(codec::shard_key(shard).as_slice())?;
        txn.open_table(tables::PROMOTIONS)?
            .remove(codec::shard_key(shard).as_slice())?;
        txn.commit()?;
        Ok(())
    }

    /// Starts installing a snapshot of `shard` (§6.7), in one durable
    /// commit: removes what the index holds of the shard, as
    /// [`Index::remove_shard`] does, except its step-down and promotion,
    /// and sets its applied position to `marker`. A learner gives the
    /// marker the `seq` [`Seq::MAX`](skys3_types::Seq::MAX), after any
    /// record its log can hold: replay then applies none of the records
    /// the snapshot replaces, even if the install is cut short, and the
    /// marker tells the replica so when it opens again.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if redb fails; nothing changes then.
    pub fn begin_install(&self, shard: &ShardRef, marker: EpochSeq) -> Result<(), IndexError> {
        let mut txn = self.db.begin_write()?;
        txn.set_durability(Durability::Immediate)?;
        {
            let mut writer = IndexWriter::open(&txn)?;
            writer.remove_shard(shard)?;
            writer.set_applied(shard, marker)?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Stores rows of `shard` in `table` as a primary's snapshot carried
    /// them ([`IndexReader::shard_rows`]), in one non-durable commit,
    /// between [`Index::begin_install`] and [`Index::finish_install`].
    ///
    /// # Errors
    ///
    /// [`IndexError::Snapshot`] if a row is of another shard or does not
    /// decode; an [`IndexError`] if redb fails. Nothing is stored then.
    pub fn install_rows(
        &self,
        shard: &ShardRef,
        table: tables::ShardTable,
        rows: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<(), IndexError> {
        for (key, value) in rows {
            table.check(shard, key, value).map_err(IndexError::Snapshot)?;
        }
        let mut txn = self.db.begin_write()?;
        txn.set_durability(Durability::None)?;
        {
            let definition = match table {
                tables::ShardTable::Namespace => tables::NAMESPACE,
                tables::ShardTable::Uploads => tables::UPLOADS,
                tables::ShardTable::Parts => tables::PARTS,
            };
            let mut table = txn.open_table(definition)?;
            for (key, value) in rows {
                table.insert(key.as_slice(), value.as_slice())?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// Finishes installing a snapshot of `shard` whose rows are stored, in
    /// one durable commit: its applied position becomes `applied`, the
    /// position the snapshot was taken at.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if redb fails; nothing changes then.
    pub fn finish_install(&self, shard: &ShardRef, applied: EpochSeq) -> Result<(), IndexError> {
        let mut txn = self.db.begin_write()?;
        txn.set_durability(Durability::Immediate)?;
        IndexWriter::open(&txn)?.set_applied(shard, applied)?;
        txn.commit()?;
        Ok(())
    }

    /// Records where records of `shard` that a learner copied from its
    /// primary are on this node, in one durable commit (§6.7). The records
    /// are at or before the shard's applied position, so replay never
    /// restores their locations: the commit must be durable at once.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if redb fails; nothing changes then.
    pub fn store_locations(
        &self,
        shard: &ShardRef,
        locations: &[(EpochSeq, RecordLocation)],
    ) -> Result<(), IndexError> {
        let mut txn = self.db.begin_write()?;
        txn.set_durability(Durability::Immediate)?;
        {
            let mut writer = IndexWriter::open(&txn)?;
            for (position, location) in locations {
                writer.put_location(shard, *position, location)?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// Changes the node's local copy of control state (§6.2) in one
    /// durable commit, and returns what `update` returns.
    ///
    /// Control state does not come from the log, so replay cannot restore
    /// it; each change is made durable at once instead. Changes are rare.
    ///
    /// # Errors
    ///
    /// Returns `update`'s error, or an [`IndexError`] if redb fails. On an
    /// error nothing changes.
    pub fn update_control<R>(
        &self,
        update: impl FnOnce(&mut ControlWriter<'_>) -> Result<R, IndexError>,
    ) -> Result<R, IndexError> {
        let mut txn = self.db.begin_write()?;
        txn.set_durability(Durability::Immediate)?;
        let result = update(&mut ControlWriter::open(&txn)?)?;
        txn.commit()?;
        Ok(result)
    }

    /// The import ranges of `bucket` and their checkpoints (§9.1), or
    /// `None` if its import never started on this node.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if reading or decoding fails.
    pub fn import_ranges(&self, bucket: &BucketId) -> Result<Option<ImportRanges>, IndexError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(tables::IMPORTS)?;
        let Some(value) = table.get(bucket.as_str())? else {
            return Ok(None);
        };
        codec::decode_import(value.value())
            .map(Some)
            .map_err(IndexError::codec("imports"))
    }

    /// Stores the import ranges of `bucket` and their checkpoints, or
    /// removes them, in one durable commit.
    ///
    /// A checkpoint does not come from the log, so replay cannot restore
    /// it; it is made durable at once instead, once a page of `IMPORT`
    /// records it covers is applied. Each store costs one sync.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if the ranges cannot be encoded or the
    /// commit fails; nothing changes then.
    pub fn set_import_ranges(
        &self,
        bucket: &BucketId,
        import: Option<&ImportRanges>,
    ) -> Result<(), IndexError> {
        let mut txn = self.db.begin_write()?;
        txn.set_durability(Durability::Immediate)?;
        {
            let mut table = txn.open_table(tables::IMPORTS)?;
            match import {
                Some(import) => {
                    let value =
                        codec::encode_import(import).map_err(IndexError::codec("imports"))?;
                    table.insert(bucket.as_str(), value.as_slice())?;
                }
                None => {
                    table.remove(bucket.as_str())?;
                }
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// Keeps `config` as the gateway's configuration of its shard, in one
    /// durable commit (§6.2). The shard map is not in the log, so replay
    /// cannot restore it, but it is a cache: a stale or lost configuration
    /// costs a redirect.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if the configuration does not encode or
    /// redb fails; nothing changes then.
    pub fn store_route(&self, config: &ShardConfig) -> Result<(), IndexError> {
        let shard = ShardRef::new(config.bucket_id.clone(), config.shard);
        let value = codec::encode_route(config).map_err(IndexError::codec("shard_map"))?;
        let mut txn = self.db.begin_write()?;
        txn.set_durability(Durability::Immediate)?;
        txn.open_table(tables::SHARD_MAP)?
            .insert(codec::shard_key(&shard).as_slice(), value.as_slice())?;
        txn.commit()?;
        Ok(())
    }

    /// Records, in one durable commit, that this node's replica of `shard`
    /// stepped down as primary in `epoch` for a planned handoff (§5.4): it
    /// must never serve in that epoch again, even after a restart, since
    /// a step-down message still in flight lets the candidate take over
    /// at once. A later epoch replaces the record.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if redb fails; nothing changes then.
    pub fn store_step_down(&self, shard: &ShardRef, epoch: Epoch) -> Result<(), IndexError> {
        let mut txn = self.db.begin_write()?;
        txn.set_durability(Durability::Immediate)?;
        txn.open_table(tables::STEP_DOWNS)?
            .insert(codec::shard_key(shard).as_slice(), epoch.get())?;
        txn.commit()?;
        Ok(())
    }

    /// Records, in one durable commit, that this node's replica of a shard,
    /// as its primary, proposes `config` to promote a learner (§6.3,
    /// §6.7), before it issues the compare-and-swap: until it learns the
    /// outcome, across restarts too, it keeps waiting for the learner,
    /// since a proposal that landed unseen made the learner a member. A
    /// later proposal replaces the record; one whose epoch the replica has
    /// reached since is settled, so it is ignored rather than removed.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if encoding or redb fails; nothing changes
    /// then.
    pub fn store_promotion(&self, config: &ShardConfig) -> Result<(), IndexError> {
        let shard = ShardRef::new(config.bucket_id.clone(), config.shard);
        let value = codec::encode_route(config).map_err(IndexError::codec("promotions"))?;
        let mut txn = self.db.begin_write()?;
        txn.set_durability(Durability::Immediate)?;
        txn.open_table(tables::PROMOTIONS)?
            .insert(codec::shard_key(&shard).as_slice(), value.as_slice())?;
        txn.commit()?;
        Ok(())
    }

    /// Removes the gateway's configuration of `shard`, in one durable
    /// commit.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if redb fails; nothing changes then.
    pub fn forget_route(&self, shard: &ShardRef) -> Result<(), IndexError> {
        let mut txn = self.db.begin_write()?;
        txn.set_durability(Durability::Immediate)?;
        txn.open_table(tables::SHARD_MAP)?
            .remove(codec::shard_key(shard).as_slice())?;
        txn.commit()?;
        Ok(())
    }

    /// Makes everything applied so far durable, and returns which segments
    /// of each log replay no longer needs.
    ///
    /// `logs` gives the state of each disk's log, taken just before. The
    /// checkpoint folds the logs' segment summaries into what the index
    /// knows about each segment, stores that in the same durable commit as
    /// the applied positions, and then compares the two: a segment that is
    /// not the last of its class is releasable once the index knows every
    /// record in it and each is at or before its shard's applied position.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if the commit fails. The node must then
    /// stop applying records: redb refuses further writes after an I/O
    /// error, and recovery replays from the last durable checkpoint.
    pub fn checkpoint(&self, logs: &BTreeMap<Label, LogState>) -> Result<Checkpoint, IndexError> {
        let coverage = {
            let mut coverage = self.coverage();
            for (disk, log) in logs {
                merge_log(coverage.entry(disk.clone()).or_default(), log);
            }
            coverage.clone()
        };
        let mut txn = self.db.begin_write()?;
        txn.set_durability(Durability::Immediate)?;
        // Quick repair stores the allocator state with the checkpoint, so
        // reopening after a crash need not walk the whole database.
        txn.set_quick_repair(true);
        {
            let mut table = txn.open_table(COVERAGE)?;
            for disk in logs.keys() {
                let prefix = codec::coverage_prefix(disk);
                table.retain(|key, _| !key.starts_with(&prefix))?;
                for (&segment, summary) in coverage.get(disk).into_iter().flatten() {
                    let value =
                        codec::encode_summary(summary).map_err(IndexError::codec("coverage"))?;
                    table.insert(
                        codec::coverage_key(disk, segment).as_slice(),
                        value.as_slice(),
                    )?;
                }
            }
        }
        let applied = IndexWriter::open(&txn)?.applied_positions()?;
        txn.commit()?;
        let releasable = logs
            .iter()
            .map(|(disk, log)| {
                let known = coverage.get(disk).cloned().unwrap_or_default();
                (disk.clone(), releasable(log, &known, &applied))
            })
            .collect();
        Ok(Checkpoint {
            applied,
            releasable,
        })
    }

    /// Returns what the index knows about the segments of `disk`.
    pub(crate) fn coverage_of(&self, disk: &Label) -> BTreeMap<SegmentId, SegmentSummary> {
        self.coverage().get(disk).cloned().unwrap_or_default()
    }

    /// Replaces what the index knows about the segments of `disk`, after
    /// replay has read them. The next checkpoint stores it.
    pub(crate) fn set_coverage(&self, disk: &Label, segments: BTreeMap<SegmentId, SegmentSummary>) {
        self.coverage().insert(disk.clone(), segments);
    }

    fn coverage(&self) -> MutexGuard<'_, Coverage> {
        // Updates replace whole values, which a panic cannot leave torn.
        self.coverage.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Folds a log's summaries into what the index knows about its segments,
/// and forgets segments the log no longer holds.
fn merge_log(known: &mut BTreeMap<SegmentId, SegmentSummary>, log: &LogState) {
    known.retain(|id, _| log.segments.iter().any(|segment| segment.id == *id));
    for (&id, summary) in &log.summaries {
        match known.get_mut(&id) {
            Some(coverage) => {
                // A gap leaves the coverage as it was; replay then reads
                // the segment past it.
                coverage.merge(summary);
            }
            None if summary.start == 0 => {
                known.insert(id, summary.clone());
            }
            None => {}
        }
    }
}

/// Returns the segments of `log` that replay no longer needs.
fn releasable(
    log: &LogState,
    known: &BTreeMap<SegmentId, SegmentSummary>,
    applied: &BTreeMap<ShardRef, EpochSeq>,
) -> Vec<SegmentId> {
    let last = |class: SegmentClass| {
        log.segments
            .iter()
            .filter(|segment| segment.class == class)
            .map(|segment| segment.id)
            .max()
    };
    let tails = [last(SegmentClass::Hot), last(SegmentClass::Bulk)];
    log.segments
        .iter()
        .filter(|segment| !tails.contains(&Some(segment.id)))
        .filter(|segment| {
            known.get(&segment.id).is_some_and(|coverage| {
                coverage.start == 0
                    && coverage.end == segment.len
                    && coverage.is_behind(|shard| applied.get(shard).copied())
            })
        })
        .map(|segment| segment.id)
        .collect()
}

fn builder(config: &IndexConfig) -> redb::Builder {
    let mut builder = redb::Builder::new();
    builder.set_cache_size(config.cache_bytes);
    builder
}

/// Syncs the directory that holds `path`, so a new file there survives a
/// crash.
/// The magic number redb writes at the start of a database file.
const REDB_MAGIC: [u8; 9] = [b'r', b'e', b'd', b'b', 0x1A, 0x0A, 0xA9, 0x0D, 0x0A];

/// Whether a database file of `len` bytes that begins with `head` is one
/// whose creation never finished, so it holds no commit and is created
/// again rather than refused.
///
/// redb creates a database in two synced steps: the header without its
/// magic number, then the magic number. A crash between them, or a torn
/// write of the second, leaves a file that redb refuses forever as "not a
/// redb database", which would keep the node from ever starting. After the
/// magic number is complete, every header write repeats it, so a file
/// whose first bytes are each zero or the magic number's own byte, without
/// being the whole magic number, was never created. Anything else is
/// passed to redb to judge.
fn unfinished_creation(head: &[u8], len: u64) -> bool {
    len > 0
        && head != REDB_MAGIC
        && head
            .iter()
            .zip(REDB_MAGIC)
            .all(|(&byte, magic)| byte == 0 || byte == magic)
}

fn sync_parent(path: &Path) -> io::Result<()> {
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    File::open(parent)?.sync_all()
}

/// redb storage on a simulated disk's block file.
#[derive(Debug)]
struct SimStorage(SimBlockFile);

impl StorageBackend for SimStorage {
    fn len(&self) -> io::Result<u64> {
        self.0.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.0.read_at(offset, out)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.0.set_len(len)
    }

    fn sync_data(&self) -> io::Result<()> {
        self.0.sync()
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.0.write_at(offset, data)
    }
}
