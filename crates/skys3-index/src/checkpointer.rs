//! Replay after a restart and periodic checkpoints, across a node's logs.

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use skys3_io::{BlockingPool, Disk};
use skys3_log::record::ShardRef;
use skys3_log::{LogError, RecordLocation, SegmentId, SegmentLog, SegmentSummary};
use skys3_types::{EpochSeq, Label};
use tokio::time::MissedTickBehavior;

use crate::error::IndexError;
use crate::index::{Applier, Checkpoint, Index, LogState};

/// How many records replay applies per commit.
const REPLAY_BATCH: usize = 256;

/// The records replay must apply, by shard: each record's position, and
/// the disk, log, and location that hold it.
type Pending<'a, D> =
    BTreeMap<ShardRef, Vec<(EpochSeq, &'a Label, &'a SegmentLog<D>, RecordLocation)>>;

/// What a replay did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplayReport {
    /// The byte range of each segment that replay read, by disk and
    /// segment. A segment the checkpoint already covered is absent.
    pub scanned: BTreeMap<(Label, SegmentId), Range<u64>>,
    /// How many records it applied.
    pub applied: usize,
}

/// A node's index together with the logs it indexes: replays the logs
/// after a restart, and runs the durable checkpoints (§10.2).
///
/// Index I/O runs on `pool`, never on the async reactor (§10.4).
#[derive(Debug)]
pub struct Checkpointer<D: Disk> {
    index: Arc<Index>,
    logs: BTreeMap<Label, SegmentLog<D>>,
    pool: BlockingPool,
}

impl<D: Disk> Checkpointer<D> {
    /// Returns a checkpointer for `index` and each disk's log, by disk ID.
    #[must_use]
    pub fn new(
        index: Arc<Index>,
        logs: BTreeMap<Label, SegmentLog<D>>,
        pool: BlockingPool,
    ) -> Self {
        Self { index, logs, pool }
    }

    /// Returns the index.
    #[must_use]
    pub fn index(&self) -> &Arc<Index> {
        &self.index
    }

    /// Returns each disk's log, by disk ID.
    #[must_use]
    pub fn logs(&self) -> &BTreeMap<Label, SegmentLog<D>> {
        &self.logs
    }

    /// Applies, with `applier`, every log record after its shard's applied
    /// position, and returns what it read and applied.
    ///
    /// Run it once after opening the index and the logs, before the node
    /// appends anything: it reads each segment up to the length the log
    /// recovered. It skips the part of a segment that the index knows to be
    /// at or before every shard's applied position, decodes every record it
    /// applies, and applies each shard's records in position order across
    /// every disk, so a shard whose records span disks replays correctly.
    ///
    /// # Errors
    ///
    /// [`IndexError::Damaged`] or [`IndexError::Scan`] if a record replay
    /// needs is damaged, and otherwise any error from the log, the index,
    /// or `applier`. The index is then left partly replayed; replaying
    /// again after a restart is safe.
    pub async fn replay<A>(&self, applier: Arc<A>) -> Result<ReplayReport, IndexError>
    where
        A: Applier + Send + Sync + 'static,
    {
        let mut report = ReplayReport::default();
        let index = Arc::clone(&self.index);
        let applied = self
            .pool
            .run(move || index.read()?.applied_positions())
            .await??;
        // Every disk is scanned before anything is applied: applying a
        // shard's record advances its applied position, which would hide
        // an earlier record of the shard on a disk not yet scanned.
        let mut pending: Pending<'_, D> = BTreeMap::new();
        for (disk, log) in &self.logs {
            self.scan(disk, log, &applied, &mut pending, &mut report)
                .await?;
        }
        for mut records in pending.into_values() {
            // A shard's extents are in bulk segments and its other records
            // in hot ones, possibly on several disks, so the scans find them
            // in segment order, not position order. A record found on two
            // disks is applied once: `Index::apply` skips the second.
            records.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
            for chunk in records.chunks(REPLAY_BATCH) {
                let mut batch = Vec::with_capacity(chunk.len());
                for &(_, _, log, location) in chunk {
                    batch.push((read_record(log, location).await?, location));
                }
                let index = Arc::clone(&self.index);
                let applier = Arc::clone(&applier);
                report.applied += self
                    .pool
                    .run(move || index.apply(&*applier, &batch))
                    .await??;
            }
        }
        Ok(report)
    }

    /// Reads the segments of `disk` that replay needs, adds the records to
    /// apply to `pending`, and updates what the index knows about each
    /// segment.
    async fn scan<'a>(
        &self,
        disk: &'a Label,
        log: &'a SegmentLog<D>,
        applied: &BTreeMap<ShardRef, EpochSeq>,
        pending: &mut Pending<'a, D>,
        report: &mut ReplayReport,
    ) -> Result<(), IndexError> {
        let known = self.index.coverage_of(disk);
        let summaries = log.summaries();
        let mut coverage = BTreeMap::new();
        for segment in log.segments() {
            let Some(summary) = summaries.get(&segment.id) else {
                // Released by an earlier checkpoint: behind it entirely.
                if let Some(prior) = known.get(&segment.id) {
                    coverage.insert(segment.id, prior.clone());
                }
                continue;
            };
            // The length the log recovered, all of it durable.
            let end = summary.start;
            let prior = known.get(&segment.id).filter(|prior| {
                prior.start == 0
                    && prior.end <= end
                    && prior.is_behind(|shard| applied.get(shard).copied())
            });
            let from = prior.map_or(0, |prior| prior.end);
            let mut scanned = SegmentSummary::starting_at(from);
            let mut scanner = log.scan_range(segment.id, from, end)?;
            while let Some(record) = scanner.next().await? {
                let header = &record.header;
                scanned.add(&header.shard, header.position, record.location.end());
                let done = applied.get(&header.shard);
                if done.is_none_or(|done| header.position > *done) {
                    pending.entry(header.shard.clone()).or_default().push((
                        header.position,
                        disk,
                        log,
                        record.location,
                    ));
                }
            }
            if from < end {
                report.scanned.insert((disk.clone(), segment.id), from..end);
            }
            let mut known_now = prior
                .cloned()
                .unwrap_or_else(|| SegmentSummary::starting_at(0));
            known_now.merge(&scanned);
            coverage.insert(segment.id, known_now);
        }
        self.index.set_coverage(disk, coverage);
        Ok(())
    }

    /// Makes the index durable and releases the log segments that replay
    /// no longer needs.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if the checkpoint fails; see
    /// [`Index::checkpoint`].
    pub async fn checkpoint(&self) -> Result<Checkpoint, IndexError> {
        let logs: BTreeMap<_, _> = self
            .logs
            .iter()
            .map(|(disk, log)| (disk.clone(), LogState::of(log)))
            .collect();
        let index = Arc::clone(&self.index);
        let checkpoint = self.pool.run(move || index.checkpoint(&logs)).await??;
        for (disk, segments) in &checkpoint.releasable {
            if let Some(log) = self.logs.get(disk) {
                log.release(segments.iter().copied());
            }
        }
        Ok(checkpoint)
    }

    /// Runs a checkpoint every `interval` until one fails, and returns its
    /// error. The node must then stop: see [`Index::checkpoint`].
    pub async fn run(&self, interval: Duration) -> IndexError {
        let mut ticks = tokio::time::interval(interval);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // The first tick completes at once.
        ticks.tick().await;
        loop {
            ticks.tick().await;
            if let Err(error) = self.checkpoint().await {
                return error;
            }
        }
    }
}

/// Reads and decodes a record that replay applies. A record whose CRC
/// verified but whose body does not decode is damage.
async fn read_record<D: Disk>(
    log: &SegmentLog<D>,
    location: RecordLocation,
) -> Result<skys3_log::LogRecord, IndexError> {
    log.read(location).await.map_err(|error| match error {
        LogError::Damaged { .. } | LogError::WrongLength { .. } => IndexError::Damaged {
            location,
            source: error,
        },
        error => error.into(),
    })
}
