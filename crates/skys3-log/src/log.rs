//! The segment log: one disk's append-only segments, written by group
//! commit and read back by location.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use bytes::Bytes;
use skys3_io::{Clock, Disk, SegmentFile};
use skys3_types::EpochSeq;
use tokio::sync::{mpsc, oneshot};

use crate::commit::{Committer, Request};
use crate::config::LogConfig;
use crate::record::{
    DecodeError, EncodeError, FieldError, LogRecord, MAX_HEADER_LEN, MAX_PAYLOAD_LEN, Problem,
    RecordHeader, ShardRef,
};
use crate::recovery::{RecoveryError, RecoveryReport, recover};
use crate::scan::SegmentScanner;
use crate::segment::{RecordLocation, SegmentClass, SegmentId, SegmentInfo, SegmentSummary};

/// The longest record: the largest header plus the largest payload.
pub const MAX_RECORD_LEN: u32 = MAX_HEADER_LEN + MAX_PAYLOAD_LEN;

/// How many records may wait for the committer before appenders wait to
/// queue more.
const QUEUE_LEN: usize = 1024;

/// Why the log could not append or read a record.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum LogError {
    /// An I/O error took the disk out of service. Nothing is acknowledged
    /// after it; the disk needs recovery, after a host restart (see
    /// [`recovery`](crate::recovery)).
    #[error("the log's disk is out of service: {0}")]
    OutOfService(#[source] Arc<io::Error>),
    /// The log's committer is gone, because its runtime shut down.
    #[error("the log is closed")]
    Closed,
    /// The record cannot be encoded.
    #[error(transparent)]
    Encode(#[from] EncodeError),
    /// Bytes given to [`SegmentLog::append_encoded`] are not one whole,
    /// valid record.
    #[error("not a valid log record: {0}")]
    InvalidRecord(#[source] DecodeError),
    /// A record for a hot segment carries more payload than
    /// `inline_max_bytes`.
    #[error("inline payload of {len} bytes exceeds inline_max_bytes ({max})")]
    InlineTooLarge {
        /// The payload's length.
        len: u64,
        /// `inline_max_bytes`.
        max: u64,
    },
    /// The log has no segment with this id.
    #[error("no segment {0}")]
    UnknownSegment(SegmentId),
    /// Reading a record failed.
    #[error("cannot read the record at {location}: {source}")]
    Read {
        /// The record's location.
        location: RecordLocation,
        /// The read error.
        source: io::Error,
    },
    /// The bytes at a location are not a valid record: the location is
    /// wrong, or the record was damaged after it was acknowledged.
    #[error("the record at {location} is damaged: {source}")]
    Damaged {
        /// The record's location.
        location: RecordLocation,
        /// What is wrong with it.
        source: DecodeError,
    },
    /// The record at a location is not as long as the location says.
    #[error("the record at {location} is {record_len} bytes long")]
    WrongLength {
        /// The location.
        location: RecordLocation,
        /// The length of the record found there.
        record_len: usize,
    },
}

/// Counts of the log's group commits since it was opened.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LogStats {
    /// Group commits that succeeded.
    pub group_commits: u64,
    /// Records those group commits acknowledged.
    pub records: u64,
    /// Bytes those group commits wrote.
    pub bytes: u64,
}

impl LogStats {
    /// The mean number of records per group commit, or zero before the
    /// first.
    #[must_use]
    pub fn records_per_commit(&self) -> f64 {
        if self.group_commits == 0 {
            0.0
        } else {
            // Counts stay far below 2^52, where the conversion is exact.
            self.records as f64 / self.group_commits as f64
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct Counters {
    group_commits: AtomicU64,
    records: AtomicU64,
    bytes: AtomicU64,
}

impl Counters {
    pub(crate) fn record_commit(&self, records: u64, bytes: u64) {
        self.group_commits.fetch_add(1, Ordering::Relaxed);
        self.records.fetch_add(records, Ordering::Relaxed);
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    fn snapshot(&self) -> LogStats {
        LogStats {
            group_commits: self.group_commits.load(Ordering::Relaxed),
            records: self.records.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
        }
    }
}

/// What the log tracks for replay: each segment's summary, and the
/// segments the index has released.
#[derive(Debug, Default)]
struct Tracking {
    summaries: BTreeMap<SegmentId, SegmentSummary>,
    released: BTreeSet<SegmentId>,
}

/// State shared by the log's handles and its committer.
pub(crate) struct Shared<F> {
    segments: Mutex<BTreeMap<SegmentId, (SegmentClass, Arc<F>)>>,
    tracking: Mutex<Tracking>,
    failure: OnceLock<Arc<io::Error>>,
    pub(crate) stats: Counters,
}

impl<F: SegmentFile> Shared<F> {
    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<SegmentId, (SegmentClass, Arc<F>)>> {
        // The lock guards plain inserts and lookups, which cannot leave the
        // map inconsistent if a panic interrupts them.
        self.segments.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn tracking(&self) -> std::sync::MutexGuard<'_, Tracking> {
        // Plain inserts and updates, as for `lock`.
        self.tracking.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Adds segment `id`, whose bytes up to its current length were already
    /// durable when the log learned of it: a recovered segment, or a new,
    /// empty one.
    pub(crate) fn add_segment(&self, id: SegmentId, class: SegmentClass, file: Arc<F>) {
        let len = file.len();
        // The segment is listed before its summary, so a reader that takes
        // summaries and then segments finds every summarized segment.
        self.lock().insert(id, (class, file));
        self.tracking()
            .summaries
            .insert(id, SegmentSummary::starting_at(len));
    }

    /// Adds acknowledged records to their segments' summaries.
    pub(crate) fn summarize<'a>(
        &self,
        records: impl IntoIterator<Item = (&'a ShardRef, EpochSeq, &'a RecordLocation)>,
    ) {
        let mut tracking = self.tracking();
        for (shard, position, location) in records {
            if let Some(summary) = tracking.summaries.get_mut(&location.segment) {
                summary.add(shard, position, location.end());
            }
        }
    }

    pub(crate) fn last_segment(&self, class: SegmentClass) -> Option<(SegmentId, Arc<F>)> {
        self.lock()
            .iter()
            .rev()
            .find(|(_, (c, _))| *c == class)
            .map(|(&id, (_, file))| (id, Arc::clone(file)))
    }

    fn file(&self, id: SegmentId) -> Option<Arc<F>> {
        self.lock().get(&id).map(|(_, file)| Arc::clone(file))
    }

    /// Takes the disk out of service, and returns the error that did.
    pub(crate) fn take_out_of_service(&self, error: io::Error) -> Arc<io::Error> {
        Arc::clone(self.failure.get_or_init(|| Arc::new(error)))
    }
}

impl<F> fmt::Debug for Shared<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shared")
            .field("failure", &self.failure.get())
            .field("stats", &self.stats)
            .finish_non_exhaustive()
    }
}

/// The append-only log on one disk, shared by every shard replica on it
/// (§10.1, §10.4).
///
/// [`SegmentLog::open`] recovers the disk's segments and starts the disk's
/// group committer, a task on the current Tokio runtime. Appenders encode
/// their records and queue them; the committer writes them in groups and
/// acknowledges each record only once `fdatasync` covers it, and, for a
/// record in a new segment file, once a directory `fsync` covers the file.
/// All I/O runs on the disk's own workers, never on the reactor.
///
/// **Segments.** `EXTENT` records go to bulk segments and every other kind
/// to hot segments ([`SegmentClass::of`]). A class's records go to its
/// latest segment. Before a group commit writes to it, the committer starts
/// a new segment if the group's records for the class would take a
/// non-empty segment past `segment_bytes`. So segments change only between
/// group commits, and a segment exceeds `segment_bytes` only when one
/// group's records for its class do.
///
/// **Ordering.** Records of one class are written in the order they were
/// queued. Records of different classes are synced by the same group commit
/// but are durable independently, so a record that depends on another, such
/// as a `PUT` on its extents, is appended after the other is acknowledged.
///
/// **Failures.** The first I/O error on the write path takes the disk out
/// of service: every record not yet acknowledged fails with
/// [`LogError::OutOfService`], and so does every later append and read.
/// After a failed sync the unsynced bytes may be lost even if a later sync
/// succeeds, so the log never retries and never acknowledges after it.
///
/// Cloning a log returns another handle to it. The committer stops once
/// every handle is dropped.
pub struct SegmentLog<D: Disk> {
    shared: Arc<Shared<D::File>>,
    requests: mpsc::Sender<Request>,
    clock: Arc<dyn Clock>,
    inline_max_bytes: u64,
}

impl<D: Disk> Clone for SegmentLog<D> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            requests: self.requests.clone(),
            clock: Arc::clone(&self.clock),
            inline_max_bytes: self.inline_max_bytes,
        }
    }
}

impl<D: Disk> fmt::Debug for SegmentLog<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SegmentLog")
            .field("shared", &self.shared)
            .finish_non_exhaustive()
    }
}

impl<D: Disk> SegmentLog<D> {
    /// Recovers the log on `disk` and starts its group committer on the
    /// current Tokio runtime. Group-commit delays are measured on `clock`.
    ///
    /// Recovery cuts torn tails and refuses to start on records it cannot
    /// use or on damage to synced records; see [`recovery`](crate::recovery).
    ///
    /// # Errors
    ///
    /// Returns a [`RecoveryError`] if recovery fails or refuses to start.
    ///
    /// # Panics
    ///
    /// Panics if called outside a Tokio runtime.
    pub async fn open(
        disk: D,
        config: LogConfig,
        clock: Arc<dyn Clock>,
    ) -> Result<(Self, RecoveryReport), RecoveryError> {
        let recovered = recover(&disk, &config).await?;
        let shared = Arc::new(Shared {
            segments: Mutex::default(),
            tracking: Mutex::default(),
            failure: OnceLock::new(),
            stats: Counters::default(),
        });
        for (id, segment) in recovered.segments {
            shared.add_segment(id, segment.class, segment.file);
        }
        let (requests, receiver) = mpsc::channel(QUEUE_LEN);
        let inline_max_bytes = config.inline_max_bytes;
        let committer = Committer::new(
            disk,
            Arc::clone(&shared),
            receiver,
            Arc::clone(&clock),
            config,
            recovered.next_id,
        );
        tokio::spawn(committer.run());
        let log = Self {
            shared,
            requests,
            clock,
            inline_max_bytes,
        };
        Ok((log, recovered.report))
    }

    /// Appends `record` and returns its location once it is durable.
    ///
    /// The record is encoded on the calling task. Dropping the returned
    /// future does not withdraw a record already queued: it may still be
    /// written and become durable.
    ///
    /// # Errors
    ///
    /// [`LogError::Encode`] if the record breaks the format,
    /// [`LogError::InlineTooLarge`] if a record for a hot segment carries
    /// more than `inline_max_bytes` of payload, and
    /// [`LogError::OutOfService`] or [`LogError::Closed`] if the record was
    /// not made durable. A record that fails this way is never acknowledged.
    pub async fn append(&self, record: &LogRecord) -> Result<RecordLocation, LogError> {
        let class = SegmentClass::of(record.kind());
        self.check_inline(class, record.body.payload().len() as u64)?;
        let bytes = record.to_bytes()?;
        self.submit(class, bytes, record.shard.clone(), record.position)
            .await
    }

    /// Appends one record that is already encoded, such as one received
    /// from a shard's primary, and returns its location once it is durable.
    ///
    /// # Errors
    ///
    /// [`LogError::InvalidRecord`] if `bytes` is not exactly one record
    /// whose fixed header and CRC verify, and otherwise as
    /// [`SegmentLog::append`].
    pub async fn append_encoded(&self, bytes: Bytes) -> Result<RecordLocation, LogError> {
        let header = RecordHeader::decode(&bytes).map_err(LogError::InvalidRecord)?;
        if let Some(extra) = bytes
            .len()
            .checked_sub(header.record_len())
            .filter(|&n| n > 0)
        {
            let problem = Problem::TrailingBytes(extra as u64);
            return Err(LogError::InvalidRecord(
                FieldError::new("record", problem).into(),
            ));
        }
        let class = SegmentClass::of(header.kind);
        self.check_inline(class, header.payload_len.into())?;
        self.submit(class, bytes, header.shard, header.position)
            .await
    }

    fn check_inline(&self, class: SegmentClass, payload_len: u64) -> Result<(), LogError> {
        if class == SegmentClass::Hot && payload_len > self.inline_max_bytes {
            return Err(LogError::InlineTooLarge {
                len: payload_len,
                max: self.inline_max_bytes,
            });
        }
        Ok(())
    }

    async fn submit(
        &self,
        class: SegmentClass,
        bytes: Bytes,
        shard: ShardRef,
        position: EpochSeq,
    ) -> Result<RecordLocation, LogError> {
        self.check_in_service()?;
        let (reply, acknowledged) = oneshot::channel();
        let request = Request {
            class,
            bytes,
            shard,
            position,
            arrival: self.clock.now(),
            reply,
        };
        if self.requests.send(request).await.is_err() {
            return Err(self.closed());
        }
        match acknowledged.await {
            Ok(Ok(location)) => Ok(location),
            Ok(Err(error)) => Err(LogError::OutOfService(error)),
            Err(_) => Err(self.closed()),
        }
    }

    /// Reads and decodes the record at `location`.
    ///
    /// # Errors
    ///
    /// [`LogError::UnknownSegment`] or [`LogError::Read`] if the location
    /// does not exist, [`LogError::Damaged`] or [`LogError::WrongLength`] if
    /// it does not hold exactly one valid record, and
    /// [`LogError::OutOfService`] once the disk is out of service.
    pub async fn read(&self, location: RecordLocation) -> Result<LogRecord, LogError> {
        self.check_in_service()?;
        let file = self
            .shared
            .file(location.segment)
            .ok_or(LogError::UnknownSegment(location.segment))?;
        let bytes = file
            .read_at(location.offset, location.len as usize)
            .await
            .map_err(|source| LogError::Read { location, source })?;
        let (record, record_len) =
            LogRecord::decode(&bytes).map_err(|source| LogError::Damaged { location, source })?;
        if record_len != bytes.len() {
            return Err(LogError::WrongLength {
                location,
                record_len,
            });
        }
        Ok(record)
    }

    /// Returns a scanner over the records of segment `id`, from its start
    /// to its current length, for replay.
    ///
    /// # Errors
    ///
    /// [`LogError::UnknownSegment`] if the log has no such segment, and
    /// [`LogError::OutOfService`] once the disk is out of service.
    pub fn scan(&self, id: SegmentId) -> Result<SegmentScanner<D::File>, LogError> {
        self.check_in_service()?;
        let file = self.shared.file(id).ok_or(LogError::UnknownSegment(id))?;
        let end = file.len();
        Ok(SegmentScanner::new(file, id, 0, end))
    }

    /// Returns a scanner over the records of segment `id` from offset
    /// `from`, which must be where a record starts, to offset `to`, or to
    /// the segment's current length if that is shorter.
    ///
    /// # Errors
    ///
    /// As [`SegmentLog::scan`].
    pub fn scan_range(
        &self,
        id: SegmentId,
        from: u64,
        to: u64,
    ) -> Result<SegmentScanner<D::File>, LogError> {
        self.check_in_service()?;
        let file = self.shared.file(id).ok_or(LogError::UnknownSegment(id))?;
        let end = file.len().min(to);
        Ok(SegmentScanner::new(file, id, from.min(end), end))
    }

    /// Returns the summary of each segment the log holds and the index has
    /// not released, in id order (see [`SegmentSummary`]).
    #[must_use]
    pub fn summaries(&self) -> BTreeMap<SegmentId, SegmentSummary> {
        let tracking = self.shared.tracking();
        tracking
            .summaries
            .iter()
            .filter(|(id, _)| !tracking.released.contains(id))
            .map(|(&id, summary)| (id, summary.clone()))
            .collect()
    }

    /// Records that replay no longer needs segments `ids`: every record in
    /// them is behind the index's durable checkpoint (§10.2). A release
    /// lasts until the log is reopened, after which the index releases the
    /// segments again from its checkpoint. Ids the log does not hold are
    /// ignored.
    ///
    /// Only released segments may be reclaimed (§10.3, M1-22), and
    /// reclaiming must still keep the payload the index references.
    pub fn release(&self, ids: impl IntoIterator<Item = SegmentId>) {
        let held = self.shared.lock();
        let mut tracking = self.shared.tracking();
        for id in ids {
            if held.contains_key(&id) {
                tracking.released.insert(id);
                tracking.summaries.remove(&id);
            }
        }
    }

    /// Returns the segments the index has released since the log opened.
    #[must_use]
    pub fn released(&self) -> BTreeSet<SegmentId> {
        self.shared.tracking().released.clone()
    }

    /// Returns the log's segments in id order, which is creation order.
    #[must_use]
    pub fn segments(&self) -> Vec<SegmentInfo> {
        self.shared
            .lock()
            .iter()
            .map(|(&id, (class, file))| SegmentInfo {
                id,
                class: *class,
                len: file.len(),
            })
            .collect()
    }

    /// Returns counts of the group commits since the log was opened.
    #[must_use]
    pub fn stats(&self) -> LogStats {
        self.shared.stats.snapshot()
    }

    /// Returns the error that took the disk out of service, if any.
    #[must_use]
    pub fn failure(&self) -> Option<Arc<io::Error>> {
        self.shared.failure.get().cloned()
    }

    /// Returns whether the disk is in service: no I/O error has occurred
    /// on its write path.
    #[must_use]
    pub fn is_in_service(&self) -> bool {
        self.shared.failure.get().is_none()
    }

    fn check_in_service(&self) -> Result<(), LogError> {
        match self.failure() {
            Some(error) => Err(LogError::OutOfService(error)),
            None => Ok(()),
        }
    }

    /// The error for a request the committer did not answer.
    fn closed(&self) -> LogError {
        self.failure()
            .map_or(LogError::Closed, LogError::OutOfService)
    }
}
