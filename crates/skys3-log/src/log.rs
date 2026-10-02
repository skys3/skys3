//! The segment log: one disk's append-only segments, written by group
//! commit and read back by location.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use skys3_io::{Clock, Disk, MonoTime, SegmentFile};
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
use crate::segment::{
    RecordLocation, SegmentClass, SegmentId, SegmentInfo, SegmentSummary, file_name,
};

/// The longest record: the largest header plus the largest payload.
pub const MAX_RECORD_LEN: u32 = MAX_HEADER_LEN + MAX_PAYLOAD_LEN;

/// How many records may wait for the committer before appenders wait to
/// queue more.
const QUEUE_LEN: usize = 1024;

/// The longest a record appended with [`SegmentLog::append_lazy`] waits
/// for another record to start a group commit before it is committed on its
/// own. Lazy records therefore add at most one sync per this interval to an
/// otherwise idle disk, and none to a busy one.
pub const LAZY_MAX_DELAY: Duration = Duration::from_secs(1);

/// How long a segment [`SegmentLog::retire`] removed stays readable by
/// location: a read that found a record's location before compaction moved
/// or dropped the record still finishes (§10.3).
pub const RETIRE_GRACE: Duration = Duration::from_secs(60);

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
    /// [`SegmentLog::retire`] refused a segment: the last of its class, or
    /// one the index has not released.
    #[error("segment {0} is the last of its class or not released")]
    NotRetirable(SegmentId),
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

/// What the log tracks for replay and compaction: each segment's summary,
/// the segments the index has released, and since when each segment but
/// the last of its class has taken no records.
#[derive(Debug, Default)]
struct Tracking {
    summaries: BTreeMap<SegmentId, SegmentSummary>,
    released: BTreeSet<SegmentId>,
    sealed: BTreeMap<SegmentId, MonoTime>,
}

/// A segment [`SegmentLog::retire`] removed, still readable by location
/// until [`RETIRE_GRACE`] after `at`.
struct Retired<F> {
    file: Arc<F>,
    at: MonoTime,
}

/// State shared by the log's handles and its committer.
pub(crate) struct Shared<F> {
    segments: Mutex<BTreeMap<SegmentId, (SegmentClass, Arc<F>)>>,
    retired: Mutex<BTreeMap<SegmentId, Retired<F>>>,
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

    /// Records that segment `id` takes no records from `at` on, as a newer
    /// segment of its class does.
    pub(crate) fn seal(&self, id: SegmentId, at: MonoTime) {
        self.tracking().sealed.entry(id).or_insert(at);
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

    fn retired(&self) -> std::sync::MutexGuard<'_, BTreeMap<SegmentId, Retired<F>>> {
        // Plain inserts and removals, as for `lock`.
        self.retired.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The file of segment `id`, also if it was retired within the grace.
    ///
    /// It looks in the retired segments with the segments lock held, the
    /// order [`SegmentLog::retire`] takes them in, so a segment that
    /// retirement moves from one to the other is always in one of them.
    fn readable(&self, id: SegmentId) -> Option<Arc<F>> {
        let segments = self.lock();
        segments
            .get(&id)
            .map(|(_, file)| Arc::clone(file))
            .or_else(|| self.retired().get(&id).map(|r| Arc::clone(&r.file)))
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
    disk: Arc<D>,
    requests: mpsc::Sender<Request>,
    clock: Arc<dyn Clock>,
    inline_max_bytes: u64,
}

impl<D: Disk> Clone for SegmentLog<D> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            disk: Arc::clone(&self.disk),
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

/// A record queued for a group commit by [`SegmentLog::queue`], whose
/// acknowledgement has not been awaited yet. Dropping it does not withdraw
/// the record.
#[must_use = "a queued record is acknowledged only through `durable`"]
pub struct Queued<D: Disk> {
    log: SegmentLog<D>,
    acknowledged: oneshot::Receiver<crate::commit::Reply>,
}

impl<D: Disk> fmt::Debug for Queued<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Queued").finish_non_exhaustive()
    }
}

impl<D: Disk> Queued<D> {
    /// Returns the record's location once it is durable.
    ///
    /// # Errors
    ///
    /// [`LogError::OutOfService`] or [`LogError::Closed`] if the record was
    /// not made durable.
    pub async fn durable(self) -> Result<RecordLocation, LogError> {
        match self.acknowledged.await {
            Ok(Ok(location)) => Ok(location),
            Ok(Err(error)) => Err(LogError::OutOfService(error)),
            Err(_) => Err(self.log.closed()),
        }
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
            retired: Mutex::default(),
            tracking: Mutex::default(),
            failure: OnceLock::new(),
            stats: Counters::default(),
        });
        let opened = clock.now();
        for (id, segment) in recovered.segments {
            shared.add_segment(id, segment.class, segment.file);
            // How long a recovered segment has taken no records is unknown,
            // so it counts from now.
            shared.seal(id, opened);
        }
        for class in SegmentClass::ALL {
            if let Some((last, _)) = shared.last_segment(class) {
                shared.tracking().sealed.remove(&last);
            }
        }
        let disk = Arc::new(disk);
        let (requests, receiver) = mpsc::channel(QUEUE_LEN);
        let inline_max_bytes = config.inline_max_bytes;
        let committer = Committer::new(
            Arc::clone(&disk),
            Arc::clone(&shared),
            receiver,
            Arc::clone(&clock),
            config,
            recovered.next_id,
        );
        tokio::spawn(committer.run());
        let log = Self {
            shared,
            disk,
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
        self.queue(record, false).await?.durable().await
    }

    /// Appends `record` without starting a group commit for it: it joins
    /// the next group that another record starts, or commits on its own
    /// after [`LAZY_MAX_DELAY`]. It returns the record's location once it
    /// is durable, like [`SegmentLog::append`].
    ///
    /// Records that may be lost in a crash without harm use it, such as a
    /// `FLUSHED`, which the flusher repeats idempotently (§7.1). Records of
    /// one class are still written in the order they were queued, lazy or
    /// not.
    ///
    /// # Errors
    ///
    /// As [`SegmentLog::append`].
    pub async fn append_lazy(&self, record: &LogRecord) -> Result<RecordLocation, LogError> {
        self.queue(record, true).await?.durable().await
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
        self.queue_encoded(bytes, false).await?.durable().await
    }

    /// Encodes `record` and queues it for a group commit, lazily if `lazy`
    /// (see [`SegmentLog::append_lazy`]), and returns once it is queued.
    /// [`Queued::durable`] then waits for its acknowledgement.
    ///
    /// Records of one class are written in the order they were queued, so a
    /// caller that awaits each `queue` before the next fixes their order,
    /// whatever order it awaits their acknowledgements in.
    ///
    /// # Errors
    ///
    /// As [`SegmentLog::append`], for what fails before the record is
    /// queued.
    pub async fn queue(&self, record: &LogRecord, lazy: bool) -> Result<Queued<D>, LogError> {
        let class = SegmentClass::of(record.kind());
        self.check_inline(class, record.body.payload().len() as u64)?;
        let bytes = record.to_bytes()?;
        self.submit(class, bytes, record.shard.clone(), record.position, lazy)
            .await
    }

    /// Queues one encoded record for a group commit, like
    /// [`SegmentLog::queue`].
    ///
    /// # Errors
    ///
    /// As [`SegmentLog::append_encoded`], for what fails before the record
    /// is queued.
    pub async fn queue_encoded(&self, bytes: Bytes, lazy: bool) -> Result<Queued<D>, LogError> {
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
        self.submit(class, bytes, header.shard, header.position, lazy)
            .await
    }

    /// Checks that [`SegmentLog::append`] would accept `record`, without
    /// encoding its payload: that it encodes, and that a record for a hot
    /// segment carries at most `inline_max_bytes` of payload. A shard
    /// checks a record this way before it gives the record a position, so
    /// a record that cannot be appended never leaves a gap in its log.
    ///
    /// # Errors
    ///
    /// [`LogError::Encode`] or [`LogError::InlineTooLarge`], as `append`
    /// would return them.
    pub fn check(&self, record: &LogRecord) -> Result<(), LogError> {
        let class = SegmentClass::of(record.kind());
        self.check_inline(class, record.body.payload().len() as u64)?;
        record.check()?;
        Ok(())
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
        lazy: bool,
    ) -> Result<Queued<D>, LogError> {
        self.check_in_service()?;
        let (reply, acknowledged) = oneshot::channel();
        let request = Request {
            class,
            bytes,
            shard,
            position,
            arrival: self.clock.now(),
            lazy,
            reply,
        };
        if self.requests.send(request).await.is_err() {
            return Err(self.closed());
        }
        Ok(Queued {
            log: self.clone(),
            acknowledged,
        })
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
        let bytes = self.read_bytes(location).await?;
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

    /// Reads the record at `location` as it is encoded, once its fixed
    /// header and CRC verify: what compaction copies, byte for byte
    /// (§10.3).
    ///
    /// # Errors
    ///
    /// As [`SegmentLog::read`], except that a body that breaks the format
    /// under a valid CRC is not detected.
    pub async fn read_encoded(&self, location: RecordLocation) -> Result<Bytes, LogError> {
        let bytes = self.read_bytes(location).await?;
        let header = RecordHeader::decode(&bytes)
            .map_err(|source| LogError::Damaged { location, source })?;
        if header.record_len() != bytes.len() {
            return Err(LogError::WrongLength {
                location,
                record_len: header.record_len(),
            });
        }
        Ok(bytes)
    }

    /// Reads the bytes at `location`, of a segment the log holds or one
    /// retired within [`RETIRE_GRACE`].
    async fn read_bytes(&self, location: RecordLocation) -> Result<Bytes, LogError> {
        self.check_in_service()?;
        let file = self
            .shared
            .readable(location.segment)
            .ok_or(LogError::UnknownSegment(location.segment))?;
        file.read_at(location.offset, location.len as usize)
            .await
            .map_err(|source| LogError::Read { location, source })
    }

    /// Returns a scanner over the records of segment `id`, from its start
    /// to its current length, for replay.
    ///
    /// A segment [`SegmentLog::segments`] listed can still be scanned for
    /// [`RETIRE_GRACE`] after compaction retires it, and a scanner keeps
    /// its segment's file for as long as it lives. A caller that lists the
    /// segments and then scans them one after another therefore needs to
    /// start every scan within [`RETIRE_GRACE`] of the listing; one that
    /// may take longer takes all its scanners at once with
    /// [`SegmentLog::scan_all`].
    ///
    /// # Errors
    ///
    /// [`LogError::UnknownSegment`] if the log has no such segment, and
    /// [`LogError::OutOfService`] once the disk is out of service.
    pub fn scan(&self, id: SegmentId) -> Result<SegmentScanner<D::File>, LogError> {
        self.check_in_service()?;
        let file = self
            .shared
            .readable(id)
            .ok_or(LogError::UnknownSegment(id))?;
        let end = file.len();
        Ok(SegmentScanner::new(file, id, 0, end))
    }

    /// Returns a scanner over each segment the log holds, in id order, as
    /// [`SegmentLog::scan`] would, all taken at one instant: compaction
    /// cannot retire a segment between them, and each keeps its file for
    /// as long as it lives, so scanning them takes as long as it needs.
    ///
    /// # Errors
    ///
    /// [`LogError::OutOfService`] once the disk is out of service.
    pub fn scan_all(&self) -> Result<Vec<SegmentScanner<D::File>>, LogError> {
        self.check_in_service()?;
        Ok(self
            .shared
            .lock()
            .iter()
            .map(|(&id, (_, file))| SegmentScanner::new(Arc::clone(file), id, 0, file.len()))
            .collect())
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
        let file = self
            .shared
            .readable(id)
            .ok_or(LogError::UnknownSegment(id))?;
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
    /// Only released segments may be retired ([`SegmentLog::retire`]), and
    /// compaction must first copy the payload the index references (§10.3).
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

    /// Returns how long segment `id` has taken no records: since the
    /// committer started a newer segment of its class or, for a segment
    /// the log recovered when it opened, since then. `None` for the last
    /// segment of each class, which still takes records, and for a segment
    /// the log does not hold.
    #[must_use]
    pub fn sealed_for(&self, id: SegmentId) -> Option<Duration> {
        let sealed = self.shared.tracking().sealed.get(&id).copied()?;
        Some(self.clock.now().saturating_duration_since(sealed))
    }

    /// Removes segment `id`, once compaction has copied whatever in it is
    /// still needed and the index no longer locates anything in it
    /// (§10.3), and returns its length. The log stops listing and
    /// summarizing it, removes its file, and syncs the directory, so the
    /// removal is durable when this returns. Reads by location and scans
    /// still find it for [`RETIRE_GRACE`]: a read that found a location
    /// before compaction moved or dropped the record finishes, and so does
    /// a scan of segments listed before (see [`SegmentLog::scan`]).
    ///
    /// Only a segment the index has released ([`SegmentLog::release`])
    /// that is not the last of its class may be retired. Segment ids are
    /// therefore never reused: the last segment of each class, which holds
    /// the highest id, stays.
    ///
    /// # Errors
    ///
    /// [`LogError::UnknownSegment`] or [`LogError::NotRetirable`] for any
    /// other segment; [`LogError::OutOfService`] if removing the file or
    /// syncing the directory fails, which takes the disk out of service,
    /// as any I/O error on the write path does (§10.4). The segment is
    /// unlisted then, but its file may survive a crash; nothing locates
    /// anything in it by then, so a later compaction removes it again.
    pub async fn retire(&self, id: SegmentId) -> Result<u64, LogError> {
        self.check_in_service()?;
        self.drop_retired();
        let at = self.clock.now();
        let (class, len) = {
            // Segments, then tracking, then retired: the order every
            // method that takes more than one of the locks keeps.
            let mut segments = self.shared.lock();
            let mut tracking = self.shared.tracking();
            let class = segments
                .get(&id)
                .map(|(class, _)| *class)
                .ok_or(LogError::UnknownSegment(id))?;
            let last = segments
                .iter()
                .rev()
                .find(|(_, (c, _))| *c == class)
                .map(|(&last, _)| last);
            if last == Some(id) || !tracking.released.contains(&id) {
                return Err(LogError::NotRetirable(id));
            }
            tracking.released.remove(&id);
            tracking.summaries.remove(&id);
            tracking.sealed.remove(&id);
            let (class, file) = segments.remove(&id).ok_or(LogError::UnknownSegment(id))?;
            let len = file.len();
            // Moved to the retired segments under the segments lock, so a
            // read or a scan finds it in one or the other throughout.
            self.shared.retired().insert(id, Retired { file, at });
            (class, len)
        };
        let removed = match self.disk.remove(&file_name(class, id)).await {
            Ok(()) => self.disk.sync_dir().await,
            Err(error) => Err(error),
        };
        match removed {
            Ok(()) => Ok(len),
            Err(error) => Err(LogError::OutOfService(
                self.shared.take_out_of_service(error),
            )),
        }
    }

    /// Stops serving reads of the segments retired more than
    /// [`RETIRE_GRACE`] ago, so that the space they take is freed, and
    /// returns how many it dropped. Compaction calls it on every pass.
    pub fn drop_retired(&self) -> usize {
        let now = self.clock.now();
        let mut retired = self.shared.retired();
        let before = retired.len();
        retired.retain(|_, retired| now.saturating_duration_since(retired.at) < RETIRE_GRACE);
        before - retired.len()
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
