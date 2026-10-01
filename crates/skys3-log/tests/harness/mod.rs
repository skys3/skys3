//! Shared helpers for the segment log's tests: records, a simulated disk
//! that crashes or fails a sync at a chosen operation and audits what the
//! log acknowledged, and a workload of concurrent appenders.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use skys3_io::disk::SimFile;
use skys3_io::{Clock, Disk, MonotonicClock, SegmentFile, SimDisk, SimMount};
use skys3_log::record::{Delete, Extent, Put, PutData};
use skys3_log::segment::parse_file_name;
use skys3_log::{
    LogConfig, LogError, LogRecord, RecordBody, RecordLocation, RecoveryError, RecoveryReport,
    SegmentId, SegmentLog, ShardRef,
};
use skys3_types::{BucketId, ETag, Epoch, EpochSeq, Seq, ShardId};

pub fn shard(n: u8) -> ShardRef {
    ShardRef::new(BucketId::new("b-test").unwrap(), ShardId::new(n))
}

fn position(seq: u64) -> EpochSeq {
    EpochSeq::new(Epoch::new(1), Seq::new(seq))
}

/// A `DELETE` record, which goes to a hot segment.
pub fn delete(shard_no: u8, seq: u64) -> LogRecord {
    LogRecord {
        shard: shard(shard_no),
        position: position(seq),
        body: RecordBody::Delete(Delete {
            key: format!("key-{seq}"),
        }),
    }
}

/// A `PUT` record with `len` bytes of inline data, which goes to a hot
/// segment.
pub fn inline_put(shard_no: u8, seq: u64, len: usize) -> LogRecord {
    LogRecord {
        shard: shard(shard_no),
        position: position(seq),
        body: RecordBody::Put(Put {
            key: format!("key-{seq}"),
            size: len as u64,
            last_modified_ms: 1_700_000_000_000,
            etag: ETag::new("d41d8cd98f00b204e9800998ecf8427e").unwrap(),
            inherited_identity: None,
            metadata: BTreeMap::new(),
            tags: BTreeMap::new(),
            checksums: BTreeMap::new(),
            copy_source: None,
            data: PutData::Inline(fill(seq, len)),
        }),
    }
}

/// An `EXTENT` record with `len` bytes of payload, which goes to a bulk
/// segment.
pub fn extent(shard_no: u8, seq: u64, len: usize) -> LogRecord {
    LogRecord {
        shard: shard(shard_no),
        position: position(seq),
        body: RecordBody::Extent(Extent {
            key: format!("key-{seq}"),
            offset: 0,
            data: fill(seq, len),
        }),
    }
}

fn fill(seq: u64, len: usize) -> Bytes {
    (0..len)
        .map(|i| (seq as usize).wrapping_mul(31).wrapping_add(i) as u8)
        .collect()
}

/// The clock every test log uses: the runtime's, without drift.
pub fn clock() -> Arc<dyn Clock> {
    Arc::new(MonotonicClock::new())
}

/// Small segments and group commits, so a short workload rolls over
/// segments and splits into several groups.
pub fn small_config() -> LogConfig {
    LogConfig {
        inline_max_bytes: 512,
        segment_bytes: 2048,
        group_commit_max_delay: Duration::from_micros(200),
        group_commit_max_bytes: 1024,
    }
}

/// Opens the log on `disk`.
pub async fn open<D: Disk>(
    disk: D,
    config: LogConfig,
) -> Result<(SegmentLog<D>, RecoveryReport), RecoveryError> {
    SegmentLog::open(disk, config, clock()).await
}

/// A paused current-thread runtime, as simulation-style tests use.
pub fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap()
}

// ---------------------------------------------------------------------------
// A disk with planned faults that audits durability.
// ---------------------------------------------------------------------------

/// Faults planned by operation number, and what the disk saw.
#[derive(Debug, Default)]
pub struct Audit {
    /// Write-path operations so far: creates, removes, appends, syncs, and
    /// truncations.
    pub ops: u64,
    /// Syncs so far, of files and of the directory.
    pub syncs: u64,
    /// Crash the device just before this operation: a power loss.
    pub crash_at: Option<u64>,
    /// Kill the process just before this operation: it and every later
    /// operation fail without reaching the device, whose page cache
    /// survives.
    pub kill_at: Option<u64>,
    /// Whether the process was killed.
    pub killed: bool,
    /// Fail this sync.
    pub fail_sync_at: Option<u64>,
    /// Whether a planned sync failure happened.
    pub sync_failed: bool,
    /// Per file: the length the last successful sync covered, as long as no
    /// sync of the file has failed.
    pub synced: BTreeMap<String, u64>,
    /// Files whose sync failed. Nothing in them may be acknowledged after.
    pub sync_failed_files: BTreeSet<String>,
    /// Files created since the last successful directory sync.
    pub undurable_entries: BTreeSet<String>,
    /// How long each sync takes, in the runtime's time.
    pub sync_delay: Duration,
}

/// A mount of a [`SimDisk`] that crashes the device, or fails a sync, at a
/// planned operation, and records which bytes successful syncs covered.
///
/// Each write-path operation yields to the runtime once, as a real disk's
/// I/O does, so appenders can queue records while a group commit is in
/// progress.
#[derive(Clone, Debug)]
pub struct AuditedMount {
    inner: SimMount,
    audit: Arc<Mutex<Audit>>,
}

impl AuditedMount {
    pub fn new(disk: &SimDisk, audit: Audit) -> Self {
        Self {
            inner: disk.mount(),
            audit: Arc::new(Mutex::new(audit)),
        }
    }

    pub fn audit(&self) -> std::sync::MutexGuard<'_, Audit> {
        self.audit.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Counts an operation, crashing the device or killing the process
    /// first if it is the planned one, and yields.
    async fn op(&self) -> io::Result<()> {
        {
            let mut audit = self.audit();
            if audit.crash_at == Some(audit.ops) {
                self.inner.disk().crash();
            }
            if audit.kill_at == Some(audit.ops) {
                audit.killed = true;
            }
            if audit.killed {
                return Err(io::Error::other("the process was killed"));
            }
            audit.ops += 1;
        }
        tokio::task::yield_now().await;
        Ok(())
    }

    /// Counts a sync, arranging for it to fail if it is the planned one.
    async fn sync(&self) -> io::Result<bool> {
        self.op().await?;
        let delay = self.audit().sync_delay;
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        let mut audit = self.audit();
        let fail = audit.fail_sync_at == Some(audit.syncs);
        audit.syncs += 1;
        if fail {
            audit.sync_failed = true;
            self.inner.disk().fail_next_syncs(1);
        }
        Ok(fail)
    }

    /// Checks that every acknowledged record was covered by a successful
    /// sync of its file, made after any failed sync of it, and that its
    /// file's directory entry was synced.
    pub fn check_acknowledged(&self, acknowledged: &[(LogRecord, RecordLocation)]) {
        let audit = self.audit();
        for (record, location) in acknowledged {
            let name = audit
                .synced
                .keys()
                .find(|name| parse_file_name(name).map(|(_, id)| id) == Some(location.segment))
                .unwrap_or_else(|| panic!("{location} was acknowledged, but never synced"));
            assert!(
                location.end() <= audit.synced[name],
                "{record:?} at {location} was acknowledged beyond the synced length {}",
                audit.synced[name]
            );
            assert!(
                !audit.undurable_entries.contains(name),
                "{location} was acknowledged before the directory entry of {name} was synced"
            );
        }
    }
}

impl Disk for AuditedMount {
    type File = AuditedFile;

    async fn create(&self, name: &str) -> io::Result<AuditedFile> {
        self.op().await?;
        let file = self.inner.create(name).await?;
        self.audit().undurable_entries.insert(name.to_owned());
        Ok(AuditedFile {
            inner: file,
            name: name.to_owned(),
            mount: self.clone(),
        })
    }

    async fn open(&self, name: &str) -> io::Result<AuditedFile> {
        let file = self.inner.open(name).await?;
        Ok(AuditedFile {
            inner: file,
            name: name.to_owned(),
            mount: self.clone(),
        })
    }

    async fn remove(&self, name: &str) -> io::Result<()> {
        self.op().await?;
        self.inner.remove(name).await
    }

    async fn list(&self) -> io::Result<Vec<String>> {
        self.inner.list().await
    }

    async fn sync_dir(&self) -> io::Result<()> {
        // Entries created before the sync starts are covered by it.
        let pending: Vec<_> = self.audit().undurable_entries.iter().cloned().collect();
        self.sync().await?;
        self.inner.sync_dir().await?;
        let mut audit = self.audit();
        for name in pending {
            audit.undurable_entries.remove(&name);
        }
        Ok(())
    }
}

/// A file on an [`AuditedMount`].
#[derive(Debug)]
pub struct AuditedFile {
    inner: SimFile,
    name: String,
    mount: AuditedMount,
}

impl SegmentFile for AuditedFile {
    fn len(&self) -> u64 {
        self.inner.len()
    }

    async fn append(&self, data: Bytes) -> io::Result<u64> {
        self.mount.op().await?;
        self.inner.append(data).await
    }

    async fn sync_data(&self) -> io::Result<()> {
        let len = self.inner.len();
        let planned_failure = self.mount.sync().await?;
        let result = self.inner.sync_data().await;
        let mut audit = self.mount.audit();
        match &result {
            Ok(()) if !audit.sync_failed_files.contains(&self.name) => {
                audit.synced.insert(self.name.clone(), len);
            }
            Ok(()) => {}
            Err(_) => {
                assert!(
                    planned_failure || audit.crash_at.is_some(),
                    "unplanned sync error"
                );
                audit.sync_failed_files.insert(self.name.clone());
            }
        }
        result
    }

    async fn read_at(&self, offset: u64, len: usize) -> io::Result<Bytes> {
        self.inner.read_at(offset, len).await
    }

    async fn truncate(&self, len: u64) -> io::Result<()> {
        self.mount.op().await?;
        self.inner.truncate(len).await?;
        let mut audit = self.mount.audit();
        if let Some(synced) = audit.synced.get_mut(&self.name) {
            *synced = (*synced).min(len);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// A workload of concurrent appenders.
// ---------------------------------------------------------------------------

/// The records appender `writer` of a workload appends, in order: a mix of
/// small and inline hot records and bulk extents.
pub fn writer_records(writer: u8, count: u64) -> Vec<LogRecord> {
    (0..count)
        .map(|i| {
            let seq = u64::from(writer) * 1_000 + i;
            match (seq + i) % 4 {
                0 => delete(writer, seq),
                1 => inline_put(writer, seq, (seq as usize * 37) % 400),
                2 => extent(writer, seq, 200 + (seq as usize * 53) % 900),
                _ => extent(writer, seq, 2_000),
            }
        })
        .collect()
}

/// What a workload's appenders were told.
#[derive(Debug, Default)]
pub struct Outcome {
    /// Records acknowledged, with their locations.
    pub acknowledged: Vec<(LogRecord, RecordLocation)>,
    /// Every record submitted, acknowledged or not.
    pub submitted: Vec<LogRecord>,
    /// Appends that failed.
    pub failed: u64,
}

impl Outcome {
    /// Adds what a later run's appenders were told.
    pub fn extend(&mut self, other: Outcome) {
        self.acknowledged.extend(other.acknowledged);
        self.submitted.extend(other.submitted);
        self.failed += other.failed;
    }
}

/// Runs `writers` concurrent appenders of `count` records each against
/// `log`. Each stops at its first failed append.
pub async fn run_workload<D: Disk>(log: &SegmentLog<D>, writers: u8, count: u64) -> Outcome {
    run_writers(log, 0..writers, count).await
}

/// Runs appenders `writers` with `count` records each against `log`. Each
/// stops at its first failed append.
pub async fn run_writers<D: Disk>(
    log: &SegmentLog<D>,
    writers: std::ops::Range<u8>,
    count: u64,
) -> Outcome {
    let outcome = Arc::new(Mutex::new(Outcome::default()));
    let mut tasks = Vec::new();
    for writer in writers {
        let log = log.clone();
        let outcome = Arc::clone(&outcome);
        tasks.push(tokio::spawn(async move {
            for record in writer_records(writer, count) {
                outcome.lock().unwrap().submitted.push(record.clone());
                match log.append(&record).await {
                    Ok(location) => outcome
                        .lock()
                        .unwrap()
                        .acknowledged
                        .push((record, location)),
                    Err(error) => {
                        assert!(
                            matches!(error, LogError::OutOfService(_)),
                            "unexpected error: {error}"
                        );
                        outcome.lock().unwrap().failed += 1;
                        return;
                    }
                }
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    Arc::try_unwrap(outcome).unwrap().into_inner().unwrap()
}

/// Checks a recovered log: every acknowledged record reads back at its
/// location, and every record in every segment was submitted.
pub async fn check_recovered<D: Disk>(log: &SegmentLog<D>, outcome: &Outcome) {
    for (record, location) in &outcome.acknowledged {
        let read = log
            .read(*location)
            .await
            .unwrap_or_else(|e| panic!("acknowledged record at {location} is lost: {e}"));
        assert_eq!(&read, record, "the record at {location} changed");
    }
    let mut found = 0;
    for segment in log.segments() {
        let mut scanner = log.scan(segment.id).unwrap();
        while let Some(scanned) = scanner.next().await.unwrap() {
            let record = scanned.decode().unwrap();
            assert!(
                outcome.submitted.contains(&record),
                "recovered a record that was never submitted: {record:?}"
            );
            found += 1;
        }
    }
    assert!(found >= outcome.acknowledged.len());
}

/// Returns the segment ids of `log`.
pub fn segment_ids<D: Disk>(log: &SegmentLog<D>) -> Vec<SegmentId> {
    log.segments().into_iter().map(|s| s.id).collect()
}
