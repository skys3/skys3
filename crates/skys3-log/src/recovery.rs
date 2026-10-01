//! Recovery: finding a disk's segments after a restart and cutting torn
//! tails (§10.1, §10.4).
//!
//! # What a crash can leave behind
//!
//! The log writes a group commit's records, syncs every file it wrote, and
//! acknowledges only then. It starts a new segment of a class only between
//! group commits (see [`SegmentLog`](crate::SegmentLog)), so a crash can
//! leave unsynced bytes only at the end of the **last segment of each
//! class**, and at most one group commit's worth:
//! [`LogConfig::tear_window`] bytes. Those bytes may be missing, zeroed, or
//! partly written, in any order the disk chose. Every other segment was
//! fully synced before the next one was created.
//!
//! # What recovery does
//!
//! Recovery lists the segment directory, opens every segment, and scans the
//! last segment of each class from its start. The scan stops at the first
//! bytes that are not a record whose CRC verifies. What happens next depends
//! on [`DecodeError::class`]:
//!
//! - [`ErrorClass::Incomplete`]: the segment ends inside a record. This is a
//!   torn tail, and recovery cuts the segment back to the last whole record.
//! - [`ErrorClass::Corrupt`] (bad magic, impossible lengths, or a CRC
//!   mismatch): a torn tail if nothing after it could have been synced.
//!   Recovery searches the segment from [`LogConfig::tear_window`] bytes
//!   past the bad record to its end. If no verifiable record starts there,
//!   it cuts the tail. Verifiable records inside the window are cut too:
//!   they were written by the group commit the crash interrupted, after a
//!   record that did not survive, and were never acknowledged. If a
//!   verifiable record starts beyond the window, synced records were
//!   damaged, and recovery **refuses to start** with
//!   [`RecoveryError::Damaged`] instead of discarding them.
//! - [`ErrorClass::Unsupported`] (an unknown format version or a kind this
//!   build does not define) or [`ErrorClass::Invalid`] (a verified CRC but a
//!   broken header): a whole record this build cannot use, typically from a
//!   newer build. Recovery **refuses to start** with
//!   [`RecoveryError::Unreadable`]; cutting it would lose data.
//!
//! Recovery then syncs the last segment of each class and the directory,
//! so every record it leaves in place is durable before the log appends
//! after it, even if the previous process crashed without losing its page
//! cache. Earlier segments are not read here. Replay (M1-03) reads the
//! segments it needs with [`SegmentLog::scan`](crate::SegmentLog::scan) and
//! treats any error there as damage.
//!
//! Recovery cannot tell bytes lost to a failed sync from a torn tail. A
//! failed sync takes the disk out of service, and on Linux the page cache
//! may still show the lost bytes as written until the host restarts, so the
//! disk must not be reused before the host restarts.
//!
//! [`DecodeError::class`]: crate::record::DecodeError::class

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;

use skys3_io::{Disk, SegmentFile};

use crate::config::LogConfig;
use crate::record::{DecodeError, ErrorClass};
use crate::scan::{ScanError, SegmentScanner, find_verified_record};
use crate::segment::{SegmentClass, SegmentId, SegmentInfo, file_name, parse_file_name};

/// Why recovery refused to open a disk's log.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RecoveryError {
    /// An I/O operation failed.
    #[error("log recovery failed: {0}")]
    Io(#[from] io::Error),
    /// Two segment files have the same id.
    #[error("segment id {0} is used by both a hot and a bulk segment file")]
    DuplicateId(SegmentId),
    /// A segment holds a whole record this build cannot use: one from an
    /// unknown format version or of a kind this build does not define, or
    /// one whose CRC verifies but whose header breaks the format.
    #[error("segment {segment} holds a record at offset {offset} this build cannot use: {source}")]
    Unreadable {
        /// The segment.
        segment: SegmentId,
        /// Where the record starts.
        offset: u64,
        /// Why it cannot be used.
        source: DecodeError,
    },
    /// A segment has a bad record followed, farther than a crash could
    /// reach, by records that verify: synced records were damaged.
    #[error(
        "segment {segment} is damaged at offset {offset} ({source}), \
         and a valid record follows at offset {next_valid}"
    )]
    Damaged {
        /// The segment.
        segment: SegmentId,
        /// Where the bad bytes start.
        offset: u64,
        /// What is wrong with them.
        source: DecodeError,
        /// Where the next record that verifies starts.
        next_valid: u64,
    },
}

/// A torn tail that recovery cut from a segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TornTail {
    /// The segment.
    pub segment: SegmentId,
    /// The segment's new length: the end of its last whole record.
    pub valid_len: u64,
    /// The bytes cut from the end of the segment.
    pub cut_bytes: u64,
    /// Why the bytes at `valid_len` were not a record.
    pub reason: DecodeError,
}

/// What recovery found and did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    /// Every segment, in id order, with its length after recovery.
    pub segments: Vec<SegmentInfo>,
    /// The torn tails that were cut, at most one per class.
    pub torn_tails: Vec<TornTail>,
    /// Files in the segment directory that are not segments. Recovery
    /// leaves them alone.
    pub ignored_files: Vec<String>,
}

/// A segment found by recovery, open for reading and, if it is the last
/// of its class, for appending.
#[derive(Debug)]
pub(crate) struct RecoveredSegment<F> {
    pub(crate) class: SegmentClass,
    pub(crate) file: Arc<F>,
}

/// The state recovery hands to the log.
#[derive(Debug)]
pub(crate) struct Recovered<F> {
    pub(crate) segments: BTreeMap<SegmentId, RecoveredSegment<F>>,
    /// The id the next new segment takes.
    pub(crate) next_id: Option<SegmentId>,
    pub(crate) report: RecoveryReport,
}

/// Recovers the log on `disk`.
pub(crate) async fn recover<D: Disk>(
    disk: &D,
    config: &LogConfig,
) -> Result<Recovered<D::File>, RecoveryError> {
    let mut report = RecoveryReport::default();
    let mut found = BTreeMap::new();
    for name in disk.list().await? {
        match parse_file_name(&name) {
            Some((class, id)) => {
                if found.insert(id, class).is_some() {
                    return Err(RecoveryError::DuplicateId(id));
                }
            }
            None => report.ignored_files.push(name),
        }
    }

    let mut segments = BTreeMap::new();
    let mut active = [None; 2];
    for (&id, &class) in &found {
        let file = disk.open(&file_name(class, id)).await?;
        segments.insert(
            id,
            RecoveredSegment {
                class,
                file: Arc::new(file),
            },
        );
        active[class.index()] = Some(id);
    }

    for id in active.into_iter().flatten() {
        let file = &segments[&id].file;
        if let Some(tail) = cut_torn_tail(file, id, config).await? {
            report.torn_tails.push(tail);
        }
        file.sync_data().await?;
    }
    disk.sync_dir().await?;

    report.segments = segments
        .iter()
        .map(|(&id, segment)| SegmentInfo {
            id,
            class: segment.class,
            len: segment.file.len(),
        })
        .collect();
    let next_id = match found.keys().next_back() {
        Some(last) => last.next(),
        None => Some(SegmentId::new(0)),
    };
    Ok(Recovered {
        segments,
        next_id,
        report,
    })
}

/// Scans the last segment of a class and cuts its torn tail, if any.
async fn cut_torn_tail<F: SegmentFile>(
    file: &Arc<F>,
    segment: SegmentId,
    config: &LogConfig,
) -> Result<Option<TornTail>, RecoveryError> {
    let mut scanner = SegmentScanner::new(Arc::clone(file), segment);
    let (offset, reason) = loop {
        match scanner.next().await {
            Ok(Some(_)) => {}
            Ok(None) => return Ok(None),
            Err(ScanError::Io { source, .. }) => return Err(source.into()),
            Err(ScanError::Decode { offset, source, .. }) => break (offset, source),
        }
    };
    match reason.class() {
        ErrorClass::Incomplete => {}
        ErrorClass::Corrupt => {
            let beyond = offset.saturating_add(config.tear_window());
            if let Some(next_valid) = find_verified_record(file.as_ref(), beyond).await? {
                return Err(RecoveryError::Damaged {
                    segment,
                    offset,
                    source: reason,
                    next_valid,
                });
            }
        }
        ErrorClass::Unsupported | ErrorClass::Invalid => {
            return Err(RecoveryError::Unreadable {
                segment,
                offset,
                source: reason,
            });
        }
    }
    let cut_bytes = file.len() - offset;
    file.truncate(offset).await?;
    Ok(Some(TornTail {
        segment,
        valid_len: offset,
        cut_bytes,
        reason,
    }))
}
