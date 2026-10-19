//! Recovery of a disk's fragment segments after a restart (§10.1, §10.4).
//!
//! The committer starts a new segment only between group commits, and
//! acknowledges a group only once a sync covers it, so a crash can leave
//! unsynced bytes only at the end of the **last** segment, and at most one
//! group commit's worth ([`FragmentStoreConfig::tear_window`]). Recovery
//! therefore:
//!
//! - reads every header of every other segment, checking its CRC, to build
//!   the fragment map. Any error there is damage to synced data, and
//!   recovery refuses to start rather than forget fragments;
//! - reads the last segment whole, checking each header's CRC and each
//!   payload block's, and acts on the first record that fails as the log
//!   does: an incomplete record is a torn tail, cut; a corrupt one is cut
//!   too, unless a record that verifies starts beyond the tear window,
//!   which means synced records were damaged and recovery refuses; a record
//!   of an unknown version, or a malformed header under a valid CRC, is a
//!   whole record this build cannot use, and recovery refuses;
//! - syncs the last segment and the directory, so everything it keeps is
//!   durable before the store appends after it, even if the previous
//!   process died without losing its page cache.
//!
//! Every fragment found must carry the disk number the store was opened
//! with, so a disk opened under the wrong number is refused.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;

use skys3_io::{Disk, SegmentFile};
use skys3_log::record::ErrorClass;

use super::{Bugs, FragmentSegment, FragmentStoreConfig, Located, disk_of, parse_segment_name};
use crate::FragmentId;
use crate::fragment::{DecodedHeader, FIXED_LEN, FixedHeader, FragmentDecodeError, MAGIC};

/// How much recovery reads at a time when it checks payloads or searches
/// for records.
const CHUNK_LEN: u64 = 1 << 20;

/// Why the fragment store refused to open a disk.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RecoveryError {
    /// An I/O operation failed.
    #[error("fragment store recovery failed: {0}")]
    Io(#[from] io::Error),
    /// A segment holds a record this build cannot use: an unknown format
    /// version, or a malformed header under a valid CRC.
    #[error(
        "fragment segment {segment} holds a record at offset {offset} this build cannot use: {source}"
    )]
    Unreadable {
        /// The segment.
        segment: u64,
        /// Where the record starts.
        offset: u64,
        /// Why it cannot be used.
        source: FragmentDecodeError,
    },
    /// Synced records were damaged: a bad record in a segment that was
    /// fully synced, or one in the last segment followed, beyond what a
    /// crash could leave unsynced, by a record that verifies.
    #[error("fragment segment {segment} is damaged at offset {offset}: {source}")]
    Damaged {
        /// The segment.
        segment: u64,
        /// Where the bad bytes start.
        offset: u64,
        /// What is wrong with them.
        source: FragmentDecodeError,
        /// Where the next record that verifies starts, if recovery looked.
        next_valid: Option<u64>,
    },
    /// A fragment names another disk: the disk was opened under the wrong
    /// number.
    #[error("fragment {id} in segment {segment} belongs to disk {disk}, not this one")]
    ForeignFragment {
        /// The segment.
        segment: u64,
        /// The fragment's ID.
        id: FragmentId,
        /// The disk number in the ID.
        disk: u8,
    },
}

/// A torn tail that recovery cut from the last segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TornTail {
    /// The segment.
    pub segment: u64,
    /// The segment's new length: the end of its last whole record.
    pub valid_len: u64,
    /// The bytes cut.
    pub cut_bytes: u64,
    /// Why the bytes at `valid_len` were not a record.
    pub reason: FragmentDecodeError,
}

/// What recovery found and did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    /// Every fragment segment, in number order, with its length after
    /// recovery.
    pub segments: Vec<FragmentSegment>,
    /// The fragments found.
    pub fragments: usize,
    /// The torn tail cut from the last segment, if any.
    pub torn_tail: Option<TornTail>,
    /// Files on the disk that are not fragment segments, such as the log's
    /// segments. Recovery leaves them alone.
    pub ignored_files: Vec<String>,
}

/// The state recovery hands to the store.
pub(crate) struct Recovered<F> {
    pub(crate) segments: BTreeMap<u64, Arc<F>>,
    pub(crate) fragments: Vec<(FragmentId, Located)>,
    pub(crate) next_segment: Option<u64>,
    pub(crate) report: RecoveryReport,
}

/// Why reading a record stopped.
enum Stop {
    Io(io::Error),
    Decode(FragmentDecodeError),
}

impl From<io::Error> for Stop {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<FragmentDecodeError> for Stop {
    fn from(error: FragmentDecodeError) -> Self {
        Self::Decode(error)
    }
}

/// Recovers the fragment segments on `disk`.
pub(crate) async fn recover<D: Disk>(
    disk: &D,
    config: &FragmentStoreConfig,
    bugs: Bugs,
) -> Result<Recovered<D::File>, RecoveryError> {
    let mut report = RecoveryReport::default();
    let mut segments = BTreeMap::new();
    for name in disk.list().await? {
        match parse_segment_name(&name) {
            Some(id) => {
                segments.insert(id, Arc::new(disk.open(&name).await?));
            }
            None => report.ignored_files.push(name),
        }
    }
    let last = segments.keys().next_back().copied();
    let mut fragments = Vec::new();
    for (&segment, file) in &segments {
        let is_last = Some(segment) == last;
        let (found, stop) = scan(file.as_ref(), segment, is_last).await;
        for (id, located) in found {
            if disk_of(id) != config.disk {
                let disk = disk_of(id);
                return Err(RecoveryError::ForeignFragment { segment, id, disk });
            }
            fragments.push((id, located));
        }
        let Some((offset, stop)) = stop else { continue };
        let source = match stop {
            Stop::Io(error) => return Err(error.into()),
            Stop::Decode(source) => source,
        };
        match source.class() {
            ErrorClass::Unsupported | ErrorClass::Invalid => {
                return Err(RecoveryError::Unreadable {
                    segment,
                    offset,
                    source,
                });
            }
            ErrorClass::Incomplete | ErrorClass::Corrupt if !is_last => {
                return Err(RecoveryError::Damaged {
                    segment,
                    offset,
                    source,
                    next_valid: None,
                });
            }
            ErrorClass::Incomplete => {}
            ErrorClass::Corrupt => {
                let beyond = offset.saturating_add(config.tear_window());
                if let Some(next) = find_verified_record(file.as_ref(), beyond).await? {
                    return Err(RecoveryError::Damaged {
                        segment,
                        offset,
                        source,
                        next_valid: Some(next),
                    });
                }
            }
        }
        report.torn_tail = Some(TornTail {
            segment,
            valid_len: offset,
            cut_bytes: file.len() - offset,
            reason: source,
        });
        file.truncate(offset).await?;
    }
    if !bugs.skip_recovery_sync {
        if let Some(last) = last {
            segments[&last].sync_data().await?;
        }
        disk.sync_dir().await?;
    }
    report.fragments = fragments.len();
    report.segments = segments
        .iter()
        .map(|(&id, file)| FragmentSegment {
            id,
            len: file.len(),
        })
        .collect();
    let next_segment = match last {
        Some(last) => last.checked_add(1),
        None => Some(0),
    };
    Ok(Recovered {
        segments,
        fragments,
        next_segment,
        report,
    })
}

/// Reads the records of `file` from its start, checking every header and,
/// if `payloads`, every payload block. Returns the records read and, if it
/// stopped before the end, where and why.
async fn scan<F: SegmentFile>(
    file: &F,
    segment: u64,
    payloads: bool,
) -> (Vec<(FragmentId, Located)>, Option<(u64, Stop)>) {
    let end = file.len();
    let mut offset = 0;
    let mut found = Vec::new();
    while offset < end {
        match read_record(file, offset, end, payloads).await {
            Ok(decoded) => {
                let located = Located {
                    segment,
                    offset,
                    header_len: decoded.header_len,
                    payload_len: decoded.payload_len,
                };
                offset += located.record_len();
                found.push((decoded.id, located));
            }
            Err(stop) => return (found, Some((offset, stop))),
        }
    }
    (found, None)
}

/// A record's header read back by [`read_record`].
struct ReadRecord {
    id: FragmentId,
    header_len: u32,
    payload_len: u64,
}

/// Reads and checks the record at `offset` of `file`, which ends at `end`:
/// its header always, its payload blocks if `payloads`.
async fn read_record<F: SegmentFile>(
    file: &F,
    offset: u64,
    end: u64,
    payloads: bool,
) -> Result<ReadRecord, Stop> {
    let available = end - offset;
    let incomplete = |needed: u64| FragmentDecodeError::Incomplete { needed, available };
    if available < FIXED_LEN as u64 {
        return Err(incomplete(FIXED_LEN as u64).into());
    }
    let fixed = file.read_at(offset, FIXED_LEN).await?;
    let fixed = FixedHeader::peek(&fixed)?;
    if available < u64::from(fixed.header_len) {
        return Err(incomplete(fixed.header_len.into()).into());
    }
    let header = file.read_at(offset, fixed.header_len as usize).await?;
    let decoded = DecodedHeader::decode(&header)?;
    if available < fixed.record_len() {
        return Err(incomplete(fixed.record_len()).into());
    }
    if payloads {
        let start = offset + u64::from(fixed.header_len);
        let mut done = 0;
        while done < fixed.payload_len {
            let len = (fixed.payload_len - done).min(CHUNK_LEN);
            let data = file.read_at(start + done, len as usize).await?;
            decoded.verify_blocks(done / crate::fragment::BLOCK_LEN, &data)?;
            done += len;
        }
    }
    Ok(ReadRecord {
        id: decoded.id,
        header_len: fixed.header_len,
        payload_len: fixed.payload_len,
    })
}

/// Returns the offset of the first record at or after `from` whose header
/// and payload verify, or `None`. Every offset where the magic appears is
/// tried, so this reads the whole range; recovery calls it only past a
/// tear, where the range is normally empty.
async fn find_verified_record<F: SegmentFile>(file: &F, from: u64) -> io::Result<Option<u64>> {
    let end = file.len();
    let mut chunk_start = from;
    while chunk_start < end {
        let len = (end - chunk_start).min(CHUNK_LEN);
        let chunk = file.read_at(chunk_start, len as usize).await?;
        // A magic may straddle the chunk's end; the next chunk starts
        // after the last position tried here.
        let tried = chunk.len().saturating_sub(MAGIC.len() - 1).max(1);
        for at in 0..tried {
            if !chunk[at..].starts_with(&MAGIC) {
                continue;
            }
            let offset = chunk_start + at as u64;
            match read_record(file, offset, end, true).await {
                Ok(_) => return Ok(Some(offset)),
                Err(Stop::Io(error)) => return Err(error),
                Err(Stop::Decode(_)) => {}
            }
        }
        chunk_start += tried as u64;
    }
    Ok(None)
}
