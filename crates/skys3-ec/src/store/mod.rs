//! The fragment store: one disk's fragment segments (design §8.4, §10.1).
//!
//! Fragment segments are the third segment class of §10.1, beside the log's
//! hot and bulk segments, and share the disk's directory with them. They
//! hold fragment records ([`fragment`](crate::fragment)) instead of log
//! records, so the store, not the log, owns them:
//!
//! - A fragment segment file is named `frag-<id>.seg`, with the id as 16
//!   lowercase hexadecimal digits. Fragment segments have their own id
//!   counter. The log's recovery ignores these files, and the log never
//!   lists them, so log compaction (§10.3) never touches them; the store
//!   ignores the log's files likewise.
//! - [`FragmentStore::write`] appends a fragment with a group commit, syncs
//!   it, and returns its [`FragmentId`] only once the sync covers it (and a
//!   directory sync covers a new segment file). The ID is the disk's number,
//!   the segment, and the offset where the fragment was first written: an
//!   acknowledged ID is never assigned again, because segment numbers only
//!   grow and the last segment is never removed.
//! - The node-local fragment map, from ID to location, lives in memory. It
//!   is rebuilt at open from the segments' headers, which recovery reads
//!   anyway, so it needs no index table and no format change. Compaction of
//!   fragment segments (M5-07) moves fragments and updates only this map.
//! - [`FragmentStore::read`] reads a range of a fragment and verifies it
//!   against the block checksums in the fragment's header.
//!
//! All I/O goes through the [`Disk`], whose real implementation runs it on
//! the disk's blocking workers. The first I/O error on the write path takes
//! the store out of service, as it does the log (§10.4).

mod commit;
mod recovery;

use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::ops::Range;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use bytes::Bytes;
use skys3_config::StorageConfig;
use skys3_io::{Disk, SegmentFile};
use tokio::sync::{mpsc, oneshot};

use crate::FragmentId;
use crate::fragment::{
    BLOCK_LEN, DecodedHeader, FragmentDecodeError, FragmentEncodeError, FragmentHeader,
    MAX_FRAGMENT_LEN, MAX_HEADER_LEN, encode_tail,
};
use commit::{Committer, Request};
pub use recovery::{RecoveryError, RecoveryReport, TornTail};

/// How many fragments may wait for the committer before writers wait to
/// queue more.
const QUEUE_LEN: usize = 64;

/// The file name prefix of fragment segments.
const PREFIX: &str = "frag-";

/// The file name suffix of fragment segments.
const SUFFIX: &str = ".seg";

/// How a fragment store sizes segments and group commits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FragmentStoreConfig {
    /// The disk's number on its node, from 0: its position among the
    /// node's disks in label order (§10.2). It is part of every fragment ID
    /// the store assigns, so IDs are unique across the node's disks, and a
    /// disk must always be opened with the same number.
    pub disk: u8,
    /// `segment_bytes`: the size at which the store starts a new segment.
    /// A segment exceeds it only when one group commit does.
    pub segment_bytes: u64,
    /// `group_commit_max_bytes`: a group commit takes no more fragments
    /// once it holds this many bytes.
    pub group_commit_max_bytes: u64,
    /// The longest fragment the store accepts, at most
    /// [`MAX_FRAGMENT_LEN`]. Recovery's tear window counts one fragment of
    /// this length, so lowering it while the node is down can make a torn
    /// tail look like damage, never the reverse.
    pub max_fragment_bytes: u64,
}

impl FragmentStoreConfig {
    /// The settings for disk number `disk` from the `[storage]` section,
    /// which sizes the log's segments and group commits too.
    #[must_use]
    pub fn from_storage(disk: u8, storage: &StorageConfig) -> Self {
        Self {
            disk,
            segment_bytes: storage.segment_bytes,
            group_commit_max_bytes: storage.group_commit_max_bytes,
            max_fragment_bytes: MAX_FRAGMENT_LEN,
        }
    }

    /// The most bytes a crash can leave unsynced at the end of the last
    /// segment: one group commit, which holds less than
    /// `group_commit_max_bytes` plus one record of the largest size.
    #[must_use]
    pub fn tear_window(&self) -> u64 {
        self.group_commit_max_bytes
            .saturating_add(self.max_fragment_bytes.min(MAX_FRAGMENT_LEN))
            .saturating_add(MAX_HEADER_LEN.into())
    }
}

/// Why the store could not write or read a fragment.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FragmentError {
    /// An I/O error took the disk out of service. Nothing is acknowledged
    /// after it; the disk needs recovery after a host restart (§10.4).
    #[error("the fragment store's disk is out of service: {0}")]
    OutOfService(#[source] Arc<io::Error>),
    /// The store's committer is gone, because its runtime shut down.
    #[error("the fragment store is closed")]
    Closed,
    /// The fragment's header or payload breaks the format.
    #[error(transparent)]
    Encode(#[from] FragmentEncodeError),
    /// The fragment is longer than the store's `max_fragment_bytes`.
    #[error("a fragment of {len} bytes exceeds the store's limit of {max}")]
    TooLarge {
        /// The fragment's length.
        len: u64,
        /// The limit.
        max: u64,
    },
    /// The store holds no fragment with this ID.
    #[error("no fragment {0}")]
    UnknownFragment(FragmentId),
    /// The range does not lie within the fragment.
    #[error("range {range:?} is outside fragment {id} of {len} bytes")]
    OutOfRange {
        /// The fragment.
        id: FragmentId,
        /// The range asked for.
        range: Range<u64>,
        /// The fragment's length.
        len: u64,
    },
    /// Reading the fragment failed.
    #[error("cannot read fragment {id}: {source}")]
    Read {
        /// The fragment.
        id: FragmentId,
        /// The read error.
        source: io::Error,
    },
    /// The fragment's header or the payload range read fails its checksum
    /// or breaks the format: the fragment is damaged, and a reader treats
    /// it as missing (§8.5).
    #[error("fragment {id} is damaged: {source}")]
    Damaged {
        /// The fragment.
        id: FragmentId,
        /// What is wrong with it.
        source: FragmentDecodeError,
    },
    /// A segment whose fragments were all reclaimed could not be removed.
    #[error("cannot remove fragment segment {name}: {source}")]
    Remove {
        /// The segment's file name.
        name: String,
        /// The error.
        source: io::Error,
    },
}

/// A fragment segment: its number and length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FragmentSegment {
    /// The segment's number on its disk.
    pub id: u64,
    /// Its length in bytes, every fragment written or recovered included.
    pub len: u64,
}

/// A range of a fragment, verified against its block checksums.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FragmentRange {
    /// The fragment's header.
    pub header: FragmentHeader,
    /// The bytes of the range.
    pub data: Bytes,
    /// The CRC32C of `data`, computed from the verified bytes, so a reader
    /// on another node can check them after the transfer.
    pub crc32c: u32,
}

/// Where a fragment is: its segment, offset, and lengths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Located {
    pub(crate) segment: u64,
    pub(crate) offset: u64,
    pub(crate) header_len: u32,
    pub(crate) payload_len: u64,
}

impl Located {
    fn record_len(&self) -> u64 {
        u64::from(self.header_len) + self.payload_len
    }
}

/// Bugs seeded for the crash simulation to catch (test support).
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Bugs {
    pub(crate) acknowledge_before_sync: bool,
    pub(crate) skip_directory_sync: bool,
    pub(crate) skip_recovery_sync: bool,
}

/// A bug to seed in a store, for tests that show the crash simulation
/// catches it.
#[cfg(feature = "test-util")]
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeededBug {
    /// Acknowledge a group's fragments once written, before the sync.
    AcknowledgeBeforeSync,
    /// Never sync the directory after creating a segment file.
    SkipDirectorySync,
    /// Skip recovery's syncs of the last segment and the directory.
    SkipRecoverySync,
}

/// State shared by the store's handles and its committer.
pub(crate) struct Shared<F> {
    segments: Mutex<BTreeMap<u64, Arc<F>>>,
    map: Mutex<BTreeMap<FragmentId, Located>>,
    /// The bytes of reclaimed records in each segment, in this life.
    dead: Mutex<BTreeMap<u64, u64>>,
    failure: OnceLock<Arc<io::Error>>,
}

impl<F> Shared<F> {
    fn segments(&self) -> MutexGuard<'_, BTreeMap<u64, Arc<F>>> {
        // Plain inserts and lookups, which a panic cannot leave half done.
        self.segments.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn map(&self) -> MutexGuard<'_, BTreeMap<FragmentId, Located>> {
        // As for `segments`.
        self.map.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn dead(&self) -> MutexGuard<'_, BTreeMap<u64, u64>> {
        // As for `segments`.
        self.dead.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn add_segment(&self, id: u64, file: Arc<F>) {
        self.segments().insert(id, file);
    }

    pub(crate) fn last_segment(&self) -> Option<(u64, Arc<F>)> {
        self.segments()
            .last_key_value()
            .map(|(&id, file)| (id, Arc::clone(file)))
    }

    /// Records where fragments are. A later record of an ID replaces an
    /// earlier one: a copy compaction made.
    pub(crate) fn insert(&self, fragments: impl IntoIterator<Item = (FragmentId, Located)>) {
        self.map().extend(fragments);
    }

    pub(crate) fn take_out_of_service(&self, error: io::Error) -> Arc<io::Error> {
        Arc::clone(self.failure.get_or_init(|| Arc::new(error)))
    }
}

/// The fragment store of one disk (design §8.4).
///
/// [`FragmentStore::open`] recovers the disk's fragment segments, rebuilds
/// the fragment map, and starts the store's committer, a task on the
/// current Tokio runtime. Cloning a store returns another handle to it; the
/// committer stops once every handle is dropped.
pub struct FragmentStore<D: Disk> {
    shared: Arc<Shared<D::File>>,
    disk: Arc<D>,
    requests: mpsc::Sender<Request>,
    max_fragment_bytes: u64,
}

impl<D: Disk> Clone for FragmentStore<D> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            disk: Arc::clone(&self.disk),
            requests: self.requests.clone(),
            max_fragment_bytes: self.max_fragment_bytes,
        }
    }
}

impl<D: Disk> fmt::Debug for FragmentStore<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FragmentStore")
            .field("fragments", &self.shared.map().len())
            .field("failure", &self.shared.failure.get())
            .finish_non_exhaustive()
    }
}

impl<D: Disk> FragmentStore<D> {
    /// Recovers the fragment segments on `disk` and starts the store's
    /// committer on the current Tokio runtime.
    ///
    /// # Errors
    ///
    /// A [`RecoveryError`] if recovery fails or refuses to start; see
    /// [`RecoveryError`] for when it refuses.
    ///
    /// # Panics
    ///
    /// Panics if called outside a Tokio runtime.
    pub async fn open(
        disk: D,
        config: FragmentStoreConfig,
    ) -> Result<(Self, RecoveryReport), RecoveryError> {
        Self::open_with(disk, config, Bugs::default()).await
    }

    /// Opens the store as [`FragmentStore::open`] does, with `bug` seeded.
    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    pub async fn open_with_bug(
        disk: D,
        config: FragmentStoreConfig,
        bug: SeededBug,
    ) -> Result<(Self, RecoveryReport), RecoveryError> {
        let bugs = Bugs {
            acknowledge_before_sync: bug == SeededBug::AcknowledgeBeforeSync,
            skip_directory_sync: bug == SeededBug::SkipDirectorySync,
            skip_recovery_sync: bug == SeededBug::SkipRecoverySync,
        };
        Self::open_with(disk, config, bugs).await
    }

    async fn open_with(
        disk: D,
        config: FragmentStoreConfig,
        bugs: Bugs,
    ) -> Result<(Self, RecoveryReport), RecoveryError> {
        let recovered = recovery::recover(&disk, &config, bugs).await?;
        let shared = Arc::new(Shared {
            segments: Mutex::new(recovered.segments),
            map: Mutex::default(),
            dead: Mutex::default(),
            failure: OnceLock::new(),
        });
        shared.insert(recovered.fragments);
        let (requests, receiver) = mpsc::channel(QUEUE_LEN);
        let max_fragment_bytes = config.max_fragment_bytes.min(MAX_FRAGMENT_LEN);
        let disk = Arc::new(disk);
        let committer = Committer::new(
            Arc::clone(&disk),
            Arc::clone(&shared),
            receiver,
            config,
            recovered.next_segment,
            bugs,
        );
        tokio::spawn(committer.run());
        let store = Self {
            shared,
            disk,
            requests,
            max_fragment_bytes,
        };
        Ok((store, recovered.report))
    }

    /// Writes a fragment and returns its ID once it is durable.
    ///
    /// The header and the block checksums are encoded on the calling task.
    /// Dropping the returned future does not withdraw a fragment already
    /// queued: it may still become durable, as an orphan nothing references
    /// (§8.4).
    ///
    /// # Errors
    ///
    /// [`FragmentError::Encode`] if the header breaks the format or the
    /// payload's length is not the stripe's fragment length,
    /// [`FragmentError::TooLarge`] for a payload over `max_fragment_bytes`,
    /// and
    /// [`FragmentError::OutOfService`] or [`FragmentError::Closed`] if the
    /// fragment was not made durable. A fragment that fails this way is
    /// never acknowledged.
    pub async fn write(
        &self,
        header: &FragmentHeader,
        payload: Bytes,
    ) -> Result<FragmentId, FragmentError> {
        let len = payload.len() as u64;
        if len > self.max_fragment_bytes {
            return Err(FragmentError::TooLarge {
                len,
                max: self.max_fragment_bytes,
            });
        }
        let tail = encode_tail(header, &payload).map_err(FragmentEncodeError)?;
        self.check_in_service()?;
        let (reply, acknowledged) = oneshot::channel();
        let request = Request {
            tail,
            payload,
            reply,
        };
        if self.requests.send(request).await.is_err() {
            return Err(self.closed());
        }
        match acknowledged.await {
            Ok(Ok(id)) => Ok(id),
            Ok(Err(error)) => Err(FragmentError::OutOfService(error)),
            Err(_) => Err(self.closed()),
        }
    }

    /// Reads bytes `range` of fragment `id`, verified against the checksums
    /// of the blocks it covers, with the fragment's header.
    ///
    /// # Errors
    ///
    /// [`FragmentError::UnknownFragment`], [`FragmentError::OutOfRange`],
    /// [`FragmentError::Read`], [`FragmentError::Damaged`] if the header or
    /// a block fails its checksum, and [`FragmentError::OutOfService`] once
    /// the disk is out of service.
    pub async fn read(
        &self,
        id: FragmentId,
        range: Range<u64>,
    ) -> Result<FragmentRange, FragmentError> {
        let (located, file) = self.locate(id)?;
        if range.start > range.end || range.end > located.payload_len {
            return Err(FragmentError::OutOfRange {
                id,
                range,
                len: located.payload_len,
            });
        }
        let decoded = Self::read_header(id, located, &file).await?;
        if range.is_empty() {
            return Ok(FragmentRange {
                header: decoded.header,
                data: Bytes::new(),
                crc32c: 0,
            });
        }
        let first = range.start / BLOCK_LEN;
        let start = first * BLOCK_LEN;
        let end = range.end.div_ceil(BLOCK_LEN) * BLOCK_LEN;
        let end = end.min(located.payload_len);
        let at = located.offset + u64::from(located.header_len) + start;
        let blocks = file
            .read_at(at, (end - start) as usize)
            .await
            .map_err(|source| FragmentError::Read { id, source })?;
        decoded
            .verify_blocks(first, &blocks)
            .map_err(|source| FragmentError::Damaged { id, source })?;
        let data = blocks.slice((range.start - start) as usize..(range.end - start) as usize);
        Ok(FragmentRange {
            header: decoded.header,
            crc32c: crc32c::crc32c(&data),
            data,
        })
    }

    /// Reads fragment `id`'s header, verified against its checksum.
    ///
    /// # Errors
    ///
    /// As [`FragmentStore::read`].
    pub async fn header(&self, id: FragmentId) -> Result<FragmentHeader, FragmentError> {
        let (located, file) = self.locate(id)?;
        Ok(Self::read_header(id, located, &file).await?.header)
    }

    /// Reclaims fragment `id`, an orphan (§8.4) or, later, a released
    /// fragment (§8.7): it is no longer read or listed, and once every
    /// record of its segment is reclaimed and the segment is not the last,
    /// the segment file is removed. Returns whether the store held the
    /// fragment.
    ///
    /// Reclaiming is not recorded on the disk. A fragment whose segment
    /// survives a restart is found again by recovery, and is reclaimed
    /// again once its node asks about it; segments that hold live
    /// fragments too wait for fragment-segment compaction (plan M5-07).
    ///
    /// # Errors
    ///
    /// [`FragmentError::Remove`] if a segment file could not be removed;
    /// the fragment is reclaimed all the same, and the file stays until
    /// the next life finds it again.
    pub async fn reclaim(&self, id: FragmentId) -> Result<bool, FragmentError> {
        let Some(located) = self.shared.map().remove(&id) else {
            return Ok(false);
        };
        let removable = {
            let mut segments = self.shared.segments();
            let mut dead = self.shared.dead();
            let bytes = dead.entry(located.segment).or_default();
            *bytes += located.record_len();
            let last = segments.keys().next_back().copied();
            let whole = segments
                .get(&located.segment)
                .is_some_and(|file| file.len() <= *bytes);
            // Only the committer appends, and only to the last segment.
            let removable = whole && last != Some(located.segment);
            if removable {
                dead.remove(&located.segment);
                segments.remove(&located.segment);
            }
            removable
        };
        if removable {
            let name = segment_name(located.segment);
            self.disk
                .remove(&name)
                .await
                .map_err(|source| FragmentError::Remove { name, source })?;
        }
        Ok(true)
    }

    /// The length of fragment `id`'s payload, or `None` if the store holds
    /// no such fragment.
    #[must_use]
    pub fn len(&self, id: FragmentId) -> Option<u64> {
        self.shared
            .map()
            .get(&id)
            .map(|located| located.payload_len)
    }

    /// The IDs of every fragment the store holds, in order.
    #[must_use]
    pub fn ids(&self) -> Vec<FragmentId> {
        self.shared.map().keys().copied().collect()
    }

    /// The store's segments, in number order.
    #[must_use]
    pub fn segments(&self) -> Vec<FragmentSegment> {
        self.shared
            .segments()
            .iter()
            .map(|(&id, file)| FragmentSegment {
                id,
                len: file.len(),
            })
            .collect()
    }

    /// Whether the disk is in service: no I/O error has occurred on the
    /// write path.
    #[must_use]
    pub fn is_in_service(&self) -> bool {
        self.shared.failure.get().is_none()
    }

    fn locate(&self, id: FragmentId) -> Result<(Located, Arc<D::File>), FragmentError> {
        self.check_in_service()?;
        let located = self
            .shared
            .map()
            .get(&id)
            .copied()
            .ok_or(FragmentError::UnknownFragment(id))?;
        let file = self
            .shared
            .segments()
            .get(&located.segment)
            .cloned()
            .ok_or(FragmentError::UnknownFragment(id))?;
        Ok((located, file))
    }

    async fn read_header(
        id: FragmentId,
        located: Located,
        file: &D::File,
    ) -> Result<DecodedHeader, FragmentError> {
        let bytes = file
            .read_at(located.offset, located.header_len as usize)
            .await
            .map_err(|source| FragmentError::Read { id, source })?;
        let damaged = |source| FragmentError::Damaged { id, source };
        let decoded = DecodedHeader::decode(&bytes).map_err(damaged)?;
        if decoded.id != id || decoded.payload_len != located.payload_len {
            let mismatch = crate::fragment::inconsistent(
                "id",
                "the record at the fragment's location is another fragment",
            );
            return Err(damaged(mismatch.into()));
        }
        Ok(decoded)
    }

    fn check_in_service(&self) -> Result<(), FragmentError> {
        match self.shared.failure.get() {
            Some(error) => Err(FragmentError::OutOfService(Arc::clone(error))),
            None => Ok(()),
        }
    }

    fn closed(&self) -> FragmentError {
        self.shared
            .failure
            .get()
            .map_or(FragmentError::Closed, |error| {
                FragmentError::OutOfService(Arc::clone(error))
            })
    }
}

/// The file name of fragment segment `id`.
pub(crate) fn segment_name(id: u64) -> String {
    format!("{PREFIX}{id:016x}{SUFFIX}")
}

/// Parses a fragment segment's file name, as [`segment_name`] writes it.
pub(crate) fn parse_segment_name(name: &str) -> Option<u64> {
    let digits = name.strip_prefix(PREFIX)?.strip_suffix(SUFFIX)?;
    let canonical = digits.len() == 16
        && digits
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
    canonical.then(|| u64::from_str_radix(digits, 16).ok())?
}

/// The ID of the fragment first written to disk `disk`, segment `segment`,
/// at `offset`: the disk in the top byte, the segment in the next 56 bits,
/// and the offset in the low 64.
pub(crate) fn compose_id(disk: u8, segment: u64, offset: u64) -> FragmentId {
    FragmentId::new(u128::from(disk) << 120 | u128::from(segment) << 64 | u128::from(offset))
}

/// The disk number in fragment ID `id`.
pub(crate) fn disk_of(id: FragmentId) -> u8 {
    (id.get() >> 120) as u8
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_names_round_trip() {
        for id in [0, 1, 0x2a, u64::MAX] {
            assert_eq!(parse_segment_name(&segment_name(id)), Some(id));
        }
        assert_eq!(segment_name(42), "frag-000000000000002a.seg");
        for name in [
            "hot-000000000000002a.seg",
            "frag-000000000000002A.seg",
            "frag-2a.seg",
            "frag-000000000000002a.log",
            "frag-+00000000000002a.seg",
            "disk.json",
        ] {
            assert_eq!(parse_segment_name(name), None, "{name}");
        }
    }

    #[test]
    fn ids_carry_the_disk_segment_and_offset() {
        let id = compose_id(3, (1 << 56) - 1, u64::MAX);
        assert_eq!(disk_of(id), 3);
        assert_eq!(id.get() >> 64 & ((1 << 56) - 1), (1 << 56) - 1);
        assert_eq!(id.get() as u64, u64::MAX);
        assert_ne!(compose_id(0, 1, 0), compose_id(1, 1, 0));
        assert_ne!(compose_id(0, 1, 0), compose_id(0, 0, 1));
    }

    #[test]
    fn the_tear_window_covers_a_group_and_a_record() {
        let config = FragmentStoreConfig::from_storage(2, &StorageConfig::default());
        assert_eq!(config.disk, 2);
        assert_eq!(
            config.tear_window(),
            config.group_commit_max_bytes + MAX_FRAGMENT_LEN + u64::from(MAX_HEADER_LEN)
        );
        let small = FragmentStoreConfig {
            max_fragment_bytes: 4096,
            ..config
        };
        assert_eq!(
            small.tear_window(),
            small.group_commit_max_bytes + 4096 + u64::from(MAX_HEADER_LEN)
        );
    }
}
