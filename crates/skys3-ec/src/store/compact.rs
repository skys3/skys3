//! Compaction of fragment segments (§10.3): the live fragments of a
//! segment that holds little else are copied, byte for byte, to the newest
//! segment, and the segment is removed.
//!
//! A copy keeps its fragment's ID, which names where the fragment was
//! first written, so stripe layouts never change: only the node-local
//! fragment map moves. The steps make a crash at any point lose nothing:
//!
//! 1. Each live fragment's record is read whole and verified, header and
//!    payload, and queued for the committer as a copy. A record that fails
//!    is not copied: the fragment is damaged, which reads already treat as
//!    missing (§8.5), and the map drops it. A damaged record in the newest
//!    segment would make recovery cut that segment's tail.
//! 2. The committer appends the copies with a group commit and syncs them;
//!    only then does each become its fragment's location, and only if the
//!    map still locates the fragment where compaction read it, so a
//!    fragment reclaimed meanwhile stays reclaimed.
//! 3. Once the map locates nothing in the segment, the segment is dropped
//!    from the store and its file removed, and the directory synced.
//!
//! Until step 3 the old record is intact, and after a crash recovery finds
//! both copies; the later segment's, the copy, wins (§8.4). Reads that
//! located a fragment in a removed segment keep reading it through their
//! file handle.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use bytes::Bytes;
use skys3_io::{Disk, SegmentFile};
use tokio::sync::oneshot;

use super::commit::{Record, Reply};
use super::{FragmentError, FragmentStore, Located, segment_name};
use crate::FragmentId;
use crate::fragment::DecodedHeader;

/// What one compaction pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompactionReport {
    /// Segments removed.
    pub segments: usize,
    /// Live fragments copied out of them.
    pub copied: usize,
    /// The bytes of those copies.
    pub copied_bytes: u64,
    /// Fragments dropped because their record failed its checksums.
    pub damaged: usize,
    /// The bytes of the segment files removed: the space reclaimed, less
    /// `copied_bytes`, which the copies take again.
    pub removed_bytes: u64,
}

impl CompactionReport {
    /// The disk space the pass gave back: the segments removed, less the
    /// copies written.
    #[must_use]
    pub fn reclaimed_bytes(&self) -> u64 {
        self.removed_bytes.saturating_sub(self.copied_bytes)
    }
}

impl<D: Disk> FragmentStore<D> {
    /// Compacts every segment other than the last whose live fragments
    /// take less than `live_threshold` of it (`compaction_live_threshold`,
    /// from 0 to 1): copies them to the newest segment and removes the
    /// segment (§10.3). A segment that holds no live fragment is removed
    /// whatever the threshold.
    ///
    /// # Errors
    ///
    /// [`FragmentError::OutOfService`] or [`FragmentError::Closed`] if a
    /// copy was not made durable, [`FragmentError::Read`] if a live record
    /// could not be read, and [`FragmentError::Remove`] if a segment file
    /// could not be removed. Nothing live is lost then: the segment stays
    /// until a later pass, or a later life, compacts it.
    pub async fn compact(&self, live_threshold: f64) -> Result<CompactionReport, FragmentError> {
        self.check_in_service()?;
        let mut report = CompactionReport::default();
        for (segment, len) in self.candidates(live_threshold) {
            self.compact_segment(segment, &mut report).await?;
            if self.remove_segment(segment).await? {
                report.segments += 1;
                report.removed_bytes += len;
            }
        }
        Ok(report)
    }

    /// The segments to compact, with their lengths: every one but the last
    /// whose live bytes are below `live_threshold` of its length, or none.
    fn candidates(&self, live_threshold: f64) -> Vec<(u64, u64)> {
        let mut live: BTreeMap<u64, u64> = BTreeMap::new();
        for located in self.shared.map().values() {
            *live.entry(located.segment).or_default() += located.record_len();
        }
        let segments = self.shared.segments();
        let last = segments.keys().next_back().copied();
        segments
            .iter()
            .filter(|&(&id, _)| Some(id) != last)
            .map(|(&id, file)| (id, file.len()))
            .filter(|&(id, len)| {
                let live = live.get(&id).copied().unwrap_or(0);
                // Lossy for segments past 2^53 bytes, which only shifts
                // the threshold by a rounding error.
                live == 0 || (live as f64) < live_threshold * len as f64
            })
            .collect()
    }

    /// Copies the live fragments of `segment` to the newest segment.
    async fn compact_segment(
        &self,
        segment: u64,
        report: &mut CompactionReport,
    ) -> Result<(), FragmentError> {
        let Some(file) = self.shared.segments().get(&segment).cloned() else {
            return Ok(());
        };
        let mut live: Vec<(FragmentId, Located)> = self
            .shared
            .map()
            .iter()
            .filter(|(_, located)| located.segment == segment)
            .map(|(&id, &located)| (id, located))
            .collect();
        live.sort_by_key(|(_, located)| located.offset);
        // Copies in flight, oldest first, and their bytes.
        let mut queued: VecDeque<(oneshot::Receiver<Reply>, u64)> = VecDeque::new();
        let mut in_flight = 0;
        for (id, located) in live {
            let len = located.record_len();
            let bytes = file
                .read_at(located.offset, len as usize)
                .await
                .map_err(|source| FragmentError::Read { id, source })?;
            if !verified(id, &located, &bytes) {
                tracing::warn!(%id, segment, "compaction drops a damaged fragment");
                self.shared.forget(id, located);
                report.damaged += 1;
                continue;
            }
            let acknowledged = self
                .queue(Record::Copy {
                    id,
                    bytes,
                    from: located,
                })
                .await?;
            queued.push_back((acknowledged, len));
            in_flight += len;
            while in_flight > self.copy_window
                && let Some((acknowledged, len)) = queued.pop_front()
            {
                self.acknowledged(acknowledged).await?;
                in_flight -= len;
                report.copied += 1;
                report.copied_bytes += len;
            }
        }
        if self.bugs.remove_before_copy {
            self.drop_segment(segment).await?;
        }
        for (acknowledged, len) in queued {
            self.acknowledged(acknowledged).await?;
            report.copied += 1;
            report.copied_bytes += len;
        }
        Ok(())
    }

    /// Removes `segment` from the store and the disk, unless the map still
    /// locates a fragment in it or it is the last. Returns whether this
    /// call removed it.
    async fn remove_segment(&self, segment: u64) -> Result<bool, FragmentError> {
        let removed = {
            let map = self.shared.map();
            let mut segments = self.shared.segments();
            let last = segments.keys().next_back().copied();
            let referenced = map.values().any(|located| located.segment == segment);
            if referenced || last == Some(segment) {
                None
            } else {
                self.shared.dead().remove(&segment);
                segments.remove(&segment)
            }
        };
        let Some(file) = removed else {
            return Ok(false);
        };
        self.unlink(segment, Some(file)).await?;
        Ok(true)
    }

    /// Drops `segment` from the store, then removes its file and syncs the
    /// directory.
    async fn drop_segment(&self, segment: u64) -> Result<(), FragmentError> {
        let file = self.shared.segments().remove(&segment);
        self.unlink(segment, file).await
    }

    /// Removes the file of `segment`, which the store no longer lists, and
    /// syncs the directory. If the file cannot be removed, `file` goes back
    /// into the store, so a later pass compacts the segment again instead
    /// of leaving its file on the disk until a restart.
    async fn unlink(&self, segment: u64, file: Option<Arc<D::File>>) -> Result<(), FragmentError> {
        let name = segment_name(segment);
        if let Err(source) = self.disk.remove(&name).await {
            if let Some(file) = file {
                self.shared.restore_segment(segment, file);
            }
            return Err(FragmentError::Remove { name, source });
        }
        self.disk
            .sync_dir()
            .await
            .map_err(|source| FragmentError::Remove { name, source })
    }
}

/// Whether `bytes` is fragment `id`'s whole record as `located` describes
/// it, with a header and payload blocks that verify.
fn verified(id: FragmentId, located: &Located, bytes: &Bytes) -> bool {
    let header_len = located.header_len as usize;
    if bytes.len() as u64 != located.record_len() {
        return false;
    }
    let Ok(decoded) = DecodedHeader::decode(&bytes[..header_len]) else {
        return false;
    };
    decoded.id == id
        && decoded.payload_len == located.payload_len
        && decoded.verify_blocks(0, &bytes[header_len..]).is_ok()
}

/// The store's shared state, as compaction needs it.
impl<F> super::Shared<F> {
    /// Drops fragment `id` from the map if it is still at `located`.
    fn forget(&self, id: FragmentId, located: Located) {
        let mut map = self.map();
        if map.get(&id) == Some(&located) {
            map.remove(&id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_report_counts_the_space_given_back() {
        let report = CompactionReport {
            removed_bytes: 1000,
            copied_bytes: 300,
            ..CompactionReport::default()
        };
        assert_eq!(report.reclaimed_bytes(), 700);
        let report = CompactionReport {
            removed_bytes: 0,
            copied_bytes: 300,
            ..CompactionReport::default()
        };
        assert_eq!(report.reclaimed_bytes(), 0);
    }
}
