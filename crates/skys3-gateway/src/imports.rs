//! What the gateway knows of each shard's namespace import (§9.1), which
//! decides whether a read of a key with no entry falls through to the
//! bucket's remote.
//!
//! The authority is the key's own shard: its primary applies each
//! `IMPORT_PROGRESS`, refuses an `IMPORT` of a key its progress has passed,
//! and only removes a delete tombstone once its progress has passed the
//! key. A key the shard has passed has no object at the remote that the
//! index does not hold, whatever the remote holds there now; a key it has
//! not passed may still be imported, so the remote's object is the key's.
//!
//! Progress only grows, so what the gateway learned once that a shard has
//! passed stays true: such a key needs no question. Whether a key is not
//! passed is asked of the shard's primary each time, after the entry was
//! read, and an answer that cannot be had means no read-through.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use s3s::{S3Error, s3_error};
use skys3_index::{ImportCheckpoint, ImportRanges};
use skys3_types::{BucketDocument, BucketMode};

use crate::remote::RemoteReads;
use crate::shard::{ShardError, ShardRef, Shards};

/// `remote`, if `bucket` is a `write_back` bucket whose target it reads.
pub(crate) fn remote_of<'a>(
    remote: Option<&'a Arc<dyn RemoteReads>>,
    bucket: &BucketDocument,
) -> Option<&'a Arc<dyn RemoteReads>> {
    remote.filter(|remote| bucket.mode == BucketMode::WriteBack && remote.reads(&bucket.bucket_id))
}

/// A read that needs to know whether a shard's import has passed a key,
/// which its primary could not say. Without it, nothing is read through
/// to the remote, which may hold an object at a key the index dropped.
pub(crate) fn progress_unknown(error: &ShardError) -> S3Error {
    s3_error!(
        ServiceUnavailable,
        "A shard's namespace import progress cannot be read: {error}"
    )
}

/// The newest import progress the gateway learned from each shard, shared
/// by the gateway's object and listing operations. It holds one entry per
/// shard of the `write_back` buckets the gateway read through, and is
/// only ever replaced by newer progress.
#[derive(Debug, Clone, Default)]
pub(crate) struct ImportProgress {
    known: Arc<Mutex<HashMap<ShardRef, (u64, ImportRanges)>>>,
}

impl ImportProgress {
    fn lock(&self) -> MutexGuard<'_, HashMap<ShardRef, (u64, ImportRanges)>> {
        self.known.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether progress learned before from `shard` has passed `key`. A
    /// yes stays true; a no may be stale.
    pub(crate) fn known_passed(&self, shard: &ShardRef, key: &str) -> bool {
        self.lock()
            .get(shard)
            .is_some_and(|(_, ranges)| ranges.passed(key))
    }

    /// Asks `shard`'s primary for its progress, remembers it if it is
    /// newer than what is known, and returns the newest known: `None` if
    /// the shard applied none, so that it has passed nothing.
    ///
    /// # Errors
    ///
    /// As [`Shards::import_progress`].
    pub(crate) async fn fetch<H: Shards>(
        &self,
        shards: &H,
        shard: &ShardRef,
    ) -> Result<Option<ImportRanges>, ShardError> {
        let fetched = shards.import_progress(shard).await?;
        let mut known = self.lock();
        if let Some((update, ranges)) = fetched
            && known.get(shard).is_none_or(|(newest, _)| *newest < update)
        {
            known.insert(shard.clone(), (update, ranges));
        }
        Ok(known.get(shard).map(|(_, ranges)| ranges.clone()))
    }

    /// Whether `shard` has passed `key` now: as known, or else as its
    /// primary answers.
    ///
    /// # Errors
    ///
    /// As [`Shards::import_progress`].
    pub(crate) async fn passed<H: Shards>(
        &self,
        shards: &H,
        shard: &ShardRef,
        key: &str,
    ) -> Result<bool, ShardError> {
        if self.known_passed(shard, key) {
            return Ok(true);
        }
        let ranges = self.fetch(shards, shard).await?;
        Ok(ranges.is_some_and(|ranges| ranges.passed(key)))
    }

    /// How far every shard of `bucket` has got without a gap: `Done` once
    /// all are done, which is known for good, and otherwise the earliest
    /// position among them. Asks each shard not known to be done.
    ///
    /// # Errors
    ///
    /// The first error of [`Shards::import_progress`].
    pub(crate) async fn bucket<H: Shards>(
        &self,
        shards: &H,
        bucket: &BucketDocument,
    ) -> Result<ImportCheckpoint, ShardError> {
        let mut earliest: Option<Option<String>> = None;
        for shard in ShardRef::all(bucket) {
            let known = self.lock().get(&shard).map(|(_, ranges)| ranges.clone());
            let ranges = match known {
                Some(ranges) if ranges.is_done() => continue,
                _ => self.fetch(shards, &shard).await?.unwrap_or_default(),
            };
            if let ImportCheckpoint::Running { after } = ranges.position() {
                // `None`, nothing passed, sorts first.
                earliest = Some(earliest.map_or(after.clone(), |earliest| earliest.min(after)));
            }
        }
        Ok(match earliest {
            Some(after) => ImportCheckpoint::Running { after },
            None => ImportCheckpoint::Done,
        })
    }
}

#[cfg(test)]
mod tests {
    use skys3_types::{BucketId, ShardId};

    use super::*;

    fn running(after: &str) -> ImportRanges {
        ImportCheckpoint::Running {
            after: Some(after.to_owned()),
        }
        .into()
    }

    #[test]
    fn only_what_progress_has_passed_is_known() {
        let progress = ImportProgress::default();
        let shard = ShardRef {
            bucket: BucketId::new("b-1").unwrap(),
            shard: ShardId::new(0),
        };
        assert!(!progress.known_passed(&shard, "a"));
        progress.lock().insert(shard.clone(), (2, running("m")));
        assert!(progress.known_passed(&shard, "a"));
        assert!(progress.known_passed(&shard, "m"));
        assert!(!progress.known_passed(&shard, "n"));
    }
}
