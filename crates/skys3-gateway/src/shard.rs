//! The shard interface the gateway calls, and key-to-shard routing (design
//! §3, §4.1).
//!
//! The gateway routes each key to its shard with the frozen hash of
//! [`shard_for_key`] and sends the request to that shard's primary.
//! [`Shards`] is the gateway's view of the shard primaries: on a single
//! node every shard is local, and from M2 on an implementation forwards to
//! the primaries the shard map names. [`LocalShards`](crate::LocalShards)
//! is the single-node implementation.

use std::fmt;
use std::future::Future;

use bytes::Bytes;
use skys3_index::Entry;
use skys3_log::RecordBody;
use skys3_log::record::{Extent, ExtentRef};
use skys3_types::{BucketDocument, BucketId, EpochSeq, ShardId, shard_for_key};

use crate::conditions::{ConditionFailed, Precondition};

/// One shard of one bucket.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShardRef {
    /// The bucket's ID, never its S3 name, so a recreated bucket has new
    /// shards.
    pub bucket: BucketId,
    /// The shard's number within the bucket.
    pub shard: ShardId,
}

impl ShardRef {
    /// The shard that holds `key` in `bucket`: `hash(bucket_id, key) mod
    /// shards` (design §4.1), over the key's bytes exactly as stored.
    ///
    /// ```
    /// use skys3_gateway::ShardRef;
    /// use skys3_types::{BucketId, ShardCount, shard_for_key};
    /// # let bucket: skys3_types::BucketDocument = skys3_types::RegisterDocument::from_json(
    /// #     br#"{"bucket_id":"b-7f3a","name":"photos","mode":"local","shards":8,"replicas":1,
    /// #     "min_write_replicas":1,"clean_copies":0,"created_unix_ms":0,"proposal_id":"p"}"#)?;
    ///
    /// let shard = ShardRef::for_key(&bucket, "photos/cat.jpg");
    /// assert_eq!(shard.bucket, bucket.bucket_id);
    /// assert_eq!(shard.shard.get(), 7);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn for_key(bucket: &BucketDocument, key: &str) -> Self {
        Self {
            bucket: bucket.bucket_id.clone(),
            shard: shard_for_key(&bucket.bucket_id, key.as_bytes(), bucket.shards),
        }
    }

    /// Every shard of `bucket`, in shard order.
    pub fn all(bucket: &BucketDocument) -> impl ExactSizeIterator<Item = Self> + '_ {
        bucket.shards.shards().map(|shard| Self {
            bucket: bucket.bucket_id.clone(),
            shard,
        })
    }
}

impl fmt::Display for ShardRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.bucket, self.shard)
    }
}

/// What a shard holds, as deleting its bucket needs to know.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ShardSummary {
    /// Keys with a live object: every entry except delete tombstones.
    pub objects: u64,
    /// Entries whose latest change has not reached the bucket's target:
    /// dirty, flushing, and conflicted entries, delete tombstones included
    /// (design §4.2). A `local` bucket has no target, so every entry of one
    /// counts.
    pub unflushed: u64,
}

impl ShardSummary {
    /// Adds another shard's counts.
    #[must_use]
    pub const fn add(self, other: Self) -> Self {
        Self {
            objects: self.objects.saturating_add(other.objects),
            unflushed: self.unflushed.saturating_add(other.unflushed),
        }
    }
}

/// The shard primaries, as the gateway sees them.
///
/// Calls go to a shard's current primary, which orders them with the
/// shard's writes. Like `skys3-control`'s `ControlStore`, the methods
/// return `impl Future + Send`, so the trait is used through generics.
pub trait Shards: fmt::Debug + Clone + Send + Sync + 'static {
    /// Creates the shard for `bucket`, which is not yet in the control
    /// store. Opening a shard that is already open for the same bucket
    /// succeeds.
    ///
    /// # Errors
    ///
    /// [`ShardError::Unavailable`] if the shard cannot be created.
    fn open(
        &self,
        shard: &ShardRef,
        bucket: &BucketDocument,
    ) -> impl Future<Output = Result<(), ShardError>> + Send;

    /// Seals the shard and reports what it holds. Once `seal` returns, no
    /// client write commits on the shard until every seal is lifted, and
    /// the summary counts every write committed before it. Seals nest:
    /// each [`Shards::seal`] needs its own [`Shards::unseal`]. Flushing
    /// continues while a shard is sealed.
    ///
    /// # Errors
    ///
    /// [`ShardError::NotFound`] or [`ShardError::Unavailable`]; the shard
    /// is then not sealed.
    fn seal(
        &self,
        shard: &ShardRef,
    ) -> impl Future<Output = Result<ShardSummary, ShardError>> + Send;

    /// Lifts one seal.
    ///
    /// # Errors
    ///
    /// [`ShardError::NotFound`] or [`ShardError::Unavailable`].
    fn unseal(&self, shard: &ShardRef) -> impl Future<Output = Result<(), ShardError>> + Send;

    /// Drops the shard and its local state, once its bucket's register is
    /// deleted. Removing a shard that does not exist succeeds.
    ///
    /// # Errors
    ///
    /// [`ShardError::Unavailable`]; the shard's state is then left for
    /// startup recovery to reclaim.
    fn remove(&self, shard: &ShardRef) -> impl Future<Output = Result<(), ShardError>> + Send;

    /// The entry of `key`: its latest committed version, a delete
    /// tombstone, or `None` (§9.2). Every write acknowledged before the call
    /// is in it.
    ///
    /// # Errors
    ///
    /// [`ShardError::NotFound`] or [`ShardError::Unavailable`].
    fn entry(
        &self,
        shard: &ShardRef,
        key: &str,
    ) -> impl Future<Output = Result<Option<Entry>, ShardError>> + Send;

    /// The payload of the record at `position`, as an entry's
    /// [`Payload`](skys3_index::Payload) names it: an `EXTENT`'s bytes, or
    /// a `PUT`'s inline bytes. A position's payload never changes.
    ///
    /// # Errors
    ///
    /// [`ShardError::NotFound`] or [`ShardError::Unavailable`].
    fn payload(
        &self,
        shard: &ShardRef,
        position: EpochSeq,
    ) -> impl Future<Output = Result<Bytes, ShardError>> + Send;

    /// Commits one extent of a large body (§5.1), and returns the reference
    /// a `PUT` names it by, once it is durable and applied. An extent no
    /// `PUT` references is garbage that compaction reclaims (§10.3).
    ///
    /// # Errors
    ///
    /// [`ShardError::Sealed`], and otherwise as [`Shards::entry`].
    fn append_extent(
        &self,
        shard: &ShardRef,
        extent: Extent,
    ) -> impl Future<Output = Result<ExtentRef, ShardError>> + Send;

    /// Commits `body`, a record that names a key (a `PUT`, a `DELETE`, or a
    /// `FLUSHED`), if `condition` holds of the key's entry when the record
    /// is sequenced, and returns the record's position once it is durable
    /// and applied. The condition sees every write of the key sequenced
    /// before the record, acknowledged or not, so conditional writes are
    /// linearizable. A `PUT` that references extents is accepted only once
    /// every one is applied.
    ///
    /// # Errors
    ///
    /// The outer error as [`Shards::append_extent`], and
    /// [`ShardError::Invalid`] for a record the shard refuses; the inner one
    /// says why `condition` did not hold, and then nothing was written.
    fn write(
        &self,
        shard: &ShardRef,
        body: RecordBody,
        condition: Precondition,
    ) -> impl Future<Output = Result<Result<EpochSeq, ConditionFailed>, ShardError>> + Send;
}

/// A shard request that failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ShardError {
    /// The shard does not exist.
    #[error("shard {0} does not exist")]
    NotFound(ShardRef),
    /// The shard is sealed while its bucket is being deleted, and refuses
    /// client writes.
    #[error("shard {0} is sealed while its bucket is deleted")]
    Sealed(ShardRef),
    /// The shard's primary could not serve the request.
    #[error("shard {shard} is unavailable: {reason}")]
    Unavailable {
        /// The shard.
        shard: ShardRef,
        /// Why.
        reason: String,
    },
    /// The shard refused a record that breaks a rule of the write path,
    /// which the gateway should have caught.
    #[error("shard {shard} refused the record: {reason}")]
    Invalid {
        /// The shard.
        shard: ShardRef,
        /// Why.
        reason: String,
    },
}

impl From<&ShardRef> for skys3_log::ShardRef {
    fn from(shard: &ShardRef) -> Self {
        Self::new(shard.bucket.clone(), shard.shard)
    }
}

#[cfg(test)]
mod tests {
    use skys3_types::{BucketMode, ProposalId, ShardCount};

    use super::*;

    pub(crate) fn bucket(id: &str, shards: u32) -> BucketDocument {
        BucketDocument {
            bucket_id: BucketId::new(id).unwrap(),
            name: "photos".parse().unwrap(),
            mode: BucketMode::Local,
            shards: ShardCount::new(shards).unwrap(),
            replicas: 1,
            min_write_replicas: 1,
            clean_copies: 0,
            target: None,
            created_unix_ms: 0,
            proposal_id: ProposalId::new("p").unwrap(),
        }
    }

    #[test]
    fn keys_route_with_the_frozen_hash() {
        let doc = bucket("b-7f3a", 8);
        let shard = ShardRef::for_key(&doc, "photos/cat.jpg");
        assert_eq!(shard.to_string(), "b-7f3a/7");
        for key in ["", "a", "photos/cat.jpg", "日本語/キー"] {
            let shard = ShardRef::for_key(&doc, key);
            assert_eq!(
                shard.shard,
                shard_for_key(&doc.bucket_id, key.as_bytes(), doc.shards)
            );
        }
        // A recreated bucket has a new ID, and so routes keys afresh.
        let other = bucket("b-other", 8);
        let moved = (0..64)
            .map(|n| format!("key-{n}"))
            .filter(|key| {
                ShardRef::for_key(&doc, key).shard != ShardRef::for_key(&other, key).shard
            })
            .count();
        assert!(moved > 0);
    }

    #[test]
    fn every_shard_of_a_bucket_is_listed() {
        let doc = bucket("b-1", 3);
        let all: Vec<_> = ShardRef::all(&doc).map(|s| s.to_string()).collect();
        assert_eq!(all, ["b-1/0", "b-1/1", "b-1/2"]);
        assert_eq!(ShardRef::all(&bucket("b-1", 256)).len(), 256);
    }

    #[test]
    fn summaries_add_without_overflow() {
        let one = ShardSummary {
            objects: 2,
            unflushed: 1,
        };
        assert_eq!(
            one.add(one),
            ShardSummary {
                objects: 4,
                unflushed: 2
            }
        );
        let full = ShardSummary {
            objects: u64::MAX,
            unflushed: u64::MAX,
        };
        assert_eq!(full.add(one), full);
    }

    #[test]
    fn errors_name_the_shard() {
        let shard = ShardRef::for_key(&bucket("b-1", 1), "k");
        assert_eq!(
            ShardError::NotFound(shard.clone()).to_string(),
            "shard b-1/0 does not exist"
        );
        let error = ShardError::Unavailable {
            shard,
            reason: "disk out of service".into(),
        };
        assert_eq!(
            error.to_string(),
            "shard b-1/0 is unavailable: disk out of service"
        );
    }
}
