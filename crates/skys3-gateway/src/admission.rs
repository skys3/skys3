//! Admission control (design §7.6, §13): whether a write that adds data may
//! proceed, or must wait with `503 SlowDown`.

use std::fmt;

use s3s::{S3Error, s3_error};
use skys3_types::BucketDocument;

use crate::shard::ShardRef;

/// Why admission control turned a write away. The client is answered
/// `503 SlowDown`, which S3 clients retry with backoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
pub enum Refusal {
    /// The bucket's dirty-data budget (`max_dirty_bytes`) is used up: its
    /// writes wait for the remote target to catch up.
    #[error("the bucket has reached its dirty-data budget; retry as its writes are flushed")]
    BucketBudget,
    /// The cluster's dirty-data budget is used up.
    #[error("the cluster has reached its dirty-data budget; retry as its writes are flushed")]
    ClusterBudget,
    /// The disk that holds the key's shard, or the node's data directory,
    /// is low on space.
    #[error("the node is low on disk space; retry later")]
    DiskSpace,
}

impl Refusal {
    /// A short name for metrics labels: `bucket_budget`, `cluster_budget`,
    /// or `disk_space`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BucketBudget => "bucket_budget",
            Self::ClusterBudget => "cluster_budget",
            Self::DiskSpace => "disk_space",
        }
    }
}

impl From<Refusal> for S3Error {
    fn from(refusal: Refusal) -> Self {
        s3_error!(SlowDown, "{refusal}")
    }
}

/// Decides whether a write that adds data may proceed now.
///
/// The gateway asks before every write that stores bytes or makes a new
/// version of an object: PutObject, CopyObject, the tagging writes, and
/// CreateMultipartUpload, UploadPart, and CompleteMultipartUpload. It asks
/// after the request is authorized and before its body is read, so a
/// refused upload sends no bytes. Deletes and aborts are always admitted:
/// they add no dirty bytes, and they are how a client frees space.
///
/// The node's implementation checks the dirty-data budgets of the bucket
/// and the cluster (`skys3_flush::DirtyBudget`) and the free space of the
/// shard's disk. [`AdmitAll`] admits everything.
pub trait Admission: fmt::Debug + Send + Sync + 'static {
    /// Whether a write that adds data to `shard` of `bucket` may proceed.
    ///
    /// # Errors
    ///
    /// Why not; the client is answered `503 SlowDown`.
    fn admit(&self, bucket: &BucketDocument, shard: &ShardRef) -> Result<(), Refusal>;
}

/// An [`Admission`] that admits every write.
#[derive(Debug, Clone, Copy, Default)]
pub struct AdmitAll;

impl Admission for AdmitAll {
    fn admit(&self, _bucket: &BucketDocument, _shard: &ShardRef) -> Result<(), Refusal> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use s3s::S3ErrorCode;

    use super::*;

    #[test]
    fn refusals_are_slow_downs() {
        for (refusal, name) in [
            (Refusal::BucketBudget, "bucket_budget"),
            (Refusal::ClusterBudget, "cluster_budget"),
            (Refusal::DiskSpace, "disk_space"),
        ] {
            assert_eq!(refusal.as_str(), name);
            let error = S3Error::from(refusal);
            assert_eq!(*error.code(), S3ErrorCode::SlowDown);
            assert_eq!(error.message(), Some(refusal.to_string().as_str()));
        }
    }
}
