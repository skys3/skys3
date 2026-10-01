//! The shards of a single node: [`Shards`] over the node's
//! [`ShardSet`], where every shard is local and this node its only member
//! (plan M1-04, M1-09).

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use skys3_index::{Entry, ListPage, ListQuery, Part, Upload};
use skys3_io::Disk;
use skys3_log::RecordBody;
use skys3_log::record::{Extent, ExtentRef};
use skys3_shard::{Outcome, Rejection, Shard, ShardSet};
use skys3_types::{BucketDocument, Epoch, EpochSeq, NodeId, ShardConfig};

use crate::conditions::{ConditionFailed, Precondition};
use crate::shard::{ShardError, ShardRef, ShardSummary, Shards, UploadParts};

/// The node's shard replicas, as the gateway calls them.
///
/// A bucket's shards open in epoch 1 with this node as their only member,
/// and so their primary. Replication (plan M2) gives shards configurations
/// from the shard map instead; until then every shard has one copy, whatever
/// the bucket's `replicas`.
pub struct LocalShards<D: Disk> {
    set: Arc<ShardSet<D>>,
    node: NodeId,
}

impl<D: Disk> Clone for LocalShards<D> {
    fn clone(&self) -> Self {
        Self {
            set: Arc::clone(&self.set),
            node: self.node.clone(),
        }
    }
}

impl<D: Disk> fmt::Debug for LocalShards<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalShards")
            .field("node", &self.node)
            .finish_non_exhaustive()
    }
}

impl<D: Disk> LocalShards<D> {
    /// The shards of `set`, on the node `node`.
    #[must_use]
    pub fn new(set: ShardSet<D>, node: NodeId) -> Self {
        Self {
            set: Arc::new(set),
            node,
        }
    }

    /// The node's shard set.
    #[must_use]
    pub fn set(&self) -> &ShardSet<D> {
        &self.set
    }

    /// The configuration a shard of `bucket` opens in.
    #[must_use]
    pub fn config(&self, shard: &ShardRef, bucket: &BucketDocument) -> ShardConfig {
        ShardConfig {
            bucket_id: shard.bucket.clone(),
            shard: shard.shard,
            epoch: Epoch::new(1),
            primary: self.node.clone(),
            members: vec![self.node.clone()],
            learners: Vec::new(),
            min_write_replicas: 1,
            replicas: 1,
            proposal_id: bucket.proposal_id.clone(),
        }
    }

    async fn find(&self, shard: &ShardRef) -> Result<Shard<D>, ShardError> {
        self.set
            .get(&shard.into())
            .await
            .ok_or_else(|| ShardError::NotFound(shard.clone()))
    }
}

/// The gateway's view of a shard error.
fn convert(shard: &ShardRef, error: skys3_shard::ShardError) -> ShardError {
    match error {
        skys3_shard::ShardError::NotFound(_) => ShardError::NotFound(shard.clone()),
        skys3_shard::ShardError::Sealed(_) => ShardError::Sealed(shard.clone()),
        skys3_shard::ShardError::InvalidRecord { reason, .. } => ShardError::Invalid {
            shard: shard.clone(),
            reason,
        },
        other => ShardError::Unavailable {
            shard: shard.clone(),
            reason: other.to_string(),
        },
    }
}

impl<D: Disk> Shards for LocalShards<D> {
    async fn open(&self, shard: &ShardRef, bucket: &BucketDocument) -> Result<(), ShardError> {
        self.set
            .open(&self.config(shard, bucket))
            .await
            .map(drop)
            .map_err(|error| convert(shard, error))
    }

    async fn seal(&self, shard: &ShardRef) -> Result<ShardSummary, ShardError> {
        let summary = self
            .set
            .seal(&shard.into())
            .await
            .map_err(|error| convert(shard, error))?;
        Ok(ShardSummary {
            objects: summary.objects,
            unflushed: summary.unflushed,
        })
    }

    async fn unseal(&self, shard: &ShardRef) -> Result<(), ShardError> {
        self.set
            .unseal(&shard.into())
            .await
            .map_err(|error| convert(shard, error))
    }

    async fn remove(&self, shard: &ShardRef) -> Result<(), ShardError> {
        self.set
            .remove(&shard.into())
            .await
            .map_err(|error| convert(shard, error))
    }

    async fn entry(&self, shard: &ShardRef, key: &str) -> Result<Option<Entry>, ShardError> {
        let local = self.find(shard).await?;
        local
            .entry(key)
            .await
            .map_err(|error| convert(shard, error))
    }

    async fn upload(
        &self,
        shard: &ShardRef,
        key: &str,
        upload: EpochSeq,
        after: u16,
        limit: usize,
    ) -> Result<Option<UploadParts>, ShardError> {
        let local = self.find(shard).await?;
        local
            .upload(key, upload, after, limit)
            .await
            .map_err(|error| convert(shard, error))
    }

    async fn uploads(
        &self,
        shard: &ShardRef,
        prefix: &str,
        after: Option<(String, Option<EpochSeq>)>,
        limit: usize,
    ) -> Result<Vec<(String, EpochSeq, Upload)>, ShardError> {
        let local = self.find(shard).await?;
        local
            .uploads(prefix, after, limit)
            .await
            .map_err(|error| convert(shard, error))
    }

    async fn parts(
        &self,
        shard: &ShardRef,
        upload: EpochSeq,
        after: u16,
        limit: usize,
    ) -> Result<Vec<(u16, Part)>, ShardError> {
        let local = self.find(shard).await?;
        local
            .parts(upload, after, limit)
            .await
            .map_err(|error| convert(shard, error))
    }

    async fn list(&self, shard: &ShardRef, query: &ListQuery) -> Result<ListPage, ShardError> {
        let local = self.find(shard).await?;
        local
            .list(query.clone())
            .await
            .map_err(|error| convert(shard, error))
    }

    async fn payload(&self, shard: &ShardRef, position: EpochSeq) -> Result<Bytes, ShardError> {
        let local = self.find(shard).await?;
        local
            .payload(position)
            .await
            .map_err(|error| convert(shard, error))
    }

    async fn append_extent(
        &self,
        shard: &ShardRef,
        extent: Extent,
    ) -> Result<ExtentRef, ShardError> {
        let local = self.find(shard).await?;
        local
            .append_extent(extent)
            .await
            .map_err(|error| convert(shard, error))
    }

    async fn write(
        &self,
        shard: &ShardRef,
        body: RecordBody,
        condition: Precondition,
    ) -> Result<Result<EpochSeq, ConditionFailed>, ShardError> {
        let local = self.find(shard).await?;
        let committed = if condition == Precondition::None {
            local.commit(body).await.map(Ok)
        } else {
            local.commit_if(body, |entry| condition.check(entry)).await
        };
        let committed = committed.map_err(|error| convert(shard, error))?;
        Ok(committed.and_then(|committed| match committed.outcome {
            Outcome::Rejected(Rejection::NoSuchUpload) => Err(ConditionFailed::NoSuchUpload),
            Outcome::Rejected(Rejection::PartChanged { .. }) => Err(ConditionFailed::InvalidPart),
            // Other rejections are of records only the node writes, such as
            // a `FLUSHED` that lost a race, which their writer expects.
            _ => Ok(committed.position),
        }))
    }
}
