//! The shards of a single node: [`Shards`] over the node's
//! [`ShardSet`], where every shard is local and this node its only member
//! (plan M1-04, M1-09).

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use skys3_index::{Entry, ListPage, ListQuery, Part, Upload};
use skys3_io::Disk;
use skys3_log::RecordBody;
use skys3_log::record::{Extent, ExtentRef};
use skys3_shard::{
    Committed, FlushState, Outcome, ReadId, ReadPlan, Registered, Rejection, Shard, ShardSet,
    StreamedBody,
};
use skys3_types::{BucketDocument, Epoch, EpochSeq, NodeId, ShardConfig};

use crate::conditions::{ConditionFailed, Precondition};
use crate::shard::{ShardError, ShardRef, ShardSummary, Shards, UploadParts, WriteOutcome};

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

    /// The replica of `shard`, if it serves client requests: a member
    /// refuses with [`ShardError::NotPrimary`] (§5.1). Reads and writes
    /// check this in the replica; payloads and seals check it here.
    async fn serving(&self, shard: &ShardRef) -> Result<Shard<D>, ShardError> {
        let local = self.find(shard).await?;
        local
            .check_readable()
            .map_err(|error| convert(shard, error))?;
        Ok(local)
    }

    /// The replica of `shard` on `holder`, which must be this node, as a
    /// holder of read payload (§8.7): any replica that runs serves what it
    /// holds, whatever its role.
    async fn holding(&self, shard: &ShardRef, holder: &NodeId) -> Result<Shard<D>, ShardError> {
        self.check_holder(shard, holder)?;
        self.find(shard).await
    }

    /// Checks that `holder` is this node. Registrations are the node's and
    /// outlive its replicas, so renewing and releasing one needs none.
    fn check_holder(&self, shard: &ShardRef, holder: &NodeId) -> Result<(), ShardError> {
        if *holder == self.node {
            Ok(())
        } else {
            Err(ShardError::NotFound(shard.clone()))
        }
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
        skys3_shard::ShardError::NotPrimary { primary, epoch, .. } => ShardError::NotPrimary {
            shard: shard.clone(),
            primary,
            epoch,
        },
        skys3_shard::ShardError::NotAcknowledged {
            position, reason, ..
        } => ShardError::NotAcknowledged {
            shard: shard.clone(),
            position,
            reason,
        },
        // Too few members left to take writes (§6.4): not acknowledged,
        // `503 SlowDown`, here and across the forward wire. A pending write
        // refused so was applied, as a write not acknowledged may be (§5.2).
        error @ skys3_shard::ShardError::UnderReplicated { .. } => ShardError::NotAcknowledged {
            shard: shard.clone(),
            position: None,
            reason: error.to_string(),
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
            .serving(shard)
            .await?
            .seal()
            .await
            .map_err(|error| convert(shard, error))?;
        Ok(ShardSummary {
            objects: summary.objects,
            unflushed: summary.unflushed,
        })
    }

    async fn unseal(&self, shard: &ShardRef) -> Result<(), ShardError> {
        self.serving(shard).await?.unseal();
        Ok(())
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
        let local = self.serving(shard).await?;
        local
            .payload(position)
            .await
            .map_err(|error| convert(shard, error))
    }

    fn node(&self) -> Option<NodeId> {
        Some(self.node.clone())
    }

    async fn plan(&self, shard: &ShardRef, key: &str) -> Result<ReadPlan, ShardError> {
        let local = self.find(shard).await?;
        local.plan(key).await.map_err(|error| convert(shard, error))
    }

    async fn register(
        &self,
        shard: &ShardRef,
        holder: &NodeId,
        key: &str,
        version: EpochSeq,
        layout: Vec<ExtentRef>,
    ) -> Result<Option<Registered>, ShardError> {
        let local = self.holding(shard, holder).await?;
        local
            .register_read(self.set.reads(), key, version, layout)
            .await
            .map_err(|error| convert(shard, error))
    }

    async fn renew(
        &self,
        shard: &ShardRef,
        holder: &NodeId,
        read: ReadId,
    ) -> Result<bool, ShardError> {
        self.check_holder(shard, holder)?;
        Ok(self.set.reads().renew(read))
    }

    async fn release(
        &self,
        shard: &ShardRef,
        holder: &NodeId,
        read: ReadId,
    ) -> Result<(), ShardError> {
        self.check_holder(shard, holder)?;
        self.set.reads().release(read);
        Ok(())
    }

    async fn fetch(
        &self,
        shard: &ShardRef,
        holder: &NodeId,
        read: ReadId,
        position: EpochSeq,
    ) -> Result<Bytes, ShardError> {
        let local = self.holding(shard, holder).await?;
        local
            .read_registered(self.set.reads(), read, position)
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

    async fn announce(&self, shard: &ShardRef, body: StreamedBody) -> Result<(), ShardError> {
        let local = self.find(shard).await?;
        local.announce(body).map_err(|error| convert(shard, error))
    }

    async fn flushed(
        &self,
        shard: &ShardRef,
        key: &str,
        version: EpochSeq,
        wait: Duration,
    ) -> Result<FlushState, ShardError> {
        let mut flush = self
            .serving(shard)
            .await?
            .await_flush(key, version)
            .map_err(|error| convert(shard, error))?;
        match tokio::time::timeout(wait, flush.answered()).await {
            Ok(Some(state)) => Ok(state),
            Ok(None) => Err(ShardError::Unavailable {
                shard: shard.clone(),
                reason: "the shard's flusher stopped".to_owned(),
            }),
            Err(_) => Ok(FlushState::Pending),
        }
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
        Ok(committed.and_then(written))
    }

    /// Sequences the writes together, in one pass of the shard's sequencer
    /// ([`Shard::commit_all_if`]), so that they share a group commit.
    async fn write_all(
        &self,
        shard: &ShardRef,
        writes: Vec<(RecordBody, Precondition)>,
    ) -> Vec<WriteOutcome> {
        let local = match self.find(shard).await {
            Ok(local) => local,
            Err(error) => return writes.iter().map(|_| Err(error.clone())).collect(),
        };
        let (bodies, conditions): (Vec<_>, Vec<_>) = writes.into_iter().unzip();
        local
            .commit_all_if(bodies, |at, entry| conditions[at].check(entry))
            .await
            .into_iter()
            .map(|committed| match committed {
                Ok(committed) => Ok(committed.and_then(written)),
                Err(error) => Err(convert(shard, error)),
            })
            .collect()
    }
}

/// What a committed write did, as [`Shards::write`] reports it.
fn written(committed: Committed) -> Result<EpochSeq, ConditionFailed> {
    match committed.outcome {
        Outcome::Rejected(Rejection::NoSuchUpload) => Err(ConditionFailed::NoSuchUpload),
        Outcome::Rejected(Rejection::PartChanged { .. }) => Err(ConditionFailed::InvalidPart),
        // Other rejections are of records only the node writes, such as a
        // `FLUSHED` that lost a race, which their writer expects.
        _ => Ok(committed.position),
    }
}

#[cfg(test)]
mod tests {
    use skys3_types::{BucketId, ShardId};

    use super::*;

    #[test]
    fn too_few_members_is_not_acknowledged() {
        let shard = ShardRef {
            bucket: BucketId::new("b-1").unwrap(),
            shard: ShardId::new(0),
        };
        let error = skys3_shard::ShardError::UnderReplicated {
            shard: (&shard).into(),
            copies: 1,
            min_write_replicas: 2,
        };
        let converted = convert(&shard, error);
        assert!(
            matches!(
                &converted,
                ShardError::NotAcknowledged { position: None, reason, .. }
                    if reason.contains("min_write_replicas")
            ),
            "{converted:?}"
        );
    }
}
