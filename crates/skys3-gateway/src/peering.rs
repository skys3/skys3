//! The gateway's part in native replication from peer clusters (design
//! §7.8): relaying staged frames to the shard primaries.

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use skys3_log::record::{Extent, ExtentRef};
use skys3_peer::{ExtentSink, SinkError};
use skys3_types::{BucketDocument, BucketName};

use crate::shard::{ShardError, ShardRef, Shards};

/// Looks up a bucket by name, as the gateway's local copy of the bucket
/// registers holds it ([`Gateway::bucket`](crate::Gateway::bucket)).
pub type BucketLookup = Arc<dyn Fn(&BucketName) -> Option<BucketDocument> + Send + Sync>;

/// Stages a source's frames on the primary of each key's shard, through
/// the gateway's [`Shards`]: in process when this node is the primary,
/// otherwise forwarded over the intra-cluster transport
/// ([`RoutedShards`](crate::routing::RoutedShards)). Each frame is an
/// `EXTENT` record of the key, at its piece offset, which the primary
/// commits once every member of the shard has it durably, as it does the
/// extents of a client's upload.
#[derive(Clone)]
pub struct PeerExtents<S> {
    shards: S,
    buckets: BucketLookup,
}

impl<S: fmt::Debug> fmt::Debug for PeerExtents<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerExtents")
            .field("shards", &self.shards)
            .finish_non_exhaustive()
    }
}

impl<S: Shards> PeerExtents<S> {
    /// Stages through `shards`, finding destination buckets with
    /// `buckets`.
    #[must_use]
    pub fn new(shards: S, buckets: BucketLookup) -> Self {
        Self { shards, buckets }
    }
}

impl<S: Shards> ExtentSink for PeerExtents<S> {
    async fn append(
        &self,
        bucket: &BucketName,
        key: &str,
        offset: u64,
        data: Bytes,
    ) -> Result<ExtentRef, SinkError> {
        let Some(document) = (self.buckets)(bucket) else {
            return Err(SinkError::Refused(format!("no bucket {bucket}")));
        };
        let shard = ShardRef::for_key(&document, key);
        let extent = Extent {
            key: key.to_owned(),
            offset,
            data,
        };
        self.shards
            .append_extent(&shard, extent)
            .await
            .map_err(|error| match error {
                // The bucket is being deleted, or the record breaks
                // a rule: sending it again cannot help.
                ShardError::Sealed(_) | ShardError::Invalid { .. } => {
                    SinkError::Refused(error.to_string())
                }
                _ => SinkError::Unavailable(error.to_string()),
            })
    }
}

#[cfg(test)]
mod tests {
    use skys3_types::{BucketId, BucketMode, ProposalId, ShardCount};

    use super::*;
    use crate::stub::MemoryShards;

    #[tokio::test]
    async fn frames_are_staged_as_extents_in_the_shard_of_their_key() {
        let shards = MemoryShards::new().await;
        let document = BucketDocument {
            bucket_id: BucketId::new("b-dest").unwrap(),
            name: "archive".parse().unwrap(),
            mode: BucketMode::Local,
            shards: ShardCount::new(4).unwrap(),
            replicas: 1,
            min_write_replicas: 1,
            clean_copies: 0,
            target: None,
            created_unix_ms: 0,
            proposal_id: ProposalId::new("p").unwrap(),
        };
        for shard in ShardRef::all(&document) {
            shards.open(&shard, &document).await.unwrap();
        }
        let name = document.name.clone();
        let lookup = document.clone();
        let sink = PeerExtents::new(
            shards.clone(),
            Arc::new(move |wanted: &BucketName| (*wanted == lookup.name).then(|| lookup.clone())),
        );
        assert!(format!("{sink:?}").contains("PeerExtents"));
        let key = "photos/cat.jpg";
        let data = Bytes::from_static(b"staged bytes");
        let extent = sink.append(&name, key, 4096, data.clone()).await.unwrap();
        assert_eq!(extent.len, 12);
        let shard = ShardRef::for_key(&document, key);
        assert_eq!(shards.payload(&shard, extent.position).await.unwrap(), data);
        // No entry references it, so no client sees it.
        assert_eq!(shards.entry(&shard, key).await.unwrap(), None);

        let unknown = BucketName::new("unknown").unwrap();
        let error = sink.append(&unknown, key, 0, data.clone()).await;
        assert!(matches!(error, Err(SinkError::Refused(_))), "{error:?}");
        // A bucket being deleted refuses staging for good.
        shards.seal(&shard).await.unwrap();
        let error = sink.append(&name, key, 0, data.clone()).await;
        assert!(matches!(error, Err(SinkError::Refused(_))), "{error:?}");
        shards.unseal(&shard).await.unwrap();
        // An unavailable shard may stage it later.
        shards.set_unavailable(true);
        let error = sink.append(&name, key, 0, data).await;
        assert!(matches!(error, Err(SinkError::Unavailable(_))), "{error:?}");
    }
}
