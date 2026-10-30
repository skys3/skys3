//! The gateway's reads of `write_back` targets (design §9.1) and of
//! `read_only` buckets' origins (§9.5), served by the flush service, which
//! holds each bucket's target and import, and each origin.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use skys3_flush::FlushService;
use skys3_gateway::{
    OriginHead, OriginScope, RemoteError, RemoteFuture, RemoteListing, RemoteObject, RemotePage,
    RemoteReads,
};
use skys3_index::ImportCheckpoint;
use skys3_io::Disk;
use skys3_remote::ObjectStore;
use skys3_types::{BucketId, ETag};

/// [`RemoteReads`] over a node's flush service: the gateway's reads of
/// `write_back` targets and `read_only` origins, as the node binary makes
/// them. Simulations build theirs from it too.
pub struct NodeRemote<S, D>(Arc<FlushService<S, D>>);

impl<S, D> NodeRemote<S, D> {
    /// The reads of the targets and origins `flush` follows.
    #[must_use]
    pub fn new(flush: Arc<FlushService<S, D>>) -> Self {
        Self(flush)
    }
}

impl<S, D> fmt::Debug for NodeRemote<S, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("NodeRemote").field(&self.0).finish()
    }
}

impl<S: ObjectStore, D: Disk> NodeRemote<S, D> {
    /// The reader of `bucket`'s target, or of its origin.
    fn reader(&self, bucket: &BucketId) -> Result<skys3_flush::RemoteReader<S>, RemoteError> {
        self.0
            .remote(bucket)
            .or_else(|| Some(self.0.origin(bucket)?.remote().clone()))
            .ok_or_else(|| RemoteError("the bucket's target is not followed here".to_owned()))
    }
}

/// The status of an answer that refuses the credentials.
const FORBIDDEN: u16 = 403;

fn failed(error: impl fmt::Display) -> RemoteError {
    RemoteError(error.to_string())
}

impl<S: ObjectStore, D: Disk> RemoteReads for NodeRemote<S, D> {
    fn import(&self, bucket: &BucketId) -> Option<ImportCheckpoint> {
        self.0.remote(bucket).map(|reader| reader.import())
    }

    fn passed(&self, bucket: &BucketId, key: &str) -> bool {
        self.0
            .remote(bucket)
            .is_none_or(|reader| reader.passed(key))
    }

    fn head<'a>(
        &'a self,
        bucket: &'a BucketId,
        key: &'a str,
    ) -> RemoteFuture<'a, Option<RemoteObject>> {
        Box::pin(async move {
            let found = self.reader(bucket)?.head(key).await.map_err(failed)?;
            Ok(found.map(|found| RemoteObject {
                object: found.object,
                version_id: found.version_id,
            }))
        })
    }

    fn get<'a>(
        &'a self,
        bucket: &'a BucketId,
        key: &'a str,
        etag: &'a ETag,
        range: Range<u64>,
    ) -> RemoteFuture<'a, Option<bytes::Bytes>> {
        Box::pin(async move {
            let reader = self.reader(bucket)?;
            reader.get(key, etag, range).await.map_err(failed)
        })
    }

    fn origin_scope(&self, bucket: &BucketId) -> Option<OriginScope> {
        let origin = self.0.origin(bucket)?;
        Some(OriginScope::new(origin.target(), origin.credentials()))
    }

    fn revalidate<'a>(
        &'a self,
        bucket: &'a BucketId,
        key: &'a str,
    ) -> RemoteFuture<'a, OriginHead> {
        Box::pin(async move {
            let reader = self.reader(bucket)?;
            match reader.head(key).await {
                Ok(Some(found)) => Ok(OriginHead::Found(Box::new(RemoteObject {
                    object: found.object,
                    version_id: found.version_id,
                }))),
                Ok(None) => Ok(OriginHead::Missing),
                Err(error) if error.status() == Some(FORBIDDEN) => Ok(OriginHead::Denied),
                Err(error) => Err(failed(error)),
            }
        })
    }

    fn list<'a>(
        &'a self,
        bucket: &'a BucketId,
        listing: RemoteListing,
    ) -> RemoteFuture<'a, RemotePage> {
        Box::pin(async move {
            let reader = self.reader(bucket)?;
            let (items, next) = reader
                .list(
                    &listing.prefix,
                    listing.delimiter.as_deref(),
                    listing.start_after.as_deref(),
                    listing.token,
                    listing.max_items,
                )
                .await
                .map_err(failed)?;
            Ok(RemotePage { items, next })
        })
    }
}
