//! The gateway's reads of `write_back` targets (design §9.1), served by the
//! flush service, which holds each bucket's target and import.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use skys3_flush::FlushService;
use skys3_gateway::{
    RemoteError, RemoteFuture, RemoteListing, RemoteObject, RemotePage, RemoteReads,
};
use skys3_index::ImportCheckpoint;
use skys3_io::Disk;
use skys3_remote::ObjectStore;
use skys3_types::{BucketId, ETag};

/// [`RemoteReads`] over a node's flush service.
pub(crate) struct NodeRemote<S, D>(pub(crate) Arc<FlushService<S, D>>);

impl<S, D> fmt::Debug for NodeRemote<S, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("NodeRemote").field(&self.0).finish()
    }
}

impl<S: ObjectStore, D: Disk> NodeRemote<S, D> {
    fn reader(&self, bucket: &BucketId) -> Result<skys3_flush::RemoteReader<S>, RemoteError> {
        self.0
            .remote(bucket)
            .ok_or_else(|| RemoteError("the bucket's target is not followed here".to_owned()))
    }
}

fn failed(error: impl fmt::Display) -> RemoteError {
    RemoteError(error.to_string())
}

impl<S: ObjectStore, D: Disk> RemoteReads for NodeRemote<S, D> {
    fn import(&self, bucket: &BucketId) -> Option<ImportCheckpoint> {
        self.0.remote(bucket).map(|reader| reader.import())
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
