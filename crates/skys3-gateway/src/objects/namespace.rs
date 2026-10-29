//! Reads that depend on the namespace import of a `write_back` bucket
//! (§9.1): keys the import has not reached, which fall through to the
//! remote, and imported stubs, whose metadata is loaded on first read.

use std::sync::Arc;

use s3s::dto::StreamingBlob;
use s3s::{S3Error, S3Result, s3_error};
use skys3_index::{Entry, EntryState, ObjectVersion};
use skys3_log::RecordBody;
use skys3_log::record::Adopt;
use skys3_types::{BucketDocument, BucketMode};

use super::{Found, Objects};
use crate::buckets::shard_error;
use crate::conditions::{Precondition, no_such_key};
use crate::remote::{RemoteError, RemoteReads};
use crate::shard::{ShardRef, Shards};

/// What a read of a key serves.
pub(super) struct Lookup {
    /// The object.
    pub(super) object: ObjectVersion,
    /// Whether the object is only at the remote: the key has no entry, and
    /// the import has not reached it.
    pub(super) remote: bool,
}

impl<H: Shards> Objects<H> {
    /// The remote reads of `bucket`, if it is a `write_back` bucket whose
    /// target this node reads.
    fn remote_of(&self, bucket: &BucketDocument) -> Option<&Arc<dyn RemoteReads>> {
        self.remote
            .as_ref()
            .filter(|_| bucket.mode == BucketMode::WriteBack)
    }

    /// The object a read of `key` in `shard` serves (§9.1).
    ///
    /// A key with no entry falls through to a remote HEAD while the
    /// bucket's import has not passed it; once it has, or for a delete
    /// tombstone, the key has no object. An imported stub has its
    /// metadata loaded first ([`Objects::load`]).
    ///
    /// # Errors
    ///
    /// `404 NoSuchKey`, `503 ServiceUnavailable` if the remote cannot
    /// answer, and the shard's errors.
    pub(super) async fn lookup(
        &self,
        bucket: &BucketDocument,
        shard: &ShardRef,
        key: &str,
    ) -> S3Result<Lookup> {
        let entry = self.shards.entry(shard, key).await.map_err(shard_error)?;
        let Some(remote) = self.remote_of(bucket) else {
            return local(entry);
        };
        match entry {
            Some(entry) if unloaded(&entry) => {
                let entry = self.load(&**remote, bucket, shard, key, entry).await?;
                local(entry)
            }
            Some(entry) => local(Some(entry)),
            None => {
                if remote.passed(&bucket.bucket_id, key) {
                    return Err(no_such_key());
                }
                let found = remote
                    .head(&bucket.bucket_id, key)
                    .await
                    .map_err(unavailable)?
                    .ok_or_else(no_such_key)?;
                Ok(Lookup {
                    object: found.object,
                    remote: true,
                })
            }
        }
    }

    /// Loads the metadata of `entry`, an imported stub of `key`, from the
    /// remote, commits it as an `ADOPT` of the stub's version (§9.1), and
    /// returns the key's entry afterwards. The `ADOPT` is dropped if a
    /// write of the key came first; the entry then holds that write. A
    /// remote that no longer has the object leaves the stub as it is.
    ///
    /// # Errors
    ///
    /// `503 ServiceUnavailable` if the remote cannot answer, and the
    /// shard's errors.
    async fn load(
        &self,
        remote: &dyn RemoteReads,
        bucket: &BucketDocument,
        shard: &ShardRef,
        key: &str,
        entry: Entry,
    ) -> S3Result<Option<Entry>> {
        let Some(found) = remote
            .head(&bucket.bucket_id, key)
            .await
            .map_err(unavailable)?
        else {
            return Ok(Some(entry));
        };
        let Some(stub) = &entry.object else {
            return Ok(Some(entry));
        };
        let same = found.object.local_etag == stub.local_etag;
        if !same {
            tracing::warn!(%shard, key,
                "an imported object changed at the remote out of band; adopting the remote's");
        }
        let adopt = Adopt {
            key: key.to_owned(),
            expected_seq: entry.version.seq,
            size: found.object.size,
            // The listing's `Last-Modified` stays while the object does.
            last_modified_ms: if same {
                stub.last_modified_ms
            } else {
                found.object.last_modified_ms
            },
            remote_etag: found.object.local_etag,
            remote_version_id: found.version_id,
            metadata: found.object.metadata,
            checksums: Default::default(),
        };
        let written = self
            .shards
            .write(shard, RecordBody::Adopt(adopt), Precondition::None)
            .await;
        if let Err(error) = written {
            // The stub still serves, as it did before.
            tracing::warn!(%shard, key, %error, "cannot record an imported object's metadata");
            return Ok(Some(entry));
        }
        self.shards.entry(shard, key).await.map_err(shard_error)
    }

    /// The bytes of `found`, an object only at the remote.
    ///
    /// # Errors
    ///
    /// `503 ServiceUnavailable` if the remote cannot answer or the object
    /// changed since it was read.
    pub(super) async fn read_remote(
        &self,
        bucket: &BucketDocument,
        key: &str,
        found: &Found,
    ) -> S3Result<StreamingBlob> {
        let remote = self.remote_of(bucket).ok_or_else(|| unavailable(gone()))?;
        let bytes = remote
            .get(
                &bucket.bucket_id,
                key,
                &found.object.local_etag,
                found.bytes.clone(),
            )
            .await
            .map_err(unavailable)?
            .ok_or_else(|| unavailable(gone()))?;
        Ok(StreamingBlob::from_bytes(bytes))
    }
}

/// Whether `entry` is an imported stub whose metadata is not loaded yet:
/// clean or evicted, with neither a checksum nor a `Content-Type`. Every
/// local write stores a checksum, and loaded metadata always has a
/// `Content-Type` (§9.1).
fn unloaded(entry: &Entry) -> bool {
    matches!(entry.state, EntryState::Clean | EntryState::Evicted)
        && entry.object.as_ref().is_some_and(|object| {
            object.checksums.is_empty() && !object.metadata.contains_key("content-type")
        })
}

/// What a read of a local entry serves.
fn local(entry: Option<Entry>) -> S3Result<Lookup> {
    let object = entry
        .and_then(|entry| entry.object)
        .ok_or_else(no_such_key)?;
    Ok(Lookup {
        object,
        remote: false,
    })
}

fn gone() -> RemoteError {
    RemoteError("the object changed at the remote while it was read".to_owned())
}

fn unavailable(error: RemoteError) -> S3Error {
    s3_error!(
        ServiceUnavailable,
        "The bucket's remote target cannot be read: {error}"
    )
}
