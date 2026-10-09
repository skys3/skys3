//! DeleteObject and DeleteObjects (design §4.2, §11).
//!
//! **DeleteObject** commits a tombstone, also for a key with no object,
//! and answers `204` either way. In a `local` bucket without a backup
//! target, which is never flushed, a `FLUSHED` record follows that removes
//! the tombstone (§4.2); with one, the tombstone stays until the flusher
//! has deleted the key at the backup (§8.9). In a `write_through` bucket,
//! and a `local` one with `backup_ack = "write_through"`, the answer waits
//! until the remote no longer holds the key (§7.5).
//!
//! **DeleteObjects** deletes up to [`MAX_DELETE_KEYS`] keys of one bucket,
//! each as DeleteObject would, and answers with what happened to each key:
//! `Deleted`, or an `Error` with the S3 code a DeleteObject of it would
//! have answered. In quiet mode only the errors are listed. As in S3:
//!
//! - the request needs `Content-MD5` or an `x-amz-checksum-*` value, which
//!   the gateway checks against the body before it is parsed
//!   (`400 InvalidRequest` without one);
//! - an empty list, or more than 1,000 keys, is `400 MalformedXML`, and an
//!   empty key `400 UserKeyMustBeSpecified`;
//! - each key is authorized on its own ([`KeyDecisions`]): one the caller
//!   may not delete gets `AccessDenied`, and the others are deleted;
//! - a key's `ETag` makes its delete conditional, like `If-Match` on
//!   DeleteObject; its `LastModifiedTime` and `Size` conditions answer
//!   `NotImplemented`, and a version ID other than `null`
//!   `InvalidArgument`, as for DeleteObject.
//!
//! Keys are deleted concurrently, at most [`DELETE_CONCURRENCY`] at a time,
//! so their records share group commits. A crash can leave some deleted
//! and not others, as in S3, where the operation is not atomic either.

use std::time::Duration;

use s3s::dto::{
    DeleteObjectInput, DeleteObjectOutput, DeleteObjectsInput, DeleteObjectsOutput, DeletedObject,
    Error as KeyError, ObjectIdentifier,
};
use s3s::{S3Error, S3Request, S3Result, s3_error};
use skys3_log::RecordBody;
use skys3_log::record::{Delete, Flushed};
use skys3_types::{BucketDocument, BucketMode, EpochSeq};
use tokio::task::JoinSet;

use super::Objects;
use super::write_through::reach_remote;
use crate::authz::{KeyDecisions, access_denied};
use crate::buckets::shard_error;
use crate::checksum::ExpectedChecksums;
use crate::conditions::Precondition;
use crate::limits::MAX_KEY_BYTES;
use crate::peer_s3::ApplyBy;
use crate::shard::{ShardRef, Shards};

/// The most keys one DeleteObjects request deletes (the S3 limit).
pub const MAX_DELETE_KEYS: usize = 1000;

/// How many deletes of one DeleteObjects request run at once.
const DELETE_CONCURRENCY: usize = 64;

impl<H: Shards> Objects<H> {
    pub(crate) async fn delete(
        &self,
        bucket: &BucketDocument,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<DeleteObjectOutput> {
        let apply_by = ApplyBy::of(&req.extensions);
        let input = req.input;
        if input.if_match_last_modified_time.is_some() || input.if_match_size.is_some() {
            return Err(s3_error!(
                NotImplemented,
                "x-amz-if-match-last-modified-time and x-amz-if-match-size are not supported"
            ));
        }
        let condition = Precondition::of_write(input.if_match.as_ref(), None)?.apply_by(apply_by);
        let shard = ShardRef::for_key(bucket, &input.key);
        let delete = self.removal(bucket);
        delete
            .remove(&self.shards, shard, input.key, condition)
            .await?;
        Ok(DeleteObjectOutput::default())
    }

    pub(crate) async fn delete_objects(
        &self,
        bucket: &BucketDocument,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<DeleteObjectsOutput> {
        let S3Request {
            input,
            headers,
            extensions,
            ..
        } = req;
        let expected = ExpectedChecksums::from_headers(&headers)?;
        if expected.content_md5().is_none() && expected.checksum().is_none() {
            return Err(s3_error!(
                InvalidRequest,
                "Missing required header for this request: Content-MD5 or x-amz-checksum-*"
            ));
        }
        let objects = input.delete.objects;
        if objects.is_empty() || objects.len() > MAX_DELETE_KEYS {
            return Err(s3_error!(
                MalformedXML,
                "A DeleteObjects request names from 1 to {MAX_DELETE_KEYS} keys"
            ));
        }
        if objects.iter().any(|object| object.key.is_empty()) {
            return Err(s3_error!(
                UserKeyMustBeSpecified,
                "Each Object must specify a Key"
            ));
        }
        let decisions = extensions
            .get::<KeyDecisions>()
            .cloned()
            .unwrap_or_default();
        let removal = self.removal(bucket);
        let apply_by = ApplyBy::of(&extensions);
        let mut outcomes: Vec<Option<S3Result<()>>> = objects.iter().map(|_| None).collect();
        let mut pending = JoinSet::new();
        let mut work = objects.iter().enumerate();
        loop {
            while pending.len() < DELETE_CONCURRENCY {
                let Some((index, object)) = work.next() else {
                    break;
                };
                let condition = match key_condition(object) {
                    Ok(_) if object.key == crate::peer_s3::DESCRIPTOR_KEY => {
                        outcomes[index] = Some(Err(crate::peer_s3::reserved_key()));
                        continue;
                    }
                    Ok(condition) if decisions.allows(index) => condition.apply_by(apply_by),
                    Ok(_) => {
                        outcomes[index] = Some(Err(access_denied()));
                        continue;
                    }
                    Err(error) => {
                        outcomes[index] = Some(Err(error));
                        continue;
                    }
                };
                let shards = self.shards.clone();
                let shard = ShardRef::for_key(bucket, &object.key);
                let key = object.key.clone();
                pending.spawn(async move {
                    (index, removal.remove(&shards, shard, key, condition).await)
                });
            }
            let Some(joined) = pending.join_next().await else {
                break;
            };
            let (index, outcome) = joined.map_err(|error| {
                tracing::error!(%error, "a delete of DeleteObjects panicked");
                s3_error!(InternalError)
            })?;
            outcomes[index] = Some(outcome);
        }

        let quiet = input.delete.quiet.unwrap_or(false);
        let mut deleted = Vec::new();
        let mut errors = Vec::new();
        for (object, outcome) in objects.into_iter().zip(outcomes) {
            match outcome.unwrap_or_else(|| Err(s3_error!(InternalError))) {
                Ok(()) if quiet => {}
                Ok(()) => deleted.push(DeletedObject {
                    key: Some(object.key),
                    version_id: object.version_id,
                    ..DeletedObject::default()
                }),
                Err(error) => errors.push(KeyError {
                    code: Some(error.code().as_str().to_owned()),
                    message: error.message().map(str::to_owned),
                    key: Some(object.key),
                    version_id: object.version_id,
                }),
            }
        }
        Ok(DeleteObjectsOutput {
            deleted: (!deleted.is_empty()).then_some(deleted),
            errors: (!errors.is_empty()).then_some(errors),
            ..DeleteObjectsOutput::default()
        })
    }
}

impl<H> Objects<H> {
    /// How `bucket`'s deletes are answered.
    fn removal(&self, bucket: &BucketDocument) -> Removal {
        Removal {
            unflushed: bucket.mode == BucketMode::Local && !self.write_through.backed_up(bucket),
            wait: self.write_through.wait_of(bucket),
        }
    }
}

/// The precondition of one key of a DeleteObjects request.
///
/// # Errors
///
/// The error DeleteObject would answer for the same key and conditions.
fn key_condition(object: &ObjectIdentifier) -> S3Result<Precondition> {
    if object.key.len() > MAX_KEY_BYTES {
        return Err(s3_error!(
            KeyTooLongError,
            "Your key is too long; the limit is {MAX_KEY_BYTES} bytes"
        ));
    }
    if object.version_id.as_deref().is_some_and(|id| id != "null") {
        return Err(s3_error!(InvalidArgument, "Invalid version id specified"));
    }
    if object.last_modified_time.is_some() || object.size.is_some() {
        return Err(s3_error!(
            NotImplemented,
            "LastModifiedTime and Size conditions are not supported"
        ));
    }
    Ok(object.e_tag.as_ref().map_or(Precondition::None, |etag| {
        Precondition::Matches(etag.value().to_owned())
    }))
}

/// How a bucket's deletes are answered.
#[derive(Debug, Clone, Copy)]
struct Removal {
    /// Whether the bucket is never flushed: a `local` one without a backup
    /// target (§8.9).
    unflushed: bool,
    /// How long a delete waits for the remote flush, in a `write_through`
    /// bucket (§7.5).
    wait: Option<Duration>,
}

impl Removal {
    /// Commits a tombstone for `key` in `shard` if `condition` holds, and
    /// answers once it is durable and applied, and in a `write_through`
    /// bucket once the remote no longer holds the key.
    ///
    /// Nothing flushes a `local` bucket without a backup target, so its
    /// tombstone would stay forever. There a `FLUSHED` follows that removes
    /// it if no later write of the key came first; it is not part of the
    /// answer, and if a crash loses it, the tombstone only takes space. A
    /// backup target's flusher removes the tombstone itself, once the key
    /// is deleted there.
    async fn remove<H: Shards>(
        self,
        shards: &H,
        shard: ShardRef,
        key: String,
        condition: Precondition,
    ) -> Result<(), S3Error> {
        let delete = RecordBody::Delete(Delete { key: key.clone() });
        let position = shards
            .write(&shard, delete, condition)
            .await
            .map_err(shard_error)??;
        if let Some(timeout) = self.wait {
            return reach_remote(shards, &shard, &key, position, timeout).await;
        }
        if self.unflushed {
            remove_tombstone(shards, shard, key, position);
        }
        Ok(())
    }
}

/// Commits, in the background, the `FLUSHED` that removes the tombstone
/// of `key` at `position` from the shard of a `local` bucket without a
/// backup target.
fn remove_tombstone<H: Shards>(shards: &H, shard: ShardRef, key: String, position: EpochSeq) {
    let shards = shards.clone();
    let flushed = RecordBody::Flushed(Flushed {
        key,
        seq: position.seq,
        remote_etag: None,
        remote_version_id: None,
    });
    tokio::spawn(async move {
        if let Err(error) = shards.write(&shard, flushed, Precondition::None).await {
            tracing::debug!(%error, "a local tombstone was not removed");
        }
    });
}
