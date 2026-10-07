//! Reads of `read_only` buckets (§9.5): every read is checked against the
//! bucket's origin, and served from the bucket's shards only for the
//! version the origin was seen to hold.
//!
//! **Revalidation.** Under `freshness = "revalidate"`, the default, a GET
//! or HEAD first HEADs the key at the origin ([`RemoteReads::revalidate`]),
//! so it serves the version the origin held at some point during the
//! request. Under `freshness = "ttl"`, a read of a key whose origin answer
//! is younger than `freshness_ttl_seconds` uses that answer instead
//! ([`OriginValidations`]), so it serves a version the origin held at most
//! the TTL before the request; an older answer is renewed by a HEAD. A
//! missing object answers `404 NoSuchKey`, and one the bucket's
//! credentials may not read `403 AccessDenied`, under either mode.
//!
//! **The cache.** The key's entry in its shard caches the origin's version
//! as the entries of a `write_back` bucket cache its remote's: a read
//! serves the entry if it holds the ETag the origin answered, from its
//! holders or the hot cache, or by a fill when it is evicted (§9.2). An
//! entry that holds another version, or none, is brought up to date by the
//! GET: an `IMPORT` creates the stub of a key that has no entry, and an
//! `ADOPT` of the entry's version records the origin's (§9.1, §9.2). Both
//! are conditional, so of concurrent reads of the same change one record
//! applies; a read that then finds still another version asks the origin
//! again, at most [`MAX_ORIGIN_ROUNDS`] times. A HEAD commits nothing, and
//! answers what the origin answered. Since nothing writes a `read_only`
//! bucket, its entries are only ever clean or evicted, and the clean cache
//! evicts them like any other (§9.3).
//!
//! **Fills.** Only a shard's primary fills (§9.2), and fills are not
//! forwarded, so a gateway on another node reads an evicted version
//! straight from the origin, under `If-Match` on the ETag it validated,
//! [`ORIGIN_CHUNK_BYTES`] at a time, and caches nothing.

use std::io;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use s3s::dto::StreamingBlob;
use s3s::{S3Error, S3Result, s3_error};
use skys3_config::Freshness;
use skys3_index::{Entry, EntryState};
use skys3_log::RecordBody;
use skys3_log::record::{Adopt, Import};
use skys3_types::{BucketDocument, BucketId, ETag, EpochSeq};
use tokio::sync::mpsc;
use tokio::time::Instant;

use super::namespace::{Lookup, local, unloaded};
use super::{Found, Objects, download};
use crate::buckets::shard_error;
use crate::conditions::{Precondition, no_such_key};
use crate::origin::{OriginHead, Seen};
use crate::remote::{RemoteError, RemoteObject, RemoteReads};
use crate::shard::{ShardRef, Shards};

/// How many times a GET asks the origin again after another read cached a
/// different version of the key in between, before it answers `503`.
pub(super) const MAX_ORIGIN_ROUNDS: usize = 3;

/// The most bytes one GET of a direct read from the origin asks for.
pub(super) const ORIGIN_CHUNK_BYTES: u64 = 8 << 20;

impl<H: Shards> Objects<H> {
    /// The reads of `bucket`'s origin.
    ///
    /// # Errors
    ///
    /// `503 ServiceUnavailable` if this node does not read origins.
    pub(super) fn origin(&self) -> S3Result<&Arc<dyn RemoteReads>> {
        self.remote
            .as_ref()
            .ok_or_else(|| origin_unavailable("this node does not read origins"))
    }

    /// The object a read of `key` in `shard`, of the `read_only` bucket
    /// `bucket`, serves: the origin's version, cached in the key's entry
    /// for a GET (`planned`), with its read plan.
    ///
    /// # Errors
    ///
    /// `404 NoSuchKey` and `403 AccessDenied` as the origin answers, `503
    /// ServiceUnavailable` if the origin cannot be asked or the key keeps
    /// changing, and the shard's errors.
    pub(super) async fn origin_lookup(
        &self,
        bucket: &BucketDocument,
        shard: &ShardRef,
        key: &str,
        planned: bool,
    ) -> S3Result<Lookup> {
        let remote = self.origin()?;
        let scope = remote
            .origin_scope(&bucket.bucket_id)
            .ok_or_else(|| origin_unavailable("the bucket's origin is not read here yet"))?;
        let settings = self.buckets.get(&bucket.name);
        let ttl = (settings.freshness == Freshness::Ttl).then(|| settings.freshness_ttl());
        let known = ttl.and_then(|ttl| self.validations.fresh(&scope, key, ttl));
        match known {
            Some(Seen::Missing) => return Err(no_such_key()),
            Some(Seen::Denied) => return Err(denied()),
            Some(Seen::Present(etag)) => {
                let plan = self.read_plan(shard, key, planned).await?;
                if serves(plan.entry.as_ref(), &etag) {
                    return local(plan);
                }
            }
            None if ttl.is_none() && self.validations.skips_revalidation() => {
                // The seeded bug: whatever is cached is served.
                let plan = self.read_plan(shard, key, planned).await?;
                if plan.entry.as_ref().is_some_and(cached) {
                    return local(plan);
                }
            }
            None => {}
        }
        for _ in 0..MAX_ORIGIN_ROUNDS {
            let sent = Instant::now();
            let head = remote
                .revalidate(&bucket.bucket_id, key)
                .await
                .map_err(|error| origin_unavailable(error))?;
            self.validations
                .record(&scope, key, sent, Seen::from(&head));
            let found = match head {
                OriginHead::Found(found) => found,
                OriginHead::Missing => return Err(no_such_key()),
                OriginHead::Denied => return Err(denied()),
            };
            let etag = found.object.local_etag.clone();
            let plan = self.read_plan(shard, key, planned).await?;
            if serves(plan.entry.as_ref(), &etag) {
                return local(plan);
            }
            if !planned {
                return Ok(Lookup {
                    version: EpochSeq::default(),
                    object: found.object,
                    remote: true,
                    evicted: false,
                    layout: Vec::new(),
                    holders: Vec::new(),
                });
            }
            self.cache_origin(shard, key, plan.entry, found).await?;
            let plan = self.read_plan(shard, key, true).await?;
            if serves(plan.entry.as_ref(), &etag) {
                return local(plan);
            }
            tracing::debug!(%shard, key, "another read cached another version; asking again");
        }
        Err(s3_error!(
            ServiceUnavailable,
            "The object keeps changing at the bucket's origin; please retry"
        ))
    }

    /// Records `found`, the origin's version of `key`, in the key's entry,
    /// `entry`: an `IMPORT` of a key without one, then an `ADOPT` of the
    /// entry's version with the origin's metadata. Either is dropped if the
    /// entry changed first.
    ///
    /// # Errors
    ///
    /// The shard's errors.
    async fn cache_origin(
        &self,
        shard: &ShardRef,
        key: &str,
        entry: Option<Entry>,
        found: RemoteObject,
    ) -> S3Result<()> {
        let object = found.object;
        let entry = match entry {
            Some(entry) => entry,
            None => {
                let import = Import {
                    key: key.to_owned(),
                    size: object.size,
                    last_modified_ms: object.last_modified_ms,
                    etag: object.local_etag.clone(),
                    storage_class: object.storage_class.clone(),
                };
                self.shards
                    .write(shard, RecordBody::Import(import), Precondition::None)
                    .await
                    .map_err(shard_error)??;
                match self.shards.entry(shard, key).await.map_err(shard_error)? {
                    Some(entry) => entry,
                    None => return Ok(()),
                }
            }
        };
        let adopt = Adopt {
            key: key.to_owned(),
            expected_seq: entry.version.seq,
            size: object.size,
            last_modified_ms: object.last_modified_ms,
            remote_etag: object.local_etag,
            remote_version_id: found.version_id,
            metadata: object.metadata,
            checksums: Default::default(),
        };
        self.shards
            .write(shard, RecordBody::Adopt(adopt), Precondition::None)
            .await
            .map_err(shard_error)??;
        Ok(())
    }

    /// The bytes of `found`, an evicted version of `key` that this node
    /// cannot fill, read straight from `bucket`'s origin under `If-Match`
    /// on its ETag: `None` if the origin no longer holds it. The first
    /// chunk is read before the response starts; a later one that fails
    /// breaks the response off.
    ///
    /// # Errors
    ///
    /// `503 ServiceUnavailable` if the origin cannot be read.
    pub(super) async fn read_origin(
        &self,
        bucket: &BucketDocument,
        key: &str,
        found: &Found,
    ) -> S3Result<Option<StreamingBlob>> {
        let remote = Arc::clone(self.origin()?);
        let read = OriginRead {
            remote,
            bucket: bucket.bucket_id.clone(),
            key: key.to_owned(),
            etag: found.object.local_etag.clone(),
        };
        let bytes = found.bytes.clone();
        let first = bytes.start..bytes.end.min(bytes.start + ORIGIN_CHUNK_BYTES);
        let Some(data) = read
            .chunk(first.clone())
            .await
            .map_err(origin_unavailable)?
        else {
            // What a HEAD saw is out of date: the next round asks again.
            if let Some(scope) = read.remote.origin_scope(&bucket.bucket_id) {
                self.validations.forget(&scope, key);
            }
            return Ok(None);
        };
        if first.end == bytes.end {
            return Ok(Some(StreamingBlob::from_bytes(data)));
        }
        let (sender, receiver) = mpsc::channel(1);
        let rest = first.end..bytes.end;
        tokio::spawn(async move { read.stream(rest, data, sender).await });
        Ok(Some(download::filled(receiver, bytes.end - bytes.start)))
    }
}

/// A read straight from an origin, under `If-Match` on `etag`.
struct OriginRead {
    remote: Arc<dyn RemoteReads>,
    bucket: BucketId,
    key: String,
    etag: ETag,
}

impl OriginRead {
    /// The bytes `range`, or `None` if the origin no longer holds the
    /// version.
    async fn chunk(&self, range: Range<u64>) -> Result<Option<Bytes>, RemoteError> {
        let data = self
            .remote
            .get(&self.bucket, &self.key, &self.etag, range.clone())
            .await?;
        match data {
            Some(data) if data.len() as u64 != range.end - range.start => Err(RemoteError(
                format!("the origin sent {} bytes of {range:?}", data.len()),
            )),
            data => Ok(data),
        }
    }

    /// Sends `first`, then the bytes `rest` a chunk at a time, until the
    /// response is dropped or a chunk fails.
    async fn stream(self, rest: Range<u64>, first: Bytes, sender: mpsc::Sender<io::Result<Bytes>>) {
        if sender.send(Ok(first)).await.is_err() {
            return;
        }
        let mut at = rest.start;
        while at < rest.end {
            let end = rest.end.min(at + ORIGIN_CHUNK_BYTES);
            let sent = match self.chunk(at..end).await {
                Ok(Some(data)) => sender.send(Ok(data)).await,
                Ok(None) => {
                    let changed = "the object changed at the origin while it was read";
                    sender.send(Err(io::Error::other(changed))).await
                }
                Err(error) => sender.send(Err(io::Error::other(error))).await,
            };
            if sent.is_err() {
                return;
            }
            at = end;
        }
    }
}

/// Whether `entry` caches a version of the origin with its metadata:
/// clean or evicted, and not a stub whose metadata is not loaded.
fn cached(entry: &Entry) -> bool {
    matches!(entry.state, EntryState::Clean | EntryState::Evicted)
        && entry.object.is_some()
        && !unloaded(entry)
}

/// Whether `entry` caches the origin's version whose ETag is `etag`.
fn serves(entry: Option<&Entry>, etag: &ETag) -> bool {
    entry.is_some_and(|entry| {
        cached(entry)
            && entry
                .object
                .as_ref()
                .is_some_and(|object| object.local_etag == *etag)
    })
}

/// The answer to a read of an object the bucket's credentials may not read
/// at its origin.
fn denied() -> S3Error {
    s3_error!(
        AccessDenied,
        "The bucket's origin refuses its credentials this object"
    )
}

fn origin_unavailable(error: impl std::fmt::Display) -> S3Error {
    s3_error!(
        ServiceUnavailable,
        "The bucket's origin cannot be read: {error}"
    )
}
