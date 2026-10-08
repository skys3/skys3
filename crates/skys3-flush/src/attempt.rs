//! One flush of one key: the conditional request of §7.2 for the key's
//! latest committed version, and the recovery after a failed precondition.

use std::collections::BTreeMap;
use std::fmt;

use base64::Engine as _;
use bytes::{Bytes, BytesMut};
use skys3_config::ConflictPolicy;
use skys3_index::{Entry, EntryState, ObjectVersion, Payload};
use skys3_io::Disk;
use skys3_log::RecordBody;
use skys3_log::record::{Adopt, Metadata, TagSet};
use skys3_remote::probe::ConditionalOperation;
use skys3_remote::{
    CompleteMultipartUpload, DeleteObject, HeadObject, ObjectInfo, ObjectStore, PutObject, S3Error,
    S3ErrorKind, UserMetadata, WriteOutput, WritePrecondition,
};
use skys3_shard::{Outcome as Applied, Shard};
use skys3_types::{ETag, EpochSeq, Seq, WriteIdentity};

use crate::copy::Copied;
use crate::import::loaded_metadata;
use crate::peer::{Bodies, LinkError};
use crate::stream::{Streams, test_hooks};
use crate::target::Target;

/// How many times one attempt follows a failed precondition with another
/// request before it gives up and retries later: the remote kept changing.
pub(crate) const MAX_ROUNDS: usize = 3;

/// What a flush put at the remote for one version of a key, until the
/// `FLUSHED` that records it is applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Remote {
    /// The version flushed.
    pub(crate) seq: Seq,
    /// The remote ETag, or `None` after a delete.
    pub(crate) etag: Option<ETag>,
    /// The remote version ID, on a versioned remote.
    pub(crate) version_id: Option<String>,
}

/// A key held in conflict: the remote holds a write SkyS3 did not make
/// (§7.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Conflict {
    /// The local version that could not be flushed.
    pub seq: Seq,
    /// The ETag of the remote's current object, or `None` if the remote
    /// object the flush conditioned on is gone.
    pub remote_etag: Option<ETag>,
    /// The remote object's `skys3-wid` metadata, if it has any.
    pub remote_identity: Option<String>,
}

/// How an attempt ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// The remote holds the version: record it with a `FLUSHED` unless
    /// `record` is false (a tombstone the import has not passed, §4.2).
    Flushed {
        /// The version's position.
        version: EpochSeq,
        /// What the remote holds.
        remote: Remote,
        /// Whether to commit the `FLUSHED` now.
        record: bool,
    },
    /// Nothing to flush: the entry is clean or gone, or the remote already
    /// holds its version.
    Settled,
    /// The remote changed out of band.
    Conflict(Conflict),
    /// The `discard_local` policy dropped the version for the remote's
    /// out-of-band write, which an `ADOPT` made the key's version (§7.2).
    Discarded {
        /// The version dropped.
        version: EpochSeq,
    },
    /// A retryable failure: back off and try again.
    Retry(String),
}

/// What the remote holds at a key after a failed precondition, compared
/// with the version being flushed.
pub(crate) enum Found {
    /// Nothing.
    Missing,
    /// This version: an earlier attempt succeeded and its answer was lost.
    Mine(ObjectInfo),
    /// The object the flush conditioned on, or an earlier write of this
    /// shard, which the version supersedes: retry conditioned on its ETag.
    Supersede(ETag),
    /// A write SkyS3 did not make.
    Foreign(ObjectInfo),
}

/// The request that writes an object version, sent once per round with the
/// round's precondition.
pub(crate) enum Write<'r> {
    /// A whole object.
    Put(&'r PutObject),
    /// The completion of a remote multipart upload whose parts are
    /// uploaded.
    Complete(&'r CompleteMultipartUpload),
}

/// One flush of one key.
pub(crate) struct Attempt<'a, S, D: Disk> {
    pub(crate) shard: &'a Shard<D>,
    pub(crate) target: &'a Target<S>,
    pub(crate) key: &'a str,
    /// The streams of the shard's uploads (§7.3).
    pub(crate) streams: &'a Streams,
    /// How the key's conflict was resolved, if it was in one (§7.2):
    /// `overwrite` sends the version unconditionally, `discard_local`
    /// adopts the remote's write if it is still there, and `hold` flushes
    /// as usual.
    pub(crate) resolution: Option<ConflictPolicy>,
    /// The streamed bodies of the shard, for a native target (§7.8).
    pub(crate) bodies: &'a Bodies,
    /// Called once a native target's `COMMIT` is sent: only the seeded bug
    /// [`PeerBug::AnsweredOnCommit`](crate::PeerBug::AnsweredOnCommit)
    /// sets it.
    pub(crate) early: Option<&'a (dyn Fn() + Send + Sync)>,
}

impl<S: ObjectStore, D: Disk> Attempt<'_, S, D> {
    /// Flushes the key's latest committed version. `pending` is what an
    /// earlier flush of the key put at the remote while its `FLUSHED` is
    /// not yet applied; it describes the remote better than the entry does.
    pub(crate) async fn run(&self, pending: Option<&Remote>) -> Outcome {
        // Asked before the entry is read: once the import has passed the
        // key, the entry read next holds its `IMPORT` (§9.1).
        let passed = self.target.import.passed(self.key);
        let entry = match self.shard.entry(self.key).await {
            Ok(Some(entry)) => entry,
            Ok(None) => return Outcome::Settled,
            Err(error) => return Outcome::Retry(error.to_string()),
        };
        if matches!(entry.state, EntryState::Clean | EntryState::Evicted)
            || pending.is_some_and(|remote| remote.seq == entry.version.seq)
        {
            return Outcome::Settled;
        }
        if let Some(native) = &self.target.native {
            // A streamed body's `PUT`: its flush stages what the body's
            // own stream has not (§7.8).
            if let Some(upload) = entry.object.as_ref().and_then(|o| o.write_identity) {
                self.bodies.claim(upload);
            }
            let known = match pending {
                Some(remote) => (remote.etag.clone(), remote.version_id.clone()),
                None => (entry.remote_etag.clone(), entry.remote_version_id.clone()),
            };
            return self
                .native(native, &entry, known, passed)
                .await
                .unwrap_or_else(|error| Outcome::Retry(error.to_string()));
        }
        if self.resolution == Some(ConflictPolicy::DiscardLocal) {
            match self.discard(&entry).await {
                Ok(Some(outcome)) => return outcome,
                Ok(None) => {}
                Err(error) => return Outcome::Retry(error.to_string()),
            }
        }
        let known = match pending {
            Some(remote) => remote.etag.clone(),
            None => entry.remote_etag.clone(),
        };
        let result = match &entry.object {
            Some(object) => self.put(&entry, object, known, passed).await,
            None => self.delete(&entry, known, passed).await,
        };
        result.unwrap_or_else(|error| Outcome::Retry(error.to_string()))
    }

    pub(crate) fn remote_key(&self) -> String {
        format!("{}{}", self.target.prefix, self.key)
    }

    /// Tells the seeded bug, if any, that a native target's `COMMIT` is
    /// sent.
    pub(crate) fn sent(&self) {
        if let Some(early) = self.early {
            early();
        }
    }

    /// Whether `operation` is sent with its precondition: the target
    /// honors it, and the key's conflict was not resolved by overwriting
    /// the remote's write (§7.2).
    fn protected(&self, operation: ConditionalOperation) -> bool {
        self.resolution != Some(ConflictPolicy::Overwrite)
            && self.target.writes.operation(operation).is_protected()
    }

    /// Resolves the key's conflict under `discard_local` (§7.2): HEADs the
    /// key and, if it still holds a write SkyS3 did not make, commits an
    /// `ADOPT` of that write naming `entry`'s version, which drops the
    /// version, an acknowledged write. Returns `None` if the remote holds
    /// no such write any more: there is nothing to adopt, and the version
    /// is flushed as any other.
    async fn discard(&self, entry: &Entry) -> Result<Option<Outcome>, Failure> {
        let version = entry.version;
        let written = entry.object.as_ref().and_then(|o| o.write_identity);
        let identity = self
            .target
            .identity(self.shard.shard(), written.unwrap_or(version));
        let info = match self.inspect(&identity, version, None).await? {
            Found::Foreign(info) => info,
            Found::Mine(info) => return Ok(Some(flushed(version, Some(info), true))),
            Found::Missing | Found::Supersede(_) => return Ok(None),
        };
        let last_modified_ms = info.last_modified_ms.unwrap_or_else(|| {
            u64::try_from(self.target.wall.now().as_millis()).unwrap_or(u64::MAX)
        });
        let adopt = Adopt {
            key: self.key.to_owned(),
            expected_seq: version.seq,
            size: info.size,
            last_modified_ms,
            remote_etag: info.etag.clone(),
            remote_version_id: info.version_id.clone().map(|id| id.0),
            metadata: loaded_metadata(&info),
            checksums: Default::default(),
        };
        let committed = self.shard.commit(RecordBody::Adopt(adopt)).await?;
        match committed.outcome {
            Applied::Applied(_) => Ok(Some(Outcome::Discarded { version })),
            // A write committed after the version: it is the one to resolve.
            Applied::Rejected(rejection) => Err(Failure::Local(format!(
                "the remote's write was not adopted: {rejection}"
            ))),
        }
    }

    /// Flushes an object version (§7.2): `If-Match` on the known remote
    /// ETag, `If-None-Match: *` where the key is known to be absent, and a
    /// HEAD first where the remote state is unknown. A version made of
    /// parts is sent as a remote multipart upload with the same part
    /// boundaries, so that the remote ETag is the local one (§7.4); a
    /// streamed single PUT by completing the remote upload its body
    /// streamed to (§7.3); any other version with `PutObject`.
    async fn put(
        &self,
        entry: &Entry,
        object: &ObjectVersion,
        known: Option<ETag>,
        passed: bool,
    ) -> Result<Outcome, Failure> {
        let version = entry.version;
        let identity = self
            .target
            .identity(self.shard.shard(), object.write_identity.unwrap_or(version));
        let expected = match known {
            Some(etag) => Some(etag),
            None if passed => None,
            // Written locally before the import reached the key (§9.1):
            // the local version is newer than whatever the remote holds.
            None => match self.inspect(&identity, version, None).await? {
                Found::Missing => None,
                Found::Mine(info) => return Ok(flushed(version, Some(info), true)),
                Found::Supersede(etag) => Some(etag),
                Found::Foreign(info) => Some(info.etag),
            },
        };
        if let Payload::Parts { upload, parts } = &object.payload {
            return self
                .put_parts(version, object, *upload, parts, &identity, expected)
                .await;
        }
        // A streamed single PUT completes the remote upload its body
        // streamed to (§7.3); a `TAGS` version has an identity of its own.
        if let Some(upload) = object.write_identity
            && self.streamable(object)
            && let Some(claim) = self.streams.claim(upload).await
        {
            return self
                .complete_body(version, object, upload, claim, &identity, expected)
                .await;
        }
        // A copy of a clean source in the same remote bucket is copied
        // there, unless the source changed (§7.2, §11).
        let mut expected = expected;
        if let Some(source) = self.copy_source(object) {
            match self
                .copy(version, object, source, &identity, expected)
                .await?
            {
                Copied::Done(outcome) => return Ok(outcome),
                Copied::Upload(etag) => expected = etag,
            }
        }
        let request = put_request(self.remote_key(), object, &identity)?;
        let _permit = self.target.reserve(object.size).await;
        let body = read_payload(self.shard, &object.payload, object.size).await?;
        let request = PutObject { body, ..request };
        Ok(
            match self
                .conditional(Write::Put(&request), &identity, version, expected)
                .await?
            {
                Ok(output) => written(version, output),
                Err(outcome) => outcome,
            },
        )
    }

    /// Sends `write` conditioned on `expected`, the remote ETag it
    /// replaces (`None`: the key is absent), unless the target does not
    /// honor the operation's preconditions. After a failed precondition it
    /// HEADs the key and follows the §7.2 rule. Returns the store's answer
    /// to the write, or the outcome the HEAD settled.
    pub(crate) async fn conditional(
        &self,
        write: Write<'_>,
        identity: &WriteIdentity,
        version: EpochSeq,
        mut expected: Option<ETag>,
    ) -> Result<Result<WriteOutput, Outcome>, Failure> {
        let protected = self.protected(match write {
            Write::Put(_) => ConditionalOperation::PutObject,
            Write::Complete(_) => ConditionalOperation::CompleteMultipartUpload,
        });
        for _ in 0..MAX_ROUNDS {
            let precondition = match (&expected, protected) {
                (_, false) => WritePrecondition::None,
                (Some(etag), true) => WritePrecondition::IfMatch(etag.clone()),
                (None, true) => WritePrecondition::IfAbsent,
            };
            let store = &self.target.store;
            let result = match write {
                Write::Put(request) => {
                    store
                        .put_object(request.clone().with_precondition(precondition))
                        .await
                }
                Write::Complete(request) => {
                    let request = CompleteMultipartUpload {
                        precondition,
                        ..request.clone()
                    };
                    store.complete_multipart_upload(request).await
                }
            };
            let error = match result {
                Ok(output) => return Ok(Ok(output)),
                Err(error) if failed_precondition(&error) => error,
                Err(error) => return Err(Failure::Remote(error)),
            };
            match self.inspect(identity, version, expected.as_ref()).await? {
                Found::Mine(mut info) => {
                    if matches!(write, Write::Complete(_)) {
                        self.seeded_etag(&mut info).await;
                    }
                    return Ok(Err(flushed(version, Some(info), true)));
                }
                Found::Supersede(etag) => expected = Some(etag),
                // The object the flush conditioned on is gone, perhaps
                // deleted by an earlier flush whose answer was lost: no
                // remote write can be overwritten, so create the version.
                Found::Missing => expected = None,
                Found::Foreign(info) => {
                    tracing::debug!(key = self.key, %error, "a flush found a foreign object");
                    return Ok(Err(Outcome::Conflict(conflict(version, Some(info)))));
                }
            }
        }
        Err(Failure::Changing)
    }

    /// Flushes a tombstone with `DeleteObject` and `If-Match` on the known
    /// remote ETag. Without one, a HEAD finds what to delete: an earlier
    /// flush of the key may have landed without its `FLUSHED`.
    async fn delete(
        &self,
        entry: &Entry,
        known: Option<ETag>,
        record: bool,
    ) -> Result<Outcome, Failure> {
        let version = entry.version;
        if test_hooks::forgets_deletes() {
            return Ok(flushed(version, None, record));
        }
        if self.resolution == Some(ConflictPolicy::Overwrite) {
            // The local delete wins over whatever the remote holds.
            let request = DeleteObject::new(self.remote_key());
            self.target
                .store
                .delete_object(request)
                .await
                .map_err(Failure::Remote)?;
            return Ok(flushed(version, None, record));
        }
        let mut expected = known;
        for _ in 0..MAX_ROUNDS {
            let etag = match expected.take() {
                Some(etag) => etag,
                None => match self.inspect_for_delete(version, record).await? {
                    Ok(etag) => etag,
                    Err(outcome) => return Ok(outcome),
                },
            };
            let mut request = DeleteObject::new(self.remote_key());
            if self.protected(ConditionalOperation::DeleteObject) {
                request = request.with_if_match(etag.clone());
            }
            match self.target.store.delete_object(request).await {
                Ok(_) => return Ok(flushed(version, None, record)),
                Err(error) if failed_precondition(&error) => {}
                Err(error) => return Err(Failure::Remote(error)),
            }
            match self.inspect_for_delete(version, record).await? {
                Ok(etag) => expected = Some(etag),
                Err(outcome) => return Ok(outcome),
            }
        }
        Err(Failure::Changing)
    }

    /// What a tombstone's flush deletes: the ETag of the remote object, or
    /// the outcome if there is nothing this shard may delete.
    async fn inspect_for_delete(
        &self,
        version: EpochSeq,
        record: bool,
    ) -> Result<Result<ETag, Outcome>, Failure> {
        let info = match self.head().await? {
            None => return Ok(Err(flushed(version, None, record))),
            Some(info) => info,
        };
        // A foreign object at a key the import has not passed predates the
        // local delete, which replaces it; past the import it is a write
        // SkyS3 did not make.
        if self.ours_before(&info, version) || !record {
            Ok(Ok(info.etag))
        } else {
            Ok(Err(Outcome::Conflict(conflict(version, Some(info)))))
        }
    }

    /// HEADs the key and compares what the remote holds with the version
    /// being flushed, whose write identity is `identity`. `expected` is
    /// the ETag the failed request was conditioned on.
    pub(crate) async fn inspect(
        &self,
        identity: &WriteIdentity,
        version: EpochSeq,
        expected: Option<&ETag>,
    ) -> Result<Found, Failure> {
        let Some(info) = self.head().await? else {
            return Ok(Found::Missing);
        };
        let found = if info
            .metadata
            .write_identity()
            .is_some_and(|value| identity.matches(value))
        {
            Found::Mine(info)
        } else if expected == Some(&info.etag) || self.ours_before(&info, version) {
            Found::Supersede(info.etag)
        } else {
            Found::Foreign(info)
        };
        Ok(found)
    }

    /// Whether the remote object is a write of this shard from before
    /// `version`: one SkyS3 flushed earlier, which the version supersedes.
    fn ours_before(&self, info: &ObjectInfo, version: EpochSeq) -> bool {
        info.metadata
            .write_identity()
            .and_then(|value| value.parse::<WriteIdentity>().ok())
            .is_some_and(|wid| self.wrote_before(&wid, version))
    }

    /// Whether `wid` names a write of this shard from before `version`.
    pub(crate) fn wrote_before(&self, wid: &WriteIdentity, version: EpochSeq) -> bool {
        let shard = self.shard.shard();
        wid.cluster == self.target.cluster
            && wid.bucket == shard.bucket
            && wid.shard == shard.shard
            && wid.position < version
    }

    async fn head(&self) -> Result<Option<ObjectInfo>, Failure> {
        match self
            .target
            .store
            .head_object(HeadObject::new(self.remote_key()))
            .await
        {
            Ok(info) => Ok(Some(info)),
            Err(error) if error.kind() == S3ErrorKind::NoSuchKey => Ok(None),
            Err(error) => Err(Failure::Remote(error)),
        }
    }
}

/// Reads `size` bytes of `payload` from `shard`'s local log: those of a
/// version, or of a part of one.
pub(crate) async fn read_payload<D: Disk>(
    shard: &Shard<D>,
    payload: &Payload,
    size: u64,
) -> Result<Bytes, Failure> {
    let body = match payload {
        Payload::Inline(position) => shard.payload(*position).await?,
        Payload::Extents(extents) => {
            let mut body = BytesMut::new();
            for extent in extents {
                body.extend_from_slice(&shard.payload(extent.position).await?);
            }
            body.freeze()
        }
        // An imported or evicted stub changed by `TAGS`: its bytes must be
        // filled from the remote first (plan M1-20).
        Payload::None => return Err(Failure::Local("the version has no local bytes".into())),
        Payload::Parts { .. } => {
            return Err(Failure::Local(
                "a multipart object is read part by part".into(),
            ));
        }
    };
    if body.len() as u64 != size {
        return Err(Failure::Local(format!(
            "the version's bytes are {} long, not {size}",
            body.len()
        )));
    }
    Ok(body)
}

/// Why an attempt must be retried.
pub(crate) enum Failure {
    /// The remote failed.
    Remote(S3Error),
    /// The local shard failed, or the version cannot be sent as it is.
    Local(String),
    /// Failed preconditions kept finding a newer remote object.
    Changing,
    /// The link to a native target failed, or no answer came in time.
    Link(LinkError),
    /// A native target did not apply the write (§7.8).
    Peer(String),
}

impl Failure {
    /// The failure of a link to a native target.
    pub(crate) fn link(error: LinkError) -> Self {
        Failure::Link(error)
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Failure::Remote(error) => write!(f, "the remote failed: {error}"),
            Failure::Local(reason) => f.write_str(reason),
            Failure::Changing => f.write_str("the remote object kept changing"),
            Failure::Link(error) => write!(f, "the link to the peer failed: {error}"),
            Failure::Peer(reason) => write!(f, "the peer did not apply the write: {reason}"),
        }
    }
}

impl From<skys3_shard::ShardError> for Failure {
    fn from(error: skys3_shard::ShardError) -> Self {
        Failure::Local(error.to_string())
    }
}

/// Whether a conditional write failed because of the remote's state:
/// `412`, `404 NoSuchKey` to `If-Match` on a missing object, or `409
/// ConditionalRequestConflict` from a racing write (§7.2). Nothing was
/// written; the remote must be read.
pub(crate) fn failed_precondition(error: &S3Error) -> bool {
    matches!(
        error.kind(),
        S3ErrorKind::PreconditionFailed
            | S3ErrorKind::NoSuchKey
            | S3ErrorKind::ConditionalRequestConflict
    )
}

/// The outcome of a write the remote accepted.
pub(crate) fn written(version: EpochSeq, output: WriteOutput) -> Outcome {
    Outcome::Flushed {
        version,
        remote: Remote {
            seq: version.seq,
            etag: Some(output.etag),
            version_id: output.version_id.map(|id| id.0),
        },
        record: true,
    }
}

pub(crate) fn flushed(version: EpochSeq, info: Option<ObjectInfo>, record: bool) -> Outcome {
    let (etag, version_id) = match info {
        Some(info) => (Some(info.etag), info.version_id.map(|id| id.0)),
        None => (None, None),
    };
    Outcome::Flushed {
        version,
        remote: Remote {
            seq: version.seq,
            etag,
            version_id,
        },
        record,
    }
}

pub(crate) fn conflict(version: EpochSeq, info: Option<ObjectInfo>) -> Conflict {
    Conflict {
        seq: version.seq,
        remote_identity: info
            .as_ref()
            .and_then(|info| info.metadata.write_identity().map(str::to_owned)),
        remote_etag: info.map(|info| info.etag),
    }
}

/// The `PutObject` of a version, without its body: stored metadata split
/// into user metadata, `Content-Type`, and the other standard headers, the
/// tags, the write identity, and `Content-MD5` where the local ETag is the
/// body's MD5 (§7.4).
pub(crate) fn put_request(
    key: String,
    object: &ObjectVersion,
    identity: &WriteIdentity,
) -> Result<PutObject, Failure> {
    let mut request = request_headers(key, &object.metadata, &object.tags, identity)?;
    // A single PUT's ETag is the MD5 of its bytes; a copy's need not be.
    if object.copy_source.is_none() {
        request.content_md5 = content_md5(&object.local_etag);
    }
    Ok(request)
}

/// A `PutObject` without a body of an object with stored `metadata` and
/// `tags`, written with `identity`: the metadata split into user metadata,
/// `Content-Type`, and the other standard headers.
pub(crate) fn request_headers(
    key: String,
    stored: &Metadata,
    tags: &TagSet,
    identity: &WriteIdentity,
) -> Result<PutObject, Failure> {
    let mut metadata = UserMetadata::new();
    let mut headers = BTreeMap::new();
    let mut request = PutObject::new(key, Bytes::new());
    for (name, value) in stored {
        if let Some(user) = name.strip_prefix("x-amz-meta-") {
            metadata
                .insert(user, value.clone())
                .map_err(|error| Failure::Local(error.to_string()))?;
        } else if name == "content-type" {
            request.content_type = Some(value.clone());
        } else if PutObject::HEADERS.contains(&name.as_str()) {
            headers.insert(name.clone(), value.clone());
        }
    }
    metadata.set_write_identity(identity);
    request.metadata = metadata;
    request.headers = headers;
    request.tags = tags.clone();
    Ok(request)
}

/// The base64 `Content-MD5` of an ETag that is an MD5 digest in hex.
pub(crate) fn content_md5(etag: &ETag) -> Option<String> {
    let hex = etag.as_str().as_bytes();
    if hex.len() != 32 {
        return None;
    }
    let mut digest = [0u8; 16];
    let (pairs, _) = hex.as_chunks::<2>();
    for (byte, pair) in digest.iter_mut().zip(pairs) {
        let text = std::str::from_utf8(pair).ok()?;
        *byte = u8::from_str_radix(text, 16).ok()?;
    }
    Some(base64::engine::general_purpose::STANDARD.encode(digest))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_md5_is_the_base64_of_an_md5_etag() {
        let etag = ETag::new("5d41402abc4b2a76b9719d911017c592").unwrap();
        assert_eq!(
            content_md5(&etag).as_deref(),
            Some("XUFAKrxLKna5cZ2REBfFkg==")
        );
        assert_eq!(content_md5(&ETag::new("abc-2").unwrap()), None);
        let not_hex = ETag::new("zz41402abc4b2a76b9719d911017c592").unwrap();
        assert_eq!(content_md5(&not_hex), None);
    }

    #[test]
    fn failed_preconditions_are_412_404_and_409() {
        for (kind, failed) in [
            (S3ErrorKind::PreconditionFailed, true),
            (S3ErrorKind::NoSuchKey, true),
            (S3ErrorKind::ConditionalRequestConflict, true),
            (S3ErrorKind::NoSuchUpload, false),
            (S3ErrorKind::InternalError, false),
            (S3ErrorKind::SlowDown, false),
        ] {
            assert_eq!(failed_precondition(&S3Error::new(kind, "")), failed);
        }
    }
}
