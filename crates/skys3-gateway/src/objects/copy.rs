//! CopyObject (design §7.2, §10.1, §11).
//!
//! A copy reads the source object's bytes from its shard, or from its
//! fragments if it is coded (§8.5), and writes them
//! into the destination's shard as a new body, inline or as `EXTENT`
//! records, exactly as a PUT of the same bytes would be stored. It then
//! commits a `PUT` that records its source: the source bucket's ID, its
//! key, the version copied (its `seq` and ETag), and the source's
//! `remote_etag` if the source was clean, which the flusher needs to choose
//! a remote server-side copy (plan M4-07). Until then a copy flushes as a
//! regular upload. The copy's write identity names its own `PUT` (§7.2).
//!
//! A copy is always a single-part object. Of a source stored by a single
//! PUT, it keeps the ETag and checksums, since its bytes are the same. Of a
//! multipart source, whose ETag and checksums are derived from parts the
//! copy does not keep, it takes the MD5 of its bytes as its ETag and
//! full-object checksums of the source's algorithms (or CRC64NVME), computed
//! while the bytes are copied, so the flush reproduces them with a regular
//! upload. With `x-amz-checksum-algorithm`, its checksum is that
//! algorithm's instead, computed unless the source has a full-object one.
//! `Last-Modified` is the time of the copy.
//!
//! - **Metadata** (`x-amz-metadata-directive`): `COPY`, the default, keeps
//!   the source's stored headers and user metadata and ignores the
//!   request's; `REPLACE` takes them from the request, with PutObject's
//!   limits. A write identity the source carries is never copied (§7.2).
//! - **Tags** (`x-amz-tagging-directive`): `COPY`, the default, keeps the
//!   source's tags; `REPLACE` takes `x-amz-tagging`, or none. Copying a
//!   tagged source's tags needs `s3:GetObjectTagging` on the source and
//!   `s3:PutObjectTagging` on the copy, which the access hook decided
//!   ([`CopiedTags`]); without them the copy answers `403 AccessDenied`.
//! - **Conditions.** `x-amz-copy-source-if-match`,
//!   `-if-none-match`, `-if-modified-since`, and `-if-unmodified-since` are
//!   evaluated against the source as a GET evaluates its conditions, except
//!   that every failure answers `412 PreconditionFailed`. `If-Match` and
//!   `If-None-Match: *` apply to the destination as on PutObject.
//! - **Refusals, as in S3.** A copy of an object onto itself that does not
//!   replace its metadata is `400 InvalidRequest`. S3 also takes a new
//!   storage class or website redirect as a change, but SkyS3 stores
//!   neither (a PutObject's are accepted and ignored), so such a copy would
//!   change nothing and is refused too, rather than answer success for a
//!   change it drops. A version ID other than `null` is
//!   `400 InvalidArgument`, and a source larger than 5 GiB
//!   `400 InvalidRequest`. Copying a byte range is UploadPartCopy, a
//!   multipart operation.

use bytes::Bytes;
use s3s::dto::{CopyObjectInput, CopyObjectOutput, CopyObjectResult, CopySource as S3CopySource};
use s3s::{S3ErrorCode, S3Request, S3Result, s3_error};
use skys3_index::{EntryState, ObjectVersion, Payload};
use skys3_log::RecordBody;
use skys3_log::record::{CopySource, ExtentRef, Metadata, Put, PutData};
use skys3_types::checksum::{Checksum, ChecksumAlgorithm, Checksums};
use skys3_types::{BucketDocument, ETag, EpochSeq, VersionIdentity, WriteIdentity};

use super::tagging::parse_tagging_header;
use super::upload::Upload;
use super::{
    MAX_OBJECT_BYTES, Objects, USER_METADATA_PREFIX, coded, download, metadata_of, now_ms,
};
use crate::authz::{CopiedTags, access_denied};
use crate::buckets::shard_error;
use crate::checksum::{DEFAULT_ALGORITHM, Digests, PooledHasher, parse_algorithm};
use crate::conditions::{
    Precondition, ReadConditions, last_modified, no_such_key, precondition_failed, s3_etag,
};
use crate::shard::{ShardRef, Shards};

/// What a copy does with the source's metadata or tags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Directive {
    /// Keep the source's.
    Copy,
    /// Take the request's.
    Replace,
}

impl Directive {
    /// Parses an `x-amz-metadata-directive` or `x-amz-tagging-directive`
    /// value; `what` names the header for the error.
    fn parse(value: Option<&str>, what: &str) -> S3Result<Self> {
        match value {
            None | Some("COPY") => Ok(Self::Copy),
            Some("REPLACE") => Ok(Self::Replace),
            Some(_) => Err(s3_error!(InvalidArgument, "Unknown {what} directive.")),
        }
    }
}

/// The bucket name and key of a CopyObject's or UploadPartCopy's source,
/// its `x-amz-copy-source`.
///
/// # Errors
///
/// `501 NotImplemented` for an access point or outpost ARN, and
/// `400 InvalidArgument` for a version ID other than `null`.
pub(crate) fn copy_source(source: &S3CopySource) -> S3Result<(&str, &str)> {
    let S3CopySource::Bucket {
        bucket,
        key,
        version_id,
    } = source
    else {
        return Err(s3_error!(
            NotImplemented,
            "x-amz-copy-source must name a bucket and a key"
        ));
    };
    if version_id.as_deref().is_some_and(|id| id != "null") {
        return Err(s3_error!(InvalidArgument, "Invalid version id specified"));
    }
    Ok((bucket, key))
}

impl<H: Shards> Objects<H> {
    /// Copies the object `req` names as its source, in `source_bucket`,
    /// into `bucket`.
    pub(crate) async fn copy(
        &self,
        bucket: &BucketDocument,
        source_bucket: &BucketDocument,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<CopyObjectOutput> {
        let tags_allowed = req
            .extensions
            .get::<CopiedTags>()
            .is_some_and(|copied| copied.allowed());
        let input = req.input;
        let (_, source_key) = copy_source(&input.copy_source)?;
        let source_key = source_key.to_owned();
        let metadata_directive = Directive::parse(
            input.metadata_directive.as_ref().map(|d| d.as_str()),
            "metadata",
        )?;
        let tagging_directive = Directive::parse(
            input.tagging_directive.as_ref().map(|d| d.as_str()),
            "tagging",
        )?;
        // A storage class or website redirect is not stored, so a copy onto
        // itself that only sets one would change nothing.
        let onto_itself = bucket.bucket_id == source_bucket.bucket_id && input.key == source_key;
        if onto_itself && metadata_directive == Directive::Copy {
            return Err(s3_error!(
                InvalidRequest,
                "This copy request is illegal because it is trying to copy an object to itself \
                 without changing the object's metadata, storage class, website redirect \
                 location or encryption attributes."
            ));
        }
        let replaced_metadata = match metadata_directive {
            Directive::Copy => None,
            Directive::Replace => Some(metadata_of(
                standard_headers!(input),
                input.metadata.as_ref(),
            )?),
        };
        let replaced_tags = match tagging_directive {
            Directive::Copy => None,
            Directive::Replace => Some(
                input
                    .tagging
                    .as_deref()
                    .map(parse_tagging_header)
                    .transpose()?
                    .unwrap_or_default(),
            ),
        };
        let algorithm = input
            .checksum_algorithm
            .as_ref()
            .map(|algorithm| parse_algorithm(algorithm.as_str()))
            .transpose()?;
        let condition =
            Precondition::of_write(input.if_match.as_ref(), input.if_none_match.as_ref())?;

        let source_shard = ShardRef::for_key(source_bucket, &source_key);
        let entry = self
            .shards
            .entry(&source_shard, &source_key)
            .await
            .map_err(shard_error)?;
        let Some((version, state, remote_etag, object)) = entry.and_then(|entry| {
            let object = entry.object?;
            Some((entry.version, entry.state, entry.remote_etag, object))
        }) else {
            return Err(no_such_key());
        };
        if tagging_directive == Directive::Copy && !object.tags.is_empty() && !tags_allowed {
            return Err(access_denied());
        }
        let conditions = ReadConditions {
            if_match: input.copy_source_if_match.as_ref(),
            if_none_match: input.copy_source_if_none_match.as_ref(),
            if_modified_since: input.copy_source_if_modified_since.as_ref(),
            if_unmodified_since: input.copy_source_if_unmodified_since.as_ref(),
        };
        conditions.check(&object).map_err(|error| {
            if *error.code() == S3ErrorCode::NotModified {
                precondition_failed()
            } else {
                error
            }
        })?;
        if object.size > MAX_OBJECT_BYTES {
            return Err(s3_error!(
                InvalidRequest,
                "The specified copy source is larger than the maximum allowable size for a \
                 copy source: {MAX_OBJECT_BYTES}"
            ));
        }
        let shard = ShardRef::for_key(bucket, &input.key);
        self.admit(bucket, &shard)?;
        if condition != Precondition::None {
            let entry = self
                .shards
                .entry(&shard, &input.key)
                .await
                .map_err(shard_error)?;
            condition.check(entry.as_ref())?;
        }

        // A multipart source's ETag and checksums describe parts that the
        // copy, a single-part object, does not keep: they are computed again
        // over its bytes, so a flush by PutObject reproduces them (§11).
        let multipart = matches!(object.payload, Payload::Parts { .. });
        let wanted: Vec<ChecksumAlgorithm> = match algorithm {
            Some(algorithm) => vec![algorithm],
            None if multipart => {
                let kept: Vec<_> = object
                    .checksums
                    .keys()
                    .copied()
                    .filter(|&algorithm| algorithm != ChecksumAlgorithm::Md5)
                    .collect();
                if kept.is_empty() {
                    vec![DEFAULT_ALGORITHM]
                } else {
                    kept
                }
            }
            None => Vec::new(),
        };
        // A full-object checksum of a single-part source is still right.
        let reusable = |algorithm: &ChecksumAlgorithm| {
            object
                .checksums
                .get(algorithm)
                .filter(|checksum| !multipart && checksum.parts().is_none())
                .cloned()
        };
        let mut hashed: Vec<_> = wanted
            .iter()
            .copied()
            .filter(|algorithm| reusable(algorithm).is_none())
            .collect();
        if multipart {
            hashed.push(ChecksumAlgorithm::Md5);
        }
        let (data, digests) = self
            .copy_bytes(
                (&source_shard, &source_key, version),
                &object,
                &shard,
                &input.key,
                &hashed,
            )
            .await?;
        let checksums = if wanted.is_empty() {
            object.checksums.clone()
        } else {
            wanted
                .iter()
                .map(|&algorithm| {
                    let checksum = match digests.get(&algorithm) {
                        Some(digest) => Checksum::full_object(algorithm, digest).ok(),
                        None => reusable(&algorithm),
                    };
                    checksum
                        .map(|checksum| (algorithm, checksum))
                        .ok_or_else(unavailable)
                })
                .collect::<S3Result<Checksums>>()?
        };
        let etag = match digests.get(&ChecksumAlgorithm::Md5) {
            Some(md5) => {
                let md5 = <[u8; 16]>::try_from(md5.as_slice()).map_err(|_| unavailable())?;
                ETag::from_md5(&md5)
            }
            None => object.local_etag.clone(),
        };
        let clean = matches!(state, EntryState::Clean | EntryState::Evicted);
        let put = Put {
            key: input.key,
            size: object.size,
            last_modified_ms: now_ms(),
            etag: etag.clone(),
            inherited_identity: None,
            metadata: replaced_metadata.unwrap_or_else(|| without_identity(&object.metadata)),
            tags: replaced_tags.unwrap_or_else(|| object.tags.clone()),
            checksums,
            copy_source: Some(CopySource {
                bucket: source_bucket.bucket_id.clone(),
                key: source_key,
                version: VersionIdentity::new(version.seq, object.local_etag.clone()),
                remote_etag: remote_etag.filter(|_| clean),
            }),
            data,
        };
        let copied = ObjectVersion {
            last_modified_ms: put.last_modified_ms,
            local_etag: etag,
            checksums: put.checksums.clone(),
            ..object
        };
        let key = put.key.clone();
        let version = self
            .shards
            .write(&shard, RecordBody::Put(put), condition)
            .await
            .map_err(shard_error)??;
        self.acknowledge(bucket, &shard, &key, version).await?;
        let mut result = CopyObjectResult {
            e_tag: Some(s3_etag(&copied)),
            last_modified: Some(last_modified(&copied)),
            ..CopyObjectResult::default()
        };
        set_checksums!(result, &copied.checksums);
        Ok(CopyObjectOutput {
            copy_object_result: Some(result),
            ..CopyObjectOutput::default()
        })
    }

    /// Writes the bytes of `object`, the version at `version` of
    /// `source_key` in `source`, into `shard` as the body of a `PUT` of
    /// `key`, and returns where they are and their digests in the `hashed`
    /// algorithms. A coded source is read from its fragments, since its
    /// replicas drop their copies once it is coded (§8.5).
    async fn copy_bytes(
        &self,
        (source, source_key, version): (&ShardRef, &str, EpochSeq),
        object: &ObjectVersion,
        shard: &ShardRef,
        key: &str,
        hashed: &[ChecksumAlgorithm],
    ) -> S3Result<(PutData, Digests)> {
        let (mut coded, positions) = if object.coded.is_some() {
            let body = self
                .coded_source(source, source_key, version, object)
                .await?;
            (Some(body), Vec::new())
        } else {
            let positions = match &object.payload {
                Payload::Inline(position) => vec![(*position, object.size)],
                Payload::Extents(extents) => extent_positions(extents),
                Payload::Parts { upload, parts } => {
                    let whole = 0..object.size;
                    let (extents, _) =
                        download::part_extents(&self.shards, source, *upload, parts, whole).await?;
                    extent_positions(&extents)
                }
                Payload::None => return Err(download::not_cached()),
            };
            (None, positions)
        };
        let mut upload = Upload::new(
            self.shards.clone(),
            shard.clone(),
            key.to_owned(),
            self.inline_max_bytes,
            self.extent_bytes,
            self.max_body_duration,
        );
        let mut hasher = (!hashed.is_empty())
            .then(|| PooledHasher::new(hashed.iter().copied(), self.pool.clone()));
        let mut copied = 0u64;
        let mut positions = positions.into_iter();
        loop {
            let data: Bytes = if let Some(body) = &mut coded {
                match body.recv().await {
                    Some(piece) => piece.map_err(coded::unreadable_source)?,
                    None => break,
                }
            } else {
                let Some((position, len)) = positions.next() else {
                    break;
                };
                let data = self
                    .shards
                    .payload(source, position)
                    .await
                    .map_err(shard_error)?;
                if data.len() as u64 != len {
                    return Err(size_mismatch());
                }
                data
            };
            copied += data.len() as u64;
            upload.push(&data).await?;
            if let Some(hasher) = &mut hasher {
                hasher.update(data).await.map_err(|_| unavailable())?;
            }
        }
        if copied != object.size {
            return Err(size_mismatch());
        }
        let data = upload.finish().await?;
        let digests = match hasher {
            Some(hasher) => hasher.finish().await.map_err(|_| unavailable())?,
            None => Digests::new(),
        };
        Ok((data, digests))
    }
}

/// Each extent's position and length.
fn extent_positions(extents: &[ExtentRef]) -> Vec<(EpochSeq, u64)> {
    extents
        .iter()
        .map(|extent| (extent.position, u64::from(extent.len)))
        .collect()
}

fn size_mismatch() -> s3s::S3Error {
    s3_error!(
        InternalError,
        "the stored object's bytes do not match its size"
    )
}

/// `metadata` without a write identity, which an object adopted or imported
/// from the remote can carry, and which names that write, not this copy.
fn without_identity(metadata: &Metadata) -> Metadata {
    let identity = format!("{USER_METADATA_PREFIX}{}", WriteIdentity::METADATA_KEY);
    metadata
        .iter()
        .filter(|(name, _)| **name != identity)
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

fn unavailable() -> s3s::S3Error {
    s3_error!(ServiceUnavailable, "The hashing pool is shut down")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn directives_parse_as_s3_spells_them() {
        assert_eq!(Directive::parse(None, "m").unwrap(), Directive::Copy);
        assert_eq!(
            Directive::parse(Some("COPY"), "m").unwrap(),
            Directive::Copy
        );
        assert_eq!(
            Directive::parse(Some("REPLACE"), "m").unwrap(),
            Directive::Replace
        );
        let error = Directive::parse(Some("replace"), "metadata").unwrap_err();
        assert_eq!(*error.code(), S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn copies_never_carry_a_write_identity() {
        let metadata = Metadata::from([
            ("content-type".to_owned(), "text/plain".to_owned()),
            ("x-amz-meta-color".to_owned(), "tabby".to_owned()),
            ("x-amz-meta-skys3-wid".to_owned(), "c/b/0/1.2".to_owned()),
        ]);
        let kept: Vec<_> = without_identity(&metadata).into_keys().collect();
        assert_eq!(kept, ["content-type", "x-amz-meta-color"]);
    }

    #[test]
    fn tags_and_sources_parse() {
        let mut input = CopyObjectInput::builder()
            .bucket("b".to_owned())
            .key("k".to_owned())
            .copy_source(S3CopySource::Bucket {
                bucket: "src".into(),
                key: "a/b".into(),
                version_id: Some("null".into()),
            })
            .build()
            .unwrap();
        assert_eq!(copy_source(&input.copy_source).unwrap(), ("src", "a/b"));
        input.copy_source = S3CopySource::Bucket {
            bucket: "src".into(),
            key: "a/b".into(),
            version_id: Some("3HL4kqtJlcpXroDTDmJ".into()),
        };
        let error = copy_source(&input.copy_source).unwrap_err();
        assert_eq!(*error.code(), S3ErrorCode::InvalidArgument);
        input.copy_source = S3CopySource::AccessPoint {
            partition: "aws".into(),
            region: "us-east-1".into(),
            account_id: "123456789012".into(),
            access_point_name: "ap".into(),
            key: "k".into(),
            version_id: None,
        };
        let error = copy_source(&input.copy_source).unwrap_err();
        assert_eq!(*error.code(), S3ErrorCode::NotImplemented);
    }
}
