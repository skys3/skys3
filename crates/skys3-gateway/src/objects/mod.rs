//! Object operations: PutObject, GetObject, HeadObject, and DeleteObject
//! (design §5.1, §7.2, §9.2, §11).
//!
//! **PutObject.** The body streams through the [`ChecksumValidator`] and
//! into the key's shard: a body up to `inline_max_bytes` goes inline in
//! the `PUT` record, a longer one is committed as `EXTENT` records of
//! `extent_bytes` while it arrives ([`Upload`]). The `PUT` is committed only
//! once the body has been read to its end without error and has passed
//! every check, since an `aws-chunked` body is authenticated only then, and
//! the request is answered only once the record is durable and applied.
//! `If-None-Match: *` and `If-Match` are checked by the shard when it
//! sequences the `PUT` ([`Precondition`]), and once before the body is read,
//! so a write bound to fail does not upload its body first.
//!
//! **Metadata.** The record keeps the standard headers S3 stores
//! (`Cache-Control`, `Content-Disposition`, `Content-Encoding`,
//! `Content-Language`, `Content-Type`, `Expires`) and user metadata under
//! its full `x-amz-meta-` name. User metadata, names and values, is limited
//! to 2 KiB less the bytes reserved for the write identity (§7.2), and the
//! identity's own name, `x-amz-meta-skys3-wid`, is refused on input and left
//! out of responses.
//!
//! **GetObject and HeadObject** resolve the key in its shard's index,
//! evaluate the conditional headers ([`ReadConditions`]), and serve a single
//! byte range (`bytes=a-b`, `a-`, or `-n`) with `206`, or `416` for one
//! the object cannot satisfy. A `Range` header that does not parse as one
//! range, such as a list of ranges, is ignored, as S3 does
//! ([`ignore_unsupported_range`]). Stored checksums are returned with
//! `x-amz-checksum-mode: ENABLED` for a whole object.
//!
//! **DeleteObject** commits a tombstone, also for a key with no object,
//! and answers `204` either way. In a `local` bucket, which is never
//! flushed, a `FLUSHED` record follows that removes the tombstone (§4.2).

mod download;
mod upload;

use std::time::{SystemTime, UNIX_EPOCH};

use http::request::Parts;
use http_body_util::BodyExt;
use s3s::dto::{
    ChecksumType as S3ChecksumType, DeleteObjectInput, DeleteObjectOutput, GetObjectInput,
    GetObjectOutput, HeadObjectInput, HeadObjectOutput, PutObjectInput, PutObjectOutput, Range,
    StreamingBlob,
};
use s3s::{S3Error, S3ErrorCode, S3Request, S3Result, s3_error};
use skys3_index::ObjectVersion;
use skys3_io::BlockingPool;
use skys3_log::RecordBody;
use skys3_log::record::{Delete, Flushed, MAX_METADATA_LEN, Metadata, Put};
use skys3_types::checksum::{ChecksumAlgorithm, Checksums};
use skys3_types::{BucketDocument, BucketMode, WriteIdentity};

use crate::buckets::{GatewayConfig, shard_error};
use crate::checksum::{ChecksumValidator, ExpectedChecksums};
use crate::conditions::{Precondition, ReadConditions, last_modified, no_such_key, s3_etag};
use crate::shard::{ShardRef, Shards};
use crate::sigv4::{BodyError, Trailers};
use upload::Upload;

/// The largest object a single PUT stores: 5 GiB, S3's limit.
pub const MAX_OBJECT_BYTES: u64 = 5 << 30;

/// S3's limit on user metadata, names and values together, in bytes.
const S3_USER_METADATA_BYTES: usize = 2048;

/// The user metadata SkyS3 accepts, in bytes: S3's limit, less what the
/// write identity takes when an object is flushed (§7.2).
pub const MAX_USER_METADATA_BYTES: usize =
    S3_USER_METADATA_BYTES - WriteIdentity::METADATA_RESERVED_BYTES;

/// The prefix of user metadata headers, under which the stored metadata
/// keeps them.
const USER_METADATA_PREFIX: &str = "x-amz-meta-";

/// The `Content-Type` of an object stored without one, as S3 answers.
const DEFAULT_CONTENT_TYPE: &str = "binary/octet-stream";

/// Sets the checksum fields of a PutObject, GetObject, or HeadObject output
/// from stored checksums. `Content-MD5` is stored as a checksum too, but S3
/// returns it as the ETag only.
macro_rules! set_checksums {
    ($output:expr, $checksums:expr) => {{
        let checksums: &Checksums = $checksums;
        for (algorithm, checksum) in checksums {
            let value = Some(checksum.to_string());
            match algorithm {
                ChecksumAlgorithm::Crc32 => $output.checksum_crc32 = value,
                ChecksumAlgorithm::Crc32c => $output.checksum_crc32c = value,
                ChecksumAlgorithm::Crc64Nvme => $output.checksum_crc64nvme = value,
                ChecksumAlgorithm::Sha1 => $output.checksum_sha1 = value,
                ChecksumAlgorithm::Sha256 => $output.checksum_sha256 = value,
                ChecksumAlgorithm::Md5 => continue,
            }
            $output.checksum_type = Some(S3ChecksumType::from_static(
                checksum.checksum_type().as_str(),
            ));
        }
    }};
}

/// Fills a GetObject or HeadObject output from a resolved read.
macro_rules! describe {
    ($output:expr, $found:expr, $checksum_mode:expr) => {{
        let found: &Found = &$found;
        let object = &found.object;
        let header = |name: &str| object.metadata.get(name).cloned();
        $output.accept_ranges = Some("bytes".to_owned());
        $output.cache_control = header("cache-control");
        $output.content_disposition = header("content-disposition");
        $output.content_encoding = header("content-encoding");
        $output.content_language = header("content-language");
        $output.content_type =
            Some(header("content-type").unwrap_or_else(|| DEFAULT_CONTENT_TYPE.to_owned()));
        $output.expires = header("expires");
        $output.content_length = i64::try_from(found.bytes.end - found.bytes.start).ok();
        $output.content_range = found.content_range.clone();
        $output.e_tag = Some(s3_etag(object));
        $output.last_modified = Some(last_modified(object));
        let metadata: s3s::dto::Metadata = user_metadata(&object.metadata).collect();
        $output.metadata = (!metadata.is_empty()).then_some(metadata);
        let enabled = $checksum_mode
            .as_ref()
            .is_some_and(|mode| mode.as_str() == s3s::dto::ChecksumMode::ENABLED);
        if enabled && found.content_range.is_none() {
            set_checksums!($output, &object.checksums);
        }
    }};
}

/// The object operations, over the shards.
pub(crate) struct Objects<H> {
    shards: H,
    inline_max_bytes: usize,
    extent_bytes: usize,
    pool: Option<BlockingPool>,
}

impl<H: Shards> Objects<H> {
    pub(crate) fn new(shards: H, config: &GatewayConfig) -> Self {
        Self {
            shards,
            inline_max_bytes: usize::try_from(config.inline_max_bytes).unwrap_or(usize::MAX),
            extent_bytes: usize::try_from(config.extent_bytes).unwrap_or(usize::MAX),
            pool: config.hashing_pool.clone(),
        }
    }

    pub(crate) async fn put(
        &self,
        bucket: &BucketDocument,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<PutObjectOutput> {
        let S3Request {
            mut input,
            headers,
            extensions,
            ..
        } = req;
        if input.tagging.is_some() || input.write_offset_bytes.is_some() {
            return Err(s3_error!(
                NotImplemented,
                "x-amz-tagging and x-amz-write-offset-bytes are not supported yet"
            ));
        }
        if input
            .content_length
            .is_some_and(|length| u64::try_from(length).map_or(true, |n| n > MAX_OBJECT_BYTES))
        {
            return Err(entity_too_large());
        }
        let metadata = stored_metadata(&input)?;
        let condition =
            Precondition::of_write(input.if_match.as_ref(), input.if_none_match.as_ref())?;
        let shard = ShardRef::for_key(bucket, &input.key);
        if condition != Precondition::None {
            let entry = self
                .shards
                .entry(&shard, &input.key)
                .await
                .map_err(shard_error)?;
            condition.check(entry.as_ref())?;
        }

        let expected = ExpectedChecksums::from_headers(&headers)?;
        let trailers = extensions.get::<Trailers>().cloned();
        let mut validator = ChecksumValidator::on(expected, trailers, self.pool.clone())?;
        let mut upload = Upload::new(
            self.shards.clone(),
            shard.clone(),
            input.key.clone(),
            self.inline_max_bytes,
            self.extent_bytes,
        );
        let mut body = s3s::Body::from(input.body.take().unwrap_or_else(empty_blob));
        let mut length = 0u64;
        while let Some(frame) = body.frame().await {
            let Ok(data) = frame.map_err(|error| body_error(&*error))?.into_data() else {
                continue;
            };
            length += data.len() as u64;
            if length > MAX_OBJECT_BYTES {
                return Err(entity_too_large());
            }
            upload.push(&data).await?;
            validator.update(data).await?;
        }
        let verified = validator.finish().await?;
        let data = upload.finish().await?;

        let put = Put {
            key: input.key,
            size: verified.length,
            last_modified_ms: now_ms(),
            etag: verified.etag.clone(),
            inherited_identity: None,
            metadata,
            tags: Default::default(),
            checksums: verified.checksums,
            copy_source: None,
            data,
        };
        let checksums = put.checksums.clone();
        self.shards
            .write(&shard, RecordBody::Put(put), condition)
            .await
            .map_err(shard_error)??;
        let mut output = PutObjectOutput {
            e_tag: Some(s3s::dto::ETag::Strong(verified.etag.as_str().to_owned())),
            ..PutObjectOutput::default()
        };
        set_checksums!(output, &checksums);
        Ok(output)
    }

    pub(crate) async fn get(
        &self,
        bucket: &BucketDocument,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<GetObjectOutput> {
        let input = &req.input;
        let conditions = ReadConditions {
            if_match: input.if_match.as_ref(),
            if_none_match: input.if_none_match.as_ref(),
            if_modified_since: input.if_modified_since.as_ref(),
            if_unmodified_since: input.if_unmodified_since.as_ref(),
        };
        let found = self
            .resolve(
                bucket,
                &input.key,
                conditions,
                input.range,
                input.part_number,
            )
            .await?;
        let body = download::read(
            &self.shards,
            &found.shard,
            &found.object.payload,
            found.bytes.clone(),
        )
        .await?;
        let mut output = GetObjectOutput {
            body: Some(body),
            ..GetObjectOutput::default()
        };
        describe!(output, found, input.checksum_mode);
        Ok(output)
    }

    pub(crate) async fn head(
        &self,
        bucket: &BucketDocument,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<HeadObjectOutput> {
        let input = &req.input;
        let conditions = ReadConditions {
            if_match: input.if_match.as_ref(),
            if_none_match: input.if_none_match.as_ref(),
            if_modified_since: input.if_modified_since.as_ref(),
            if_unmodified_since: input.if_unmodified_since.as_ref(),
        };
        let found = self
            .resolve(
                bucket,
                &input.key,
                conditions,
                input.range,
                input.part_number,
            )
            .await?;
        let mut output = HeadObjectOutput::default();
        describe!(output, found, input.checksum_mode);
        Ok(output)
    }

    pub(crate) async fn delete(
        &self,
        bucket: &BucketDocument,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<DeleteObjectOutput> {
        let input = req.input;
        if input.if_match_last_modified_time.is_some() || input.if_match_size.is_some() {
            return Err(s3_error!(
                NotImplemented,
                "x-amz-if-match-last-modified-time and x-amz-if-match-size are not supported"
            ));
        }
        let condition = Precondition::of_write(input.if_match.as_ref(), None)?;
        let shard = ShardRef::for_key(bucket, &input.key);
        let delete = RecordBody::Delete(Delete {
            key: input.key.clone(),
        });
        let position = self
            .shards
            .write(&shard, delete, condition)
            .await
            .map_err(shard_error)??;
        if bucket.mode == BucketMode::Local {
            // Nothing flushes a local bucket, so its tombstone would stay
            // forever. The `FLUSHED` removes it if no later write of the key
            // came first; it is not part of the answer, and if a crash loses
            // it, the tombstone only takes space.
            let shards = self.shards.clone();
            let flushed = RecordBody::Flushed(Flushed {
                key: input.key,
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
        Ok(DeleteObjectOutput::default())
    }

    /// Resolves a read: the key's object, the conditions checked, and the
    /// bytes to serve.
    async fn resolve(
        &self,
        bucket: &BucketDocument,
        key: &str,
        conditions: ReadConditions<'_>,
        range: Option<Range>,
        part_number: Option<i32>,
    ) -> S3Result<Found> {
        let shard = ShardRef::for_key(bucket, key);
        let object = self
            .shards
            .entry(&shard, key)
            .await
            .map_err(shard_error)?
            .and_then(|entry| entry.object)
            .ok_or_else(no_such_key)?;
        conditions.check(&object)?;
        let bytes = match (range, part_number) {
            (Some(_), Some(_)) => {
                return Err(s3_error!(
                    InvalidRequest,
                    "Cannot specify both Range header and partNumber query parameter"
                ));
            }
            (Some(range), None) => {
                let bytes = range.check(object.size).map_err(|_| {
                    s3_error!(InvalidRange, "The requested range is not satisfiable")
                })?;
                return Ok(Found {
                    shard,
                    content_range: Some(format!(
                        "bytes {}-{}/{}",
                        bytes.start,
                        bytes.end - 1,
                        object.size
                    )),
                    object,
                    bytes,
                });
            }
            // An object stored by a single PUT has one part.
            (None, Some(1) | None) => 0..object.size,
            (None, Some(_)) => return Err(invalid_part_number()),
        };
        Ok(Found {
            shard,
            object,
            bytes,
            content_range: None,
        })
    }
}

/// A resolved read.
struct Found {
    shard: ShardRef,
    object: ObjectVersion,
    /// The bytes to serve.
    bytes: std::ops::Range<u64>,
    /// `Content-Range`, for a range request.
    content_range: Option<String>,
}

/// The metadata a PUT stores: the standard headers S3 keeps, and user
/// metadata under its full header name.
///
/// # Errors
///
/// `400 InvalidArgument` for the write identity's reserved name, and
/// `400 MetadataTooLarge` for user metadata over
/// [`MAX_USER_METADATA_BYTES`] or metadata over what a record holds.
fn stored_metadata(input: &PutObjectInput) -> S3Result<Metadata> {
    let standard = [
        ("cache-control", &input.cache_control),
        ("content-disposition", &input.content_disposition),
        ("content-encoding", &input.content_encoding),
        ("content-language", &input.content_language),
        ("content-type", &input.content_type),
        ("expires", &input.expires),
    ];
    let mut metadata: Metadata = standard
        .into_iter()
        .filter_map(|(name, value)| Some((name.to_owned(), value.clone()?)))
        .collect();
    let mut user_bytes = 0;
    for (name, value) in input.metadata.iter().flatten() {
        let name = name.to_ascii_lowercase();
        if name == WriteIdentity::METADATA_KEY {
            return Err(s3_error!(
                InvalidArgument,
                "x-amz-meta-{name} is reserved for SkyS3's write identity"
            ));
        }
        user_bytes += name.len() + value.len();
        metadata.insert(format!("{USER_METADATA_PREFIX}{name}"), value.clone());
    }
    let total: usize = metadata.iter().map(|(n, v)| n.len() + v.len()).sum();
    if user_bytes > MAX_USER_METADATA_BYTES || total > MAX_METADATA_LEN {
        return Err(s3_error!(
            MetadataTooLarge,
            "Your metadata headers exceed the maximum allowed metadata size: \
             {MAX_USER_METADATA_BYTES} bytes of user metadata"
        ));
    }
    Ok(metadata)
}

/// The user metadata clients see: names without their prefix, and never
/// the write identity, which an object imported or adopted from the remote
/// can carry (§7.2).
fn user_metadata(metadata: &Metadata) -> impl Iterator<Item = (String, String)> + '_ {
    metadata.iter().filter_map(|(name, value)| {
        let name = name.strip_prefix(USER_METADATA_PREFIX)?;
        (name != WriteIdentity::METADATA_KEY).then(|| (name.to_owned(), value.clone()))
    })
}

/// Drops a `Range` header that `s3s` cannot parse as one byte range from a
/// GetObject or HeadObject request: S3 ignores it and serves the whole
/// object, as RFC 9110 allows, where `s3s` would refuse the request.
pub(crate) fn ignore_unsupported_range(parts: &mut Parts) {
    let reads = parts.method == http::Method::GET || parts.method == http::Method::HEAD;
    let unsupported = parts.headers.get(http::header::RANGE).is_some_and(|value| {
        value
            .to_str()
            .map_or(true, |text| Range::parse(text).is_err())
    });
    if reads && unsupported {
        parts.headers.remove(http::header::RANGE);
    }
}

/// The S3 error for an object body that could not be read.
fn body_error(error: &(dyn std::error::Error + 'static)) -> S3Error {
    match BodyError::find(error) {
        Some(error) => error.to_s3_error(),
        None => s3_error!(
            IncompleteBody,
            "You did not provide the number of bytes specified by the Content-Length HTTP header"
        ),
    }
}

/// `416 InvalidPartNumber`, which `s3s` has no code for.
fn invalid_part_number() -> S3Error {
    let code = S3ErrorCode::from_bytes(b"InvalidPartNumber").unwrap_or(S3ErrorCode::InvalidRange);
    let mut error = S3Error::with_message(code, "The requested partnumber is not satisfiable");
    error.set_status_code(http::StatusCode::RANGE_NOT_SATISFIABLE);
    error
}

fn entity_too_large() -> S3Error {
    s3_error!(
        EntityTooLarge,
        "Your proposed upload exceeds the maximum allowed object size of 5 GiB"
    )
}

fn empty_blob() -> StreamingBlob {
    StreamingBlob::from_bytes(bytes::Bytes::new())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        })
}
