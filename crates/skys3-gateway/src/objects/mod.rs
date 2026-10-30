//! Object operations: PutObject, GetObject, HeadObject, DeleteObject,
//! DeleteObjects, CopyObject, and object tagging (design §5.1, §7.2, §9.2,
//! §10.1, §11).
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
//! so a write bound to fail does not upload its body first. A body of a
//! `write_back` bucket that reaches `streaming_flush_min_bytes` commits an
//! `UPLOAD_BEGIN` while it streams, and its `PUT` inherits that record's
//! write identity (§7.2); a body that fails leaves the record naming no
//! write.
//!
//! **Metadata.** The record keeps the standard headers S3 stores
//! (`Cache-Control`, `Content-Disposition`, `Content-Encoding`,
//! `Content-Language`, `Content-Type`, `Expires`) and user metadata under
//! its full `x-amz-meta-` name. User metadata, names and values, is limited
//! to 2 KiB less the bytes reserved for the write identity (§7.2), and the
//! identity's own name, `x-amz-meta-skys3-wid`, is refused on input and left
//! out of responses. Tags given with `x-amz-tagging` are part of the `PUT`
//! record, so an object and its tags commit together and share one write
//! identity (`tagging`).
//!
//! **GetObject and HeadObject** resolve the key in its shard's index,
//! evaluate the conditional headers ([`ReadConditions`]), and serve a single
//! byte range (`bytes=a-b`, `a-`, or `-n`) with `206`, or `416` for one
//! the object cannot satisfy. A `Range` header that does not parse as one
//! range, such as a list of ranges, is ignored, as S3 does
//! ([`ignore_unsupported_range`]). Stored checksums are returned with
//! `x-amz-checksum-mode: ENABLED` for a whole object, and the stored storage
//! class unless it is `STANDARD`. The `response-*` query parameters replace
//! the stored headers they name; S3 refuses them in an anonymous request,
//! and so does SkyS3. `partNumber=N` serves the `N`th part of a multipart
//! object, with `206` and `x-amz-mp-parts-count`, and `partNumber=1` the
//! whole of any other. An object with tags reports their number in
//! `x-amz-tagging-count`. A GET reads its bytes from a holder its shard's
//! primary names in the read plan (§9.2), the gateway's own node first,
//! under a registration that keeps them on the holder while they stream
//! (`holders`); when no holder still has the version, the read resolves
//! the key again. Before it asks a holder, a GET looks the version up in
//! the node's [`HotCache`], and a whole object read from another node's
//! holder fills it. A GET of an evicted version of a `write_back`
//! bucket reads it through a fill from the remote ([`Fills`]); when the fill
//! finds the remote changed and adopts its version, the read resolves the
//! key again, at most [`MAX_FILL_ROUNDS`] times.
//!
//! DeleteObject and DeleteObjects are in `delete`, CopyObject in `copy`,
//! GetObjectTagging, PutObjectTagging, and DeleteObjectTagging in `tagging`,
//! multipart uploads in `multipart`, and UploadPartCopy, which reads its
//! source as a GET does, in `part_copy`.

mod download;
mod holders;
mod upload;

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use http::request::Parts;
use http_body_util::BodyExt;
use s3s::dto::{
    GetObjectInput, GetObjectOutput, HeadObjectInput, HeadObjectOutput, PutObjectInput,
    PutObjectOutput, Range, StreamingBlob,
};
use s3s::{S3Error, S3ErrorCode, S3Request, S3Result, s3_error};
use skys3_index::{ObjectVersion, Payload};
use skys3_io::BlockingPool;
use skys3_log::RecordBody;
use skys3_log::record::{
    ExtentRef, IDENTITY_METADATA_RESERVED, MAX_METADATA_LEN, Metadata, Put, PutData,
};
use skys3_types::checksum::ChecksumAlgorithm;
use skys3_types::{BucketDocument, BucketMode, EpochSeq, NodeId, WriteIdentity};

use crate::admission::Admission;
use crate::buckets::{GatewayConfig, shard_error};
use crate::checksum::{ChecksumValidator, ExpectedChecksums, VerifiedBody};
use crate::conditions::{Precondition, ReadConditions, last_modified, s3_etag};
use crate::fill::{FillError, Fills};
use crate::hot_cache::{HotCache, ObjectName, VersionName};
use crate::remote::RemoteReads;
use crate::shard::{ShardRef, Shards};
use crate::sigv4::{Authenticated, BodyError, Trailers};
pub(crate) use copy::copy_source;
pub use delete::MAX_DELETE_KEYS;
use holders::{HolderReads, Keep};
pub use tagging::{MAX_OBJECT_TAGS, MAX_TAG_KEY_CHARS, MAX_TAG_VALUE_CHARS};
#[cfg(any(test, feature = "test-util"))]
pub(crate) use tagging::{parse_tagging_header, tagging_header, tags_from_xml};
use upload::Upload;

/// How many times a GET resolves its key again after a read-through fill
/// found the key changed, such as after the fill adopted the remote's
/// version, or after no holder still had the version its plan named
/// (§9.2), before it answers `503`.
const MAX_FILL_ROUNDS: usize = 3;

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

/// Sets the checksum value fields of an S3 output or part from stored
/// checksums, and returns the type of the last one set. `Content-MD5` is
/// stored as a checksum too, but S3 returns it as the ETag only.
macro_rules! set_checksum_values {
    ($output:expr, $checksums:expr) => {{
        let checksums: &skys3_types::checksum::Checksums = $checksums;
        let mut checksum_type = None;
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
            checksum_type = Some(checksum.checksum_type());
        }
        checksum_type
    }};
}

/// Sets the checksum fields of an output that has a checksum type, such as
/// PutObject's, GetObject's, or a CopyObject result, from stored checksums.
macro_rules! set_checksums {
    ($output:expr, $checksums:expr) => {{
        if let Some(checksum_type) = set_checksum_values!($output, $checksums) {
            $output.checksum_type =
                Some(s3s::dto::ChecksumType::from_static(checksum_type.as_str()));
        }
    }};
}

/// The standard headers S3 stores with an object, from the input of a
/// PutObject, a CopyObject, or a CreateMultipartUpload.
macro_rules! standard_headers {
    ($input:expr) => {
        [
            ("cache-control", &$input.cache_control),
            ("content-disposition", &$input.content_disposition),
            ("content-encoding", &$input.content_encoding),
            ("content-language", &$input.content_language),
            ("content-type", &$input.content_type),
            ("expires", &$input.expires),
        ]
    };
}

/// The [`Overrides`] of a GetObject or HeadObject input.
macro_rules! overrides {
    ($input:expr) => {
        Overrides {
            cache_control: $input.response_cache_control.as_deref(),
            content_disposition: $input.response_content_disposition.as_deref(),
            content_encoding: $input.response_content_encoding.as_deref(),
            content_language: $input.response_content_language.as_deref(),
            content_type: $input.response_content_type.as_deref(),
            expires: $input.response_expires.as_ref(),
        }
    };
}

/// Fills a GetObject or HeadObject output from a resolved read, then
/// replaces the headers the request overrides.
macro_rules! describe {
    ($output:expr, $found:expr, $checksum_mode:expr, $overrides:expr) => {{
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
        $output.parts_count = found.parts_count;
        $output.e_tag = Some(s3_etag(object));
        $output.last_modified = Some(last_modified(object));
        $output.storage_class = storage_class(object);
        let metadata: s3s::dto::Metadata = user_metadata(&object.metadata).collect();
        $output.metadata = (!metadata.is_empty()).then_some(metadata);
        $output.tag_count = i32::try_from(object.tags.len())
            .ok()
            .filter(|&count| count > 0);
        let overrides: Checked = $overrides;
        let replace = |field: &mut Option<String>, value: Option<String>| {
            if value.is_some() {
                *field = value;
            }
        };
        replace(&mut $output.cache_control, overrides.cache_control);
        replace(
            &mut $output.content_disposition,
            overrides.content_disposition,
        );
        replace(&mut $output.content_encoding, overrides.content_encoding);
        replace(&mut $output.content_language, overrides.content_language);
        replace(&mut $output.content_type, overrides.content_type);
        replace(&mut $output.expires, overrides.expires);
        let enabled = $checksum_mode
            .as_ref()
            .is_some_and(|mode| mode.as_str() == s3s::dto::ChecksumMode::ENABLED);
        if enabled && found.content_range.is_none() {
            set_checksums!($output, &object.checksums);
        }
    }};
}

// After the macros, which they use.
mod copy;
mod delete;
mod multipart;
mod namespace;
mod part_copy;
mod tagging;

#[cfg(feature = "test-util")]
pub(crate) use part_copy::CopyRange;

pub use multipart::{MAX_MULTIPART_OBJECT_BYTES, MIN_PART_BYTES, parse_upload_id, upload_id};

/// The object operations, over the shards.
pub(crate) struct Objects<H> {
    shards: H,
    inline_max_bytes: usize,
    extent_bytes: usize,
    streaming_flush_min_bytes: Option<u64>,
    max_body_duration: Duration,
    pool: Option<BlockingPool>,
    fills: Option<Arc<dyn Fills>>,
    admission: Arc<dyn Admission>,
    remote: Option<Arc<dyn RemoteReads>>,
    holders: HolderReads,
    hot_cache: HotCache,
}

impl<H: Shards> Objects<H> {
    pub(crate) fn new(shards: H, config: &GatewayConfig) -> Self {
        Self {
            shards,
            inline_max_bytes: usize::try_from(config.inline_max_bytes).unwrap_or(usize::MAX),
            extent_bytes: usize::try_from(config.extent_bytes).unwrap_or(usize::MAX),
            streaming_flush_min_bytes: config.streaming_flush_min_bytes,
            max_body_duration: config.max_body_duration,
            pool: config.hashing_pool.clone(),
            fills: config.fills.clone(),
            admission: Arc::clone(&config.admission),
            remote: config.remote.clone(),
            holders: HolderReads::new(config.read_registration_renew_interval),
            hot_cache: config.hot_cache.clone(),
        }
    }

    /// Asks admission control whether a write that adds data to `shard`
    /// may proceed (§7.6, §13).
    fn admit(&self, bucket: &BucketDocument, shard: &ShardRef) -> S3Result<()> {
        self.admission.admit(bucket, shard).map_err(|refusal| {
            tracing::debug!(%shard, %refusal, "admission control refused a write");
            S3Error::from(refusal)
        })
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
        if input.write_offset_bytes.is_some() {
            return Err(s3_error!(
                NotImplemented,
                "x-amz-write-offset-bytes is not supported"
            ));
        }
        if input
            .content_length
            .is_some_and(|length| u64::try_from(length).map_or(true, |n| n > MAX_OBJECT_BYTES))
        {
            return Err(entity_too_large());
        }
        let metadata = stored_metadata(&input)?;
        let tags = input
            .tagging
            .as_deref()
            .map(tagging::parse_tagging_header)
            .transpose()?
            .unwrap_or_default();
        let condition =
            Precondition::of_write(input.if_match.as_ref(), input.if_none_match.as_ref())?;
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

        let expected = ExpectedChecksums::from_headers(&headers)?;
        let trailers = extensions.get::<Trailers>().cloned();
        let validator = ChecksumValidator::on(expected, trailers, self.pool.clone())?;
        // Only a bucket with a remote target streams a PUT to it (§7.3).
        let stream_from = self
            .streaming_flush_min_bytes
            .filter(|_| bucket.mode == BucketMode::WriteBack);
        let (verified, data, inherited_identity) = self
            .receive(
                &shard,
                &input.key,
                input.body.take(),
                validator,
                stream_from,
            )
            .await?;

        let put = Put {
            key: input.key,
            size: verified.length,
            last_modified_ms: now_ms(),
            etag: verified.etag.clone(),
            inherited_identity,
            metadata,
            tags,
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

    /// Streams a request body of `key` into `shard` through `validator`:
    /// inline, or as `EXTENT` records while it arrives ([`Upload`]). It
    /// returns once the body has been read to its end and passed its
    /// checks, and its extents are applied, with the position of the
    /// `UPLOAD_BEGIN` it committed once it reached `stream_from` bytes,
    /// whose write identity its `PUT` inherits (§7.2).
    ///
    /// # Errors
    ///
    /// `400 EntityTooLarge` past [`MAX_OBJECT_BYTES`], which is also the
    /// largest part; `400 RequestTimeout` once a body streamed as extents
    /// has taken longer than `max_body_duration`, waiting for the client's
    /// next bytes included; the body's and the validator's errors.
    async fn receive(
        &self,
        shard: &ShardRef,
        key: &str,
        body: Option<StreamingBlob>,
        mut validator: ChecksumValidator,
        stream_from: Option<u64>,
    ) -> S3Result<(VerifiedBody, PutData, Option<EpochSeq>)> {
        let mut upload = Upload::new(
            self.shards.clone(),
            shard.clone(),
            key.to_owned(),
            self.inline_max_bytes,
            self.extent_bytes,
            self.max_body_duration,
        )
        .streamed(stream_from);
        let mut body = s3s::Body::from(body.unwrap_or_else(empty_blob));
        let mut length = 0u64;
        while let Some(frame) = upload.before_deadline(body.frame()).await? {
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
        let identity = upload.identity();
        let data = upload.finish().await?;
        Ok((verified, data, identity))
    }

    pub(crate) async fn get(
        &self,
        bucket: &BucketDocument,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<GetObjectOutput> {
        let input = &req.input;
        let overrides = overrides!(input).check(&req.extensions)?;
        let conditions = ReadConditions {
            if_match: input.if_match.as_ref(),
            if_none_match: input.if_none_match.as_ref(),
            if_modified_since: input.if_modified_since.as_ref(),
            if_unmodified_since: input.if_unmodified_since.as_ref(),
        };
        let (range, part_number) = (input.range, input.part_number);
        let (found, body) = self
            .open(bucket, &input.key, conditions, |shard, version, object| {
                select(shard, version, object, range, part_number)
            })
            .await?;
        let mut output = GetObjectOutput {
            body: Some(body),
            ..GetObjectOutput::default()
        };
        describe!(output, found, input.checksum_mode, overrides);
        Ok(output)
    }

    /// Resolves a read of `key` whose bytes `selector` chooses, and opens
    /// them: at the remote for a key only there (§9.1), from the hot cache,
    /// from a holder the read plan names, or through a fill for an evicted
    /// version (§9.2). A GET reads this way, and so does UploadPartCopy its
    /// source. When no holder still has the planned version, or a fill finds
    /// the key changed, the key is resolved again, at most
    /// [`MAX_FILL_ROUNDS`] times.
    ///
    /// # Errors
    ///
    /// The errors of [`Objects::resolve`], and `503 ServiceUnavailable`
    /// when the bytes cannot be read.
    async fn open(
        &self,
        bucket: &BucketDocument,
        key: &str,
        conditions: ReadConditions<'_>,
        selector: impl Fn(ShardRef, EpochSeq, ObjectVersion) -> S3Result<Found>,
    ) -> S3Result<(Found, StreamingBlob)> {
        let mut rounds = 0;
        loop {
            let found = self
                .resolve(bucket, key, conditions, &selector, true)
                .await?;
            if found.remote {
                let body = self.read_remote(bucket, key, &found).await?;
                return Ok((found, body));
            }
            if let Some(body) = self.read_hot(bucket, key, &found) {
                return Ok((found, body));
            }
            if !found.evicted {
                if found.holders.is_empty() {
                    // A version whose bytes no replica holds, as a copy
                    // of an evicted object.
                    return Err(download::not_cached());
                }
                match self.read_held(bucket, key, &found).await {
                    Some(body) => return Ok((found, body)),
                    // No holder still has the version: it was replaced or
                    // evicted since the plan, so the key is resolved again.
                    None if rounds + 1 < MAX_FILL_ROUNDS => {
                        rounds += 1;
                        continue;
                    }
                    None => {
                        return Err(s3_error!(
                            ServiceUnavailable,
                            "The object changed while it was read; please retry"
                        ));
                    }
                }
            }
            match self.fill(bucket, key, &found).await? {
                Ok(body) => return Ok((found, body)),
                Err(FillError::Changed) if rounds + 1 < MAX_FILL_ROUNDS => rounds += 1,
                Err(error) => {
                    return Err(s3_error!(
                        ServiceUnavailable,
                        "The object's bytes could not be read from the bucket's target: {error}"
                    ));
                }
            }
        }
    }

    pub(crate) async fn head(
        &self,
        bucket: &BucketDocument,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<HeadObjectOutput> {
        let input = &req.input;
        let overrides = overrides!(input).check(&req.extensions)?;
        let conditions = ReadConditions {
            if_match: input.if_match.as_ref(),
            if_none_match: input.if_none_match.as_ref(),
            if_modified_since: input.if_modified_since.as_ref(),
            if_unmodified_since: input.if_unmodified_since.as_ref(),
        };
        let (range, part_number) = (input.range, input.part_number);
        let selector = |shard, version, object| select(shard, version, object, range, part_number);
        let found = self
            .resolve(bucket, &input.key, conditions, selector, false)
            .await?;
        let mut output = HeadObjectOutput::default();
        describe!(output, found, input.checksum_mode, overrides);
        Ok(output)
    }

    /// The bytes of `found`, a version of `key`, from the hot cache, if it
    /// holds that version (§9.2).
    fn read_hot(&self, bucket: &BucketDocument, key: &str, found: &Found) -> Option<StreamingBlob> {
        if found.bytes.is_empty() {
            return None;
        }
        let (object, version) = hot_names(bucket, key, found);
        let data = self.hot_cache.get(&object, &version, found.bytes.clone())?;
        Some(StreamingBlob::from_bytes(data))
    }

    /// The bytes of `found`, a version of `key`, from the first of the
    /// plan's holders that still holds it (§9.2), or `None` if none does.
    /// A read of the whole object from another node's holder fills the hot
    /// cache, if it admits the object.
    async fn read_held(
        &self,
        bucket: &BucketDocument,
        key: &str,
        found: &Found,
    ) -> Option<StreamingBlob> {
        if found.bytes.is_empty() {
            return Some(empty_blob());
        }
        let local = self.shards.node();
        let whole = found.bytes == (0..found.object.size);
        for holder in self.holders.order(&found.holders, local.as_ref()) {
            let keep = (whole
                && Some(&holder) != local.as_ref()
                && self.hot_cache.admits(found.object.size))
            .then(|| {
                let (object, version) = hot_names(bucket, key, found);
                Keep {
                    cache: self.hot_cache.clone(),
                    object,
                    version,
                    size: found.object.size,
                }
            });
            let opened = self
                .holders
                .open(
                    &self.shards,
                    (&found.shard, &holder),
                    key,
                    found.version,
                    found.layout.clone(),
                    found.bytes.clone(),
                    keep,
                )
                .await;
            match opened {
                Ok(Some(body)) => return Some(body),
                Ok(None) => {
                    tracing::debug!(shard = %found.shard, %holder, key,
                        "a holder no longer has the version");
                }
                Err(error) => {
                    tracing::debug!(shard = %found.shard, %holder, key, %error,
                        "a holder did not serve the version");
                }
            }
        }
        None
    }

    /// Reads the bytes of `found`, an evicted version of `key`, through a
    /// read-through fill (§9.2).
    ///
    /// # Errors
    ///
    /// `503 ServiceUnavailable` for a bucket that is not `write_back` or a
    /// gateway without [`Fills`]; the fill's errors.
    async fn fill(
        &self,
        bucket: &BucketDocument,
        key: &str,
        found: &Found,
    ) -> S3Result<Result<StreamingBlob, FillError>> {
        let Some(fills) = self
            .fills
            .as_ref()
            .filter(|_| bucket.mode == BucketMode::WriteBack)
        else {
            return Err(download::not_cached());
        };
        let read = fills
            .read(&found.shard, key, found.version, found.bytes.clone())
            .await;
        Ok(read.map(|body| download::filled(body, found.bytes.end - found.bytes.start)))
    }

    /// Resolves a read: the key's object, the conditions checked, and the
    /// bytes `selector` chooses to serve, with where to read them if
    /// `planned`.
    async fn resolve(
        &self,
        bucket: &BucketDocument,
        key: &str,
        conditions: ReadConditions<'_>,
        selector: impl Fn(ShardRef, EpochSeq, ObjectVersion) -> S3Result<Found>,
        planned: bool,
    ) -> S3Result<Found> {
        let shard = ShardRef::for_key(bucket, key);
        let found = self.lookup(bucket, &shard, key, planned).await?;
        conditions.check(&found.object)?;
        let mut selected = selector(shard, found.version, found.object)?;
        selected.remote = found.remote;
        selected.evicted = found.evicted;
        selected.layout = found.layout;
        selected.holders = found.holders;
        Ok(selected)
    }
}

/// The names the hot cache keeps `found`, a version of `key` in `bucket`,
/// under: the bucket and key, and the version's position and ETag.
fn hot_names(bucket: &BucketDocument, key: &str, found: &Found) -> (ObjectName, VersionName) {
    let object = ObjectName {
        bucket: bucket.bucket_id.clone(),
        key: key.to_owned(),
    };
    let version = VersionName {
        position: found.version,
        etag: found.object.local_etag.clone(),
    };
    (object, version)
}

/// Selects the bytes of `object` a read serves: a `Range`, a part, or the
/// whole object.
fn select(
    shard: ShardRef,
    version: EpochSeq,
    object: ObjectVersion,
    range: Option<Range>,
    part_number: Option<i32>,
) -> S3Result<Found> {
    let bytes = match (range, part_number) {
        (Some(_), Some(_)) => {
            return Err(s3_error!(
                InvalidRequest,
                "Cannot specify both Range header and partNumber query parameter"
            ));
        }
        (Some(range), None) => {
            let bytes = range
                .check(object.size)
                .map_err(|_| s3_error!(InvalidRange, "The requested range is not satisfiable"))?;
            return Ok(Found::range(shard, version, object, bytes, None));
        }
        (None, Some(number)) => {
            if let Payload::Parts { parts, .. } = &object.payload {
                // A multipart object's parts are numbered in order from
                // 1, whatever numbers they were uploaded as.
                let index = usize::try_from(number)
                    .ok()
                    .and_then(|n| n.checked_sub(1))
                    .filter(|&i| i < parts.len())
                    .ok_or_else(invalid_part_number)?;
                let start: u64 = parts[..index].iter().map(|part| part.size).sum();
                let bytes = start..start + parts[index].size;
                let count = i32::try_from(parts.len()).ok();
                return Ok(Found::range(shard, version, object, bytes, count));
            }
            // An object stored by a single PUT has one part.
            if number != 1 {
                return Err(invalid_part_number());
            }
            0..object.size
        }
        (None, None) => 0..object.size,
    };
    Ok(Found {
        shard,
        version,
        object,
        bytes,
        content_range: None,
        parts_count: None,
        remote: false,
        evicted: false,
        layout: Vec::new(),
        holders: Vec::new(),
    })
}

/// A resolved read.
struct Found {
    shard: ShardRef,
    /// The version's position, which names it (§9.2).
    version: EpochSeq,
    object: ObjectVersion,
    /// The bytes to serve.
    bytes: std::ops::Range<u64>,
    /// `Content-Range`, for a range or part of the object.
    content_range: Option<String>,
    /// The number of parts of a multipart object, for a read of one of
    /// them (`x-amz-mp-parts-count`).
    parts_count: Option<i32>,
    /// Whether the object is only at the remote, which serves its bytes
    /// (§9.1).
    remote: bool,
    /// Whether the entry is evicted, so a fill serves its bytes (§9.2).
    evicted: bool,
    /// The version's bytes as log positions, from the read plan.
    layout: Vec<ExtentRef>,
    /// The nodes the read plan names as holders of the bytes.
    holders: Vec<NodeId>,
}

impl Found {
    /// A read of the `bytes` of `object`, with their `Content-Range` unless
    /// they are empty.
    fn range(
        shard: ShardRef,
        version: EpochSeq,
        object: ObjectVersion,
        bytes: std::ops::Range<u64>,
        parts_count: Option<i32>,
    ) -> Self {
        let content_range = (!bytes.is_empty())
            .then(|| format!("bytes {}-{}/{}", bytes.start, bytes.end - 1, object.size));
        Self {
            shard,
            version,
            object,
            bytes,
            content_range,
            parts_count,
            remote: false,
            evicted: false,
            layout: Vec::new(),
            holders: Vec::new(),
        }
    }
}

/// The response headers a GetObject or HeadObject request overrides with
/// its `response-*` query parameters.
struct Overrides<'a> {
    cache_control: Option<&'a str>,
    content_disposition: Option<&'a str>,
    content_encoding: Option<&'a str>,
    content_language: Option<&'a str>,
    content_type: Option<&'a str>,
    expires: Option<&'a s3s::dto::Timestamp>,
}

/// [`Overrides`] checked, as the header values they become.
#[derive(Default)]
struct Checked {
    cache_control: Option<String>,
    content_disposition: Option<String>,
    content_encoding: Option<String>,
    content_language: Option<String>,
    content_type: Option<String>,
    expires: Option<String>,
}

impl Overrides<'_> {
    /// Checks the overrides of a request.
    ///
    /// # Errors
    ///
    /// `400 InvalidRequest` for overrides in an anonymous request, as S3
    /// answers, and for a value that is not a valid header value.
    fn check(self, extensions: &http::Extensions) -> S3Result<Checked> {
        let texts = [
            self.cache_control,
            self.content_disposition,
            self.content_encoding,
            self.content_language,
            self.content_type,
        ];
        if texts.iter().all(Option::is_none) && self.expires.is_none() {
            return Ok(Checked::default());
        }
        if extensions.get::<Authenticated>().is_none() {
            return Err(s3_error!(
                InvalidRequest,
                "Request specific response headers cannot be used for anonymous GET requests."
            ));
        }
        let value = |text: Option<&str>| -> S3Result<Option<String>> {
            let Some(text) = text else { return Ok(None) };
            http::HeaderValue::from_str(text).map_err(|_| {
                s3_error!(
                    InvalidRequest,
                    "A response header override is not a valid header value"
                )
            })?;
            Ok(Some(text.to_owned()))
        };
        let expires = match self.expires {
            Some(expires) => {
                let mut date = Vec::new();
                expires
                    .format(s3s::dto::TimestampFormat::HttpDate, &mut date)
                    .map_err(|_| s3_error!(InvalidRequest, "response-expires is out of range"))?;
                Some(String::from_utf8_lossy(&date).into_owned())
            }
            None => None,
        };
        Ok(Checked {
            cache_control: value(self.cache_control)?,
            content_disposition: value(self.content_disposition)?,
            content_encoding: value(self.content_encoding)?,
            content_language: value(self.content_language)?,
            content_type: value(self.content_type)?,
            expires,
        })
    }
}

/// The storage class a GetObject or HeadObject answers: the stored one,
/// left out for `STANDARD`, as S3 does.
fn storage_class(object: &ObjectVersion) -> Option<s3s::dto::StorageClass> {
    object
        .storage_class
        .as_deref()
        .filter(|class| *class != s3s::dto::StorageClass::STANDARD)
        .map(|class| s3s::dto::StorageClass::from(class.to_owned()))
}

/// The metadata a PUT or a copy with the `REPLACE` directive stores: the
/// standard headers S3 keeps ([`standard_headers`]), and user metadata
/// under its full header name.
///
/// # Errors
///
/// `400 InvalidArgument` for the write identity's reserved name, and
/// `400 MetadataTooLarge` for user metadata over
/// [`MAX_USER_METADATA_BYTES`] or metadata over what a record holds.
fn stored_metadata(input: &PutObjectInput) -> S3Result<Metadata> {
    metadata_of(standard_headers!(input), input.metadata.as_ref())
}

/// [`stored_metadata`] from the standard headers, as [`standard_headers!`]
/// lists them, and the user metadata of a PutObject, a CopyObject, or a
/// CreateMultipartUpload.
fn metadata_of(
    standard: [(&str, &Option<String>); 6],
    user: Option<&s3s::dto::Metadata>,
) -> S3Result<Metadata> {
    let mut metadata: Metadata = standard
        .into_iter()
        .filter_map(|(name, value)| Some((name.to_owned(), value.clone()?)))
        .collect();
    let mut user_bytes = 0;
    for (name, value) in user.into_iter().flatten() {
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
    // The stored metadata leaves room for a write identity, which a peer
    // cluster that receives the object stores with it (§7.2, §7.8).
    let max_total = MAX_METADATA_LEN - IDENTITY_METADATA_RESERVED;
    if user_bytes > MAX_USER_METADATA_BYTES || total > max_total {
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
