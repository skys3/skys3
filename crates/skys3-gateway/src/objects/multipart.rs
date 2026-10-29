//! Multipart uploads (design §7.2, §7.4, §11): CreateMultipartUpload,
//! UploadPart, CompleteMultipartUpload, AbortMultipartUpload, ListParts, and
//! ListMultipartUploads.
//!
//! An upload lives in its key's shard as `MPU_CREATE`, `MPU_PART`,
//! `MPU_COMPLETE`, and `MPU_ABORT` records. Its ID is the position of its
//! `MPU_CREATE` record, which is unique within the shard and is the write
//! identity of the object it completes (§7.2): 32 lowercase hex digits, the
//! epoch and then the sequence number.
//!
//! - **Parts** stream into the shard as a PUT body does, inline or as
//!   `EXTENT` records, through the [`ChecksumValidator`], which also
//!   computes the checksum every part of the upload carries: the one the
//!   upload was created with, or CRC64NVME. A part whose request supplies a
//!   checksum of another algorithm than the upload's is refused (`400
//!   InvalidRequest`); without an upload algorithm, any is checked.
//! - **Completion** checks the listed parts against the stored ones (`400
//!   InvalidPart`, `InvalidPartOrder`, `EntityTooSmall` for a part under
//!   5 MiB other than the last), derives the multipart ETag and the
//!   object's `COMPOSITE` or `FULL_OBJECT` checksum from the parts' (§7.4),
//!   and commits an `MPU_COMPLETE` that names each part's `MPU_PART`
//!   position. The shard applies it only if those are still the stored
//!   parts, so a part uploaded again meanwhile fails the completion instead
//!   of changing it. `If-Match` and `If-None-Match: *` are checked as for
//!   PutObject.
//! - **Abort** commits an `MPU_ABORT`, which releases the parts' bytes for
//!   compaction (§10.3).
//! - **Listing** of uploads merges every shard's uploads in key order, then
//!   by age, with prefixes, delimiters, and markers as S3 has them.

use std::time::{Duration, UNIX_EPOCH};

use s3s::dto::{
    AbortMultipartUploadInput, AbortMultipartUploadOutput, ChecksumAlgorithm as S3Algorithm,
    ChecksumType as S3ChecksumType, CommonPrefix, CompleteMultipartUploadInput,
    CompleteMultipartUploadOutput, CreateMultipartUploadInput, CreateMultipartUploadOutput,
    ListMultipartUploadsInput, ListMultipartUploadsOutput, ListPartsInput, ListPartsOutput,
    MultipartUpload, Part as S3Part, Timestamp, UploadPartInput, UploadPartOutput,
};
use s3s::{S3Request, S3Result, s3_error};
use skys3_index::{Part, Upload};
use skys3_log::RecordBody;
use skys3_log::record::{CompletedPart, MpuAbort, MpuComplete, MpuCreate, MpuPart, UploadChecksum};
use skys3_types::checksum::{Checksum, ChecksumAlgorithm, ChecksumType, Checksums};
use skys3_types::limits::MAX_PARTS;
use skys3_types::{BucketDocument, Epoch, EpochSeq, Seq};

use super::{Objects, entity_too_large, metadata_of, now_ms};
use crate::buckets::shard_error;
use crate::checksum::{
    ChecksumValidator, DEFAULT_ALGORITHM, ExpectedChecksums, MultipartChecksum, MultipartEtag,
};
use crate::conditions::{Precondition, invalid_part, no_such_upload};
use crate::shard::{ShardRef, Shards};
use crate::sigv4::Trailers;

/// The smallest part S3 accepts in a completed upload, but for the last.
pub const MIN_PART_BYTES: u64 = 5 << 20;

/// The largest object a multipart upload makes: 5 TiB, S3's limit.
pub const MAX_MULTIPART_OBJECT_BYTES: u64 = 5 << 40;

/// The most parts or uploads one listing page returns, and the default.
const MAX_PAGE: usize = 1000;

/// The checksum value of `algorithm` an S3 input carries, if any.
macro_rules! checksum_value {
    ($input:expr, $algorithm:expr) => {
        match $algorithm {
            ChecksumAlgorithm::Crc32 => $input.checksum_crc32.as_deref(),
            ChecksumAlgorithm::Crc32c => $input.checksum_crc32c.as_deref(),
            ChecksumAlgorithm::Crc64Nvme => $input.checksum_crc64nvme.as_deref(),
            ChecksumAlgorithm::Sha1 => $input.checksum_sha1.as_deref(),
            ChecksumAlgorithm::Sha256 => $input.checksum_sha256.as_deref(),
            ChecksumAlgorithm::Md5 => None,
        }
    };
}

/// An upload ID: the position of the upload's `MPU_CREATE`.
#[must_use]
pub fn upload_id(upload: EpochSeq) -> String {
    format!("{:016x}{:016x}", upload.epoch.get(), upload.seq.get())
}

/// The upload an upload ID names, or `None` for text no upload ID has.
#[must_use]
pub fn parse_upload_id(id: &str) -> Option<EpochSeq> {
    let hex = |text: &str| {
        text.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            .then(|| u64::from_str_radix(text, 16).ok())
            .flatten()
    };
    if id.len() != 32 || !id.is_ascii() {
        return None;
    }
    let (epoch, seq) = id.split_at(16);
    Some(EpochSeq::new(Epoch::new(hex(epoch)?), Seq::new(hex(seq)?)))
}

/// The upload of `key` that `id` names, which must be open, with up to
/// `limit` of its parts after part `after`, all from one read of the shard.
async fn open_upload<H: Shards>(
    shards: &H,
    shard: &ShardRef,
    key: &str,
    id: &str,
    (after, limit): (u16, usize),
) -> S3Result<(EpochSeq, Upload, Vec<(u16, Part)>)> {
    let position = parse_upload_id(id).ok_or_else(no_such_upload)?;
    let (upload, parts) = shards
        .upload(shard, key, position, after, limit)
        .await
        .map_err(shard_error)?
        .ok_or_else(no_such_upload)?;
    Ok((position, upload, parts))
}

/// No parts: [`open_upload`] only checks that the upload is open.
const NO_PARTS: (u16, usize) = (0, 0);

/// The checksum an upload asks for, from CreateMultipartUpload's
/// `x-amz-checksum-algorithm` and `x-amz-checksum-type`.
fn upload_checksum(
    algorithm: Option<&S3Algorithm>,
    checksum_type: Option<&S3ChecksumType>,
) -> S3Result<Option<UploadChecksum>> {
    let Some(algorithm) = algorithm else {
        if checksum_type.is_some() {
            return Err(s3_error!(
                InvalidRequest,
                "The x-amz-checksum-type header can only be used with the \
                 x-amz-checksum-algorithm header."
            ));
        }
        return Ok(None);
    };
    let name = algorithm.as_str();
    let algorithm: ChecksumAlgorithm = name.parse().map_err(|_| {
        let unsupported = ["MD5", "SHA512", "XXHASH3", "XXHASH64", "XXHASH128"];
        if unsupported.iter().any(|u| u.eq_ignore_ascii_case(name)) {
            s3_error!(
                NotImplemented,
                "The {name} checksum algorithm is not supported"
            )
        } else {
            s3_error!(InvalidRequest, "Checksum algorithm {name} is not valid")
        }
    })?;
    let checksum_type = match checksum_type.map(S3ChecksumType::as_str) {
        None => algorithm.default_multipart_type(),
        Some("COMPOSITE") => Some(ChecksumType::Composite),
        Some("FULL_OBJECT") => Some(ChecksumType::FullObject),
        Some(other) => {
            return Err(s3_error!(
                InvalidRequest,
                "Checksum type {other} is not valid"
            ));
        }
    };
    checksum_type
        .map(|checksum_type| UploadChecksum {
            algorithm,
            checksum_type,
        })
        .filter(|checksum| checksum.is_valid())
        .map(Some)
        .ok_or_else(|| {
            s3_error!(
                InvalidRequest,
                "The {name} algorithm does not support this checksum type"
            )
        })
}

/// The checksum every part of `upload` carries, and the type of the
/// object's.
fn effective_checksum(upload: &Upload) -> UploadChecksum {
    upload.checksum.unwrap_or(UploadChecksum {
        algorithm: DEFAULT_ALGORITHM,
        checksum_type: ChecksumType::FullObject,
    })
}

fn timestamp(ms: u64) -> Timestamp {
    Timestamp::from(UNIX_EPOCH + Duration::from_millis(ms))
}

/// A listing's page size: `max`, at most [`MAX_PAGE`], by default that.
fn page_size(max: Option<i32>, name: &str) -> S3Result<usize> {
    match max {
        None => Ok(MAX_PAGE),
        Some(max) => usize::try_from(max)
            .map(|max| max.min(MAX_PAGE))
            .map_err(|_| s3_error!(InvalidArgument, "{name} must not be negative")),
    }
}

impl<H: Shards> Objects<H> {
    pub(crate) async fn create_upload(
        &self,
        bucket: &BucketDocument,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<CreateMultipartUploadOutput> {
        let input = req.input;
        let metadata = metadata_of(standard_headers!(input), input.metadata.as_ref())?;
        // The tags are the completed object's, kept in the `MPU_CREATE`.
        let tags = input
            .tagging
            .as_deref()
            .map(super::tagging::parse_tagging_header)
            .transpose()?
            .unwrap_or_default();
        let checksum = upload_checksum(
            input.checksum_algorithm.as_ref(),
            input.checksum_type.as_ref(),
        )?;
        let shard = ShardRef::for_key(bucket, &input.key);
        self.admit(bucket, &shard)?;
        let create = MpuCreate {
            key: input.key.clone(),
            initiated_ms: now_ms(),
            metadata,
            tags,
            checksum,
        };
        let position = self
            .shards
            .write(&shard, RecordBody::MpuCreate(create), Precondition::None)
            .await
            .map_err(shard_error)??;
        Ok(CreateMultipartUploadOutput {
            bucket: Some(input.bucket),
            key: Some(input.key),
            upload_id: Some(upload_id(position)),
            checksum_algorithm: checksum.map(|c| S3Algorithm::from_static(c.algorithm.name())),
            checksum_type: checksum.map(|c| S3ChecksumType::from_static(c.checksum_type.as_str())),
            ..CreateMultipartUploadOutput::default()
        })
    }

    pub(crate) async fn upload_part(
        &self,
        bucket: &BucketDocument,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<UploadPartOutput> {
        let S3Request {
            mut input,
            headers,
            extensions,
            ..
        } = req;
        let part_number = u16::try_from(input.part_number)
            .ok()
            .filter(|&n| n >= 1 && u32::from(n) <= MAX_PARTS)
            .ok_or_else(|| {
                s3_error!(
                    InvalidArgument,
                    "Part number must be an integer between 1 and {MAX_PARTS}, inclusive"
                )
            })?;
        if input.content_length.is_some_and(|length| {
            u64::try_from(length).map_or(true, |n| n > super::MAX_OBJECT_BYTES)
        }) {
            return Err(entity_too_large());
        }
        let shard = ShardRef::for_key(bucket, &input.key);
        self.admit(bucket, &shard)?;
        let (upload, state, _) =
            open_upload(&self.shards, &shard, &input.key, &input.upload_id, NO_PARTS).await?;
        let required = effective_checksum(&state).algorithm;
        let expected = ExpectedChecksums::from_headers(&headers)?;
        if let (Some(declared), Some(supplied)) = (state.checksum, expected.checksum())
            && supplied.algorithm() != declared.algorithm
        {
            return Err(s3_error!(
                InvalidRequest,
                "Checksum Type mismatch occurred, expected checksum Type: {}, actual checksum \
                 Type: {}",
                declared.algorithm.name().to_ascii_lowercase(),
                supplied.algorithm().name().to_ascii_lowercase()
            ));
        }
        let trailers = extensions.get::<Trailers>().cloned();
        let validator =
            ChecksumValidator::requiring(expected, trailers, self.pool.clone(), Some(required))?;
        let (verified, data, _) = self
            .receive(&shard, &input.key, input.body.take(), validator, None)
            .await?;
        let part = MpuPart {
            key: input.key,
            upload,
            part_number,
            size: verified.length,
            last_modified_ms: now_ms(),
            etag: verified.etag.clone(),
            checksums: verified.checksums,
            data,
        };
        let checksums = part.checksums.clone();
        self.shards
            .write(&shard, RecordBody::MpuPart(part), Precondition::None)
            .await
            .map_err(shard_error)??;
        let mut output = UploadPartOutput {
            e_tag: Some(s3s::dto::ETag::Strong(verified.etag.as_str().to_owned())),
            ..UploadPartOutput::default()
        };
        set_checksum_values!(output, &checksums);
        Ok(output)
    }

    pub(crate) async fn complete_upload(
        &self,
        bucket: &BucketDocument,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<CompleteMultipartUploadOutput> {
        let input = req.input;
        let condition =
            Precondition::of_write(input.if_match.as_ref(), input.if_none_match.as_ref())?;
        let shard = ShardRef::for_key(bucket, &input.key);
        self.admit(bucket, &shard)?;
        let every_part = (0, MAX_PARTS as usize);
        let (upload, state, stored) = open_upload(
            &self.shards,
            &shard,
            &input.key,
            &input.upload_id,
            every_part,
        )
        .await?;
        let checksum = effective_checksum(&state);
        if let Some(requested) = &input.checksum_type
            && requested.as_str() != checksum.checksum_type.as_str()
        {
            return Err(s3_error!(
                InvalidRequest,
                "The upload was created using the {} checksum type",
                checksum.checksum_type
            ));
        }
        let listed = input
            .multipart_upload
            .as_ref()
            .and_then(|upload| upload.parts.as_deref())
            .unwrap_or_default();
        if listed.is_empty() {
            return Err(s3_error!(
                InvalidRequest,
                "You must specify at least one part"
            ));
        }
        let parts = match_parts(listed, &stored, checksum.algorithm)?;

        let mut etag = MultipartEtag::new();
        let mut object_checksum =
            MultipartChecksum::new(checksum.algorithm, checksum.checksum_type)
                .map_err(|error| s3_error!(InternalError, "{error}"))?;
        let mut size = 0u64;
        for (_, part) in &parts {
            let md5 = part.etag.md5().ok_or_else(invalid_part)?;
            etag.push(&md5)
                .map_err(|error| s3_error!(InvalidRequest, "{error}"))?;
            let digest = part
                .checksums
                .get(&checksum.algorithm)
                .ok_or_else(|| s3_error!(InternalError, "a part has no checksum"))?;
            object_checksum
                .push(digest.digest(), part.size)
                .map_err(|error| s3_error!(InternalError, "{error}"))?;
            size += part.size;
        }
        if size > MAX_MULTIPART_OBJECT_BYTES {
            return Err(entity_too_large());
        }
        if input
            .mpu_object_size
            .is_some_and(|expected| u64::try_from(expected).ok() != Some(size))
        {
            return Err(s3_error!(
                InvalidRequest,
                "The provided x-amz-mp-object-size does not match the object's size"
            ));
        }
        let object_checksum = object_checksum
            .finish()
            .map_err(|error| s3_error!(InternalError, "{error}"))?;
        if let Some(value) = checksum_value!(input, checksum.algorithm) {
            let supplied = Checksum::parse(checksum.algorithm, value)
                .map_err(|_| s3_error!(InvalidRequest, "The checksum value is not valid"))?;
            // The type and part count count too: `<digest>-<parts>` for a
            // composite checksum, and no suffix for a full-object one.
            if supplied != object_checksum {
                return Err(s3_error!(
                    BadDigest,
                    "The {} you specified did not match the calculated checksum.",
                    checksum.algorithm.header_name()
                ));
            }
        }
        let etag = etag.finish().ok_or_else(invalid_part)?;
        let checksums = Checksums::from([(checksum.algorithm, object_checksum)]);
        let complete = MpuComplete {
            key: input.key.clone(),
            upload,
            last_modified_ms: now_ms(),
            size,
            etag: etag.clone(),
            checksums: checksums.clone(),
            parts: parts
                .iter()
                .map(|(number, part)| CompletedPart {
                    number: *number,
                    position: part.position,
                })
                .collect(),
        };
        self.shards
            .write(&shard, RecordBody::MpuComplete(complete), condition)
            .await
            .map_err(shard_error)??;
        let mut output = CompleteMultipartUploadOutput {
            bucket: Some(input.bucket.clone()),
            location: Some(format!("/{}/{}", input.bucket, input.key)),
            key: Some(input.key),
            e_tag: Some(s3s::dto::ETag::Strong(etag.as_str().to_owned())),
            ..CompleteMultipartUploadOutput::default()
        };
        set_checksums!(output, &checksums);
        Ok(output)
    }

    pub(crate) async fn abort_upload(
        &self,
        bucket: &BucketDocument,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<AbortMultipartUploadOutput> {
        let input = req.input;
        if input.if_match_initiated_time.is_some() {
            return Err(s3_error!(
                NotImplemented,
                "x-amz-if-match-initiated-time is not supported"
            ));
        }
        let shard = ShardRef::for_key(bucket, &input.key);
        // The shard refuses the abort too if the upload has gone meanwhile;
        // looking first keeps an ID that was never an upload out of the log.
        let (upload, _, _) =
            open_upload(&self.shards, &shard, &input.key, &input.upload_id, NO_PARTS).await?;
        let abort = MpuAbort {
            key: input.key,
            upload,
        };
        self.shards
            .write(&shard, RecordBody::MpuAbort(abort), Precondition::None)
            .await
            .map_err(shard_error)??;
        Ok(AbortMultipartUploadOutput::default())
    }

    pub(crate) async fn list_parts(
        &self,
        bucket: &BucketDocument,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<ListPartsOutput> {
        let input = req.input;
        let limit = page_size(input.max_parts, "max-parts")?;
        let after = match input.part_number_marker {
            None => 0,
            Some(marker) => u16::try_from(marker.clamp(0, i32::from(u16::MAX))).unwrap_or(0),
        };
        let shard = ShardRef::for_key(bucket, &input.key);
        // One more part than the page holds tells whether it is the last.
        // An empty page (`max-parts=0`) is the last, as in S3.
        let look = if limit == 0 { 0 } else { limit + 1 };
        let (_, state, mut page) = open_upload(
            &self.shards,
            &shard,
            &input.key,
            &input.upload_id,
            (after, look),
        )
        .await?;
        let truncated = page.len() > limit;
        page.truncate(limit);
        let parts = page
            .iter()
            .map(|(number, part)| {
                let mut listed = S3Part {
                    part_number: Some(i32::from(*number)),
                    e_tag: Some(s3s::dto::ETag::Strong(part.etag.as_str().to_owned())),
                    size: i64::try_from(part.size).ok(),
                    last_modified: Some(timestamp(part.last_modified_ms)),
                    ..S3Part::default()
                };
                set_checksum_values!(listed, &part.checksums);
                listed
            })
            .collect();
        Ok(ListPartsOutput {
            bucket: Some(input.bucket),
            key: Some(input.key),
            upload_id: Some(input.upload_id),
            max_parts: i32::try_from(limit).ok(),
            part_number_marker: input.part_number_marker,
            next_part_number_marker: page
                .last()
                .filter(|_| truncated)
                .map(|(number, _)| i32::from(*number)),
            is_truncated: Some(truncated),
            parts: Some(parts),
            checksum_algorithm: state
                .checksum
                .map(|c| S3Algorithm::from_static(c.algorithm.name())),
            checksum_type: state
                .checksum
                .map(|c| S3ChecksumType::from_static(c.checksum_type.as_str())),
            ..ListPartsOutput::default()
        })
    }

    pub(crate) async fn list_uploads(
        &self,
        bucket: &BucketDocument,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<ListMultipartUploadsOutput> {
        let input = req.input;
        let limit = page_size(input.max_uploads, "max-uploads")?;
        let prefix = input.prefix.clone().unwrap_or_default();
        let delimiter = input.delimiter.clone().filter(|d| !d.is_empty());
        let after = match (&input.key_marker, &input.upload_id_marker) {
            (None, _) => None,
            (Some(key), None) => Some((key.clone(), None)),
            (Some(key), Some(id)) => {
                let upload = parse_upload_id(id)
                    .ok_or_else(|| s3_error!(InvalidArgument, "Invalid uploadId marker"))?;
                Some((key.clone(), Some(upload)))
            }
        };
        let mut merge = UploadMerge::new(&self.shards, bucket, &prefix, after.clone(), limit);
        // An empty page (`max-uploads=0`) is the last, as in S3.
        let mut truncated = false;
        // A key marker that is itself a common prefix resumes after it.
        let mut last_prefix = after
            .as_ref()
            .zip(delimiter.as_deref())
            .and_then(|((key, _), d)| common_prefix(key, &prefix, d))
            .filter(|cp| after.as_ref().is_some_and(|(key, _)| key == cp));
        let mut uploads = Vec::new();
        let mut prefixes = Vec::new();
        let mut next_marker = None;
        while limit > 0
            && let Some((key, upload, state)) = merge.next().await?
        {
            let rolled = delimiter
                .as_deref()
                .and_then(|d| common_prefix(&key, &prefix, d));
            if rolled.is_some() && rolled == last_prefix {
                continue;
            }
            if uploads.len() + prefixes.len() == limit {
                truncated = true;
                break;
            }
            if let Some(rolled) = rolled {
                next_marker = Some((rolled.clone(), None));
                last_prefix = Some(rolled.clone());
                prefixes.push(CommonPrefix {
                    prefix: Some(rolled),
                });
                continue;
            }
            next_marker = Some((key.clone(), Some(upload_id(upload))));
            let checksum = state.checksum;
            uploads.push(MultipartUpload {
                key: Some(key),
                upload_id: Some(upload_id(upload)),
                initiated: Some(timestamp(state.initiated_ms)),
                checksum_algorithm: checksum.map(|c| S3Algorithm::from_static(c.algorithm.name())),
                checksum_type: checksum
                    .map(|c| S3ChecksumType::from_static(c.checksum_type.as_str())),
                ..MultipartUpload::default()
            });
        }
        let (next_key_marker, next_upload_id_marker) = match next_marker.filter(|_| truncated) {
            Some((key, id)) => (Some(key), id),
            None => (None, None),
        };
        Ok(ListMultipartUploadsOutput {
            bucket: Some(input.bucket),
            prefix: input.prefix,
            delimiter: input.delimiter,
            key_marker: input.key_marker,
            upload_id_marker: input.upload_id_marker,
            max_uploads: i32::try_from(limit).ok(),
            is_truncated: Some(truncated),
            next_key_marker,
            next_upload_id_marker,
            uploads: Some(uploads),
            common_prefixes: (!prefixes.is_empty()).then_some(prefixes),
            ..ListMultipartUploadsOutput::default()
        })
    }
}

/// The parts a completion lists, matched to the stored parts, in order.
///
/// # Errors
///
/// `400 InvalidPartOrder` for part numbers out of order, `400 InvalidPart`
/// for a part that is missing or whose ETag or checksum differs, and `400
/// EntityTooSmall` for a part under [`MIN_PART_BYTES`] before the last.
fn match_parts<'a>(
    listed: &[s3s::dto::CompletedPart],
    stored: &'a [(u16, Part)],
    algorithm: ChecksumAlgorithm,
) -> S3Result<Vec<(u16, &'a Part)>> {
    let mut parts: Vec<(u16, &Part)> = Vec::with_capacity(listed.len());
    for (index, entry) in listed.iter().enumerate() {
        let number = entry
            .part_number
            .and_then(|n| u16::try_from(n).ok())
            .ok_or_else(invalid_part)?;
        if parts
            .last()
            .is_some_and(|(previous, _)| *previous >= number)
        {
            return Err(s3_error!(
                InvalidPartOrder,
                "The list of parts was not in ascending order. Parts must be ordered by part \
                 number."
            ));
        }
        let part = stored
            .binary_search_by_key(&number, |(n, _)| *n)
            .map(|at| &stored[at].1)
            .map_err(|_| invalid_part())?;
        let etag_matches = entry
            .e_tag
            .as_ref()
            .is_some_and(|etag| etag.value() == part.etag.as_str());
        let checksum_matches = checksum_value!(entry, algorithm).is_none_or(|value| {
            Checksum::parse(algorithm, value)
                .ok()
                .zip(part.checksums.get(&algorithm))
                .is_some_and(|(supplied, stored)| supplied == *stored)
        });
        if !etag_matches || !checksum_matches {
            return Err(invalid_part());
        }
        if index + 1 < listed.len() && part.size < MIN_PART_BYTES {
            return Err(s3_error!(
                EntityTooSmall,
                "Your proposed upload is smaller than the minimum allowed object size."
            ));
        }
        parts.push((number, part));
    }
    Ok(parts)
}

/// The common prefix `key` rolls up into under `prefix` and `delimiter`,
/// if any: the key up to and including the first delimiter after the
/// prefix.
fn common_prefix(key: &str, prefix: &str, delimiter: &str) -> Option<String> {
    let rest = key.strip_prefix(prefix)?;
    let at = rest.find(delimiter)?;
    Some(format!("{prefix}{}", &rest[..at + delimiter.len()]))
}

/// The open uploads of every shard of a bucket, merged in key order and by
/// age, read a page per shard at a time.
struct UploadMerge<'a, H> {
    shards: &'a H,
    prefix: &'a str,
    page: usize,
    cursors: Vec<Cursor>,
}

/// One shard's place in an [`UploadMerge`].
struct Cursor {
    shard: ShardRef,
    buffered: std::collections::VecDeque<(String, EpochSeq, Upload)>,
    after: Option<(String, Option<EpochSeq>)>,
    done: bool,
}

impl Cursor {
    /// The key and upload of the next buffered upload.
    fn front(&self) -> Option<(&str, EpochSeq)> {
        self.buffered
            .front()
            .map(|(key, upload, _)| (key.as_str(), *upload))
    }
}

impl<'a, H: Shards> UploadMerge<'a, H> {
    fn new(
        shards: &'a H,
        bucket: &BucketDocument,
        prefix: &'a str,
        after: Option<(String, Option<EpochSeq>)>,
        limit: usize,
    ) -> Self {
        let cursors = ShardRef::all(bucket)
            .map(|shard| Cursor {
                shard,
                buffered: Default::default(),
                after: after.clone(),
                done: false,
            })
            .collect();
        Self {
            shards,
            prefix,
            page: limit.clamp(1, MAX_PAGE) + 1,
            cursors,
        }
    }

    /// The next upload in order, or `None` once every shard is done.
    async fn next(&mut self) -> S3Result<Option<(String, EpochSeq, Upload)>> {
        for cursor in &mut self.cursors {
            if cursor.buffered.is_empty() && !cursor.done {
                let page = self
                    .shards
                    .uploads(&cursor.shard, self.prefix, cursor.after.take(), self.page)
                    .await
                    .map_err(shard_error)?;
                cursor.done = page.len() < self.page;
                cursor.after = page
                    .last()
                    .map(|(key, upload, _)| (key.clone(), Some(*upload)));
                cursor.buffered.extend(page);
            }
        }
        let next = self
            .cursors
            .iter_mut()
            .filter(|cursor| !cursor.buffered.is_empty())
            .min_by(|a, b| a.front().cmp(&b.front()));
        Ok(next.and_then(|cursor| cursor.buffered.pop_front()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upload_ids_name_positions() {
        let position = EpochSeq::new(Epoch::new(3), Seq::new(0x1234));
        let id = upload_id(position);
        assert_eq!(id, "00000000000000030000000000001234");
        assert_eq!(parse_upload_id(&id), Some(position));
        for bad in [
            "",
            "0000000000000003000000000000123",
            "0000000000000003000000000000123G",
            "000000000000000300000000000012A4",
            "+000000000000003000000000000123",
            "00000000000000030000000000001234\u{e9}",
        ] {
            assert_eq!(parse_upload_id(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn keys_roll_up_into_common_prefixes() {
        assert_eq!(common_prefix("a/b/c", "", "/").as_deref(), Some("a/"));
        assert_eq!(common_prefix("a/b/c", "a/", "/").as_deref(), Some("a/b/"));
        assert_eq!(common_prefix("a/b", "a/b", "/"), None);
        assert_eq!(common_prefix("x/b", "a/", "/"), None);
        assert_eq!(common_prefix("a--b--c", "", "--").as_deref(), Some("a--"));
    }

    #[test]
    fn upload_checksums_follow_s3() {
        let algorithm = |name: &'static str| S3Algorithm::from_static(name);
        let kind = |name: &'static str| S3ChecksumType::from_static(name);
        assert_eq!(upload_checksum(None, None).unwrap(), None);
        assert_eq!(
            upload_checksum(Some(&algorithm("sha256")), None).unwrap(),
            UploadChecksum::of(ChecksumAlgorithm::Sha256)
        );
        assert_eq!(
            upload_checksum(Some(&algorithm("CRC32")), Some(&kind("FULL_OBJECT")))
                .unwrap()
                .unwrap()
                .checksum_type,
            ChecksumType::FullObject
        );
        let code = |a: Option<S3Algorithm>, t: Option<S3ChecksumType>| {
            upload_checksum(a.as_ref(), t.as_ref())
                .unwrap_err()
                .code()
                .as_str()
                .to_owned()
        };
        assert_eq!(code(None, Some(kind("COMPOSITE"))), "InvalidRequest");
        assert_eq!(code(Some(algorithm("SHA512")), None), "NotImplemented");
        assert_eq!(code(Some(algorithm("ADLER")), None), "InvalidRequest");
        assert_eq!(
            code(Some(algorithm("SHA1")), Some(kind("FULL_OBJECT"))),
            "InvalidRequest"
        );
        assert_eq!(
            code(Some(algorithm("CRC32")), Some(kind("PARTIAL"))),
            "InvalidRequest"
        );
    }
}
