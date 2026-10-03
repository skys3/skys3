//! UploadPartCopy (design §7.4, §11): a part of a multipart upload copied
//! from an object anywhere in the cluster, whole or a byte range of it.
//!
//! The source is read as a GET reads it ([`Objects::open`]): from the hot
//! cache, from a holder its shard's read plan names, on this node or another
//! (§9.2), through a fill from the remote for an evicted version of a
//! `write_back` bucket, or at the remote for a key the namespace import has
//! not reached (§9.1). The bytes stream into the destination's shard as an
//! UploadPart body does, inline or as `EXTENT` records, and commit as an
//! `MPU_PART`. The part is then the destination's own: the source may change
//! or go once the copy has answered, and completion, ListParts, reads by
//! part, and the flush treat it as any other part. A copy adds no record
//! kind and no wire operation.
//!
//! - **ETag and checksums.** The part's ETag is the MD5 of the bytes
//!   copied, which is what S3 gives a copied part of an object stored
//!   without SSE-KMS or SSE-C (which SkyS3 rejects). The completed object's
//!   ETag, the MD5 of its parts' MD5s with `-N`, is therefore S3's. The part
//!   carries the upload's checksum ([`effective_checksum`]: the algorithm
//!   it was created with, or CRC64NVME), computed over the bytes copied. The
//!   source's stored checksums describe other bytes, or other part
//!   boundaries, and are never reused.
//! - **Range** (`x-amz-copy-source-range`): `bytes=first-last`, both
//!   offsets given, zero-based and inclusive ([`CopyRange`]). Any other
//!   form answers `400 InvalidArgument`, and a range the source cannot
//!   satisfy, `last` at or past its end, `416 InvalidRange`. As in S3, a
//!   range may be copied only from a source larger than
//!   [`MIN_RANGED_SOURCE_BYTES`] (5 MiB); a valid range of a smaller one
//!   answers `400 InvalidRequest`. Without a range the part is the whole
//!   source, whatever its size. A part is at most 5 GiB
//!   (`400 InvalidRequest`), but a range may come from a larger multipart
//!   source.
//! - **Conditions.** `x-amz-copy-source-if-match`, `-if-none-match`,
//!   `-if-modified-since`, and `-if-unmodified-since` are evaluated against
//!   the source as a GET evaluates its conditions, except that every
//!   failure answers `412 PreconditionFailed`, as for CopyObject. They are
//!   checked before the range.
//! - **What is not copied.** The part takes no metadata or tags from its
//!   source: the completed object has the upload's. Authorization also
//!   requires `s3:GetObject` on the source (`crate::authz`).
//!
//! The request is checked in this order: the part number, the source and
//! the range's form, admission control, the upload (`404 NoSuchUpload`),
//! then the source (`404 NoSuchKey`, `412`, `416`, then `400
//! InvalidRequest` for a range of a small source or a part over 5 GiB).

use std::ops::Range;

use http_body_util::BodyExt;
use s3s::dto::{CopyPartResult, StreamingBlob, UploadPartCopyInput, UploadPartCopyOutput};
use s3s::{S3Error, S3ErrorCode, S3Request, S3Result, s3_error};
use skys3_index::ObjectVersion;
use skys3_log::RecordBody;
use skys3_log::record::{MpuPart, PutData};
use skys3_types::checksum::{Checksum, ChecksumAlgorithm, Checksums};
use skys3_types::{BucketDocument, ETag, EpochSeq};

use super::multipart::{NO_PARTS, effective_checksum, open_upload, part_number, timestamp};
use super::upload::Upload;
use super::{Found, MAX_OBJECT_BYTES, Objects, copy_source, now_ms};
use crate::buckets::shard_error;
use crate::checksum::PooledHasher;
use crate::conditions::{Precondition, ReadConditions, precondition_failed};
use crate::shard::{ShardRef, Shards};

/// The size a copy source must exceed for a range of it to be copied: S3
/// copies a range only of a source larger than 5 MiB, and answers
/// `400 InvalidRequest` otherwise.
pub const MIN_RANGED_SOURCE_BYTES: u64 = 5 << 20;

/// The bytes of a copy source that `x-amz-copy-source-range` names:
/// `bytes=first-last`, zero-based and inclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CopyRange {
    /// The offset of the first byte to copy.
    pub first: u64,
    /// The offset of the last byte to copy.
    pub last: u64,
}

impl CopyRange {
    /// Parses an `x-amz-copy-source-range` value. Unlike a GET's `Range`,
    /// it must give both offsets, as decimal digits, and only one range.
    ///
    /// # Errors
    ///
    /// `400 InvalidArgument` for any other form, and for `first` after
    /// `last`.
    pub fn parse(text: &str) -> S3Result<Self> {
        let offset = |digits: &str| {
            (!digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()))
                .then(|| digits.parse::<u64>().ok())
                .flatten()
        };
        text.strip_prefix("bytes=")
            .and_then(|spec| spec.split_once('-'))
            .and_then(|(first, last)| Some((offset(first)?, offset(last)?)))
            .filter(|(first, last)| first <= last)
            .map(|(first, last)| Self { first, last })
            .ok_or_else(|| {
                s3_error!(
                    InvalidArgument,
                    "The x-amz-copy-source-range value must be of the form bytes=first-last \
                     where first and last are the zero-based offsets of the first and last \
                     bytes to copy"
                )
            })
    }

    /// The bytes this range names of a source of `size` bytes.
    ///
    /// # Errors
    ///
    /// `416 InvalidRange` if the range ends at or past the end of the
    /// source.
    pub fn within(self, size: u64) -> S3Result<Range<u64>> {
        if self.last >= size {
            return Err(s3_error!(
                InvalidRange,
                "The requested range is not satisfiable: the copy source has {size} bytes"
            ));
        }
        Ok(self.first..self.last + 1)
    }
}

impl<H: Shards> Objects<H> {
    /// Copies the source `req` names, in `source_bucket`, or the range of it
    /// the request names, as a part of an upload in `bucket`.
    pub(crate) async fn upload_part_copy(
        &self,
        bucket: &BucketDocument,
        source_bucket: &BucketDocument,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<UploadPartCopyOutput> {
        let input = req.input;
        let part_number = part_number(input.part_number)?;
        let (_, source_key) = copy_source(&input.copy_source)?;
        let range = input
            .copy_source_range
            .as_deref()
            .map(CopyRange::parse)
            .transpose()?;
        let shard = ShardRef::for_key(bucket, &input.key);
        self.admit(bucket, &shard)?;
        let (upload, state, _) =
            open_upload(&self.shards, &shard, &input.key, &input.upload_id, NO_PARTS).await?;
        let algorithm = effective_checksum(&state).algorithm;

        let conditions = ReadConditions {
            if_match: input.copy_source_if_match.as_ref(),
            if_none_match: input.copy_source_if_none_match.as_ref(),
            if_modified_since: input.copy_source_if_modified_since.as_ref(),
            if_unmodified_since: input.copy_source_if_unmodified_since.as_ref(),
        };
        let selector =
            |shard, version, object: ObjectVersion| select(shard, version, object, range);
        let (found, body) = self
            .open(source_bucket, source_key, conditions, selector)
            .await
            .map_err(|error| {
                if *error.code() == S3ErrorCode::NotModified {
                    precondition_failed()
                } else {
                    error
                }
            })?;
        let size = found.bytes.end - found.bytes.start;
        let (etag, checksums, data) = self
            .receive_copy(&shard, &input.key, body, size, algorithm)
            .await?;
        let part = MpuPart {
            key: input.key,
            upload,
            part_number,
            size,
            last_modified_ms: now_ms(),
            etag: etag.clone(),
            checksums: checksums.clone(),
            data,
        };
        let last_modified = timestamp(part.last_modified_ms);
        self.shards
            .write(&shard, RecordBody::MpuPart(part), Precondition::None)
            .await
            .map_err(shard_error)??;
        let mut result = CopyPartResult {
            e_tag: Some(s3s::dto::ETag::Strong(etag.as_str().to_owned())),
            last_modified: Some(last_modified),
            ..CopyPartResult::default()
        };
        set_checksum_values!(result, &checksums);
        Ok(UploadPartCopyOutput {
            copy_part_result: Some(result),
            ..UploadPartCopyOutput::default()
        })
    }

    /// Streams `body`, the `size` bytes of a copy source, into `shard` as
    /// the body of a part of `key`, and returns the part's ETag, its
    /// checksum of `algorithm`, and where its bytes are.
    ///
    /// # Errors
    ///
    /// `503 ServiceUnavailable` if the source's bytes break off or are not
    /// `size` bytes, if they take longer than a body streamed as extents may
    /// ([`too_slow`]), or if the hashing pool is shut down; the shard's
    /// errors.
    async fn receive_copy(
        &self,
        shard: &ShardRef,
        key: &str,
        body: StreamingBlob,
        size: u64,
        algorithm: ChecksumAlgorithm,
    ) -> S3Result<(ETag, Checksums, PutData)> {
        let mut upload = Upload::new(
            self.shards.clone(),
            shard.clone(),
            key.to_owned(),
            self.inline_max_bytes,
            self.extent_bytes,
            self.max_body_duration,
        );
        let mut hasher = PooledHasher::new([ChecksumAlgorithm::Md5, algorithm], self.pool.clone());
        let mut body = s3s::Body::from(body);
        let mut copied = 0u64;
        while let Some(frame) = upload
            .before_deadline(body.frame())
            .await
            .map_err(too_slow)?
        {
            let frame = frame.map_err(|error| {
                tracing::debug!(key, %error, "a copy source's bytes broke off");
                source_broke_off()
            })?;
            let Ok(data) = frame.into_data() else {
                continue;
            };
            copied += data.len() as u64;
            if copied > size {
                return Err(source_broke_off());
            }
            upload.push(&data).await.map_err(too_slow)?;
            hasher.update(data).await.map_err(|_| pool_closed())?;
        }
        if copied != size {
            return Err(source_broke_off());
        }
        let digests = hasher.finish().await.map_err(|_| pool_closed())?;
        let md5 = digests
            .get(&ChecksumAlgorithm::Md5)
            .and_then(|md5| <[u8; 16]>::try_from(md5.as_slice()).ok())
            .ok_or_else(pool_closed)?;
        let checksum = digests
            .get(&algorithm)
            .and_then(|digest| Checksum::full_object(algorithm, digest).ok())
            .ok_or_else(pool_closed)?;
        let data = upload.finish().await.map_err(too_slow)?;
        Ok((
            ETag::from_md5(&md5),
            Checksums::from([(algorithm, checksum)]),
            data,
        ))
    }
}

/// Selects the bytes of `object`, a copy source, that a part copies: the
/// `range`, or the whole object.
///
/// # Errors
///
/// `416 InvalidRange` for a range the object cannot satisfy, and
/// `400 InvalidRequest` for more than [`MAX_OBJECT_BYTES`].
fn select(
    shard: ShardRef,
    version: EpochSeq,
    object: ObjectVersion,
    range: Option<CopyRange>,
) -> S3Result<Found> {
    let bytes = match range {
        // The range is checked against the source first, so a range past
        // the end of a small source is still `416 InvalidRange`.
        Some(range) => {
            let bytes = range.within(object.size)?;
            if object.size <= MIN_RANGED_SOURCE_BYTES {
                return Err(s3_error!(
                    InvalidRequest,
                    "The specified copy source is not supported as a byte-range copy source"
                ));
            }
            bytes
        }
        None => 0..object.size,
    };
    if bytes.end - bytes.start > MAX_OBJECT_BYTES {
        return Err(s3_error!(
            InvalidRequest,
            "The specified copy source is larger than the maximum allowable size for a copy \
             source: {MAX_OBJECT_BYTES}"
        ));
    }
    Ok(Found::range(shard, version, object, bytes, None))
}

fn source_broke_off() -> S3Error {
    s3_error!(
        ServiceUnavailable,
        "The copy source's bytes could not be read; please retry"
    )
}

/// A body streamed as extents has a deadline (`max_body_duration`), which
/// keeps compaction from dropping its first extents before its record names
/// them. For a client's body, missing it is the client's fault
/// (`400 RequestTimeout`); for a copy, it is the source that was slow, so
/// the copy answers `503` and may be retried.
fn too_slow(error: S3Error) -> S3Error {
    if *error.code() == S3ErrorCode::RequestTimeout {
        s3_error!(
            ServiceUnavailable,
            "The copy source was read too slowly; please retry"
        )
    } else {
        error
    }
}

fn pool_closed() -> S3Error {
    s3_error!(ServiceUnavailable, "The hashing pool is shut down")
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn code(text: &str) -> S3ErrorCode {
        CopyRange::parse(text).unwrap_err().code().clone()
    }

    #[test]
    fn ranges_give_both_offsets() {
        assert_eq!(
            CopyRange::parse("bytes=0-0").unwrap(),
            CopyRange { first: 0, last: 0 }
        );
        assert_eq!(
            CopyRange::parse("bytes=5242880-10485759").unwrap(),
            CopyRange {
                first: 5 << 20,
                last: (10 << 20) - 1
            }
        );
        assert_eq!(CopyRange::parse("bytes=007-9").unwrap().first, 7);
        // The forms s3-tests' test_multipart_copy_improper_range sends, and
        // others a GET's Range takes but a copy does not.
        for text in [
            "0-2",
            "bytes=0",
            "bytes=hello-world",
            "bytes=0-bar",
            "bytes=hello-",
            "bytes=0-2,3-5",
            "bytes=-5",
            "bytes=5-",
            "bytes=3-2",
            "bytes=+1-2",
            "bytes= 1-2",
            "Bytes=1-2",
            "bytes=1-18446744073709551616",
            "",
        ] {
            assert_eq!(code(text), S3ErrorCode::InvalidArgument, "{text}");
        }
    }

    #[test]
    fn ranges_must_end_within_the_source() {
        let range = CopyRange { first: 0, last: 9 };
        assert_eq!(range.within(10).unwrap(), 0..10);
        let error = range.within(9).unwrap_err();
        assert_eq!(*error.code(), S3ErrorCode::InvalidRange);
        assert_eq!(
            error.status_code(),
            Some(http::StatusCode::RANGE_NOT_SATISFIABLE)
        );
        // s3-tests' test_multipart_copy_invalid_range: 22 bytes of 5.
        let error = CopyRange { first: 0, last: 21 }.within(5).unwrap_err();
        assert_eq!(*error.code(), S3ErrorCode::InvalidRange);
        assert!(CopyRange { first: 0, last: 0 }.within(0).is_err());
    }

    #[test]
    fn a_slow_source_is_the_servers_delay() {
        let late = too_slow(s3_error!(RequestTimeout, "late"));
        assert_eq!(*late.code(), S3ErrorCode::ServiceUnavailable);
        let other = too_slow(s3_error!(InternalError, "broken"));
        assert_eq!(*other.code(), S3ErrorCode::InternalError);
    }

    fn test_shard() -> ShardRef {
        ShardRef {
            bucket: skys3_types::BucketId::new("b-test").unwrap(),
            shard: skys3_types::ShardId::new(0),
        }
    }

    /// An object of `size` bytes, whose bytes `select` never reads.
    fn sized(size: u64) -> ObjectVersion {
        ObjectVersion {
            size,
            last_modified_ms: 0,
            local_etag: ETag::from_md5(&[0; 16]),
            write_identity: None,
            metadata: skys3_log::record::Metadata::new(),
            tags: skys3_log::record::TagSet::new(),
            checksums: Checksums::new(),
            storage_class: None,
            copy_source: None,
            payload: skys3_index::Payload::None,
        }
    }

    fn selected(size: u64, range: Option<CopyRange>) -> S3Result<Range<u64>> {
        select(test_shard(), EpochSeq::default(), sized(size), range).map(|found| found.bytes)
    }

    #[test]
    fn ranges_need_a_source_over_five_mib() {
        let first_byte = Some(CopyRange { first: 0, last: 0 });
        let refused = selected(MIN_RANGED_SOURCE_BYTES, first_byte).unwrap_err();
        assert_eq!(*refused.code(), S3ErrorCode::InvalidRequest);
        assert_eq!(
            selected(MIN_RANGED_SOURCE_BYTES + 1, first_byte).unwrap(),
            0..1
        );
        // Whole small sources copy, and a range past the end of one is
        // still unsatisfiable rather than unsupported.
        assert_eq!(selected(5, None).unwrap(), 0..5);
        assert_eq!(selected(0, None).unwrap(), 0..0);
        let past = selected(5, Some(CopyRange { first: 0, last: 21 })).unwrap_err();
        assert_eq!(*past.code(), S3ErrorCode::InvalidRange);
    }

    #[test]
    fn a_part_is_at_most_five_gib() {
        let shard = test_shard();
        let object = sized(7 << 30);
        let whole = select(shard.clone(), EpochSeq::default(), object.clone(), None);
        assert_eq!(*whole.err().unwrap().code(), S3ErrorCode::InvalidRequest);
        let range = CopyRange {
            first: 1 << 30,
            last: (1 << 30) + MAX_OBJECT_BYTES - 1,
        };
        let found = select(
            shard.clone(),
            EpochSeq::default(),
            object.clone(),
            Some(range),
        );
        assert_eq!(found.unwrap().bytes, range.first..range.last + 1);
        let range = CopyRange {
            last: range.last + 1,
            ..range
        };
        let error = select(shard, EpochSeq::default(), object, Some(range))
            .err()
            .unwrap();
        assert_eq!(*error.code(), S3ErrorCode::InvalidRequest);
    }

    proptest! {
        #[test]
        fn ranges_round_trip(first in any::<u64>(), len in any::<u64>(), size in any::<u64>()) {
            let last = first.saturating_add(len);
            let range = CopyRange::parse(&format!("bytes={first}-{last}")).unwrap();
            prop_assert_eq!(range, CopyRange { first, last });
            match range.within(size) {
                Ok(bytes) => {
                    prop_assert!(last < size);
                    prop_assert_eq!(bytes.end - bytes.start, last - first + 1);
                }
                Err(_) => prop_assert!(last >= size),
            }
        }

        #[test]
        fn parsing_never_panics_and_accepts_only_the_one_form(text in "\\PC{0,40}") {
            if let Ok(range) = CopyRange::parse(&text) {
                prop_assert!(range.first <= range.last);
                prop_assert!(text.starts_with("bytes="));
                prop_assert_eq!(text.matches('-').count(), 1);
            }
        }
    }
}
