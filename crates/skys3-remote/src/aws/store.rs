//! [`ObjectStore`] for [`AwsS3`]: requests built from the model, and
//! responses converted back.

use std::collections::{BTreeMap, HashMap};
use std::ops::Range;

use aws_sdk_s3::primitives::{ByteStream, DateTime, DateTimeFormat};
use aws_sdk_s3::types::{self as sdk, CompletedMultipartUpload};
use bytes::Bytes;
use skys3_types::ETag;

use super::AwsS3;
use super::error::{malformed, map_sdk_error};
use crate::model::{
    AbortMultipartUpload, CompleteMultipartUpload, CopyObject, CreateMultipartUpload, DeleteObject,
    DeleteOutput, GetObject, GetOutput, HeadObject, ListObjectsV2, ListObjectsV2Output, ListParts,
    ListPartsOutput, ListedObject, ListedPart, MAX_LIST_KEYS, MAX_LIST_PARTS, MetadataDirective,
    ObjectInfo, PutObject, UploadId, UploadPart, VersionId, WriteOutput, WritePrecondition,
};
use crate::{ObjectStore, S3Error, S3ErrorKind, S3Result, UserMetadata};

/// The `x-amz-tagging` value of a tag set: a URL-encoded query string.
pub(super) fn tagging(tags: &BTreeMap<String, String>) -> String {
    fn encode(out: &mut String, text: &str) {
        for byte in text.bytes() {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
                out.push(char::from(byte));
            } else {
                out.push_str(&format!("%{byte:02X}"));
            }
        }
    }
    let mut out = String::new();
    for (i, (key, value)) in tags.iter().enumerate() {
        if i > 0 {
            out.push('&');
        }
        encode(&mut out, key);
        out.push('=');
        encode(&mut out, value);
    }
    out
}

/// The `Expires` header of a write's standard headers, if it is an HTTP
/// date; the SDK sends only a parsed one.
fn expires(headers: &BTreeMap<String, String>) -> Option<DateTime> {
    let value = headers.get("expires")?;
    DateTime::from_str(value, DateTimeFormat::HttpDate).ok()
}

/// `If-None-Match` and `If-Match` header values for a write precondition.
fn precondition_headers(precondition: &WritePrecondition) -> (Option<String>, Option<String>) {
    match precondition {
        WritePrecondition::None => (None, None),
        WritePrecondition::IfAbsent => (Some("*".to_owned()), None),
        WritePrecondition::IfMatch(etag) => (None, Some(etag.to_quoted())),
    }
}

/// Parses an ETag from a response. S3 quotes it; some compatible stores do
/// not.
fn parse_etag(operation: &str, value: Option<&str>) -> S3Result<ETag> {
    let value = value.ok_or_else(|| malformed(operation, "no ETag"))?;
    let parsed = if value.starts_with('"') {
        ETag::from_quoted(value)
    } else {
        ETag::new(value)
    };
    parsed.map_err(|error| malformed(operation, format!("ETag {value:?}: {error}")))
}

fn version(value: Option<&str>) -> Option<VersionId> {
    value.map(|id| VersionId(id.to_owned()))
}

fn size(operation: &str, value: Option<i64>) -> S3Result<u64> {
    let value = value.ok_or_else(|| malformed(operation, "no size"))?;
    u64::try_from(value).map_err(|_| malformed(operation, format!("negative size {value}")))
}

/// Converts a model count to the SDK's `i32`, saturating.
fn count(value: u32) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

/// The `x-amz-meta-*` map of a request, or `None` for empty metadata.
fn metadata_map(metadata: &UserMetadata) -> Option<HashMap<String, String>> {
    (!metadata.is_empty()).then(|| {
        metadata
            .iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect()
    })
}

/// User metadata from a response. Entries that [`UserMetadata`] cannot
/// hold unchanged, which other writers may have stored (non-ASCII values,
/// for example), are left out; the write identity is always ASCII.
fn user_metadata(map: Option<&HashMap<String, String>>) -> UserMetadata {
    let mut metadata = UserMetadata::new();
    for (key, value) in map.into_iter().flatten() {
        // A rejected entry is dropped, as documented above.
        let _ = metadata.insert(key, value.clone());
    }
    metadata
}

/// Parses a `Content-Range: bytes <first>-<last>/<size>` header into the
/// half-open range and the object's size.
fn content_range(value: &str) -> Option<(Range<u64>, u64)> {
    let (range, total) = value.strip_prefix("bytes ")?.split_once('/')?;
    let (first, last) = range.split_once('-')?;
    let first: u64 = first.parse().ok()?;
    let last: u64 = last.parse().ok()?;
    let total: u64 = total.parse().ok()?;
    (first <= last && last < total).then(|| (first..last + 1, total))
}

/// Percent-encodes a key for `x-amz-copy-source`, which the SDK sends as
/// given. Unreserved characters and `/` stay as they are.
fn encode_copy_source(key: &str) -> String {
    let mut encoded = String::with_capacity(key.len());
    for byte in key.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~' | b'/') {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

impl ObjectStore for AwsS3 {
    async fn put_object(&self, request: PutObject) -> S3Result<WriteOutput> {
        const OP: &str = "PutObject";
        let (if_none_match, if_match) = precondition_headers(&request.precondition);
        let output = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(request.key)
            .content_length(i64::try_from(request.body.len()).unwrap_or(i64::MAX))
            .body(ByteStream::from(request.body))
            .set_metadata(metadata_map(&request.metadata))
            .set_content_type(request.content_type)
            .set_cache_control(request.headers.get("cache-control").cloned())
            .set_content_disposition(request.headers.get("content-disposition").cloned())
            .set_content_encoding(request.headers.get("content-encoding").cloned())
            .set_content_language(request.headers.get("content-language").cloned())
            .set_expires(expires(&request.headers))
            .set_tagging((!request.tags.is_empty()).then(|| tagging(&request.tags)))
            .set_content_md5(request.content_md5)
            .set_if_none_match(if_none_match)
            .set_if_match(if_match)
            .send()
            .await
            .map_err(|error| map_sdk_error(OP, error))?;
        Ok(WriteOutput {
            etag: parse_etag(OP, output.e_tag())?,
            version_id: version(output.version_id()),
        })
    }

    async fn get_object(&self, request: GetObject) -> S3Result<GetOutput> {
        const OP: &str = "GetObject";
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(request.key)
            .set_version_id(request.version_id.map(|v| v.0))
            .set_range(request.range.map(|range| range.to_string()))
            .set_if_match(request.if_match.map(|etag| etag.to_quoted()))
            .set_if_none_match(request.if_none_match.map(|etag| etag.to_quoted()))
            .send()
            .await
            .map_err(|error| map_sdk_error(OP, error))?;
        let etag = parse_etag(OP, output.e_tag())?;
        let length = size(OP, output.content_length())?;
        let (range, size) = match output.content_range() {
            Some(value) => {
                let (range, size) = content_range(value)
                    .ok_or_else(|| malformed(OP, format!("Content-Range {value:?}")))?;
                (Some(range), size)
            }
            None => (None, length),
        };
        let info = ObjectInfo {
            etag,
            size,
            version_id: version(output.version_id()),
            metadata: user_metadata(output.metadata()),
            content_type: output.content_type().map(str::to_owned),
        };
        let body = output.body.collect().await.map_err(|error| {
            S3Error::new(
                S3ErrorKind::Timeout,
                format!("{OP}: the response body was cut off: {error}"),
            )
        })?;
        let body: Bytes = body.into_bytes();
        if body.len() as u64 != length {
            return Err(malformed(
                OP,
                format!("{} body bytes, expected {length}", body.len()),
            ));
        }
        Ok(GetOutput { info, body, range })
    }

    async fn head_object(&self, request: HeadObject) -> S3Result<ObjectInfo> {
        const OP: &str = "HeadObject";
        let output = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(request.key)
            .set_version_id(request.version_id.map(|v| v.0))
            .set_if_match(request.if_match.map(|etag| etag.to_quoted()))
            .set_if_none_match(request.if_none_match.map(|etag| etag.to_quoted()))
            .send()
            .await
            .map_err(|error| map_sdk_error(OP, error))?;
        Ok(ObjectInfo {
            etag: parse_etag(OP, output.e_tag())?,
            size: size(OP, output.content_length())?,
            version_id: version(output.version_id()),
            metadata: user_metadata(output.metadata()),
            content_type: output.content_type().map(str::to_owned),
        })
    }

    async fn delete_object(&self, request: DeleteObject) -> S3Result<DeleteOutput> {
        let output = self
            .client
            .delete_object()
            .bucket(&self.bucket)
            .key(request.key)
            .set_version_id(request.version_id.map(|v| v.0))
            .set_if_match(request.if_match.map(|etag| etag.to_quoted()))
            .send()
            .await
            .map_err(|error| map_sdk_error("DeleteObject", error))?;
        Ok(DeleteOutput {
            version_id: version(output.version_id()),
            delete_marker: output.delete_marker().unwrap_or(false),
        })
    }

    async fn list_objects_v2(&self, request: ListObjectsV2) -> S3Result<ListObjectsV2Output> {
        const OP: &str = "ListObjectsV2";
        let output = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(request.prefix)
            .set_delimiter(request.delimiter)
            .max_keys(count(request.max_keys.min(MAX_LIST_KEYS)))
            .set_continuation_token(request.continuation_token)
            .set_start_after(request.start_after)
            .send()
            .await
            .map_err(|error| map_sdk_error(OP, error))?;
        let objects = output
            .contents()
            .iter()
            .map(|object| {
                Ok(ListedObject {
                    key: object
                        .key()
                        .ok_or_else(|| malformed(OP, "an object without a key"))?
                        .to_owned(),
                    etag: parse_etag(OP, object.e_tag())?,
                    size: size(OP, object.size())?,
                })
            })
            .collect::<S3Result<_>>()?;
        let common_prefixes = output
            .common_prefixes()
            .iter()
            .filter_map(|prefix| prefix.prefix().map(str::to_owned))
            .collect();
        let is_truncated = output.is_truncated().unwrap_or(false);
        let next_continuation_token = output.next_continuation_token().map(str::to_owned);
        if is_truncated && next_continuation_token.is_none() {
            return Err(malformed(
                OP,
                "a truncated page without a continuation token",
            ));
        }
        Ok(ListObjectsV2Output {
            objects,
            common_prefixes,
            is_truncated,
            next_continuation_token,
        })
    }

    async fn copy_object(&self, request: CopyObject) -> S3Result<WriteOutput> {
        const OP: &str = "CopyObject";
        let mut source = format!(
            "{}/{}",
            self.bucket,
            encode_copy_source(&request.source_key)
        );
        if let Some(version) = &request.source_version_id {
            source.push_str("?versionId=");
            source.push_str(&encode_copy_source(&version.0));
        }
        let (if_none_match, if_match) = precondition_headers(&request.precondition);
        let mut builder = self
            .client
            .copy_object()
            .bucket(&self.bucket)
            .key(request.key)
            .copy_source(source)
            .set_copy_source_if_match(request.source_if_match.map(|etag| etag.to_quoted()))
            .set_if_none_match(if_none_match)
            .set_if_match(if_match);
        builder = match request.metadata_directive {
            MetadataDirective::Copy => builder.metadata_directive(sdk::MetadataDirective::Copy),
            MetadataDirective::Replace {
                metadata,
                content_type,
            } => builder
                .metadata_directive(sdk::MetadataDirective::Replace)
                .set_metadata(metadata_map(&metadata))
                .set_content_type(content_type),
        };
        let output = builder
            .send()
            .await
            .map_err(|error| map_sdk_error(OP, error))?;
        let result = output
            .copy_object_result()
            .ok_or_else(|| malformed(OP, "no CopyObjectResult"))?;
        Ok(WriteOutput {
            etag: parse_etag(OP, result.e_tag())?,
            version_id: version(output.version_id()),
        })
    }

    async fn create_multipart_upload(&self, request: CreateMultipartUpload) -> S3Result<UploadId> {
        const OP: &str = "CreateMultipartUpload";
        let output = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(request.key)
            .set_metadata(metadata_map(&request.metadata))
            .set_content_type(request.content_type)
            .set_cache_control(request.headers.get("cache-control").cloned())
            .set_content_disposition(request.headers.get("content-disposition").cloned())
            .set_content_encoding(request.headers.get("content-encoding").cloned())
            .set_content_language(request.headers.get("content-language").cloned())
            .set_expires(expires(&request.headers))
            .set_tagging((!request.tags.is_empty()).then(|| tagging(&request.tags)))
            .send()
            .await
            .map_err(|error| map_sdk_error(OP, error))?;
        let upload_id = output
            .upload_id()
            .ok_or_else(|| malformed(OP, "no UploadId"))?;
        Ok(UploadId(upload_id.to_owned()))
    }

    async fn upload_part(&self, request: UploadPart) -> S3Result<ETag> {
        const OP: &str = "UploadPart";
        let output = self
            .client
            .upload_part()
            .bucket(&self.bucket)
            .key(request.key)
            .upload_id(request.upload_id.0)
            .part_number(count(request.part_number))
            .content_length(i64::try_from(request.body.len()).unwrap_or(i64::MAX))
            .body(ByteStream::from(request.body))
            .set_content_md5(request.content_md5)
            .send()
            .await
            .map_err(|error| map_sdk_error(OP, error))?;
        parse_etag(OP, output.e_tag())
    }

    async fn complete_multipart_upload(
        &self,
        request: CompleteMultipartUpload,
    ) -> S3Result<WriteOutput> {
        const OP: &str = "CompleteMultipartUpload";
        let parts = request
            .parts
            .iter()
            .map(|part| {
                sdk::CompletedPart::builder()
                    .part_number(count(part.part_number))
                    .e_tag(part.etag.to_quoted())
                    .build()
            })
            .collect();
        let (if_none_match, if_match) = precondition_headers(&request.precondition);
        let output = self
            .client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(request.key)
            .upload_id(request.upload_id.0)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            )
            .set_if_none_match(if_none_match)
            .set_if_match(if_match)
            .send()
            .await
            .map_err(|error| map_sdk_error(OP, error))?;
        Ok(WriteOutput {
            etag: parse_etag(OP, output.e_tag())?,
            version_id: version(output.version_id()),
        })
    }

    async fn abort_multipart_upload(&self, request: AbortMultipartUpload) -> S3Result<()> {
        self.client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(request.key)
            .upload_id(request.upload_id.0)
            .send()
            .await
            .map_err(|error| map_sdk_error("AbortMultipartUpload", error))?;
        Ok(())
    }

    async fn list_parts(&self, request: ListParts) -> S3Result<ListPartsOutput> {
        const OP: &str = "ListParts";
        let output = self
            .client
            .list_parts()
            .bucket(&self.bucket)
            .key(request.key)
            .upload_id(request.upload_id.0)
            .set_part_number_marker(request.part_number_marker.map(|marker| marker.to_string()))
            .max_parts(count(request.max_parts.min(MAX_LIST_PARTS)))
            .send()
            .await
            .map_err(|error| map_sdk_error(OP, error))?;
        let parts = output
            .parts()
            .iter()
            .map(|part| {
                let number = part
                    .part_number()
                    .and_then(|n| u32::try_from(n).ok())
                    .ok_or_else(|| malformed(OP, "a part without a valid number"))?;
                Ok(ListedPart {
                    part_number: number,
                    etag: parse_etag(OP, part.e_tag())?,
                    size: size(OP, part.size())?,
                })
            })
            .collect::<S3Result<_>>()?;
        let is_truncated = output.is_truncated().unwrap_or(false);
        let next_part_number_marker = match output.next_part_number_marker() {
            Some(marker) => Some(
                marker
                    .parse()
                    .map_err(|_| malformed(OP, format!("NextPartNumberMarker {marker:?}")))?,
            ),
            None => None,
        };
        if is_truncated && next_part_number_marker.is_none() {
            return Err(malformed(OP, "a truncated page without a marker"));
        }
        Ok(ListPartsOutput {
            parts,
            is_truncated,
            next_part_number_marker,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_ranges_parse() {
        assert_eq!(content_range("bytes 0-9/10"), Some((0..10, 10)));
        assert_eq!(content_range("bytes 5-5/100"), Some((5..6, 100)));
        for bad in [
            "bytes 5-4/10",
            "bytes 0-10/10",
            "bytes */10",
            "items 0-1/2",
            "bytes 0-1",
            "bytes a-1/2",
            "bytes 0-b/2",
            "bytes 0-1/c",
        ] {
            assert_eq!(content_range(bad), None, "{bad}");
        }
    }

    #[test]
    fn copy_sources_are_percent_encoded() {
        assert_eq!(encode_copy_source("a/b-c_d.e~f"), "a/b-c_d.e~f");
        assert_eq!(encode_copy_source("a b+c?&%"), "a%20b%2Bc%3F%26%25");
        assert_eq!(encode_copy_source("caf\u{e9}"), "caf%C3%A9");
    }

    #[test]
    fn etags_parse_quoted_or_bare() {
        assert_eq!(parse_etag("Op", Some("\"abc\"")).unwrap().as_str(), "abc");
        assert_eq!(parse_etag("Op", Some("abc")).unwrap().as_str(), "abc");
        assert!(parse_etag("Op", None).is_err());
        assert!(parse_etag("Op", Some("\"a b\"")).is_err());
        assert!(size("Op", Some(-1)).is_err());
        assert!(size("Op", None).is_err());
        assert_eq!(count(u32::MAX), i32::MAX);
    }

    #[test]
    fn foreign_metadata_is_filtered() {
        let map = HashMap::from([
            ("owner".to_owned(), "team".to_owned()),
            ("note".to_owned(), "caf\u{e9}".to_owned()),
        ]);
        let metadata = user_metadata(Some(&map));
        assert_eq!(metadata.get("owner"), Some("team"));
        assert_eq!(metadata.len(), 1);
        assert!(user_metadata(None).is_empty());
        assert_eq!(metadata_map(&UserMetadata::new()), None);
    }
}
