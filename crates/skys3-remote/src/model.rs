//! Requests and responses of the [`ObjectStore`](crate::ObjectStore)
//! operations.
//!
//! Each request is a struct with public fields and a constructor that takes
//! the required ones; the optional ones default to their S3 defaults and are
//! set with `with_*` methods. Names follow the S3 API, so the AWS SDK client
//! maps each field to the parameter of the same name.

use std::collections::BTreeMap;
use std::fmt;
use std::ops::Range;

use bytes::Bytes;
use skys3_types::ETag;

use crate::UserMetadata;

/// The longest object key S3 accepts, in bytes of UTF-8.
pub const MAX_KEY_LEN: usize = 1024;

/// The most keys one `ListObjectsV2` page returns.
pub const MAX_LIST_KEYS: u32 = 1000;

/// The most parts one `ListParts` page returns.
pub const MAX_LIST_PARTS: u32 = 1000;

/// The part numbers S3 accepts.
pub const PART_NUMBERS: std::ops::RangeInclusive<u32> = 1..=10_000;

/// An object version, as the store names it. Opaque.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VersionId(pub String);

impl fmt::Display for VersionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A multipart upload, as the store names it. Opaque.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UploadId(pub String);

impl fmt::Display for UploadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The precondition of a write (design §7.2).
///
/// A store that honors it evaluates it against the key's current object
/// when it applies the write: a failed precondition is `412 Precondition
/// Failed`, `If-Match` on a key without a current object is `404
/// NoSuchKey`, and another write applied to the key while this one was in
/// progress is `409 ConditionalRequestConflict`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub enum WritePrecondition {
    /// Write unconditionally.
    #[default]
    None,
    /// `If-None-Match: *`: write only if the key has no current object.
    IfAbsent,
    /// `If-Match: <etag>`: write only if the key's current object has this
    /// ETag.
    IfMatch(ETag),
}

impl WritePrecondition {
    /// Whether this is a precondition at all.
    pub fn is_some(&self) -> bool {
        !matches!(self, WritePrecondition::None)
    }
}

/// A byte range of a `GetObject`, the typed form of a `Range` header.
///
/// ```
/// use skys3_remote::ByteRange;
///
/// assert_eq!(ByteRange::inclusive(0, 99).unwrap().resolve(50), Some(0..50));
/// assert_eq!(ByteRange::suffix(10).resolve(50), Some(40..50));
/// assert_eq!(ByteRange::from_offset(50).resolve(50), None);
/// assert_eq!(ByteRange::suffix(10).to_string(), "bytes=-10");
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ByteRange(RangeSpec);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum RangeSpec {
    From(u64),
    Inclusive(u64, u64),
    Suffix(u64),
}

impl ByteRange {
    /// `bytes=<first>-`: from `first` to the end.
    pub fn from_offset(first: u64) -> Self {
        ByteRange(RangeSpec::From(first))
    }

    /// `bytes=<first>-<last>`, both inclusive, or `None` if `last < first`.
    pub fn inclusive(first: u64, last: u64) -> Option<Self> {
        (first <= last).then_some(ByteRange(RangeSpec::Inclusive(first, last)))
    }

    /// `bytes=-<len>`: the last `len` bytes.
    pub fn suffix(len: u64) -> Self {
        ByteRange(RangeSpec::Suffix(len))
    }

    /// Returns the bytes this range selects from an object of `size` bytes,
    /// as a half-open range, or `None` if it selects none, which S3 answers
    /// with `416 InvalidRange` (RFC 9110, section 14.1.2).
    ///
    /// A range that extends past the end is cut at the end, and a suffix
    /// longer than the object selects all of it.
    pub fn resolve(self, size: u64) -> Option<Range<u64>> {
        match self.0 {
            RangeSpec::From(first) => (first < size).then_some(first..size),
            RangeSpec::Inclusive(first, last) => {
                (first < size).then(|| first..last.saturating_add(1).min(size))
            }
            RangeSpec::Suffix(len) => (len > 0 && size > 0).then(|| size - len.min(size)..size),
        }
    }
}

impl fmt::Display for ByteRange {
    /// Writes the `Range` header value.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            RangeSpec::From(first) => write!(f, "bytes={first}-"),
            RangeSpec::Inclusive(first, last) => write!(f, "bytes={first}-{last}"),
            RangeSpec::Suffix(len) => write!(f, "bytes=-{len}"),
        }
    }
}

/// `PutObject`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PutObject {
    /// The object key.
    pub key: String,
    /// The object's contents.
    pub body: Bytes,
    /// User metadata, `x-amz-meta-*`.
    pub metadata: UserMetadata,
    /// `Content-Type`.
    pub content_type: Option<String>,
    /// The other standard headers S3 stores with an object, by lowercase
    /// name: `cache-control`, `content-disposition`, `content-encoding`,
    /// `content-language`, and `expires`. Other names are not sent.
    pub headers: BTreeMap<String, String>,
    /// The object's tags (`x-amz-tagging`), by tag key.
    pub tags: BTreeMap<String, String>,
    /// `Content-MD5`: the base64 MD5 digest of the body, which the store
    /// verifies.
    pub content_md5: Option<String>,
    /// `If-None-Match: *` or `If-Match`.
    pub precondition: WritePrecondition,
}

impl PutObject {
    /// The standard headers [`PutObject::headers`] may hold.
    pub const HEADERS: [&'static str; 5] = [
        "cache-control",
        "content-disposition",
        "content-encoding",
        "content-language",
        "expires",
    ];

    /// An unconditional PUT of `body` without metadata.
    pub fn new(key: impl Into<String>, body: impl Into<Bytes>) -> Self {
        PutObject {
            key: key.into(),
            body: body.into(),
            metadata: UserMetadata::new(),
            content_type: None,
            headers: BTreeMap::new(),
            tags: BTreeMap::new(),
            content_md5: None,
            precondition: WritePrecondition::None,
        }
    }

    /// Sets the standard headers other than `Content-Type`.
    pub fn with_headers(mut self, headers: BTreeMap<String, String>) -> Self {
        self.headers = headers;
        self
    }

    /// Sets the tags.
    pub fn with_tags(mut self, tags: BTreeMap<String, String>) -> Self {
        self.tags = tags;
        self
    }

    /// Sets `Content-MD5`.
    pub fn with_content_md5(mut self, content_md5: impl Into<String>) -> Self {
        self.content_md5 = Some(content_md5.into());
        self
    }

    /// Sets the user metadata.
    pub fn with_metadata(mut self, metadata: UserMetadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// Sets the content type.
    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = Some(content_type.into());
        self
    }

    /// Sets the precondition.
    pub fn with_precondition(mut self, precondition: WritePrecondition) -> Self {
        self.precondition = precondition;
        self
    }
}

/// `GetObject`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GetObject {
    /// The object key.
    pub key: String,
    /// A specific version, instead of the current one.
    pub version_id: Option<VersionId>,
    /// `Range`: part of the object instead of all of it.
    pub range: Option<ByteRange>,
    /// `If-Match`: fail with `412` unless the object has this ETag.
    pub if_match: Option<ETag>,
    /// `If-None-Match`: answer `304 Not Modified` if the object has this
    /// ETag.
    pub if_none_match: Option<ETag>,
}

impl GetObject {
    /// A GET of the whole current object.
    pub fn new(key: impl Into<String>) -> Self {
        GetObject {
            key: key.into(),
            ..GetObject::default()
        }
    }

    /// Reads a specific version.
    pub fn with_version_id(mut self, version_id: VersionId) -> Self {
        self.version_id = Some(version_id);
        self
    }

    /// Reads part of the object.
    pub fn with_range(mut self, range: ByteRange) -> Self {
        self.range = Some(range);
        self
    }

    /// Sets `If-Match`.
    pub fn with_if_match(mut self, etag: ETag) -> Self {
        self.if_match = Some(etag);
        self
    }

    /// Sets `If-None-Match`.
    pub fn with_if_none_match(mut self, etag: ETag) -> Self {
        self.if_none_match = Some(etag);
        self
    }
}

/// `HeadObject`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HeadObject {
    /// The object key.
    pub key: String,
    /// A specific version, instead of the current one.
    pub version_id: Option<VersionId>,
    /// `If-Match`: fail with `412` unless the object has this ETag.
    pub if_match: Option<ETag>,
    /// `If-None-Match`: answer `304 Not Modified` if the object has this
    /// ETag.
    pub if_none_match: Option<ETag>,
}

impl HeadObject {
    /// A HEAD of the current object.
    pub fn new(key: impl Into<String>) -> Self {
        HeadObject {
            key: key.into(),
            ..HeadObject::default()
        }
    }

    /// Reads a specific version.
    pub fn with_version_id(mut self, version_id: VersionId) -> Self {
        self.version_id = Some(version_id);
        self
    }

    /// Sets `If-None-Match`.
    pub fn with_if_none_match(mut self, etag: ETag) -> Self {
        self.if_none_match = Some(etag);
        self
    }
}

/// `DeleteObject`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DeleteObject {
    /// The object key.
    pub key: String,
    /// Removes this version permanently. Without it, a versioned store adds
    /// a delete marker, and an unversioned store removes the object.
    pub version_id: Option<VersionId>,
    /// `If-Match`: delete only if the current object has this ETag.
    pub if_match: Option<ETag>,
}

impl DeleteObject {
    /// An unconditional delete of the current object.
    pub fn new(key: impl Into<String>) -> Self {
        DeleteObject {
            key: key.into(),
            ..DeleteObject::default()
        }
    }

    /// Removes a specific version.
    pub fn with_version_id(mut self, version_id: VersionId) -> Self {
        self.version_id = Some(version_id);
        self
    }

    /// Sets `If-Match`.
    pub fn with_if_match(mut self, etag: ETag) -> Self {
        self.if_match = Some(etag);
        self
    }
}

/// `ListObjectsV2`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListObjectsV2 {
    /// Lists only keys that start with this prefix.
    pub prefix: String,
    /// Rolls keys up to their first delimiter after the prefix into common
    /// prefixes.
    pub delimiter: Option<String>,
    /// The most keys and common prefixes to return, at most
    /// [`MAX_LIST_KEYS`] (larger values are treated as that). Zero returns
    /// an empty page that is not truncated, which says nothing about the
    /// keys, so callers never send it.
    pub max_keys: u32,
    /// Continues a listing from the previous page's
    /// [`ListObjectsV2Output::next_continuation_token`].
    pub continuation_token: Option<String>,
    /// Lists only entries, keys and common prefixes alike, that sort after
    /// this string. A common prefix at or before it is left out even if
    /// keys under it sort after it. Ignored with a continuation token.
    pub start_after: Option<String>,
}

impl ListObjectsV2 {
    /// Lists keys under `prefix`, [`MAX_LIST_KEYS`] at a time.
    pub fn new(prefix: impl Into<String>) -> Self {
        ListObjectsV2 {
            prefix: prefix.into(),
            delimiter: None,
            max_keys: MAX_LIST_KEYS,
            continuation_token: None,
            start_after: None,
        }
    }

    /// Sets the delimiter.
    pub fn with_delimiter(mut self, delimiter: impl Into<String>) -> Self {
        self.delimiter = Some(delimiter.into());
        self
    }

    /// Sets the page size.
    pub fn with_max_keys(mut self, max_keys: u32) -> Self {
        self.max_keys = max_keys;
        self
    }

    /// Continues from a previous page.
    pub fn with_continuation_token(mut self, token: impl Into<String>) -> Self {
        self.continuation_token = Some(token.into());
        self
    }

    /// Starts after `key`.
    pub fn with_start_after(mut self, key: impl Into<String>) -> Self {
        self.start_after = Some(key.into());
        self
    }
}

/// What a `CopyObject` does with the source's metadata
/// (`x-amz-metadata-directive`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum MetadataDirective {
    /// `COPY`: keep the source's user metadata and content type.
    #[default]
    Copy,
    /// `REPLACE`: use these instead. SkyS3 always replaces, so the copy
    /// carries its own write identity (design §7.2).
    Replace {
        /// The copy's user metadata.
        metadata: UserMetadata,
        /// The copy's content type.
        content_type: Option<String>,
    },
}

/// `CopyObject` within one bucket.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CopyObject {
    /// The source key.
    pub source_key: String,
    /// A specific source version, instead of the current one.
    pub source_version_id: Option<VersionId>,
    /// `x-amz-copy-source-if-match`: copy only if the source has this ETag.
    pub source_if_match: Option<ETag>,
    /// The destination key.
    pub key: String,
    /// What to do with the source's metadata.
    pub metadata_directive: MetadataDirective,
    /// The destination's precondition.
    pub precondition: WritePrecondition,
}

impl CopyObject {
    /// An unconditional copy of the current `source_key` to `key`, keeping
    /// its metadata.
    pub fn new(source_key: impl Into<String>, key: impl Into<String>) -> Self {
        CopyObject {
            source_key: source_key.into(),
            source_version_id: None,
            source_if_match: None,
            key: key.into(),
            metadata_directive: MetadataDirective::Copy,
            precondition: WritePrecondition::None,
        }
    }

    /// Copies a specific source version.
    pub fn with_source_version_id(mut self, version_id: VersionId) -> Self {
        self.source_version_id = Some(version_id);
        self
    }

    /// Sets `x-amz-copy-source-if-match`.
    pub fn with_source_if_match(mut self, etag: ETag) -> Self {
        self.source_if_match = Some(etag);
        self
    }

    /// Sets the metadata directive.
    pub fn with_metadata_directive(mut self, directive: MetadataDirective) -> Self {
        self.metadata_directive = directive;
        self
    }

    /// Sets the destination's precondition.
    pub fn with_precondition(mut self, precondition: WritePrecondition) -> Self {
        self.precondition = precondition;
        self
    }
}

/// `CreateMultipartUpload`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CreateMultipartUpload {
    /// The object key.
    pub key: String,
    /// The completed object's user metadata, including its write identity
    /// (design §7.3).
    pub metadata: UserMetadata,
    /// The completed object's content type.
    pub content_type: Option<String>,
}

impl CreateMultipartUpload {
    /// Starts an upload to `key` without metadata.
    pub fn new(key: impl Into<String>) -> Self {
        CreateMultipartUpload {
            key: key.into(),
            ..CreateMultipartUpload::default()
        }
    }

    /// Sets the user metadata.
    pub fn with_metadata(mut self, metadata: UserMetadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// Sets the content type.
    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = Some(content_type.into());
        self
    }
}

/// `UploadPart`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UploadPart {
    /// The object key the upload was created for.
    pub key: String,
    /// The upload.
    pub upload_id: UploadId,
    /// The part number, in [`PART_NUMBERS`]. Uploading a number again
    /// replaces the part.
    pub part_number: u32,
    /// The part's contents.
    pub body: Bytes,
}

/// One part named in a `CompleteMultipartUpload`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletedPart {
    /// The part number.
    pub part_number: u32,
    /// The ETag `UploadPart` returned for it.
    pub etag: ETag,
}

/// `CompleteMultipartUpload`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompleteMultipartUpload {
    /// The object key the upload was created for.
    pub key: String,
    /// The upload.
    pub upload_id: UploadId,
    /// The parts that make up the object, in ascending part-number order.
    /// Uploaded parts not named here are discarded.
    pub parts: Vec<CompletedPart>,
    /// `If-None-Match: *` or `If-Match`.
    pub precondition: WritePrecondition,
}

/// `AbortMultipartUpload`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AbortMultipartUpload {
    /// The object key the upload was created for.
    pub key: String,
    /// The upload.
    pub upload_id: UploadId,
}

/// `ListParts`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListParts {
    /// The object key the upload was created for.
    pub key: String,
    /// The upload.
    pub upload_id: UploadId,
    /// Lists parts after this part number.
    pub part_number_marker: Option<u32>,
    /// The most parts to return, at most [`MAX_LIST_PARTS`] (larger values
    /// are treated as that). Zero returns an empty page that is not
    /// truncated, like `ListObjectsV2`'s `max_keys`.
    pub max_parts: u32,
}

impl ListParts {
    /// Lists the upload's parts, [`MAX_LIST_PARTS`] at a time.
    pub fn new(key: impl Into<String>, upload_id: UploadId) -> Self {
        ListParts {
            key: key.into(),
            upload_id,
            part_number_marker: None,
            max_parts: MAX_LIST_PARTS,
        }
    }
}

/// An object's attributes, as `HeadObject` returns them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectInfo {
    /// The ETag.
    pub etag: ETag,
    /// The size in bytes.
    pub size: u64,
    /// The version, on a versioned store.
    pub version_id: Option<VersionId>,
    /// User metadata.
    pub metadata: UserMetadata,
    /// Content type.
    pub content_type: Option<String>,
}

/// The result of a write that creates an object: `PutObject`, `CopyObject`,
/// or `CompleteMultipartUpload`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteOutput {
    /// The new object's ETag, which a later conditional write names in
    /// `If-Match`.
    pub etag: ETag,
    /// The new version, on a versioned store.
    pub version_id: Option<VersionId>,
}

/// The result of `GetObject`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetOutput {
    /// The object's attributes. The size is the whole object's.
    pub info: ObjectInfo,
    /// The bytes read: the whole object, or the selected range.
    pub body: Bytes,
    /// The byte range `body` holds, as a half-open range, if the request
    /// had one (`Content-Range`).
    pub range: Option<Range<u64>>,
}

/// The result of `DeleteObject`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DeleteOutput {
    /// The delete marker that was added, or the version that was removed.
    pub version_id: Option<VersionId>,
    /// Whether the delete added a delete marker, or removed one.
    pub delete_marker: bool,
}

/// One object in a `ListObjectsV2` page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListedObject {
    /// The key.
    pub key: String,
    /// The ETag.
    pub etag: ETag,
    /// The size in bytes.
    pub size: u64,
}

/// A `ListObjectsV2` page.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListObjectsV2Output {
    /// Objects, in key order.
    pub objects: Vec<ListedObject>,
    /// Common prefixes, in order, each ending with the delimiter.
    pub common_prefixes: Vec<String>,
    /// Whether more results follow.
    pub is_truncated: bool,
    /// The token for the next page, if the page is truncated. Opaque.
    pub next_continuation_token: Option<String>,
}

/// One part in a `ListParts` page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListedPart {
    /// The part number.
    pub part_number: u32,
    /// The part's ETag: the MD5 of its contents on S3.
    pub etag: ETag,
    /// The size in bytes.
    pub size: u64,
}

/// A `ListParts` page.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListPartsOutput {
    /// Parts, in part-number order.
    pub parts: Vec<ListedPart>,
    /// Whether more parts follow.
    pub is_truncated: bool,
    /// The marker for the next page, if the page is truncated.
    pub next_part_number_marker: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges_resolve_like_rfc_9110() {
        let inclusive = |first, last| ByteRange::inclusive(first, last).unwrap();
        assert_eq!(ByteRange::inclusive(5, 4), None);
        assert_eq!(inclusive(0, 0).resolve(10), Some(0..1));
        assert_eq!(inclusive(3, 6).resolve(10), Some(3..7));
        assert_eq!(inclusive(3, u64::MAX).resolve(10), Some(3..10));
        assert_eq!(inclusive(10, 20).resolve(10), None);
        assert_eq!(inclusive(0, 5).resolve(0), None);
        assert_eq!(ByteRange::from_offset(0).resolve(10), Some(0..10));
        assert_eq!(ByteRange::from_offset(9).resolve(10), Some(9..10));
        assert_eq!(ByteRange::from_offset(0).resolve(0), None);
        assert_eq!(ByteRange::suffix(3).resolve(10), Some(7..10));
        assert_eq!(ByteRange::suffix(30).resolve(10), Some(0..10));
        assert_eq!(ByteRange::suffix(0).resolve(10), None);
        assert_eq!(ByteRange::suffix(3).resolve(0), None);
    }

    #[test]
    fn ranges_format_as_headers() {
        assert_eq!(ByteRange::from_offset(7).to_string(), "bytes=7-");
        assert_eq!(ByteRange::inclusive(1, 2).unwrap().to_string(), "bytes=1-2");
        assert_eq!(ByteRange::suffix(4).to_string(), "bytes=-4");
    }

    #[test]
    fn request_builders_set_fields() {
        let etag = ETag::new("abc").unwrap();
        let mut metadata = UserMetadata::new();
        metadata.insert("k", "v").unwrap();

        let put = PutObject::new("k", "body")
            .with_metadata(metadata.clone())
            .with_content_type("text/plain")
            .with_precondition(WritePrecondition::IfAbsent);
        assert_eq!(put.body, Bytes::from("body"));
        assert_eq!(put.metadata, metadata);
        assert_eq!(put.content_type.as_deref(), Some("text/plain"));
        assert!(put.precondition.is_some());
        assert!(!WritePrecondition::None.is_some());

        let version = VersionId("v1".into());
        let get = GetObject::new("k")
            .with_version_id(version.clone())
            .with_range(ByteRange::suffix(1))
            .with_if_match(etag.clone())
            .with_if_none_match(etag.clone());
        assert_eq!(get.version_id.as_ref(), Some(&version));
        assert_eq!(get.range, Some(ByteRange::suffix(1)));
        assert_eq!(get.if_match.as_ref(), Some(&etag));
        assert_eq!(get.if_none_match.as_ref(), Some(&etag));

        let head = HeadObject::new("k")
            .with_version_id(version.clone())
            .with_if_none_match(etag.clone());
        assert_eq!(head.version_id.as_ref(), Some(&version));
        assert_eq!(head.if_none_match.as_ref(), Some(&etag));

        let delete = DeleteObject::new("k")
            .with_version_id(version.clone())
            .with_if_match(etag.clone());
        assert_eq!(delete.version_id.as_ref(), Some(&version));
        assert_eq!(delete.if_match.as_ref(), Some(&etag));

        let list = ListObjectsV2::new("p/")
            .with_delimiter("/")
            .with_max_keys(5)
            .with_continuation_token("t")
            .with_start_after("p/a");
        assert_eq!(list.delimiter.as_deref(), Some("/"));
        assert_eq!(list.max_keys, 5);
        assert_eq!(list.continuation_token.as_deref(), Some("t"));
        assert_eq!(list.start_after.as_deref(), Some("p/a"));

        let copy = CopyObject::new("src", "dst")
            .with_source_version_id(version.clone())
            .with_source_if_match(etag.clone())
            .with_metadata_directive(MetadataDirective::Replace {
                metadata: metadata.clone(),
                content_type: None,
            })
            .with_precondition(WritePrecondition::IfMatch(etag.clone()));
        assert_eq!(copy.source_version_id.as_ref(), Some(&version));
        assert_eq!(copy.source_if_match.as_ref(), Some(&etag));
        assert_ne!(copy.metadata_directive, MetadataDirective::Copy);
        assert_eq!(copy.precondition, WritePrecondition::IfMatch(etag));

        let create = CreateMultipartUpload::new("k")
            .with_metadata(metadata.clone())
            .with_content_type("a/b");
        assert_eq!(create.metadata, metadata);
        assert_eq!(create.content_type.as_deref(), Some("a/b"));

        let upload = UploadId("u1".into());
        let parts = ListParts::new("k", upload.clone());
        assert_eq!(parts.max_parts, MAX_LIST_PARTS);
        assert_eq!(upload.to_string(), "u1");
        assert_eq!(version.to_string(), "v1");
    }
}
