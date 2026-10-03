//! The kind-specific bodies of the defined record kinds.
//!
//! Each body is encoded as its kind-specific header, field by field in
//! declaration order, using the primitives of the `wire` module. Only
//! `PUT` and `MPU_PART` (inline data) and `EXTENT` records have a payload.
//! The multipart bodies are in the `multipart` module.

use std::collections::BTreeMap;

use bytes::Bytes;
use skys3_types::RegisterDocument;
use skys3_types::checksum::{Checksum, ChecksumAlgorithm, Checksums};
use skys3_types::{
    BucketId, ETag, EpochSeq, NodeId, ProposalId, Seq, ShardConfig, VersionIdentity, WriteIdentity,
};

use super::error::{FieldError, Problem};
use super::multipart::{MpuAbort, MpuComplete, MpuCreate, MpuPart, PartFlushed};
use super::wire::{Reader, Writer};
use super::{MAX_PAYLOAD_LEN, RecordKind, ShardRef};

/// The longest object key, in bytes (S3's limit).
pub const MAX_KEY_LEN: usize = 1024;

/// The most bytes of object metadata: the sum of every name and value in a
/// [`Metadata`] map.
pub const MAX_METADATA_LEN: usize = 8 * 1024;

/// The stored metadata entry that carries the write identity of a version
/// another SkyS3 cluster wrote (§7.2, §7.8): `x-amz-meta-skys3-wid`, as on
/// any remote.
pub const IDENTITY_METADATA: &str = "x-amz-meta-skys3-wid";

/// The bytes of [`MAX_METADATA_LEN`] that [`IDENTITY_METADATA`] may take,
/// with the longest identity. The metadata of a client's write, and of a
/// peer's `COMMIT` without the entry, stays this much below the limit, so
/// the identity always fits (§7.2).
pub const IDENTITY_METADATA_RESERVED: usize = IDENTITY_METADATA.len() + WriteIdentity::MAX_LEN;

/// The most tags on an object.
pub const MAX_TAGS: usize = 50;

/// The longest tag key, in bytes: S3's 128 characters of up to 4 bytes.
pub const MAX_TAG_KEY_LEN: usize = 512;

/// The longest tag value, in bytes: S3's 256 characters of up to 4 bytes.
pub const MAX_TAG_VALUE_LEN: usize = 1024;

/// The most extents a `PUT` references: a 5 GiB object, S3's largest single
/// PUT, in extents of 64 KiB, the smallest `extent_bytes` allows
/// ([`skys3_types::limits::MAX_EXTENTS_PER_PUT`]).
pub const MAX_EXTENTS: usize = skys3_types::limits::MAX_EXTENTS_PER_PUT;

/// The longest remote version ID, in bytes.
pub const MAX_VERSION_ID_LEN: usize = 1024;

/// The longest storage class name, in bytes.
pub const MAX_STORAGE_CLASS_LEN: usize = 64;

/// An object's stored HTTP metadata, keyed by lowercase header name.
///
/// It holds the standard headers S3 stores with an object (`content-type`,
/// `cache-control`, `content-disposition`, `content-encoding`,
/// `content-language`, `expires`) and user metadata under its full
/// `x-amz-meta-<name>` header name. Names are nonempty HTTP tokens in
/// lowercase; names and values together are at most [`MAX_METADATA_LEN`]
/// bytes.
pub type Metadata = BTreeMap<String, String>;

/// An object's tags, keyed by tag key: at most [`MAX_TAGS`] of them.
pub type TagSet = BTreeMap<String, String>;

/// A committed object write (§5.1). A copy commits as a `PUT` that records
/// its source (§10.1, §11).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Put {
    /// The object key.
    pub key: String,
    /// The object's size in bytes.
    pub size: u64,
    /// When the primary committed the write, in milliseconds since the Unix
    /// epoch: the object's `Last-Modified`, the same on every replica.
    pub last_modified_ms: u64,
    /// The ETag clients see (`local_etag`, §4.2).
    pub etag: ETag,
    /// The position of the record whose write identity this write inherits
    /// (§7.2): the `UPLOAD_BEGIN` of a streamed single PUT. `None` means the
    /// identity names this record.
    pub inherited_identity: Option<EpochSeq>,
    /// The object's metadata.
    pub metadata: Metadata,
    /// The object's tags (`x-amz-tagging`).
    pub tags: TagSet,
    /// The client checksums.
    pub checksums: Checksums,
    /// For a copy, its source.
    pub copy_source: Option<CopySource>,
    /// The object's bytes, inline or in extents.
    pub data: PutData,
}

/// Where a `PUT`'s bytes are.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PutData {
    /// In the record's payload, up to `inline_max_bytes`. Its length is the
    /// object's size.
    Inline(Bytes),
    /// In `EXTENT` records of the same shard, streamed before the `PUT`, in
    /// object order. Their lengths add up to the object's size.
    Extents(Vec<ExtentRef>),
}

/// A reference to an `EXTENT` record by its log position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExtentRef {
    /// The `EXTENT` record's position in the same shard's log. It precedes
    /// the referencing record.
    pub position: EpochSeq,
    /// The extent's payload length, from 1 to [`MAX_PAYLOAD_LEN`].
    pub len: u32,
}

/// The source of a copy (§10.1): what the flusher needs to choose a remote
/// `CopyObject` with `x-amz-copy-source-if-match` (§11).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopySource {
    /// The source bucket.
    pub bucket: BucketId,
    /// The source key.
    pub key: String,
    /// The source version that was copied.
    pub version: VersionIdentity,
    /// The source's `remote_etag`, if it was clean when copied.
    pub remote_etag: Option<ETag>,
}

/// A committed delete: a tombstone until it is flushed (§9.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delete {
    /// The object key.
    pub key: String,
}

/// One extent of a large body (§5.1), in a bulk segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Extent {
    /// The key of the object the extent belongs to.
    pub key: String,
    /// The extent's byte offset within the body it belongs to.
    pub offset: u64,
    /// The extent's bytes: the record's payload, from 1 to
    /// [`MAX_PAYLOAD_LEN`] bytes.
    pub data: Bytes,
}

/// Starts a streamed single PUT and fixes its write identity (§7.2): the
/// record's position names the write, and the `PUT` that completes the
/// upload inherits it ([`Put::inherited_identity`]). It changes no entry,
/// and an upload that fails leaves it naming no write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadBegin {
    /// The object key.
    pub key: String,
}

/// Replaces an object's tags (`PutObjectTagging`, or `DeleteObjectTagging`
/// with an empty set).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tags {
    /// The object key.
    pub key: String,
    /// The new tags.
    pub tags: TagSet,
}

/// The remote accepted a flush (§7.1): `FLUSHED(key, seq, remote_etag,
/// remote_version_id)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Flushed {
    /// The object key.
    pub key: String,
    /// The sequence number of the version that was flushed.
    pub seq: Seq,
    /// The ETag the remote returned. `None` for a flushed delete.
    pub remote_etag: Option<ETag>,
    /// The version ID the remote returned, when it is versioned.
    pub remote_version_id: Option<String>,
}

/// A remote object found by the namespace import (§9.1). It creates a stub
/// only if the key has no entry at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Import {
    /// The object key.
    pub key: String,
    /// The object's size in bytes.
    pub size: u64,
    /// The object's `Last-Modified`, in milliseconds since the Unix epoch.
    pub last_modified_ms: u64,
    /// The object's remote ETag.
    pub etag: ETag,
    /// The object's storage class, if the listing gave one.
    pub storage_class: Option<String>,
}

/// Adopts a remote version that changed out of band (§9.2):
/// `ADOPT(key, remote_etag, remote_version_id, metadata)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Adopt {
    /// The object key.
    pub key: String,
    /// The sequence number of the clean version the read plan named. The
    /// record applies only if the entry is still clean at it.
    pub expected_seq: Seq,
    /// The remote object's size in bytes.
    pub size: u64,
    /// The remote object's `Last-Modified`, in milliseconds since the Unix
    /// epoch.
    pub last_modified_ms: u64,
    /// The remote object's ETag.
    pub remote_etag: ETag,
    /// The remote object's version ID, when the remote is versioned.
    pub remote_version_id: Option<String>,
    /// The remote object's metadata.
    pub metadata: Metadata,
    /// The checksums the remote returned.
    pub checksums: Checksums,
}

/// The kind-specific content of a record.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RecordBody {
    /// `PUT`.
    Put(Put),
    /// `DELETE`.
    Delete(Delete),
    /// `EXTENT`.
    Extent(Extent),
    /// `MPU_CREATE`.
    MpuCreate(MpuCreate),
    /// `MPU_PART`.
    MpuPart(MpuPart),
    /// `MPU_COMPLETE`.
    MpuComplete(MpuComplete),
    /// `MPU_ABORT`.
    MpuAbort(MpuAbort),
    /// `UPLOAD_BEGIN`.
    UploadBegin(UploadBegin),
    /// `TAGS`.
    Tags(Tags),
    /// `FLUSHED`.
    Flushed(Flushed),
    /// `PART_FLUSHED`.
    PartFlushed(PartFlushed),
    /// `IMPORT`.
    Import(Import),
    /// `ADOPT`.
    Adopt(Adopt),
    /// `CONFIG`: the replica's full shard configuration (§6.2). Its bucket,
    /// shard, and epoch are the record's.
    Config(ShardConfig),
    /// `TRUNCATE(shard, epoch, seq)` (§6.6): its sequence number is the
    /// last that stays valid, and every record of the shard at a later
    /// sequence number from an earlier epoch is invalid
    /// ([`truncated_by`](super::truncated_by)). It has no kind-specific
    /// fields.
    Truncate,
}

impl RecordBody {
    /// The record kind.
    #[must_use]
    pub const fn kind(&self) -> RecordKind {
        match self {
            Self::Put(_) => RecordKind::Put,
            Self::Delete(_) => RecordKind::Delete,
            Self::Extent(_) => RecordKind::Extent,
            Self::MpuCreate(_) => RecordKind::MpuCreate,
            Self::MpuPart(_) => RecordKind::MpuPart,
            Self::MpuComplete(_) => RecordKind::MpuComplete,
            Self::MpuAbort(_) => RecordKind::MpuAbort,
            Self::UploadBegin(_) => RecordKind::UploadBegin,
            Self::Tags(_) => RecordKind::Tags,
            Self::Flushed(_) => RecordKind::Flushed,
            Self::PartFlushed(_) => RecordKind::PartFlushed,
            Self::Import(_) => RecordKind::Import,
            Self::Adopt(_) => RecordKind::Adopt,
            Self::Config(_) => RecordKind::Config,
            Self::Truncate => RecordKind::Truncate,
        }
    }

    /// The object key, for kinds that name one.
    #[must_use]
    pub fn key(&self) -> Option<&str> {
        match self {
            Self::Put(Put { key, .. })
            | Self::Delete(Delete { key })
            | Self::Extent(Extent { key, .. })
            | Self::MpuCreate(MpuCreate { key, .. })
            | Self::MpuPart(MpuPart { key, .. })
            | Self::MpuComplete(MpuComplete { key, .. })
            | Self::MpuAbort(MpuAbort { key, .. })
            | Self::UploadBegin(UploadBegin { key })
            | Self::Tags(Tags { key, .. })
            | Self::Flushed(Flushed { key, .. })
            | Self::PartFlushed(PartFlushed { key, .. })
            | Self::Import(Import { key, .. })
            | Self::Adopt(Adopt { key, .. }) => Some(key),
            Self::Config(_) | Self::Truncate => None,
        }
    }

    /// The record's payload: the inline bytes of a `PUT` or an `MPU_PART`,
    /// or an `EXTENT`'s data.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        match self {
            Self::Put(Put { data, .. }) | Self::MpuPart(MpuPart { data, .. }) => data.payload(),
            Self::Extent(Extent { data, .. }) => data,
            _ => &[],
        }
    }

    /// Writes the kind-specific header of a record at `position` in
    /// `shard`, and checks the fields' invariants.
    pub(crate) fn encode(
        &self,
        w: &mut Writer<'_>,
        shard: &ShardRef,
        position: EpochSeq,
    ) -> Result<(), FieldError> {
        match self {
            Self::Put(put) => put.encode(w, position),
            Self::Delete(delete) => w.str16("delete.key", &delete.key, 1, MAX_KEY_LEN),
            Self::Extent(extent) => extent.encode(w),
            Self::MpuCreate(create) => create.encode(w),
            Self::MpuPart(part) => part.encode(w, position),
            Self::MpuComplete(complete) => complete.encode(w, position),
            Self::MpuAbort(abort) => abort.encode(w, position),
            Self::UploadBegin(begin) => w.str16("upload_begin.key", &begin.key, 1, MAX_KEY_LEN),
            Self::Tags(tags) => {
                w.str16("tags.key", &tags.key, 1, MAX_KEY_LEN)?;
                write_tags(w, "tags.tags", &tags.tags)
            }
            Self::Flushed(flushed) => flushed.encode(w),
            Self::PartFlushed(flushed) => flushed.encode(w, position),
            Self::Import(import) => import.encode(w),
            Self::Adopt(adopt) => adopt.encode(w),
            Self::Config(config) => encode_config(w, config, shard, position),
            Self::Truncate => Ok(()),
        }
    }

    /// Decodes the body of a record of a defined `kind` from its
    /// kind-specific header and its payload.
    pub(crate) fn decode(
        kind: RecordKind,
        version: u16,
        header: &[u8],
        payload: &[u8],
        shard: &ShardRef,
        position: EpochSeq,
    ) -> Result<Self, FieldError> {
        let mut r = Reader::new(header, version);
        let body = match kind {
            RecordKind::Put => Self::Put(Put::decode(&mut r, payload, position)?),
            RecordKind::Delete => Self::Delete(Delete {
                key: r.str16("delete.key", 1, MAX_KEY_LEN)?,
            }),
            RecordKind::Extent => Self::Extent(Extent::decode(&mut r, payload)?),
            RecordKind::MpuCreate => Self::MpuCreate(MpuCreate::decode(&mut r)?),
            RecordKind::MpuPart => Self::MpuPart(MpuPart::decode(&mut r, payload, position)?),
            RecordKind::MpuComplete => Self::MpuComplete(MpuComplete::decode(&mut r, position)?),
            RecordKind::MpuAbort => Self::MpuAbort(MpuAbort::decode(&mut r, position)?),
            RecordKind::UploadBegin => Self::UploadBegin(UploadBegin {
                key: r.str16("upload_begin.key", 1, MAX_KEY_LEN)?,
            }),
            RecordKind::Tags => Self::Tags(Tags {
                key: r.str16("tags.key", 1, MAX_KEY_LEN)?,
                tags: read_tags(&mut r, "tags.tags")?,
            }),
            RecordKind::Flushed => Self::Flushed(Flushed::decode(&mut r)?),
            RecordKind::PartFlushed => Self::PartFlushed(PartFlushed::decode(&mut r, position)?),
            RecordKind::Import => Self::Import(Import::decode(&mut r)?),
            RecordKind::Adopt => Self::Adopt(Adopt::decode(&mut r)?),
            RecordKind::Config => Self::Config(decode_config(&mut r, shard, position)?),
            RecordKind::Truncate => Self::Truncate,
            // `RecordHeader::decode` admits only defined kinds.
            _ => {
                return Err(FieldError::new(
                    "header.kind",
                    Problem::Inconsistent("not a defined kind"),
                ));
            }
        };
        r.finish("body")?;
        let expected = body.payload().len();
        if payload.len() != expected {
            return Err(FieldError::new(
                "payload",
                Problem::Inconsistent("the payload length does not match the body"),
            ));
        }
        Ok(body)
    }
}

/// The field names a `PUT`'s data reports errors under.
const PUT_DATA_FIELDS: DataFields = DataFields {
    data: "put.data",
    extents: "put.extents",
};

/// The field names of a body's [`PutData`], for error reports.
#[derive(Clone, Copy)]
pub(super) struct DataFields {
    pub(super) data: &'static str,
    pub(super) extents: &'static str,
}

impl PutData {
    const INLINE: u8 = 0;
    const EXTENTS: u8 = 1;
    /// An encoded [`ExtentRef`]: epoch, seq, and length.
    const EXTENT_REF_LEN: usize = 8 + 8 + 4;

    /// The inline bytes; extents are in records of their own.
    pub(super) fn payload(&self) -> &[u8] {
        match self {
            Self::Inline(data) => data,
            Self::Extents(_) => &[],
        }
    }

    /// Writes the data's tag and extent references, checking that they hold
    /// `size` bytes and precede `position`.
    pub(super) fn encode(
        &self,
        w: &mut Writer<'_>,
        size: u64,
        position: EpochSeq,
        fields: DataFields,
    ) -> Result<(), FieldError> {
        match self {
            Self::Inline(data) => {
                w.u8(Self::INLINE);
                check_size(fields.data, size, data.len() as u64)
            }
            Self::Extents(extents) => {
                w.u8(Self::EXTENTS);
                w.len32(fields.extents, extents.len(), 1, MAX_EXTENTS)?;
                for extent in extents {
                    check_extent(fields.extents, extent, position)?;
                    w.position(extent.position);
                    w.u32(extent.len);
                }
                check_size(fields.extents, size, extents_len(extents))
            }
        }
    }

    /// Reads data written by [`PutData::encode`]; inline data is `payload`.
    pub(super) fn decode(
        r: &mut Reader<'_>,
        size: u64,
        payload: &[u8],
        position: EpochSeq,
        fields: DataFields,
    ) -> Result<Self, FieldError> {
        match r.u8(fields.data)? {
            Self::INLINE => {
                check_size(fields.data, size, payload.len() as u64)?;
                Ok(Self::Inline(Bytes::copy_from_slice(payload)))
            }
            Self::EXTENTS => {
                let count = r.len32(fields.extents, 1, MAX_EXTENTS)?;
                r.check_count(fields.extents, count, Self::EXTENT_REF_LEN)?;
                let mut extents = Vec::with_capacity(count);
                for _ in 0..count {
                    let extent = ExtentRef {
                        position: r.position(fields.extents)?,
                        len: r.u32(fields.extents)?,
                    };
                    check_extent(fields.extents, &extent, position)?;
                    extents.push(extent);
                }
                check_size(fields.extents, size, extents_len(&extents))?;
                Ok(Self::Extents(extents))
            }
            tag => Err(FieldError::new(fields.data, Problem::InvalidTag(tag))),
        }
    }
}

impl Put {
    fn encode(&self, w: &mut Writer<'_>, position: EpochSeq) -> Result<(), FieldError> {
        w.str16("put.key", &self.key, 1, MAX_KEY_LEN)?;
        w.u64(self.size);
        w.u64(self.last_modified_ms);
        write_etag(w, "put.etag", &self.etag)?;
        w.present(self.inherited_identity.is_some());
        if let Some(identity) = self.inherited_identity {
            check_precedes("put.inherited_identity", identity, position)?;
            w.position(identity);
        }
        write_metadata(w, "put.metadata", &self.metadata)?;
        write_tags(w, "put.tags", &self.tags)?;
        write_checksums(w, "put.checksums", &self.checksums)?;
        w.present(self.copy_source.is_some());
        if let Some(source) = &self.copy_source {
            w.str8(
                "put.copy_source.bucket",
                source.bucket.as_str(),
                1,
                BucketId::MAX_LEN,
            )?;
            w.str16("put.copy_source.key", &source.key, 1, MAX_KEY_LEN)?;
            w.u64(source.version.seq.get());
            write_etag(w, "put.copy_source.etag", &source.version.etag)?;
            write_option_etag(
                w,
                "put.copy_source.remote_etag",
                source.remote_etag.as_ref(),
            )?;
        }
        self.data.encode(w, self.size, position, PUT_DATA_FIELDS)
    }

    fn decode(r: &mut Reader<'_>, payload: &[u8], position: EpochSeq) -> Result<Self, FieldError> {
        let key = r.str16("put.key", 1, MAX_KEY_LEN)?;
        let size = r.u64("put.size")?;
        let last_modified_ms = r.u64("put.last_modified_ms")?;
        let etag = read_etag(r, "put.etag")?;
        let inherited_identity = if r.present("put.inherited_identity")? {
            let identity = r.position("put.inherited_identity")?;
            check_precedes("put.inherited_identity", identity, position)?;
            Some(identity)
        } else {
            None
        };
        let metadata = read_metadata(r, "put.metadata")?;
        let tags = read_tags(r, "put.tags")?;
        let checksums = read_checksums(r, "put.checksums")?;
        let copy_source = if r.present("put.copy_source")? {
            let bucket = r.str8("put.copy_source.bucket", 1, BucketId::MAX_LEN)?;
            let bucket = BucketId::new(bucket).map_err(|e| invalid("put.copy_source.bucket", e))?;
            let key = r.str16("put.copy_source.key", 1, MAX_KEY_LEN)?;
            let seq = Seq::new(r.u64("put.copy_source.seq")?);
            let etag = read_etag(r, "put.copy_source.etag")?;
            let remote_etag = read_option_etag(r, "put.copy_source.remote_etag")?;
            Some(CopySource {
                bucket,
                key,
                version: VersionIdentity::new(seq, etag),
                remote_etag,
            })
        } else {
            None
        };
        let data = PutData::decode(r, size, payload, position, PUT_DATA_FIELDS)?;
        Ok(Self {
            key,
            size,
            last_modified_ms,
            etag,
            inherited_identity,
            metadata,
            tags,
            checksums,
            copy_source,
            data,
        })
    }
}

/// The total length of `extents`, or `u64::MAX` if it overflows, which no
/// size can match.
fn extents_len(extents: &[ExtentRef]) -> u64 {
    extents
        .iter()
        .try_fold(0u64, |total, e| total.checked_add(u64::from(e.len)))
        .unwrap_or(u64::MAX)
}

fn check_size(field: &'static str, size: u64, data_len: u64) -> Result<(), FieldError> {
    if size == data_len {
        Ok(())
    } else {
        Err(FieldError::new(
            field,
            Problem::Inconsistent("the data length differs from the object size"),
        ))
    }
}

fn check_extent(
    field: &'static str,
    extent: &ExtentRef,
    position: EpochSeq,
) -> Result<(), FieldError> {
    if extent.len == 0 || extent.len > MAX_PAYLOAD_LEN {
        return Err(FieldError::new(
            field,
            Problem::Invalid(format!(
                "an extent is {} bytes; it must be from 1 to {MAX_PAYLOAD_LEN}",
                extent.len
            )),
        ));
    }
    check_precedes(field, extent.position, position)
}

pub(super) fn check_precedes(
    field: &'static str,
    earlier: EpochSeq,
    position: EpochSeq,
) -> Result<(), FieldError> {
    if earlier < position {
        Ok(())
    } else {
        Err(FieldError::new(
            field,
            Problem::Inconsistent("names a position at or after the record's own"),
        ))
    }
}

impl Extent {
    fn encode(&self, w: &mut Writer<'_>) -> Result<(), FieldError> {
        w.str16("extent.key", &self.key, 1, MAX_KEY_LEN)?;
        w.u64(self.offset);
        Self::check(self.offset, self.data.len())
    }

    fn decode(r: &mut Reader<'_>, payload: &[u8]) -> Result<Self, FieldError> {
        let key = r.str16("extent.key", 1, MAX_KEY_LEN)?;
        let offset = r.u64("extent.offset")?;
        Self::check(offset, payload.len())?;
        Ok(Self {
            key,
            offset,
            data: Bytes::copy_from_slice(payload),
        })
    }

    fn check(offset: u64, len: usize) -> Result<(), FieldError> {
        if len == 0 {
            return Err(FieldError::new("extent.data", Problem::Empty));
        }
        match offset.checked_add(len as u64) {
            Some(_) => Ok(()),
            None => Err(FieldError::new(
                "extent.offset",
                Problem::Inconsistent("the extent ends past the largest offset"),
            )),
        }
    }
}

impl Flushed {
    fn encode(&self, w: &mut Writer<'_>) -> Result<(), FieldError> {
        w.str16("flushed.key", &self.key, 1, MAX_KEY_LEN)?;
        w.u64(self.seq.get());
        write_option_etag(w, "flushed.remote_etag", self.remote_etag.as_ref())?;
        write_option_str(
            w,
            "flushed.remote_version_id",
            self.remote_version_id.as_deref(),
            MAX_VERSION_ID_LEN,
        )
    }

    fn decode(r: &mut Reader<'_>) -> Result<Self, FieldError> {
        Ok(Self {
            key: r.str16("flushed.key", 1, MAX_KEY_LEN)?,
            seq: Seq::new(r.u64("flushed.seq")?),
            remote_etag: read_option_etag(r, "flushed.remote_etag")?,
            remote_version_id: read_option_str(r, "flushed.remote_version_id", MAX_VERSION_ID_LEN)?,
        })
    }
}

impl Import {
    fn encode(&self, w: &mut Writer<'_>) -> Result<(), FieldError> {
        w.str16("import.key", &self.key, 1, MAX_KEY_LEN)?;
        w.u64(self.size);
        w.u64(self.last_modified_ms);
        write_etag(w, "import.etag", &self.etag)?;
        write_option_str(
            w,
            "import.storage_class",
            self.storage_class.as_deref(),
            MAX_STORAGE_CLASS_LEN,
        )
    }

    fn decode(r: &mut Reader<'_>) -> Result<Self, FieldError> {
        Ok(Self {
            key: r.str16("import.key", 1, MAX_KEY_LEN)?,
            size: r.u64("import.size")?,
            last_modified_ms: r.u64("import.last_modified_ms")?,
            etag: read_etag(r, "import.etag")?,
            storage_class: read_option_str(r, "import.storage_class", MAX_STORAGE_CLASS_LEN)?,
        })
    }
}

impl Adopt {
    fn encode(&self, w: &mut Writer<'_>) -> Result<(), FieldError> {
        w.str16("adopt.key", &self.key, 1, MAX_KEY_LEN)?;
        w.u64(self.expected_seq.get());
        w.u64(self.size);
        w.u64(self.last_modified_ms);
        write_etag(w, "adopt.remote_etag", &self.remote_etag)?;
        write_option_str(
            w,
            "adopt.remote_version_id",
            self.remote_version_id.as_deref(),
            MAX_VERSION_ID_LEN,
        )?;
        write_metadata(w, "adopt.metadata", &self.metadata)?;
        write_checksums(w, "adopt.checksums", &self.checksums)
    }

    fn decode(r: &mut Reader<'_>) -> Result<Self, FieldError> {
        Ok(Self {
            key: r.str16("adopt.key", 1, MAX_KEY_LEN)?,
            expected_seq: Seq::new(r.u64("adopt.expected_seq")?),
            size: r.u64("adopt.size")?,
            last_modified_ms: r.u64("adopt.last_modified_ms")?,
            remote_etag: read_etag(r, "adopt.remote_etag")?,
            remote_version_id: read_option_str(r, "adopt.remote_version_id", MAX_VERSION_ID_LEN)?,
            metadata: read_metadata(r, "adopt.metadata")?,
            checksums: read_checksums(r, "adopt.checksums")?,
        })
    }
}

fn encode_config(
    w: &mut Writer<'_>,
    config: &ShardConfig,
    shard: &ShardRef,
    position: EpochSeq,
) -> Result<(), FieldError> {
    if config.bucket_id != shard.bucket || config.shard != shard.shard {
        return Err(FieldError::new(
            "config.shard",
            Problem::Inconsistent("the configuration is for another shard"),
        ));
    }
    if config.epoch != position.epoch {
        return Err(FieldError::new(
            "config.epoch",
            Problem::Inconsistent("the configuration's epoch is not the record's"),
        ));
    }
    config.validate().map_err(|e| invalid("config", e))?;
    w.str8(
        "config.primary",
        config.primary.as_str(),
        1,
        NodeId::MAX_LEN,
    )?;
    for (field, nodes, min) in [
        ("config.members", &config.members, 1),
        ("config.learners", &config.learners, 0),
    ] {
        w.len8(field, nodes.len(), min, u8::MAX.into())?;
        for node in nodes {
            w.str8(field, node.as_str(), 1, NodeId::MAX_LEN)?;
        }
    }
    w.u8(config.min_write_replicas);
    w.u8(config.replicas);
    w.str8(
        "config.proposal_id",
        config.proposal_id.as_str(),
        1,
        ProposalId::MAX_LEN,
    )
}

fn decode_config(
    r: &mut Reader<'_>,
    shard: &ShardRef,
    position: EpochSeq,
) -> Result<ShardConfig, FieldError> {
    let node = |r: &mut Reader<'_>, field| -> Result<NodeId, FieldError> {
        NodeId::new(r.str8(field, 1, NodeId::MAX_LEN)?).map_err(|e| invalid(field, e))
    };
    let primary = node(r, "config.primary")?;
    let mut lists = [Vec::new(), Vec::new()];
    for ((field, min), list) in [("config.members", 1), ("config.learners", 0)]
        .into_iter()
        .zip(&mut lists)
    {
        let count = r.len8(field, min, u8::MAX.into())?;
        for _ in 0..count {
            list.push(node(r, field)?);
        }
    }
    let [members, learners] = lists;
    let min_write_replicas = r.u8("config.min_write_replicas")?;
    let replicas = r.u8("config.replicas")?;
    let proposal_id = r.str8("config.proposal_id", 1, ProposalId::MAX_LEN)?;
    let config = ShardConfig {
        bucket_id: shard.bucket.clone(),
        shard: shard.shard,
        epoch: position.epoch,
        primary,
        members,
        learners,
        min_write_replicas,
        replicas,
        proposal_id: ProposalId::new(proposal_id).map_err(|e| invalid("config.proposal_id", e))?,
    };
    config.validate().map_err(|e| invalid("config", e))?;
    Ok(config)
}

pub(super) fn invalid(field: &'static str, error: impl std::fmt::Display) -> FieldError {
    FieldError::new(field, Problem::Invalid(error.to_string()))
}

pub(super) fn write_etag(
    w: &mut Writer<'_>,
    field: &'static str,
    etag: &ETag,
) -> Result<(), FieldError> {
    w.str16(field, etag.as_str(), 1, ETag::MAX_LEN)
}

pub(super) fn read_etag(r: &mut Reader<'_>, field: &'static str) -> Result<ETag, FieldError> {
    ETag::new(r.str16(field, 1, ETag::MAX_LEN)?).map_err(|e| invalid(field, e))
}

fn write_option_etag(
    w: &mut Writer<'_>,
    field: &'static str,
    etag: Option<&ETag>,
) -> Result<(), FieldError> {
    w.present(etag.is_some());
    etag.map_or(Ok(()), |etag| write_etag(w, field, etag))
}

fn read_option_etag(r: &mut Reader<'_>, field: &'static str) -> Result<Option<ETag>, FieldError> {
    if r.present(field)? {
        read_etag(r, field).map(Some)
    } else {
        Ok(None)
    }
}

fn write_option_str(
    w: &mut Writer<'_>,
    field: &'static str,
    text: Option<&str>,
    max: usize,
) -> Result<(), FieldError> {
    w.present(text.is_some());
    text.map_or(Ok(()), |text| w.str16(field, text, 1, max))
}

fn read_option_str(
    r: &mut Reader<'_>,
    field: &'static str,
    max: usize,
) -> Result<Option<String>, FieldError> {
    if r.present(field)? {
        r.str16(field, 1, max).map(Some)
    } else {
        Ok(None)
    }
}

/// Whether `name` is a lowercase HTTP token (RFC 9110, section 5.6.2).
fn is_metadata_name(name: &str) -> bool {
    !name.is_empty()
        && name.bytes().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || b"!#$%&'*+-.^_`|~".contains(&b)
        })
}

/// Adds an entry's bytes to a running total, checking the total's limit.
fn add_len(
    field: &'static str,
    total: &mut usize,
    entry: &[&str],
    max: usize,
) -> Result<(), FieldError> {
    let len = entry
        .iter()
        .map(|s| s.len())
        .fold(*total, usize::saturating_add);
    if len > max {
        return Err(FieldError::new(
            field,
            Problem::TooLong {
                len: len as u64,
                max: max as u64,
            },
        ));
    }
    *total = len;
    Ok(())
}

pub(super) fn write_metadata(
    w: &mut Writer<'_>,
    field: &'static str,
    metadata: &Metadata,
) -> Result<(), FieldError> {
    w.len16(field, metadata.len(), 0, MAX_METADATA_LEN)?;
    let mut total = 0;
    for (name, value) in metadata {
        add_len(field, &mut total, &[name, value], MAX_METADATA_LEN)?;
        if !is_metadata_name(name) {
            return Err(invalid(
                field,
                format_args!("{name:?} is not a lowercase header name"),
            ));
        }
        w.str16(field, name, 1, MAX_METADATA_LEN)?;
        w.str16(field, value, 0, MAX_METADATA_LEN)?;
    }
    Ok(())
}

pub(super) fn read_metadata(
    r: &mut Reader<'_>,
    field: &'static str,
) -> Result<Metadata, FieldError> {
    let count = r.len16(field, 0, MAX_METADATA_LEN)?;
    let mut metadata = Metadata::new();
    let mut total = 0;
    for _ in 0..count {
        let name = r.str16(field, 1, MAX_METADATA_LEN)?;
        let value = r.str16(field, 0, MAX_METADATA_LEN)?;
        add_len(field, &mut total, &[&name, &value], MAX_METADATA_LEN)?;
        if !is_metadata_name(&name) {
            return Err(invalid(
                field,
                format_args!("{name:?} is not a lowercase header name"),
            ));
        }
        insert_sorted(field, &mut metadata, name, value)?;
    }
    Ok(metadata)
}

pub(super) fn write_tags(
    w: &mut Writer<'_>,
    field: &'static str,
    tags: &TagSet,
) -> Result<(), FieldError> {
    w.len8(field, tags.len(), 0, MAX_TAGS)?;
    for (key, value) in tags {
        w.str16(field, key, 1, MAX_TAG_KEY_LEN)?;
        w.str16(field, value, 0, MAX_TAG_VALUE_LEN)?;
    }
    Ok(())
}

pub(super) fn read_tags(r: &mut Reader<'_>, field: &'static str) -> Result<TagSet, FieldError> {
    let count = r.len8(field, 0, MAX_TAGS)?;
    let mut tags = TagSet::new();
    for _ in 0..count {
        let key = r.str16(field, 1, MAX_TAG_KEY_LEN)?;
        let value = r.str16(field, 0, MAX_TAG_VALUE_LEN)?;
        insert_sorted(field, &mut tags, key, value)?;
    }
    Ok(tags)
}

pub(super) fn write_checksums(
    w: &mut Writer<'_>,
    field: &'static str,
    checksums: &Checksums,
) -> Result<(), FieldError> {
    w.len8(field, checksums.len(), 0, ChecksumAlgorithm::ALL.len())?;
    for (&algorithm, checksum) in checksums {
        checksum.check(algorithm).map_err(|e| invalid(field, e))?;
        w.u8(algorithm.code());
        w.raw(checksum.digest());
        w.u16(checksum.parts().unwrap_or(0));
    }
    Ok(())
}

/// The first format version with a part count after each checksum digest.
const PART_COUNTS_SINCE: u16 = 2;

pub(super) fn read_checksums(
    r: &mut Reader<'_>,
    field: &'static str,
) -> Result<Checksums, FieldError> {
    let part_counts = r.version() >= PART_COUNTS_SINCE;
    let count = r.len8(field, 0, ChecksumAlgorithm::ALL.len())?;
    let mut checksums = Checksums::new();
    for _ in 0..count {
        let code = r.u8(field)?;
        let algorithm = ChecksumAlgorithm::from_code(code)
            .ok_or(FieldError::new(field, Problem::InvalidTag(code)))?;
        let digest = r.take(field, algorithm.digest_len())?;
        let parts = if part_counts { r.u16(field)? } else { 0 };
        let checksum = match parts {
            0 => Checksum::full_object(algorithm, digest),
            parts => Checksum::composite(algorithm, digest, parts.into()),
        }
        .map_err(|e| invalid(field, e))?;
        insert_sorted(field, &mut checksums, algorithm, checksum)?;
    }
    Ok(checksums)
}

/// Inserts a decoded map entry, requiring keys in strictly increasing order
/// so that every map has exactly one encoding.
fn insert_sorted<K: Ord, V>(
    field: &'static str,
    map: &mut BTreeMap<K, V>,
    key: K,
    value: V,
) -> Result<(), FieldError> {
    if map.last_key_value().is_some_and(|(last, _)| *last >= key) {
        return Err(FieldError::new(field, Problem::Unsorted));
    }
    map.insert(key, value);
    Ok(())
}
