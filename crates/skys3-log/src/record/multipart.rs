//! The bodies of the multipart upload records: `MPU_CREATE`, `MPU_PART`,
//! `MPU_COMPLETE`, and `MPU_ABORT` (§7.2, §7.4), and `PART_FLUSHED`, which
//! follows an upload's remote counterpart while it streams (§7.3).
//!
//! An upload is named by the position of its `MPU_CREATE` record, which is
//! unique within the shard. The other records name it by that position, and
//! the object an upload completes inherits it as its write identity (§7.2).

use skys3_types::ETag;
use skys3_types::EpochSeq;
use skys3_types::checksum::{ChecksumAlgorithm, ChecksumType, Checksums};
use skys3_types::limits::MAX_PARTS;

use super::body::{
    DataFields, check_precedes, invalid, read_checksums, read_etag, read_metadata, read_tags,
    write_checksums, write_etag, write_metadata, write_tags,
};
use super::error::{FieldError, Problem};
use super::wire::{Reader, Writer};
use super::{MAX_KEY_LEN, Metadata, PutData, TagSet};

/// Opens a multipart upload of `key`: `MPU_CREATE`.
///
/// It holds what the completed object takes from the upload's start: its
/// metadata, tags, and checksum, and the upload's position, which is its
/// write identity (§7.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MpuCreate {
    /// The object key.
    pub key: String,
    /// When the primary opened the upload, in milliseconds since the Unix
    /// epoch (ListMultipartUploads' `Initiated`).
    pub initiated_ms: u64,
    /// The completed object's metadata.
    pub metadata: Metadata,
    /// The completed object's tags.
    pub tags: TagSet,
    /// The checksum the client chose for the upload, if it chose one
    /// (§7.4).
    pub checksum: Option<UploadChecksum>,
}

/// The checksum a multipart upload's parts carry, and how the object's is
/// derived from them (§7.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UploadChecksum {
    /// The algorithm: any but MD5.
    pub algorithm: ChecksumAlgorithm,
    /// `COMPOSITE` for a checksum of the parts' checksums, `FULL_OBJECT` for
    /// a CRC of the whole object.
    pub checksum_type: ChecksumType,
}

impl UploadChecksum {
    /// The checksum of `algorithm` of the type S3 gives it by default.
    #[must_use]
    pub fn of(algorithm: ChecksumAlgorithm) -> Option<Self> {
        Some(Self {
            algorithm,
            checksum_type: algorithm.default_multipart_type()?,
        })
    }

    /// Whether S3 defines a multipart checksum of this algorithm and type.
    #[must_use]
    pub const fn is_valid(self) -> bool {
        match self.checksum_type {
            ChecksumType::Composite => self.algorithm.supports_composite(),
            ChecksumType::FullObject => self.algorithm.supports_full_object_multipart(),
        }
    }

    fn encode(self, w: &mut Writer<'_>) -> Result<(), FieldError> {
        if !self.is_valid() {
            return Err(Self::not_defined(self.algorithm, self.checksum_type));
        }
        w.u8(self.algorithm.code());
        w.u8(match self.checksum_type {
            ChecksumType::FullObject => Self::FULL_OBJECT,
            ChecksumType::Composite => Self::COMPOSITE,
        });
        Ok(())
    }

    fn decode(r: &mut Reader<'_>) -> Result<Self, FieldError> {
        let code = r.u8(Self::FIELD)?;
        let algorithm = ChecksumAlgorithm::from_code(code)
            .ok_or(FieldError::new(Self::FIELD, Problem::InvalidTag(code)))?;
        let checksum_type = match r.u8(Self::FIELD)? {
            Self::FULL_OBJECT => ChecksumType::FullObject,
            Self::COMPOSITE => ChecksumType::Composite,
            tag => return Err(FieldError::new(Self::FIELD, Problem::InvalidTag(tag))),
        };
        let checksum = Self {
            algorithm,
            checksum_type,
        };
        if !checksum.is_valid() {
            return Err(Self::not_defined(algorithm, checksum_type));
        }
        Ok(checksum)
    }

    const FIELD: &'static str = "mpu_create.checksum";
    const FULL_OBJECT: u8 = 0;
    const COMPOSITE: u8 = 1;

    fn not_defined(algorithm: ChecksumAlgorithm, checksum_type: ChecksumType) -> FieldError {
        invalid(
            Self::FIELD,
            format_args!("S3 defines no {checksum_type} multipart checksum of {algorithm}"),
        )
    }
}

/// One part of a multipart upload: `MPU_PART`. A later part with the same
/// number replaces it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MpuPart {
    /// The object key.
    pub key: String,
    /// The position of the upload's `MPU_CREATE`.
    pub upload: EpochSeq,
    /// The part number, from 1 to [`MAX_PARTS`].
    pub part_number: u16,
    /// The part's size in bytes.
    pub size: u64,
    /// When the primary committed the part, in milliseconds since the Unix
    /// epoch.
    pub last_modified_ms: u64,
    /// The part's ETag: the MD5 of its bytes.
    pub etag: ETag,
    /// The part's checksums (§7.4).
    pub checksums: Checksums,
    /// The part's bytes, inline or in extents.
    pub data: PutData,
}

/// Completes a multipart upload: `MPU_COMPLETE`. The object it commits
/// inherits the write identity of the upload's `MPU_CREATE` (§7.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MpuComplete {
    /// The object key.
    pub key: String,
    /// The position of the upload's `MPU_CREATE`: the object's write
    /// identity.
    pub upload: EpochSeq,
    /// When the primary completed the upload, in milliseconds since the Unix
    /// epoch: the object's `Last-Modified`.
    pub last_modified_ms: u64,
    /// The object's size in bytes: the sum of its parts' sizes.
    pub size: u64,
    /// The object's multipart ETag (§7.4).
    pub etag: ETag,
    /// The object's checksums, derived from its parts' (§7.4).
    pub checksums: Checksums,
    /// The object's parts, in increasing part number: from 1 to
    /// [`MAX_PARTS`] of them.
    pub parts: Vec<CompletedPart>,
}

/// A part a completed upload keeps: its number and the position of the
/// `MPU_PART` record that stored it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CompletedPart {
    /// The part number.
    pub number: u16,
    /// The position of the part's `MPU_PART` record.
    pub position: EpochSeq,
}

/// Aborts a multipart upload: `MPU_ABORT`. Its parts' bytes are released
/// (§10.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MpuAbort {
    /// The object key.
    pub key: String,
    /// The position of the upload's `MPU_CREATE`.
    pub upload: EpochSeq,
}

/// The longest remote multipart upload ID a `PART_FLUSHED` carries, in
/// bytes. S3 does not bound it; AWS's are about 150 bytes.
pub const MAX_UPLOAD_ID_LEN: usize = 1024;

/// The remote side of a streamed multipart upload moved on (§7.3):
/// `PART_FLUSHED(upload, part, remote_etag)`.
///
/// The flusher of the shard's primary opens a remote multipart upload for
/// each local one, uploads each stored part to it, and completes or aborts
/// it. Each step is recorded with a `PART_FLUSHED` that rides the next
/// group commit, like a `FLUSHED`, and names the remote upload by its ID,
/// so the shard log keeps the remote upload IDs (§7.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartFlushed {
    /// The object key.
    pub key: String,
    /// The position of the local upload's `MPU_CREATE`.
    pub upload: EpochSeq,
    /// The remote upload's ID: from 1 to [`MAX_UPLOAD_ID_LEN`] bytes.
    pub remote_upload_id: String,
    /// What happened to the remote upload.
    pub step: RemoteStep,
}

/// A step of a remote multipart upload, as a [`PartFlushed`] records it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteStep {
    /// The remote upload was opened for the local one.
    Opened,
    /// The remote upload holds a local part.
    Part {
        /// The part number, from 1 to [`MAX_PARTS`].
        number: u16,
        /// The position of the `MPU_PART` record whose bytes were sent. It
        /// follows the upload and precedes the `PART_FLUSHED`.
        position: EpochSeq,
        /// The ETag the remote returned for the part.
        remote_etag: ETag,
    },
    /// The remote upload is completed or aborted: nothing refers to it any
    /// more.
    Ended,
}

impl RemoteStep {
    const OPENED: u8 = 0;
    const PART: u8 = 1;
    const ENDED: u8 = 2;
}

/// The field names an `MPU_PART`'s data reports errors under.
const PART_DATA_FIELDS: DataFields = DataFields {
    data: "mpu_part.data",
    extents: "mpu_part.extents",
};

/// An encoded [`CompletedPart`]: number, epoch, and seq.
const COMPLETED_PART_LEN: usize = 2 + 8 + 8;

impl MpuCreate {
    pub(super) fn encode(&self, w: &mut Writer<'_>) -> Result<(), FieldError> {
        w.str16("mpu_create.key", &self.key, 1, MAX_KEY_LEN)?;
        w.u64(self.initiated_ms);
        write_metadata(w, "mpu_create.metadata", &self.metadata)?;
        write_tags(w, "mpu_create.tags", &self.tags)?;
        w.present(self.checksum.is_some());
        self.checksum.map_or(Ok(()), |checksum| checksum.encode(w))
    }

    pub(super) fn decode(r: &mut Reader<'_>) -> Result<Self, FieldError> {
        Ok(Self {
            key: r.str16("mpu_create.key", 1, MAX_KEY_LEN)?,
            initiated_ms: r.u64("mpu_create.initiated_ms")?,
            metadata: read_metadata(r, "mpu_create.metadata")?,
            tags: read_tags(r, "mpu_create.tags")?,
            checksum: if r.present("mpu_create.checksum")? {
                Some(UploadChecksum::decode(r)?)
            } else {
                None
            },
        })
    }
}

impl MpuPart {
    pub(super) fn encode(&self, w: &mut Writer<'_>, position: EpochSeq) -> Result<(), FieldError> {
        w.str16("mpu_part.key", &self.key, 1, MAX_KEY_LEN)?;
        check_precedes("mpu_part.upload", self.upload, position)?;
        w.position(self.upload);
        check_part_number("mpu_part.part_number", self.part_number)?;
        w.u16(self.part_number);
        w.u64(self.size);
        w.u64(self.last_modified_ms);
        write_etag(w, "mpu_part.etag", &self.etag)?;
        write_checksums(w, "mpu_part.checksums", &self.checksums)?;
        self.data.encode(w, self.size, position, PART_DATA_FIELDS)
    }

    pub(super) fn decode(
        r: &mut Reader<'_>,
        payload: &[u8],
        position: EpochSeq,
    ) -> Result<Self, FieldError> {
        let key = r.str16("mpu_part.key", 1, MAX_KEY_LEN)?;
        let upload = r.position("mpu_part.upload")?;
        check_precedes("mpu_part.upload", upload, position)?;
        let part_number = r.u16("mpu_part.part_number")?;
        check_part_number("mpu_part.part_number", part_number)?;
        let size = r.u64("mpu_part.size")?;
        Ok(Self {
            key,
            upload,
            part_number,
            size,
            last_modified_ms: r.u64("mpu_part.last_modified_ms")?,
            etag: read_etag(r, "mpu_part.etag")?,
            checksums: read_checksums(r, "mpu_part.checksums")?,
            data: PutData::decode(r, size, payload, position, PART_DATA_FIELDS)?,
        })
    }
}

impl MpuComplete {
    pub(super) fn encode(&self, w: &mut Writer<'_>, position: EpochSeq) -> Result<(), FieldError> {
        w.str16("mpu_complete.key", &self.key, 1, MAX_KEY_LEN)?;
        check_precedes("mpu_complete.upload", self.upload, position)?;
        w.position(self.upload);
        w.u64(self.last_modified_ms);
        w.u64(self.size);
        write_etag(w, "mpu_complete.etag", &self.etag)?;
        write_checksums(w, "mpu_complete.checksums", &self.checksums)?;
        let field = "mpu_complete.parts";
        w.len16(field, self.parts.len(), 1, MAX_PARTS as usize)?;
        let mut previous = 0;
        for part in &self.parts {
            check_completed_part(part, previous, self.upload, position)?;
            previous = part.number;
            w.u16(part.number);
            w.position(part.position);
        }
        Ok(())
    }

    pub(super) fn decode(r: &mut Reader<'_>, position: EpochSeq) -> Result<Self, FieldError> {
        let key = r.str16("mpu_complete.key", 1, MAX_KEY_LEN)?;
        let upload = r.position("mpu_complete.upload")?;
        check_precedes("mpu_complete.upload", upload, position)?;
        let last_modified_ms = r.u64("mpu_complete.last_modified_ms")?;
        let size = r.u64("mpu_complete.size")?;
        let etag = read_etag(r, "mpu_complete.etag")?;
        let checksums = read_checksums(r, "mpu_complete.checksums")?;
        let field = "mpu_complete.parts";
        let count = r.len16(field, 1, MAX_PARTS as usize)?;
        r.check_count(field, count, COMPLETED_PART_LEN)?;
        let mut parts = Vec::with_capacity(count);
        let mut previous = 0;
        for _ in 0..count {
            let part = CompletedPart {
                number: r.u16(field)?,
                position: r.position(field)?,
            };
            check_completed_part(&part, previous, upload, position)?;
            previous = part.number;
            parts.push(part);
        }
        Ok(Self {
            key,
            upload,
            last_modified_ms,
            size,
            etag,
            checksums,
            parts,
        })
    }
}

impl MpuAbort {
    pub(super) fn encode(&self, w: &mut Writer<'_>, position: EpochSeq) -> Result<(), FieldError> {
        w.str16("mpu_abort.key", &self.key, 1, MAX_KEY_LEN)?;
        check_precedes("mpu_abort.upload", self.upload, position)?;
        w.position(self.upload);
        Ok(())
    }

    pub(super) fn decode(r: &mut Reader<'_>, position: EpochSeq) -> Result<Self, FieldError> {
        let key = r.str16("mpu_abort.key", 1, MAX_KEY_LEN)?;
        let upload = r.position("mpu_abort.upload")?;
        check_precedes("mpu_abort.upload", upload, position)?;
        Ok(Self { key, upload })
    }
}

impl PartFlushed {
    pub(super) fn encode(&self, w: &mut Writer<'_>, position: EpochSeq) -> Result<(), FieldError> {
        w.str16("part_flushed.key", &self.key, 1, MAX_KEY_LEN)?;
        check_precedes("part_flushed.upload", self.upload, position)?;
        w.position(self.upload);
        w.str16(
            "part_flushed.remote_upload_id",
            &self.remote_upload_id,
            1,
            MAX_UPLOAD_ID_LEN,
        )?;
        match &self.step {
            RemoteStep::Opened => w.u8(RemoteStep::OPENED),
            RemoteStep::Part {
                number,
                position: part,
                remote_etag,
            } => {
                w.u8(RemoteStep::PART);
                check_flushed_part(*number, self.upload, *part, position)?;
                w.u16(*number);
                w.position(*part);
                write_etag(w, "part_flushed.remote_etag", remote_etag)?;
            }
            RemoteStep::Ended => w.u8(RemoteStep::ENDED),
        }
        Ok(())
    }

    pub(super) fn decode(r: &mut Reader<'_>, position: EpochSeq) -> Result<Self, FieldError> {
        let key = r.str16("part_flushed.key", 1, MAX_KEY_LEN)?;
        let upload = r.position("part_flushed.upload")?;
        check_precedes("part_flushed.upload", upload, position)?;
        let remote_upload_id = r.str16("part_flushed.remote_upload_id", 1, MAX_UPLOAD_ID_LEN)?;
        let step = match r.u8("part_flushed.step")? {
            RemoteStep::OPENED => RemoteStep::Opened,
            RemoteStep::PART => {
                let number = r.u16("part_flushed.number")?;
                let part = r.position("part_flushed.position")?;
                check_flushed_part(number, upload, part, position)?;
                RemoteStep::Part {
                    number,
                    position: part,
                    remote_etag: read_etag(r, "part_flushed.remote_etag")?,
                }
            }
            RemoteStep::ENDED => RemoteStep::Ended,
            tag => {
                return Err(FieldError::new(
                    "part_flushed.step",
                    Problem::InvalidTag(tag),
                ));
            }
        };
        Ok(Self {
            key,
            upload,
            remote_upload_id,
            step,
        })
    }
}

/// Checks a flushed part: a valid number, stored after the upload opened
/// and before the record at `position`.
fn check_flushed_part(
    number: u16,
    upload: EpochSeq,
    part: EpochSeq,
    position: EpochSeq,
) -> Result<(), FieldError> {
    let field = "part_flushed.position";
    check_part_number("part_flushed.number", number)?;
    check_precedes(field, upload, part)?;
    check_precedes(field, part, position)
}

fn check_part_number(field: &'static str, number: u16) -> Result<(), FieldError> {
    if number == 0 || u32::from(number) > MAX_PARTS {
        return Err(invalid(
            field,
            format_args!("part number {number} is not from 1 to {MAX_PARTS}"),
        ));
    }
    Ok(())
}

/// Checks a completed part: a valid number above the previous one, stored
/// after the upload opened and before it completes.
fn check_completed_part(
    part: &CompletedPart,
    previous: u16,
    upload: EpochSeq,
    position: EpochSeq,
) -> Result<(), FieldError> {
    let field = "mpu_complete.parts";
    check_part_number(field, part.number)?;
    if part.number <= previous {
        return Err(FieldError::new(field, Problem::Unsorted));
    }
    check_precedes(field, upload, part.position)?;
    check_precedes(field, part.position, position)
}
