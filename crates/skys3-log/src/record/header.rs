//! The fixed header every record starts with, and the record kinds.

use std::fmt;

use skys3_types::{BucketId, Epoch, EpochSeq, KeyHash, Seq, ShardId};

use super::error::{DecodeError, FieldError, Problem};
use super::{FORMAT_VERSION, MAGIC, MAX_HEADER_LEN, MAX_PAYLOAD_LEN, MIN_FORMAT_VERSION};

/// Byte offsets of the fixed header's fields. The layout is documented in
/// the [module documentation](super).
mod offset {
    pub(super) const MAGIC: usize = 0;
    pub(super) const CRC: usize = 4;
    pub(super) const VERSION: usize = 8;
    pub(super) const KIND: usize = 10;
    pub(super) const HEADER_LEN: usize = 12;
    pub(super) const PAYLOAD_LEN: usize = 16;
    pub(super) const SHARD: usize = 20;
    pub(super) const BUCKET_LEN: usize = 21;
    pub(super) const RESERVED: usize = 22;
    pub(super) const EPOCH: usize = 24;
    pub(super) const SEQ: usize = 32;
    pub(super) const KEY_HASH: usize = 40;
    pub(super) const BUCKET: usize = 48;
    pub(super) const END: usize = 80;
}

/// The first byte the CRC covers: everything after the magic and the CRC
/// itself.
pub(crate) const CRC_START: usize = offset::VERSION;

/// The width of the bucket ID field, which holds a [`BucketId`] of up to
/// [`BucketId::MAX_LEN`] bytes, zero-padded.
const BUCKET_FIELD_LEN: usize = offset::END - offset::BUCKET;
const _: () = assert!(BucketId::MAX_LEN <= BUCKET_FIELD_LEN);

/// [`RecordHeader::LEN`] as the fixed header stores lengths.
const FIXED_LEN: u32 = offset::END as u32;

/// The kind of a log record (§10.1).
///
/// Every kind the design lists has a code. A kind whose body this build does
/// not define yet is *reserved*: the decoder rejects it with
/// [`DecodeError::UnsupportedKind`] instead of guessing at its layout. The
/// codes follow the order of the design's list and never change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum RecordKind {
    /// A committed object write, including a copy (defined).
    Put,
    /// A committed delete: a tombstone (defined).
    Delete,
    /// One extent of a large body, streamed before the record that
    /// references it (defined).
    Extent,
    /// Opens a multipart upload (defined).
    MpuCreate,
    /// One part of a multipart upload (defined).
    MpuPart,
    /// Completes a multipart upload (defined).
    MpuComplete,
    /// Aborts a multipart upload (defined).
    MpuAbort,
    /// Starts a streamed single PUT and fixes its write identity
    /// (defined).
    UploadBegin,
    /// The remote accepted a flush (defined).
    Flushed,
    /// A streamed upload's remote counterpart moved on: it was opened,
    /// took a part, or ended (defined).
    PartFlushed,
    /// Replaces an object's tags (defined).
    Tags,
    /// Creates a stub for a remote object found by the namespace import
    /// (defined).
    Import,
    /// Adopts a remote version that changed out of band (defined).
    Adopt,
    /// Publishes an erasure-coded object version's layout (defined).
    EcPublish,
    /// Moves erasure-coded fragments (reserved for M5).
    EcRelocate,
    /// Releases a replicated copy after encoding (reserved for M5).
    EcRelease,
    /// Invalidates a replica's records past a sequence number (defined).
    Truncate,
    /// A replica's full shard configuration (defined).
    Config,
}

impl RecordKind {
    /// Every kind, in code order.
    pub const ALL: [Self; 18] = [
        Self::Put,
        Self::Delete,
        Self::Extent,
        Self::MpuCreate,
        Self::MpuPart,
        Self::MpuComplete,
        Self::MpuAbort,
        Self::UploadBegin,
        Self::Flushed,
        Self::PartFlushed,
        Self::Tags,
        Self::Import,
        Self::Adopt,
        Self::EcPublish,
        Self::EcRelocate,
        Self::EcRelease,
        Self::Truncate,
        Self::Config,
    ];

    /// The kind's code in the fixed header, from 1 to 18. Code 0 is never
    /// used, so zeroed bytes are never a valid kind.
    #[must_use]
    pub const fn code(self) -> u16 {
        match self {
            Self::Put => 1,
            Self::Delete => 2,
            Self::Extent => 3,
            Self::MpuCreate => 4,
            Self::MpuPart => 5,
            Self::MpuComplete => 6,
            Self::MpuAbort => 7,
            Self::UploadBegin => 8,
            Self::Flushed => 9,
            Self::PartFlushed => 10,
            Self::Tags => 11,
            Self::Import => 12,
            Self::Adopt => 13,
            Self::EcPublish => 14,
            Self::EcRelocate => 15,
            Self::EcRelease => 16,
            Self::Truncate => 17,
            Self::Config => 18,
        }
    }

    /// The kind with `code`, if any.
    #[must_use]
    pub fn from_code(code: u16) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.code() == code)
    }

    /// The kind's name as the design writes it, such as `"MPU_CREATE"`.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Put => "PUT",
            Self::Delete => "DELETE",
            Self::Extent => "EXTENT",
            Self::MpuCreate => "MPU_CREATE",
            Self::MpuPart => "MPU_PART",
            Self::MpuComplete => "MPU_COMPLETE",
            Self::MpuAbort => "MPU_ABORT",
            Self::UploadBegin => "UPLOAD_BEGIN",
            Self::Flushed => "FLUSHED",
            Self::PartFlushed => "PART_FLUSHED",
            Self::Tags => "TAGS",
            Self::Import => "IMPORT",
            Self::Adopt => "ADOPT",
            Self::EcPublish => "EC_PUBLISH",
            Self::EcRelocate => "EC_RELOCATE",
            Self::EcRelease => "EC_RELEASE",
            Self::Truncate => "TRUNCATE",
            Self::Config => "CONFIG",
        }
    }

    /// Whether this build defines the kind's body, and so can encode and
    /// decode it.
    #[must_use]
    pub const fn is_defined(self) -> bool {
        matches!(
            self,
            Self::Put
                | Self::Delete
                | Self::Extent
                | Self::MpuCreate
                | Self::MpuPart
                | Self::MpuComplete
                | Self::MpuAbort
                | Self::UploadBegin
                | Self::Flushed
                | Self::PartFlushed
                | Self::Tags
                | Self::Import
                | Self::Adopt
                | Self::EcPublish
                | Self::Truncate
                | Self::Config
        )
    }

    /// Whether records of this kind name an object key, and so carry its
    /// [`KeyHash`]. `CONFIG` and `TRUNCATE` concern the whole shard.
    #[must_use]
    pub const fn has_key(self) -> bool {
        !matches!(self, Self::Truncate | Self::Config)
    }
}

impl fmt::Display for RecordKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// The shard a record belongs to: a bucket and a shard number within it.
///
/// Segment files are shared by every shard replica on a disk (§10.1), so
/// the record names its bucket as well as its shard number.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShardRef {
    /// The shard's bucket.
    pub bucket: BucketId,
    /// The shard's number within the bucket.
    pub shard: ShardId,
}

impl ShardRef {
    /// Names shard `shard` of `bucket`.
    #[must_use]
    pub const fn new(bucket: BucketId, shard: ShardId) -> Self {
        Self { bucket, shard }
    }
}

impl fmt::Display for ShardRef {
    /// Writes `<bucket>/<shard>`, the form register paths use.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.bucket, self.shard)
    }
}

/// A record's fixed header, decoded and verified.
///
/// [`RecordHeader::decode`] checks the framing and the CRC of the whole
/// record, and the header's own fields, without parsing the kind-specific
/// header or the payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordHeader {
    /// The format version the record was written in, from
    /// [`MIN_FORMAT_VERSION`] to [`FORMAT_VERSION`].
    pub version: u16,
    /// The record kind. Always a defined kind.
    pub kind: RecordKind,
    /// The shard the record belongs to.
    pub shard: ShardRef,
    /// The record's log position.
    pub position: EpochSeq,
    /// The hash of the record's key, for kinds that name one.
    pub key_hash: Option<KeyHash>,
    /// The length of the header: the fixed header plus the kind-specific
    /// header.
    pub header_len: u32,
    /// The length of the payload that follows the header.
    pub payload_len: u32,
}

impl RecordHeader {
    /// The length of the fixed header, and so the smallest record.
    pub const LEN: usize = offset::END;

    /// Reads the total length of the record at the start of `buf` from its
    /// fixed header, checking the magic, the format version, and that both
    /// lengths are in range. The CRC is not checked; the whole record is
    /// needed for that.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError::Incomplete`] if `buf` is shorter than the
    /// fixed header (and what it holds matches the magic so far), and
    /// otherwise the framing errors of [`DecodeError`].
    pub fn peek_len(buf: &[u8]) -> Result<usize, DecodeError> {
        let magic_len = buf.len().min(MAGIC.len());
        if buf[..magic_len] != MAGIC[..magic_len] {
            return Err(DecodeError::BadMagic);
        }
        let Some(fixed) = buf.get(..Self::LEN) else {
            return Err(DecodeError::Incomplete {
                needed: Self::LEN,
                available: buf.len(),
            });
        };
        let version = read_u16(fixed, offset::VERSION);
        if !(MIN_FORMAT_VERSION..=FORMAT_VERSION).contains(&version) {
            return Err(DecodeError::UnsupportedVersion(version));
        }
        let header_len = read_u32(fixed, offset::HEADER_LEN);
        if !(FIXED_LEN..=MAX_HEADER_LEN).contains(&header_len) {
            return Err(DecodeError::FrameLength {
                field: "header_len",
                len: header_len,
                min: FIXED_LEN,
                max: MAX_HEADER_LEN,
            });
        }
        let payload_len = read_u32(fixed, offset::PAYLOAD_LEN);
        if payload_len > MAX_PAYLOAD_LEN {
            return Err(DecodeError::FrameLength {
                field: "payload_len",
                len: payload_len,
                min: 0,
                max: MAX_PAYLOAD_LEN,
            });
        }
        // Both lengths are bounded, so the sum cannot overflow. (A `u32`
        // widens losslessly to `usize` on every supported target.)
        Ok(header_len as usize + payload_len as usize)
    }

    /// Decodes and verifies the fixed header of the record at the start of
    /// `buf`, which must hold the whole record.
    ///
    /// The checks run in this order: magic, format version, lengths, that
    /// `buf` holds the whole record, the CRC, and then the header's fields.
    /// So a field is interpreted only once the CRC shows it is what the
    /// writer wrote.
    ///
    /// # Errors
    ///
    /// Returns a [`DecodeError`] if any check fails.
    pub fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        let record_len = Self::peek_len(buf)?;
        let Some(record) = buf.get(..record_len) else {
            return Err(DecodeError::Incomplete {
                needed: record_len,
                available: buf.len(),
            });
        };
        let stored = read_u32(record, offset::CRC);
        let computed = crc32c::crc32c(&record[CRC_START..]);
        if stored != computed {
            return Err(DecodeError::ChecksumMismatch { stored, computed });
        }

        let code = read_u16(record, offset::KIND);
        let kind = RecordKind::from_code(code).ok_or(DecodeError::UnknownKind(code))?;
        if !kind.is_defined() {
            return Err(DecodeError::UnsupportedKind(kind));
        }
        if read_u16(record, offset::RESERVED) != 0 {
            return Err(FieldError::new("header.reserved", Problem::NonZeroReserved).into());
        }
        let shard = ShardRef::new(decode_bucket(record)?, ShardId::new(record[offset::SHARD]));
        let position = EpochSeq::new(
            Epoch::new(read_u64(record, offset::EPOCH)),
            Seq::new(read_u64(record, offset::SEQ)),
        );
        let raw_hash = read_u64(record, offset::KEY_HASH);
        let key_hash = if kind.has_key() {
            Some(KeyHash::from_raw(raw_hash))
        } else if raw_hash == 0 {
            None
        } else {
            return Err(FieldError::new(
                "header.key_hash",
                Problem::Inconsistent("a record without a key has a key hash"),
            )
            .into());
        };
        Ok(Self {
            version: read_u16(record, offset::VERSION),
            kind,
            shard,
            position,
            key_hash,
            header_len: read_u32(record, offset::HEADER_LEN),
            payload_len: read_u32(record, offset::PAYLOAD_LEN),
        })
    }

    /// The length of the whole record: header and payload.
    #[must_use]
    pub fn record_len(&self) -> usize {
        // Both lengths are bounded, so the sum cannot overflow.
        self.header_len as usize + self.payload_len as usize
    }

    /// Appends the fixed header to `out` with the lengths and the CRC set to
    /// zero; [`seal`] fills them in once the rest of the record is written.
    pub(crate) fn write_unsealed(
        out: &mut Vec<u8>,
        kind: RecordKind,
        shard: &ShardRef,
        position: EpochSeq,
        key_hash: Option<KeyHash>,
    ) {
        let start = out.len();
        out.resize(start + Self::LEN, 0);
        let fixed = &mut out[start..];
        fixed[offset::MAGIC..offset::CRC].copy_from_slice(&MAGIC);
        write(fixed, offset::VERSION, &FORMAT_VERSION.to_le_bytes());
        write(fixed, offset::KIND, &kind.code().to_le_bytes());
        fixed[offset::SHARD] = shard.shard.get();
        let bucket = shard.bucket.as_str().as_bytes();
        // A bucket ID is at most 25 bytes, which the static assertion above
        // shows fits the field, so the length fits a `u8`.
        fixed[offset::BUCKET_LEN] = u8::try_from(bucket.len()).unwrap_or(u8::MAX);
        write(fixed, offset::EPOCH, &position.epoch.get().to_le_bytes());
        write(fixed, offset::SEQ, &position.seq.get().to_le_bytes());
        let hash = key_hash.map_or(0, KeyHash::get);
        write(fixed, offset::KEY_HASH, &hash.to_le_bytes());
        write(fixed, offset::BUCKET, bucket);
    }
}

/// Fills in the lengths and the CRC of the record that `record` holds
/// exactly, as [`RecordHeader::write_unsealed`] started it.
pub(crate) fn seal(record: &mut [u8], header_len: u32, payload_len: u32) {
    write(record, offset::HEADER_LEN, &header_len.to_le_bytes());
    write(record, offset::PAYLOAD_LEN, &payload_len.to_le_bytes());
    let crc = crc32c::crc32c(&record[CRC_START..]);
    write(record, offset::CRC, &crc.to_le_bytes());
}

fn decode_bucket(record: &[u8]) -> Result<BucketId, DecodeError> {
    let field = &record[offset::BUCKET..offset::END];
    let len = usize::from(record[offset::BUCKET_LEN]);
    if len > BucketId::MAX_LEN {
        return Err(FieldError::new(
            "header.bucket_id",
            Problem::TooLong {
                len: len as u64,
                max: BucketId::MAX_LEN as u64,
            },
        )
        .into());
    }
    let (id, padding) = field.split_at(len);
    if padding.iter().any(|&b| b != 0) {
        return Err(FieldError::new("header.bucket_id", Problem::NonZeroReserved).into());
    }
    let id = std::str::from_utf8(id)
        .map_err(|_| FieldError::new("header.bucket_id", Problem::NotUtf8))?;
    BucketId::new(id)
        .map_err(|e| FieldError::new("header.bucket_id", Problem::Invalid(e.to_string())).into())
}

fn write(buf: &mut [u8], at: usize, bytes: &[u8]) {
    buf[at..at + bytes.len()].copy_from_slice(bytes);
}

fn read<const N: usize>(buf: &[u8], at: usize) -> [u8; N] {
    let mut bytes = [0; N];
    bytes.copy_from_slice(&buf[at..at + N]);
    bytes
}

fn read_u16(buf: &[u8], at: usize) -> u16 {
    u16::from_le_bytes(read(buf, at))
}

fn read_u32(buf: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(read(buf, at))
}

fn read_u64(buf: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(read(buf, at))
}
