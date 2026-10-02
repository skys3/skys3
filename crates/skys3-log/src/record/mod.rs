//! The log record format, version 2 (§10.1).
//!
//! A record is a fixed header, a kind-specific header, and a payload:
//!
//! ```text
//! | fixed header (80 bytes) | kind-specific header | payload |
//! |<-------------- header_len ------------------->|<- payload_len ->|
//! ```
//!
//! Every integer is little-endian. There is no padding between records or
//! inside the kind-specific header.
//!
//! # Fixed header
//!
//! | Offset | Size | Field |
//! |---:|---:|---|
//! | 0 | 4 | Magic, the ASCII bytes `SKYL` ([`MAGIC`]) |
//! | 4 | 4 | CRC32C of bytes 8 to the end of the record |
//! | 8 | 2 | Format version ([`FORMAT_VERSION`]) |
//! | 10 | 2 | Record kind ([`RecordKind::code`]) |
//! | 12 | 4 | `header_len`: fixed plus kind-specific header, from 80 to [`MAX_HEADER_LEN`] |
//! | 16 | 4 | `payload_len`: from 0 to [`MAX_PAYLOAD_LEN`] |
//! | 20 | 1 | Shard number within the bucket |
//! | 21 | 1 | Length of the bucket ID, from 1 to 25 |
//! | 22 | 2 | Reserved, zero |
//! | 24 | 8 | Epoch |
//! | 32 | 8 | Sequence number |
//! | 40 | 8 | Key hash ([`KeyHash`]); zero for `CONFIG` and `TRUNCATE`, which name no key |
//! | 48 | 32 | Bucket ID, zero-padded |
//!
//! Segment files are shared by every shard on a disk, so the shard is named
//! by its bucket ID and shard number, not the shard number alone.
//!
//! **The CRC** is CRC-32C (Castagnoli, as in iSCSI and ext4) of every byte
//! from offset 8 to the end of the payload: the rest of the fixed header,
//! the kind-specific header, and the payload. It covers every byte of the
//! record except the magic, which is a constant checked byte for byte, and
//! the CRC field itself.
//!
//! **Versions.** The format version covers the fixed header and every
//! defined body. Any change to either takes a new version, and a reader
//! rejects every version it does not know
//! ([`DecodeError::UnsupportedVersion`]) instead of guessing. This build
//! writes version 2 ([`FORMAT_VERSION`]) and reads versions 1 and 2
//! ([`MIN_FORMAT_VERSION`]). Version 2 adds a `u16` part count after each
//! checksum digest of `PUT` and `ADOPT`, zero for a `FULL_OBJECT`
//! checksum; a version 1 checksum has none and is `FULL_OBJECT`. Defining the
//! body of a reserved kind does not change the version: a reader that
//! predates it rejects the kind as [`DecodeError::UnsupportedKind`]. The
//! rules for upgrading a node across versions are decided in M7-09.
//!
//! **Positions.** Every record has a log position: its `(epoch, seq)`,
//! including `EXTENT` records, which `PUT` records reference by position.
//! `CONFIG` and `TRUNCATE` records are appended by a replica for itself and
//! take no sequence number of their own. Their sequence number field names
//! the replica's position they refer to: for `TRUNCATE`, the last sequence
//! number that stays valid; for `CONFIG`, the replica's last sequence number
//! when it adopted the configuration.
//!
//! # Kind-specific headers
//!
//! Each defined kind's fields are encoded in the declaration order of its
//! body type ([`Put`], [`Delete`], [`Extent`], [`MpuCreate`], [`MpuPart`],
//! [`MpuComplete`], [`MpuAbort`], [`Tags`], [`Flushed`], [`Import`],
//! [`Adopt`], `CONFIG` as [`ShardConfig`](skys3_types::ShardConfig)
//! without the bucket, shard, and epoch the fixed header holds, and
//! `TRUNCATE`, which has no fields):
//!
//! - integers as fixed-width little-endian values; positions as epoch then
//!   sequence number; timestamps as milliseconds since the Unix epoch,
//! - text as a length (`u8` for bucket, node, and proposal IDs, otherwise
//!   `u16`) followed by UTF-8 bytes,
//! - an option as a presence byte, 0 or 1, followed by the value if
//!   present,
//! - a map as a count followed by its entries in strictly increasing key
//!   order, so every map has exactly one encoding,
//! - checksums as a map from the algorithm's code (`u8`) to its digest,
//!   followed by the `u16` part count of a `COMPOSITE` checksum, or zero,
//! - a `PUT`'s or `MPU_PART`'s data as a tag byte: 0 for inline data, which
//!   is the payload; or 1 for extents, followed by a `u32` count and, per
//!   extent, its position and a `u32` length,
//! - an `MPU_CREATE`'s checksum as an option of the algorithm's code (`u8`)
//!   and the type (`u8`: 0 for `FULL_OBJECT`, 1 for `COMPOSITE`), and an
//!   `MPU_COMPLETE`'s parts as a `u16` count followed by each part's
//!   number (`u16`) and position, in increasing part number.
//!
//! # Decoding
//!
//! Decoding reads the fixed header, checks the magic, the version, and both
//! lengths against their bounds, and only then looks at the rest of the
//! buffer, which must hold `header_len + payload_len` bytes. It verifies the
//! CRC before it interprets any other field. Every length and count in the
//! kind-specific header is checked against its bound and against the bytes
//! that remain before anything is allocated, and arithmetic on decoded
//! values is checked. A defined kind must use exactly its header's bytes
//! and have a payload only if the kind has one. The encoding is canonical:
//! a buffer of the current version that decodes re-encodes to the same
//! bytes. A record of an older version re-encodes in the current one.

mod body;
mod error;
mod header;
mod multipart;
mod wire;

use bytes::Bytes;
use skys3_types::{EpochSeq, KeyHash, limits};

pub use body::{
    Adopt, CopySource, Delete, Extent, ExtentRef, Flushed, IDENTITY_METADATA,
    IDENTITY_METADATA_RESERVED, Import, MAX_EXTENTS, MAX_KEY_LEN, MAX_METADATA_LEN,
    MAX_STORAGE_CLASS_LEN, MAX_TAG_KEY_LEN, MAX_TAG_VALUE_LEN, MAX_TAGS, MAX_VERSION_ID_LEN,
    Metadata, Put, PutData, RecordBody, TagSet, Tags,
};
pub use error::{DecodeError, EncodeError, ErrorClass, FieldError, Problem};
pub use header::{RecordHeader, RecordKind, ShardRef};
pub use multipart::{CompletedPart, MpuAbort, MpuComplete, MpuCreate, MpuPart, UploadChecksum};
pub use skys3_types::checksum::{Checksum, ChecksumAlgorithm, ChecksumType, Checksums};

use wire::Writer;

/// Whether a `TRUNCATE` at `truncate` invalidates the record of its shard
/// at `position`: a record at a later `seq` from an earlier epoch (§6.6).
///
/// A replica that reconciles with a new primary truncates the records past
/// the longest prefix it shares with the primary's log, and takes the
/// primary's records from there on. It gives its `TRUNCATE` the epoch of
/// the first record it takes next, which is newer than every record it
/// truncates and no newer than any record it takes later, so the rule
/// tells the two apart without knowing the order in which the log holds
/// them. Replay and every read of a replica's log skip what it invalidates.
#[must_use]
pub fn truncated_by(truncate: EpochSeq, position: EpochSeq) -> bool {
    position.seq > truncate.seq && position.epoch < truncate.epoch
}

/// The bytes every record starts with.
pub const MAGIC: [u8; 4] = *b"SKYL";

/// The format version this build writes, and the newest it reads.
pub const FORMAT_VERSION: u16 = 2;

/// The oldest format version this build reads.
pub const MIN_FORMAT_VERSION: u16 = 1;

/// The largest `header_len`: the fixed header plus the kind-specific header.
/// A `PUT` that references [`MAX_EXTENTS`] extents fits with room to spare.
pub const MAX_HEADER_LEN: u32 = 2 * 1024 * 1024;

/// The largest payload: an inline body or one extent. Configuration
/// loading keeps `inline_max_bytes` and `extent_bytes` within it; the value
/// lives in [`skys3_types::limits`] so both crates share it.
pub const MAX_PAYLOAD_LEN: u32 = limits::MAX_RECORD_PAYLOAD_LEN;

/// One log record: the shard it belongs to, its log position, and its
/// kind-specific body.
///
/// ```
/// use skys3_log::record::{Delete, LogRecord, RecordBody, ShardRef};
/// use skys3_types::{BucketId, Epoch, EpochSeq, Seq, ShardId};
///
/// let record = LogRecord {
///     shard: ShardRef::new(BucketId::new("b-7f3a")?, ShardId::new(7)),
///     position: EpochSeq::new(Epoch::new(42), Seq::new(1001)),
///     body: RecordBody::Delete(Delete { key: "photos/cat.jpg".into() }),
/// };
/// let bytes = record.to_bytes()?;
/// let (decoded, len) = LogRecord::decode(&bytes)?;
/// assert_eq!((decoded, len), (record, bytes.len()));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogRecord {
    /// The shard whose log the record belongs to.
    pub shard: ShardRef,
    /// The record's position in that log.
    pub position: EpochSeq,
    /// The kind-specific content.
    pub body: RecordBody,
}

impl LogRecord {
    /// The record kind.
    #[must_use]
    pub const fn kind(&self) -> RecordKind {
        self.body.kind()
    }

    /// The object key, for kinds that name one.
    #[must_use]
    pub fn key(&self) -> Option<&str> {
        self.body.key()
    }

    /// The hash of the object key within the bucket, for kinds that name
    /// one. The fixed header stores it.
    #[must_use]
    pub fn key_hash(&self) -> Option<KeyHash> {
        self.key()
            .map(|key| KeyHash::of(&self.shard.bucket, key.as_bytes()))
    }

    /// Appends the encoded record to `out`.
    ///
    /// # Errors
    ///
    /// Returns an [`EncodeError`] if a field is out of bounds or breaks an
    /// invariant, exactly when the decoder would reject the result. `out` is
    /// then left as it was.
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), EncodeError> {
        let start = out.len();
        let result = self.encode_at(out, start);
        if result.is_err() {
            out.truncate(start);
        }
        result
    }

    fn encode_at(&self, out: &mut Vec<u8>, start: usize) -> Result<(), EncodeError> {
        let (header_len, payload_len) = self.encode_headers(out, start)?;
        out.extend_from_slice(self.body.payload());
        header::seal(&mut out[start..], header_len, payload_len);
        Ok(())
    }

    /// Writes the fixed and kind-specific headers, unsealed, and returns
    /// `header_len` and `payload_len`.
    fn encode_headers(&self, out: &mut Vec<u8>, start: usize) -> Result<(u32, u32), EncodeError> {
        RecordHeader::write_unsealed(
            out,
            self.kind(),
            &self.shard,
            self.position,
            self.key_hash(),
        );
        self.body
            .encode(&mut Writer::new(out), &self.shard, self.position)?;
        let header_len = frame_len("header_len", out.len() - start, MAX_HEADER_LEN)?;
        let payload_len = frame_len("payload_len", self.body.payload().len(), MAX_PAYLOAD_LEN)?;
        Ok((header_len, payload_len))
    }

    /// Checks that the record can be encoded, without copying its payload:
    /// it fails exactly when [`LogRecord::encode`] would.
    ///
    /// # Errors
    ///
    /// As [`LogRecord::encode`].
    pub fn check(&self) -> Result<(), EncodeError> {
        self.encode_headers(&mut Vec::new(), 0).map(drop)
    }

    /// Encodes the record into a new buffer.
    ///
    /// # Errors
    ///
    /// As [`LogRecord::encode`].
    pub fn to_bytes(&self) -> Result<Bytes, EncodeError> {
        let mut out = Vec::new();
        self.encode(&mut out)?;
        Ok(out.into())
    }

    /// Decodes the record at the start of `buf`, and returns it with its
    /// length in bytes. Bytes after the record are not read.
    ///
    /// # Errors
    ///
    /// Returns a [`DecodeError`] if `buf` does not start with a whole, valid
    /// record of a defined kind. [`DecodeError::class`] tells a torn or
    /// damaged record apart from one this build cannot read.
    pub fn decode(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        let header = RecordHeader::decode(buf)?;
        let record_len = header.record_len();
        let header_len = header.header_len as usize;
        let body = RecordBody::decode(
            header.kind,
            header.version,
            &buf[RecordHeader::LEN..header_len],
            &buf[header_len..record_len],
            &header.shard,
            header.position,
        )?;
        let record = Self {
            shard: header.shard,
            position: header.position,
            body,
        };
        if record.key_hash() != header.key_hash {
            return Err(FieldError::new(
                "header.key_hash",
                Problem::Inconsistent("the key hash is not the hash of the record's key"),
            )
            .into());
        }
        Ok((record, record_len))
    }
}

/// Checks a frame length against its bound.
fn frame_len(field: &'static str, len: usize, max: u32) -> Result<u32, FieldError> {
    u32::try_from(len)
        .ok()
        .filter(|&len| len <= max)
        .ok_or(FieldError::new(
            field,
            Problem::TooLong {
                len: len as u64,
                max: max.into(),
            },
        ))
}
