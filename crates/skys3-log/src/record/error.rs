//! Encoding and decoding errors.

use std::fmt;

use super::{FORMAT_VERSION, MIN_FORMAT_VERSION, RecordKind};

/// A field that breaks the record format, found while encoding or decoding.
///
/// `field` names the field as `<record>.<field>`, for example `put.key` or
/// `header.bucket_id`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{field}: {problem}")]
pub struct FieldError {
    /// The field.
    pub field: &'static str,
    /// What is wrong with it.
    pub problem: Problem,
}

impl FieldError {
    pub(crate) const fn new(field: &'static str, problem: Problem) -> Self {
        Self { field, problem }
    }
}

/// What is wrong with a field.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Problem {
    /// The field runs past the end of the record's kind-specific header.
    Truncated,
    /// The field is longer, or has more entries, than the format allows.
    TooLong {
        /// The length or count found.
        len: u64,
        /// The largest allowed.
        max: u64,
    },
    /// The field is empty, and the format requires at least one byte or
    /// entry.
    Empty,
    /// A tag byte (an option's presence or an enum's variant) has no
    /// meaning.
    InvalidTag(u8),
    /// A text field is not UTF-8.
    NotUtf8,
    /// Map entries are not in strictly increasing order, so the encoding is
    /// not canonical (or a name repeats).
    Unsorted,
    /// A reserved or padding byte is not zero.
    NonZeroReserved,
    /// Bytes follow the last field of the kind-specific header.
    TrailingBytes(u64),
    /// The value is not valid for its type, for example an invalid bucket ID
    /// or ETag.
    Invalid(String),
    /// The value contradicts another field of the record.
    Inconsistent(&'static str),
}

impl fmt::Display for Problem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => f.write_str("runs past the end of the record header"),
            Self::TooLong { len, max } => write!(f, "is {len} long; the limit is {max}"),
            Self::Empty => f.write_str("is empty"),
            Self::InvalidTag(tag) => write!(f, "has invalid tag byte {tag}"),
            Self::NotUtf8 => f.write_str("is not UTF-8"),
            Self::Unsorted => f.write_str("entries are not in strictly increasing order"),
            Self::NonZeroReserved => f.write_str("reserved bytes are not zero"),
            Self::TrailingBytes(n) => write!(f, "is followed by {n} unexpected bytes"),
            Self::Invalid(reason) => write!(f, "is invalid: {reason}"),
            Self::Inconsistent(reason) => write!(f, "is inconsistent: {reason}"),
        }
    }
}

/// A record that cannot be encoded, because it would break the format.
///
/// The encoder rejects exactly the records the decoder would reject, so
/// every record that encodes also decodes back to an equal value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("cannot encode log record: {0}")]
pub struct EncodeError(#[from] pub FieldError);

/// Why bytes could not be decoded as a log record.
///
/// [`DecodeError::class`] groups the errors by what a reader can conclude
/// from them.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DecodeError {
    /// The buffer ends before the record does.
    #[error("the record needs {needed} bytes; the buffer holds {available}")]
    Incomplete {
        /// The bytes needed to go further: the fixed header, or the whole
        /// record once its lengths are known.
        needed: usize,
        /// The bytes available.
        available: usize,
    },
    /// The buffer does not start with the record magic.
    #[error("the bytes do not start with the log record magic")]
    BadMagic,
    /// The record has a format version this build cannot read.
    #[error(
        "log format version {0} is not supported; this build reads versions \
         {MIN_FORMAT_VERSION} to {FORMAT_VERSION}"
    )]
    UnsupportedVersion(u16),
    /// A length in the fixed header is out of range.
    #[error("{field} is {len}; it must be from {min} to {max}")]
    FrameLength {
        /// `header_len` or `payload_len`.
        field: &'static str,
        /// The length found.
        len: u32,
        /// The smallest allowed.
        min: u32,
        /// The largest allowed.
        max: u32,
    },
    /// The CRC32C does not match the record's bytes.
    #[error("CRC32C mismatch: the record stores {stored:#010x}, its bytes give {computed:#010x}")]
    ChecksumMismatch {
        /// The checksum stored in the record.
        stored: u32,
        /// The checksum of the bytes it covers.
        computed: u32,
    },
    /// The record kind code is not one this format version defines.
    #[error("unknown log record kind {0}")]
    UnknownKind(u16),
    /// The record kind is reserved, but this build does not define its body
    /// yet.
    #[error("log record kind {0} is reserved but not yet supported")]
    UnsupportedKind(RecordKind),
    /// The CRC verifies, but a field breaks the format.
    #[error("malformed log record: {0}")]
    Malformed(#[from] FieldError),
}

/// What a [`DecodeError`] tells a reader about the bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorClass {
    /// More bytes are needed. At the end of a log, this is a torn write.
    Incomplete,
    /// The framing or the CRC does not verify: the bytes are damaged, or
    /// were never a complete record. At the end of a log, this is a torn
    /// write.
    Corrupt,
    /// The record is from a format version or uses a kind this build cannot
    /// read, typically because a newer build wrote it. It must not be
    /// discarded as damage.
    Unsupported,
    /// The CRC verifies but the content breaks the format, so the writer was
    /// faulty. It must not be discarded as damage either.
    Invalid,
}

impl DecodeError {
    /// Classifies the error.
    ///
    /// Recovery cuts a torn tail back to the last record whose CRC verifies
    /// (§10.1). Only [`ErrorClass::Incomplete`] and [`ErrorClass::Corrupt`]
    /// errors can be a torn tail; the others mean the log holds a whole
    /// record this build cannot use.
    #[must_use]
    pub const fn class(&self) -> ErrorClass {
        match self {
            Self::Incomplete { .. } => ErrorClass::Incomplete,
            Self::BadMagic | Self::FrameLength { .. } | Self::ChecksumMismatch { .. } => {
                ErrorClass::Corrupt
            }
            Self::UnsupportedVersion(_) | Self::UnknownKind(_) | Self::UnsupportedKind(_) => {
                ErrorClass::Unsupported
            }
            Self::Malformed(_) => ErrorClass::Invalid,
        }
    }
}
