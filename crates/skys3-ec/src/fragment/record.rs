//! Fragment records: framing, checksums, encoding, and decoding.

use bytes::Bytes;
use skys3_log::record::{ErrorClass, FieldError, Problem};

use super::header::FragmentHeader;
use super::wire::{Reader, inconsistent};
use crate::{FragmentId, codec};

/// The magic that starts every fragment record: the ASCII bytes `SKYF`.
pub const MAGIC: [u8; 4] = *b"SKYF";

/// The fragment record format this build writes and reads.
pub const FORMAT_VERSION: u16 = 1;

/// The length of the fixed header.
pub const FIXED_LEN: usize = 40;

/// The longest header: fixed header, fields, and block checksums.
pub const MAX_HEADER_LEN: u32 = 256 * 1024;

/// The longest fragment payload. A stripe of `k` data fragments therefore
/// holds at most `k` times this.
pub const MAX_FRAGMENT_LEN: u64 = 256 * 1024 * 1024;

/// The payload is checksummed in blocks of this many bytes, so a read of a
/// range verifies only the blocks it covers.
pub const BLOCK_LEN: u64 = 64 * 1024;

/// Why bytes could not be decoded as a fragment record.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum FragmentDecodeError {
    /// The buffer ends before the record does.
    #[error("the fragment record needs {needed} bytes; the buffer holds {available}")]
    Incomplete {
        /// The bytes needed to go further.
        needed: u64,
        /// The bytes available.
        available: u64,
    },
    /// The buffer does not start with [`MAGIC`].
    #[error("the bytes do not start with the fragment record magic")]
    BadMagic,
    /// The record has a format version this build cannot read.
    #[error(
        "fragment format version {0} is not supported; this build reads version {FORMAT_VERSION}"
    )]
    UnsupportedVersion(u16),
    /// A length in the fixed header is out of range.
    #[error("{field} is {len}; it must be from {min} to {max}")]
    FrameLength {
        /// `header_len` or `payload_len`.
        field: &'static str,
        /// The length found.
        len: u64,
        /// The smallest allowed.
        min: u64,
        /// The largest allowed.
        max: u64,
    },
    /// The header's CRC32C does not match its bytes.
    #[error("header CRC32C mismatch: stored {stored:#010x}, computed {computed:#010x}")]
    ChecksumMismatch {
        /// The checksum stored in the header.
        stored: u32,
        /// The checksum of the bytes it covers.
        computed: u32,
    },
    /// A payload block's CRC32C does not match the header's.
    #[error(
        "payload block {block} CRC32C mismatch: stored {stored:#010x}, computed {computed:#010x}"
    )]
    BlockMismatch {
        /// The block's number within the payload.
        block: u64,
        /// The checksum the header stores for the block.
        stored: u32,
        /// The checksum of the block's bytes.
        computed: u32,
    },
    /// The header's CRC verifies, but a field breaks the format.
    #[error("malformed fragment header: {0}")]
    Malformed(#[from] FieldError),
}

impl FragmentDecodeError {
    /// Classifies the error as the log's record errors are (§10.1): only
    /// [`ErrorClass::Incomplete`] and [`ErrorClass::Corrupt`] can be a torn
    /// write.
    #[must_use]
    pub const fn class(&self) -> ErrorClass {
        match self {
            Self::Incomplete { .. } => ErrorClass::Incomplete,
            Self::BadMagic
            | Self::FrameLength { .. }
            | Self::ChecksumMismatch { .. }
            | Self::BlockMismatch { .. } => ErrorClass::Corrupt,
            Self::UnsupportedVersion(_) => ErrorClass::Unsupported,
            Self::Malformed(_) => ErrorClass::Invalid,
        }
    }
}

/// A fragment header that cannot be encoded, because it would break the
/// format. The encoder rejects exactly what the decoder would.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("cannot encode fragment record: {0}")]
pub struct FragmentEncodeError(#[from] pub FieldError);

/// A fragment record: the fragment's ID, its header, and its payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FragmentRecord {
    /// The ID the fragment store assigned.
    pub id: FragmentId,
    /// The header.
    pub header: FragmentHeader,
    /// The fragment's bytes, as the stripe's codec produced them.
    pub payload: Bytes,
}

impl FragmentRecord {
    /// Encodes the record.
    ///
    /// # Errors
    ///
    /// [`FragmentEncodeError`] if the header breaks the format or the
    /// payload's length does not fit the stripe.
    pub fn to_bytes(&self) -> Result<Bytes, FragmentEncodeError> {
        let tail = encode_tail(&self.header, &self.payload)?;
        let mut bytes = seal(self.id, self.payload.len() as u64, &tail);
        bytes.extend_from_slice(&self.payload);
        Ok(bytes.into())
    }

    /// Decodes and fully verifies the record at the start of `bytes`, and
    /// returns it with its length.
    ///
    /// # Errors
    ///
    /// A [`FragmentDecodeError`], checked in the order recovery relies on:
    /// the framing, then the header's CRC, then its fields, then each
    /// payload block's CRC.
    pub fn decode(bytes: &[u8]) -> Result<(Self, usize), FragmentDecodeError> {
        let fixed = FixedHeader::peek(bytes)?;
        let len = fixed.record_len();
        let header_len = fixed.header_len as usize;
        let available = bytes.len() as u64;
        if available < len {
            return Err(FragmentDecodeError::Incomplete {
                needed: len,
                available,
            });
        }
        // `len` fits in the buffer, so in a usize.
        let len = len as usize;
        let decoded = DecodedHeader::decode(&bytes[..header_len])?;
        let payload = &bytes[header_len..len];
        decoded.verify_blocks(0, payload)?;
        let record = Self {
            id: decoded.id,
            header: decoded.header,
            payload: Bytes::copy_from_slice(payload),
        };
        Ok((record, len))
    }
}

/// A fixed header, with its framing checked but not its CRC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FixedHeader {
    pub(crate) header_len: u32,
    pub(crate) payload_len: u64,
}

impl FixedHeader {
    /// Checks the magic, the version, and both lengths at the start of
    /// `bytes`, which must hold at least the fixed header.
    pub(crate) fn peek(bytes: &[u8]) -> Result<Self, FragmentDecodeError> {
        let Some(fixed) = bytes.get(..FIXED_LEN) else {
            return Err(FragmentDecodeError::Incomplete {
                needed: FIXED_LEN as u64,
                available: bytes.len() as u64,
            });
        };
        if fixed[..4] != MAGIC {
            return Err(FragmentDecodeError::BadMagic);
        }
        let version = u16::from_le_bytes([fixed[8], fixed[9]]);
        if version != FORMAT_VERSION {
            return Err(FragmentDecodeError::UnsupportedVersion(version));
        }
        let header_len = u32::from_le_bytes([fixed[12], fixed[13], fixed[14], fixed[15]]);
        let mut payload = [0; 8];
        payload.copy_from_slice(&fixed[16..24]);
        let payload_len = u64::from_le_bytes(payload);
        if !(1..=MAX_FRAGMENT_LEN).contains(&payload_len) {
            return Err(FragmentDecodeError::FrameLength {
                field: "payload_len",
                len: payload_len,
                min: 1,
                max: MAX_FRAGMENT_LEN,
            });
        }
        let min_header = FIXED_LEN as u64 + 4 * blocks(payload_len);
        if !(min_header..=u64::from(MAX_HEADER_LEN)).contains(&u64::from(header_len)) {
            return Err(FragmentDecodeError::FrameLength {
                field: "header_len",
                len: header_len.into(),
                min: min_header,
                max: MAX_HEADER_LEN.into(),
            });
        }
        Ok(Self {
            header_len,
            payload_len,
        })
    }

    /// The length of the whole record.
    pub(crate) fn record_len(self) -> u64 {
        u64::from(self.header_len) + self.payload_len
    }
}

/// A header decoded and verified: its CRC and its fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodedHeader {
    pub(crate) id: FragmentId,
    pub(crate) header: FragmentHeader,
    pub(crate) payload_len: u64,
    /// The CRC32C of each payload block.
    pub(crate) blocks: Vec<u32>,
}

impl DecodedHeader {
    /// Decodes `bytes`, which must be exactly one record's header.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, FragmentDecodeError> {
        let fixed = FixedHeader::peek(bytes)?;
        if bytes.len() != fixed.header_len as usize {
            return Err(FragmentDecodeError::Incomplete {
                needed: fixed.header_len.into(),
                available: bytes.len() as u64,
            });
        }
        let stored = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        let computed = crc32c::crc32c(&bytes[8..]);
        if stored != computed {
            return Err(FragmentDecodeError::ChecksumMismatch { stored, computed });
        }
        if bytes[10..12] != [0, 0] {
            return Err(FieldError {
                field: "reserved",
                problem: Problem::NonZeroReserved,
            }
            .into());
        }
        let mut id = [0; 16];
        id.copy_from_slice(&bytes[24..FIXED_LEN]);
        let id = FragmentId::new(u128::from_le_bytes(id));
        let mut r = Reader::new(&bytes[FIXED_LEN..]);
        let header = FragmentHeader::decode(&mut r)?;
        check_payload_len(&header, fixed.payload_len)?;
        let count = blocks(fixed.payload_len) as usize;
        if r.remaining() != 4 * count {
            return Err(inconsistent("blocks", "the header length does not fit its fields").into());
        }
        let blocks = (0..count)
            .map(|_| r.u32("blocks"))
            .collect::<Result<_, _>>()?;
        r.finish("blocks")?;
        Ok(Self {
            id,
            header,
            payload_len: fixed.payload_len,
            blocks,
        })
    }

    /// Verifies `data`, which holds the payload's blocks from block `first`
    /// on, each whole except the payload's last.
    pub(crate) fn verify_blocks(&self, first: u64, data: &[u8]) -> Result<(), FragmentDecodeError> {
        for (n, chunk) in data.chunks(BLOCK_LEN as usize).enumerate() {
            let block = first + n as u64;
            let computed = crc32c::crc32c(chunk);
            let stored = self.blocks.get(block as usize).copied();
            if stored != Some(computed) {
                return Err(FragmentDecodeError::BlockMismatch {
                    block,
                    stored: stored.unwrap_or(0),
                    computed,
                });
            }
        }
        Ok(())
    }
}

/// The number of checksum blocks of a payload of `len` bytes.
pub(crate) fn blocks(len: u64) -> u64 {
    len.div_ceil(BLOCK_LEN)
}

/// Checks the payload's length: within bounds, and, for a codec this build
/// knows, the stripe's fragment length.
fn check_payload_len(header: &FragmentHeader, len: u64) -> Result<(), FieldError> {
    if len == 0 || len > MAX_FRAGMENT_LEN {
        let problem = match len {
            0 => Problem::Empty,
            _ => Problem::TooLong {
                len,
                max: MAX_FRAGMENT_LEN,
            },
        };
        return Err(FieldError {
            field: "payload",
            problem,
        });
    }
    let stripe = &header.stripe;
    if let Ok(codec) = codec(stripe.codec) {
        let expected = codec.fragment_len(stripe.geometry, stripe.data_len).ok();
        if expected != Some(len) {
            return Err(inconsistent(
                "payload",
                "its length is not the stripe's fragment length",
            ));
        }
    }
    Ok(())
}

/// Encodes everything of a header after the fixed header: the fields, then
/// one CRC32C per payload block.
pub(crate) fn encode_tail(header: &FragmentHeader, payload: &[u8]) -> Result<Vec<u8>, FieldError> {
    let mut tail = Vec::new();
    header.encode(&mut tail)?;
    check_payload_len(header, payload.len() as u64)?;
    for block in payload.chunks(BLOCK_LEN as usize) {
        tail.extend_from_slice(&crc32c::crc32c(block).to_le_bytes());
    }
    let len = FIXED_LEN + tail.len();
    if len > MAX_HEADER_LEN as usize {
        return Err(FieldError {
            field: "header",
            problem: Problem::TooLong {
                len: len as u64,
                max: MAX_HEADER_LEN.into(),
            },
        });
    }
    Ok(tail)
}

/// Returns the whole header of fragment `id`: the fixed header, with its
/// CRC, and `tail` from [`encode_tail`].
pub(crate) fn seal(id: FragmentId, payload_len: u64, tail: &[u8]) -> Vec<u8> {
    let header_len = FIXED_LEN + tail.len();
    let mut out = Vec::with_capacity(header_len);
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&[0; 4]);
    out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&[0; 2]);
    // `encode_tail` bounds the header by MAX_HEADER_LEN, a u32.
    out.extend_from_slice(&(header_len as u32).to_le_bytes());
    out.extend_from_slice(&payload_len.to_le_bytes());
    out.extend_from_slice(&id.get().to_le_bytes());
    out.extend_from_slice(tail);
    let crc = crc32c::crc32c(&out[8..]);
    out[4..8].copy_from_slice(&crc.to_le_bytes());
    out
}
