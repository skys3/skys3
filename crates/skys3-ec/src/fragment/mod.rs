//! The fragment record format, version 1 (design §8.4, §10.1).
//!
//! A fragment segment holds fragment records back to back. A record is a
//! header and a payload, the fragment's bytes:
//!
//! ```text
//! | fixed header (40 bytes) | fields | block CRCs | payload |
//! |<---------------- header_len --------------->|<- payload_len ->|
//! ```
//!
//! Every integer is little-endian, with no padding.
//!
//! # Fixed header
//!
//! | Offset | Size | Field |
//! |---:|---:|---|
//! | 0 | 4 | Magic, the ASCII bytes `SKYF` ([`MAGIC`]) |
//! | 4 | 4 | CRC32C of bytes 8 to `header_len`: the rest of the header |
//! | 8 | 2 | Format version ([`FORMAT_VERSION`]) |
//! | 10 | 2 | Reserved, zero |
//! | 12 | 4 | `header_len`, from 40 plus the block CRCs to [`MAX_HEADER_LEN`] |
//! | 16 | 8 | `payload_len`, from 1 to [`MAX_FRAGMENT_LEN`] |
//! | 24 | 16 | The fragment's [`FragmentId`](crate::FragmentId) |
//!
//! # Fields
//!
//! The fields of [`FragmentHeader`] in declaration order, encoded as the
//! log's record bodies are (`skys3_log::record`): text as a length (`u8`
//! for the bucket ID, `u16` otherwise) and UTF-8 bytes, positions as epoch
//! then sequence number, maps as a count and their entries in strictly
//! increasing key order. In order: bucket ID, shard number (`u8`), key,
//! version position, attempt (epoch and number, `u64` each), stripe number
//! and count (`u32` each), stripe offset and data length (`u64` each), `k`
//! and `m` (`u8` each), codec ID (`u16`), fragment index (`u8`), object
//! size and `Last-Modified` in milliseconds (`u64` each), ETag, identity
//! position, metadata (a `u16` count of name and value pairs), tags (a
//! `u8` count of key and value pairs), checksums (a `u8` count, then per
//! checksum the algorithm's code, its digest, and a `u16` part count, zero
//! for `FULL_OBJECT`), and parts (a `u16` count of a `u16` number and a
//! `u64` size).
//!
//! # Block CRCs
//!
//! One CRC32C per [`BLOCK_LEN`] bytes of payload, the last block possibly
//! shorter, so a read of a range verifies only the blocks it covers. The
//! header's own CRC covers them.
//!
//! # Decoding
//!
//! A reader checks the magic, the version, and both lengths, then the
//! header's CRC, and only then interprets the fields and the block CRCs.
//! Every length and count is checked against its bound and the bytes that
//! remain before anything is allocated. The fields must fill the header
//! exactly, the fragment index must lie within the geometry, the stripe
//! within the object, and, for a codec this build knows, the payload
//! length must be the stripe's fragment length. Errors are classified as
//! the log's are ([`FragmentDecodeError::class`]): an incomplete or
//! corrupt record can be a torn tail, an unknown version or a malformed
//! header under a valid CRC cannot. The encoding is canonical: a record
//! that decodes re-encodes to the same bytes.

mod header;
mod record;
mod wire;

pub use header::{FragmentHeader, ObjectMeta, PartSize, StripeInfo};
pub use record::{
    BLOCK_LEN, FIXED_LEN, FORMAT_VERSION, FragmentDecodeError, FragmentEncodeError, FragmentRecord,
    MAGIC, MAX_FRAGMENT_LEN, MAX_HEADER_LEN,
};
pub(crate) use record::{DecodedHeader, FixedHeader, encode_tail, seal};
pub(crate) use wire::inconsistent;
