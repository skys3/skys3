//! Checksums and ETags at the protocol boundary (design §7.4, §11).
//!
//! - [`ExpectedChecksums`] reads what a request supplies: `Content-MD5`,
//!   and one `x-amz-checksum-*` value (CRC32, CRC32C, CRC64NVME, SHA1, or
//!   SHA256) in a header or in a trailer that `x-amz-trailer` declares.
//! - [`ChecksumValidator`] hashes the body while it streams, on a
//!   [`BlockingPool`](skys3_io::BlockingPool) rather than the reactor,
//!   checks every supplied value once the body ends, and returns a
//!   [`VerifiedBody`]: the MD5 ETag and the checksums to store with the
//!   object. A trailing checksum is read from the [`Trailers`](crate::Trailers) the
//!   `aws-chunked` decoder publishes when the body ends.
//! - [`MultipartEtag`] and [`MultipartChecksum`] derive a multipart
//!   object's ETag and `COMPOSITE` or `FULL_OBJECT` checksum from its
//!   parts'.
//! - [`Hasher`] and [`digest`] compute each algorithm's digest.
//!
//! An object uploaded without an `x-amz-checksum-*` value gets a computed
//! [`DEFAULT_ALGORITHM`] (CRC64NVME) checksum, as S3 adds one. The stored
//! form, [`Checksums`](skys3_types::checksum::Checksums), lives in
//! `skys3-types`, so log records and index entries carry it.
//!
//! # Answers
//!
//! A request whose checksums fail gets S3's answer, as an
//! [`IntegrityError`]:
//!
//! | Case | Answer |
//! |---|---|
//! | `Content-MD5` is not the base64 of 16 bytes, is empty, or repeats | `400 InvalidDigest` |
//! | `Content-MD5` does not match the body | `400 BadDigest` |
//! | An `x-amz-checksum-*` value, header or trailer, does not match the body | `400 BadDigest` |
//! | An `x-amz-checksum-*` value is not base64 | `400 BadDigest` |
//! | An `x-amz-checksum-*` value is base64 of the wrong length | `400 InvalidRequest` |
//! | `x-amz-sdk-checksum-algorithm` names another algorithm than the value | `400 BadDigest` |
//! | `x-amz-sdk-checksum-algorithm` without a value or declared trailer | `400 InvalidRequest` |
//! | More than one `x-amz-checksum-*` value, headers and trailers together | `400 InvalidRequest` |
//! | An algorithm name S3 does not define | `400 InvalidRequest` |
//! | SHA512, MD5 (`x-amz-checksum-md5`), or an XXHASH algorithm | `501 NotImplemented` |
//! | A trailing checksum on a body without trailers | `400 InvalidRequest` |
//!
//! The SigV4 layer answers `400 XAmzContentSHA256Mismatch` for a body that
//! does not match `x-amz-content-sha256`, and `400 MalformedTrailerError`
//! when a declared trailer is missing ([`BodyError`](crate::BodyError)).
//! SkyS3 never answers `XAmzContentChecksumMismatch`, which some
//! S3-compatible stores use for a checksum mismatch: the S3 API reference
//! and the `s3-tests` suite expect `BadDigest`, and SDKs treat both alike,
//! as a client error not to retry.

mod error;
mod hasher;
mod multipart;
mod pooled;
mod request;

pub use error::IntegrityError;
pub use hasher::{Digests, Hasher, Hashers, digest};
pub use multipart::{MultipartChecksum, MultipartEtag};
pub use pooled::{HASH_BATCH_BYTES, PooledHasher};
pub(crate) use request::parse_algorithm;
pub use request::{
    ChecksumValidator, DEFAULT_ALGORITHM, ExpectedChecksum, ExpectedChecksums, VerifiedBody,
};

#[cfg(test)]
mod tests;
