//! Size limits shared by the storage engine and configuration loading.
//!
//! The log record format (`skys3-log`, design §10.1) bounds its payloads,
//! and configuration loading (`skys3-config`) keeps `inline_max_bytes` and
//! `extent_bytes` within those bounds, so a valid configuration never asks
//! for a record the log rejects. Both crates read the limits from here, so
//! neither depends on the other.

/// The largest single PUT S3 accepts, in bytes (5 GiB).
pub const MAX_SINGLE_PUT_BYTES: u64 = 5 * 1024 * 1024 * 1024;

/// The most parts a multipart upload can have (the S3 limit), and so the
/// largest part count of a multipart ETag or composite checksum.
pub const MAX_PARTS: u32 = 10_000;

/// The largest log record payload, in bytes (16 MiB): one inline body or one
/// extent. `inline_max_bytes` and `extent_bytes` are at most this.
pub const MAX_RECORD_PAYLOAD_LEN: u32 = 16 * 1024 * 1024;

/// The smallest `extent_bytes`, in bytes (64 KiB).
pub const MIN_EXTENT_LEN: u32 = 64 * 1024;

/// The least a peer destination charges against `peer_staging_quota_bytes`
/// for each staging it opens and each extent it stages (64 KiB, the
/// smallest `extent_bytes`, design §7.8). The memory its staging index
/// takes then grows with the quota, as staging in the smallest extents
/// would, however small the frames a source sends.
pub const MIN_STAGING_CHARGE: u64 = MIN_EXTENT_LEN as u64;

/// The most extents one `PUT` record references: a
/// [`MAX_SINGLE_PUT_BYTES`] object in extents of [`MIN_EXTENT_LEN`].
pub const MAX_EXTENTS_PER_PUT: usize = (MAX_SINGLE_PUT_BYTES / MIN_EXTENT_LEN as u64) as usize;

const _: () = assert!(MAX_EXTENTS_PER_PUT == 81_920);
const _: () = assert!(MIN_EXTENT_LEN <= MAX_RECORD_PAYLOAD_LEN);
