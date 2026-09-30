//! Shard numbers, shard counts, and the key-to-shard hash (§4.1).
//!
//! # The shard hash, version 1
//!
//! A key's shard is fixed for the life of its bucket, so this function can
//! never change. It is specified here in full so that any implementation, in
//! any language, computes the same result:
//!
//! 1. Build the byte string `"skys3-shard-v1" 0x00 bucket_id 0x00 key`: the
//!    ASCII domain tag, a zero byte, the bucket ID's bytes, a zero byte, and
//!    the key's bytes exactly as stored (S3 keys are compared byte for byte,
//!    so nothing is normalized).
//! 2. Hash it with SHA-256 (FIPS 180-4).
//! 3. The [`KeyHash`] is the first 8 bytes of the digest as a big-endian
//!    `u64`.
//! 4. The shard is `key_hash mod shards_per_bucket`.
//!
//! A bucket ID never contains a zero byte, so the encoding is unambiguous.
//! The domain tag keeps this hash independent of any other SHA-256 of the
//! same bytes. With at most 256 shards, the modulo bias is below 2⁻⁵⁶. The
//! result can be checked with standard tools:
//!
//! ```text
//! $ printf 'skys3-shard-v1\0b-7f3a\0photos/cat.jpg' | sha256sum | cut -c1-16
//! ```
//!
//! Golden vectors in `tests/golden.rs` freeze the function.

use std::fmt;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::BucketId;

/// The number of a shard within its bucket, from 0 to 255 (a bucket has at
/// most [`ShardCount::MAX`] shards).
///
/// A shard is named by its bucket and this number, as in the register path
/// `shards/<bucket>/<n>.json` and the write identity (§6.1, §7.2).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct ShardId(u8);

impl ShardId {
    /// The longest decimal text form, `"255"`.
    pub const MAX_TEXT_LEN: usize = 3;

    /// Wraps a shard number. Every `u8` is a valid shard number, because a
    /// bucket has at most [`ShardCount::MAX`] (256) shards.
    #[must_use]
    pub const fn new(number: u8) -> Self {
        Self(number)
    }

    /// The shard number.
    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }
}

impl From<u8> for ShardId {
    fn from(number: u8) -> Self {
        Self(number)
    }
}

impl fmt::Display for ShardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// A shard count that is out of range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("shards_per_bucket is {0}; it must be from 1 to 256")]
pub struct ShardCountError(pub u32);

/// A bucket's number of shards (`shards_per_bucket`, §4.1): from 1 to
/// [`ShardCount::MAX`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct ShardCount(u16);

impl ShardCount {
    /// The largest shard count, 256.
    pub const MAX: u32 = 256;

    /// Validates a shard count.
    ///
    /// # Errors
    ///
    /// Returns [`ShardCountError`] unless `count` is from 1 to
    /// [`ShardCount::MAX`].
    pub const fn new(count: u32) -> Result<Self, ShardCountError> {
        if count >= 1 && count <= Self::MAX {
            // Lossless: `count` is at most 256.
            Ok(Self(count as u16))
        } else {
            Err(ShardCountError(count))
        }
    }

    /// The shard count.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0 as u32
    }

    /// Whether `shard` is one of this many shards.
    #[must_use]
    pub const fn contains(self, shard: ShardId) -> bool {
        (shard.0 as u16) < self.0
    }

    /// Every shard number, in order.
    pub fn shards(self) -> impl DoubleEndedIterator<Item = ShardId> + ExactSizeIterator {
        // Every shard number is below 256, so the conversion never fails.
        (0..self.0).map(|n| ShardId(u8::try_from(n).unwrap_or(u8::MAX)))
    }
}

impl TryFrom<u32> for ShardCount {
    type Error = ShardCountError;

    fn try_from(count: u32) -> Result<Self, Self::Error> {
        Self::new(count)
    }
}

impl From<ShardCount> for u32 {
    fn from(count: ShardCount) -> Self {
        count.get()
    }
}

impl fmt::Display for ShardCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// The 64-bit hash of a key within its bucket (shard hash version 1; see the
/// [module documentation](self)).
///
/// ```
/// use skys3_types::{BucketId, KeyHash, ShardCount};
///
/// let bucket = BucketId::new("b-7f3a")?;
/// let hash = KeyHash::of(&bucket, b"photos/cat.jpg");
/// assert_eq!(hash.get(), 0xb07d_3a17_b8ce_3057);
/// assert_eq!(hash.shard(ShardCount::new(8)?).get(), 7);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct KeyHash(u64);

impl KeyHash {
    /// The domain tag that starts every hashed byte string.
    pub const DOMAIN_TAG: &'static [u8] = b"skys3-shard-v1";

    /// Hashes `key` within `bucket`.
    #[must_use]
    pub fn of(bucket: &BucketId, key: &[u8]) -> Self {
        let digest = Sha256::new()
            .chain_update(Self::DOMAIN_TAG)
            .chain_update([0])
            .chain_update(bucket.as_str().as_bytes())
            .chain_update([0])
            .chain_update(key)
            .finalize();
        let mut prefix = [0; 8];
        prefix.copy_from_slice(&digest[..8]);
        Self(u64::from_be_bytes(prefix))
    }

    /// Wraps a hash value computed earlier, for example one read back from
    /// storage.
    #[must_use]
    pub const fn from_raw(hash: u64) -> Self {
        Self(hash)
    }

    /// The hash value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The shard this hash assigns its key to among `shards` shards.
    #[must_use]
    pub const fn shard(self, shards: ShardCount) -> ShardId {
        // The remainder is below 256, so the cast is lossless.
        ShardId((self.0 % shards.0 as u64) as u8)
    }
}

/// The shard of `key` in `bucket`: `hash(bucket_id, key) mod shards` (§4.1).
///
/// Shorthand for [`KeyHash::of`] followed by [`KeyHash::shard`].
#[must_use]
pub fn shard_for_key(bucket: &BucketId, key: &[u8], shards: ShardCount) -> ShardId {
    KeyHash::of(bucket, key).shard(shards)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_counts_are_bounded() {
        assert_eq!(ShardCount::new(0), Err(ShardCountError(0)));
        assert_eq!(ShardCount::new(257), Err(ShardCountError(257)));
        assert_eq!(ShardCount::new(1).map(ShardCount::get), Ok(1));
        assert_eq!(ShardCount::new(256).map(ShardCount::get), Ok(256));
        assert_eq!(
            ShardCountError(0).to_string(),
            "shards_per_bucket is 0; it must be from 1 to 256"
        );
    }

    #[test]
    fn shard_counts_enumerate_their_shards() {
        let max = ShardCount::new(ShardCount::MAX).unwrap();
        let all: Vec<_> = max.shards().collect();
        assert_eq!(all.len(), 256);
        assert_eq!(all.first(), Some(&ShardId::new(0)));
        assert_eq!(all.last(), Some(&ShardId::new(255)));
        assert!(max.contains(ShardId::new(255)));
        let eight = ShardCount::new(8).unwrap();
        assert!(eight.contains(ShardId::new(7)));
        assert!(!eight.contains(ShardId::new(8)));
        assert_eq!(eight.shards().next_back(), Some(ShardId::new(7)));
        assert_eq!(eight.to_string(), "8");
        assert_eq!(u32::from(eight), 8);
    }

    #[test]
    fn shard_counts_serialize_as_validated_numbers() {
        let eight = ShardCount::new(8).unwrap();
        assert_eq!(serde_json::to_string(&eight).unwrap(), "8");
        assert_eq!(serde_json::from_str::<ShardCount>("8").unwrap(), eight);
        let err = serde_json::from_str::<ShardCount>("300").unwrap_err();
        assert!(err.to_string().contains("from 1 to 256"), "{err}");
        assert!(serde_json::from_str::<ShardCount>("0").is_err());
    }

    #[test]
    fn shard_ids_display_and_serialize_as_numbers() {
        let id = ShardId::from(5);
        assert_eq!(id.get(), 5);
        assert_eq!(id.to_string(), "5");
        assert_eq!(ShardId::new(255).to_string().len(), ShardId::MAX_TEXT_LEN);
        assert_eq!(serde_json::to_string(&id).unwrap(), "5");
        assert!(serde_json::from_str::<ShardId>("256").is_err());
    }

    #[test]
    fn key_hash_depends_on_bucket_and_key() {
        let a = BucketId::new("a").unwrap();
        let b = BucketId::new("b").unwrap();
        assert_ne!(KeyHash::of(&a, b"k"), KeyHash::of(&b, b"k"));
        assert_ne!(KeyHash::of(&a, b"k"), KeyHash::of(&a, b"k2"));
        // The separator keeps the bucket and key apart.
        let ab = BucketId::new("ab").unwrap();
        assert_ne!(KeyHash::of(&a, b"bk"), KeyHash::of(&ab, b"k"));
        let hash = KeyHash::of(&a, b"k");
        assert_eq!(KeyHash::from_raw(hash.get()), hash);
        let one = ShardCount::new(1).unwrap();
        assert_eq!(shard_for_key(&a, b"k", one), ShardId::new(0));
    }
}
