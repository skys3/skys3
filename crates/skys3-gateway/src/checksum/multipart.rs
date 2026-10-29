//! A multipart object's ETag and checksum, from its parts' (§7.4).
//!
//! Both fold the parts in part order. Their inputs are digests, not data:
//! at most 10,000 parts of 32 bytes, so a completion hashes at most about
//! 320 KiB.

use std::num::NonZeroU16;

use md5::{Digest as _, Md5};
use skys3_types::ETag;
use skys3_types::checksum::{Checksum, ChecksumAlgorithm, ChecksumError, ChecksumType};
use skys3_types::limits::MAX_PARTS;

use super::hasher::{Hasher, crc_algorithm, crc_bytes, crc_value};

/// Builds a multipart object's ETag: the MD5 of its parts' MD5 digests,
/// concatenated in part order, in hex, then `-` and the part count.
///
/// ```
/// use skys3_gateway::checksum::MultipartEtag;
///
/// let mut etag = MultipartEtag::new();
/// etag.push(&[0; 16])?;
/// assert_eq!(etag.finish().unwrap().as_str(), "4ae71336e44bf9bf79d2752e234818a5-1");
/// # Ok::<(), skys3_types::checksum::ChecksumError>(())
/// ```
#[derive(Debug, Clone, Default)]
pub struct MultipartEtag {
    md5: Md5,
    parts: u16,
}

impl MultipartEtag {
    /// A builder with no parts yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds the next part's MD5, the digest its ETag holds
    /// ([`ETag::md5`]).
    ///
    /// # Errors
    ///
    /// [`ChecksumError::PartCount`] past [`MAX_PARTS`] parts.
    pub fn push(&mut self, part_md5: &[u8; 16]) -> Result<(), ChecksumError> {
        self.parts = next_part(self.parts)?;
        self.md5.update(part_md5);
        Ok(())
    }

    /// The ETag, or `None` if no part was added.
    #[must_use]
    pub fn finish(self) -> Option<ETag> {
        let parts = NonZeroU16::new(self.parts)?;
        Some(ETag::multipart(&self.md5.finalize().into(), parts))
    }
}

/// Builds a multipart object's checksum from its parts' checksums.
///
/// A `COMPOSITE` checksum is the digest of the parts' digests concatenated
/// in part order, with the part count. A `FULL_OBJECT` checksum is the CRC
/// of the whole object, which CRCs allow computing from the parts' CRCs and
/// lengths alone.
#[derive(Debug)]
pub struct MultipartChecksum {
    algorithm: ChecksumAlgorithm,
    fold: Fold,
    parts: u16,
}

#[derive(Debug)]
enum Fold {
    Composite(Hasher),
    FullObject(Option<u64>),
}

impl MultipartChecksum {
    /// A builder for a checksum of `algorithm` and `checksum_type`.
    ///
    /// # Errors
    ///
    /// [`ChecksumError::NotComposite`] or [`ChecksumError::NotFullObject`]
    /// for a combination S3 does not have.
    pub fn new(
        algorithm: ChecksumAlgorithm,
        checksum_type: ChecksumType,
    ) -> Result<Self, ChecksumError> {
        let fold = match checksum_type {
            ChecksumType::Composite if algorithm.supports_composite() => {
                Fold::Composite(Hasher::new(algorithm))
            }
            ChecksumType::Composite => return Err(ChecksumError::NotComposite(algorithm)),
            ChecksumType::FullObject if algorithm.supports_full_object_multipart() => {
                Fold::FullObject(None)
            }
            ChecksumType::FullObject => return Err(ChecksumError::NotFullObject(algorithm)),
        };
        Ok(Self {
            algorithm,
            fold,
            parts: 0,
        })
    }

    /// Adds the next part: its checksum's digest, and its length in bytes.
    ///
    /// # Errors
    ///
    /// [`ChecksumError::DigestLength`] for a digest of another algorithm,
    /// and [`ChecksumError::PartCount`] past [`MAX_PARTS`] parts.
    pub fn push(&mut self, part_digest: &[u8], part_len: u64) -> Result<(), ChecksumError> {
        Checksum::full_object(self.algorithm, part_digest)?;
        self.parts = next_part(self.parts)?;
        match &mut self.fold {
            Fold::Composite(hasher) => hasher.update(part_digest),
            Fold::FullObject(crc) => {
                let part = crc_value(part_digest);
                *crc = Some(match *crc {
                    None => part,
                    Some(head) => {
                        let algorithm = crc_algorithm(self.algorithm)
                            .expect("full-object multipart checksums are CRCs");
                        crc_fast::checksum_combine(algorithm, head, part, part_len)
                    }
                });
            }
        }
        Ok(())
    }

    /// The object's checksum.
    ///
    /// # Errors
    ///
    /// [`ChecksumError::PartCount`] if no part was added.
    pub fn finish(self) -> Result<Checksum, ChecksumError> {
        if self.parts == 0 {
            return Err(ChecksumError::PartCount(0));
        }
        match self.fold {
            Fold::Composite(hasher) => {
                Checksum::composite(self.algorithm, &hasher.finish(), self.parts.into())
            }
            Fold::FullObject(crc) => {
                let crc = crc.unwrap_or_default();
                Checksum::full_object(self.algorithm, &crc_bytes(self.algorithm, crc))
            }
        }
    }
}

fn next_part(parts: u16) -> Result<u16, ChecksumError> {
    let next = u32::from(parts) + 1;
    if next > MAX_PARTS {
        return Err(ChecksumError::PartCount(next));
    }
    Ok(parts + 1)
}
