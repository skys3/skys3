//! Object checksums as S3 reports them (§7.4): the algorithms, the
//! checksum types of multipart objects, and the stored form of a checksum.
//!
//! An object stores at most one checksum per algorithm ([`Checksums`]).
//! Each [`Checksum`] is a digest, plus a part count when it is a
//! `COMPOSITE` multipart checksum: a digest of the parts' digests, which S3
//! prints as `<base64>-<parts>`. Every other checksum is `FULL_OBJECT`: a
//! digest of the whole object's bytes, whether it was uploaded in one
//! request or, for the CRCs, combined from its parts' CRCs.
//!
//! ```
//! use skys3_types::checksum::{Checksum, ChecksumAlgorithm, ChecksumType};
//!
//! let algorithm: ChecksumAlgorithm = "sha256".parse()?;
//! let header = "uWBwpe1dxI4Vw8Gf0X9ynOdw/SS6VBzfWm9giiv1sf4=-3";
//! let checksum = Checksum::parse(algorithm, header)?;
//! assert_eq!(checksum.checksum_type(), ChecksumType::Composite);
//! assert_eq!(checksum.parts(), Some(3));
//! assert_eq!(algorithm.header_name(), "x-amz-checksum-sha256");
//! assert_eq!(checksum.to_string(), header);
//! # Ok::<(), skys3_types::checksum::ChecksumError>(())
//! ```

use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroU16;
use std::str::FromStr;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;

use crate::limits::MAX_PARTS;

/// An object's checksums (§7.4), at most one per algorithm, stored with
/// its log records and index entry so a flush forwards them and reads
/// return them.
pub type Checksums = BTreeMap<ChecksumAlgorithm, Checksum>;

/// A checksum algorithm S3 clients can supply: the `x-amz-checksum-*`
/// algorithms, and MD5 for `Content-MD5`.
///
/// The enum is exhaustive on purpose: every algorithm needs a hasher, so
/// adding one must touch every `match` on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ChecksumAlgorithm {
    /// CRC-32 (`x-amz-checksum-crc32`).
    Crc32,
    /// CRC-32C (`x-amz-checksum-crc32c`).
    Crc32c,
    /// CRC-64/NVME (`x-amz-checksum-crc64nvme`).
    Crc64Nvme,
    /// SHA-1 (`x-amz-checksum-sha1`).
    Sha1,
    /// SHA-256 (`x-amz-checksum-sha256`).
    Sha256,
    /// MD5 (`Content-MD5`).
    Md5,
}

impl ChecksumAlgorithm {
    /// Every algorithm, in code order.
    pub const ALL: [Self; 6] = [
        Self::Crc32,
        Self::Crc32c,
        Self::Crc64Nvme,
        Self::Sha1,
        Self::Sha256,
        Self::Md5,
    ];

    /// The algorithms of `x-amz-checksum-*` headers: every one but MD5.
    pub const FLEXIBLE: [Self; 5] = [
        Self::Crc32,
        Self::Crc32c,
        Self::Crc64Nvme,
        Self::Sha1,
        Self::Sha256,
    ];

    /// The algorithm's code in log records and index entries.
    #[must_use]
    pub const fn code(self) -> u8 {
        match self {
            Self::Crc32 => 1,
            Self::Crc32c => 2,
            Self::Crc64Nvme => 3,
            Self::Sha1 => 4,
            Self::Sha256 => 5,
            Self::Md5 => 6,
        }
    }

    /// The algorithm with `code`, if any.
    #[must_use]
    pub fn from_code(code: u8) -> Option<Self> {
        Self::ALL.into_iter().find(|a| a.code() == code)
    }

    /// The length of a digest, in bytes. A CRC is its big-endian value.
    #[must_use]
    pub const fn digest_len(self) -> usize {
        match self {
            Self::Crc32 | Self::Crc32c => 4,
            Self::Crc64Nvme => 8,
            Self::Sha1 => 20,
            Self::Sha256 => 32,
            Self::Md5 => 16,
        }
    }

    /// The name S3 uses in `x-amz-sdk-checksum-algorithm`,
    /// `x-amz-checksum-algorithm`, and messages, such as `CRC64NVME`.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Crc32 => "CRC32",
            Self::Crc32c => "CRC32C",
            Self::Crc64Nvme => "CRC64NVME",
            Self::Sha1 => "SHA1",
            Self::Sha256 => "SHA256",
            Self::Md5 => "MD5",
        }
    }

    /// The lowercase header that carries a checksum of this algorithm:
    /// `x-amz-checksum-<name>`, or `content-md5` for MD5.
    #[must_use]
    pub const fn header_name(self) -> &'static str {
        match self {
            Self::Crc32 => "x-amz-checksum-crc32",
            Self::Crc32c => "x-amz-checksum-crc32c",
            Self::Crc64Nvme => "x-amz-checksum-crc64nvme",
            Self::Sha1 => "x-amz-checksum-sha1",
            Self::Sha256 => "x-amz-checksum-sha256",
            Self::Md5 => "content-md5",
        }
    }

    /// The flexible algorithm whose header is `name`, compared without
    /// regard to case.
    #[must_use]
    pub fn from_header_name(name: &str) -> Option<Self> {
        Self::FLEXIBLE
            .into_iter()
            .find(|a| a.header_name().eq_ignore_ascii_case(name))
    }

    /// Whether a multipart object can have a `COMPOSITE` checksum of this
    /// algorithm: CRC32, CRC32C, SHA1, and SHA256.
    #[must_use]
    pub const fn supports_composite(self) -> bool {
        matches!(self, Self::Crc32 | Self::Crc32c | Self::Sha1 | Self::Sha256)
    }

    /// Whether a multipart object can have a `FULL_OBJECT` checksum of this
    /// algorithm, combined from its parts': the CRCs.
    #[must_use]
    pub const fn supports_full_object_multipart(self) -> bool {
        matches!(self, Self::Crc32 | Self::Crc32c | Self::Crc64Nvme)
    }

    /// The checksum type a multipart upload gets when it names this
    /// algorithm without a type, as S3 chooses: `FULL_OBJECT` for
    /// CRC64NVME, `COMPOSITE` for the others. `None` for MD5, which is not
    /// a multipart checksum algorithm.
    #[must_use]
    pub const fn default_multipart_type(self) -> Option<ChecksumType> {
        match self {
            Self::Crc64Nvme => Some(ChecksumType::FullObject),
            Self::Crc32 | Self::Crc32c | Self::Sha1 | Self::Sha256 => Some(ChecksumType::Composite),
            Self::Md5 => None,
        }
    }
}

impl fmt::Display for ChecksumAlgorithm {
    /// Writes the S3 name, such as `CRC32C`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl FromStr for ChecksumAlgorithm {
    type Err = ChecksumError;

    /// Parses a flexible algorithm's S3 name without regard to case, as
    /// `x-amz-sdk-checksum-algorithm` carries it. `MD5` is rejected: in
    /// those headers it names S3's `x-amz-checksum-md5`, not
    /// `Content-MD5`.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::FLEXIBLE
            .into_iter()
            .find(|a| a.name().eq_ignore_ascii_case(s))
            .ok_or_else(|| ChecksumError::UnknownAlgorithm(s.to_owned()))
    }
}

/// How a multipart object's checksum was formed (`x-amz-checksum-type`).
/// A single-part object's checksums are always `FULL_OBJECT`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ChecksumType {
    /// A checksum of the whole object's bytes (`FULL_OBJECT`).
    FullObject,
    /// A checksum of the parts' checksums, with a part count
    /// (`COMPOSITE`).
    Composite,
}

impl ChecksumType {
    /// The header value: `FULL_OBJECT` or `COMPOSITE`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FullObject => "FULL_OBJECT",
            Self::Composite => "COMPOSITE",
        }
    }
}

impl fmt::Display for ChecksumType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ChecksumType {
    type Err = ChecksumError;

    /// Parses `FULL_OBJECT` or `COMPOSITE`, without regard to case.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        [Self::FullObject, Self::Composite]
            .into_iter()
            .find(|t| t.as_str().eq_ignore_ascii_case(s))
            .ok_or_else(|| ChecksumError::UnknownType(s.to_owned()))
    }
}

/// One stored checksum: a digest, and the part count of a `COMPOSITE`
/// checksum.
///
/// Which algorithm made the digest is the key it is stored under in
/// [`Checksums`]; [`Checksum::check`] tells whether the two fit.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Checksum {
    digest: Box<[u8]>,
    parts: Option<NonZeroU16>,
}

impl Checksum {
    /// A `FULL_OBJECT` checksum.
    ///
    /// # Errors
    ///
    /// [`ChecksumError::DigestLength`] if `digest` is not as long as
    /// `algorithm`'s digests.
    pub fn full_object(algorithm: ChecksumAlgorithm, digest: &[u8]) -> Result<Self, ChecksumError> {
        let checksum = Self {
            digest: digest.into(),
            parts: None,
        };
        checksum.check(algorithm)?;
        Ok(checksum)
    }

    /// A `COMPOSITE` checksum of `parts` parts: `digest` is the digest of
    /// the parts' digests.
    ///
    /// # Errors
    ///
    /// [`ChecksumError::DigestLength`] for a digest of the wrong length,
    /// [`ChecksumError::NotComposite`] for an algorithm without composite
    /// checksums, and [`ChecksumError::PartCount`] unless `parts` is from 1
    /// to [`MAX_PARTS`].
    pub fn composite(
        algorithm: ChecksumAlgorithm,
        digest: &[u8],
        parts: u32,
    ) -> Result<Self, ChecksumError> {
        let parts = u16::try_from(parts)
            .ok()
            .filter(|&n| u32::from(n) <= MAX_PARTS)
            .and_then(NonZeroU16::new)
            .ok_or(ChecksumError::PartCount(parts))?;
        let checksum = Self {
            digest: digest.into(),
            parts: Some(parts),
        };
        checksum.check(algorithm)?;
        Ok(checksum)
    }

    /// Parses a header value: the digest in padded standard base64,
    /// followed by `-<parts>` for a composite checksum.
    ///
    /// # Errors
    ///
    /// [`ChecksumError::NotBase64`] if the digest is not base64,
    /// [`ChecksumError::PartCount`] for a malformed part count, and the
    /// errors of [`Checksum::full_object`] and [`Checksum::composite`].
    pub fn parse(algorithm: ChecksumAlgorithm, value: &str) -> Result<Self, ChecksumError> {
        let (encoded, parts) = match value.rsplit_once('-') {
            Some((encoded, parts)) => (encoded, Some(parse_parts(parts)?)),
            None => (value, None),
        };
        let digest = decode_digest(encoded)?;
        match parts {
            Some(parts) => Self::composite(algorithm, &digest, parts),
            None => Self::full_object(algorithm, &digest),
        }
    }

    /// Checks that this checksum can be one of `algorithm`.
    ///
    /// # Errors
    ///
    /// [`ChecksumError::DigestLength`] or [`ChecksumError::NotComposite`].
    pub fn check(&self, algorithm: ChecksumAlgorithm) -> Result<(), ChecksumError> {
        if self.digest.len() != algorithm.digest_len() {
            return Err(ChecksumError::DigestLength {
                algorithm,
                len: self.digest.len(),
            });
        }
        if self.parts.is_some() && !algorithm.supports_composite() {
            return Err(ChecksumError::NotComposite(algorithm));
        }
        Ok(())
    }

    /// The digest: of the object, or of its parts' digests.
    #[must_use]
    pub fn digest(&self) -> &[u8] {
        &self.digest
    }

    /// The part count of a composite checksum.
    #[must_use]
    pub fn parts(&self) -> Option<u16> {
        self.parts.map(NonZeroU16::get)
    }

    /// `COMPOSITE` if the checksum has a part count, else `FULL_OBJECT`.
    #[must_use]
    pub fn checksum_type(&self) -> ChecksumType {
        if self.parts.is_some() {
            ChecksumType::Composite
        } else {
            ChecksumType::FullObject
        }
    }
}

impl fmt::Display for Checksum {
    /// Writes the header value: base64, then `-<parts>` if composite.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&STANDARD.encode(&self.digest))?;
        match self.parts {
            Some(parts) => write!(f, "-{parts}"),
            None => Ok(()),
        }
    }
}

impl fmt::Debug for Checksum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Checksum({self})")
    }
}

/// Decodes a digest in padded standard base64, the encoding of every
/// checksum header (`Content-MD5` included).
///
/// # Errors
///
/// [`ChecksumError::NotBase64`] if `encoded` is not canonical base64.
pub fn decode_digest(encoded: &str) -> Result<Vec<u8>, ChecksumError> {
    STANDARD
        .decode(encoded)
        .map_err(|_| ChecksumError::NotBase64)
}

/// Encodes a digest as checksum headers carry it.
#[must_use]
pub fn encode_digest(digest: &[u8]) -> String {
    STANDARD.encode(digest)
}

fn parse_parts(text: &str) -> Result<u32, ChecksumError> {
    // Digits only: `u32::from_str` would accept a leading `+`.
    if text.is_empty() || text.len() > 5 || !text.bytes().all(|b| b.is_ascii_digit()) {
        return Err(ChecksumError::MalformedParts);
    }
    text.parse().map_err(|_| ChecksumError::MalformedParts)
}

/// Why a checksum, or its algorithm or type, was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ChecksumError {
    /// The digest is not canonical padded base64.
    #[error("the checksum is not base64")]
    NotBase64,
    /// The digest's length does not fit the algorithm.
    #[error("a {algorithm} digest cannot be {len} bytes long")]
    DigestLength {
        /// The algorithm.
        algorithm: ChecksumAlgorithm,
        /// The digest's length, in bytes.
        len: usize,
    },
    /// The algorithm has no composite checksums.
    #[error("{0} checksums cannot be composite")]
    NotComposite(ChecksumAlgorithm),
    /// The algorithm has no `FULL_OBJECT` checksums of multipart objects:
    /// only CRCs combine.
    #[error("{0} checksums of multipart objects cannot be FULL_OBJECT")]
    NotFullObject(ChecksumAlgorithm),
    /// A composite checksum's part count is not a number.
    #[error("the part count of a composite checksum is not a number")]
    MalformedParts,
    /// A composite checksum's part count is out of range.
    #[error("a composite checksum cannot have {0} parts; the limit is {MAX_PARTS}")]
    PartCount(u32),
    /// The name is not a supported checksum algorithm.
    #[error("{0:?} is not a supported checksum algorithm")]
    UnknownAlgorithm(String),
    /// The name is not a checksum type.
    #[error("{0:?} is not a checksum type")]
    UnknownType(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn algorithms_round_trip_through_codes_names_and_headers() {
        for algorithm in ChecksumAlgorithm::ALL {
            assert_eq!(
                ChecksumAlgorithm::from_code(algorithm.code()),
                Some(algorithm)
            );
            assert_eq!(algorithm.to_string(), algorithm.name());
        }
        for algorithm in ChecksumAlgorithm::FLEXIBLE {
            assert_eq!(algorithm.name().to_lowercase().parse(), Ok(algorithm));
            let header = algorithm.header_name().to_uppercase();
            assert_eq!(
                ChecksumAlgorithm::from_header_name(&header),
                Some(algorithm)
            );
        }
        assert_eq!(ChecksumAlgorithm::from_code(0), None);
        assert_eq!(ChecksumAlgorithm::Md5.header_name(), "content-md5");
        assert_eq!(ChecksumAlgorithm::from_header_name("content-md5"), None);
        assert_eq!(
            "MD5".parse::<ChecksumAlgorithm>(),
            Err(ChecksumError::UnknownAlgorithm("MD5".into()))
        );
    }

    #[test]
    fn multipart_support_follows_s3() {
        use ChecksumAlgorithm::*;
        let composite: Vec<_> = ChecksumAlgorithm::ALL
            .into_iter()
            .filter(|a| a.supports_composite())
            .collect();
        assert_eq!(composite, [Crc32, Crc32c, Sha1, Sha256]);
        let full: Vec<_> = ChecksumAlgorithm::ALL
            .into_iter()
            .filter(|a| a.supports_full_object_multipart())
            .collect();
        assert_eq!(full, [Crc32, Crc32c, Crc64Nvme]);
        assert_eq!(
            Crc64Nvme.default_multipart_type(),
            Some(ChecksumType::FullObject)
        );
        assert_eq!(Sha1.default_multipart_type(), Some(ChecksumType::Composite));
        assert_eq!(Md5.default_multipart_type(), None);
    }

    #[test]
    fn types_parse_and_print() {
        for t in [ChecksumType::FullObject, ChecksumType::Composite] {
            assert_eq!(t.to_string().to_lowercase().parse(), Ok(t));
        }
        assert!(matches!(
            "PARTIAL".parse::<ChecksumType>(),
            Err(ChecksumError::UnknownType(_))
        ));
    }

    #[test]
    fn header_values_parse_and_print() {
        let crc = Checksum::parse(ChecksumAlgorithm::Crc32, "JRTCyQ==").unwrap();
        assert_eq!(crc.digest(), [0x25, 0x14, 0xc2, 0xc9]);
        assert_eq!(crc.checksum_type(), ChecksumType::FullObject);
        assert_eq!(crc.parts(), None);
        assert_eq!(crc.to_string(), "JRTCyQ==");
        assert_eq!(format!("{crc:?}"), "Checksum(JRTCyQ==)");
        let composite = Checksum::parse(ChecksumAlgorithm::Crc32c, "MDaLrw==-10000").unwrap();
        assert_eq!(composite.parts(), Some(10_000));
        assert_eq!(composite.to_string(), "MDaLrw==-10000");

        fn parse(algorithm: ChecksumAlgorithm, value: &str) -> ChecksumError {
            Checksum::parse(algorithm, value).unwrap_err()
        }
        use ChecksumAlgorithm::*;
        assert_eq!(parse(Crc32, "bad"), ChecksumError::NotBase64);
        assert_eq!(parse(Crc32, "JRTCyQ"), ChecksumError::NotBase64);
        assert_eq!(
            parse(Crc32, ""),
            ChecksumError::DigestLength {
                algorithm: Crc32,
                len: 0
            }
        );
        assert_eq!(
            parse(Sha256, "JRTCyQ=="),
            ChecksumError::DigestLength {
                algorithm: Sha256,
                len: 4
            }
        );
        assert_eq!(parse(Crc32, "JRTCyQ==-0"), ChecksumError::PartCount(0));
        assert_eq!(
            parse(Crc32, "JRTCyQ==-10001"),
            ChecksumError::PartCount(10_001)
        );
        for parts in ["", "+1", "x", "123456"] {
            assert_eq!(
                parse(Crc32, &format!("JRTCyQ==-{parts}")),
                ChecksumError::MalformedParts
            );
        }
        assert_eq!(
            parse(Crc64Nvme, "L/E4WYn8v98=-2"),
            ChecksumError::NotComposite(Crc64Nvme)
        );
        assert_eq!(
            Checksum::composite(Md5, &[0; 16], 2),
            Err(ChecksumError::NotComposite(Md5))
        );
        assert_eq!(
            Checksum::composite(Sha1, &[0; 20], 70_000),
            Err(ChecksumError::PartCount(70_000))
        );
    }

    #[test]
    fn check_matches_digests_to_algorithms() {
        let checksum = Checksum::full_object(ChecksumAlgorithm::Sha1, &[7; 20]).unwrap();
        assert!(checksum.check(ChecksumAlgorithm::Sha1).is_ok());
        assert!(checksum.check(ChecksumAlgorithm::Sha256).is_err());
        assert_eq!(encode_digest(checksum.digest()), checksum.to_string());
        assert_eq!(decode_digest(&checksum.to_string()).unwrap(), [7; 20]);
        let error = ChecksumError::DigestLength {
            algorithm: ChecksumAlgorithm::Sha1,
            len: 3,
        };
        assert_eq!(error.to_string(), "a SHA1 digest cannot be 3 bytes long");
    }
}
