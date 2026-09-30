//! Write identities (§7.2) and version identities (§9.2).

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::position::parse_canonical_u64;
use crate::{BucketId, ClusterId, EpochSeq, IdError, ParseNumberError, Seq, ShardId};

/// The identity of one local write, carried in the user metadata of every
/// object SkyS3 writes to a remote (§7.2).
///
/// Its text form is `<cluster>/<bucket>/<shard>/<epoch>.<seq>`, at most
/// [`WriteIdentity::MAX_LEN`] (96) bytes. The ID limits guarantee the bound:
/// the fixed parts take at most 47 bytes (a 3-digit shard, two 20-digit
/// numbers, and 4 separators), and [`ClusterId::MAX_LEN`] +
/// [`BucketId::MAX_LEN`] is 49.
///
/// The text form is canonical: every identity has exactly one, and
/// [`FromStr`] accepts only that one. To decide whether a remote object
/// carries this identity, as the 412 recovery rule does, compare its
/// metadata value with [`WriteIdentity::matches`] rather than parsing it.
///
/// ```
/// use skys3_types::{BucketId, ClusterId, Epoch, EpochSeq, Seq, ShardId, WriteIdentity};
///
/// let wid = WriteIdentity::new(
///     ClusterId::new("skys3-prod-a")?,
///     BucketId::new("b-7f3a")?,
///     ShardId::new(5),
///     EpochSeq::new(Epoch::new(42), Seq::new(1001)),
/// );
/// assert_eq!(wid.to_string(), "skys3-prod-a/b-7f3a/5/42.1001");
/// assert!(wid.matches("skys3-prod-a/b-7f3a/5/42.1001"));
/// assert_eq!("skys3-prod-a/b-7f3a/5/42.1001".parse::<WriteIdentity>()?, wid);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WriteIdentity {
    /// The cluster that made the write.
    pub cluster: ClusterId,
    /// The bucket written.
    pub bucket: BucketId,
    /// The shard whose log holds the write's record.
    pub shard: ShardId,
    /// The position of the record the identity names: the committing `PUT`
    /// or `DELETE`, the `MPU_CREATE` of a multipart upload, or the
    /// `UPLOAD_BEGIN` of a streamed single PUT (§7.2).
    pub position: EpochSeq,
}

// The ID limits keep every write identity within its reservation.
const _: () = assert!(
    ClusterId::MAX_LEN + BucketId::MAX_LEN + ShardId::MAX_TEXT_LEN + EpochSeq::MAX_TEXT_LEN + 3
        == WriteIdentity::MAX_LEN
);

impl WriteIdentity {
    /// The longest text form in bytes.
    pub const MAX_LEN: usize = 96;

    /// The user-metadata key that carries the identity. On the wire it is the
    /// header `x-amz-meta-skys3-wid`.
    pub const METADATA_KEY: &'static str = "skys3-wid";

    /// The user-metadata bytes reserved for the identity: its key plus the
    /// longest value. S3 counts both keys and values against the 2 KiB
    /// user-metadata limit, so SkyS3 lowers the limit it enforces on clients
    /// by this much (§7.2).
    pub const METADATA_RESERVED_BYTES: usize = Self::METADATA_KEY.len() + Self::MAX_LEN;

    /// Assembles an identity.
    #[must_use]
    pub const fn new(
        cluster: ClusterId,
        bucket: BucketId,
        shard: ShardId,
        position: EpochSeq,
    ) -> Self {
        Self {
            cluster,
            bucket,
            shard,
            position,
        }
    }

    /// Whether `value`, for example the `skys3-wid` metadata of a remote
    /// object, is exactly this identity's text form.
    ///
    /// Byte comparison with the canonical form needs no parsing, so it is the
    /// right check for untrusted values.
    #[must_use]
    pub fn matches(&self, value: &str) -> bool {
        value.len() <= Self::MAX_LEN && value == self.to_string()
    }
}

impl fmt::Display for WriteIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{}/{}/{}",
            self.cluster, self.bucket, self.shard, self.position
        )
    }
}

/// Why a string was rejected as a write identity.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ParseWriteIdentityError {
    /// The string is longer than [`WriteIdentity::MAX_LEN`].
    #[error("write identity is {0} bytes long; the limit is {max}", max = WriteIdentity::MAX_LEN)]
    TooLong(usize),
    /// The string does not have four `/`-separated parts.
    #[error("write identity {0:?} is not of the form <cluster>/<bucket>/<shard>/<epoch>.<seq>")]
    Shape(String),
    /// The cluster or bucket ID is invalid.
    #[error("write identity has an invalid ID: {0}")]
    Id(#[from] IdError),
    /// The shard is not a canonical decimal number below 256.
    #[error("write identity has an invalid shard {0:?}")]
    Shard(String),
    /// The `<epoch>.<seq>` part is invalid.
    #[error("write identity has an invalid position: {0}")]
    Position(#[from] ParseNumberError),
}

impl FromStr for WriteIdentity {
    type Err = ParseWriteIdentityError;

    /// Parses the canonical text form, rejecting every other spelling.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() > Self::MAX_LEN {
            return Err(ParseWriteIdentityError::TooLong(s.len()));
        }
        let mut parts = s.split('/');
        let (Some(cluster), Some(bucket), Some(shard), Some(position), None) = (
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
        ) else {
            return Err(ParseWriteIdentityError::Shape(s.to_owned()));
        };
        let shard = parse_canonical_u64(shard)
            .ok()
            .and_then(|n| u8::try_from(n).ok())
            .ok_or_else(|| ParseWriteIdentityError::Shard(shard.to_owned()))?;
        Ok(Self {
            cluster: ClusterId::new(cluster)?,
            bucket: BucketId::new(bucket)?,
            shard: ShardId::new(shard),
            position: position.parse()?,
        })
    }
}

impl Serialize for WriteIdentity {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for WriteIdentity {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

/// Why a string was rejected as an entity tag.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ETagError {
    /// The tag is empty.
    #[error("entity tag is empty")]
    Empty,
    /// The tag is longer than [`ETag::MAX_LEN`].
    #[error("entity tag is {0} bytes long; the limit is {max}", max = ETag::MAX_LEN)]
    TooLong(usize),
    /// The tag contains a character outside `etagc` (RFC 9110, section
    /// 8.8.3): a control character, a space, a `"`, or a non-ASCII byte.
    #[error("entity tag contains {0:?}, which is not allowed")]
    InvalidChar(char),
    /// A quoted tag is not enclosed in double quotes. Weak tags (`W/"..."`)
    /// are rejected too: S3 never returns them.
    #[error("{0:?} is not a quoted strong entity tag")]
    NotQuoted(String),
}

/// An S3 strong entity tag, stored without its surrounding quotes.
///
/// For an object this is an MD5 hex digest, or for a multipart upload the
/// MD5 of the part digests followed by `-<parts>`; other providers may use
/// other opaque values. SkyS3 compares ETags byte for byte.
///
/// ```
/// use skys3_types::ETag;
///
/// let etag = ETag::from_quoted("\"9b2cf535f27731c974343645a3985328\"")?;
/// assert_eq!(etag.as_str(), "9b2cf535f27731c974343645a3985328");
/// assert_eq!(etag.to_quoted(), "\"9b2cf535f27731c974343645a3985328\"");
/// # Ok::<(), skys3_types::ETagError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ETag(String);

impl ETag {
    /// The longest tag accepted, in bytes, without quotes. S3 ETags are at
    /// most 38 bytes; the limit bounds what an untrusted remote can make
    /// SkyS3 store.
    pub const MAX_LEN: usize = 256;

    /// Validates an unquoted tag value.
    ///
    /// # Errors
    ///
    /// Returns an [`ETagError`] if `value` is empty, too long, or contains a
    /// character outside `etagc`.
    pub fn new(value: impl Into<String>) -> Result<Self, ETagError> {
        let value = value.into();
        if value.is_empty() {
            return Err(ETagError::Empty);
        }
        if value.len() > Self::MAX_LEN {
            return Err(ETagError::TooLong(value.len()));
        }
        if let Some(ch) = value.chars().find(|&c| !matches!(c, '!' | '#'..='~')) {
            return Err(ETagError::InvalidChar(ch));
        }
        Ok(Self(value))
    }

    /// Parses the quoted form of an HTTP `ETag` header, such as
    /// `"9b2cf535f27731c974343645a3985328"`.
    ///
    /// # Errors
    ///
    /// Returns [`ETagError::NotQuoted`] if `quoted` is not enclosed in double
    /// quotes, and otherwise the errors of [`ETag::new`].
    pub fn from_quoted(quoted: &str) -> Result<Self, ETagError> {
        quoted
            .strip_prefix('"')
            .and_then(|rest| rest.strip_suffix('"'))
            .ok_or_else(|| ETagError::NotQuoted(quoted.to_owned()))
            .and_then(Self::new)
    }

    /// The tag without quotes.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The tag in double quotes, as HTTP headers carry it.
    #[must_use]
    pub fn to_quoted(&self) -> String {
        format!("\"{}\"", self.0)
    }
}

impl fmt::Display for ETag {
    /// Writes the tag without quotes.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for ETag {
    type Err = ETagError;

    /// Parses an unquoted tag, like [`ETag::new`].
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl Serialize for ETag {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for ETag {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// The identity of one committed version of a key: its `seq` and the ETag
/// clients see (§9.2).
///
/// A version never changes, so any holder of an exact version identity can
/// serve its bytes. Read plans name versions by it, and the hot cache is
/// keyed by bucket, key, and version identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionIdentity {
    /// The sequence number of the record that committed the version.
    pub seq: Seq,
    /// The version's ETag as clients see it (`local_etag`, §4.2).
    pub etag: ETag,
}

impl VersionIdentity {
    /// Pairs a sequence number and an ETag.
    #[must_use]
    pub const fn new(seq: Seq, etag: ETag) -> Self {
        Self { seq, etag }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Epoch;

    fn wid() -> WriteIdentity {
        WriteIdentity::new(
            ClusterId::new("c1").unwrap(),
            BucketId::new("b1").unwrap(),
            ShardId::new(7),
            EpochSeq::new(Epoch::new(3), Seq::new(9)),
        )
    }

    #[test]
    fn write_identity_reserves_key_and_value() {
        assert_eq!(WriteIdentity::METADATA_RESERVED_BYTES, 105);
    }

    #[test]
    fn write_identity_matches_only_its_canonical_text() {
        let wid = wid();
        assert!(wid.matches("c1/b1/7/3.9"));
        for other in [
            "c1/b1/7/3.09",
            "c1/b1/07/3.9",
            "c1/b1/7/3.9 ",
            "C1/b1/7/3.9",
            "",
        ] {
            assert!(!wid.matches(other), "{other:?}");
        }
        assert!(!wid.matches(&"x".repeat(1000)));
    }

    #[test]
    fn write_identity_parse_rejects_each_malformed_part() {
        let err = |s: &str| s.parse::<WriteIdentity>().unwrap_err();
        assert_eq!(err(&"a".repeat(97)), ParseWriteIdentityError::TooLong(97));
        for shape in ["", "c1/b1/7", "c1/b1/7/3.9/x", "c1\\b1\\7\\3.9"] {
            assert_eq!(err(shape), ParseWriteIdentityError::Shape(shape.to_owned()));
        }
        assert!(matches!(err("C1/b1/7/3.9"), ParseWriteIdentityError::Id(_)));
        assert!(matches!(err("c1//7/3.9"), ParseWriteIdentityError::Id(_)));
        for shard in ["256", "07", "-1", "x", ""] {
            assert_eq!(
                err(&format!("c1/b1/{shard}/3.9")),
                ParseWriteIdentityError::Shard(shard.to_owned())
            );
        }
        assert!(matches!(
            err("c1/b1/7/3"),
            ParseWriteIdentityError::Position(_)
        ));
        assert_eq!(
            err("c1/b1/7/3").to_string(),
            "write identity has an invalid position: \"3\" is not of the form <epoch>.<seq>"
        );
    }

    #[test]
    fn write_identity_serializes_as_its_text_form() {
        let wid = wid();
        let json = serde_json::to_string(&wid).unwrap();
        assert_eq!(json, "\"c1/b1/7/3.9\"");
        assert_eq!(serde_json::from_str::<WriteIdentity>(&json).unwrap(), wid);
        assert!(serde_json::from_str::<WriteIdentity>("\"c1/b1/7\"").is_err());
    }

    #[test]
    fn etags_validate_their_characters_and_length() {
        assert!(ETag::new("abc-3").is_ok());
        assert!(ETag::new("!#~").is_ok());
        assert_eq!(ETag::new(""), Err(ETagError::Empty));
        assert!(ETag::new("e".repeat(ETag::MAX_LEN)).is_ok());
        assert_eq!(
            ETag::new("e".repeat(ETag::MAX_LEN + 1)),
            Err(ETagError::TooLong(257))
        );
        for (bad, ch) in [
            ("a\"b", '"'),
            ("a b", ' '),
            ("a\u{7f}", '\u{7f}'),
            ("é", 'é'),
        ] {
            assert_eq!(ETag::new(bad), Err(ETagError::InvalidChar(ch)));
        }
        assert_eq!("x".parse::<ETag>().unwrap().to_string(), "x");
    }

    #[test]
    fn etags_parse_only_quoted_strong_tags() {
        assert_eq!(ETag::from_quoted("\"x\"").unwrap().as_str(), "x");
        for bad in ["x", "\"x", "x\"", "W/\"x\"", "\""] {
            assert_eq!(
                ETag::from_quoted(bad),
                Err(ETagError::NotQuoted(bad.to_owned())),
                "{bad:?}"
            );
        }
        assert_eq!(ETag::from_quoted("\"\""), Err(ETagError::Empty));
        assert_eq!(
            ETagError::NotQuoted("x".into()).to_string(),
            "\"x\" is not a quoted strong entity tag"
        );
    }

    #[test]
    fn version_identities_round_trip_through_json() {
        let version = VersionIdentity::new(Seq::new(12), ETag::new("d41d8cd9").unwrap());
        let json = serde_json::to_string(&version).unwrap();
        assert_eq!(json, r#"{"seq":12,"etag":"d41d8cd9"}"#);
        assert_eq!(
            serde_json::from_str::<VersionIdentity>(&json).unwrap(),
            version
        );
        assert!(serde_json::from_str::<VersionIdentity>(r#"{"seq":12,"etag":"a b"}"#).is_err());
    }
}
