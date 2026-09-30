//! Validated string identifiers: clusters, buckets, nodes, labels, bucket
//! names, and proposal IDs.
//!
//! Cluster, bucket, and node IDs and labels share one character set: lowercase
//! ASCII letters, digits, and `-`, starting and ending with a letter or digit
//! (the lowercase form of a DNS label, RFC 1123). Identifiers of that form are
//! safe everywhere SkyS3 puts them: control-store register paths and etcd
//! keys, file names on case-insensitive file systems, HTTP header values such
//! as the write identity (§7.2), and metric labels. None of them can contain
//! `/`, `.`, or `\0`, which the write identity and the shard hash use as
//! separators.
//!
//! Every type validates in its constructor, in [`FromStr`], and when it is
//! deserialized, so a value of the type is always valid.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Why a string was rejected as an identifier.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum IdError {
    /// The string is shorter than the identifier's minimum length.
    #[error("{kind} {value:?} is shorter than {min} bytes")]
    TooShort {
        /// The kind of identifier, for example `"cluster ID"`.
        kind: &'static str,
        /// The rejected string.
        value: String,
        /// The minimum length in bytes.
        min: usize,
    },
    /// The string is longer than the identifier's maximum length.
    #[error("{kind} is {len} bytes long; the limit is {max}")]
    TooLong {
        /// The kind of identifier.
        kind: &'static str,
        /// The length of the rejected string in bytes.
        len: usize,
        /// The maximum length in bytes.
        max: usize,
    },
    /// The string contains a character the identifier does not allow.
    #[error("{kind} {value:?} contains {ch:?} at byte {index}; {allowed}")]
    InvalidChar {
        /// The kind of identifier.
        kind: &'static str,
        /// The rejected string.
        value: String,
        /// The first offending character.
        ch: char,
        /// Its byte offset.
        index: usize,
        /// A description of the allowed characters.
        allowed: &'static str,
    },
    /// The string starts or ends with a character that is allowed only
    /// inside the identifier.
    #[error("{kind} {value:?} must start and end with a lowercase letter or a digit")]
    InvalidEdge {
        /// The kind of identifier.
        kind: &'static str,
        /// The rejected string.
        value: String,
    },
    /// The string breaks a rule specific to the identifier.
    #[error("{kind} {value:?} {rule}")]
    Rule {
        /// The kind of identifier.
        kind: &'static str,
        /// The rejected string.
        value: String,
        /// The rule that was broken, as a phrase ("must not ...").
        rule: &'static str,
    },
}

const LABEL_CHARS: &str = "allowed are lowercase ASCII letters, digits, and '-'";

/// Checks the lowercase DNS-label form shared by cluster, bucket, and node
/// IDs and labels.
fn validate_label(kind: &'static str, value: &str, max: usize) -> Result<(), IdError> {
    check_length(kind, value, 1, max)?;
    if let Some((index, ch)) = value
        .char_indices()
        .find(|&(_, c)| !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'))
    {
        return Err(IdError::InvalidChar {
            kind,
            value: value.to_owned(),
            ch,
            index,
            allowed: LABEL_CHARS,
        });
    }
    if value.starts_with('-') || value.ends_with('-') {
        return Err(IdError::InvalidEdge {
            kind,
            value: value.to_owned(),
        });
    }
    Ok(())
}

fn check_length(kind: &'static str, value: &str, min: usize, max: usize) -> Result<(), IdError> {
    if value.len() < min {
        return Err(IdError::TooShort {
            kind,
            value: value.to_owned(),
            min,
        });
    }
    if value.len() > max {
        return Err(IdError::TooLong {
            kind,
            len: value.len(),
            max,
        });
    }
    Ok(())
}

/// Defines a validated string identifier with the common trait impls.
macro_rules! string_id {
    (
        $(#[$meta:meta])*
        $name:ident, kind = $kind:literal, validate = $validate:expr
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            /// The kind of identifier, as error messages name it.
            pub const KIND: &'static str = $kind;

            /// Validates `value` and wraps it.
            ///
            /// # Errors
            ///
            /// Returns an [`IdError`] naming the first rule `value` breaks.
            pub fn new(value: impl Into<String>) -> Result<Self, IdError> {
                let value = value.into();
                let validate: fn(&str) -> Result<(), IdError> = $validate;
                validate(&value)?;
                Ok(Self(value))
            }

            /// The identifier as a string slice.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Unwraps the identifier into its string.
            #[must_use]
            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = IdError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::new(s)
            }
        }

        impl TryFrom<String> for $name {
            type Error = IdError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = IdError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl From<$name> for String {
            fn from(id: $name) -> Self {
                id.0
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let value = String::deserialize(deserializer)?;
                Self::new(value).map_err(serde::de::Error::custom)
            }
        }
    };
}

string_id! {
    /// The ID of a SkyS3 cluster (`[cluster] cluster_id`, §14).
    ///
    /// 1 to [`ClusterId::MAX_LEN`] bytes of lowercase ASCII letters, digits,
    /// and `-`, starting and ending with a letter or digit. The limit keeps
    /// every [`WriteIdentity`](crate::WriteIdentity) within its 96 bytes
    /// (§7.2).
    ///
    /// ```
    /// use skys3_types::ClusterId;
    ///
    /// let id = ClusterId::new("skys3-prod-a")?;
    /// assert_eq!(id.as_str(), "skys3-prod-a");
    /// assert!(ClusterId::new("Prod").is_err());
    /// # Ok::<(), skys3_types::IdError>(())
    /// ```
    ClusterId, kind = "cluster ID", validate = |s| validate_label(ClusterId::KIND, s, ClusterId::MAX_LEN)
}

impl ClusterId {
    /// The maximum length in bytes.
    pub const MAX_LEN: usize = 24;
}

string_id! {
    /// The ID of a bucket, which keys its shards (§4.1) and appears in its
    /// write identities (§7.2).
    ///
    /// A bucket ID is not the S3 bucket name ([`BucketName`]). It is
    /// assigned when the bucket is created and never reused within the
    /// cluster, even after the bucket is deleted. A recreated bucket
    /// therefore hashes keys differently and writes identities that no
    /// object flushed by its predecessor can carry, so the 412 recovery rule
    /// (§7.2) can never mistake an old remote object for its own write.
    ///
    /// 1 to [`BucketId::MAX_LEN`] bytes of lowercase ASCII letters, digits,
    /// and `-`, starting and ending with a letter or digit.
    ///
    /// ```
    /// use skys3_types::BucketId;
    ///
    /// let id: BucketId = "b-7f3a".parse()?;
    /// assert_eq!(id.to_string(), "b-7f3a");
    /// # Ok::<(), skys3_types::IdError>(())
    /// ```
    BucketId, kind = "bucket ID", validate = |s| validate_label(BucketId::KIND, s, BucketId::MAX_LEN)
}

impl BucketId {
    /// The maximum length in bytes.
    pub const MAX_LEN: usize = 25;
}

string_id! {
    /// The ID of a node, as it registers itself in `nodes/<node-id>.json`
    /// (§6.1).
    ///
    /// 1 to [`NodeId::MAX_LEN`] bytes of lowercase ASCII letters, digits,
    /// and `-`, starting and ending with a letter or digit.
    NodeId, kind = "node ID", validate = |s| validate_label(NodeId::KIND, s, NodeId::MAX_LEN)
}

impl NodeId {
    /// The maximum length in bytes.
    pub const MAX_LEN: usize = 63;
}

string_id! {
    /// A placement label or local name: a node's `zone` and `rack` (§6.7)
    /// or a disk ID.
    ///
    /// 1 to [`Label::MAX_LEN`] bytes of lowercase ASCII letters, digits, and
    /// `-`, starting and ending with a letter or digit.
    Label, kind = "label", validate = |s| validate_label(Label::KIND, s, Label::MAX_LEN)
}

impl Label {
    /// The maximum length in bytes.
    pub const MAX_LEN: usize = 63;
}

string_id! {
    /// The S3 name of a bucket, as clients address it.
    ///
    /// Enforces the structural S3 naming rules: 3 to 63 bytes of lowercase
    /// ASCII letters, digits, `.`, and `-`; starting and ending with a
    /// letter or digit; no two adjacent periods; and not formatted as an
    /// IPv4 address. Policy on names reserved by AWS, such as the `xn--`
    /// prefix, belongs to bucket creation, not to this type.
    BucketName, kind = "bucket name", validate = validate_bucket_name
}

impl BucketName {
    /// The minimum length in bytes.
    pub const MIN_LEN: usize = 3;
    /// The maximum length in bytes.
    pub const MAX_LEN: usize = 63;
}

fn validate_bucket_name(value: &str) -> Result<(), IdError> {
    const KIND: &str = BucketName::KIND;
    check_length(KIND, value, BucketName::MIN_LEN, BucketName::MAX_LEN)?;
    if let Some((index, ch)) = value
        .char_indices()
        .find(|&(_, c)| !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.'))
    {
        return Err(IdError::InvalidChar {
            kind: KIND,
            value: value.to_owned(),
            ch,
            index,
            allowed: "allowed are lowercase ASCII letters, digits, '.', and '-'",
        });
    }
    let edge_ok = |c: Option<char>| c.is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit());
    if !edge_ok(value.chars().next()) || !edge_ok(value.chars().next_back()) {
        return Err(IdError::InvalidEdge {
            kind: KIND,
            value: value.to_owned(),
        });
    }
    let rule = |rule| IdError::Rule {
        kind: KIND,
        value: value.to_owned(),
        rule,
    };
    if value.contains("..") {
        return Err(rule("must not contain two adjacent periods"));
    }
    if value.parse::<std::net::Ipv4Addr>().is_ok() {
        return Err(rule("must not be formatted as an IPv4 address"));
    }
    Ok(())
}

string_id! {
    /// The unique ID a proposer writes into every control-store register
    /// value (§6.1).
    ///
    /// After a lost response, the proposer re-reads the register: if it
    /// holds the proposer's own `proposal_id`, the write succeeded. Every
    /// write must therefore use a fresh ID. [`ProposalId::from_u128`] encodes
    /// 128 random or otherwise unique bits.
    ///
    /// 1 to [`ProposalId::MAX_LEN`] bytes of ASCII letters, digits, `-`, and
    /// `_`.
    ProposalId, kind = "proposal ID", validate = validate_proposal_id
}

impl ProposalId {
    /// The maximum length in bytes.
    pub const MAX_LEN: usize = 64;

    /// Encodes 128 bits as 26 characters of Crockford base32, most
    /// significant bits first, the text form ULIDs use.
    ///
    /// The caller supplies the uniqueness, for example 128 random bits, or a
    /// timestamp and random bits laid out as a ULID.
    ///
    /// ```
    /// use skys3_types::ProposalId;
    ///
    /// assert_eq!(ProposalId::from_u128(0).as_str(), "00000000000000000000000000");
    /// assert_eq!(ProposalId::from_u128(u128::MAX).as_str(), "7ZZZZZZZZZZZZZZZZZZZZZZZZZ");
    /// ```
    #[must_use]
    pub fn from_u128(bits: u128) -> Self {
        const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
        let encoded = (0..26)
            .rev()
            .map(|digit| {
                // 26 digits of 5 bits cover 130 bits; the top digit holds 3.
                let index = ((bits >> (digit * 5)) & 0x1f) as usize;
                char::from(ALPHABET[index])
            })
            .collect();
        Self(encoded)
    }
}

fn validate_proposal_id(value: &str) -> Result<(), IdError> {
    const KIND: &str = ProposalId::KIND;
    check_length(KIND, value, 1, ProposalId::MAX_LEN)?;
    if let Some((index, ch)) = value
        .char_indices()
        .find(|&(_, c)| !(c.is_ascii_alphanumeric() || c == '-' || c == '_'))
    {
        return Err(IdError::InvalidChar {
            kind: KIND,
            value: value.to_owned(),
            ch,
            index,
            allowed: "allowed are ASCII letters, digits, '-', and '_'",
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_ids_accept_the_design_examples() {
        assert!(ClusterId::new("skys3-prod-a").is_ok());
        assert!(BucketId::new("b-7f3a").is_ok());
        assert!(NodeId::new("node-3").is_ok());
        assert!(Label::new("us-east-1a").is_ok());
        assert!(ClusterId::new("a").is_ok());
        assert!(ClusterId::new("0").is_ok());
    }

    #[test]
    fn label_ids_enforce_their_length_limits() {
        assert!(ClusterId::new("a".repeat(ClusterId::MAX_LEN)).is_ok());
        assert_eq!(
            ClusterId::new("a".repeat(ClusterId::MAX_LEN + 1)),
            Err(IdError::TooLong {
                kind: "cluster ID",
                len: 25,
                max: 24
            })
        );
        assert!(BucketId::new("b".repeat(BucketId::MAX_LEN)).is_ok());
        assert!(BucketId::new("b".repeat(BucketId::MAX_LEN + 1)).is_err());
        assert!(NodeId::new("n".repeat(NodeId::MAX_LEN)).is_ok());
        assert!(NodeId::new("n".repeat(NodeId::MAX_LEN + 1)).is_err());
        assert!(matches!(
            NodeId::new(""),
            Err(IdError::TooShort { min: 1, .. })
        ));
    }

    #[test]
    fn label_ids_reject_characters_outside_the_set() {
        for bad in ["Prod", "a/b", "a.b", "a_b", "a b", "a\0b", "é"] {
            let err = BucketId::new(bad).unwrap_err();
            assert!(matches!(err, IdError::InvalidChar { .. }), "{bad:?}: {err}");
        }
        let err = ClusterId::new("ab/c").unwrap_err();
        assert_eq!(
            err.to_string(),
            "cluster ID \"ab/c\" contains '/' at byte 2; \
             allowed are lowercase ASCII letters, digits, and '-'"
        );
    }

    #[test]
    fn label_ids_reject_hyphens_at_the_edges() {
        for bad in ["-a", "a-", "-"] {
            assert!(matches!(NodeId::new(bad), Err(IdError::InvalidEdge { .. })));
        }
        assert!(NodeId::new("a--b").is_ok());
    }

    #[test]
    fn bucket_names_follow_the_s3_structural_rules() {
        for good in ["abc", "my.bucket-1", "0ab", &"a".repeat(63)] {
            assert!(BucketName::new(good).is_ok(), "{good:?}");
        }
        let variant = |e: IdError| match e {
            IdError::TooShort { .. } => "short",
            IdError::TooLong { .. } => "long",
            IdError::InvalidChar { .. } => "char",
            IdError::InvalidEdge { .. } => "edge",
            IdError::Rule { .. } => "rule",
        };
        let long = "a".repeat(64);
        for (bad, expected) in [
            ("ab", "short"),
            (long.as_str(), "long"),
            ("My-bucket", "char"),
            ("my_bucket", "char"),
            (".abc", "edge"),
            ("abc-", "edge"),
            ("a..b", "rule"),
            ("192.168.5.4", "rule"),
        ] {
            assert_eq!(
                variant(BucketName::new(bad).unwrap_err()),
                expected,
                "{bad:?}"
            );
        }
        assert_eq!(
            BucketName::new("a..b").unwrap_err().to_string(),
            "bucket name \"a..b\" must not contain two adjacent periods"
        );
    }

    #[test]
    fn proposal_ids_accept_tokens_and_reject_others() {
        assert!(ProposalId::new("01J8Z6K3V2Q4").is_ok());
        assert!(ProposalId::new("a_b-C").is_ok());
        assert!(ProposalId::new("x".repeat(ProposalId::MAX_LEN)).is_ok());
        assert!(ProposalId::new("x".repeat(ProposalId::MAX_LEN + 1)).is_err());
        assert!(ProposalId::new("").is_err());
        assert!(matches!(
            ProposalId::new("a.b"),
            Err(IdError::InvalidChar {
                ch: '.',
                index: 1,
                ..
            })
        ));
    }

    #[test]
    fn proposal_ids_from_bits_use_crockford_base32() {
        let id = ProposalId::from_u128(0x0123_4567_89ab_cdef_fedc_ba98_7654_3210);
        assert_eq!(id.as_str().len(), 26);
        assert!(ProposalId::new(id.as_str()).is_ok());
        assert_eq!(
            ProposalId::from_u128(31).as_str(),
            "0000000000000000000000000Z"
        );
        assert_eq!(
            ProposalId::from_u128(32).as_str(),
            "00000000000000000000000010"
        );
        assert_ne!(ProposalId::from_u128(1), ProposalId::from_u128(2));
    }

    #[test]
    fn ids_convert_and_serialize_as_plain_strings() {
        let id = NodeId::try_from("node-7").unwrap();
        assert_eq!(id.as_ref(), "node-7");
        assert_eq!(String::from(id.clone()), "node-7");
        assert_eq!(id.clone().into_string(), "node-7");
        assert_eq!(NodeId::try_from(String::from("node-7")).unwrap(), id);
        assert_eq!(serde_json::to_string(&id).unwrap(), "\"node-7\"");
        assert_eq!(serde_json::from_str::<NodeId>("\"node-7\"").unwrap(), id);
        let err = serde_json::from_str::<NodeId>("\"Node-7\"").unwrap_err();
        assert!(err.to_string().contains("node ID \"Node-7\""), "{err}");
        assert!(serde_json::from_str::<NodeId>("7").is_err());
    }
}
