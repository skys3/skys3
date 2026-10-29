//! User metadata: the `x-amz-meta-*` headers of an object.

use std::collections::BTreeMap;

use skys3_types::WriteIdentity;

/// Why a user-metadata entry was rejected.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum MetadataError {
    /// The key is empty, or has a character that is not allowed in an HTTP
    /// header name.
    #[error("invalid user-metadata key {0:?}")]
    InvalidKey(String),
    /// The value has a character other than printable ASCII, or leading or
    /// trailing whitespace, which HTTP would not carry unchanged.
    #[error("invalid value for user-metadata key {0:?}")]
    InvalidValue(String),
}

/// An object's user metadata: the `x-amz-meta-*` headers, keyed without the
/// prefix.
///
/// Keys are stored in lowercase, as S3 returns them, and must be HTTP token
/// characters. Values must be printable ASCII without leading or trailing
/// whitespace, so that they reach the store and come back byte for byte;
/// the write identity (design §7.2) relies on that.
///
/// The size limit is the store's to enforce: S3 answers `400
/// MetadataTooLarge` when [`UserMetadata::size`] exceeds
/// [`UserMetadata::S3_LIMIT`].
///
/// ```
/// use skys3_remote::UserMetadata;
///
/// let mut metadata = UserMetadata::new();
/// metadata.insert("Owner", "team-a")?;
/// assert_eq!(metadata.get("owner"), Some("team-a"));
/// assert_eq!(metadata.size(), "owner".len() + "team-a".len());
/// # Ok::<(), skys3_remote::MetadataError>(())
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct UserMetadata(BTreeMap<String, String>);

impl UserMetadata {
    /// S3's limit on user metadata, in bytes: the sum of the lengths of
    /// every key (without `x-amz-meta-`) and value.
    pub const S3_LIMIT: usize = 2048;

    /// Returns empty metadata.
    pub fn new() -> Self {
        UserMetadata::default()
    }

    /// Sets `key` to `value` and returns the previous value. The key is
    /// stored in lowercase.
    ///
    /// # Errors
    ///
    /// Returns a [`MetadataError`] if the key or the value is not valid.
    pub fn insert(
        &mut self,
        key: &str,
        value: impl Into<String>,
    ) -> Result<Option<String>, MetadataError> {
        let key = key.to_ascii_lowercase();
        if key.is_empty() || !key.bytes().all(is_token_byte) {
            return Err(MetadataError::InvalidKey(key));
        }
        let value = value.into();
        let printable = value.bytes().all(|b| matches!(b, b' '..=b'~'));
        if !printable || value.trim() != value {
            return Err(MetadataError::InvalidValue(key));
        }
        Ok(self.0.insert(key, value))
    }

    /// Returns the value of `key`, which must be lowercase.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    /// Removes `key`, which must be lowercase, and returns its value.
    pub fn remove(&mut self, key: &str) -> Option<String> {
        self.0.remove(key)
    }

    /// Iterates over the entries in key order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// Returns the number of entries.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether there are no entries.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The size S3 counts against [`UserMetadata::S3_LIMIT`]: the lengths
    /// of every key and value, in bytes.
    pub fn size(&self) -> usize {
        self.0.iter().map(|(k, v)| k.len() + v.len()).sum()
    }

    /// Sets the write identity, `x-amz-meta-skys3-wid` (design §7.2).
    pub fn set_write_identity(&mut self, identity: &WriteIdentity) {
        self.0
            .insert(WriteIdentity::METADATA_KEY.to_owned(), identity.to_string());
    }

    /// Returns the raw write-identity value, if any. Compare it with
    /// [`WriteIdentity::matches`] rather than parsing it: another writer may
    /// have put anything there.
    pub fn write_identity(&self) -> Option<&str> {
        self.get(WriteIdentity::METADATA_KEY)
    }
}

/// Whether `byte` is an RFC 9110 `tchar`, the characters of a header name.
fn is_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte)
}

#[cfg(test)]
mod tests {
    use skys3_types::{BucketId, ClusterId, Epoch, EpochSeq, Seq, ShardId};

    use super::*;

    #[test]
    fn keys_are_lowercased_and_validated() {
        let mut metadata = UserMetadata::new();
        assert!(metadata.is_empty());
        assert_eq!(metadata.insert("Content-Owner", "a"), Ok(None));
        assert_eq!(metadata.insert("content-owner", "b"), Ok(Some("a".into())));
        assert_eq!(metadata.get("content-owner"), Some("b"));
        assert_eq!(metadata.len(), 1);
        assert_eq!(
            metadata.insert("", "x"),
            Err(MetadataError::InvalidKey(String::new()))
        );
        assert_eq!(
            metadata.insert("a b", "x"),
            Err(MetadataError::InvalidKey("a b".into()))
        );
        assert_eq!(
            metadata.insert("k", "caf\u{e9}"),
            Err(MetadataError::InvalidValue("k".into()))
        );
        assert_eq!(
            metadata.insert("k", " padded"),
            Err(MetadataError::InvalidValue("k".into()))
        );
        assert_eq!(
            metadata.insert("k", "line\nbreak"),
            Err(MetadataError::InvalidValue("k".into()))
        );
        assert_eq!(metadata.insert("k", ""), Ok(None));
        assert_eq!(metadata.remove("k"), Some(String::new()));
        assert_eq!(
            metadata.iter().collect::<Vec<_>>(),
            [("content-owner", "b")]
        );
    }

    #[test]
    fn size_counts_keys_and_values() {
        let mut metadata = UserMetadata::new();
        metadata.insert("ab", "cde").unwrap();
        metadata.insert("f", "").unwrap();
        assert_eq!(metadata.size(), 6);
    }

    #[test]
    fn write_identity_round_trip() {
        let wid = WriteIdentity::new(
            ClusterId::new("c").unwrap(),
            BucketId::new("b").unwrap(),
            ShardId::new(1),
            EpochSeq::new(Epoch::new(2), Seq::new(3)),
        );
        let mut metadata = UserMetadata::new();
        assert_eq!(metadata.write_identity(), None);
        metadata.set_write_identity(&wid);
        assert_eq!(metadata.write_identity(), Some("c/b/1/2.3"));
        assert!(wid.matches(metadata.write_identity().unwrap()));
        assert!(metadata.size() <= WriteIdentity::METADATA_RESERVED_BYTES);
    }
}
