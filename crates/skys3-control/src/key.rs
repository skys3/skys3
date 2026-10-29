//! Register keys and the register layout (design §6.1).
//!
//! Every backend stores the same registers under its cluster prefix:
//!
//! | Key | Document |
//! |---|---|
//! | `cluster.json` | [`ClusterDocument`] |
//! | `coordinator.lease` | [`CoordinatorLease`] |
//! | `nodes/<node-id>.json` | [`NodeRegistration`] |
//! | `buckets/<bucket-name>.json` | [`BucketDocument`] |
//! | `shards/<bucket-id>/<n>.json` | [`ShardConfig`] |
//! | `identity/...` | defined with the authorization model |
//!
//! A bucket register is keyed by the bucket's S3 name, which is how
//! requests find it; its shards are keyed by the bucket ID, which is never
//! reused, so a bucket recreated under the same name never inherits an old
//! shard register.
//!
//! [`RegisterKey`] is a key relative to the cluster prefix. Backends map it
//! to an object key, an etcd key, or a file path, so its grammar is the
//! intersection of what those accept: segments of ASCII letters, digits,
//! `-`, `_`, and `.`, separated by `/`, where no segment is empty or starts
//! with `.`. [`TypedKey`] pairs a layout key with the document type stored
//! there.

use std::fmt;
use std::marker::PhantomData;

use skys3_types::{
    BucketDocument, BucketId, BucketName, ClusterDocument, CoordinatorLease, NodeId,
    NodeRegistration, RegisterDocument, ShardConfig, ShardId,
};

/// A key that is not a valid register key or prefix.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid register key {key:?}: {reason}")]
pub struct KeyError {
    pub(crate) key: String,
    pub(crate) reason: &'static str,
}

impl KeyError {
    /// The rejected key.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }
}

/// Checks the register-key grammar, for keys and (without the trailing
/// `/`) prefixes.
fn check_path(path: &str) -> Result<(), &'static str> {
    if path.is_empty() {
        return Err("it is empty");
    }
    if path.len() > RegisterKey::MAX_LEN {
        return Err("it is longer than 512 bytes");
    }
    for segment in path.split('/') {
        if segment.is_empty() {
            return Err("it has an empty segment");
        }
        if segment.starts_with('.') {
            return Err("a segment starts with '.'");
        }
        if !segment
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        {
            return Err("allowed are ASCII letters, digits, '-', '_', '.', and '/'");
        }
    }
    Ok(())
}

/// A register's key, relative to the cluster prefix.
///
/// ```
/// use skys3_control::RegisterKey;
///
/// let key = RegisterKey::new("shards/b-7f3a/5.json")?;
/// assert_eq!(key.as_str(), "shards/b-7f3a/5.json");
/// assert!(RegisterKey::new("../etc/passwd").is_err());
/// # Ok::<(), skys3_control::KeyError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RegisterKey(String);

impl RegisterKey {
    /// The longest key, in bytes.
    pub const MAX_LEN: usize = 512;

    /// Validates `key` against the register-key grammar.
    ///
    /// # Errors
    ///
    /// Returns a [`KeyError`] if `key` breaks the grammar.
    pub fn new(key: impl Into<String>) -> Result<Self, KeyError> {
        let key = key.into();
        match check_path(&key) {
            Ok(()) => Ok(Self(key)),
            Err(reason) => Err(KeyError { key, reason }),
        }
    }

    /// `cluster.json`.
    #[must_use]
    pub fn cluster() -> Self {
        Self("cluster.json".to_owned())
    }

    /// `coordinator.lease`.
    #[must_use]
    pub fn coordinator_lease() -> Self {
        Self("coordinator.lease".to_owned())
    }

    /// `nodes/<node-id>.json`.
    #[must_use]
    pub fn node(node: &NodeId) -> Self {
        Self(format!("nodes/{node}.json"))
    }

    /// `buckets/<bucket-name>.json`.
    #[must_use]
    pub fn bucket(name: &BucketName) -> Self {
        Self(format!("buckets/{name}.json"))
    }

    /// `shards/<bucket-id>/<n>.json`.
    #[must_use]
    pub fn shard(bucket: &BucketId, shard: ShardId) -> Self {
        Self(format!("shards/{bucket}/{shard}.json"))
    }

    /// `identity/<name>`, for the identity registers.
    ///
    /// # Errors
    ///
    /// Returns a [`KeyError`] if the resulting key breaks the grammar.
    pub fn identity(name: &str) -> Result<Self, KeyError> {
        Self::new(format!("identity/{name}"))
    }

    /// The key as a string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether the key is under `prefix`.
    #[must_use]
    pub fn starts_with(&self, prefix: &KeyPrefix) -> bool {
        self.0.starts_with(prefix.as_str())
    }

    /// Classifies the key by the register layout.
    #[must_use]
    pub fn kind(&self) -> RegisterKind {
        let key = self.as_str();
        if key == "cluster.json" {
            return RegisterKind::Cluster;
        }
        if key == "coordinator.lease" {
            return RegisterKind::CoordinatorLease;
        }
        let json = |rest: &str| rest.strip_suffix(".json").map(str::to_owned);
        if let Some(node) = key.strip_prefix("nodes/").and_then(json)
            && let Ok(node) = NodeId::new(node)
        {
            return RegisterKind::Node(node);
        }
        if let Some(name) = key.strip_prefix("buckets/").and_then(json)
            && let Ok(name) = BucketName::new(name)
        {
            return RegisterKind::Bucket(name);
        }
        if let Some((bucket, shard)) = key
            .strip_prefix("shards/")
            .and_then(|rest| rest.split_once('/'))
            && let Ok(bucket) = BucketId::new(bucket)
            && let Some(shard) = json(shard).and_then(|n| parse_shard(&n))
        {
            return RegisterKind::Shard(bucket, shard);
        }
        if key.starts_with("identity/") {
            return RegisterKind::Identity;
        }
        RegisterKind::Other
    }
}

/// Parses the canonical decimal form of a shard number.
fn parse_shard(text: &str) -> Option<ShardId> {
    let canonical = !text.is_empty()
        && text.bytes().all(|b| b.is_ascii_digit())
        && (text == "0" || !text.starts_with('0'));
    canonical
        .then(|| text.parse::<u8>().ok())
        .flatten()
        .map(ShardId::new)
}

impl fmt::Display for RegisterKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for RegisterKey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// What a [`RegisterKey`] names in the register layout.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RegisterKind {
    /// `cluster.json`.
    Cluster,
    /// `coordinator.lease`.
    CoordinatorLease,
    /// A node registration.
    Node(NodeId),
    /// A bucket register.
    Bucket(BucketName),
    /// A shard configuration register.
    Shard(BucketId, ShardId),
    /// An identity register.
    Identity,
    /// A key outside the layout, such as a probe's scratch key.
    Other,
}

/// A key prefix to list registers under: the root, or a path ending in
/// `/`.
///
/// ```
/// use skys3_control::{KeyPrefix, RegisterKey};
///
/// let key = RegisterKey::new("nodes/node-1.json")?;
/// assert!(key.starts_with(&KeyPrefix::nodes()));
/// assert!(key.starts_with(&KeyPrefix::root()));
/// assert!(!key.starts_with(&KeyPrefix::buckets()));
/// # Ok::<(), skys3_control::KeyError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeyPrefix(String);

impl KeyPrefix {
    /// Validates `prefix`: empty, or a register-key path followed by `/`.
    ///
    /// # Errors
    ///
    /// Returns a [`KeyError`] if `prefix` breaks the grammar.
    pub fn new(prefix: impl Into<String>) -> Result<Self, KeyError> {
        let prefix = prefix.into();
        if prefix.is_empty() {
            return Ok(Self(prefix));
        }
        let Some(path) = prefix.strip_suffix('/') else {
            return Err(KeyError {
                key: prefix,
                reason: "a prefix must be empty or end with '/'",
            });
        };
        match check_path(path) {
            Ok(()) => Ok(Self(prefix)),
            Err(reason) => Err(KeyError {
                key: prefix,
                reason,
            }),
        }
    }

    /// The empty prefix: every register.
    #[must_use]
    pub fn root() -> Self {
        Self(String::new())
    }

    /// `nodes/`.
    #[must_use]
    pub fn nodes() -> Self {
        Self("nodes/".to_owned())
    }

    /// `buckets/`.
    #[must_use]
    pub fn buckets() -> Self {
        Self("buckets/".to_owned())
    }

    /// `shards/`.
    #[must_use]
    pub fn shards() -> Self {
        Self("shards/".to_owned())
    }

    /// `shards/<bucket-id>/`: one bucket's shards.
    #[must_use]
    pub fn bucket_shards(bucket: &BucketId) -> Self {
        Self(format!("shards/{bucket}/"))
    }

    /// `identity/`.
    #[must_use]
    pub fn identity() -> Self {
        Self("identity/".to_owned())
    }

    /// The prefix as a string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for KeyPrefix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A layout key together with the document type stored under it.
///
/// ```
/// use skys3_control::TypedKey;
/// use skys3_types::{BucketId, ShardId};
///
/// let key = TypedKey::shard(&BucketId::new("b-7f3a")?, ShardId::new(5));
/// assert_eq!(key.key().as_str(), "shards/b-7f3a/5.json");
/// # Ok::<(), skys3_types::IdError>(())
/// ```
pub struct TypedKey<D> {
    key: RegisterKey,
    document: PhantomData<fn() -> D>,
}

impl<D: RegisterDocument> TypedKey<D> {
    /// Pairs `key` with the document type `D`, for registers the layout
    /// constructors do not cover, such as identity registers.
    #[must_use]
    pub fn new(key: RegisterKey) -> Self {
        Self {
            key,
            document: PhantomData,
        }
    }

    /// The register key.
    #[must_use]
    pub fn key(&self) -> &RegisterKey {
        &self.key
    }
}

impl TypedKey<ClusterDocument> {
    /// `cluster.json`.
    #[must_use]
    pub fn cluster() -> Self {
        Self::new(RegisterKey::cluster())
    }
}

impl TypedKey<CoordinatorLease> {
    /// `coordinator.lease`.
    #[must_use]
    pub fn coordinator_lease() -> Self {
        Self::new(RegisterKey::coordinator_lease())
    }
}

impl TypedKey<NodeRegistration> {
    /// `nodes/<node-id>.json`.
    #[must_use]
    pub fn node(node: &NodeId) -> Self {
        Self::new(RegisterKey::node(node))
    }
}

impl TypedKey<BucketDocument> {
    /// `buckets/<bucket-name>.json`.
    #[must_use]
    pub fn bucket(name: &BucketName) -> Self {
        Self::new(RegisterKey::bucket(name))
    }
}

impl TypedKey<ShardConfig> {
    /// `shards/<bucket-id>/<n>.json`.
    #[must_use]
    pub fn shard(bucket: &BucketId, shard: ShardId) -> Self {
        Self::new(RegisterKey::shard(bucket, shard))
    }
}

impl<D> Clone for TypedKey<D> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            document: PhantomData,
        }
    }
}

impl<D> fmt::Debug for TypedKey<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("TypedKey").field(&self.key).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(s: &str) -> RegisterKey {
        RegisterKey::new(s).unwrap()
    }

    #[test]
    fn layout_keys_follow_the_design() {
        let node = NodeId::new("node-3").unwrap();
        let bucket = BucketId::new("b-7f3a").unwrap();
        let name = BucketName::new("photos.example").unwrap();
        assert_eq!(RegisterKey::cluster().as_str(), "cluster.json");
        assert_eq!(
            RegisterKey::coordinator_lease().as_str(),
            "coordinator.lease"
        );
        assert_eq!(RegisterKey::node(&node).as_str(), "nodes/node-3.json");
        assert_eq!(
            RegisterKey::bucket(&name).as_str(),
            "buckets/photos.example.json"
        );
        assert_eq!(
            RegisterKey::shard(&bucket, ShardId::new(12)).as_str(),
            "shards/b-7f3a/12.json"
        );
        assert_eq!(
            RegisterKey::identity("roles/reader.json").unwrap().as_str(),
            "identity/roles/reader.json"
        );
        for layout in [
            RegisterKey::cluster(),
            RegisterKey::coordinator_lease(),
            RegisterKey::node(&node),
            RegisterKey::bucket(&name),
            RegisterKey::shard(&bucket, ShardId::new(255)),
        ] {
            assert_eq!(RegisterKey::new(layout.as_str()).unwrap(), layout);
        }
        assert_eq!(TypedKey::cluster().key(), &RegisterKey::cluster());
        assert_eq!(
            TypedKey::coordinator_lease().key(),
            &RegisterKey::coordinator_lease()
        );
        assert_eq!(TypedKey::node(&node).key(), &RegisterKey::node(&node));
        assert_eq!(TypedKey::bucket(&name).key(), &RegisterKey::bucket(&name));
        let typed = TypedKey::shard(&bucket, ShardId::new(1));
        assert_eq!(format!("{:?}", typed.clone()), format!("{typed:?}"));
    }

    #[test]
    fn keys_are_classified_by_the_layout() {
        let node = NodeId::new("node-3").unwrap();
        let bucket = BucketId::new("b-7f3a").unwrap();
        let name = BucketName::new("photos").unwrap();
        assert_eq!(key("cluster.json").kind(), RegisterKind::Cluster);
        assert_eq!(
            key("coordinator.lease").kind(),
            RegisterKind::CoordinatorLease
        );
        assert_eq!(key("nodes/node-3.json").kind(), RegisterKind::Node(node));
        assert_eq!(
            key("buckets/photos.json").kind(),
            RegisterKind::Bucket(name)
        );
        assert_eq!(
            key("shards/b-7f3a/7.json").kind(),
            RegisterKind::Shard(bucket, ShardId::new(7))
        );
        assert_eq!(key("identity/roles/x.json").kind(), RegisterKind::Identity);
        for other in [
            "nodes/Node-3.json",
            "nodes/node-3",
            "buckets/ab.json",
            "shards/b-7f3a/07.json",
            "shards/b-7f3a/256.json",
            "shards/b-7f3a.json",
            "shards/B/1.json",
            "probe/0",
        ] {
            assert_eq!(key(other).kind(), RegisterKind::Other, "{other}");
        }
    }

    #[test]
    fn keys_reject_paths_that_are_unsafe_somewhere() {
        for bad in [
            "",
            "/cluster.json",
            "nodes/",
            "nodes//x",
            "../x",
            "nodes/.x.tmp",
            ".lock",
            "a b",
            "a\\b",
            "caf\u{e9}",
        ] {
            let error = RegisterKey::new(bad).unwrap_err();
            assert_eq!(error.key(), bad);
        }
        assert!(RegisterKey::new("a".repeat(RegisterKey::MAX_LEN)).is_ok());
        let long = RegisterKey::new("a".repeat(RegisterKey::MAX_LEN + 1)).unwrap_err();
        assert!(long.to_string().contains("512 bytes"), "{long}");
        assert_eq!(key("A_b.c-1/x").to_string(), "A_b.c-1/x");
        assert_eq!(key("x").as_ref(), "x");
    }

    #[test]
    fn prefixes_are_empty_or_end_with_a_slash() {
        assert_eq!(KeyPrefix::new("").unwrap(), KeyPrefix::root());
        assert_eq!(KeyPrefix::new("nodes/").unwrap(), KeyPrefix::nodes());
        assert_eq!(KeyPrefix::buckets().as_str(), "buckets/");
        assert_eq!(KeyPrefix::shards().to_string(), "shards/");
        assert_eq!(KeyPrefix::identity().as_str(), "identity/");
        let bucket = BucketId::new("b-1").unwrap();
        assert_eq!(KeyPrefix::bucket_shards(&bucket).as_str(), "shards/b-1/");
        for bad in ["nodes", "/", "nodes//", "../"] {
            assert!(KeyPrefix::new(bad).is_err(), "{bad}");
        }
        assert!(key("shards/b-1/0.json").starts_with(&KeyPrefix::bucket_shards(&bucket)));
        assert!(!key("shards/b-10/0.json").starts_with(&KeyPrefix::bucket_shards(&bucket)));
    }
}
