//! The values the index stores: namespace entries and control-state
//! copies.

use skys3_log::record::{Checksums, CopySource, ExtentRef, Metadata, TagSet};
use skys3_types::{ETag, EpochSeq, Generation};

/// Where an entry is in its life cycle (§4.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum EntryState {
    /// Committed locally and not yet flushed: the payload is on every
    /// member and is never evicted.
    Dirty,
    /// The flusher has taken the latest version.
    Flushing,
    /// Matches the remote.
    Clean,
    /// A flush found the remote changed out of band (§7.2).
    Conflict,
    /// A clean entry whose payload was dropped: a stub with metadata only.
    Evicted,
}

impl EntryState {
    /// Every state, in code order.
    pub const ALL: [Self; 5] = [
        Self::Dirty,
        Self::Flushing,
        Self::Clean,
        Self::Conflict,
        Self::Evicted,
    ];

    /// The state's code in an encoded entry.
    #[must_use]
    pub const fn code(self) -> u8 {
        match self {
            Self::Dirty => 1,
            Self::Flushing => 2,
            Self::Clean => 3,
            Self::Conflict => 4,
            Self::Evicted => 5,
        }
    }

    /// The state with `code`, if any.
    #[must_use]
    pub fn from_code(code: u8) -> Option<Self> {
        Self::ALL.into_iter().find(|state| state.code() == code)
    }
}

/// A key's entry in its shard's namespace index (§4.2, §9.1).
///
/// An entry describes the key's latest committed version. Its fields are
/// the same on every replica of the shard, because every replica applies the
/// same records: payload is named by log position, never by a node-local
/// location (see [`Payload`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// The position of the record that committed this version. Its
    /// sequence number is the `seq` of the version identity (§9.2).
    pub version: EpochSeq,
    /// The entry's state.
    pub state: EntryState,
    /// The object, or `None` for a tombstone: a delete not yet flushed
    /// (§9.1). A flushed delete removes the entry.
    pub object: Option<ObjectVersion>,
    /// The ETag the remote returned for the latest flushed version
    /// (`remote_etag`, §4.2), or `None` if the remote state is unknown or
    /// the key is absent there.
    pub remote_etag: Option<ETag>,
    /// The remote version ID, when the remote is versioned.
    pub remote_version_id: Option<String>,
}

/// An object version's metadata and where its bytes are.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectVersion {
    /// The object's size in bytes.
    pub size: u64,
    /// `Last-Modified`, in milliseconds since the Unix epoch.
    pub last_modified_ms: u64,
    /// The ETag clients see (`local_etag`, §4.2).
    pub local_etag: ETag,
    /// The position of the record whose write identity the version
    /// inherits (§7.2), or `None` if the identity names
    /// [`Entry::version`].
    pub write_identity: Option<EpochSeq>,
    /// Stored HTTP metadata.
    pub metadata: Metadata,
    /// Tags.
    pub tags: TagSet,
    /// Client checksums (§7.4).
    pub checksums: Checksums,
    /// The remote storage class, if known.
    pub storage_class: Option<String>,
    /// For a copy, its source (§11).
    pub copy_source: Option<CopySource>,
    /// Where the object's bytes are on this replica.
    pub payload: Payload,
}

/// Where an object version's bytes are, by log position.
///
/// The node-local location map (§10.2) resolves each position to a
/// segment, offset, and length on this node's disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Payload {
    /// No local bytes: an evicted entry or an imported stub.
    None,
    /// Inline in the record at this position, usually the version's own
    /// `PUT`.
    Inline(EpochSeq),
    /// In these `EXTENT` records, in object order.
    Extents(Vec<ExtentRef>),
}

/// A node's local copy of one control-store register (§6.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlEntry {
    /// The configuration generation the copy was taken at.
    pub generation: Generation,
    /// The register's version in the control store.
    pub version: String,
    /// The register's value, as the control store holds it.
    pub value: Vec<u8>,
}
