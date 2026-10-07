//! The values the index stores: namespace entries, multipart uploads and
//! their parts, and control-state copies.

use skys3_log::record::{Checksums, CopySource, ExtentRef, Metadata, TagSet, UploadChecksum};
use skys3_types::{AttemptId, CodedStripe, ETag, EpochSeq, Generation};

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
    /// Where the object's bytes are on this replica, as the write that
    /// stored them named them. Once the version is [`coded`](Self::coded),
    /// the replicas drop them (§8.4) and the positions may no longer be
    /// located.
    pub payload: Payload,
    /// The version's erasure-coded layout, once an `EC_PUBLISH` of it was
    /// applied (§8.4); `None` while it is only replicated. A `TAGS` record
    /// keeps it, as it keeps the bytes, though it moves [`Entry::version`]
    /// past the version the fragments name ([`Coded::version`]); any other
    /// write replaces it.
    pub coded: Option<Coded>,
}

/// An object version's erasure-coded layout, as its `EC_PUBLISH` record
/// gave it (§8.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Coded {
    /// The position of the `EC_PUBLISH` record. Replicas drop the
    /// version's replicated bytes once they know it committed.
    pub publish: EpochSeq,
    /// The position of the record that committed the version the
    /// fragments were written for, as the `EC_PUBLISH` named it: with the
    /// object's ETag, the version identity the fragment headers carry. It
    /// precedes [`publish`](Self::publish), and stays when a `TAGS` record
    /// moves [`Entry::version`] past it.
    pub version: EpochSeq,
    /// The attempt that wrote the fragments.
    pub attempt: AttemptId,
    /// The stripes, in order, covering the object exactly once.
    pub stripes: Vec<CodedStripe>,
}

/// Where an object version's bytes are, by log position.
///
/// The node-local location map (§10.2) resolves each position to a
/// segment, offset, and length on this node's disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Payload {
    /// No local bytes: an evicted entry that is not a multipart object, or
    /// an imported stub.
    None,
    /// Inline in the record at this position, usually the version's own
    /// `PUT`.
    Inline(EpochSeq),
    /// In these `EXTENT` records, in object order.
    Extents(Vec<ExtentRef>),
    /// In the parts of the completed multipart upload opened at `upload`,
    /// in object order. Each part's [`Part`] row says where its bytes are.
    /// An evicted multipart object keeps this payload, for its part
    /// boundaries, and its parts hold no bytes.
    Parts {
        /// The position of the upload's `MPU_CREATE`.
        upload: EpochSeq,
        /// The object's parts, in increasing part number.
        parts: Vec<ObjectPart>,
    },
}

/// One part of a multipart object, as its entry lists it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ObjectPart {
    /// The part number the client uploaded it as.
    pub number: u16,
    /// The part's size in bytes.
    pub size: u64,
}

/// An open multipart upload: what its `MPU_CREATE` fixed (§7.4). The
/// upload is named by that record's position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Upload {
    /// When the upload was opened, in milliseconds since the Unix epoch.
    pub initiated_ms: u64,
    /// The completed object's metadata.
    pub metadata: Metadata,
    /// The completed object's tags.
    pub tags: TagSet,
    /// The checksum the client chose, if any.
    pub checksum: Option<UploadChecksum>,
}

/// A part of a multipart upload: of an open upload, or of the object a
/// completed upload made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Part {
    /// The position of the `MPU_PART` record that stored the part.
    pub position: EpochSeq,
    /// The part's size in bytes.
    pub size: u64,
    /// When the part was stored, in milliseconds since the Unix epoch.
    pub last_modified_ms: u64,
    /// The part's ETag: the MD5 of its bytes.
    pub etag: ETag,
    /// The part's checksums.
    pub checksums: Checksums,
    /// Where the part's bytes are: [`Payload::Inline`] in its `MPU_PART`
    /// record, or [`Payload::Extents`], those a stored part or a fill
    /// committed. A part of an evicted multipart object has none
    /// ([`Payload::None`]): the object's entry keeps its parts, and the
    /// parts keep their boundaries, ETags, and checksums (§9.3).
    pub payload: Payload,
}

/// The remote multipart upload that the primary's flusher opened for a
/// local upload, to stream its parts while the client uploads (§7.3). The
/// local upload is named by the position of its `MPU_CREATE`; the remote
/// one stays recorded until a `PART_FLUSHED` ends it, also after the local
/// upload completed or was aborted, so the flusher can still complete or
/// abort it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteUpload {
    /// The object key.
    pub key: String,
    /// The remote upload's ID.
    pub id: String,
}

/// A part a [`RemoteUpload`] holds: which local part was sent, and the
/// remote's ETag for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemotePart {
    /// The position of the `MPU_PART` record whose bytes were sent.
    pub position: EpochSeq,
    /// The ETag the remote returned for the part.
    pub etag: ETag,
}

/// The parts a [`RemoteUpload`] holds, in part order.
pub type RemoteParts = Vec<(u16, RemotePart)>;

/// How far a bucket's namespace import has got (§9.1): what the import
/// resumes from after a restart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportCheckpoint {
    /// The import is running. Every remote object whose key is at most
    /// `after` has its `IMPORT` record committed; `None` before the first.
    Running {
        /// The last key imported, without the target's prefix.
        after: Option<String>,
    },
    /// Every object the remote prefix held when it was listed is imported.
    Done,
}

impl ImportCheckpoint {
    /// Whether the import has passed `key`: the remote object at `key`,
    /// if the listing found one, has its `IMPORT` record committed.
    #[must_use]
    pub fn passed(&self, key: &str) -> bool {
        match self {
            Self::Running { after } => after.as_deref().is_some_and(|after| key <= after),
            Self::Done => true,
        }
    }
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
