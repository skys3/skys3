//! The peer protocol's messages (design §7.8) and the rules they follow.
//!
//! A source sends `BEGIN`, `DATA`, `COMMIT`, and `BATCH`; a destination
//! sends `DURABLE`, `RESUME`, and `APPLIED`; both send `HELLO` and `ABORT`.
//! [`Message::validate`] checks every limit, and both encoding and decoding
//! call it, so a message that one end can send the other accepts.

use std::collections::BTreeSet;

use bytes::Bytes;
use skys3_log::record::{
    MAX_KEY_LEN, MAX_METADATA_LEN, MAX_TAG_KEY_LEN, MAX_TAG_VALUE_LEN, MAX_TAGS, Metadata, TagSet,
};
use skys3_types::checksum::Checksums;
use skys3_types::limits::{MAX_PARTS, MAX_SINGLE_PUT_BYTES};
use skys3_types::{BucketName, ClusterId, ETag, WriteIdentity};

use crate::error::{MessageError, ensure};
use crate::frame::MAX_PAYLOAD_LEN;
use crate::negotiation::{Capabilities, VersionRange};
use crate::ranges::ByteRanges;

/// The largest piece, in bytes: a single PUT's body or one part, at most
/// 5 GiB each in S3. `DATA` offsets and durable ranges stay within it.
pub const MAX_PIECE_BYTES: u64 = MAX_SINGLE_PUT_BYTES;

/// The largest object, in bytes (5 TiB, the S3 limit).
pub const MAX_OBJECT_BYTES: u64 = 5 << 40;

/// The most objects in one `BATCH`.
pub const MAX_BATCH_ITEMS: usize = 1024;

/// The most pieces one `DURABLE` or `RESUME` reports.
pub const MAX_REPORTED_PIECES: usize = 16 * 1024;

/// The most ranges one `DURABLE` or `RESUME` reports, over all its pieces.
/// A destination holding more reports a subset, which is always safe: the
/// source resends what it was not told is durable.
pub const MAX_REPORTED_RANGES: usize = 16 * 1024;

/// The longest explanation an `APPLIED` error or an `ABORT` carries, in
/// bytes.
pub const MAX_REASON_LEN: usize = 1024;

/// One message of the peer protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// Both ends, once per connection: who they are and what they speak.
    Hello(Hello),
    /// Source: open staging for one write identity.
    Begin(Begin),
    /// Source: bytes of a staged piece.
    Data(Data),
    /// Destination: what is now durable on every member of its shard.
    Durable(StagedRanges),
    /// Destination: everything it holds durably for a write identity, the
    /// answer to every `BEGIN`.
    Resume(StagedRanges),
    /// Source: publish a staged object, or delete one, under a
    /// precondition.
    Commit(Commit),
    /// Destination: the result of a `COMMIT`, or of one `BATCH` item.
    Applied(Applied),
    /// Source: small objects with their bytes inline, applied in one group
    /// commit.
    Batch(Batch),
    /// Either end: the staging of a write identity is discarded.
    Abort(Abort),
}

/// `HELLO`: an end's cluster, protocol versions, and capabilities. See
/// [`negotiate`](crate::negotiate).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hello {
    /// The sender's cluster.
    pub cluster: ClusterId,
    /// The protocol versions the sender speaks.
    pub versions: VersionRange,
    /// The sender's capabilities.
    pub capabilities: Capabilities,
}

/// `BEGIN`: open private staging at the destination for one write
/// identity. A `BEGIN` for an identity the destination already stages, as
/// after a reconnect, keeps the staging, provided its bucket and key are
/// the same; either way the destination answers with a `RESUME`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Begin {
    /// The write identity, naming the source record that opened the upload
    /// (design §7.2).
    pub identity: WriteIdentity,
    /// The destination bucket.
    pub bucket: BucketName,
    /// The object's key in the destination bucket.
    pub key: String,
}

/// `DATA`: bytes of a staged piece at an offset. It belongs to the write
/// identity of the `BEGIN` on its stream. Its CRC32C travels with it and is
/// checked when it is decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Data {
    /// The piece: a single PUT's body or one part, by the ID the source
    /// gives it.
    pub piece: u64,
    /// The offset of the first byte in the piece.
    pub offset: u64,
    /// The bytes, at most `peer_frame_bytes` of them.
    pub bytes: Bytes,
}

/// `DURABLE` and `RESUME`: the durable byte ranges of a write identity's
/// pieces. A `DURABLE` lists the pieces whose ranges grew, each with all of
/// its durable ranges, so a lost `DURABLE` costs nothing once the next one
/// arrives. A `RESUME` lists every piece.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedRanges {
    /// The write identity.
    pub identity: WriteIdentity,
    /// The durable ranges of each piece, by piece ID.
    pub pieces: std::collections::BTreeMap<u64, ByteRanges>,
}

/// `COMMIT`: publish an object or a delete at the destination under a
/// precondition. Keyed by write identity: a replay returns the result
/// stored for the first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit {
    /// The write identity the published object carries.
    pub identity: WriteIdentity,
    /// The destination bucket.
    pub bucket: BucketName,
    /// The object's key in the destination bucket.
    pub key: String,
    /// What the destination's current version of the key must be.
    pub precondition: Precondition,
    /// The write.
    pub write: Write,
}

/// A `COMMIT`'s precondition, which the destination evaluates in its own
/// shard log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Precondition {
    /// The key has no current version.
    Absent,
    /// The current version carries this write identity.
    Matches(WriteIdentity),
    /// None: the write applies whatever the current version is, as
    /// `flush_conflict_policy = "overwrite"` asks.
    Unconditional,
}

/// What a `COMMIT` writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Write {
    /// A new version of the key.
    Put(Put),
    /// A delete of the key.
    Delete,
}

/// A new version: its attributes and where its bytes are.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Put {
    /// The size in bytes.
    pub size: u64,
    /// The ETag, which the destination keeps (design §7.4).
    pub etag: ETag,
    /// When the source committed the version, in milliseconds since the
    /// Unix epoch.
    pub last_modified_ms: u64,
    /// The stored HTTP metadata, in the form of the log's `PUT` records.
    pub metadata: Metadata,
    /// The tags.
    pub tags: TagSet,
    /// The final checksums.
    pub checksums: Checksums,
    /// Where the bytes are.
    pub data: PutData,
}

/// Where a version's bytes are.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PutData {
    /// One staged piece holds the whole body.
    Staged {
        /// The piece.
        piece: u64,
    },
    /// A multipart object: one staged piece per part, in part order, so
    /// the destination keeps the part boundaries and the multipart ETag.
    Multipart(Vec<StagedPart>),
    /// The bytes themselves. Only `BATCH` items carry them.
    Inline(Bytes),
}

/// One part of a multipart object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StagedPart {
    /// The part number, from 1 to 10,000.
    pub number: u16,
    /// The staged piece that holds the part.
    pub piece: u64,
    /// The part's size in bytes.
    pub size: u64,
    /// The MD5 of the part's bytes, whose hex form is its ETag.
    pub md5: [u8; 16],
}

/// `APPLIED`: what the destination did with a write identity's `COMMIT`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Applied {
    /// The write identity.
    pub identity: WriteIdentity,
    /// The result.
    pub outcome: Outcome,
}

/// The result of a `COMMIT`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The write is committed at the destination.
    Committed {
        /// The ETag of the version published, `None` for a delete.
        etag: Option<ETag>,
    },
    /// The precondition does not hold.
    PreconditionFailed {
        /// The write identity of the current version, `None` if the key
        /// has none.
        current: Option<WriteIdentity>,
    },
    /// The destination did not apply the write.
    Failed {
        /// Why.
        error: ApplyError,
        /// A human-readable explanation.
        reason: String,
    },
}

/// Why a destination did not apply a `COMMIT`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ApplyError {
    /// The staged bytes do not cover the object, or the staging is gone.
    /// The source sends `BEGIN` again and resends what the `RESUME` lacks.
    Incomplete,
    /// The staged bytes do not match the final checksums. The source
    /// discards the staging and sends the object again.
    ChecksumMismatch,
    /// The destination refuses the write, for example because the bucket
    /// does not receive from this source. Retrying does not help.
    Refused,
    /// The destination could not apply the write now; the source retries.
    Unavailable,
}

/// `BATCH`: small objects with their bytes inline, and deletes. Each item
/// is a `COMMIT` with [`PutData::Inline`] or [`Write::Delete`]. Items have
/// distinct keys and distinct write identities, and the destination answers
/// each with an `APPLIED` that names it by its identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Batch {
    /// The items.
    pub items: Vec<Commit>,
}

/// `ABORT`: the staging of a write identity is discarded. From the source,
/// a request; from the destination, a notice that it discarded or refused
/// the staging.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Abort {
    /// The write identity.
    pub identity: WriteIdentity,
    /// Why.
    pub reason: AbortReason,
    /// A human-readable explanation.
    pub detail: String,
}

/// Why staging was discarded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AbortReason {
    /// The source no longer needs it: the upload was aborted or replaced.
    Cancelled,
    /// It was not committed within `peer_staging_ttl_seconds`.
    Expired,
    /// The source's staged bytes would exceed `peer_staging_quota_bytes`.
    QuotaExceeded,
    /// The destination refuses it, for example a `BEGIN` whose bucket or
    /// key differs from the staging it already holds.
    Refused,
}

impl Message {
    /// The message's name in the design, such as `"COMMIT"`.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Hello(_) => "HELLO",
            Self::Begin(_) => "BEGIN",
            Self::Data(_) => "DATA",
            Self::Durable(_) => "DURABLE",
            Self::Resume(_) => "RESUME",
            Self::Commit(_) => "COMMIT",
            Self::Applied(_) => "APPLIED",
            Self::Batch(_) => "BATCH",
            Self::Abort(_) => "ABORT",
        }
    }

    /// Checks the protocol's rules and limits.
    ///
    /// # Errors
    ///
    /// [`MessageError::Invalid`] naming the first field that breaks one, or
    /// [`MessageError::PayloadTooLong`] for `DATA` or `BATCH` bytes past
    /// [`MAX_PAYLOAD_LEN`].
    pub fn validate(&self) -> Result<(), MessageError> {
        match self {
            Self::Hello(_) => Ok(()),
            Self::Begin(begin) => check_key("begin.key", &begin.key),
            Self::Data(data) => data.validate(),
            Self::Durable(ranges) | Self::Resume(ranges) => ranges.validate(),
            Self::Commit(commit) => commit.validate(false),
            Self::Applied(applied) => match &applied.outcome {
                Outcome::Failed { reason, .. } => check_reason("applied.reason", reason),
                Outcome::Committed { .. } | Outcome::PreconditionFailed { .. } => Ok(()),
            },
            Self::Batch(batch) => batch.validate(),
            Self::Abort(abort) => check_reason("abort.detail", &abort.detail),
        }
    }
}

impl Data {
    fn validate(&self) -> Result<(), MessageError> {
        let len = self.bytes.len() as u64;
        if len > u64::from(MAX_PAYLOAD_LEN) {
            return Err(MessageError::PayloadTooLong(len));
        }
        ensure(len > 0, "data.bytes", || "is empty".to_owned())?;
        ensure(
            self.offset
                .checked_add(len)
                .is_some_and(|end| end <= MAX_PIECE_BYTES),
            "data.offset",
            || format!("{} + {len} bytes ends past {MAX_PIECE_BYTES}", self.offset),
        )
    }
}

impl StagedRanges {
    fn validate(&self) -> Result<(), MessageError> {
        let pieces = self.pieces.len();
        ensure(pieces <= MAX_REPORTED_PIECES, "ranges.pieces", || {
            format!("{pieces} pieces exceed the limit of {MAX_REPORTED_PIECES}")
        })?;
        let ranges: usize = self.pieces.values().map(ByteRanges::len).sum();
        ensure(ranges <= MAX_REPORTED_RANGES, "ranges.ranges", || {
            format!("{ranges} ranges exceed the limit of {MAX_REPORTED_RANGES}")
        })?;
        for (piece, ranges) in &self.pieces {
            ensure(ranges.end() <= MAX_PIECE_BYTES, "ranges.ranges", || {
                format!("piece {piece} has a range past {MAX_PIECE_BYTES}")
            })?;
        }
        Ok(())
    }
}

impl Commit {
    /// Checks a `COMMIT`, or a `BATCH` item when `in_batch`.
    fn validate(&self, in_batch: bool) -> Result<(), MessageError> {
        check_key("commit.key", &self.key)?;
        let Write::Put(put) = &self.write else {
            return Ok(());
        };
        put.validate()?;
        let inline = matches!(put.data, PutData::Inline(_));
        ensure(inline == in_batch, "commit.put.data", || {
            if in_batch {
                "a BATCH item carries its bytes inline".to_owned()
            } else {
                "only BATCH items carry their bytes inline".to_owned()
            }
        })
    }
}

impl Put {
    fn validate(&self) -> Result<(), MessageError> {
        let size = self.size;
        ensure(size <= MAX_OBJECT_BYTES, "put.size", || {
            format!("{size} exceeds the limit of {MAX_OBJECT_BYTES}")
        })?;
        check_metadata(&self.metadata)?;
        check_tags(&self.tags)?;
        for (&algorithm, checksum) in &self.checksums {
            checksum
                .check(algorithm)
                .map_err(|error| MessageError::invalid("put.checksums", error))?;
        }
        match &self.data {
            PutData::Staged { .. } => ensure(size <= MAX_PIECE_BYTES, "put.size", || {
                format!("{size} exceeds the single piece limit of {MAX_PIECE_BYTES}")
            }),
            PutData::Multipart(parts) => check_parts(parts, size),
            PutData::Inline(bytes) => ensure(bytes.len() as u64 == size, "put.size", || {
                format!("is {size}, but {} bytes are inline", bytes.len())
            }),
        }
    }
}

impl Batch {
    fn validate(&self) -> Result<(), MessageError> {
        let count = self.items.len();
        ensure(
            (1..=MAX_BATCH_ITEMS).contains(&count),
            "batch.items",
            || format!("holds {count} items; it must hold 1 to {MAX_BATCH_ITEMS}"),
        )?;
        let mut keys = BTreeSet::new();
        let mut identities = BTreeSet::new();
        let mut inline = 0u64;
        for item in &self.items {
            item.validate(true)?;
            ensure(
                keys.insert((&item.bucket, &item.key)),
                "batch.items",
                || format!("holds key {:?} twice", item.key),
            )?;
            // Each item's APPLIED names it by its identity alone, and a
            // destination treats a known identity as a replay.
            ensure(identities.insert(&item.identity), "batch.items", || {
                format!("holds write identity {} twice", item.identity)
            })?;
            if let Write::Put(Put {
                data: PutData::Inline(bytes),
                ..
            }) = &item.write
            {
                inline += bytes.len() as u64;
            }
        }
        if inline > u64::from(MAX_PAYLOAD_LEN) {
            return Err(MessageError::PayloadTooLong(inline));
        }
        Ok(())
    }
}

fn check_key(field: &'static str, key: &str) -> Result<(), MessageError> {
    ensure(!key.is_empty() && key.len() <= MAX_KEY_LEN, field, || {
        format!("is {} bytes long; it must be 1 to {MAX_KEY_LEN}", key.len())
    })
}

fn check_reason(field: &'static str, reason: &str) -> Result<(), MessageError> {
    ensure(reason.len() <= MAX_REASON_LEN, field, || {
        format!(
            "is {} bytes long; the limit is {MAX_REASON_LEN}",
            reason.len()
        )
    })
}

/// The rules of the log's `PUT` records, so that the destination can
/// store whatever it accepts: lowercase header names, and at most
/// [`MAX_METADATA_LEN`] bytes of names and values.
fn check_metadata(metadata: &Metadata) -> Result<(), MessageError> {
    let mut total = 0usize;
    for (name, value) in metadata {
        let token = !name.is_empty()
            && name.bytes().all(|b| {
                b.is_ascii_lowercase() || b.is_ascii_digit() || b"!#$%&'*+-.^_`|~".contains(&b)
            });
        ensure(token, "put.metadata", || {
            format!("{name:?} is not a lowercase header name")
        })?;
        total = total.saturating_add(name.len()).saturating_add(value.len());
    }
    ensure(total <= MAX_METADATA_LEN, "put.metadata", || {
        format!("{total} bytes exceed the limit of {MAX_METADATA_LEN}")
    })
}

fn check_tags(tags: &TagSet) -> Result<(), MessageError> {
    ensure(tags.len() <= MAX_TAGS, "put.tags", || {
        format!("{} tags exceed the limit of {MAX_TAGS}", tags.len())
    })?;
    for (key, value) in tags {
        ensure(
            !key.is_empty() && key.len() <= MAX_TAG_KEY_LEN && value.len() <= MAX_TAG_VALUE_LEN,
            "put.tags",
            || format!("tag {key:?} is empty or too long"),
        )?;
    }
    Ok(())
}

/// Parts are numbered in increasing order from 1 to [`MAX_PARTS`], each in
/// its own piece of at most [`MAX_PIECE_BYTES`], and their sizes add up to
/// the object's.
fn check_parts(parts: &[StagedPart], size: u64) -> Result<(), MessageError> {
    const FIELD: &str = "put.parts";
    ensure(
        !parts.is_empty() && parts.len() <= MAX_PARTS as usize,
        FIELD,
        || format!("holds {} parts; it must hold 1 to {MAX_PARTS}", parts.len()),
    )?;
    let mut pieces = BTreeSet::new();
    let mut previous = 0;
    let mut total = 0u64;
    for part in parts {
        let number = part.number;
        ensure(
            number > previous && u32::from(number) <= MAX_PARTS,
            FIELD,
            || format!("part {number} is out of order or past {MAX_PARTS}"),
        )?;
        ensure(pieces.insert(part.piece), FIELD, || {
            format!("piece {} holds two parts", part.piece)
        })?;
        ensure(part.size <= MAX_PIECE_BYTES, FIELD, || {
            format!("part {number} exceeds {MAX_PIECE_BYTES} bytes")
        })?;
        previous = number;
        total += part.size;
    }
    ensure(total == size, FIELD, || {
        format!("the parts hold {total} bytes, but the object is {size}")
    })
}
