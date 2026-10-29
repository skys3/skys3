//! The shard state machine: what applying a record does to the index
//! (§4.2, §9.1, §9.2).
//!
//! [`StateMachine::apply`] is deterministic. It reads only the record and
//! the index, never a clock, randomness, or anything else local to the
//! node, so every replica that applies the same records in the same order
//! reaches the same entries. Live writes and replay after a crash run it
//! through [`Index::apply`](skys3_index::Index::apply), which records the
//! shard's applied position after each record.
//!
//! A record whose transition is not allowed, such as an `IMPORT` of a key
//! that already has an entry, is rejected: the namespace and the location
//! map are left as they were, the shard's applied position still advances
//! past it, and the [`Outcome`] says why.

use std::cell::RefCell;
use std::fmt;

use skys3_index::{Applier, Entry, EntryState, IndexError, IndexWriter, ObjectVersion, Payload};
use skys3_log::record::{Adopt, Delete, Flushed, Import, Put, PutData, Tags};
use skys3_log::{LogRecord, RecordBody, RecordKind, RecordLocation, ShardRef};
use skys3_types::{EpochSeq, Seq};

/// The shard state machine. It holds no state of its own: everything it
/// decides comes from the record and the index.
#[derive(Debug, Clone, Copy, Default)]
pub struct StateMachine;

/// What applying one record did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The record changed the index, or was one that never does.
    Applied(Effect),
    /// The record's transition is not allowed; the index is unchanged
    /// except for the shard's applied position.
    Rejected(Rejection),
}

impl Outcome {
    /// Whether the record was applied.
    #[must_use]
    pub const fn is_applied(&self) -> bool {
        matches!(self, Self::Applied(_))
    }
}

/// The change an applied record made.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Effect {
    /// A `PUT` or `TAGS` stored a new version of the key, in `state`:
    /// [`EntryState::Dirty`], or [`EntryState::Conflict`] if the key was in
    /// conflict.
    Stored {
        /// The entry's state now.
        state: EntryState,
    },
    /// A `DELETE` stored a tombstone, in `state` as for [`Effect::Stored`].
    Tombstoned {
        /// The entry's state now.
        state: EntryState,
    },
    /// A `FLUSHED` of the current version made the entry clean.
    Cleaned,
    /// A `FLUSHED` of the current version, a tombstone, removed the entry.
    Removed,
    /// A `FLUSHED` of an older version recorded what the remote now holds,
    /// leaving the newer version dirty.
    RemoteRecorded,
    /// An `IMPORT` created a stub.
    Imported,
    /// An `ADOPT` replaced a clean version with the remote's.
    Adopted,
    /// An `EXTENT` was entered into the location map.
    Located,
    /// A `CONFIG` or `TRUNCATE`, which change no entry.
    Unchanged,
}

/// Why a record's transition was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Rejection {
    /// The key has no entry.
    #[error("the key has no entry")]
    NoEntry,
    /// The key's entry is a tombstone.
    #[error("the key is deleted")]
    Deleted,
    /// An `IMPORT` found an entry: a local change, a tombstone, or an
    /// earlier import.
    #[error("the key already has an entry")]
    HasEntry,
    /// An `ADOPT` found the entry no longer clean: a local write committed
    /// after the read plan.
    #[error("the entry is {0:?}, not clean")]
    NotClean(EntryState),
    /// An `ADOPT` found the entry clean at another version than the read
    /// plan named.
    #[error("the entry is at seq {current}, not {expected}")]
    VersionChanged {
        /// The `seq` the record named.
        expected: Seq,
        /// The entry's `seq`.
        current: Seq,
    },
    /// A `FLUSHED` names a version newer than the entry's.
    #[error("the flushed seq {flushed} is newer than the entry's seq {current}")]
    UnknownVersion {
        /// The `seq` the record named.
        flushed: Seq,
        /// The entry's `seq`.
        current: Seq,
    },
    /// A `FLUSHED` of the current version found the entry clean already.
    #[error("the entry is clean already")]
    AlreadyClean,
    /// A `FLUSHED` of an older version found a newer version clean.
    #[error("a newer version is clean")]
    Stale,
    /// A `FLUSHED` of an object carries no remote ETag, or one of a
    /// tombstone carries one.
    #[error("the remote ETag does not match the flushed version")]
    RemoteEtagMismatch,
    /// A record kind this state machine does not apply.
    #[error("{0:?} records are not applied by this build")]
    Unsupported(RecordKind),
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Applied(effect) => write!(f, "applied: {effect:?}"),
            Self::Rejected(rejection) => write!(f, "rejected: {rejection}"),
        }
    }
}

/// The outcome of a rule that may reject.
type Rule = Result<Effect, Rejection>;

impl StateMachine {
    /// Applies `record`, stored at `location` on this node, to `index`, and
    /// returns what it did.
    ///
    /// Only records that carry payload (an `EXTENT`, or a `PUT` with
    /// inline data) use `location`, and only for the node-local location
    /// map; entries never depend on it.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if reading or writing the index fails. A
    /// rejected transition is not an error.
    pub fn apply(
        self,
        index: &mut IndexWriter<'_>,
        record: &LogRecord,
        location: RecordLocation,
    ) -> Result<Outcome, IndexError> {
        let shard = &record.shard;
        let position = record.position;
        let rule = match &record.body {
            RecordBody::Put(put) => put_object(index, shard, position, put, location)?,
            RecordBody::Delete(Delete { key }) => delete(index, shard, position, key)?,
            RecordBody::Extent(_) => {
                index.put_location(shard, position, &location)?;
                Ok(Effect::Located)
            }
            RecordBody::Tags(tags) => set_tags(index, shard, position, tags)?,
            RecordBody::Flushed(flushed) => flush(index, shard, flushed)?,
            RecordBody::Import(import) => import_stub(index, shard, position, import)?,
            RecordBody::Adopt(adopt) => adopt_remote(index, shard, position, adopt)?,
            // A replica's own bookkeeping: the configuration it adopted, or
            // where reconciliation cut its log (§6.6, M2-12).
            RecordBody::Config(_) | RecordBody::Truncate => Ok(Effect::Unchanged),
            other => Err(Rejection::Unsupported(other.kind())),
        };
        Ok(match rule {
            Ok(effect) => Outcome::Applied(effect),
            Err(rejection) => Outcome::Rejected(rejection),
        })
    }
}

impl Applier for StateMachine {
    fn apply(
        &self,
        index: &mut IndexWriter<'_>,
        record: &LogRecord,
        location: RecordLocation,
    ) -> Result<(), IndexError> {
        StateMachine::apply(*self, index, record, location).map(drop)
    }
}

/// An [`Applier`] that runs the state machine and keeps each outcome, in
/// the order the records were applied. Records the index skips, because
/// they were applied already, have no outcome.
#[derive(Debug, Default)]
pub struct Recorder {
    outcomes: RefCell<Vec<(EpochSeq, Outcome)>>,
}

impl Recorder {
    /// Returns the outcomes kept so far, by record position, and forgets
    /// them.
    pub fn take(&self) -> Vec<(EpochSeq, Outcome)> {
        self.outcomes.take()
    }
}

impl Applier for Recorder {
    fn apply(
        &self,
        index: &mut IndexWriter<'_>,
        record: &LogRecord,
        location: RecordLocation,
    ) -> Result<(), IndexError> {
        let outcome = StateMachine.apply(index, record, location)?;
        self.outcomes.borrow_mut().push((record.position, outcome));
        Ok(())
    }
}

/// The state a client write leaves the key in: dirty, unless the key is in
/// conflict, which only the conflict policy resolves (§7.2).
fn written_state(prior: Option<&Entry>) -> EntryState {
    match prior.map(|entry| entry.state) {
        Some(EntryState::Conflict) => EntryState::Conflict,
        _ => EntryState::Dirty,
    }
}

/// `PUT`: a new version of the key, whatever its entry held. The remote
/// fields still describe what the remote holds, which the next flush
/// conditions on (§7.2).
fn put_object(
    index: &mut IndexWriter<'_>,
    shard: &ShardRef,
    position: EpochSeq,
    put: &Put,
    location: RecordLocation,
) -> Result<Rule, IndexError> {
    let prior = index.entry(shard, &put.key)?;
    let payload = match &put.data {
        PutData::Inline(_) => {
            index.put_location(shard, position, &location)?;
            Payload::Inline(position)
        }
        PutData::Extents(extents) => Payload::Extents(extents.clone()),
    };
    let state = written_state(prior.as_ref());
    let object = ObjectVersion {
        size: put.size,
        last_modified_ms: put.last_modified_ms,
        local_etag: put.etag.clone(),
        write_identity: put.inherited_identity,
        metadata: put.metadata.clone(),
        tags: put.tags.clone(),
        checksums: put.checksums.clone(),
        storage_class: None,
        copy_source: put.copy_source.clone(),
        payload,
    };
    store(index, shard, &put.key, position, state, Some(object), prior)?;
    Ok(Ok(Effect::Stored { state }))
}

/// `DELETE`: a tombstone, kept until the delete is flushed (§9.1). A key
/// with no entry gets one too, so a later `IMPORT` cannot resurrect it.
fn delete(
    index: &mut IndexWriter<'_>,
    shard: &ShardRef,
    position: EpochSeq,
    key: &str,
) -> Result<Rule, IndexError> {
    let prior = index.entry(shard, key)?;
    let state = written_state(prior.as_ref());
    store(index, shard, key, position, state, None, prior)?;
    Ok(Ok(Effect::Tombstoned { state }))
}

/// Stores a client write's entry, keeping the remote fields of `prior`.
fn store(
    index: &mut IndexWriter<'_>,
    shard: &ShardRef,
    key: &str,
    version: EpochSeq,
    state: EntryState,
    object: Option<ObjectVersion>,
    prior: Option<Entry>,
) -> Result<(), IndexError> {
    let (remote_etag, remote_version_id) = prior.map_or((None, None), |prior| {
        (prior.remote_etag, prior.remote_version_id)
    });
    let entry = Entry {
        version,
        state,
        object,
        remote_etag,
        remote_version_id,
    };
    index.put_entry(shard, key, &entry)
}

/// `TAGS`: replaces a live object's tags as a new version, which the
/// flusher must send to the remote. The new version keeps the object's
/// bytes and `Last-Modified`, and its write identity names the `TAGS`
/// record.
fn set_tags(
    index: &mut IndexWriter<'_>,
    shard: &ShardRef,
    position: EpochSeq,
    tags: &Tags,
) -> Result<Rule, IndexError> {
    let Some(mut entry) = index.entry(shard, &tags.key)? else {
        return Ok(Err(Rejection::NoEntry));
    };
    let state = written_state(Some(&entry));
    let Some(object) = &mut entry.object else {
        return Ok(Err(Rejection::Deleted));
    };
    object.tags.clone_from(&tags.tags);
    object.write_identity = None;
    entry.version = position;
    entry.state = state;
    index.put_entry(shard, &tags.key, &entry)?;
    Ok(Ok(Effect::Stored { state }))
}

/// `FLUSHED(key, seq, remote_etag, remote_version_id)` (§7.1).
///
/// - Of the current version: the entry becomes clean, or is removed if it
///   is a tombstone. A clean entry rejects it as a duplicate.
/// - Of an older version, while a newer one waits to be flushed: the remote
///   now holds the older version, so its ETag and version ID are recorded
///   for the next flush to condition on, and the entry stays as it is. A
///   newer version that is clean already rejects it.
/// - Of a version newer than the entry's, or of a key with no entry: it
///   names nothing this replica holds, and is rejected.
fn flush(
    index: &mut IndexWriter<'_>,
    shard: &ShardRef,
    flushed: &Flushed,
) -> Result<Rule, IndexError> {
    let Some(mut entry) = index.entry(shard, &flushed.key)? else {
        return Ok(Err(Rejection::NoEntry));
    };
    let current = entry.version.seq;
    let clean = matches!(entry.state, EntryState::Clean | EntryState::Evicted);
    if flushed.seq > current {
        return Ok(Err(Rejection::UnknownVersion {
            flushed: flushed.seq,
            current,
        }));
    }
    if clean {
        return Ok(Err(if flushed.seq == current {
            Rejection::AlreadyClean
        } else {
            Rejection::Stale
        }));
    }
    let effect = if flushed.seq < current {
        Effect::RemoteRecorded
    } else if entry.object.is_some() != flushed.remote_etag.is_some() {
        return Ok(Err(Rejection::RemoteEtagMismatch));
    } else if entry.object.is_none() {
        index.remove_entry(shard, &flushed.key)?;
        return Ok(Ok(Effect::Removed));
    } else {
        entry.state = EntryState::Clean;
        Effect::Cleaned
    };
    entry.remote_etag.clone_from(&flushed.remote_etag);
    entry
        .remote_version_id
        .clone_from(&flushed.remote_version_id);
    index.put_entry(shard, &flushed.key, &entry)?;
    Ok(Ok(effect))
}

/// `IMPORT`: a stub for a remote object, only if the key has no entry at
/// all (§9.1). Its user metadata and content type are loaded later.
fn import_stub(
    index: &mut IndexWriter<'_>,
    shard: &ShardRef,
    position: EpochSeq,
    import: &Import,
) -> Result<Rule, IndexError> {
    if index.entry(shard, &import.key)?.is_some() {
        return Ok(Err(Rejection::HasEntry));
    }
    let entry = Entry {
        version: position,
        state: EntryState::Evicted,
        object: Some(ObjectVersion {
            size: import.size,
            last_modified_ms: import.last_modified_ms,
            local_etag: import.etag.clone(),
            write_identity: None,
            metadata: Default::default(),
            tags: Default::default(),
            checksums: Default::default(),
            storage_class: import.storage_class.clone(),
            copy_source: None,
            payload: Payload::None,
        }),
        remote_etag: Some(import.etag.clone()),
        remote_version_id: None,
    };
    index.put_entry(shard, &import.key, &entry)?;
    Ok(Ok(Effect::Imported))
}

/// `ADOPT`: the remote changed out of band, and the entry is still clean at
/// the version the read plan named (§9.2). The remote version replaces it
/// as a stub, at the `ADOPT` record's position, so no cache serves the old
/// version's bytes under the new identity. Its tags are unknown and left
/// empty.
fn adopt_remote(
    index: &mut IndexWriter<'_>,
    shard: &ShardRef,
    position: EpochSeq,
    adopt: &Adopt,
) -> Result<Rule, IndexError> {
    let Some(entry) = index.entry(shard, &adopt.key)? else {
        return Ok(Err(Rejection::NoEntry));
    };
    if !matches!(entry.state, EntryState::Clean | EntryState::Evicted) {
        return Ok(Err(Rejection::NotClean(entry.state)));
    }
    if entry.version.seq != adopt.expected_seq {
        return Ok(Err(Rejection::VersionChanged {
            expected: adopt.expected_seq,
            current: entry.version.seq,
        }));
    }
    let adopted = Entry {
        version: position,
        state: EntryState::Evicted,
        object: Some(ObjectVersion {
            size: adopt.size,
            last_modified_ms: adopt.last_modified_ms,
            local_etag: adopt.remote_etag.clone(),
            write_identity: None,
            metadata: adopt.metadata.clone(),
            tags: Default::default(),
            checksums: adopt.checksums.clone(),
            storage_class: None,
            copy_source: None,
            payload: Payload::None,
        }),
        remote_etag: Some(adopt.remote_etag.clone()),
        remote_version_id: adopt.remote_version_id.clone(),
    };
    index.put_entry(shard, &adopt.key, &adopted)?;
    Ok(Ok(Effect::Adopted))
}
