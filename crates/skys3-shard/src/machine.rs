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
//! that already has an entry, is rejected: the namespace and the uploads
//! are left as they were, the shard's applied position still advances past
//! it, and the [`Outcome`] says why. The only change a rejected record
//! makes is to release the bytes of an `MPU_PART` whose upload is gone,
//! which nothing can reach (see the `multipart` module).

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;

use skys3_index::{
    Applier, Coded, Entry, EntryState, IndexError, IndexWriter, ObjectVersion, Payload,
};
use skys3_log::record::{
    Adopt, Delete, EcPublish, EcRelocate, Flushed, IDENTITY_METADATA, Import, Put, PutData, Tags,
};
use skys3_log::{LogRecord, RecordBody, RecordKind, RecordLocation, ShardRef};
use skys3_types::{CodedStripe, EpochSeq, FragmentLocation, Seq};

use crate::multipart;
use crate::seeded::{SeededBug, is_seeded};

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
    /// A `FLUSHED` of an older version, or an `IMPORT` of a key written
    /// before the import reached it, recorded what the remote holds,
    /// leaving the entry dirty.
    RemoteRecorded,
    /// An `IMPORT` created a stub.
    Imported,
    /// An `ADOPT` replaced the version it named with the remote's: a
    /// clean one after a fill, or a dirty one that a conflict policy
    /// discarded (§7.2).
    Adopted,
    /// An `EXTENT` was entered into the location map.
    Located,
    /// An `MPU_CREATE` opened an upload.
    UploadCreated,
    /// An `MPU_PART` stored a part, replacing any with its number.
    PartStored,
    /// An `MPU_ABORT` removed an upload and its `parts`, and released their
    /// bytes.
    Aborted {
        /// How many parts the upload had.
        parts: usize,
    },
    /// A `PART_FLUSHED` recorded a step of a streamed upload's remote
    /// counterpart (§7.3): it was opened, took a part, or ended.
    RemoteUploadMoved,
    /// An `UPLOAD_BEGIN` fixed the write identity of a streamed PUT
    /// (§7.2). It changes no entry: only the `PUT` that inherits the
    /// identity does.
    UploadBegun,
    /// An `EC_PUBLISH` marked the key's current version coded (§8.4).
    Published,
    /// An `EC_RELOCATE` moved fragments of the current version's layout to
    /// new locations (§8.6).
    Relocated,
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
    /// An `IMPORT` found an entry other than a local change whose remote
    /// state is unknown: an earlier import, or an entry whose remote ETag
    /// is known.
    #[error("the key already has an entry")]
    HasEntry,
    /// An `ADOPT` found the entry at another version than it named, and
    /// not clean: a local write committed after the read plan or the
    /// conflict it resolves.
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
    /// An `MPU_PART`, `MPU_COMPLETE`, or `MPU_ABORT` names an upload that
    /// is not open: never opened, or completed or aborted already.
    #[error("the upload is not open")]
    NoSuchUpload,
    /// An `MPU_COMPLETE` lists a part that is missing, or was replaced
    /// after the completion read it.
    #[error("part {number} is not the part the completion names")]
    PartChanged {
        /// The part number.
        number: u16,
    },
    /// A `PART_FLUSHED` names a remote upload this replica does not hold
    /// for the local upload: never opened, ended already, or replaced by
    /// another.
    #[error("the remote upload is not recorded")]
    NoRemoteUpload,
    /// An `EC_PUBLISH` names a version that is no longer the key's
    /// current one: an overwrite, a delete, or a tag change committed
    /// after the encoder read it (§8.4). Its fragments become orphans.
    #[error("the published version is superseded")]
    Superseded,
    /// An `EC_PUBLISH` names a version that another `EC_PUBLISH` coded
    /// already: an attempt that outlived its primary's life.
    #[error("the version is coded already")]
    AlreadyCoded,
    /// An `EC_RELOCATE` names a version that is not coded.
    #[error("the version is not coded")]
    NotCoded,
    /// An `EC_RELOCATE` moves a fragment that the layout no longer locates
    /// where the record says it was, such as one another repair or move
    /// relocated first, or would put two fragments of a stripe on one node.
    /// Its new fragments become orphans.
    #[error(
        "the layout no longer locates fragment {index} of stripe {stripe} where the record moves it from"
    )]
    Moved {
        /// The stripe's number.
        stripe: u32,
        /// The fragment's index.
        index: u8,
    },
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
pub(crate) type Rule = Result<Effect, Rejection>;

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
            RecordBody::MpuCreate(create) => multipart::create(index, shard, position, create)?,
            RecordBody::MpuPart(part) => {
                multipart::store_part(index, shard, position, part, location)?
            }
            RecordBody::MpuComplete(complete) => {
                multipart::complete(index, shard, position, complete)?
            }
            RecordBody::MpuAbort(abort) => multipart::abort(index, shard, abort)?,
            // Its position is the identity; the `PUT` that completes the
            // upload records it (`put_object`).
            RecordBody::UploadBegin(_) => Ok(Effect::UploadBegun),
            RecordBody::Tags(tags) => set_tags(index, shard, position, tags)?,
            RecordBody::Flushed(flushed) => flush(index, shard, flushed)?,
            RecordBody::PartFlushed(flushed) => multipart::part_flushed(index, shard, flushed)?,
            RecordBody::Import(import) => import_stub(index, shard, position, import)?,
            RecordBody::Adopt(adopt) => adopt_remote(index, shard, position, adopt)?,
            RecordBody::EcPublish(publish) => publish_coded(index, shard, position, publish)?,
            RecordBody::EcRelocate(relocate) => relocate_coded(index, shard, relocate)?,
            // A replica's own bookkeeping: the configuration it adopted,
            // which the index keeps as the replica's local copy of it
            // (§6.2), or where reconciliation cut its log (§6.6).
            RecordBody::Config(config) => {
                index.put_config(config)?;
                Ok(Effect::Unchanged)
            }
            RecordBody::Truncate => Ok(Effect::Unchanged),
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
pub(crate) fn written_state(prior: Option<&Entry>) -> EntryState {
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
        coded: None,
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

/// Stores a client write's entry, keeping the remote fields of `prior`, and
/// dropping the parts of a multipart object `prior` held.
pub(crate) fn store(
    index: &mut IndexWriter<'_>,
    shard: &ShardRef,
    key: &str,
    version: EpochSeq,
    state: EntryState,
    object: Option<ObjectVersion>,
    prior: Option<Entry>,
) -> Result<(), IndexError> {
    multipart::drop_parts(index, shard, prior.as_ref())?;
    release_coded(index, shard, key, prior.as_ref())?;
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
/// record: an identity the object carried from another cluster (§7.8) is
/// dropped with its metadata entry.
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
    object.metadata.remove(IDENTITY_METADATA);
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
///
/// A key written locally before the import reached it has an entry whose
/// remote state is unknown: dirty, with no remote ETag. The `IMPORT` then
/// records what the remote held instead, as its remote ETag, so the
/// flush replaces that object with `If-Match` (§7.2) rather than finding
/// it foreign. Every other entry rejects it.
fn import_stub(
    index: &mut IndexWriter<'_>,
    shard: &ShardRef,
    position: EpochSeq,
    import: &Import,
) -> Result<Rule, IndexError> {
    if let Some(mut entry) = index.entry(shard, &import.key)? {
        let unknown = matches!(entry.state, EntryState::Dirty | EntryState::Conflict)
            && entry.remote_etag.is_none();
        if !unknown {
            return Ok(Err(Rejection::HasEntry));
        }
        entry.remote_etag = Some(import.etag.clone());
        index.put_entry(shard, &import.key, &entry)?;
        return Ok(Ok(Effect::RemoteRecorded));
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
            coded: None,
        }),
        remote_etag: Some(import.etag.clone()),
        remote_version_id: None,
    };
    index.put_entry(shard, &import.key, &entry)?;
    Ok(Ok(Effect::Imported))
}

/// `ADOPT`: the remote changed out of band, and the entry is still at the
/// version the record names. The remote version replaces it as a stub, at
/// the `ADOPT` record's position, so no cache serves the old version's
/// bytes under the new identity. Its tags are unknown and left empty.
///
/// A fill names a clean version, the one its read plan named (§9.2). The
/// `discard_local` conflict policy names a dirty one, a version or a
/// tombstone, whose flush found an out-of-band write: the local version is
/// dropped (§7.2). A clean version never becomes dirty again at its `seq`,
/// so a fill's `ADOPT` still applies only while its version is clean, and
/// any write committed after the version named rejects either.
fn adopt_remote(
    index: &mut IndexWriter<'_>,
    shard: &ShardRef,
    position: EpochSeq,
    adopt: &Adopt,
) -> Result<Rule, IndexError> {
    let Some(entry) = index.entry(shard, &adopt.key)? else {
        return Ok(Err(Rejection::NoEntry));
    };
    if entry.version.seq != adopt.expected_seq {
        if !matches!(entry.state, EntryState::Clean | EntryState::Evicted) {
            return Ok(Err(Rejection::NotClean(entry.state)));
        }
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
            coded: None,
        }),
        remote_etag: Some(adopt.remote_etag.clone()),
        remote_version_id: adopt.remote_version_id.clone(),
    };
    multipart::drop_parts(index, shard, Some(&entry))?;
    release_coded(index, shard, &adopt.key, Some(&entry))?;
    index.put_entry(shard, &adopt.key, &adopted)?;
    Ok(Ok(Effect::Adopted))
}

/// `EC_PUBLISH`: the version the record names is coded, if it is still the
/// key's current version and not coded yet (§8.4). Its stripes become the
/// version's layout, and their fragments enter the shard's index from node
/// to fragments. The entry keeps its version, state, and payload: the
/// replicas drop the replicated bytes only once they know the record
/// committed (see [`Holder::Coded`](skys3_index::Holder::Coded)).
fn publish_coded(
    index: &mut IndexWriter<'_>,
    shard: &ShardRef,
    position: EpochSeq,
    publish: &EcPublish,
) -> Result<Rule, IndexError> {
    let Some(mut entry) = index.entry(shard, &publish.key)? else {
        return Ok(Err(Rejection::NoEntry));
    };
    let Some(object) = &mut entry.object else {
        return Ok(Err(Rejection::Deleted));
    };
    let current = entry.version == publish.version
        && object.local_etag == publish.etag
        && object.size == publish.size;
    if !current && !(is_seeded(SeededBug::PublishSuperseded) && object.size == publish.size) {
        return Ok(Err(Rejection::Superseded));
    }
    if object.coded.is_some() {
        return Ok(Err(Rejection::AlreadyCoded));
    }
    let coded = Coded {
        publish: position,
        attempt: publish.attempt,
        stripes: publish.stripes.clone(),
    };
    index.put_fragments(shard, &publish.key, &coded)?;
    object.coded = Some(coded);
    index.put_entry(shard, &publish.key, &entry)?;
    Ok(Ok(Effect::Published))
}

/// `EC_RELOCATE`: the fragments the record moves get their new locations
/// in the layout of the version it names, if that is still the key's
/// current version, coded, and the layout still locates each fragment where
/// the record moves it from (§8.6). The shard's index from node to
/// fragments follows. Otherwise nothing changes, and the new fragments are
/// orphans. The entry keeps everything else, its `EC_PUBLISH` position
/// included, which is what replicas drop their copies by.
fn relocate_coded(
    index: &mut IndexWriter<'_>,
    shard: &ShardRef,
    relocate: &EcRelocate,
) -> Result<Rule, IndexError> {
    let Some(mut entry) = index.entry(shard, &relocate.key)? else {
        return Ok(Err(Rejection::NoEntry));
    };
    let Some(object) = &mut entry.object else {
        return Ok(Err(Rejection::Deleted));
    };
    if entry.version != relocate.version || object.local_etag != relocate.etag {
        return Ok(Err(Rejection::Superseded));
    }
    let Some(coded) = &mut object.coded else {
        return Ok(Err(Rejection::NotCoded));
    };
    // Every move of a stripe lands before the stripe is checked, so two
    // fragments may trade nodes.
    let mut moved_stripes: BTreeMap<usize, Vec<FragmentLocation>> = BTreeMap::new();
    for moved in &relocate.moves {
        let rejected = Rejection::Moved {
            stripe: moved.stripe,
            index: moved.index,
        };
        let Some(number) = usize::try_from(moved.stripe)
            .ok()
            .filter(|number| *number < coded.stripes.len())
        else {
            return Ok(Err(rejected));
        };
        let fragments = moved_stripes
            .entry(number)
            .or_insert_with(|| coded.stripes[number].fragments().to_vec());
        match fragments.get_mut(usize::from(moved.index)) {
            Some(location) if *location == moved.from => *location = moved.to.clone(),
            _ => return Ok(Err(rejected)),
        }
    }
    let mut stripes = coded.stripes.clone();
    for (number, fragments) in moved_stripes {
        let stripe = &stripes[number];
        let Ok(relocated) = CodedStripe::new(
            stripe.number(),
            stripe.offset(),
            stripe.data_len(),
            stripe.geometry(),
            stripe.codec(),
            fragments,
        ) else {
            let index = relocate
                .moves
                .iter()
                .find(|moved| moved.stripe == stripe.number())
                .map_or(0, |moved| moved.index);
            return Ok(Err(Rejection::Moved {
                stripe: stripe.number(),
                index,
            }));
        };
        stripes[number] = relocated;
    }
    for moved in &relocate.moves {
        index.move_fragment(
            shard,
            &relocate.key,
            (moved.stripe, moved.index),
            &moved.from,
            &moved.to,
        )?;
    }
    coded.stripes = stripes;
    index.put_entry(shard, &relocate.key, &entry)?;
    Ok(Ok(Effect::Relocated))
}

/// Removes the fragments of `prior`'s coded layout, if it has one, from the
/// shard's index from node to fragments, as a write replaces the version.
/// The fragments themselves stay until the version is released (plan
/// M5-07).
fn release_coded(
    index: &mut IndexWriter<'_>,
    shard: &ShardRef,
    key: &str,
    prior: Option<&Entry>,
) -> Result<(), IndexError> {
    if let Some(coded) = prior
        .and_then(|prior| prior.object.as_ref())
        .and_then(|object| object.coded.as_ref())
    {
        index.remove_fragments(shard, key, coded)?;
    }
    Ok(())
}
