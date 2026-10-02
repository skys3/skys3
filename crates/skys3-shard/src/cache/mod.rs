//! The clean cache of a node (§4.2, §9.3): the node-local transitions
//! between clean and evicted, and [`CleanCache`], which decides what to
//! evict.
//!
//! **Transitions.** A read-through fill makes an evicted entry clean
//! ([`fill`], §9.2), and eviction makes a clean entry evicted ([`evict`]).
//! Both change only which payload this node holds for the version an entry
//! names: never the version, its metadata, or its remote fields. No record
//! makes them, so every replica decides them for itself, and they are
//! committed with [`Index::update_local`](skys3_index::Index::update_local).
//! The state machine treats clean and evicted entries alike, so applying a
//! record gives the same entry whichever of the two a replica holds, and
//! replay after a crash, which may lose such a change, never contradicts
//! it. Each transition names the version it was decided for, and is
//! refused once the entry holds another: a write that committed in between
//! wins. Dirty payload is never evicted: [`evict`] moves only a clean
//! entry.
//!
//! **Multipart objects** keep their part boundaries when evicted: the
//! entry keeps its [`Payload::Parts`], and each part keeps its row, with
//! its ETag and checksums, but no bytes. A fill commits extents that end at
//! the part boundaries, and gives each part its own again.
//!
//! **What to evict** ([`CleanCache`]): every replica tells the node's cache
//! which of its entries became clean with local bytes and which stopped
//! being so. A replica keeps a clean copy only if its rank in the shard's
//! configuration, the primary first and then the other members in order,
//! is below the bucket's `clean_copies`; the others drop theirs at once.
//! The copies kept are evicted least recently used first, within
//! `cache_max_bytes_per_node` and each disk's room under the §9.3 capacity
//! model ([`CacheSettings`]).

mod ledger;

use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};

use skys3_index::{Entry, EntryState, Index, IndexError, IndexReader, IndexWriter, Payload};
use skys3_log::record::ExtentRef;
use skys3_log::{LogRecord, RecordBody, RecordLocation, ShardRef};
use skys3_types::EpochSeq;

use crate::machine::{Effect, Outcome};

pub use ledger::{CacheMetrics, CacheSettings, CacheUsage, CleanCache};
pub(crate) use ledger::{Hot, Note};

/// Why a cache transition was refused. The entry is unchanged.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum CacheRefusal {
    /// The key has no entry.
    #[error("the key has no entry")]
    NoEntry,
    /// The key's entry is a tombstone.
    #[error("the key is deleted")]
    Deleted,
    /// The entry holds another version than the one named.
    #[error("the entry is at {current}, not {expected}")]
    VersionChanged {
        /// The version named.
        expected: EpochSeq,
        /// The entry's version.
        current: EpochSeq,
    },
    /// The entry is not in the state the transition starts from.
    #[error("the entry is {0:?}")]
    State(EntryState),
    /// The payload does not fit the object: a fill must hold its bytes in
    /// extents that add up to its size, and, for a multipart object, end
    /// at its part boundaries.
    #[error("the payload does not hold the object's bytes")]
    Payload,
}

/// Evicted → Clean: the bytes of the evicted version `version` of `key`
/// were filled from the remote into `payload`, `EXTENT` records of the
/// shard (§9.2). The entry becomes clean with that payload, unless it no
/// longer is that evicted version. The extents of a multipart object go to
/// its parts, so they must end at its part boundaries.
///
/// # Errors
///
/// The outer error is the index's; the inner one says why the transition
/// was refused.
pub fn fill(
    index: &mut IndexWriter<'_>,
    shard: &ShardRef,
    key: &str,
    version: EpochSeq,
    payload: Payload,
) -> Result<Result<(), CacheRefusal>, IndexError> {
    let mut entry = match changeable(index.entry(shard, key)?, version, EntryState::Evicted) {
        Ok(entry) => entry,
        Err(refusal) => return Ok(Err(refusal)),
    };
    let Some(object) = &mut entry.object else {
        return Ok(Err(CacheRefusal::Deleted));
    };
    let Payload::Extents(extents) = payload else {
        return Ok(Err(CacheRefusal::Payload));
    };
    let filled: u64 = extents.iter().map(|extent| u64::from(extent.len)).sum();
    if filled != object.size {
        return Ok(Err(CacheRefusal::Payload));
    }
    match &object.payload {
        Payload::Parts { upload, parts } => {
            let (upload, mut rest) = (*upload, extents.as_slice());
            let mut rows = Vec::with_capacity(parts.len());
            for listed in parts {
                let (Some(mut part), Some(taken)) = (
                    index.part(shard, upload, listed.number)?,
                    take(&mut rest, listed.size),
                ) else {
                    return Ok(Err(CacheRefusal::Payload));
                };
                part.payload = Payload::Extents(taken);
                rows.push((listed.number, part));
            }
            for (number, part) in rows {
                index.put_part(shard, upload, number, &part)?;
            }
        }
        _ => object.payload = Payload::Extents(extents),
    }
    entry.state = EntryState::Clean;
    index.put_entry(shard, key, &entry)?;
    Ok(Ok(()))
}

/// Clean → Evicted: drops this node's payload of the clean version
/// `version` of `key`, leaving a stub with its metadata (§4.2). Dirty
/// payload is never evicted. A multipart object keeps its parts, without
/// their bytes. The bytes stay in the log until compaction reclaims them
/// (§10.3), so a read that found where they are before may still finish.
///
/// # Errors
///
/// As [`fill`].
pub fn evict(
    index: &mut IndexWriter<'_>,
    shard: &ShardRef,
    key: &str,
    version: EpochSeq,
) -> Result<Result<(), CacheRefusal>, IndexError> {
    let mut entry = match changeable(index.entry(shard, key)?, version, EntryState::Clean) {
        Ok(entry) => entry,
        Err(refusal) => return Ok(Err(refusal)),
    };
    let Some(object) = &mut entry.object else {
        return Ok(Err(CacheRefusal::Deleted));
    };
    match &object.payload {
        Payload::Parts { upload, parts } => {
            for listed in parts {
                if let Some(mut part) = index.part(shard, *upload, listed.number)? {
                    part.payload = Payload::None;
                    index.put_part(shard, *upload, listed.number, &part)?;
                }
            }
        }
        _ => object.payload = Payload::None,
    }
    entry.state = EntryState::Evicted;
    index.put_entry(shard, key, &entry)?;
    Ok(Ok(()))
}

/// The entry, if it is at `version` and in `state`.
fn changeable(
    entry: Option<Entry>,
    version: EpochSeq,
    state: EntryState,
) -> Result<Entry, CacheRefusal> {
    let entry = entry.ok_or(CacheRefusal::NoEntry)?;
    if entry.version != version {
        return Err(CacheRefusal::VersionChanged {
            expected: version,
            current: entry.version,
        });
    }
    if entry.state != state {
        return Err(CacheRefusal::State(entry.state));
    }
    Ok(entry)
}

/// Takes the extents at the front of `rest` that hold exactly `size`
/// bytes, or `None` if they do not end there.
fn take(rest: &mut &[ExtentRef], size: u64) -> Option<Vec<ExtentRef>> {
    let mut held = 0;
    let mut count = 0;
    while held < size {
        held += u64::from(rest.get(count)?.len);
        count += 1;
    }
    if held != size {
        return None;
    }
    let (taken, left) = rest.split_at(count);
    *rest = left;
    Some(taken.to_vec())
}

/// Whether `payload`, an entry's in `shard`, has its bytes on this node: a
/// multipart object's parts have none once it is evicted, or once a `TAGS`
/// copied an evicted one.
pub(crate) fn holds_bytes(
    index: &IndexReader,
    shard: &ShardRef,
    payload: &Payload,
) -> Result<bool, IndexError> {
    Ok(match payload {
        Payload::None => false,
        Payload::Inline(_) | Payload::Extents(_) => true,
        // A part's bytes are dropped and filled with every other part's.
        Payload::Parts { upload, .. } => index
            .parts(shard, *upload, 0, 1)?
            .first()
            .is_none_or(|(_, part)| !matches!(part.payload, Payload::None)),
    })
}

/// What `entry` of `key` means for the cache: a clean entry with an object
/// is a [`Note::Clean`], anything else a [`Note::Gone`]. `last_used` is
/// when it was last used, if not now.
pub(crate) fn note(
    index: &IndexReader,
    shard: &ShardRef,
    key: String,
    entry: Option<&Entry>,
    last_used: Option<u64>,
) -> Result<Note, IndexError> {
    let clean = entry.filter(|entry| entry.state == EntryState::Clean);
    Ok(
        match clean.and_then(|entry| Some((entry, entry.object.as_ref()?))) {
            Some((entry, object)) => Note::Clean {
                key,
                version: entry.version,
                size: object.size,
                held: holds_bytes(index, shard, &object.payload)?,
                last_used,
            },
            None => Note::Gone { key },
        },
    )
}

/// What applying `records` did to the clean entries of `shard`, as the
/// cache learns it: an entry a `FLUSHED` made clean, and every entry a
/// write, an `ADOPT`, or a removal replaced. `outcomes` are the records'
/// outcomes, by position, and `index` holds what they did.
pub(crate) fn notes(
    index: &Index,
    shard: &ShardRef,
    records: &[(LogRecord, RecordLocation)],
    outcomes: &[(EpochSeq, Outcome)],
) -> Result<Vec<Note>, IndexError> {
    let keys: BTreeMap<EpochSeq, &str> = records
        .iter()
        .filter_map(|(record, _)| Some((record.position, key_of(&record.body)?)))
        .collect();
    let mut notes = Vec::new();
    let mut reader = None;
    for (position, outcome) in outcomes {
        let Some(&key) = keys.get(position) else {
            continue;
        };
        match outcome {
            Outcome::Applied(Effect::Cleaned) => {
                let reader = match &mut reader {
                    Some(reader) => reader,
                    None => reader.insert(index.read()?),
                };
                let entry = reader.entry(shard, key)?;
                notes.push(note(reader, shard, key.to_owned(), entry.as_ref(), None)?);
            }
            Outcome::Applied(
                Effect::Stored { .. }
                | Effect::Tombstoned { .. }
                | Effect::Removed
                | Effect::Adopted,
            ) => notes.push(Note::Gone {
                key: key.to_owned(),
            }),
            _ => {}
        }
    }
    Ok(notes)
}

/// The key a record names, if it may change whether an entry is clean.
fn key_of(body: &RecordBody) -> Option<&str> {
    Some(match body {
        RecordBody::Put(put) => &put.key,
        RecordBody::Delete(delete) => &delete.key,
        RecordBody::Tags(tags) => &tags.key,
        RecordBody::MpuComplete(complete) => &complete.key,
        RecordBody::Flushed(flushed) => &flushed.key,
        RecordBody::Adopt(adopt) => &adopt.key,
        _ => return None,
    })
}

/// A replica's link to its node's [`CleanCache`], once it has one.
///
/// Every change of the replica's entries that the cache follows, a record
/// applied, a fill, an eviction, and a scan's read, runs under the link's
/// lock together with its report, so the cache learns them in the order
/// the index made them: a report never undoes a later one.
#[derive(Debug, Default)]
pub(crate) struct Link {
    cache: OnceLock<CleanCache>,
    order: Mutex<()>,
}

impl Link {
    /// The cache, once the replica reports to one.
    pub(crate) fn get(&self) -> Option<&CleanCache> {
        self.cache.get()
    }

    /// Links the replica to `cache`; returns `false` if it has one already.
    pub(crate) fn set(&self, cache: &CleanCache) -> bool {
        self.cache.set(cache.clone()).is_ok()
    }

    /// Holds the replica's changes, and their reports, in order.
    pub(crate) fn order(&self) -> MutexGuard<'_, ()> {
        // The lock guards no data.
        self.order.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
