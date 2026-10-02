//! The node-local cache transitions of §4.2: a read-through fill makes an
//! evicted entry clean (§9.2), and eviction makes a clean entry evicted
//! (§9.3).
//!
//! Both change only which payload this node holds for the version an entry
//! names: never the version, its metadata, or its remote fields. No record
//! makes them, so every replica decides them for itself, and they are
//! committed with [`Index::update_local`](skys3_index::Index::update_local).
//! The state machine treats clean and evicted entries alike, so applying a
//! record gives the same entry whichever of the two a replica holds, and
//! replay after a crash, which may lose such a change, never contradicts
//! it.
//!
//! Each transition names the version it was decided for, and is refused
//! once the entry holds another: a write that committed in between wins.

use skys3_index::{EntryState, IndexError, IndexWriter, Payload};
use skys3_log::ShardRef;
use skys3_types::EpochSeq;

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
    /// extents that add up to its size.
    #[error("the payload does not hold the object's bytes")]
    Payload,
    /// The object is a completed multipart upload. An evicted stub has no
    /// place for its part boundaries yet, which `partNumber` reads and the
    /// flush of a later `TAGS` need, so it is not evicted (plan M1-21).
    #[error("multipart objects are not evicted")]
    Multipart,
}

/// Evicted → Clean: the bytes of the evicted version `version` of `key`
/// were filled from the remote into `payload`, `EXTENT` records of the
/// shard (§9.2). The entry becomes clean with that payload, unless it no
/// longer is that evicted version.
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
    let Some(mut entry) = index.entry(shard, key)? else {
        return Ok(Err(CacheRefusal::NoEntry));
    };
    if entry.version != version {
        return Ok(Err(CacheRefusal::VersionChanged {
            expected: version,
            current: entry.version,
        }));
    }
    if entry.state != EntryState::Evicted {
        return Ok(Err(CacheRefusal::State(entry.state)));
    }
    let Some(object) = &mut entry.object else {
        return Ok(Err(CacheRefusal::Deleted));
    };
    let Payload::Extents(extents) = &payload else {
        return Ok(Err(CacheRefusal::Payload));
    };
    let filled: u64 = extents.iter().map(|extent| u64::from(extent.len)).sum();
    if filled != object.size {
        return Ok(Err(CacheRefusal::Payload));
    }
    object.payload = payload;
    entry.state = EntryState::Clean;
    index.put_entry(shard, key, &entry)?;
    Ok(Ok(()))
}

/// Clean → Evicted: drops this node's payload of the clean version
/// `version` of `key`, leaving a stub with its metadata (§4.2). Dirty
/// payload is never evicted. The bytes stay in the log until compaction
/// reclaims them (§10.3), so a read that resolved the version before may
/// still finish.
///
/// This is the transition only. Choosing what to evict, within
/// `cache_max_bytes_per_node`, is plan M1-21's.
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
    let Some(mut entry) = index.entry(shard, key)? else {
        return Ok(Err(CacheRefusal::NoEntry));
    };
    if entry.version != version {
        return Ok(Err(CacheRefusal::VersionChanged {
            expected: version,
            current: entry.version,
        }));
    }
    if entry.state != EntryState::Clean {
        return Ok(Err(CacheRefusal::State(entry.state)));
    }
    let Some(object) = &mut entry.object else {
        return Ok(Err(CacheRefusal::Deleted));
    };
    if matches!(object.payload, Payload::Parts { .. }) {
        return Ok(Err(CacheRefusal::Multipart));
    }
    object.payload = Payload::None;
    entry.state = EntryState::Evicted;
    index.put_entry(shard, key, &entry)?;
    Ok(Ok(()))
}
