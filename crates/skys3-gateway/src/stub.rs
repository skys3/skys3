//! An in-memory [`Shards`] stub, for tests until the shard state machine
//! (plan M1-04) lands.
//!
//! [`MemoryShards`] keeps each shard's entries in a map with the state the
//! design gives them (§4.2), routes keys with the frozen hash, and honors
//! seals the way the trait requires. Tests write entries with
//! [`MemoryShards::put`] and flush them with [`MemoryShards::flush`].

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use skys3_types::{BucketDocument, BucketId};

use crate::shard::{ShardError, ShardRef, ShardSummary, Shards};

/// The state of one entry in a stub shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryState {
    /// A committed PUT that has not reached the target.
    Dirty,
    /// A PUT that matches the target.
    Clean,
    /// A committed DELETE that has not reached the target.
    Tombstone,
}

/// Shards held in memory. Clones share the shards.
#[derive(Debug, Clone, Default)]
pub struct MemoryShards {
    state: Arc<Mutex<State>>,
}

#[derive(Debug, Default)]
struct State {
    shards: BTreeMap<ShardRef, StubShard>,
    /// When set, every request fails with [`ShardError::Unavailable`].
    unavailable: bool,
}

#[derive(Debug)]
struct StubShard {
    seals: u32,
    entries: BTreeMap<String, EntryState>,
}

impl MemoryShards {
    /// No shards.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Commits a client write of `key` to its shard of `bucket`, as `state`.
    ///
    /// # Errors
    ///
    /// [`ShardError::NotFound`] if the shard is not open, and
    /// [`ShardError::Sealed`] while it is sealed.
    pub fn put(
        &self,
        bucket: &BucketDocument,
        key: &str,
        state: EntryState,
    ) -> Result<ShardRef, ShardError> {
        let shard = ShardRef::for_key(bucket, key);
        let mut guard = self.state();
        let stub = guard
            .shards
            .get_mut(&shard)
            .ok_or_else(|| ShardError::NotFound(shard.clone()))?;
        if stub.seals > 0 {
            return Err(ShardError::Sealed(shard));
        }
        stub.entries.insert(key.to_owned(), state);
        Ok(shard)
    }

    /// Flushes every entry of `bucket`: dirty entries become clean and
    /// tombstones go away. Flushing ignores seals.
    pub fn flush(&self, bucket: &BucketId) {
        for (_, stub) in self
            .state()
            .shards
            .iter_mut()
            .filter(|(s, _)| s.bucket == *bucket)
        {
            stub.entries
                .retain(|_, state| *state != EntryState::Tombstone);
            stub.entries
                .values_mut()
                .for_each(|state| *state = EntryState::Clean);
        }
    }

    /// The open shards of `bucket`.
    #[must_use]
    pub fn open_shards(&self, bucket: &BucketId) -> Vec<ShardRef> {
        self.state()
            .shards
            .keys()
            .filter(|shard| shard.bucket == *bucket)
            .cloned()
            .collect()
    }

    /// The number of open shards, of every bucket.
    #[must_use]
    pub fn len(&self) -> usize {
        self.state().shards.len()
    }

    /// Whether no shard is open.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether `shard` is sealed, or `None` if it is not open.
    #[must_use]
    pub fn is_sealed(&self, shard: &ShardRef) -> Option<bool> {
        self.state().shards.get(shard).map(|stub| stub.seals > 0)
    }

    /// Makes every later request fail with [`ShardError::Unavailable`], or
    /// succeed again.
    pub fn set_unavailable(&self, unavailable: bool) {
        self.state().unavailable = unavailable;
    }

    /// Runs `action` on the state, or fails if the stub is unavailable.
    fn with<T>(
        &self,
        shard: &ShardRef,
        action: impl FnOnce(&mut State) -> Result<T, ShardError>,
    ) -> Result<T, ShardError> {
        let mut state = self.state();
        if state.unavailable {
            return Err(ShardError::Unavailable {
                shard: shard.clone(),
                reason: "the stub is unavailable".to_owned(),
            });
        }
        action(&mut state)
    }
}

impl Shards for MemoryShards {
    async fn open(&self, shard: &ShardRef, _bucket: &BucketDocument) -> Result<(), ShardError> {
        self.with(shard, |state| {
            state.shards.entry(shard.clone()).or_insert(StubShard {
                seals: 0,
                entries: BTreeMap::new(),
            });
            Ok(())
        })
    }

    async fn seal(&self, shard: &ShardRef) -> Result<ShardSummary, ShardError> {
        self.with(shard, |state| {
            let stub = state
                .shards
                .get_mut(shard)
                .ok_or_else(|| ShardError::NotFound(shard.clone()))?;
            stub.seals += 1;
            let count = |f: fn(&EntryState) -> bool| stub.entries.values().filter(|s| f(s)).count();
            Ok(ShardSummary {
                objects: count(|s| *s != EntryState::Tombstone) as u64,
                unflushed: count(|s| *s != EntryState::Clean) as u64,
            })
        })
    }

    async fn unseal(&self, shard: &ShardRef) -> Result<(), ShardError> {
        self.with(shard, |state| {
            let stub = state
                .shards
                .get_mut(shard)
                .ok_or_else(|| ShardError::NotFound(shard.clone()))?;
            stub.seals = stub.seals.saturating_sub(1);
            Ok(())
        })
    }

    async fn remove(&self, shard: &ShardRef) -> Result<(), ShardError> {
        self.with(shard, |state| {
            state.shards.remove(shard);
            Ok(())
        })
    }
}
