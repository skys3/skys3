//! The in-memory control store, for tests and simulation.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::Bytes;
use skys3_types::Generation;
use tokio::sync::watch;

use crate::feed::ChangeFeed;
use crate::key::{KeyPrefix, RegisterKey};
use crate::store::{ControlError, ControlStore, Expected, PutOutcome, Version, Versioned};

/// A control store held in memory: registers in a map, versioned by a
/// counter of writes, so no two writes share a version.
///
/// Clones share the registers. Every call answers at once and never fails;
/// wrap the store in `faults::FaultyStore` (feature `test-util`) to inject
/// conflicts, outages, and lost requests and responses. Its change feed is
/// notified by every write.
#[derive(Debug, Clone)]
pub struct MemoryControlStore {
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    state: Mutex<State>,
    written: watch::Sender<()>,
}

#[derive(Debug, Default)]
struct State {
    registers: BTreeMap<RegisterKey, Versioned>,
    writes: u64,
}

impl MemoryControlStore {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            shared: Arc::new(Shared {
                state: Mutex::default(),
                written: watch::Sender::new(()),
            }),
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        // Every critical section leaves the state consistent.
        self.shared
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Every register, in key order, for checking a test's end state.
    #[must_use]
    pub fn registers(&self) -> Vec<(RegisterKey, Versioned)> {
        let state = self.state();
        state
            .registers
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }
}

impl Default for MemoryControlStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ControlStore for MemoryControlStore {
    type Changes = ChangeFeed<Self>;

    async fn get(&self, key: &RegisterKey) -> Result<Option<Versioned>, ControlError> {
        Ok(self.state().registers.get(key).cloned())
    }

    async fn put_if(
        &self,
        key: &RegisterKey,
        expected: Expected,
        value: Bytes,
    ) -> Result<PutOutcome, ControlError> {
        let mut state = self.state();
        let current = state.registers.get(key).map(|current| &current.version);
        let holds = match (&expected, current) {
            (Expected::Absent, None) => true,
            (Expected::Version(expected), Some(current)) => expected == current,
            _ => false,
        };
        if !holds {
            return Ok(PutOutcome::PreconditionFailed);
        }
        state.writes += 1;
        let version = Version::new(state.writes.to_string());
        state.registers.insert(
            key.clone(),
            Versioned {
                value,
                version: version.clone(),
            },
        );
        drop(state);
        self.shared.written.send_replace(());
        Ok(PutOutcome::Written(version))
    }

    async fn list(&self, prefix: &KeyPrefix) -> Result<Vec<(RegisterKey, Version)>, ControlError> {
        let state = self.state();
        Ok(state
            .registers
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.version.clone()))
            .collect())
    }

    async fn changes(&self, after: Generation) -> Result<Self::Changes, ControlError> {
        Ok(ChangeFeed::notified(
            self.clone(),
            after,
            self.shared.written.subscribe(),
        ))
    }
}
