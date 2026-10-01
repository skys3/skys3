//! The control store as the node uses it, and the node's local copy of
//! bucket bindings and identity configuration (design §6.2).
//!
//! The node keeps a durable copy of every register under `buckets/` and
//! `identity/` in its index, tagged with the generation it was read at and
//! the time its sync started ([`ControlCopy`]). At startup it serves the
//! gateway and STS from that copy until the control store answers
//! ([`NodeStore`]), so a node restarts with its buckets and identity
//! configuration while the store is unreachable. Writes, which only bucket
//! creation and deletion make, fail as unavailable until then. Each sync
//! after that replaces the copy.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use bytes::Bytes;
use skys3_control::{
    ControlError, ControlStore, DeleteOutcome, Expected, KeyPrefix, PutOutcome, RegisterKey,
    RetryPolicy, Version, Versioned, read_cluster,
};
use skys3_index::{ControlEntry, Index, IndexError};
use skys3_types::{ClusterId, Generation};

/// The prefixes of the registers the node copies.
fn copied() -> [KeyPrefix; 2] {
    [KeyPrefix::buckets(), KeyPrefix::identity()]
}

/// A complete copy of the registers under `buckets/` and `identity/`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlCopy {
    /// The generation `cluster.json` held before the registers were read:
    /// the copy holds every change it announced.
    pub generation: Generation,
    /// When the sync that read the copy started, as time since the Unix
    /// epoch. The identity copy's age runs from it (§6.2).
    pub synced_at: Duration,
    /// Every copied register.
    pub registers: BTreeMap<RegisterKey, Versioned>,
}

impl ControlCopy {
    /// Reads every copied register from `store`. `started` is the wall
    /// time at which the sync started.
    ///
    /// # Errors
    ///
    /// The store's error once `retry` is exhausted, and
    /// [`ControlError::NotBootstrapped`] or
    /// [`ControlError::ClusterMismatch`] if `cluster.json` is missing or
    /// names another cluster.
    pub async fn fetch<C: ControlStore>(
        store: &C,
        cluster: &ClusterId,
        retry: &RetryPolicy,
        started: Duration,
    ) -> Result<Self, ControlError> {
        let generation = read_cluster(store, cluster, retry).await?.value.generation;
        let mut registers = BTreeMap::new();
        for prefix in copied() {
            for (key, _) in retrying(retry, || store.list(&prefix)).await? {
                if let Some(value) = retrying(retry, || store.get(&key)).await? {
                    registers.insert(key, value);
                }
            }
        }
        Ok(Self {
            generation,
            synced_at: started,
            registers,
        })
    }

    /// The copy `index` keeps, or `None` if the node never synced.
    ///
    /// # Errors
    ///
    /// The index's error. A kept key that is not a register key is left
    /// out with a warning.
    pub fn load(index: &Index) -> Result<Option<Self>, IndexError> {
        let reader = index.read()?;
        let Some(generation) = reader.control_generation()? else {
            return Ok(None);
        };
        let synced_at = reader.control_synced_at()?.unwrap_or_default();
        let mut registers = BTreeMap::new();
        for (key, entry) in reader.control_entries()? {
            match RegisterKey::new(key.as_str()) {
                Ok(key) => {
                    let value = Versioned {
                        value: Bytes::from(entry.value),
                        version: Version::new(entry.version),
                    };
                    registers.insert(key, value);
                }
                Err(error) => tracing::warn!(%key, %error, "ignoring a kept register"),
            }
        }
        Ok(Some(Self {
            generation,
            synced_at,
            registers,
        }))
    }

    /// Replaces the copy `index` keeps with this one, durably.
    ///
    /// # Errors
    ///
    /// The index's error; the kept copy is then unchanged.
    pub fn save(&self, index: &Index) -> Result<(), IndexError> {
        index.update_control(|control| {
            control.clear()?;
            for (key, value) in &self.registers {
                let entry = ControlEntry {
                    generation: self.generation,
                    version: value.version.as_str().to_owned(),
                    value: value.value.to_vec(),
                };
                control.put(key.as_str(), &entry)?;
            }
            control.set_synced_at(self.synced_at)?;
            control.set_generation(self.generation)
        })
    }
}

/// Runs `request` until it succeeds, fails for good, or `policy` is
/// exhausted, backing off between attempts.
async fn retrying<T, F, Fut>(policy: &RetryPolicy, mut request: F) -> Result<T, ControlError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, ControlError>>,
{
    let mut backoff = policy.initial_backoff;
    let mut attempts = 1;
    loop {
        match request().await {
            Err(error) if error.is_retryable() && attempts < policy.max_attempts => {
                tokio::time::sleep(backoff).await;
                backoff = backoff.saturating_mul(2).min(policy.max_backoff);
                attempts += 1;
            }
            result => return result,
        }
    }
}

/// The control store the gateway and STS use: the node's store, or its
/// local copy until the store has answered once.
///
/// Clones share the state.
#[derive(Debug, Clone)]
pub struct NodeStore<C> {
    store: C,
    /// The copy reads are served from while the store has not answered.
    kept: Arc<RwLock<Option<Arc<ControlCopy>>>>,
}

impl<C: ControlStore> NodeStore<C> {
    /// `store`, used directly.
    pub fn live(store: C) -> Self {
        Self {
            store,
            kept: Arc::default(),
        }
    }

    /// `store`, read through `copy` until [`NodeStore::go_live`].
    pub fn from_copy(store: C, copy: ControlCopy) -> Self {
        Self {
            store,
            kept: Arc::new(RwLock::new(Some(Arc::new(copy)))),
        }
    }

    /// Uses the store directly from now on.
    pub fn go_live(&self) {
        *self.kept.write().unwrap_or_else(PoisonError::into_inner) = None;
    }

    /// Whether requests go to the store rather than the local copy.
    pub fn is_live(&self) -> bool {
        self.kept().is_none()
    }

    /// The store itself.
    pub fn store(&self) -> &C {
        &self.store
    }

    fn kept(&self) -> Option<Arc<ControlCopy>> {
        self.kept
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn unavailable() -> ControlError {
        ControlError::Unavailable(
            "the control store has not answered since the node started; the node serves its \
             local copy"
                .to_owned(),
        )
    }
}

impl<C: ControlStore> ControlStore for NodeStore<C> {
    type Changes = C::Changes;

    async fn get(&self, key: &RegisterKey) -> Result<Option<Versioned>, ControlError> {
        let Some(copy) = self.kept() else {
            return self.store.get(key).await;
        };
        if !copied().iter().any(|prefix| key.starts_with(prefix)) {
            return Err(Self::unavailable());
        }
        Ok(copy.registers.get(key).cloned())
    }

    async fn put_if(
        &self,
        key: &RegisterKey,
        expected: Expected,
        value: Bytes,
    ) -> Result<PutOutcome, ControlError> {
        if self.kept().is_some() {
            return Err(Self::unavailable());
        }
        self.store.put_if(key, expected, value).await
    }

    async fn delete_if(
        &self,
        key: &RegisterKey,
        expected: &Version,
    ) -> Result<DeleteOutcome, ControlError> {
        if self.kept().is_some() {
            return Err(Self::unavailable());
        }
        self.store.delete_if(key, expected).await
    }

    async fn list(&self, prefix: &KeyPrefix) -> Result<Vec<(RegisterKey, Version)>, ControlError> {
        let Some(copy) = self.kept() else {
            return self.store.list(prefix).await;
        };
        if !copied()
            .iter()
            .any(|copied| prefix.as_str().starts_with(copied.as_str()))
        {
            return Err(Self::unavailable());
        }
        Ok(copy
            .registers
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.version.clone()))
            .collect())
    }

    async fn changes(&self, after: Generation) -> Result<Self::Changes, ControlError> {
        self.store.changes(after).await
    }
}

#[cfg(test)]
mod tests {
    use skys3_control::{MemoryControlStore, ProposalIds, bootstrap};
    use skys3_index::IndexConfig;
    use skys3_io::SimDisk;

    use super::*;

    fn key(text: &str) -> RegisterKey {
        RegisterKey::new(text).unwrap()
    }

    async fn store_with_registers() -> (MemoryControlStore, ClusterId) {
        let store = MemoryControlStore::new();
        let cluster = ClusterId::new("c").unwrap();
        let mut ids = ProposalIds::seeded(1);
        bootstrap(&store, &cluster, ids.next_id(), &RetryPolicy::default())
            .await
            .unwrap();
        for name in ["buckets/a.json", "identity/roles/r.json", "nodes/n.json"] {
            let outcome = store
                .put_if(&key(name), Expected::Absent, Bytes::from_static(b"{}"))
                .await
                .unwrap();
            assert!(matches!(outcome, PutOutcome::Written(_)));
        }
        (store, cluster)
    }

    #[tokio::test]
    async fn the_copy_round_trips_through_the_index() {
        let (store, cluster) = store_with_registers().await;
        let copy = ControlCopy::fetch(
            &store,
            &cluster,
            &RetryPolicy::default(),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        let keys: Vec<_> = copy.registers.keys().map(RegisterKey::as_str).collect();
        assert_eq!(keys, ["buckets/a.json", "identity/roles/r.json"]);
        assert_eq!(copy.generation, Generation::new(1));

        let disk = SimDisk::new(1);
        let index = Index::open_sim(&disk.mount(), "index.redb", &IndexConfig::default()).unwrap();
        assert_eq!(ControlCopy::load(&index).unwrap(), None);
        copy.save(&index).unwrap();
        assert_eq!(ControlCopy::load(&index).unwrap(), Some(copy));
    }

    #[tokio::test]
    async fn the_kept_copy_serves_reads_until_the_store_answers() {
        let (store, cluster) = store_with_registers().await;
        let retry = RetryPolicy::default();
        let copy = ControlCopy::fetch(&store, &cluster, &retry, Duration::ZERO)
            .await
            .unwrap();
        // A register written after the copy is not seen through it.
        let late = key("buckets/late.json");
        let _ = store
            .put_if(&late, Expected::Absent, Bytes::from_static(b"{}"))
            .await
            .unwrap();
        let node = NodeStore::from_copy(store.clone(), copy);
        assert!(!node.is_live());
        assert!(node.get(&late).await.unwrap().is_none());
        assert!(node.get(&key("buckets/a.json")).await.unwrap().is_some());
        let listed = node.list(&KeyPrefix::buckets()).await.unwrap();
        assert_eq!(listed.len(), 1);
        for refused in [
            node.get(&key("cluster.json")).await.map(drop),
            node.list(&KeyPrefix::nodes()).await.map(drop),
            node.put_if(&late, Expected::Absent, Bytes::new())
                .await
                .map(drop),
            node.delete_if(&late, &Version::new("v")).await.map(drop),
        ] {
            assert!(matches!(refused, Err(ControlError::Unavailable(_))));
        }

        node.go_live();
        assert!(node.is_live());
        assert!(node.get(&late).await.unwrap().is_some());
        assert_eq!(node.list(&KeyPrefix::buckets()).await.unwrap().len(), 2);
        let version = node.get(&late).await.unwrap().unwrap().version;
        assert_eq!(
            node.delete_if(&late, &version).await.unwrap(),
            DeleteOutcome::Deleted
        );
        assert!(
            node.put_if(&late, Expected::Absent, Bytes::from_static(b"{}"))
                .await
                .is_ok()
        );
        assert!(node.changes(Generation::ZERO).await.is_ok());
        assert_eq!(
            node.store().list(&KeyPrefix::root()).await.unwrap().len(),
            5
        );
    }

    #[tokio::test]
    async fn retries_end_on_success_or_exhaustion() {
        let policy = RetryPolicy {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
        };
        let mut calls = 0;
        let result: Result<(), _> = retrying(&policy, || {
            calls += 1;
            async { Err(ControlError::Unavailable("down".to_owned())) }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(calls, 3);
        let mut calls = 0;
        let result = retrying(&policy, || {
            calls += 1;
            let done = calls == 2;
            async move {
                if done {
                    Ok(7)
                } else {
                    Err(ControlError::Indeterminate("lost".to_owned()))
                }
            }
        })
        .await;
        assert_eq!(result.unwrap(), 7);
    }
}
