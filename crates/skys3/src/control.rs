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
//! after that replaces the copy. A sync lists the registers and reads only
//! those whose version differs from the copy's: a version names one value
//! (§6.1), so the others are unchanged.
//!
//! The file control store is opened through [`open_file_store`], which
//! records the node and data directory that own it and refuses a store
//! that is not this node's, or that looks reset under a node that has
//! synced from it before.

use std::collections::BTreeMap;
use std::future::Future;
use std::io;
use std::path::Path;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use skys3_control::{
    ControlError, ControlStore, DeleteOutcome, Expected, FileControlStore, FileStoreConfig,
    KeyPrefix, PutOutcome, RegisterKey, RetryPolicy, Version, Versioned, bootstrap, read_cluster,
};
use skys3_index::{ControlEntry, Index, IndexError};
use skys3_io::BlockingPool;
use skys3_types::{ClusterId, Generation, NodeId, ProposalId};

use crate::datadir::{self, DataDirError, HasFormat};
use crate::node::StartError;
use crate::storage::on_pool;

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
        Self::fetch_since(store, cluster, retry, started, None).await
    }

    /// Reads a new copy of the copied registers from `store`, as
    /// [`ControlCopy::fetch`] does, but reads only the registers whose
    /// listed version differs from `previous`, an earlier copy: the value
    /// of a register at a version it had is the one `previous` holds
    /// (§6.1). Registers that are gone are left out.
    ///
    /// # Errors
    ///
    /// As [`ControlCopy::fetch`].
    pub async fn fetch_since<C: ControlStore>(
        store: &C,
        cluster: &ClusterId,
        retry: &RetryPolicy,
        started: Duration,
        previous: Option<&Self>,
    ) -> Result<Self, ControlError> {
        let generation = read_cluster(store, cluster, retry).await?.value.generation;
        let mut registers = BTreeMap::new();
        for prefix in copied() {
            for (key, version) in retrying(retry, || store.list(&prefix)).await? {
                let kept = previous
                    .and_then(|previous| previous.registers.get(&key))
                    .filter(|kept| kept.version == version);
                if let Some(kept) = kept {
                    registers.insert(key, kept.clone());
                } else if let Some(value) = retrying(retry, || store.get(&key)).await? {
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

/// The copy of control state `index` keeps, or `None` if the node never
/// synced. Index work runs on `pool`.
///
/// # Errors
///
/// [`StartError::Index`] if the index fails.
pub async fn kept_copy(
    index: &Arc<Index>,
    pool: &BlockingPool,
) -> Result<Option<ControlCopy>, StartError> {
    let index = Arc::clone(index);
    on_pool(pool, move || ControlCopy::load(&index)).await
}

/// Syncs the node's control state from `store`, which it opened, at
/// startup: bootstraps `cluster.json` with `proposal` unless the node
/// synced before (`kept` is the copy its index keeps: a store such a node
/// opens must already hold `cluster.json`), reads a fresh copy, and keeps
/// it in `index`. If the store does not answer, a node with a kept copy
/// runs from it (§6.2). `started` is the wall time at which the sync
/// started. Index work runs on `pool`.
///
/// # Errors
///
/// [`StartError::Control`] if the store does not answer and the node has
/// no kept copy, and [`StartError::Index`] if the index fails.
#[allow(clippy::too_many_arguments, reason = "the startup sync's inputs")]
pub async fn open<C: ControlStore + Clone>(
    store: C,
    kept: Option<ControlCopy>,
    cluster: &ClusterId,
    proposal: ProposalId,
    retry: &RetryPolicy,
    index: &Arc<Index>,
    pool: &BlockingPool,
    started: Duration,
) -> Result<(NodeStore<C>, ControlCopy), StartError> {
    let fetched = async {
        if kept.is_none() {
            bootstrap(&store, cluster, proposal, retry).await?;
        }
        ControlCopy::fetch_since(&store, cluster, retry, started, kept.as_ref()).await
    }
    .await;
    match (fetched, kept) {
        (Ok(copy), _) => {
            let (index, saved) = (Arc::clone(index), copy.clone());
            on_pool(pool, move || saved.save(&index)).await?;
            Ok((NodeStore::live(store), copy))
        }
        (Err(error), Some(kept)) => {
            tracing::warn!(
                %error,
                generation = %kept.generation,
                "the control store does not answer; running from the local copy"
            );
            Ok((NodeStore::from_copy(Some(store), kept.clone()), kept))
        }
        (Err(error), None) => Err(error.into()),
    }
}

/// Reads a fresh copy of the control state from `store` and keeps it in
/// `index`, as each sync after startup does, reading only the registers
/// that changed since the copy `index` keeps ([`ControlCopy::fetch_since`]).
/// `started` is the wall time at which the sync started.
///
/// # Errors
///
/// [`StartError::Control`] if the store fails, and [`StartError::Index`]
/// if the index does; the kept copy is then unchanged.
pub async fn refresh<C: ControlStore>(
    store: &C,
    cluster: &ClusterId,
    retry: &RetryPolicy,
    index: &Arc<Index>,
    pool: &BlockingPool,
    started: Duration,
) -> Result<ControlCopy, StartError> {
    let previous = kept_copy(index, pool).await?;
    let copy = ControlCopy::fetch_since(store, cluster, retry, started, previous.as_ref()).await?;
    let (index, kept) = (Arc::clone(index), copy.clone());
    on_pool(pool, move || kept.save(&index)).await?;
    Ok(copy)
}

/// Whether the node should sync its copy, whose last sync started at
/// `synced_at` (time since the Unix epoch), at `now`, although the
/// generation did not move: once half of `max_staleness` has passed since
/// that start, or if the clock stepped back before it. STS measures the
/// identity copy's age from the start of its last sync (§6.2), so a node
/// whose store answers refreshes the copy well before it goes stale, even
/// while nothing changes; with only the changed registers read
/// ([`ControlCopy::fetch_since`]), such a sync costs the listings.
#[must_use]
pub fn sync_due(synced_at: Option<Duration>, now: Duration, max_staleness: Duration) -> bool {
    synced_at
        .and_then(|at| now.checked_sub(at))
        .is_none_or(|age| age >= max_staleness / 2)
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

/// The file control store's record of its owner, `.owner.json` in its
/// directory, beside the store's own lock file.
pub const OWNER_FILE: &str = ".owner.json";

/// The node and data directory a file control store belongs to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreOwner {
    format: u32,
    cluster_id: ClusterId,
    node_id: NodeId,
    instance_id: String,
}

impl StoreOwner {
    /// The owner record of `node` in `cluster`, whose data directory has
    /// `instance_id`.
    #[must_use]
    pub fn new(cluster_id: ClusterId, node_id: NodeId, instance_id: String) -> Self {
        Self {
            format: datadir::FORMAT,
            cluster_id,
            node_id,
            instance_id,
        }
    }
}

impl HasFormat for StoreOwner {
    fn format(&self) -> u32 {
        self.format
    }
}

/// Why [`open_file_store`] did not give the node a store.
#[derive(Debug, thiserror::Error)]
pub enum OpenStoreError {
    /// The store could not be opened or read; it may answer later.
    #[error(transparent)]
    Unavailable(#[from] ControlError),
    /// The store is not the one this node synced from: its directory is
    /// missing or names no owner, or it has no `cluster.json`. A volume
    /// that is not mounted looks like this. The node never replaces such a
    /// store by itself (§6.2): rebuilding one is an operator's decision.
    #[error("{0}")]
    Reset(String),
    /// The store belongs to another node or data directory.
    #[error("{0}")]
    Foreign(String),
}

impl From<DataDirError> for OpenStoreError {
    fn from(error: DataDirError) -> Self {
        match error {
            DataDirError::Io { path, source } => Self::Unavailable(ControlError::Io(
                io::Error::new(source.kind(), format!("{}: {source}", path.display())),
            )),
            other => Self::Foreign(other.to_string()),
        }
    }
}

/// A file control store [`open_file_store`] opened.
#[derive(Debug)]
pub struct OpenedStore {
    /// The store.
    pub store: FileControlStore,
    /// Whether this open claimed an empty store for the node. Its
    /// registers are then new, so they say nothing about which buckets
    /// exist.
    pub claimed: bool,
}

/// Opens the file control store in `root` for `owner`, running its I/O on
/// `pool`.
///
/// A store that names `owner` is opened. One that names another node or
/// data directory is refused. A node that has synced from a store before
/// (`initialized`) requires its directory, its owner record, and
/// `cluster.json` to exist; otherwise the store is reported as reset and
/// left alone. Only a node that never synced claims a store, and only an
/// empty one.
///
/// # Errors
///
/// [`OpenStoreError`].
pub async fn open_file_store(
    root: &Path,
    owner: &StoreOwner,
    initialized: bool,
    pool: &BlockingPool,
) -> Result<OpenedStore, OpenStoreError> {
    let exists = {
        let root = root.to_owned();
        on(pool, move || match std::fs::symlink_metadata(&root) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(ControlError::Io(error).into()),
        })
        .await?
    };
    if initialized && !exists {
        return Err(OpenStoreError::Reset(format!(
            "the control store directory {} is missing; this node has synced from it before, \
             so it is not created again (is its volume mounted?)",
            root.display()
        )));
    }
    let config = FileStoreConfig {
        root: root.to_owned(),
        node: owner.node_id.clone(),
    };
    let store =
        FileControlStore::open(config, pool.clone())
            .await
            .map_err(|error| match error {
                ControlError::SecondNode { .. } => OpenStoreError::Foreign(error.to_string()),
                other => OpenStoreError::Unavailable(other),
            })?;
    let recorded = {
        let path = root.join(OWNER_FILE);
        on(pool, move || Ok(datadir::read_json::<StoreOwner>(&path)?)).await?
    };
    let claimed = match recorded {
        Some(recorded) if recorded == *owner => false,
        Some(recorded) => {
            return Err(OpenStoreError::Foreign(format!(
                "the control store in {} belongs to node {} (data directory instance {}) in \
                 cluster {}, not to node {} (instance {}); the file backend serves one node",
                root.display(),
                recorded.node_id,
                recorded.instance_id,
                recorded.cluster_id,
                owner.node_id,
                owner.instance_id
            )));
        }
        None if initialized => {
            return Err(OpenStoreError::Reset(format!(
                "the control store in {} names no owner, but this node has synced from its \
                 store before; it is not claimed again",
                root.display()
            )));
        }
        None => {
            if !store.list(&KeyPrefix::root()).await?.is_empty() {
                return Err(OpenStoreError::Foreign(format!(
                    "the control store in {} holds registers but names no owner; a node \
                     claims only an empty store",
                    root.display()
                )));
            }
            let (root, owner) = (root.to_owned(), owner.clone());
            on(pool, move || {
                Ok(datadir::write_json(&root, OWNER_FILE, &owner)?)
            })
            .await?;
            true
        }
    };
    if initialized && store.get(&RegisterKey::cluster()).await?.is_none() {
        return Err(OpenStoreError::Reset(format!(
            "the control store in {} has no cluster.json, but this node has synced from it \
             before; it is not bootstrapped again",
            root.display()
        )));
    }
    Ok(OpenedStore { store, claimed })
}

/// Runs `job` on `pool`.
async fn on<T: Send + 'static>(
    pool: &BlockingPool,
    job: impl FnOnce() -> Result<T, OpenStoreError> + Send + 'static,
) -> Result<T, OpenStoreError> {
    pool.run(job)
        .await
        .map_err(|error| OpenStoreError::Unavailable(ControlError::Io(io::Error::from(error))))?
}

/// The control store the gateway and STS use: the node's store, or its
/// local copy until the store has answered once. A node that could not
/// open its store at startup has none until [`NodeStore::attach`].
///
/// Clones share the state.
#[derive(Debug, Clone)]
pub struct NodeStore<C> {
    state: Arc<RwLock<State<C>>>,
}

#[derive(Debug)]
struct State<C> {
    store: Option<C>,
    /// The copy reads are served from while the store has not answered.
    kept: Option<Arc<ControlCopy>>,
}

/// Where a request goes.
enum Route<C> {
    Store(C),
    Copy(Arc<ControlCopy>),
}

impl<C: ControlStore + Clone> NodeStore<C> {
    /// `store`, used directly.
    pub fn live(store: C) -> Self {
        Self::with(Some(store), None)
    }

    /// `store`, if the node has one, read through `copy` until
    /// [`NodeStore::go_live`].
    pub fn from_copy(store: Option<C>, copy: ControlCopy) -> Self {
        Self::with(store, Some(Arc::new(copy)))
    }

    fn with(store: Option<C>, kept: Option<Arc<ControlCopy>>) -> Self {
        Self {
            state: Arc::new(RwLock::new(State { store, kept })),
        }
    }

    /// Gives a node that runs from its copy the store it opened since.
    pub fn attach(&self, store: C) {
        self.state
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .store = Some(store);
    }

    /// Uses the store directly from now on. A node without a store keeps
    /// its copy.
    pub fn go_live(&self) {
        let mut state = self.state.write().unwrap_or_else(PoisonError::into_inner);
        if state.store.is_some() {
            state.kept = None;
        }
    }

    /// Whether requests go to the store rather than the local copy.
    pub fn is_live(&self) -> bool {
        self.kept().is_none()
    }

    /// The store itself, if the node has one.
    pub fn store(&self) -> Option<C> {
        self.state
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .store
            .clone()
    }

    fn kept(&self) -> Option<Arc<ControlCopy>> {
        self.state
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .kept
            .clone()
    }

    fn route(&self) -> Route<C> {
        let state = self.state.read().unwrap_or_else(PoisonError::into_inner);
        match (&state.kept, &state.store) {
            (Some(copy), _) => Route::Copy(Arc::clone(copy)),
            (None, Some(store)) => Route::Store(store.clone()),
            (None, None) => unreachable!("a node goes live only with a store"),
        }
    }

    fn unavailable() -> ControlError {
        ControlError::Unavailable(
            "the control store has not answered since the node started; the node serves its \
             local copy"
                .to_owned(),
        )
    }
}

impl<C: ControlStore + Clone> ControlStore for NodeStore<C> {
    type Changes = C::Changes;

    async fn get(&self, key: &RegisterKey) -> Result<Option<Versioned>, ControlError> {
        let copy = match self.route() {
            Route::Store(store) => return store.get(key).await,
            Route::Copy(copy) => copy,
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
        match self.route() {
            Route::Store(store) => store.put_if(key, expected, value).await,
            Route::Copy(_) => Err(Self::unavailable()),
        }
    }

    async fn delete_if(
        &self,
        key: &RegisterKey,
        expected: &Version,
    ) -> Result<DeleteOutcome, ControlError> {
        match self.route() {
            Route::Store(store) => store.delete_if(key, expected).await,
            Route::Copy(_) => Err(Self::unavailable()),
        }
    }

    async fn list(&self, prefix: &KeyPrefix) -> Result<Vec<(RegisterKey, Version)>, ControlError> {
        let copy = match self.route() {
            Route::Store(store) => return store.list(prefix).await,
            Route::Copy(copy) => copy,
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
        match self.store() {
            Some(store) => store.changes(after).await,
            None => Err(Self::unavailable()),
        }
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
        let node = NodeStore::from_copy(Some(store.clone()), copy);
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
            node.store()
                .unwrap()
                .list(&KeyPrefix::root())
                .await
                .unwrap()
                .len(),
            5
        );
    }

    #[tokio::test]
    async fn a_node_without_a_store_keeps_its_copy_until_one_is_attached() {
        let (store, cluster) = store_with_registers().await;
        let copy = ControlCopy::fetch(&store, &cluster, &RetryPolicy::default(), Duration::ZERO)
            .await
            .unwrap();
        let node = NodeStore::from_copy(None, copy);
        node.go_live();
        assert!(!node.is_live());
        assert!(node.store().is_none());
        assert!(node.get(&key("buckets/a.json")).await.unwrap().is_some());
        assert!(matches!(
            node.changes(Generation::ZERO).await,
            Err(ControlError::Unavailable(_))
        ));
        node.attach(store);
        node.go_live();
        assert!(node.is_live());
        assert!(node.get(&key("nodes/n.json")).await.unwrap().is_some());
    }

    /// A store that counts the reads it serves.
    #[derive(Clone, Debug)]
    struct Counting {
        store: MemoryControlStore,
        gets: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl ControlStore for Counting {
        type Changes = <MemoryControlStore as ControlStore>::Changes;

        async fn get(&self, key: &RegisterKey) -> Result<Option<Versioned>, ControlError> {
            self.gets.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.store.get(key).await
        }

        async fn put_if(
            &self,
            key: &RegisterKey,
            expected: Expected,
            value: Bytes,
        ) -> Result<PutOutcome, ControlError> {
            self.store.put_if(key, expected, value).await
        }

        async fn delete_if(
            &self,
            key: &RegisterKey,
            expected: &Version,
        ) -> Result<DeleteOutcome, ControlError> {
            self.store.delete_if(key, expected).await
        }

        async fn list(
            &self,
            prefix: &KeyPrefix,
        ) -> Result<Vec<(RegisterKey, Version)>, ControlError> {
            self.store.list(prefix).await
        }

        async fn changes(&self, after: Generation) -> Result<Self::Changes, ControlError> {
            self.store.changes(after).await
        }
    }

    #[tokio::test]
    async fn a_sync_reads_only_the_registers_that_changed() {
        let (store, cluster) = store_with_registers().await;
        let counting = Counting {
            store: store.clone(),
            gets: Arc::default(),
        };
        let gets = || counting.gets.load(std::sync::atomic::Ordering::SeqCst);
        let retry = RetryPolicy::default();
        let disk = SimDisk::new(3);
        let index = Arc::new(
            Index::open_sim(&disk.mount(), "index.redb", &IndexConfig::default()).unwrap(),
        );
        let pool = BlockingPool::inline("index");
        let first = refresh(
            &counting,
            &cluster,
            &retry,
            &index,
            &pool,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        // cluster.json, and the two copied registers.
        assert_eq!(gets(), 3);

        // One register changes, one is added, and one is deleted.
        let changed = key("identity/roles/r.json");
        let version = first.registers[&changed].version.clone();
        let _ = store
            .put_if(
                &changed,
                Expected::Version(version),
                Bytes::from_static(b"{\"x\":1}"),
            )
            .await
            .unwrap();
        let added = key("buckets/b.json");
        let _ = store
            .put_if(&added, Expected::Absent, Bytes::from_static(b"{}"))
            .await
            .unwrap();
        let gone = key("buckets/a.json");
        let version = first.registers[&gone].version.clone();
        let _ = store.delete_if(&gone, &version).await.unwrap();
        let second = refresh(
            &counting,
            &cluster,
            &retry,
            &index,
            &pool,
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        assert_eq!(gets(), 3 + 3);
        assert_eq!(
            second,
            ControlCopy::fetch(&store, &cluster, &retry, Duration::from_secs(2))
                .await
                .unwrap()
        );
        assert_eq!(second.synced_at, Duration::from_secs(2));
        assert_eq!(ControlCopy::load(&index).unwrap(), Some(second));

        // Nothing changed: only cluster.json is read.
        refresh(
            &counting,
            &cluster,
            &retry,
            &index,
            &pool,
            Duration::from_secs(3),
        )
        .await
        .unwrap();
        assert_eq!(gets(), 3 + 3 + 1);
    }

    #[test]
    fn a_sync_is_due_once_half_the_staleness_bound_passed() {
        let bound = Duration::from_secs(100);
        let at = Some(Duration::from_secs(1000));
        assert!(!sync_due(at, Duration::from_secs(1000), bound));
        assert!(!sync_due(at, Duration::from_secs(1049), bound));
        assert!(sync_due(at, Duration::from_secs(1050), bound));
        // A clock that stepped back, and a node that never synced.
        assert!(sync_due(at, Duration::from_secs(999), bound));
        assert!(sync_due(None, Duration::from_secs(1000), bound));
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
