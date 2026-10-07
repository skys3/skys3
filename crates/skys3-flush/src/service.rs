//! The flushers of every `write_back` bucket, and of every `local` bucket
//! with a backup target, whose shards are open on a node, each with its
//! target's capability probe.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use skys3_config::{BucketsConfig, ConflictPolicy};
use skys3_io::{Disk, SystemWallClock, WallClock};
use skys3_log::ShardRef;
use skys3_remote::ObjectStore;
use skys3_remote::probe::{ConditionalOperation, ConditionalProbe, ConditionalWrites};
use skys3_shard::{Shard, ShardSet};
use skys3_types::{BucketDocument, BucketId, BucketMode, ClusterId, RemoteTarget, ShardId};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::budget::DirtyBudget;
use crate::conflict::{Unresolved, effective_policy, may_discard};
use crate::fill::Filler;
use crate::import::{self, ImportJob, ImportState, ImportStatus, RemoteReader, Stop};
use crate::metrics::{Counters, FlushMetrics, Gauges};
use crate::shard::{Ready, ShardFlusher, ShardStatus};
use crate::target::{FlushSettings, Target};

/// Builds the store of a bucket's remote target.
pub type Connect<S> = Box<dyn Fn(&RemoteTarget) -> S + Send + Sync>;

/// Where a target's capability probe is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbeStatus {
    /// Not finished: flushing waits for it. `error` is why the last run
    /// failed, if one did; the probe runs again after a backoff.
    Running {
        /// The last run's error.
        error: Option<String>,
    },
    /// Finished. The operations that are not protected are sent
    /// unconditionally (§7.2).
    Done {
        /// The operations whose preconditions the target does not honor.
        unprotected: Vec<ConditionalOperation>,
    },
}

/// A bucket's flush state on this node.
#[derive(Debug, Clone, PartialEq)]
pub struct BucketStatus {
    /// The bucket's name.
    pub name: String,
    /// Whether the target is the backup target of a `local` bucket (§8.9),
    /// rather than a `write_back` bucket's system of record.
    pub backup: bool,
    /// What a flush that finds an out-of-band write does (§7.2).
    pub conflict_policy: ConflictPolicy,
    /// The target's capability probe.
    pub probe: ProbeStatus,
    /// Each open shard's flusher.
    pub shards: Vec<(ShardId, ShardStatus)>,
    /// Remote multipart uploads that flushes left open and that wait to be
    /// aborted ([`Target::orphaned_uploads`]).
    pub orphaned_uploads: u64,
    /// This node's share of the bucket's dirty-data budget
    /// ([`DirtyBudget`]).
    pub dirty_budget: u64,
    /// The bucket's namespace import (§9.1); a backup target has none.
    pub import: Option<ImportStatus>,
}

impl BucketStatus {
    /// The bucket's gauges at wall-clock time `now`.
    #[must_use]
    pub fn gauges(&self, now: Duration) -> Gauges {
        let age =
            |since: Option<Duration>| since.map_or(0.0, |s| now.saturating_sub(s).as_secs_f64());
        let statuses = self.shards.iter().map(|(_, status)| status);
        Gauges {
            dirty_bytes: statuses.clone().map(|s| s.dirty_bytes).sum(),
            oldest_dirty_age: age(statuses.clone().filter_map(|s| s.oldest_dirty).min()),
            flush_lag: age(statuses.clone().filter_map(|s| s.oldest_pending).min()),
            conflicted_keys: statuses.map(|s| s.conflicts.len() as u64).sum(),
            orphaned_uploads: self.orphaned_uploads,
            dirty_budget: self.dirty_budget,
        }
    }
}

/// The flushers of a node (§7.1).
///
/// [`FlushService::reconcile`] makes them follow the node's buckets and
/// open shards: for each `write_back` bucket it connects to the target,
/// probes which preconditions the target honors (§7.2), retrying until the
/// probe succeeds, and runs a [`ShardFlusher`] for each of the bucket's
/// shards open on the node. A flusher tracks its shard's dirty keys at
/// once, so their bytes count against the [`DirtyBudget`] even while the
/// target is unreachable, and starts flushing when the probe is done.
/// Flushers of buckets and shards that are gone are stopped. Each bucket's
/// [`Filler`] reads evicted versions from the same target (§9.2); it needs
/// no probe.
///
/// Each bucket also runs its namespace import (§9.1) from the moment it is
/// followed: on attach, or resumed from its checkpoint after a restart.
/// Its flushers keep tombstones and resolve writes of unknown remote state
/// by its progress, and [`FlushService::remote`] reads the remote for the
/// keys it has not reached.
///
/// **Backup targets** (§8.9). A `local` bucket whose settings
/// ([`FlushService::with_buckets`]) name a `backup_target` is flushed to
/// it by the same flushers, with the same probe, ordering, conditional
/// writes, write identity, and streaming. What differs comes from the
/// target not being the bucket's system of record. Nothing is imported
/// from it, so every key counts as imported, and a key without a remote
/// ETag is sent with `If-None-Match: *`. Nothing is read from it, so the
/// bucket has no [`Filler`] and no [`RemoteReader`]. Its flushers count
/// no dirty bytes against the [`DirtyBudget`], since the cluster holds
/// them durably and a backup outage must not stop the bucket's writes;
/// the flush metrics measure how far the backup lags. And nothing is
/// evicted: the node's clean cache is never told a `local` bucket's
/// `clean_copies`, so it keeps every payload that a `FLUSHED` made clean
/// (`skys3_shard::CleanCache::evicts`).
///
/// **Conflicts** (§7.2). A bucket's flushers apply its
/// `flush_conflict_policy` from its settings, `hold` without them: a key
/// held in conflict waits for [`FlushService::resolve`], and `overwrite`
/// and `discard_local` resolve conflicts as they are found. Only a
/// `write_back` bucket whose own table names `discard_local` discards; a
/// backup target holds instead.
pub struct FlushService<S, D> {
    cluster: ClusterId,
    settings: FlushSettings,
    connect: Connect<S>,
    wall: Arc<dyn WallClock>,
    metrics: FlushMetrics,
    budget: Arc<DirtyBudget>,
    /// Each bucket's settings, if the service has them.
    bucket_settings: Option<BucketsConfig>,
    /// The nonce of each capability probe's next run, if they are given
    /// rather than fresh ([`FlushService::with_probe_nonces`]).
    probe_nonces: Option<Arc<AtomicU64>>,
    buckets: Mutex<BTreeMap<BucketId, BucketFlusher<S>>>,
    /// The import tasks of buckets no longer followed, told to stop and
    /// not yet waited for: the bucket's next import waits for them first.
    stopping: Mutex<BTreeMap<BucketId, Vec<JoinHandle<()>>>>,
    _disk: std::marker::PhantomData<fn() -> D>,
}

impl<S, D> fmt::Debug for FlushService<S, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlushService")
            .field("cluster", &self.cluster)
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

/// The flushers of one bucket.
struct BucketFlusher<S> {
    name: String,
    /// The conflict policy its flushers apply.
    conflict_policy: ConflictPolicy,
    /// Whether an operator may resolve its conflicts with `discard_local`.
    may_discard: bool,
    probe: Arc<Mutex<ProbeStatus>>,
    probe_task: JoinHandle<()>,
    /// The target, once the probe is done.
    target: Ready<S>,
    shards: BTreeMap<ShardId, ShardFlusher>,
    /// What only the target of a `write_back` bucket, its system of
    /// record, has; `None` for a backup target.
    record: Option<SystemOfRecord<S>>,
}

/// What only a `write_back` bucket's target has: the bucket's namespace
/// import (§9.1) and reads of the target (§9.2).
struct SystemOfRecord<S> {
    /// What client reads of the remote use.
    remote: RemoteReader<S>,
    import: Arc<ImportState>,
    /// Stops the import when raised or dropped.
    stop_import: watch::Sender<bool>,
    /// The import's task, until it is waited for or handed on.
    import_task: Option<JoinHandle<()>>,
    filler: Filler<S>,
}

impl<S> Drop for BucketFlusher<S> {
    fn drop(&mut self) {
        self.probe_task.abort();
        // Not aborted: the import stops between steps, and its task ends
        // once no checkpoint write of it is pending (`import::Stop`).
        if let Some(record) = &self.record {
            record.stop_import.send_replace(true);
        }
    }
}

/// What a bucket's flushers send its writes to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    /// The target of a `write_back` bucket, its system of record.
    WriteBack,
    /// The backup target of a `local` bucket (§8.9).
    Backup,
}

impl<S: ObjectStore, D: Disk> FlushService<S, D> {
    /// A service for the cluster `cluster`, whose targets' stores `connect`
    /// builds, with its metrics in `metrics`.
    pub fn new(
        cluster: ClusterId,
        settings: FlushSettings,
        connect: Connect<S>,
        metrics: FlushMetrics,
    ) -> Self {
        Self {
            cluster,
            settings,
            connect,
            wall: Arc::new(SystemWallClock),
            metrics,
            budget: Arc::new(DirtyBudget::unlimited()),
            bucket_settings: None,
            probe_nonces: None,
            buckets: Mutex::default(),
            stopping: Mutex::default(),
            _disk: std::marker::PhantomData,
        }
    }

    /// Sets the clock that dirty ages are measured on.
    #[must_use]
    pub fn with_wall_clock(mut self, wall: Arc<dyn WallClock>) -> Self {
        self.wall = wall;
        self
    }

    /// Gives the targets' capability probe runs the nonces `first`,
    /// `first + 1`, and so on, in the order they start, instead of fresh
    /// ones: a simulation derives `first` from its seed, so that its probe
    /// keys, which an import's split points see, replay too.
    #[must_use]
    pub fn with_probe_nonces(mut self, first: u64) -> Self {
        self.probe_nonces = Some(Arc::new(AtomicU64::new(first)));
        self
    }

    /// Counts the flushers' dirty bytes against `budget`.
    #[must_use]
    pub fn with_budget(mut self, budget: Arc<DirtyBudget>) -> Self {
        self.budget = budget;
        self
    }

    /// Takes each bucket's `import_parallel_streams`, `backup_target`, and
    /// `flush_conflict_policy` from `buckets`; otherwise every import has
    /// `settings.import_streams` streams, no `local` bucket is flushed, and
    /// conflicts are held.
    #[must_use]
    pub fn with_buckets(mut self, buckets: BucketsConfig) -> Self {
        self.bucket_settings = Some(buckets);
        self
    }

    /// The budget the flushers' dirty bytes count against.
    #[must_use]
    pub fn budget(&self) -> &Arc<DirtyBudget> {
        &self.budget
    }

    /// The target `bucket`'s writes are flushed to, if any: a `write_back`
    /// bucket's own, or a `local` bucket's backup target (§8.9). Index
    /// snapshots (§8.9, plan M4-11) go to a `local` bucket's backup target
    /// by default.
    #[must_use]
    pub fn target_of(&self, bucket: &BucketDocument) -> Option<RemoteTarget> {
        self.followed(bucket).map(|(target, _)| target)
    }

    /// The target `bucket`'s writes are flushed to, and its role.
    fn followed(&self, bucket: &BucketDocument) -> Option<(RemoteTarget, Role)> {
        match (bucket.mode, &bucket.target) {
            (BucketMode::WriteBack, Some(target)) => Some((target.clone(), Role::WriteBack)),
            (BucketMode::Local, _) => {
                let settings = self.bucket_settings.as_ref()?.get(&bucket.name);
                Some((settings.backup_target.clone()?, Role::Backup))
            }
            _ => None,
        }
    }

    /// Starts and stops flushers so that every `write_back` bucket in
    /// `buckets`, and every `local` one with a backup target, with shards
    /// open in `set` as their primary is flushed, and nothing else.
    pub async fn reconcile(&self, buckets: &[BucketDocument], set: &ShardSet<D>) {
        let mut open = Vec::new();
        for bucket in buckets {
            let Some((target, role)) = self.followed(bucket) else {
                continue;
            };
            let mut shards = Vec::new();
            for shard in bucket.shards.shards() {
                let shard_ref = ShardRef::new(bucket.bucket_id.clone(), shard);
                // Only a shard's primary flushes it (§7.1): a member or a
                // learner refuses the `FLUSHED` records its primary does
                // not send.
                if let Some(shard) = set.get(&shard_ref).await
                    && !shard.is_stopped()
                    && !shard.role().follows()
                {
                    shards.push(shard);
                }
            }
            open.push((bucket, target, role, shards));
        }
        // What waits for a backup is durable in the cluster: it counts
        // against no budget.
        self.budget.plan(
            open.iter()
                .filter(|(_, _, role, _)| *role == Role::WriteBack)
                .map(|(bucket, _, _, shards)| {
                    (*bucket, u32::try_from(shards.len()).unwrap_or(u32::MAX))
                }),
        );
        let mut flushers = self.lock();
        let wanted: BTreeSet<&BucketId> =
            open.iter().map(|(bucket, ..)| &bucket.bucket_id).collect();
        flushers.retain(|id, flusher| {
            let keep = wanted.contains(id);
            if !keep {
                self.metrics.remove(&flusher.name);
                if let Some(record) = &mut flusher.record {
                    record.stop_import.send_replace(true);
                    if let Some(task) = record.import_task.take() {
                        let mut stopping = lock(&self.stopping);
                        let tasks = stopping.entry(id.clone()).or_default();
                        tasks.retain(|task| !task.is_finished());
                        tasks.push(task);
                    }
                }
            }
            keep
        });
        for (bucket, target, role, shards) in open {
            let flusher = flushers
                .entry(bucket.bucket_id.clone())
                .or_insert_with(|| self.start(bucket, &target, role, set));
            let charged = role == Role::WriteBack;
            self.follow(&bucket.bucket_id, flusher, shards, charged);
        }
    }

    /// Starts a bucket's flushers with its target's probe, which makes the
    /// target ready once it succeeds, and, for a `write_back` bucket, its
    /// namespace import.
    fn start(
        &self,
        bucket: &BucketDocument,
        target: &RemoteTarget,
        role: Role,
        set: &ShardSet<D>,
    ) -> BucketFlusher<S> {
        let name = bucket.name.as_str().to_owned();
        let import = (role == Role::WriteBack).then(|| Arc::new(ImportState::default()));
        let configured = self
            .bucket_settings
            .as_ref()
            .map_or(ConflictPolicy::Hold, |buckets| {
                buckets.get(&bucket.name).flush_conflict_policy
            });
        let backup = role == Role::Backup;
        let conflict_policy = effective_policy(configured, backup);
        if backup && configured == ConflictPolicy::DiscardLocal {
            tracing::warn!(bucket = name,
                "a backup target never discards local writes; its conflicts are held");
        }
        let parts = TargetParts {
            store: Arc::new((self.connect)(target)),
            prefix: target.prefix.clone().unwrap_or_default(),
            cluster: self.cluster.clone(),
            settings: self.settings.clone(),
            wall: Arc::clone(&self.wall),
            counters: self.metrics.counters(&name),
            import: import.clone(),
            conflict_policy,
        };
        let record = import.map(|import| self.system_of_record(bucket, &parts, import, set));
        let probe = Arc::new(Mutex::new(ProbeStatus::Running { error: None }));
        let (ready, target) = watch::channel(None);
        let nonces = self.probe_nonces.clone();
        let probe_task = tokio::spawn(run_probe(parts, nonces, Arc::clone(&probe), ready));
        BucketFlusher {
            name,
            conflict_policy,
            may_discard: may_discard(configured, backup),
            probe,
            probe_task,
            target,
            shards: BTreeMap::new(),
            record,
        }
    }

    /// Starts the namespace import of `bucket`, a `write_back` bucket whose
    /// target `parts` holds, and makes the reads of the target.
    fn system_of_record(
        &self,
        bucket: &BucketDocument,
        parts: &TargetParts<S>,
        import: Arc<ImportState>,
        set: &ShardSet<D>,
    ) -> SystemOfRecord<S> {
        let (store, prefix) = (Arc::clone(&parts.store), parts.prefix.clone());
        let filler = Filler::new(
            Arc::clone(&store),
            prefix.clone(),
            parts.settings.clone(),
            parts.counters.clone(),
            Arc::clone(&parts.wall),
        );
        let streams =
            self.bucket_settings
                .as_ref()
                .map_or(self.settings.import_streams, |buckets| {
                    usize::try_from(buckets.get(&bucket.name).import_parallel_streams)
                        .unwrap_or(usize::MAX)
                });
        let previous = lock(&self.stopping)
            .remove(&bucket.bucket_id)
            .unwrap_or_default();
        let (stop_import, stop) = Stop::new();
        let import_task = tokio::spawn(import::run(ImportJob {
            store: Arc::clone(&store),
            prefix: prefix.clone(),
            bucket: bucket.clone(),
            set: set.clone(),
            settings: self.settings.clone(),
            streams,
            wall: Arc::clone(&self.wall),
            state: Arc::clone(&import),
            stop,
            previous,
        }));
        SystemOfRecord {
            remote: RemoteReader::new(store, prefix, Arc::clone(&import), Arc::clone(&self.wall)),
            import,
            stop_import,
            import_task: Some(import_task),
            filler,
        }
    }

    /// Runs a flusher for each of `shards`, with their dirty bytes counted
    /// against the budget if `charged`, and stops the bucket's other
    /// flushers.
    fn follow(
        &self,
        bucket: &BucketId,
        flusher: &mut BucketFlusher<S>,
        shards: Vec<Shard<D>>,
        charged: bool,
    ) {
        let open: BTreeSet<ShardId> = shards.iter().map(|shard| shard.shard().shard).collect();
        flusher
            .shards
            .retain(|id, shard| open.contains(id) && !shard.is_stopped());
        for shard in shards {
            let id = shard.shard().shard;
            flusher.shards.entry(id).or_insert_with(|| {
                let charge = charged.then(|| self.budget.charge(bucket));
                let (ready, wall) = (flusher.target.clone(), Arc::clone(&self.wall));
                ShardFlusher::start(shard, ready, wall, charge)
            });
        }
    }

    /// The flush state of `bucket`, if it is flushed here.
    #[must_use]
    pub fn status(&self, bucket: &BucketId) -> Option<BucketStatus> {
        let flushers = self.lock();
        let flusher = flushers.get(bucket)?;
        Some(BucketStatus {
            name: flusher.name.clone(),
            backup: flusher.record.is_none(),
            conflict_policy: flusher.conflict_policy,
            probe: lock(&flusher.probe).clone(),
            shards: flusher
                .shards
                .iter()
                .map(|(id, shard)| (*id, shard.status()))
                .collect(),
            orphaned_uploads: flusher
                .target
                .borrow()
                .as_ref()
                .map_or(0, |target| target.orphaned_uploads() as u64),
            dirty_budget: self
                .budget
                .usage(bucket)
                .map_or(u64::MAX, |usage| usage.share),
            import: flusher.record.as_ref().map(|record| record.import.status()),
        })
    }

    /// Resolves the conflict that one of `bucket`'s flushers on this node
    /// holds `key` in, under `policy`, as an operator asks through the
    /// admin API (§7.2): the key returns to dirty, and its next flush
    /// retries the conditional request (`hold`), overwrites the remote's
    /// write (`overwrite`), or adopts it and drops the local version
    /// (`discard_local`). See [`ShardFlusher::resolve`].
    ///
    /// # Errors
    ///
    /// [`Unresolved::NotFlushed`] if the bucket is not flushed here,
    /// [`Unresolved::NotOptedIn`] for `discard_local` on a bucket whose
    /// own table does not choose it, or a backup target, and
    /// [`Unresolved::NotHeld`] if no flusher here holds the key in
    /// conflict: its shard's primary may be another node.
    pub async fn resolve(
        &self,
        bucket: &BucketId,
        key: &str,
        policy: ConflictPolicy,
    ) -> Result<(), Unresolved> {
        let resolver = {
            let flushers = self.lock();
            let flusher = flushers.get(bucket).ok_or(Unresolved::NotFlushed)?;
            if policy == ConflictPolicy::DiscardLocal && !flusher.may_discard {
                return Err(Unresolved::NotOptedIn);
            }
            flusher
                .shards
                .values()
                .find_map(|shard| shard.resolver_of(key))
                .ok_or(Unresolved::NotHeld)?
        };
        resolver.resolve(key, policy).await
    }

    /// The read-through fills of `bucket`'s target, if the bucket is a
    /// `write_back` bucket this node follows.
    #[must_use]
    pub fn filler(&self, bucket: &BucketId) -> Option<Filler<S>> {
        let flushers = self.lock();
        Some(flushers.get(bucket)?.record.as_ref()?.filler.clone())
    }

    /// The remote target of `bucket` and its import's progress, for client
    /// reads of keys the import has not reached and lazily loaded metadata
    /// (§9.1), if the bucket is a `write_back` bucket this node follows.
    #[must_use]
    pub fn remote(&self, bucket: &BucketId) -> Option<RemoteReader<S>> {
        let flushers = self.lock();
        Some(flushers.get(bucket)?.record.as_ref()?.remote.clone())
    }

    /// Sets the flush gauges of every bucket flushed here.
    pub fn refresh_metrics(&self) {
        let now = self.wall.now();
        let ids: Vec<BucketId> = self.lock().keys().cloned().collect();
        for id in ids {
            if let Some(status) = self.status(&id) {
                self.metrics.set(&status.name, status.gauges(now));
            }
        }
    }

    /// Stops every flusher and import. Flushes in flight are abandoned;
    /// their keys stay dirty and are flushed after the next start. An
    /// import resumes from its stored checkpoints.
    pub async fn shutdown(&self) {
        let flushers = std::mem::take(&mut *self.lock());
        for (_, mut flusher) in flushers {
            // The import stops between steps; once its task ends, none of
            // its checkpoint writes is pending (`import::Stop`).
            if let Some(record) = &mut flusher.record {
                record.stop_import.send_replace(true);
                if let Some(task) = record.import_task.take() {
                    let _ = task.await;
                }
            }
            for (_, shard) in std::mem::take(&mut flusher.shards) {
                shard.stop().await;
            }
            self.metrics.remove(&flusher.name);
        }
        let stopping = std::mem::take(&mut *lock(&self.stopping));
        for task in stopping.into_values().flatten() {
            let _ = task.await;
        }
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<BucketId, BucketFlusher<S>>> {
        lock(&self.buckets)
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// What a bucket's [`Target`] is built from once its probe is done.
struct TargetParts<S> {
    store: Arc<S>,
    prefix: String,
    cluster: ClusterId,
    settings: FlushSettings,
    wall: Arc<dyn WallClock>,
    counters: Counters,
    /// The bucket's import; `None` for a backup target, which imports
    /// nothing, so every key counts as imported.
    import: Option<Arc<ImportState>>,
    conflict_policy: ConflictPolicy,
}

impl<S> TargetParts<S> {
    fn build(self, writes: ConditionalWrites) -> Target<S> {
        let target = Target::new(self.store, self.prefix, writes, self.cluster, self.settings)
            .with_wall_clock(self.wall)
            .with_counters(self.counters)
            .with_conflict_policy(self.conflict_policy);
        match self.import {
            Some(import) => target.with_import(import),
            None => target,
        }
    }
}

/// Probes the target until a run succeeds, backing off between runs, and
/// then makes the target ready.
async fn run_probe<S: ObjectStore>(
    parts: TargetParts<S>,
    nonces: Option<Arc<AtomicU64>>,
    status: Arc<Mutex<ProbeStatus>>,
    ready: watch::Sender<Option<Arc<Target<S>>>>,
) {
    let mut failures = 0;
    loop {
        // A fresh nonce per run: a failed run may leave scratch keys behind,
        // which would fail the next run's `If-None-Match: *` that must hold.
        let probe = match &nonces {
            Some(next) => {
                ConditionalProbe::new(&parts.prefix, next.fetch_add(1, Ordering::Relaxed))
            }
            None => ConditionalProbe::with_fresh_nonce(&parts.prefix),
        };
        match probe.run(&*parts.store).await {
            Ok(found) => {
                let unprotected = found.unprotected();
                if !unprotected.is_empty() {
                    tracing::warn!(
                        ?unprotected,
                        "the target does not honor every precondition; \
                        these operations are sent unconditionally (§7.2)"
                    );
                }
                *lock(&status) = ProbeStatus::Done { unprotected };
                ready.send_replace(Some(Arc::new(parts.build(found))));
                return;
            }
            Err(error) => {
                failures += 1;
                tracing::warn!(%error, "the target's capability probe failed");
                *lock(&status) = ProbeStatus::Running {
                    error: Some(error.to_string()),
                };
                tokio::time::sleep(parts.settings.backoff(failures)).await;
            }
        }
    }
}
