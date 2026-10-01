//! The flushers of every `write_back` bucket whose shards are open on a
//! node, each with its target's capability probe.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use skys3_io::{Disk, SystemWallClock, WallClock};
use skys3_log::ShardRef;
use skys3_remote::ObjectStore;
use skys3_remote::probe::{ConditionalOperation, ConditionalProbe, ConditionalWrites};
use skys3_shard::{Shard, ShardSet};
use skys3_types::{BucketDocument, BucketId, BucketMode, ClusterId, RemoteTarget, ShardId};
use tokio::task::JoinHandle;

use crate::metrics::{FlushMetrics, Gauges};
use crate::shard::{ShardFlusher, ShardStatus};
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
    /// The target's capability probe.
    pub probe: ProbeStatus,
    /// Each open shard's flusher.
    pub shards: Vec<(ShardId, ShardStatus)>,
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
        }
    }
}

/// The flushers of a node (§7.1).
///
/// [`FlushService::reconcile`] makes them follow the node's buckets and
/// open shards: for each `write_back` bucket it connects to the target,
/// probes which preconditions the target honors (§7.2), retrying until the
/// probe succeeds, and then runs a [`ShardFlusher`] for each of the
/// bucket's shards open on the node. Flushers of buckets and shards that
/// are gone are stopped.
///
/// Only the `hold` conflict policy exists so far (§7.2); `overwrite` and
/// `discard_local` arrive with plan M4.
pub struct FlushService<S, D> {
    cluster: ClusterId,
    settings: FlushSettings,
    connect: Connect<S>,
    wall: Arc<dyn WallClock>,
    metrics: FlushMetrics,
    buckets: Mutex<BTreeMap<BucketId, BucketFlusher<S>>>,
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
    prefix: String,
    store: Arc<S>,
    probe: Arc<Mutex<ProbeStatus>>,
    writes: Arc<Mutex<Option<ConditionalWrites>>>,
    probe_task: JoinHandle<()>,
    target: Option<Arc<Target<S>>>,
    shards: BTreeMap<ShardId, ShardFlusher>,
}

impl<S> Drop for BucketFlusher<S> {
    fn drop(&mut self) {
        self.probe_task.abort();
    }
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
            buckets: Mutex::default(),
            _disk: std::marker::PhantomData,
        }
    }

    /// Sets the clock that dirty ages are measured on.
    #[must_use]
    pub fn with_wall_clock(mut self, wall: Arc<dyn WallClock>) -> Self {
        self.wall = wall;
        self
    }

    /// Starts and stops flushers so that every `write_back` bucket in
    /// `buckets` with shards open in `set` is flushed, and nothing else.
    pub async fn reconcile(&self, buckets: &[BucketDocument], set: &ShardSet<D>) {
        let mut open = Vec::new();
        for bucket in buckets {
            let (BucketMode::WriteBack, Some(target)) = (bucket.mode, &bucket.target) else {
                continue;
            };
            let mut shards = Vec::new();
            for shard in bucket.shards.shards() {
                let shard_ref = ShardRef::new(bucket.bucket_id.clone(), shard);
                if let Some(shard) = set.get(&shard_ref).await
                    && !shard.is_stopped()
                {
                    shards.push(shard);
                }
            }
            open.push((bucket, target, shards));
        }
        let mut flushers = self.lock();
        let wanted: BTreeSet<&BucketId> =
            open.iter().map(|(bucket, ..)| &bucket.bucket_id).collect();
        flushers.retain(|id, flusher| {
            let keep = wanted.contains(id);
            if !keep {
                self.metrics.remove(&flusher.name);
            }
            keep
        });
        for (bucket, target, shards) in open {
            let flusher = flushers
                .entry(bucket.bucket_id.clone())
                .or_insert_with(|| self.start(bucket, target));
            self.follow(flusher, shards);
        }
    }

    /// Starts a bucket's flushers with its target's probe.
    fn start(&self, bucket: &BucketDocument, target: &RemoteTarget) -> BucketFlusher<S> {
        let store = Arc::new((self.connect)(target));
        let prefix = target.prefix.clone().unwrap_or_default();
        let probe = Arc::new(Mutex::new(ProbeStatus::Running { error: None }));
        let writes = Arc::new(Mutex::default());
        let probe_task = tokio::spawn(run_probe(
            Arc::clone(&store),
            ConditionalProbe::with_fresh_nonce(&prefix),
            Arc::clone(&probe),
            Arc::clone(&writes),
            self.settings.clone(),
        ));
        BucketFlusher {
            name: bucket.name.as_str().to_owned(),
            prefix,
            store,
            probe,
            writes,
            probe_task,
            target: None,
            shards: BTreeMap::new(),
        }
    }

    /// Runs a flusher for each of `shards` once the probe is done, and
    /// stops the bucket's other flushers.
    fn follow(&self, flusher: &mut BucketFlusher<S>, shards: Vec<Shard<D>>) {
        if flusher.target.is_none() {
            let Some(writes) = *lock(&flusher.writes) else {
                return;
            };
            let target = Target::new(
                Arc::clone(&flusher.store),
                flusher.prefix.clone(),
                writes,
                self.cluster.clone(),
                self.settings.clone(),
            )
            .with_wall_clock(Arc::clone(&self.wall))
            .with_counters(self.metrics.counters(&flusher.name));
            flusher.target = Some(Arc::new(target));
        }
        let Some(target) = &flusher.target else {
            return;
        };
        let open: BTreeSet<ShardId> = shards.iter().map(|shard| shard.shard().shard).collect();
        flusher
            .shards
            .retain(|id, shard| open.contains(id) && !shard.is_stopped());
        for shard in shards {
            let id = shard.shard().shard;
            flusher
                .shards
                .entry(id)
                .or_insert_with(|| ShardFlusher::spawn(shard, Arc::clone(target)));
        }
    }

    /// The flush state of `bucket`, if it is flushed here.
    #[must_use]
    pub fn status(&self, bucket: &BucketId) -> Option<BucketStatus> {
        let flushers = self.lock();
        let flusher = flushers.get(bucket)?;
        Some(BucketStatus {
            name: flusher.name.clone(),
            probe: lock(&flusher.probe).clone(),
            shards: flusher
                .shards
                .iter()
                .map(|(id, shard)| (*id, shard.status()))
                .collect(),
        })
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

    /// Stops every flusher. Flushes in flight are abandoned; their keys stay
    /// dirty and are flushed after the next start.
    pub async fn shutdown(&self) {
        let flushers = std::mem::take(&mut *self.lock());
        for (_, mut flusher) in flushers {
            for (_, shard) in std::mem::take(&mut flusher.shards) {
                shard.stop().await;
            }
            self.metrics.remove(&flusher.name);
        }
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<BucketId, BucketFlusher<S>>> {
        lock(&self.buckets)
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Probes `store` until a run succeeds, backing off between runs.
async fn run_probe<S: ObjectStore>(
    store: Arc<S>,
    probe: ConditionalProbe,
    status: Arc<Mutex<ProbeStatus>>,
    writes: Arc<Mutex<Option<ConditionalWrites>>>,
    settings: FlushSettings,
) {
    let mut failures = 0;
    loop {
        match probe.run(&*store).await {
            Ok(found) => {
                let unprotected = found.unprotected();
                if !unprotected.is_empty() {
                    tracing::warn!(
                        ?unprotected,
                        "the target does not honor every precondition; \
                        these operations are sent unconditionally (§7.2)"
                    );
                }
                *lock(&writes) = Some(found);
                *lock(&status) = ProbeStatus::Done { unprotected };
                return;
            }
            Err(error) => {
                failures += 1;
                tracing::warn!(%error, "the target's capability probe failed");
                *lock(&status) = ProbeStatus::Running {
                    error: Some(error.to_string()),
                };
                tokio::time::sleep(settings.backoff(failures)).await;
            }
        }
    }
}
