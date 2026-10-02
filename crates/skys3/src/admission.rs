//! Admission control on a node (design §7.6, §13): a write that adds data
//! waits with `503 SlowDown` while its bucket's or the cluster's dirty-data
//! budget is used up, or while the disk that holds its shard, or the data
//! directory's file system, is low on space.
//!
//! The free-space margin, `storage.disk_min_free_bytes`, keeps a disk from
//! filling: the first write error takes a disk out of service until the
//! host restarts (§10.4), so a full disk must be avoided, not survived.

use std::collections::BTreeSet;
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use skys3_flush::{DirtyBudget, Exhausted};
use skys3_gateway::{Admission, Refusal, ShardRef};
use skys3_io::BlockingPool;
use skys3_obs::MetricsRegistry;
use skys3_shard::CleanCache;
use skys3_types::{BucketDocument, Label};

/// A place whose free space is watched.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Place {
    /// The data directory, which holds the index: every write needs it.
    DataDir,
    /// A log disk, which holds the records of the shards placed on it.
    Disk(Label),
}

impl fmt::Display for Place {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DataDir => f.write_str("the data directory"),
            Self::Disk(label) => write!(f, "disk {label}"),
        }
    }
}

/// A watched place: where it is, and the pool its I/O runs on (§10.4).
pub(crate) type Watched = (Place, PathBuf, BlockingPool);

/// Which of the node's places are low on free space.
#[derive(Debug)]
pub(crate) struct DiskSpace {
    min_free: u64,
    low: Mutex<BTreeSet<Place>>,
    /// The clean cache, told the space of each log disk (design §9.3).
    cache: Option<CleanCache>,
}

impl DiskSpace {
    /// Places are low below `min_free` bytes free; 0 turns the check off.
    pub(crate) fn new(min_free: u64) -> Self {
        Self {
            min_free,
            low: Mutex::default(),
            cache: None,
        }
    }

    /// Also tells `cache` the space of each log disk at every refresh.
    pub(crate) fn with_cache(self, cache: CleanCache) -> Self {
        Self {
            cache: Some(cache),
            ..self
        }
    }

    /// Records that `place` has `available` bytes free.
    pub(crate) fn update(&self, place: &Place, available: u64) {
        let low = available < self.min_free;
        let mut places = self.lock();
        if low && places.insert(place.clone()) {
            tracing::warn!(%place, available, min_free = self.min_free,
                "low on disk space; writes that add data get 503 SlowDown");
        } else if !low && places.remove(place) {
            tracing::info!(%place, available, "disk space recovered; writes are admitted");
        }
    }

    /// Whether a write to a shard on `disk` must wait for space.
    pub(crate) fn is_low(&self, disk: &Label) -> bool {
        let places = self.lock();
        !places.is_empty()
            && (places.contains(&Place::DataDir) || places.contains(&Place::Disk(disk.clone())))
    }

    /// Reads the free space of every place in `watched`. A place whose
    /// space cannot be read keeps its last state.
    pub(crate) async fn refresh(&self, watched: &[Watched]) {
        if self.min_free == 0 && self.cache.is_none() {
            return;
        }
        for (place, path, pool) in watched {
            let path = path.clone();
            match pool.run(move || skys3_io::disk::space(&path)).await {
                Ok(Ok(space)) => {
                    if let (Some(cache), Place::Disk(disk)) = (&self.cache, place) {
                        cache.set_space(disk, space);
                    }
                    if self.min_free > 0 {
                        self.update(place, space.available);
                    }
                }
                Ok(Err(error)) => tracing::warn!(%place, %error, "cannot read the free space"),
                Err(error) => tracing::warn!(%place, %error, "cannot read the free space"),
            }
        }
    }

    fn lock(&self) -> MutexGuard<'_, BTreeSet<Place>> {
        self.low.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Refreshes `space` every `interval`.
pub(crate) async fn watch_space(space: Arc<DiskSpace>, watched: Vec<Watched>, interval: Duration) {
    loop {
        tokio::time::sleep(interval).await;
        space.refresh(&watched).await;
    }
}

/// The disk label a shard's records are on.
pub(crate) type DiskOf = Box<dyn Fn(&ShardRef) -> Label + Send + Sync>;

/// The label set of the refusal counter: `reason`.
type Labels = Vec<(&'static str, &'static str)>;

/// The node's [`Admission`]: free space first, then the dirty budgets.
pub(crate) struct NodeAdmission {
    budget: Arc<DirtyBudget>,
    space: Arc<DiskSpace>,
    disk_of: DiskOf,
    refusals: Family<Labels, Counter>,
}

impl fmt::Debug for NodeAdmission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeAdmission")
            .field("budget", &self.budget)
            .field("space", &self.space)
            .finish_non_exhaustive()
    }
}

impl NodeAdmission {
    /// Admission by `budget` and `space`, with refusals counted in
    /// `registry`.
    pub(crate) fn new(
        budget: Arc<DirtyBudget>,
        space: Arc<DiskSpace>,
        disk_of: DiskOf,
        registry: &MetricsRegistry,
    ) -> Self {
        let refusals = Family::default();
        registry.register(
            "admission_refusals",
            "Writes answered 503 SlowDown by admission control, by reason: bucket_budget, \
             cluster_budget, or disk_space.",
            refusals.clone(),
        );
        Self {
            budget,
            space,
            disk_of,
            refusals,
        }
    }
}

impl Admission for NodeAdmission {
    fn admit(&self, bucket: &BucketDocument, shard: &ShardRef) -> Result<(), Refusal> {
        let verdict = if self.space.is_low(&(self.disk_of)(shard)) {
            Err(Refusal::DiskSpace)
        } else {
            self.budget
                .check(&bucket.bucket_id)
                .map_err(|exhausted| match exhausted {
                    Exhausted::Bucket => Refusal::BucketBudget,
                    Exhausted::Cluster => Refusal::ClusterBudget,
                })
        };
        if let Err(refusal) = verdict {
            self.refusals
                .get_or_create(&vec![("reason", refusal.as_str())])
                .inc();
        }
        verdict
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use skys3_types::{BucketId, BucketMode, ProposalId, ShardCount, ShardId};

    use super::*;

    fn label(name: &str) -> Label {
        Label::new(name).unwrap()
    }

    fn bucket() -> BucketDocument {
        BucketDocument {
            bucket_id: BucketId::new("b-1").unwrap(),
            name: "photos".parse().unwrap(),
            mode: BucketMode::Local,
            shards: ShardCount::new(2).unwrap(),
            replicas: 1,
            min_write_replicas: 1,
            clean_copies: 0,
            target: None,
            created_unix_ms: 0,
            proposal_id: ProposalId::new("p").unwrap(),
        }
    }

    #[test]
    fn low_places_refuse_the_shards_on_them() {
        let space = Arc::new(DiskSpace::new(100));
        let registry = MetricsRegistry::new();
        // Shard 0 is on disk-0, shard 1 on disk-1.
        let disk_of: DiskOf = Box::new(|shard| label(&format!("disk-{}", shard.shard.get())));
        let admission = NodeAdmission::new(
            Arc::new(DirtyBudget::unlimited()),
            Arc::clone(&space),
            disk_of,
            &registry,
        );
        let bucket = bucket();
        let shard = |n| ShardRef {
            bucket: bucket.bucket_id.clone(),
            shard: ShardId::new(n),
        };
        assert_eq!(admission.admit(&bucket, &shard(0)), Ok(()));

        space.update(&Place::Disk(label("disk-1")), 99);
        assert_eq!(admission.admit(&bucket, &shard(0)), Ok(()));
        assert_eq!(admission.admit(&bucket, &shard(1)), Err(Refusal::DiskSpace));
        space.update(&Place::DataDir, 0);
        assert_eq!(admission.admit(&bucket, &shard(0)), Err(Refusal::DiskSpace));
        space.update(&Place::DataDir, 100);
        space.update(&Place::Disk(label("disk-1")), 100);
        assert_eq!(admission.admit(&bucket, &shard(1)), Ok(()));
        let text = registry.encode().unwrap();
        assert!(
            text.contains("skys3_admission_refusals_total{reason=\"disk_space\"} 2"),
            "{text}"
        );
        assert!(format!("{admission:?}").contains("NodeAdmission"));
        assert_eq!(Place::DataDir.to_string(), "the data directory");
    }

    #[tokio::test]
    async fn free_space_is_read_on_the_pool() {
        let dir = tempfile::tempdir().unwrap();
        let pool = BlockingPool::new("space", NonZeroUsize::MIN).unwrap();
        let disk = label("disk-0");
        let watched = vec![
            (
                Place::Disk(disk.clone()),
                dir.path().to_owned(),
                pool.clone(),
            ),
            (Place::DataDir, dir.path().join("missing"), pool.clone()),
        ];
        // No file system has this much free.
        let space = DiskSpace::new(u64::MAX);
        space.refresh(&watched).await;
        assert!(space.is_low(&disk));
        assert!(
            !space.is_low(&label("disk-1")),
            "the data directory is unread"
        );
        // The check is off at 0, and the clean cache learns each disk's
        // room still.
        let cache = CleanCache::new(
            skys3_shard::CacheSettings {
                max_bytes: u64::MAX,
                reserve_fraction: 0.0,
            },
            skys3_shard::CacheMetrics::default(),
        );
        let off = DiskSpace::new(0).with_cache(cache.clone());
        off.update(&Place::DataDir, 0);
        off.refresh(&watched).await;
        assert!(!off.is_low(&disk));
        assert!(cache.usage().limit < u64::MAX);
        pool.shutdown();
        space.refresh(&watched).await;
        assert!(space.is_low(&disk), "an unread place keeps its state");
    }
}
