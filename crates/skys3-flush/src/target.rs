//! What every shard flusher of one remote target shares: the store, the
//! preconditions it honors, the settings, and the window and byte bound of
//! its requests in flight (§7.7).

use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use skys3_config::{ConflictPolicy, FlushConfig, PeeringConfig};
use skys3_io::{SystemWallClock, WallClock};
use skys3_log::ShardRef;
use skys3_remote::UploadId;
use skys3_remote::probe::ConditionalWrites;
use skys3_types::{ClusterId, EpochSeq, WriteIdentity};
use tokio::sync::{Semaphore, SemaphorePermit};

use crate::concurrency::{ConcurrencyBug, ConcurrencyStatus, Membership, Paced, Window, hooks};
use crate::metrics::Counters;

/// The most abandoned remote uploads a target keeps to abort; beyond it,
/// they are left to the remote bucket's lifecycle rule (§7.3).
const MAX_ORPHANS: usize = 4096;

/// Remote multipart uploads left open, by remote key and upload ID.
type Orphans = VecDeque<(String, UploadId)>;

/// How far a namespace import has got (§9.1), as the flusher needs to know.
///
/// A key the import has passed is in the index exactly as the remote held
/// it, so a key without a remote ETag is absent there. Before the import
/// passes a key, its remote state is unknown, and a tombstone must stay in
/// the index (§4.2). A bucket's import ([`ImportState`](crate::ImportState))
/// implements it; [`ImportDone`] says every key is passed.
pub trait ImportProgress: Send + Sync + 'static {
    /// Whether the import has passed `key`.
    fn passed(&self, key: &str) -> bool;
}

/// The [`ImportProgress`] of a bucket whose import is complete.
#[derive(Debug, Clone, Copy, Default)]
pub struct ImportDone;

impl ImportProgress for ImportDone {
    fn passed(&self, _key: &str) -> bool {
        true
    }
}

/// How a shard flusher paces itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushSettings {
    /// `flush_min_concurrency_per_shard`: the least requests per shard the
    /// adaptive window of a target allows in flight (§7.7).
    pub min_concurrency: u32,
    /// `flush_max_concurrency_per_shard`: the most requests per shard the
    /// window allows, and the most keys, and parts of streamed uploads,
    /// one shard flushes at once. Equal bounds fix the window.
    pub max_concurrency: u32,
    /// `flush_max_inflight_bytes_per_target`: the bytes all flushers of a
    /// target hold in memory for requests at once: a whole object's body
    /// for a single PUT, and a part's for each part of a multipart or
    /// streamed upload in flight, from reading it until its answer. A
    /// larger body takes the whole budget.
    pub max_inflight_bytes: u64,
    /// `import_max_keys_per_second`: the most remote keys a bucket's
    /// namespace import lists a second (§9.1).
    pub import_keys_per_second: u64,
    /// The most keys one listing request of the import asks for, at most
    /// S3's 1,000 ([`IMPORT_PAGE_KEYS`](crate::IMPORT_PAGE_KEYS)): each
    /// page ends with a checkpoint.
    pub import_page_keys: u32,
    /// The listing streams of a bucket's import, unless the service has
    /// the bucket's `import_parallel_streams`
    /// ([`FlushService::with_buckets`](crate::FlushService::with_buckets)).
    pub import_streams: usize,
    /// The first delay before a key is retried after a failure.
    pub min_backoff: Duration,
    /// The longest delay between retries of a key; delays double up to it.
    pub max_backoff: Duration,
    /// `extent_bytes`: the size of the `EXTENT` records a read-through
    /// fill commits (§9.2), as a PUT's body is committed.
    pub extent_bytes: u64,
    /// Whether multipart uploads and large single PUTs stream to the
    /// remote while the client uploads (§7.3). Without it, an object is
    /// sent only once it commits; remote uploads streamed earlier are still
    /// completed or aborted.
    pub streaming: bool,
    /// `flush_part_bytes`: the part size of the remote multipart upload a
    /// streamed single PUT is sent as (§7.3).
    pub part_bytes: u64,
    /// How long after the last announcement of a streamed single PUT's body
    /// its `PUT` may still commit: `peer_staging_ttl_seconds`, twice the
    /// gateway's deadline for a body (§10.3), which leaves room for clock
    /// drift and the commit itself. A remote upload whose `PUT` has not
    /// committed by then never completes, and is aborted.
    pub body_timeout: Duration,
}

impl FlushSettings {
    /// The settings of the `[flush]` section.
    #[must_use]
    pub fn from_config(config: &FlushConfig) -> Self {
        Self {
            min_concurrency: config.flush_min_concurrency_per_shard.max(1),
            max_concurrency: config
                .flush_max_concurrency_per_shard
                .max(config.flush_min_concurrency_per_shard)
                .max(1),
            max_inflight_bytes: config.flush_max_inflight_bytes_per_target,
            import_keys_per_second: config.import_max_keys_per_second,
            part_bytes: config.flush_part_bytes,
            ..Self::default()
        }
    }

    /// The delay before retry number `attempt`, counting from 1.
    #[must_use]
    pub fn backoff(&self, attempt: u32) -> Duration {
        let factor = 1u32 << attempt.saturating_sub(1).min(20);
        self.min_backoff
            .saturating_mul(factor)
            .min(self.max_backoff)
    }
}

impl Default for FlushSettings {
    fn default() -> Self {
        Self {
            min_concurrency: FlushConfig::default().flush_min_concurrency_per_shard,
            max_concurrency: FlushConfig::default().flush_max_concurrency_per_shard,
            max_inflight_bytes: FlushConfig::default().flush_max_inflight_bytes_per_target,
            import_keys_per_second: FlushConfig::default().import_max_keys_per_second,
            import_page_keys: crate::IMPORT_PAGE_KEYS,
            import_streams: 1,
            min_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(30),
            extent_bytes: 1 << 20,
            streaming: true,
            part_bytes: FlushConfig::default().flush_part_bytes,
            body_timeout: PeeringConfig::default().peer_staging_ttl(),
        }
    }
}

/// One remote target as its shard flushers see it.
pub struct Target<S> {
    /// The store, each request through the window.
    pub(crate) store: Paced<S>,
    /// The requests in flight (§7.7).
    window: Arc<Window>,
    pub(crate) prefix: String,
    pub(crate) writes: ConditionalWrites,
    pub(crate) cluster: ClusterId,
    pub(crate) settings: FlushSettings,
    pub(crate) import: Arc<dyn ImportProgress>,
    pub(crate) wall: Arc<dyn WallClock>,
    pub(crate) counters: Counters,
    /// What a flush that finds an out-of-band write does (§7.2).
    pub(crate) conflict_policy: ConflictPolicy,
    /// The in-flight budget, in KiB.
    inflight: Semaphore,
    inflight_kib: u32,
    /// The bytes reserved from it.
    inflight_bytes: AtomicU64,
    /// Remote multipart uploads that flushes left open, oldest first.
    orphans: Mutex<Orphans>,
}

impl<S> fmt::Debug for Target<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Target")
            .field("prefix", &self.prefix)
            .field("writes", &self.writes)
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

impl<S> Target<S> {
    /// A target: `store`, under the key prefix `prefix` (empty for a whole
    /// bucket), honoring the preconditions in `writes` (from the
    /// capability probe), written by `cluster`. Every key is taken as
    /// imported, ages are measured on the system clock, conflicts are held,
    /// and nothing is counted until the `with_*` methods say otherwise.
    pub fn new(
        store: Arc<S>,
        prefix: impl Into<String>,
        writes: ConditionalWrites,
        cluster: ClusterId,
        settings: FlushSettings,
    ) -> Self {
        let inflight_kib = u32::try_from(settings.max_inflight_bytes.div_ceil(1024))
            .unwrap_or(u32::MAX)
            .clamp(1, u32::try_from(Semaphore::MAX_PERMITS).unwrap_or(u32::MAX));
        let window = Arc::new(Window::new((
            settings.min_concurrency,
            settings.max_concurrency,
        )));
        Self {
            store: Paced::new(store, Arc::clone(&window)),
            window,
            prefix: prefix.into(),
            writes,
            cluster,
            settings,
            import: Arc::new(ImportDone),
            wall: Arc::new(SystemWallClock),
            counters: Counters::default(),
            conflict_policy: ConflictPolicy::Hold,
            inflight: Semaphore::new(inflight_kib as usize),
            inflight_kib,
            inflight_bytes: AtomicU64::new(0),
            orphans: Mutex::default(),
        }
    }

    /// Sets how far the bucket's import has got.
    #[must_use]
    pub fn with_import(mut self, import: Arc<dyn ImportProgress>) -> Self {
        self.import = import;
        self
    }

    /// Sets the clock that dirty ages are measured on.
    #[must_use]
    pub fn with_wall_clock(mut self, wall: Arc<dyn WallClock>) -> Self {
        self.wall = wall;
        self
    }

    /// Sets the counters flushes, retries, and conflicts are counted in.
    #[must_use]
    pub fn with_counters(mut self, counters: Counters) -> Self {
        self.window.count_throttles(counters.throttles.clone());
        self.counters = counters;
        self
    }

    /// Sets what a flush that finds an out-of-band write does (§7.2):
    /// `hold` the key, `overwrite` the remote's write, or `discard_local`,
    /// adopting the remote's write in place of the local version. The
    /// caller decides whether the bucket may discard
    /// ([`FlushService`](crate::FlushService) allows it only for a
    /// `write_back` bucket whose own table opted in).
    #[must_use]
    pub fn with_conflict_policy(mut self, policy: ConflictPolicy) -> Self {
        self.conflict_policy = policy;
        self
    }

    /// What a flush that finds an out-of-band write does.
    pub fn conflict_policy(&self) -> ConflictPolicy {
        self.conflict_policy
    }

    /// The preconditions the target honors.
    pub fn writes(&self) -> &ConditionalWrites {
        &self.writes
    }

    /// The counters its flushers count events in.
    pub fn counters(&self) -> &Counters {
        &self.counters
    }

    /// The window of requests in flight to the target, and the bytes held
    /// for them (§7.7).
    pub fn concurrency(&self) -> ConcurrencyStatus {
        ConcurrencyStatus {
            inflight_bytes: self.inflight_bytes.load(Ordering::Relaxed),
            ..self.window.status()
        }
    }

    /// Counts a shard's flusher among those sending to the target, which
    /// widens the window's bounds, until the membership is dropped.
    pub(crate) fn join(&self) -> Membership {
        self.window.join()
    }

    /// How many remote multipart uploads flushes left open, because their
    /// abort failed or their flusher stopped mid-flight, wait to be
    /// aborted (`Target::abort_orphaned_uploads`).
    pub fn orphaned_uploads(&self) -> usize {
        self.lock_orphans().len()
    }

    /// Keeps the open remote upload `upload_id` of the remote key `key` to
    /// abort later.
    pub(crate) fn orphan(&self, key: String, upload_id: UploadId) {
        let mut orphans = self.lock_orphans();
        if orphans.len() < MAX_ORPHANS {
            orphans.push_back((key, upload_id));
        } else {
            tracing::warn!(key, %upload_id,
                "a remote upload is left open for the bucket's lifecycle rule to abort");
        }
    }

    /// Takes the oldest open remote upload kept to abort.
    pub(crate) fn take_orphan(&self) -> Option<(String, UploadId)> {
        self.lock_orphans().pop_front()
    }

    fn lock_orphans(&self) -> MutexGuard<'_, Orphans> {
        // Every update is a single push or pop.
        self.orphans.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The write identity of the record at `position` of `shard` (§7.2).
    pub(crate) fn identity(&self, shard: &ShardRef, position: EpochSeq) -> WriteIdentity {
        WriteIdentity::new(
            self.cluster.clone(),
            shard.bucket.clone(),
            shard.shard,
            position,
        )
    }

    /// Waits until `size` bytes fit the target's in-flight budget, and
    /// holds them until the reservation is dropped.
    pub(crate) async fn reserve(&self, size: u64) -> Reservation<'_> {
        let kib = u32::try_from(size.div_ceil(1024))
            .unwrap_or(u32::MAX)
            .clamp(1, self.inflight_kib);
        let permit = if hooks::bug() == ConcurrencyBug::IgnoresInflightBytes {
            None
        } else {
            self.inflight.acquire_many(kib).await.ok()
        };
        self.inflight_bytes.fetch_add(size, Ordering::Relaxed);
        Reservation {
            _permit: permit,
            bytes: size,
            counted: &self.inflight_bytes,
        }
    }
}

/// Bytes held within a target's in-flight budget, until dropped.
pub(crate) struct Reservation<'t> {
    _permit: Option<SemaphorePermit<'t>>,
    bytes: u64,
    counted: &'t AtomicU64,
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        self.counted.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use skys3_remote::probe::{OperationSupport, PreconditionSupport};

    use super::*;

    fn target() -> Target<()> {
        let honored = OperationSupport {
            if_none_match: Some(PreconditionSupport::Honored),
            if_match: PreconditionSupport::Honored,
        };
        let writes = ConditionalWrites {
            put_object: honored,
            complete_multipart_upload: honored,
            delete_object: honored,
        };
        let cluster = ClusterId::new("c-test").unwrap();
        Target::new(
            Arc::new(()),
            "p/",
            writes,
            cluster,
            FlushSettings::default(),
        )
    }

    #[test]
    fn orphaned_uploads_are_kept_oldest_first_up_to_a_limit() {
        let target = target();
        assert_eq!(target.take_orphan(), None);
        for n in 0..=MAX_ORPHANS {
            target.orphan(format!("p/k{n}"), UploadId(format!("u{n}")));
        }
        // The one past the limit is left to the lifecycle rule.
        assert_eq!(target.orphaned_uploads(), MAX_ORPHANS);
        assert_eq!(
            target.take_orphan(),
            Some(("p/k0".to_owned(), UploadId("u0".to_owned())))
        );
        assert_eq!(target.orphaned_uploads(), MAX_ORPHANS - 1);
        assert!(format!("{target:?}").contains("p/"));
    }
}
