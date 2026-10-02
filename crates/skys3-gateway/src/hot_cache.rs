//! The node-local hot cache (design §9.2).
//!
//! Each node's gateway keeps the bytes of objects it recently read from
//! another node's holder in memory, keyed by bucket, key, and version
//! identity: the position that names the version and its ETag. A GET
//! looks the cache up only for the version its read plan names, after the
//! shard's primary resolved the key under its lease, and a version's bytes
//! never change, so an entry can only ever serve the bytes it was filled
//! with to a read of that exact version. A later version of the key misses,
//! and drops the entry it supersedes.
//!
//! The cache keeps whole objects only, of at most an eighth of its
//! capacity each, so one large object cannot flush it, and it evicts the
//! least recently used while it holds more than its capacity,
//! `hot_cache_bytes_per_node`. A read fills it only once it has streamed
//! every byte of the object from a holder on another node: the gateway's
//! own replicas are local already, and a read that breaks off keeps
//! nothing. Clones of a cache share it.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::Bytes;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Unit;
use skys3_obs::MetricsRegistry;
use skys3_types::{BucketId, ETag, EpochSeq};

/// The largest object the cache keeps, as a share of its capacity: an
/// object of more than `capacity / MAX_OBJECT_SHARE` bytes is not kept.
const MAX_OBJECT_SHARE: u64 = 8;

/// The hot cache metrics (`docs/skys3-metrics.md`):
/// `skys3_hot_cache_bytes`, `skys3_hot_cache_hits_total`,
/// `skys3_hot_cache_misses_total`, and `skys3_hot_cache_evictions_total`.
/// The default metrics belong to no registry.
#[derive(Debug, Clone, Default)]
pub struct HotCacheMetrics {
    bytes: Gauge,
    hits: Counter,
    misses: Counter,
    evictions: Counter,
}

impl HotCacheMetrics {
    /// Registers the hot cache metrics in `registry`.
    #[must_use]
    pub fn register(registry: &MetricsRegistry) -> Self {
        let metrics = Self::default();
        registry.register_with_unit(
            "hot_cache",
            "Bytes of objects this node's hot cache holds.",
            Unit::Bytes,
            metrics.bytes.clone(),
        );
        registry.register(
            "hot_cache_hits",
            "GETs this node's gateway served from its hot cache.",
            metrics.hits.clone(),
        );
        registry.register(
            "hot_cache_misses",
            "GETs this node's gateway looked up in its hot cache without finding the version.",
            metrics.misses.clone(),
        );
        registry.register(
            "hot_cache_evictions",
            "Objects this node's hot cache dropped as least recently used.",
            metrics.evictions.clone(),
        );
        metrics
    }
}

/// What a [`HotCache`] holds and has done since it was made.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HotCacheUsage {
    /// Bytes of the objects held.
    pub bytes: u64,
    /// Objects held.
    pub entries: u64,
    /// Lookups that found the version they named.
    pub hits: u64,
    /// Lookups that did not.
    pub misses: u64,
    /// Objects dropped as least recently used.
    pub evictions: u64,
}

/// A node's hot cache of recently read objects (§9.2), bounded by
/// `hot_cache_bytes_per_node` and held in memory. Clones share the cache.
#[derive(Clone)]
pub struct HotCache {
    inner: Arc<Inner>,
}

struct Inner {
    capacity: u64,
    state: Mutex<State>,
    metrics: HotCacheMetrics,
    /// A seeded bug for the simulation, which only `test-util` builds can
    /// set: lookups ignore the version.
    ignore_version: AtomicBool,
}

impl fmt::Debug for HotCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HotCache")
            .field("capacity", &self.inner.capacity)
            .field("usage", &self.usage())
            .finish()
    }
}

impl HotCache {
    /// A cache of up to `capacity` bytes, `hot_cache_bytes_per_node`; one
    /// of 0 keeps nothing.
    #[must_use]
    pub fn new(capacity: u64) -> Self {
        Self::with_metrics(capacity, HotCacheMetrics::default())
    }

    /// A cache of up to `capacity` bytes that reports to `metrics`.
    #[must_use]
    pub fn with_metrics(capacity: u64, metrics: HotCacheMetrics) -> Self {
        Self {
            inner: Arc::new(Inner {
                capacity,
                state: Mutex::default(),
                metrics,
                ignore_version: AtomicBool::new(false),
            }),
        }
    }

    /// The most bytes the cache holds.
    #[must_use]
    pub fn capacity(&self) -> u64 {
        self.inner.capacity
    }

    /// What the cache holds and has done.
    #[must_use]
    pub fn usage(&self) -> HotCacheUsage {
        let state = self.lock();
        HotCacheUsage {
            bytes: state.bytes,
            entries: state.entries.len() as u64,
            hits: state.hits,
            misses: state.misses,
            evictions: state.evictions,
        }
    }

    /// Seeds a bug for the simulation to catch: from now on, a lookup
    /// returns whatever version of the key the cache holds.
    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    pub fn ignore_versions(&self) {
        self.inner.ignore_version.store(true, Ordering::Relaxed);
    }

    /// Whether the cache would keep an object of `size` bytes.
    pub(crate) fn admits(&self, size: u64) -> bool {
        size > 0 && size <= self.inner.capacity / MAX_OBJECT_SHARE
    }

    /// The bytes `range` of `version` of `object`, if the cache holds that
    /// version; a lookup that finds an older or newer version drops it.
    pub(crate) fn get(
        &self,
        object: &ObjectName,
        version: &VersionName,
        range: Range<u64>,
    ) -> Option<Bytes> {
        let ignore_version = self.inner.ignore_version.load(Ordering::Relaxed);
        let mut state = self.lock();
        let state = &mut *state;
        let found = match state.entries.get_mut(object) {
            Some(cached) if cached.version == *version || ignore_version => {
                // Bounds are within the object, so they fit in `usize`.
                let len = cached.data.len() as u64;
                (range.end <= len).then(|| {
                    state.recency.remove(&cached.used);
                    state.clock += 1;
                    cached.used = state.clock;
                    state.recency.insert(cached.used, object.clone());
                    cached.data.slice(range.start as usize..range.end as usize)
                })
            }
            Some(_) => {
                self.remove(state, object);
                None
            }
            None => None,
        };
        if found.is_some() {
            state.hits += 1;
            self.inner.metrics.hits.inc();
        } else {
            state.misses += 1;
            self.inner.metrics.misses.inc();
        }
        found
    }

    /// Keeps `data`, every byte of `version` of `object`, unless the cache
    /// does not admit its size or already holds a later version. Evicts
    /// the least recently used objects while it holds more than its
    /// capacity.
    pub(crate) fn insert(&self, object: ObjectName, version: VersionName, data: Bytes) {
        if !self.admits(data.len() as u64) {
            return;
        }
        let mut state = self.lock();
        let state = &mut *state;
        if let Some(cached) = state.entries.get(&object) {
            if cached.version.position > version.position {
                return;
            }
            self.remove(state, &object);
        }
        state.clock += 1;
        let used = state.clock;
        state.bytes += data.len() as u64;
        state.recency.insert(used, object.clone());
        state.entries.insert(
            object,
            Cached {
                version,
                data,
                used,
            },
        );
        while state.bytes > self.inner.capacity {
            let Some((_, oldest)) = state.recency.pop_first() else {
                break;
            };
            self.remove(state, &oldest);
            state.evictions += 1;
            self.inner.metrics.evictions.inc();
        }
        self.inner.metrics.bytes.set(gauge(state.bytes));
    }

    /// Drops `object`'s entry, if there is one.
    fn remove(&self, state: &mut State, object: &ObjectName) {
        if let Some(cached) = state.entries.remove(object) {
            state.recency.remove(&cached.used);
            state.bytes -= cached.data.len() as u64;
            self.inner.metrics.bytes.set(gauge(state.bytes));
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // Every update leaves the state consistent.
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

fn gauge(bytes: u64) -> i64 {
    i64::try_from(bytes).unwrap_or(i64::MAX)
}

/// An object the cache may hold a version of: a key of a bucket.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ObjectName {
    pub bucket: BucketId,
    pub key: String,
}

/// A version's identity (§9.2): the position of the record that wrote it,
/// and its ETag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VersionName {
    pub position: EpochSeq,
    pub etag: ETag,
}

/// One object held.
struct Cached {
    version: VersionName,
    data: Bytes,
    /// When the object was last used, on the cache's own clock: its key in
    /// [`State::recency`].
    used: u64,
}

#[derive(Default)]
struct State {
    entries: HashMap<ObjectName, Cached>,
    /// The objects held by when they were last used, least recent first.
    recency: BTreeMap<u64, ObjectName>,
    /// Counts uses.
    clock: u64,
    bytes: u64,
    hits: u64,
    misses: u64,
    evictions: u64,
}

#[cfg(test)]
mod tests {
    use skys3_types::{Epoch, Seq};

    use super::*;

    fn object(key: &str) -> ObjectName {
        ObjectName {
            bucket: BucketId::new("b-1").unwrap(),
            key: key.to_owned(),
        }
    }

    fn version(seq: u64, etag: &str) -> VersionName {
        VersionName {
            position: EpochSeq::new(Epoch::new(1), Seq::new(seq)),
            etag: ETag::new(etag).unwrap(),
        }
    }

    const A: &str = "0123456789abcdef0123456789abcdef";
    const B: &str = "fedcba9876543210fedcba9876543210";

    #[test]
    fn only_the_version_kept_is_served() {
        let cache = HotCache::new(800);
        assert!(cache.admits(100));
        assert!(!cache.admits(101));
        assert!(!cache.admits(0));
        cache.insert(object("k"), version(1, A), Bytes::from_static(b"first"));
        assert_eq!(
            cache.get(&object("k"), &version(1, A), 1..4).as_deref(),
            Some(&b"irs"[..])
        );
        // A range past the object is not served from it.
        assert_eq!(cache.get(&object("k"), &version(1, A), 0..6), None);
        // The same position with another ETag is another version.
        assert_eq!(cache.get(&object("k"), &version(1, B), 0..5), None);
        // That lookup dropped the entry.
        assert_eq!(cache.usage().entries, 0);
        assert_eq!(cache.get(&object("k"), &version(1, A), 0..5), None);
        assert_eq!(
            cache.usage(),
            HotCacheUsage {
                hits: 1,
                misses: 3,
                ..HotCacheUsage::default()
            }
        );
    }

    #[test]
    fn a_later_version_replaces_an_earlier_one_and_never_the_reverse() {
        let cache = HotCache::new(800);
        cache.insert(object("k"), version(2, B), Bytes::from_static(b"second"));
        // A read of the earlier version that finishes later keeps nothing.
        cache.insert(object("k"), version(1, A), Bytes::from_static(b"first"));
        assert_eq!(cache.get(&object("k"), &version(1, A), 0..5), None);
        cache.insert(object("k"), version(2, B), Bytes::from_static(b"second"));
        cache.insert(object("k"), version(3, A), Bytes::from_static(b"third"));
        assert_eq!(cache.usage().bytes, 5);
        assert_eq!(cache.get(&object("k"), &version(2, B), 0..6), None);
        assert_eq!(cache.usage().bytes, 0);
        // Objects the cache does not admit are not kept.
        cache.insert(object("k"), version(4, A), Bytes::from(vec![0; 101]));
        assert_eq!(cache.usage().entries, 0);
        HotCache::new(0).insert(object("k"), version(1, A), Bytes::from_static(b"x"));
    }

    #[test]
    fn the_least_recently_used_are_evicted_over_the_capacity() {
        let registry = MetricsRegistry::new();
        let cache = HotCache::with_metrics(320, HotCacheMetrics::register(&registry));
        assert_eq!(cache.capacity(), 320);
        for (n, key) in ["a", "b", "c", "d"].into_iter().enumerate() {
            cache.insert(object(key), version(n as u64, A), Bytes::from(vec![0; 40]));
        }
        assert_eq!(cache.usage().bytes, 160);
        // `a` is used, so `b` is now the least recently used.
        assert!(cache.get(&object("a"), &version(0, A), 0..40).is_some());
        for (n, key) in ["e", "f", "g", "h"].into_iter().enumerate() {
            cache.insert(object(key), version(n as u64, A), Bytes::from(vec![0; 40]));
        }
        assert_eq!(cache.usage().bytes, 320);
        cache.insert(object("i"), version(1, A), Bytes::from(vec![0; 40]));
        let usage = cache.usage();
        assert_eq!((usage.bytes, usage.entries, usage.evictions), (320, 8, 1));
        assert!(cache.get(&object("b"), &version(1, A), 0..40).is_none());
        assert!(cache.get(&object("a"), &version(0, A), 0..40).is_some());
        let text = registry.encode().unwrap();
        for line in [
            "skys3_hot_cache_bytes 320",
            "skys3_hot_cache_hits_total 2",
            "skys3_hot_cache_misses_total 1",
            "skys3_hot_cache_evictions_total 1",
        ] {
            assert!(text.contains(line), "{line} in {text}");
        }
        assert!(format!("{cache:?}").contains("capacity: 320"));
    }

    #[cfg(feature = "test-util")]
    #[test]
    fn the_seeded_bug_serves_any_version() {
        let cache = HotCache::new(800);
        cache.insert(object("k"), version(1, A), Bytes::from_static(b"first"));
        cache.ignore_versions();
        assert!(cache.get(&object("k"), &version(2, B), 0..5).is_some());
    }
}
