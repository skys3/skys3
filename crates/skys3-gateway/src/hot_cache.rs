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
//! capacity each, so one large object cannot flush it. Its capacity,
//! `hot_cache_bytes_per_node`, bounds the objects it holds and the fills
//! in progress together: a read that may fill the cache first reserves
//! the object's size ([`HotCache::reserve`]), evicting the least recently
//! used objects to make room, and a read that cannot reserve, because
//! fills in progress already hold the room or one already fills the same
//! version, streams without keeping anything. A fill keeps the pieces it
//! streams, which grow as they arrive, and becomes an entry only once it
//! has every byte of the object; a fill that breaks off, or is dropped,
//! releases its reservation. Clones of a cache share it.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::{Bytes, BytesMut};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Unit;
use skys3_obs::MetricsRegistry;
use skys3_types::{BucketId, ETag, EpochSeq};

/// The largest object the cache keeps, as a share of its capacity: an
/// object of more than `capacity / MAX_OBJECT_SHARE` bytes is not kept.
const MAX_OBJECT_SHARE: u64 = 8;

/// The hot cache metrics (`docs/skys3-metrics.md`):
/// `skys3_hot_cache_bytes`, `skys3_hot_cache_filling_bytes`,
/// `skys3_hot_cache_hits_total`, `skys3_hot_cache_misses_total`, and
/// `skys3_hot_cache_evictions_total`. The default metrics belong to no
/// registry.
#[derive(Debug, Clone, Default)]
pub struct HotCacheMetrics {
    bytes: Gauge,
    filling: Gauge,
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
        registry.register_with_unit(
            "hot_cache_filling",
            "Bytes this node's hot cache reserves for fills in progress.",
            Unit::Bytes,
            metrics.filling.clone(),
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
    /// Bytes reserved for fills in progress; with `bytes`, at most the
    /// capacity.
    pub reserved: u64,
    /// Objects held.
    pub entries: u64,
    /// Lookups that found the version they named.
    pub hits: u64,
    /// Lookups that did not.
    pub misses: u64,
    /// Objects dropped as least recently used.
    pub evictions: u64,
    /// Fills refused because the room was reserved, or the same version
    /// was already filling.
    pub refused: u64,
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

    /// The most bytes the cache holds and reserves for fills together.
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
            reserved: state.reserved,
            entries: state.entries.len() as u64,
            hits: state.hits,
            misses: state.misses,
            evictions: state.evictions,
            refused: state.refused,
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
                (range.end <= cached.len).then(|| {
                    state.recency.remove(&cached.used);
                    state.clock += 1;
                    cached.used = state.clock;
                    state.recency.insert(cached.used, object.clone());
                    slice(&cached.chunks, range)
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

    /// Reserves room for a fill of `version` of `object`, `size` bytes,
    /// evicting the least recently used objects as needed: `None` if the
    /// cache does not admit the size, already holds this version or a
    /// later one, already fills this version, or reserves too much for
    /// other fills to make room. The objects held and the reservations
    /// never exceed the capacity together.
    pub(crate) fn reserve(
        &self,
        object: ObjectName,
        version: VersionName,
        size: u64,
    ) -> Option<Fill> {
        if !self.admits(size) {
            return None;
        }
        let mut state = self.lock();
        let state = &mut *state;
        if state
            .entries
            .get(&object)
            .is_some_and(|cached| cached.version.position >= version.position)
        {
            return None;
        }
        let filling = (object.clone(), version.position);
        if state.filling.contains(&filling) || state.reserved + size > self.inner.capacity {
            state.refused += 1;
            return None;
        }
        while state.bytes + state.reserved + size > self.inner.capacity {
            let Some((_, oldest)) = state.recency.pop_first() else {
                break;
            };
            self.remove(state, &oldest);
            state.evictions += 1;
            self.inner.metrics.evictions.inc();
        }
        state.reserved += size;
        state.filling.insert(filling);
        self.inner.metrics.filling.set(gauge(state.reserved));
        Some(Fill {
            cache: self.clone(),
            object,
            version,
            size,
            chunks: Vec::new(),
            len: 0,
            reserved: true,
        })
    }

    /// Releases `fill`'s reservation and, if `chunks` are every byte of
    /// its object, keeps them, unless the cache holds a later version by
    /// now.
    fn finish(&self, fill: &mut Fill, chunks: Option<Vec<Bytes>>) {
        let mut state = self.lock();
        let state = &mut *state;
        state.reserved -= fill.size;
        state
            .filling
            .remove(&(fill.object.clone(), fill.version.position));
        self.inner.metrics.filling.set(gauge(state.reserved));
        fill.reserved = false;
        let Some(chunks) = chunks else {
            return;
        };
        if let Some(cached) = state.entries.get(&fill.object) {
            if cached.version.position > fill.version.position {
                return;
            }
            self.remove(state, &fill.object.clone());
        }
        // The reservation held the room: the objects held still fit.
        state.clock += 1;
        let used = state.clock;
        state.bytes += fill.size;
        state.recency.insert(used, fill.object.clone());
        state.entries.insert(
            fill.object.clone(),
            Cached {
                version: fill.version.clone(),
                chunks,
                len: fill.size,
                used,
            },
        );
        self.inner.metrics.bytes.set(gauge(state.bytes));
    }

    /// Drops `object`'s entry, if there is one.
    fn remove(&self, state: &mut State, object: &ObjectName) {
        if let Some(cached) = state.entries.remove(object) {
            state.recency.remove(&cached.used);
            state.bytes -= cached.len;
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

/// A fill in progress: room reserved in the cache for one version of an
/// object, and the pieces of it streamed so far. Dropping it before
/// [`Fill::finish`] releases the room and keeps nothing.
pub(crate) struct Fill {
    cache: HotCache,
    object: ObjectName,
    version: VersionName,
    size: u64,
    /// The pieces streamed so far: the same buffers the response sent,
    /// so keeping them copies nothing.
    chunks: Vec<Bytes>,
    len: u64,
    /// Whether the reservation is still held.
    reserved: bool,
}

impl fmt::Debug for Fill {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Fill")
            .field("object", &self.object)
            .field("version", &self.version)
            .field("size", &self.size)
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

impl Fill {
    /// Adds the next piece of the object. Pieces past its size are not
    /// kept, and the fill then keeps nothing.
    pub(crate) fn push(&mut self, data: &Bytes) {
        self.len = self.len.saturating_add(data.len() as u64);
        if self.len <= self.size {
            self.chunks.push(data.clone());
        } else {
            self.chunks = Vec::new();
        }
    }

    /// Releases the reservation and, if the fill has every byte of the
    /// object, keeps them.
    pub(crate) fn finish(mut self) {
        let chunks = std::mem::take(&mut self.chunks);
        let whole = self.len == self.size;
        let cache = self.cache.clone();
        cache.finish(&mut self, whole.then_some(chunks));
    }
}

impl Drop for Fill {
    fn drop(&mut self) {
        if self.reserved {
            let cache = self.cache.clone();
            cache.finish(self, None);
        }
    }
}

/// The bytes `range` of an object held as `chunks`: a slice of one chunk
/// when the range lies within it, a copy otherwise.
fn slice(chunks: &[Bytes], range: Range<u64>) -> Bytes {
    // Bounds are within an object held in memory, so they fit in `usize`.
    let (start, end) = (range.start as usize, range.end as usize);
    let mut offset = 0;
    let mut out: Option<BytesMut> = None;
    for chunk in chunks {
        let (from, to) = (offset, offset + chunk.len());
        offset = to;
        if to <= start || chunk.is_empty() {
            continue;
        }
        if from >= end {
            break;
        }
        let piece = chunk.slice(start.max(from) - from..end.min(to) - from);
        if from <= start && end <= to {
            return piece;
        }
        out.get_or_insert_with(|| BytesMut::with_capacity(end - start))
            .extend_from_slice(&piece);
    }
    out.map_or_else(Bytes::new, BytesMut::freeze)
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

/// One object held, as the pieces it streamed in.
struct Cached {
    version: VersionName,
    chunks: Vec<Bytes>,
    len: u64,
    /// When the object was last used, on the cache's own clock: its key in
    /// [`State::recency`].
    used: u64,
}

#[derive(Default)]
struct State {
    entries: HashMap<ObjectName, Cached>,
    /// The objects held by when they were last used, least recent first.
    recency: BTreeMap<u64, ObjectName>,
    /// The versions being filled.
    filling: HashSet<(ObjectName, EpochSeq)>,
    /// Counts uses.
    clock: u64,
    bytes: u64,
    reserved: u64,
    hits: u64,
    misses: u64,
    evictions: u64,
    refused: u64,
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

    /// Fills the cache with `data`, every byte of `version` of `object`,
    /// if it reserves room.
    fn insert(cache: &HotCache, object: ObjectName, version: VersionName, data: Bytes) {
        if let Some(mut fill) = cache.reserve(object, version, data.len() as u64) {
            fill.push(&data);
            fill.finish();
        }
    }

    #[test]
    fn fills_in_progress_count_against_the_capacity() {
        let cache = HotCache::new(800);
        insert(
            &cache,
            object("old"),
            version(1, A),
            Bytes::from(vec![0; 100]),
        );
        // A burst of misses: each reserves its object's size, evicting the
        // object held, until fills hold all the room; the rest keep
        // nothing, and the bound holds throughout.
        let mut fills = Vec::new();
        for n in 0..12 {
            if let Some(fill) = cache.reserve(object(&format!("k{n}")), version(1, A), 100) {
                fills.push(fill);
            }
            let usage = cache.usage();
            assert!(usage.bytes + usage.reserved <= 800, "{usage:?}");
        }
        let usage = cache.usage();
        assert_eq!(fills.len(), 8);
        assert_eq!((usage.bytes, usage.reserved), (0, 800));
        assert_eq!((usage.evictions, usage.refused), (1, 4));
        // A fill's buffers grow with what streamed: nothing yet.
        assert!(format!("{:?}", fills[0]).contains("len: 0"));
        // A second fill of a version already filling is refused, even with
        // room.
        drop(fills.pop());
        assert!(cache.reserve(object("k0"), version(1, A), 100).is_none());
        assert_eq!(cache.usage().refused, 5);
        // Fills that break off or overrun release their room and keep
        // nothing; the rest keep their objects.
        let mut broken = fills.pop().unwrap();
        broken.push(&Bytes::from(vec![1; 60]));
        drop(broken);
        let mut overrun = fills.pop().unwrap();
        overrun.push(&Bytes::from(vec![1; 60]));
        overrun.push(&Bytes::from(vec![1; 60]));
        overrun.finish();
        assert_eq!(cache.usage().reserved, 500);
        for mut fill in fills.drain(..) {
            fill.push(&Bytes::from(vec![2; 40]));
            fill.push(&Bytes::from(vec![3; 60]));
            fill.finish();
        }
        let usage = cache.usage();
        assert_eq!((usage.bytes, usage.reserved, usage.entries), (500, 0, 5));
        // A range across pieces is copied, one within a piece sliced.
        let across = cache.get(&object("k0"), &version(1, A), 30..50).unwrap();
        assert_eq!(&across[..], &[[2u8; 10], [3u8; 10]].concat()[..]);
        let within = cache.get(&object("k0"), &version(1, A), 50..60).unwrap();
        assert_eq!(&within[..], &[3u8; 10]);
        let empty = cache.get(&object("k0"), &version(1, A), 5..5).unwrap();
        assert!(empty.is_empty());
    }

    #[test]
    fn only_the_version_kept_is_served() {
        let cache = HotCache::new(800);
        assert!(cache.admits(100));
        assert!(!cache.admits(101));
        assert!(!cache.admits(0));
        insert(
            &cache,
            object("k"),
            version(1, A),
            Bytes::from_static(b"first"),
        );
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
        insert(
            &cache,
            object("k"),
            version(2, B),
            Bytes::from_static(b"second"),
        );
        // A read of the earlier version that finishes later keeps nothing.
        insert(
            &cache,
            object("k"),
            version(1, A),
            Bytes::from_static(b"first"),
        );
        assert_eq!(cache.get(&object("k"), &version(1, A), 0..5), None);
        insert(
            &cache,
            object("k"),
            version(2, B),
            Bytes::from_static(b"second"),
        );
        insert(
            &cache,
            object("k"),
            version(3, A),
            Bytes::from_static(b"third"),
        );
        assert_eq!(cache.usage().bytes, 5);
        assert_eq!(cache.get(&object("k"), &version(2, B), 0..6), None);
        assert_eq!(cache.usage().bytes, 0);
        // Objects the cache does not admit are not kept.
        insert(
            &cache,
            object("k"),
            version(4, A),
            Bytes::from(vec![0; 101]),
        );
        assert_eq!(cache.usage().entries, 0);
        insert(
            &HotCache::new(0),
            object("k"),
            version(1, A),
            Bytes::from_static(b"x"),
        );
    }

    #[test]
    fn the_least_recently_used_are_evicted_over_the_capacity() {
        let registry = MetricsRegistry::new();
        let cache = HotCache::with_metrics(320, HotCacheMetrics::register(&registry));
        assert_eq!(cache.capacity(), 320);
        for (n, key) in ["a", "b", "c", "d"].into_iter().enumerate() {
            insert(
                &cache,
                object(key),
                version(n as u64, A),
                Bytes::from(vec![0; 40]),
            );
        }
        assert_eq!(cache.usage().bytes, 160);
        // `a` is used, so `b` is now the least recently used.
        assert!(cache.get(&object("a"), &version(0, A), 0..40).is_some());
        for (n, key) in ["e", "f", "g", "h"].into_iter().enumerate() {
            insert(
                &cache,
                object(key),
                version(n as u64, A),
                Bytes::from(vec![0; 40]),
            );
        }
        assert_eq!(cache.usage().bytes, 320);
        insert(&cache, object("i"), version(1, A), Bytes::from(vec![0; 40]));
        let usage = cache.usage();
        assert_eq!((usage.bytes, usage.entries, usage.evictions), (320, 8, 1));
        assert!(cache.get(&object("b"), &version(1, A), 0..40).is_none());
        assert!(cache.get(&object("a"), &version(0, A), 0..40).is_some());
        let text = registry.encode().unwrap();
        for line in [
            "skys3_hot_cache_bytes 320",
            "skys3_hot_cache_filling_bytes 0",
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
        insert(
            &cache,
            object("k"),
            version(1, A),
            Bytes::from_static(b"first"),
        );
        cache.ignore_versions();
        assert!(cache.get(&object("k"), &version(2, B), 0..5).is_some());
    }
}
