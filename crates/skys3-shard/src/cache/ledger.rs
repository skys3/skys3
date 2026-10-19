//! [`CleanCache`]: the clean payload a node holds, and its eviction (§9.3).

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Unit;
use skys3_io::Disk;
use skys3_io::disk::Space;
use skys3_log::ShardRef;
use skys3_obs::MetricsRegistry;
use skys3_types::{BucketId, EpochSeq, Label};
use tokio::sync::Notify;

use crate::set::ShardSet;

/// The cache's bounds: `[cache]` in the configuration (§14).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CacheSettings {
    /// `cache_max_bytes_per_node`: the most clean payload the node keeps.
    pub max_bytes: u64,
    /// `reserve_fraction`: the share of each disk kept free of clean
    /// payload, for learner catch-up and filesystem overhead.
    pub reserve_fraction: f64,
}

impl Default for CacheSettings {
    /// The configuration's defaults: 1 TiB, and a tenth of each disk.
    fn default() -> Self {
        Self {
            max_bytes: 1 << 40,
            reserve_fraction: 0.10,
        }
    }
}

/// What the cache holds, as [`CleanCache::usage`] reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CacheUsage {
    /// The clean payload kept, in bytes.
    pub bytes: u64,
    /// The clean entries kept.
    pub entries: u64,
    /// The most the node keeps: `cache_max_bytes_per_node`, or less while
    /// its disks have less room.
    pub limit: u64,
    /// Clean payloads evicted so far.
    pub evictions: u64,
}

/// The cache metrics (`docs/skys3-metrics.md`): `skys3_clean_cache_bytes`,
/// `skys3_clean_cache_limit_bytes`, and
/// `skys3_clean_cache_evictions_total`. The default metrics belong to no
/// registry.
#[derive(Debug, Clone, Default)]
pub struct CacheMetrics {
    bytes: Gauge,
    limit: Gauge,
    evictions: Counter,
}

impl CacheMetrics {
    /// Registers the cache metrics in `registry`.
    #[must_use]
    pub fn register(registry: &MetricsRegistry) -> Self {
        let metrics = Self::default();
        registry.register_with_unit(
            "clean_cache",
            "Bytes of clean payload this node keeps as cache.",
            Unit::Bytes,
            metrics.bytes.clone(),
        );
        registry.register_with_unit(
            "clean_cache_limit",
            "The most clean payload this node keeps: cache_max_bytes_per_node, or less while its \
             disks have less room above reserve_fraction.",
            Unit::Bytes,
            metrics.limit.clone(),
        );
        registry.register(
            "clean_cache_evictions",
            "Clean payloads this node evicted, by LRU or beyond clean_copies.",
            metrics.evictions.clone(),
        );
        metrics
    }
}

/// The clean payload one node keeps, and its eviction (§9.3).
///
/// Every shard replica open on the node reports to it
/// ([`ShardSet::use_cache`]): which entries became clean with local bytes,
/// by a `FLUSHED` record or a fill, and which stopped being so, by a write,
/// an `ADOPT`, or eviction. Reads of an entry through
/// [`Shard::entry`](crate::Shard::entry) mark it used. A replica opened on
/// a node with entries already clean reports them from a scan, as used
/// when they were last modified, before anything used since.
///
/// - **Copies.** A replica keeps a clean payload only if its rank is below
///   its bucket's `clean_copies` ([`CleanCache::set_clean_copies`]): the
///   primary, or a replica alone, is rank 0, and the other members follow
///   in the order of the configuration. Learners keep none. A clean entry
///   whose bytes are not on the node is evicted too, so that its state
///   says so.
/// - **Buckets not told.** Only the buckets the cache is told a
///   `clean_copies` for are evicted ([`CleanCache::evicts`]). The others
///   keep every payload, and the cache neither counts nor evicts it: a
///   `local` bucket, whose replicas are its durable home even once a
///   backup target made its entries clean (§8.9), and a bucket whose
///   policy the node has not read yet, since it opens its replicas, and
///   receives `FLUSHED` records of a new bucket, before it reads the
///   bucket's policy, and a copy evicted then would be lost for good. A
///   bucket's replicas are scanned once it is told.
/// - **Bounds.** The payloads kept are evicted least recently used first
///   while they hold more than `cache_max_bytes_per_node`, or while one
///   disk's hold more than its room. A disk's room is what clean payload
///   may take of it under the §9.3 capacity model: its size less
///   `reserve_fraction` of it, less everything on it that is not clean
///   cache ([`CleanCache::set_space`]). Payload this node evicted counts as
///   free, though compaction reclaims it later (§10.3).
/// - **Dirty payload is never evicted**: eviction goes through
///   [`Shard::evict`](crate::Shard::evict), which moves only an entry that
///   is still clean at the version the cache knew.
///
/// [`CleanCache::run`] evicts in the background. What the cache knows is
/// held in memory only: a restarted node scans its shards again.
///
/// Cloning a cache returns another handle to the same cache.
#[derive(Clone)]
pub struct CleanCache {
    inner: Arc<Inner>,
}

impl fmt::Debug for CleanCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CleanCache")
            .field("usage", &self.usage())
            .finish_non_exhaustive()
    }
}

struct Inner {
    ledger: Mutex<Ledger>,
    wake: Notify,
    metrics: CacheMetrics,
}

/// What a replica reports about one key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Note {
    /// The key's entry is clean at `version`, an object of `size` bytes;
    /// `held` says whether its bytes are on the node.
    Clean {
        key: String,
        version: EpochSeq,
        size: u64,
        held: bool,
        /// When the entry was last used, in milliseconds since the Unix
        /// epoch, if not now: a scan's entries rank by when they were last
        /// modified.
        last_used: Option<u64>,
    },
    /// The key's entry is not clean, or is gone.
    Gone { key: String },
}

/// An entry chosen for eviction.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Victim {
    shard: ShardRef,
    key: String,
    version: EpochSeq,
    size: u64,
    disk: Label,
}

/// The share of a disk's clean payload, its most recently used, that
/// compaction copies rather than evicts (§10.3).
const HOT_SHARE: f64 = 0.5;

/// Which clean entries of a disk compaction copies: those used at or
/// after `from`, if any ([`CleanCache::hot`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Hot {
    from: Option<Tick>,
}

/// When an entry was last used, for LRU order: the time it was last
/// modified, for an entry a scan found, or [`NOW`] for one used since;
/// then the order the cache learned it in, which also makes ticks unique.
type Tick = (u64, u64);

/// The first half of the tick of an entry used while the node runs.
const NOW: u64 = u64::MAX;

#[derive(Debug)]
struct Slot {
    version: EpochSeq,
    size: u64,
    tick: Tick,
}

/// The clean entries of one replica.
#[derive(Debug)]
struct Held {
    disk: Label,
    /// The replica's rank among the holders of clean copies, as it last
    /// reported it: `None` for a learner, or before any report.
    rank: Option<u8>,
    keys: HashMap<String, Slot>,
}

/// What clean payload takes of one disk.
#[derive(Debug, Default)]
struct DiskBytes {
    /// Clean payload kept.
    cached: u64,
    /// Payload evicted since the node started, which compaction has not
    /// reclaimed yet as far as the cache knows.
    released: u64,
    /// The most clean payload the disk may hold, once its space is known.
    room: Option<u64>,
}

#[derive(Debug)]
struct Ledger {
    settings: CacheSettings,
    clean_copies: HashMap<BucketId, u8>,
    shards: HashMap<ShardRef, Held>,
    /// Each disk's clean entries, least recently used first.
    lru: BTreeMap<Label, BTreeMap<Tick, (ShardRef, String)>>,
    disks: BTreeMap<Label, DiskBytes>,
    bytes: u64,
    entries: u64,
    next: u64,
    /// Entries to evict whatever the bounds: copies beyond `clean_copies`,
    /// and clean entries without their bytes.
    doomed: VecDeque<Victim>,
    /// Replicas whose clean entries are still to be scanned.
    scans: VecDeque<ShardRef>,
    evictions: u64,
}

impl CleanCache {
    /// An empty cache within `settings`, which exports `metrics`.
    #[must_use]
    pub fn new(settings: CacheSettings, metrics: CacheMetrics) -> Self {
        let ledger = Ledger {
            settings,
            clean_copies: HashMap::new(),
            shards: HashMap::new(),
            lru: BTreeMap::new(),
            disks: BTreeMap::new(),
            bytes: 0,
            entries: 0,
            next: 0,
            doomed: VecDeque::new(),
            scans: VecDeque::new(),
            evictions: 0,
        };
        let cache = Self {
            inner: Arc::new(Inner {
                ledger: Mutex::new(ledger),
                wake: Notify::new(),
                metrics,
            }),
        };
        cache.publish(&cache.ledger());
        cache
    }

    /// Sets each bucket's `clean_copies`, replacing what was set before:
    /// the buckets whose clean payload the node evicts. Leave out every
    /// `local` bucket.
    ///
    /// The setting applies to entries as they become clean, to a replica's
    /// entries when it opens, and, for a bucket whose setting changed, to
    /// the copies kept already: those of replicas ranked at or beyond it
    /// are evicted. A copy evicted is not brought back when the setting
    /// grows. The replicas of a bucket set for the first time are scanned
    /// at the next reclaim. A bucket left out is no longer evicted: its
    /// replicas keep every payload, which the cache stops counting.
    pub fn set_clean_copies(&self, copies: impl IntoIterator<Item = (BucketId, u8)>) {
        let mut ledger = self.ledger();
        let copies: HashMap<BucketId, u8> = copies.into_iter().collect();
        let mut new = Vec::new();
        let mut changed = Vec::new();
        for (bucket, &count) in &copies {
            match ledger.clean_copies.get(bucket) {
                None => new.push(bucket.clone()),
                Some(&old) if old != count => changed.push((bucket.clone(), count)),
                Some(_) => {}
            }
        }
        let gone: Vec<BucketId> = ledger
            .clean_copies
            .keys()
            .filter(|bucket| !copies.contains_key(*bucket))
            .cloned()
            .collect();
        ledger.clean_copies = copies;
        for bucket in gone {
            ledger.keep_all(&bucket);
        }
        for (bucket, copies) in changed {
            ledger.doom_beyond(&bucket, copies);
        }
        let scans = ledger.rescan(&new);
        let wake = scans || ledger.over();
        self.publish(&ledger);
        drop(ledger);
        if wake {
            self.inner.wake.notify_one();
        }
    }

    /// The `clean_copies` of `bucket`, if the cache was told it.
    #[must_use]
    pub fn clean_copies(&self, bucket: &BucketId) -> Option<u8> {
        self.ledger().clean_copies.get(bucket).copied()
    }

    /// Whether the node evicts clean payload of `bucket`: whether the cache
    /// was told its `clean_copies` ([`CleanCache::set_clean_copies`]).
    /// The replicas of other buckets keep every payload.
    #[must_use]
    pub fn evicts(&self, bucket: &BucketId) -> bool {
        self.ledger().clean_copies.contains_key(bucket)
    }

    /// Records the space of `disk`, as last read: the room it has for
    /// clean payload is its available bytes, plus the clean payload it
    /// holds and the payload evicted from it, less `reserve_fraction` of
    /// its size.
    pub fn set_space(&self, disk: &Label, space: Space) {
        let mut ledger = self.ledger();
        let reserve = ledger.settings.reserve_fraction.clamp(0.0, 1.0) * space.total as f64;
        let bytes = ledger.disks.entry(disk.clone()).or_default();
        let held = space
            .available
            .saturating_add(bytes.cached + bytes.released);
        // A float of at most `space.total`, so it fits.
        let room = held.saturating_sub(reserve as u64);
        bytes.room = Some(room);
        let over = ledger.over();
        self.publish(&ledger);
        drop(ledger);
        if over {
            self.inner.wake.notify_one();
        }
    }

    /// What the cache holds now.
    #[must_use]
    pub fn usage(&self) -> CacheUsage {
        let ledger = self.ledger();
        CacheUsage {
            bytes: ledger.bytes,
            entries: ledger.entries,
            limit: ledger.limit(),
            evictions: ledger.evictions,
        }
    }

    /// Scans the replicas of `set` that opened since the last call, and
    /// evicts until the cache is within its bounds. Returns how many
    /// payloads it evicted.
    pub async fn reclaim<D: Disk>(&self, set: &ShardSet<D>) -> usize {
        loop {
            // The guard is dropped before the scan awaits.
            let next = self.ledger().scans.pop_front();
            let Some(shard) = next else { break };
            self.scan(set, &shard).await;
        }
        let mut evicted = 0;
        loop {
            let next = self.ledger().victim();
            let Some(victim) = next else { break };
            evicted += usize::from(self.evict(set, victim).await);
        }
        evicted
    }

    /// Reclaims ([`CleanCache::reclaim`]) whenever a replica opens or the
    /// cache goes over its bounds, until the future is dropped.
    pub async fn run<D: Disk>(&self, set: ShardSet<D>) {
        loop {
            self.reclaim(&set).await;
            self.inner.wake.notified().await;
        }
    }

    /// Starts following the replica of `shard`, whose records are on
    /// `disk`: its clean entries are scanned at the next reclaim.
    pub(crate) fn attach(&self, shard: &ShardRef, disk: &Label) {
        let mut ledger = self.ledger();
        ledger.forget_shard(shard);
        ledger.disks.entry(disk.clone()).or_default();
        ledger.shards.insert(
            shard.clone(),
            Held {
                disk: disk.clone(),
                rank: None,
                keys: HashMap::new(),
            },
        );
        ledger.scans.push_back(shard.clone());
        self.publish(&ledger);
        drop(ledger);
        self.inner.wake.notify_one();
    }

    /// Forgets the replica of `shard`, which the node dropped.
    pub(crate) fn forget_shard(&self, shard: &ShardRef) {
        let mut ledger = self.ledger();
        ledger.forget_shard(shard);
        self.publish(&ledger);
    }

    /// Takes `notes` from the replica of `shard`, whose rank among the
    /// holders of clean copies is `rank` (`None` for a learner).
    pub(crate) fn note(&self, shard: &ShardRef, rank: Option<u8>, notes: Vec<Note>) {
        if notes.is_empty() {
            return;
        }
        let mut ledger = self.ledger();
        // A bucket not told keeps every payload, which the cache does not
        // count; it is scanned once told.
        let Some(&copies) = ledger.clean_copies.get(&shard.bucket) else {
            return;
        };
        let Some(held) = ledger.shards.get_mut(shard) else {
            return;
        };
        held.rank = rank;
        let disk = held.disk.clone();
        let keeps = rank.is_some_and(|rank| rank < copies);
        for note in notes {
            match note {
                Note::Clean {
                    key,
                    version,
                    size,
                    held,
                    last_used,
                } => {
                    if keeps && held {
                        let tick = (last_used.unwrap_or(NOW), ledger.next_tick());
                        ledger.track(shard, &disk, key, version, size, tick);
                    } else {
                        ledger.remove(shard, &key);
                        ledger.doomed.push_back(Victim {
                            shard: shard.clone(),
                            key,
                            version,
                            size,
                            disk: disk.clone(),
                        });
                    }
                }
                Note::Gone { key } => {
                    ledger.remove(shard, &key);
                }
            }
        }
        let over = ledger.over();
        self.publish(&ledger);
        drop(ledger);
        if over {
            self.inner.wake.notify_one();
        }
    }

    /// Which clean entries of `disk` compaction copies rather than evicts
    /// (§10.3): the most recently used ones that together hold at most
    /// [`HOT_SHARE`] of the clean payload the cache keeps on the disk, and
    /// none while the cache is over its bounds.
    pub(crate) fn hot(&self, disk: &Label) -> Hot {
        let ledger = self.ledger();
        if ledger.over() {
            return Hot { from: None };
        }
        let cached = ledger.disks.get(disk).map_or(0, |bytes| bytes.cached);
        // The share is a fraction of a byte count, which fits a u64.
        let budget = (cached as f64 * HOT_SHARE) as u64;
        let mut held = 0u64;
        let mut from = None;
        for (&tick, (shard, key)) in ledger.lru.get(disk).into_iter().flatten().rev() {
            let size = ledger
                .shards
                .get(shard)
                .and_then(|held| held.keys.get(key))
                .map_or(0, |slot| slot.size);
            held = held.saturating_add(size);
            if held > budget {
                break;
            }
            from = Some(tick);
        }
        Hot { from }
    }

    /// Whether the cache keeps the clean version `version` of `key` and
    /// ranks it among the entries `hot` names.
    pub(crate) fn is_hot(&self, hot: Hot, shard: &ShardRef, key: &str, version: EpochSeq) -> bool {
        let Some(from) = hot.from else {
            return false;
        };
        let ledger = self.ledger();
        ledger
            .shards
            .get(shard)
            .and_then(|held| held.keys.get(key))
            .is_some_and(|slot| slot.version == version && slot.tick >= from)
    }

    /// Takes note that compaction removed `bytes` of payload that no entry
    /// held from `disk`: the disk's room no longer counts them as free on
    /// top of its available space (§9.3, §10.3). It counts every such
    /// byte, evicted or replaced, so the room never exceeds what the disk
    /// really has.
    pub(crate) fn reclaimed(&self, disk: &Label, bytes: u64) {
        let mut ledger = self.ledger();
        if let Some(disk) = ledger.disks.get_mut(disk) {
            disk.released = disk.released.saturating_sub(bytes);
        }
    }

    /// Marks the clean entry of `key`, if the cache keeps it, as used now.
    pub(crate) fn touch(&self, shard: &ShardRef, key: &str) {
        let mut ledger = self.ledger();
        let tick = (NOW, ledger.next_tick());
        let Some(held) = ledger.shards.get_mut(shard) else {
            return;
        };
        let disk = held.disk.clone();
        let Some(slot) = held.keys.get_mut(key) else {
            return;
        };
        let old = std::mem::replace(&mut slot.tick, tick);
        if let Some(lru) = ledger.lru.get_mut(&disk)
            && let Some(entry) = lru.remove(&old)
        {
            lru.insert(tick, entry);
        }
    }

    /// Reads the clean entries of the replica of `shard` open in `set`, a
    /// page at a time.
    async fn scan<D: Disk>(&self, set: &ShardSet<D>, shard: &ShardRef) {
        let Some(replica) = set.get(shard).await else {
            return;
        };
        let mut after = None;
        loop {
            match replica.scan_cache(after).await {
                Ok(Some(next)) => after = Some(next),
                Ok(None) => return,
                Err(error) => {
                    tracing::warn!(%shard, %error, "cannot scan a shard's clean entries");
                    return;
                }
            }
        }
    }

    /// Evicts `victim`, and returns whether it did.
    async fn evict<D: Disk>(&self, set: &ShardSet<D>, victim: Victim) -> bool {
        let Some(replica) = set.get(&victim.shard).await else {
            return false;
        };
        match replica.evict(&victim.key, victim.version).await {
            Ok(Ok(())) => {
                let mut ledger = self.ledger();
                ledger.disks.entry(victim.disk).or_default().released += victim.size;
                ledger.evictions += 1;
                self.inner.metrics.evictions.inc();
                self.publish(&ledger);
                true
            }
            // A write, an `ADOPT`, or another eviction came first.
            Ok(Err(refusal)) => {
                tracing::debug!(shard = %victim.shard, key = victim.key, %refusal,
                    "an entry was not evicted");
                false
            }
            Err(error) => {
                tracing::debug!(shard = %victim.shard, key = victim.key, %error,
                    "an entry was not evicted");
                false
            }
        }
    }

    fn ledger(&self) -> MutexGuard<'_, Ledger> {
        // Every update leaves the ledger consistent before it can panic.
        self.inner
            .ledger
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn publish(&self, ledger: &Ledger) {
        let gauge = |value: u64| i64::try_from(value).unwrap_or(i64::MAX);
        self.inner.metrics.bytes.set(gauge(ledger.bytes));
        self.inner.metrics.limit.set(gauge(ledger.limit()));
    }
}

impl Ledger {
    fn next_tick(&mut self) -> u64 {
        self.next += 1;
        self.next
    }

    /// Keeps the clean entry of `key` at `version`, used at `tick`, or
    /// later if it was used since.
    fn track(
        &mut self,
        shard: &ShardRef,
        disk: &Label,
        key: String,
        version: EpochSeq,
        size: u64,
        tick: Tick,
    ) {
        let tick = match self.remove(shard, &key) {
            Some(old) if old.version == version => tick.max(old.tick),
            _ => tick,
        };
        let Some(held) = self.shards.get_mut(shard) else {
            return;
        };
        held.keys.insert(
            key.clone(),
            Slot {
                version,
                size,
                tick,
            },
        );
        self.lru
            .entry(disk.clone())
            .or_default()
            .insert(tick, (shard.clone(), key));
        self.disks.entry(disk.clone()).or_default().cached += size;
        self.bytes += size;
        self.entries += 1;
    }

    /// Stops keeping the entry of `key`, and returns what was kept.
    fn remove(&mut self, shard: &ShardRef, key: &str) -> Option<Slot> {
        let held = self.shards.get_mut(shard)?;
        let slot = held.keys.remove(key)?;
        let disk = held.disk.clone();
        if let Some(lru) = self.lru.get_mut(&disk) {
            lru.remove(&slot.tick);
        }
        let bytes = self.disks.entry(disk).or_default();
        bytes.cached = bytes.cached.saturating_sub(slot.size);
        self.bytes = self.bytes.saturating_sub(slot.size);
        self.entries = self.entries.saturating_sub(1);
        Some(slot)
    }

    /// Evicts the copies kept by the replicas of `bucket` ranked at or
    /// beyond `copies`.
    fn doom_beyond(&mut self, bucket: &BucketId, copies: u8) {
        let beyond: Vec<(ShardRef, Label, Vec<String>)> = self
            .shards
            .iter()
            .filter(|(shard, held)| {
                &shard.bucket == bucket
                    && !held.keys.is_empty()
                    && held.rank.is_none_or(|rank| rank >= copies)
            })
            .map(|(shard, held)| {
                let mut keys: Vec<String> = held.keys.keys().cloned().collect();
                keys.sort_unstable();
                (shard.clone(), held.disk.clone(), keys)
            })
            .collect();
        for (shard, disk, keys) in beyond {
            for key in keys {
                if let Some(slot) = self.remove(&shard, &key) {
                    self.doomed.push_back(Victim {
                        shard: shard.clone(),
                        key,
                        version: slot.version,
                        size: slot.size,
                        disk: disk.clone(),
                    });
                }
            }
        }
    }

    /// Stops counting and evicting the payload of `bucket`, whose replicas
    /// now keep all of it.
    fn keep_all(&mut self, bucket: &BucketId) {
        let kept: Vec<(ShardRef, String)> = self
            .shards
            .iter()
            .filter(|(shard, _)| &shard.bucket == bucket)
            .flat_map(|(shard, held)| held.keys.keys().map(|key| (shard.clone(), key.clone())))
            .collect();
        for (shard, key) in kept {
            self.remove(&shard, &key);
        }
        self.doomed.retain(|victim| &victim.shard.bucket != bucket);
    }

    /// Queues a scan of every replica of `buckets`, and returns whether it
    /// queued any.
    fn rescan(&mut self, buckets: &[BucketId]) -> bool {
        let mut queued = false;
        for shard in self.shards.keys() {
            if buckets.contains(&shard.bucket) && !self.scans.contains(shard) {
                self.scans.push_back(shard.clone());
                queued = true;
            }
        }
        queued
    }

    fn forget_shard(&mut self, shard: &ShardRef) {
        let keys: Vec<String> = self
            .shards
            .get(shard)
            .map(|held| held.keys.keys().cloned().collect())
            .unwrap_or_default();
        for key in keys {
            self.remove(shard, &key);
        }
        self.shards.remove(shard);
        self.scans.retain(|scanned| scanned != shard);
        self.doomed.retain(|victim| &victim.shard != shard);
    }

    /// The most clean payload the node may keep: `cache_max_bytes_per_node`,
    /// or the room of its disks if that is less and every disk's is known.
    fn limit(&self) -> u64 {
        let rooms: Option<u64> = self
            .disks
            .values()
            .map(|disk| disk.room)
            .try_fold(0u64, |sum, room| Some(sum.saturating_add(room?)));
        match rooms {
            Some(rooms) if !self.disks.is_empty() => rooms.min(self.settings.max_bytes),
            _ => self.settings.max_bytes,
        }
    }

    /// Whether anything must be evicted.
    fn over(&self) -> bool {
        !self.doomed.is_empty()
            || self.bytes > self.settings.max_bytes
            || self.disks.values().any(DiskBytes::over)
    }

    /// The next entry to evict, which the cache then no longer keeps.
    fn victim(&mut self) -> Option<Victim> {
        if let Some(victim) = self.doomed.pop_front() {
            return Some(victim);
        }
        let disk = if self.bytes > self.settings.max_bytes {
            // The least recently used entry of the node.
            self.lru
                .iter()
                .filter_map(|(disk, lru)| Some((lru.first_key_value()?.0, disk)))
                .min()
                .map(|(_, disk)| disk.clone())?
        } else {
            self.disks
                .iter()
                .find(|(_, bytes)| bytes.over())
                .map(|(disk, _)| disk.clone())?
        };
        let (shard, key) = self.lru.get(&disk)?.first_key_value()?.1.clone();
        let slot = self.remove(&shard, &key)?;
        Some(Victim {
            shard,
            key,
            version: slot.version,
            size: slot.size,
            disk,
        })
    }
}

impl DiskBytes {
    fn over(&self) -> bool {
        self.room.is_some_and(|room| self.cached > room)
    }
}

#[cfg(test)]
mod tests;
