//! The clean cache (§9.3): clean payload is kept on up to `clean_copies`
//! replicas and evicted least recently used first within the node's bound,
//! dirty payload never is, and a restarted node finds its clean entries
//! again.

mod support;

use std::sync::Arc;

use skys3_index::{EntryState, Index, Payload};
use skys3_io::{SimDisk, SimMount};
use skys3_log::SegmentLog;
use skys3_shard::{CacheMetrics, CacheSettings, CleanCache, Shard, ShardSet};
use skys3_types::{NodeId, ShardConfig};
use support::{
    apply, at, delete, extent, flushed, index_config, open_log, pool, put, record, runtime, shard,
};

/// A cache of at most `max_bytes`, with no disk space to account.
fn cache(max_bytes: u64) -> CleanCache {
    CleanCache::new(
        CacheSettings {
            max_bytes,
            reserve_fraction: 0.0,
        },
        CacheMetrics::default(),
    )
}

/// A shard set over `index` and `log`, reporting to `cache`.
async fn set(
    index: &Arc<Index>,
    log: &SegmentLog<SimMount>,
    cache: &CleanCache,
) -> ShardSet<SimMount> {
    let set = ShardSet::new(Arc::clone(index), log.clone(), pool());
    set.use_cache(cache).await;
    set
}

/// The state of `key` in `shard`, and whether its bytes are readable
/// here, read without using the entry as a client read would.
async fn state(shard: &Shard<SimMount>, key: &str) -> (EntryState, bool) {
    let read = shard.index().read().unwrap();
    let entry = read.entry(shard.shard(), key).unwrap().unwrap();
    let readable = match entry.object.unwrap().payload {
        Payload::Inline(position) => shard.payload(position).await.is_ok(),
        Payload::Extents(extents) => {
            let mut all = true;
            for extent in extents {
                all &= shard.payload(extent.position).await.is_ok();
            }
            all
        }
        _ => false,
    };
    (entry.state, readable)
}

fn key(n: usize) -> String {
    format!("k{n}")
}

#[test]
fn a_workload_larger_than_the_cache_keeps_every_dirty_byte() {
    runtime().block_on(async {
        let disk = SimDisk::new(5);
        let log = open_log(disk.mount()).await;
        let index =
            Arc::new(Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap());
        let cache = cache(100);
        let set = set(&index, &log, &cache).await;
        let shard = set
            .open(&support::config(&support::shard(0), 1))
            .await
            .unwrap();
        let mut versions = Vec::new();
        for n in 0..8 {
            let written = shard.commit(put(&key(n), 30, n as u64)).await.unwrap();
            versions.push(written.position);
        }
        // 240 dirty bytes, more than the cache holds, and none clean.
        assert_eq!(cache.reclaim(&set).await, 0);
        assert_eq!(cache.usage().bytes, 0);

        // Five are flushed, and a read of the first makes the second the
        // least recently used.
        for (n, version) in versions.iter().enumerate().take(5) {
            shard
                .commit(flushed(&key(n), version.seq.get(), false))
                .await
                .unwrap();
        }
        assert_eq!(cache.usage().bytes, 150);
        shard.entry(&key(0)).await.unwrap();
        assert_eq!(cache.reclaim(&set).await, 2);
        let usage = cache.usage();
        assert_eq!((usage.bytes, usage.entries, usage.evictions), (90, 3, 2));
        for (n, expected) in [
            (0, (EntryState::Clean, true)),
            (1, (EntryState::Evicted, false)),
            (2, (EntryState::Evicted, false)),
            (3, (EntryState::Clean, true)),
            (4, (EntryState::Clean, true)),
        ] {
            assert_eq!(state(&shard, &key(n)).await, expected, "{}", key(n));
        }
        for n in 5..8 {
            assert_eq!(state(&shard, &key(n)).await, (EntryState::Dirty, true));
        }

        // An evicted key is filled again, and the oldest clean one goes.
        let filled = shard.append_extent(extent(&key(1), 0, 30)).await.unwrap();
        shard
            .fill(&key(1), versions[1], Payload::Extents(vec![filled]))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cache.usage().bytes, 120);
        assert_eq!(cache.reclaim(&set).await, 1);
        assert_eq!(state(&shard, &key(1)).await, (EntryState::Clean, true));
        assert_eq!(state(&shard, &key(3)).await.0, EntryState::Evicted);
        // A write makes a clean entry dirty, and the cache forgets it.
        shard.commit(delete(&key(4))).await.unwrap();
        assert_eq!(cache.usage().bytes, 60);

        // A restarted node scans its clean entries, oldest first by when
        // they were last modified, and evicts within its bound.
        shard.close().await.unwrap();
        drop((shard, set));
        let cache = self::cache(40);
        let set = self::set(&index, &log, &cache).await;
        let shard = set
            .open(&support::config(&support::shard(0), 1))
            .await
            .unwrap();
        assert_eq!(cache.reclaim(&set).await, 1);
        assert_eq!(state(&shard, &key(0)).await.0, EntryState::Evicted);
        assert_eq!(state(&shard, &key(1)).await, (EntryState::Clean, true));
        for n in 5..8 {
            assert_eq!(state(&shard, &key(n)).await, (EntryState::Dirty, true));
        }
        assert_eq!(cache.usage().bytes, 30);

        // A dropped shard leaves the cache.
        set.remove(&shard.shard().clone()).await.unwrap();
        assert_eq!(cache.usage().bytes, 0);
    });
}

#[test]
fn a_cache_attached_late_finds_the_open_shards() {
    runtime().block_on(async {
        let disk = SimDisk::new(6);
        let log = open_log(disk.mount()).await;
        let index =
            Arc::new(Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap());
        let set = ShardSet::new(Arc::clone(&index), log.clone(), pool());
        let shard = set
            .open(&support::config(&support::shard(0), 1))
            .await
            .unwrap();
        let written = shard.commit(put("k", 10, 1)).await.unwrap().position;
        shard
            .commit(flushed("k", written.seq.get(), false))
            .await
            .unwrap();
        assert!(set.cache().is_none());
        let cache = cache(0);
        set.use_cache(&cache).await;
        // A second cache is ignored.
        set.use_cache(&self::cache(1000)).await;
        assert_eq!(cache.reclaim(&set).await, 1);
        assert_eq!(state(&shard, "k").await.0, EntryState::Evicted);
        assert!(format!("{:?}", set.cache().unwrap()).contains("evictions: 1"));
    });
}

/// A configuration of shard 0 whose primary is node 1, with members
/// nodes 1 to 3 and learner node 4.
fn replicated() -> ShardConfig {
    let node = |n: u8| format!("node-{n}").parse::<NodeId>().unwrap();
    ShardConfig {
        members: vec![node(1), node(2), node(3)],
        learners: vec![node(4)],
        replicas: 3,
        ..support::config(&shard(0), 1)
    }
}

#[test]
fn members_beyond_clean_copies_drop_their_copies_and_keep_dirty_ones() {
    runtime().block_on(async {
        for (n, keeps) in [(1, true), (2, true), (3, false), (4, false)] {
            let disk = SimDisk::new(n);
            let log = open_log(disk.mount()).await;
            let index =
                Arc::new(Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap());
            // What the replica applied: `a` flushed, `b` dirty.
            for (seq, body) in [
                (1, put("a", 20, 1)),
                (2, put("b", 20, 2)),
                (3, flushed("a", 1, false)),
            ] {
                apply(&index, &record(at(seq), body));
            }
            let cache = cache(1000);
            cache.set_clean_copies([(shard(0).bucket.clone(), 2)]);
            let set = set(&index, &log, &cache).await;
            let node = format!("node-{n}").parse::<NodeId>().unwrap();
            let replica = set.open_replica(&replicated(), &node).await.unwrap();
            assert_eq!(cache.reclaim(&set).await, usize::from(!keeps), "node {n}");
            let read = index.read().unwrap();
            let a = read.entry(&shard(0), "a").unwrap().unwrap();
            let b = read.entry(&shard(0), "b").unwrap().unwrap();
            let expected = if keeps {
                EntryState::Clean
            } else {
                EntryState::Evicted
            };
            assert_eq!(a.state, expected, "node {n}");
            assert_eq!(b.state, EntryState::Dirty, "node {n}");
            assert_eq!(
                b.object.unwrap().payload,
                Payload::Inline(at(2)),
                "node {n}"
            );
            assert_eq!(cache.usage().bytes, if keeps { 20 } else { 0 });
            drop(replica);
        }
    });
}

#[test]
fn copies_are_ranked_by_the_configuration() {
    // Rank 0 is the primary, wherever it is in the member list.
    runtime().block_on(async {
        let disk = SimDisk::new(9);
        let log = open_log(disk.mount()).await;
        let index =
            Arc::new(Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap());
        apply(&index, &record(at(1), put("a", 20, 1)));
        apply(&index, &record(at(2), flushed("a", 1, false)));
        let mut config = replicated();
        config.primary = "node-3".parse().unwrap();
        let cache = cache(1000);
        cache.set_clean_copies([(shard(0).bucket.clone(), 1)]);
        let set = set(&index, &log, &cache).await;
        // Node 1 is the first member after the primary: rank 1, and one
        // copy is kept, on the primary.
        let node = "node-1".parse::<NodeId>().unwrap();
        let _replica = set.open_replica(&config, &node).await.unwrap();
        assert_eq!(cache.reclaim(&set).await, 1);
        let entry = index
            .read()
            .unwrap()
            .entry(&shard(0), "a")
            .unwrap()
            .unwrap();
        assert_eq!(entry.state, EntryState::Evicted);
        assert_eq!(entry.version, at(1));
    });
}

#[test]
fn a_member_opened_before_the_bucket_policy_keeps_its_copies() {
    // A restarted node opens its replicas, and scans them, before it
    // reads the buckets' `clean_copies`: the scan must not evict copies
    // the policy keeps.
    runtime().block_on(async {
        let disk = SimDisk::new(2);
        let log = open_log(disk.mount()).await;
        let index =
            Arc::new(Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap());
        apply(&index, &record(at(1), put("a", 20, 1)));
        apply(&index, &record(at(2), flushed("a", 1, false)));
        let cache = cache(1000);
        let set = set(&index, &log, &cache).await;
        // Node 2 ranks 1, within two copies.
        let node = "node-2".parse::<NodeId>().unwrap();
        let replica = set.open_replica(&replicated(), &node).await.unwrap();
        assert_eq!(cache.reclaim(&set).await, 0);
        assert_eq!(cache.usage().bytes, 20);
        cache.set_clean_copies([(shard(0).bucket.clone(), 2)]);
        assert_eq!(cache.reclaim(&set).await, 0);
        assert_eq!(state(&replica, "a").await.0, EntryState::Clean);
        // The policy lowered to one copy evicts it.
        cache.set_clean_copies([(shard(0).bucket.clone(), 1)]);
        assert_eq!(cache.reclaim(&set).await, 1);
        assert_eq!(state(&replica, "a").await.0, EntryState::Evicted);
        assert_eq!(cache.usage().bytes, 0);
    });
}
