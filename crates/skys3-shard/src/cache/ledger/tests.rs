use skys3_types::{Epoch, Seq, ShardId};

use super::*;

fn shard(n: u8) -> ShardRef {
    ShardRef::new(BucketId::new("b-cache").unwrap(), ShardId::new(n))
}

fn disk(n: u8) -> Label {
    Label::new(format!("disk-{n}")).unwrap()
}

fn at(seq: u64) -> EpochSeq {
    EpochSeq::new(Epoch::new(1), Seq::new(seq))
}

fn clean(key: &str, seq: u64, size: u64) -> Note {
    Note::Clean {
        key: key.to_owned(),
        version: at(seq),
        size,
        held: true,
        last_used: None,
    }
}

fn gone(key: &str) -> Note {
    Note::Gone {
        key: key.to_owned(),
    }
}

/// A cache of at most `max_bytes`, with shards 0 and 1 on disks 0 and 1.
fn cache(max_bytes: u64) -> CleanCache {
    let cache = CleanCache::new(
        CacheSettings {
            max_bytes,
            reserve_fraction: 0.25,
        },
        CacheMetrics::default(),
    );
    cache.attach(&shard(0), &disk(0));
    cache.attach(&shard(1), &disk(1));
    cache.ledger().scans.clear();
    cache
}

/// The keys the cache would evict now, in order.
fn victims(cache: &CleanCache) -> Vec<(ShardRef, String)> {
    let mut ledger = cache.ledger();
    std::iter::from_fn(|| ledger.victim().map(|victim| (victim.shard, victim.key))).collect()
}

fn named(shard_n: u8, key: &str) -> (ShardRef, String) {
    (shard(shard_n), key.to_owned())
}

#[test]
fn the_least_recently_used_entries_go_first_once_the_cache_is_full() {
    let cache = cache(100);
    cache.note(
        &shard(0),
        Some(0),
        vec![clean("a", 1, 40), clean("b", 2, 40)],
    );
    cache.note(&shard(1), Some(0), vec![clean("c", 1, 40)]);
    assert_eq!(
        cache.usage(),
        CacheUsage {
            bytes: 120,
            entries: 3,
            limit: 100,
            evictions: 0,
        }
    );
    // A read of `a` makes `b` the oldest.
    cache.touch(&shard(0), "a");
    cache.touch(&shard(0), "missing");
    cache.touch(&shard(7), "a");
    assert_eq!(victims(&cache), [named(0, "b")]);
    assert_eq!(cache.usage().bytes, 80);
    assert!(victims(&cache).is_empty());
}

#[test]
fn entries_that_stop_being_clean_are_forgotten() {
    let cache = cache(100);
    cache.note(
        &shard(0),
        Some(0),
        vec![clean("a", 1, 60), clean("b", 2, 30)],
    );
    // An overwrite, and a newer clean version of the same key.
    cache.note(&shard(0), Some(0), vec![gone("a"), clean("b", 5, 50)]);
    assert_eq!(cache.usage().bytes, 50);
    assert_eq!(cache.usage().entries, 1);
    cache.note(&shard(0), Some(0), vec![clean("c", 6, 60)]);
    assert_eq!(victims(&cache), [named(0, "b")]);
    // Notes of a shard the cache does not follow are ignored.
    cache.note(&shard(9), Some(0), vec![clean("x", 1, 500)]);
    assert_eq!(cache.usage().bytes, 60);
    cache.forget_shard(&shard(0));
    assert_eq!(
        cache.usage(),
        CacheUsage {
            limit: 100,
            ..CacheUsage::default()
        }
    );
    cache.note(&shard(0), Some(0), vec![clean("c", 6, 60)]);
    assert_eq!(cache.usage().bytes, 0);
}

#[test]
fn copies_beyond_clean_copies_and_entries_without_bytes_are_evicted_at_once() {
    let cache = cache(1000);
    cache.set_clean_copies([(shard(0).bucket.clone(), 2)]);
    cache.note(&shard(0), Some(0), vec![clean("primary", 1, 10)]);
    cache.note(&shard(0), Some(1), vec![clean("member", 2, 10)]);
    cache.note(&shard(0), Some(2), vec![clean("third", 3, 10)]);
    cache.note(&shard(0), None, vec![clean("learner", 4, 10)]);
    let evicted_bytes = Note::Clean {
        key: "stub".to_owned(),
        version: at(5),
        size: 10,
        held: false,
        last_used: None,
    };
    cache.note(&shard(0), Some(0), vec![evicted_bytes]);
    assert_eq!(cache.usage().bytes, 20);
    assert_eq!(
        victims(&cache),
        [named(0, "third"), named(0, "learner"), named(0, "stub"),]
    );
    // A bucket the cache was not told about keeps one copy.
    cache.set_clean_copies([]);
    cache.note(&shard(0), Some(1), vec![clean("member", 2, 10)]);
    assert_eq!(victims(&cache), [named(0, "member")]);
    // A copy that was kept and is no longer is not counted twice.
    assert_eq!(cache.usage().bytes, 10);
}

#[test]
fn scanned_entries_rank_by_age_before_any_used_since() {
    let cache = cache(100);
    let scanned = |key: &str, seq, last_used| Note::Clean {
        key: key.to_owned(),
        version: at(seq),
        size: 30,
        held: true,
        last_used: Some(last_used),
    };
    cache.note(&shard(0), Some(0), vec![clean("live", 9, 30)]);
    cache.note(
        &shard(0),
        Some(0),
        vec![scanned("new", 1, 2_000), scanned("old", 2, 1_000)],
    );
    // A scan that finds an entry used since keeps it as recent.
    cache.note(&shard(0), Some(0), vec![scanned("live", 9, 0)]);
    cache.note(&shard(0), Some(0), vec![clean("more", 10, 30)]);
    assert_eq!(victims(&cache), [named(0, "old")]);
    cache.note(&shard(0), Some(0), vec![clean("again", 11, 30)]);
    assert_eq!(victims(&cache), [named(0, "new")]);
    cache.note(&shard(0), Some(0), vec![clean("last", 12, 30)]);
    assert_eq!(victims(&cache), [named(0, "live")]);
}

#[test]
fn each_disk_keeps_its_cache_within_its_room() {
    let cache = cache(1_000);
    cache.note(
        &shard(0),
        Some(0),
        vec![clean("a", 1, 100), clean("b", 2, 100)],
    );
    cache.note(&shard(1), Some(0), vec![clean("c", 1, 100)]);
    // Disk 0 has 50 bytes free of 1,000, and keeps a quarter free: its
    // room is 50 + 200 - 250 = 0.
    cache.set_space(
        &disk(0),
        Space {
            total: 1_000,
            available: 50,
        },
    );
    // Disk 1 has room for 400 + 100 - 250 bytes.
    cache.set_space(
        &disk(1),
        Space {
            total: 1_000,
            available: 400,
        },
    );
    assert_eq!(cache.usage().limit, 250);
    assert_eq!(victims(&cache), [named(0, "a"), named(0, "b")]);
    // Payload it evicted counts as free, though no compaction reclaimed
    // it yet: the disk's room does not shrink by what it evicted.
    for size in [100, 100] {
        cache.ledger().disks.get_mut(&disk(0)).unwrap().released += size;
    }
    cache.set_space(
        &disk(0),
        Space {
            total: 1_000,
            available: 50,
        },
    );
    assert_eq!(cache.usage().limit, 250);
    cache.note(&shard(0), Some(0), vec![clean("d", 3, 100)]);
    cache.note(&shard(1), Some(0), vec![clean("e", 2, 200)]);
    assert_eq!(victims(&cache), [named(0, "d"), named(1, "c")]);
    assert_eq!(cache.usage().bytes, 200);
}

#[test]
fn a_node_without_room_on_a_disk_is_limited_by_the_others() {
    let cache = cache(1_000);
    cache.set_space(
        &disk(0),
        Space {
            total: 1_000,
            available: 300,
        },
    );
    // Disk 1's space is not known yet.
    assert_eq!(cache.usage().limit, 1_000);
    cache.set_space(
        &disk(1),
        Space {
            total: 1_000,
            available: 2_000,
        },
    );
    assert_eq!(cache.usage().limit, 1_000);
    cache.set_space(
        &disk(1),
        Space {
            total: 1_000,
            available: 300,
        },
    );
    assert_eq!(cache.usage().limit, 100);
}

#[test]
fn metrics_follow_the_cache() {
    let registry = MetricsRegistry::new();
    let cache = CleanCache::new(
        CacheSettings {
            max_bytes: 64,
            ..CacheSettings::default()
        },
        CacheMetrics::register(&registry),
    );
    cache.attach(&shard(0), &disk(0));
    cache.note(&shard(0), Some(0), vec![clean("a", 1, 48)]);
    cache.inner.metrics.evictions.inc();
    let text = registry.encode().unwrap();
    for line in [
        "skys3_clean_cache_bytes 48",
        "skys3_clean_cache_limit_bytes 64",
        "skys3_clean_cache_evictions_total 1",
    ] {
        assert!(text.contains(line), "{line} in {text}");
    }
    assert!(format!("{cache:?}").contains("bytes: 48"));
    assert_eq!(CacheSettings::default().max_bytes, 1 << 40);
}
