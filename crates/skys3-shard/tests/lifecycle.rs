//! Lifecycle passes on a shard's primary (§8.7): random rules over random
//! objects and uploads, of random ages, prefixes, sizes, and tags, against
//! a reference evaluator; expirations that race client writes; and which
//! replicas a node's pass visits.

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use proptest::prelude::*;
use skys3_index::{EntryState, Index};
use skys3_io::{BlockingPool, MonotonicClock, SimDisk, SimMount};
use skys3_log::record::Put;
use skys3_log::{LogConfig, RecordBody, SegmentLog};
use skys3_obs::MetricsRegistry;
use skys3_shard::lifecycle::{LifecycleMetrics, LifecycleReport, Tombstones, expire, run_pass};
use skys3_shard::{Shard, ShardError, ShardSet};
use skys3_types::lifecycle::{
    DAY_MS, Expiration, LifecycleConfiguration, LifecycleRule, RuleFilter,
};
use skys3_types::{BucketDocument, BucketMode, ProposalId, ShardCount};
use support::{config, delete, index_config, log_config, mpu_create, pool, put, runtime};

/// A shard on a fresh disk whose group commits wait `delay` for more
/// records.
async fn open(delay: Duration) -> (SimDisk, Shard<SimMount>) {
    let disk = SimDisk::new(11);
    let log_config = LogConfig {
        group_commit_max_delay: delay,
        ..log_config()
    };
    let (log, _) = SegmentLog::open(disk.mount(), log_config, Arc::new(MonotonicClock::new()))
        .await
        .unwrap();
    let index = Arc::new(Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap());
    let shard = Shard::open(&config(&support::shard(0), 1), log, index, pool())
        .await
        .unwrap();
    (disk, shard)
}

/// A `PUT` of `key` with `size` bytes, `tags`, written at `written_ms`.
fn object(key: &str, size: usize, tags: &BTreeMap<String, String>, written_ms: u64) -> RecordBody {
    let RecordBody::Put(body) = put(key, size, 1) else {
        unreachable!()
    };
    RecordBody::Put(Put {
        last_modified_ms: written_ms,
        tags: tags.clone(),
        ..body
    })
}

/// An `MPU_CREATE` of `key` initiated at `initiated_ms`.
fn upload(key: &str, initiated_ms: u64) -> RecordBody {
    let RecordBody::MpuCreate(mut body) = mpu_create(key) else {
        unreachable!()
    };
    body.initiated_ms = initiated_ms;
    RecordBody::MpuCreate(body)
}

fn rule(
    id: &str,
    prefix: &str,
    expiration: Option<Expiration>,
    abort: Option<u32>,
) -> LifecycleRule {
    LifecycleRule {
        id: id.to_owned(),
        enabled: true,
        filter: RuleFilter {
            prefix: prefix.to_owned(),
            ..RuleFilter::default()
        },
        expiration,
        abort_upload_days: abort,
    }
}

// The reference evaluator: S3's rules, written out on their own.

/// The first midnight UTC after `days` days from `start_ms`.
fn reference_due(start_ms: u64, days: u32) -> u64 {
    let due = start_ms + u64::from(days) * DAY_MS;
    due - due % DAY_MS + DAY_MS
}

fn reference_matches(
    filter: &RuleFilter,
    key: &str,
    size: u64,
    tags: &BTreeMap<String, String>,
) -> bool {
    if !key.starts_with(filter.prefix.as_str()) {
        return false;
    }
    for (name, value) in &filter.tags {
        if tags.get(name) != Some(value) {
            return false;
        }
    }
    if let Some(bound) = filter.size_greater_than
        && size <= bound
    {
        return false;
    }
    if let Some(bound) = filter.size_less_than
        && size >= bound
    {
        return false;
    }
    true
}

fn reference_expired(
    config: &LifecycleConfiguration,
    key: &str,
    size: u64,
    tags: &BTreeMap<String, String>,
    written_ms: u64,
    now_ms: u64,
) -> bool {
    config.rules.iter().any(|rule| {
        rule.enabled
            && reference_matches(&rule.filter, key, size, tags)
            && match rule.expiration {
                Some(Expiration::Days(days)) => reference_due(written_ms, days) <= now_ms,
                Some(Expiration::DateMs(date)) => date <= now_ms,
                None => false,
            }
    })
}

fn reference_aborted(
    config: &LifecycleConfiguration,
    key: &str,
    initiated_ms: u64,
    now_ms: u64,
) -> bool {
    config.rules.iter().any(|rule| {
        rule.enabled
            && key.starts_with(rule.filter.prefix.as_str())
            && rule
                .abort_upload_days
                .is_some_and(|days| reference_due(initiated_ms, days) <= now_ms)
    })
}

// Random rules, objects, and uploads.

const PREFIXES: [&str; 4] = ["", "a/", "a/b/", "b/"];

fn prefix() -> impl Strategy<Value = String> {
    prop::sample::select(&PREFIXES[..]).prop_map(str::to_owned)
}

fn tag_set() -> impl Strategy<Value = BTreeMap<String, String>> {
    (
        prop::option::of(prop::sample::select(&["1", "2"][..])),
        prop::option::of(Just("x")),
    )
        .prop_map(|(t, u)| {
            let mut tags = BTreeMap::new();
            if let Some(t) = t {
                tags.insert("t".to_owned(), t.to_owned());
            }
            if let Some(u) = u {
                tags.insert("u".to_owned(), u.to_owned());
            }
            tags
        })
}

/// Ages within 20 days of the epoch's day 100.
fn time_ms() -> impl Strategy<Value = u64> {
    (0..20 * DAY_MS).prop_map(|offset| 100 * DAY_MS + offset)
}

fn lifecycle_rule() -> impl Strategy<Value = LifecycleRule> {
    let expiration = prop_oneof![
        Just(None),
        (1..8u32).prop_map(|days| Some(Expiration::Days(days))),
        (100..125u64).prop_map(|day| Some(Expiration::DateMs(day * DAY_MS))),
    ];
    let sizes = (prop::option::of(0..150u64), prop::option::of(100..300u64));
    (
        any::<u8>(),
        prefix(),
        tag_set(),
        sizes,
        expiration,
        prop::option::of(1..8u32),
        prop::bool::weighted(0.85),
    )
        .prop_map(
            |(id, prefix, tags, (greater, less), expiration, abort, enabled)| {
                // An upload rule filters by prefix only, and a rule needs an
                // action and a non-empty size range.
                let (tags, greater, less) = if abort.is_some() {
                    (BTreeMap::new(), None, None)
                } else {
                    let less = less.filter(|less| greater.is_none_or(|greater| *less > greater));
                    (tags, greater, less)
                };
                let expiration = if abort.is_none() && expiration.is_none() {
                    Some(Expiration::Days(1))
                } else {
                    expiration
                };
                LifecycleRule {
                    id: id.to_string(),
                    enabled,
                    filter: RuleFilter {
                        prefix,
                        tags,
                        size_greater_than: greater,
                        size_less_than: less,
                        legacy_prefix: false,
                    },
                    expiration,
                    abort_upload_days: abort,
                }
            },
        )
}

fn configuration() -> impl Strategy<Value = LifecycleConfiguration> {
    prop::collection::vec(lifecycle_rule(), 1..5).prop_map(|rules| {
        let mut seen = BTreeSet::new();
        let rules = rules
            .into_iter()
            .filter(|rule| seen.insert(rule.id.clone()))
            .collect();
        LifecycleConfiguration { rules }
    })
}

#[derive(Debug, Clone)]
struct Object {
    prefix: String,
    size: u64,
    tags: BTreeMap<String, String>,
    written_ms: u64,
    /// Deleted again by its client before the pass: a tombstone the pass
    /// must leave alone.
    deleted: bool,
}

fn objects() -> impl Strategy<Value = Vec<Object>> {
    let object = (
        prefix(),
        0..300u64,
        tag_set(),
        time_ms(),
        prop::bool::weighted(0.1),
    )
        .prop_map(|(prefix, size, tags, written_ms, deleted)| Object {
            prefix,
            size,
            tags,
            written_ms,
            deleted,
        });
    prop::collection::vec(object, 0..24)
}

fn uploads() -> impl Strategy<Value = Vec<(String, u64)>> {
    prop::collection::vec((prefix(), time_ms()), 0..8)
}

/// Runs a pass at `now_ms` over a shard holding `objects` and `uploads`,
/// and checks what it left against the reference evaluator.
async fn check_pass(
    config: LifecycleConfiguration,
    objects: Vec<Object>,
    uploads: Vec<(String, u64)>,
    now_ms: u64,
) {
    config.validate().unwrap();
    let (_disk, shard) = open(Duration::ZERO).await;
    let mut kept = BTreeMap::new();
    let mut expected = LifecycleReport::default();
    for (n, object) in objects.iter().enumerate() {
        let key = format!("{}k{n}", object.prefix);
        let size = usize::try_from(object.size).unwrap();
        let body = self::object(&key, size, &object.tags, object.written_ms);
        let position = shard.commit(body).await.unwrap().position;
        if object.deleted {
            let tombstone = shard.commit(delete(&key)).await.unwrap().position;
            kept.insert(key, Some(tombstone));
            continue;
        }
        let expired = reference_expired(
            &config,
            &key,
            object.size,
            &object.tags,
            object.written_ms,
            now_ms,
        );
        expected.expired += u64::from(expired);
        kept.insert(key, (!expired).then_some(position));
    }
    let mut open_uploads = BTreeMap::new();
    for (n, (prefix, initiated_ms)) in uploads.iter().enumerate() {
        let key = format!("{prefix}u{n}");
        let position = shard
            .commit(upload(&key, *initiated_ms))
            .await
            .unwrap()
            .position;
        let aborted = reference_aborted(&config, &key, *initiated_ms, now_ms);
        expected.aborted += u64::from(aborted);
        open_uploads.insert(position, (key, !aborted));
    }

    let report = expire(&shard, &config, now_ms, Tombstones::Remove)
        .await
        .unwrap();
    assert_eq!(report, expected, "{config:#?}");
    for (key, version) in &kept {
        let entry = shard.entry(key).await.unwrap();
        match version {
            // A kept version or tombstone is untouched; an expired version
            // leaves no entry, its tombstone removed as a local delete's is.
            Some(version) => assert_eq!(entry.map(|e| e.version), Some(*version), "{key}"),
            None => assert!(entry.is_none(), "{key}: {entry:?}"),
        }
    }
    for (upload, (key, open)) in &open_uploads {
        let found = shard.upload(key, *upload, 0, 0).await.unwrap();
        assert_eq!(found.is_some(), *open, "{key}");
    }
    // A second pass finds nothing more to do.
    let again = expire(&shard, &config, now_ms, Tombstones::Remove)
        .await
        .unwrap();
    assert_eq!(again, LifecycleReport::default());
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn a_pass_expires_what_the_rules_say(
        config in configuration(),
        objects in objects(),
        uploads in uploads(),
        now_ms in (100 * DAY_MS)..(130 * DAY_MS),
    ) {
        runtime().block_on(check_pass(config, objects, uploads, now_ms));
    }
}

#[test]
fn every_page_of_a_large_shard_is_visited() {
    runtime().block_on(async {
        let (_disk, shard) = open(Duration::ZERO).await;
        let none = BTreeMap::new();
        for n in 0..600 {
            let prefix = if n % 2 == 0 { "old/" } else { "new/" };
            let key = format!("{prefix}{n:04}");
            shard.commit(object(&key, 4, &none, 0)).await.unwrap();
        }
        for n in 0..300 {
            shard
                .commit(upload(&format!("old/u{n:03}"), 0))
                .await
                .unwrap();
        }
        let config = LifecycleConfiguration {
            rules: vec![rule("old", "old/", Some(Expiration::Days(1)), Some(1))],
        };
        let report = expire(&shard, &config, 10 * DAY_MS, Tombstones::Remove)
            .await
            .unwrap();
        assert_eq!((report.expired, report.aborted), (300, 300));
        let left = shard.entries(None, 1000).await.unwrap();
        assert_eq!(left.len(), 300);
        assert!(left.iter().all(|(key, _)| key.starts_with("new/")));
        assert!(shard.uploads("", None, 10).await.unwrap().is_empty());
    });
}

#[test]
fn an_expiration_leaves_a_version_written_after_the_scan() {
    runtime().block_on(async {
        // Each group commit waits, so a write stays unapplied for a while.
        let (_disk, shard) = open(Duration::from_millis(50)).await;
        let none = BTreeMap::new();
        shard.commit(object("a", 4, &none, 0)).await.unwrap();
        shard.commit(object("b", 4, &none, 0)).await.unwrap();
        // A new version of `a` is sequenced, not yet applied, when the pass
        // reads its page: the pass sees the old version as expired, and its
        // conditional delete finds the new one.
        let writer = shard.clone();
        let overwrite = tokio::spawn(async move {
            writer
                .commit(object("a", 5, &BTreeMap::new(), 20 * DAY_MS))
                .await
        });
        tokio::task::yield_now().await;
        let config = LifecycleConfiguration {
            rules: vec![rule("all", "", Some(Expiration::Days(1)), None)],
        };
        let report = expire(&shard, &config, 10 * DAY_MS, Tombstones::Remove)
            .await
            .unwrap();
        let written = overwrite.await.unwrap().unwrap().position;
        assert_eq!(report.expired, 1);
        assert_eq!(shard.entry("a").await.unwrap().unwrap().version, written);
        assert_eq!(shard.entry("b").await.unwrap(), None);
    });
}

/// A bucket document for `shards` shards of `bucket`, the support's.
fn bucket(mode: BucketMode, lifecycle: Option<LifecycleConfiguration>) -> BucketDocument {
    let shard = support::shard(0);
    BucketDocument {
        bucket_id: shard.bucket.clone(),
        name: "logs".parse().unwrap(),
        mode,
        shards: ShardCount::new(2).unwrap(),
        replicas: 1,
        min_write_replicas: 1,
        clean_copies: 0,
        target: None,
        created_unix_ms: 0,
        lifecycle,
        proposal_id: ProposalId::new("p").unwrap(),
    }
}

#[test]
fn a_node_runs_passes_on_the_shards_it_leads() {
    runtime().block_on(async {
        let disk = SimDisk::new(3);
        let (log, _) =
            SegmentLog::open(disk.mount(), log_config(), Arc::new(MonotonicClock::new()))
                .await
                .unwrap();
        let index =
            Arc::new(Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap());
        let pool: BlockingPool = pool();
        let set = ShardSet::new(index, log, pool);
        // Shard 0 is this node's alone; of shard 1 it is a member.
        let mut follower = config(&support::shard(1), 1);
        let (primary, member) = ("node-0".parse().unwrap(), "node-1".parse().unwrap());
        follower.primary = primary;
        follower.members = vec![follower.primary.clone(), member];
        follower.replicas = 2;
        let led = set.open(&config(&support::shard(0), 1)).await.unwrap();
        let none = BTreeMap::new();
        for key in ["x", "y"] {
            led.commit(object(key, 1, &none, 0)).await.unwrap();
        }
        set.open_replica(&follower, &follower.members[1])
            .await
            .unwrap();

        let config = LifecycleConfiguration {
            rules: vec![rule("all", "", Some(Expiration::Days(1)), None)],
        };
        let registry = MetricsRegistry::new();
        let metrics = LifecycleMetrics::register(&registry);
        let now = 10 * DAY_MS;
        // Only a local bucket with a configuration is evaluated.
        let others = [
            bucket(BucketMode::Local, None),
            bucket(BucketMode::WriteBack, Some(config.clone())),
        ];
        let report = run_pass(&set, &others, now, &metrics, |_| Tombstones::Remove).await;
        assert_eq!(report, LifecycleReport::default());
        let local = [bucket(BucketMode::Local, Some(config))];
        let report = run_pass(&set, &local, now, &metrics, |_| Tombstones::Remove).await;
        assert_eq!(report.expired, 2);
        assert_eq!(report.failed, 0);
        let text = registry.encode().unwrap();
        assert!(text.contains("skys3_lifecycle_expired_total 2"), "{text}");
        assert!(
            text.contains("skys3_lifecycle_aborted_uploads_total 0"),
            "{text}"
        );

        // A sealed shard refuses the pass's deletes: the pass reports it.
        led.commit(object("z", 1, &none, 0)).await.unwrap();
        set.seal(&support::shard(0)).await.unwrap();
        let report = run_pass(&set, &local, now, &metrics, |_| Tombstones::Remove).await;
        assert_eq!((report.expired, report.failed), (0, 1));
        let (_, error) = expire(
            &led,
            &local[0].lifecycle.clone().unwrap(),
            now,
            Tombstones::Remove,
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ShardError::Sealed(_)), "{error:?}");
        set.unseal(&support::shard(0)).await.unwrap();
        let report = run_pass(&set, &local, now, &metrics, |_| Tombstones::Remove).await;
        assert_eq!(report.expired, 1);
    });
}

#[test]
fn a_backed_up_bucket_keeps_its_tombstones_for_the_backup() {
    runtime().block_on(async {
        let (_disk, shard) = open(Duration::ZERO).await;
        let none = BTreeMap::new();
        let written = shard.commit(object("a", 4, &none, 0)).await.unwrap();
        let config = LifecycleConfiguration {
            rules: vec![rule("all", "", Some(Expiration::Days(1)), None)],
        };
        let report = expire(&shard, &config, 10 * DAY_MS, Tombstones::Keep)
            .await
            .unwrap();
        assert_eq!(report.expired, 1);
        // The tombstone waits for the backup's flusher, which deletes the
        // key there and removes it (§8.9).
        let tombstone = shard.entry("a").await.unwrap().unwrap();
        assert!(tombstone.object.is_none());
        assert!(tombstone.version > written.position);
        assert_eq!(tombstone.state, EntryState::Dirty);
        // A second pass leaves it.
        let again = expire(&shard, &config, 10 * DAY_MS, Tombstones::Keep)
            .await
            .unwrap();
        assert_eq!(again, LifecycleReport::default());
    });
}
