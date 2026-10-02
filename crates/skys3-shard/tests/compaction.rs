//! Segment compaction (§10.3): released segments below the live threshold
//! are reclaimed; what an entry, an open upload, or a shard's membership
//! still needs is copied or, for cold clean payload, evicted; and nothing
//! is lost across a crash.

mod support;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use skys3_index::{Checkpointer, EntryState, Index, Payload};
use skys3_io::{SimDisk, SimMount};
use skys3_log::{LogRecord, RecordBody, SegmentLog};
use skys3_obs::MetricsRegistry;
use skys3_shard::{
    CacheMetrics, CacheSettings, CleanCache, CompactionMetrics, CompactionSettings, Compactor,
    Effect, Outcome, ReadSettings, Shard, ShardSet, StateMachine,
};
use skys3_types::{NodeId, Seq, ShardConfig};
use support::{
    config, delete, extent, flushed, index_config, mpu_complete, mpu_create, mpu_part, open_log,
    pool, put, put_extents, runtime, shard as shard_ref, streamed, tags, upload_begin,
};

/// A node over one simulated disk: its log, index, and shards.
struct Node {
    log: SegmentLog<SimMount>,
    index: Arc<Index>,
    checkpointer: Checkpointer<SimMount>,
    set: ShardSet<SimMount>,
}

impl Node {
    /// Opens the node on `disk`, replaying its log.
    async fn open(disk: &SimDisk) -> Self {
        let mount = disk.mount();
        let log = open_log(mount.clone()).await;
        let index = Arc::new(Index::open_sim(&mount, "index.redb", &index_config()).unwrap());
        let checkpointer = Checkpointer::new(
            Arc::clone(&index),
            BTreeMap::from([(
                ShardSet::<SimMount>::SINGLE_DISK.parse().unwrap(),
                log.clone(),
            )]),
            pool(),
        );
        checkpointer.replay(Arc::new(StateMachine)).await.unwrap();
        let set = ShardSet::new(Arc::clone(&index), log.clone(), pool());
        // Payload goes as soon as nothing names it, unless a test sets a
        // release delay.
        set.reads().configure(ReadSettings {
            release_delay: Duration::ZERO,
            ..ReadSettings::default()
        });
        Self {
            log,
            index,
            checkpointer,
            set,
        }
    }

    /// A compactor of the node's log, with `ttl` for unreferenced extents.
    fn compactor(&self, ttl: Duration) -> Compactor<SimMount> {
        Compactor::new(
            self.set.clone(),
            CompactionSettings {
                live_threshold: 0.5,
                unreferenced_ttl: ttl,
            },
            CompactionMetrics::default(),
        )
    }

    /// The bytes of `key`'s payload in `shard`, as reads find them, or
    /// `None` if the entry has none here.
    async fn bytes(&self, shard: &Shard<SimMount>, key: &str) -> Option<Vec<u8>> {
        let entry = self
            .index
            .read()
            .unwrap()
            .entry(shard.shard(), key)
            .unwrap()?;
        let positions = match entry.object?.payload {
            Payload::Inline(position) => vec![position],
            Payload::Extents(extents) => extents.iter().map(|e| e.position).collect(),
            Payload::Parts { upload, .. } => {
                let parts = self
                    .index
                    .read()
                    .unwrap()
                    .parts(shard.shard(), upload, 0, 100);
                let mut positions = Vec::new();
                for (_, part) in parts.unwrap() {
                    match part.payload {
                        Payload::Inline(position) => positions.push(position),
                        Payload::Extents(extents) => {
                            positions.extend(extents.iter().map(|e| e.position));
                        }
                        _ => return None,
                    }
                }
                positions
            }
            Payload::None => return None,
        };
        let mut bytes = Vec::new();
        for position in positions {
            bytes.extend_from_slice(&shard.payload(position).await.unwrap());
        }
        Some(bytes)
    }

    /// Every record the log holds.
    async fn records(&self) -> Vec<LogRecord> {
        let mut records = Vec::new();
        for segment in self.log.segments() {
            let mut scanner = self.log.scan(segment.id).unwrap();
            while let Some(scanned) = scanner.next().await.unwrap() {
                records.push(scanned.decode().unwrap());
            }
        }
        records
    }
}

fn key(n: u64) -> String {
    format!("k{n}")
}

/// The pattern of `len` bytes the support's records carry.
fn pattern(len: usize) -> Vec<u8> {
    support::fill(len).to_vec()
}

/// Writes `rounds` versions of each of `keys` keys to `shard`, inline.
async fn overwrite(shard: &Shard<SimMount>, keys: u64, rounds: u64) {
    for round in 0..rounds {
        for n in 0..keys {
            shard
                .commit(put(&key(n), 400, round * 100 + n))
                .await
                .unwrap();
        }
    }
}

#[test]
fn overwritten_segments_are_reclaimed_and_survive_a_crash() {
    runtime().block_on(async {
        let disk = SimDisk::new(1);
        let node = Node::open(&disk).await;
        let shard = node.set.open(&config(&shard_ref(0), 1)).await.unwrap();
        overwrite(&shard, 4, 20).await;
        // Nothing is released before a checkpoint, so nothing is reclaimed.
        let compactor = node.compactor(Duration::ZERO);
        assert_eq!(compactor.compact().await.unwrap().segments, 0);

        node.checkpointer.checkpoint().await.unwrap();
        let segments = node.log.segments().len();
        let used = disk.used_bytes();
        let report = compactor.compact().await.unwrap();
        assert!(report.segments > 0, "{report:?}");
        assert!(report.reclaimed_bytes > report.copied_bytes, "{report:?}");
        assert_eq!(report.evictions, 0);
        assert_eq!(
            node.log.segments().len() as u64,
            segments as u64 - report.segments
        );
        // Retired segments stay readable for a grace, so their bytes stay
        // on the simulated disk until then; their files are gone.
        assert!(disk.used_bytes() <= used + report.copied_bytes);
        for n in 0..4 {
            assert_eq!(node.bytes(&shard, &key(n)).await, Some(pattern(400)));
        }
        // A second pass finds nothing more to do among what is released.
        let again = compactor.compact().await.unwrap();
        assert_eq!(again.segments, 0, "{again:?}");

        let dump = node.index.read().unwrap().dump().unwrap();
        drop((shard, node));
        disk.crash();
        let node = Node::open(&disk).await;
        assert_eq!(node.index.read().unwrap().dump().unwrap(), dump);
        let shard = node.set.open(&config(&shard_ref(0), 1)).await.unwrap();
        for n in 0..4 {
            assert_eq!(node.bytes(&shard, &key(n)).await, Some(pattern(400)));
        }
    });
}

#[test]
fn registered_reads_and_recently_unreferenced_payload_are_kept() {
    runtime().block_on(async {
        let disk = SimDisk::new(7);
        let node = Node::open(&disk).await;
        let reads = node.set.reads();
        reads.configure(ReadSettings {
            release_delay: Duration::from_millis(300),
            ..ReadSettings::default()
        });
        let shard = node.set.open(&config(&shard_ref(0), 1)).await.unwrap();
        // A gateway plans a read of the first version of k0, which is then
        // overwritten many times before it registers: the plan's bytes are
        // unreferenced but still located, so the holder serves them.
        let first = shard.commit(put(&key(0), 400, 7)).await.unwrap();
        let plan = shard.plan(&key(0)).await.unwrap();
        overwrite(&shard, 4, 20).await;
        let version = plan.entry.as_ref().unwrap().version;
        assert_eq!(version, first.position);
        let registered = shard
            .register_read(reads, &key(0), version, plan.layout.clone())
            .await
            .unwrap()
            .expect("the planned bytes are still located");
        let position = registered.layout[0].position;
        node.checkpointer.checkpoint().await.unwrap();

        // Nothing is dropped before it has been unreferenced for the
        // release delay.
        let compactor = node.compactor(Duration::ZERO);
        let report = compactor.compact().await.unwrap();
        assert_eq!(report.segments, 0, "{report:?}");
        tokio::time::sleep(Duration::from_millis(400)).await;
        let report = compactor.compact().await.unwrap();
        assert!(report.segments > 0, "{report:?}");

        // The registered version was copied, not dropped, and its read
        // goes on; once released, the next passes may drop it.
        let bytes = shard.read_registered(reads, registered.id, position).await;
        assert_eq!(bytes.unwrap().to_vec(), pattern(400));
        assert!(reads.is_pinned(shard.shard(), position));
        reads.release(registered.id);
        assert!(!reads.is_pinned(shard.shard(), position));
        let refused = shard.read_registered(reads, registered.id, position).await;
        assert!(refused.is_err());
        for n in 0..4 {
            assert_eq!(node.bytes(&shard, &key(n)).await, Some(pattern(400)));
        }
    });
}

#[test]
fn dirty_extents_and_open_uploads_are_copied_and_unnamed_extents_wait() {
    runtime().block_on(async {
        let disk = SimDisk::new(2);
        let node = Node::open(&disk).await;
        let shard = node.set.open(&config(&shard_ref(0), 1)).await.unwrap();
        // A dirty object in extents, an open upload with an extent-backed
        // part, and a body that never got its PUT.
        let mut extents = Vec::new();
        for n in 0..3 {
            let appended = shard.append_extent(extent("big", n * 700, 700)).await;
            extents.push(appended.unwrap());
        }
        shard.commit(put_extents("big", extents, 1)).await.unwrap();
        let upload = shard.commit(mpu_create("mp")).await.unwrap().position;
        let part = shard.append_extent(extent("mp", 0, 900)).await.unwrap();
        let stored = shard
            .commit(mpu_part("mp", upload, 1, 0, vec![part]))
            .await
            .unwrap()
            .position;
        shard.append_extent(extent("lost", 0, 1500)).await.unwrap();
        // Many overwrites fill hot and bulk segments with garbage.
        overwrite(&shard, 2, 40).await;
        for round in 0..12 {
            let mut extents = Vec::new();
            for n in 0..2 {
                let appended = shard.append_extent(extent("garbage", n * 1000, 1000)).await;
                extents.push(appended.unwrap());
            }
            shard
                .commit(put_extents("garbage", extents, round))
                .await
                .unwrap();
        }
        node.checkpointer.checkpoint().await.unwrap();

        // The orphaned extent's segment is young: it waits.
        let young = node.compactor(Duration::from_secs(3600)).compact().await;
        let young = young.unwrap();
        let held = node
            .records()
            .await
            .iter()
            .any(|record| record.body.key() == Some("lost"));
        assert!(held, "an unnamed extent went before its TTL: {young:?}");

        let report = node.compactor(Duration::ZERO).compact().await.unwrap();
        assert!(report.segments > 0, "{report:?}");
        let records = node.records().await;
        assert!(
            !records
                .iter()
                .any(|record| record.body.key() == Some("lost")),
            "an unnamed extent outlived its TTL"
        );
        let big: Vec<u8> = (0..3).flat_map(|_| pattern(700)).collect();
        assert_eq!(node.bytes(&shard, "big").await, Some(big));
        // The open upload completes from its copied part.
        let completed = mpu_complete("mp", upload, &[(1, stored)], 900);
        let completed = shard.commit(completed).await.unwrap();
        assert!(completed.position > upload);
        assert_eq!(node.bytes(&shard, "mp").await, Some(pattern(900)));
    });
}

#[test]
fn cold_clean_payload_is_evicted_and_hot_payload_copied() {
    runtime().block_on(async {
        let disk = SimDisk::new(3);
        let node = Node::open(&disk).await;
        let cache = CleanCache::new(
            CacheSettings {
                max_bytes: 1 << 20,
                reserve_fraction: 0.0,
            },
            CacheMetrics::default(),
        );
        node.set.use_cache(&cache).await;
        let shard = node.set.open(&config(&shard_ref(0), 1)).await.unwrap();
        // Ten clean keys, then garbage after them.
        let clean = |n: u64| format!("clean-{n}");
        for n in 0..10 {
            let written = shard.commit(put(&clean(n), 400, n)).await.unwrap();
            let seq = written.position.seq.get();
            shard.commit(flushed(&clean(n), seq, false)).await.unwrap();
        }
        cache.reclaim(&node.set).await;
        overwrite(&shard, 1, 60).await;
        // Reads make the last three keys the most recently used.
        for n in 7..10 {
            shard.entry(&clean(n)).await.unwrap();
        }
        node.checkpointer.checkpoint().await.unwrap();

        let registry = MetricsRegistry::new();
        let compactor = Compactor::new(
            node.set.clone(),
            CompactionSettings {
                live_threshold: 0.9,
                unreferenced_ttl: Duration::ZERO,
            },
            CompactionMetrics::register(&registry),
        );
        let report = compactor.compact().await.unwrap();
        assert!(report.evictions > 0, "{report:?}");
        let read = node.index.read().unwrap();
        let state = |n: u64| read.entry(shard.shard(), &clean(n)).unwrap().unwrap().state;
        for n in 7..10 {
            assert_eq!(state(n), EntryState::Clean, "{}", clean(n));
        }
        let evicted = (0..7).filter(|&n| state(n) == EntryState::Evicted).count();
        assert_eq!(evicted as u64, report.evictions);
        for n in 7..10 {
            assert_eq!(node.bytes(&shard, &clean(n)).await, Some(pattern(400)));
        }
        assert_eq!(cache.usage().bytes, 400 * (10 - evicted as u64));
        let text = registry.encode().unwrap();
        for name in [
            "skys3_compaction_segments_total",
            "skys3_compaction_reclaimed_bytes_total",
            "skys3_compaction_copied_bytes_total",
            "skys3_compaction_evictions_total",
            "skys3_compaction_write_amplification",
        ] {
            assert!(text.contains(name), "{name} missing from {text}");
        }
        let amplification: f64 = text
            .lines()
            .find_map(|line| line.strip_prefix("skys3_compaction_write_amplification "))
            .unwrap()
            .parse()
            .unwrap();
        assert!(amplification > 1.0, "{text}");
    });
}

#[test]
fn the_latest_config_record_stays_and_held_shards_wait() {
    runtime().block_on(async {
        let disk = SimDisk::new(4);
        let node = Node::open(&disk).await;
        let shard = node.set.open(&config(&shard_ref(0), 1)).await.unwrap();
        overwrite(&shard, 2, 20).await;
        let newer = config(&shard_ref(0), 2);
        shard.reconfigure(&newer).await.unwrap();
        overwrite(&shard, 2, 40).await;
        // A shard that is no longer open holds its segments.
        let other = node.set.open(&config(&shard_ref(1), 1)).await.unwrap();
        overwrite(&other, 1, 3).await;
        other.close().await.unwrap();
        drop(other);
        node.checkpointer.checkpoint().await.unwrap();

        let report = node.compactor(Duration::ZERO).compact().await.unwrap();
        assert!(report.segments > 0, "{report:?}");
        let records = node.records().await;
        let configs: Vec<&ShardConfig> = records
            .iter()
            .filter(|record| record.shard == shard_ref(0))
            .filter_map(|record| match &record.body {
                RecordBody::Config(config) => Some(config),
                _ => None,
            })
            .collect();
        assert!(configs.contains(&&newer), "the newest CONFIG record went");
        assert!(
            records.iter().any(|record| record.shard == shard_ref(1)),
            "a closed shard's records went"
        );
        let kept = node.index.read().unwrap().config(&shard_ref(0)).unwrap();
        assert_eq!(kept.as_ref(), Some(&newer));
    });
}

#[test]
fn a_replica_holds_what_its_members_may_lack() {
    runtime().block_on(async {
        let disk = SimDisk::new(5);
        let node = Node::open(&disk).await;
        let alone = node.set.open(&config(&shard_ref(2), 1)).await.unwrap();
        overwrite(&alone, 2, 30).await;
        alone.close().await.unwrap();
        drop((alone, node));
        // The node restarts, and the shard has a second member now. Its
        // primary has not heard from it, so it knows no commit watermark:
        // a member may lack any record, and none goes, although replay
        // applied them all.
        disk.kill();
        let node = Node::open(&disk).await;
        let primary: NodeId = "node-1".parse().unwrap();
        let replicated = ShardConfig {
            members: vec![primary.clone(), "node-2".parse().unwrap()],
            replicas: 2,
            ..config(&shard_ref(2), 2)
        };
        let shard = node.set.open_replica(&replicated, &primary).await.unwrap();
        assert_eq!(shard.replicated_through(), Some(Seq::ZERO));
        node.checkpointer.checkpoint().await.unwrap();
        assert!(node.log.released().len() > 1);
        let before = node.records().await;
        let report = node.compactor(Duration::ZERO).compact().await.unwrap();
        assert!(report.assessed > 0, "{report:?}");
        assert_eq!(report.segments, 0, "{report:?}");
        assert_eq!(node.records().await, before);

        // Alone again, the same replica holds nothing back.
        drop((shard, node));
        disk.kill();
        let node = Node::open(&disk).await;
        let alone = node.set.open(&config(&shard_ref(2), 3)).await.unwrap();
        assert_eq!(alone.replicated_through(), None);
        node.checkpointer.checkpoint().await.unwrap();
        let report = node.compactor(Duration::ZERO).compact().await.unwrap();
        assert!(report.segments > 0, "{report:?}");
    });
}

#[test]
fn clean_bytes_a_tags_made_dirty_again_are_copied() {
    runtime().block_on(async {
        let disk = SimDisk::new(6);
        let node = Node::open(&disk).await;
        let cache = CleanCache::new(
            CacheSettings {
                max_bytes: 1 << 20,
                reserve_fraction: 0.0,
            },
            CacheMetrics::default(),
        );
        node.set.use_cache(&cache).await;
        let shard = node.set.open(&config(&shard_ref(0), 1)).await.unwrap();
        let written = shard.commit(put("cold", 300, 1)).await.unwrap();
        let seq = written.position.seq.get();
        shard.commit(flushed("cold", seq, false)).await.unwrap();
        overwrite(&shard, 1, 40).await;
        node.checkpointer.checkpoint().await.unwrap();
        // A TAGS makes the clean entry dirty with the same bytes, so
        // compaction copies them rather than evicting them.
        shard.commit(tags("cold", "x")).await.unwrap();
        shard.commit(delete(&key(0))).await.unwrap();
        node.compactor(Duration::ZERO).compact().await.unwrap();
        assert_eq!(node.bytes(&shard, "cold").await, Some(pattern(300)));
        let entry = node.index.read().unwrap().entry(shard.shard(), "cold");
        assert_eq!(entry.unwrap().unwrap().state, EntryState::Dirty);
    });
}

#[test]
fn a_streamed_put_keeps_its_extents_and_identity_while_it_streams() {
    runtime().block_on(async {
        let disk = SimDisk::new(7);
        let node = Node::open(&disk).await;
        let shard = node.set.open(&config(&shard_ref(0), 1)).await.unwrap();
        // A streamed PUT fixes its identity and sends part of its body...
        let begin = shard.commit(upload_begin("streamed")).await.unwrap();
        assert_eq!(begin.outcome, Outcome::Applied(Effect::UploadBegun));
        let begin = begin.position;
        let mut extents = Vec::new();
        for n in 0..2 {
            let appended = shard.append_extent(extent("streamed", n * 700, 700)).await;
            extents.push(appended.unwrap());
        }
        // ...while other writes fill hot and bulk segments with garbage,
        // and a checkpoint releases them all.
        overwrite(&shard, 2, 40).await;
        for round in 0..12 {
            let mut garbage = Vec::new();
            for n in 0..2 {
                let appended = shard.append_extent(extent("garbage", n * 1000, 1000)).await;
                garbage.push(appended.unwrap());
            }
            shard
                .commit(put_extents("garbage", garbage, round))
                .await
                .unwrap();
        }
        node.checkpointer.checkpoint().await.unwrap();

        // Compaction drops the applied UPLOAD_BEGIN, which nothing reads
        // again, and keeps the body's extents: nothing names them yet, and
        // their segment is younger than the TTL, which bounds how long a
        // body may stream.
        let report = node.compactor(Duration::from_secs(3600)).compact().await;
        let report = report.unwrap();
        assert!(report.segments > 0, "{report:?}");
        let records = node.records().await;
        assert!(
            !records.iter().any(|record| record.position == begin),
            "the applied UPLOAD_BEGIN outlived compaction"
        );
        for sent in &extents {
            let kept = records
                .iter()
                .any(|record| record.position == sent.position);
            assert!(kept, "an extent of the streaming body went: {report:?}");
        }

        // The rest of the body arrives, and the PUT inherits the identity.
        let last = shard.append_extent(extent("streamed", 1400, 700)).await;
        extents.push(last.unwrap());
        let body = streamed(put_extents("streamed", extents, 1), begin);
        let committed = shard.commit(body).await.unwrap();
        assert!(committed.outcome.is_applied(), "{:?}", committed.outcome);
        let whole: Vec<u8> = (0..3).flat_map(|_| pattern(700)).collect();
        assert_eq!(node.bytes(&shard, "streamed").await, Some(whole.clone()));

        // Named now, the extents are copied like any dirty payload, and the
        // identity lives in the entry, through compaction and a crash.
        node.checkpointer.checkpoint().await.unwrap();
        overwrite(&shard, 2, 40).await;
        node.checkpointer.checkpoint().await.unwrap();
        let report = node.compactor(Duration::ZERO).compact().await.unwrap();
        assert!(report.segments > 0, "{report:?}");
        drop((shard, node));
        disk.crash();
        let node = Node::open(&disk).await;
        let shard = node.set.open(&config(&shard_ref(0), 1)).await.unwrap();
        assert_eq!(node.bytes(&shard, "streamed").await, Some(whole));
        let entry = shard.entry("streamed").await.unwrap().unwrap();
        assert_eq!(entry.version, committed.position);
        assert_eq!(entry.object.unwrap().write_identity, Some(begin));
    });
}
