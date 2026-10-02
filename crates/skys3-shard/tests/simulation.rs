//! Simulation scenarios for the shard runtime, run by CI's simulation job
//! with a larger seed set.
//!
//! Each seed runs one node over several restarts. In each life the node
//! replays its log, opens a few shards (sometimes in a newer epoch), and
//! runs concurrent writers against them: inline and extent-backed `PUT`s,
//! `DELETE`, `TAGS`, `FLUSHED`, `IMPORT`, `ADOPT`, and batches of `PUT`s
//! and `DELETE`s sequenced together, some conditional, with seals,
//! checkpoints, and sometimes a failed sync. Then the node loses power or
//! is killed. The checks:
//!
//! - every record a shard acknowledged is in the log after the crash,
//! - replay builds exactly the index that applying the log's records in
//!   position order to an empty index builds, and so does the live path
//!   once its writers are done, although the log acknowledges records out
//!   of order,
//! - a writer's commit returns only once its record is applied, and
//! - a sealed shard refuses client writes.
//!
//! A second scenario runs the clean cache (§9.3) over restarts: writers
//! put, delete, flush, read, fill, and evict, some evictions decided before
//! a write, while the cache evicts within a small bound. Every dirty and
//! clean entry keeps its bytes, the cache counts exactly the clean bytes
//! and stays within its bound, and the index differs from replaying the
//! log only in which clean payloads it kept.
//!
//! A third scenario runs segment compaction (§10.3) beside such writers,
//! multipart uploads among them, and cuts the power at a sync, most often
//! one of a compaction pass: inside a copy's group commit, the index commit
//! that moves locations, or the directory sync that removes a segment.
//! Every acknowledged record is still in the log or was in a segment
//! compaction reclaimed; replay builds the index that applying all of them
//! does, but for which clean payloads the node kept; every payload an entry
//! or a part names is located and holds its record; and each shard's
//! latest `CONFIG` record is in the log.

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use skys3_index::{Checkpointer, EntryState, Index, IndexDump, Payload};
use skys3_io::{SimDisk, SimDiskFaults, SimMount, SimPower, SyncCut};
use skys3_log::{LogRecord, RecordBody, SegmentLog, ShardRef};
use skys3_shard::{
    CacheMetrics, CacheSettings, CleanCache, CompactionMetrics, CompactionSettings, Compactor,
    ReadSettings, Shard, ShardError, ShardSet, StateMachine,
};
use skys3_sim::{Runner, SimContext};
use skys3_types::{BucketId, EpochSeq, Label, ShardId};
use support::{
    adopt, config, delete, extent, flushed, import, index_config, mpu_abort, mpu_complete,
    mpu_create, mpu_part, open_log, pool, put, put_extents, runtime, tags,
};

/// Torn writes on half of the crashes.
const TORN: SimDiskFaults = SimDiskFaults {
    sync_error_probability: 0.0,
    torn_write_probability: 0.5,
    capacity: None,
};

type Acknowledged = Arc<Mutex<BTreeMap<(ShardRef, EpochSeq), LogRecord>>>;

fn shard_ref(n: u8) -> ShardRef {
    ShardRef::new(BucketId::new("b-sim").unwrap(), ShardId::new(n))
}

fn disk_label() -> Label {
    Label::new("disk-0").unwrap()
}

/// Reads every record the log holds.
async fn log_records(log: &SegmentLog<SimMount>) -> Vec<LogRecord> {
    let mut records = Vec::new();
    for segment in log.segments() {
        let mut scanner = log.scan(segment.id).unwrap();
        while let Some(scanned) = scanner.next().await.unwrap() {
            records.push(LogRecord::decode(&scanned.bytes).unwrap().0);
        }
    }
    records
}

/// The index that applying `records` in position order to an empty index
/// builds, without locations, which are node-local.
fn model(mut records: Vec<LogRecord>) -> IndexDump {
    records.sort_by(|a, b| (&a.shard, a.position).cmp(&(&b.shard, b.position)));
    let disk = SimDisk::new(0);
    let index = Index::open_sim(&disk.mount(), "model.redb", &index_config()).unwrap();
    let records: Vec<_> = records
        .into_iter()
        .map(|record| {
            let location = support::location(record.position);
            (record, location)
        })
        .collect();
    index.apply(&StateMachine, &records).unwrap();
    without_locations(index.read().unwrap().dump().unwrap())
}

fn without_locations(mut dump: IndexDump) -> IndexDump {
    dump.locations.clear();
    dump
}

/// One writer: a few operations on random shards and keys. It stops at the
/// first error other than a seal, which a failed sync causes.
async fn writer(
    shards: Vec<Shard<SimMount>>,
    seed: u64,
    acknowledged: Acknowledged,
) -> Result<(), String> {
    let mut rng = SmallRng::seed_from_u64(seed);
    for _ in 0..rng.random_range(4..16) {
        let shard = &shards[rng.random_range(0..shards.len())];
        let key = format!("key-{}", rng.random_range(0..5));
        let current = shard
            .index()
            .read()
            .unwrap()
            .entry(shard.shard(), &key)
            .unwrap();
        let seq = current
            .as_ref()
            .map_or(rng.random_range(0..9), |e| e.version.seq.get());
        let body = match rng.random_range(0..22) {
            0..=4 => put(&key, rng.random_range(0..400), seed),
            5..=7 => {
                let mut extents = Vec::new();
                let mut offset = 0;
                for _ in 0..rng.random_range(1..=3) {
                    let len = rng.random_range(600..2000);
                    match shard.append_extent(extent(&key, offset, len)).await {
                        Ok(reference) => {
                            let record = LogRecord {
                                shard: shard.shard().clone(),
                                position: reference.position,
                                body: RecordBody::Extent(extent(&key, offset, len)),
                            };
                            record_ack(&acknowledged, record);
                            extents.push(reference);
                        }
                        Err(ShardError::Sealed(_)) => break,
                        Err(error) => return Err(error.to_string()),
                    }
                    offset += len as u64;
                }
                if extents.is_empty() {
                    continue;
                }
                put_extents(&key, extents, seed)
            }
            8..=9 => delete(&key),
            10 => tags(&key, "x"),
            11..=14 => {
                let deleted = current.as_ref().is_some_and(|e| e.object.is_none());
                flushed(&key, seq, deleted)
            }
            15 => import(&key, seed),
            16..=17 => adopt(&key, seq, seed),
            18..=19 => {
                batch(shard, &mut rng, seed, &acknowledged).await?;
                continue;
            }
            _ => {
                shard.seal().await.map_err(|e| e.to_string())?;
                let refused = shard.commit(put(&key, 1, seed)).await;
                shard.unseal();
                if refused != Err(ShardError::Sealed(shard.shard().clone())) {
                    return Err(format!("a sealed shard took a write: {refused:?}"));
                }
                continue;
            }
        };
        match shard.commit(body.clone()).await {
            Ok(committed) => {
                if shard.applied() < committed.position {
                    return Err("a commit returned before its record was applied".into());
                }
                let record = LogRecord {
                    shard: shard.shard().clone(),
                    position: committed.position,
                    body,
                };
                record_ack(&acknowledged, record);
            }
            Err(ShardError::Sealed(_)) => {}
            Err(error) => return Err(error.to_string()),
        }
    }
    Ok(())
}

/// Commits writes of a few keys together, as a peer's `BATCH` is applied
/// (§7.8), some of them only if their key has no object.
async fn batch(
    shard: &Shard<SimMount>,
    rng: &mut SmallRng,
    seed: u64,
    acknowledged: &Acknowledged,
) -> Result<(), String> {
    let keys: BTreeSet<String> = (0..rng.random_range(2..=4))
        .map(|_| format!("key-{}", rng.random_range(0..5)))
        .collect();
    let bodies: Vec<RecordBody> = keys
        .iter()
        .map(|key| {
            if rng.random_bool(0.8) {
                put(key, rng.random_range(0..400), seed)
            } else {
                delete(key)
            }
        })
        .collect();
    let conditional: Vec<bool> = bodies.iter().map(|_| rng.random_bool(0.5)).collect();
    let results = shard
        .commit_all_if(bodies.clone(), |at, entry| {
            let exists = entry.is_some_and(|entry| entry.object.is_some());
            if conditional[at] && exists {
                Err(())
            } else {
                Ok(())
            }
        })
        .await;
    for (body, result) in bodies.into_iter().zip(results) {
        match result {
            Ok(Ok(committed)) => {
                if shard.applied() < committed.position {
                    return Err("a batch returned before its record was applied".into());
                }
                let record = LogRecord {
                    shard: shard.shard().clone(),
                    position: committed.position,
                    body,
                };
                record_ack(acknowledged, record);
            }
            Ok(Err(())) | Err(ShardError::Sealed(_)) => {}
            Err(error) => return Err(error.to_string()),
        }
    }
    Ok(())
}

fn record_ack(acknowledged: &Acknowledged, record: LogRecord) {
    let key = (record.shard.clone(), record.position);
    acknowledged.lock().unwrap().insert(key, record);
}

/// Runs one seed.
fn scenario(context: &mut SimContext) -> Result<(), Box<dyn std::error::Error>> {
    let disk = context.disk_with_faults(TORN);
    let mut rng = SmallRng::seed_from_u64(context.fork_seed());
    let restarts = rng.random_range(2..=4);
    let pool = pool();
    let acknowledged: Acknowledged = Arc::default();
    let mut epoch = 1;
    runtime().block_on(async {
        for _ in 0..restarts {
            let mount = disk.mount();
            let log = open_log(mount.clone()).await;
            let index = Arc::new(Index::open_sim(&mount, "index.redb", &index_config())?);
            let checkpointer = Checkpointer::new(
                Arc::clone(&index),
                BTreeMap::from([(disk_label(), log.clone())]),
                pool.clone(),
            );
            checkpointer.replay(Arc::new(StateMachine)).await?;

            let in_log = log_records(&log).await;
            let by_position: BTreeMap<_, _> = in_log
                .iter()
                .map(|record| ((record.shard.clone(), record.position), record))
                .collect();
            for (key, record) in acknowledged.lock().unwrap().iter() {
                assert_eq!(
                    by_position.get(key),
                    Some(&record),
                    "an acknowledged record was lost"
                );
            }
            let replayed = without_locations(index.read()?.dump()?);
            assert_eq!(replayed, model(in_log), "replay reproduced the index");

            if rng.random_bool(0.3) {
                epoch += 1;
            }
            let set = ShardSet::new(Arc::clone(&index), log.clone(), pool.clone());
            let mut shards = Vec::new();
            for n in 0..3 {
                shards.push(set.open(&config(&shard_ref(n), epoch)).await?);
            }
            let failing = rng.random_bool(0.2);
            let writers: Vec<_> = (0..rng.random_range(2..8))
                .map(|_| {
                    let task = writer(shards.clone(), rng.random(), Arc::clone(&acknowledged));
                    tokio::spawn(task)
                })
                .collect();
            if failing {
                // A sync fails while the writers run: the disk goes out of
                // service, and each shard that writes stops.
                tokio::task::yield_now().await;
                disk.fail_next_syncs(1);
            } else if rng.random_bool(0.5) {
                tokio::task::yield_now().await;
                checkpointer.checkpoint().await?;
            }
            let mut failed = false;
            for task in writers {
                if let Err(error) = task.await? {
                    assert!(failing, "a writer failed without a fault: {error}");
                    failed = true;
                }
            }
            if !failed {
                let live = without_locations(index.read()?.dump()?);
                assert_eq!(
                    live,
                    model(log_records(&log).await),
                    "the live path applied in order"
                );
                if rng.random_bool(0.5) {
                    checkpointer.checkpoint().await?;
                }
            }

            drop((set, shards, checkpointer, log));
            if failed || rng.random_bool(0.7) {
                // Power loss, which a disk out of service needs before it is
                // used again (§10.4).
                disk.crash();
                drop(index);
            } else {
                // The process is killed: the disk keeps every byte written.
                std::mem::forget(index);
            }
        }
        Ok(())
    })
}

#[test]
fn concurrent_writers_apply_in_order_and_replay_after_a_crash() {
    Runner::new().run(scenario);
}

/// One writer of the eviction scenario: `PUT`s, `DELETE`s, `FLUSHED` of
/// the current version, `IMPORT`, `ADOPT`, reads, and fills of evicted
/// entries, on random shards and keys. It stops at the first error.
async fn cache_writer(shards: Vec<Shard<SimMount>>, seed: u64) -> Result<(), String> {
    let mut rng = SmallRng::seed_from_u64(seed);
    for _ in 0..rng.random_range(8..24) {
        let shard = &shards[rng.random_range(0..shards.len())];
        let key = format!("key-{}", rng.random_range(0..8));
        let current = shard
            .index()
            .read()
            .unwrap()
            .entry(shard.shard(), &key)
            .unwrap();
        let body = match rng.random_range(0..20) {
            0..=4 => put(&key, rng.random_range(0..400), seed),
            5..=6 => {
                let mut extents = Vec::new();
                let mut offset = 0;
                for _ in 0..rng.random_range(1..=2) {
                    let len = rng.random_range(100..600);
                    let appended = shard.append_extent(extent(&key, offset, len)).await;
                    extents.push(appended.map_err(|e| e.to_string())?);
                    offset += len as u64;
                }
                put_extents(&key, extents, seed)
            }
            7 => delete(&key),
            8..=11 => match &current {
                Some(entry) if entry.state == EntryState::Dirty => {
                    flushed(&key, entry.version.seq.get(), entry.object.is_none())
                }
                _ => continue,
            },
            12 => import(&key, seed),
            13 => match &current {
                Some(entry) => adopt(&key, entry.version.seq.get(), seed),
                None => continue,
            },
            14..=15 => {
                shard.entry(&key).await.map_err(|e| e.to_string())?;
                continue;
            }
            // An eviction decided for the version the entry holds, in any
            // state: only a clean one goes.
            16 => {
                if let Some(entry) = current {
                    shard
                        .evict(&key, entry.version)
                        .await
                        .map_err(|e| e.to_string())?
                        .ok();
                }
                continue;
            }
            // An eviction decided before a write committed: the write wins.
            17 => {
                let Some(entry) = current.filter(|e| e.state == EntryState::Clean) else {
                    continue;
                };
                let written = shard.commit(put(&key, 10, seed)).await;
                written.map_err(|e| e.to_string())?;
                let late = shard.evict(&key, entry.version).await;
                if late.map_err(|e| e.to_string())?.is_ok() {
                    return Err(format!("an eviction of {key} undid a later write"));
                }
                continue;
            }
            _ => {
                // A read-through fill of an evicted entry (§9.2).
                let Some((version, object)) = current
                    .filter(|entry| entry.state == EntryState::Evicted)
                    .and_then(|entry| Some((entry.version, entry.object?)))
                else {
                    continue;
                };
                let mut filled = Vec::new();
                let mut offset = 0;
                while offset < object.size {
                    let len = (object.size - offset).min(300);
                    let appended = shard
                        .append_extent(extent(&key, offset, len as usize))
                        .await;
                    filled.push(appended.map_err(|e| e.to_string())?);
                    offset += len;
                }
                let payload = Payload::Extents(filled);
                shard
                    .fill(&key, version, payload)
                    .await
                    .map_err(|e| e.to_string())?
                    .ok();
                continue;
            }
        };
        shard.commit(body).await.map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// Checks that every dirty entry of `index`, and every clean one, has its
/// bytes located on the node, and returns the bytes of the clean ones.
fn check_payloads(index: &Index) -> Result<u64, String> {
    let read = index.read().map_err(|e| e.to_string())?;
    let mut clean = 0;
    for ((shard, key), entry) in read.dump().map_err(|e| e.to_string())?.entries {
        let Some(object) = entry.object else {
            continue;
        };
        let positions = match &object.payload {
            Payload::None => Vec::new(),
            Payload::Inline(position) => vec![*position],
            Payload::Extents(extents) => extents.iter().map(|e| e.position).collect(),
            Payload::Parts { .. } => return Err(format!("{key} is a multipart object")),
        };
        if entry.state == EntryState::Evicted {
            if !positions.is_empty() {
                return Err(format!("the evicted {key} kept its payload"));
            }
            continue;
        }
        let located = positions
            .iter()
            .all(|&position| matches!(read.location(&shard, position), Ok(Some(_))));
        let sized = object.size == 0 || !positions.is_empty();
        if !located || !sized {
            return Err(format!("{key} is {:?} without its bytes", entry.state));
        }
        if entry.state == EntryState::Clean {
            clean += object.size;
        }
    }
    Ok(clean)
}

/// `dump` as it is whichever clean entries this node keeps the payload
/// of: clean and evicted entries alike, without payload, and no locations.
fn without_cache(dump: IndexDump) -> IndexDump {
    let mut dump = without_locations(dump);
    for entry in dump.entries.values_mut() {
        if matches!(entry.state, EntryState::Clean | EntryState::Evicted) {
            entry.state = EntryState::Evicted;
            if let Some(object) = &mut entry.object {
                object.payload = Payload::None;
            }
        }
    }
    dump
}

/// Checks that `index` holds what applying the records of `log` in
/// position order does, but for which clean payloads it kept: eviction
/// and fills change nothing else, and never a dirty entry.
async fn check_entries(index: &Index, log: &SegmentLog<SimMount>) -> Result<(), String> {
    let live = without_cache(index.read().unwrap().dump().unwrap());
    if live != without_cache(model(log_records(log).await)) {
        return Err("eviction changed more than which clean payloads are kept".into());
    }
    Ok(())
}

/// Runs one seed of the eviction scenario.
fn eviction(context: &mut SimContext) -> Result<(), Box<dyn std::error::Error>> {
    let disk = context.disk_with_faults(TORN);
    let mut rng = SmallRng::seed_from_u64(context.fork_seed());
    let max_bytes = rng.random_range(0..3000);
    let pool = pool();
    runtime().block_on(async {
        for _ in 0..rng.random_range(2..=4) {
            let mount = disk.mount();
            let log = open_log(mount.clone()).await;
            let index = Arc::new(Index::open_sim(&mount, "index.redb", &index_config())?);
            let checkpointer = Checkpointer::new(
                Arc::clone(&index),
                BTreeMap::from([(disk_label(), log.clone())]),
                pool.clone(),
            );
            checkpointer.replay(Arc::new(StateMachine)).await?;
            check_payloads(&index)?;
            check_entries(&index, &log).await?;

            let settings = CacheSettings {
                max_bytes,
                reserve_fraction: 0.0,
            };
            let cache = CleanCache::new(settings, CacheMetrics::default());
            let set = ShardSet::new(Arc::clone(&index), log.clone(), pool.clone());
            set.use_cache(&cache).await;
            let mut shards = Vec::new();
            for n in 0..2 {
                shards.push(set.open(&config(&shard_ref(n), 1)).await?);
            }
            // The cache evicts while the writers run, and stops between
            // rounds once they are done.
            let done = Arc::new(AtomicBool::new(false));
            let evictor = {
                let (cache, set, done) = (cache.clone(), set.clone(), Arc::clone(&done));
                tokio::spawn(async move {
                    while !done.load(Ordering::SeqCst) {
                        cache.reclaim(&set).await;
                        tokio::task::yield_now().await;
                    }
                })
            };
            let writers: Vec<_> = (0..rng.random_range(2..6))
                .map(|_| tokio::spawn(cache_writer(shards.clone(), rng.random())))
                .collect();
            for writer in writers {
                writer.await??;
                if rng.random_bool(0.3) {
                    checkpointer.checkpoint().await?;
                }
            }
            done.store(true, Ordering::SeqCst);
            evictor.await?;
            cache.reclaim(&set).await;
            // Every clean byte the index holds is one the cache keeps, and
            // the cache is within its bound; no dirty byte went.
            let clean = check_payloads(&index)?;
            check_entries(&index, &log).await?;
            let usage = cache.usage();
            assert_eq!(usage.bytes, clean, "the cache counts every clean byte");
            assert!(usage.bytes <= max_bytes, "{usage:?} over {max_bytes}");

            drop((set, shards, checkpointer, log));
            if rng.random_bool(0.6) {
                disk.crash();
                drop(index);
            } else {
                std::mem::forget(index);
            }
        }
        Ok(())
    })
}

/// Eviction (§9.3) under restarts: a workload larger than the cache keeps
/// every dirty byte, evicts only clean payload, fills evicted keys again,
/// and a restarted node finds its clean entries.
///
/// A seed costs about four of the other scenario's: 256 seeds take about a
/// minute in a debug build.
#[test]
fn eviction_keeps_every_dirty_byte_across_restarts() {
    Runner::with_cost(8, 4).run(eviction);
}

/// Every record compaction may have dropped: the records of the released
/// segments, as the compaction scenario saw them before each pass, by
/// shard and position.
type Archive = BTreeMap<(ShardRef, EpochSeq), LogRecord>;

/// Adds the records of each segment `log` released to `archive`. A disk
/// that lost power reads nothing more.
async fn archive_released(log: &SegmentLog<SimMount>, archive: &Mutex<Archive>) {
    for id in log.released() {
        let Ok(mut scanner) = log.scan(id) else {
            return;
        };
        while let Ok(Some(scanned)) = scanner.next().await {
            let record = scanned.decode().unwrap();
            let key = (record.shard.clone(), record.position);
            archive.lock().unwrap().insert(key, record);
        }
    }
}

/// Commits `body` to `shard` and records it as acknowledged.
async fn commit_acked(
    shard: &Shard<SimMount>,
    body: RecordBody,
    acknowledged: &Acknowledged,
) -> Result<EpochSeq, String> {
    let committed = shard
        .commit(body.clone())
        .await
        .map_err(|e| e.to_string())?;
    let record = LogRecord {
        shard: shard.shard().clone(),
        position: committed.position,
        body,
    };
    record_ack(acknowledged, record);
    Ok(committed.position)
}

/// Appends an extent of `len` bytes of `key` to `shard`, and records it as
/// acknowledged.
async fn extent_acked(
    shard: &Shard<SimMount>,
    key: &str,
    len: usize,
    acknowledged: &Acknowledged,
) -> Result<skys3_log::record::ExtentRef, String> {
    let appended = shard
        .append_extent(extent(key, 0, len))
        .await
        .map_err(|e| e.to_string())?;
    let record = LogRecord {
        shard: shard.shard().clone(),
        position: appended.position,
        body: RecordBody::Extent(extent(key, 0, len)),
    };
    record_ack(acknowledged, record);
    Ok(appended)
}

/// A multipart upload of `key`: one or two parts, inline or in an extent,
/// then completed, aborted, or left open.
async fn multipart(
    shard: &Shard<SimMount>,
    key: &str,
    rng: &mut SmallRng,
    acknowledged: &Acknowledged,
) -> Result<(), String> {
    let upload = commit_acked(shard, mpu_create(key), acknowledged).await?;
    let mut parts = Vec::new();
    let mut size = 0;
    for number in 1..=rng.random_range(1..=2u16) {
        let len = rng.random_range(1..900);
        let body = if len < 400 {
            mpu_part(key, upload, number, len, Vec::new())
        } else {
            let extent = extent_acked(shard, key, len, acknowledged).await?;
            mpu_part(key, upload, number, 0, vec![extent])
        };
        size += len as u64;
        parts.push((number, commit_acked(shard, body, acknowledged).await?));
    }
    let end = match rng.random_range(0..4) {
        0 => return Ok(()),
        1 => mpu_abort(key, upload),
        _ => mpu_complete(key, upload, &parts, size),
    };
    commit_acked(shard, end, acknowledged).await.map(drop)
}

/// One writer of the compaction scenario: `PUT`s inline and in extents,
/// `DELETE`, `TAGS`, `FLUSHED` of the current version, reads, and multipart
/// uploads, on random shards and keys. It stops at the first error.
async fn compaction_writer(
    shards: Vec<Shard<SimMount>>,
    seed: u64,
    acknowledged: Acknowledged,
) -> Result<(), String> {
    let mut rng = SmallRng::seed_from_u64(seed);
    for _ in 0..rng.random_range(16..48) {
        let shard = &shards[rng.random_range(0..shards.len())];
        let key = format!("key-{}", rng.random_range(0..6));
        // The index fails once the power is cut.
        let current = shard
            .index()
            .read()
            .and_then(|read| read.entry(shard.shard(), &key))
            .map_err(|e| e.to_string())?;
        let body = match rng.random_range(0..20) {
            0..=5 => put(&key, rng.random_range(0..400), seed),
            6..=7 => {
                let mut extents = Vec::new();
                for _ in 0..rng.random_range(1..=3) {
                    let len = rng.random_range(300..1500);
                    extents.push(extent_acked(shard, &key, len, &acknowledged).await?);
                }
                put_extents(&key, extents, seed)
            }
            8 => delete(&key),
            9 => tags(&key, "x"),
            10..=13 => match &current {
                Some(entry) if entry.state == EntryState::Dirty => {
                    flushed(&key, entry.version.seq.get(), entry.object.is_none())
                }
                _ => continue,
            },
            14..=15 => {
                shard.entry(&key).await.map_err(|e| e.to_string())?;
                continue;
            }
            _ => {
                multipart(shard, &key, &mut rng, &acknowledged).await?;
                continue;
            }
        };
        commit_acked(shard, body, &acknowledged).await?;
    }
    Ok(())
}

/// `dump` as it is whichever clean payloads the node kept, as
/// [`without_cache`] makes it, and with the parts of evicted multipart
/// objects without their bytes too, as well as the parts of objects a
/// later write replaced, which nothing reads.
fn without_cached_parts(dump: IndexDump) -> IndexDump {
    let mut dump = without_cache(dump);
    let named = |state: Option<EntryState>| -> Vec<(ShardRef, EpochSeq)> {
        dump.entries
            .iter()
            .filter(|(_, entry)| state.is_none_or(|state| entry.state == state))
            .filter_map(
                |((shard, _), entry)| match &entry.object.as_ref()?.payload {
                    Payload::Parts { upload, .. } => Some((shard.clone(), *upload)),
                    _ => None,
                },
            )
            .collect()
    };
    let evicted = named(Some(EntryState::Evicted));
    let live: Vec<(ShardRef, EpochSeq)> = named(None)
        .into_iter()
        .chain(
            dump.uploads
                .keys()
                .map(|(shard, _, upload)| (shard.clone(), *upload)),
        )
        .collect();
    for ((shard, upload, _), part) in &mut dump.parts {
        let upload = (shard.clone(), *upload);
        if evicted.contains(&upload) || !live.contains(&upload) {
            part.payload = Payload::None;
        }
    }
    dump
}

/// Drops the bytes of each version in `expected` whose bytes `found` does
/// not hold: a `TAGS` of an evicted entry makes a dirty version without
/// bytes, the evicted ones it inherits (§9.3), so any state can lack them.
/// Payload that an entry names is checked by location instead.
fn without_bytes_where_dropped(found: &IndexDump, expected: &mut IndexDump) {
    let mut uploads = Vec::new();
    for (key, entry) in &found.entries {
        let Some(object) = &entry.object else {
            continue;
        };
        match &object.payload {
            Payload::None => {
                if let Some(object) = expected
                    .entries
                    .get_mut(key)
                    .and_then(|entry| entry.object.as_mut())
                {
                    object.payload = Payload::None;
                }
            }
            Payload::Parts { upload, .. } => {
                let bare = found
                    .parts
                    .iter()
                    .filter(|((shard, part_upload, _), _)| *shard == key.0 && part_upload == upload)
                    .all(|(_, part)| part.payload == Payload::None);
                if bare {
                    uploads.push((key.0.clone(), *upload));
                }
            }
            _ => {}
        }
    }
    for ((shard, upload, _), part) in &mut expected.parts {
        if uploads.contains(&(shard.clone(), *upload)) {
            part.payload = Payload::None;
        }
    }
}

/// The first row where `found` and `expected` differ, for a failure's
/// message.
fn difference(found: &IndexDump, expected: &IndexDump) -> String {
    fn first<K: std::fmt::Debug + Ord, V: std::fmt::Debug + PartialEq>(
        table: &str,
        found: &BTreeMap<K, V>,
        expected: &BTreeMap<K, V>,
    ) -> Option<String> {
        let keys: std::collections::BTreeSet<&K> = found.keys().chain(expected.keys()).collect();
        keys.into_iter()
            .find(|key| found.get(key) != expected.get(key))
            .map(|key| {
                format!(
                    "{table} {key:?}: {:?} instead of {:?}",
                    found.get(key),
                    expected.get(key)
                )
            })
    }
    first("entry", &found.entries, &expected.entries)
        .or_else(|| first("upload", &found.uploads, &expected.uploads))
        .or_else(|| first("part", &found.parts, &expected.parts))
        .or_else(|| first("applied", &found.applied, &expected.applied))
        .unwrap_or_else(|| "elsewhere".to_owned())
}

/// The positions of the records that hold `payload`.
fn positions(payload: &Payload) -> Vec<EpochSeq> {
    match payload {
        Payload::Inline(position) => vec![*position],
        Payload::Extents(extents) => extents.iter().map(|extent| extent.position).collect(),
        Payload::None | Payload::Parts { .. } => Vec::new(),
    }
}

/// The positions that the parts of the upload `upload` of `shard` in
/// `dump` name, each with its shard.
fn part_positions<'a>(
    dump: &'a IndexDump,
    shard: &'a ShardRef,
    upload: EpochSeq,
) -> impl Iterator<Item = (&'a ShardRef, EpochSeq)> {
    dump.parts
        .iter()
        .filter(move |((part_shard, part_upload, _), _)| {
            part_shard == shard && *part_upload == upload
        })
        .flat_map(move |(_, part)| {
            positions(&part.payload)
                .into_iter()
                .map(move |p| (shard, p))
        })
}

/// Checks what compaction must keep (see the module docs), given every
/// record compaction may have dropped in `archive`.
async fn check_compacted(
    index: &Index,
    log: &SegmentLog<SimMount>,
    archive: &Archive,
    acknowledged: &Acknowledged,
) -> Result<(), String> {
    let in_log = log_records(log).await;
    let mut records = archive.clone();
    for record in &in_log {
        let key = (record.shard.clone(), record.position);
        records.insert(key, record.clone());
    }
    for (key, record) in acknowledged.lock().unwrap().iter() {
        if records.get(key) != Some(record) {
            return Err(format!("the acknowledged record at {key:?} was lost"));
        }
    }
    let dump = index.read().unwrap().dump().unwrap();
    let found = without_cached_parts(dump.clone());
    let mut expected = without_cached_parts(model(records.values().cloned().collect()));
    without_bytes_where_dropped(&found, &mut expected);
    if found != expected {
        return Err(format!(
            "the index is not what applying every record makes it: {}",
            difference(&found, &expected)
        ));
    }

    // Every payload an entry or a part names holds its record.
    let read = index.read().unwrap();
    let mut named = Vec::new();
    for ((shard, _), entry) in &dump.entries {
        let Some(object) = &entry.object else {
            continue;
        };
        if let Payload::Parts { upload, .. } = &object.payload {
            named.extend(part_positions(&dump, shard, *upload));
        }
        named.extend(positions(&object.payload).into_iter().map(|p| (shard, p)));
    }
    for (shard, _, upload) in dump.uploads.keys() {
        named.extend(part_positions(&dump, shard, *upload));
    }
    for (shard, position) in named {
        let location = read.location(shard, position).map_err(|e| e.to_string())?;
        let Some(location) = location else {
            return Err(format!("{shard} locates no payload at {position}"));
        };
        let found = log.read(location).await.map_err(|e| e.to_string())?;
        if records.get(&(shard.clone(), position)) != Some(&found) {
            return Err(format!("{shard} locates another record for {position}"));
        }
    }

    // Each shard's latest CONFIG record is in the log.
    for (shard, config) in read.configs().map_err(|e| e.to_string())? {
        let kept = in_log.iter().any(|record| {
            record.shard == shard && matches!(&record.body, RecordBody::Config(c) if *c == config)
        });
        if !kept {
            return Err(format!("the latest CONFIG record of {shard} is gone"));
        }
    }
    Ok(())
}

/// Until `stop`, makes each clean key of `shards` dirty with a `TAGS`,
/// which keeps its bytes, and clean again with a `FLUSHED` of the new
/// version, one key at a time.
async fn tag_clean(
    shards: Vec<Shard<SimMount>>,
    acknowledged: Acknowledged,
    stop: Arc<AtomicBool>,
) -> Result<(), String> {
    while !stop.load(Ordering::SeqCst) {
        let mut tagged = false;
        for shard in &shards {
            let read = shard.index().read().map_err(|e| e.to_string())?;
            let entries = read.entries(shard.shard(), None, 100);
            for (key, entry) in entries.map_err(|e| e.to_string())? {
                if stop.load(Ordering::SeqCst) {
                    return Ok(());
                }
                if entry.state == EntryState::Clean && entry.object.is_some() {
                    let tagged_at = commit_acked(shard, tags(&key, "raced"), &acknowledged).await?;
                    let clean = flushed(&key, tagged_at.seq.get(), false);
                    commit_acked(shard, clean, &acknowledged).await?;
                    tagged = true;
                }
            }
        }
        if !tagged {
            return Ok(());
        }
    }
    Ok(())
}

/// Before or after its sync, at random.
fn either_side(rng: &mut SmallRng) -> SyncCut {
    if rng.random_bool(0.5) {
        SyncCut::Before
    } else {
        SyncCut::After
    }
}

/// The maintenance of one life of the compaction scenario until `done`:
/// checkpoints, each followed by a compaction pass, and the cache's
/// evictions. It fails on an error unless the power was cut.
async fn maintain(
    compactor: Compactor<SimMount>,
    checkpointer: Arc<Checkpointer<SimMount>>,
    archive: Arc<Mutex<Archive>>,
    power: SimPower,
    done: Arc<AtomicBool>,
) -> Result<(), String> {
    let (_, log) = checkpointer.logs().first_key_value().expect("one disk");
    let log = log.clone();
    while !done.load(Ordering::SeqCst) && !power.is_cut() {
        let passed = async {
            checkpointer.checkpoint().await.map_err(|e| e.to_string())?;
            archive_released(&log, &archive).await;
            compactor.compact().await.map_err(|e| e.to_string())
        };
        match passed.await {
            Err(error) if !power.is_cut() => return Err(error),
            _ => {}
        }
        tokio::task::yield_now().await;
    }
    Ok(())
}

/// Runs one seed of the compaction scenario.
fn compaction(context: &mut SimContext) -> Result<(), Box<dyn std::error::Error>> {
    let disk = context.disk_with_faults(TORN);
    let power = SimPower::new();
    disk.set_power(&power);
    let mut rng = SmallRng::seed_from_u64(context.fork_seed());
    let pool = pool();
    let acknowledged: Acknowledged = Arc::default();
    let archive = Arc::new(Mutex::new(Archive::new()));
    let mut epoch = 1;
    runtime().block_on(async {
        for _ in 0..rng.random_range(2..=4) {
            // No cut is planned until one is drawn.
            power.cut_at_sync(u64::MAX, SyncCut::Before);
            let mount = disk.mount();
            let log = open_log(mount.clone()).await;
            let index = Arc::new(Index::open_sim(&mount, "index.redb", &index_config())?);
            let checkpointer = Arc::new(Checkpointer::new(
                Arc::clone(&index),
                BTreeMap::from([(disk_label(), log.clone())]),
                pool.clone(),
            ));
            checkpointer.replay(Arc::new(StateMachine)).await?;
            let archived = archive.lock().unwrap().clone();
            check_compacted(&index, &log, &archived, &acknowledged).await?;

            if rng.random_bool(0.3) {
                epoch += 1;
            }
            let set = ShardSet::new(Arc::clone(&index), log.clone(), pool.clone());
            // No gateway reads here, so unreferenced payload goes at once.
            set.reads().configure(ReadSettings {
                release_delay: Duration::ZERO,
                ..ReadSettings::default()
            });
            let cache = rng.random_bool(0.7).then(|| {
                let settings = CacheSettings {
                    max_bytes: rng.random_range(0..4000),
                    reserve_fraction: 0.0,
                };
                CleanCache::new(settings, CacheMetrics::default())
            });
            if let Some(cache) = &cache {
                set.use_cache(cache).await;
            }
            let mut shards = Vec::new();
            for n in 0..2 {
                shards.push(set.open(&config(&shard_ref(n), epoch)).await?);
            }
            // Extents may be on their way while writers run, so unnamed
            // ones wait.
            let settings = CompactionSettings {
                live_threshold: rng.random_range(0.3..0.95),
                unreferenced_ttl: Duration::from_secs(3600),
            };
            if rng.random_bool(0.2) {
                let sync = power.syncs() + rng.random_range(0..200);
                power.cut_at_sync(sync, either_side(&mut rng));
            }
            let done = Arc::new(AtomicBool::new(false));
            let maintenance = tokio::spawn(maintain(
                Compactor::new(set.clone(), settings, CompactionMetrics::default()),
                Arc::clone(&checkpointer),
                Arc::clone(&archive),
                power.clone(),
                Arc::clone(&done),
            ));
            let evictor = cache.clone().map(|cache| {
                let (set, done) = (set.clone(), Arc::clone(&done));
                tokio::spawn(async move {
                    while !done.load(Ordering::SeqCst) {
                        cache.reclaim(&set).await;
                        tokio::task::yield_now().await;
                    }
                })
            });
            let writers: Vec<_> = (0..rng.random_range(3..7))
                .map(|_| {
                    let writer =
                        compaction_writer(shards.clone(), rng.random(), Arc::clone(&acknowledged));
                    tokio::spawn(writer)
                })
                .collect();
            for writer in writers {
                if let Err(error) = writer.await?
                    && !power.is_cut()
                {
                    return Err(error.into());
                }
            }
            done.store(true, Ordering::SeqCst);
            maintenance.await??;
            if let Some(evictor) = evictor {
                evictor.await?;
            }

            if !power.is_cut() {
                // Nothing arrives any more, so unnamed extents go at once,
                // and the power fails at a sync of the pass, most often.
                checkpointer.checkpoint().await?;
                archive_released(&log, &archive).await;
                if rng.random_bool(0.7) {
                    let sync = power.syncs() + rng.random_range(0..16);
                    power.cut_at_sync(sync, either_side(&mut rng));
                }
                let quiet = CompactionSettings {
                    unreferenced_ttl: Duration::ZERO,
                    ..settings
                };
                let compactor = Compactor::new(set.clone(), quiet, CompactionMetrics::default());
                // A TAGS of a clean key while the pass runs makes bytes it
                // may have decided to evict dirty again.
                let stop = Arc::new(AtomicBool::new(false));
                let racer = rng.random_bool(0.5).then(|| {
                    tokio::spawn(tag_clean(
                        shards.clone(),
                        Arc::clone(&acknowledged),
                        Arc::clone(&stop),
                    ))
                });
                let passed = compactor.compact().await;
                stop.store(true, Ordering::SeqCst);
                let raced = match racer {
                    Some(racer) => racer.await?,
                    None => Ok(()),
                };
                if !power.is_cut() {
                    raced?;
                    passed?;
                    let archived = archive.lock().unwrap().clone();
                    check_compacted(&index, &log, &archived, &acknowledged).await?;
                }
            }

            drop((set, shards, cache, checkpointer, log));
            if power.is_cut() || rng.random_bool(0.6) {
                disk.crash();
                drop(index);
            } else {
                std::mem::forget(index);
            }
        }
        Ok(())
    })
}

/// Compaction (§10.3) under power cuts at its syncs: nothing acknowledged
/// is lost, replay reproduces the index, every named payload is located,
/// and each shard's latest `CONFIG` record stays.
#[test]
fn compaction_loses_nothing_when_the_power_fails() {
    Runner::with_cost(4, 8).run(compaction);
}
