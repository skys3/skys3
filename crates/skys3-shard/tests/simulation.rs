//! Simulation scenarios for the shard runtime, run by CI's simulation job
//! with a larger seed set.
//!
//! Each seed runs one node over several restarts. In each life the node
//! replays its log, opens a few shards (sometimes in a newer epoch), and
//! runs concurrent writers against them: inline and extent-backed `PUT`s,
//! `DELETE`, `TAGS`, `FLUSHED`, `IMPORT`, and `ADOPT`, with seals,
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

mod support;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use skys3_index::{Checkpointer, Index, IndexDump};
use skys3_io::{SimDisk, SimDiskFaults, SimMount};
use skys3_log::{LogRecord, RecordBody, SegmentLog, ShardRef};
use skys3_shard::{Shard, ShardError, ShardSet, StateMachine};
use skys3_sim::{Runner, SimContext};
use skys3_types::{BucketId, EpochSeq, Label, ShardId};
use support::{
    adopt, config, delete, extent, flushed, import, index_config, open_log, pool, put, put_extents,
    runtime, tags,
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
        let body = match rng.random_range(0..20) {
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
