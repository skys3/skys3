//! Conditional writes and reads of a shard: `commit_if` checks a key's
//! entry as of the record's position, also while earlier writes of the key
//! are sequenced but not yet applied (§5.1), and `commit_all_if` does so
//! for records of several keys sequenced together.

mod support;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use skys3_index::{Entry, Index};
use skys3_io::{BlockingPool, SimDisk, SimMount};
use skys3_log::{LogConfig, RecordBody, SegmentLog};
use skys3_shard::{Shard, ShardError};
use skys3_types::Seq;
use support::{at, config, delete, extent, fill, index_config, log_config, pool, put, runtime};

/// A shard on a fresh disk whose group commits wait `delay` for more
/// records.
async fn open(delay: Duration) -> (SimDisk, Shard<SimMount>) {
    let disk = SimDisk::new(5);
    let clock = Arc::new(skys3_io::MonotonicClock::new());
    let log_config = LogConfig {
        group_commit_max_delay: delay,
        ..log_config()
    };
    let (log, _) = SegmentLog::open(disk.mount(), log_config, clock)
        .await
        .unwrap();
    let index = Arc::new(Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap());
    let pool: BlockingPool = pool();
    let shard = Shard::open(&config(&support::shard(0), 1), log, index, pool)
        .await
        .unwrap();
    (disk, shard)
}

/// `If-None-Match: *`: the key has no object.
fn absent(entry: Option<&Entry>) -> Result<(), &'static str> {
    match entry.and_then(|entry| entry.object.as_ref()) {
        Some(_) => Err("exists"),
        None => Ok(()),
    }
}

#[test]
fn a_condition_sees_writes_sequenced_but_not_applied() {
    runtime().block_on(async {
        // Each group commit waits, so a write stays unapplied for a while.
        let (_disk, shard) = open(Duration::from_millis(50)).await;
        let writer = shard.clone();
        let first = tokio::spawn(async move { writer.commit(put("a", 3, 1)).await });
        // Let the write be sequenced; it is not applied before its group
        // commit's delay has passed.
        tokio::task::yield_now().await;
        assert_eq!(shard.applied(), at(0));
        assert_eq!(shard.entry("a").await.unwrap(), None);

        let refused = shard.commit_if(put("a", 4, 2), absent).await.unwrap();
        assert_eq!(refused, Err("exists"));
        let committed = first.await.unwrap().unwrap();
        assert_eq!(committed.position, at(1));
        // Nothing was appended for the refused write.
        assert_eq!(shard.applied(), at(1));
        let entry = shard.entry("a").await.unwrap().unwrap();
        assert_eq!(entry.version, at(1));

        // A delete clears the way, also before it is applied.
        let writer = shard.clone();
        let deleted = tokio::spawn(async move { writer.commit(delete("a")).await });
        tokio::task::yield_now().await;
        let created = shard.commit_if(put("a", 5, 3), absent).await.unwrap();
        assert_eq!(created.unwrap().position, at(3));
        assert_eq!(deleted.await.unwrap().unwrap().position, at(2));
    });
}

#[test]
fn conditional_creations_race_to_one_winner() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        let (_disk, shard) = open(Duration::ZERO).await;
        for round in 0..16 {
            let key = format!("k{round}");
            let tasks: Vec<_> = (0..8)
                .map(|n| {
                    let (shard, key) = (shard.clone(), key.clone());
                    tokio::spawn(async move {
                        // Unconditional writes of other keys interleave.
                        shard
                            .commit(put(&format!("{key}-{n}"), 1, n))
                            .await
                            .unwrap();
                        shard.commit_if(put(&key, 2, n), absent).await.unwrap()
                    })
                })
                .collect();
            let mut won = 0;
            for task in tasks {
                won += usize::from(task.await.unwrap().is_ok());
            }
            assert_eq!(won, 1, "round {round}");
        }
    });
}

#[test]
fn compare_and_swap_loses_no_update() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        let (_disk, shard) = open(Duration::ZERO).await;
        shard.commit(put("counter", 0, 0)).await.unwrap();
        let tasks: Vec<_> = (0..6)
            .map(|_| {
                let shard = shard.clone();
                tokio::spawn(async move {
                    for _ in 0..8 {
                        loop {
                            let seen = shard.entry("counter").await.unwrap().unwrap();
                            let next = seen.object.as_ref().unwrap().size + 1;
                            let len = usize::try_from(next).unwrap();
                            let swapped = shard
                                .commit_if(put("counter", len, next), |entry| {
                                    match entry.map(|entry| entry.version) {
                                        Some(version) if version == seen.version => Ok(()),
                                        _ => Err(()),
                                    }
                                })
                                .await
                                .unwrap();
                            if swapped.is_ok() {
                                break;
                            }
                        }
                    }
                })
            })
            .collect();
        for task in tasks {
            task.await.unwrap();
        }
        let entry = shard.entry("counter").await.unwrap().unwrap();
        assert_eq!(entry.object.unwrap().size, 48);
    });
}

#[test]
fn an_abandoned_conditional_write_leaves_nothing_behind() {
    runtime().block_on(async {
        let (_disk, shard) = open(Duration::ZERO).await;
        // Dropped while it reads the index.
        let abandoned = shard.commit_if(put("a", 1, 1), absent);
        let finished = tokio::select! {
            biased;
            _ = abandoned => true,
            () = std::future::ready(()) => false,
        };
        assert!(!finished);
        assert_eq!(shard.entry("a").await.unwrap(), None);
        shard.commit(put("a", 1, 1)).await.unwrap();
        let refused = shard.commit_if(put("a", 1, 2), absent).await.unwrap();
        assert_eq!(refused, Err("exists"));
    });
}

#[test]
fn conditions_need_a_keyed_record() {
    runtime().block_on(async {
        let (_disk, shard) = open(Duration::ZERO).await;
        let extent = RecordBody::Extent(extent("a", 0, 10));
        let error = shard.commit_if(extent, absent).await.unwrap_err();
        assert!(matches!(error, ShardError::InvalidRecord { .. }), "{error}");
        shard.close().await.unwrap();
        let error = shard.commit_if(put("a", 1, 1), absent).await.unwrap_err();
        assert!(matches!(error, ShardError::Unavailable { .. }), "{error}");
    });
}

#[test]
fn payloads_are_read_by_position() {
    runtime().block_on(async {
        let (_disk, shard) = open(Duration::ZERO).await;
        let inline = shard.commit(put("a", 7, 1)).await.unwrap();
        assert_eq!(shard.payload(inline.position).await.unwrap(), fill(7));
        let reference = shard.append_extent(extent("b", 0, 600)).await.unwrap();
        assert_eq!(reference.len, 600);
        assert_eq!(shard.payload(reference.position).await.unwrap(), fill(600));
        // A delete holds no payload, and no record is at a later position.
        let deleted = shard.commit(delete("a")).await.unwrap();
        for position in [deleted.position, at(99)] {
            let error = shard.payload(position).await.unwrap_err();
            assert!(matches!(error, ShardError::Unavailable { .. }), "{error}");
        }
        let entry = shard.entry("a").await.unwrap().unwrap();
        assert!(entry.object.is_none());
        assert_eq!(entry.version.seq, Seq::new(3));
        let _ = Bytes::new();
    });
}

/// A shard whose log takes a whole batch in one group commit, and its log.
async fn open_batched() -> (SimDisk, Shard<SimMount>, SegmentLog<SimMount>) {
    open_grouped(1 << 20, Duration::ZERO).await
}

/// A shard whose group commits hold `max_bytes` and wait `delay` for more
/// records, and its log.
async fn open_grouped(
    max_bytes: u64,
    delay: Duration,
) -> (SimDisk, Shard<SimMount>, SegmentLog<SimMount>) {
    let disk = SimDisk::new(9);
    let clock = Arc::new(skys3_io::MonotonicClock::new());
    let log_config = LogConfig {
        segment_bytes: 1 << 20,
        group_commit_max_bytes: max_bytes,
        group_commit_max_delay: delay,
        ..log_config()
    };
    let (log, _) = SegmentLog::open(disk.mount(), log_config, clock)
        .await
        .unwrap();
    let index = Arc::new(Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap());
    let shard = Shard::open(&config(&support::shard(0), 1), log.clone(), index, pool())
        .await
        .unwrap();
    (disk, shard, log)
}

#[test]
fn a_batch_is_sequenced_in_one_pass_and_one_group_commit() {
    runtime().block_on(async {
        let (_disk, shard, log) = open_batched().await;
        for key in ["k3", "k7"] {
            shard.commit(put(key, 1, 0)).await.unwrap();
        }
        let before = log.stats();
        let mut bodies: Vec<_> = (0..32).map(|n| put(&format!("k{n}"), 10, n)).collect();
        // A repeated key and a record that names no key are refused.
        bodies.push(put("k1", 10, 99));
        bodies.push(RecordBody::Extent(extent("k40", 0, 10)));
        let mut checked = Vec::new();
        let results = shard
            .commit_all_if(bodies, |at, entry| {
                checked.push(at);
                absent(entry)
            })
            .await;
        assert_eq!(results.len(), 34);
        assert_eq!(checked, (0..32).collect::<Vec<_>>());

        // The keys that exist fail their condition; the others take
        // consecutive positions after the two earlier writes.
        let mut next = 3;
        for (n, result) in results.iter().take(32).enumerate() {
            match result {
                Ok(Err(refused)) => {
                    assert!(n == 3 || n == 7, "k{n}");
                    assert_eq!(*refused, "exists");
                }
                Ok(Ok(committed)) => {
                    assert_eq!(committed.position, at(next), "k{n}");
                    next += 1;
                }
                Err(error) => panic!("k{n}: {error}"),
            }
        }
        for result in &results[32..] {
            assert!(
                matches!(result, Err(ShardError::InvalidRecord { .. })),
                "{result:?}"
            );
        }
        // Thirty records, one group commit.
        let after = log.stats();
        assert_eq!(after.records - before.records, 30);
        assert_eq!(after.group_commits - before.group_commits, 1);
        let entry = shard.entry("k31").await.unwrap().unwrap();
        assert_eq!(entry.version, at(32));
        assert!(shard.commit_all_if(Vec::new(), absent_at).await.is_empty());
    });
}

/// [`absent`], for a check that also gets the record's index.
fn absent_at(_: usize, entry: Option<&Entry>) -> Result<(), &'static str> {
    absent(entry)
}

#[test]
fn a_batch_waits_for_unapplied_writes_of_its_keys() {
    runtime().block_on(async {
        // Each group commit waits, so a write stays unapplied for a while.
        let (_disk, shard) = open(Duration::from_millis(50)).await;
        let writer = shard.clone();
        let first = tokio::spawn(async move { writer.commit(put("a", 3, 1)).await });
        tokio::task::yield_now().await;
        assert_eq!(shard.applied(), at(0));

        let results = shard
            .commit_all_if(vec![put("b", 4, 2), put("a", 4, 3)], absent_at)
            .await;
        assert_eq!(first.await.unwrap().unwrap().position, at(1));
        assert_eq!(
            results[0].as_ref().unwrap().as_ref().unwrap().position,
            at(2)
        );
        assert_eq!(results[1], Ok(Err("exists")));
        assert_eq!(shard.applied(), at(2));
    });
}

#[test]
fn batched_and_single_conditional_creations_race_to_one_winner() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        let (_disk, shard) = open(Duration::ZERO).await;
        for round in 0..16u64 {
            let keys: Vec<_> = (0..4).map(|n| format!("r{round}-{n}")).collect();
            let batches: Vec<_> = (0..4u64)
                .map(|writer| {
                    let (shard, keys) = (shard.clone(), keys.clone());
                    tokio::spawn(async move {
                        if writer % 2 == 0 {
                            let bodies = keys.iter().map(|key| put(key, 2, writer)).collect();
                            let results = shard.commit_all_if(bodies, absent_at).await;
                            results.into_iter().map(|r| r.unwrap().is_ok()).collect()
                        } else {
                            let mut won = Vec::new();
                            for key in keys.iter().rev() {
                                let result = shard.commit_if(put(key, 2, writer), absent).await;
                                won.push(result.unwrap().is_ok());
                            }
                            won.reverse();
                            won
                        }
                    })
                })
                .collect();
            let mut winners = [0; 4];
            for task in batches {
                for (key, won) in task.await.unwrap().into_iter().enumerate() {
                    winners[key] += usize::from(won);
                }
            }
            assert_eq!(winners, [1; 4], "round {round}");
        }
    });
}

#[test]
fn a_batch_on_a_sealed_or_closed_shard_is_refused() {
    runtime().block_on(async {
        let (_disk, shard) = open(Duration::ZERO).await;
        shard.seal().await.unwrap();
        let results = shard
            .commit_all_if(vec![put("a", 1, 1), put("b", 1, 2)], absent_at)
            .await;
        for result in results {
            assert!(matches!(result, Err(ShardError::Sealed(_))), "{result:?}");
        }
        shard.unseal();
        shard.close().await.unwrap();
        let results = shard.commit_all_if(vec![put("a", 1, 1)], absent_at).await;
        assert!(
            matches!(results[..], [Err(ShardError::Unavailable { .. })]),
            "{results:?}"
        );
    });
}

/// A batch shares one group commit only as far as `group_commit_max_bytes`
/// allows: the log keeps its cap, which bounds what a crash can leave
/// unsynced (§10.4), so a larger batch takes one group commit per cap.
#[test]
fn a_batch_takes_one_group_commit_per_group_commit_max_bytes() {
    runtime().block_on(async {
        // Each group waits for more records, so only the cap closes one.
        let cap = 16 << 10;
        let (_disk, shard, log) = open_grouped(cap, Duration::from_millis(20)).await;

        // Just under the cap: 25 records, one group commit.
        let before = log.stats();
        let bodies = (0..25).map(|n| put(&format!("a{n}"), 400, n)).collect();
        let results = shard.commit_all_if(bodies, absent_at).await;
        assert!(results.iter().all(|r| matches!(r, Ok(Ok(_)))));
        let after = log.stats();
        assert_eq!(after.records - before.records, 25);
        let bytes = after.bytes - before.bytes;
        assert!(bytes < cap, "{bytes} bytes");
        assert_eq!(after.group_commits - before.group_commits, 1);

        // Four times that: about one group commit per cap, never one per
        // record.
        let before = log.stats();
        let bodies = (0..100).map(|n| put(&format!("b{n}"), 400, n)).collect();
        let results = shard.commit_all_if(bodies, absent_at).await;
        assert!(results.iter().all(|r| matches!(r, Ok(Ok(_)))));
        let after = log.stats();
        let bytes = after.bytes - before.bytes;
        let groups = after.group_commits - before.group_commits;
        assert!(bytes > 3 * cap, "{bytes} bytes");
        assert!(
            (2..=bytes.div_ceil(cap)).contains(&groups),
            "{groups} group commits for {bytes} bytes"
        );
    });
}
