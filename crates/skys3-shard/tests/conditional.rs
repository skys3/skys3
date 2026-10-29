//! Conditional writes and reads of a shard: `commit_if` checks a key's
//! entry as of the record's position, also while earlier writes of the key
//! are sequenced but not yet applied (§5.1).

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
