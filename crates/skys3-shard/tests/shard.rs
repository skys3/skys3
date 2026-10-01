//! The shard runtime: sequencing, the commit pipeline, seals, failures, and
//! the node's set of shards.

mod support;

use std::sync::Arc;

use skys3_index::{EntryState, Index, IndexDump};
use skys3_io::{BlockingPool, SimDisk, SimMount};
use skys3_log::record::ExtentRef;
use skys3_log::{RecordBody, RecordKind, SegmentLog};
use skys3_shard::{
    Committed, Effect, Outcome, Rejection, Shard, ShardError, ShardSet, ShardSummary, StateMachine,
};
use skys3_types::{Epoch, EpochSeq, NodeId, Seq};
use support::{
    at, config, delete, extent, flushed, import, index_config, open_log, pool, put, put_extents,
    runtime, shard, tags,
};

/// A node's disk, log, index, and pool.
struct Node {
    disk: SimDisk,
    log: SegmentLog<SimMount>,
    index: Arc<Index>,
    pool: BlockingPool,
}

impl Node {
    async fn new() -> Self {
        let disk = SimDisk::new(3);
        let log = open_log(disk.mount()).await;
        let index =
            Arc::new(Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap());
        Self {
            disk,
            log,
            index,
            pool: pool(),
        }
    }

    async fn open(&self, epoch: u64) -> Result<Shard<SimMount>, ShardError> {
        Shard::open(
            &config(&shard(0), epoch),
            self.log.clone(),
            Arc::clone(&self.index),
            self.pool.clone(),
        )
        .await
    }

    fn set(&self) -> ShardSet<SimMount> {
        ShardSet::new(Arc::clone(&self.index), self.log.clone(), self.pool.clone())
    }

    fn dump(&self) -> IndexDump {
        self.index.read().unwrap().dump().unwrap()
    }
}

fn applied(committed: &Committed) -> &Outcome {
    assert!(committed.outcome.is_applied(), "{committed:?}");
    &committed.outcome
}

fn invalid(result: Result<Committed, ShardError>) -> String {
    match result {
        Err(ShardError::InvalidRecord { reason, .. }) => reason,
        other => panic!("expected an invalid record, got {other:?}"),
    }
}

#[test]
fn a_new_shard_adopts_its_epoch_and_commits_in_order() {
    runtime().block_on(async {
        let node = Node::new().await;
        let shard0 = node.open(1).await.unwrap();
        // The CONFIG record at (1, 0) is applied first.
        assert_eq!(shard0.applied(), at(0));
        let first = shard0.commit(put("a", 10, 1)).await.unwrap();
        assert_eq!(first.position, at(1));
        assert_eq!(
            applied(&first),
            &Outcome::Applied(Effect::Stored {
                state: EntryState::Dirty
            })
        );
        let second = shard0.commit(import("a", 2)).await.unwrap();
        assert_eq!(second.position, at(2));
        assert_eq!(second.outcome, Outcome::Rejected(Rejection::HasEntry));
        assert_eq!(shard0.applied(), at(2));
        let reader = shard0.index().read().unwrap();
        assert_eq!(reader.applied(&shard(0)).unwrap(), Some(at(2)));
        assert_eq!(
            reader.entry(&shard(0), "a").unwrap().unwrap().version,
            at(1)
        );
        assert_eq!(shard0.shard(), &shard(0));
        assert!(format!("{shard0:?}").contains("next"));
    });
}

#[test]
fn reopening_continues_the_sequence_and_adopts_newer_epochs() {
    runtime().block_on(async {
        let node = Node::new().await;
        let shard0 = node.open(1).await.unwrap();
        shard0.commit(put("a", 1, 1)).await.unwrap();
        drop(shard0);

        let shard0 = node.open(1).await.unwrap();
        assert_eq!(shard0.commit(delete("a")).await.unwrap().position, at(2));
        drop(shard0);

        // A newer epoch: a CONFIG at (3, 2), then (3, 3).
        let shard0 = node.open(3).await.unwrap();
        let epoch3 = |seq| EpochSeq::new(Epoch::new(3), Seq::new(seq));
        assert_eq!(shard0.applied(), epoch3(2));
        assert_eq!(
            shard0.commit(put("b", 1, 3)).await.unwrap().position,
            epoch3(3)
        );
        drop(shard0);

        let error = node.open(2).await.unwrap_err();
        assert!(matches!(error, ShardError::Configuration { .. }), "{error}");
        assert!(
            error
                .to_string()
                .contains("older than the applied position 3.3")
        );
    });
}

#[test]
fn only_a_single_member_configuration_opens() {
    runtime().block_on(async {
        let node = Node::new().await;
        let mut two = config(&shard(0), 1);
        two.members.push("node-2".parse::<NodeId>().unwrap());
        let mut learner = config(&shard(0), 1);
        learner.learners.push("node-2".parse::<NodeId>().unwrap());
        for config in [two, learner] {
            let result = Shard::open(
                &config,
                node.log.clone(),
                Arc::clone(&node.index),
                node.pool.clone(),
            )
            .await;
            assert!(matches!(result, Err(ShardError::Configuration { .. })));
        }
    });
}

#[test]
fn a_put_waits_for_its_extents_to_be_applied() {
    runtime().block_on(async {
        let node = Node::new().await;
        let shard0 = node.open(1).await.unwrap();
        let first = shard0.append_extent(extent("big", 0, 400)).await.unwrap();
        let second = shard0.append_extent(extent("big", 400, 400)).await.unwrap();
        assert_eq!(
            first,
            ExtentRef {
                position: at(1),
                len: 400
            }
        );
        assert_eq!(second.position, at(2));
        let committed = shard0
            .commit(put_extents("big", vec![first, second], 3))
            .await
            .unwrap();
        assert_eq!(committed.position, at(3));
        applied(&committed);

        // An extent that is not applied (here, one that does not exist) is
        // refused, and takes no position.
        let missing = ExtentRef {
            position: at(9),
            len: 400,
        };
        let reason = invalid(shard0.commit(put_extents("big", vec![missing], 4)).await);
        assert!(reason.contains("not applied"), "{reason}");
        assert_eq!(shard0.commit(delete("big")).await.unwrap().position, at(4));
    });
}

#[test]
fn records_the_log_refuses_take_no_position() {
    runtime().block_on(async {
        let node = Node::new().await;
        let shard0 = node.open(1).await.unwrap();
        // More inline payload than inline_max_bytes.
        let reason = invalid(shard0.commit(put("a", 513, 1)).await);
        assert!(reason.contains("inline_max_bytes"), "{reason}");
        // A key longer than the format allows.
        let reason = invalid(shard0.commit(put(&"k".repeat(2000), 1, 1)).await);
        assert!(reason.contains("put.key"), "{reason}");
        // A replica appends CONFIG and TRUNCATE for itself.
        invalid(shard0.commit(RecordBody::Truncate).await);
        invalid(
            shard0
                .commit(RecordBody::Config(config(&shard(0), 1)))
                .await,
        );
        assert_eq!(shard0.commit(put("a", 1, 1)).await.unwrap().position, at(1));
    });
}

/// Runs writers concurrently, some with extents in bulk segments, and
/// checks that the index is what applying their records in position order
/// gives.
async fn concurrent_writes() {
    {
        let node = Node::new().await;
        let shard0 = node.open(1).await.unwrap();
        let mut tasks = Vec::new();
        for n in 0..24u64 {
            let shard0 = shard0.clone();
            tasks.push(tokio::spawn(async move {
                let key = format!("k{}", n % 5);
                let mut committed = Vec::new();
                if n % 3 == 0 {
                    // A large body: bulk-segment extents, then the PUT.
                    let one = shard0.append_extent(extent(&key, 0, 2000)).await.unwrap();
                    let two = shard0
                        .append_extent(extent(&key, 2000, 2000))
                        .await
                        .unwrap();
                    let body = put_extents(&key, vec![one, two], n);
                    committed.push((body.clone(), shard0.commit(body).await.unwrap()));
                } else {
                    let body = put(&key, 100, n);
                    committed.push((body.clone(), shard0.commit(body).await.unwrap()));
                }
                // Once a commit returns, its record is applied.
                assert!(shard0.applied() >= committed[0].1.position);
                committed
            }));
        }
        let mut puts = Vec::new();
        for task in tasks {
            puts.extend(task.await.unwrap());
        }

        // Applying the same records in position order, as replay would,
        // gives the same entries.
        let (_, model) = support::new_index();
        puts.sort_by_key(|(_, committed)| committed.position);
        for (body, committed) in &puts {
            let record = skys3_log::LogRecord {
                shard: shard(0),
                position: committed.position,
                body: body.clone(),
            };
            let outcome = support::apply(&model, &record).unwrap();
            assert_eq!(&outcome, &committed.outcome);
        }
        let live = node.dump();
        let model = model.read().unwrap().dump().unwrap();
        assert_eq!(live.entries, model.entries);
        assert_eq!(live.entries.len(), 5);
    }
}

#[test]
fn concurrent_writes_apply_in_position_order() {
    runtime().block_on(concurrent_writes());
}

#[test]
fn concurrent_writes_on_worker_threads_apply_in_position_order() {
    // Worker threads deliver acknowledgements out of position order.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_time()
        .build()
        .unwrap();
    for _ in 0..16 {
        runtime.block_on(concurrent_writes());
    }
}

#[test]
fn seals_fence_client_writes_and_count_entries() {
    runtime().block_on(async {
        let node = Node::new().await;
        let shard0 = node.open(1).await.unwrap();
        shard0.commit(put("a", 1, 1)).await.unwrap();
        shard0.commit(put("b", 1, 2)).await.unwrap();
        shard0.commit(delete("c")).await.unwrap();
        shard0.commit(flushed("a", 1, false)).await.unwrap();
        shard0.commit(import("d", 5)).await.unwrap();

        let summary = shard0.seal().await.unwrap();
        assert_eq!(
            summary,
            ShardSummary {
                objects: 3,
                unflushed: 2
            }
        );
        assert!(shard0.is_sealed());
        for body in [put("e", 1, 6), delete("a"), tags("a", "x")] {
            let result = shard0.commit(body).await;
            assert_eq!(result, Err(ShardError::Sealed(shard(0))));
        }
        let result = shard0.append_extent(extent("e", 0, 10)).await;
        assert!(matches!(result, Err(ShardError::Sealed(_))));
        // Flushing and importing continue.
        applied(&shard0.commit(flushed("b", 2, false)).await.unwrap());
        applied(&shard0.commit(import("f", 7)).await.unwrap());

        // Seals nest.
        let summary = shard0.seal().await.unwrap();
        assert_eq!(
            summary,
            ShardSummary {
                objects: 4,
                unflushed: 1
            }
        );
        shard0.unseal();
        assert!(shard0.is_sealed());
        shard0.unseal();
        assert!(!shard0.is_sealed());
        shard0.unseal();
        shard0.commit(put("e", 1, 8)).await.unwrap();
    });
}

#[test]
fn a_seal_counts_every_write_sequenced_before_it() {
    runtime().block_on(async {
        let node = Node::new().await;
        let shard0 = node.open(1).await.unwrap();
        let writes: Vec<_> = (0..10u64)
            .map(|n| {
                let shard0 = shard0.clone();
                tokio::spawn(async move { shard0.commit(put(&format!("k{n}"), 1, n)).await })
            })
            .collect();
        // Let every write take its position, but not commit.
        tokio::task::yield_now().await;
        let summary = shard0.seal().await.unwrap();
        assert_eq!(summary.objects, 10);
        for write in writes {
            write.await.unwrap().unwrap();
        }
    });
}

#[test]
fn an_abandoned_seal_is_lifted() {
    runtime().block_on(async {
        let node = Node::new().await;
        let shard0 = node.open(1).await.unwrap();
        tokio::select! {
            biased;
            _ = shard0.seal() => panic!("the seal cannot finish before its barrier"),
            () = std::future::ready(()) => {}
        }
        assert!(!shard0.is_sealed());
        shard0.commit(put("a", 1, 1)).await.unwrap();
    });
}

#[test]
fn a_failed_append_stops_the_shard() {
    runtime().block_on(async {
        let node = Node::new().await;
        let shard0 = node.open(1).await.unwrap();
        shard0.commit(put("a", 1, 1)).await.unwrap();
        node.disk.fail_next_syncs(1);
        let result = shard0.commit(put("b", 1, 2)).await;
        let Err(ShardError::Unavailable { reason, .. }) = result else {
            panic!("expected the shard to stop, got {result:?}");
        };
        assert!(reason.contains("did not become durable"), "{reason}");
        for result in [
            shard0.commit(put("c", 1, 3)).await.map(drop),
            shard0.seal().await.map(drop),
            shard0.close().await,
        ] {
            assert!(
                matches!(result, Err(ShardError::Unavailable { .. })),
                "{result:?}"
            );
        }
        assert!(!shard0.is_sealed());
        assert_eq!(shard0.applied(), at(1));
    });
}

#[test]
fn the_node_opens_seals_and_removes_shards() {
    runtime().block_on(async {
        let node = Node::new().await;
        let set = node.set();
        let opened = set.open(&config(&shard(0), 1)).await.unwrap();
        opened.commit(put("a", 1, 1)).await.unwrap();
        // Opening again returns the same shard, which continues its
        // sequence.
        let again = set.open(&config(&shard(0), 1)).await.unwrap();
        assert_eq!(again.commit(put("b", 1, 2)).await.unwrap().position, at(2));
        set.open(&config(&shard(1), 1)).await.unwrap();
        assert_eq!(set.shards().await, [shard(0), shard(1)]);
        assert!(format!("{set:?}").contains("ShardSet"));

        let summary = set.seal(&shard(0)).await.unwrap();
        assert_eq!(
            summary,
            ShardSummary {
                objects: 2,
                unflushed: 2
            }
        );
        assert_eq!(
            opened.commit(put("c", 1, 3)).await,
            Err(ShardError::Sealed(shard(0)))
        );
        set.unseal(&shard(0)).await.unwrap();
        assert_eq!(
            set.seal(&shard(7)).await,
            Err(ShardError::NotFound(shard(7)))
        );
        assert_eq!(
            set.unseal(&shard(7)).await,
            Err(ShardError::NotFound(shard(7)))
        );

        set.remove(&shard(0)).await.unwrap();
        assert!(set.get(&shard(0)).await.is_none());
        let dump = node.dump();
        assert!(dump.entries.keys().all(|(s, _)| *s != shard(0)));
        assert!(dump.locations.keys().all(|(s, _)| *s != shard(0)));
        assert!(!dump.applied.contains_key(&shard(0)));
        assert!(dump.applied.contains_key(&shard(1)), "other shards stay");
        let result = opened.commit(put("d", 1, 4)).await;
        assert!(
            matches!(result, Err(ShardError::Unavailable { .. })),
            "{result:?}"
        );
        // Removing a shard that is not open succeeds.
        set.remove(&shard(0)).await.unwrap();

        // A removed shard opens afresh.
        let reopened = set.open(&config(&shard(0), 1)).await.unwrap();
        assert_eq!(reopened.applied(), at(0));
    });
}

#[test]
fn errors_name_the_shard() {
    let error = ShardError::Unavailable {
        shard: shard(2),
        reason: "disk out of service".into(),
    };
    assert_eq!(
        error.to_string(),
        "shard b-test/2 is unavailable: disk out of service"
    );
    assert_eq!(
        ShardError::Sealed(shard(2)).to_string(),
        "shard b-test/2 is sealed while its bucket is deleted"
    );
    assert_eq!(
        ShardError::NotFound(shard(2)).to_string(),
        "shard b-test/2 is not open"
    );
    let _ = (StateMachine, RecordKind::Put);
}
