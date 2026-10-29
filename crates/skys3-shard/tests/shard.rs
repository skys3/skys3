//! The shard runtime: sequencing, the commit pipeline, seals, failures, and
//! the node's set of shards.

mod support;

use std::sync::Arc;

use skys3_index::{EntryState, Index, IndexDump};
use skys3_io::{BlockingPool, SimDisk, SimMount};
use skys3_log::record::ExtentRef;
use skys3_log::{RecordBody, RecordKind, SegmentLog};
use skys3_shard::{
    Committed, Effect, Outcome, Shard, ShardError, ShardSet, ShardSummary, StateMachine,
};
use skys3_types::{Epoch, EpochSeq, NodeId, Seq};
use support::{
    at, config, delete, extent, flushed, import, index_config, mpu_complete, mpu_create, mpu_part,
    open_log, pool, put, put_extents, runtime, shard, tags,
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
        assert_eq!(second.outcome, Outcome::Applied(Effect::RemoteRecorded));
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
fn a_shard_being_removed_opens_again_only_once_removed() {
    runtime().block_on(async {
        let node = Node::new().await;
        let set = Arc::new(node.set());
        let opened = set.open(&config(&shard(0), 1)).await.unwrap();
        opened.commit(put("a", 1, 1)).await.unwrap();

        // Hold the index's only thread: the next write cannot be applied,
        // so the removal cannot close the shard, and nothing can read or
        // change the index.
        let (release, gate) = std::sync::mpsc::channel::<()>();
        let held = node.pool.run(move || gate.recv().unwrap());
        let write = tokio::spawn({
            let opened = opened.clone();
            async move { opened.commit(put("b", 1, 2)).await }
        });
        let removal = tokio::spawn({
            let set = Arc::clone(&set);
            async move { set.remove(&shard(0)).await }
        });
        while set.get(&shard(0)).await.is_some() {
            tokio::task::yield_now().await;
        }
        assert!(set.shards().await.is_empty());
        let reopen = tokio::spawn({
            let set = Arc::clone(&set);
            async move { set.open(&config(&shard(0), 1)).await }
        });
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert!(!removal.is_finished());
        assert!(!reopen.is_finished(), "the shard reopened mid-removal");

        release.send(()).unwrap();
        held.await.unwrap();
        // Acknowledged before the removal finished, and removed with it.
        assert_eq!(write.await.unwrap().unwrap().position, at(2));
        removal.await.unwrap().unwrap();
        let reopened = reopen.await.unwrap().unwrap();
        // The reopened shard starts afresh, and the index agrees.
        assert_eq!(reopened.applied(), at(0));
        let dump = node.dump();
        assert_eq!(dump.applied.get(&shard(0)), Some(&at(0)));
        assert!(dump.entries.keys().all(|(s, _)| *s != shard(0)));
        let committed = reopened.commit(put("c", 1, 3)).await.unwrap();
        assert_eq!(committed.position, at(1));
        assert_eq!(node.dump().applied.get(&shard(0)), Some(&at(1)));
    });
}

#[test]
fn an_abandoned_removal_still_finishes() {
    runtime().block_on(async {
        let node = Node::new().await;
        let set = node.set();
        let opened = set.open(&config(&shard(0), 1)).await.unwrap();
        opened.commit(put("a", 1, 1)).await.unwrap();
        let shard0 = shard(0);
        tokio::select! {
            biased;
            _ = set.remove(&shard0) => panic!("the removal cannot finish at once"),
            () = std::future::ready(()) => {}
        }
        let reopened = set.open(&config(&shard(0), 1)).await.unwrap();
        assert_eq!(reopened.applied(), at(0));
        assert!(node.dump().entries.keys().all(|(s, _)| *s != shard(0)));
    });
}

#[test]
fn opening_an_open_shard_compares_epochs() {
    runtime().block_on(async {
        let node = Node::new().await;
        let set = node.set();
        let epoch = |epoch, seq| EpochSeq::new(Epoch::new(epoch), Seq::new(seq));
        let opened = set.open(&config(&shard(0), 1)).await.unwrap();
        opened.commit(put("a", 1, 1)).await.unwrap();

        // A newer epoch: the open shard adopts it after the write sequenced
        // before, with a CONFIG at (2, 2), and continues at (2, 3).
        let epoch2 = config(&shard(0), 2);
        let (write, again) = tokio::join!(opened.commit(put("b", 1, 2)), set.open(&epoch2));
        assert_eq!(write.unwrap().position, at(2));
        let again = again.unwrap();
        assert_eq!(again.applied(), epoch(2, 2));
        assert_eq!(again.config(), config(&shard(0), 2));
        assert_eq!(
            opened.commit(put("c", 1, 3)).await.unwrap().position,
            epoch(2, 3)
        );

        // The same configuration changes nothing.
        set.open(&config(&shard(0), 2)).await.unwrap();
        assert_eq!(opened.applied(), epoch(2, 3));

        // An older epoch, or another configuration in the same one, is
        // refused.
        let mut other = config(&shard(0), 2);
        other.replicas = 3;
        for (config, reason) in [
            (
                config(&shard(0), 1),
                "epoch 1 is older than the shard's epoch 2",
            ),
            (other, "differs from the shard's in epoch 2"),
        ] {
            let error = set.open(&config).await.unwrap_err();
            assert!(matches!(error, ShardError::Configuration { .. }), "{error}");
            assert!(error.to_string().contains(reason), "{error}");
        }

        // A sealed shard adopts a newer epoch too, and two opens in it
        // append one CONFIG.
        set.seal(&shard(0)).await.unwrap();
        let epoch4 = config(&shard(0), 4);
        let (first, second) = tokio::join!(set.open(&epoch4), set.open(&epoch4));
        first.unwrap();
        second.unwrap();
        assert_eq!(opened.applied(), epoch(4, 3));
        assert!(opened.is_sealed());
        set.unseal(&shard(0)).await.unwrap();
        assert_eq!(
            opened.commit(put("d", 1, 4)).await.unwrap().position,
            epoch(4, 4)
        );

        // Replay finds the same: reopening after a restart continues in
        // epoch 4.
        opened.close().await.unwrap();
        let reopened = node.open(4).await.unwrap();
        assert_eq!(reopened.applied(), epoch(4, 4));
    });
}

#[test]
fn reconfiguring_checks_the_configuration() {
    runtime().block_on(async {
        let node = Node::new().await;
        let shard0 = node.open(1).await.unwrap();
        let mut two = config(&shard(0), 2);
        two.members.push("node-2".parse::<NodeId>().unwrap());
        let error = shard0.reconfigure(&two).await.unwrap_err();
        assert!(matches!(error, ShardError::Configuration { .. }), "{error}");
        let error = shard0.reconfigure(&config(&shard(1), 2)).await.unwrap_err();
        assert!(
            error.to_string().contains("is of shard b-test/1"),
            "{error}"
        );
        assert_eq!(shard0.applied(), at(0));

        // A stopped shard keeps its configuration, and adopts no newer one.
        shard0.close().await.unwrap();
        shard0.reconfigure(&config(&shard(0), 1)).await.unwrap();
        let error = shard0.reconfigure(&config(&shard(0), 2)).await.unwrap_err();
        assert!(matches!(error, ShardError::Unavailable { .. }), "{error}");
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

#[test]
fn shards_stay_on_the_disk_their_hash_picks() {
    runtime().block_on(async {
        let node = Node::new().await;
        let other = SimDisk::new(4);
        let other_log = open_log(other.mount()).await;
        let label = |name: &str| skys3_types::Label::new(name).unwrap();
        let logs = std::collections::BTreeMap::from([
            (label("disk-a"), node.log.clone()),
            (label("disk-b"), other_log.clone()),
        ]);
        let set = ShardSet::with_disks(Arc::clone(&node.index), logs, node.pool.clone());
        let mut used = std::collections::BTreeSet::new();
        for n in 0..16 {
            let disk = set.disk_of(&shard(n)).clone();
            assert_eq!(set.disk_of(&shard(n)), &disk, "the choice is stable");
            let opened = set.open(&config(&shard(n), 1)).await.unwrap();
            opened.commit(put("k", 1, u64::from(n))).await.unwrap();
            let log = if disk == label("disk-a") {
                &node.log
            } else {
                &other_log
            };
            assert!(
                log.summaries()
                    .values()
                    .any(|summary| summary.positions.contains_key(&shard(n))),
                "shard {n} wrote to {disk}"
            );
            used.insert(disk);
        }
        assert_eq!(used.len(), 2, "both disks hold shards");
        set.close_all().await.unwrap();
        let closed = set.get(&shard(0)).await.unwrap();
        assert!(matches!(
            closed.commit(put("k", 1, 99)).await,
            Err(ShardError::Unavailable { .. })
        ));
        assert_eq!(closed.summary().await.unwrap().objects, 1);
        set.close_all().await.unwrap_err();
    });
}

#[test]
fn subscribers_see_applied_client_writes() {
    runtime().block_on(async {
        let node = Node::new().await;
        let shard = node.open(1).await.unwrap();
        let mut changes = shard.subscribe();
        let first = shard.commit(put("a", 3, 1)).await.unwrap();
        let tagged = shard.commit(tags("a", "x")).await.unwrap();
        let deleted = shard.commit(delete("b")).await.unwrap();
        // An upload and its parts change no version; its completion does.
        let upload = shard.commit(mpu_create("m")).await.unwrap().position;
        let part = shard.commit(mpu_part("m", upload, 1, 4, vec![])).await;
        let parts = [(1, part.unwrap().position)];
        let completed = shard.commit(mpu_complete("m", upload, &parts, 4)).await;
        let completed = completed.unwrap();
        assert!(completed.outcome.is_applied());
        // A rejected TAGS and a FLUSHED change no version and are not
        // reported. The lazy FLUSHED rides the next commit's group.
        shard.commit(tags("missing", "x")).await.unwrap();
        let lazy = shard.commit_lazy(flushed("a", tagged.position.seq.get(), false));
        let (lazy, next) = tokio::join!(lazy, shard.commit(put("c", 1, 2)));
        assert!(lazy.unwrap().outcome.is_applied());
        let next = next.unwrap();
        let expected = [
            ("a", first.position, Some(3)),
            ("a", tagged.position, None),
            ("b", deleted.position, Some(0)),
            ("m", completed.position, Some(4)),
            ("c", next.position, Some(1)),
        ];
        for (key, position, size) in expected {
            let change = changes.recv().await.unwrap();
            assert_eq!(
                (change.key.as_str(), change.position, change.size),
                (key, position, size)
            );
        }
        let entries = shard.entries(None, 10).await.unwrap();
        let keys: Vec<_> = entries.iter().map(|(key, _)| key.as_str()).collect();
        assert_eq!(keys, ["a", "b", "c", "m"]);
        assert_eq!(shard.entries(Some("a".into()), 1).await.unwrap()[0].0, "b");

        // A new subscription ends the old one, and closing ends both.
        let mut newer = shard.subscribe();
        assert!(changes.recv().await.is_none());
        assert!(!shard.is_stopped());
        shard.close().await.unwrap();
        assert!(shard.is_stopped());
        assert!(newer.recv().await.is_none());
        assert!(shard.subscribe().recv().await.is_none());
    });
}

#[test]
fn a_pending_flushed_does_not_hold_up_conditional_writes() {
    runtime().block_on(async {
        let node = Node::new().await;
        let shard = node.open(1).await.unwrap();
        let first = shard.commit(put("a", 3, 1)).await.unwrap();
        // Nothing else is written, so the FLUSHED waits up to a second.
        let lazy = tokio::spawn({
            let shard = shard.clone();
            let body = flushed("a", first.position.seq.get(), false);
            async move { shard.commit_lazy(body).await }
        });
        tokio::task::yield_now().await;
        let started = std::time::Instant::now();
        let written = shard
            .commit_if(put("a", 4, 2), |entry| match entry {
                Some(_) => Ok(()),
                None => Err("missing"),
            })
            .await
            .unwrap()
            .unwrap();
        assert!(started.elapsed() < skys3_log::LAZY_MAX_DELAY);
        assert!(written.outcome.is_applied());
        // The FLUSHED came first, and rode the conditional write's group.
        assert_eq!(
            lazy.await.unwrap().unwrap().outcome,
            Outcome::Applied(Effect::Cleaned)
        );
    });
}

#[test]
fn a_subscription_during_an_apply_hears_of_it() {
    runtime().block_on(async {
        let node = Node::new().await;
        // Two pool threads: one for the held apply, one for the scan.
        let pool = BlockingPool::new("index", std::num::NonZeroUsize::new(2).unwrap()).unwrap();
        let shard = Shard::open(
            &config(&shard(0), 1),
            node.log.clone(),
            Arc::clone(&node.index),
            pool,
        )
        .await
        .unwrap();

        // Hold the index's write lock, so the next apply waits for it.
        let (held, holding) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel::<()>();
        let index = Arc::clone(&node.index);
        let holder = std::thread::spawn(move || {
            index
                .update_control(|_| {
                    held.send(()).unwrap();
                    released.recv().unwrap();
                    Ok(())
                })
                .unwrap();
        });
        holding.recv().unwrap();
        let writer = shard.clone();
        let commit = tokio::spawn(async move { writer.commit(put("a", 3, 1)).await });
        // The record becomes durable, and its apply waits for the lock.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!commit.is_finished());

        // A subscriber that scans now finds nothing: the write is not in
        // the index yet, so it must be reported.
        let mut changes = shard.subscribe();
        assert!(shard.entries(None, 10).await.unwrap().is_empty());
        release.send(()).unwrap();
        holder.join().unwrap();
        let committed = commit.await.unwrap().unwrap();
        assert!(committed.outcome.is_applied());
        let change = changes.try_recv().expect("the write is reported");
        assert_eq!(
            (change.key.as_str(), change.position, change.size),
            ("a", committed.position, Some(3))
        );
    });
}
