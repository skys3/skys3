//! Replicated shards changing configuration without the network (§5.1,
//! §6.4): a primary removing a member commits what the member held back,
//! members switch epochs at the primary's `seq`, and a shard left with
//! fewer members than `min_write_replicas` refuses writes but still reads.

mod support;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use skys3_index::Index;
use skys3_io::{BlockingPool, Clock, MonotonicClock, SimDisk, SimMount};
use skys3_log::{LogRecord, RecordBody, SegmentLog};
use skys3_shard::{Pending, Role, Shard, ShardError};
use skys3_types::{Epoch, EpochSeq, NodeId, ProposalId, Seq, ShardConfig};
use support::{config, delete, flushed, index_config, open_log, pool, put, runtime, shard};

fn node(n: u8) -> NodeId {
    format!("node-{n}").parse().unwrap()
}

/// Shard 0 on `members`, node 1 its primary, in `epoch`; three replicas,
/// and writes need two copies.
fn configuration(epoch: u64, members: &[u8]) -> ShardConfig {
    ShardConfig {
        members: members.iter().map(|n| node(*n)).collect(),
        replicas: 3,
        min_write_replicas: 2,
        proposal_id: ProposalId::new(format!("p-{epoch}")).unwrap(),
        ..config(&shard(0), epoch)
    }
}

fn position(epoch: u64, seq: u64) -> EpochSeq {
    EpochSeq::new(Epoch::new(epoch), Seq::new(seq))
}

/// The record at `position`, and its encoding.
fn encoded(at: EpochSeq, body: RecordBody) -> (LogRecord, Bytes) {
    let record = support::record(at, body);
    let bytes = record.to_bytes().unwrap();
    (record, bytes)
}

struct Replica {
    log: SegmentLog<SimMount>,
    index: Arc<Index>,
    pool: BlockingPool,
}

impl Replica {
    async fn new(seed: u64) -> Self {
        let disk = SimDisk::new(seed);
        Self {
            log: open_log(disk.mount()).await,
            index: Arc::new(Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap()),
            pool: pool(),
        }
    }

    async fn open(&self, config: &ShardConfig, node: &NodeId) -> Shard<SimMount> {
        Shard::open_replica(
            config,
            node,
            self.log.clone(),
            Arc::clone(&self.index),
            self.pool.clone(),
        )
        .await
        .unwrap()
    }
}

/// Waits until `done` holds, for at most ten seconds.
async fn until(mut done: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the condition did not hold in time");
}

fn configuration_error(result: Result<impl std::fmt::Debug, ShardError>) -> String {
    match result {
        Err(ShardError::Configuration { reason, .. }) => reason,
        other => panic!("expected a configuration error, got {other:?}"),
    }
}

#[test]
fn a_primary_removing_a_member_commits_what_it_held_back() {
    runtime().block_on(async {
        let replica = Replica::new(3).await;
        let primary = replica.open(&configuration(1, &[1, 2, 3]), &node(1)).await;
        let leader = Arc::clone(primary.leader().unwrap());
        leader.synced(&node(2), Seq::ZERO);
        leader.synced(&node(3), Seq::ZERO);
        until(|| primary.is_serving()).await;
        let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::new());
        leader.start_leases(clock, Duration::from_secs(60));

        // Node 2 holds the write, node 3 does not: it waits.
        let writer = {
            let primary = primary.clone();
            tokio::spawn(async move { primary.commit(put("k", 10, 1)).await })
        };
        until(|| primary.last_sequenced() == Seq::new(1)).await;
        leader.acknowledged(&node(2), Seq::new(1));
        primary.settle().await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!writer.is_finished());

        // Removing node 3: its CONFIG record follows the write, and once it
        // is durable the write commits on the remaining members.
        let removed = configuration(2, &[1, 2]);
        let reconfigured = {
            let (primary, removed) = (primary.clone(), removed.clone());
            tokio::spawn(async move { primary.reconfigure(&removed).await })
        };
        let committed = writer.await.unwrap().unwrap();
        assert_eq!(committed.position, position(1, 1));
        assert_eq!(leader.members(), [node(2)]);
        assert!(!leader.is_member(&node(3)));
        assert_eq!(leader.acked(&node(3)), None);
        // The CONFIG record shares seq 1, which node 2 holds already.
        reconfigured.await.unwrap().unwrap();
        assert_eq!(primary.config(), removed);
        assert_eq!(primary.sequencing(), Epoch::new(2));
        assert_eq!(primary.applied(), position(2, 1));

        // A removed member's grants count for nothing; node 2's suffice.
        let stamp = leader.lease_stamp().unwrap();
        leader.granted(&node(3), stamp);
        assert_eq!(leader.lease(&node(3)), None);
        leader.granted(&node(2), stamp);
        assert!(leader.holds_leases());
        assert_eq!(
            primary.entry("k").await.unwrap().unwrap().version,
            position(1, 1)
        );

        // Later records are of the new epoch, and so is what links send.
        let writer = {
            let primary = primary.clone();
            tokio::spawn(async move { primary.commit(delete("k")).await })
        };
        until(|| primary.last_sequenced() == Seq::new(2)).await;
        let Pending::Ready(records) = leader.records_after(Seq::new(1), 8) else {
            panic!("the record is in memory");
        };
        assert_eq!(
            (records[0].seq, records[0].epoch),
            (Seq::new(2), Epoch::new(2))
        );
        leader.acknowledged(&node(2), Seq::new(2));
        assert_eq!(writer.await.unwrap().unwrap().position, position(2, 2));
        // The log gives back the records with their own epochs.
        let tail = primary.read_tail(Seq::ZERO, Seq::new(2)).await.unwrap();
        let positions: Vec<EpochSeq> = tail.iter().map(|(at, _)| *at).collect();
        assert_eq!(positions, [position(1, 1), position(2, 2)]);

        // Left alone, below its min_write_replicas: reads, but no writes.
        let alone = configuration(3, &[1]);
        primary.reconfigure(&alone).await.unwrap();
        assert!(leader.members().is_empty());
        match primary.commit(put("k", 10, 2)).await {
            Err(ShardError::UnderReplicated {
                copies,
                min_write_replicas,
                ..
            }) => assert_eq!((copies, min_write_replicas), (1, 2)),
            other => panic!("expected the write to be refused, got {other:?}"),
        }
        let refused = primary
            .commit_if(put("k", 10, 2), |_| Ok::<(), ()>(()))
            .await;
        assert!(matches!(refused, Err(ShardError::UnderReplicated { .. })));
        primary.check_readable().unwrap();
        assert!(primary.entry("k").await.unwrap().is_some());
        // Flushing is not a client write.
        let flush = primary.commit(flushed("k", 2, true)).await.unwrap();
        assert_eq!(flush.position, position(3, 3));
        assert_eq!(primary.role(), Role::Primary);
    });
}

/// Takes the record of `body` at `at` on `member`, in a session of `epoch`.
fn take(member: &Shard<SimMount>, epoch: u64, at: EpochSeq, body: RecordBody) -> bool {
    let (record, bytes) = encoded(at, body);
    member
        .receive(None, Epoch::new(epoch), record, bytes, false)
        .unwrap()
}

#[test]
fn a_member_switches_epochs_where_its_primary_did() {
    runtime().block_on(async {
        let replica = Replica::new(5).await;
        let member = replica.open(&configuration(1, &[1, 2, 3]), &node(2)).await;
        assert!(take(&member, 1, position(1, 1), delete("a")));

        // Changes it does not support.
        let takeover = ShardConfig {
            primary: node(2),
            ..configuration(2, &[1, 2])
        };
        let reason = configuration_error(member.reconfigure(&takeover).await);
        assert!(reason.contains("takes over"), "{reason}");
        let joined = configuration(2, &[1, 2, 3, 4]);
        let reason = configuration_error(member.reconfigure(&joined).await);
        assert!(reason.contains("learner"), "{reason}");
        let without = configuration(2, &[1, 3]);
        let reason = configuration_error(member.reconfigure(&without).await);
        assert!(reason.contains("not a member"), "{reason}");
        let learners = ShardConfig {
            learners: vec![node(4)],
            ..configuration(2, &[1, 2])
        };
        let reason = configuration_error(member.reconfigure(&learners).await);
        assert!(reason.contains("learners"), "{reason}");

        // A newer configuration is recorded; the member still takes the
        // earlier epoch's tail, in sessions of the new one only (R2).
        let removed = configuration(2, &[1, 2]);
        member.reconfigure(&removed).await.unwrap();
        member.reconfigure(&removed).await.unwrap();
        assert_eq!(member.config(), removed);
        assert_eq!(member.sequencing(), Epoch::new(1));
        let (record, bytes) = encoded(position(1, 2), delete("b"));
        let older = member.receive(None, Epoch::new(1), record, bytes, false);
        assert!(configuration_error(older).contains("epoch 1"));
        assert!(take(&member, 2, position(1, 2), delete("b")));
        // The primary appended its CONFIG record after seq 3: the member
        // does once it holds seq 3.
        member.follow(Epoch::new(2), Seq::new(3)).unwrap();
        assert_eq!(member.sequencing(), Epoch::new(1));
        assert!(take(&member, 2, position(1, 3), delete("c")));
        assert_eq!(member.sequencing(), Epoch::new(2));
        let (record, bytes) = encoded(position(1, 4), delete("d"));
        let stale = member.receive(None, Epoch::new(2), record, bytes, false);
        assert!(
            matches!(&stale, Err(ShardError::InvalidRecord { reason, .. })
                if reason.contains("not of the replica's epoch 2")),
            "{stale:?}"
        );
        assert!(take(&member, 2, position(2, 4), delete("d")));
        member.settle().await;
        member.commit_through(Seq::new(4));
        until(|| member.applied() == position(2, 4)).await;
        // Following changes nothing once aligned.
        member.follow(Epoch::new(2), Seq::new(9)).unwrap();
        assert_eq!(member.sequencing(), Epoch::new(2));

        // A member restarted with a newer configuration than its log
        // opens in its log's epoch; the first record of the new epoch
        // switches it, at the seq before it.
        member.close().await.unwrap();
        assert!(member.follow(Epoch::new(3), Seq::new(4)).is_err());
        let newer = configuration(3, &[1, 2]);
        let member = replica.open(&newer, &node(2)).await;
        assert_eq!(member.applied(), position(2, 4));
        assert_eq!(member.sequencing(), Epoch::new(2));
        assert_eq!(member.config(), newer);
        // Its primary has not appended the CONFIG record yet.
        member.follow(Epoch::new(2), Seq::new(4)).unwrap();
        assert_eq!(member.sequencing(), Epoch::new(2));
        assert!(take(&member, 3, position(3, 5), delete("e")));
        assert_eq!(member.sequencing(), Epoch::new(3));
        member.settle().await;
        member.commit_through(Seq::new(5));
        until(|| member.applied() == position(3, 5)).await;
    });
}

#[test]
fn a_member_that_diverged_never_switches() {
    runtime().block_on(async {
        let replica = Replica::new(6).await;
        let member = replica.open(&configuration(1, &[1, 2, 3]), &node(2)).await;
        assert!(take(&member, 1, position(1, 1), delete("a")));
        assert!(take(&member, 1, position(1, 2), delete("b")));
        member
            .reconfigure(&configuration(2, &[1, 2]))
            .await
            .unwrap();
        // Its primary's CONFIG record is at seq 1, but it holds seq 2 of
        // the earlier epoch: it never switches, and refuses the new epoch.
        member.follow(Epoch::new(2), Seq::new(1)).unwrap();
        assert_eq!(member.sequencing(), Epoch::new(1));
        let (record, bytes) = encoded(position(2, 3), delete("c"));
        assert!(
            member
                .receive(None, Epoch::new(2), record, bytes, false)
                .is_err()
        );
    });
}

/// A primary restarted with a newer configuration than its log, after its
/// own removal of a member landed unseen for example, opens in its log's
/// epoch, rolls forward what its members hold of it, and appends its
/// `CONFIG` record once every member of the new configuration reported.
#[test]
fn a_primary_opened_in_a_newer_epoch_aligns_once_its_members_reported() {
    runtime().block_on(async {
        let replica = Replica::new(8).await;
        // An earlier life of node 1 held two records of epoch 1.
        let earlier = ShardConfig {
            primary: node(2),
            ..configuration(1, &[1, 2, 3])
        };
        let life = replica.open(&earlier, &node(1)).await;
        assert!(take(&life, 1, position(1, 1), delete("a")));
        assert!(take(&life, 1, position(1, 2), delete("b")));
        life.settle().await;
        life.commit_through(Seq::new(2));
        until(|| life.applied() == position(1, 2)).await;
        life.close().await.unwrap();

        let removed = configuration(2, &[1, 2]);
        let primary = replica.open(&removed, &node(1)).await;
        let leader = Arc::clone(primary.leader().unwrap());
        assert_eq!(leader.members(), [node(2)]);
        assert_eq!(primary.sequencing(), Epoch::new(1));
        assert!(!primary.align());
        // Node 2 holds a record of epoch 1 the primary lost.
        let (record, bytes) = encoded(position(1, 3), delete("c"));
        assert!(
            primary
                .receive(None, Epoch::new(2), record, bytes, false)
                .unwrap()
        );
        leader.synced(&node(2), Seq::new(3));
        assert!(!primary.is_serving());
        assert!(primary.align());
        assert!(!primary.align());
        assert_eq!(primary.sequencing(), Epoch::new(2));
        until(|| primary.is_serving()).await;
        assert_eq!(primary.applied(), position(2, 3));
        let writer = {
            let primary = primary.clone();
            tokio::spawn(async move { primary.commit(delete("d")).await })
        };
        until(|| primary.last_sequenced() == Seq::new(4)).await;
        leader.acknowledged(&node(2), Seq::new(4));
        assert_eq!(writer.await.unwrap().unwrap().position, position(2, 4));
    });
}

/// A primary whose member has not reported yet keeps the new
/// configuration's `CONFIG` record until it has: the member may hold
/// records of the earlier epoch the primary does not.
#[test]
fn a_primary_appends_a_new_configuration_once_its_members_reported() {
    runtime().block_on(async {
        let replica = Replica::new(9).await;
        let primary = replica.open(&configuration(1, &[1, 2, 3]), &node(1)).await;
        let leader = Arc::clone(primary.leader().unwrap());
        // Node 3 is removed before node 2 reported: the record waits.
        let removed = configuration(2, &[1, 2]);
        primary.reconfigure(&removed).await.unwrap();
        assert_eq!(primary.config(), removed);
        assert_eq!(primary.sequencing(), Epoch::new(1));
        assert_eq!(leader.members(), [node(2), node(3)]);
        leader.synced(&node(2), Seq::ZERO);
        assert!(primary.align());
        until(|| leader.members() == [node(2)]).await;
        until(|| primary.is_serving() && primary.applied() == position(2, 0)).await;
    });
}

#[test]
fn a_shard_alone_reconfigures_as_before() {
    runtime().block_on(async {
        let replica = Replica::new(7).await;
        let alone = replica.open(&config(&shard(0), 1), &node(1)).await;
        alone.commit(delete("a")).await.unwrap();
        let shared = ShardConfig {
            members: vec![node(1), node(2)],
            ..config(&shard(0), 2)
        };
        let reason = configuration_error(alone.reconfigure(&shared).await);
        assert!(reason.contains("learners"), "{reason}");
        alone.reconfigure(&config(&shard(0), 2)).await.unwrap();
        assert_eq!(alone.applied(), position(2, 1));
        assert_eq!(alone.sequencing(), Epoch::new(2));
        // What it stores: the bytes of its object versions.
        alone.commit(put("b", 10, 1)).await.unwrap();
        alone.commit(put("c", 32, 1)).await.unwrap();
        assert_eq!(alone.stored_bytes().await.unwrap(), 42);
    });
}
