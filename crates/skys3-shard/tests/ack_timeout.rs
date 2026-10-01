//! Acknowledgement timeouts on replicated shards (§5.2): a write the
//! members do not acknowledge in time fails without being undone, a
//! fail-fast shard refuses new writes while one is late, and seals and
//! closing do not wait forever for a member.

mod support;

use std::sync::Arc;
use std::time::Duration;

use skys3_index::Index;
use skys3_io::{BlockingPool, SimDisk, SimMount};
use skys3_log::SegmentLog;
use skys3_shard::{AckMode, AckTimeout, Committed, Leader, Shard, ShardError};
use skys3_types::{Epoch, NodeId, Seq, ShardConfig};
use support::{
    at, config, delete, entry, index_config, open_log, pool, put, record, runtime, shard,
};

/// The timeout of these tests: short, so they run quickly, and long next
/// to a sync of the simulated disk, so a write the members acknowledge at
/// once commits within it.
const TIMEOUT: Duration = Duration::from_millis(200);

/// Every other wait in these tests.
const WAIT: Duration = Duration::from_secs(10);

fn node(n: u8) -> NodeId {
    format!("node-{n}").parse().unwrap()
}

/// Shard 0 in epoch 1 on nodes 1 to 3, with node 1 the primary.
fn replicated() -> ShardConfig {
    ShardConfig {
        members: vec![node(1), node(2), node(3)],
        replicas: 3,
        ..config(&shard(0), 1)
    }
}

struct Replica {
    log: SegmentLog<SimMount>,
    index: Arc<Index>,
    pool: BlockingPool,
}

impl Replica {
    async fn new() -> Self {
        let disk = SimDisk::new(11);
        Self {
            log: open_log(disk.mount()).await,
            index: Arc::new(Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap()),
            pool: pool(),
        }
    }

    async fn open(&self, node: &NodeId, ack: AckTimeout) -> Shard<SimMount> {
        let shard = Shard::open_replica(
            &replicated(),
            node,
            self.log.clone(),
            Arc::clone(&self.index),
            self.pool.clone(),
        )
        .await
        .unwrap();
        shard.set_ack_timeout(ack);
        shard
    }

    /// The primary, serving once both members reported an empty log.
    async fn primary(&self, ack: AckTimeout) -> (Shard<SimMount>, Arc<Leader>) {
        let primary = self.open(&node(1), ack).await;
        let leader = Arc::clone(primary.leader().unwrap());
        leader.synced(&node(2), Seq::ZERO);
        leader.synced(&node(3), Seq::ZERO);
        until(|| primary.is_serving()).await;
        (primary, leader)
    }
}

/// Waits until `done` holds.
async fn until(mut done: impl FnMut() -> bool) {
    tokio::time::timeout(WAIT, async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the condition did not hold in time");
}

/// Both members acknowledge every record through `seq`.
fn acknowledge(leader: &Leader, seq: u64) {
    leader.acknowledged(&node(2), Seq::new(seq));
    leader.acknowledged(&node(3), Seq::new(seq));
}

/// Why `result` was not acknowledged.
fn not_acknowledged<T: std::fmt::Debug>(result: Result<T, ShardError>) -> String {
    match result {
        Err(ShardError::NotAcknowledged { reason, .. }) => reason,
        other => panic!("expected the write not to be acknowledged, got {other:?}"),
    }
}

/// Commits `body` on a task of its own, so a test can acknowledge it.
fn spawn_commit(
    primary: &Shard<SimMount>,
    body: skys3_log::RecordBody,
) -> tokio::task::JoinHandle<Result<Committed, ShardError>> {
    let primary = primary.clone();
    tokio::spawn(async move { primary.commit(body).await })
}

/// A failed write keeps its position: it commits once the members catch
/// up, in order with the writes after it, so a later `DELETE` of the key
/// wins.
#[test]
fn a_write_that_times_out_still_commits_in_order() {
    runtime().block_on(async {
        let replica = Replica::new().await;
        let ack = AckTimeout::wait_through(TIMEOUT);
        let (primary, leader) = replica.primary(ack).await;
        assert_eq!(primary.ack_timeout(), Some(ack));

        // The error names the position the write took.
        match primary.commit(put("k", 10, 1)).await {
            Err(ShardError::NotAcknowledged {
                position, reason, ..
            }) => {
                assert_eq!(position, Some(at(1)));
                assert!(reason.contains("200ms"), "{reason}");
            }
            other => panic!("expected the write not to be acknowledged, got {other:?}"),
        }
        // Waiting through takes later writes all the same, and each waits
        // its own timeout.
        not_acknowledged(primary.commit(delete("k")).await);
        assert_eq!(primary.last_sequenced(), Seq::new(2));
        assert_eq!(primary.applied(), at(0));

        // The members catch up: both records commit, in order.
        acknowledge(&leader, 2);
        until(|| primary.applied() == at(2)).await;
        let deleted = entry(&replica.index, "k").unwrap();
        assert!(deleted.object.is_none());
        let writer = spawn_commit(&primary, put("k", 10, 3));
        until(|| primary.last_sequenced() == Seq::new(3)).await;
        acknowledge(&leader, 3);
        assert_eq!(writer.await.unwrap().unwrap().position, at(3));
        // A shard whose members keep up closes as before.
        tokio::time::timeout(WAIT, primary.close())
            .await
            .unwrap()
            .unwrap();
    });
}

/// Fail-fast: once a write timed out, new writes are refused at once,
/// without a position, until the late record is applied.
#[test]
fn fail_fast_refuses_writes_while_one_is_late() {
    runtime().block_on(async {
        let replica = Replica::new().await;
        let (primary, leader) = replica.primary(AckTimeout::fail_fast(TIMEOUT)).await;
        assert_eq!(primary.ack_timeout().unwrap().mode, AckMode::FailFast);

        not_acknowledged(primary.commit(put("k", 10, 1)).await);
        // A refused write takes no position.
        let refused = primary.commit(put("other", 10, 2)).await;
        assert!(
            matches!(
                &refused,
                Err(ShardError::NotAcknowledged { position: None, .. })
            ),
            "{refused:?}"
        );
        let refused = not_acknowledged(refused);
        assert!(refused.contains("still waits"), "{refused}");
        let lazy = primary.commit_lazy(delete("other")).await;
        not_acknowledged(lazy);
        assert_eq!(primary.last_sequenced(), Seq::new(1));

        // One member is not enough.
        leader.acknowledged(&node(2), Seq::new(1));
        not_acknowledged(primary.commit(put("other", 10, 2)).await);
        leader.acknowledged(&node(3), Seq::new(1));
        until(|| primary.applied() == at(1)).await;
        let writer = spawn_commit(&primary, put("other", 10, 2));
        until(|| primary.last_sequenced() == Seq::new(2)).await;
        acknowledge(&leader, 2);
        assert_eq!(writer.await.unwrap().unwrap().position, at(2));
        assert!(entry(&replica.index, "k").unwrap().object.is_some());
    });
}

/// A seal waits for the writes before it at most the timeout, and lifts
/// itself when it gives up; a conditional write waits for an earlier write
/// of its key no longer either.
#[test]
fn seals_and_conditional_writes_do_not_wait_forever() {
    runtime().block_on(async {
        let replica = Replica::new().await;
        let (primary, leader) = replica.primary(AckTimeout::wait_through(TIMEOUT)).await;
        let writer = spawn_commit(&primary, put("k", 10, 1));
        until(|| primary.last_sequenced() == Seq::new(1)).await;
        let reason = not_acknowledged(primary.seal().await);
        assert!(reason.contains("did not acknowledge"), "{reason}");
        assert!(!primary.is_sealed());
        let conditional = primary
            .commit_if(put("k", 10, 2), |_| Ok::<(), ()>(()))
            .await;
        not_acknowledged(conditional.map(drop));
        // Nothing was sequenced for it.
        assert_eq!(primary.last_sequenced(), Seq::new(1));
        not_acknowledged(writer.await.unwrap());
        acknowledge(&leader, 1);
        until(|| primary.applied() == at(1)).await;
        assert_eq!(primary.seal().await.unwrap().objects, 1);
    });
}

/// Closing a primary whose members are late stops it after the timeout,
/// by which the writers waiting have timed out, and nothing more is
/// applied.
#[test]
fn closing_a_primary_does_not_wait_for_late_members() {
    runtime().block_on(async {
        let replica = Replica::new().await;
        let (primary, leader) = replica.primary(AckTimeout::wait_through(TIMEOUT)).await;
        let writer = spawn_commit(&primary, put("k", 10, 1));
        until(|| primary.last_sequenced() == Seq::new(1)).await;
        tokio::time::timeout(WAIT, primary.close())
            .await
            .expect("closing gives up on the members")
            .unwrap();
        assert!(primary.is_stopped());
        not_acknowledged(writer.await.unwrap());
        // A late acknowledgement applies nothing on a closed shard.
        acknowledge(&leader, 1);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(primary.applied(), at(0));
        assert!(matches!(
            primary.commit(delete("k")).await,
            Err(ShardError::Unavailable { .. })
        ));
        assert!(matches!(
            primary.close().await,
            Err(ShardError::Unavailable { .. })
        ));
    });
}

/// A member waits for its primary's commit watermark to apply what it
/// holds; closing it does not wait for a primary that is gone.
#[test]
fn closing_a_member_does_not_wait_for_its_primary() {
    runtime().block_on(async {
        let replica = Replica::new().await;
        let member = replica.open(&node(2), AckTimeout::fail_fast(TIMEOUT)).await;
        let session = member.begin_session();
        let held = record(at(1), put("k", 10, 1));
        let bytes = held.to_bytes().unwrap();
        assert_eq!(
            member.receive(Some(session), Epoch::new(1), held, bytes, false),
            Ok(true)
        );
        member.settle().await;
        assert_eq!(*member.durable().borrow(), Seq::new(1));
        tokio::time::timeout(WAIT, member.close())
            .await
            .expect("closing gives up on the primary")
            .unwrap();
        assert!(member.is_stopped());
        assert_eq!(member.applied(), at(0));
    });
}

/// A shard alone waits only for its disk, and takes no timeout.
#[test]
fn a_shard_alone_has_no_timeout() {
    runtime().block_on(async {
        let replica = Replica::new().await;
        let alone = Shard::open(
            &config(&shard(1), 1),
            replica.log.clone(),
            Arc::clone(&replica.index),
            replica.pool.clone(),
        )
        .await
        .unwrap();
        alone.set_ack_timeout(AckTimeout::fail_fast(Duration::ZERO));
        assert_eq!(alone.ack_timeout(), None);
        alone.commit(put("k", 10, 1)).await.unwrap();
        assert_eq!(AckTimeout::default(), AckTimeout::DEFAULT);
        assert_eq!(AckTimeout::DEFAULT.mode, AckMode::WaitThrough);
    });
}
