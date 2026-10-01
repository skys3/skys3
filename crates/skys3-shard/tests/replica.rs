//! Replicated shard replicas without the network: roles, a member taking
//! its primary's records, and the primary's commit rule (§5.1).

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use skys3_index::Index;
use skys3_io::{BlockingPool, Clock, MonoTime, SimDisk, SimMount};
use skys3_log::{LogRecord, RecordBody, SegmentLog};
use skys3_shard::{Pending, Role, Shard, ShardError};
use skys3_types::{Epoch, EpochSeq, NodeId, Seq, ShardConfig};
use support::{at, config, delete, index_config, open_log, pool, put, record, runtime, shard};

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
        let disk = SimDisk::new(5);
        Self {
            log: open_log(disk.mount()).await,
            index: Arc::new(Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap()),
            pool: pool(),
        }
    }

    async fn open(
        &self,
        config: &ShardConfig,
        node: &NodeId,
    ) -> Result<Shard<SimMount>, ShardError> {
        Shard::open_replica(
            config,
            node,
            self.log.clone(),
            Arc::clone(&self.index),
            self.pool.clone(),
        )
        .await
    }
}

/// A clock that moves only when a test moves it.
#[derive(Debug, Default)]
struct ManualClock(AtomicU64);

impl ManualClock {
    fn advance(&self, by: Duration) {
        let nanos = u64::try_from(by.as_nanos()).unwrap();
        self.0.fetch_add(nanos, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> MonoTime {
        MonoTime::from_nanos(self.0.load(Ordering::SeqCst))
    }

    fn runtime_deadline(&self, _: MonoTime) -> tokio::time::Instant {
        unreachable!("the shard sets no timer on the lease clock")
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

/// The record at `seq` of epoch 1, and its encoding.
fn encoded(seq: u64, body: RecordBody) -> (LogRecord, Bytes) {
    let record = record(at(seq), body);
    let bytes = record.to_bytes().unwrap();
    (record, bytes)
}

fn configuration_error(result: Result<impl std::fmt::Debug, ShardError>) -> String {
    match result {
        Err(ShardError::Configuration { reason, .. }) => reason,
        other => panic!("expected a configuration error, got {other:?}"),
    }
}

#[test]
fn roles_follow_the_configuration() {
    runtime().block_on(async {
        let replica = Replica::new().await;
        let shared = replicated();
        let outsider = configuration_error(replica.open(&shared, &node(4)).await.map(drop));
        assert!(outsider.contains("not a member"), "{outsider}");
        let learners = ShardConfig {
            learners: vec![node(4)],
            ..shared.clone()
        };
        let reason = configuration_error(replica.open(&learners, &node(1)).await.map(drop));
        assert!(reason.contains("learners"), "{reason}");
        let headless = ShardConfig {
            primary: node(9),
            ..shared.clone()
        };
        let reason = configuration_error(replica.open(&headless, &node(1)).await.map(drop));
        assert!(reason.contains("primary is not a member"), "{reason}");
        let unnamed = Shard::open(
            &shared,
            replica.log.clone(),
            Arc::clone(&replica.index),
            replica.pool.clone(),
        )
        .await;
        assert!(configuration_error(unnamed.map(drop)).contains("named node"));

        // A member serves nothing, and names its primary.
        let member = replica.open(&shared, &node(2)).await.unwrap();
        assert_eq!(member.role(), Role::Member);
        assert!(!member.is_serving());
        assert!(member.leader().is_none());
        match member.commit(delete("k")).await {
            Err(ShardError::NotPrimary { primary, epoch, .. }) => {
                assert_eq!((primary, epoch), (node(1), Epoch::new(1)));
            }
            other => panic!("expected a redirect, got {other:?}"),
        }
        assert!(matches!(
            member.entry("k").await,
            Err(ShardError::NotPrimary { .. })
        ));
        // A replicated shard changes only by removing members (§6.4).
        let takeover = ShardConfig {
            epoch: Epoch::new(2),
            primary: node(2),
            ..shared.clone()
        };
        let reason = configuration_error(member.reconfigure(&takeover).await);
        assert!(reason.contains("takes over"), "{reason}");
        member.close().await.unwrap();

        // A single member is alone, whether or not it is named.
        let alone = replica.open(&config(&shard(1), 1), &node(1)).await.unwrap();
        assert_eq!(alone.role(), Role::Alone);
        assert!(alone.is_serving());
        assert_eq!(alone.entry("k").await, Ok(None));
        // A stopped replica serves no reads: its index may be stale.
        alone.close().await.unwrap();
        let stopped = alone.entry("k").await;
        assert!(
            matches!(stopped, Err(ShardError::Unavailable { .. })),
            "{stopped:?}"
        );
        assert!(alone.check_readable().is_err());
    });
}

#[test]
fn a_member_takes_its_primarys_records_in_order() {
    runtime().block_on(async {
        let replica = Replica::new().await;
        let member = replica.open(&replicated(), &node(2)).await.unwrap();
        let session = member.begin_session();
        let epoch = Epoch::new(1);
        let (first, first_bytes) = encoded(1, support::put("a", 10, 1));
        let (second, second_bytes) = encoded(2, delete("a"));
        let (third, third_bytes) = encoded(3, delete("b"));

        assert_eq!(
            member.receive(
                Some(session),
                epoch,
                first.clone(),
                first_bytes.clone(),
                false
            ),
            Ok(true)
        );
        // A gap, a repeat, another epoch, and an earlier session.
        let gap = member.receive(Some(session), epoch, third, third_bytes, false);
        assert!(
            matches!(gap, Err(ShardError::InvalidRecord { .. })),
            "{gap:?}"
        );
        let repeat = member.receive(Some(session), epoch, first, first_bytes, false);
        assert_eq!(repeat, Ok(false));
        let older = member.receive(
            Some(session),
            Epoch::ZERO,
            second.clone(),
            second_bytes.clone(),
            false,
        );
        assert!(configuration_error(older).contains("epoch 0"));
        // A record of another epoch, wrapped in an append of the member's.
        for wrapped in [Epoch::ZERO, Epoch::new(2)] {
            let record = LogRecord {
                position: EpochSeq::new(wrapped, Seq::new(2)),
                ..second.clone()
            };
            let bytes = record.to_bytes().unwrap();
            let taken = member.receive(Some(session), epoch, record, bytes, false);
            assert!(
                matches!(&taken, Err(ShardError::InvalidRecord { reason, .. })
                    if reason.contains("not of the replica's epoch")),
                "{taken:?}"
            );
        }
        assert_eq!(member.last_sequenced(), Seq::new(1));
        let config_record = record(at(1), RecordBody::Config(replicated()));
        let config_bytes = config_record.to_bytes().unwrap();
        let own = member.receive(Some(session), epoch, config_record, config_bytes, false);
        assert!(
            matches!(own, Err(ShardError::InvalidRecord { .. })),
            "{own:?}"
        );
        let other = record(at(2), delete("x"));
        let other = LogRecord {
            shard: shard(5),
            ..other
        };
        let other_bytes = other.to_bytes().unwrap();
        let foreign = member.receive(Some(session), epoch, other, other_bytes, false);
        assert!(
            matches!(foreign, Err(ShardError::InvalidRecord { .. })),
            "{foreign:?}"
        );
        let newer = member.begin_session();
        let stale = member.receive(
            Some(session),
            epoch,
            second.clone(),
            second_bytes.clone(),
            true,
        );
        assert!(configuration_error(stale).contains("earlier session"));
        assert_eq!(
            member.receive(Some(newer), epoch, second, second_bytes, true),
            Ok(true)
        );
        assert_eq!(member.last_sequenced(), Seq::new(2));

        // Durable, not applied until the watermark covers it.
        member.settle().await;
        let durable = member.durable();
        assert_eq!(*durable.borrow(), Seq::new(2));
        assert_eq!(member.applied(), at(0));
        member.commit_through(Seq::new(1));
        until(|| member.applied() == at(1)).await;
        let entry = replica.index.read().unwrap().entry(&shard(0), "a").unwrap();
        assert!(entry.unwrap().object.is_some());
        member.commit_through(Seq::new(2));
        until(|| member.applied() == at(2)).await;

        // The log gives the records back by seq.
        let tail = member.read_tail(Seq::ZERO, Seq::new(2)).await.unwrap();
        let seqs: Vec<u64> = tail
            .iter()
            .map(|(position, _)| position.seq.get())
            .collect();
        assert_eq!(seqs, [1, 2]);
        assert_eq!(LogRecord::decode(&tail[0].1).unwrap().0.position, at(1));
        let missing = member.read_tail(Seq::ZERO, Seq::new(3)).await;
        assert!(
            matches!(missing, Err(ShardError::Unavailable { .. })),
            "{missing:?}"
        );
    });
}

#[test]
fn a_primary_commits_once_every_member_holds_the_record() {
    runtime().block_on(async {
        let replica = Replica::new().await;
        let primary = replica.open(&replicated(), &node(1)).await.unwrap();
        assert_eq!(primary.role(), Role::Primary);
        let leader = Arc::clone(primary.leader().unwrap());
        assert_eq!(leader.members(), [node(2), node(3)]);
        assert!(leader.claim_links());
        assert!(!leader.claim_links());
        // Until every member reported, the primary serves nothing.
        assert!(matches!(
            primary.commit(delete("k")).await,
            Err(ShardError::Unavailable { .. })
        ));
        assert!(matches!(
            primary.list(Default::default()).await,
            Err(ShardError::Unavailable { .. })
        ));
        leader.synced(&node(2), Seq::ZERO);
        assert!(!primary.is_serving());
        leader.synced(&node(9), Seq::ZERO);
        leader.synced(&node(3), Seq::ZERO);
        until(|| primary.is_serving()).await;

        let writer = {
            let primary = primary.clone();
            tokio::spawn(async move { primary.commit(put("k", 10, 1)).await })
        };
        until(|| matches!(leader.records_after(Seq::ZERO, 8), Pending::Ready(r) if r.len() == 1))
            .await;
        let Pending::Ready(records) = leader.records_after(Seq::ZERO, 8) else {
            unreachable!()
        };
        assert_eq!(records[0].seq, Seq::new(1));
        assert!(!records[0].lazy);
        // Durable here and on one member: not committed.
        let changes = leader.subscribe(&node(3)).unwrap();
        assert!(leader.subscribe(&node(9)).is_none());
        leader.acknowledged(&node(2), Seq::new(1));
        primary.settle().await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!writer.is_finished());
        assert_eq!(leader.commit(), Seq::ZERO);
        assert_eq!(leader.acked(&node(2)), Some(Seq::new(1)));
        leader.acknowledged(&node(3), Seq::new(1));
        let committed = writer.await.unwrap().unwrap();
        assert_eq!(committed.position, at(1));
        until(|| leader.commit() == Seq::new(1)).await;
        // The watermark moved, so the link to every member wakes.
        assert!(changes.has_changed().unwrap());

        // Committed records leave memory, and come back from the log for a
        // member that is behind.
        assert_eq!(
            leader.records_after(Seq::ZERO, 8),
            Pending::InLog(Seq::new(1))
        );
        leader.restore(
            Seq::ZERO,
            primary.read_tail(Seq::ZERO, Seq::new(1)).await.unwrap(),
        );
        let Pending::Ready(records) = leader.records_after(Seq::ZERO, 8) else {
            panic!("the records were restored");
        };
        assert_eq!(records.len(), 1);
        assert_eq!(
            leader.records_after(Seq::new(1), 8),
            Pending::Ready(Vec::new())
        );
    });
}

#[test]
fn a_restarted_primary_rolls_forward_what_a_member_holds() {
    runtime().block_on(async {
        let replica = Replica::new().await;
        let primary = replica.open(&replicated(), &node(1)).await.unwrap();
        let leader = Arc::clone(primary.leader().unwrap());
        // A member returns a record the primary lost.
        let (lost, bytes) = encoded(1, put("k", 10, 1));
        let epoch = Epoch::new(1);
        assert_eq!(
            primary.receive(None, epoch, lost.clone(), bytes.clone(), false),
            Ok(true)
        );
        // Another member returns it too: it is taken once.
        assert_eq!(primary.receive(None, epoch, lost, bytes, false), Ok(false));
        leader.synced(&node(2), Seq::new(1));
        leader.synced(&node(3), Seq::ZERO);
        // The record is sent on to the member that lacks it, and the
        // primary serves once it is committed.
        until(|| matches!(leader.records_after(Seq::ZERO, 8), Pending::Ready(r) if r.len() == 1))
            .await;
        assert!(!primary.is_serving());
        leader.acknowledged(&node(3), Seq::new(1));
        until(|| primary.is_serving()).await;
        assert_eq!(primary.applied(), at(1));
        let clock = Arc::new(ManualClock::default());
        leader.start_leases(clock, Duration::from_secs(4));
        let stamp = leader.lease_stamp().unwrap();
        leader.granted(&node(2), stamp);
        leader.granted(&node(3), stamp);
        let entry = primary.entry("k").await.unwrap().unwrap();
        assert!(entry.object.is_some());
    });
}

/// Why `result` is unavailable.
fn unavailable<T: std::fmt::Debug>(result: Result<T, ShardError>) -> String {
    match result {
        Err(ShardError::Unavailable { reason, .. }) => reason,
        other => panic!("expected the shard to be unavailable, got {other:?}"),
    }
}

/// Reads, listings, and conditional checks need a valid lease from every
/// member; unconditional writes do not (§5.4).
#[test]
fn a_primary_reads_only_under_every_members_lease() {
    runtime().block_on(async {
        let replica = Replica::new().await;
        let primary = replica.open(&replicated(), &node(1)).await.unwrap();
        let leader = Arc::clone(primary.leader().unwrap());
        assert!(leader.claim_links());
        leader.synced(&node(2), Seq::ZERO);
        leader.synced(&node(3), Seq::ZERO);
        until(|| primary.is_serving()).await;

        // Serving, but without leases: no read, and no stamp to send.
        assert!(!leader.holds_leases());
        assert_eq!(leader.lease_stamp(), None);
        let reason = unavailable(primary.entry("k").await);
        assert!(reason.contains("lease"), "{reason}");
        leader.granted(&node(2), MonoTime::ZERO);
        assert_eq!(leader.lease(&node(2)), None);

        let clock = Arc::new(ManualClock::default());
        clock.advance(Duration::from_secs(10));
        leader.start_leases(
            Arc::clone(&clock) as Arc<dyn Clock>,
            Duration::from_millis(400),
        );
        let stamp = leader.lease_stamp().unwrap();
        clock.advance(Duration::from_millis(100));
        leader.granted(&node(2), stamp);
        // A node that is not a member grants nothing.
        leader.granted(&node(9), stamp);
        assert_eq!(leader.lease(&node(9)), None);
        assert_eq!(
            leader.lease(&node(2)),
            Some(stamp + Duration::from_millis(400))
        );
        unavailable(primary.list(Default::default()).await);
        leader.granted(&node(3), stamp);
        assert!(leader.holds_leases());
        assert_eq!(primary.entry("k").await.unwrap(), None);
        primary.list(Default::default()).await.unwrap();
        primary.uploads("", None, 10).await.unwrap();

        // The leases count from the stamp: they lapse 400 ms after it.
        clock.advance(Duration::from_millis(300));
        assert!(!leader.holds_leases());
        unavailable(primary.entry("k").await);
        // A conditional write needs a read of its key, so it waits for a
        // lease too; an unconditional one commits on the members'
        // acknowledgements alone.
        let conditional = primary
            .commit_if(put("k", 10, 1), |_| Ok::<(), ()>(()))
            .await;
        unavailable(conditional.map(drop));
        let writer = {
            let primary = primary.clone();
            tokio::spawn(async move { primary.commit(delete("k")).await })
        };
        until(|| primary.last_sequenced() == Seq::new(1)).await;
        leader.acknowledged(&node(2), Seq::new(1));
        leader.acknowledged(&node(3), Seq::new(1));
        assert_eq!(writer.await.unwrap().unwrap().position, at(1));

        // Fresh acknowledgements renew the leases.
        let stamp = leader.lease_stamp().unwrap();
        leader.granted(&node(2), stamp);
        leader.granted(&node(3), stamp);
        let entry = primary.entry("k").await.unwrap().unwrap();
        assert!(entry.object.is_none());
        let conditional = {
            let primary = primary.clone();
            tokio::spawn(async move {
                primary
                    .commit_if(put("k", 10, 2), |entry| {
                        let deleted = entry.is_some_and(|e| e.object.is_none());
                        deleted.then_some(()).ok_or(())
                    })
                    .await
            })
        };
        until(|| primary.last_sequenced() == Seq::new(2)).await;
        leader.acknowledged(&node(2), Seq::new(2));
        leader.acknowledged(&node(3), Seq::new(2));
        let committed = conditional.await.unwrap().unwrap().unwrap();
        assert_eq!(committed.position, at(2));
    });
}
