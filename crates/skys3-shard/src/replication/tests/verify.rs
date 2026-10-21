//! A re-admitted learner whose log reconciliation cannot place (§6.6,
//! §6.7): the learner names the epoch of its last record, and the primary
//! checks that it holds a record of that epoch at that `seq`. Either end
//! may read its log back to tell, a scan of every segment that on a large
//! log takes longer than the link timeout, while the other end waits for
//! its next frame. Each end now keeps the session alive meanwhile, so one
//! session verifies the learner, which then catches up (found by the
//! kill, partition, and peering suite, plan M8-08).

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use skys3_index::{Index, IndexConfig};
use skys3_io::{BlockingPool, MonotonicClock, SimDisk};
use skys3_log::record::Delete;
use skys3_log::{LogConfig, LogRecord, RecordBody, SegmentLog};
use skys3_net::{MessageKind, TokioNetwork};
use skys3_types::{Epoch, EpochSeq, RegisterDocument as _, Seq, ShardConfig};
use tokio::time::Instant;

use super::backfill::learner_config;
use super::catch_up::SlowMount;
use super::removal::{initial, put};
use super::takeover::run;
use super::{Link, Pki, connect, durable_through, node, shard};
use crate::lineage::Lineage;
use crate::replication::wire::{self, Append, AppendAck, Backfill, Beacon, Sync, SyncAck};
use crate::replication::{Replication, ReplicationConfig};
use crate::set::ShardSet;
use crate::shard::{Role, Shard};

/// The records the shard's first node wrote alone, in epoch 1: several
/// segments, each read slowly.
const RECORDS: u64 = 40;

/// The last `seq` the hand-written learner reports.
const LEARNER_LAST: u64 = 5;

fn log_config() -> LogConfig {
    LogConfig {
        inline_max_bytes: 512,
        segment_bytes: 8192,
        group_commit_max_delay: Duration::ZERO,
        group_commit_max_bytes: 16 * 1024,
    }
}

fn link_timeout() -> Duration {
    ReplicationConfig::default().link_timeout
}

/// A node's shard set, on a disk whose log reads are slow while `slow` is
/// set, after the node led the shard alone in epoch 1 and wrote
/// [`RECORDS`] records, and restarted: its lineage is known only from its
/// last record on.
async fn led_alone(
    n: u8,
    seed: u64,
    slow: &Arc<AtomicBool>,
) -> (ShardSet<SlowMount>, Arc<MonotonicClock>) {
    let disk = SimDisk::new(seed);
    let mount = SlowMount::new(disk.mount(), Arc::clone(slow));
    let clock = Arc::new(MonotonicClock::new());
    let (log, _) = SegmentLog::open(mount, log_config(), Arc::clone(&clock) as _)
        .await
        .unwrap();
    let index =
        Arc::new(Index::open_sim(&disk.mount(), "index.redb", &IndexConfig::default()).unwrap());
    let pool = || BlockingPool::new("index", NonZeroUsize::MIN).unwrap();
    let alone = ShardConfig {
        primary: node(n),
        members: vec![node(n)],
        replicas: 1,
        min_write_replicas: 1,
        ..initial()
    };
    let old = Shard::open_replica(&alone, &node(n), log.clone(), Arc::clone(&index), pool())
        .await
        .unwrap();
    for i in 0..RECORDS {
        old.commit(put(&format!("k{i}"), 400)).await.unwrap();
    }
    old.close().await.unwrap();
    (ShardSet::new(index, log, pool()), clock)
}

#[test]
fn a_learner_that_reads_its_log_back_to_name_its_last_record_keeps_the_session() {
    run(learner_end());
}

/// Node 2, re-admitted as a learner with its old log, is verified by a
/// hand-written primary that waits at most the link timeout for each
/// frame, and then itself takes longer than that to give its verdict,
/// beaconing meanwhile. The learner keeps its log and takes the next
/// record.
async fn learner_end() {
    let pki = Pki::new();
    let slow = Arc::new(AtomicBool::new(false));
    let (set, clock) = led_alone(2, 43, &slow).await;
    let transport = pki.transport(&node(2));
    let replication: Replication<TokioNetwork, SlowMount> = Replication::new(
        node(2),
        set,
        transport.clone(),
        BTreeMap::new(),
        clock,
        ReplicationConfig::default(),
    );
    let learner = replication.open(&learner_config()).await.unwrap();
    assert_eq!(learner.role(), Role::Learner);
    let listener = transport
        .bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let serving = replication.clone();
    tokio::spawn(async move { serving.serve(listener).await });

    // The primary's lineage is known only from the learner's last record
    // on, in epoch 3: the learner reads its log back, slowly, to name that
    // record's epoch.
    slow.store(true, Ordering::SeqCst);
    let mut link = connect(&pki, 1, address).await;
    let config = learner_config();
    let request = Sync {
        config: config.to_json().unwrap(),
        primary_last: RECORDS,
        sequencing: config.epoch.get(),
        ..Sync::default()
    }
    .with_lineage(&Lineage::new(EpochSeq::new(
        Epoch::new(3),
        Seq::new(RECORDS),
    )));
    link.send(&wire::frame(MessageKind::Sync, &request, Bytes::new()))
        .await
        .unwrap();
    let started = Instant::now();
    let mut pending = 0;
    let answer = loop {
        let frame = tokio::time::timeout(link_timeout(), link.recv())
            .await
            .expect("a frame within the link timeout")
            .unwrap()
            .expect("the link is open");
        let answer: SyncAck = wire::body(&frame, MessageKind::SyncAck).unwrap();
        if answer.done {
            break answer;
        }
        assert!(frame.payload.is_empty());
        pending += 1;
    };
    assert_eq!(
        (answer.last, answer.unverified, answer.fresh),
        (RECORDS, 1, false)
    );
    assert!(
        started.elapsed() > link_timeout() && pending > 0,
        "the learner's read took {:?}, with {pending} answers meanwhile",
        started.elapsed()
    );
    slow.store(false, Ordering::SeqCst);

    // The primary reads its own log back for longer than the link timeout,
    // beaconing meanwhile, and then verifies the record.
    let verifying = Instant::now();
    while verifying.elapsed() < link_timeout() * 3 {
        let beacon = Beacon {
            epoch: config.epoch.get(),
            commit: 0,
            lease: None,
        };
        link.send(&wire::frame(MessageKind::Beacon, &beacon, Bytes::new()))
            .await
            .unwrap();
        tokio::time::sleep(link_timeout() / 4).await;
    }
    let keep = Backfill {
        keep: true,
        ..Backfill::default()
    };
    link.send(&wire::frame(MessageKind::Backfill, &keep, Bytes::new()))
        .await
        .unwrap();
    // The learner keeps its log, and takes the primary's next record.
    append(&mut link, RECORDS + 1).await;
    durable_through(&mut link, RECORDS + 1).await;
}

/// Sends the record at `(3, seq)`.
async fn append(link: &mut Link, seq: u64) {
    let record = LogRecord {
        shard: shard(),
        position: EpochSeq::new(Epoch::new(3), Seq::new(seq)),
        body: RecordBody::Delete(Delete { key: "k".into() }),
    };
    let body = Append {
        epoch: 3,
        commit: 0,
        lazy: false,
        lease: None,
    };
    let frame = wire::frame(MessageKind::Append, &body, record.to_bytes().unwrap());
    link.send(&frame).await.unwrap();
}

/// What the hand-written learner saw.
#[derive(Default)]
struct Seen {
    /// Sessions the primary opened.
    sessions: AtomicUsize,
    /// The last `seq` it holds: [`LEARNER_LAST`] once the primary kept its
    /// log, and then what the primary sent.
    last: AtomicU64,
}

/// Node 2, hand-written: a re-admitted learner whose log ends at
/// [`LEARNER_LAST`] in epoch 1, which reconciliation cannot place. As the
/// real learner, it waits at most the link timeout for each frame, past
/// the primary's beacons, and acknowledges every record it is sent.
async fn verified_learner(pki: &Pki, seen: Arc<Seen>) -> SocketAddr {
    let listener = pki
        .transport(&node(2))
        .bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok(incoming) = listener.accept().await {
            let Ok(mut link) = incoming.handshake().await else {
                continue;
            };
            seen.sessions.fetch_add(1, Ordering::SeqCst);
            let seen = Arc::clone(&seen);
            tokio::spawn(async move {
                if within(&mut link).await.is_none() {
                    return;
                }
                let answer = SyncAck {
                    last: LEARNER_LAST,
                    done: true,
                    epoch: 3,
                    unverified: 1,
                    ..SyncAck::default()
                };
                let frame = wire::frame(MessageKind::SyncAck, &answer, Bytes::new());
                if link.send(&frame).await.is_err() {
                    return;
                }
                loop {
                    let Some(frame) = within(&mut link).await else {
                        return;
                    };
                    if frame.header.kind == MessageKind::Beacon {
                        continue;
                    }
                    let verdict: Backfill = wire::body(&frame, MessageKind::Backfill).unwrap();
                    assert!(verdict.keep, "the primary did not verify the log");
                    seen.last.store(LEARNER_LAST, Ordering::SeqCst);
                    break;
                }
                loop {
                    let Some(frame) = within(&mut link).await else {
                        return;
                    };
                    let lease = if frame.header.kind == MessageKind::Beacon {
                        wire::body::<Beacon>(&frame, MessageKind::Beacon)
                            .unwrap()
                            .lease
                    } else {
                        let append: Append = wire::body(&frame, MessageKind::Append).unwrap();
                        let (record, _) = LogRecord::decode(&frame.payload).unwrap();
                        seen.last
                            .fetch_max(record.position.seq.get(), Ordering::SeqCst);
                        append.lease
                    };
                    let ack = AppendAck {
                        durable: seen.last.load(Ordering::SeqCst),
                        rejected: 0,
                        lease,
                    };
                    let frame = wire::frame(MessageKind::AppendAck, &ack, Bytes::new());
                    if link.send(&frame).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    address
}

/// The next frame, if one comes within the link timeout.
async fn within(link: &mut Link) -> Option<skys3_net::Frame> {
    tokio::time::timeout(link_timeout(), link.recv())
        .await
        .ok()?
        .ok()
        .flatten()
}

#[test]
fn a_primary_that_reads_its_log_back_to_verify_a_learner_keeps_the_session() {
    run(primary_end());
}

/// Node 1 led the shard alone and restarted, and now leads it with node 2
/// re-admitted as a learner, whose log ends before node 1's lineage is
/// known. Node 1 reads its log back, slowly, to verify the learner's last
/// record, and then sends it the rest of its log: one session does it all.
async fn primary_end() {
    let pki = Pki::new();
    let seen = Arc::new(Seen::default());
    let address = verified_learner(&pki, Arc::clone(&seen)).await;
    let slow = Arc::new(AtomicBool::new(false));
    let (set, clock) = led_alone(1, 47, &slow).await;
    slow.store(true, Ordering::SeqCst);
    let peers = BTreeMap::from([(node(2), address.to_string().parse().unwrap())]);
    let primary: Replication<TokioNetwork, SlowMount> = Replication::new(
        node(1),
        set,
        pki.transport(&node(1)),
        peers,
        clock,
        ReplicationConfig::default(),
    );
    let _shard = primary.open(&learner_config()).await.unwrap();
    while seen.sessions.load(Ordering::SeqCst) == 0 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    while seen.last.load(Ordering::SeqCst) < RECORDS {
        assert_eq!(
            seen.sessions.load(Ordering::SeqCst),
            1,
            "a session ended while the primary read its log"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(seen.sessions.load(Ordering::SeqCst), 1);
    slow.store(false, Ordering::SeqCst);
}
