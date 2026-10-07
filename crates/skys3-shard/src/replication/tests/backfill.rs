//! Backfill and re-admission (§6.4, §6.7): a new learner gets a snapshot
//! of the primary's index and then the payload it lacks, over real TCP;
//! and a re-admitted learner, driven by a hand-written primary, keeps an
//! old log the primary verifies, or installs a snapshot in its place.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use skys3_control::{MemoryControlStore, RetryPolicy};
use skys3_index::{
    Entry, EntryState, Index, IndexConfig, ObjectVersion, Payload, ShardTable, codec,
};
use skys3_io::{BlockingPool, MonotonicClock, SimDisk, SimMount};
use skys3_log::record::{Delete, Extent, Flushed, MpuCreate, MpuPart, Put, PutData};
use skys3_log::{LogConfig, LogRecord, RecordBody, SegmentLog};
use skys3_net::{MessageKind, TokioNetwork};
use skys3_types::{ETag, Epoch, EpochSeq, ProposalId, RegisterDocument, Seq, ShardConfig};

use super::learners::{Promotions, add_learner, current, nodes, two_members};
use super::removal::{initial, put, register, until};
use super::takeover::run;
use super::{Link, Pki, WAIT, connect, durable_through, next, node, shard};
use crate::lineage::Lineage;
use crate::replication::wire::{
    self, Append, Backfill, BackfillAck, Row, SnapshotRows, Sync, SyncAck,
};
use crate::replication::{
    ControlRegisters, Replaced, Replication, ReplicationConfig, ShardRegisters as _,
};
use crate::cache::{CacheMetrics, CacheSettings, CleanCache};
use crate::set::ShardSet;
use crate::shard::{Role, Shard};

#[test]
fn a_new_learner_gets_a_snapshot_and_then_its_payload() {
    run(new_learner(true));
}

/// A `local` bucket's nodes keep every payload, also of an entry a backup
/// target made clean (§8.9): a learner gets that payload too.
#[test]
fn a_learner_of_a_bucket_whose_payload_is_kept_gets_its_clean_payload() {
    run(new_learner(false));
}

/// A learner joins a shard of two members; with `evicts`, their caches
/// evict the bucket's clean payload, as a `write_back` bucket's.
async fn new_learner(evicts: bool) {
    let pki = Pki::new();
    let store = MemoryControlStore::new();
    let promotions = Promotions {
        registers: ControlRegisters::new(store.clone(), RetryPolicy::default()),
        delay: Duration::ZERO,
        scripted: Mutex::default(),
        proposed: Arc::default(),
        epochs: Arc::default(),
    };
    let nodes = nodes(&pki, &store, &two_members(), promotions, None).await;
    if evicts {
        for replication in &nodes.replications {
            let cache = CleanCache::new(CacheSettings::default(), CacheMetrics::default());
            cache.set_clean_copies([(shard().bucket, 1)]);
            replication.set().use_cache(&cache).await;
        }
    }
    nodes.replications[1].open(&two_members()).await.unwrap();
    let primary = nodes.replications[0].open(&two_members()).await.unwrap();
    until(|| primary.is_serving()).await;

    // Inline, in an extent, flushed and so clean, and an open upload.
    let small = primary.commit(put("a", 10)).await.unwrap().position;
    let extent = Extent {
        key: "big".into(),
        offset: 0,
        data: Bytes::from(vec![9; 600]),
    };
    let extent = primary.append_extent(extent).await.unwrap();
    let Put { etag, .. } = put_body("big");
    let big = Put {
        size: 600,
        etag,
        data: PutData::Extents(vec![extent]),
        ..put_body("big")
    };
    primary.commit(RecordBody::Put(big)).await.unwrap();
    let clean = primary.commit(put("c", 20)).await.unwrap().position;
    let flushed = Flushed {
        key: "c".into(),
        seq: clean.seq,
        remote_etag: Some(put_body("c").etag),
        remote_version_id: None,
    };
    primary.commit(RecordBody::Flushed(flushed)).await.unwrap();
    let create = MpuCreate {
        key: "m".into(),
        initiated_ms: 1,
        metadata: BTreeMap::new(),
        tags: BTreeMap::new(),
        checksum: None,
    };
    let upload = primary.commit(RecordBody::MpuCreate(create)).await.unwrap();
    let part = MpuPart {
        key: "m".into(),
        upload: upload.position,
        part_number: 1,
        size: 5,
        last_modified_ms: 1,
        etag: put_body("m").etag,
        checksums: BTreeMap::new(),
        data: PutData::Inline(Bytes::from_static(b"parts")),
    };
    let part = primary.commit(RecordBody::MpuPart(part)).await.unwrap();

    // The learner installs a snapshot, backfills its payload, and is
    // promoted.
    add_learner(&store, &nodes, &two_members()).await;
    until_promoted(&store).await;
    let learner = current(&nodes.replications[2]).await;
    assert!(learner.read_tail(Seq::ZERO, Seq::new(1)).await.is_err());
    assert_eq!(learner.payload(small).await.unwrap(), vec![7; 10]);
    assert_eq!(
        learner.payload(extent.position).await.unwrap(),
        vec![9; 600]
    );
    assert_eq!(&learner.payload(part.position).await.unwrap()[..], b"parts");
    let entry = |key: &str| {
        let read = learner.index().read().unwrap();
        read.entry(&shard(), key).unwrap().unwrap()
    };
    if evicts {
        // The clean entry's payload stays with the remote.
        assert_eq!(entry("c").state, EntryState::Evicted);
        assert_eq!(entry("c").object.unwrap().payload, Payload::None);
        assert!(learner.payload(clean).await.is_err());
    } else {
        assert_eq!(entry("c").state, EntryState::Clean);
        assert_eq!(learner.payload(clean).await.unwrap(), vec![7; 20]);
    }
    assert_eq!(entry("a").state, EntryState::Dirty);
}

/// The body of a 10-byte `PUT` of `key`.
fn put_body(key: &str) -> Put {
    match put(key, 10) {
        RecordBody::Put(put) => put,
        _ => unreachable!("put makes a PUT"),
    }
}

/// Waits until the register makes node 3 a member.
async fn until_promoted(store: &MemoryControlStore) {
    let deadline = tokio::time::Instant::now() + WAIT;
    while !register(store).await.is_member(&node(3)) {
        assert!(tokio::time::Instant::now() < deadline, "no promotion");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Shard `b-1/0` in epoch 3, led by node 1 alone, with node 2 its learner.
fn learner_config() -> ShardConfig {
    ShardConfig {
        epoch: Epoch::new(3),
        members: vec![node(1)],
        learners: vec![node(2)],
        replicas: 2,
        min_write_replicas: 1,
        proposal_id: ProposalId::new("p-3").unwrap(),
        ..initial()
    }
}

/// Node 2, serving links on a loopback port, with the shard open as a
/// learner of [`learner_config`], whose log holds three records of epoch
/// 1 from when node 2 led the shard alone. The node restarted since, so
/// its lineage is known only from its last record on.
async fn readmitted(pki: &Pki) -> (SocketAddr, Replication<TokioNetwork, SimMount>) {
    let disk = SimDisk::new(41);
    let log_config = LogConfig {
        inline_max_bytes: 512,
        segment_bytes: 8192,
        group_commit_max_delay: Duration::ZERO,
        group_commit_max_bytes: 16 * 1024,
    };
    let clock = Arc::new(MonotonicClock::new());
    let (log, _) = SegmentLog::open(disk.mount(), log_config, Arc::clone(&clock) as _)
        .await
        .unwrap();
    let index =
        Arc::new(Index::open_sim(&disk.mount(), "index.redb", &IndexConfig::default()).unwrap());
    let pool = || BlockingPool::new("index", NonZeroUsize::MIN).unwrap();
    let alone = ShardConfig {
        primary: node(2),
        members: vec![node(2)],
        replicas: 1,
        min_write_replicas: 1,
        ..initial()
    };
    let old = Shard::open_replica(&alone, &node(2), log.clone(), Arc::clone(&index), pool())
        .await
        .unwrap();
    for key in ["a", "b", "c"] {
        old.commit(put(key, 10)).await.unwrap();
    }
    old.close().await.unwrap();
    let set = ShardSet::new(index, log, pool());
    let transport = pki.transport(&node(2));
    let replication = Replication::new(
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
    (address, replication)
}

/// Opens a session of node 1, a primary whose log ends at `last` with
/// lineage `lineage`, and returns the learner's final answer.
async fn session(link: &mut Link, last: u64, lineage: &Lineage) -> SyncAck {
    let config = learner_config();
    let request = Sync {
        config: config.to_json().unwrap(),
        primary_last: last,
        sequencing: config.epoch.get(),
        ..Sync::default()
    }
    .with_lineage(lineage);
    link.send(&wire::frame(MessageKind::Sync, &request, Bytes::new()))
        .await
        .unwrap();
    let frame = next(link).await.expect("a SyncAck");
    wire::body(&frame, MessageKind::SyncAck).unwrap()
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

/// Sends a snapshot at `(epoch, seq)` holding a dirty entry of `z`, and
/// returns the learner's answer.
async fn snapshot(link: &mut Link, epoch: u64, seq: u64) -> BackfillAck {
    let entry = Entry {
        version: EpochSeq::new(Epoch::new(3), Seq::new(2)),
        state: EntryState::Dirty,
        object: Some(ObjectVersion {
            size: 1,
            last_modified_ms: 1,
            local_etag: ETag::new(format!("{:032x}", 1)).unwrap(),
            write_identity: None,
            metadata: BTreeMap::new(),
            tags: BTreeMap::new(),
            checksums: BTreeMap::new(),
            storage_class: None,
            copy_source: None,
            payload: Payload::Inline(EpochSeq::new(Epoch::new(3), Seq::new(2))),
            coded: None,
        }),
        remote_etag: None,
        remote_version_id: None,
    };
    let row = Row {
        table: ShardTable::Namespace.code(),
        key: codec::entry_key(&shard(), "z"),
        value: codec::encode_entry(&entry).unwrap(),
    };
    for (rows, done) in [(vec![row], false), (Vec::new(), true)] {
        let header = Backfill {
            epoch,
            seq,
            done,
            ..Backfill::default()
        };
        let rows = SnapshotRows { rows }.to_payload();
        link.send(&wire::frame(MessageKind::Backfill, &header, rows))
            .await
            .unwrap();
    }
    let frame = next(link).await.expect("a BackfillAck");
    wire::body(&frame, MessageKind::BackfillAck).unwrap()
}

#[test]
fn a_readmitted_learner_keeps_a_log_the_primary_verifies_or_installs_a_snapshot() {
    run(readmission());
}

async fn readmission() {
    let pki = Pki::new();
    let (address, replication) = readmitted(&pki).await;
    let replica = || async { current(&replication).await };

    // The primary's lineage is known only from seq 3 on, in epoch 3: the
    // learner cannot tell, and names its last record's epoch instead.
    let primary = Lineage::new(EpochSeq::new(Epoch::new(3), Seq::new(3)));
    let mut link = connect(&pki, 1, address).await;
    let answer = session(&mut link, 3, &primary).await;
    assert_eq!(
        (answer.last, answer.unverified, answer.fresh),
        (3, 1, false)
    );
    // The primary holds that record: the learner keeps its log, and takes
    // the primary's next record.
    let keep = Backfill {
        keep: true,
        ..Backfill::default()
    };
    link.send(&wire::frame(MessageKind::Backfill, &keep, Bytes::new()))
        .await
        .unwrap();
    append(&mut link, 4).await;
    durable_through(&mut link, 4).await;
    assert_eq!(replica().await.sequencing(), Epoch::new(3));

    // A snapshot of an older epoch than the learner's is refused.
    let mut link = connect(&pki, 1, address).await;
    let primary = Lineage::new(EpochSeq::new(Epoch::new(3), Seq::new(9)));
    let answer = session(&mut link, 9, &primary).await;
    assert_eq!(
        (answer.last, answer.unverified, answer.fresh),
        (4, 0, false)
    );
    assert!(!snapshot(&mut link, 2, 9).await.refused.is_empty());

    // A snapshot replaces the learner's log and index.
    let mut link = connect(&pki, 1, address).await;
    session(&mut link, 9, &primary).await;
    assert_eq!(snapshot(&mut link, 3, 9).await.refused, "");
    let learner = replica().await;
    assert_eq!(learner.applied(), EpochSeq::new(Epoch::new(3), Seq::new(9)));
    assert_eq!(learner.role(), Role::Learner);
    let read = learner.index().read().unwrap();
    assert!(read.entry(&shard(), "a").unwrap().is_none());
    assert!(read.entry(&shard(), "z").unwrap().is_some());
    drop(read);
    // Its records of older epochs are gone, and it reports holding the
    // snapshot.
    assert!(learner.read_tail(Seq::new(2), Seq::new(3)).await.is_err());
    let mut link = connect(&pki, 1, address).await;
    let answer = session(&mut link, 9, &primary).await;
    assert_eq!((answer.last, answer.fresh), (9, false));

    // An install cut short leaves the learner holding nothing.
    learner
        .index()
        .begin_install(&shard(), EpochSeq::new(Epoch::new(3), Seq::MAX))
        .unwrap();
    let (_, log) = replication.set().logs().next().unwrap();
    let pool = BlockingPool::new("reopened", NonZeroUsize::MIN).unwrap();
    let index = Arc::clone(learner.index());
    let reopened = Shard::open_replica(&learner_config(), &node(2), log.clone(), index, pool)
        .await
        .unwrap();
    assert_eq!(reopened.applied(), EpochSeq::default());
    let read = reopened.index().read().unwrap();
    assert!(read.entry(&shard(), "z").unwrap().is_none());
    // Only a learner opens so.
    let member = ShardConfig {
        members: vec![node(1), node(2)],
        learners: Vec::new(),
        ..learner_config()
    };
    let pool = BlockingPool::new("member", NonZeroUsize::MIN).unwrap();
    let index = Arc::clone(learner.index());
    assert!(
        Shard::open_replica(&member, &node(2), log.clone(), index, pool)
            .await
            .is_err()
    );
}

#[test]
fn a_backfill_is_refused_to_a_primary_the_learner_does_not_follow() {
    run(refusals());
}

async fn refusals() {
    let pki = Pki::new();
    let (address, _replication) = readmitted(&pki).await;
    let open = |config: &ShardConfig| Backfill {
        config: config.to_json().unwrap(),
        ..Backfill::default()
    };
    let other = ShardConfig {
        epoch: Epoch::new(4),
        ..learner_config()
    };
    for (from, request) in [
        (3, open(&learner_config())),
        (1, open(&other)),
        (1, Backfill::default()),
    ] {
        let mut link = connect(&pki, from, address).await;
        link.send(&wire::frame(MessageKind::Backfill, &request, Bytes::new()))
            .await
            .unwrap();
        let frame = next(&mut link).await.expect("an answer");
        let answer: BackfillAck = wire::body(&frame, MessageKind::BackfillAck).unwrap();
        assert!(!answer.refused.is_empty());
    }
    // The learner's own primary is asked for nothing: its index holds no
    // dirty entry without payload.
    let mut link = connect(&pki, 1, address).await;
    let request = open(&learner_config());
    link.send(&wire::frame(MessageKind::Backfill, &request, Bytes::new()))
        .await
        .unwrap();
    let frame = next(&mut link).await.expect("an answer");
    let answer: BackfillAck = wire::body(&frame, MessageKind::BackfillAck).unwrap();
    assert!(answer.complete && answer.refused.is_empty(), "{answer:?}");
    assert_eq!(answer.applied(), EpochSeq::new(Epoch::new(1), Seq::new(3)));
}

#[test]
fn a_removed_member_that_stayed_up_is_readmitted_as_a_learner() {
    run(removed_and_readmitted());
}

/// Node 2 is removed while it is up, and is not told: its replica stays
/// open as a member of epoch 1. The driver then adds it back as a
/// learner, without a restart. A member never becomes a learner by a
/// configuration change, so the node closes that replica and opens again
/// as a learner, which joins and is promoted.
async fn removed_and_readmitted() {
    let pki = Pki::new();
    let store = MemoryControlStore::new();
    let promotions = Promotions {
        registers: ControlRegisters::new(store.clone(), RetryPolicy::default()),
        delay: Duration::ZERO,
        scripted: Mutex::default(),
        proposed: Arc::default(),
        epochs: Arc::default(),
    };
    let nodes = nodes(&pki, &store, &two_members(), promotions, None).await;
    let stale = nodes.replications[1].open(&two_members()).await.unwrap();
    let primary = nodes.replications[0].open(&two_members()).await.unwrap();
    let leader = Arc::clone(primary.leader().unwrap());
    until(|| primary.is_serving()).await;
    primary.commit(put("a", 10)).await.unwrap();

    let registers = ControlRegisters::new(store.clone(), RetryPolicy::default());
    let removed = ShardConfig {
        epoch: Epoch::new(2),
        members: vec![node(1)],
        min_write_replicas: 1,
        proposal_id: ProposalId::new("remove-2").unwrap(),
        ..two_members()
    };
    assert_eq!(
        registers.replace(&two_members(), &removed).await.unwrap(),
        Replaced::Accepted
    );
    nodes.replications[0].open(&removed).await.unwrap();
    primary.commit(put("b", 10)).await.unwrap();
    assert_eq!(stale.role(), Role::Member);

    let readmitted = ShardConfig {
        epoch: Epoch::new(3),
        learners: vec![node(2)],
        proposal_id: ProposalId::new("readmit-2").unwrap(),
        ..removed.clone()
    };
    assert_eq!(
        registers.replace(&removed, &readmitted).await.unwrap(),
        Replaced::Accepted
    );
    let learner = nodes.replications[1].open(&readmitted).await.unwrap();
    assert_eq!(learner.role(), Role::Learner);
    assert!(stale.is_stopped());
    nodes.replications[0].open(&readmitted).await.unwrap();
    let deadline = tokio::time::Instant::now() + WAIT;
    while !register(&store).await.is_member(&node(2)) {
        assert!(tokio::time::Instant::now() < deadline, "no promotion");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    primary.commit(put("c", 10)).await.unwrap();
    let member = current(&nodes.replications[1]).await;
    assert!(*member.durable().borrow() >= leader.commit());
}

#[test]
fn a_learner_a_takeover_left_out_rejoins_under_the_new_primary() {
    run(rejoining());
}

/// A learner of node 1 that node 3's takeover left out, and that the
/// coordinator then added again under node 3, without a restart: a
/// learner's replica follows only its own primary, so it stops and opens
/// again as a learner of node 3. A newer configuration of the same
/// primary is adopted in place.
async fn rejoining() {
    let disk = SimDisk::new(43);
    let clock = Arc::new(MonotonicClock::new());
    let (log, _) = SegmentLog::open(disk.mount(), LogConfig::default(), clock as _)
        .await
        .unwrap();
    let index =
        Arc::new(Index::open_sim(&disk.mount(), "index.redb", &IndexConfig::default()).unwrap());
    let pool = BlockingPool::new("index", NonZeroUsize::MIN).unwrap();
    let set = ShardSet::new(index, log, pool);
    let learner = ShardConfig {
        epoch: Epoch::new(3),
        members: vec![node(1), node(3)],
        learners: vec![node(2)],
        ..initial()
    };
    let first = set.open_replica(&learner, &node(2)).await.unwrap();
    assert_eq!(first.role(), Role::Learner);

    let taken_over = ShardConfig {
        epoch: Epoch::new(5),
        primary: node(3),
        members: vec![node(3)],
        learners: vec![node(2)],
        ..initial()
    };
    let again = set.open_replica(&taken_over, &node(2)).await.unwrap();
    assert!(first.is_stopped());
    assert_eq!(again.role(), Role::Learner);
    assert_eq!(again.config(), taken_over);

    let another = ShardConfig {
        epoch: Epoch::new(6),
        learners: vec![node(2), node(4)],
        ..taken_over
    };
    let same = set.open_replica(&another, &node(2)).await.unwrap();
    assert!(!again.is_stopped());
    assert!(same.durable().same_channel(&again.durable()));

    // Dropped, and added again under the same primary once node 4, which
    // the replica never knew as a learner, became a member: it opens again.
    let grown = ShardConfig {
        epoch: Epoch::new(9),
        members: vec![node(3), node(4), node(5)],
        learners: vec![node(2)],
        ..another
    };
    let reopened = set.open_replica(&grown, &node(2)).await.unwrap();
    assert!(same.is_stopped());
    assert_eq!(reopened.role(), Role::Learner);
    assert_eq!(reopened.config(), grown);
}
