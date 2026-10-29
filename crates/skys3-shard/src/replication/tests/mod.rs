//! A member's end of the protocol over real TCP, driven by a hand-written
//! primary that also breaks the rules, and a primary's link to a
//! hand-written member whose log stalls.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType,
};
use skys3_index::{Index, IndexConfig};
use skys3_io::{BlockingPool, MonotonicClock, SimDisk, SimMount};
use skys3_log::record::Delete;
use skys3_log::{LogConfig, LogRecord, RecordBody, SegmentLog, ShardRef};
use skys3_net::{
    CertificateDer, Connection, Credentials, Frame, MessageKind, PeerIdentity, PrivateKeyDer,
    TokioNetwork, Transport,
};
use skys3_types::{
    BucketId, ClusterId, Epoch, EpochSeq, NodeId, ProposalId, RegisterDocument, Seq, ShardConfig,
    ShardId,
};
use tokio::net::TcpStream;

mod handoff;
mod learners;
mod removal;
mod takeover;

use super::wire::{self, Append, AppendAck, Beacon, StepDown, Sync, SyncAck};
use super::{Replication, ReplicationConfig};
use crate::lineage::Lineage;
use crate::set::ShardSet;

/// Every wait in these tests.
const WAIT: Duration = Duration::from_secs(30);

struct Pki {
    cluster: ClusterId,
    issuer: Issuer<'static, KeyPair>,
    ca: CertificateDer<'static>,
}

impl Pki {
    fn new() -> Self {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
        let ca = params.self_signed(&key).unwrap().der().clone();
        Self {
            cluster: ClusterId::new("test").unwrap(),
            issuer: Issuer::new(params, key),
            ca,
        }
    }

    fn transport(&self, node: &NodeId) -> Transport<TokioNetwork> {
        let identity = PeerIdentity::Node(node.clone()).spiffe_id(&self.cluster);
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.subject_alt_names = vec![SanType::URI(identity.as_str().try_into().unwrap())];
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ClientAuth,
            ExtendedKeyUsagePurpose::ServerAuth,
        ];
        let cert = params.signed_by(&key, &self.issuer).unwrap().der().clone();
        let key = PrivateKeyDer::try_from(key.serialize_der()).unwrap();
        let credentials = Credentials::new(
            self.cluster.clone(),
            vec![cert],
            key,
            std::slice::from_ref(&self.ca),
        )
        .unwrap();
        Transport::new(TokioNetwork, &credentials)
    }
}

fn node(n: u8) -> NodeId {
    format!("node-{n}").parse().unwrap()
}

fn shard() -> ShardRef {
    ShardRef::new(BucketId::new("b-1").unwrap(), ShardId::new(0))
}

/// Shard `b-1/0` in epoch 2, with node 1 its primary and node 2 a member.
fn config() -> ShardConfig {
    ShardConfig {
        bucket_id: BucketId::new("b-1").unwrap(),
        shard: ShardId::new(0),
        epoch: Epoch::new(2),
        primary: node(1),
        members: vec![node(1), node(2)],
        learners: Vec::new(),
        min_write_replicas: 1,
        replicas: 2,
        proposal_id: ProposalId::new("p").unwrap(),
    }
}

/// A node's shard set on a disk of its own, and its clock.
async fn shard_set(seed: u64) -> (ShardSet<SimMount>, Arc<MonotonicClock>) {
    let disk = SimDisk::new(seed);
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
    let index = Index::open_sim(&disk.mount(), "index.redb", &IndexConfig::default()).unwrap();
    let pool = BlockingPool::new("index", NonZeroUsize::MIN).unwrap();
    (ShardSet::new(Arc::new(index), log, pool), clock)
}

/// Node 2, serving links on a loopback port, with the shard open as a
/// member.
async fn member(pki: &Pki) -> (SocketAddr, Replication<TokioNetwork, SimMount>) {
    let (set, clock) = shard_set(9).await;
    let transport = pki.transport(&node(2));
    let replication = Replication::new(
        node(2),
        set,
        transport.clone(),
        BTreeMap::new(),
        clock,
        ReplicationConfig::default(),
    );
    assert!(replication.grace(&shard()).is_none());
    replication.open(&config()).await.unwrap();
    let listener = transport
        .bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let serving = replication.clone();
    tokio::spawn(async move { serving.serve(listener).await });
    (address, replication)
}

type Link = Connection<TcpStream>;

async fn connect(pki: &Pki, from: u8, to: SocketAddr) -> Link {
    let address = to.to_string().parse().unwrap();
    pki.transport(&node(from))
        .connect(&node(2), &address)
        .await
        .unwrap()
}

async fn next(link: &mut Link) -> Option<Frame> {
    tokio::time::timeout(WAIT, link.recv())
        .await
        .expect("an answer in time")
        .ok()
        .flatten()
}

/// Sends a `Sync` of `config` and returns the answers up to the last.
async fn sync(link: &mut Link, config: &ShardConfig, primary_last: u64) -> Vec<(SyncAck, Bytes)> {
    // A primary that holds records of its own epoch up to `primary_last`.
    let lineage = Lineage::new(EpochSeq::new(config.epoch, Seq::new(primary_last)));
    let request = Sync {
        config: config.to_json().unwrap(),
        primary_last,
        sequencing: config.epoch.get(),
        ..Sync::default()
    }
    .with_lineage(&lineage);
    link.send(&wire::frame(MessageKind::Sync, &request, Bytes::new()))
        .await
        .unwrap();
    let mut answers = Vec::new();
    loop {
        let frame = next(link).await.expect("a SyncAck");
        let answer: SyncAck = wire::body(&frame, MessageKind::SyncAck).unwrap();
        let done = answer.done || !answer.refused.is_empty();
        answers.push((answer, frame.payload));
        if done {
            return answers;
        }
    }
}

fn refusal(answers: &[(SyncAck, Bytes)]) -> String {
    answers.last().unwrap().0.refused.clone()
}

fn record(seq: u64, key: &str) -> Bytes {
    let record = LogRecord {
        shard: shard(),
        position: EpochSeq::new(Epoch::new(2), Seq::new(seq)),
        body: RecordBody::Delete(Delete { key: key.into() }),
    };
    record.to_bytes().unwrap()
}

async fn append(link: &mut Link, epoch: u64, seq: u64) {
    let body = Append {
        epoch,
        commit: 0,
        lazy: false,
        lease: None,
    };
    // A peer that already closed the link refuses the frame; the next
    // receive tells.
    let _ = link
        .send(&wire::frame(MessageKind::Append, &body, record(seq, "k")))
        .await;
}

/// The next acknowledgement, if the link is still open.
async fn ack(link: &mut Link) -> Option<AppendAck> {
    let frame = next(link).await?;
    Some(wire::body(&frame, MessageKind::AppendAck).unwrap())
}

/// Acknowledgements until one covers `seq`.
async fn durable_through(link: &mut Link, seq: u64) {
    while ack(link).await.expect("the link is open").durable < seq {}
}

#[test]
fn a_member_follows_only_its_primary() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        tokio::time::timeout(WAIT * 4, scenario())
            .await
            .expect("the test finished in time");
    });
}

async fn scenario() {
    let pki = Pki::new();
    let (address, replication) = member(&pki).await;
    let config = config();
    // Opening the shard as a member started its grace.
    let grace = replication.grace(&shard()).expect("the member's grace");
    let mut grants = grace.subscribe();
    grants.borrow_and_update();

    // Refusals: a node that is not the primary it names, a shard not
    // open here, an older epoch (R2), and another configuration.
    let mut link = connect(&pki, 3, address).await;
    assert!(refusal(&sync(&mut link, &config, 0).await).contains("not the primary"));
    let mut link = connect(&pki, 1, address).await;
    let other = ShardConfig {
        shard: ShardId::new(1),
        ..config.clone()
    };
    assert!(refusal(&sync(&mut link, &other, 0).await).contains("not open"));
    let mut link = connect(&pki, 1, address).await;
    let older = ShardConfig {
        epoch: Epoch::new(1),
        ..config.clone()
    };
    let answers = sync(&mut link, &older, 0).await;
    assert!(refusal(&answers).contains("older"));
    assert_eq!(answers[0].0.epoch, 2);
    let mut link = connect(&pki, 1, address).await;
    let changed = ShardConfig {
        replicas: 3,
        ..config.clone()
    };
    assert!(refusal(&sync(&mut link, &changed, 0).await).contains("differs"));

    // A session: appends in order are acknowledged once durable, and
    // so is every beacon.
    let mut first = connect(&pki, 1, address).await;
    let answers = sync(&mut first, &config, 0).await;
    assert_eq!(answers.len(), 1);
    assert_eq!((answers[0].0.last, answers[0].0.epoch), (0, 2));
    append(&mut first, 2, 1).await;
    durable_through(&mut first, 1).await;
    // No stamp came yet, so no acknowledgement granted a lease.
    assert!(!grants.has_changed().unwrap());
    let beacon = Beacon {
        epoch: 2,
        commit: 1,
        lease: Some(7_000),
    };
    first
        .send(&wire::frame(MessageKind::Beacon, &beacon, Bytes::new()))
        .await
        .unwrap();
    // The answer echoes the stamp, which grants a lease and restarts the
    // member's grace.
    let answer = ack(&mut first).await.unwrap();
    assert_eq!((answer.durable, answer.lease), (1, Some(7_000)));
    assert!(grants.has_changed().unwrap());
    assert!(!grace.has_passed());

    // A later session ends the earlier one, and gets back the record
    // its primary does not hold.
    let mut second = connect(&pki, 1, address).await;
    let answers = sync(&mut second, &config, 0).await;
    assert_eq!(answers.len(), 2);
    assert_eq!(answers[0].1, record(1, "k"));
    assert_eq!(answers[1].0.last, 1);
    append(&mut first, 2, 2).await;
    while ack(&mut first).await.is_some() {}
    // An append of an older epoch is refused with the member's.
    append(&mut second, 1, 2).await;
    let mut rejected = 0;
    while let Some(ack) = ack(&mut second).await {
        rejected = ack.rejected;
    }
    assert_eq!(rejected, 2);

    // A gap ends the session; the member still holds only seq 1.
    let mut third = connect(&pki, 1, address).await;
    let answers = sync(&mut third, &config, 1).await;
    assert_eq!(answers.last().unwrap().0.last, 1);
    append(&mut third, 2, 5).await;
    while ack(&mut third).await.is_some() {}
    let mut fourth = connect(&pki, 1, address).await;
    assert_eq!(
        sync(&mut fourth, &config, 1).await.last().unwrap().0.last,
        1
    );
}

#[test]
fn a_step_down_counts_once_the_member_holds_its_records() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        tokio::time::timeout(WAIT, step_down())
            .await
            .expect("the test finished in time");
    });
}

async fn step_down() {
    let pki = Pki::new();
    let (address, replication) = member(&pki).await;
    let grace = replication.grace(&shard()).unwrap();
    let send = |epoch, last| {
        let body = StepDown { epoch, last };
        wire::frame(MessageKind::StepDown, &body, Bytes::new())
    };
    let mut link = connect(&pki, 1, address).await;
    sync(&mut link, &config(), 0).await;
    append(&mut link, 2, 1).await;
    durable_through(&mut link, 1).await;
    // A step-down past what the member holds does not count: the member
    // waits for its grace instead. It waits a link timeout for the
    // records, and the next frame must come within another.
    link.send(&send(2, 5)).await.unwrap();
    tokio::time::sleep(ReplicationConfig::default().link_timeout * 5 / 4).await;
    assert_eq!(grace.stepped_down(), None);
    // One it holds every record of counts for its epoch.
    link.send(&send(2, 1)).await.unwrap();
    grace.stepped_down_since(Epoch::new(2)).await;
    assert_eq!(grace.stepped_down(), Some(Epoch::new(2)));
    // One of an older epoch is refused with the member's (rule R2).
    link.send(&send(1, 1)).await.unwrap();
    let mut rejected = 0;
    while let Some(ack) = ack(&mut link).await {
        rejected = ack.rejected;
    }
    assert_eq!(rejected, 2);
}

#[test]
fn beacons_come_well_within_the_link_timeout() {
    let config = ReplicationConfig::default();
    assert_eq!(config.beacon_every(true), config.beacon_interval);
    // The renewal interval equals the link timeout by default: a busy link
    // beacons four times as often.
    assert_eq!(config.lease_renew_interval, config.link_timeout);
    assert_eq!(config.beacon_every(false), config.link_timeout / 4);
    let slow = ReplicationConfig {
        beacon_interval: Duration::from_secs(5),
        lease_renew_interval: Duration::from_millis(100),
        ..config
    };
    assert_eq!(slow.beacon_every(true), slow.link_timeout / 4);
    assert_eq!(slow.beacon_every(false), Duration::from_millis(100));
}

/// A member that takes a session, answers every beacon, and acknowledges
/// no record: its log is stuck behind a slow sync. Counts its sessions.
async fn stalled_member(pki: &Pki, sessions: Arc<AtomicUsize>) -> SocketAddr {
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
            sessions.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let answer = SyncAck {
                    last: 0,
                    done: true,
                    epoch: 2,
                    refused: String::new(),
                };
                if next(&mut link).await.is_none() {
                    return;
                }
                let frame = wire::frame(MessageKind::SyncAck, &answer, Bytes::new());
                if link.send(&frame).await.is_err() {
                    return;
                }
                while let Some(frame) = next(&mut link).await {
                    let Ok(beacon) = wire::body::<Beacon>(&frame, MessageKind::Beacon) else {
                        // An append: never durable while the log is stuck.
                        continue;
                    };
                    let ack = AppendAck {
                        durable: 0,
                        rejected: 0,
                        lease: beacon.lease,
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

/// A busy primary beacons on its renewal interval, which equals the link
/// timeout by default. While its member's log acknowledges nothing, the
/// answers to those beacons were all the link heard, one per timeout, so
/// the link timed out and reconnected over and over. Beacons now come at
/// least every quarter of the timeout: through a stall of three timeouts,
/// the link stays up and the primary keeps its lease.
#[test]
fn a_busy_link_whose_member_stalls_stays_up_and_keeps_its_lease() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        tokio::time::timeout(WAIT, stalled_link())
            .await
            .expect("the test finished in time");
    });
}

async fn stalled_link() {
    let pki = Pki::new();
    let sessions = Arc::new(AtomicUsize::new(0));
    let address = stalled_member(&pki, Arc::clone(&sessions)).await;
    // Idle beacons on the renewal interval too, so the primary beacons as
    // a busy one does whether or not records flow.
    let timing = ReplicationConfig {
        beacon_interval: ReplicationConfig::default().lease_renew_interval,
        ..ReplicationConfig::default()
    };
    let (set, clock) = shard_set(11).await;
    let peers = BTreeMap::from([(node(2), address.to_string().parse().unwrap())]);
    let primary = Replication::new(node(1), set, pki.transport(&node(1)), peers, clock, timing);
    let shard = primary.open(&config()).await.unwrap();
    let leader = Arc::clone(shard.leader().unwrap());
    while !leader.holds_leases() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let until = tokio::time::Instant::now() + timing.link_timeout * 3;
    while tokio::time::Instant::now() < until {
        assert!(leader.holds_leases(), "the primary lost its lease");
        assert_eq!(sessions.load(Ordering::SeqCst), 1, "the link reconnected");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
