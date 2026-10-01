//! Member removal over real TCP (§6.4): a primary and two members, each
//! member reached through a proxy the test can cut, with the shard's
//! register in an in-memory control store.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use skys3_control::{
    Expected, MemoryControlStore, ProposalIds, RetryPolicy, TypedKey, propose_document, read,
};
use skys3_io::SimMount;
use skys3_log::RecordBody;
use skys3_log::record::{Put, PutData};
use skys3_net::TokioNetwork;
use skys3_obs::MetricsRegistry;
use skys3_types::{BucketId, ETag, Epoch, ProposalId, ShardConfig, ShardId};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::{Pki, WAIT, node, shard, shard_set};
use crate::ack::AckTimeout;
use crate::error::ShardError;
use crate::replication::{
    BoxFuture, ControlRegisters, Replaced, Replication, ReplicationConfig, ReplicationMetrics,
    ShardRegisters,
};
use crate::shard::Shard;

fn ms(ms: u64) -> Duration {
    Duration::from_millis(ms)
}

/// Fast links, and members suspected after 300 ms. Requests wait through
/// a removal.
fn timing() -> ReplicationConfig {
    ReplicationConfig {
        beacon_interval: ms(20),
        link_timeout: ms(200),
        reconnect_delay: ms(20),
        lease_renew_interval: ms(50),
        primary_lease: ms(400),
        primary_grace: ms(600),
        ack_timeout: AckTimeout::wait_through(ms(3000)),
        member_suspect_after: ms(300),
    }
}

/// Shard `b-1/0` on nodes 1 to 3, node 1 its primary; writes need two
/// copies.
fn initial() -> ShardConfig {
    ShardConfig {
        bucket_id: BucketId::new("b-1").unwrap(),
        shard: ShardId::new(0),
        epoch: Epoch::new(1),
        primary: node(1),
        members: vec![node(1), node(2), node(3)],
        learners: Vec::new(),
        min_write_replicas: 2,
        replicas: 3,
        proposal_id: ProposalId::new("p-1").unwrap(),
    }
}

fn put(key: &str, len: usize) -> RecordBody {
    RecordBody::Put(Put {
        key: key.to_owned(),
        size: len as u64,
        last_modified_ms: 1_700_000_000_000,
        etag: ETag::new(format!("{len:032x}")).unwrap(),
        inherited_identity: None,
        metadata: BTreeMap::new(),
        tags: BTreeMap::new(),
        checksums: BTreeMap::new(),
        copy_source: None,
        data: PutData::Inline(Bytes::from(vec![7; len])),
    })
}

/// A TCP proxy to one member, which the test can cut: it then drops every
/// connection, and every new one at once.
struct Proxy {
    address: SocketAddr,
    cut: Arc<AtomicBool>,
    connections: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl Proxy {
    async fn to(target: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cut = Arc::new(AtomicBool::new(false));
        let connections: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::default();
        let (accepting, tasks) = (Arc::clone(&cut), Arc::clone(&connections));
        tokio::spawn(async move {
            while let Ok((mut inbound, _)) = listener.accept().await {
                if accepting.load(Ordering::SeqCst) {
                    continue;
                }
                let task = tokio::spawn(async move {
                    if let Ok(mut outbound) = TcpStream::connect(target).await {
                        let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                    }
                });
                tasks
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(task);
            }
        });
        Self {
            address,
            cut,
            connections,
        }
    }

    fn cut(&self) {
        self.cut.store(true, Ordering::SeqCst);
        let connections = self.connections.lock();
        for task in connections
            .unwrap_or_else(PoisonError::into_inner)
            .drain(..)
        {
            task.abort();
        }
    }
}

/// Node `n` serving links on a loopback port, with the shard open as a
/// member.
async fn member(pki: &Pki, n: u8) -> (SocketAddr, Replication<TokioNetwork, SimMount>) {
    let (set, clock) = shard_set(20 + u64::from(n)).await;
    let transport = pki.transport(&node(n));
    let replication = Replication::new(
        node(n),
        set,
        transport.clone(),
        BTreeMap::new(),
        clock,
        timing(),
    );
    replication.open(&initial()).await.unwrap();
    let listener = transport
        .bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let serving = replication.clone();
    tokio::spawn(async move { serving.serve(listener).await });
    (address, replication)
}

/// Waits until `done` holds.
async fn until(mut done: impl FnMut() -> bool) {
    while !done() {
        tokio::time::sleep(ms(5)).await;
    }
}

/// The shard's register.
async fn register(store: &MemoryControlStore) -> ShardConfig {
    let key = TypedKey::shard(&BucketId::new("b-1").unwrap(), ShardId::new(0));
    read(store, &key).await.unwrap().unwrap().value
}

/// Commits a `PUT` and returns how long it took.
async fn timed_put(primary: &Shard<SimMount>, key: &str) -> (Result<(), ShardError>, Duration) {
    let started = Instant::now();
    let committed = primary.commit(put(key, 100)).await.map(drop);
    (committed, started.elapsed())
}

#[test]
fn a_primary_removes_a_member_that_stops_responding() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        tokio::time::timeout(WAIT, removal())
            .await
            .expect("the test finished in time");
    });
}

async fn removal() {
    let pki = Pki::new();
    let (address2, member2) = member(&pki, 2).await;
    let (address3, _member3) = member(&pki, 3).await;
    let (proxy2, proxy3) = (Proxy::to(address2).await, Proxy::to(address3).await);
    let store = MemoryControlStore::new();
    let key = TypedKey::shard(&BucketId::new("b-1").unwrap(), ShardId::new(0));
    let policy = RetryPolicy::default();
    let created = propose_document(&store, &key, Expected::Absent, &initial(), &policy).await;
    assert!(created.is_ok());

    let (set, clock) = shard_set(31).await;
    let peers = BTreeMap::from([
        (node(2), proxy2.address.to_string().parse().unwrap()),
        (node(3), proxy3.address.to_string().parse().unwrap()),
    ]);
    let replication = Replication::new(
        node(1),
        set,
        pki.transport(&node(1)),
        peers,
        clock,
        timing(),
    )
    .with_removal(
        ControlRegisters::new(store.clone(), policy),
        ProposalIds::seeded(7),
    );
    let primary = replication.open(&initial()).await.unwrap();
    let leader = Arc::clone(primary.leader().unwrap());
    until(|| primary.is_serving() && leader.holds_leases()).await;
    let (written, _) = timed_put(&primary, "a").await;
    written.unwrap();
    assert_eq!(replication.exposure().await.shards, 0);

    // Node 3 stops responding: a write in flight waits about
    // member_suspect_after plus the removal, then commits without error.
    proxy3.cut();
    let (written, stalled) = timed_put(&primary, "b").await;
    written.unwrap();
    assert!(stalled >= ms(200), "the write stalled only {stalled:?}");
    assert!(stalled < ms(1500), "the write stalled {stalled:?}");
    let removed = register(&store).await;
    assert_eq!(
        (removed.epoch, removed.members.clone()),
        (Epoch::new(2), vec![node(1), node(2)])
    );
    assert_eq!(primary.config(), removed);
    assert_eq!(leader.members(), [node(2)]);
    // Node 2 follows into the new epoch, and the primary reads under its
    // lease alone.
    let replica2 = member2.set().get(&shard()).await.unwrap();
    until(|| replica2.sequencing() == Epoch::new(2) && leader.holds_leases()).await;
    assert!(primary.entry("b").await.unwrap().is_some());
    // Later writes are not held up.
    let (written, took) = timed_put(&primary, "c").await;
    written.unwrap();
    assert!(took < ms(200), "the write took {took:?}");

    // Everything the shard holds now has two copies, not three.
    let exposure = replication.exposure().await;
    assert_eq!((exposure.shards, exposure.bytes), (1, 300));
    let registry = MetricsRegistry::new();
    let metrics = ReplicationMetrics::register(&registry);
    tokio::select! {
        () = replication.report_exposure(&metrics, ms(10)) => unreachable!(),
        () = tokio::time::sleep(ms(30)) => {}
    }
    let text = registry.encode().unwrap();
    assert!(
        text.contains("skys3_under_replicated_bytes 300\n"),
        "{text}"
    );

    // Node 2 stops responding too. Removing it leaves fewer copies than
    // min_write_replicas: the write in flight is applied but not
    // acknowledged, and later ones are refused, while reads go on.
    proxy2.cut();
    let (written, _) = timed_put(&primary, "d").await;
    assert!(
        matches!(written, Err(ShardError::UnderReplicated { copies: 1, .. })),
        "{written:?}"
    );
    assert_eq!(register(&store).await.members, [node(1)]);
    let (refused, _) = timed_put(&primary, "e").await;
    assert!(matches!(refused, Err(ShardError::UnderReplicated { .. })));
    assert!(primary.entry("d").await.unwrap().is_some());
    assert!(primary.entry("e").await.unwrap().is_none());
    assert_eq!(replication.exposure().await.bytes, 400);
}

/// A register that keeps another configuration: the primary adopts it if
/// it only removes members, and is deposed if it names another primary.
#[derive(Debug)]
struct Taken(ShardConfig);

impl ShardRegisters for Taken {
    fn replace<'a>(
        &'a self,
        _: &'a ShardConfig,
        _: &'a ShardConfig,
    ) -> BoxFuture<'a, Result<Replaced, skys3_control::ControlError>> {
        let held = self.0.clone();
        Box::pin(async move { Ok(Replaced::Holds(Some(held))) })
    }
}

#[test]
fn a_primary_follows_or_yields_to_the_register() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        tokio::time::timeout(WAIT, yields())
            .await
            .expect("the test finished in time");
    });
}

/// Opens a primary of `initial()` whose members never answer, removing
/// them through `registers`.
async fn lonely_primary(
    pki: &Pki,
    registers: impl ShardRegisters,
) -> (Replication<TokioNetwork, SimMount>, Shard<SimMount>) {
    let (set, clock) = shard_set(41).await;
    // Nothing listens on the members' addresses.
    let unused = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = unused.local_addr().unwrap().to_string();
    drop(unused);
    let peers = BTreeMap::from([
        (node(2), address.parse().unwrap()),
        (node(3), address.parse().unwrap()),
    ]);
    let replication = Replication::new(
        node(1),
        set,
        pki.transport(&node(1)),
        peers,
        clock,
        timing(),
    )
    .with_removal(registers, ProposalIds::seeded(3));
    let primary = replication.open(&initial()).await.unwrap();
    (replication, primary)
}

async fn yields() {
    let pki = Pki::new();
    // The coordinator removed node 3 already: the primary adopts that.
    let coordinated = ShardConfig {
        epoch: Epoch::new(2),
        members: vec![node(1), node(2)],
        ..initial()
    };
    let (_, primary) = lonely_primary(&pki, Taken(coordinated.clone())).await;
    until(|| primary.config() == coordinated).await;
    assert!(!primary.is_stopped());

    // Another node took over: the primary stops.
    let taken = ShardConfig {
        epoch: Epoch::new(2),
        primary: node(2),
        ..initial()
    };
    let (_, primary) = lonely_primary(&pki, Taken(taken)).await;
    until(|| primary.is_stopped()).await;
    assert!(matches!(
        primary.commit(put("a", 1)).await,
        Err(ShardError::Unavailable { .. })
    ));
}
