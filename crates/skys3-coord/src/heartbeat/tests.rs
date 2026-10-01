use std::net::SocketAddr;

use proptest::prelude::*;
use skys3_control::Version;
use skys3_control::{
    Expected, MemoryControlStore, ProposalOutcome, bootstrap, propose_delete, propose_document,
};
use skys3_io::MonotonicClock;
use skys3_net::TokioNetwork;
use skys3_types::{CoordinatorLease, DiskInfo, Label};

use super::*;
use crate::admin::AdminEndpoint;
use crate::lease::Leadership;
use crate::push::{ControlHints, PushError};
use crate::registry::{NodeState, RegistryConfig};
use crate::testing::Pki;

/// Every wait in these tests.
const WAIT: Duration = Duration::from_secs(30);

fn node(n: u8) -> NodeId {
    NodeId::new(format!("node-{n}")).unwrap()
}

fn address(addr: SocketAddr) -> NodeAddress {
    addr.to_string().parse().unwrap()
}

fn retry() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 3,
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(10),
    }
}

fn config() -> HeartbeatConfig {
    HeartbeatConfig {
        interval: Duration::from_millis(20),
        timeout: Duration::from_secs(5),
        resolve_interval: Duration::from_millis(50),
        retry: retry(),
    }
}

fn clock() -> Arc<dyn Clock> {
    Arc::new(MonotonicClock::new())
}

fn profile(n: u8, address: NodeAddress) -> NodeProfile {
    NodeProfile {
        node: node(n),
        address,
        zone: Some(Label::new("zone-a").unwrap()),
        rack: None,
        disks: vec![DiskInfo {
            disk_id: Label::new("disk-0").unwrap(),
            capacity_bytes: 1 << 30,
        }],
    }
}

/// A bootstrapped store whose coordinator lease names `holder`.
async fn store(pki: &Pki, holder: u8) -> MemoryControlStore {
    let store = MemoryControlStore::new();
    let mut ids = ProposalIds::seeded(1);
    bootstrap(&store, &pki.cluster, ids.next_id(), &retry())
        .await
        .unwrap();
    let lease = CoordinatorLease {
        holder: node(holder),
        proposal_id: ids.next_id(),
    };
    let outcome = propose_document(
        &store,
        &TypedKey::coordinator_lease(),
        Expected::Absent,
        &lease,
        &retry(),
    )
    .await
    .unwrap();
    assert!(matches!(outcome, ProposalOutcome::Accepted(_)));
    store
}

/// Node `n` as coordinator candidate: registered, and answering
/// heartbeats on a loopback port with its registry, as `leadership` says.
struct Coordinator {
    registry: NodeRegistry,
    leader: watch::Sender<Leadership>,
}

async fn coordinator(pki: &Pki, store: &MemoryControlStore, n: u8) -> Coordinator {
    let transport = pki.transport(&node(n));
    let listener = transport
        .bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr = address(listener.local_addr().unwrap());
    crate::join::register(
        store,
        &pki.cluster,
        &profile(n, addr),
        &mut ProposalIds::seeded(u64::from(n)),
        &retry(),
    )
    .await
    .unwrap();
    let registry = NodeRegistry::new(clock(), RegistryConfig::new(Duration::from_secs(60)));
    let (leader, leadership) = watch::channel(Leadership::Coordinator {
        version: Version::new("held"),
        until: MonoTime::MAX,
    });
    let endpoint = AdminEndpoint::new(ControlHints::new()).with_heartbeats(
        registry.clone(),
        leadership,
        clock(),
    );
    tokio::spawn(async move { endpoint.serve(listener).await });
    Coordinator { registry, leader }
}

fn heartbeater(
    pki: &Pki,
    store: &MemoryControlStore,
    n: u8,
) -> Heartbeater<MemoryControlStore, TokioNetwork> {
    Heartbeater::new(
        store.clone(),
        pki.transport(&node(n)),
        pki.cluster.clone(),
        profile(
            n,
            format!("127.0.0.1:{}", 7000 + u16::from(n))
                .parse()
                .unwrap(),
        ),
        clock(),
        config(),
        ProposalIds::seeded(u64::from(n) + 100),
    )
}

/// Waits until `status` satisfies `done`.
async fn until(
    status: &mut watch::Receiver<HeartbeatStatus>,
    done: impl FnMut(&HeartbeatStatus) -> bool,
) -> HeartbeatStatus {
    tokio::time::timeout(WAIT, status.wait_for(done))
        .await
        .expect("the heartbeater did not get there in time")
        .unwrap()
        .clone()
}

#[test]
fn heartbeat_frames_round_trip_and_bad_ones_are_refused() {
    let frame = Heartbeat::frame(4);
    assert_eq!(frame.header.kind, MessageKind::NodeHeartbeat);
    assert_eq!(Heartbeat::from_frame(&frame), Ok(Heartbeat {}));
    let ack = HeartbeatAck {
        coordinator: true,
        unregistered: true,
    };
    let answer = ack.frame(4);
    assert_eq!(answer.header.kind, MessageKind::AdminReply);
    assert_eq!(HeartbeatAck::from_frame(&answer, 4), Ok(ack));

    assert_eq!(
        HeartbeatAck::from_frame(&answer, 5),
        Err(HeartbeatError::RequestId {
            expected: 5,
            found: 4
        })
    );
    assert_eq!(
        Heartbeat::from_frame(&answer),
        Err(HeartbeatError::Kind {
            expected: MessageKind::NodeHeartbeat,
            found: MessageKind::AdminReply
        })
    );
    let loaded = Frame::new(frame.header.clone(), Bytes::from_static(b"x"));
    assert_eq!(
        Heartbeat::from_frame(&loaded),
        Err(HeartbeatError::Payload(1))
    );
    let malformed = Frame::new(
        Header::new(MessageKind::AdminReply).with_body(vec![0x08]),
        Bytes::new(),
    );
    assert!(matches!(
        HeartbeatAck::from_frame(&malformed, 0),
        Err(HeartbeatError::Malformed(_))
    ));
}

proptest! {
    #[test]
    fn decoding_any_body_never_panics_and_answers_round_trip(
        body in proptest::collection::vec(any::<u8>(), 0..64),
        request in any::<u64>(),
    ) {
        let heartbeat = Frame::new(
            Header::new(MessageKind::NodeHeartbeat).with_body(body.clone()),
            Bytes::new(),
        );
        let _ = Heartbeat::from_frame(&heartbeat);
        let answer = Frame::new(
            Header::new(MessageKind::AdminReply)
                .with_request_id(request)
                .with_body(body),
            Bytes::new(),
        );
        if let Ok(ack) = HeartbeatAck::from_frame(&answer, request) {
            prop_assert_eq!(HeartbeatAck::from_frame(&ack.frame(request), request), Ok(ack));
        }
    }
}

#[tokio::test]
async fn a_node_registers_and_heartbeats_to_the_coordinator() {
    let pki = Pki::new();
    let store = store(&pki, 1).await;
    let coordinator = coordinator(&pki, &store, 1).await;
    let heartbeater = heartbeater(&pki, &store, 2);
    let mut status = heartbeater.subscribe();
    let task = tokio::spawn(heartbeater.run());

    let reached = until(&mut status, |status| status.delivered >= 3).await;
    assert_eq!(reached.coordinator, Some(node(1)));
    assert_eq!(reached.registrations, 1);
    let registration = read(&store, &TypedKey::node(&node(2)))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(registration.value.zone, Some(Label::new("zone-a").unwrap()));

    // The coordinator lists it and hears from it.
    coordinator.registry.refresh(&store).await.unwrap();
    let delivered = status.borrow().delivered;
    until(&mut status, |status| status.delivered > delivered).await;
    let entry = coordinator.registry.get(&node(2)).unwrap();
    assert_eq!(entry.state, NodeState::Live);

    // Forgotten while it runs, the node registers again once told.
    let deleted = propose_delete(
        &store,
        TypedKey::node(&node(2)).key(),
        &registration.version,
        &retry(),
    )
    .await
    .unwrap();
    assert_eq!(deleted, skys3_control::DeletionOutcome::Deleted);
    coordinator.registry.refresh(&store).await.unwrap();
    until(&mut status, |status| status.registrations >= 2).await;
    assert!(
        read(&store, &TypedKey::node(&node(2)))
            .await
            .unwrap()
            .is_some()
    );
    task.abort();
}

#[tokio::test]
async fn a_node_follows_the_coordinator_when_the_lease_moves() {
    let pki = Pki::new();
    let store = store(&pki, 1).await;
    let old = coordinator(&pki, &store, 1).await;
    let new = coordinator(&pki, &store, 3).await;
    // Node 1 no longer coordinates, but the lease still names it.
    old.leader
        .send_replace(Leadership::Follower { holder: None });
    let heartbeater = heartbeater(&pki, &store, 2);
    let mut status = heartbeater.subscribe();
    let task = tokio::spawn(heartbeater.run());
    until(&mut status, |status| status.registrations == 1).await;

    // The lease moves to node 3, and the node follows it.
    let lease = read(&store, &TypedKey::coordinator_lease())
        .await
        .unwrap()
        .unwrap();
    let moved = CoordinatorLease {
        holder: node(3),
        proposal_id: ProposalIds::seeded(9).next_id(),
    };
    let _ = propose_document(
        &store,
        &TypedKey::coordinator_lease(),
        Expected::Version(lease.version),
        &moved,
        &retry(),
    )
    .await
    .unwrap();
    let reached = until(&mut status, |status| status.delivered >= 2).await;
    assert_eq!(reached.coordinator, Some(node(3)));
    assert!(old.registry.get(&node(2)).is_none());
    drop(new.leader);
    task.abort();
}

#[tokio::test]
async fn the_coordinator_records_its_own_heartbeats_directly() {
    let pki = Pki::new();
    let store = store(&pki, 4).await;
    let registry = NodeRegistry::new(clock(), RegistryConfig::new(Duration::from_secs(60)));
    // The lease names node 4, but its tenure has ended: it delivers
    // nothing to itself, and keeps looking the coordinator up.
    let (leader, leadership) = watch::channel(Leadership::Follower { holder: None });
    let heartbeater = heartbeater(&pki, &store, 4).with_local(registry.clone(), leadership);
    let mut status = heartbeater.subscribe();
    let task = tokio::spawn(heartbeater.run());
    until(&mut status, |status| status.registrations == 1).await;
    tokio::time::sleep(config().resolve_interval * 3).await;
    assert_eq!(status.borrow().delivered, 0);
    leader.send_replace(Leadership::Coordinator {
        version: Version::new("held"),
        until: MonoTime::MAX,
    });
    let reached = until(&mut status, |status| status.delivered >= 2).await;
    assert_eq!(reached.coordinator, Some(node(4)));
    registry.refresh(&store).await.unwrap();
    assert_eq!(registry.get(&node(4)).unwrap().state, NodeState::Live);
    task.abort();
}

#[tokio::test]
async fn heartbeats_to_an_unreachable_coordinator_fail_quietly() {
    let pki = Pki::new();
    let store = store(&pki, 5).await;
    // Node 5 is registered at a port nobody listens on, and node 6's admin
    // port override points there too.
    let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = address(closed.local_addr().unwrap());
    drop(closed);
    crate::join::register(
        &store,
        &pki.cluster,
        &profile(5, addr.clone()),
        &mut ProposalIds::seeded(5),
        &retry(),
    )
    .await
    .unwrap();
    let port = std::num::NonZeroU16::new(addr.port()).unwrap();
    let heartbeater = heartbeater(&pki, &store, 6).with_admin_port(port);
    assert!(format!("{heartbeater:?}").contains("node-6"));
    let mut status = heartbeater.subscribe();
    let task = tokio::spawn(heartbeater.run());
    until(&mut status, |status| status.registrations == 1).await;
    tokio::time::sleep(config().resolve_interval * 3).await;
    assert_eq!(status.borrow().delivered, 0);
    task.abort();
}

#[tokio::test]
async fn the_endpoint_answers_heartbeats_only_from_nodes() {
    let pki = Pki::new();
    let store = store(&pki, 1).await;
    let coordinator = coordinator(&pki, &store, 1).await;
    let addr = read(&store, &TypedKey::node(&node(1)))
        .await
        .unwrap()
        .unwrap()
        .value
        .address;

    // A node gets an answer, in order, on one connection.
    let transport = pki.transport(&node(7));
    let mut connection = tokio::time::timeout(WAIT, transport.connect(&node(1), &addr))
        .await
        .unwrap()
        .unwrap();
    for request in [3, 4] {
        connection.send(&Heartbeat::frame(request)).await.unwrap();
        let answer = tokio::time::timeout(WAIT, connection.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let ack = HeartbeatAck::from_frame(&answer, request).unwrap();
        assert!(ack.coordinator && !ack.unregistered);
    }
    // Once the coordinator has listed the nodes, it tells node 7, which
    // never registered.
    coordinator.registry.refresh(&store).await.unwrap();
    connection.send(&Heartbeat::frame(5)).await.unwrap();
    let answer = tokio::time::timeout(WAIT, connection.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(HeartbeatAck::from_frame(&answer, 5).unwrap().unregistered);
    // A malformed heartbeat ends the connection.
    connection
        .send(&Frame::new(
            Header::new(MessageKind::NodeHeartbeat),
            Bytes::from_static(b"payload"),
        ))
        .await
        .unwrap();
    let closed = tokio::time::timeout(WAIT, connection.recv()).await.unwrap();
    assert!(!matches!(closed, Ok(Some(_))), "{closed:?}");

    // An operator tool's certificate may send admin messages, but it is
    // not a node, so its heartbeat ends the connection.
    let tool = pki.tool("ops");
    let mut connection = tokio::time::timeout(WAIT, tool.connect(&node(1), &addr))
        .await
        .unwrap()
        .unwrap();
    connection.send(&Heartbeat::frame(1)).await.unwrap();
    let closed = tokio::time::timeout(WAIT, connection.recv()).await.unwrap();
    assert!(!matches!(closed, Ok(Some(_))), "{closed:?}");
    assert!(coordinator.registry.get(&node(7)).is_none());

    let error = PushError::NotANode(skys3_net::PeerIdentity::Admin(Label::new("ops").unwrap()));
    assert!(error.to_string().contains("not a node"), "{error}");
    assert!(
        PushError::Unexpected(MessageKind::Beacon)
            .to_string()
            .contains("Beacon")
    );
}
