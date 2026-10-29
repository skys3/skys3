//! Simulation scenarios for the intra-cluster transport over `turmoil`'s
//! network, run by CI's simulation job with a larger seed set.
//!
//! Three nodes serve the transport. A driver host, holding a node's
//! credentials, pipelines frames with seeded payload sizes to each of them
//! while links are delayed by seeded latencies and held, and checks every
//! acknowledgement. Around that traffic it checks, in simulated time, that:
//!
//! - a partitioned node fails within [`HANDSHAKE_TIMEOUT`], and a repaired
//!   link connects again; a host that never answers the handshake fails
//!   after it (every sixteenth seed);
//! - an operator tool's certificate reaches only admin messages;
//! - a certificate from another CA is refused.

#[path = "../common/mod.rs"]
mod common;

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use common::{Leaf, TestCa};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rustls::CertificateError;
use skys3_net::{
    Connection, Credentials, Frame, HANDSHAKE_TIMEOUT, Header, MessageKind, Role, Transport,
    TransportError, TurmoilNetwork,
};
use skys3_sim::{Runner, SimContext};
use skys3_types::{NodeAddress, NodeId};

const NODES: [&str; 3] = ["n1", "n2", "n3"];
const PORT: u16 = 7400;
const FRAMES_PER_NODE: usize = 16;
const MAX_PAYLOAD: usize = 64 * 1024;

type Error = Box<dyn std::error::Error>;

/// Credentials are generated once for all seeds: key generation is the
/// slow part, and the scenario does not depend on the keys.
struct Pki {
    nodes: Vec<Arc<Credentials>>,
    driver: Arc<Credentials>,
    admin: Arc<Credentials>,
    rogue: Arc<Credentials>,
}

fn pki() -> &'static Pki {
    static PKI: OnceLock<Pki> = OnceLock::new();
    PKI.get_or_init(|| {
        let ca = TestCa::new("cluster ca");
        let rogue = TestCa::new("rogue ca");
        Pki {
            nodes: NODES
                .iter()
                .map(|name| Arc::new(ca.credentials(&Leaf::node(name))))
                .collect(),
            driver: Arc::new(ca.credentials(&Leaf::node("n0"))),
            admin: Arc::new(ca.credentials(&Leaf::admin("ops"))),
            rogue: Arc::new(rogue.credentials(&Leaf::node("n0"))),
        }
    })
}

fn address(host: &str) -> NodeAddress {
    format!("{host}:{PORT}").parse().unwrap()
}

fn node_id(name: &str) -> NodeId {
    NodeId::new(name).unwrap()
}

/// The acknowledgement body for a payload: its length and byte sum.
fn digest(payload: &[u8]) -> Bytes {
    let sum = payload
        .iter()
        .fold(0u64, |sum, byte| sum.wrapping_add(u64::from(*byte)));
    let mut body = (payload.len() as u64).to_be_bytes().to_vec();
    body.extend_from_slice(&sum.to_be_bytes());
    Bytes::from(body)
}

/// A node: accepts connections and answers each request with its reply
/// kind, carrying the request's digest.
async fn serve(credentials: Arc<Credentials>) -> turmoil::Result {
    let transport = Transport::new(TurmoilNetwork, &credentials);
    let listener = transport
        .bind((std::net::Ipv4Addr::UNSPECIFIED, PORT).into())
        .await?;
    loop {
        let incoming = listener.accept().await?;
        tokio::spawn(async move {
            // Refused handshakes are the peers' problem; they check them.
            if let Ok(connection) = incoming.handshake().await {
                let _ = answer(connection).await;
            }
        });
    }
}

async fn answer(mut connection: Connection<turmoil::net::TcpStream>) -> Result<(), TransportError> {
    while let Some(frame) = connection.recv().await? {
        let kind = match frame.header.kind {
            MessageKind::Append => MessageKind::AppendAck,
            MessageKind::Beacon => MessageKind::BeaconAck,
            MessageKind::Handoff => MessageKind::AdminReply,
            _ => continue,
        };
        let reply = Header::new(kind)
            .with_request_id(frame.header.request_id)
            .with_body(digest(&frame.payload));
        connection.send(&Frame::new(reply, Bytes::new())).await?;
    }
    connection.close().await
}

/// Pipelines appends and beacons to `node` while the link is held and
/// released, then checks every reply in order.
async fn exchange(
    transport: &Transport<TurmoilNetwork>,
    node: &str,
    rng: &mut SmallRng,
) -> Result<(), Error> {
    let connection = transport
        .connect(&node_id(node), &address(node))
        .await
        .map_err(|e| format!("connecting to {node}: {e}"))?;
    let (mut receiver, mut sender) = connection.into_split();
    let mut expected = Vec::new();
    let mut frames = Vec::new();
    for request_id in 1..=FRAMES_PER_NODE as u64 {
        let (kind, reply) = if rng.random_bool(0.25) {
            (MessageKind::Beacon, MessageKind::BeaconAck)
        } else {
            (MessageKind::Append, MessageKind::AppendAck)
        };
        let len = rng.random_range(0..=MAX_PAYLOAD);
        let payload = vec![rng.random::<u8>(); len];
        expected.push((reply, request_id, digest(&payload)));
        frames.push(Frame::new(
            Header::new(kind).with_request_id(request_id),
            payload,
        ));
    }
    let hold_after = rng.random_range(0..FRAMES_PER_NODE);
    let hold_for = Duration::from_millis(rng.random_range(1..200));
    let send = async {
        for (index, frame) in frames.iter().enumerate() {
            if index == hold_after {
                turmoil::hold("driver", node);
                tokio::time::sleep(hold_for).await;
                turmoil::release("driver", node);
            }
            sender.send(frame).await?;
        }
        Ok::<_, TransportError>(())
    };
    let receive = async move {
        let mut replies = Vec::new();
        while replies.len() < FRAMES_PER_NODE {
            let frame = receiver.recv().await?.expect("a reply to every request");
            assert!(frame.payload.is_empty());
            replies.push((
                frame.header.kind,
                frame.header.request_id,
                frame.header.body,
            ));
        }
        Ok::<_, TransportError>(replies)
    };
    let (sent, replies) = tokio::join!(send, receive);
    sent.map_err(|e| format!("sending to {node}: {e}"))?;
    let replies = replies.map_err(|e| format!("receiving from {node}: {e}"))?;
    assert_eq!(replies, expected, "replies from {node}");
    // Close without waiting for the node's close: turmoil delivers a reset
    // for a socket that is already gone ahead of data still in flight,
    // which real TCP does not, so waiting would race on simulator details.
    sender.close().await?;
    Ok(())
}

/// The driver's checks, in an order drawn from the seed.
async fn drive(seed: u64, check_silent: bool) -> Result<(), Error> {
    let pki = pki();
    let mut rng = SmallRng::seed_from_u64(seed);
    let transport = Transport::new(TurmoilNetwork, &pki.driver);

    // Let the nodes bind before the first connection.
    tokio::time::sleep(Duration::from_millis(10)).await;
    let mut order = NODES.to_vec();
    for i in (1..order.len()).rev() {
        order.swap(i, rng.random_range(0..=i));
    }
    for node in &order {
        exchange(&transport, node, &mut rng).await?;
    }

    // A partitioned node cannot be reached; within the handshake timeout
    // the attempt fails, and after a repair it succeeds.
    let cut = order[0];
    turmoil::partition("driver", cut);
    let started = tokio::time::Instant::now();
    let error = transport
        .connect(&node_id(cut), &address(cut))
        .await
        .err()
        .unwrap();
    assert!(
        matches!(
            error,
            TransportError::HandshakeTimeout | TransportError::Io(_)
        ),
        "{error}"
    );
    assert!(started.elapsed() <= HANDSHAKE_TIMEOUT + Duration::from_secs(1));
    turmoil::repair("driver", cut);
    exchange(&transport, cut, &mut rng).await?;

    // A host that accepts TCP but never answers the handshake. Waiting out
    // the timeout takes thousands of simulated ticks, so only every
    // sixteenth seed checks it.
    if check_silent {
        let started = tokio::time::Instant::now();
        let error = transport
            .connect(&node_id("n9"), &address("silent"))
            .await
            .err()
            .unwrap();
        assert!(matches!(error, TransportError::HandshakeTimeout), "{error}");
        assert!(started.elapsed() >= HANDSHAKE_TIMEOUT);
    }

    // An operator tool reaches admin messages only.
    let admin = Transport::new(TurmoilNetwork, &pki.admin);
    let target = order[rng.random_range(0..order.len())];
    let mut connection = admin
        .connect(&node_id(target), &address(target))
        .await
        .map_err(|e| format!("admin connecting to {target}: {e}"))?;
    let refused = connection
        .send(&Frame::new(Header::new(MessageKind::Append), ""))
        .await;
    assert!(matches!(
        refused,
        Err(TransportError::Unauthorized {
            role: Role::Admin,
            ..
        })
    ));
    connection
        .send(&Frame::new(
            Header::new(MessageKind::Handoff).with_request_id(5),
            "n2",
        ))
        .await?;
    let reply = connection.recv().await?.expect("a reply");
    assert_eq!(reply.header.kind, MessageKind::AdminReply);
    assert_eq!(reply.header.body, digest(b"n2"));

    // A certificate from another CA is refused.
    let rogue = Transport::new(TurmoilNetwork, &pki.rogue);
    let error = rogue
        .connect(&node_id(target), &address(target))
        .await
        .err()
        .unwrap();
    assert!(
        matches!(
            error.tls_error(),
            Some(rustls::Error::InvalidCertificate(
                CertificateError::UnknownIssuer
            ))
        ),
        "{error}"
    );
    Ok(())
}

#[test]
fn nodes_talk_over_a_delayed_network_and_refuse_strangers() {
    Runner::new().run(|context: &mut SimContext| {
        let pki = pki();
        let latency = Duration::from_millis(context.rng().random_range(1..20));
        let mut builder = context.builder();
        builder
            .simulation_duration(Duration::from_secs(120))
            .min_message_latency(Duration::from_millis(1))
            .max_message_latency(latency + Duration::from_millis(1));
        let mut sim = builder.build();
        for (name, credentials) in NODES.iter().zip(&pki.nodes) {
            let credentials = credentials.clone();
            sim.host(*name, move || serve(credentials.clone()));
        }
        sim.host("silent", || async {
            let listener = turmoil::net::TcpListener::bind(("0.0.0.0", PORT)).await?;
            let mut held = Vec::new();
            loop {
                held.push(listener.accept().await?);
            }
        });
        let check_silent = context.seed().is_multiple_of(16);
        let seed = context.fork_seed();
        sim.client("driver", async move { drive(seed, check_silent).await });
        sim.run()
    });
}
