use std::net::SocketAddr;

use proptest::prelude::*;
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType,
};
use skys3_net::{CertificateDer, Credentials, PrivateKeyDer, TokioNetwork};
use skys3_types::ClusterId;

use super::*;

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
    NodeId::new(format!("node-{n}")).unwrap()
}

fn address(addr: SocketAddr) -> NodeAddress {
    addr.to_string().parse().unwrap()
}

/// Node `n` serving pushes on a loopback port, with its hints.
async fn listening(pki: &Pki, n: u8) -> (NodeAddress, ControlHints) {
    let transport = pki.transport(&node(n));
    let listener = transport
        .bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr = address(listener.local_addr().unwrap());
    let hints = ControlHints::new();
    let serving = hints.clone();
    tokio::spawn(async move { serving.serve(listener).await });
    (addr, hints)
}

#[test]
fn frames_carry_the_generation_and_nothing_else() {
    let frame = ControlChanged::frame(Generation::new(7), 3);
    assert_eq!(frame.header.kind, MessageKind::ControlChanged);
    assert_eq!(frame.header.request_id, 3);
    assert_eq!(ControlChanged::from_frame(&frame), Ok(Generation::new(7)));

    let mut payload = frame.clone();
    payload.payload = Bytes::from_static(b"x");
    assert_eq!(
        ControlChanged::from_frame(&payload),
        Err(HintError::Payload(1))
    );
    let mut kind = frame.clone();
    kind.header.kind = MessageKind::Beacon;
    assert_eq!(
        ControlChanged::from_frame(&kind),
        Err(HintError::Kind(MessageKind::Beacon))
    );
    let zero = ControlChanged::frame(Generation::ZERO, 1);
    assert_eq!(
        ControlChanged::from_frame(&zero),
        Err(HintError::ZeroGeneration)
    );
    let mut malformed = frame;
    malformed.header.body = Bytes::from_static(&[0x08]);
    let error = ControlChanged::from_frame(&malformed).unwrap_err();
    assert!(matches!(error, HintError::Malformed(_)), "{error}");
    assert!(
        error
            .to_string()
            .starts_with("malformed ControlChanged body")
    );
}

proptest! {
    #[test]
    fn any_body_decodes_or_is_refused(body in proptest::collection::vec(any::<u8>(), 0..64)) {
        let frame = Frame::new(
            Header::new(MessageKind::ControlChanged).with_body(body),
            Bytes::new(),
        );
        if let Ok(generation) = ControlChanged::from_frame(&frame) {
            prop_assert!(generation > Generation::ZERO);
            let again = ControlChanged::frame(generation, 0);
            prop_assert_eq!(ControlChanged::from_frame(&again), Ok(generation));
        }
    }

    #[test]
    fn every_generation_round_trips(generation in 1..=u64::MAX, request in any::<u64>()) {
        let frame = ControlChanged::frame(Generation::new(generation), request);
        let decoded = Frame::decode(&frame.encode().unwrap()).unwrap().unwrap().0;
        prop_assert_eq!(
            ControlChanged::from_frame(&decoded),
            Ok(Generation::new(generation))
        );
        prop_assert_eq!(decoded.header.request_id, request);
    }
}

#[tokio::test]
async fn hints_keep_the_newest_generation() {
    let hints = ControlHints::default();
    assert_eq!(hints.latest(), Generation::ZERO);
    let waiting = {
        let hints = hints.clone();
        tokio::spawn(async move { hints.newer_than(Generation::new(2)).await })
    };
    assert!(hints.hint(Generation::new(2)));
    assert!(!hints.hint(Generation::new(2)));
    assert!(!hints.hint(Generation::new(1)));
    hints.announce(Generation::new(5)).await;
    let woken = tokio::time::timeout(WAIT, waiting).await.unwrap().unwrap();
    assert_eq!(woken, Generation::new(5));
    assert_eq!(hints.latest(), Generation::new(5));
    // Already newer: no wait.
    assert_eq!(
        hints.newer_than(Generation::new(4)).await,
        Generation::new(5)
    );
}

#[tokio::test]
async fn a_push_reaches_every_node_and_reports_those_it_missed() {
    let pki = Pki::new();
    let (two, hints_two) = listening(&pki, 2).await;
    let (three, hints_three) = listening(&pki, 3).await;
    // A port nobody listens on.
    let closed = {
        let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        address(socket.local_addr().unwrap())
    };
    let local = ControlHints::new();
    let me: NodeAddress = "127.0.0.1:1".parse().unwrap();
    let peers = BTreeMap::from([
        (node(1), me),
        (node(2), two.clone()),
        (node(3), three),
        (node(4), closed),
    ]);
    let pusher = Pusher::new(pki.transport(&node(1)), peers, WAIT).with_local(local.clone());
    assert!(format!("{pusher:?}").starts_with("Pusher"));
    let pushed = tokio::time::timeout(WAIT, pusher.push(Generation::new(4)))
        .await
        .unwrap();
    assert_eq!(
        pushed.delivered,
        BTreeSet::from([node(1), node(2), node(3)])
    );
    assert_eq!(pushed.failed.keys().collect::<Vec<_>>(), [&node(4)]);
    assert_eq!(local.latest(), Generation::new(4));
    assert_eq!(hints_two.latest(), Generation::new(4));
    assert_eq!(hints_three.latest(), Generation::new(4));

    // An older generation changes nothing; the node set can be replaced.
    pusher.set_peers(BTreeMap::from([(node(2), two)]));
    tokio::time::timeout(WAIT, pusher.announce(Generation::new(3)))
        .await
        .unwrap();
    assert_eq!(hints_two.latest(), Generation::new(4));
    assert_eq!(hints_three.latest(), Generation::new(4));

    // Without local hints, this node is skipped.
    let pusher = Pusher::new(
        pki.transport(&node(1)),
        BTreeMap::from([(node(1), "127.0.0.1:1".parse().unwrap())]),
        WAIT,
    );
    assert_eq!(pusher.push(Generation::new(9)).await, Pushed::default());
}

#[tokio::test]
async fn a_push_to_an_impostor_or_a_silent_node_fails() {
    let pki = Pki::new();
    // Node 2's address, served by node 3.
    let (impostor, _) = listening(&pki, 3).await;
    // A node that accepts but never answers.
    let silent = pki.transport(&node(5));
    let listener = silent.bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let silent_addr = address(listener.local_addr().unwrap());
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok(incoming) = listener.accept().await {
            if let Ok(connection) = incoming.handshake().await {
                held.push(connection);
            }
        }
    });
    let peers = BTreeMap::from([(node(2), impostor), (node(5), silent_addr)]);
    let pusher = Pusher::new(pki.transport(&node(1)), peers, Duration::from_millis(500));
    let pushed = tokio::time::timeout(WAIT, pusher.push(Generation::new(2)))
        .await
        .unwrap();
    assert!(pushed.delivered.is_empty());
    assert!(pushed.failed[&node(2)].contains("node-3"), "{pushed:?}");
    assert!(
        pushed.failed[&node(5)].contains("no acknowledgement"),
        "{pushed:?}"
    );
    tokio::time::timeout(WAIT, pusher.announce(Generation::new(3)))
        .await
        .unwrap();
}

#[tokio::test]
async fn a_node_refuses_anything_but_pushes() {
    let pki = Pki::new();
    let (addr, hints) = listening(&pki, 2).await;
    let transport = pki.transport(&node(1));
    let mut connection = tokio::time::timeout(WAIT, transport.connect(&node(2), &addr))
        .await
        .unwrap()
        .unwrap();
    // A one-way push gets no reply, and still counts.
    connection
        .send(&ControlChanged::frame(Generation::new(6), 0))
        .await
        .unwrap();
    connection
        .send(&ControlChanged::frame(Generation::new(7), 9))
        .await
        .unwrap();
    let reply = tokio::time::timeout(WAIT, connection.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(reply.header.kind, MessageKind::AdminReply);
    assert_eq!(reply.header.request_id, 9);
    assert_eq!(hints.latest(), Generation::new(7));

    // Anything else ends the connection.
    connection
        .send(&Frame::new(
            Header::new(MessageKind::NodeHeartbeat),
            Bytes::new(),
        ))
        .await
        .unwrap();
    let closed = tokio::time::timeout(WAIT, connection.recv()).await.unwrap();
    assert!(!matches!(closed, Ok(Some(_))), "{closed:?}");
    assert_eq!(hints.latest(), Generation::new(7));
}

#[tokio::test]
async fn a_push_connection_ends_on_a_failed_handshake_or_an_answer_out_of_turn() {
    let pki = Pki::new();
    let (addr, hints) = listening(&pki, 2).await;
    // A plain TCP client that sends garbage fails the handshake; the
    // listener keeps serving.
    let mut stream = tokio::net::TcpStream::connect(addr.to_string())
        .await
        .unwrap();
    tokio::io::AsyncWriteExt::write_all(&mut stream, b"not tls")
        .await
        .unwrap();
    drop(stream);
    let pusher = Pusher::new(
        pki.transport(&node(1)),
        BTreeMap::from([(node(2), addr)]),
        WAIT,
    );
    let pushed = tokio::time::timeout(WAIT, pusher.push(Generation::new(3)))
        .await
        .unwrap();
    assert_eq!(pushed.delivered, BTreeSet::from([node(2)]));
    assert_eq!(hints.latest(), Generation::new(3));

    // A node that answers with something else is not counted.
    let odd = pki.transport(&node(6));
    let listener = odd.bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let odd_addr = address(listener.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok(incoming) = listener.accept().await {
            if let Ok(mut connection) = incoming.handshake().await {
                let _ = connection.recv().await;
                let _ = connection.send(&reply(12345)).await;
                let _ = connection.close().await;
            }
        }
    });
    pusher.set_peers(BTreeMap::from([(node(6), odd_addr)]));
    let pushed = tokio::time::timeout(WAIT, pusher.push(Generation::new(4)))
        .await
        .unwrap();
    assert!(
        pushed.failed[&node(6)].contains("unexpected answer"),
        "{pushed:?}"
    );
}
