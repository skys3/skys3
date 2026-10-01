//! The transport over real TCP on the loopback interface: who is let in,
//! who is refused, and what each role may send.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use common::{Bounded, CLUSTER, Leaf, TestCa, Validity, cluster, pem, raw_client, raw_server};
use rustls::pki_types::ServerName;
use rustls::{AlertDescription, CertificateError};
use skys3_net::{
    ALPN_PROTOCOL, Connection, Credentials, Frame, FrameError, Header, IdentityError, MessageKind,
    PeerIdentity, PkiError, Role, TokioNetwork, Transport, TransportError, read_frame, write_frame,
};
use skys3_types::{ClusterId, NodeAddress, NodeId};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, TlsConnector};

type Conn = Connection<TcpStream>;

fn node_id(name: &str) -> NodeId {
    NodeId::new(name).unwrap()
}

fn address(addr: SocketAddr) -> NodeAddress {
    addr.to_string().parse().unwrap()
}

/// A node listening on an ephemeral loopback port, whose accepted
/// handshakes are sent back on a channel.
struct Server {
    addr: SocketAddr,
    handshakes: tokio::sync::mpsc::UnboundedReceiver<Result<Conn, TransportError>>,
}

async fn serve(credentials: &Credentials) -> Server {
    let transport = Transport::new(TokioNetwork, credentials);
    let listener = transport
        .bind("127.0.0.1:0".parse().unwrap())
        .bounded()
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, handshakes) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Ok(incoming) = listener.accept().await {
            assert!(incoming.remote_addr().ip().is_loopback());
            let tx = tx.clone();
            tokio::spawn(async move {
                let _ = tx.send(incoming.handshake().bounded().await);
            });
        }
    });
    Server { addr, handshakes }
}

impl Server {
    async fn next(&mut self) -> Result<Conn, TransportError> {
        self.handshakes.recv().bounded().await.unwrap()
    }
}

/// The certificate error behind a failed handshake or read.
fn cert_error(error: &TransportError) -> CertificateError {
    match error.tls_error() {
        Some(rustls::Error::InvalidCertificate(cert)) => cert.clone(),
        other => panic!("expected a certificate error, got {other:?} from {error}"),
    }
}

/// The identity error carried by a certificate error.
fn identity_error(error: &TransportError) -> IdentityError {
    match cert_error(error) {
        CertificateError::Other(other) => other
            .0
            .downcast_ref::<IdentityError>()
            .unwrap_or_else(|| panic!("not an identity error: {other:?}"))
            .clone(),
        other => panic!("expected an identity error, got {other:?}"),
    }
}

/// Connects with a raw TLS client and returns the server's handshake
/// result.
async fn raw_attempt(
    server: &mut Server,
    config: Arc<rustls::ClientConfig>,
) -> Result<Conn, TransportError> {
    let stream = TcpStream::connect(server.addr).bounded().await.unwrap();
    let name = ServerName::try_from("skys3-node").unwrap();
    // The client side may succeed or fail; the server's verdict matters.
    let client = TlsConnector::from(config)
        .connect(name, stream)
        .bounded()
        .await;
    let result = server.next().bounded().await;
    drop(client);
    result
}

#[tokio::test]
async fn nodes_exchange_frames_with_raw_payloads() {
    let ca = TestCa::new("ca");
    let server_creds = ca.credentials(&Leaf::node("n1"));
    let client_creds = ca.credentials(&Leaf::node("n2"));
    let mut server = serve(&server_creds).bounded().await;

    let client = Transport::new(TokioNetwork, &client_creds);
    assert_eq!(client.identity(), &PeerIdentity::Node(node_id("n2")));
    let mut outbound = client
        .connect(&node_id("n1"), &address(server.addr))
        .bounded()
        .await
        .unwrap();
    let mut inbound = server.next().bounded().await.unwrap();
    assert_eq!(outbound.peer(), &PeerIdentity::Node(node_id("n1")));
    assert_eq!(inbound.peer(), &PeerIdentity::Node(node_id("n2")));

    let payload = Bytes::from(vec![0xab; 3 << 20]);
    let append = Frame::new(
        Header::new(MessageKind::Append)
            .with_request_id(7)
            .with_body(&b"epoch 3 seq 9"[..]),
        payload.clone(),
    );
    // A frame larger than the socket buffers is sent and received at the
    // same time: sent alone, it would wait for a reader forever.
    let (sent, received) = async { tokio::join!(outbound.send(&append), inbound.recv()) }
        .bounded()
        .await;
    sent.unwrap();
    assert_eq!(received.unwrap(), Some(append));

    // Replies flow back over split halves, from separate tasks, and closing
    // one direction ends the other side's stream cleanly.
    let (mut receiver, mut sender) = inbound.into_split();
    assert_eq!(sender.peer(), receiver.peer());
    let ack = Frame::new(Header::new(MessageKind::AppendAck).with_request_id(7), "");
    let sent = ack.clone();
    tokio::spawn(async move {
        sender.send(&sent).bounded().await.unwrap();
        sender.close().bounded().await.unwrap();
    })
    .bounded()
    .await
    .unwrap();
    let (mut out_receiver, mut out_sender) = outbound.into_split();
    assert_eq!(out_receiver.recv().bounded().await.unwrap(), Some(ack));
    assert_eq!(out_receiver.recv().bounded().await.unwrap(), None);
    assert_eq!(out_receiver.peer().node_id(), Some(&node_id("n1")));
    out_sender.close().bounded().await.unwrap();
    assert_eq!(receiver.recv().bounded().await.unwrap(), None);

    // A peer that vanishes without closing is an error, not a clean end.
    let mut outbound = client
        .connect(&node_id("n1"), &address(server.addr))
        .bounded()
        .await
        .unwrap();
    drop(server.next().bounded().await.unwrap());
    let error = outbound.recv().bounded().await.err().unwrap();
    assert!(
        matches!(error, TransportError::Frame(FrameError::Truncated)),
        "{error}"
    );
}

#[tokio::test]
async fn a_client_without_a_certificate_is_refused() {
    let ca = TestCa::new("ca");
    let mut server = serve(&ca.credentials(&Leaf::node("n1"))).bounded().await;
    let error = raw_attempt(&mut server, raw_client(None, &[ALPN_PROTOCOL]))
        .bounded()
        .await
        .err()
        .unwrap();
    assert!(
        matches!(
            error.tls_error(),
            Some(rustls::Error::NoCertificatesPresented)
        ),
        "{error}"
    );
    assert!(error.to_string().starts_with("TLS handshake failed"));
}

#[tokio::test]
async fn a_client_from_another_ca_is_refused() {
    let ca = TestCa::new("ca");
    let rogue = TestCa::new("rogue");
    let mut server = serve(&ca.credentials(&Leaf::node("n1"))).bounded().await;

    // A rogue certificate with a correct identity, presented by our own
    // transport: the server refuses it, and the client sees the alert on
    // its first read. The client also refuses the server, since it trusts
    // only the rogue CA.
    let rogue_creds = rogue.credentials(&Leaf::node("n2"));
    let client = Transport::new(TokioNetwork, &rogue_creds);
    let error = client
        .connect(&node_id("n1"), &address(server.addr))
        .bounded()
        .await
        .err()
        .unwrap();
    assert_eq!(cert_error(&error), CertificateError::UnknownIssuer);
    assert!(server.next().bounded().await.is_err());

    // Trusting both CAs gets past the client's check; the server still
    // refuses the rogue certificate.
    let issued = rogue.issue(&Leaf::node("n2"));
    let both = Credentials::new(
        cluster(),
        vec![issued.cert],
        issued.key,
        &[rogue.cert(), ca.cert()],
    )
    .unwrap();
    let mut connection = Transport::new(TokioNetwork, &both)
        .connect(&node_id("n1"), &address(server.addr))
        .bounded()
        .await
        .unwrap();
    let server_error = server.next().bounded().await.err().unwrap();
    assert_eq!(cert_error(&server_error), CertificateError::UnknownIssuer);
    let client_error = connection.recv().bounded().await.err().unwrap();
    assert!(
        matches!(
            client_error.tls_error(),
            Some(rustls::Error::AlertReceived(AlertDescription::UnknownCA))
        ),
        "{client_error}"
    );
}

#[tokio::test]
async fn expired_and_not_yet_valid_certificates_are_refused() {
    let ca = TestCa::new("ca");
    let mut server = serve(&ca.credentials(&Leaf::node("n1"))).bounded().await;
    for (validity, expected) in [
        (Validity::Expired, CertificateError::Expired),
        (Validity::NotYetValid, CertificateError::NotValidYet),
    ] {
        let issued = ca.issue(&Leaf::node("n2").validity(validity));
        let error = raw_attempt(&mut server, raw_client(Some(issued), &[ALPN_PROTOCOL]))
            .bounded()
            .await
            .err()
            .unwrap();
        assert_eq!(cert_error(&error), expected, "{validity:?}");
    }

    // A client refuses an expired server certificate the same way.
    let mut expired_server = serve(&ca.credentials(&Leaf::node("n3").validity(Validity::Expired)))
        .bounded()
        .await;
    let client = Transport::new(TokioNetwork, &ca.credentials(&Leaf::node("n2")));
    let error = client
        .connect(&node_id("n3"), &address(expired_server.addr))
        .bounded()
        .await
        .err()
        .unwrap();
    assert_eq!(cert_error(&error), CertificateError::Expired);
    drop(expired_server.next().bounded().await);
}

#[tokio::test]
async fn certificates_without_the_needed_key_usage_are_refused() {
    let ca = TestCa::new("ca");
    let mut server = serve(&ca.credentials(&Leaf::node("n1"))).bounded().await;
    let mut server_only = Leaf::node("n2");
    server_only.client_auth = false;
    let error = raw_attempt(
        &mut server,
        raw_client(Some(ca.issue(&server_only)), &[ALPN_PROTOCOL]),
    )
    .bounded()
    .await
    .err()
    .unwrap();
    assert_eq!(cert_error(&error), CertificateError::InvalidPurpose);
}

#[tokio::test]
async fn identities_of_other_clusters_or_without_a_spiffe_id_are_refused() {
    let ca = TestCa::new("ca");
    let mut server = serve(&ca.credentials(&Leaf::node("n1"))).bounded().await;

    let other_cluster = Leaf::new("spiffe://other-cluster/node/n2");
    let error = raw_attempt(
        &mut server,
        raw_client(Some(ca.issue(&other_cluster)), &[ALPN_PROTOCOL]),
    )
    .bounded()
    .await
    .err()
    .unwrap();
    assert!(
        matches!(identity_error(&error), IdentityError::WrongCluster { .. }),
        "{error}"
    );

    for (leaf, expected) in [
        (Leaf::new("https://example.com/n2"), IdentityError::Missing),
        (
            Leaf {
                uris: vec![
                    format!("spiffe://{CLUSTER}/node/n2"),
                    format!("spiffe://{CLUSTER}/node/n3"),
                ],
                ..Leaf::node("n2")
            },
            IdentityError::Ambiguous,
        ),
    ] {
        let error = raw_attempt(
            &mut server,
            raw_client(Some(ca.issue(&leaf)), &[ALPN_PROTOCOL]),
        )
        .bounded()
        .await
        .err()
        .unwrap();
        assert_eq!(identity_error(&error), expected);
    }
}

#[tokio::test]
async fn peers_must_speak_the_cluster_protocol() {
    let ca = TestCa::new("ca");
    let mut server = serve(&ca.credentials(&Leaf::node("n1"))).bounded().await;
    let error = raw_attempt(
        &mut server,
        raw_client(Some(ca.issue(&Leaf::node("n2"))), &[b"skys3-cluster/0"]),
    )
    .bounded()
    .await
    .err()
    .unwrap();
    assert!(
        matches!(
            error.tls_error(),
            Some(rustls::Error::NoApplicationProtocol)
        ),
        "{error}"
    );
    let error = raw_attempt(
        &mut server,
        raw_client(Some(ca.issue(&Leaf::node("n2"))), &[]),
    )
    .bounded()
    .await
    .err()
    .unwrap();
    assert!(matches!(error, TransportError::WrongProtocol), "{error}");
}

#[tokio::test]
async fn admins_may_send_only_admin_messages() {
    let ca = TestCa::new("ca");
    let mut server = serve(&ca.credentials(&Leaf::node("n1"))).bounded().await;
    let admin_creds = ca.credentials(&Leaf::admin("ops"));
    let admin = Transport::new(TokioNetwork, &admin_creds);

    // Operator tools connect but never serve.
    let error = admin
        .bind("127.0.0.1:0".parse().unwrap())
        .bounded()
        .await
        .err()
        .unwrap();
    assert!(
        matches!(error, TransportError::NotANode(Role::Admin)),
        "{error}"
    );

    let mut outbound = admin
        .connect(&node_id("n1"), &address(server.addr))
        .bounded()
        .await
        .unwrap();
    let mut inbound = server.next().bounded().await.unwrap();
    assert_eq!(inbound.peer().role(), Role::Admin);

    // The transport refuses to send what the role may not send, and the
    // connection stays usable.
    for kind in [
        MessageKind::Append,
        MessageKind::Beacon,
        MessageKind::Forward,
    ] {
        let error = outbound
            .send(&Frame::new(Header::new(kind), ""))
            .bounded()
            .await
            .err()
            .unwrap();
        assert!(
            matches!(error, TransportError::Unauthorized { role: Role::Admin, kind: k } if k == kind),
            "{error}"
        );
    }
    let handoff = Frame::new(Header::new(MessageKind::Handoff).with_request_id(1), "");
    outbound.send(&handoff).bounded().await.unwrap();
    assert_eq!(inbound.recv().bounded().await.unwrap(), Some(handoff));
    let reply = Frame::new(Header::new(MessageKind::AdminReply).with_request_id(1), "");
    inbound.send(&reply).bounded().await.unwrap();
    assert_eq!(outbound.recv().bounded().await.unwrap(), Some(reply));
}

#[tokio::test]
async fn a_node_refuses_replication_and_lease_messages_from_an_admin() {
    let ca = TestCa::new("ca");
    let mut server = serve(&ca.credentials(&Leaf::node("n1"))).bounded().await;
    for kind in [
        MessageKind::Append,
        MessageKind::Sync,
        MessageKind::BeaconAck,
        MessageKind::StepDown,
        MessageKind::Forward,
    ] {
        // A tool that bypasses its own transport's check.
        let stream = TcpStream::connect(server.addr).bounded().await.unwrap();
        let config = raw_client(Some(ca.issue(&Leaf::admin("ops"))), &[ALPN_PROTOCOL]);
        let mut tls = TlsConnector::from(config)
            .connect(ServerName::try_from("skys3-node").unwrap(), stream)
            .bounded()
            .await
            .unwrap();
        write_frame(&mut tls, &Frame::new(Header::new(kind), "forged"))
            .bounded()
            .await
            .unwrap();
        let mut inbound = server.next().bounded().await.unwrap();
        let error = inbound.recv().bounded().await.err().unwrap();
        assert!(
            matches!(error, TransportError::Unauthorized { role: Role::Admin, kind: k } if k == kind),
            "{error}"
        );
        assert_eq!(
            error.to_string(),
            format!("admin may not send {kind:?} messages")
        );
    }
}

#[tokio::test]
async fn a_client_checks_which_node_it_reached() {
    let ca = TestCa::new("ca");
    let mut server = serve(&ca.credentials(&Leaf::node("n1"))).bounded().await;
    let client = Transport::new(TokioNetwork, &ca.credentials(&Leaf::node("n2")));
    let error = client
        .connect(&node_id("n9"), &address(server.addr))
        .bounded()
        .await
        .err()
        .unwrap();
    assert!(
        matches!(
            &error,
            TransportError::UnexpectedPeer { expected, found }
                if expected == &node_id("n9") && found == &PeerIdentity::Node(node_id("n1"))
        ),
        "{error}"
    );
    drop(server.next().bounded().await);

    // A server presenting an operator tool's certificate is not a node.
    let listener = TcpListener::bind("127.0.0.1:0").bounded().await.unwrap();
    let addr = listener.local_addr().unwrap();
    let acceptor = TlsAcceptor::from(raw_server(ca.issue(&Leaf::admin("ops")), &[ALPN_PROTOCOL]));
    tokio::spawn(async move {
        let (stream, _) = listener.accept().bounded().await.unwrap();
        let _ = acceptor.accept(stream).bounded().await;
    });
    let error = client
        .connect(&node_id("n1"), &address(addr))
        .bounded()
        .await
        .err()
        .unwrap();
    assert_eq!(
        identity_error(&error),
        IdentityError::WrongRole {
            found: Role::Admin,
            expected: Role::Node
        }
    );
}

#[tokio::test]
async fn connection_failures_are_network_errors() {
    let ca = TestCa::new("ca");
    let client = Transport::new(TokioNetwork, &ca.credentials(&Leaf::node("n2")));
    // A port that was just released refuses connections.
    let addr = TcpListener::bind("127.0.0.1:0")
        .bounded()
        .await
        .unwrap()
        .local_addr()
        .unwrap();
    let error = client
        .connect(&node_id("n1"), &address(addr))
        .bounded()
        .await
        .err()
        .unwrap();
    assert!(matches!(error, TransportError::Io(_)), "{error}");
    assert!(error.tls_error().is_none());

    let transport = Transport::new(TokioNetwork, &ca.credentials(&Leaf::node("n1")));
    let listener = TcpListener::bind("127.0.0.1:0").bounded().await.unwrap();
    let taken = listener.local_addr().unwrap();
    let error = transport.bind(taken).bounded().await.err().unwrap();
    assert!(matches!(error, TransportError::Io(_)), "{error}");

    // A DNS name resolves before connecting.
    let error = client
        .connect(
            &node_id("n1"),
            &format!("localhost:{}", addr.port()).parse().unwrap(),
        )
        .bounded()
        .await
        .err()
        .unwrap();
    assert!(matches!(error, TransportError::Io(_)), "{error}");
    let error = client
        .connect(
            &node_id("n1"),
            &format!("[::1]:{}", addr.port()).parse().unwrap(),
        )
        .bounded()
        .await
        .err()
        .unwrap();
    assert!(matches!(error, TransportError::Io(_)), "{error}");
}

#[tokio::test]
async fn a_peer_that_breaks_the_frame_format_is_disconnected() {
    let ca = TestCa::new("ca");
    let mut server = serve(&ca.credentials(&Leaf::node("n1"))).bounded().await;
    let stream = TcpStream::connect(server.addr).bounded().await.unwrap();
    let config = raw_client(Some(ca.issue(&Leaf::node("n2"))), &[ALPN_PROTOCOL]);
    let mut tls = TlsConnector::from(config)
        .connect(ServerName::try_from("skys3-node").unwrap(), stream)
        .bounded()
        .await
        .unwrap();
    let mut inbound = server.next().bounded().await.unwrap();
    use tokio::io::AsyncWriteExt;
    tls.write_all(&[0xff; 8]).bounded().await.unwrap();
    tls.flush().bounded().await.unwrap();
    let error = inbound.recv().bounded().await.err().unwrap();
    assert!(
        matches!(error, TransportError::Frame(FrameError::HeaderTooLong(_))),
        "{error}"
    );
    // The reader of a raw stream sees the same frames as a connection.
    let mut empty: &[u8] = &[];
    assert!(read_frame(&mut empty).bounded().await.unwrap().is_none());
}

#[test]
fn credentials_load_from_pem_files() {
    let ca = TestCa::new("ca");
    let issued = ca.issue(&Leaf::node("n7"));
    let dir = tempfile::tempdir().unwrap();
    let cert = dir.path().join("node.crt");
    let key = dir.path().join("node.key");
    let bundle = dir.path().join("ca.crt");
    std::fs::write(&cert, pem("CERTIFICATE", &issued.cert)).unwrap();
    std::fs::write(&key, pem("PRIVATE KEY", issued.key.secret_der())).unwrap();
    std::fs::write(&bundle, pem("CERTIFICATE", &ca.cert())).unwrap();

    let credentials = Credentials::load(cluster(), &cert, &key, &bundle).unwrap();
    assert_eq!(credentials.identity(), &PeerIdentity::Node(node_id("n7")));
    assert_eq!(credentials.cluster(), &cluster());
    let debug = format!("{credentials:?}");
    assert!(
        debug.contains("n7") && !debug.contains("PRIVATE"),
        "{debug}"
    );

    let missing = dir.path().join("missing");
    let error = Credentials::load(cluster(), &cert, &missing, &bundle).unwrap_err();
    assert!(matches!(error, PkiError::Read { .. }), "{error}");
    assert!(error.to_string().contains("missing"), "{error}");

    let error = Credentials::load(cluster(), &cert, &cert, &bundle).unwrap_err();
    assert!(
        matches!(
            error,
            PkiError::Pem {
                what: "private key",
                ..
            }
        ),
        "{error}"
    );
    let error = Credentials::from_pem(cluster(), b"", &[], b"").unwrap_err();
    assert!(matches!(error, PkiError::Pem { .. }), "{error}");
    let key_pem = pem("PRIVATE KEY", ca.issue(&Leaf::node("n7")).key.secret_der());
    let error = Credentials::from_pem(cluster(), b"", key_pem.as_bytes(), b"").unwrap_err();
    assert!(matches!(error, PkiError::NoCertificate), "{error}");
}

#[test]
fn credentials_are_checked_when_assembled() {
    let ca = TestCa::new("ca");
    let issued = ca.issue(&Leaf::node("n1"));
    let other = ca.issue(&Leaf::node("n2"));

    let error =
        Credentials::new(cluster(), vec![], issued.key.clone_key(), &[ca.cert()]).unwrap_err();
    assert!(matches!(error, PkiError::NoCertificate), "{error}");

    let error = Credentials::new(
        cluster(),
        vec![issued.cert.clone()],
        issued.key.clone_key(),
        &[],
    )
    .unwrap_err();
    assert!(matches!(error, PkiError::NoCa), "{error}");

    let error = Credentials::new(
        cluster(),
        vec![issued.cert.clone()],
        other.key.clone_key(),
        &[ca.cert()],
    )
    .unwrap_err();
    assert!(matches!(error, PkiError::Key(_)), "{error}");

    let error = Credentials::new(
        ClusterId::new("elsewhere").unwrap(),
        vec![issued.cert.clone()],
        issued.key.clone_key(),
        &[ca.cert()],
    )
    .unwrap_err();
    assert!(
        matches!(
            error,
            PkiError::Identity(IdentityError::WrongCluster { .. })
        ),
        "{error}"
    );

    let garbage = skys3_net::CertificateDer::from(vec![0x30, 0x03, 0x02, 0x01, 0x01]);
    let error = Credentials::new(
        cluster(),
        vec![garbage.clone()],
        issued.key.clone_key(),
        &[ca.cert()],
    )
    .unwrap_err();
    assert!(
        matches!(
            error,
            PkiError::Certificate {
                what: "certificate chain",
                ..
            }
        ),
        "{error}"
    );
    let error = Credentials::new(
        cluster(),
        vec![issued.cert.clone()],
        issued.key.clone_key(),
        &[garbage],
    )
    .unwrap_err();
    assert!(
        matches!(
            error,
            PkiError::Certificate {
                what: "CA bundle",
                ..
            }
        ),
        "{error}"
    );
}
