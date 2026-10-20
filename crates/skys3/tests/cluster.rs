//! Nodes with `[cluster] multi_node = true` (plan M8-04; design §6.2,
//! §6.7, §12), on the in-process stand-in for etcd: they register, send
//! heartbeats to the coordinator the lease names, and take pushed
//! generations over the intra-cluster transport; a node refuses a peer
//! from another CA or another cluster, caps its in-flight handshakes, and
//! does not start over another data directory's registration of its ID.
//!
//! No node runs an elector in this build (plan M8-06), so a stand-in plays
//! the coordinator: it holds `coordinator.lease`, is registered, and
//! answers heartbeats as coordinator.

mod support;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType, date_time_ymd,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use skys3::{Node, NodeError, StartError};
use skys3_control::Version;
use skys3_control::etcd_fake::Fake;
use skys3_coord::{
    AdminEndpoint, ControlChanged, ControlHints, Heartbeat, HeartbeatAck, Leadership, NodeRegistry,
    RegistryConfig,
};
use skys3_io::{Clock, MonoTime, MonotonicClock};
use skys3_net::{ALPN_PROTOCOL, Credentials, Frame, Header, MessageKind, TokioNetwork, Transport};
use skys3_types::{
    ClusterId, CoordinatorLease, Generation, Label, NodeAddress, NodeId, NodeRegistration,
    ProposalId, RegisterDocument,
};
use support::process::free_port;
use support::{admin_json, config_text, eventually, http_get};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;

/// The cluster of `config_text`.
const CLUSTER: &str = "node-test";

/// How long a test waits on the network before it fails.
const WAIT: Duration = Duration::from_secs(30);

/// `der` as a PEM block labeled `label`.
fn pem(label: &str, der: &[u8]) -> String {
    use base64::Engine as _;
    let text = base64::engine::general_purpose::STANDARD.encode(der);
    let lines: Vec<&str> = text
        .as_bytes()
        .chunks(64)
        .map(|line| std::str::from_utf8(line).unwrap())
        .collect();
    format!(
        "-----BEGIN {label}-----\n{}\n-----END {label}-----\n",
        lines.join("\n")
    )
}

/// A throwaway CA.
struct Ca {
    issuer: Issuer<'static, KeyPair>,
    der: CertificateDer<'static>,
}

/// A certificate chain and its key.
struct Leaf {
    chain: Vec<CertificateDer<'static>>,
    key: PrivatePkcs8KeyDer<'static>,
}

impl Ca {
    fn new(name: &str) -> Self {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, name);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let cert = params.self_signed(&key).unwrap();
        Self {
            der: cert.der().clone(),
            issuer: Issuer::new(params, key),
        }
    }

    /// A node certificate naming `spiffe://<cluster>/node/<node>`, also
    /// valid for `localhost`, so that a plain TLS client can verify it.
    fn node(&self, cluster: &str, node: &str) -> Leaf {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        let uri = format!("spiffe://{cluster}/node/{node}");
        params.subject_alt_names = vec![
            SanType::URI(uri.as_str().try_into().unwrap()),
            SanType::DnsName("localhost".try_into().unwrap()),
        ];
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ClientAuth,
            ExtendedKeyUsagePurpose::ServerAuth,
        ];
        (params.not_before, params.not_after) =
            (date_time_ymd(2000, 1, 1), date_time_ymd(4000, 1, 1));
        let cert = params.signed_by(&key, &self.issuer).unwrap();
        Leaf {
            chain: vec![cert.der().clone()],
            key: PrivatePkcs8KeyDer::from(key.serialize_der()),
        }
    }
}

impl Leaf {
    /// Writes the chain and key into `dir` as `<name>.pem` and
    /// `<name>.key`.
    fn write(&self, dir: &Path, name: &str) -> (PathBuf, PathBuf) {
        let cert = dir.join(format!("{name}.pem"));
        let key = dir.join(format!("{name}.key"));
        std::fs::write(&cert, pem("CERTIFICATE", &self.chain[0])).unwrap();
        std::fs::write(&key, pem("PRIVATE KEY", self.key.secret_pkcs8_der())).unwrap();
        (cert, key)
    }

    /// Transport credentials of `cluster` trusting `ca`.
    fn credentials(&self, cluster: &str, ca: &Ca) -> Credentials {
        Credentials::new(
            ClusterId::new(cluster).unwrap(),
            self.chain.clone(),
            PrivateKeyDer::Pkcs8(self.key.clone_key()),
            std::slice::from_ref(&ca.der),
        )
        .unwrap()
    }
}

/// The cluster's CA, a second cluster's CA that the nodes also trust (a CA
/// shared by two clusters), and a CA nobody trusts.
struct Pki {
    cluster: Ca,
    other: Ca,
    foreign: Ca,
    /// The nodes' CA bundle: the cluster's and the other cluster's CAs.
    bundle: PathBuf,
}

impl Pki {
    fn new(dir: &Path) -> Self {
        let (cluster, other, foreign) = (Ca::new("cluster"), Ca::new("other"), Ca::new("foreign"));
        let bundle = dir.join("ca-bundle.pem");
        let text = pem("CERTIFICATE", &cluster.der) + &pem("CERTIFICATE", &other.der);
        std::fs::write(&bundle, text).unwrap();
        Self {
            cluster,
            other,
            foreign,
            bundle,
        }
    }
}

/// The `[control_store]` lines of the stand-in for etcd.
fn etcd(fake: &Fake) -> String {
    format!(
        "backend = \"etcd\"\netcd_endpoints = [\"{}\"]",
        fake.url("http")
    )
}

/// A node of the cluster in `dir`: node `node` over `store`, with
/// `multi_node`, its transport on `port` with the certificate of `cert`
/// and at most `handshakes` handshakes at once.
fn node_config(
    dir: &Path,
    pki: &Pki,
    store: &str,
    node: &str,
    cert: &str,
    port: u16,
    handshakes: u32,
) -> String {
    let (cert, key) = pki.cluster.node(CLUSTER, cert).write(dir, "node");
    let transport = format!(
        "[transport]\nlisten = \"127.0.0.1:{port}\"\nmax_inflight_handshakes = {handshakes}\n\
         tls_cert_file = \"{}\"\ntls_key_file = \"{}\"\ntls_ca_file = \"{}\"",
        cert.display(),
        key.display(),
        pki.bundle.display()
    );
    config_text(dir, "127.0.0.1:0", "127.0.0.1:0", &transport)
        .replace(
            &format!("cluster_id = \"{CLUSTER}\""),
            &format!("cluster_id = \"{CLUSTER}\"\nmulti_node = true"),
        )
        .replace(
            "[node]\n",
            &format!("[node]\nnode_id = \"{node}\"\nzone = \"z1\"\nrack = \"r-{node}\"\n"),
        )
        .replace("backend = \"file\"", store)
}

/// The admin API's `cluster` object of `node`.
async fn cluster_state(node: &Node) -> serde_json::Value {
    admin_json(&node.admin_addr().to_string(), "/v1/health").await["cluster"].clone()
}

/// The value of one series of `node`'s `/metrics`, or 0 if it is absent.
async fn metric(node: &Node, series: &str) -> f64 {
    let (_, text) = http_get(&node.admin_addr().to_string(), "/metrics")
        .await
        .unwrap();
    text.lines()
        .find_map(|line| line.strip_prefix(series)?.strip_prefix(' '))
        .map_or(0.0, |value| value.parse().unwrap())
}

fn transport_handshakes(outcome: &str) -> String {
    format!("skys3_handshakes_total{{listener=\"transport\",outcome=\"{outcome}\"}}")
}

/// The stand-in coordinator `coord`: registered, named by the lease, and
/// answering heartbeats as coordinator. Returns its transport.
async fn coordinator(pki: &Pki, fake: &Fake) -> Transport<TokioNetwork> {
    let credentials = pki
        .cluster
        .node(CLUSTER, "coord")
        .credentials(CLUSTER, &pki.cluster);
    let transport = Transport::new(TokioNetwork, &credentials);
    let listener = transport
        .bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let address: NodeAddress = listener.local_addr().unwrap().to_string().parse().unwrap();
    let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::new());
    let registry = NodeRegistry::new(
        Arc::clone(&clock),
        RegistryConfig::new(Duration::from_secs(3600)),
    );
    let (_, leadership) = watch::channel(Leadership::Coordinator {
        version: Version::new("held"),
        until: MonoTime::MAX,
    });
    let endpoint =
        AdminEndpoint::new(ControlHints::new()).with_heartbeats(registry, leadership, clock);
    tokio::spawn(async move { endpoint.serve(listener).await });
    let coord = NodeId::new("coord").unwrap();
    let lease = CoordinatorLease {
        holder: coord.clone(),
        proposal_id: ProposalId::new("lease-1").unwrap(),
    };
    let registration = NodeRegistration {
        node_id: coord,
        address,
        zone: None,
        rack: None,
        disks: Vec::new(),
        instance_id: None,
        departing: false,
        proposal_id: ProposalId::new("coord-1").unwrap(),
    };
    fake.put(
        &format!("{CLUSTER}/coordinator.lease"),
        &lease.to_json().unwrap(),
    );
    fake.put(
        &format!("{CLUSTER}/nodes/coord.json"),
        &registration.to_json().unwrap(),
    );
    transport
}

/// Connects to the transport of `node` (`id`) at `port` as `transport`'s
/// holder, sends `frame`, and returns the answer.
async fn ask(
    transport: &Transport<TokioNetwork>,
    id: &str,
    port: u16,
    frame: &skys3_net::Frame,
) -> skys3_net::Frame {
    let address: NodeAddress = format!("127.0.0.1:{port}").parse().unwrap();
    let mut connection = transport
        .connect(&NodeId::new(id).unwrap(), &address)
        .await
        .unwrap();
    connection.send(frame).await.unwrap();
    tokio::time::timeout(WAIT, connection.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}

/// A plain TLS 1.3 client that verifies the node as `localhost` against the
/// cluster's CA, negotiates the cluster protocol, and presents `leaf`:
/// it reaches the node's own certificate checks whatever `leaf` says.
/// Returns whether the node kept the connection open after the
/// handshake.
async fn present(pki: &Pki, port: u16, leaf: &Leaf) -> bool {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut roots = rustls::RootCertStore::empty();
    roots.add(pki.cluster.der.clone()).unwrap();
    let mut config = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_client_auth_cert(
            leaf.chain.clone(),
            PrivateKeyDer::Pkcs8(leaf.key.clone_key()),
        )
        .unwrap();
    config.alpn_protocols = vec![ALPN_PROTOCOL.to_vec()];
    let stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let name = ServerName::try_from("localhost").unwrap();
    let connected = tokio_rustls::TlsConnector::from(Arc::new(config))
        .connect(name, stream)
        .await;
    let Ok(mut stream) = connected else {
        return false;
    };
    // In TLS 1.3 the server judges the client's certificate after the
    // client has finished: a refusal arrives as an alert, or a close.
    let mut byte = [0_u8; 1];
    let read = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut byte)).await;
    let kept = read.is_err();
    let _ = stream.shutdown().await;
    kept
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_register_heartbeat_and_take_pushes() {
    let root = tempfile::tempdir().unwrap();
    let pki = Pki::new(root.path());
    let fake = Fake::start(None).await;
    let store = etcd(&fake);
    let coord = coordinator(&pki, &fake).await;

    let mut nodes = Vec::new();
    let mut ports = Vec::new();
    for name in ["n1", "n2", "n3"] {
        let dir = root.path().join(name);
        std::fs::create_dir_all(&dir).unwrap();
        let port = free_port();
        let text = node_config(&dir, &pki, &store, name, name, port, 256);
        nodes.push(Node::start(text.parse().unwrap()).await.unwrap());
        ports.push(port);
    }

    // Each registers what its configuration and data directory say, and
    // heartbeats to the coordinator the lease names.
    for (node, port) in nodes.iter().zip(&ports) {
        let id = node.node_id().as_str();
        let stored = fake
            .get(&format!("{CLUSTER}/nodes/{id}.json"))
            .unwrap_or_else(|| panic!("{id} is not registered"));
        let registration = NodeRegistration::from_json(&stored).unwrap();
        assert_eq!(
            registration.address.to_string(),
            format!("127.0.0.1:{port}")
        );
        assert_eq!(registration.zone.unwrap().as_str(), "z1");
        assert_eq!(registration.rack.unwrap().as_str(), format!("r-{id}"));
        assert_eq!(registration.disks.len(), 2);
        assert!(
            registration
                .disks
                .iter()
                .all(|disk| disk.capacity_bytes > 0)
        );
        assert!(registration.instance_id.is_some());
        eventually("the node heartbeats to the coordinator", || async {
            let state = cluster_state(node).await;
            state["heartbeats"]["delivered"].as_u64() >= Some(1)
                && state["heartbeats"]["coordinator"] == "coord"
        })
        .await;
        let state = cluster_state(node).await;
        assert_eq!(state["multi_node"], true);
        assert_eq!(state["registration"]["registered"], true);
        assert_eq!(state["registration"]["error"], serde_json::Value::Null);
        assert_eq!(state["transport"]["listen"], format!("127.0.0.1:{port}"));
        assert_eq!(state["transport"]["advertise"], format!("127.0.0.1:{port}"));
        assert_eq!(state["transport"]["max_inflight_handshakes"], 256);
        assert_eq!(state["pushed_generation"], serde_json::Value::Null);
    }

    // A node answers a heartbeat itself: it is not the coordinator.
    let answer = ask(&coord, "n2", ports[1], &Heartbeat::frame(7)).await;
    let ack = HeartbeatAck::from_frame(&answer, 7).unwrap();
    assert!(!ack.coordinator, "{ack:?}");

    // A pushed generation reaches the node's hints, and costs a read of
    // `cluster.json` even when no change stands behind it.
    let pushed = Generation::new(1000);
    let reply = ask(&coord, "n1", ports[0], &ControlChanged::frame(pushed, 9)).await;
    assert_eq!(reply.header.request_id, 9);
    eventually("the push is recorded", || async {
        cluster_state(&nodes[0]).await["pushed_generation"] == 1000
    })
    .await;

    // A restart finds its registration as it left it.
    let n3 = nodes.pop().unwrap();
    let before = fake.get(&format!("{CLUSTER}/nodes/n3.json")).unwrap();
    n3.shutdown().await.unwrap();
    let dir = root.path().join("n3");
    let text = node_config(&dir, &pki, &store, "n3", "n3", ports[2], 256);
    let n3 = Node::start(text.parse().unwrap()).await.unwrap();
    assert_eq!(
        fake.get(&format!("{CLUSTER}/nodes/n3.json")).unwrap(),
        before
    );
    nodes.push(n3);

    // Another data directory with n1's ID does not start, whatever its
    // address, and leaves the registration alone.
    let registered = fake.get(&format!("{CLUSTER}/nodes/n1.json")).unwrap();
    let twin = root.path().join("twin");
    std::fs::create_dir_all(&twin).unwrap();
    let text = node_config(&twin, &pki, &store, "n1", "n1", free_port(), 256);
    let error = Node::start(text.parse().unwrap()).await.unwrap_err();
    let StartError::Cluster(message) = &error else {
        panic!("{error}");
    };
    assert!(message.contains("another data directory"), "{message}");
    assert_eq!(
        fake.get(&format!("{CLUSTER}/nodes/n1.json")).unwrap(),
        registered
    );

    // A certificate that names another node does not start one.
    let mismatch = root.path().join("mismatch");
    std::fs::create_dir_all(&mismatch).unwrap();
    let text = node_config(&mismatch, &pki, &store, "n4", "n2", free_port(), 256);
    let error = Node::start(text.parse().unwrap()).await.unwrap_err();
    let StartError::Cluster(message) = &error else {
        panic!("{error}");
    };
    assert!(message.contains("names node n2"), "{message}");

    // Without multi_node, the single-node guard still refuses the store.
    let single = root.path().join("single");
    std::fs::create_dir_all(&single).unwrap();
    let text = config_text(&single, "127.0.0.1:0", "127.0.0.1:0", "")
        .replace("backend = \"file\"", &store);
    let error = Node::start(text.parse().unwrap()).await.unwrap_err();
    let StartError::ControlStore(message) = &error else {
        panic!("{error}");
    };
    assert!(message.contains("one node per control store"), "{message}");

    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_transport_refuses_strangers_and_caps_its_handshakes() {
    let root = tempfile::tempdir().unwrap();
    let pki = Pki::new(root.path());
    let fake = Fake::start(None).await;
    let port = free_port();
    let text = node_config(root.path(), &pki, &etcd(&fake), "n1", "n1", port, 2);
    let node = Node::start(text.parse().unwrap()).await.unwrap();

    // A node of the cluster is let in.
    assert!(present(&pki, port, &pki.cluster.node(CLUSTER, "n2")).await);
    assert_eq!(metric(&node, &transport_handshakes("completed")).await, 1.0);
    // A certificate from a CA the node does not trust is refused.
    assert!(!present(&pki, port, &pki.foreign.node(CLUSTER, "n2")).await);
    // So is another cluster's node, although its CA is in the node's
    // bundle: the trust domain is the cluster ID.
    assert!(!present(&pki, port, &pki.other.node("other", "n2")).await);
    eventually("both refusals are counted", || async {
        metric(&node, &transport_handshakes("failed")).await == 2.0
    })
    .await;

    // A flood of connections that never finish a handshake holds at most
    // the cap of handshakes; the rest wait in the accept queue.
    let mut flood = Vec::new();
    for _ in 0..64 {
        flood.push(TcpStream::connect(("127.0.0.1", port)).await.unwrap());
    }
    let in_flight = "skys3_handshakes_in_flight{listener=\"transport\"}";
    eventually("the flood takes every slot", || async {
        metric(&node, in_flight).await == 2.0
    })
    .await;
    for _ in 0..10 {
        assert!(metric(&node, in_flight).await <= 2.0);
        let state = cluster_state(&node).await;
        assert!(state["transport"]["handshakes_in_flight"].as_u64() <= Some(2));
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Meanwhile a node's handshake waits.
    let waiting = tokio::time::timeout(
        Duration::from_millis(500),
        present(&pki, port, &pki.cluster.node(CLUSTER, "n3")),
    )
    .await;
    assert!(waiting.is_err(), "a handshake beyond the cap must wait");
    // Once the flood ends, nodes get in again.
    drop(flood);
    eventually("the slots are free again", || async {
        metric(&node, in_flight).await == 0.0
    })
    .await;
    assert!(present(&pki, port, &pki.cluster.node(CLUSTER, "n3")).await);

    // A node's connection whose first frame is of a class this build
    // serves nothing on is closed, as is one that ends before its first
    // frame, and the listener goes on.
    let peer = pki
        .cluster
        .node(CLUSTER, "n2")
        .credentials(CLUSTER, &pki.cluster);
    let transport = Transport::new(TokioNetwork, &peer);
    let address: NodeAddress = format!("127.0.0.1:{port}").parse().unwrap();
    let n1 = NodeId::new("n1").unwrap();
    let mut connection = transport.connect(&n1, &address).await.unwrap();
    let append = Frame::new(Header::new(MessageKind::Append), Vec::new());
    connection.send(&append).await.unwrap();
    let answer = tokio::time::timeout(WAIT, connection.recv()).await.unwrap();
    assert!(matches!(answer, Ok(None) | Err(_)), "an answer to Append");
    drop(transport.connect(&n1, &address).await.unwrap());
    eventually("both connections were let in", || async {
        metric(&node, &transport_handshakes("completed")).await == 4.0
    })
    .await;
    assert!(present(&pki, port, &pki.cluster.node(CLUSTER, "n3")).await);
    node.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_needs_usable_credentials_and_its_transport_port() {
    let root = tempfile::tempdir().unwrap();
    let pki = Pki::new(root.path());
    let fake = Fake::start(None).await;

    // A certificate file that holds no certificate.
    let dir = root.path().join("garbled");
    std::fs::create_dir_all(&dir).unwrap();
    let text = node_config(&dir, &pki, &etcd(&fake), "n1", "n1", free_port(), 256);
    std::fs::write(dir.join("node.pem"), "not a certificate\n").unwrap();
    let error = Node::start(text.parse().unwrap()).await.unwrap_err();
    let StartError::Cluster(message) = &error else {
        panic!("{error}");
    };
    assert!(message.contains("is not usable"), "{message}");

    // A transport port another socket holds.
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = taken.local_addr().unwrap().port();
    let dir = root.path().join("taken");
    std::fs::create_dir_all(&dir).unwrap();
    let text = node_config(&dir, &pki, &etcd(&fake), "n1", "n1", port, 256);
    let error = Node::start(text.parse().unwrap()).await.unwrap_err();
    assert!(matches!(error, StartError::Io { .. }), "{error}");
    assert!(
        error.to_string().contains("binding the transport"),
        "{error}"
    );
    // Neither registered.
    assert!(fake.get(&format!("{CLUSTER}/nodes/n1.json")).is_none());
}

/// A TCP forwarder to `target` on a port of its own, which refuses every
/// connection while `up` is false: an etcd that stops and starts answering
/// at the same address.
async fn forwarder(target: SocketAddr, up: Arc<AtomicBool>) -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((mut inbound, _)) = listener.accept().await {
            if !up.load(Ordering::SeqCst) {
                continue;
            }
            tokio::spawn(async move {
                if let Ok(mut outbound) = TcpStream::connect(target).await {
                    let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                }
            });
        }
    });
    port
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_claim_found_after_starting_from_the_copy_stops_the_node() {
    let root = tempfile::tempdir().unwrap();
    let pki = Pki::new(root.path());
    let fake = Fake::start(None).await;
    let up = Arc::new(AtomicBool::new(true));
    // The fake listens on loopback, and names itself `localhost:<port>`.
    let fake_port = fake
        .url("http")
        .rsplit(':')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    let target = SocketAddr::from(([127, 0, 0, 1], fake_port));
    let port = forwarder(target, Arc::clone(&up)).await;
    let store = format!("backend = \"etcd\"\netcd_endpoints = [\"http://127.0.0.1:{port}\"]");
    let dir = root.path().join("n1");
    std::fs::create_dir_all(&dir).unwrap();
    let text = node_config(&dir, &pki, &store, "n1", "n1", free_port(), 256);

    // A first start registers n1 and leaves a copy of control state.
    let node = Node::start(text.parse().unwrap()).await.unwrap();
    let key = format!("{CLUSTER}/nodes/n1.json");
    let ours = NodeRegistration::from_json(&fake.get(&key).unwrap()).unwrap();
    let instance = ours.instance_id.clone().unwrap();
    node.shutdown().await.unwrap();

    // The store does not answer: the node starts from its copy, and its
    // registration waits.
    up.store(false, Ordering::SeqCst);
    let node = Node::start(text.parse().unwrap()).await.unwrap();
    let state = cluster_state(&node).await;
    assert_eq!(state["registration"]["registered"], false);
    assert_eq!(state["registration"]["claimed_by"], serde_json::Value::Null);

    // Meanwhile another data directory registers n1, and the store
    // answers again.
    let theirs = NodeRegistration {
        instance_id: Some(Label::new("another-data-dir").unwrap()),
        proposal_id: ProposalId::new("theirs-1").unwrap(),
        ..ours
    };
    fake.put(&key, &theirs.to_json().unwrap());
    up.store(true, Ordering::SeqCst);
    eventually("the node finds the claim", || async {
        cluster_state(&node).await["registration"]["claimed_by"] == "another-data-dir"
    })
    .await;

    // The node stops, as it does when it finds the claim at startup, and
    // leaves the other registration alone.
    let stopped = tokio::time::timeout(WAIT, node.run_until(std::future::pending()))
        .await
        .expect("the node went on serving under a claimed ID");
    let Err(NodeError::Cluster(message)) = &stopped else {
        panic!("{stopped:?}");
    };
    assert!(message.contains("another-data-dir"), "{message}");
    assert!(message.contains(instance.as_str()), "{message}");
    assert!(message.contains("Node ID in use"), "{message}");
    assert_eq!(
        NodeRegistration::from_json(&fake.get(&key).unwrap()).unwrap(),
        theirs
    );
}
