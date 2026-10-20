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

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType, date_time_ymd,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use skys3::{Node, StartError};
use skys3_control::Version;
use skys3_control::etcd_fake::Fake;
use skys3_coord::{
    AdminEndpoint, ControlChanged, ControlHints, Heartbeat, HeartbeatAck, Leadership, NodeRegistry,
    RegistryConfig,
};
use skys3_io::{Clock, MonoTime, MonotonicClock};
use skys3_net::{ALPN_PROTOCOL, Credentials, Frame, Header, MessageKind, TokioNetwork, Transport};
use skys3_types::{
    ClusterId, CoordinatorLease, Generation, NodeAddress, NodeId, NodeRegistration, ProposalId,
    RegisterDocument,
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

    // A node's connection whose first frame is of erasure coding, which
    // this build serves nothing of, is closed, as is one that ends before
    // its first frame, and the listener goes on.
    let peer = pki
        .cluster
        .node(CLUSTER, "n2")
        .credentials(CLUSTER, &pki.cluster);
    let transport = Transport::new(TokioNetwork, &peer);
    let address: NodeAddress = format!("127.0.0.1:{port}").parse().unwrap();
    let n1 = NodeId::new("n1").unwrap();
    let mut connection = transport.connect(&n1, &address).await.unwrap();
    let fragment = Frame::new(Header::new(MessageKind::FragmentWrite), Vec::new());
    connection.send(&fragment).await.unwrap();
    let answer = tokio::time::timeout(WAIT, connection.recv()).await.unwrap();
    assert!(
        matches!(answer, Ok(None) | Err(_)),
        "an answer to FragmentWrite"
    );
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

/// Replication timing short enough for a test: a member takes over about
/// two seconds after its primary stops.
const FAST_REPLICATION: &str = "
[replication]
lease_renew_interval_ms = 200
primary_lease_ms = 1000
primary_grace_ms = 1600
member_suspect_after_ms = 1000
replica_ack_timeout_ms = 2500

[buckets.wide]
replicas = 4
";

/// The configuration of node `name` of the replicated cluster in `root`,
/// its transport on `port`.
fn replicated_config(
    root: &Path,
    pki: &Pki,
    store: &str,
    name: &str,
    port: u16,
) -> skys3_config::Config {
    let dir = root.join(name);
    std::fs::create_dir_all(&dir).unwrap();
    let text = node_config(&dir, pki, store, name, name, port, 256) + FAST_REPLICATION;
    text.parse().unwrap()
}

/// `GET /v1/shards` of `node`.
async fn shards_of(node: &Node) -> serde_json::Value {
    admin_json(&node.admin_addr().to_string(), "/v1/shards").await
}

/// The entry of shard `n` of `bucket` in `node`'s `/v1/shards`.
async fn shard_on(node: &Node, bucket: &str, n: u64) -> serde_json::Value {
    let answer = shards_of(node).await;
    answer["shards"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["bucket"] == bucket && entry["shard"] == n)
        .cloned()
        .unwrap_or(serde_json::Value::Null)
}

/// A key of `bucket` (`bucket_id`, four shards) that falls in shard `n`.
fn key_in(bucket_id: &str, n: u8, prefix: &str) -> String {
    let id = skys3_types::BucketId::new(bucket_id).unwrap();
    let count = skys3_types::ShardCount::new(4).unwrap();
    (0..)
        .map(|i| format!("{prefix}-{i}"))
        .find(|key| skys3_types::shard_for_key(&id, key.as_bytes(), count).get() == n)
        .unwrap()
}

/// Whether `client` reads `data` at `bucket/key`.
async fn reads(client: &aws_sdk_s3::Client, bucket: &str, key: &str, data: &[u8]) -> bool {
    match client.get_object().bucket(bucket).key(key).send().await {
        Ok(object) => object
            .body
            .collect()
            .await
            .is_ok_and(|body| body.into_bytes() == data),
        Err(_) => false,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_replicate_route_take_over_and_readmit() {
    let root = tempfile::tempdir().unwrap();
    let pki = Pki::new(root.path());
    let fake = Fake::start(None).await;
    let store = etcd(&fake);
    let names = ["n1", "n2", "n3"];
    let ports: Vec<u16> = names.iter().map(|_| free_port()).collect();
    let mut nodes = Vec::new();
    for (name, port) in names.iter().zip(&ports) {
        let config = replicated_config(root.path(), &pki, &store, name, *port);
        nodes.push(Some(Node::start(config).await.unwrap()));
    }
    let node = |nodes: &[Option<Node>], i: usize| -> String {
        format!("http://{}", nodes[i].as_ref().unwrap().gateway_addr())
    };
    let clients: Vec<_> = (0..3).map(|i| support::client(&node(&nodes, i))).collect();

    // A bucket created through one node is placed on all three, which
    // open their replicas once they learn of it.
    clients[0]
        .create_bucket()
        .bucket("replicated")
        .send()
        .await
        .unwrap();
    // More replicas than registered nodes cannot be placed, and the bucket
    // is refused (plan section 14).
    let refused = clients[1]
        .create_bucket()
        .bucket("wide")
        .send()
        .await
        .unwrap_err();
    let refused = format!("{refused:?}");
    assert!(refused.contains("cannot place"), "{refused}");
    for host in nodes.iter().flatten() {
        eventually("every node holds a replica of every shard", || async {
            let answer = shards_of(host).await;
            let shards = answer["shards"].as_array().unwrap().clone();
            shards.len() == 4
                && shards.iter().all(|entry| {
                    entry["source"] == "replica" && entry["members"].as_array().unwrap().len() == 3
                })
        })
        .await;
    }
    let first = shard_on(nodes[0].as_ref().unwrap(), "replicated", 0).await;
    assert_eq!(first["epoch"], 1, "{first}");
    assert_eq!(first["learners"], serde_json::json!([]), "{first}");
    let bucket_id = first["bucket_id"].as_str().unwrap().to_owned();
    let primaries: std::collections::BTreeSet<String> = {
        let answer = shards_of(nodes[0].as_ref().unwrap()).await;
        answer["shards"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["primary"].as_str().unwrap().to_owned())
            .collect()
    };
    assert!(
        primaries.len() >= 2,
        "placement spread no primaries: {primaries:?}"
    );

    // Writes and reads work through any node: each key goes to its
    // shard's primary, wherever it is.
    let mut written = Vec::new();
    for i in 0..12_u8 {
        let key = format!("key-{i}");
        let data = support::body(i, 3000);
        let writer = &clients[usize::from(i) % 3];
        let mut attempts = 0;
        loop {
            match writer
                .put_object()
                .bucket("replicated")
                .key(&key)
                .body(data.clone().into())
                .send()
                .await
            {
                Ok(_) => break,
                // A primary serves once its members granted it leases.
                Err(_) if attempts < 100 => {
                    attempts += 1;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(error) => panic!("PUT {key}: {error:?}"),
            }
        }
        written.push((key, data));
    }
    for client in &clients {
        for (key, data) in &written {
            assert!(reads(client, "replicated", key, data).await, "{key}");
        }
        let listed = client
            .list_objects_v2()
            .bucket("replicated")
            .send()
            .await
            .unwrap();
        assert_eq!(listed.key_count(), Some(12));
    }
    let forwarded = |answer: &serde_json::Value| answer["routing"]["forwarded"].as_u64().unwrap();
    assert!(forwarded(&shards_of(nodes[1].as_ref().unwrap()).await) > 0);
    let peers = shards_of(nodes[2].as_ref().unwrap()).await["peers"].clone();
    for (name, port) in names.iter().zip(&ports) {
        assert_eq!(peers[name], format!("127.0.0.1:{port}"), "{peers}");
    }

    // The primary of shard 0 stops. A member takes over within seconds,
    // and writes to the shard go on through the other nodes.
    let primary = first["primary"].as_str().unwrap().to_owned();
    let stopped = names.iter().position(|name| **name == primary).unwrap();
    let alive = (stopped + 1) % 3;
    let key = key_in(&bucket_id, 0, "after");
    nodes[stopped].take().unwrap().shutdown().await.unwrap();
    let stopped_at = tokio::time::Instant::now();
    let data = support::body(99, 2000);
    let mut resumed = None;
    while stopped_at.elapsed() < Duration::from_secs(30) {
        let put = clients[alive]
            .put_object()
            .bucket("replicated")
            .key(&key)
            .body(data.clone().into())
            .send()
            .await;
        if put.is_ok() {
            resumed = Some(stopped_at.elapsed());
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let resumed = resumed.expect("writes to the shard resumed");
    assert!(
        resumed < Duration::from_secs(10),
        "writes resumed after {resumed:?}"
    );
    let after = shard_on(nodes[alive].as_ref().unwrap(), "replicated", 0).await;
    assert_eq!(after["epoch"], 2, "{after}");
    assert_ne!(after["primary"], primary.as_str(), "{after}");
    assert_eq!(after["members"].as_array().unwrap().len(), 2, "{after}");
    for (key, data) in &written {
        assert!(
            reads(&clients[alive], "replicated", key, data).await,
            "{key}"
        );
    }

    // Restarted, the old primary keeps no replica of the shard, whose
    // register no longer names it.
    let config = replicated_config(root.path(), &pki, &store, &primary, ports[stopped]);
    nodes[stopped] = Some(Node::start(config).await.unwrap());
    let restarted = nodes[stopped].as_ref().unwrap();
    let entry = shard_on(restarted, "replicated", 0).await;
    assert_ne!(entry["source"], "replica", "{entry}");
    // The coordinator (plan M8-06) adds it back as a learner; here the
    // test plays it. The restarted node follows the register, its new
    // primary backfills it and promotes it to member.
    let register = format!("{CLUSTER}/shards/{bucket_id}/0.json");
    let mut config = skys3_types::ShardConfig::from_json(&fake.get(&register).unwrap()).unwrap();
    assert_eq!(config.epoch.get(), 2);
    config.epoch = skys3_types::Epoch::new(3);
    config.learners = vec![NodeId::new(primary.as_str()).unwrap()];
    config.proposal_id = ProposalId::new("test-learner").unwrap();
    fake.put(&register, &config.to_json().unwrap());
    eventually(
        "the restarted node holds a replica as learner or member",
        || async {
            let entry = shard_on(restarted, "replicated", 0).await;
            entry["source"] == "replica"
                && (entry["role"] == "learner" || entry["role"] == "member")
        },
    )
    .await;
    eventually("the learner is promoted to member", || async {
        let entry = shard_on(restarted, "replicated", 0).await;
        entry["role"] == "member" && entry["members"].as_array().unwrap().len() == 3
    })
    .await;
    let client = support::client(&node(&nodes, stopped));
    assert!(reads(&client, "replicated", &key, &data).await);
    for node in nodes.into_iter().flatten() {
        node.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_single_nodes_data_directory_does_not_join_a_cluster() {
    let root = tempfile::tempdir().unwrap();
    let pki = Pki::new(root.path());
    let fake = Fake::start(None).await;
    let dir = root.path().join("n1");
    std::fs::create_dir_all(&dir).unwrap();

    // A single node, whose shard report is its own replicas.
    let single = config_text(&dir, "127.0.0.1:0", "127.0.0.1:0", "")
        .replace("[node]\n", "[node]\nnode_id = \"n1\"\n");
    let node = Node::start(single.parse().unwrap()).await.unwrap();
    let client = support::client(&format!("http://{}", node.gateway_addr()));
    client.create_bucket().bucket("alone").send().await.unwrap();
    let answer = shards_of(&node).await;
    let shards = answer["shards"].as_array().unwrap();
    assert_eq!(shards.len(), 4, "{answer}");
    for entry in shards {
        assert_eq!(entry["source"], "replica", "{entry}");
        assert_eq!(entry["role"], "primary", "{entry}");
        assert_eq!(entry["serving"], true, "{entry}");
        assert_eq!(entry["members"], serde_json::json!(["n1"]), "{entry}");
    }
    assert!(answer.get("routing").is_none(), "{answer}");
    node.shutdown().await.unwrap();

    // Its data directory holds a bucket's shards: it does not start as a
    // node of several.
    let text = node_config(&dir, &pki, &etcd(&fake), "n1", "n1", free_port(), 256);
    let error = Node::start(text.parse().unwrap()).await.unwrap_err();
    let message = error.to_string();
    assert!(matches!(error, StartError::DataDir(_)), "{message}");
    assert!(message.contains("served as a single node"), "{message}");
    assert!(
        message.contains("Moving a single node into a cluster"),
        "{message}"
    );
    assert!(fake.get(&format!("{CLUSTER}/nodes/n1.json")).is_none());

    // A data directory that holds no bucket's shards may become a node of
    // several (over the same store), and keeps serving as one.
    let fresh = root.path().join("n2");
    std::fs::create_dir_all(&fresh).unwrap();
    let single = config_text(&fresh, "127.0.0.1:0", "127.0.0.1:0", "")
        .replace("[node]\n", "[node]\nnode_id = \"n2\"\n")
        .replace("backend = \"file\"", &etcd(&fake));
    let node = Node::start(single.parse().unwrap()).await.unwrap();
    node.shutdown().await.unwrap();
    let solo = "\n[buckets.solo]\nreplicas = 1\nmin_write_replicas = 1\n";
    let text = node_config(&fresh, &pki, &etcd(&fake), "n2", "n2", free_port(), 256) + solo;
    let node = Node::start(text.parse().unwrap()).await.unwrap();
    let client = support::client(&format!("http://{}", node.gateway_addr()));
    client.create_bucket().bucket("solo").send().await.unwrap();
    eventually("the node opens its replicas of the bucket", || async {
        let answer = shards_of(&node).await;
        let shards = answer["shards"].as_array().unwrap().clone();
        !shards.is_empty() && shards.iter().all(|entry| entry["source"] == "replica")
    })
    .await;
    node.shutdown().await.unwrap();

    // Once it holds a bucket's shards, it does not start as a single node.
    let error = Node::start(single.parse().unwrap()).await.unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("served as one node of several"),
        "{message}"
    );
}
