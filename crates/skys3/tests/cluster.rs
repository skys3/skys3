//! Nodes of a cluster (plan M8-04; design §6.2, §6.7, §12), on the
//! in-process stand-in for etcd: they register, elect a coordinator, send
//! it heartbeats, and take pushed generations over the intra-cluster
//! transport; a node refuses a peer from another CA or another cluster,
//! caps its in-flight handshakes, and does not start over another data
//! directory's registration of its ID. Replication, routing, takeover, and
//! re-admission (plan M8-05b) run on them too.

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use rustls::pki_types::{PrivateKeyDer, ServerName};
use skys3::{Node, NodeError, StartError};
use skys3_control::etcd_fake::Fake;
use skys3_coord::{ControlChanged, Heartbeat, HeartbeatAck};
use skys3_net::{ALPN_PROTOCOL, Frame, Header, MessageKind, TokioNetwork, Transport};
use skys3_types::{
    Generation, Label, NodeAddress, NodeId, NodeRegistration, ProposalId, RegisterDocument,
};
use support::cluster::{CLUSTER, Leaf, Pki, WAIT, etcd, node_config};
use support::process::free_port;
use support::{admin_json, config_text, eventually, http_get};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

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

/// The transport of `tool`, a node certificate of the cluster that no node
/// registers, to send nodes admin messages with.
fn tool_transport(pki: &Pki) -> Transport<TokioNetwork> {
    let credentials = pki
        .cluster
        .node(CLUSTER, "tool")
        .credentials(CLUSTER, &pki.cluster);
    Transport::new(TokioNetwork, &credentials)
}

/// The node `coordinator.lease` names, as `node`'s `GET /v1/cluster`
/// reports it, if any.
async fn lease_holder(node: &Node) -> Option<String> {
    let view = admin_json(&node.admin_addr().to_string(), "/v1/cluster").await;
    view["coordinator"]["holder"].as_str().map(str::to_owned)
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
    let tool = tool_transport(&pki);

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

    // One of them is elected: every node may be (plan M8-09).
    let deadline = tokio::time::Instant::now() + WAIT;
    let holder = loop {
        if let Some(holder) = lease_holder(&nodes[0]).await {
            break holder;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no node holds the coordinator lease"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(["n1", "n2", "n3"].contains(&holder.as_str()), "{holder}");

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
                && state["heartbeats"]["coordinator"] == holder.as_str()
        })
        .await;
        let state = cluster_state(node).await;
        assert_eq!(state["multi_node"], true);
        assert_eq!(state["registration"]["registered"], true);
        assert_eq!(state["registration"]["error"], serde_json::Value::Null);
        assert_eq!(state["transport"]["listen"], format!("127.0.0.1:{port}"));
        assert_eq!(state["transport"]["advertise"], format!("127.0.0.1:{port}"));
        assert_eq!(state["transport"]["max_inflight_handshakes"], 256);
    }

    // The lease holder answers a heartbeat as coordinator, and any other
    // node as a node that does not coordinate.
    for (i, port) in ports.iter().enumerate() {
        let id = format!("n{}", i + 1);
        let answer = ask(&tool, &id, *port, &Heartbeat::frame(7)).await;
        let ack = HeartbeatAck::from_frame(&answer, 7).unwrap();
        assert_eq!(ack.coordinator, id == holder, "{id}: {ack:?}");
    }

    // A pushed generation reaches the node's hints, and costs a read of
    // `cluster.json` even when no change stands behind it.
    let pushed = Generation::new(1000);
    let reply = ask(&tool, "n1", ports[0], &ControlChanged::frame(pushed, 9)).await;
    assert_eq!(reply.header.request_id, 9);
    eventually("the push is recorded", || async {
        cluster_state(&nodes[0]).await["pushed_generation"].as_u64() >= Some(1000)
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

    // Every node over the store is a node of the cluster: one without the
    // transport's certificates does not load (plan M8-09).
    let single = root.path().join("single");
    std::fs::create_dir_all(&single).unwrap();
    let text = config_text(&single, "127.0.0.1:0", "127.0.0.1:0", "")
        .replace("backend = \"file\"", &store);
    let error = text.parse::<skys3_config::Config>().unwrap_err();
    assert!(
        error.to_string().contains("transport.tls_cert_file"),
        "{error}"
    );

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

    // A node's connection whose first frame no service takes, a reply, is
    // closed, as is one that ends before its first frame, and the listener
    // goes on.
    let peer = pki
        .cluster
        .node(CLUSTER, "n2")
        .credentials(CLUSTER, &pki.cluster);
    let transport = Transport::new(TokioNetwork, &peer);
    let address: NodeAddress = format!("127.0.0.1:{port}").parse().unwrap();
    let n1 = NodeId::new("n1").unwrap();
    let mut connection = transport.connect(&n1, &address).await.unwrap();
    let reply = Frame::new(Header::new(MessageKind::ForwardReply), Vec::new());
    connection.send(&reply).await.unwrap();
    let answer = tokio::time::timeout(WAIT, connection.recv()).await.unwrap();
    assert!(
        matches!(answer, Ok(None) | Err(_)),
        "an answer to ForwardReply"
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

/// Replication timing short enough for a test: a member takes over about
/// two seconds after its primary stops, and the coordinator (with its
/// two-second lease) removes a member a second after another.
const FAST_REPLICATION: &str = "
[replication]
lease_renew_interval_ms = 200
primary_lease_ms = 1000
primary_grace_ms = 1600
member_suspect_after_ms = 1000
replica_ack_timeout_ms = 2500
removal_interval_seconds = 1

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
    let text = node_config(&dir, pki, store, name, name, port, 256).replace(
        "config_poll_interval_seconds = 1",
        "config_poll_interval_seconds = 1\ncoordinator_lease_seconds = 2",
    ) + FAST_REPLICATION;
    text.parse().unwrap()
}

/// `GET /v1/shards` of `node`.
/// Every file under `dir`, by its path, with its bytes.
fn contents(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut files = BTreeMap::new();
    let mut dirs = vec![dir.to_owned()];
    while let Some(next) = dirs.pop() {
        for entry in std::fs::read_dir(&next).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                dirs.push(path);
            } else {
                let bytes = std::fs::read(&path).unwrap();
                files.insert(path.display().to_string(), bytes);
            }
        }
    }
    files
}

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
        Err(error) => {
            eprintln!("GET {bucket}/{key}: {error:?}");
            false
        }
    }
}

/// Whether `client` reads `data` at `bucket/key` within [`WAIT`]: once a
/// node stops, each shard it led serves reads again only after its own
/// takeover and its new primary's leases, which may come after another
/// shard's, and the test's clients do not retry.
async fn reads_within_wait(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    data: &[u8],
) -> bool {
    let deadline = tokio::time::Instant::now() + WAIT;
    while tokio::time::Instant::now() < deadline {
        if reads(client, bucket, key, data).await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
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
    // The coordinator may already have handed the shard off to balance
    // primaries, which moves its epoch on.
    let first_epoch = first["epoch"].as_u64().unwrap();
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
    // Every member of a shard applies the same records (plan M8-08), and
    // the writes reached the logs of the shards they went to. Which shards
    // those are depends on the bucket's ID, drawn at random: one of the 4
    // shards gets none of the 12 keys in about one run in eight, and stays
    // at seq 0.
    let id = skys3_types::BucketId::new(&bucket_id).unwrap();
    let count = skys3_types::ShardCount::new(4).unwrap();
    let written_to: std::collections::BTreeSet<u64> = written
        .iter()
        .map(|(key, _)| u64::from(skys3_types::shard_for_key(&id, key.as_bytes(), count).get()))
        .collect();
    eventually(
        "the members of every shard applied the same records",
        || async {
            let mut applied = std::collections::BTreeMap::<u64, Vec<String>>::new();
            for host in nodes.iter().flatten() {
                for entry in shards_of(host).await["shards"].as_array().unwrap() {
                    let position = entry["applied"].as_str().unwrap().to_owned();
                    applied
                        .entry(entry["shard"].as_u64().unwrap())
                        .or_default()
                        .push(position);
                }
            }
            applied.len() == 4
                && applied.iter().all(|(shard, positions)| {
                    positions.len() == 3
                        && positions.iter().all(|position| *position == positions[0])
                        && (!written_to.contains(shard)
                            || positions[0]
                                .parse::<skys3_types::EpochSeq>()
                                .is_ok_and(|position| position.seq.get() > 0))
                })
        },
    )
    .await;
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
    assert!(after["epoch"].as_u64() > Some(first_epoch), "{after}");
    assert_ne!(after["primary"], primary.as_str(), "{after}");
    assert_eq!(after["members"].as_array().unwrap().len(), 2, "{after}");
    // Shard 0 serves again; a shard the stopped node also led may still be
    // taking over, and every shard it was a member of drops it.
    for (key, data) in &written {
        assert!(
            reads_within_wait(&clients[alive], "replicated", key, data).await,
            "{key}"
        );
    }

    // Restarted, the old primary is re-admitted: the coordinator adds it
    // back as a learner, now or already while it was down, since it is the
    // only node the shard can take (plan M8-06), and the shard's new
    // primary backfills it and promotes it to member.
    let config = replicated_config(root.path(), &pki, &store, &primary, ports[stopped]);
    nodes[stopped] = Some(Node::start(config).await.unwrap());
    let restarted = nodes[stopped].as_ref().unwrap();
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
    let data = dir.join("data");
    let before = contents(&data);

    // Its data directory holds a bucket's shards: it does not start as a
    // node of several, and is left as it was, byte for byte.
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
    let after = contents(&data);
    let changed: BTreeSet<_> = before
        .keys()
        .chain(after.keys())
        .filter(|path| before.get(*path) != after.get(*path))
        .collect();
    assert!(
        changed.is_empty(),
        "the refused start changed the data directory: {changed:?}"
    );

    // A node of a cluster serves as one; whether a data directory that
    // holds no bucket's shards may change modes is `datadir`'s unit test,
    // since a node changes control stores with it, and so its copy.
    let fresh = root.path().join("n2");
    std::fs::create_dir_all(&fresh).unwrap();
    let single = config_text(&fresh, "127.0.0.1:0", "127.0.0.1:0", "")
        .replace("[node]\n", "[node]\nnode_id = \"n2\"\n");
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
        message.contains("served as one of a cluster's nodes"),
        "{message}"
    );
    assert!(message.contains("file control store"), "{message}");
}
