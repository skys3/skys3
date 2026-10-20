//! Nodes with `[cluster] coordinator = true` (plan M8-06; design §6.7,
//! §12), on the in-process stand-in for etcd: one of them is elected
//! coordinator, and another takes over when it stops; operators hand
//! shards off, and drain nodes, through any node's admin API; a handoff
//! request reaches a primary only from the coordinator or an `admin`
//! certificate (threat T6); the coordinator re-admits a node a takeover
//! left out and replaces a lost member; and a bucket deleted through one
//! node is dropped by every node and its shard registers by the
//! coordinator.

mod support;

use std::path::Path;
use std::time::Duration;

use serde_json::{Value, json};
use skys3::Node;
use skys3_control::etcd_fake::Fake;
use skys3_coord::{Handoff, HandoffClient, HandoffError};
use skys3_net::{TokioNetwork, Transport};
use skys3_types::{BucketId, Epoch, NodeAddress, NodeId, ShardId};
use support::cluster::{CLUSTER, Pki, WAIT, etcd, node_config};
use support::process::free_port;
use support::{admin_json, eventually, http_get, http_send};

/// Short timings: a member takes over about two seconds after its primary
/// stops, a coordinator about two seconds after the last renewal of a
/// stopped one, and replacement removes a member a second after another.
const FAST: &str = "
[replication]
lease_renew_interval_ms = 200
primary_lease_ms = 1000
primary_grace_ms = 1600
member_suspect_after_ms = 1000
replica_ack_timeout_ms = 2500
removal_interval_seconds = 1
rebalance_handoff_interval_seconds = 1
rebalance_handoff_retry_seconds = 2
";

/// The configuration of node `name` of the cluster in `root`, a candidate
/// for coordinator, its transport on `port`.
fn coordinated_config(
    root: &Path,
    pki: &Pki,
    store: &str,
    name: &str,
    port: u16,
) -> skys3_config::Config {
    let dir = root.join(name);
    std::fs::create_dir_all(&dir).unwrap();
    let text = node_config(&dir, pki, store, name, name, port, 256)
        .replace("multi_node = true", "multi_node = true\ncoordinator = true")
        .replace(
            "config_poll_interval_seconds = 1",
            "config_poll_interval_seconds = 1\ncoordinator_lease_seconds = 2",
        )
        + FAST;
    text.parse().unwrap()
}

/// A cluster of nodes in a temporary directory, over the stand-in for etcd.
struct Cluster {
    root: tempfile::TempDir,
    pki: Pki,
    fake: Fake,
    store: String,
    names: Vec<String>,
    ports: Vec<u16>,
    nodes: Vec<Option<Node>>,
}

impl Cluster {
    async fn start(count: usize) -> Self {
        let root = tempfile::tempdir().unwrap();
        let pki = Pki::new(root.path());
        let fake = Fake::start(None).await;
        let store = etcd(&fake);
        let names: Vec<String> = (1..=count).map(|n| format!("n{n}")).collect();
        let ports: Vec<u16> = names.iter().map(|_| free_port()).collect();
        let mut cluster = Self {
            root,
            pki,
            fake,
            store,
            names,
            ports,
            nodes: Vec::new(),
        };
        for i in 0..count {
            let node = cluster.boot(i).await;
            cluster.nodes.push(Some(node));
        }
        cluster
    }

    async fn boot(&self, i: usize) -> Node {
        let config = coordinated_config(
            self.root.path(),
            &self.pki,
            &self.store,
            &self.names[i],
            self.ports[i],
        );
        Node::start(config).await.unwrap()
    }

    fn node(&self, i: usize) -> &Node {
        self.nodes[i].as_ref().expect("the node runs")
    }

    fn admin(&self, i: usize) -> String {
        self.node(i).admin_addr().to_string()
    }

    fn client(&self, i: usize) -> aws_sdk_s3::Client {
        support::client(&format!("http://{}", self.node(i).gateway_addr()))
    }

    fn index(&self, name: &str) -> usize {
        self.names.iter().position(|known| known == name).unwrap()
    }

    /// The running nodes, by position.
    fn running(&self) -> Vec<usize> {
        (0..self.nodes.len())
            .filter(|i| self.nodes[*i].is_some())
            .collect()
    }

    /// `GET /v1/cluster` of node `i`.
    async fn view(&self, i: usize) -> Value {
        admin_json(&self.admin(i), "/v1/cluster").await
    }

    /// The node every running node agrees is coordinator, and that says
    /// it serves a tenure, once there is one.
    async fn coordinator(&self) -> usize {
        let deadline = tokio::time::Instant::now() + WAIT;
        loop {
            if let Some(found) = self.agreed_coordinator().await {
                return found;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "no node became coordinator"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// The node every running node names as the lease holder, if that
    /// node alone says it serves a tenure.
    async fn agreed_coordinator(&self) -> Option<usize> {
        let mut holders = Vec::new();
        let mut serving = Vec::new();
        for i in self.running() {
            let view = self.view(i).await;
            holders.push(view["coordinator"]["holder"].clone());
            if view["coordinator"]["this_node"] == true {
                serving.push(i);
            }
        }
        let agreed = holders.windows(2).all(|pair| pair[0] == pair[1]);
        match serving.as_slice() {
            [one] if agreed && holders[0] == self.names[*one].as_str() => Some(*one),
            _ => None,
        }
    }

    /// Shard `n` of `bucket` in node `i`'s `/v1/shards`.
    async fn shard(&self, i: usize, bucket: &str, n: u64) -> Value {
        let answer = admin_json(&self.admin(i), "/v1/shards").await;
        answer["shards"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["bucket"] == bucket && entry["shard"] == n)
            .cloned()
            .unwrap_or(Value::Null)
    }

    /// Whether every running node holds a replica of every shard of
    /// `bucket` with `members` members, none of them a learner.
    async fn settled(&self, bucket: &str, members: usize) -> bool {
        for i in self.running() {
            let answer = admin_json(&self.admin(i), "/v1/shards").await;
            let shards: Vec<&Value> = answer["shards"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|entry| entry["bucket"] == bucket)
                .collect();
            let whole = shards.len() == 4
                && shards.iter().all(|entry| {
                    entry["source"] == "replica"
                        && entry["members"].as_array().unwrap().len() == members
                        && entry["learners"].as_array().unwrap().is_empty()
                });
            if !whole {
                return false;
            }
        }
        true
    }

    async fn shutdown(self) {
        for node in self.nodes.into_iter().flatten() {
            node.shutdown().await.unwrap();
        }
    }
}

/// PUTs `data` as `bucket/key`, retrying while the shard's primary changes,
/// as an SDK would.
async fn put(client: &aws_sdk_s3::Client, bucket: &str, key: &str, data: &[u8]) {
    for _ in 0..100 {
        let put = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(data.to_vec().into())
            .send()
            .await;
        if put.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("PUT {bucket}/{key} never succeeded");
}

/// Whether `client` reads `data` at `bucket/key`, retrying while the
/// shard's primary changes, as `put` does; the last failure is printed.
async fn reads(client: &aws_sdk_s3::Client, bucket: &str, key: &str, data: &[u8]) -> bool {
    let mut failure = String::new();
    for _ in 0..50 {
        match client.get_object().bucket(bucket).key(key).send().await {
            Ok(object) => match object.body.collect().await {
                Ok(body) => return body.into_bytes() == data,
                Err(error) => failure = format!("{error:?}"),
            },
            Err(error) => failure = format!("{error:?}"),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    eprintln!("GET {bucket}/{key} never succeeded: {failure}");
    false
}

/// `POST path` with `body` on the admin listener at `addr`: the status
/// and the JSON answer.
async fn post(addr: &str, method: &str, path: &str, body: &str) -> (u16, Value) {
    let (status, text) = http_send(addr, method, path, body).await.unwrap();
    let answer = serde_json::from_str(&text).unwrap_or_else(|error| panic!("{error}: {text}"));
    (status, answer)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_coordinator_is_elected_hands_off_and_is_succeeded() {
    let mut cluster = Cluster::start(3).await;
    let coordinator = cluster.coordinator().await;
    let coord_name = cluster.names[coordinator].clone();
    // Only the coordinator reports itself, and its placement judgement.
    let mut gauges = 0.0;
    for i in cluster.running() {
        let (_, metrics) = http_get(&cluster.admin(i), "/metrics").await.unwrap();
        let value = metrics
            .lines()
            .find_map(|line| line.strip_prefix("skys3_coordinator "))
            .unwrap();
        gauges += value.parse::<f64>().unwrap();
    }
    assert_eq!(gauges, 1.0);
    eventually("the coordinator judges placement", || async {
        cluster.view(coordinator).await["placement"].is_object()
    })
    .await;
    let health = admin_json(&cluster.admin(coordinator), "/v1/health").await;
    assert_eq!(health["placement"]["failure_domain"], "node", "{health}");
    assert_eq!(health["placement"]["eligible_nodes"], 3, "{health}");
    let other = (coordinator + 1) % 3;
    let view = cluster.view(other).await;
    assert_eq!(view["placement"], Value::Null, "{view}");
    assert_eq!(view["coordinator"]["candidate"], true, "{view}");
    assert_eq!(view["nodes"].as_array().unwrap().len(), 3, "{view}");
    // The coordinator judges the nodes' health from their heartbeats.
    eventually("the coordinator hears from every node", || async {
        let view = cluster.view(coordinator).await;
        view["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .all(|node| node["state"] == "live" && node["silent_seconds"].as_f64().unwrap() < 5.0)
    })
    .await;

    // A bucket, written through every node.
    cluster
        .client(0)
        .create_bucket()
        .bucket("coordinated")
        .send()
        .await
        .unwrap();
    eventually("every node holds every shard", || {
        cluster.settled("coordinated", 3)
    })
    .await;
    let mut written = Vec::new();
    for i in 0..6_u8 {
        let (key, data) = (format!("key-{i}"), support::body(i, 2000));
        put(
            &cluster.client(usize::from(i) % 3),
            "coordinated",
            &key,
            &data,
        )
        .await;
        written.push((key, data));
    }

    // A handoff through a node that neither leads the shard nor
    // coordinates is refused, naming both.
    let first = cluster.shard(0, "coordinated", 0).await;
    let primary = cluster.index(first["primary"].as_str().unwrap());
    let epoch = first["epoch"].as_u64().unwrap();
    let path = "/v1/shards/coordinated/0/handoff";
    if let Some(bystander) = (0..3).find(|i| *i != primary && *i != coordinator) {
        let (status, answer) = post(&cluster.admin(bystander), "POST", path, "").await;
        assert_eq!(status, 409, "{answer}");
        assert_eq!(answer["primary"], cluster.names[primary].as_str());
        assert_eq!(answer["coordinator"], coord_name.as_str());
    }
    // Malformed requests.
    let (status, _) = post(&cluster.admin(0), "POST", path, "{\"to\": 3}").await;
    assert_eq!(status, 400);
    let (status, _) = post(&cluster.admin(0), "POST", path, "{\"to\": \"n9\"}").await;
    assert_eq!(status, 409, "n9 is not a member");
    let (status, _) = post(
        &cluster.admin(0),
        "POST",
        "/v1/shards/coordinated/9/handoff",
        "",
    )
    .await;
    assert_eq!(status, 404);
    let (status, _) = post(&cluster.admin(0), "GET", path, "").await;
    assert_eq!(status, 405);

    // Through the coordinator, which chooses the member, the primary
    // moves, and writes and reads go on.
    let (status, answer) = post(&cluster.admin(coordinator), "POST", path, "").await;
    assert_eq!(status, 202, "{answer}");
    let via = if primary == coordinator {
        "primary"
    } else {
        "coordinator"
    };
    assert_eq!(answer["via"], via, "{answer}");
    assert_eq!(answer["from"], cluster.names[primary].as_str(), "{answer}");
    let to = answer["to"].as_str().unwrap().to_owned();
    assert_ne!(to, cluster.names[primary]);
    eventually("the shard's primary moved", || async {
        let entry = cluster.shard(cluster.index(&to), "coordinated", 0).await;
        entry["primary"] == to.as_str() && entry["epoch"].as_u64().unwrap() > epoch
    })
    .await;
    for (i, (key, data)) in written.iter().enumerate() {
        put(&cluster.client(i % 3), "coordinated", key, data).await;
        assert!(reads(&cluster.client((i + 1) % 3), "coordinated", key, data).await);
    }
    // The old primary left the shard; the coordinator adds it back as a
    // learner, the only node it can, and the new primary promotes it.
    eventually("the old primary is re-admitted", || async {
        let entry = cluster.shard(primary, "coordinated", 0).await;
        entry["source"] == "replica"
            && entry["role"] == "member"
            && entry["members"].as_array().unwrap().len() == 3
    })
    .await;

    // Threat T6: a valid node certificate of the cluster cannot ask a
    // primary for a handoff; an operator tool's can.
    let entry = cluster.shard(0, "coordinated", 1).await;
    let leader = NodeId::new(entry["primary"].as_str().unwrap()).unwrap();
    let address: NodeAddress = format!(
        "127.0.0.1:{}",
        cluster.ports[cluster.index(leader.as_str())]
    )
    .parse()
    .unwrap();
    let member = entry["members"]
        .as_array()
        .unwrap()
        .iter()
        .map(|member| member.as_str().unwrap())
        .find(|member| *member != leader.as_str())
        .unwrap();
    let handoff = Handoff {
        bucket: BucketId::new(entry["bucket_id"].as_str().unwrap()).unwrap(),
        shard: ShardId::new(1),
        epoch: Epoch::new(entry["epoch"].as_u64().unwrap() + 100),
        to: NodeId::new(member).unwrap(),
    };
    let intruder = cluster
        .pki
        .cluster
        .node(CLUSTER, "intruder")
        .credentials(CLUSTER, &cluster.pki.cluster);
    let refused = HandoffClient::new(Transport::new(TokioNetwork, &intruder), WAIT)
        .send(&leader, &address, &handoff)
        .await;
    assert!(
        matches!(&refused, Err(HandoffError::Refused(reason))
            if reason.contains("does not hold the coordinator lease")),
        "{refused:?}"
    );
    let tool = cluster
        .pki
        .cluster
        .tool(CLUSTER, "ops")
        .credentials(CLUSTER, &cluster.pki.cluster);
    // Admitted, and refused by the replica for its epoch only.
    let stale = HandoffClient::new(Transport::<TokioNetwork>::new(TokioNetwork, &tool), WAIT)
        .send(&leader, &address, &handoff)
        .await;
    assert!(
        matches!(&stale, Err(HandoffError::Refused(reason)) if reason.contains("epoch")),
        "{stale:?}"
    );

    // Drains are marked through any node, kept, and cleared.
    let drained = &cluster.names[other];
    let (status, answer) = post(
        &cluster.admin(coordinator),
        "POST",
        &format!("/v1/nodes/{drained}/drain"),
        "",
    )
    .await;
    assert_eq!(status, 200, "{answer}");
    assert_eq!(
        answer,
        json!({"node_id": drained, "draining": true, "changed": true, "generation": answer["generation"]})
    );
    let view = cluster.view(other).await;
    let marked = view["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| node["node_id"] == drained.as_str())
        .unwrap()
        .clone();
    assert_eq!(marked["draining"], true, "{marked}");
    let (status, answer) = post(
        &cluster.admin(other),
        "DELETE",
        &format!("/v1/nodes/{drained}/drain"),
        "",
    )
    .await;
    assert_eq!(
        (status, &answer["draining"]),
        (200, &json!(false)),
        "{answer}"
    );
    let (status, _) = post(&cluster.admin(other), "POST", "/v1/nodes/n9/drain", "").await;
    assert_eq!(status, 404);

    // The coordinator stops. Another node takes the lease, and judges
    // placement again; writes go on through the others.
    cluster.nodes[coordinator]
        .take()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    let successor = cluster.coordinator().await;
    assert_ne!(successor, coordinator);
    eventually("the successor judges placement", || async {
        cluster.view(successor).await["placement"].is_object()
    })
    .await;
    let survivor = cluster.running()[0];
    for (key, data) in &written {
        put(&cluster.client(survivor), "coordinated", key, data).await;
    }
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_member_is_replaced_and_a_deleted_bucket_dropped_everywhere() {
    let mut cluster = Cluster::start(3).await;
    let coordinator = cluster.coordinator().await;
    // The shards each node keeps without a register: its sessions'.
    let health = admin_json(&cluster.admin(coordinator), "/v1/health").await;
    let system = health["shards_open"].as_u64().unwrap();
    cluster
        .client(coordinator)
        .create_bucket()
        .bucket("replaced")
        .send()
        .await
        .unwrap();
    eventually("every node holds every shard", || {
        cluster.settled("replaced", 3)
    })
    .await;
    let data = support::body(7, 1500);
    put(&cluster.client(0), "replaced", "kept", &data).await;

    // A node other than the coordinator stops; its shards' primaries
    // remove it, and the coordinator adds it back as a learner while it
    // is down, the only node it can.
    let lost = (coordinator + 1) % 3;
    let lost_name = cluster.names[lost].clone();
    cluster.nodes[lost]
        .take()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    eventually("every shard names the lost node a learner", || async {
        let mut named = true;
        for n in 0..4 {
            let entry = cluster.shard(coordinator, "replaced", n).await;
            named &= entry["learners"] == json!([lost_name.as_str()])
                && !entry["members"]
                    .as_array()
                    .unwrap()
                    .contains(&json!(lost_name.as_str()));
        }
        named
    })
    .await;
    // Back, it joins as the learner, and is promoted.
    let node = cluster.boot(lost).await;
    cluster.nodes[lost] = Some(node);
    eventually(
        "the returning node is a member of every shard again",
        || cluster.settled("replaced", 3),
    )
    .await;
    assert!(reads(&cluster.client(lost), "replaced", "kept", &data).await);

    // The bucket is deleted through one node. Every other node drops its
    // replicas, and the coordinator the shard registers.
    let shards = admin_json(&cluster.admin(0), "/v1/shards").await;
    let bucket_id = shards["shards"][0]["bucket_id"]
        .as_str()
        .unwrap()
        .to_owned();
    // Rebalancing may be handing a primary to the returning node: retry
    // as an SDK would.
    let mut removed = false;
    for _ in 0..100 {
        let delete = cluster
            .client(0)
            .delete_object()
            .bucket("replaced")
            .key("kept")
            .send()
            .await;
        if delete.is_ok() {
            removed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(removed, "DeleteObject never succeeded");
    let deleter = (coordinator + 2) % 3;
    let mut deleted = false;
    for _ in 0..100 {
        if cluster
            .client(deleter)
            .delete_bucket()
            .bucket("replaced")
            .send()
            .await
            .is_ok()
        {
            deleted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(deleted, "DeleteBucket never succeeded");
    eventually("every node dropped its replicas", || async {
        let mut open = 0;
        for i in cluster.running() {
            let health = admin_json(&cluster.admin(i), "/v1/health").await;
            open += health["shards_open"].as_u64().unwrap();
        }
        open == system * cluster.running().len() as u64
    })
    .await;
    eventually("the coordinator deleted the shard registers", || async {
        (0..4).all(|n| {
            cluster
                .fake
                .get(&format!("{CLUSTER}/shards/{bucket_id}/{n}.json"))
                .is_none()
        })
    })
    .await;
    // Nothing opens them again.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let health = admin_json(&cluster.admin(coordinator), "/v1/health").await;
    assert_eq!(health["shards_open"], system, "{health}");
    cluster.shutdown().await;
}
