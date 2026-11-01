//! Erasure coding in the node binary (plan M8-07a; design §8.2–§8.5,
//! §8.7), on four nodes over the in-process stand-in for etcd: a large
//! object of a `local` bucket is coded and its members drop their
//! replicas; it reads back whole through every node; a fragment no layout
//! names is reclaimed as an orphan, and an operator tool's certificate
//! cannot write fragments; an overwrite codes the new version and
//! releases the old version's fragments, which their nodes reclaim; and
//! the new version reads back through the others while each node that
//! does not lead the shard is down in turn.

mod support;

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;
use skys3::Node;
use skys3_control::etcd_fake::Fake;
use skys3_ec::fragment::{FragmentHeader, ObjectMeta, StripeInfo};
use skys3_ec::{FragmentClient, FragmentWriter};
use skys3_io::RealDisk;
use skys3_log::ShardRef;
use skys3_net::{TokioNetwork, Transport};
use skys3_types::{
    AttemptId, BucketId, CodecId, ETag, Epoch, EpochSeq, Geometry, NodeAddress, NodeId, Seq,
    ShardId,
};
use support::cluster::{CLUSTER, Pki, etcd, node_config};
use support::process::free_port;
use support::{admin_json, http_get};

/// Short replication timings, so a primary removes a stopped member
/// within seconds, and erasure coding as the end-to-end scenario runs it:
/// 2+1 stripes from three eligible nodes, coded at once, with the
/// released fragments reclaimed at the next sweep and orphans asked about
/// after a second.
const SETTINGS: &str = "
[replication]
lease_renew_interval_ms = 200
primary_lease_ms = 1000
primary_grace_ms = 1600
member_suspect_after_ms = 1000
replica_ack_timeout_ms = 2500

[cache]
hot_cache_bytes_per_node = 0
reserve_fraction = 0.0

[ec]
enabled = true
min_eligible_nodes = 3
parity_fragments = 1
fragment_release_delay_seconds = 0
fragment_orphan_after_seconds = 1
";

/// The large object's size: past `ec_min_object_bytes`, and several log
/// segments long.
const LARGE: usize = 600 * 1024;

/// How long a test waits for the cluster.
const WAIT: Duration = Duration::from_secs(60);

/// Four nodes coding a `local` bucket of one shard of three members.
struct Cluster {
    root: tempfile::TempDir,
    pki: Pki,
    store: String,
    names: Vec<String>,
    ports: Vec<u16>,
    nodes: Vec<Option<Node>>,
    _fake: Fake,
}

impl Cluster {
    async fn start() -> Self {
        let root = tempfile::tempdir().unwrap();
        let pki = Pki::new(root.path());
        let fake = Fake::start(None).await;
        let store = etcd(&fake);
        let names: Vec<String> = (1..=4).map(|n| format!("n{n}")).collect();
        let ports: Vec<u16> = names.iter().map(|_| free_port()).collect();
        let mut cluster = Self {
            root,
            pki,
            store,
            names,
            ports,
            nodes: Vec::new(),
            _fake: fake,
        };
        for i in 0..4 {
            let node = cluster.boot(i).await;
            cluster.nodes.push(Some(node));
        }
        cluster
    }

    fn dir(&self, i: usize) -> std::path::PathBuf {
        self.root.path().join(&self.names[i])
    }

    async fn boot(&self, i: usize) -> Node {
        let dir = self.dir(i);
        std::fs::create_dir_all(&dir).unwrap();
        let text = node_config(
            &dir,
            &self.pki,
            &self.store,
            &self.names[i],
            &self.names[i],
            self.ports[i],
            256,
        )
        .replace(
            "shards_per_bucket = 4",
            "shards_per_bucket = 1\nreplicas = 3\nec_after_seconds = 0\n\
             ec_min_object_bytes = 262144",
        )
        .replace("[storage]\n", "[storage]\nsegment_bytes = 131072\n")
            + SETTINGS;
        Node::start(text.parse().unwrap()).await.unwrap()
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

    fn running(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.nodes.len()).filter(|i| self.nodes[*i].is_some())
    }

    /// The sum of one series over the nodes that run.
    async fn total(&self, series: &str) -> f64 {
        let mut total = 0.0;
        for i in self.running() {
            total += metric(&self.admin(i), series).await;
        }
        total
    }

    /// The nodes whose fragment stores hold fragments.
    async fn holders(&self) -> Vec<usize> {
        let mut holders = Vec::new();
        for i in self.running() {
            if metric(&self.admin(i), "skys3_fragment_stored_bytes").await > 0.0 {
                holders.push(i);
            }
        }
        holders
    }

    /// The bucket's one shard as the first node that reports it placed
    /// knows it: with three members and a primary. A node that holds no
    /// replica of the shard reports no members.
    async fn placed(&self) -> Option<serde_json::Value> {
        for i in self.running() {
            let shard = self.shard(i).await;
            let members = shard["members"].as_array().map_or(0, Vec::len);
            if members == 3 && shard["primary"].is_string() {
                return Some(shard);
            }
        }
        None
    }

    /// The bucket's one shard as `node` knows it.
    async fn shard(&self, node: usize) -> serde_json::Value {
        let answer = admin_json(&self.admin(node), "/v1/shards").await;
        answer["shards"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["bucket"] == "coded")
            .cloned()
            .unwrap_or_default()
    }

    /// The bytes of the log's bulk segments on node `i`'s disks.
    fn bulk_bytes(&self, i: usize) -> u64 {
        ["disk-a", "disk-b"]
            .iter()
            .flat_map(|disk| std::fs::read_dir(self.dir(i).join(disk)).unwrap())
            .map(|entry| entry.unwrap())
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("bulk-"))
            .map(|entry| entry.metadata().unwrap().len())
            .sum()
    }
}

/// The value of one series of a node's `/metrics`, or 0 if it is absent.
async fn metric(admin: &str, series: &str) -> f64 {
    let (_, text) = http_get(admin, "/metrics").await.unwrap();
    text.lines()
        .find_map(|line| line.strip_prefix(series)?.strip_prefix(' '))
        .map_or(0.0, |value| value.parse().unwrap())
}

/// Polls `condition` every 100 ms until it holds, for up to [`WAIT`].
async fn until<F, Fut>(what: &str, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + WAIT;
    while tokio::time::Instant::now() < deadline {
        if condition().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting until {what}");
}

/// Writes `data` at `coded/key` through `client`, retrying while the
/// primary waits for its leases.
async fn put(client: &aws_sdk_s3::Client, key: &str, data: &[u8]) {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let sent = client
            .put_object()
            .bucket("coded")
            .key(key)
            .body(data.to_vec().into())
            .send()
            .await;
        match sent {
            Ok(_) => return,
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => panic!("PUT {key}: {error:?}"),
        }
    }
}

/// Whether `client` reads `data` at `coded/key`.
async fn reads(client: &aws_sdk_s3::Client, key: &str, data: &[u8]) -> bool {
    match client.get_object().bucket("coded").key(key).send().await {
        Ok(object) => object
            .body
            .collect()
            .await
            .is_ok_and(|body| body.into_bytes() == data),
        Err(_) => false,
    }
}

/// A 64-byte fragment of a 2+1 stripe of `key` in `shard`, written by an
/// attempt no primary started: no layout ever names it.
fn stray_fragment(shard: ShardRef, key: &str) -> (FragmentHeader, Bytes) {
    let position = EpochSeq::new(Epoch::new(1), Seq::new(1));
    let header = FragmentHeader {
        shard,
        key: key.to_owned(),
        version: position,
        attempt: AttemptId::new(Epoch::new(1), 1_000_000),
        stripe: StripeInfo {
            number: 0,
            count: 1,
            offset: 0,
            data_len: 128,
            geometry: Geometry::new(2, 1).unwrap(),
            codec: CodecId::REED_SOLOMON_V1,
        },
        index: 0,
        object: ObjectMeta {
            size: 128,
            last_modified_ms: 1_767_225_600_000,
            etag: ETag::new("9b2cf535f27731c974343645a3985328").unwrap(),
            identity: position,
            metadata: BTreeMap::new(),
            tags: BTreeMap::new(),
            checksums: BTreeMap::new(),
            parts: Vec::new(),
        },
    };
    (header, Bytes::from(vec![7; 64]))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn objects_are_coded_read_degraded_released_and_orphans_reclaimed() {
    let mut cluster = Cluster::start().await;
    cluster
        .client(0)
        .create_bucket()
        .bucket("coded")
        .send()
        .await
        .unwrap();
    until("the shard has three members", || async {
        cluster.placed().await.is_some()
    })
    .await;
    let shard = cluster.placed().await.unwrap();
    let primary_name = shard["primary"].as_str().unwrap().to_owned();
    let primary = cluster
        .names
        .iter()
        .position(|n| *n == primary_name)
        .unwrap();
    let members: Vec<usize> = shard["members"]
        .as_array()
        .unwrap()
        .iter()
        .map(|member| {
            let name = member.as_str().unwrap();
            cluster.names.iter().position(|n| n == name).unwrap()
        })
        .collect();
    let bucket_id = BucketId::new(shard["bucket_id"].as_str().unwrap()).unwrap();
    for &member in &members {
        until("every member holds its replica", || async {
            cluster.shard(member).await["source"] == "replica"
        })
        .await;
    }

    // The large object is coded, as 2+1 on three of the four nodes; the
    // small one stays replicated.
    let large = support::body(1, LARGE);
    let small = support::body(2, 4096);
    put(&cluster.client(1), "large", &large).await;
    put(&cluster.client(2), "small", &small).await;
    until("the large object is coded", || async {
        cluster.total("skys3_encoded_objects_total").await >= 1.0
            && cluster.holders().await.len() == 3
    })
    .await;
    assert_eq!(cluster.total("skys3_encoded_objects_total").await, 1.0);

    // The members drop their replicas: only the last bulk segment, which
    // still takes records, may hold some of its bytes.
    for &member in &members {
        until("the member dropped the replica", || async {
            cluster.bulk_bytes(member) < (LARGE / 2) as u64
        })
        .await;
    }

    // Both read back through every node, from data fragments alone.
    for i in 0..4 {
        let client = cluster.client(i);
        assert!(reads(&client, "large", &large).await, "through n{}", i + 1);
        assert!(reads(&client, "small", &small).await, "through n{}", i + 1);
    }
    let parity = "skys3_fragment_reads_total{fragment=\"parity\",outcome=\"read\"}";
    assert_eq!(cluster.total(parity).await, 0.0, "a healthy read decoded");
    assert!(
        cluster
            .total("skys3_fragment_reads_total{fragment=\"data\",outcome=\"read\"}")
            .await
            > 0.0
    );

    // A fragment of an attempt no primary started is an orphan: its node
    // asks the primary, and reclaims it.
    let target = (0..4).find(|i| *i != primary).unwrap();
    let address: NodeAddress = format!("127.0.0.1:{}", cluster.ports[target])
        .parse()
        .unwrap();
    let target_id = NodeId::new(&cluster.names[target]).unwrap();
    let peers = BTreeMap::from([(target_id.clone(), address)]);
    let shard_ref = ShardRef::new(bucket_id, ShardId::new(0));
    let (header, data) = stray_fragment(shard_ref, "stray");
    let tool = cluster.pki.cluster.tool(CLUSTER, "ops");
    let refused = FragmentClient::<TokioNetwork, RealDisk>::new(
        Transport::new(
            TokioNetwork,
            &tool.credentials(CLUSTER, &cluster.pki.cluster),
        ),
        peers.clone(),
    );
    assert!(
        refused
            .write(&target_id, &header, data.clone())
            .await
            .is_err(),
        "an operator tool wrote a fragment"
    );
    let intruder = cluster.pki.cluster.node(CLUSTER, "n9");
    let writer = FragmentClient::<TokioNetwork, RealDisk>::new(
        Transport::new(
            TokioNetwork,
            &intruder.credentials(CLUSTER, &cluster.pki.cluster),
        ),
        peers,
    );
    let admin = cluster.admin(target);
    let reclaimed = "skys3_fragment_reclaimed_bytes_total";
    let before = metric(&admin, reclaimed).await;
    writer.write(&target_id, &header, data).await.unwrap();
    until("the orphan is reclaimed", || async {
        metric(&admin, reclaimed).await >= before + 64.0
    })
    .await;

    // An overwrite codes the new version, and the old version's fragments
    // are released to their nodes, which reclaim them.
    let released = cluster.total("skys3_fragment_released_bytes_total").await;
    let encoded = cluster.total("skys3_encoded_objects_total").await;
    let replaced = support::body(3, LARGE);
    put(&cluster.client(primary), "large", &replaced).await;
    until("the new version is coded", || async {
        cluster.total("skys3_encoded_objects_total").await > encoded
    })
    .await;
    until(
        "the old version's fragments are released and reclaimed",
        || async {
            cluster.total("skys3_fragment_released_bytes_total").await
                >= released + LARGE as f64 / 2.0
                && cluster.total("skys3_fragment_releasing_bytes").await == 0.0
        },
    )
    .await;
    for i in 0..4 {
        assert!(reads(&cluster.client(i), "large", &replaced).await);
    }

    // With each node that does not lead the shard down in turn, the new
    // version reads back through another node, decoded where the node held
    // a data fragment. Three of the four nodes hold a fragment each, so at
    // least one data fragment is on a node that does not lead. This comes
    // last: without a coordinator, the members shut down are removed from
    // the shard and not admitted again, which may leave it too few members
    // to take writes.
    let mut parity_reads = 0.0;
    for down in (0..4).filter(|i| *i != primary) {
        cluster.nodes[down]
            .take()
            .unwrap()
            .shutdown()
            .await
            .unwrap();
        let reader = (down + 1..down + 4)
            .map(|i| i % 4)
            .find(|i| *i != primary)
            .unwrap();
        let before = metric(&cluster.admin(reader), parity).await;
        let client = cluster.client(reader);
        until("the object reads back with a node down", || {
            reads(&client, "large", &replaced)
        })
        .await;
        parity_reads += metric(&cluster.admin(reader), parity).await - before;
        let node = cluster.boot(down).await;
        cluster.nodes[down] = Some(node);
    }
    assert!(parity_reads > 0.0, "no read decoded around a node down");

    for node in cluster.nodes.iter_mut().filter_map(Option::take) {
        node.shutdown().await.unwrap();
    }
}
