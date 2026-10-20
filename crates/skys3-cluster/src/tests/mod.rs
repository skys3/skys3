//! Replicated shards, register following, and asked handoffs on real
//! nodes: shard sets on simulated disks, linked by the transport over
//! loopback TCP, with their registers in an in-memory control store.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType,
};
use skys3_control::{
    ControlStore, Expected, MemoryControlStore, ProposalIds, ProposalOutcome, RetryPolicy,
    TypedKey, propose_document, read,
};
use skys3_gateway::{LocalShards, ShardRef};
use skys3_index::{Index, IndexConfig};
use skys3_io::{BlockingPool, MonotonicClock, SimDisk, SimMount};
use skys3_log::record::{Put, PutData};
use skys3_log::{LogConfig, RecordBody, SegmentLog};
use skys3_net::{
    CertificateDer, Credentials, PeerIdentity, PrivateKeyDer, TokioNetwork, Transport,
};
use skys3_shard::replication::{ControlRegisters, HandedOff, Replication, ReplicationConfig};
use skys3_shard::{AckTimeout, Shard, ShardSet};
use skys3_types::{BucketId, ClusterId, ETag, Epoch, NodeId, ProposalId, ShardConfig, ShardId};

use crate::{ReplicaObserver, ReplicatedShards};

mod follow;
mod handoff;
mod shards;

/// Every wait in these tests.
const WAIT: Duration = Duration::from_secs(30);

/// The replicated shards of a test node.
type Replicas = ReplicatedShards<TokioNetwork, SimMount>;

/// Runs `scenario` on a runtime of its own, within [`WAIT`].
fn run(scenario: impl Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        tokio::time::timeout(WAIT, scenario)
            .await
            .expect("the test finished in time");
    });
}

/// Waits until `done` holds.
async fn until(mut done: impl FnMut() -> bool) {
    while !done() {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// A throwaway node PKI.
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

    /// The transport of `node`, with credentials of its own.
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
    format!("node-{n}").parse().unwrap()
}

fn bucket() -> BucketId {
    BucketId::new("b-1").unwrap()
}

/// Shard `n` of bucket `b-1`.
fn shard(n: u8) -> ShardRef {
    ShardRef {
        bucket: bucket(),
        shard: ShardId::new(n),
    }
}

/// Shard `n` of bucket `b-1` in `epoch`, on `members`, the first its
/// primary.
fn config(n: u8, epoch: u64, members: &[u8]) -> ShardConfig {
    let members: Vec<NodeId> = members.iter().copied().map(node).collect();
    let replicas = u8::try_from(members.len()).unwrap();
    ShardConfig {
        bucket_id: bucket(),
        shard: ShardId::new(n),
        epoch: Epoch::new(epoch),
        primary: members[0].clone(),
        members,
        learners: Vec::new(),
        min_write_replicas: replicas,
        replicas,
        proposal_id: ProposalId::new(format!("p-{n}-{epoch}")).unwrap(),
    }
}

/// Fast links, so that tests need not wait long.
fn timing() -> ReplicationConfig {
    let ms = Duration::from_millis;
    ReplicationConfig {
        beacon_interval: ms(20),
        link_timeout: ms(200),
        reconnect_delay: ms(20),
        lease_renew_interval: ms(50),
        primary_lease: ms(400),
        primary_grace: ms(600),
        ack_timeout: AckTimeout::wait_through(ms(3000)),
        member_suspect_after: ms(5000),
        takeover_delay: ms(40),
    }
}

/// An inline `PUT` of `len` bytes to `key`.
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

/// A node's shard set on a disk of its own.
async fn shard_set(seed: u64) -> (ShardSet<SimMount>, Arc<MonotonicClock>) {
    let disk = SimDisk::new(seed);
    let log_config = LogConfig {
        inline_max_bytes: 512,
        segment_bytes: 64 * 1024,
        group_commit_max_delay: Duration::ZERO,
        group_commit_max_bytes: 16 * 1024,
    };
    let clock = Arc::new(MonotonicClock::new());
    let (log, _) = SegmentLog::open(disk.mount(), log_config, Arc::clone(&clock) as _)
        .await
        .unwrap();
    let index = Index::open_sim(&disk.mount(), "index.redb", &IndexConfig::default()).unwrap();
    let pool = BlockingPool::new("index", NonZeroUsize::MIN).unwrap();
    (ShardSet::new(Arc::new(index), log, pool), clock)
}

/// Writes `config` to its shard's register, created or replacing what it
/// holds.
async fn put_register(store: &MemoryControlStore, config: &ShardConfig) {
    let key = TypedKey::shard(&config.bucket_id, config.shard);
    let held = read(store, &key).await.unwrap();
    let expected = held.map_or(Expected::Absent, |held| Expected::Version(held.version));
    let policy = RetryPolicy::default();
    let proposed = propose_document(store, &key, expected, config, &policy).await;
    assert!(matches!(proposed, Ok(ProposalOutcome::Accepted(_))));
}

/// The configuration the register of `shard` holds.
async fn register_of(store: &impl ControlStore, shard: &ShardRef) -> Option<ShardConfig> {
    let key = TypedKey::shard(&shard.bucket, shard.shard);
    read(store, &key).await.unwrap().map(|held| held.value)
}

/// What a [`Recorder`] saw.
#[derive(Debug, Default)]
struct Seen {
    /// The configurations of the replicas opened, in order.
    opened: Vec<ShardConfig>,
    /// The configurations of the replicas found, in order.
    found: Vec<ShardConfig>,
    /// The handoffs begun.
    began: Vec<(ShardRef, Epoch)>,
    /// The handoffs ended, and whether their step-down was sent.
    ended: Vec<(ShardRef, Option<HandedOff>)>,
}

/// An observer that records what it is told.
#[derive(Debug, Default, Clone)]
struct Recorder(Arc<Mutex<Seen>>);

impl Recorder {
    fn seen(&self) -> std::sync::MutexGuard<'_, Seen> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl ReplicaObserver<SimMount> for Recorder {
    fn opened(&self, replica: &Shard<SimMount>) {
        self.seen().opened.push(replica.config());
    }

    fn found(&self, replica: &Shard<SimMount>) {
        self.seen().found.push(replica.config());
    }

    fn handoff_began(&self, shard: &ShardRef, epoch: Epoch) {
        self.seen().began.push((shard.clone(), epoch));
    }

    fn handoff_ended(&self, shard: &ShardRef, result: &Result<HandedOff, skys3_shard::ShardError>) {
        let handed = result.as_ref().ok().copied();
        self.seen().ended.push((shard.clone(), handed));
    }
}

/// A test node: its replicated shards, reporting to its recorder, with
/// the registers in `store`, serving the transport on a loopback port.
struct Node {
    shards: Replicas,
    seen: Recorder,
}

/// Binds a loopback listener for each of `count` nodes.
async fn listeners(pki: &Pki, count: u8) -> BTreeMap<u8, skys3_net::Listener<TokioNetwork>> {
    let mut listeners = BTreeMap::new();
    for n in 1..=count {
        let listener = pki
            .transport(&node(n))
            .bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        listeners.insert(n, listener);
    }
    listeners
}

/// `count` nodes that reach each other directly, with their registers in
/// `store`, each serving its transport.
async fn cluster<S: ControlStore>(pki: &Pki, count: u8, store: &S) -> Vec<Node> {
    let listeners = listeners(pki, count).await;
    let addresses: BTreeMap<u8, SocketAddr> = listeners
        .iter()
        .map(|(n, listener)| (*n, listener.local_addr().unwrap()))
        .collect();
    let mut nodes = Vec::new();
    for (n, listener) in listeners {
        let peers = addresses
            .iter()
            .filter(|(to, _)| **to != n)
            .map(|(to, address)| (node(*to), address.to_string().parse().unwrap()))
            .collect();
        let (set, clock) = shard_set(40 + u64::from(n)).await;
        let replication = Replication::new(
            node(n),
            set.clone(),
            pki.transport(&node(n)),
            peers,
            clock,
            timing(),
        )
        .with_removal(
            ControlRegisters::new(store.clone(), RetryPolicy::default()),
            ProposalIds::seeded(u64::from(n)),
        )
        .with_takeover();
        let serving = replication.clone();
        tokio::spawn(async move { serving.serve(listener).await });
        let seen = Recorder::default();
        let shards = ReplicatedShards::new(LocalShards::new(set, node(n)), replication)
            .with_observer(Arc::new(seen.clone()));
        nodes.push(Node { shards, seen });
    }
    nodes
}
