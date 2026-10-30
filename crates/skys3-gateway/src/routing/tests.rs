//! Routing between gateways and replicas over loopback TCP: stale maps,
//! redirects, register reads, lost answers, and every call of the shard
//! interface forwarded to another node.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use proptest::prelude::*;
use prost::Message as _;
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType,
};
use skys3_control::faults::{Fault, FaultyStore};
use skys3_control::{
    Expected, MemoryControlStore, ProposalOutcome, RetryPolicy, TypedKey, propose_document,
};
use skys3_index::{Index, IndexConfig, ListItem, ListQuery};
use skys3_io::{BlockingPool, MonotonicClock, SimDisk, SimMount};
use skys3_log::RecordBody;
use skys3_log::record::{Delete, Extent, MpuCreate, MpuPart, PutData, UploadBegin};
use skys3_net::{
    CertificateDer, Credentials, Frame, Header, MessageKind, PeerIdentity, PrivateKeyDer,
    TokioNetwork, Transport,
};
use skys3_shard::replication::{Replication, ReplicationConfig};
use skys3_shard::{Change, FlushState};
use skys3_types::{
    BucketDocument, BucketId, BucketMode, ClusterId, ETag, Epoch, EpochSeq, NodeAddress, NodeId,
    ProposalId, Seq, ShardConfig, ShardCount, ShardId,
};

use super::*;
use crate::LocalShards;
use crate::conditions::{ConditionFailed, PeerCondition, Precondition};
use crate::shard::{ShardError, ShardRef, Shards};
use crate::stub::{EntryState, MemoryShards};

/// Every wait in these tests.
const WAIT: Duration = Duration::from_secs(30);

type Store = FaultyStore<MemoryControlStore>;

type Routed = RoutedShards<LocalShards<SimMount>, SimMount, TokioNetwork, Store>;

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
    format!("node-{n}").parse().unwrap()
}

fn bucket_id() -> BucketId {
    BucketId::new("b-r").unwrap()
}

fn shard(n: u8) -> ShardRef {
    ShardRef {
        bucket: bucket_id(),
        shard: ShardId::new(n),
    }
}

/// The bucket of the test shards: every key in shard 0.
fn bucket() -> BucketDocument {
    BucketDocument {
        bucket_id: bucket_id(),
        name: "photos".parse().unwrap(),
        mode: BucketMode::Local,
        shards: ShardCount::new(1).unwrap(),
        replicas: 2,
        min_write_replicas: 1,
        clean_copies: 0,
        target: None,
        created_unix_ms: 0,
        proposal_id: ProposalId::new("p").unwrap(),
    }
}

fn config(shard: u8, epoch: u64, members: &[u8]) -> ShardConfig {
    ShardConfig {
        bucket_id: bucket_id(),
        shard: ShardId::new(shard),
        epoch: Epoch::new(epoch),
        primary: node(members[0]),
        members: members.iter().map(|n| node(*n)).collect(),
        learners: Vec::new(),
        min_write_replicas: 1,
        replicas: u8::try_from(members.len()).unwrap(),
        proposal_id: ProposalId::new("p").unwrap(),
    }
}

/// Two nodes on loopback, and a third that takes requests and never
/// answers.
///
/// Shard 0 is in epoch 2, with node 1 its only member, as the control
/// store says; node 2 still holds a replica as a member of epoch 1, whose
/// primary was node 1 too.
struct Cluster {
    nodes: Vec<(MemoryShards, Routed)>,
    served: Arc<Mutex<Vec<Served>>>,
    store: Store,
    pki: Pki,
    peers: BTreeMap<NodeId, NodeAddress>,
}

impl Cluster {
    async fn start(timing: RoutingConfig) -> Self {
        let pki = Pki::new();
        let store = FaultyStore::new(MemoryControlStore::new());
        let current = config(0, 2, &[1]);
        let key = TypedKey::shard(&bucket_id(), ShardId::new(0));
        let retry = RetryPolicy::default();
        let written = propose_document(&store, &key, Expected::Absent, &current, &retry).await;
        assert!(matches!(written, Ok(ProposalOutcome::Accepted(_))));

        let mut listeners = Vec::new();
        let mut peers = BTreeMap::new();
        for n in 1..=3 {
            let transport = pki.transport(&node(n));
            let listener = transport
                .bind("127.0.0.1:0".parse().unwrap())
                .await
                .unwrap();
            let address: NodeAddress = listener.local_addr().unwrap().to_string().parse().unwrap();
            peers.insert(node(n), address);
            listeners.push((transport, listener));
        }
        let (_, silent) = listeners.pop().unwrap();
        tokio::spawn(async move {
            // Node 3 reads requests and never answers them.
            loop {
                let Ok(incoming) = silent.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let Ok(mut connection) = incoming.handshake().await else {
                        return;
                    };
                    while let Ok(Some(_)) = connection.recv().await {}
                });
            }
        });

        let served = Arc::new(Mutex::new(Vec::new()));
        let mut nodes = Vec::new();
        for (n, (transport, listener)) in (1..).zip(listeners) {
            let shards = MemoryShards::open_as(SimDisk::new(u64::from(n)), node(n))
                .await
                .unwrap();
            let set = shards.local().set().clone();
            let opened = if n == 1 {
                current.clone()
            } else {
                config(0, 1, &[1, 2])
            };
            set.open_replica(&opened, &node(n)).await.unwrap();
            let seen = Arc::clone(&served);
            let routed = RoutedShards::new(
                node(n),
                shards.local().clone(),
                set.clone(),
                ShardMap::in_memory(),
                transport.clone(),
                peers.clone(),
                store.clone(),
            )
            .with_config(timing, retry)
            .with_observer(move |one| seen.lock().unwrap().push(one.clone()));
            let replication = Replication::new(
                node(n),
                set,
                transport,
                peers.clone(),
                Arc::new(MonotonicClock::new()),
                ReplicationConfig::default(),
            );
            tokio::spawn(serve_peers(listener, replication, routed.server().clone()));
            nodes.push((shards, routed));
        }
        Self {
            nodes,
            served,
            store,
            pki,
            peers,
        }
    }

    fn gateway(&self, n: usize) -> &Routed {
        &self.nodes[n - 1].1
    }

    fn served(&self) -> Vec<Served> {
        self.served.lock().unwrap().clone()
    }
}

fn timing() -> RoutingConfig {
    RoutingConfig {
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_millis(500),
        register_interval: Duration::ZERO,
        ..RoutingConfig::default()
    }
}

async fn timed<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(WAIT, future)
        .await
        .expect("the call finished in time")
}

fn etag(n: u64) -> ETag {
    ETag::new(format!("{n:032x}")).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stale_gateway_is_redirected_and_served_by_the_primary() {
    let cluster = Cluster::start(timing()).await;
    let gateway = cluster.gateway(2);
    // Node 2's map names itself, in an epoch before its own replica's: its
    // replica, a member, redirects it to node 1 in epoch 1, and node 1
    // serves in epoch 2 and says so.
    gateway.map().learn(config(0, 0, &[2])).await;
    let entry = timed(gateway.entry(&shard(0), "k")).await.unwrap();
    assert_eq!(entry, None);
    let stats = gateway.stats();
    assert_eq!(
        (stats.redirects, stats.forwarded, stats.register_reads),
        (1, 1, 0)
    );
    assert_eq!(gateway.map().get(&shard(0)), Some(config(0, 2, &[1])));
    let served = Served {
        shard: shard(0),
        node: node(1),
        epoch: Epoch::new(2),
    };
    assert_eq!(cluster.served(), [served]);

    // The map now names the primary in its epoch: no more redirects.
    timed(gateway.entry(&shard(0), "k")).await.unwrap();
    assert_eq!(gateway.stats().redirects, 1);
    // Node 1 serves its own gateway in process.
    timed(cluster.gateway(1).map().learn(config(0, 2, &[1]))).await;
    timed(cluster.gateway(1).entry(&shard(0), "k"))
        .await
        .unwrap();
    assert_eq!(cluster.gateway(1).stats().forwarded, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_call_is_forwarded_to_the_primary() {
    let cluster = Cluster::start(timing()).await;
    let primary = &cluster.nodes[0].0;
    let gateway = cluster.gateway(2);
    let shard = shard(0);
    let bucket = bucket();
    primary
        .put(&bucket, "dir/a", EntryState::Dirty)
        .await
        .unwrap();
    primary
        .put(&bucket, "top", EntryState::Clean)
        .await
        .unwrap();

    // Writes, with and without conditions.
    let delete = |key: &str| {
        RecordBody::Delete(Delete {
            key: key.to_owned(),
        })
    };
    let written = timed(gateway.write(&shard, delete("gone"), Precondition::None)).await;
    let at = written.unwrap().unwrap();
    let refused = timed(gateway.write(
        &shard,
        delete("gone"),
        Precondition::Matches(etag(1).to_string()),
    ))
    .await;
    assert_eq!(refused, Ok(Err(ConditionFailed::NoSuchKey)));
    let refused = timed(gateway.write(&shard, delete("top"), Precondition::Absent)).await;
    assert_eq!(refused, Ok(Err(ConditionFailed::PreconditionFailed)));
    let entry = timed(gateway.entry(&shard, "gone")).await.unwrap().unwrap();
    assert_eq!((entry.version, entry.object), (at, None));
    let exists = timed(gateway.write(&shard, delete("top"), Precondition::Exists)).await;
    assert!(matches!(exists, Ok(Ok(_))), "{exists:?}");

    // A listing, with a common prefix.
    let query = ListQuery {
        delimiter: Some("/".into()),
        max_items: 10,
        ..ListQuery::default()
    };
    let page = timed(gateway.list(&shard, &query)).await.unwrap();
    assert_eq!(page.items, [ListItem::Prefix("dir/".into())]);
    let page = timed(gateway.list(
        &shard,
        &ListQuery {
            max_items: 10,
            ..ListQuery::default()
        },
    ))
    .await
    .unwrap();
    assert!(matches!(&page.items[..], [ListItem::Object { key, .. }] if key == "dir/a"));

    // Extents and payloads.
    let extent = Extent {
        key: "big".into(),
        offset: 0,
        data: Bytes::from_static(b"extent bytes"),
    };
    let extent = timed(gateway.append_extent(&shard, extent)).await.unwrap();
    assert_eq!(extent.len, 12);
    let payload = timed(gateway.payload(&shard, extent.position))
        .await
        .unwrap();
    assert_eq!(payload, Bytes::from_static(b"extent bytes"));

    // A streamed body's announcement reaches the primary's subscriber.
    let replica = primary.local().set().get(&(&shard).into()).await.unwrap();
    let mut changes = replica.subscribe();
    let announced = announcement(extent);
    timed(gateway.announce(&shard, announced.clone()))
        .await
        .unwrap();
    assert_eq!(changes.recv().await, Some(Change::Streamed(announced)));

    // A write-through write's wait reaches the primary's flusher, which
    // answers it; a wait it does not answer in time is pending.
    let waited = tokio::spawn({
        let (gateway, shard) = (gateway.clone(), shard.clone());
        async move {
            gateway
                .flushed(&shard, "gone", at, Duration::from_secs(5))
                .await
        }
    });
    let Some(Change::Awaited(waiter)) = changes.recv().await else {
        panic!("the wait reaches the flusher");
    };
    assert_eq!((waiter.key(), waiter.version()), ("gone", at));
    waiter.answer(FlushState::Flushed);
    assert_eq!(timed(waited).await.unwrap(), Ok(FlushState::Flushed));
    let pending = gateway.flushed(&shard, "gone", at, Duration::from_millis(50));
    assert_eq!(timed(pending).await, Ok(FlushState::Pending));

    // A multipart upload and its parts.
    let create = RecordBody::MpuCreate(MpuCreate {
        key: "multi".into(),
        initiated_ms: 7,
        metadata: BTreeMap::from([("content-type".into(), "text/plain".into())]),
        tags: BTreeMap::new(),
        checksum: None,
    });
    let upload = timed(gateway.write(&shard, create, Precondition::None))
        .await
        .unwrap()
        .unwrap();
    let part = RecordBody::MpuPart(MpuPart {
        key: "multi".into(),
        upload,
        part_number: 3,
        size: 4,
        last_modified_ms: 8,
        etag: etag(3),
        checksums: BTreeMap::new(),
        data: PutData::Inline(Bytes::from_static(b"part")),
    });
    timed(gateway.write(&shard, part, Precondition::None))
        .await
        .unwrap()
        .unwrap();
    let (open, parts) = timed(gateway.upload(&shard, "multi", upload, 0, 10))
        .await
        .unwrap()
        .unwrap();
    assert_eq!((open.initiated_ms, parts.len(), parts[0].0), (7, 1, 3));
    let missing = EpochSeq::new(Epoch::new(9), Seq::new(9));
    assert_eq!(
        timed(gateway.upload(&shard, "multi", missing, 0, 10)).await,
        Ok(None)
    );
    let uploads = timed(gateway.uploads(&shard, "m", None, 10)).await.unwrap();
    assert_eq!(uploads.len(), 1);
    assert_eq!((uploads[0].0.as_str(), uploads[0].1), ("multi", upload));
    let after = Some(("multi".to_owned(), Some(upload)));
    assert!(
        timed(gateway.uploads(&shard, "", after, 10))
            .await
            .unwrap()
            .is_empty()
    );
    let parts = timed(gateway.parts(&shard, upload, 0, 10)).await.unwrap();
    assert_eq!(parts[0].1.size, 4);
    let payload = timed(gateway.payload(&shard, parts[0].1.position))
        .await
        .unwrap();
    assert_eq!(payload, Bytes::from_static(b"part"));

    // The identity of a streamed PUT.
    let begin = RecordBody::UploadBegin(UploadBegin { key: "big".into() });
    let begun = timed(gateway.write(&shard, begin, Precondition::None))
        .await
        .unwrap()
        .unwrap();
    assert!(begun > upload, "{begun} follows {upload}");

    // Seals.
    let summary = timed(gateway.seal(&shard)).await.unwrap();
    assert_eq!(summary.objects, 1);
    let sealed = timed(gateway.write(&shard, delete("x"), Precondition::None)).await;
    assert_eq!(sealed, Err(ShardError::Sealed(shard.clone())));
    timed(gateway.unseal(&shard)).await.unwrap();
    timed(gateway.write(&shard, delete("x"), Precondition::None))
        .await
        .unwrap()
        .unwrap();

    // Every call went to node 1, in epoch 2.
    let served = cluster.served();
    assert!(
        served
            .iter()
            .all(|s| s.node == node(1) && s.epoch == Epoch::new(2))
    );
    assert_eq!(gateway.stats().register_reads, 1);

    // Removing acts on node 2's own replica and its map.
    timed(gateway.remove(&shard)).await.unwrap();
    assert_eq!(gateway.map().get(&shard), None);
    let replicas = cluster.nodes[1].0.local().set().shards().await;
    assert!(replicas.is_empty(), "{replicas:?}");
}

/// A GET's plan comes from the primary, and its bytes from whichever
/// holder the gateway asks, over the wire or in process; a holder that
/// lacks the version says so, and one that does not answer fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reads_are_planned_by_the_primary_and_served_by_any_holder() {
    let cluster = Cluster::start(timing()).await;
    let shard = shard(0);
    let extent = Extent {
        key: "big".into(),
        offset: 0,
        data: Bytes::from_static(b"extent bytes"),
    };
    let primary = cluster.gateway(1);
    let extent = timed(primary.append_extent(&shard, extent)).await.unwrap();
    let put = RecordBody::Put(skys3_log::record::Put {
        key: "big".into(),
        size: 12,
        last_modified_ms: 0,
        etag: etag(1),
        inherited_identity: None,
        metadata: BTreeMap::new(),
        tags: BTreeMap::new(),
        checksums: BTreeMap::new(),
        copy_source: None,
        data: PutData::Extents(vec![extent]),
    });
    let version = timed(primary.write(&shard, put, Precondition::None))
        .await
        .unwrap()
        .unwrap();
    let observed = cluster.served().len();

    let gateway = cluster.gateway(2);
    assert_eq!(gateway.node(), Some(node(2)));
    let plan = timed(gateway.plan(&shard, "big")).await.unwrap();
    assert_eq!(plan.entry.unwrap().version, version);
    assert_eq!((plan.layout, plan.holders), (vec![extent], vec![node(1)]));
    let none = timed(gateway.plan(&shard, "none")).await.unwrap();
    assert_eq!((none.entry, none.holders), (None, Vec::new()));

    // Node 1 holds the version: the read is registered, renewed, fetched,
    // and released over the wire.
    let registered = timed(gateway.register(&shard, &node(1), "big", version, vec![extent]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(registered.layout, [extent]);
    assert_eq!(
        timed(gateway.renew(&shard, &node(1), registered.id)).await,
        Ok(true)
    );
    let fetched = timed(gateway.fetch(&shard, &node(1), registered.id, extent.position)).await;
    assert_eq!(fetched, Ok(Bytes::from_static(b"extent bytes")));
    timed(gateway.release(&shard, &node(1), registered.id))
        .await
        .unwrap();
    assert_eq!(
        timed(gateway.renew(&shard, &node(1), registered.id)).await,
        Ok(false)
    );
    let lapsed = timed(gateway.fetch(&shard, &node(1), registered.id, extent.position)).await;
    assert!(
        matches!(&lapsed, Err(ShardError::Unavailable { reason, .. }) if reason.contains("lapsed")),
        "{lapsed:?}"
    );

    // Node 2's replica never got the records, so it says it lacks them,
    // in process.
    let missing = timed(gateway.register(&shard, &node(2), "big", version, vec![extent])).await;
    assert_eq!(missing, Ok(None));
    // Node 3 never answers.
    let silent = timed(gateway.register(&shard, &node(3), "big", version, vec![extent])).await;
    assert!(
        matches!(silent, Err(ShardError::Unavailable { .. })),
        "{silent:?}"
    );
    // Node 1 has no replica of shard 1.
    let other = shard_number(1);
    let absent = timed(gateway.register(&other, &node(1), "big", version, Vec::new())).await;
    assert_eq!(absent, Err(ShardError::NotFound(other)));

    // Only the plans were observed: holders are not audited as primaries.
    assert_eq!(cluster.served().len(), observed + 2);
}

fn shard_number(n: u8) -> ShardRef {
    shard(n)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_register_is_read_only_when_no_member_answers() {
    let cluster = Cluster::start(timing()).await;
    let gateway = cluster.gateway(2);
    // The map names node 4, which has no address.
    gateway.map().learn(config(0, 1, &[4])).await;
    timed(gateway.entry(&shard(0), "k")).await.unwrap();
    assert_eq!(gateway.stats().register_reads, 1);
    assert_eq!(gateway.map().get(&shard(0)).unwrap().epoch, Epoch::new(2));

    // A map ahead of the replicas: node 1 is behind it, and the register
    // names nothing newer.
    gateway.map().learn(config(0, 5, &[1])).await;
    let behind = timed(gateway.entry(&shard(0), "k")).await.unwrap_err();
    assert!(behind.to_string().contains("still in epoch 2"), "{behind}");

    // A shard that has no register and no replica.
    let missing = timed(gateway.entry(&shard(1), "k")).await;
    assert_eq!(missing, Err(ShardError::NotFound(shard(1))));
    // A shard whose replica is not open where the map says.
    gateway.map().learn(config(1, 1, &[1])).await;
    let closed = timed(gateway.entry(&shard(1), "k")).await;
    assert_eq!(closed, Err(ShardError::NotFound(shard(1))));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn register_reads_are_spaced_out() {
    let timing = RoutingConfig {
        register_interval: Duration::from_secs(3600),
        ..timing()
    };
    let cluster = Cluster::start(timing).await;
    let gateway = cluster.gateway(2);
    gateway.map().learn(config(0, 3, &[4])).await;
    let error = timed(gateway.entry(&shard(0), "k")).await.unwrap_err();
    assert!(error.to_string().contains("node-4"), "{error}");
    let error = timed(gateway.entry(&shard(0), "k")).await.unwrap_err();
    assert!(error.to_string().contains("node-4"), "{error}");
    assert_eq!(gateway.stats().register_reads, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn requests_that_wait_for_a_register_read_share_its_result() {
    let timing = RoutingConfig {
        register_interval: Duration::from_millis(200),
        ..timing()
    };
    let cluster = Cluster::start(timing).await;
    let gateway = cluster.gateway(2);
    // The map names node 4, which has no address, in a newer epoch than
    // the register's: every request reads the register, which names
    // nothing newer.
    gateway.map().learn(config(0, 3, &[4])).await;
    let shard = shard(0);
    let entries = || async {
        let entry = || gateway.entry(&shard, "k");
        let (a, b, c, d) = tokio::join!(entry(), entry(), entry(), entry());
        [a, b, c, d]
    };

    // A read that outlasts the interval: the requests that waited for it
    // take its result instead of reading again, one after another.
    let slow = Fault::before(|| tokio::time::sleep(Duration::from_millis(600)));
    cluster.store.script([slow]);
    for result in timed(entries()).await {
        let error = result.unwrap_err();
        assert!(error.to_string().contains("node-4"), "{error}");
    }
    assert_eq!(gateway.stats().register_reads, 1);

    // Once the interval has passed, a read that fails gives its error to
    // the requests that waited for it.
    tokio::time::sleep(Duration::from_millis(300)).await;
    cluster.store.script([Fault::Fail]);
    for result in timed(entries()).await {
        let error = result.unwrap_err();
        assert!(error.to_string().contains("could not be read"), "{error}");
    }
    assert_eq!(gateway.stats().register_reads, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_served_request_is_observed_even_if_its_answer_is_lost() {
    let cluster = Cluster::start(timing()).await;
    let transport = cluster.pki.transport(&node(5));
    let mut connection = timed(transport.connect(&node(1), &cluster.peers[&node(1)]))
        .await
        .unwrap();
    let request = Request::Entry { key: "k".into() };
    let frame = wire::request_frame(&shard(0), Epoch::new(2), &request).unwrap();
    timed(connection.send(&frame)).await.unwrap();
    // The gateway goes away before the answer arrives.
    drop(connection);
    timed(async {
        while cluster.served().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let served = Served {
        shard: shard(0),
        node: node(1),
        epoch: Epoch::new(2),
    };
    assert_eq!(cluster.served(), [served]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lost_answer_fails_a_write_and_moves_a_read_on() {
    let cluster = Cluster::start(timing()).await;
    let gateway = cluster.gateway(2);
    // Node 3 takes requests and never answers; node 1 holds the shard in
    // the map's epoch.
    gateway.map().learn(config(0, 2, &[3, 1])).await;
    timed(gateway.entry(&shard(0), "k")).await.unwrap();
    let write = RecordBody::Delete(Delete { key: "k".into() });
    let lost = timed(gateway.write(&shard(0), write, Precondition::None)).await;
    let error = lost.unwrap_err();
    assert!(error.to_string().contains("may have applied it"), "{error}");
    assert_eq!(gateway.stats().register_reads, 0);
}

#[tokio::test]
async fn the_map_only_moves_forward_and_survives_a_restart() {
    let disk = SimDisk::new(3);
    let open = || {
        let index = Index::open_sim(&disk.mount(), "index.redb", &IndexConfig::default()).unwrap();
        ShardMap::load(Arc::new(index), BlockingPool::inline("test"))
    };
    let map = open().await.unwrap();
    assert!(map.all().is_empty());
    assert!(map.learn(config(0, 2, &[1])).await);
    assert!(!map.learn(config(0, 2, &[2])).await);
    assert!(!map.learn(config(0, 1, &[2])).await);
    assert!(map.learn(config(1, 1, &[2])).await);
    assert!(map.learn(config(0, 3, &[2, 1])).await);
    drop(map);

    let map = open().await.unwrap();
    assert_eq!(map.get(&shard(0)), Some(config(0, 3, &[2, 1])));
    assert_eq!(map.all().len(), 2);
    map.forget(&shard(1)).await;
    map.forget(&shard(1)).await;
    drop(map);
    let map = open().await.unwrap();
    assert_eq!(map.get(&shard(1)), None);
    assert!(format!("{map:?}").contains("routes: 1"));

    let memory = ShardMap::in_memory();
    assert!(memory.learn(config(0, 1, &[1])).await);
    memory.forget(&shard(0)).await;
    assert_eq!(memory.get(&shard(0)), None);
}

#[test]
fn refusals_and_hints_round_trip() {
    let shard = shard(0);
    let replies = [
        Reply::Redirect(config(0, 4, &[2, 1])),
        Reply::Behind(Epoch::new(3)),
        Reply::Refused(ShardError::NotFound(shard.clone())),
        Reply::Refused(ShardError::Sealed(shard.clone())),
        Reply::Refused(ShardError::Invalid {
            shard: shard.clone(),
            reason: "bad".into(),
        }),
        Reply::Refused(ShardError::Unavailable {
            shard: shard.clone(),
            reason: "é".repeat(600),
        }),
        Reply::Refused(ShardError::NotAcknowledged {
            shard: shard.clone(),
            position: Some(EpochSeq::new(Epoch::new(4), Seq::new(7))),
            reason: "late".into(),
        }),
        Reply::Refused(ShardError::NotAcknowledged {
            shard: shard.clone(),
            position: None,
            reason: "late".into(),
        }),
        Reply::Served {
            response: Response::Unsealed,
            epoch: Epoch::new(4),
            hint: Some(config(0, 4, &[1])),
        },
        Reply::Served {
            response: Response::Plan(ReadPlan {
                entry: Some(skys3_index::Entry {
                    version: EpochSeq::new(Epoch::new(4), Seq::new(9)),
                    state: skys3_index::EntryState::Dirty,
                    object: None,
                    remote_etag: None,
                    remote_version_id: None,
                }),
                layout: vec![extent_ref(3, 5), extent_ref(4, 1)],
                holders: vec![node(2), node(1)],
            }),
            epoch: Epoch::new(4),
            hint: None,
        },
        Reply::Served {
            response: Response::Plan(ReadPlan {
                entry: None,
                layout: Vec::new(),
                holders: Vec::new(),
            }),
            epoch: Epoch::new(4),
            hint: None,
        },
        Reply::Served {
            response: Response::Registered(Some(Registered {
                id: 7,
                layout: vec![extent_ref(3, 5)],
            })),
            epoch: Epoch::new(4),
            hint: None,
        },
        Reply::Served {
            response: Response::Registered(None),
            epoch: Epoch::new(4),
            hint: None,
        },
        Reply::Served {
            response: Response::Renewed(false),
            epoch: Epoch::new(4),
            hint: None,
        },
        Reply::Served {
            response: Response::Released,
            epoch: Epoch::new(4),
            hint: None,
        },
    ];
    for reply in replies {
        let frame = wire::reply_frame(&reply, 9);
        assert_eq!(frame.header.request_id, 9);
        let decoded = wire::decode_reply(&frame, &shard).unwrap();
        match (&reply, &decoded) {
            (
                Reply::Refused(ShardError::Unavailable { reason, .. }),
                Reply::Refused(ShardError::Unavailable { reason: got, .. }),
            ) => {
                assert!(reason.starts_with(got.as_str()) && got.len() <= 1024);
            }
            _ => assert_eq!(decoded, reply),
        }
    }
    // A hint of another shard is refused.
    let frame = wire::reply_frame(&Reply::Redirect(config(1, 4, &[1])), 1);
    assert!(wire::decode_reply(&frame, &shard).is_err());
    // A refusal that carries a payload is refused.
    let mut frame = wire::reply_frame(&Reply::Behind(Epoch::new(1)), 1);
    frame.payload = Bytes::from_static(b"x");
    assert!(wire::decode_reply(&frame, &shard).is_err());
    // Only a write that was not acknowledged carries a position.
    let reply = wire::ForwardReply {
        outcome: wire::OUTCOME_UNAVAILABLE,
        epoch: 0,
        config: Vec::new(),
        reason: String::new(),
        position: Some(wire::Position { epoch: 1, seq: 1 }),
    };
    let frame = Frame::new(
        Header::new(MessageKind::ForwardReply).with_body(reply.encode_to_vec()),
        Bytes::new(),
    );
    assert!(wire::decode_reply(&frame, &shard).is_err());
    // NotPrimary never reaches the wire as such: it becomes a redirect.
    let not_primary = ShardError::NotPrimary {
        shard: shard.clone(),
        primary: node(1),
        epoch: Epoch::new(1),
    };
    let frame = wire::reply_frame(&Reply::Refused(not_primary), 1);
    assert!(matches!(
        wire::decode_reply(&frame, &shard),
        Ok(Reply::Refused(ShardError::Unavailable { .. }))
    ));

    // Answers about reads that make no sense are refused.
    let served = |answer: wire::Result| {
        let reply = wire::ForwardReply {
            outcome: wire::OUTCOME_SERVED,
            epoch: 1,
            config: Vec::new(),
            reason: String::new(),
            position: None,
        };
        let answer = wire::Answer {
            result: Some(answer),
        };
        Frame::new(
            Header::new(MessageKind::ForwardReply).with_body(reply.encode_to_vec()),
            Bytes::from(answer.encode_to_vec()),
        )
    };
    let empty_extent = wire::Layout {
        extents: vec![wire::ExtentAnswer {
            position: Some(wire::Position { epoch: 1, seq: 1 }),
            len: 0,
        }],
    };
    let malformed = [
        wire::Result::Plan(wire::PlanAnswer {
            entry: Vec::new(),
            layout: None,
            holders: vec!["node-1".into()],
        }),
        wire::Result::Plan(wire::PlanAnswer {
            entry: Vec::new(),
            layout: Some(empty_extent),
            holders: Vec::new(),
        }),
        wire::Result::Plan(wire::PlanAnswer {
            entry: vec![1, 2, 3],
            layout: None,
            holders: Vec::new(),
        }),
        wire::Result::Plan(wire::PlanAnswer {
            entry: Vec::new(),
            layout: None,
            holders: vec!["Not A Node".into()],
        }),
        wire::Result::Registered(wire::RegisteredAnswer {
            read: None,
            layout: Some(wire::Layout::default()),
        }),
        wire::Result::Registered(wire::RegisteredAnswer {
            read: Some(1),
            layout: Some(wire::Layout {
                extents: vec![wire::ExtentAnswer {
                    position: None,
                    len: 1,
                }],
            }),
        }),
    ];
    for answer in malformed {
        let frame = served(answer.clone());
        assert!(wire::decode_reply(&frame, &shard).is_err(), "{answer:?}");
    }
}

fn extent_ref(seq: u64, len: u32) -> skys3_log::record::ExtentRef {
    skys3_log::record::ExtentRef {
        position: EpochSeq::new(Epoch::new(4), Seq::new(seq)),
        len,
    }
}

/// An announcement of the streamed body of `big` begun at 4.7, with
/// metadata, tags, and `extent` at offset 0.
fn announcement(extent: skys3_log::record::ExtentRef) -> StreamedBody {
    StreamedBody {
        key: "big".into(),
        upload: EpochSeq::new(Epoch::new(4), Seq::new(7)),
        metadata: BTreeMap::from([("x-amz-meta-color".into(), "blue".into())]),
        tags: BTreeMap::from([("team".into(), "a".into())]),
        extents: vec![(0, extent)],
    }
}

#[test]
fn announcements_round_trip_and_malformed_ones_are_refused() {
    let shard = shard(0);
    let request = Request::Announce(announcement(extent_ref(9, 100)));
    let frame = wire::request_frame(&shard, Epoch::new(1), &request).unwrap();
    assert_eq!(
        wire::decode_request(&frame),
        Ok((shard.clone(), Epoch::new(1), request.clone()))
    );
    let reply = Reply::Served {
        response: Response::Announced,
        epoch: Epoch::new(1),
        hint: None,
    };
    let frame = wire::reply_frame(&reply, 3);
    assert_eq!(wire::decode_reply(&frame, &shard), Ok(reply));

    let Request::Announce(body) = request else {
        unreachable!("an announcement")
    };
    let Ok(wire::Op::Announce(valid)) =
        wire::request_frame(&shard, Epoch::new(1), &Request::Announce(body.clone())).map(|frame| {
            prost::Message::decode(frame.header.body)
                .map(|forward: wire::Forward| forward.op.unwrap())
                .unwrap()
        })
    else {
        panic!("an announcement encodes as one");
    };
    let refused = |change: &dyn Fn(&mut wire::Announcement)| {
        let mut announced = valid.clone();
        change(&mut announced);
        let forward = wire::Forward {
            bucket_id: "b-r".into(),
            shard: 0,
            epoch: 1,
            op: Some(wire::Op::Announce(announced)),
        };
        let frame = Frame::new(
            Header::new(MessageKind::Forward).with_body(prost::Message::encode_to_vec(&forward)),
            Bytes::new(),
        );
        wire::decode_request(&frame).is_err()
    };
    assert!(refused(&|a| a.key.clear()));
    assert!(refused(&|a| a.key = "k".repeat(1025)));
    assert!(refused(&|a| a.upload = None));
    assert!(refused(&|a| a.headers = vec![0xff]));
    assert!(refused(&|a| a.extents[0].extent = None));
    assert!(refused(&|a| {
        if let Some(extent) = &mut a.extents[0].extent {
            extent.len = 0;
        }
    }));
    assert!(refused(&|a| {
        let extent = a.extents[0].clone();
        a.extents = vec![extent; skys3_log::record::MAX_EXTENTS + 1];
    }));
    // A body announced with too much metadata does not encode.
    let mut big = body;
    big.metadata
        .insert("x-amz-meta-big".into(), "v".repeat(9 * 1024));
    assert!(wire::request_frame(&shard, Epoch::new(1), &Request::Announce(big)).is_err());
}

#[test]
fn flush_waits_round_trip_and_malformed_ones_are_refused() {
    let shard = shard(0);
    for state in [
        FlushState::Flushed,
        FlushState::Conflict,
        FlushState::Pending,
    ] {
        let reply = Reply::Served {
            response: Response::Flushed(state),
            epoch: Epoch::new(1),
            hint: None,
        };
        let frame = wire::reply_frame(&reply, 3);
        assert_eq!(wire::decode_reply(&frame, &shard), Ok(reply));
    }
    let answer = wire::Answer {
        result: Some(wire::Result::Flushed(7)),
    };
    let reply = wire::ForwardReply {
        outcome: wire::OUTCOME_SERVED,
        epoch: 1,
        ..wire::ForwardReply::default()
    };
    let frame = Frame::new(
        Header::new(MessageKind::ForwardReply).with_body(reply.encode_to_vec()),
        Bytes::from(answer.encode_to_vec()),
    );
    assert!(wire::decode_reply(&frame, &shard).is_err());

    let valid = wire::FlushQuery {
        key: "k".into(),
        version: Some(wire::Position { epoch: 1, seq: 2 }),
        wait_ms: 5_000,
    };
    let decoded = |query: wire::FlushQuery| {
        let forward = wire::Forward {
            bucket_id: "b-r".into(),
            shard: 0,
            epoch: 1,
            op: Some(wire::Op::Flushed(query)),
        };
        let frame = Frame::new(
            Header::new(MessageKind::Forward).with_body(forward.encode_to_vec()),
            Bytes::new(),
        );
        wire::decode_request(&frame).map(|(_, _, request)| request)
    };
    assert_eq!(
        decoded(valid.clone()),
        Ok(Request::Flushed {
            key: "k".into(),
            version: EpochSeq::new(Epoch::new(1), Seq::new(2)),
            wait: Duration::from_secs(5),
        })
    );
    let refused = |change: &dyn Fn(&mut wire::FlushQuery)| {
        let mut query = valid.clone();
        change(&mut query);
        decoded(query).is_err()
    };
    assert!(refused(&|q| q.key.clear()));
    assert!(refused(&|q| q.key = "k".repeat(1025)));
    assert!(refused(&|q| q.version = None));
    assert!(refused(&|q| q.wait_ms = 60_001));
    assert!(!refused(&|q| q.wait_ms = 60_000));
}

#[test]
fn malformed_requests_are_refused() {
    let shard = shard(0);
    let request = Request::Entry { key: "k".into() };
    let mut frame = wire::request_frame(&shard, Epoch::new(1), &request).unwrap();
    assert_eq!(
        wire::decode_request(&frame),
        Ok((shard.clone(), Epoch::new(1), request))
    );
    frame.payload = Bytes::from_static(b"stray");
    assert!(wire::decode_request(&frame).is_err());

    let forward = |op: wire::Op, shard_number: u32| {
        let body = wire::Forward {
            bucket_id: "b-r".into(),
            shard: shard_number,
            epoch: 1,
            op: Some(op),
        };
        Frame::new(
            Header::new(MessageKind::Forward).with_body(prost::Message::encode_to_vec(&body)),
            Bytes::new(),
        )
    };
    assert!(wire::decode_request(&forward(wire::Op::Seal(true), 256)).is_err());
    let condition = wire::Condition {
        kind: 1,
        etag: "x".into(),
        ..wire::Condition::default()
    };
    assert!(wire::decode_request(&forward(wire::Op::Write(condition), 0)).is_err());
    let query = wire::UploadsQuery {
        prefix: String::new(),
        after_key: None,
        after_upload: Some(wire::Position { epoch: 1, seq: 1 }),
        limit: 1,
    };
    assert!(wire::decode_request(&forward(wire::Op::Uploads(query), 0)).is_err());
    let parts = wire::PartsQuery {
        upload: None,
        after: 0,
        limit: 1,
    };
    assert!(wire::decode_request(&forward(wire::Op::Parts(parts), 0)).is_err());

    // An extent request must carry an extent of its own shard.
    let delete = RecordBody::Delete(Delete { key: "k".into() });
    let write = Request::Write {
        body: delete,
        condition: Precondition::Exists,
    };
    let mut frame = wire::request_frame(&shard, Epoch::new(1), &write).unwrap();
    let record = frame.payload.clone();
    frame.header = forward(wire::Op::AppendExtent(true), 0).header;
    frame.payload = record.clone();
    assert!(wire::decode_request(&frame).is_err());
    let mut frame = forward(wire::Op::Write(wire::Condition::default()), 1);
    frame.payload = record;
    assert!(wire::decode_request(&frame).is_err());

    // A registration carries its layout, and only it, as its payload.
    let register = |payload: &'static [u8]| {
        let mut frame = forward(
            wire::Op::Register(wire::RegisterQuery {
                key: "k".into(),
                version: Some(wire::Position { epoch: 1, seq: 2 }),
            }),
            0,
        );
        frame.payload = Bytes::from_static(payload);
        frame
    };
    assert!(matches!(
        wire::decode_request(&register(b"")),
        Ok((_, _, Request::Register { layout, .. })) if layout.is_empty()
    ));
    assert!(wire::decode_request(&register(&[0xff, 0xff])).is_err());
    let without_version = forward(
        wire::Op::Register(wire::RegisterQuery {
            key: "k".into(),
            version: None,
        }),
        0,
    );
    assert!(wire::decode_request(&without_version).is_err());
    let fetch = forward(
        wire::Op::Fetch(wire::FetchQuery {
            read: 1,
            position: None,
        }),
        0,
    );
    assert!(wire::decode_request(&fetch).is_err());
    let mut renew = forward(wire::Op::Renew(1), 0);
    renew.payload = Bytes::from_static(b"stray");
    assert!(wire::decode_request(&renew).is_err());
}

/// A peer cluster's conditions travel to the primary whole, and a
/// condition with fields of the other kind is refused.
#[test]
fn peer_conditions_round_trip() {
    let shard = shard(0);
    let identity = |seq: u64| -> skys3_types::WriteIdentity {
        format!("prod-us/b-src/5/42.{seq}").parse().unwrap()
    };
    let delete = RecordBody::Delete(Delete { key: "k".into() });
    for expected in [
        skys3_peer::Precondition::Absent,
        skys3_peer::Precondition::Matches(identity(1)),
        skys3_peer::Precondition::Unconditional,
    ] {
        let write = Request::Write {
            body: delete.clone(),
            condition: Precondition::Peer(PeerCondition {
                identity: identity(2),
                expected,
                cluster: ClusterId::new("prod-eu").unwrap(),
                shard: shard.clone(),
            }),
        };
        let frame = wire::request_frame(&shard, Epoch::new(1), &write).unwrap();
        assert_eq!(
            wire::decode_request(&frame),
            Ok((shard.clone(), Epoch::new(1), write))
        );
    }
    let record = wire::request_frame(
        &shard,
        Epoch::new(1),
        &Request::Write {
            body: delete,
            condition: Precondition::None,
        },
    )
    .unwrap()
    .payload;
    let peer = wire::Condition {
        kind: 4,
        identity: identity(2).to_string(),
        cluster: "prod-eu".into(),
        ..wire::Condition::default()
    };
    let malformed = [
        // A plain condition with a peer's fields.
        wire::Condition {
            kind: 1,
            ..peer.clone()
        },
        // An expected identity where none belongs, or a bad one.
        wire::Condition {
            expected: identity(1).to_string(),
            ..peer.clone()
        },
        wire::Condition {
            kind: 5,
            expected: "not an identity".into(),
            ..peer.clone()
        },
        wire::Condition {
            identity: "prod-us/b-src".into(),
            ..peer.clone()
        },
        wire::Condition {
            cluster: "Prod_EU".into(),
            ..peer.clone()
        },
        wire::Condition {
            etag: "x".into(),
            ..peer.clone()
        },
        wire::Condition { kind: 7, ..peer },
    ];
    for condition in malformed {
        let body = wire::Forward {
            bucket_id: "b-r".into(),
            shard: 0,
            epoch: 1,
            op: Some(wire::Op::Write(condition.clone())),
        };
        let frame = Frame::new(
            Header::new(MessageKind::Forward).with_body(prost::Message::encode_to_vec(&body)),
            record.clone(),
        );
        assert!(wire::decode_request(&frame).is_err(), "{condition:?}");
    }
}

proptest! {
    /// Whatever a peer sends, decoding never panics, and a request comes
    /// back as it was sent.
    #[test]
    fn frames_from_any_bytes(body in proptest::collection::vec(any::<u8>(), 0..256),
                             payload in proptest::collection::vec(any::<u8>(), 0..64),
                             key in "[a-z/]{0,12}", epoch: u64, limit in 0usize..10_000) {
        for kind in [MessageKind::Forward, MessageKind::ForwardReply] {
            let frame = Frame::new(Header::new(kind).with_body(body.clone()), payload.clone());
            let _ = wire::decode_request(&frame);
            let _ = wire::decode_reply(&frame, &shard(0));
        }
        let requests = [
            Request::Entry { key: key.clone() },
            Request::List(ListQuery {
                prefix: key.clone(),
                delimiter: Some("/".into()),
                start_after: Some(key.clone()),
                max_items: limit,
            }),
            Request::Uploads { prefix: key.clone(), after: Some((key.clone(), None)), limit },
            Request::Payload(EpochSeq::new(Epoch::new(epoch), Seq::new(1))),
            Request::Plan { key: key.clone() },
            Request::Register {
                key: key.clone(),
                version: EpochSeq::new(Epoch::new(epoch), Seq::new(2)),
                layout: (0..limit % 5)
                    .map(|n| skys3_log::record::ExtentRef {
                        position: EpochSeq::new(Epoch::new(epoch), Seq::new(n as u64)),
                        len: u32::try_from(limit).unwrap() + 1,
                    })
                    .collect(),
            },
            Request::Renew(epoch),
            Request::Release(epoch),
            Request::Fetch {
                read: epoch,
                position: EpochSeq::new(Epoch::new(1), Seq::new(epoch)),
            },
            Request::Write {
                body: RecordBody::Delete(Delete { key: format!("k{key}") }),
                condition: Precondition::Peer(PeerCondition {
                    identity: format!("prod-us/b-src/{}/{epoch}.{limit}", limit % 256)
                        .parse()
                        .unwrap(),
                    expected: match limit % 3 {
                        0 => skys3_peer::Precondition::Absent,
                        1 => skys3_peer::Precondition::Unconditional,
                        _ => skys3_peer::Precondition::Matches(
                            format!("prod-eu/b-r/0/{limit}.{epoch}").parse().unwrap(),
                        ),
                    },
                    cluster: ClusterId::new("prod-eu").unwrap(),
                    shard: shard(3),
                }),
            },
            Request::Flushed {
                key: format!("k{key}"),
                version: EpochSeq::new(Epoch::new(epoch), Seq::new(4)),
                wait: Duration::from_millis(limit as u64),
            },
            Request::Announce(StreamedBody {
                key: format!("k{key}"),
                upload: EpochSeq::new(Epoch::new(epoch), Seq::new(3)),
                metadata: BTreeMap::from([(format!("x-amz-meta-k{}", limit % 7), key.clone())]),
                tags: BTreeMap::from([(format!("t{key}"), key.clone())]),
                extents: (0..limit % 5)
                    .map(|n| (n as u64 * 9, extent_ref(n as u64, 9)))
                    .collect(),
            }),
        ];
        for request in requests {
            let frame = wire::request_frame(&shard(3), Epoch::new(epoch), &request).unwrap();
            prop_assert_eq!(
                wire::decode_request(&frame),
                Ok((shard(3), Epoch::new(epoch), request))
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batched_writes_are_sequenced_on_a_local_primary_and_routed_otherwise() {
    let cluster = Cluster::start(timing()).await;
    let shard = shard(0);
    let deletes = |keys: &[&str], condition: Precondition| -> Vec<_> {
        keys.iter()
            .map(|key| {
                let body = RecordBody::Delete(Delete {
                    key: (*key).to_owned(),
                });
                (body, condition.clone())
            })
            .collect()
    };

    // Node 1, the primary, sequences its gateway's batch in process: the
    // writes take consecutive positions, and one request is observed.
    let local = cluster.gateway(1);
    timed(local.map().learn(config(0, 2, &[1]))).await;
    let written =
        timed(local.write_all(&shard, deletes(&["a", "b", "c"], Precondition::None))).await;
    let positions: Vec<_> = written
        .into_iter()
        .map(|written| written.unwrap().unwrap().seq.get())
        .collect();
    assert_eq!(
        positions,
        [positions[0], positions[0] + 1, positions[0] + 2]
    );
    assert_eq!(local.stats().forwarded, 0);
    assert_eq!(cluster.served().len(), 1);
    // Conditions are checked as for single writes.
    let written = timed(local.write_all(&shard, deletes(&["a", "x"], Precondition::Exists))).await;
    assert_eq!(
        written,
        [
            Ok(Err(ConditionFailed::NoSuchKey)),
            Ok(Err(ConditionFailed::NoSuchKey))
        ]
    );

    // Node 2's map names itself, but its replica is only a member: each
    // write is routed on its own, and node 1 serves them.
    let remote = cluster.gateway(2);
    timed(remote.map().learn(config(0, 0, &[2]))).await;
    let written = timed(remote.write_all(&shard, deletes(&["d", "e"], Precondition::None))).await;
    assert!(
        written.iter().all(|w| matches!(w, Ok(Ok(_)))),
        "{written:?}"
    );
    assert_eq!(remote.stats().forwarded, 2);
    let entry = timed(remote.entry(&shard, "e")).await.unwrap().unwrap();
    assert!(entry.object.is_none());
}
