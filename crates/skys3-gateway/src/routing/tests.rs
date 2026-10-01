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
use skys3_control::{
    Expected, MemoryControlStore, ProposalOutcome, RetryPolicy, TypedKey, propose_document,
};
use skys3_index::{Index, IndexConfig, ListItem, ListQuery};
use skys3_io::{BlockingPool, MonotonicClock, SimDisk, SimMount};
use skys3_log::RecordBody;
use skys3_log::record::{Delete, Extent, MpuCreate, MpuPart, PutData};
use skys3_net::{
    CertificateDer, Credentials, Frame, Header, MessageKind, PeerIdentity, PrivateKeyDer,
    TokioNetwork, Transport,
};
use skys3_shard::replication::{Replication, ReplicationConfig};
use skys3_types::{
    BucketDocument, BucketId, BucketMode, ClusterId, ETag, Epoch, EpochSeq, NodeAddress, NodeId,
    ProposalId, Seq, ShardConfig, ShardCount, ShardId,
};

use super::*;
use crate::LocalShards;
use crate::conditions::{ConditionFailed, Precondition};
use crate::shard::{ShardError, ShardRef, Shards};
use crate::stub::{EntryState, MemoryShards};

/// Every wait in these tests.
const WAIT: Duration = Duration::from_secs(30);

type Routed = RoutedShards<LocalShards<SimMount>, SimMount, TokioNetwork, MemoryControlStore>;

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
}

impl Cluster {
    async fn start(timing: RoutingConfig) -> Self {
        let pki = Pki::new();
        let store = MemoryControlStore::new();
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
            let shards = MemoryShards::open(SimDisk::new(u64::from(n)))
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
        Self { nodes, served }
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
