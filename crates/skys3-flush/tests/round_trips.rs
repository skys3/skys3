//! Round trips per object for a workload of small objects: the S3 REST
//! flush against the native peer protocol's `BATCH` (design §7.7, §7.8;
//! plan M6-05).
//!
//! A round trip is a request whose answer the sender waits for before it
//! goes on. Over REST, every request the flusher sends is at least one: the
//! simulated store counts them. The TCP and TLS handshakes of its
//! connections would add more, and are not counted. Over the peer protocol,
//! a connection costs two, the QUIC handshake and the `HELLO` exchange, and
//! each `BATCH` one more: the source sends the whole batch, finishes its
//! stream, and only then waits for the `APPLIED`s.

mod support;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType,
};
use skys3_config::{BucketPair, Config, CongestionControl};
use skys3_flush::{FlushSettings, Target};
use skys3_gateway::stub::MemoryShards;
use skys3_gateway::{BucketLookup, GatewayConfig, PeerCommits, PeerExtents, ShardRef, Shards};
use skys3_net::{CertificateDer, Credentials, PrivateKeyDer};
use skys3_peer::{
    BatchBuilder, Capabilities, Commit, Destination, EndpointSettings, Outcome, PeerEndpoint,
    PeerTls, PeerTrust, Precondition, Put, PutData, Refusal, Staging, StagingLimits,
    StagingService, Write, send_batch,
};
use skys3_types::{
    BucketDocument, BucketId, BucketMode, BucketName, ClusterId, ProposalId, ShardCount,
};
use support::{Node, cluster, md5_etag, remote, runtime, settings, writes};

/// The objects of the workload.
const OBJECTS: u64 = 1500;

const SOURCE: &str = "prod-us";
const DESTINATION: &str = "prod-eu";

/// The longest a test waits on the network.
const WAIT: Duration = Duration::from_secs(30);

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(WAIT, future)
        .await
        .expect("a network wait took too long")
}

/// The body of object `n`.
fn body(n: u64) -> String {
    format!("small object number {n}")
}

/// Round trips per object over REST: the shard's flusher sends every
/// object to the simulated store.
fn rest_round_trips() -> u64 {
    runtime().block_on(async {
        let node = Node::open(5).await;
        for n in 0..OBJECTS {
            node.put(&format!("logs/{n}"), &body(n)).await;
        }
        let store = remote(5, false);
        // As many requests in flight as a shard flushes to a far target
        // (§7.7): the count of round trips does not depend on it.
        let settings = FlushSettings {
            min_concurrency: 64,
            max_concurrency: 64,
            ..settings()
        };
        let target = Arc::new(Target::new(
            Arc::new(store.clone()),
            "",
            writes(true, true, true),
            cluster(),
            settings,
        ));
        let flusher = node.flusher(&target);
        node.settle(&flusher).await;
        assert_eq!(store.keys().len() as u64, OBJECTS);
        store.stats().requests
    })
}

/// A cluster's certificate authority.
struct Ca {
    issuer: Issuer<'static, KeyPair>,
    cert: CertificateDer<'static>,
}

impl Ca {
    fn new() -> Self {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
        let cert = params.self_signed(&key).unwrap().der().clone();
        Self {
            issuer: Issuer::new(params, key),
            cert,
        }
    }

    /// An endpoint on loopback for node `node` of `cluster`.
    fn endpoint(&self, cluster: &str, node: &str, trust: PeerTrust) -> PeerEndpoint {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        let uri = format!("spiffe://{cluster}/node/{node}");
        params.subject_alt_names = vec![SanType::URI(uri.as_str().try_into().unwrap())];
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ClientAuth,
            ExtendedKeyUsagePurpose::ServerAuth,
        ];
        let cert = params.signed_by(&key, &self.issuer).unwrap().der().clone();
        let key = PrivateKeyDer::try_from(key.serialize_der()).unwrap();
        let credentials = Credentials::new(
            ClusterId::new(cluster).unwrap(),
            vec![cert],
            key,
            std::slice::from_ref(&self.cert),
        )
        .unwrap();
        let tls = PeerTls::new(&credentials, Arc::new(trust)).unwrap();
        let settings = EndpointSettings {
            congestion_control: CongestionControl::Cubic,
            connect_timeout: Duration::from_secs(5),
            max_inflight_bytes: 64 << 20,
            capabilities: Capabilities::KNOWN,
            idle_timeout: skys3_peer::IDLE_TIMEOUT,
        };
        PeerEndpoint::bind("127.0.0.1:0".parse().unwrap(), &tls, settings).unwrap()
    }
}

/// The receiving bucket `archive`, in four shards.
fn archive() -> BucketDocument {
    BucketDocument {
        bucket_id: BucketId::new("b-archive").unwrap(),
        name: "archive".parse().unwrap(),
        mode: BucketMode::Local,
        shards: ShardCount::new(4).unwrap(),
        replicas: 1,
        min_write_replicas: 1,
        clean_copies: 0,
        target: None,
        created_unix_ms: 0,
        lifecycle: None,
        proposal_id: ProposalId::new("p").unwrap(),
    }
}

/// The destination's settings: `archive` receives from `prod-us`.
fn receiving() -> GatewayConfig {
    let config: Config = format!(
        "[cluster]\ncluster_id = \"{DESTINATION}\"\n[control_store]\n\
         etcd_endpoints = [\"https://etcd.invalid:2379\"]\n\
         [transport]\ntls_cert_file = \"/etc/skys3/node.crt\"\n\
         tls_key_file = \"/etc/skys3/node.key\"\ntls_ca_file = \"/etc/skys3/ca.crt\"\n\
         [buckets.archive]\nmode = \"local\"\npeer_source = \"{SOURCE}\"\n\
         [peering.peers.{SOURCE}]\nca_file = \"/etc/skys3/{SOURCE}.crt\"\n"
    )
    .parse()
    .unwrap();
    GatewayConfig::new(&config)
}

/// The commit that sends object `n` with its bytes inline.
fn commit(n: u64) -> Commit {
    let body = body(n);
    Commit {
        identity: format!("{SOURCE}/b-src/0/1.{}", n + 1).parse().unwrap(),
        bucket: "archive".parse().unwrap(),
        key: format!("logs/{n}"),
        precondition: Precondition::Absent,
        write: Write::Put(Put {
            size: body.len() as u64,
            etag: md5_etag(body.as_bytes()),
            last_modified_ms: 1_700_000_000_000,
            metadata: BTreeMap::from([
                ("content-type".to_owned(), "text/plain".to_owned()),
                ("cache-control".to_owned(), "no-cache".to_owned()),
                ("x-amz-meta-owner".to_owned(), "team-a".to_owned()),
            ]),
            tags: BTreeMap::from([("kind".to_owned(), "test".to_owned())]),
            checksums: BTreeMap::new(),
            data: PutData::Inline(Bytes::from(body)),
        }),
    }
}

/// Round trips over the peer protocol: one connection, and the objects in
/// as few batches as the protocol's limits allow.
fn peer_round_trips() -> u64 {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        // The destination: a node that applies batches to `archive`.
        let shards = MemoryShards::new().await;
        let archive = archive();
        for shard in ShardRef::all(&archive) {
            shards.open(&shard, &archive).await.unwrap();
        }
        let found = archive.clone();
        let lookup: BucketLookup =
            Arc::new(move |name: &BucketName| (*name == found.name).then(|| found.clone()));
        let staging = Arc::new(Staging::new(StagingLimits {
            quota_bytes: 1 << 30,
            ttl: Duration::from_secs(3600),
        }));
        let service = StagingService::new(
            staging,
            PeerExtents::new(shards.clone(), Arc::clone(&lookup)),
        )
        .with_commits(PeerCommits::new(shards.clone(), lookup, &receiving()));
        let (us, eu) = (Ca::new(), Ca::new());
        let mut trust = PeerTrust::new();
        let pair = BucketPair {
            source: BucketId::new("b-src").unwrap(),
            destination: "archive".parse().unwrap(),
        };
        trust
            .add(
                ClusterId::new(SOURCE).unwrap(),
                std::slice::from_ref(&us.cert),
                [pair],
            )
            .unwrap();
        let destination = eu.endpoint(DESTINATION, "eu-1", trust);
        let accepting = destination.clone();
        tokio::spawn(async move {
            while let Some(incoming) = accepting.accept().await {
                let service = service.clone();
                tokio::spawn(async move {
                    if let Ok(connection) = incoming.establish().await {
                        service.serve_connection(connection).await;
                    }
                });
            }
        });

        // The source: one connection, then a stream per batch.
        let mut trust = PeerTrust::new();
        trust
            .add(
                ClusterId::new(DESTINATION).unwrap(),
                std::slice::from_ref(&eu.cert),
                Vec::<BucketPair>::new(),
            )
            .unwrap();
        let source = us.endpoint(SOURCE, "us-1", trust);
        let to = Destination {
            cluster: ClusterId::new(DESTINATION).unwrap(),
            address: destination.local_addr().unwrap(),
        };
        let connection = bounded(source.connect(&to)).await.unwrap();
        // The QUIC handshake, then both HELLOs.
        let mut round_trips = 2;
        let mut builder = BatchBuilder::new();
        let mut batches = Vec::new();
        for n in 0..OBJECTS {
            let commit = commit(n);
            match builder.push(&commit) {
                Ok(()) => {}
                Err(Refusal::Full) => {
                    batches.extend(builder.take());
                    builder.push(&commit).unwrap();
                }
                Err(other) => panic!("object {n}: {other}"),
            }
        }
        batches.extend(builder.take());
        for batch in &batches {
            let mut stream = bounded(connection.open_stream()).await.unwrap();
            let outcomes = bounded(send_batch(&mut stream, batch)).await.unwrap();
            round_trips += 1;
            for (item, outcome) in batch.items.iter().zip(outcomes) {
                let Write::Put(put) = &item.write else {
                    unreachable!()
                };
                let committed = Outcome::Committed {
                    etag: Some(put.etag.clone()),
                };
                assert_eq!(outcome, Some(committed), "{}", item.key);
            }
        }
        connection.close();

        // Every object is published at the destination.
        for n in [0, OBJECTS / 2, OBJECTS - 1] {
            let key = format!("logs/{n}");
            let shard = ShardRef::for_key(&archive, &key);
            let entry = shards.entry(&shard, &key).await.unwrap().unwrap();
            assert_eq!(entry.object.unwrap().size, body(n).len() as u64);
        }
        round_trips
    })
}

#[test]
fn small_objects_need_fewer_round_trips_over_batches_than_over_rest() {
    let rest = rest_round_trips();
    let peer = peer_round_trips();
    let per_object = |round_trips: u64| round_trips as f64 / OBJECTS as f64;
    println!(
        "{OBJECTS} small objects: S3 REST {rest} round trips ({:.3} per object), \
         peer BATCH {peer} ({:.4} per object)",
        per_object(rest),
        per_object(peer),
    );
    // At least one request per object over REST.
    assert!(rest >= OBJECTS, "{rest} requests");
    // Two to connect, and one per batch of at most 1,024 objects.
    assert_eq!(peer, 2 + OBJECTS.div_ceil(1024));
    assert!(peer * 100 < rest, "{peer} round trips against {rest}");
}
