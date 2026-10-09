//! Native replication into a receiving bucket (design §7.8), from the
//! wire to the shard: a source cluster stages an object over loopback QUIC
//! and commits it, the destination publishes it in one record of the key's
//! shard, and duplicate `COMMIT`s, preconditions, `ABORT`, and the
//! destination's own clients are answered as the design says.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::{Setup, config, setup_with};
use http::Method;
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType,
};
use skys3_config::{BucketPair, CongestionControl};
use skys3_gateway::stub::MemoryShards;
use skys3_gateway::{BucketLookup, GatewayConfig, PeerCommits, PeerExtents, ShardRef, Shards};
use skys3_net::{CertificateDer, Credentials, PrivateKeyDer};
use skys3_peer::{
    Abort, AbortReason, Applied, ApplyError, Batch, BatchBuilder, Begin, Capabilities, Commit,
    CommitSink, Data, Destination, EndpointSettings, Message, MessageStream, Outcome,
    PeerConnection, PeerEndpoint, PeerTls, PeerTrust, Precondition, Put, PutData, StagedObject,
    Staging, StagingLimits, StagingService, Write, send_batch,
};
use skys3_types::{
    BucketDocument, BucketId, BucketMode, BucketName, ClusterId, ETag, Epoch, NodeId, ProposalId,
    ShardConfig, ShardCount, WriteIdentity,
};

/// The longest a test waits on the network.
const WAIT: Duration = Duration::from_secs(30);

const SOURCE: &str = "prod-us";
/// The destination: the gateway tests' cluster.
const DESTINATION: &str = "test";

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(WAIT, future)
        .await
        .expect("a network wait took too long")
}

/// `archive` receives from `prod-us`, which may write it from its bucket
/// `b-src`.
fn receiving() -> GatewayConfig {
    config(
        "[transport]\ntls_cert_file = \"/etc/skys3/node.crt\"\n\
         tls_key_file = \"/etc/skys3/node.key\"\ntls_ca_file = \"/etc/skys3/ca.crt\"\n\
         [buckets.archive]\nmode = \"local\"\npeer_source = \"prod-us\"\n\
         [peering.peers.prod-us]\nca_file = \"/etc/skys3/prod-us.crt\"\n\
         buckets = [{ source = \"b-src\", destination = \"archive\" }]\n",
    )
}

fn identity(seq: u64) -> WriteIdentity {
    format!("{SOURCE}/b-src/5/42.{seq}").parse().unwrap()
}

fn md5_etag(data: &[u8]) -> ETag {
    use md5::Digest as _;
    let digest = md5::Md5::digest(data);
    ETag::new(
        digest
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>(),
    )
    .unwrap()
}

fn commit(seq: u64, key: &str, precondition: Precondition, body: &[u8]) -> Commit {
    Commit {
        identity: identity(seq),
        bucket: "archive".parse().unwrap(),
        key: key.to_owned(),
        precondition,
        write: Write::Put(Put {
            size: body.len() as u64,
            etag: md5_etag(body),
            last_modified_ms: 1_700_000_000_000,
            metadata: BTreeMap::from([("content-type".to_owned(), "text/plain".to_owned())]),
            tags: BTreeMap::new(),
            checksums: BTreeMap::new(),
            data: PutData::Staged { piece: 1 },
        }),
        // Protocol version 2 refuses a COMMIT without one; these never
        // expire.
        apply_by_ms: Some(u64::MAX),
    }
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
        };
        PeerEndpoint::bind("127.0.0.1:0".parse().unwrap(), &tls, settings).unwrap()
    }
}

/// A destination gateway whose node serves native replication into
/// `archive`, and a source that streams to it.
struct Link {
    setup: Setup,
    staging: Arc<Staging>,
    source: PeerEndpoint,
    destination: PeerEndpoint,
}

impl Link {
    async fn start() -> Self {
        let config = receiving();
        let setup = setup_with(config.clone()).await;
        let archive = setup.create_local("archive").await;
        let lookup: BucketLookup =
            Arc::new(move |name: &BucketName| (*name == archive.name).then(|| archive.clone()));
        let staging = Arc::new(Staging::new(StagingLimits {
            quota_bytes: 1 << 30,
            ttl: Duration::from_secs(3600),
        }));
        let service = StagingService::new(
            Arc::clone(&staging),
            PeerExtents::new(setup.shards.clone(), Arc::clone(&lookup)),
        )
        .with_commits(PeerCommits::new(setup.shards.clone(), lookup, &config));

        let (us, here) = (Ca::new(), Ca::new());
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
        let destination = here.endpoint(DESTINATION, "node-1", trust);
        let mut trust = PeerTrust::new();
        trust
            .add(
                ClusterId::new(DESTINATION).unwrap(),
                std::slice::from_ref(&here.cert),
                Vec::<BucketPair>::new(),
            )
            .unwrap();
        let source = us.endpoint(SOURCE, "us-1", trust);
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
        Self {
            setup,
            staging,
            source,
            destination,
        }
    }

    /// A new connection, as after a link drop, and a stream on it.
    async fn stream(&self) -> (PeerConnection, MessageStream) {
        let to = Destination {
            cluster: ClusterId::new(DESTINATION).unwrap(),
            address: self.destination.local_addr().unwrap(),
        };
        let connection = bounded(self.source.connect(&to)).await.unwrap();
        let stream = bounded(connection.open_stream()).await.unwrap();
        (connection, stream)
    }
}

async fn send(stream: &mut MessageStream, message: Message) {
    bounded(stream.send(&message)).await.unwrap();
}

/// The next message that is not a `DURABLE`.
async fn next(stream: &mut MessageStream) -> Message {
    loop {
        match bounded(stream.recv()).await.unwrap() {
            Some(Message::Durable(_)) => {}
            Some(message) => return message,
            None => panic!("the stream ended"),
        }
    }
}

/// Stages `body` for `key` as piece 1 under `seq`, in frames of `frame`
/// bytes, without waiting for them to be durable.
async fn stage(stream: &mut MessageStream, seq: u64, key: &str, body: &[u8], frame: usize) {
    let begin = Begin {
        identity: identity(seq),
        bucket: "archive".parse().unwrap(),
        key: key.to_owned(),
    };
    send(stream, Message::Begin(begin)).await;
    assert!(matches!(next(stream).await, Message::Resume(_)));
    for (n, chunk) in body.chunks(frame).enumerate() {
        let data = Data {
            piece: 1,
            offset: (n * frame) as u64,
            bytes: Bytes::copy_from_slice(chunk),
        };
        send(stream, Message::Data(data)).await;
    }
}

async fn applied(stream: &mut MessageStream, commit: &Commit) -> Outcome {
    send(stream, Message::Commit(commit.clone())).await;
    match next(stream).await {
        Message::Applied(Applied { identity, outcome }) => {
            assert_eq!(identity, commit.identity);
            outcome
        }
        other => panic!("expected APPLIED, got {other:?}"),
    }
}

#[tokio::test]
async fn commits_publish_staged_objects_once() {
    let link = Link::start().await;
    let body: Vec<u8> = (0..10_000u32).map(|n| b'a' + (n % 26) as u8).collect();
    let first = commit(1, "logs/today.txt", Precondition::Absent, &body);
    let committed = Outcome::Committed {
        etag: Some(md5_etag(&body)),
    };

    // The COMMIT follows the frames without waiting for their DURABLEs:
    // the destination settles them first.
    let (connection, mut stream) = link.stream().await;
    stage(&mut stream, 1, "logs/today.txt", &body, 4096).await;
    assert_eq!(applied(&mut stream, &first).await, committed);
    assert!(link.staging.is_empty(), "the commit consumes its staging");
    // The link drops before the APPLIED arrives, as far as the source
    // knows.
    connection.close();

    // The object is visible to the destination's clients, without the
    // write identity.
    let get = link
        .setup
        .call(Method::GET, "/archive/logs/today.txt", &[], "")
        .await;
    get.assert(200, None);
    assert_eq!(get.body.as_bytes(), &body[..]);
    let etag = get.headers["etag"].to_str().unwrap();
    assert_eq!(etag, format!("\"{}\"", md5_etag(&body)));
    assert!(get.headers.get("x-amz-meta-skys3-wid").is_none());

    // The replay, on a new connection with no BEGIN and no staging,
    // returns the stored result and writes nothing.
    let (connection, mut stream) = link.stream().await;
    let archive = link.setup.register("archive").await.unwrap();
    let shard = ShardRef::for_key(&archive, "logs/today.txt");
    let before = link.setup.shards.entry(&shard, "logs/today.txt").await;
    assert_eq!(applied(&mut stream, &first).await, committed);
    let after = link.setup.shards.entry(&shard, "logs/today.txt").await;
    assert_eq!(before, after);

    // A precondition that fails names the current write identity, with
    // or without staging.
    let stale = commit(2, "logs/today.txt", Precondition::Absent, b"new");
    let current = Outcome::PreconditionFailed {
        current: Some(identity(1)),
    };
    assert_eq!(applied(&mut stream, &stale).await, current);
    stage(&mut stream, 2, "logs/today.txt", b"new", 4096).await;
    assert_eq!(applied(&mut stream, &stale).await, current);
    // Staging survives a precondition failure, so the source can commit
    // it under the precondition the destination named.
    let next_version = commit(
        2,
        "logs/today.txt",
        Precondition::Matches(identity(1)),
        b"new",
    );
    assert!(matches!(
        applied(&mut stream, &next_version).await,
        Outcome::Committed { .. }
    ));
    let get = link
        .setup
        .call(Method::GET, "/archive/logs/today.txt", &[], "")
        .await;
    assert_eq!(get.body, "new");

    // ABORT discards the staging: the COMMIT that follows has nothing to
    // publish.
    stage(&mut stream, 3, "logs/aborted.txt", &body, 4096).await;
    let abort = Abort {
        identity: identity(3),
        reason: AbortReason::Cancelled,
        detail: String::new(),
    };
    send(&mut stream, Message::Abort(abort)).await;
    let aborted = commit(3, "logs/aborted.txt", Precondition::Absent, &body);
    let outcome = applied(&mut stream, &aborted).await;
    assert!(
        matches!(
            outcome,
            Outcome::Failed {
                error: ApplyError::Incomplete,
                ..
            }
        ),
        "{outcome:?}"
    );
    assert!(link.staging.is_empty());
    link.setup
        .call(Method::GET, "/archive/logs/aborted.txt", &[], "")
        .await
        .assert(404, Some("NoSuchKey"));

    stream.finish().unwrap();
    assert_eq!(bounded(stream.recv()).await.unwrap(), None);
    connection.close();
}

/// `commit`, with `body` inline, as a `BATCH` carries it.
fn inline(seq: u64, key: &str, precondition: Precondition, body: &[u8]) -> Commit {
    let mut commit = commit(seq, key, precondition, body);
    if let Write::Put(put) = &mut commit.write {
        put.data = PutData::Inline(Bytes::copy_from_slice(body));
    }
    commit
}

#[tokio::test]
async fn a_batch_publishes_small_objects_in_one_round_trip() {
    let link = Link::start().await;
    let bodies: Vec<String> = (1..=50).map(|n| format!("small object {n}")).collect();
    let mut builder = BatchBuilder::new();
    for (seq, body) in (1..).zip(&bodies) {
        let item = inline(
            seq,
            &format!("small/{seq}"),
            Precondition::Absent,
            body.as_bytes(),
        );
        builder.push(&item).unwrap();
    }
    let batch = builder.take().unwrap();
    let (connection, mut stream) = link.stream().await;
    let outcomes = bounded(send_batch(&mut stream, &batch)).await.unwrap();
    for (body, outcome) in bodies.iter().zip(&outcomes) {
        let committed = Outcome::Committed {
            etag: Some(md5_etag(body.as_bytes())),
        };
        assert_eq!(outcome.as_ref(), Some(&committed));
    }
    // Nothing was staged, and the objects are visible to the
    // destination's clients, without the write identity.
    assert!(link.staging.is_empty());
    let get = link
        .setup
        .call(Method::GET, "/archive/small/7", &[], "")
        .await;
    get.assert(200, None);
    assert_eq!(get.body, "small object 7");
    assert!(get.headers.get("x-amz-meta-skys3-wid").is_none());

    // The batch again, as after lost APPLIEDs, on a stream of its own:
    // the stored results, and nothing written.
    let archive = link.setup.register("archive").await.unwrap();
    let shard = ShardRef::for_key(&archive, "small/7");
    let before = link.setup.shards.entry(&shard, "small/7").await;
    let mut stream = bounded(connection.open_stream()).await.unwrap();
    let replayed = bounded(send_batch(&mut stream, &batch)).await.unwrap();
    assert_eq!(replayed, outcomes);
    assert_eq!(link.setup.shards.entry(&shard, "small/7").await, before);

    // The next batch deletes one, and finds another changed.
    let next = Batch {
        items: vec![
            Commit {
                write: Write::Delete,
                ..commit(51, "small/1", Precondition::Matches(identity(1)), b"")
            },
            inline(52, "small/2", Precondition::Matches(identity(9)), b"newer"),
        ],
    };
    let mut stream = bounded(connection.open_stream()).await.unwrap();
    let outcomes = bounded(send_batch(&mut stream, &next)).await.unwrap();
    assert_eq!(
        outcomes,
        [
            Some(Outcome::Committed { etag: None }),
            Some(Outcome::PreconditionFailed {
                current: Some(identity(2))
            })
        ]
    );
    link.setup
        .call(Method::GET, "/archive/small/1", &[], "")
        .await
        .assert(404, Some("NoSuchKey"));
    connection.close();
}

/// A request: its method, URI, headers, and body.
type Request<'a> = (Method, &'a str, &'a [(&'a str, &'a str)], &'a str);

#[tokio::test]
async fn a_receiving_bucket_is_read_only_to_its_own_clients() {
    let setup = setup_with(receiving()).await;
    setup.create_local("archive").await;
    setup.create_local("photos").await;
    let tagging = "<Tagging><TagSet></TagSet></Tagging>";
    let delete_objects = "<Delete><Object><Key>k</Key></Object></Delete>";
    let copy = [("x-amz-copy-source", "/photos/k")];
    let denied: [Request<'_>; 9] = [
        (Method::PUT, "/archive/k", &[], "local"),
        (Method::DELETE, "/archive/k", &[], ""),
        (Method::POST, "/archive?delete", &[], delete_objects),
        (Method::PUT, "/archive/copy", &copy, ""),
        (Method::PUT, "/archive/k?tagging", &[], tagging),
        (Method::DELETE, "/archive/k?tagging", &[], ""),
        (Method::POST, "/archive/k?uploads", &[], ""),
        (
            Method::PUT,
            "/archive/k?partNumber=1&uploadId=x",
            &[],
            "part",
        ),
        (Method::DELETE, "/archive/k?uploadId=x", &[], ""),
    ];
    for (method, uri, headers, body) in denied {
        let answer = setup.call(method.clone(), uri, headers, body).await;
        answer.assert(403, Some("AccessDenied"));
        assert!(
            answer.body.contains("prod-us"),
            "{method} {uri}: {answer:?}"
        );
    }
    let complete = "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>\
                    <ETag>\"x\"</ETag></Part></CompleteMultipartUpload>";
    setup
        .call(Method::POST, "/archive/k?uploadId=x", &[], complete)
        .await
        .assert(403, Some("AccessDenied"));
    // Reads are served, and other buckets take writes.
    setup
        .call(Method::GET, "/archive/k", &[], "")
        .await
        .assert(404, Some("NoSuchKey"));
    setup
        .call(Method::PUT, "/photos/k", &[], "local")
        .await
        .assert(200, None);

    // `peer_local_writes` lets them write.
    let config = config(
        "[transport]\ntls_cert_file = \"/x.crt\"\ntls_key_file = \"/x.key\"\n\
         tls_ca_file = \"/ca.crt\"\n[buckets.archive]\nmode = \"local\"\n\
         peer_source = \"prod-us\"\npeer_local_writes = true\n\
         [peering.peers.prod-us]\nca_file = \"/us.crt\"\n",
    );
    let setup = setup_with(config).await;
    setup.create_local("archive").await;
    setup
        .call(Method::PUT, "/archive/k", &[], "local")
        .await
        .assert(200, None);
}

/// A replay that reaches a new primary, after the destination lost its
/// staging index and the old primary, finds the stored result in the
/// shard's log.
#[tokio::test]
async fn a_commit_replayed_after_a_primary_change_returns_the_stored_result() {
    let config = receiving();
    let shards = MemoryShards::new().await;
    let archive = BucketDocument {
        bucket_id: BucketId::new("b-archive").unwrap(),
        name: "archive".parse().unwrap(),
        mode: BucketMode::Local,
        shards: ShardCount::new(2).unwrap(),
        replicas: 1,
        min_write_replicas: 1,
        clean_copies: 0,
        target: None,
        created_unix_ms: 0,
        lifecycle: None,
        proposal_id: ProposalId::new("p").unwrap(),
    };
    let key = "k";
    let shard = ShardRef::for_key(&archive, key);
    shards.open(&shard, &archive).await.unwrap();
    let lookup = |archive: &BucketDocument| -> BucketLookup {
        let archive = archive.clone();
        Arc::new(move |name: &BucketName| (*name == archive.name).then(|| archive.clone()))
    };

    // Node 1, the primary in epoch 1, stages and publishes the object.
    let body = b"published before the primary changed";
    let extents = PeerExtents::new(shards.clone(), lookup(&archive));
    let extent =
        skys3_peer::ExtentSink::append(&extents, &archive.name, key, 0, Bytes::from_static(body))
            .await
            .unwrap();
    let staged = StagedObject {
        bucket: archive.name.clone(),
        key: key.to_owned(),
        pieces: BTreeMap::from([(1, BTreeMap::from([(0, extent)]))]),
    };
    let first = commit(1, key, Precondition::Absent, body);
    let commits = PeerCommits::new(shards.clone(), lookup(&archive), &config);
    let committed = Outcome::Committed {
        etag: Some(md5_etag(body)),
    };
    assert_eq!(commits.apply(&first, Some(&staged)).await, committed);
    let published = shards.entry(&shard, key).await.unwrap();

    // Node 2 holds node 1's log and becomes the primary in epoch 2.
    shards.disk().crash();
    let node = NodeId::new("node-2").unwrap();
    let shards = MemoryShards::open_as(shards.disk().clone(), node.clone())
        .await
        .unwrap();
    let mut epoch_two: ShardConfig = shards.local().config(&shard, &archive);
    epoch_two.epoch = Epoch::new(2);
    epoch_two.primary = node.clone();
    epoch_two.members = vec![node.clone()];
    let replica = shards
        .local()
        .set()
        .open_replica(&epoch_two, &node)
        .await
        .unwrap();
    assert_eq!(replica.sequencing(), Epoch::new(2));

    // The replay returns the stored result, and the key is unchanged.
    let commits = PeerCommits::new(shards.clone(), lookup(&archive), &config);
    assert_eq!(commits.apply(&first, None).await, committed);
    assert_eq!(shards.entry(&shard, key).await.unwrap(), published);
    // The next version applies on the new primary.
    let second = commit(2, key, Precondition::Matches(identity(1)), b"");
    assert_eq!(
        commits.apply(&second, None).await,
        Outcome::Committed {
            etag: Some(md5_etag(b""))
        }
    );
}
