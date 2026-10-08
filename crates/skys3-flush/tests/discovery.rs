//! Discovery and fallback of targets that may be SkyS3 peers (§7.8): the
//! descriptor read from the target's S3 endpoint and verified, QUIC while
//! its handshake succeeds, S3 REST after the quarantine while it fails, the
//! way back once it succeeds again, and the status and metric of each.

mod support;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType, date_time_ymd,
};
use skys3_config::{BucketsConfig, Config};
use skys3_flush::{
    FlushMetrics, FlushService, LinkError, PeerBug, PeerLink, PeerTransport, Transport,
    TransportStatus, test_hooks,
};
use skys3_io::{ManualWallClock, SimMount};
use skys3_net::{CertificateDer, Credentials, PrivateKeyDer};
use skys3_obs::MetricsRegistry;
use skys3_peer::{
    DESCRIPTOR_KEY, DescriptorSigner, DescriptorVerifier, PeerDescriptor, PeerTls, PeerTrust,
};
use skys3_remote::{ObjectStore, PutObject};
use skys3_sim::SimS3;
use skys3_sim::s3::SimS3Config;
use skys3_types::{
    BucketDocument, BucketMode, BucketName, ClusterId, ProposalId, RemoteTarget, ShardCount,
};
use support::peer::{BUCKET, Destination, Link};
use support::{Node, cluster, runtime, settings, shard_ref};
use tokio::time::Instant;

/// The destination cluster.
const DESTINATION: &str = "dest";
/// When the tests run, on the wall clock descriptors are checked on.
const NOW: Duration = Duration::from_secs(1_800_000_000);
/// How long a peer target waits before its first S3 flush.
const QUARANTINE: Duration = Duration::from_secs(2);

/// A CA of the destination cluster, and a node certificate from it.
struct Pki {
    ca: CertificateDer<'static>,
    signer: DescriptorSigner,
}

fn pki(cluster: &str) -> Pki {
    let ca_key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.distinguished_name.push(DnType::CommonName, cluster);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
    let ca = params.self_signed(&ca_key).unwrap().der().clone();
    let issuer = Issuer::new(params, ca_key);
    let key = KeyPair::generate().unwrap();
    let mut leaf = CertificateParams::new(Vec::<String>::new()).unwrap();
    leaf.subject_alt_names = vec![SanType::URI(
        format!("spiffe://{cluster}/node/n1").try_into().unwrap(),
    )];
    leaf.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ClientAuth,
        ExtendedKeyUsagePurpose::ServerAuth,
    ];
    (leaf.not_before, leaf.not_after) = (date_time_ymd(2000, 1, 1), date_time_ymd(4000, 1, 1));
    let cert = leaf.signed_by(&key, &issuer).unwrap().der().clone();
    let credentials = Credentials::new(
        ClusterId::new(cluster).unwrap(),
        vec![cert],
        PrivateKeyDer::Pkcs8(key.serialize_der().into()),
        std::slice::from_ref(&ca),
    )
    .unwrap();
    let signer = PeerTls::new(&credentials, Arc::new(PeerTrust::new()))
        .unwrap()
        .descriptor_signer();
    Pki { ca, signer }
}

/// The destination's descriptor of its bucket, signed by `pki`.
fn descriptor(pki: &Pki, cluster: &str, issued: Duration) -> Bytes {
    let descriptor = PeerDescriptor::new(
        ClusterId::new(cluster).unwrap(),
        BucketName::new(BUCKET).unwrap(),
        support::cluster(),
        vec!["dest.example.internal:7443".to_owned()],
        issued,
    );
    pki.signer.sign(&descriptor).unwrap()
}

/// A `write_back` bucket whose target is the destination's bucket, under
/// the prefix `team/`.
fn bucket() -> BucketDocument {
    BucketDocument {
        bucket_id: shard_ref().bucket,
        name: BucketName::new("photos").unwrap(),
        mode: BucketMode::WriteBack,
        shards: ShardCount::new(1).unwrap(),
        replicas: 1,
        min_write_replicas: 1,
        clean_copies: 1,
        target: Some(RemoteTarget {
            endpoint: "https://dest.example.internal".to_owned(),
            bucket: BUCKET.to_owned(),
            prefix: Some("team/".to_owned()),
        }),
        created_unix_ms: 0,
        lifecycle: None,
        proposal_id: ProposalId::new("p-1").unwrap(),
    }
}

fn buckets(transport: &str) -> BucketsConfig {
    let text = format!(
        "[cluster]\ncluster_id = \"c-test\"\n\
         [control_store]\netcd_endpoints = [\"https://etcd.invalid:2379\"]\n\
         [buckets.photos]\ntarget_transport = \"{transport}\"\n"
    );
    text.parse::<Config>().unwrap().buckets().clone()
}

struct World {
    node: Node,
    store: SimS3,
    destination: Arc<Destination>,
    service: FlushService<SimS3, SimMount>,
    metrics: FlushMetrics,
    pki: Pki,
}

/// A node whose service flushes `photos` to the destination, with
/// `target_transport` as given, trusting the destination's CA.
async fn world(transport: &str) -> World {
    let node = Node::open(7).await;
    let store = SimS3::new(7, SimS3Config::default());
    let destination = Destination::start(true).await;
    let pki = pki(DESTINATION);
    let mut trust = PeerTrust::new();
    trust
        .add(
            ClusterId::new(DESTINATION).unwrap(),
            std::slice::from_ref(&pki.ca),
            [],
        )
        .unwrap();
    let to = Arc::clone(&destination);
    let peers = PeerTransport::new(
        Box::new(move |_descriptor: &PeerDescriptor| {
            let to = Arc::clone(&to);
            Box::pin(async move {
                if to.faults().down {
                    return Err(LinkError::new("UDP is blocked"));
                }
                Ok(Arc::new(Link(to)) as Arc<dyn PeerLink>)
            })
        }),
        DescriptorVerifier::new(Arc::new(trust)),
        1024,
    )
    .with_timeout(Duration::from_secs(1))
    .with_connect_timeout(Duration::from_millis(500))
    .with_reprobe(Duration::from_millis(500), Duration::from_secs(2))
    .with_quarantine(QUARANTINE);
    let metrics = FlushMetrics::register(&MetricsRegistry::new());
    let service = FlushService::new(
        cluster(),
        settings(),
        Box::new({
            let store = store.clone();
            move |_| store.clone()
        }),
        metrics.clone(),
    )
    .with_wall_clock(Arc::new(ManualWallClock::new(NOW)))
    .with_buckets(buckets(transport))
    .with_peer_transport(peers);
    World {
        node,
        store,
        destination,
        service,
        metrics,
        pki,
    }
}

impl World {
    /// Reconciles until `done` holds of the bucket's transport status.
    async fn until(&self, what: &str, done: impl Fn(&World, &TransportStatus) -> bool) {
        let bucket = bucket();
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            self.service
                .reconcile(std::slice::from_ref(&bucket), &self.node.set)
                .await;
            self.service.refresh_metrics();
            let status = self.service.status(&bucket.bucket_id).unwrap();
            let transport = status.transport.expect("the target has a discovery");
            if done(self, &transport) {
                return;
            }
            assert!(Instant::now() < deadline, "never {what}: {transport:?}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn serve_descriptor(&self, bytes: Bytes) {
        self.store
            .put_object(PutObject::new(DESCRIPTOR_KEY, bytes))
            .await
            .unwrap();
    }

    async fn at_destination(&self, key: &str) -> bool {
        self.destination
            .entry(&format!("team/{key}"))
            .await
            .is_some()
    }

    fn at_store(&self, key: &str) -> bool {
        self.store.object(&format!("team/{key}")).is_some()
    }
}

#[test]
fn flushing_falls_back_to_s3_while_quic_fails_and_returns() {
    runtime().block_on(async {
        let world = world("auto").await;
        world
            .serve_descriptor(descriptor(&world.pki, DESTINATION, NOW))
            .await;
        world.node.put("a", "over quic").await;
        world
            .until("flushed over QUIC", |_, t| {
                t.in_use == Some(Transport::Quic)
            })
            .await;
        world.until_at_destination("a").await;
        let status = world.service.status(&bucket().bucket_id).unwrap();
        let transport = status.transport.unwrap();
        assert!(status.native && transport.peer, "{transport:?}");
        assert!(
            transport.reason.contains("QUIC to cluster dest"),
            "{transport:?}"
        );
        assert_eq!(world.metrics.transport("photos"), Some(Transport::Quic));

        // UDP is blocked: the next flush's link fails, the handshake
        // too, and the target falls back to S3 REST, after the quarantine.
        world.destination.faults().down = true;
        world.node.put("b", "over s3").await;
        world
            .until("fell back", |_, t| t.in_use == Some(Transport::S3))
            .await;
        let fell_back = Instant::now();
        let status = world.service.status(&bucket().bucket_id).unwrap();
        let transport = status.transport.unwrap();
        assert!(!status.native && transport.peer, "{transport:?}");
        assert_eq!(transport.switches, 1);
        assert!(transport.reason.contains("UDP is blocked"), "{transport:?}");
        assert_eq!(world.metrics.transport("photos"), Some(Transport::S3));
        world
            .until("flushed over S3", |world, _| world.at_store("b"))
            .await;
        assert!(fell_back.elapsed() >= QUARANTINE - Duration::from_millis(100));
        assert!(!world.at_destination("b").await);

        // UDP is allowed again: a re-probe finds QUIC, and flushes use it.
        world.destination.faults().down = false;
        world
            .until("returned to QUIC", |_, t| t.in_use == Some(Transport::Quic))
            .await;
        world.node.put("c", "over quic again").await;
        world.until_at_destination("c").await;
        let transport = world
            .service
            .status(&bucket().bucket_id)
            .unwrap()
            .transport
            .unwrap();
        assert_eq!(transport.switches, 2);
        assert_eq!(world.metrics.transport("photos"), Some(Transport::Quic));

        // A bucket no longer flushed has no transport series.
        world.service.reconcile(&[], &world.node.set).await;
        world.service.refresh_metrics();
        assert_eq!(world.metrics.transport("photos"), None);
    });
}

impl World {
    async fn until_at_destination(&self, key: &str) {
        let deadline = Instant::now() + Duration::from_secs(120);
        while !self.at_destination(key).await {
            assert!(Instant::now() < deadline, "{key} never reached the peer");
            self.service.reconcile(&[bucket()], &self.node.set).await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

#[test]
fn targets_without_a_valid_descriptor_are_plain_s3() {
    runtime().block_on(async {
        // No descriptor: not a peer; probed and flushed over S3.
        let world = world("auto").await;
        world.node.put("a", "plain").await;
        world
            .until("decided", |_, t| t.in_use == Some(Transport::S3))
            .await;
        world
            .until("flushed over S3", |world, _| world.at_store("a"))
            .await;
        let transport = world
            .service
            .status(&bucket().bucket_id)
            .unwrap()
            .transport
            .unwrap();
        assert!(!transport.peer, "{transport:?}");
        assert!(
            transport.reason.contains("no peer descriptor"),
            "{transport:?}"
        );
        world.service.refresh_metrics();
        assert_eq!(world.metrics.transport("photos"), None);

        // A forged descriptor (another CA's), and an expired one.
        for expired in [false, true] {
            let world = self::world("auto").await;
            let (bytes, refusal) = if expired {
                let issued = NOW - Duration::from_secs(7200);
                (descriptor(&world.pki, DESTINATION, issued), "expired")
            } else {
                (
                    descriptor(&pki(DESTINATION), DESTINATION, NOW),
                    "not trusted",
                )
            };
            world.serve_descriptor(bytes).await;
            world.node.put("a", "plain").await;
            world
                .until("refused the descriptor", |_, t| t.reason.contains(refusal))
                .await;
            world
                .until("flushed over S3", |world, _| world.at_store("a"))
                .await;
            assert!(!world.at_destination("a").await);
        }
    });
}

async fn world_with_descriptor(transport: &str, bytes: Bytes) -> World {
    let world = self::world(transport).await;
    world.serve_descriptor(bytes).await;
    world
}

#[test]
fn an_unverified_descriptor_is_the_seeded_bug() {
    runtime().block_on(async {
        test_hooks::seed_peer_bug(PeerBug::UnverifiedDescriptor);
        let forged = descriptor(&pki(DESTINATION), DESTINATION, NOW);
        let world = world_with_descriptor("auto", forged).await;
        world.node.put("a", "misled").await;
        world.until_at_destination("a").await;
        test_hooks::seed_peer_bug(PeerBug::None);
    });
}

#[test]
fn a_native_target_waits_for_quic() {
    runtime().block_on(async {
        let world = world("native").await;
        world
            .serve_descriptor(descriptor(&world.pki, DESTINATION, NOW))
            .await;
        world.destination.faults().down = true;
        world.node.put("a", "waits").await;
        world
            .until("tried a handshake", |_, t| {
                t.reason.contains("UDP is blocked")
            })
            .await;
        tokio::time::sleep(QUARANTINE * 3).await;
        world
            .until("still waiting", |_, t| t.in_use.is_none())
            .await;
        assert!(!world.at_store("a"));
        // The metric shows neither transport.
        assert_eq!(world.metrics.transport("photos"), None);
        world.destination.faults().down = false;
        world.until_at_destination("a").await;
        assert!(!world.at_store("a"));
    });
}
