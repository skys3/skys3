//! A SkyS3 peer that the cluster's buckets flush to over the native peer
//! protocol (§7.8, plans M6-06 and M6-07).
//!
//! With [`ClusterConfig::peer`](crate::ClusterConfig::peer), the targets
//! of the `write_back` buckets and the backup targets of the `local` ones
//! name the bucket [`BUCKET`] of a destination cluster of one node, the
//! host [`HOST`], with the `target_transport` of [`Peer::transport`]. The
//! destination runs the real thing on a simulated disk: its shards and
//! index, recovered in each life, the staging service, whose `COMMIT`s
//! apply through the gateway's peer commits, and its S3 gateway, which
//! serves the bucket's signed peer descriptor and takes the source's
//! flushes over S3 REST under the access key of its peer. The nodes reach
//! the native service over TCP on the simulated network, one connection
//! per stream, so that partitions, holds, message loss, and restarts of
//! the peer ([`Fault::PeerRestart`]) cut streams in the middle, and the
//! gateway through a relay host (see [`peer_s3`](crate::peer_s3)), which
//! those faults do not cut: they block the native transport as a firewall
//! that blocks UDP would. The destination ends a stream that sends nothing
//! for [`Peer::idle`], as a QUIC connection's idle timeout does.
//!
//! The nodes' discovery reads the descriptor through the gateway, checks
//! it against the destination's CA, and tries a TCP connection to the
//! address it names within [`Peer::connect_timeout`]: QUIC if that
//! succeeds, S3 REST through the relay if not.
//!
//! The audits:
//!
//! - **`FLUSHED` only after `APPLIED`.** Each `FLUSHED` record a flusher
//!   decides on for a native target names the write identity of the
//!   `COMMIT` whose `APPLIED` it follows; the destination must have
//!   answered that identity `committed`, or named it as the key's current
//!   identity, before (`PeerBug::FlushedOnCommit` breaks this).
//! - **At each acknowledgement**, with `ack_policy` or `backup_ack =
//!   "write_through"`: the destination holds the write, as the remote
//!   store does without a peer (`PeerBug::AnsweredOnCommit` breaks this).
//! - **At the end**, after a power loss of the destination too: it holds
//!   exactly what every member holds of each key, and no write identity in
//!   more than one of its `PUT` records, over either transport: each write
//!   applied once.
//! - **No `COMMIT` outlives the quarantine.** No `COMMIT` reaches the
//!   destination after an S3 write of its key that was acknowledged after
//!   the `COMMIT` was sent (`PeerBug::NoQuarantine` breaks this).
//! - **No QUIC to an unverified peer.** While the destination serves a
//!   descriptor that does not verify ([`DescriptorKind`]), no `COMMIT`
//!   reaches it (`PeerBug::UnverifiedDescriptor` breaks this).
//! - **The transport status and metric follow the switches.** Sampled on
//!   every live node: the metric of each peer target names the transport
//!   its status does, and its count of switches grows whenever that
//!   changes. With `target_transport = "auto"` and a descriptor that
//!   verifies, every peer target is back on QUIC once the faults healed.
//!
//! [`Fault::PeerRestart`]: crate::Fault::PeerRestart

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use http::Request;
use s3s::{Body, S3Error};
use skys3_config::{BucketPair, TargetTransport};
use skys3_control::{
    Expected, MemoryControlStore, ProposalIds, ProposalOutcome, RetryPolicy, TypedKey, bootstrap,
    bump_generation, propose_document,
};
use skys3_flush::{
    BoxFuture, FlushService, FlushedRecord, LinkError, PeerBug, PeerLink, PeerReceive, PeerSend,
    PeerStream, PeerTransport, Transport,
};
use skys3_gateway::sigv4::{AuthMethod, Authenticated};
use skys3_gateway::{
    Authenticator, BucketLookup, Gateway, GatewayConfig, IdSource, PeerCommits, PeerDescriptors,
    PeerExtents, Permissions, Principal, ShardRef, Shards,
};
use skys3_index::{Index, IndexConfig, ListItem, ListQuery};
use skys3_io::{BlockingPool, Drift, MonoTime, SimDisk, SimMount, WallClock};
use skys3_log::record::IDENTITY_METADATA;
use skys3_log::{LogConfig, RecordBody};
use skys3_peer::{
    DescriptorSigner, DescriptorVerifier, Inbound, Message, Outbound, Outcome, PREFIX_LEN,
    PeerDescriptor, PeerTls, PeerTrust, Staging, StagingLimits, StagingService, StreamError,
    parse_prefix,
};
use skys3_sim::NodeClock;
use skys3_sim::check::Violation;
use skys3_types::{
    BucketDocument, BucketId, BucketMode, BucketName, ClusterId, Label, NodeId, ProposalId,
    ShardCount,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use turmoil::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use turmoil::net::{TcpListener, TcpStream};

use crate::backup::Flushers;
use crate::node::{BoxError, INDEX_FILE};
use crate::peer_s3::{ACCESS_KEY, ACCESS_KEY_HEADER, SimStore};
use crate::pki::Pki;
use crate::s3;

/// The destination's host.
pub(crate) const HOST: &str = "peer";
/// The port its peer service listens on.
const PORT: u16 = 7600;
/// The endpoint that native targets name.
pub(crate) const ENDPOINT: &str = "https://peer.sim.internal";
/// The destination bucket, which every source bucket flushes to under a
/// key prefix of its own, as it would to the remote store.
pub(crate) const BUCKET: &str = "peer-sink";
/// The destination cluster.
const CLUSTER: &str = "peer-sim";
/// The address the destination's descriptor names for its native service.
const ADVERTISED: &str = "peer:7600";
/// How long the destination's descriptors are backdated when they are to
/// be expired: past their lifetime.
const EXPIRED_BY: Duration = Duration::from_secs(2 * 3600);
/// How often the driver samples the transport of the live nodes' peer
/// targets.
const SAMPLE_INTERVAL: Duration = Duration::from_millis(250);

/// A SkyS3 peer that the buckets flush to over the native protocol, and
/// the seeded bug the flushers run, if any.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Peer {
    /// `peer_frame_bytes`: the size of `DATA` frames, and of the largest
    /// object a `BATCH` carries.
    pub frame_bytes: u64,
    /// How long a stream may make no progress before its flush fails.
    pub timeout: Duration,
    /// `peer_staging_ttl_seconds` at the destination.
    pub staging_ttl: Duration,
    /// The latency of every message between a node and the peer, each
    /// way: another cluster is further away than the nodes are from each
    /// other.
    pub latency: Duration,
    /// The buckets' `target_transport`: `native`, or `auto` to fall back
    /// to S3 REST while the native transport is blocked.
    pub transport: TargetTransport,
    /// `peer_connect_timeout`: how long a node's handshake with the peer
    /// may take before the target falls back.
    pub connect_timeout: Duration,
    /// The least and the most time between re-probes of a target that
    /// fell back.
    pub reprobe: (Duration, Duration),
    /// How long the destination waits for the next message of a stream
    /// before it ends the stream, as a QUIC connection's idle timeout.
    pub idle: Duration,
    /// The descriptor the destination serves.
    pub descriptor: DescriptorKind,
    /// UDP blocks aimed at `COMMIT`s: the first `count` times a node sends
    /// a `COMMIT`, at least `every` apart, the links between every node
    /// and the peer are held for `hold`, which catches the `COMMIT` on its
    /// way.
    pub aimed: AimedBlocks,
    /// A seeded bug of the flushers' native transport.
    pub bug: PeerBug,
}

/// UDP blocks aimed at `COMMIT`s on their way ([`Peer::aimed`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AimedBlocks {
    /// How many.
    pub count: usize,
    /// How long each holds the links.
    pub hold: Duration,
    /// The least time between the starts of two.
    pub every: Duration,
}

/// The descriptor the destination serves.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DescriptorKind {
    /// Signed with the key of its node's certificate, which the source
    /// trusts.
    #[default]
    Signed,
    /// Signed with a key the source does not trust, under the
    /// destination's name.
    Forged,
    /// Signed as it should be, but expired.
    Expired,
}

impl Default for Peer {
    /// Frames of 256 bytes, so that the workload's bodies go in batches
    /// and in several frames, short timeouts, and 20 ms each way, on the
    /// native transport only.
    fn default() -> Self {
        Self {
            frame_bytes: 256,
            timeout: Duration::from_secs(2),
            staging_ttl: Duration::from_secs(5),
            latency: Duration::from_millis(20),
            transport: TargetTransport::Native,
            connect_timeout: Duration::from_secs(1),
            reprobe: (Duration::from_secs(1), Duration::from_secs(4)),
            idle: Duration::from_secs(8),
            descriptor: DescriptorKind::Signed,
            aimed: AimedBlocks::default(),
            bug: PeerBug::None,
        }
    }
}

impl Peer {
    /// How the nodes' flush services reach the peer, whose descriptors
    /// `verifier` checks and whose `COMMIT`s `log` records.
    pub(crate) fn transport(
        self,
        verifier: DescriptorVerifier,
        log: Arc<PeerLog>,
    ) -> PeerTransport {
        PeerTransport::new(
            Box::new(move |descriptor: &PeerDescriptor| {
                let (address, log) = (descriptor.addresses.first().cloned(), Arc::clone(&log));
                Box::pin(async move {
                    let address = address.ok_or_else(|| LinkError::new("no address"))?;
                    let (host, port) = address
                        .rsplit_once(':')
                        .and_then(|(host, port)| Some((host.to_owned(), port.parse().ok()?)))
                        .ok_or_else(|| LinkError::new("not host:port"))?;
                    // The handshake: a connection the destination accepts.
                    drop(
                        TcpStream::connect((host.as_str(), port))
                            .await
                            .map_err(link)?,
                    );
                    Ok(Arc::new(TcpLink { host, port, log }) as Arc<dyn PeerLink>)
                })
            }),
            verifier,
            self.frame_bytes,
        )
        .with_timeout(self.timeout)
        .with_connect_timeout(self.connect_timeout)
        .with_reprobe(self.reprobe.0, self.reprobe.1)
        .with_commit_window(self.commit_window())
        .with_quarantine(self.quarantine())
    }

    /// How long after it is sent a `COMMIT` may still apply: the
    /// destination's idle timeout, so that a `COMMIT` held on the way for
    /// less than that still can, as the seeded bug of no quarantine needs.
    #[must_use]
    pub fn commit_window(self) -> Duration {
        self.idle
    }

    /// How long a node's flushers wait before they flush a shard over S3
    /// REST to the peer: more than the commit window, which is all the
    /// node binary's rule needs with the simulation's exact clocks.
    #[must_use]
    pub fn quarantine(self) -> Duration {
        2 * self.timeout + self.idle
    }
}

/// The simulated wall clock, which the destination checks apply-by times
/// on.
#[derive(Debug, Clone, Copy)]
struct SimWall;

impl WallClock for SimWall {
    fn now(&self) -> Duration {
        turmoil::since_epoch().unwrap_or_default()
    }
}

/// What the nodes know of the peer: how to check its descriptors, and the
/// record the audits keep.
#[derive(Clone, Debug)]
pub(crate) struct PeerSide {
    pub verifier: DescriptorVerifier,
    pub log: Arc<PeerLog>,
}

/// What the run saw of the two transports, which the audits check.
#[derive(Debug, Default)]
pub(crate) struct PeerLog {
    /// The descriptor the destination serves.
    descriptor: DescriptorKind,
    /// The `COMMIT`s the nodes sent: each write identity's key, and when
    /// it was last sent.
    commits: Mutex<BTreeMap<String, (String, Duration)>>,
    /// When each key's S3 writes were acknowledged.
    s3_writes: Mutex<BTreeMap<String, Vec<Duration>>>,
    /// `COMMIT`s the destination received.
    received: AtomicU64,
    /// S3 writes the destination acknowledged.
    acknowledged: AtomicU64,
    violation: Mutex<Option<Violation>>,
    /// The aimed blocks still to start, and when the last one started.
    aimed: Mutex<(AimedBlocks, Option<Duration>)>,
    /// Whether an aimed block is due.
    aim: AtomicBool,
}

impl PeerLog {
    fn new(descriptor: DescriptorKind, aimed: AimedBlocks) -> Self {
        Self {
            descriptor,
            aimed: Mutex::new((aimed, None)),
            ..Self::default()
        }
    }

    /// Whether a block aimed at a `COMMIT` on its way is due: the driver
    /// starts it at once.
    pub(crate) fn take_aim(&self) -> Option<Duration> {
        self.aim
            .swap(false, Ordering::Relaxed)
            .then(|| lock(&self.aimed).0.hold)
    }

    /// A node sent the `COMMIT` of `identity`, a write of `key`.
    fn commit_sent(&self, identity: String, key: String) {
        {
            let mut aimed = lock(&self.aimed);
            let now = now();
            let (blocks, last) = &mut *aimed;
            if blocks.count > 0 && last.is_none_or(|last| now >= last + blocks.every) {
                blocks.count -= 1;
                *last = Some(now);
                self.aim.store(true, Ordering::Relaxed);
            }
        }
        lock(&self.commits).insert(identity, (key, now()));
    }

    /// An S3 write of `key` was acknowledged.
    pub(crate) fn s3_write(&self, key: &str) {
        self.acknowledged.fetch_add(1, Ordering::Relaxed);
        lock(&self.s3_writes)
            .entry(key.to_owned())
            .or_default()
            .push(now());
    }

    /// The destination received the `COMMIT` of `identity`, of `key`, which
    /// may apply until `apply_by_ms` on the simulated wall clock.
    fn commit_received(&self, identity: &str, key: &str, apply_by_ms: Option<u64>) {
        self.received.fetch_add(1, Ordering::Relaxed);
        if self.descriptor != DescriptorKind::Signed {
            self.violate(Violation {
                key: key.to_owned(),
                reason: format!(
                    "the COMMIT of {identity} reached the peer, whose descriptor is {:?}: a \
                     node used QUIC with a peer it could not verify",
                    self.descriptor
                ),
                operations: Vec::new(),
            });
        }
        let Some(sent) = lock(&self.commits).get(identity).map(|(_, sent)| *sent) else {
            return;
        };
        // One that arrives after its apply-by time is never applied.
        let wall = u64::try_from(SimWall.now().as_millis()).unwrap_or(u64::MAX);
        if apply_by_ms.is_some_and(|by| wall > by) {
            return;
        }
        let received = now();
        let crossed = lock(&self.s3_writes).get(key).and_then(|acks| {
            acks.iter()
                .find(|ack| sent < **ack && **ack < received)
                .copied()
        });
        if let Some(ack) = crossed {
            self.violate(Violation {
                key: key.to_owned(),
                reason: format!(
                    "the COMMIT of {identity}, sent at {sent:?}, reached the peer at \
                     {received:?}, after an S3 write of the key acknowledged at {ack:?}: a \
                     key was flushed over S3 while its COMMIT was outstanding"
                ),
                operations: Vec::new(),
            });
        }
    }

    fn violate(&self, violation: Violation) {
        lock(&self.violation).get_or_insert(violation);
    }
}

/// The simulated time since the run began.
fn now() -> Duration {
    turmoil::sim_elapsed().unwrap_or_default()
}

/// Samples the transport each live node's peer targets use, and checks
/// that the metric and the status agree and that each change of transport
/// is counted.
#[derive(Debug, Default)]
pub(crate) struct TransportAudit {
    next: Duration,
    /// Each service's targets, by the service's place among the flushers
    /// and the bucket: the transport last seen, and the switches counted.
    seen: BTreeMap<(usize, BucketId), (Option<Transport>, u64)>,
    /// Samples of a verified peer target on S3 REST, and on QUIC.
    on_s3: u64,
    on_quic: u64,
}

impl TransportAudit {
    /// Samples the targets of `buckets` on the live services of
    /// `flushers`, if the last sample is old enough at `elapsed`.
    pub(crate) fn sample(
        &mut self,
        elapsed: Duration,
        flushers: &Flushers,
        buckets: &[BucketDocument],
    ) -> Result<(), Violation> {
        if elapsed < self.next {
            return Ok(());
        }
        self.next = elapsed + SAMPLE_INTERVAL;
        for (index, service) in live(flushers) {
            service.refresh_metrics();
            for bucket in buckets {
                let Some(status) = service.status(&bucket.bucket_id) else {
                    continue;
                };
                let Some(transport) = status.transport else {
                    continue;
                };
                let violation = |reason: String| Violation {
                    key: bucket.name.to_string(),
                    reason,
                    operations: Vec::new(),
                };
                let metric = service.metrics().transport(&status.name);
                let exported = transport.peer || transport.configured == TargetTransport::Native;
                let expected = if exported { transport.in_use } else { None };
                if metric != expected {
                    return Err(violation(format!(
                        "the transport metric says {metric:?}, the status {:?}",
                        transport.in_use
                    )));
                }
                if transport.peer {
                    match transport.in_use {
                        Some(Transport::S3) => self.on_s3 += 1,
                        Some(Transport::Quic) => self.on_quic += 1,
                        None => {}
                    }
                }
                let now = (transport.in_use, transport.switches);
                if let Some((before, switches)) =
                    self.seen.insert((index, bucket.bucket_id.clone()), now)
                {
                    let switched = before.is_some() && now.0 != before;
                    if now.1 < switches || (switched && now.1 == switches) {
                        return Err(violation(format!(
                            "the transport went from {before:?} to {:?}, and the switches \
                             from {switches} to {}",
                            now.0, now.1
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    /// `audit`, with what the samples saw.
    pub(crate) fn add_to(&self, audit: PeerAudit) -> PeerAudit {
        PeerAudit {
            on_s3: self.on_s3,
            on_quic: self.on_quic,
            switches: self.seen.values().map(|(_, switches)| switches).sum(),
            ..audit
        }
    }
}

/// The live flush services, with their place among `flushers`.
fn live(flushers: &Flushers) -> Vec<(usize, Arc<FlushService<SimStore, SimMount>>)> {
    lock(flushers)
        .iter()
        .enumerate()
        .filter_map(|(index, service)| Some((index, service.upgrade()?)))
        .collect()
}

/// Waits, for at most `limit`, until every peer target among `buckets` on
/// the live services of `flushers` uses QUIC. Returns the first that does
/// not, if one does not in time.
pub(crate) async fn back_on_quic(
    flushers: Arc<Flushers>,
    buckets: Vec<BucketDocument>,
    limit: Duration,
) -> Option<String> {
    let deadline = tokio::time::Instant::now() + limit;
    loop {
        let mut lagging = None;
        for (_, service) in live(&flushers) {
            for bucket in &buckets {
                let transport = service
                    .status(&bucket.bucket_id)
                    .and_then(|status| status.transport);
                if let Some(transport) = transport
                    && transport.in_use != Some(Transport::Quic)
                {
                    lagging.get_or_insert(format!(
                        "{} stays on {:?}: {}",
                        bucket.name, transport.in_use, transport.reason
                    ));
                }
            }
        }
        if lagging.is_none() || tokio::time::Instant::now() >= deadline {
            return lagging;
        }
        tokio::time::sleep(SAMPLE_INTERVAL).await;
    }
}

/// What the run found at the peer.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PeerAudit {
    /// `FLUSHED` records checked against the destination's answers.
    pub flushed: u64,
    /// The destination's lives.
    pub lives: u64,
    /// Objects the destination holds at the end.
    pub objects: usize,
    /// `PUT` records in the destination's log at the end.
    pub writes: usize,
    /// `COMMIT`s the destination received.
    pub commits: u64,
    /// Writes the destination acknowledged over S3 REST.
    pub s3_writes: u64,
    /// Samples of a peer target on S3 REST, after its descriptor verified.
    pub on_s3: u64,
    /// Samples of a peer target on QUIC.
    pub on_quic: u64,
    /// Changes of transport, over every target sampled.
    pub switches: u64,
}

/// What the destination holds at the end, by key, and what the run found.
pub(crate) struct Finished {
    pub objects: BTreeMap<String, String>,
    pub audit: PeerAudit,
}

/// What the destination holds now: read from its index while it runs,
/// and what it held when it last stopped while it is down.
#[derive(Default)]
struct View {
    index: Option<Arc<Index>>,
    frozen: BTreeMap<String, String>,
}

/// The destination cluster, over every life of its node.
pub(crate) struct Destination {
    node: NodeId,
    disk: SimDisk,
    document: BucketDocument,
    /// The configuration its commits apply under.
    config: skys3_config::Config,
    log: LogConfig,
    index: IndexConfig,
    checkpoints: Duration,
    limits: StagingLimits,
    view: Mutex<View>,
    /// The write identities the destination answered `committed`, or
    /// named as a key's current one.
    applied: Mutex<BTreeSet<String>>,
    violation: Mutex<Option<Violation>>,
    flushed: AtomicU64,
    lives: AtomicU64,
    /// The control store its gateway reads its bucket from.
    control: MemoryControlStore,
    /// The source cluster.
    source: ClusterId,
    /// What signs its descriptors, and what the source trusts.
    signer: DescriptorSigner,
    forger: DescriptorSigner,
    ca: skys3_net::CertificateDer<'static>,
    /// How long a stream may send nothing.
    idle: Duration,
    record: Arc<PeerLog>,
}

impl Destination {
    /// A destination on `disk` that receives from `source`, into its
    /// bucket from each of `buckets`, with the log and index settings of
    /// the nodes.
    pub(crate) fn new(
        disk: SimDisk,
        source: &ClusterId,
        buckets: &[BucketId],
        peer: Peer,
        log: LogConfig,
        index: IndexConfig,
        checkpoints: Duration,
    ) -> Result<Self, BoxError> {
        let pairs: Vec<String> = buckets
            .iter()
            .map(|bucket| format!("{{ source = \"{bucket}\", destination = \"{BUCKET}\" }}"))
            .collect();
        let text = format!(
            "[cluster]\ncluster_id = \"{CLUSTER}\"\n\
             [control_store]\netcd_endpoints = [\"https://etcd.sim.internal:2379\"]\n\
             [transport]\ntls_cert_file = \"/n.crt\"\ntls_key_file = \"/n.key\"\n\
             tls_ca_file = \"/ca.crt\"\n\
             [buckets.{BUCKET}]\nmode = \"local\"\npeer_source = \"{source}\"\n\
             [peering]\nquic_advertise = [\"{ADVERTISED}\"]\n\
             [peering.peers.{source}]\nca_file = \"/peer.crt\"\n\
             buckets = [{}]\n\
             s3_access_key_ids = [\"{ACCESS_KEY}\"]\n",
            pairs.join(", ")
        );
        let cluster = ClusterId::new(CLUSTER)?;
        let node = NodeId::new("peer-1")?;
        // The destination's own CA, and one the source does not trust that
        // forges its descriptors.
        let (pki, forgery) = (Pki::ed25519()?, Pki::ed25519()?);
        let signer = |pki: &Pki| -> Result<DescriptorSigner, BoxError> {
            let credentials = pki.node(&cluster, &node)?;
            let tls = PeerTls::new(&credentials, Arc::new(PeerTrust::new()))
                .ok_or("the destination's certificate names no node")?;
            Ok(tls.descriptor_signer())
        };
        let document = BucketDocument {
            bucket_id: BucketId::new("b-peer")?,
            name: BucketName::new(BUCKET)?,
            mode: BucketMode::Local,
            shards: ShardCount::new(2)?,
            replicas: 1,
            min_write_replicas: 1,
            clean_copies: 1,
            target: None,
            created_unix_ms: 0,
            lifecycle: None,
            proposal_id: ProposalId::new("p-peer")?,
        };
        let control = MemoryControlStore::new();
        register(&control, &cluster, &document)?;
        let (signer, forger) = (signer(&pki)?, signer(&forgery)?);
        Ok(Self {
            node,
            disk,
            document,
            config: text.parse()?,
            log,
            index,
            checkpoints,
            limits: StagingLimits {
                quota_bytes: 1 << 30,
                ttl: peer.staging_ttl,
            },
            view: Mutex::default(),
            applied: Mutex::default(),
            violation: Mutex::default(),
            flushed: AtomicU64::new(0),
            lives: AtomicU64::new(0),
            control,
            source: source.clone(),
            signer,
            forger,
            ca: pki.ca().clone(),
            idle: peer.idle,
            record: Arc::new(PeerLog::new(peer.descriptor, peer.aimed)),
        })
    }

    /// What the nodes know of the destination: they trust its CA for the
    /// bucket pairs of `buckets`.
    pub(crate) fn side(&self, buckets: &[BucketId]) -> Result<PeerSide, BoxError> {
        let mut trust = PeerTrust::new();
        let destination = BucketName::new(BUCKET)?;
        trust.add(
            ClusterId::new(CLUSTER)?,
            std::slice::from_ref(&self.ca),
            buckets.iter().map(|source| BucketPair {
                source: source.clone(),
                destination: destination.clone(),
            }),
        )?;
        Ok(PeerSide {
            verifier: DescriptorVerifier::new(Arc::new(trust)),
            log: Arc::clone(&self.record),
        })
    }

    /// Stops the destination's disk: a power loss drops what was not
    /// synced. What it held is kept for the audits while it is down.
    pub(crate) fn stop(&self, power_loss: bool) {
        let mut view = lock(&self.view);
        if let Some(index) = view.index.take() {
            match self.objects(&index) {
                Ok(objects) => view.frozen = objects,
                Err(error) => tracing::warn!(%error, "the peer's objects could not be read"),
            }
        }
        if power_loss {
            self.disk.crash();
        } else {
            self.disk.kill();
        }
    }

    /// The value the destination holds for `key` of its bucket: the ETag
    /// of its object, or `None`.
    pub(crate) fn value(&self, key: &str) -> Option<String> {
        let view = lock(&self.view);
        if let Some(index) = &view.index {
            let shard = ShardRef::for_key(&self.document, key);
            let read = index
                .read()
                .and_then(|read| read.entry(&(&shard).into(), key));
            match read {
                Ok(entry) => {
                    return entry
                        .and_then(|entry| entry.object)
                        .map(|object| object.local_etag.to_string());
                }
                Err(error) => tracing::warn!(%error, "the peer's index could not be read"),
            }
        }
        view.frozen.get(key).cloned()
    }

    /// Every object of the destination bucket in `index`, by key.
    fn objects(&self, index: &Index) -> Result<BTreeMap<String, String>, BoxError> {
        let read = index.read()?;
        let mut objects = BTreeMap::new();
        for shard in ShardRef::all(&self.document) {
            let mut query = ListQuery {
                max_items: 256,
                ..ListQuery::default()
            };
            loop {
                let page = read.list(&(&shard).into(), &query)?;
                for item in page.items {
                    if let ListItem::Object { key, object } = item {
                        query.start_after = Some(key.clone());
                        objects.insert(key, object.local_etag.to_string());
                    }
                }
                if !page.truncated {
                    break;
                }
            }
        }
        Ok(objects)
    }

    /// Checks a `FLUSHED` record a flusher decided on: the destination
    /// must have applied the identity it names.
    fn flushed(&self, record: &FlushedRecord) {
        self.flushed.fetch_add(1, Ordering::Relaxed);
        if lock(&self.applied).contains(&record.identity) {
            return;
        }
        let what = if record.delete { "delete" } else { "version" };
        lock(&self.violation).get_or_insert(Violation {
            key: format!("{}/{}", record.shard.bucket, record.key),
            reason: format!(
                "the {what} at {} is recorded flushed as {}, which the peer never applied",
                record.version, record.identity
            ),
            operations: Vec::new(),
        });
    }

    /// Calls [`Destination::flushed`] for each `FLUSHED` record a flusher
    /// on this thread decides on, until the next call.
    pub(crate) fn observe(self: &Arc<Self>) {
        let destination = Arc::clone(self);
        skys3_flush::test_hooks::observe_flushed(Some(Box::new(move |record| {
            destination.flushed(record);
        })));
    }

    /// Whether a UDP block aimed at a `COMMIT` on its way is due, and how
    /// long it holds the links.
    pub(crate) fn take_aim(&self) -> Option<Duration> {
        self.record.take_aim()
    }

    /// The first violation the audits found while the run went on.
    pub(crate) fn violation(&self) -> Option<Violation> {
        lock(&self.violation)
            .take()
            .or_else(|| lock(&self.record.violation).take())
    }

    /// Recovers the destination from its disk, outside the simulation,
    /// and returns what it holds, by key, with what the run found: the
    /// `PUT` records in its log, each of a write identity of its own, or
    /// the violation if one repeats.
    pub(crate) fn finish(&self) -> Result<Result<Finished, Violation>, BoxError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()?;
        let storage = runtime.block_on(self.recover())?;
        let objects = self.objects(&storage.index)?;
        let objects = runtime.block_on(self.written(&storage, objects))?;
        let mut writes: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (_, log) in storage.shards.set().logs() {
            for mut scanner in log.scan_all()? {
                while let Some(scanned) = runtime.block_on(scanner.next())? {
                    let record = scanned.decode()?;
                    if let RecordBody::Put(put) = record.body
                        && let Some(identity) = put.metadata.get(IDENTITY_METADATA)
                    {
                        writes
                            .entry(identity.clone())
                            .or_default()
                            .push(format!("PUT {} at {}", put.key, record.position));
                    }
                }
            }
        }
        if let Some((identity, puts)) = writes.iter().find(|(_, puts)| puts.len() > 1) {
            return Ok(Err(Violation {
                key: identity.clone(),
                reason: format!("the peer applied {identity} {} times", puts.len()),
                operations: puts.clone(),
            }));
        }
        let audit = PeerAudit {
            flushed: self.flushed.load(Ordering::Relaxed),
            lives: self.lives.load(Ordering::Relaxed),
            objects: objects.len(),
            writes: writes.len(),
            commits: self.record.received.load(Ordering::Relaxed),
            s3_writes: self.record.acknowledged.load(Ordering::Relaxed),
            ..PeerAudit::default()
        };
        Ok(Ok(Finished { objects, audit }))
    }

    /// What the writes of `objects`, by key and ETag, wrote: as for the
    /// remote store, the MD5 of the bytes of a streamed single `PUT` that
    /// the flushers sent over S3 REST as a multipart upload, read back
    /// through a gateway over `storage`.
    async fn written(
        &self,
        storage: &skys3::storage::Storage<SimMount>,
        objects: BTreeMap<String, String>,
    ) -> Result<BTreeMap<String, String>, BoxError> {
        if !objects.values().any(|etag| etag.contains('-')) {
            return Ok(objects);
        }
        for shard in ShardRef::all(&self.document) {
            storage.shards.open(&shard, &self.document).await?;
        }
        let mut config = GatewayConfig::new(&self.config);
        config.inline_max_bytes = 512;
        config.extent_bytes = 512;
        let gateway = Gateway::new(
            config,
            self.control.clone(),
            storage.shards.clone(),
            IdSource::seeded(u64::MAX),
            PeerKeys,
        )
        .await?;
        let mut written = BTreeMap::new();
        for (key, etag) in objects {
            let value = if etag.contains('-') {
                let request = Request::get(format!("/{BUCKET}/{key}")).body(Body::empty())?;
                let response = gateway.handle(request).await;
                let body = http_body_util::BodyExt::collect(response.into_body())
                    .await
                    .map_err(|error| format!("reading {key} back: {error}"))?
                    .to_bytes();
                crate::cluster::written(&body, &skys3_types::ETag::new(etag)?)
            } else {
                etag
            };
            written.insert(key, value);
        }
        Ok(written)
    }

    /// Recovers the destination's storage from its disk.
    async fn recover(&self) -> Result<skys3::storage::Storage<skys3_io::SimMount>, BoxError> {
        let mount = self.disk.mount();
        let index = Index::open_sim(&mount, INDEX_FILE, &self.index)?;
        let clock = NodeClock {
            drift: Drift::NONE,
            start: MonoTime::from_nanos(0),
        };
        Ok(skys3::storage::recover(
            vec![(Label::new("disk-0")?, mount)],
            self.log.clone(),
            Arc::new(clock.start()),
            Arc::new(index),
            BlockingPool::inline("peer"),
            self.node.clone(),
        )
        .await?)
    }
}

/// Runs one life of the destination until it crashes.
pub(crate) async fn run(destination: Arc<Destination>) -> Result<(), BoxError> {
    destination.lives.fetch_add(1, Ordering::Relaxed);
    let storage = destination.recover().await?;
    let document = destination.document.clone();
    for shard in ShardRef::all(&document) {
        storage.shards.open(&shard, &document).await?;
    }
    let (checkpointer, interval) = (Arc::clone(&storage.checkpointer), destination.checkpoints);
    tokio::spawn(async move { checkpointer.run(interval).await });
    *lock(&destination.view) = View {
        index: Some(Arc::clone(&storage.index)),
        frozen: BTreeMap::new(),
    };
    let lookup: BucketLookup =
        Arc::new(move |name: &BucketName| (*name == document.name).then(|| document.clone()));
    let mut gateway = GatewayConfig::new(&destination.config);
    gateway.inline_max_bytes = 512;
    gateway.extent_bytes = 512;
    // The flushers' parts are as small as the nodes' gateways take.
    gateway.min_part_bytes = 1;
    gateway.descriptors = Some(Arc::new(Descriptors(Arc::clone(&destination))));
    let service = StagingService::new(
        Arc::new(Staging::new(destination.limits)),
        PeerExtents::new(storage.shards.clone(), Arc::clone(&lookup)),
    )
    .with_commits(
        PeerCommits::new(
            storage.shards.clone().with_wall_clock(Arc::new(SimWall)),
            lookup,
            &gateway,
        )
        .with_wall_clock(Arc::new(SimWall)),
    );
    // The gateway, which serves the descriptor and takes S3 flushes.
    let life = destination.lives.load(Ordering::Relaxed);
    let s3 = Gateway::new(
        gateway,
        destination.control.clone(),
        storage.shards.clone(),
        IdSource::seeded(life),
        PeerKeys,
    )
    .await?;
    let s3_listener = s3::bind().await?;
    tokio::spawn(async move {
        if let Err(error) = s3::serve(s3_listener, s3).await {
            tracing::debug!(%error, "the peer's gateway stopped");
        }
    });
    let listener = TcpListener::bind(("0.0.0.0", PORT)).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let (read, write) = stream.into_split();
        let inbound = TcpInbound {
            read,
            idle: destination.idle,
            log: Arc::clone(&destination.record),
            sender: TcpOutbound {
                write: Arc::new(tokio::sync::Mutex::new(Some(write))),
                destination: Arc::clone(&destination),
            },
        };
        let service = service.clone();
        tokio::spawn(async move {
            if let Err(error) = service.serve(inbound).await {
                tracing::debug!(%error, "a peer stream failed");
            }
        });
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Reads the next frame from `read`, or `None` at the end of the stream.
async fn read_frame<R: AsyncRead + Unpin>(read: &mut R) -> io::Result<Option<Message>> {
    let mut prefix = [0; PREFIX_LEN];
    let mut filled = 0;
    while filled < PREFIX_LEN {
        match read.read(&mut prefix[filled..]).await? {
            0 if filled == 0 => return Ok(None),
            0 => return Err(io::ErrorKind::UnexpectedEof.into()),
            n => filled += n,
        }
    }
    let invalid = |error| io::Error::new(io::ErrorKind::InvalidData, error);
    let (header_len, payload_len) = parse_prefix(&prefix).map_err(invalid)?;
    let mut header = vec![0; header_len];
    read.read_exact(&mut header).await?;
    let mut payload = vec![0; payload_len];
    read.read_exact(&mut payload).await?;
    Message::decode_parts(&header, Bytes::from(payload))
        .map(Some)
        .map_err(invalid)
}

/// Writes `message` as one frame to `write`.
async fn write_frame(write: &mut OwnedWriteHalf, message: &Message) -> io::Result<()> {
    let frame = message
        .encode()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    write.write_all(&frame).await
}

/// A node's link to the peer at `host:port`: a TCP connection per
/// stream, whose `COMMIT`s `log` records.
struct TcpLink {
    host: String,
    port: u16,
    log: Arc<PeerLog>,
}

impl PeerLink for TcpLink {
    fn open(&self) -> BoxFuture<'_, Result<PeerStream, LinkError>> {
        Box::pin(async {
            let stream = TcpStream::connect((self.host.as_str(), self.port))
                .await
                .map_err(link)?;
            let (read, write) = stream.into_split();
            Ok(PeerStream {
                sender: Box::new(TcpSend(Some(write), Arc::clone(&self.log))),
                receiver: Box::new(TcpReceive(read)),
                batches: true,
            })
        })
    }
}

fn link(error: io::Error) -> LinkError {
    LinkError::new(error.to_string())
}

struct TcpSend(Option<OwnedWriteHalf>, Arc<PeerLog>);

impl PeerSend for TcpSend {
    fn send<'a>(&'a mut self, message: &'a Message) -> BoxFuture<'a, Result<(), LinkError>> {
        Box::pin(async move {
            if let Message::Commit(commit) = message {
                self.1
                    .commit_sent(commit.identity.to_string(), commit.key.clone());
            }
            let write = self
                .0
                .as_mut()
                .ok_or_else(|| LinkError::new("the stream is finished"))?;
            write_frame(write, message).await.map_err(link)
        })
    }

    fn finish(&mut self) -> Result<(), LinkError> {
        // Dropping the write half ends the stream.
        self.0.take();
        Ok(())
    }
}

struct TcpReceive(OwnedReadHalf);

impl PeerReceive for TcpReceive {
    fn recv(&mut self) -> BoxFuture<'_, Result<Option<Message>, LinkError>> {
        Box::pin(async { read_frame(&mut self.0).await.map_err(link) })
    }
}

/// A stream the destination accepted, which ends once it sent nothing for
/// `idle`.
struct TcpInbound {
    read: OwnedReadHalf,
    idle: Duration,
    log: Arc<PeerLog>,
    sender: TcpOutbound,
}

impl Inbound for TcpInbound {
    type Sender = TcpOutbound;

    async fn recv(&mut self) -> Result<Option<Message>, StreamError> {
        let message = tokio::time::timeout(self.idle, read_frame(&mut self.read))
            .await
            .map_err(|_| StreamError::Transport("the stream idled out".to_owned()))?
            .map_err(|error| StreamError::Transport(error.to_string()))?;
        if let Some(Message::Commit(commit)) = &message {
            self.log.commit_received(
                &commit.identity.to_string(),
                &commit.key,
                commit.apply_by_ms,
            );
        }
        Ok(message)
    }

    fn sender(&self) -> TcpOutbound {
        self.sender.clone()
    }
}

/// The destination's side of a stream it answers on, which notes what it
/// answers for the audits.
#[derive(Clone)]
struct TcpOutbound {
    write: Arc<tokio::sync::Mutex<Option<OwnedWriteHalf>>>,
    destination: Arc<Destination>,
}

impl Outbound for TcpOutbound {
    async fn send(&self, message: &Message) -> Result<(), StreamError> {
        if let Message::Applied(applied) = message {
            let mut known = lock(&self.destination.applied);
            match &applied.outcome {
                Outcome::Committed { .. } => {
                    known.insert(applied.identity.to_string());
                }
                Outcome::PreconditionFailed {
                    current: Some(current),
                } => {
                    known.insert(current.to_string());
                }
                _ => {}
            }
        }
        let mut write = self.write.lock().await;
        let write = write
            .as_mut()
            .ok_or_else(|| StreamError::Transport("the stream is finished".to_owned()))?;
        write_frame(write, message)
            .await
            .map_err(|error| StreamError::Transport(error.to_string()))
    }

    async fn finish(&self) -> Result<(), StreamError> {
        self.write.lock().await.take();
        Ok(())
    }
}

/// Registers the destination's bucket in its control store, once, outside
/// the simulation.
fn register(
    control: &MemoryControlStore,
    cluster: &ClusterId,
    document: &BucketDocument,
) -> Result<(), BoxError> {
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    runtime.block_on(async {
        let retry = RetryPolicy::default();
        let mut ids = ProposalIds::seeded(1);
        bootstrap(control, cluster, ids.next_id(), &retry).await?;
        let key = TypedKey::bucket(&document.name);
        let outcome = propose_document(control, &key, Expected::Absent, document, &retry).await?;
        if !matches!(outcome, ProposalOutcome::Accepted(_)) {
            return Err("the peer's bucket exists in a fresh control store".into());
        }
        bump_generation(control, cluster, &mut ids, &retry).await?;
        Ok::<_, BoxError>(())
    })?;
    Ok(())
}

/// The destination's descriptors, as its gateway serves them.
struct Descriptors(Arc<Destination>);

impl std::fmt::Debug for Descriptors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Descriptors")
    }
}

impl PeerDescriptors for Descriptors {
    fn descriptor(&self, bucket: &BucketName, source: &ClusterId) -> Option<Bytes> {
        let destination = &self.0;
        if bucket.as_str() != BUCKET || *source != destination.source {
            return None;
        }
        let now = turmoil::since_epoch().unwrap_or_default();
        let issued = match destination.record.descriptor {
            DescriptorKind::Expired => now.saturating_sub(EXPIRED_BY),
            DescriptorKind::Signed | DescriptorKind::Forged => now,
        };
        let descriptor = PeerDescriptor::new(
            ClusterId::new(CLUSTER).ok()?,
            bucket.clone(),
            source.clone(),
            vec![ADVERTISED.to_owned()],
            issued,
        );
        let signer = match destination.record.descriptor {
            DescriptorKind::Forged => &destination.forger,
            DescriptorKind::Signed | DescriptorKind::Expired => &destination.signer,
        };
        signer.sign(&descriptor).ok()
    }
}

/// Takes each request as signed by the access key its header names, with
/// every permission: the source's flushers name [`ACCESS_KEY`], which the
/// destination knows as its peer's.
#[derive(Debug, Clone, Copy)]
struct PeerKeys;

impl Authenticator for PeerKeys {
    async fn authenticate(&self, mut request: Request<Body>) -> Result<Request<Body>, S3Error> {
        let key = request
            .headers()
            .get(ACCESS_KEY_HEADER)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("AKIASIMCLIENT")
            .to_owned();
        request.extensions_mut().insert(Authenticated {
            principal: Principal::new(key.clone(), Permissions::allow_all()),
            access_key_id: key,
            method: AuthMethod::Header,
        });
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_are_read_back_whole_or_not_at_all() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let message = Message::Abort(skys3_peer::Abort {
            identity: "src/b-1/0/1.2".parse().unwrap(),
            reason: skys3_peer::AbortReason::Cancelled,
            detail: "gone".to_owned(),
        });
        let frame = message.encode().unwrap();
        runtime.block_on(async {
            let mut whole = &frame[..];
            assert_eq!(read_frame(&mut whole).await.unwrap(), Some(message));
            assert_eq!(read_frame(&mut whole).await.unwrap(), None);
            let mut cut = &frame[..3];
            assert!(read_frame(&mut cut).await.is_err());
            let mut garbage = &[0xff; 16][..];
            assert!(read_frame(&mut garbage).await.is_err());
        });
    }
}
