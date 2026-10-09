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
//! the native service over real QUIC on the simulated network
//! ([`peer_wire`](crate::peer_wire)): Quinn's endpoints, the node binary's
//! handshake and connection pool, and the destination's stream service,
//! with mutual TLS against each side's trust bundle. Partitions, holds,
//! message loss, and restarts of the peer ([`Fault::PeerRestart`]) lose
//! datagrams, stall connections until their idle timeout
//! ([`Peer::idle`]), and cut streams in the middle; the destination also
//! loses protocol messages and drops connections itself
//! ([`ProtocolFaults`]). The nodes reach the gateway through a relay host
//! (see [`peer_s3`](crate::peer_s3)), which those faults do not cut: they
//! block the native transport as a firewall that blocks UDP would.
//!
//! The nodes' discovery reads the descriptor through the gateway, checks
//! it against the destination's CA, and tries a QUIC handshake with the
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
//! - **The protocol itself** (plan M6-08), on every stream the
//!   destination serves ([`peer_wire`](crate::peer_wire)): no `DATA`
//!   resends a byte its stream's `RESUME` reported durable
//!   (`PeerBug::ResendDurable` breaks this); every `APPLIED` for the
//!   identity the key's current version carries says `committed` with its
//!   ETag, however many copies of the `COMMIT` arrive and from whichever
//!   node; and a `COMMIT` whose stream covered its whole object is never
//!   answered `incomplete`. Once the run is over, the staging that is
//!   left expires and gives back its whole quota charge.
//!
//! [`Fault::PeerRestart`]: crate::Fault::PeerRestart

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use http::Request;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use s3s::{Body, S3Error};
use skys3_config::{BucketPair, PeeringConfig, TargetTransport};
use skys3_control::{
    Expected, MemoryControlStore, ProposalIds, ProposalOutcome, RetryPolicy, TypedKey, bootstrap,
    bump_generation, propose_document,
};
use skys3_flush::{FlushService, FlushedRecord, PeerBug, Transport};
use skys3_gateway::sigv4::{AuthMethod, Authenticated};
use skys3_gateway::{
    Authenticator, BucketLookup, Gateway, GatewayConfig, IdSource, PeerCommits, PeerDescriptors,
    PeerExtents, Permissions, Principal, ShardRef, Shards,
};
use skys3_index::{Index, IndexConfig, ListItem, ListQuery};
use skys3_io::{BlockingPool, Drift, MonoTime, SimDisk, SimMount};
use skys3_log::record::IDENTITY_METADATA;
use skys3_log::{LogConfig, RecordBody};
use skys3_peer::{
    Applied, DescriptorSigner, DescriptorVerifier, EndpointSettings, Outcome, PeerDescriptor,
    PeerTls, PeerTrust, Staging, StagingLimits, StagingService,
};
use skys3_sim::NodeClock;
use skys3_sim::check::Violation;
use skys3_types::WriteIdentity;
use skys3_types::{
    BucketDocument, BucketId, BucketMode, BucketName, ClusterId, Label, NodeId, ProposalId,
    ShardCount,
};

use crate::backup::Flushers;
use crate::node::{BoxError, INDEX_FILE};
use crate::peer_s3::{ACCESS_KEY, ACCESS_KEY_HEADER, SimStore};
pub use crate::peer_wire::ProtocolFaults;
use crate::pki::Pki;
use crate::{peer_wire, s3};

/// The destination's host.
pub(crate) const HOST: &str = "peer";
/// The port its peer endpoint listens on.
pub(crate) const PORT: u16 = 7600;
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
    /// The QUIC idle timeout of every connection, at both ends: a
    /// connection that hears nothing for this long is lost, with its
    /// streams. Quiet connections send keep-alives six times as often.
    pub idle: Duration,
    /// The descriptor the destination serves.
    pub descriptor: DescriptorKind,
    /// Faults aimed at `COMMIT`s: the first `count` times a node sends
    /// a `COMMIT`, at least `every` apart, links are held for `hold`,
    /// which catches the `COMMIT` on its way.
    pub aimed: AimedBlocks,
    /// Messages the destination loses and connections it drops.
    pub faults: ProtocolFaults,
    /// A seeded bug of the flushers' native transport.
    pub bug: PeerBug,
}

/// Faults aimed at `COMMIT`s on their way ([`Peer::aimed`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AimedBlocks {
    /// How many.
    pub count: usize,
    /// How long each holds the links.
    pub hold: Duration,
    /// The least time between the starts of two.
    pub every: Duration,
    /// Which links.
    pub scope: AimScope,
    /// Whether the links are held once the `COMMIT` reaches the
    /// destination, which takes as long as the hold to apply it, as a slow
    /// one may: it is still outstanding when the links come back, though
    /// its sender may have closed its connection meanwhile. Without, the
    /// sender holds its link before the `COMMIT` leaves.
    pub late: bool,
}

/// Which links a fault aimed at a `COMMIT` holds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AimScope {
    /// UDP blocked between every node and the peer.
    #[default]
    Udp,
    /// The link between the node that sent the `COMMIT` and the peer: its
    /// `COMMIT` arrives late, while the other nodes' streams go on.
    Sender,
    /// The sender deposed: it is cut off from the other nodes for
    /// `isolated`, long enough for a member to take its shards over and
    /// flush the same write again, while its link to the peer is held, so
    /// that its own `COMMIT` arrives too, after its successor's or before.
    Deposed {
        /// How long the sender is cut off from the other nodes.
        isolated: Duration,
    },
}

/// A fault aimed at a `COMMIT` that a node sent: the driver starts it at
/// once.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Aim {
    /// The node that sent the `COMMIT`, by position.
    pub node: usize,
    /// How long the links are held.
    pub hold: Duration,
    /// Which links.
    pub scope: AimScope,
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
            faults: ProtocolFaults::default(),
            bug: PeerBug::None,
        }
    }
}

impl Peer {
    /// How long a node's flushers wait before they flush a shard over S3
    /// REST to the peer: as the node binary's, twice the timeout of a
    /// stream and the destination's idle timeout.
    #[must_use]
    pub fn quarantine(self) -> Duration {
        2 * self.timeout + self.idle
    }

    /// The settings of every peer endpoint, the nodes' and the
    /// destination's.
    pub(crate) fn settings(self) -> EndpointSettings {
        EndpointSettings {
            connect_timeout: self.connect_timeout,
            idle_timeout: self.idle,
            ..EndpointSettings::from_config(&PeeringConfig::default())
        }
    }
}

/// What the nodes know of the peer: how to check its descriptors, each
/// node's peer certificate, by position, the settings of their endpoints,
/// and the record the audits keep.
#[derive(Clone, Debug)]
pub(crate) struct PeerSide {
    pub verifier: DescriptorVerifier,
    pub nodes: Vec<PeerTls>,
    pub settings: EndpointSettings,
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
    /// The aimed faults still to start, and when the last one started.
    aimed: Mutex<(AimedBlocks, Option<Duration>)>,
    /// The aimed faults due.
    aims: Mutex<Vec<Aim>>,
    /// The aimed `COMMIT`s the destination is to apply late, and the
    /// faults to start once they arrive.
    late: Mutex<BTreeMap<String, Aim>>,
}

impl PeerLog {
    fn new(descriptor: DescriptorKind, aimed: AimedBlocks) -> Self {
        Self {
            descriptor,
            aimed: Mutex::new((aimed, None)),
            ..Self::default()
        }
    }

    /// The next fault aimed at a `COMMIT` on its way, if one is due: the
    /// driver starts it at once.
    pub(crate) fn take_aim(&self) -> Option<Aim> {
        let mut aims = lock(&self.aims);
        (!aims.is_empty()).then(|| aims.remove(0))
    }

    /// The node at `node` sent the `COMMIT` of `identity`, a write of
    /// `key`. Returns whether its sender is to hold its link to the peer at
    /// once, before the `COMMIT` leaves: a fault is aimed at it, which the
    /// driver starts at its next step. A fault aimed at a `COMMIT` to apply
    /// late ([`AimedBlocks::late`]) starts once the `COMMIT` arrives.
    pub(crate) fn commit_sent(&self, identity: String, key: String, node: usize) -> bool {
        let mut aim = false;
        {
            let mut aimed = lock(&self.aimed);
            let now = now();
            let (blocks, last) = &mut *aimed;
            if blocks.count > 0 && last.is_none_or(|last| now >= last + blocks.every) {
                blocks.count -= 1;
                *last = Some(now);
                let fault = Aim {
                    node,
                    hold: blocks.hold,
                    scope: blocks.scope,
                };
                if blocks.late {
                    lock(&self.late).insert(identity.clone(), fault);
                } else {
                    lock(&self.aims).push(fault);
                    aim = true;
                }
            }
        }
        lock(&self.commits).insert(identity, (key, now()));
        aim
    }

    /// How late the destination applies the `COMMIT` of `identity`, which
    /// just arrived, if a fault aimed at it is to apply it late: the fault
    /// starts now. Once.
    pub(crate) fn take_late(&self, identity: &str) -> Option<Duration> {
        let fault = lock(&self.late).remove(identity)?;
        lock(&self.aims).push(fault);
        Some(fault.hold)
    }

    /// An S3 write of `key` was acknowledged.
    pub(crate) fn s3_write(&self, key: &str) {
        self.acknowledged.fetch_add(1, Ordering::Relaxed);
        lock(&self.s3_writes)
            .entry(key.to_owned())
            .or_default()
            .push(now());
    }

    /// The destination received the `COMMIT` of `identity`, of `key`.
    fn commit_received(&self, identity: &str, key: &str) {
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
    /// Bytes of `DATA` frames the destination received.
    pub data_bytes: u64,
    /// `RESUME`s that reported durable bytes, and those bytes: what the
    /// sources did not send again after a reconnect.
    pub resumes: u64,
    /// Bytes the `RESUME`s reported durable.
    pub resumed_bytes: u64,
    /// Messages the destination lost ([`ProtocolFaults`]).
    pub lost: u64,
    /// Connections the destination dropped ([`ProtocolFaults`]).
    pub dropped: u64,
    /// `COMMIT`s answered `incomplete`.
    pub incomplete: u64,
    /// `COMMIT`s that arrived late ([`ProtocolFaults`]).
    pub late: u64,
    /// `ABORT`s that told a source its staging had expired.
    pub expired: u64,
    /// Write identities whose `COMMIT` arrived more than once.
    pub replayed: u64,
    /// Write identities whose `COMMIT` arrived from more than one node: a
    /// deposed primary's and its successor's.
    pub from_two_nodes: u64,
}

/// What the destination counts of the protocol, for [`PeerAudit`].
#[derive(Debug, Default)]
pub(crate) struct ProtocolCounts {
    data_bytes: AtomicU64,
    resumes: AtomicU64,
    resumed_bytes: AtomicU64,
    lost: AtomicU64,
    dropped: AtomicU64,
    incomplete: AtomicU64,
    late: AtomicU64,
    expired: AtomicU64,
}

impl ProtocolCounts {
    pub(crate) fn data(&self, bytes: u64) {
        self.data_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn resumed(&self, bytes: u64) {
        if bytes > 0 {
            self.resumes.fetch_add(1, Ordering::Relaxed);
            self.resumed_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    pub(crate) fn lost(&self) {
        self.lost.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn dropped(&self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn incomplete(&self) {
        self.incomplete.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn late(&self) {
        self.late.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn expired(&self) {
        self.expired.fetch_add(1, Ordering::Relaxed);
    }

    /// `audit`, with the counts.
    fn add_to(&self, audit: PeerAudit) -> PeerAudit {
        let load = |count: &AtomicU64| count.load(Ordering::Relaxed);
        PeerAudit {
            data_bytes: load(&self.data_bytes),
            resumes: load(&self.resumes),
            resumed_bytes: load(&self.resumed_bytes),
            lost: load(&self.lost),
            dropped: load(&self.dropped),
            incomplete: load(&self.incomplete),
            late: load(&self.late),
            expired: load(&self.expired),
            ..audit
        }
    }
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
    record: Arc<PeerLog>,
    /// Its TLS identity on its peer endpoint, which trusts the source's
    /// peer CA, and the settings of every peer endpoint.
    tls: PeerTls,
    settings: EndpointSettings,
    /// The nodes' peer certificates, by position.
    nodes: Vec<PeerTls>,
    /// What its endpoints draw from, mixed with the life.
    seed: u64,
    /// The faults it injects, and what it draws them from.
    faults: ProtocolFaults,
    rng: Mutex<SmallRng>,
    counts: ProtocolCounts,
    /// How many times each write identity's `COMMIT` arrived, and from
    /// which nodes.
    arrivals: Mutex<BTreeMap<String, (u64, BTreeSet<String>)>>,
    /// The identities whose staging a source aborted.
    aborted: Mutex<BTreeSet<WriteIdentity>>,
    /// The staging of its latest life.
    staging: Mutex<Option<Arc<Staging>>>,
}

impl Destination {
    /// A destination on `disk` that receives from `source`, whose nodes
    /// are `nodes`, into its bucket from each of `buckets`, with the log
    /// and index settings of the nodes, and whose draws come from `seed`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        disk: SimDisk,
        source: &ClusterId,
        nodes: &[NodeId],
        buckets: &[BucketId],
        peer: Peer,
        log: LogConfig,
        index: IndexConfig,
        checkpoints: Duration,
        seed: u64,
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
        // The destination's own CA, one the source does not trust that
        // forges its descriptors, and the source's peer CA. Ed25519 keys
        // give every handshake the same size in every run.
        let (pki, forgery, sources) = (Pki::ed25519()?, Pki::ed25519()?, Pki::ed25519()?);
        let destination = BucketName::new(BUCKET)?;
        let pairs = || {
            buckets.iter().map(|bucket| BucketPair {
                source: bucket.clone(),
                destination: destination.clone(),
            })
        };
        let mut trust = PeerTrust::new();
        trust.add(source.clone(), std::slice::from_ref(sources.ca()), pairs())?;
        let tls = PeerTls::new(&pki.node(&cluster, &node)?, Arc::new(trust))
            .ok_or("the destination's certificate names no node")?;
        let mut trust = PeerTrust::new();
        trust.add(cluster.clone(), std::slice::from_ref(pki.ca()), pairs())?;
        let trust = Arc::new(trust);
        let nodes = nodes
            .iter()
            .map(|id| {
                PeerTls::new(&sources.node(source, id)?, Arc::clone(&trust))
                    .ok_or_else(|| BoxError::from("a node's certificate names no node"))
            })
            .collect::<Result<Vec<_>, BoxError>>()?;
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
            record: Arc::new(PeerLog::new(peer.descriptor, peer.aimed)),
            tls,
            settings: peer.settings(),
            nodes,
            seed,
            faults: peer.faults,
            rng: Mutex::new(SmallRng::seed_from_u64(seed)),
            counts: ProtocolCounts::default(),
            arrivals: Mutex::default(),
            aborted: Mutex::default(),
            staging: Mutex::default(),
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
            nodes: self.nodes.clone(),
            settings: self.settings.clone(),
            log: Arc::clone(&self.record),
        })
    }

    /// Its TLS identity on its peer endpoint.
    pub(crate) fn tls(&self) -> &PeerTls {
        &self.tls
    }

    /// The settings of its peer endpoint.
    pub(crate) fn settings(&self) -> &EndpointSettings {
        &self.settings
    }

    /// What its endpoints draw from.
    pub(crate) fn seed(&self) -> u64 {
        self.seed
    }

    /// How late it applies the `COMMIT` of `identity`, if at all.
    pub(crate) fn late(&self, identity: &WriteIdentity) -> Option<Duration> {
        let aimed = self.record.take_late(&identity.to_string());
        aimed.or_else(|| {
            self.draw(self.faults.late_per_mille).then(|| {
                self.counts.late();
                self.faults.late_by
            })
        })
    }

    /// The faults it injects into the protocol.
    pub(crate) fn faults(&self) -> ProtocolFaults {
        self.faults
    }

    /// Whether a fault of `per_mille` strikes now. Nothing is drawn for a
    /// fault that never strikes, so runs without one draw as before.
    pub(crate) fn draw(&self, per_mille: u16) -> bool {
        per_mille > 0 && lock(&self.rng).random_range(0..1000) < per_mille
    }

    /// What it counts of the protocol.
    pub(crate) fn counts(&self) -> &ProtocolCounts {
        &self.counts
    }

    /// Records the first violation the protocol's audits find.
    pub(crate) fn violate(&self, key: String, reason: String) {
        lock(&self.violation).get_or_insert(Violation {
            key,
            reason,
            operations: Vec::new(),
        });
    }

    /// The `COMMIT` of `identity`, a write of `key`, arrived from `node`,
    /// alone or in a batch.
    pub(crate) fn commit_arrived(&self, identity: &WriteIdentity, key: &str, node: &str) {
        let identity = identity.to_string();
        self.record.commit_received(&identity, key);
        let mut arrivals = lock(&self.arrivals);
        let (count, nodes) = arrivals.entry(identity).or_default();
        *count += 1;
        nodes.insert(node.to_owned());
    }

    /// Whether the destination holds staging of `identity` now.
    pub(crate) fn is_staged(&self, identity: &WriteIdentity) -> bool {
        lock(&self.staging)
            .as_ref()
            .is_some_and(|staging| staging.staged(identity).is_some())
    }

    /// A source aborted the staging of `identity`.
    pub(crate) fn aborted(&self, identity: &WriteIdentity) {
        lock(&self.aborted).insert(identity.clone());
    }

    /// Whether a source ever aborted the staging of `identity`.
    pub(crate) fn was_aborted(&self, identity: &WriteIdentity) -> bool {
        lock(&self.aborted).contains(identity)
    }

    /// The destination answers `applied`, the result of a `COMMIT` of
    /// `key`, if the tap knows its key: what it answered `committed`, or
    /// named as the key's current identity, is applied; and an identity
    /// the key's current version carries is answered `committed` with
    /// that version's ETag, as often as its `COMMIT` arrives.
    pub(crate) fn answered(&self, applied: &Applied, key: Option<&str>) {
        {
            let mut known = lock(&self.applied);
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
        let identity = applied.identity.to_string();
        let Some((key, (Some(carried), etag))) =
            key.and_then(|key| Some((key, self.current(key)?)))
        else {
            return;
        };
        if carried != identity {
            return;
        }
        let consistent = matches!(
            &applied.outcome,
            Outcome::Committed { etag: Some(answered) } if answered.as_str() == etag
        );
        if !consistent {
            self.violate(
                key.to_owned(),
                format!(
                    "the peer's current version of {key} carries {identity}, with ETag {etag},                      yet a COMMIT of it was answered {:?}",
                    applied.outcome
                ),
            );
        }
    }

    /// The write identity the current version of `key` carries, if any,
    /// and its ETag; `None` if it has none, or while the destination is
    /// down.
    fn current(&self, key: &str) -> Option<(Option<String>, String)> {
        let view = lock(&self.view);
        let index = view.index.as_ref()?;
        let shard = ShardRef::for_key(&self.document, key);
        let entry = index
            .read()
            .and_then(|read| read.entry(&(&shard).into(), key))
            .ok()??;
        let object = entry.object?;
        Some((
            object.metadata.get(IDENTITY_METADATA).cloned(),
            object.local_etag.to_string(),
        ))
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

    /// The next fault aimed at a `COMMIT` on its way, if one is due.
    pub(crate) fn take_aim(&self) -> Option<Aim> {
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
        if let Some(violation) = self.staging_left() {
            return Ok(Err(violation));
        }
        let (replayed, from_two_nodes) =
            lock(&self.arrivals)
                .values()
                .fold((0, 0), |(replayed, two), (times, nodes)| {
                    (
                        replayed + u64::from(*times > 1),
                        two + u64::from(nodes.len() > 1),
                    )
                });
        let audit = PeerAudit {
            flushed: self.flushed.load(Ordering::Relaxed),
            lives: self.lives.load(Ordering::Relaxed),
            objects: objects.len(),
            writes: writes.len(),
            commits: self.record.received.load(Ordering::Relaxed),
            s3_writes: self.record.acknowledged.load(Ordering::Relaxed),
            replayed,
            from_two_nodes,
            ..PeerAudit::default()
        };
        Ok(Ok(Finished {
            objects,
            audit: self.counts.add_to(audit),
        }))
    }

    /// Expires every staging of the destination's last life, as once its
    /// TTL passed with nothing more of it, abandoned bodies included: the
    /// quota charged to the source must come back whole.
    fn staging_left(&self) -> Option<Violation> {
        let staging = lock(&self.staging).clone()?;
        let later = tokio::time::Instant::now() + Duration::from_secs(1 << 30);
        staging.expire(later);
        let used = staging.used(&self.source);
        (used != 0 || !staging.is_empty()).then(|| Violation {
            key: self.source.to_string(),
            reason: format!(
                "once every staging expired, {} remain and {used} bytes of the source's quota \
                 are still charged",
                staging.len()
            ),
            operations: Vec::new(),
        })
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
    let staging = Arc::new(Staging::new(destination.limits));
    *lock(&destination.staging) = Some(Arc::clone(&staging));
    // Staging expires as streams use it. A destination that serves other
    // sources too sweeps it at least once a TTL; this one sweeps on a
    // timer, so that staging expires whether or not the nodes stream.
    let (sweeping, ttl) = (Arc::clone(&staging), destination.limits.ttl);
    tokio::spawn(async move {
        let mut ticks = tokio::time::interval(ttl);
        loop {
            ticks.tick().await;
            sweeping.expire(tokio::time::Instant::now());
        }
    });
    let service = StagingService::new(
        staging,
        PeerExtents::new(storage.shards.clone(), Arc::clone(&lookup)),
    )
    .with_commits(PeerCommits::new(storage.shards.clone(), lookup, &gateway));
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
    peer_wire::serve(destination, service, life).await
}

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
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
