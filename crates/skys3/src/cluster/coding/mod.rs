//! Erasure coding on a node (design §8.2–§8.7; plans M8-07a, M8-07b): with
//! `[ec] enabled = true` and `[cluster] multi_node = true`, the node runs
//! what the coding simulation runs on each of its nodes, through the same
//! library code (`skys3_ec`).
//!
//! - **Fragment stores.** One [`FragmentStore`] per disk of `[node]
//!   disks`, in the disk's own directory beside its log segments (design
//!   §10.1: the log ignores `frag-<id>.seg` files, and the store every
//!   other file). A disk's number in its fragment IDs is its position
//!   among the node's disks in label order, as for shard placement
//!   (§10.2), so it never changes for a data directory.
//! - **The fragment server.** The node's transport hands it every
//!   connection whose first frame is a fragment write, read, hold, or
//!   release, and the orphan queries to the judges of the replicas it
//!   holds ([`CodingServices`]). Mutual TLS names the sender, and only node
//!   certificates may send these kinds (design §12).
//! - **Per replica of a `local` bucket's shard.** One [`Attempts`] tracker
//!   shared by an [`Encoder`], a [`Repairer`], and the replica's
//!   [`OrphanJudge`], and a [`Releaser`]. Each works only while the replica
//!   leads the shard and serves, so they run on every replica and act on
//!   the primary's. A replica that stops or closes takes them with it; the
//!   replica that opens in its place starts afresh, as a new life of the
//!   primary does (design §8.4, §8.6).
//! - **Repair and moves.** The repairer checks the shard's fragments on
//!   their nodes, rebuilds those lost on other nodes, and, in a pass that
//!   found none lost, moves fragments off nodes that are draining or
//!   departing, out of over-full failure domains, and toward each node's
//!   share (§8.3, §8.6). It passes every tenth of
//!   `fragment_repair_after_seconds`, from 1 to 30 seconds
//!   ([`RepairSettings::from_config`]), and every repairer of the node
//!   shares one [`RepairBandwidth`] of `repair_bytes_per_second_per_node`
//!   and the repair metrics.
//! - **Fragment release and orphans.** The node keeps released fragments
//!   in its [`FragmentHolds`] for `fragment_release_delay_seconds` and
//!   while reads hold them, sweeps them every `index_checkpoint_interval`
//!   with fragment-segment compaction, and asks the primaries about
//!   fragments it has held for `fragment_orphan_after_seconds`
//!   ([`OrphanReclaimer`]).
//! - **Coded reads.** The gateway reads coded objects from their
//!   fragments through a [`FragmentReadClient`] that holds what it reads
//!   ([`NodeCoding::source`]).
//!
//! The node places fragments on the nodes registered under `nodes/`, as it
//! last read them, under the `[ec]` policy and `[cluster] failure_domain`
//! ([`FragmentPlanner`]): a registration marked departing or draining
//! receives none, and its fragments are moved off; a node the node's own
//! fragment traffic found silent is suspect, ranked after live nodes
//! ([`liveness`]). [`CodingServices::held`] counts what the node's stores
//! hold, which tells when a drained node is empty.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Unit;
use skys3_config::{BucketSettings, Config};
use skys3_coord::{Candidate, FragmentPlanner, GeometryPolicy, Topology};
use skys3_ec::orphans::PrimariesOf;
use skys3_ec::read::{HoldFuture, ReadFuture};
use skys3_ec::{
    Attempts, EncodeEvent, EncodeStep, Encoder, EncoderSettings, FragmentClient, FragmentHolds,
    FragmentId, FragmentReadClient, FragmentRequest, FragmentServer, FragmentSource, FragmentStore,
    FragmentStoreConfig, HoldSettings, OrphanClient, OrphanJudge, OrphanReclaimer, OrphanServer,
    PlannerSource, ReleaseClient, ReleaseMetrics, ReleaseSettings, Releaser, RepairBandwidth,
    RepairMetrics, RepairSettings, Repairer,
};
use skys3_gateway::ShardRef;
use skys3_gateway::routing::ShardMap;
use skys3_io::{BlockingPool, Clock, MonotonicClock, RealDisk, WallClock};
use skys3_net::{Frame, MessageKind, Peers, Receiver, Sender, TokioNetwork, Transport};
use skys3_obs::MetricsRegistry;
use skys3_shard::{Shard, ShardSet};
use skys3_types::{BucketDocument, BucketMode, Label, NodeId};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::task::{JoinHandle, JoinSet};

use super::replication::{ClusterShards, Registrations};
use crate::node::StartError;
use liveness::{Silence, Watched};

pub(crate) mod liveness;

/// The node's fragment server.
pub(crate) type NodeFragments = FragmentServer<RealDisk>;
/// The node's judges of orphan queries, one per replica of a coded shard.
type NodeOrphans = OrphanServer<RealDisk>;
/// The node's fragment reads, for its gateway and its repairers.
type ReadClient = FragmentReadClient<TokioNetwork, RealDisk>;
/// The node's fragment writes, for its encoders and repairers.
type Writer = Watched<FragmentClient<TokioNetwork, RealDisk>>;

/// How often the node follows its replicas of `local` buckets' shards.
const FOLLOW_INTERVAL: Duration = Duration::from_secs(1);
/// The longest wait between two orphan sweeps: a sweep asks only about
/// fragments held for `fragment_orphan_after_seconds`, so a sweep a minute
/// costs little and delays reclamation by at most that.
const MAX_ORPHAN_SWEEP: Duration = Duration::from_secs(60);
/// The bounds of an encoder's scan interval (see [`scan_interval`]).
const MIN_SCAN: Duration = Duration::from_secs(1);
const MAX_SCAN: Duration = Duration::from_secs(60);

/// How often an encoder scans its shard: a tenth of `ec_after_seconds`,
/// from 1 to 60 seconds, so an object is coded at most a tenth late, and
/// a large shard is not scanned more than once a minute.
pub(crate) fn scan_interval(settings: &BucketSettings) -> Duration {
    (settings.ec_after() / 10).clamp(MIN_SCAN, MAX_SCAN)
}

/// How often the orphan reclaimer sweeps: a quarter of
/// `fragment_orphan_after_seconds`, at most [`MAX_ORPHAN_SWEEP`].
pub(crate) fn orphan_sweep_interval(orphan_after: Duration) -> Duration {
    (orphan_after / 4).clamp(Duration::from_secs(1), MAX_ORPHAN_SWEEP)
}

/// The erasure-coding metrics of a node, which the metrics reference
/// describes. Clones share them.
#[derive(Debug, Clone, Default)]
pub(crate) struct CodingMetrics {
    encoded: Counter,
    superseded: Counter,
    abandoned: Counter,
    stored: Gauge,
    reads: Family<Vec<(&'static str, &'static str)>, Counter>,
}

impl CodingMetrics {
    /// Registers the metrics in `registry`.
    pub(crate) fn register(registry: &MetricsRegistry) -> Self {
        let metrics = Self::default();
        registry.register(
            "encoded_objects",
            "Object versions this node's shard primaries erasure-coded: their EC_PUBLISH \
             committed and coded the version.",
            metrics.encoded.clone(),
        );
        registry.register(
            "encodings_superseded",
            "Encodings whose EC_PUBLISH committed but was rejected, because the version was \
             overwritten, deleted, or retagged meanwhile; their fragments are orphans.",
            metrics.superseded.clone(),
        );
        registry.register(
            "encodings_abandoned",
            "Encodings that failed before they appended their EC_PUBLISH: fragments that \
             could not be written after every re-plan, or a replica that stopped leading.",
            metrics.abandoned.clone(),
        );
        registry.register_with_unit(
            "fragment_stored",
            "Bytes of fragments this node's fragment stores hold, released ones included.",
            Unit::Bytes,
            metrics.stored.clone(),
        );
        registry.register(
            "fragment_reads",
            "Fragment reads this node's gateway made for coded GETs, by fragment (data or \
             parity) and outcome (read or failed). Parity reads mean degraded reads.",
            metrics.reads.clone(),
        );
        metrics
    }

    /// Counts what an encoder reports.
    fn observe(&self, event: &EncodeEvent) {
        match event.step {
            EncodeStep::Committed {
                published: true, ..
            } => self.encoded.inc(),
            EncodeStep::Committed {
                published: false, ..
            } => self.superseded.inc(),
            EncodeStep::Abandoned => self.abandoned.inc(),
            _ => 0,
        };
    }

    /// Counts a fragment read.
    fn read(&self, parity: bool, read: bool) {
        let fragment = if parity { "parity" } else { "data" };
        let outcome = if read { "read" } else { "failed" };
        self.reads
            .get_or_create(&vec![("fragment", fragment), ("outcome", outcome)])
            .inc();
    }
}

/// The gateway's fragment reads, counted by fragment and outcome.
#[derive(Debug)]
struct CountedReads {
    inner: Watched<ReadClient>,
    metrics: CodingMetrics,
}

impl FragmentSource for CountedReads {
    fn read(&self, request: FragmentRequest) -> ReadFuture<'_> {
        Box::pin(async move {
            let identity = &request.identity;
            let parity = usize::from(identity.index) >= identity.stripe.geometry.data_fragments();
            let read = self.inner.read(request).await;
            self.metrics.read(parity, read.is_ok());
            read
        })
    }

    fn register(&self) -> Option<(u64, Duration)> {
        self.inner.register()
    }

    fn hold(
        &self,
        node: NodeId,
        registration: u64,
        fragments: Vec<FragmentId>,
        end: bool,
    ) -> HoldFuture<'_> {
        self.inner.hold(node, registration, fragments, end)
    }
}

/// What the node's transport serves of erasure coding: fragment writes,
/// reads, holds, and releases, and orphan queries. Clones share them.
#[derive(Clone)]
pub(crate) struct CodingServices {
    fragments: NodeFragments,
    orphans: NodeOrphans,
}

impl std::fmt::Debug for CodingServices {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CodingServices")
            .field("stores", &self.fragments.stores().len())
            .finish_non_exhaustive()
    }
}

/// What a node's fragment stores hold ([`CodingServices::held`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct HeldFragments {
    /// The fragments the stores hold, released ones included.
    pub(crate) fragments: usize,
    /// Those of them no shard primary has released: fragments a layout
    /// may name, or orphans not reclaimed yet. A drained node is empty
    /// once this is 0.
    pub(crate) unreleased: usize,
}

impl CodingServices {
    /// What the node's fragment stores hold now.
    pub(crate) fn held(&self) -> HeldFragments {
        let holds = self.fragments.holds();
        let mut held = HeldFragments::default();
        for id in self.fragments.stores().iter().flat_map(FragmentStore::ids) {
            held.fragments += 1;
            if !holds.is_some_and(|holds| holds.is_released(id)) {
                held.unreleased += 1;
            }
        }
        held
    }

    /// Whether a connection whose first frame is of `kind` is one of
    /// erasure coding. Answers never open one.
    pub(crate) fn is_coding(kind: MessageKind) -> bool {
        matches!(
            kind,
            MessageKind::FragmentWrite
                | MessageKind::FragmentWritten
                | MessageKind::OrphanQuery
                | MessageKind::OrphanVerdicts
                | MessageKind::FragmentRead
                | MessageKind::FragmentData
                | MessageKind::FragmentRelease
                | MessageKind::FragmentHold
                | MessageKind::FragmentNoted
        )
    }

    /// Serves an authenticated connection whose first frame, `first`, is of
    /// erasure coding, until it ends. One that opens with an answer is
    /// closed.
    pub(crate) async fn serve<S>(&self, link: (Receiver<S>, Sender<S>), first: Frame)
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        match first.header.kind {
            MessageKind::FragmentWrite => self.fragments.serve(link, first).await,
            MessageKind::FragmentRead | MessageKind::FragmentHold => {
                self.fragments.serve_reads(link, first).await;
            }
            MessageKind::FragmentRelease => self.fragments.serve_releases(link, first).await,
            MessageKind::OrphanQuery => self.orphans.serve(link, first).await,
            kind => tracing::info!(
                peer = %link.0.peer(),
                ?kind,
                "closing an intra-cluster connection that opened with an answer"
            ),
        }
    }
}

/// What erasure coding runs for one replica: its tasks, which end with it.
struct ReplicaCoding {
    shard: Shard<RealDisk>,
    tasks: Vec<JoinHandle<()>>,
}

impl ReplicaCoding {
    /// Whether the replica still runs, and so do its tasks.
    fn runs(&self) -> bool {
        !self.shard.is_stopped() && self.tasks.iter().all(|task| !task.is_finished())
    }
}

impl Drop for ReplicaCoding {
    fn drop(&mut self) {
        // Only asks for cancellation: a task polled on another worker
        // finishes that poll first, which is why the repairer's backlog is
        // forgotten by its own task ([`finally`]), not here.
        for task in &self.tasks {
            task.abort();
        }
    }
}

/// `future`, then `cleanup` once `future` is dropped, whether it completed
/// or its task was aborted. A task's future is dropped only after its last
/// poll, so `cleanup` runs after anything that poll did, even when the
/// abort was asked for while another worker polled it.
async fn finally<F: Future>(future: F, cleanup: impl FnOnce()) -> F::Output {
    /// Runs its closure when dropped.
    struct Cleanup<C: FnOnce()>(Option<C>);

    impl<C: FnOnce()> Drop for Cleanup<C> {
        fn drop(&mut self) {
            if let Some(cleanup) = self.0.take() {
                cleanup();
            }
        }
    }

    let _cleanup = Cleanup(Some(cleanup));
    future.await
}

/// Erasure coding on a node (see the [module](self) docs).
pub(crate) struct NodeCoding {
    node: NodeId,
    config: Config,
    stores: Vec<FragmentStore<RealDisk>>,
    server: NodeFragments,
    orphans: NodeOrphans,
    holds: FragmentHolds,
    transport: Transport<TokioNetwork>,
    peers: Peers,
    silence: Silence,
    writer: Arc<Writer>,
    releases: Arc<ReleaseClient<TokioNetwork, RealDisk>>,
    reads: ReadClient,
    /// The repairers' reads, which take no holds.
    repair_reads: Arc<dyn FragmentSource>,
    planner: PlannerSource,
    primaries: PrimariesOf,
    wall: Arc<dyn WallClock>,
    pool: BlockingPool,
    metrics: CodingMetrics,
    repair: RepairSettings,
    bandwidth: RepairBandwidth,
    repairs: RepairMetrics,
    replicas: Mutex<BTreeMap<skys3_log::ShardRef, ReplicaCoding>>,
}

impl std::fmt::Debug for NodeCoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeCoding")
            .field("node", &self.node)
            .field("stores", &self.stores.len())
            .finish_non_exhaustive()
    }
}

impl NodeCoding {
    /// Recovers a fragment store on each of `disks`, the node's disks by
    /// label, and prepares the node's coding over the transport, peers,
    /// registrations, and shard map of `cluster`. Index work runs on
    /// `pool`, and the metrics go to `registry`.
    ///
    /// # Errors
    ///
    /// [`StartError::Fragments`] if a fragment store cannot be recovered,
    /// and [`StartError::Unsupported`] for an `[ec]` policy that defines no
    /// geometry.
    pub(crate) async fn open(
        config: &Config,
        node: &NodeId,
        disks: &[(Label, RealDisk)],
        cluster: &ClusterShards,
        (wall, pool): (Arc<dyn WallClock>, BlockingPool),
        registry: &MetricsRegistry,
    ) -> Result<Self, StartError> {
        let policy = GeometryPolicy::from_config(config.ec())
            .map_err(|error| StartError::Unsupported(format!("[ec]: {error}")))?;
        let stores = open_stores(config, disks).await?;
        let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::new());
        let holds = FragmentHolds::new(
            clock,
            HoldSettings::from_config(config.ec(), config.storage()),
        )
        .with_metrics(ReleaseMetrics::register(registry));
        let server = FragmentServer::new(stores.clone()).with_holds(holds.clone());
        let transport = cluster.transport().clone();
        let peers = cluster.peers().clone();
        let silence = Silence::default();
        let writer = FragmentClient::new(transport.clone(), peers.clone())
            .with_local(node.clone(), server.clone());
        let releases = ReleaseClient::new(
            node.clone(),
            transport.clone(),
            peers.clone(),
            Some(server.clone()),
        );
        let read_client = || {
            FragmentReadClient::new(node.clone(), transport.clone(), peers.clone())
                .with_local(server.clone())
        };
        let reads = read_client().with_holds(config.storage().read_registration_renew_interval());
        let unheld = read_client();
        let registrations = cluster.registrations();
        let level = config.cluster().failure_domain;
        let planned = Arc::clone(&registrations);
        let judge = silence.clone();
        let planner: PlannerSource = Arc::new(move || {
            let nodes: Vec<Candidate> = lock(&planned)
                .values()
                .map(|(_, registration)| judge.judge(Candidate::from_registration(registration)))
                .collect();
            FragmentPlanner::new(Topology::new(level, nodes), policy)
        });
        let repair = RepairSettings::from_config(config.ec());
        Ok(Self {
            node: node.clone(),
            config: config.clone(),
            primaries: primaries(cluster.routed().map().clone(), registrations),
            stores,
            server,
            orphans: OrphanServer::default(),
            holds,
            transport,
            peers,
            writer: Arc::new(Watched::new(writer, silence.clone())),
            releases: Arc::new(releases),
            reads,
            repair_reads: Arc::new(Watched::new(unheld, silence.clone())),
            silence,
            planner,
            wall,
            pool,
            metrics: CodingMetrics::register(registry),
            repair,
            bandwidth: RepairBandwidth::new(repair.bytes_per_second),
            repairs: RepairMetrics::register(registry),
            replicas: Mutex::default(),
        })
    }

    /// What the node's transport serves of erasure coding.
    pub(crate) fn services(&self) -> CodingServices {
        CodingServices {
            fragments: self.server.clone(),
            orphans: self.orphans.clone(),
        }
    }

    /// Where the node's gateway reads coded objects' fragments from (§8.5):
    /// its own stores directly, other nodes' over the transport, holding
    /// what each read may fetch while it streams (§8.7).
    pub(crate) fn source(&self) -> Arc<dyn FragmentSource> {
        Arc::new(CountedReads {
            inner: Watched::new(self.reads.clone(), self.silence.clone()),
            metrics: self.metrics.clone(),
        })
    }

    /// Runs the node-wide work until the node's background tasks end:
    /// sweeps of released fragments and fragment-segment compaction every
    /// `index_checkpoint_interval`, and orphan reclamation.
    pub(crate) fn spawn(self: &Arc<Self>, background: &mut JoinSet<()>) {
        let coding = Arc::clone(self);
        background.spawn(async move { coding.sweep_released().await });
        let orphan_after = self.config.ec().fragment_orphan_after();
        let client = OrphanClient::new(
            self.node.clone(),
            self.transport.clone(),
            self.peers.clone(),
            Arc::clone(&self.primaries),
            self.orphans.clone(),
        );
        let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::new());
        let reclaimer = OrphanReclaimer::new(
            self.node.clone(),
            self.stores.clone(),
            client,
            clock,
            orphan_after,
        )
        .with_holds(self.holds.clone());
        background.spawn(reclaimer.run(orphan_sweep_interval(orphan_after)));
    }

    /// Reclaims the released fragments that waited out the delay and no
    /// read holds, then compacts the stores, every
    /// `index_checkpoint_interval`, as the log's segments are (§10.3), and
    /// reports the bytes the stores hold.
    async fn sweep_released(&self) {
        let interval = self.config.storage().index_checkpoint_interval();
        let threshold = self.config.storage().compaction_live_threshold;
        loop {
            self.report_stored();
            tokio::time::sleep(interval).await;
            let report = self.holds.sweep(&self.stores, threshold).await;
            if report.reclaimed > 0 || report.compaction.segments > 0 || report.failed > 0 {
                tracing::debug!(?report, "a fragment release sweep ended");
            }
        }
    }

    fn report_stored(&self) {
        let stored: u64 = self.stores.iter().map(FragmentStore::live_bytes).sum();
        self.metrics
            .stored
            .set(i64::try_from(stored).unwrap_or(i64::MAX));
    }

    /// Follows the replicas of `set` every second, for as long as the task
    /// runs: see [`NodeCoding::reconcile`]. `buckets` gives the buckets the
    /// node knows.
    pub(crate) async fn follow(
        self: Arc<Self>,
        set: ShardSet<RealDisk>,
        buckets: impl Fn() -> Vec<BucketDocument> + Send + 'static,
    ) {
        loop {
            self.reconcile(&buckets(), &set).await;
            tokio::time::sleep(FOLLOW_INTERVAL).await;
        }
    }

    /// Starts erasure coding on every replica open in `set` of a shard of a
    /// `local` bucket of `buckets` that has none, and stops it on those
    /// that stopped or whose bucket is gone. Returns how many replicas it
    /// runs on.
    pub(crate) async fn reconcile(
        &self,
        buckets: &[BucketDocument],
        set: &ShardSet<RealDisk>,
    ) -> usize {
        let mut wanted = Vec::new();
        for bucket in buckets.iter().filter(|b| b.mode == BucketMode::Local) {
            let settings = self.config.buckets().get(&bucket.name);
            for shard in ShardRef::all(bucket) {
                if let Some(replica) = set.get(&(&shard).into()).await
                    && !replica.is_stopped()
                {
                    wanted.push((replica, settings.clone()));
                }
            }
        }
        let started: Vec<(Shard<RealDisk>, BucketSettings)> = {
            let mut replicas = lock(&self.replicas);
            let keep: BTreeSet<&skys3_log::ShardRef> =
                wanted.iter().map(|(replica, _)| replica.shard()).collect();
            let gone: Vec<skys3_log::ShardRef> = replicas
                .iter()
                .filter(|(shard, running)| !keep.contains(shard) || !running.runs())
                .map(|(shard, _)| shard.clone())
                .collect();
            for shard in gone {
                replicas.remove(&shard);
                self.orphans.remove(&shard);
                tracing::debug!(%shard, "erasure coding stopped on a replica");
            }
            wanted
                .into_iter()
                .filter(|(replica, _)| !replicas.contains_key(replica.shard()))
                .collect()
        };
        for (replica, settings) in started {
            let running = self.start_replica(replica, &settings);
            lock(&self.replicas).insert(running.shard.shard().clone(), running);
        }
        lock(&self.replicas).len()
    }

    /// Starts erasure coding on `replica`, a shard of a `local` bucket with
    /// `settings`: its judge answers orphan queries, and its encoder,
    /// repairer, and releaser work while it leads.
    fn start_replica(&self, replica: Shard<RealDisk>, settings: &BucketSettings) -> ReplicaCoding {
        let shard = replica.shard().clone();
        // One tracker of the shard's fragment-writing attempts on this
        // node, which every writer of fragments and the judge share.
        let attempts = Attempts::default();
        self.orphans.insert(OrphanJudge::new(
            replica.clone(),
            attempts.clone(),
            self.pool.clone(),
        ));
        let metrics = self.metrics.clone();
        let encoder = Encoder::new(
            replica.clone(),
            Arc::clone(&self.writer),
            Arc::clone(&self.planner),
            Arc::clone(&self.wall),
            EncoderSettings::from_bucket(settings),
        )
        .with_attempts(attempts.clone())
        .with_observer(Arc::new(move |event: &EncodeEvent| metrics.observe(event)));
        let repairer = Repairer::new(
            replica.clone(),
            Arc::clone(&self.repair_reads),
            Arc::clone(&self.writer),
            Arc::clone(&self.planner),
            self.pool.clone(),
            self.repair,
        )
        .with_attempts(attempts)
        .with_bandwidth(self.bandwidth.clone())
        .with_metrics(self.repairs.clone());
        let releaser = Releaser::new(
            replica.clone(),
            Arc::clone(&self.releases),
            Arc::clone(&self.planner),
            self.pool.clone(),
            ReleaseSettings::default(),
        );
        tracing::debug!(%shard, "erasure coding runs on a replica");
        // A repairer stopped in a pass leaves the fragments it knew lost
        // counted; its task forgets them once it is gone, after its last
        // update, and the shard's next primary finds them again.
        let repairs = self.repairs.clone();
        let repairing = finally(repairer.run(), move || repairs.forget(&shard));
        ReplicaCoding {
            shard: replica,
            tasks: vec![
                tokio::spawn(encoder.run(scan_interval(settings))),
                tokio::spawn(repairing),
                tokio::spawn(releaser.run()),
            ],
        }
    }

    /// Stops erasure coding on every replica, as the node stops.
    pub(crate) fn stop(&self) {
        let stopped = std::mem::take(&mut *lock(&self.replicas));
        for shard in stopped.keys() {
            self.orphans.remove(shard);
        }
    }
}

/// Opens a fragment store on each of `disks`: disk number `n` is the
/// `n`-th disk in label order (§10.2).
async fn open_stores(
    config: &Config,
    disks: &[(Label, RealDisk)],
) -> Result<Vec<FragmentStore<RealDisk>>, StartError> {
    let mut ordered: Vec<&(Label, RealDisk)> = disks.iter().collect();
    ordered.sort_by(|a, b| a.0.cmp(&b.0));
    let mut stores = Vec::with_capacity(ordered.len());
    for (number, (label, disk)) in ordered.into_iter().enumerate() {
        let number = u8::try_from(number).map_err(|_| {
            StartError::Unsupported(format!(
                "disk {label} is disk number {number}; fragment IDs number at most 256 disks"
            ))
        })?;
        let settings = FragmentStoreConfig::from_storage(number, config.storage());
        let (store, report) =
            FragmentStore::open(disk.clone(), settings)
                .await
                .map_err(|source| StartError::Fragments {
                    disk: label.clone(),
                    source,
                })?;
        tracing::info!(
            disk = %label,
            number,
            segments = report.segments.len(),
            fragments = report.fragments,
            torn_tail = ?report.torn_tail,
            "recovered the fragment store"
        );
        stores.push(store);
    }
    Ok(stores)
}

/// Whom a fragment node asks about a shard's fragments (§8.4): the
/// members and learners `map` knows for the shard, its primary first,
/// since only a replica that leads answers; or, for a shard the node has
/// never routed to, every registered node, of which the leading replica
/// answers.
fn primaries(map: ShardMap, registrations: Registrations) -> PrimariesOf {
    Arc::new(move |shard: &skys3_log::ShardRef| {
        let named = ShardRef {
            bucket: shard.bucket.clone(),
            shard: shard.shard,
        };
        match map.get(&named) {
            Some(config) => {
                let mut nodes = vec![config.primary.clone()];
                for node in config.members.iter().chain(&config.learners) {
                    if !nodes.contains(node) {
                        nodes.push(node.clone());
                    }
                }
                nodes
            }
            None => lock(&registrations).keys().cloned().collect(),
        }
    })
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // Plain inserts and removals, which a panic cannot leave half done.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::mpsc;
    use std::task::{Context, Poll};

    use super::*;

    /// A future whose first poll reports that it started, blocks its
    /// worker until told to go on, then records an update, as a repair
    /// pass's backlog update would, and stays pending.
    struct BlockingPoll {
        started: mpsc::Sender<()>,
        go: mpsc::Receiver<()>,
        log: Arc<Mutex<Vec<&'static str>>>,
    }

    impl Future for BlockingPoll {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            self.started.send(()).unwrap();
            self.go.recv().unwrap();
            lock(&self.log).push("update");
            Poll::Pending
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_cleanup_runs_after_the_last_poll_of_an_aborted_task() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let (started, polling) = mpsc::channel();
        let (go, waiting) = mpsc::channel();
        let polled = BlockingPoll {
            started,
            go: waiting,
            log: Arc::clone(&log),
        };
        let cleaned = Arc::clone(&log);
        let task = tokio::spawn(finally(polled, move || lock(&cleaned).push("forget")));
        // The task is in the middle of a poll on a worker when it is
        // aborted, as a repairer in a pass is when its replica's coding is
        // dropped.
        polling.recv().unwrap();
        task.abort();
        // A cleanup run when the abort is asked for, as the replica's drop
        // ran it, would land here, before the poll's update.
        assert!(lock(&log).is_empty());
        go.send(()).unwrap();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(*lock(&log), ["update", "forget"]);
    }

    #[tokio::test]
    async fn the_cleanup_runs_once_the_future_completes() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let cleaned = Arc::clone(&log);
        let done = finally(async { 7 }, move || lock(&cleaned).push("forget")).await;
        assert_eq!(done, 7);
        assert_eq!(*lock(&log), ["forget"]);
    }
}
