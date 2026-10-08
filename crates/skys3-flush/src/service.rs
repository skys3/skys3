//! The flushers of every `write_back` bucket, and of every `local` bucket
//! with a backup target, whose shards are open on a node, each with its
//! target's capability probe; and the origins of `read_only` buckets.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use skys3_config::{BucketsConfig, ConflictPolicy, TargetTransport};
use skys3_io::{Disk, SystemWallClock, WallClock};
use skys3_log::ShardRef;
use skys3_remote::ObjectStore;
use skys3_remote::probe::{
    ConditionalOperation, ConditionalProbe, ConditionalWrites, CopySupport, OperationSupport,
    PreconditionSupport,
};
use skys3_shard::{Shard, ShardSet};
use skys3_types::{
    BucketDocument, BucketId, BucketMode, BucketName, ClusterId, RemoteTarget, ShardId,
};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::budget::DirtyBudget;
use crate::concurrency::ConcurrencyStatus;
use crate::conflict::{Unresolved, effective_policy, may_discard};
use crate::fill::Filler;
use crate::import::{self, ImportJob, ImportState, ImportStatus, RemoteReader, Stop};
use crate::metrics::{Counters, FlushMetrics, Gauges};
use crate::origin::{self, Origin, OriginConnect};
use crate::peer::discovery::{Choice, Discovery, Probe};
use crate::peer::{Native, PeerBug, PeerTransport, TransportStatus, hooks as peer_hooks};
use crate::shard::{Ready, ShardFlusher, ShardStatus};
use crate::target::{CopySources, FlushSettings, Target};

/// Builds the store of a bucket's remote target.
pub type Connect<S> = Box<dyn Fn(&RemoteTarget) -> S + Send + Sync>;

/// Where a target's capability probe is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbeStatus {
    /// Not finished: flushing waits for it. `error` is why the last run
    /// failed, if one did; the probe runs again after a backoff.
    Running {
        /// The last run's error.
        error: Option<String>,
    },
    /// Finished. The operations that are not protected are sent
    /// unconditionally (§7.2).
    Done {
        /// The operations whose preconditions the target does not honor.
        unprotected: Vec<ConditionalOperation>,
        /// Whether copies of clean sources in the same remote bucket are
        /// sent as server-side copies; otherwise they are uploaded (§7.2,
        /// §11). Copies are probed after the writes, so that flushing need
        /// not wait for them: until that probe succeeds, this is `false`.
        server_side_copy: bool,
    },
}

/// A bucket's flush state on this node.
#[derive(Debug, Clone, PartialEq)]
pub struct BucketStatus {
    /// The bucket's name.
    pub name: String,
    /// Whether the target is the backup target of a `local` bucket (§8.9),
    /// rather than a `write_back` bucket's system of record.
    pub backup: bool,
    /// What a flush that finds an out-of-band write does (§7.2).
    pub conflict_policy: ConflictPolicy,
    /// The target's capability probe.
    pub probe: ProbeStatus,
    /// Each open shard's flusher.
    pub shards: Vec<(ShardId, ShardStatus)>,
    /// Remote multipart uploads that flushes left open and that wait to be
    /// aborted ([`Target::orphaned_uploads`]).
    pub orphaned_uploads: u64,
    /// The window of requests in flight to the target (§7.7), once the
    /// probe is done ([`Target::concurrency`]).
    pub concurrency: Option<ConcurrencyStatus>,
    /// This node's share of the bucket's dirty-data budget
    /// ([`DirtyBudget`]).
    pub dirty_budget: u64,
    /// The bucket's namespace import (§9.1); a backup target has none.
    pub import: Option<ImportStatus>,
    /// Whether the target is a SkyS3 peer flushed to over the native
    /// protocol (§7.8), rather than over S3 REST.
    pub native: bool,
    /// What the discovery of a target that may be a SkyS3 peer found: the
    /// transport in use and why (§7.8). `None` for a target flushed over
    /// S3 REST only, by its `target_transport` or because the node reaches
    /// no peers.
    pub transport: Option<TransportStatus>,
}

impl BucketStatus {
    /// The bucket's gauges at wall-clock time `now`.
    #[must_use]
    pub fn gauges(&self, now: Duration) -> Gauges {
        let age =
            |since: Option<Duration>| since.map_or(0.0, |s| now.saturating_sub(s).as_secs_f64());
        let statuses = self.shards.iter().map(|(_, status)| status);
        Gauges {
            dirty_bytes: statuses.clone().map(|s| s.dirty_bytes).sum(),
            oldest_dirty_age: age(statuses.clone().filter_map(|s| s.oldest_dirty).min()),
            flush_lag: age(statuses.clone().filter_map(|s| s.oldest_pending).min()),
            conflicted_keys: statuses.map(|s| s.conflicts.len() as u64).sum(),
            orphaned_uploads: self.orphaned_uploads,
            dirty_budget: self.dirty_budget,
            concurrency: self.concurrency.map_or(0, |window| u64::from(window.limit)),
            inflight_bytes: self.concurrency.map_or(0, |window| window.inflight_bytes),
            base_round_trip: self
                .concurrency
                .and_then(|window| window.base_round_trip)
                .map_or(0.0, |base| base.as_secs_f64()),
        }
    }
}

/// The flushers of a node (§7.1).
///
/// [`FlushService::reconcile`] makes them follow the node's buckets and
/// open shards: for each `write_back` bucket it connects to the target,
/// probes which preconditions the target honors (§7.2), retrying until the
/// probe succeeds, and runs a [`ShardFlusher`] for each of the bucket's
/// shards open on the node. A flusher tracks its shard's dirty keys at
/// once, so their bytes count against the [`DirtyBudget`] even while the
/// target is unreachable, and starts flushing when the probe is done.
/// Flushers of buckets and shards that are gone are stopped. Each bucket's
/// [`Filler`] reads evicted versions from the same target (§9.2); it needs
/// no probe.
///
/// Each bucket also runs its namespace import (§9.1) from the moment it is
/// followed: on attach, or resumed from its checkpoint after a restart.
/// Its flushers keep tombstones and resolve writes of unknown remote state
/// by its progress, and [`FlushService::remote`] reads the remote for the
/// keys it has not reached.
///
/// **Backup targets** (§8.9). A `local` bucket whose settings
/// ([`FlushService::with_buckets`]) name a `backup_target` is flushed to
/// it by the same flushers, with the same probe, ordering, conditional
/// writes, write identity, and streaming. What differs comes from the
/// target not being the bucket's system of record. Nothing is imported
/// from it, so every key counts as imported, and a key without a remote
/// ETag is sent with `If-None-Match: *`. Nothing is read from it, so the
/// bucket has no [`Filler`] and no [`RemoteReader`]. Its flushers count
/// no dirty bytes against the [`DirtyBudget`], since the cluster holds
/// them durably and a backup outage must not stop the bucket's writes;
/// the flush metrics measure how far the backup lags. And nothing is
/// evicted: the node's clean cache is never told a `local` bucket's
/// `clean_copies`, so it keeps every payload that a `FLUSHED` made clean
/// (`skys3_shard::CleanCache::evicts`).
///
/// **Origins** (§9.5). A `read_only` bucket's target is an origin that
/// SkyS3 does not own: the service keeps only how to read it ([`Origin`]),
/// for the gateway's revalidations and forwarded listings and for fills,
/// with the credentials its `origin_profile` names
/// ([`FlushService::with_origin_connect`]). Nothing is flushed to it,
/// probed, or imported.
///
/// **Conflicts** (§7.2). A bucket's flushers apply its
/// `flush_conflict_policy` from its settings, `hold` without them: a key
/// held in conflict waits for [`FlushService::resolve`], and `overwrite`
/// and `discard_local` resolve conflicts as they are found. Only a
/// `write_back` bucket whose own table names `discard_local` discards by
/// itself, and a backup target never discards: it holds instead.
///
/// **SkyS3 peers** (§7.8). On a service that reaches peers
/// ([`FlushService::with_peer_transport`]), the target of a bucket whose
/// `target_transport` is `auto` (the default) or `native` has a discovery
/// that chooses its transport (`peer::discovery`): QUIC to a SkyS3 peer
/// whose descriptor verifies and whose handshake succeeds, S3 REST to that
/// peer while its QUIC path fails (`auto` only), or S3 REST to a store
/// that is not a peer. Without peer transport, `auto` means S3 REST and a
/// `native` target is never flushed.
///
/// - **Over QUIC** a target is ready at once, without a probe: the
///   destination evaluates every precondition itself. Its conflicts are
///   held rather than discarded, since adopting the peer's write would
///   read it over S3.
/// - **Over S3 REST to a peer** the target needs no probe either, since a
///   SkyS3 gateway honors every precondition, but each shard flusher that
///   starts on it, with the target or later as it takes a shard over,
///   sends nothing before the transport's quarantine passed
///   ([`PeerTransport::quarantine`]): no `COMMIT` sent over QUIC before it
///   started, here or on an earlier primary, can then still apply once a
///   key is flushed over S3. So does one on a target whose descriptor was
///   refused.
/// - **A change of transport** builds the target anew and restarts the
///   bucket's shard flushers on it, as a restart or a takeover does: keys
///   in flight stay dirty, and the new flushers send them again with the
///   same write identities and preconditions, which the destination
///   evaluates whichever transport carried an earlier copy.
///
/// A `write_back` bucket imports its namespace from, and fills from, the
/// target's S3 endpoint, whatever its transport.
pub struct FlushService<S, D> {
    cluster: ClusterId,
    settings: FlushSettings,
    connect: Connect<S>,
    /// Builds the stores of origins with their own credentials; without
    /// it, origins are read through `connect`, with the default chain.
    origin_connect: Option<OriginConnect<S>>,
    /// The origin of each `read_only` bucket.
    origins: Mutex<BTreeMap<BucketId, Origin<S>>>,
    wall: Arc<dyn WallClock>,
    metrics: FlushMetrics,
    budget: Arc<DirtyBudget>,
    /// Each bucket's settings, if the service has them.
    bucket_settings: Option<BucketsConfig>,
    /// The nonce of each capability probe's next run, if they are given
    /// rather than fresh ([`FlushService::with_probe_nonces`]).
    probe_nonces: Option<Arc<AtomicU64>>,
    /// How `auto` and `native` targets reach SkyS3 peers, if they can.
    peers: Option<Arc<PeerTransport>>,
    buckets: Mutex<BTreeMap<BucketId, BucketFlusher<S>>>,
    /// The copy sources of each remote bucket the buckets flush to.
    copy_sources: Mutex<BTreeMap<Location, CopySources>>,
    /// The import tasks of buckets no longer followed, told to stop and
    /// not yet waited for: the bucket's next import waits for them first.
    stopping: Mutex<BTreeMap<BucketId, Vec<JoinHandle<()>>>>,
    _disk: std::marker::PhantomData<fn() -> D>,
}

impl<S, D> fmt::Debug for FlushService<S, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlushService")
            .field("cluster", &self.cluster)
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

/// The flushers of one bucket.
struct BucketFlusher<S> {
    name: String,
    /// The policy the bucket's settings name, before the target's
    /// transport restricts it.
    configured_policy: ConflictPolicy,
    /// Whether the target is a backup target.
    backup: bool,
    /// The conflict policy its flushers apply.
    conflict_policy: ConflictPolicy,
    /// Whether an operator may resolve its conflicts with `discard_local`:
    /// it is not a backup target or a SkyS3 peer over QUIC.
    may_discard: bool,
    /// Whether its target is a SkyS3 peer over the native protocol.
    native: bool,
    /// How long each shard flusher it starts waits before it sends
    /// anything: the quarantine of a SkyS3 peer reached over S3 REST
    /// (§7.8), during which a native `COMMIT` sent before the flusher
    /// started may still apply; zero for any other target.
    quarantine: Duration,
    probe: Arc<Mutex<ProbeStatus>>,
    probe_task: JoinHandle<()>,
    /// The target, once the probe is done.
    target: Arc<Ready<S>>,
    shards: BTreeMap<ShardId, ShardFlusher>,
    /// What only the target of a `write_back` bucket, its system of
    /// record, has; `None` for a backup target.
    record: Option<SystemOfRecord<S>>,
    /// What the target is built from.
    parts: TargetParts<S>,
    /// The transport selection of a target that may be a SkyS3 peer, and
    /// the generation of the choice the target was built for.
    discovery: Option<(Discovery, u64)>,
}

/// What only a `write_back` bucket's target has: the bucket's namespace
/// import (§9.1) and reads of the target (§9.2).
struct SystemOfRecord<S> {
    /// What client reads of the remote use.
    remote: RemoteReader<S>,
    import: Arc<ImportState>,
    /// Stops the import when raised or dropped.
    stop_import: watch::Sender<bool>,
    /// The import's task, until it is waited for or handed on.
    import_task: Option<JoinHandle<()>>,
    filler: Filler<S>,
}

impl<S> Drop for BucketFlusher<S> {
    fn drop(&mut self) {
        self.probe_task.abort();
        // Not aborted: the import stops between steps, and its task ends
        // once no checkpoint write of it is pending (`import::Stop`).
        if let Some(record) = &self.record {
            record.stop_import.send_replace(true);
        }
    }
}

/// What a bucket's flushers send its writes to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    /// The target of a `write_back` bucket, its system of record.
    WriteBack,
    /// The backup target of a `local` bucket (§8.9).
    Backup,
}

impl<S: ObjectStore, D: Disk> FlushService<S, D> {
    /// A service for the cluster `cluster`, whose targets' stores `connect`
    /// builds, with its metrics in `metrics`.
    pub fn new(
        cluster: ClusterId,
        settings: FlushSettings,
        connect: Connect<S>,
        metrics: FlushMetrics,
    ) -> Self {
        Self {
            cluster,
            settings,
            connect,
            origin_connect: None,
            origins: Mutex::default(),
            wall: Arc::new(SystemWallClock),
            metrics,
            budget: Arc::new(DirtyBudget::unlimited()),
            bucket_settings: None,
            probe_nonces: None,
            peers: None,
            buckets: Mutex::default(),
            copy_sources: Mutex::default(),
            stopping: Mutex::default(),
            _disk: std::marker::PhantomData,
        }
    }

    /// Sets the clock that dirty ages are measured on.
    #[must_use]
    pub fn with_wall_clock(mut self, wall: Arc<dyn WallClock>) -> Self {
        self.wall = wall;
        self
    }

    /// Gives the targets' capability probe runs the nonces `first`,
    /// `first + 1`, and so on, in the order they start, instead of fresh
    /// ones: a simulation derives `first` from its seed, so that its probe
    /// keys, which an import's split points see, replay too.
    #[must_use]
    pub fn with_probe_nonces(mut self, first: u64) -> Self {
        self.probe_nonces = Some(Arc::new(AtomicU64::new(first)));
        self
    }

    /// Reads the origins of `read_only` buckets through the stores
    /// `connect` builds, with the credentials of each bucket's
    /// `origin_profile` (§9.5). Without it, origins are read through the
    /// service's own `connect`, with the default credential chain.
    #[must_use]
    pub fn with_origin_connect(mut self, connect: OriginConnect<S>) -> Self {
        self.origin_connect = Some(connect);
        self
    }

    /// Looks for SkyS3 peers behind the targets of buckets whose
    /// `target_transport` is `auto` or `native`, and flushes to them over
    /// the native protocol, reached through `peers`, when it can (§7.8).
    /// Without it, every target is flushed over S3 REST, and `native` ones
    /// not at all.
    #[must_use]
    pub fn with_peer_transport(mut self, peers: PeerTransport) -> Self {
        self.peers = Some(Arc::new(peers));
        self
    }

    /// Counts the flushers' dirty bytes against `budget`.
    #[must_use]
    pub fn with_budget(mut self, budget: Arc<DirtyBudget>) -> Self {
        self.budget = budget;
        self
    }

    /// Takes each bucket's `import_parallel_streams`, `backup_target`, and
    /// `flush_conflict_policy` from `buckets`; otherwise every import has
    /// `settings.import_streams` streams, no `local` bucket is flushed, and
    /// conflicts are held.
    #[must_use]
    pub fn with_buckets(mut self, buckets: BucketsConfig) -> Self {
        self.bucket_settings = Some(buckets);
        self
    }

    /// The budget the flushers' dirty bytes count against.
    #[must_use]
    pub fn budget(&self) -> &Arc<DirtyBudget> {
        &self.budget
    }

    /// The target `bucket`'s writes are flushed to, if any: a `write_back`
    /// bucket's own, or a `local` bucket's backup target (§8.9). Index
    /// snapshots (§8.9, plan M4-11) go to a `local` bucket's backup target
    /// by default.
    #[must_use]
    pub fn target_of(&self, bucket: &BucketDocument) -> Option<RemoteTarget> {
        self.followed(bucket).map(|(target, _)| target)
    }

    /// The target `bucket`'s writes are flushed to, and its role.
    fn followed(&self, bucket: &BucketDocument) -> Option<(RemoteTarget, Role)> {
        match (bucket.mode, &bucket.target) {
            (BucketMode::WriteBack, Some(target)) => Some((target.clone(), Role::WriteBack)),
            (BucketMode::Local, _) => {
                let settings = self.bucket_settings.as_ref()?.get(&bucket.name);
                Some((settings.backup_target.clone()?, Role::Backup))
            }
            _ => None,
        }
    }

    /// Starts and stops flushers so that every `write_back` bucket in
    /// `buckets`, and every `local` one with a backup target, with shards
    /// open in `set` as their primary is flushed, and nothing else.
    pub async fn reconcile(&self, buckets: &[BucketDocument], set: &ShardSet<D>) {
        self.follow_origins(buckets);
        self.follow_copy_sources(buckets);
        let mut open = Vec::new();
        for bucket in buckets {
            let Some((target, role)) = self.followed(bucket) else {
                continue;
            };
            let mut shards = Vec::new();
            for shard in bucket.shards.shards() {
                let shard_ref = ShardRef::new(bucket.bucket_id.clone(), shard);
                // Only a shard's primary flushes it (§7.1): a member or a
                // learner refuses the `FLUSHED` records its primary does
                // not send.
                if let Some(shard) = set.get(&shard_ref).await
                    && !shard.is_stopped()
                    && !shard.role().follows()
                {
                    shards.push(shard);
                }
            }
            open.push((bucket, target, role, shards));
        }
        // What waits for a backup is durable in the cluster: it counts
        // against no budget.
        self.budget.plan(
            open.iter()
                .filter(|(_, _, role, _)| *role == Role::WriteBack)
                .map(|(bucket, _, _, shards)| {
                    (*bucket, u32::try_from(shards.len()).unwrap_or(u32::MAX))
                }),
        );
        let mut flushers = self.lock();
        let wanted: BTreeSet<&BucketId> =
            open.iter().map(|(bucket, ..)| &bucket.bucket_id).collect();
        flushers.retain(|id, flusher| {
            let keep = wanted.contains(id);
            if !keep {
                self.metrics.remove(&flusher.name);
                if let Some(record) = &mut flusher.record {
                    record.stop_import.send_replace(true);
                    if let Some(task) = record.import_task.take() {
                        let mut stopping = lock(&self.stopping);
                        let tasks = stopping.entry(id.clone()).or_default();
                        tasks.retain(|task| !task.is_finished());
                        tasks.push(task);
                    }
                }
            }
            keep
        });
        for (bucket, target, role, shards) in open {
            let flusher = flushers
                .entry(bucket.bucket_id.clone())
                .or_insert_with(|| self.start(bucket, &target, role, set));
            self.follow_transport(flusher);
            let charged = role == Role::WriteBack;
            self.follow(&bucket.bucket_id, flusher, shards, charged);
        }
    }

    /// Starts a bucket's flushers, on the target its discovery chooses or
    /// with its target's probe, which makes the target ready once it
    /// succeeds, and, for a `write_back` bucket, its namespace import.
    fn start(
        &self,
        bucket: &BucketDocument,
        target: &RemoteTarget,
        role: Role,
        set: &ShardSet<D>,
    ) -> BucketFlusher<S> {
        let name = bucket.name.as_str().to_owned();
        let import = (role == Role::WriteBack).then(|| Arc::new(ImportState::default()));
        let settings = self
            .bucket_settings
            .as_ref()
            .map(|buckets| buckets.get(&bucket.name));
        let configured_policy = settings.map_or(ConflictPolicy::Hold, |settings| {
            settings.flush_conflict_policy
        });
        let transport = settings.map_or(TargetTransport::Auto, |s| s.target_transport);
        let backup = role == Role::Backup;
        let parts = TargetParts {
            store: Arc::new((self.connect)(target)),
            prefix: target.prefix.clone().unwrap_or_default(),
            cluster: self.cluster.clone(),
            settings: self.settings.clone(),
            wall: Arc::clone(&self.wall),
            counters: self.metrics.counters(&name),
            import: import.clone(),
            conflict_policy: configured_policy,
            copy_sources: self.copy_sources_of(target),
            bucket: target.bucket.parse().ok(),
        };
        let record = import.map(|import| self.system_of_record(bucket, &parts, import, set));
        let discovery = self.discovery(&name, target, transport, &parts);
        let plain = discovery.is_none();
        let mut flusher = BucketFlusher {
            name,
            configured_policy,
            backup,
            conflict_policy: configured_policy,
            may_discard: may_discard(backup),
            native: false,
            quarantine: Duration::ZERO,
            probe: Arc::new(Mutex::new(ProbeStatus::Running { error: None })),
            probe_task: tokio::spawn(async {}),
            target: Ready::new(None),
            shards: BTreeMap::new(),
            record,
            parts,
            discovery: discovery.map(|discovery| (discovery, u64::MAX)),
        };
        if plain {
            self.install(&mut flusher, Choice::PlainS3 { found: false });
        } else {
            self.follow_transport(&mut flusher);
        }
        flusher
    }

    /// The discovery of a target that may be a SkyS3 peer (§7.8): one whose
    /// `target_transport` is `auto` on a service that reaches peers, or
    /// `native`.
    fn discovery(
        &self,
        name: &str,
        target: &RemoteTarget,
        configured: TargetTransport,
        parts: &TargetParts<S>,
    ) -> Option<Discovery> {
        let stuck = |reason: &str| {
            tracing::warn!(bucket = name, reason, "a native target is not flushed");
            Some(Discovery::stuck(configured, reason))
        };
        match (configured, &self.peers) {
            (TargetTransport::S3, _) | (TargetTransport::Auto, None) => None,
            (TargetTransport::Native, None) => stuck("this node has no peer transport"),
            (_, Some(peers)) => match target.bucket.parse::<BucketName>() {
                Ok(bucket) => Some(Discovery::spawn(Probe {
                    store: Arc::clone(&parts.store),
                    bucket,
                    configured,
                    peers: Arc::clone(peers),
                    cluster: self.cluster.clone(),
                    wall: Arc::clone(&self.wall),
                })),
                // A SkyS3 bucket name it is not, so no peer holds it.
                Err(_) if configured == TargetTransport::Auto => None,
                Err(error) => stuck(&format!(
                    "the target's bucket is not a bucket name: {error}"
                )),
            },
        }
    }

    /// Builds the bucket's target anew for its discovery's current choice,
    /// if that changed since the target was built. A target without
    /// discovery is built once, as it starts, and probed.
    fn follow_transport(&self, flusher: &mut BucketFlusher<S>) {
        let Some((discovery, built)) = &mut flusher.discovery else {
            return;
        };
        let (generation, choice) = discovery.current();
        if generation != *built {
            *built = generation;
            self.install(flusher, choice);
        }
    }

    /// Builds the bucket's target for `choice`, and restarts the bucket's
    /// shard flushers on it.
    fn install(&self, flusher: &mut BucketFlusher<S>, choice: Choice) {
        flusher.probe_task.abort();
        // The flushers of the old target stop, as on a restart; their keys
        // stay dirty, and the new flushers send them again.
        flusher.shards.clear();
        let native = matches!(choice, Choice::Native(_));
        // Adopting the peer's write would read it over S3 (§7.8).
        let holds = flusher.backup || native;
        flusher.conflict_policy = effective_policy(flusher.configured_policy, holds);
        flusher.may_discard = may_discard(holds);
        flusher.native = native;
        // Over S3 REST to what may be a peer that took native commits, each
        // shard flusher waits them out as it starts: here, and as it takes
        // a shard over later.
        flusher.quarantine = match choice {
            Choice::PeerS3 | Choice::PlainS3 { found: true } => self.quarantine(),
            _ => Duration::ZERO,
        };
        if holds && flusher.configured_policy == ConflictPolicy::DiscardLocal {
            tracing::warn!(
                bucket = flusher.name,
                "a backup target or a SkyS3 peer never discards local writes; \
                its conflicts are held"
            );
        }
        let mut parts = flusher.parts.clone();
        parts.conflict_policy = flusher.conflict_policy;
        let (target, probe, probe_task) = match choice {
            // Nothing is flushed until the discovery chooses.
            Choice::Waiting => (
                Ready::new(None),
                ProbeStatus::Running { error: None },
                tokio::spawn(async {}),
            ),
            // The peer evaluates every precondition itself: nothing to
            // probe, and the target is ready before its flushers start.
            // Nothing to sweep either: a native target opens no remote
            // multipart upload, so none is ever left open (§7.8).
            Choice::Native(link) => {
                let native = self.native(&flusher.parts, link, flusher.discovery.as_ref());
                let target = Ready::new(Some(Arc::new(parts.build(native_writes(), native))));
                (target, done(), tokio::spawn(async {}))
            }
            // The shard flushers wait out the quarantine before they use
            // it.
            Choice::PeerS3 => {
                let target = Arc::new(parts.build(native_writes(), None));
                let ready = Ready::new(Some(Arc::clone(&target)));
                let task = tokio::spawn(after_quarantine(target, flusher.quarantine));
                (ready, done(), task)
            }
            Choice::PlainS3 { .. } => {
                let probe = Arc::new(Mutex::new(ProbeStatus::Running { error: None }));
                let ready = Ready::new(None);
                let nonces = self.probe_nonces.clone();
                let task = tokio::spawn(run_probe(
                    parts,
                    nonces,
                    Arc::clone(&probe),
                    Arc::clone(&ready),
                ));
                flusher.probe = probe;
                flusher.target = ready;
                flusher.probe_task = task;
                return;
            }
        };
        flusher.probe = Arc::new(Mutex::new(probe));
        flusher.target = target;
        flusher.probe_task = probe_task;
    }

    /// The native transport over `link` of a target built from `parts`,
    /// which tells its `discovery` of failed links.
    fn native(
        &self,
        parts: &TargetParts<S>,
        link: Arc<dyn crate::peer::PeerLink>,
        discovery: Option<&(Discovery, u64)>,
    ) -> Option<Native> {
        let peers = self.peers.as_ref()?;
        let bucket = parts.bucket.clone()?;
        let native = Native::new(link, bucket, peers.frame_bytes, peers.timeout);
        Some(match discovery {
            Some((discovery, _)) => native.with_alarm(discovery.alarm()),
            None => native,
        })
    }

    /// How long a peer target's S3 REST flushes wait after their flushers
    /// start ([`PeerTransport::quarantine`]).
    fn quarantine(&self) -> Duration {
        if peer_hooks::peer_bug() == PeerBug::NoQuarantine {
            return Duration::ZERO;
        }
        self.peers
            .as_ref()
            .map_or(Duration::ZERO, |peers| peers.quarantine())
    }

    /// Starts the namespace import of `bucket`, a `write_back` bucket whose
    /// target `parts` holds, and makes the reads of the target.
    fn system_of_record(
        &self,
        bucket: &BucketDocument,
        parts: &TargetParts<S>,
        import: Arc<ImportState>,
        set: &ShardSet<D>,
    ) -> SystemOfRecord<S> {
        let (store, prefix) = (Arc::clone(&parts.store), parts.prefix.clone());
        let filler = Filler::new(
            Arc::clone(&store),
            prefix.clone(),
            parts.settings.clone(),
            parts.counters.clone(),
            Arc::clone(&parts.wall),
        );
        let streams =
            self.bucket_settings
                .as_ref()
                .map_or(self.settings.import_streams, |buckets| {
                    usize::try_from(buckets.get(&bucket.name).import_parallel_streams)
                        .unwrap_or(usize::MAX)
                });
        let previous = lock(&self.stopping)
            .remove(&bucket.bucket_id)
            .unwrap_or_default();
        let (stop_import, stop) = Stop::new();
        let import_task = tokio::spawn(import::run(ImportJob {
            store: Arc::clone(&store),
            prefix: prefix.clone(),
            bucket: bucket.clone(),
            set: set.clone(),
            settings: self.settings.clone(),
            streams,
            wall: Arc::clone(&self.wall),
            state: Arc::clone(&import),
            stop,
            previous,
        }));
        SystemOfRecord {
            remote: RemoteReader::new(store, prefix, Arc::clone(&import), Arc::clone(&self.wall)),
            import,
            stop_import,
            import_task: Some(import_task),
            filler,
        }
    }

    /// Lists, for each remote bucket the buckets in `buckets` flush to,
    /// the buckets whose objects it holds: the sources a copy flushed there
    /// may be copied from server-side (§7.2, §11). Every target is reached
    /// with the node's own credentials (`connect`), so one endpoint and
    /// bucket name one remote bucket with one set of credentials. Origins
    /// have their own and are never sources: a copy from one is refused.
    fn follow_copy_sources(&self, buckets: &[BucketDocument]) {
        let mut lists: BTreeMap<Location, BTreeMap<BucketId, String>> = BTreeMap::new();
        for bucket in buckets {
            if let Some((target, _)) = self.followed(bucket) {
                let prefix = target.prefix.clone().unwrap_or_default();
                lists
                    .entry(location(&target))
                    .or_default()
                    .insert(bucket.bucket_id.clone(), prefix);
            }
        }
        // Lists are emptied rather than dropped: a target being built may
        // hold one already.
        let mut sources = lock(&self.copy_sources);
        for (at, list) in sources.iter() {
            if !lists.contains_key(at) {
                list.set(BTreeMap::new());
            }
        }
        for (at, list) in lists {
            sources.entry(at).or_default().set(list);
        }
    }

    /// The copy sources of the remote bucket `target` names.
    fn copy_sources_of(&self, target: &RemoteTarget) -> CopySources {
        lock(&self.copy_sources)
            .entry(location(target))
            .or_default()
            .clone()
    }

    /// Keeps how to read the origin of every `read_only` bucket in
    /// `buckets`, and of no other.
    fn follow_origins(&self, buckets: &[BucketDocument]) {
        let wanted = origin::origins(buckets);
        let mut origins = lock(&self.origins);
        origins.retain(|id, _| wanted.contains_key(id));
        for (id, (bucket, target)) in wanted {
            if origins.contains_key(id) {
                continue;
            }
            let name = bucket.name.as_str();
            let (store, profile) = match &self.origin_connect {
                Some(connect) => {
                    let profile = self
                        .bucket_settings
                        .as_ref()
                        .and_then(|buckets| buckets.get(&bucket.name).origin_profile.as_deref());
                    (connect(target, profile), profile)
                }
                None => ((self.connect)(target), None),
            };
            let origin = Origin::new(
                target,
                profile,
                store,
                &self.settings,
                self.metrics.counters(name),
                &self.wall,
            );
            origins.insert(id.clone(), origin);
        }
    }

    /// Runs a flusher for each of `shards`, with their dirty bytes counted
    /// against the budget if `charged`, and stops the bucket's other
    /// flushers.
    fn follow(
        &self,
        bucket: &BucketId,
        flusher: &mut BucketFlusher<S>,
        shards: Vec<Shard<D>>,
        charged: bool,
    ) {
        let open: BTreeSet<ShardId> = shards.iter().map(|shard| shard.shard().shard).collect();
        flusher
            .shards
            .retain(|id, shard| open.contains(id) && !shard.is_stopped());
        for shard in shards {
            let id = shard.shard().shard;
            flusher.shards.entry(id).or_insert_with(|| {
                let charge = charged.then(|| self.budget.charge(bucket));
                let (ready, wall) = (Arc::clone(&flusher.target), Arc::clone(&self.wall));
                ShardFlusher::start(shard, ready, wall, charge, flusher.quarantine)
            });
        }
    }

    /// The flush state of `bucket`, if it is flushed here.
    #[must_use]
    pub fn status(&self, bucket: &BucketId) -> Option<BucketStatus> {
        let flushers = self.lock();
        let flusher = flushers.get(bucket)?;
        Some(BucketStatus {
            name: flusher.name.clone(),
            backup: flusher.record.is_none(),
            conflict_policy: flusher.conflict_policy,
            probe: lock(&flusher.probe).clone(),
            shards: flusher
                .shards
                .iter()
                .map(|(id, shard)| (*id, shard.status()))
                .collect(),
            orphaned_uploads: flusher
                .target
                .get()
                .map_or(0, |target| target.orphaned_uploads() as u64),
            concurrency: flusher.target.get().map(|target| target.concurrency()),
            dirty_budget: self
                .budget
                .usage(bucket)
                .map_or(u64::MAX, |usage| usage.share),
            import: flusher.record.as_ref().map(|record| record.import.status()),
            native: flusher.native,
            transport: flusher
                .discovery
                .as_ref()
                .map(|(discovery, _)| discovery.status()),
        })
    }

    /// Resolves the conflict that one of `bucket`'s flushers on this node
    /// holds `key` in, under `policy`, as an operator asks through the
    /// admin API (§7.2): the key returns to dirty, and its next flush
    /// retries the conditional request (`hold`), overwrites the remote's
    /// write (`overwrite`), or adopts it and drops the local version
    /// (`discard_local`). See [`ShardFlusher::resolve`].
    ///
    /// # Errors
    ///
    /// [`Unresolved::NotFlushed`] if the bucket is not flushed here,
    /// [`Unresolved::Backup`] for `discard_local` on a backup target, and
    /// [`Unresolved::NotHeld`] if no flusher here holds the key in
    /// conflict: its shard's primary may be another node.
    pub async fn resolve(
        &self,
        bucket: &BucketId,
        key: &str,
        policy: ConflictPolicy,
    ) -> Result<(), Unresolved> {
        let resolver = {
            let flushers = self.lock();
            let flusher = flushers.get(bucket).ok_or(Unresolved::NotFlushed)?;
            if policy == ConflictPolicy::DiscardLocal && !flusher.may_discard {
                return Err(Unresolved::Backup);
            }
            flusher
                .shards
                .values()
                .find_map(|shard| shard.resolver_of(key))
                .ok_or(Unresolved::NotHeld)?
        };
        resolver.resolve(key, policy).await
    }

    /// The read-through fills of `bucket`'s target, if the bucket is a
    /// `write_back` bucket this node follows, or a `read_only` one.
    #[must_use]
    pub fn filler(&self, bucket: &BucketId) -> Option<Filler<S>> {
        if let Some(origin) = lock(&self.origins).get(bucket) {
            return Some(origin.filler().clone());
        }
        let flushers = self.lock();
        Some(flushers.get(bucket)?.record.as_ref()?.filler.clone())
    }

    /// How this node reads the origin of `bucket`, if it is a `read_only`
    /// bucket (§9.5).
    #[must_use]
    pub fn origin(&self, bucket: &BucketId) -> Option<Origin<S>> {
        lock(&self.origins).get(bucket).cloned()
    }

    /// The remote target of `bucket` and its import's progress, for client
    /// reads of keys the import has not reached and lazily loaded metadata
    /// (§9.1), if the bucket is a `write_back` bucket this node follows.
    #[must_use]
    pub fn remote(&self, bucket: &BucketId) -> Option<RemoteReader<S>> {
        let flushers = self.lock();
        Some(flushers.get(bucket)?.record.as_ref()?.remote.clone())
    }

    /// The metrics [`FlushService::refresh_metrics`] sets.
    #[must_use]
    pub fn metrics(&self) -> &FlushMetrics {
        &self.metrics
    }

    /// Sets the flush gauges of every bucket flushed here.
    pub fn refresh_metrics(&self) {
        let now = self.wall.now();
        let ids: Vec<BucketId> = self.lock().keys().cloned().collect();
        for id in ids {
            if let Some(status) = self.status(&id) {
                self.metrics.set(&status.name, status.gauges(now));
                self.metrics
                    .set_transport(&status.name, status.transport.as_ref());
            }
        }
    }

    /// Stops every flusher and import. Flushes in flight are abandoned;
    /// their keys stay dirty and are flushed after the next start. An
    /// import resumes from its stored checkpoints.
    pub async fn shutdown(&self) {
        lock(&self.origins).clear();
        let flushers = std::mem::take(&mut *self.lock());
        for (_, mut flusher) in flushers {
            // The import stops between steps; once its task ends, none of
            // its checkpoint writes is pending (`import::Stop`).
            if let Some(record) = &mut flusher.record {
                record.stop_import.send_replace(true);
                if let Some(task) = record.import_task.take() {
                    let _ = task.await;
                }
            }
            for (_, shard) in std::mem::take(&mut flusher.shards) {
                shard.stop().await;
            }
            self.metrics.remove(&flusher.name);
        }
        let stopping = std::mem::take(&mut *lock(&self.stopping));
        for task in stopping.into_values().flatten() {
            let _ = task.await;
        }
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<BucketId, BucketFlusher<S>>> {
        lock(&self.buckets)
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A remote bucket: its endpoint and name.
type Location = (String, String);

fn location(target: &RemoteTarget) -> Location {
    (target.endpoint.clone(), target.bucket.clone())
}

/// What a bucket's [`Target`] is built from once its probe is done, or
/// its discovery chose a transport.
struct TargetParts<S> {
    store: Arc<S>,
    prefix: String,
    cluster: ClusterId,
    settings: FlushSettings,
    wall: Arc<dyn WallClock>,
    counters: Counters,
    /// The bucket's import; `None` for a backup target, which imports
    /// nothing, so every key counts as imported.
    import: Option<Arc<ImportState>>,
    conflict_policy: ConflictPolicy,
    copy_sources: CopySources,
    /// The destination bucket, if the target's bucket is a bucket name.
    bucket: Option<BucketName>,
}

impl<S> Clone for TargetParts<S> {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            prefix: self.prefix.clone(),
            cluster: self.cluster.clone(),
            settings: self.settings.clone(),
            wall: Arc::clone(&self.wall),
            counters: self.counters.clone(),
            import: self.import.clone(),
            conflict_policy: self.conflict_policy,
            copy_sources: self.copy_sources.clone(),
            bucket: self.bucket.clone(),
        }
    }
}

impl<S> TargetParts<S> {
    /// The target, honoring `writes`, over the native protocol if `native`
    /// is given.
    fn build(self, writes: ConditionalWrites, native: Option<Native>) -> Target<S> {
        let target = Target::new(self.store, self.prefix, writes, self.cluster, self.settings)
            .with_wall_clock(self.wall)
            .with_counters(self.counters)
            .with_conflict_policy(self.conflict_policy)
            .with_copy_sources(self.copy_sources)
            .with_native(native);
        match self.import {
            Some(import) => target.with_import(import),
            None => target,
        }
    }
}

/// A finished probe that found every precondition honored.
fn done() -> ProbeStatus {
    ProbeStatus::Done {
        unprotected: Vec::new(),
        server_side_copy: false,
    }
}

/// Once `quarantine` passed, aborts the uploads the flushes to `target`,
/// a SkyS3 peer's S3 endpoint, leave open, for as long as it is used.
async fn after_quarantine<S: ObjectStore>(target: Arc<Target<S>>, quarantine: Duration) {
    tokio::time::sleep(quarantine).await;
    abort_orphans(&target).await;
}

/// What a SkyS3 peer honors: every precondition of every write, which the
/// destination evaluates in its own shard log (§7.8). Copies are sent as
/// objects: a `COMMIT` has no copy.
fn native_writes() -> ConditionalWrites {
    let honored = OperationSupport {
        if_none_match: Some(PreconditionSupport::Honored),
        if_match: PreconditionSupport::Honored,
    };
    ConditionalWrites {
        put_object: honored,
        complete_multipart_upload: honored,
        delete_object: honored,
        copy_object: CopySupport::NONE,
    }
}

/// Aborts the remote uploads that flushes left open on `target`
/// ([`Target::orphaned_uploads`]) for as long as the target is used, after
/// a backoff that grows while some stay open. Each multipart flush aborts
/// some before it opens its own; this ends those that no later multipart
/// flush here would reach, as when the key's next version is a single
/// `PUT` or the node no longer leads the shard.
async fn abort_orphans<S: ObjectStore>(target: &Target<S>) {
    let mut left = 0;
    loop {
        tokio::time::sleep(target.settings.backoff(left + 1)).await;
        target.abort_orphaned_uploads().await;
        left = if target.orphaned_uploads() == 0 {
            0
        } else {
            left + 1
        };
    }
}

/// Probes the target's writes until a run succeeds, backing off between
/// runs, and then makes the target ready, without server-side copies.
/// Then it probes copies alone the same way, and lets the target copy
/// server-side once a run finds it supports them: a flaky store fails the
/// copy steps' many requests more often, and flushing need not wait.
/// Meanwhile, and for as long as the target is used, it aborts the uploads
/// flushes leave open.
async fn run_probe<S: ObjectStore>(
    parts: TargetParts<S>,
    nonces: Option<Arc<AtomicU64>>,
    status: Arc<Mutex<ProbeStatus>>,
    ready: Arc<Ready<S>>,
) {
    let prefix = parts.prefix.clone();
    let settings = parts.settings.clone();
    // Probes are not flushes: they go to the store directly, outside the
    // target's window (§7.7).
    let store = Arc::clone(&parts.store);
    // A fresh nonce per run: a failed run may leave scratch keys behind,
    // which would fail the next run's `If-None-Match: *` that must hold.
    let probe = || match &nonces {
        Some(next) => ConditionalProbe::new(&prefix, next.fetch_add(1, Ordering::Relaxed)),
        None => ConditionalProbe::with_fresh_nonce(&prefix),
    };
    let mut failures = 0;
    let target = loop {
        match probe().run_writes(&*parts.store).await {
            Ok(found) => {
                let unprotected = found.unprotected();
                if !unprotected.is_empty() {
                    tracing::warn!(
                        ?unprotected,
                        "the target does not honor every precondition; \
                        these operations are sent unconditionally (§7.2)"
                    );
                }
                *lock(&status) = ProbeStatus::Done {
                    unprotected,
                    server_side_copy: false,
                };
                let target = Arc::new(parts.build(found, None));
                ready.set(Arc::clone(&target));
                break target;
            }
            Err(error) => {
                failures += 1;
                tracing::warn!(%error, "the target's capability probe failed");
                *lock(&status) = ProbeStatus::Running {
                    error: Some(error.to_string()),
                };
                tokio::time::sleep(settings.backoff(failures)).await;
            }
        }
    };
    let copies = async {
        let mut failures = 0;
        let support = loop {
            match probe().run_copies(&*store).await {
                Ok(support) => break support,
                Err(error) => {
                    failures += 1;
                    tracing::warn!(%error, "the target's server-side copy probe failed");
                    tokio::time::sleep(settings.backoff(failures)).await;
                }
            }
        };
        target.set_copy_support(support);
        let usable = support.is_usable();
        if !usable {
            tracing::info!(
                copy = %support,
                "the target cannot take server-side copies; copies are uploaded (§11)"
            );
        }
        if let ProbeStatus::Done {
            server_side_copy, ..
        } = &mut *lock(&status)
        {
            *server_side_copy = usable;
        }
    };
    tokio::join!(copies, abort_orphans(&target));
}
