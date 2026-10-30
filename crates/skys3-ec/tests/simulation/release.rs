//! Simulation of a coded read that holds an old plan across the release
//! of its fragments and the compaction of their segments (plan M5-07,
//! §8.7).
//!
//! Each seed codes an object over six fragment nodes, seals the segments
//! that hold its fragments, and takes a read plan of a random range. Then,
//! on the simulated clock, the object is overwritten or deleted, the shard
//! primary's releaser tells the nodes every ten seconds, each node sweeps
//! every five seconds with a compaction threshold of one, and the read
//! starts within the release delay and streams to a consumer that pauses
//! up to 45 seconds between pieces. On half of the seeds, the holds sent to
//! one to four nodes are lost for a minute. Every fragment message takes a random
//! latency.
//!
//! The read must deliver only bytes of the old version, in order; it must
//! deliver all of them unless holds were lost, and otherwise fail cleanly.
//! In the end, every released fragment must be reclaimed, the shard's
//! index of released fragments empty, every sealed segment free of dead
//! bytes, and every other fragment intact.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use rand::Rng;
use skys3_config::{EcConfig, FailureDomain};
use skys3_coord::{Candidate, FragmentPlanner, GeometryPolicy, NodeState, Topology};
use skys3_ec::fragment::FragmentHeader;
use skys3_ec::read::{HoldFuture, ReadFuture};
use skys3_ec::release::HoldsBug;
use skys3_ec::{
    CodedRead, Encoder, EncoderSettings, FragmentHolds, FragmentId, FragmentLocation,
    FragmentReadError, FragmentRequest, FragmentServer, FragmentSource, FragmentStore,
    FragmentStoreConfig, FragmentWriter, HoldSettings, PlannerSource, ReleaseSettings, ReleaseSink,
    ReleasedFragment, Releaser, TransferError, read_coded,
};
use skys3_index::{Index, IndexConfig};
use skys3_io::{BlockingPool, Clock, ManualWallClock, MonotonicClock, SimDisk, SimMount};
use skys3_log::record::{Delete, Extent, Put, PutData, ShardRef};
use skys3_log::{LogConfig, RecordBody, SegmentLog};
use skys3_shard::Shard;
use skys3_sim::SimContext;
use skys3_types::{BucketId, ETag, Epoch, NodeId, ProposalId, ShardConfig, ShardId};
use tokio::time::Instant;

use crate::support::{fragment, sample};

/// Fragment nodes `n1` to `n6`; the shard's only replica is `n0`.
const NODES: usize = 6;
/// `fragment_release_delay_seconds` and `read_registration_ttl_seconds`.
const DELAY: Duration = Duration::from_secs(60);
const TTL: Duration = Duration::from_secs(30);
/// `read_registration_renew_interval_seconds`.
const RENEW: Duration = Duration::from_secs(10);
/// How often each node sweeps its released fragments.
const SWEEP: Duration = Duration::from_secs(5);
/// Bytes of data per stripe.
const STRIPE: u64 = 128 * 1024;

fn node(n: usize) -> NodeId {
    format!("n{n}").parse().unwrap()
}

/// The gateway that reads.
fn reader() -> NodeId {
    node(9)
}

fn paused_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap()
}

/// Segments of 64 KiB: each node's fragments of the object span several.
fn store_config() -> FragmentStoreConfig {
    FragmentStoreConfig {
        disk: 0,
        segment_bytes: 64 * 1024,
        group_commit_max_bytes: 64 * 1024,
        max_fragment_bytes: 1 << 20,
    }
}

/// A seed's draw.
#[derive(Clone, Debug)]
pub(crate) struct Plan {
    disk_seed: u64,
    size: u64,
    /// Whether the old version is deleted rather than overwritten.
    delete: bool,
    /// When it is replaced.
    replace_at: Duration,
    /// When the read starts, within the release delay.
    read_at: Duration,
    range: Range<u64>,
    /// The consumer's pause after each piece, in turn.
    pauses: Vec<Duration>,
    latency_seed: u64,
    /// The nodes whose holds are lost, and when.
    lost_holds: Option<(BTreeSet<NodeId>, Range<Duration>)>,
}

fn seconds(context: &mut SimContext, range: Range<u64>) -> Duration {
    Duration::from_millis(
        context
            .rng()
            .random_range(range.start * 1000..range.end * 1000),
    )
}

impl Plan {
    pub(crate) fn draw(context: &mut SimContext) -> Self {
        let disk_seed = context.fork_seed();
        let size = context.rng().random_range(200_000..600_000);
        // At least half of the object: a few pieces.
        let start = context.rng().random_range(0..size / 2);
        let end = context.rng().random_range(size / 2 + 1..=size);
        let delete = context.rng().random_bool(0.5);
        let replace_at = seconds(context, 0..20);
        let read_at = seconds(context, 0..50);
        let pauses = (0..4).map(|_| seconds(context, 0..45)).collect();
        let latency_seed = context.fork_seed() | 1;
        // One to four nodes: more than a stripe's parity may lose its
        // holds, and its read then fails.
        let lost_holds = context.rng().random_bool(0.5).then(|| {
            let count = context.rng().random_range(1..=4);
            let mut lost = BTreeSet::new();
            while lost.len() < count {
                lost.insert(node(context.rng().random_range(1..=NODES)));
            }
            let from = seconds(context, 30..90);
            (lost, from..from + Duration::from_secs(60))
        });
        Self {
            disk_seed,
            size,
            delete,
            replace_at,
            read_at,
            range: start..end,
            pauses,
            latency_seed,
            lost_holds,
        }
    }

    /// The same draw without lost holds.
    fn faultless(mut self) -> Self {
        self.lost_holds = None;
        self
    }
}

/// The fragment nodes, reached in memory with a random latency.
struct Nodes {
    servers: BTreeMap<NodeId, FragmentServer<SimMount>>,
    holds: BTreeMap<NodeId, FragmentHolds>,
    latency: Mutex<u64>,
    registrations: AtomicU64,
    lost_holds: Option<(BTreeSet<NodeId>, Range<Duration>)>,
    start: Instant,
}

impl std::fmt::Debug for Nodes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Nodes").finish_non_exhaustive()
    }
}

impl Nodes {
    fn store(&self, node: &NodeId) -> Option<&FragmentStore<SimMount>> {
        self.servers.get(node)?.stores().first()
    }

    /// Waits for a message's latency: up to 200 ms.
    async fn latency(&self) {
        let millis = {
            let mut state = self.latency.lock().unwrap();
            *state ^= *state << 13;
            *state ^= *state >> 7;
            *state ^= *state << 17;
            *state % 200
        };
        tokio::time::sleep(Duration::from_millis(millis)).await;
    }

    fn holds_lost(&self, node: &NodeId) -> bool {
        let now = self.start.elapsed();
        self.lost_holds
            .as_ref()
            .is_some_and(|(lost, window)| lost.contains(node) && window.contains(&now))
    }
}

impl FragmentWriter for Nodes {
    async fn write(
        &self,
        node: &NodeId,
        header: &FragmentHeader,
        data: Bytes,
    ) -> Result<FragmentId, TransferError> {
        let store = self
            .store(node)
            .ok_or_else(|| TransferError::NoAnswer(node.clone()))?;
        store
            .write(header, data)
            .await
            .map_err(|_| TransferError::NoAnswer(node.clone()))
    }
}

impl FragmentSource for Nodes {
    fn read(&self, request: FragmentRequest) -> ReadFuture<'_> {
        Box::pin(async move {
            self.latency().await;
            let Some(server) = self.servers.get(&request.node) else {
                return Err(FragmentReadError::Unreachable {
                    node: request.node.clone(),
                    reason: "no such node".to_owned(),
                });
            };
            server.read_fragment(&request).await
        })
    }

    fn register(&self) -> Option<(u64, Duration)> {
        Some((self.registrations.fetch_add(1, Ordering::Relaxed), RENEW))
    }

    fn hold(
        &self,
        node: NodeId,
        registration: u64,
        fragments: Vec<FragmentId>,
        end: bool,
    ) -> HoldFuture<'_> {
        Box::pin(async move {
            self.latency().await;
            if self.holds_lost(&node) {
                return Err("the hold was lost".to_owned());
            }
            let server = self.servers.get(&node).ok_or("no such node")?;
            server
                .hold(&reader(), registration, &fragments, end)
                .map(|_| ())
        })
    }
}

impl ReleaseSink for Nodes {
    async fn release(
        &self,
        node: &NodeId,
        shard: &ShardRef,
        fragments: &[ReleasedFragment],
    ) -> Result<(), String> {
        self.latency().await;
        let server = self.servers.get(node).ok_or("no such node")?;
        server.release(shard, fragments).await.map(|_| ())
    }
}

/// A planner over the six fragment nodes, each a domain of its own.
fn planner() -> PlannerSource {
    Arc::new(|| {
        let candidates = (1..=NODES).map(|n| Candidate {
            node: node(n),
            zone: None,
            rack: None,
            capacity_bytes: 1 << 40,
            state: NodeState::Live,
            shards: 0,
            primaries: 0,
        });
        FragmentPlanner::new(
            Topology::new(FailureDomain::Node, candidates),
            GeometryPolicy::from_config(&EcConfig::default()).unwrap(),
        )
    })
}

/// The shard, alone on node `n0`, with an inline index pool so the
/// simulated clock never runs ahead of the index.
async fn alone(disk: &SimDisk) -> Shard<SimMount> {
    let config = LogConfig {
        inline_max_bytes: 512,
        segment_bytes: 1 << 20,
        group_commit_max_delay: Duration::ZERO,
        group_commit_max_bytes: 256 * 1024,
    };
    let clock = Arc::new(MonotonicClock::new());
    let (log, _) = SegmentLog::open(disk.mount(), config, clock).await.unwrap();
    let index = Index::open_sim(&disk.mount(), "index.redb", &IndexConfig::default()).unwrap();
    let shard = ShardRef::new(BucketId::new("b-release").unwrap(), ShardId::new(0));
    let config = ShardConfig {
        bucket_id: shard.bucket.clone(),
        shard: shard.shard,
        epoch: Epoch::new(1),
        primary: node(0),
        members: vec![node(0)],
        learners: Vec::new(),
        min_write_replicas: 1,
        replicas: 1,
        proposal_id: ProposalId::new("p-1").unwrap(),
    };
    Shard::open(&config, log, Arc::new(index), BlockingPool::inline("index"))
        .await
        .unwrap()
}

/// Puts `data` at `key`, in extents of at most 64 KiB.
async fn put(shard: &Shard<SimMount>, key: &str, data: &[u8], tag: u64) {
    let mut extents = Vec::new();
    for (n, chunk) in data.chunks(64 * 1024).enumerate() {
        let extent = Extent {
            key: key.to_owned(),
            offset: n as u64 * 64 * 1024,
            data: Bytes::copy_from_slice(chunk),
        };
        extents.push(shard.append_extent(extent).await.unwrap());
    }
    let body = RecordBody::Put(Put {
        key: key.to_owned(),
        size: data.len() as u64,
        last_modified_ms: 1,
        etag: ETag::new(format!("{tag:032x}")).unwrap(),
        inherited_identity: None,
        metadata: BTreeMap::new(),
        tags: BTreeMap::new(),
        checksums: BTreeMap::new(),
        copy_source: None,
        data: PutData::Extents(extents),
    });
    shard.commit(body).await.unwrap();
}

/// What a run saw: for replay checks and the seeded-bug test.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Outcome {
    /// The bytes the read delivered.
    pub(crate) delivered: usize,
    /// Why it stopped early, if it did.
    pub(crate) failure: Option<String>,
    /// The released fragments.
    pub(crate) released: usize,
}

/// Runs `plan` with `bug` seeded in every node's holds.
pub(crate) fn run(plan: &Plan, bug: Option<HoldsBug>) -> Result<Outcome, String> {
    paused_runtime().block_on(async {
        let start = Instant::now();
        let mut servers = BTreeMap::new();
        let mut holds = BTreeMap::new();
        for n in 1..=NODES {
            let disk = SimDisk::new(plan.disk_seed.wrapping_add(n as u64));
            let (store, _) = FragmentStore::open(disk.mount(), store_config())
                .await
                .unwrap();
            let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::new());
            let settings = HoldSettings {
                release_delay: DELAY,
                registration_ttl: TTL,
            };
            let node_holds = FragmentHolds::new(clock, settings).with_bug(bug);
            let server = FragmentServer::new(vec![store]).with_holds(node_holds.clone());
            servers.insert(node(n), server);
            holds.insert(node(n), node_holds);
        }
        let nodes = Arc::new(Nodes {
            servers,
            holds,
            latency: Mutex::new(plan.latency_seed),
            registrations: AtomicU64::new(1),
            lost_holds: plan.lost_holds.clone(),
            start,
        });

        // The old version, coded.
        let disk = SimDisk::new(plan.disk_seed);
        let shard = alone(&disk).await;
        let data = Bytes::from(sample(plan.size as usize, plan.disk_seed));
        put(&shard, "k", &data, 1).await;
        let settings = EncoderSettings {
            min_object_bytes: 1000,
            stripe_data_bytes: STRIPE,
            after: Duration::ZERO,
            replans: 3,
            after_backup: false,
        };
        let encoder = Encoder::new(
            shard.clone(),
            Arc::clone(&nodes),
            planner(),
            Arc::new(ManualWallClock::new(Duration::from_secs(1000))),
            settings,
        );
        encoder
            .encode("k")
            .await
            .map_err(|error| format!("encoding failed: {error}"))?;
        // Other fragments seal the segments that hold the object's.
        let mut others = Vec::new();
        for (n, server) in &nodes.servers {
            for i in 0..2 {
                let (header, payload) = fragment(&format!("other/{n}/{i}"), 48 * 1024, i);
                let id = server.stores()[0]
                    .write(&header, payload.clone())
                    .await
                    .unwrap();
                others.push((n.clone(), id, header, payload));
            }
        }

        // The plan, taken before the old version is replaced.
        let entry = shard.entry("k").await.unwrap().unwrap();
        let object = entry.object.ok_or("no object")?;
        let coded = object.coded.ok_or("the object was not coded")?;
        let old: BTreeSet<FragmentLocation> = coded
            .stripes
            .iter()
            .flat_map(|stripe| stripe.fragments().iter().cloned())
            .collect();
        let read = CodedRead {
            shard: shard.shard().clone(),
            key: "k".to_owned(),
            version: coded.version,
            etag: object.local_etag.clone(),
            size: object.size,
            stripes: coded.stripes.clone(),
            range: plan.range.clone(),
        };

        // The background: the replacement, the releaser, and the sweeps.
        let mut background = Vec::new();
        let replacing = shard.clone();
        let (delete, replace_at) = (plan.delete, plan.replace_at);
        background.push(tokio::spawn(async move {
            tokio::time::sleep(replace_at).await;
            if delete {
                let body = RecordBody::Delete(Delete { key: "k".into() });
                replacing.commit(body).await.unwrap();
            } else {
                put(&replacing, "k", &sample(100, 2), 2).await;
            }
        }));
        let releaser = Releaser::new(
            shard.clone(),
            Arc::clone(&nodes),
            planner(),
            BlockingPool::inline("release"),
            ReleaseSettings::default(),
        );
        background.push(tokio::spawn(releaser.run()));
        for (n, node_holds) in &nodes.holds {
            let stores = nodes.servers[n].stores().to_vec();
            background.push(tokio::spawn(node_holds.clone().run(stores, SWEEP, 1.0)));
        }

        // The read, holding the old plan.
        tokio::time::sleep(plan.read_at).await;
        let expected = data.slice(plan.range.start as usize..plan.range.end as usize);
        let mut delivered = Vec::new();
        let mut failure = None;
        match read_coded(Arc::clone(&nodes) as Arc<dyn FragmentSource>, read).await {
            Err(error) => failure = Some(error.to_string()),
            Ok(mut body) => {
                let mut pauses = plan.pauses.iter().cycle();
                while let Some(piece) = body.recv().await {
                    match piece {
                        Ok(piece) => delivered.extend_from_slice(&piece),
                        Err(error) => {
                            failure = Some(error.to_string());
                            break;
                        }
                    }
                    tokio::time::sleep(*pauses.next().unwrap()).await;
                }
            }
        }
        if expected[..delivered.len().min(expected.len())] != delivered[..] {
            return Err(format!(
                "the read delivered {} bytes that are not the old version's",
                delivered.len()
            ));
        }
        if failure.is_none() && delivered.len() != expected.len() {
            return Err(format!(
                "the read ended after {} of {} bytes without an error",
                delivered.len(),
                expected.len()
            ));
        }
        if plan.lost_holds.is_none()
            && let Some(failure) = &failure
        {
            return Err(format!(
                "a read that held its fragments failed after {} bytes: {failure}",
                delivered.len()
            ));
        }

        // Long after: everything released is gone, and compacted away.
        tokio::time::sleep(DELAY + TTL + Duration::from_secs(60)).await;
        for task in background {
            task.abort();
        }
        let pending = shard
            .index()
            .read()
            .unwrap()
            .released(shard.shard(), None, usize::MAX)
            .unwrap()
            .len();
        if pending > 0 {
            return Err(format!("{pending} fragments are still to be released"));
        }
        for location in &old {
            let store = nodes.store(&location.node).unwrap();
            if store.len(location.fragment).is_some() {
                return Err(format!("{location:?} was never reclaimed"));
            }
            if nodes.holds[&location.node].is_released(location.fragment) {
                return Err(format!("{location:?} is still noted as released"));
            }
        }
        for (n, server) in &nodes.servers {
            let store = &server.stores()[0];
            let segments = store.segments();
            let sealed: u64 = segments
                .iter()
                .take(segments.len().saturating_sub(1))
                .map(|segment| segment.len)
                .sum();
            if sealed > store.live_bytes() {
                return Err(format!(
                    "{n} keeps {sealed} bytes of sealed segments for {} live bytes",
                    store.live_bytes()
                ));
            }
        }
        for (n, id, header, payload) in &others {
            let store = nodes.store(n).unwrap();
            let read = store
                .read(*id, 0..payload.len() as u64)
                .await
                .map_err(|error| format!("{n}'s fragment {id} was lost: {error}"))?;
            if read.header != *header || read.data != *payload {
                return Err(format!("{n}'s fragment {id} reads back wrong"));
            }
        }
        Ok(Outcome {
            delivered: delivered.len(),
            failure,
            released: old.len(),
        })
    })
}

/// Runs the draw of `context`.
pub(crate) fn scenario(context: &mut SimContext, bug: Option<HoldsBug>) -> Result<(), String> {
    let mut plan = Plan::draw(context);
    if bug.is_some() {
        plan = plan.faultless();
    }
    run(&plan, bug).map(|_| ())
}
