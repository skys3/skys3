//! Restore drills of a shard whose members are all lost (plan M5-11,
//! design §6.9, §8.4, §8.9).
//!
//! # The cluster
//!
//! Eight nodes, `n0` to `n7`, each with a fragment store on a disk of its
//! own. One shard of a `local` bucket without a backup has its only
//! member on `n0`: the drill's subject is what the shard's index and the
//! fragments say, not replication, so one member stands for all. The
//! member runs the real encoder, repairer, orphan judge, and index
//! snapshot writer (a snapshot every second to a simulated store), and
//! every node a real orphan reclaimer. Stripes are 3+2, so losing the
//! member with up to two other nodes leaves some stripes with fewer than
//! `k` fragments.
//!
//! # The workload
//!
//! One client, step by step: it writes small objects, which stay
//! replicated, and large ones, which the encoder codes when a step asks;
//! overwrites, deletes (with the `FLUSHED` that removes a tombstone of a
//! bucket without a backup), and retags them; damages a fragment, or
//! silences a holder, and lets the repairer rebuild what was lost, also
//! right after a retag, so that a repair's headers hold the new tags;
//! sweeps every node's reclaimer; and lets time pass, so that snapshots
//! are taken between the steps. Every write is acknowledged before the
//! next step, at a known time on the nodes' clock.
//!
//! # The drill
//!
//! At drawn steps the driver assumes the member lost, with up to two other
//! nodes and their fragments, right after writing a large object and
//! coding it: it forks the snapshot store and runs the restore drill
//! (`skys3_flush::snapshot::drill`) against the fragment stores of the
//! other nodes, then lets the workload go on, as the snapshot simulation
//! of plan M4-11 does. The drill's result must match the history:
//!
//! - **Restored objects** read back, through `read_coded` over the
//!   surviving nodes, with the bytes their ETag was written with; are not
//!   older than the key's state at the snapshot (no delete or overwrite
//!   before it is undone); are not older than a version written after the
//!   snapshot whose fragments survive; and have the tags of the newest
//!   evidence: the key's tags at the snapshot, or the tags in the
//!   surviving header of the attempt that read the latest retag.
//! - **A key whose last write is a coded version** that keeps `k`
//!   fragments of each stripe on the surviving nodes is restored with it,
//!   however recently it was written.
//! - **Keys not written since the snapshot** (the report's window): a
//!   deleted key is neither restored nor lost; a replicated one is lost
//!   with its ETag; a coded one that cannot be rebuilt is lost as coded.
//! - **The restored index** installs as a learner installs a snapshot,
//!   and reads back the same entries.
//!
//! Seeded bugs of re-indexing ([`ReindexBug`]) show the drill catches
//! them.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::ops::Range;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use rand::rngs::StdRng;
use rand::seq::{IndexedRandom, SliceRandom};
use rand::{Rng, SeedableRng};
use skys3_config::{Config, EcConfig, FailureDomain};
use skys3_coord::{Candidate, FragmentPlanner, GeometryPolicy, NodeState, Topology};
use skys3_ec::fragment::FragmentHeader;
use skys3_ec::orphans::Unanswered;
use skys3_ec::read::ReadFuture;
use skys3_ec::reindex::seeded::{ReindexBug, seed_reindex_bug};
use skys3_ec::{
    Attempts, CodedRead, EncoderSettings, FoundFragment, FragmentBytes, FragmentId,
    FragmentReadError, FragmentRequest, FragmentServer, FragmentSource, FragmentStore,
    FragmentStoreConfig, FragmentWriter, OrphanConfirmer, OrphanJudge, OrphanReclaimer,
    PlannerSource, RepairSettings, Repairer, Suspect, TransferError, Verdict, read_coded,
};
use skys3_flush::snapshot::{Drill, DrillRequest, SnapshotService, drill};
use skys3_index::{Index, IndexConfig};
use skys3_io::{BlockingPool, Clock, MonotonicClock, SimMount, WallClock};
use skys3_log::record::{Delete, Extent, Flushed, Put, PutData, TagSet, Tags};
use skys3_log::{LogConfig, RecordBody, SegmentLog, ShardRef};
use skys3_shard::{Shard, ShardSet};
use skys3_sim::s3::SimS3Config;
use skys3_sim::{SimContext, SimS3};
use skys3_types::{
    BucketDocument, BucketId, BucketMode, BucketName, ETag, Epoch, EpochSeq, NodeId, ProposalId,
    Seq, ShardConfig, ShardCount, ShardId,
};

/// The nodes.
const NODES: usize = 8;
/// Where snapshots go in the snapshot store.
const SNAPSHOTS: &str = "snaps/";
/// Objects of this size or more are coded.
const MIN_CODED: usize = 1024;
/// The data of one stripe.
const STRIPE: u64 = 4096;
/// How often the member takes an index snapshot.
const SNAPSHOT_INTERVAL: Duration = Duration::from_secs(1);
/// How long a silent holder must stay silent before repair rebuilds its
/// fragments.
const LOST_AFTER: Duration = Duration::from_millis(200);

/// A restore drill run.
#[derive(Clone, Debug)]
pub struct DrillConfig {
    /// Workload steps.
    pub steps: usize,
    /// The keys written.
    pub keys: usize,
    /// How many times the drill assumes the shard lost.
    pub losses: usize,
    /// How old a fragment must be before its node asks the primary about
    /// it (`fragment_orphan_after_seconds`).
    pub orphan_after: Duration,
    /// Whether each loss takes the nodes holding the object written right
    /// before it, enough of them that its first stripe cannot be rebuilt,
    /// preferring nodes that hold none of the key's previous version;
    /// otherwise up to two nodes besides the member, drawn at random.
    pub lose_latest: bool,
    /// A bug seeded into re-indexing.
    pub bug: Option<ReindexBug>,
}

impl Default for DrillConfig {
    fn default() -> Self {
        Self {
            steps: 160,
            keys: 16,
            losses: 4,
            orphan_after: Duration::from_secs(2),
            lose_latest: false,
            bug: None,
        }
    }
}

/// What the drills of a run restored and reported, summed over its losses.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DrillReport {
    /// The drills run.
    pub drills: usize,
    /// The drills that had a snapshot to start from.
    pub with_snapshot: usize,
    /// Coded objects restored.
    pub restored: usize,
    /// Coded objects restored whose version was written after the
    /// snapshot, from their fragment headers alone.
    pub written_after: usize,
    /// Coded objects restored with tags from a header later than the
    /// snapshot's entry.
    pub retags_from_headers: usize,
    /// Coded versions that could not be restored.
    pub unrecoverable: usize,
    /// Replicated objects reported lost.
    pub lost: usize,
    /// Relocations the repairer committed.
    pub repaired: usize,
    /// Fragments the reclaimers reclaimed.
    pub reclaimed: usize,
}

/// Runs one simulation of `config`.
///
/// # Errors
///
/// The first check that fails, or a failure of the simulation.
pub fn run(context: &mut SimContext, config: &DrillConfig) -> Result<DrillReport, String> {
    seed_reindex_bug(config.bug);
    let seeds: Vec<u64> = (0..=NODES).map(|_| context.fork_seed()).collect();
    let seed = context.fork_seed();
    let losses = loss_steps(context, config);
    let outcome: Arc<Mutex<Option<Result<DrillReport, String>>>> = Arc::default();
    let mut sim = context
        .builder()
        .simulation_duration(Duration::from_secs(3600))
        .build();
    let config = config.clone();
    let result = Arc::clone(&outcome);
    sim.client("drill", async move {
        let run = async {
            let mut world = World::open(&seeds, seed, &config).await?;
            for step in 0..config.steps {
                if losses.contains(&step) {
                    world.drill().await?;
                }
                world.step().await?;
            }
            world.service.shutdown().await;
            Ok::<_, String>(world.report)
        };
        *result.lock().unwrap_or_else(PoisonError::into_inner) = Some(run.await);
        Ok(())
    });
    let ran = sim.run();
    seed_reindex_bug(None);
    ran.map_err(|error| format!("the simulation failed: {error}"))?;
    let outcome = outcome
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take();
    outcome.unwrap_or_else(|| Err("the drill client did not finish".to_owned()))
}

/// The steps at which the drill assumes the shard lost: after the first
/// quarter, so that most have a snapshot.
fn loss_steps(context: &mut SimContext, config: &DrillConfig) -> BTreeSet<usize> {
    let from = config.steps / 4;
    let mut steps = BTreeSet::new();
    while steps.len() < config.losses.min(config.steps - from) {
        steps.insert(context.rng().random_range(from..config.steps));
    }
    steps
}

/// The nodes' wall clock: the simulation's time of day.
#[derive(Debug, Clone, Copy)]
struct SimWallClock;

impl WallClock for SimWallClock {
    fn now(&self) -> Duration {
        turmoil::since_epoch().unwrap_or_default()
    }
}

fn now_ms() -> u64 {
    u64::try_from(SimWallClock.now().as_millis()).unwrap_or(u64::MAX)
}

fn node(n: usize) -> NodeId {
    format!("n{n}").parse().expect("a node ID")
}

/// One write of the history, acknowledged at `acked_ms`.
#[derive(Clone, Debug)]
struct Done {
    position: EpochSeq,
    acked_ms: u64,
    write: Write,
}

#[derive(Clone, Debug)]
enum Write {
    Put {
        etag: ETag,
        data: Bytes,
        tags: TagSet,
    },
    Delete,
    Retag {
        tags: TagSet,
    },
}

/// A key's object as of some moment of its history: the `PUT` that wrote
/// it, its tags, and the position of the write that set them.
struct State<'a> {
    put: &'a Done,
    etag: &'a ETag,
    tags: &'a TagSet,
    tagged_at: EpochSeq,
}

/// The object `writes` leave at the end, or after the writes acknowledged
/// before `before_ms`: `None` if deleted or never written.
fn state(writes: &[Done], before_ms: Option<u64>) -> Option<State<'_>> {
    let mut state = None;
    for done in writes
        .iter()
        .filter(|done| before_ms.is_none_or(|ms| done.acked_ms < ms))
    {
        state = match (&done.write, state) {
            (Write::Put { etag, tags, .. }, _) => Some(State {
                put: done,
                etag,
                tags,
                tagged_at: done.position,
            }),
            (Write::Delete, _) => None,
            (Write::Retag { tags }, Some(state)) => Some(State {
                tags,
                tagged_at: done.position,
                ..state
            }),
            (Write::Retag { .. }, None) => None,
        };
    }
    state
}

/// The fragment nodes, in this process: every node's fragment server,
/// and the nodes that do not answer.
struct Holders {
    servers: BTreeMap<NodeId, FragmentServer<SimMount>>,
    silent: Mutex<BTreeSet<NodeId>>,
}

impl fmt::Debug for Holders {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Holders").finish_non_exhaustive()
    }
}

impl Holders {
    fn silent(&self) -> std::sync::MutexGuard<'_, BTreeSet<NodeId>> {
        self.silent.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn store(&self, node: &NodeId) -> Option<&FragmentStore<SimMount>> {
        self.servers.get(node)?.stores().first()
    }
}

impl FragmentWriter for Holders {
    async fn write(
        &self,
        holder: &NodeId,
        header: &FragmentHeader,
        data: Bytes,
    ) -> Result<FragmentId, TransferError> {
        let store = self
            .store(holder)
            .filter(|_| !self.silent().contains(holder))
            .ok_or_else(|| TransferError::NoAnswer(holder.clone()))?;
        store
            .write(header, data)
            .await
            .map_err(|_| TransferError::NoAnswer(holder.clone()))
    }
}

impl FragmentSource for Holders {
    fn read(&self, request: FragmentRequest) -> ReadFuture<'_> {
        Box::pin(async move {
            let node = request.node.clone();
            let Some(store) = self.store(&node).filter(|_| !self.silent().contains(&node)) else {
                return Err(FragmentReadError::Unreachable {
                    node,
                    reason: "silent".to_owned(),
                });
            };
            let read = store.read(request.fragment, request.range.clone()).await;
            match read {
                Ok(read) if request.identity.matches(&read.header) => Ok(FragmentBytes {
                    data: read.data,
                    crc32c: read.crc32c,
                }),
                Ok(_) => Err(FragmentReadError::NotHeld {
                    node,
                    reason: "another fragment".to_owned(),
                }),
                Err(error) => Err(FragmentReadError::NotHeld {
                    node,
                    reason: error.to_string(),
                }),
            }
        })
    }
}

/// A node's orphan queries, answered by the member's judge in this
/// process.
struct Confirmer {
    node: NodeId,
    judge: Arc<OrphanJudge<SimMount>>,
}

impl OrphanConfirmer for Confirmer {
    async fn confirm(
        &self,
        shard: &ShardRef,
        suspects: &[Suspect],
    ) -> Result<Vec<Verdict>, Unanswered> {
        self.judge
            .judge(&self.node, suspects)
            .await
            .map_err(|error| Unanswered {
                shard: shard.clone(),
                reason: error.to_string(),
            })
    }
}

/// The cluster, the client's history, and what the drills found.
struct World {
    rng: StdRng,
    config: DrillConfig,
    shard: Shard<SimMount>,
    shard_ref: ShardRef,
    _set: ShardSet<SimMount>,
    holders: Arc<Holders>,
    encoder: skys3_ec::Encoder<SimMount, Holders>,
    repairer: Repairer<SimMount, Holders>,
    reclaimers: Vec<OrphanReclaimer<SimMount, Confirmer>>,
    snapshots: SimS3,
    service: SnapshotService<SimS3, SimMount>,
    history: BTreeMap<String, Vec<Done>>,
    etags: u64,
    report: DrillReport,
}

impl World {
    async fn open(seeds: &[u64], seed: u64, config: &DrillConfig) -> Result<Self, String> {
        let shard_ref = ShardRef::new(
            BucketId::new("b-drill").map_err(|e| e.to_string())?,
            ShardId::new(0),
        );
        let shard_config = ShardConfig {
            bucket_id: shard_ref.bucket.clone(),
            shard: shard_ref.shard,
            epoch: Epoch::new(1),
            primary: node(0),
            members: vec![node(0)],
            learners: Vec::new(),
            min_write_replicas: 1,
            replicas: 1,
            proposal_id: ProposalId::new("p-drill").map_err(|e| e.to_string())?,
        };
        let disk = skys3_io::SimDisk::new(seeds[NODES]);
        let mount = disk.mount();
        let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::new());
        let log_config = LogConfig {
            inline_max_bytes: 512,
            segment_bytes: 1 << 20,
            group_commit_max_delay: Duration::ZERO,
            group_commit_max_bytes: 256 * 1024,
        };
        let (log, _) = SegmentLog::open(mount.clone(), log_config, Arc::clone(&clock))
            .await
            .map_err(|e| e.to_string())?;
        let index = Index::open_sim(&mount, "index.redb", &IndexConfig::default())
            .map_err(|e| e.to_string())?;
        let pool = BlockingPool::inline("index");
        let set = ShardSet::new(Arc::new(index), log, pool.clone());
        let shard = set.open(&shard_config).await.map_err(|e| e.to_string())?;

        let mut servers = BTreeMap::new();
        for (n, seed) in seeds.iter().take(NODES).enumerate() {
            let disk = skys3_io::SimDisk::new(*seed);
            let (store, _) = FragmentStore::open(disk.mount(), store_config())
                .await
                .map_err(|e| e.to_string())?;
            servers.insert(node(n), FragmentServer::new(vec![store]));
        }
        let holders = Arc::new(Holders {
            servers,
            silent: Mutex::default(),
        });
        let attempts = Attempts::default();
        let encoder = skys3_ec::Encoder::new(
            shard.clone(),
            Arc::clone(&holders),
            planner(),
            Arc::new(SimWallClock),
            EncoderSettings {
                min_object_bytes: MIN_CODED as u64,
                stripe_data_bytes: STRIPE,
                after: Duration::ZERO,
                replans: 3,
                after_backup: false,
            },
        )
        .with_attempts(attempts.clone());
        let repairer = Repairer::new(
            shard.clone(),
            Arc::clone(&holders) as Arc<dyn FragmentSource>,
            Arc::clone(&holders),
            planner(),
            pool.clone(),
            RepairSettings {
                interval: Duration::from_millis(50),
                lost_after: LOST_AFTER,
                checks_per_node: 1 << 16,
                replans: 3,
                bytes_per_second: 1 << 40,
                // Repairs only: fragment moves (M5-09) are proved in the
                // coding harness, and here would only reshuffle the
                // fragments each drill must find.
                moves_per_pass: 0,
            },
        )
        .with_attempts(attempts.clone());
        let judge = Arc::new(OrphanJudge::new(shard.clone(), attempts, pool));
        let reclaimers = holders
            .servers
            .iter()
            .map(|(id, server)| {
                OrphanReclaimer::new(
                    id.clone(),
                    server.stores().to_vec(),
                    Confirmer {
                        node: id.clone(),
                        judge: Arc::clone(&judge),
                    },
                    Arc::clone(&clock),
                    config.orphan_after,
                )
            })
            .collect();

        let snapshots = SimS3::new(seed, SimS3Config::default());
        let settings: Config = "[cluster]\ncluster_id = \"c-drill\"\n\
             [control_store]\netcd_endpoints = [\"https://etcd.invalid:2379\"]\n\
             [buckets.drill]\nmode = \"local\"\n\
             index_snapshot_interval_seconds = 1\n\
             snapshot_target = \"https://s3.example/remote/snaps/\"\n"
            .parse()
            .map_err(|e: skys3_config::ConfigError| e.to_string())?;
        let store = snapshots.clone();
        let service =
            SnapshotService::new(Box::new(move |_| store.clone()), settings.buckets().clone())
                .with_wall_clock(Arc::new(SimWallClock));
        let document = BucketDocument {
            bucket_id: shard_ref.bucket.clone(),
            name: BucketName::new("drill").map_err(|e| e.to_string())?,
            mode: BucketMode::Local,
            shards: ShardCount::new(1).map_err(|e| e.to_string())?,
            replicas: 1,
            min_write_replicas: 1,
            clean_copies: 1,
            target: None,
            created_unix_ms: 0,
            lifecycle: None,
            proposal_id: ProposalId::new("p-drill").map_err(|e| e.to_string())?,
        };
        service
            .reconcile(std::slice::from_ref(&document), &set)
            .await;
        Ok(Self {
            rng: StdRng::seed_from_u64(seed),
            config: config.clone(),
            shard,
            shard_ref,
            _set: set,
            holders,
            encoder,
            repairer,
            reclaimers,
            snapshots,
            service,
            history: BTreeMap::new(),
            etags: 0,
            report: DrillReport::default(),
        })
    }

    fn key(&mut self) -> String {
        format!("k{:02}", self.rng.random_range(0..self.config.keys))
    }

    /// The keys whose history ends with an object.
    fn present(&self) -> Vec<String> {
        self.history
            .iter()
            .filter(|(_, writes)| state(writes, None).is_some())
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// One workload step.
    async fn step(&mut self) -> Result<(), String> {
        match self.rng.random_range(0..100) {
            0..34 => {
                let key = self.key();
                let large = self.rng.random_bool(0.7);
                self.put(&key, large).await
            }
            34..40 => self.delete().await,
            40..50 => self.retag().await.map(drop),
            50..62 => {
                self.scan().await;
                Ok(())
            }
            62..73 => {
                if let Some(key) = self.retag().await? {
                    self.damage(Some(&key)).await;
                }
                Ok(())
            }
            73..78 => {
                self.damage(None).await;
                Ok(())
            }
            78..83 => {
                self.silence().await;
                Ok(())
            }
            83..88 => {
                for reclaimer in &self.reclaimers {
                    self.report.reclaimed += reclaimer.sweep().await.reclaimed;
                }
                Ok(())
            }
            _ => {
                let pause = self.rng.random_range(20..400);
                tokio::time::sleep(Duration::from_millis(pause)).await;
                Ok(())
            }
        }
    }

    /// Writes `key`, coded later if `large`, and records it.
    async fn put(&mut self, key: &str, large: bool) -> Result<(), String> {
        let len = if large {
            self.rng.random_range(MIN_CODED..3 * STRIPE as usize)
        } else {
            self.rng.random_range(16..MIN_CODED)
        };
        let mut data = vec![0; len];
        self.rng.fill(&mut data[..]);
        let data = Bytes::from(data);
        self.etags += 1;
        let etag = ETag::new(format!("{:032x}", self.etags)).map_err(|e| e.to_string())?;
        let tags = self.tags();
        let payload = if len <= 512 {
            PutData::Inline(data.clone())
        } else {
            let mut extents = Vec::new();
            for (n, chunk) in data.chunks(4000).enumerate() {
                let extent = Extent {
                    key: key.to_owned(),
                    offset: n as u64 * 4000,
                    data: data.slice_ref(chunk),
                };
                let at = self.shard.append_extent(extent).await;
                extents.push(at.map_err(|e| e.to_string())?);
            }
            PutData::Extents(extents)
        };
        let body = RecordBody::Put(Put {
            key: key.to_owned(),
            size: len as u64,
            last_modified_ms: now_ms(),
            etag: etag.clone(),
            inherited_identity: None,
            metadata: BTreeMap::new(),
            tags: tags.clone(),
            checksums: BTreeMap::new(),
            copy_source: None,
            data: payload,
        });
        let position = self.commit(body).await?;
        self.record(key, position, Write::Put { etag, data, tags });
        Ok(())
    }

    async fn delete(&mut self) -> Result<(), String> {
        match self.present().choose(&mut self.rng).cloned() {
            Some(key) => self.delete_key(&key).await,
            None => Ok(()),
        }
    }

    /// Deletes `key`, which holds an object.
    async fn delete_key(&mut self, key: &str) -> Result<(), String> {
        let key = key.to_owned();
        let body = RecordBody::Delete(Delete { key: key.clone() });
        let position = self.commit(body).await?;
        // A `local` bucket without a backup removes the tombstone at once.
        let flushed = RecordBody::Flushed(Flushed {
            key: key.clone(),
            seq: position.seq,
            remote_etag: None,
            remote_version_id: None,
        });
        self.commit(flushed).await?;
        self.record(&key, position, Write::Delete);
        Ok(())
    }

    /// Retags a key that holds an object, and returns it.
    async fn retag(&mut self) -> Result<Option<String>, String> {
        let Some(key) = self.present().choose(&mut self.rng).cloned() else {
            return Ok(None);
        };
        self.retag_key(&key).await?;
        Ok(Some(key))
    }

    /// Retags `key`, which holds an object.
    async fn retag_key(&mut self, key: &str) -> Result<(), String> {
        let key = key.to_owned();
        let tags = self.tags();
        let body = RecordBody::Tags(Tags {
            key: key.clone(),
            tags: tags.clone(),
        });
        let position = self.commit(body).await?;
        self.record(&key, position, Write::Retag { tags });
        Ok(())
    }

    fn tags(&mut self) -> TagSet {
        BTreeMap::from([("v".to_owned(), self.rng.random_range(0..1000).to_string())])
    }

    async fn commit(&self, body: RecordBody) -> Result<EpochSeq, String> {
        let committed = self.shard.commit(body).await.map_err(|e| e.to_string())?;
        Ok(committed.position)
    }

    fn record(&mut self, key: &str, position: EpochSeq, write: Write) {
        let done = Done {
            position,
            acked_ms: now_ms(),
            write,
        };
        self.history.entry(key.to_owned()).or_default().push(done);
    }

    /// An encoder scan: codes every object that qualifies.
    async fn scan(&self) {
        for key in self.present() {
            self.encode(&key).await;
        }
    }

    async fn encode(&self, key: &str) {
        if let Err(error) = self.encoder.encode(key).await {
            tracing::debug!(key, %error, "an encoding failed");
        }
    }

    /// Reclaims a fragment of `key`'s coded layout, or of any, from its
    /// node, as a damaged disk would lose it, and repairs.
    async fn damage(&mut self, key: Option<&str>) {
        self.damage_some(key, 1).await;
    }

    /// Reclaims `count` fragments as [`World::damage`] does one, and
    /// repairs.
    async fn damage_some(&mut self, key: Option<&str>, count: usize) {
        let keys = match key {
            Some(key) => vec![key.to_owned()],
            None => self.present(),
        };
        let mut layouts = Vec::new();
        for key in keys {
            if let Ok(Some(entry)) = self.shard.entry(&key).await
                && let Some(coded) = entry.object.and_then(|object| object.coded)
            {
                layouts.extend(
                    coded
                        .stripes
                        .into_iter()
                        .flat_map(|s| s.fragments().to_vec()),
                );
            }
        }
        let chosen: Vec<_> = layouts
            .choose_multiple(&mut self.rng, count)
            .cloned()
            .collect();
        if chosen.is_empty() {
            return;
        }
        for location in chosen {
            if let Some(store) = self.holders.store(&location.node) {
                let _ = store.reclaim(location.fragment).await;
            }
        }
        for _ in 0..3 {
            let report = self.repairer.pass().await;
            self.report.repaired += report.repaired;
            if report.lost == 0 {
                break;
            }
        }
    }

    /// Silences a node long enough for repair to rebuild its fragments
    /// elsewhere; its own copies stay, as orphans.
    async fn silence(&mut self) {
        let silent = node(self.rng.random_range(1..NODES));
        self.holders.silent().insert(silent.clone());
        let report = self.repairer.pass().await;
        self.report.repaired += report.repaired;
        tokio::time::sleep(LOST_AFTER + Duration::from_millis(10)).await;
        let report = self.repairer.pass().await;
        self.report.repaired += report.repaired;
        self.holders.silent().remove(&silent);
    }

    /// The coded layout of `key`'s entry, if it has one.
    async fn layout(&self, key: &str) -> Option<skys3_index::Coded> {
        let entry = self.shard.entry(key).await.ok()??;
        entry.object?.coded
    }

    /// The keys whose entries are coded.
    async fn coded_keys(&self) -> Vec<String> {
        let mut coded = Vec::new();
        for key in self.present() {
            if self.layout(&key).await.is_some() {
                coded.push(key);
            }
        }
        coded
    }

    /// Assumes the member lost, with other nodes, runs the restore drill,
    /// and checks it against the history. Right before, the encoder scans
    /// the shard, and of four coded objects, if there are as many, one is
    /// retagged and another deleted
    /// before a snapshot is taken, so that the snapshot's tags are later
    /// than the headers' and the deleted version's fragments are orphans;
    /// then one is retagged and one of its fragments rebuilt, so that a
    /// repair's headers hold tags later than the snapshot's; and one is
    /// overwritten with a large object that is coded at once: a version
    /// written after the latest snapshot.
    async fn drill(&mut self) -> Result<(), String> {
        self.scan().await;
        let mut coded = self.coded_keys().await;
        coded.shuffle(&mut self.rng);
        let mut coded = coded.into_iter();
        let overwritten = coded.next();
        if let Some(key) = coded.next() {
            self.retag_key(&key).await?;
        }
        if let Some(key) = coded.next() {
            self.delete_key(&key).await?;
        }
        // A snapshot holds that retag and that delete.
        tokio::time::sleep(SNAPSHOT_INTERVAL + Duration::from_millis(100)).await;
        if let Some(key) = coded.next() {
            self.retag_key(&key).await?;
            self.damage_some(Some(&key), 2).await;
        }
        let key = match overwritten {
            Some(key) => key,
            None => self.key(),
        };
        let previous = self.layout(&key).await;
        self.put(&key, true).await?;
        self.encode(&key).await;
        let mut lost = BTreeSet::from([node(0)]);
        match self.layout(&key).await.filter(|_| self.config.lose_latest) {
            Some(latest) => lost.extend(holders_to_lose(&latest, previous.as_ref())),
            None => {
                for _ in 0..self.rng.random_range(0..=2) {
                    lost.insert(node(self.rng.random_range(1..NODES)));
                }
            }
        }
        let nodes: Vec<NodeId> = (0..NODES).map(node).collect();
        let snapshots = self.snapshots.fork();
        let request = DrillRequest {
            shard: &self.shard_ref,
            snapshots: &snapshots,
            prefix: SNAPSHOTS,
            home: None,
            nodes: &nodes,
            lost: &lost,
            until_ms: now_ms(),
        };
        let drilled = drill(request, &self.holders.servers)
            .await
            .map_err(|error| format!("the drill failed: {error}"))?;
        let mut survivors = Vec::new();
        for (id, server) in &self.holders.servers {
            if !lost.contains(id) {
                let found = server.found(id, &self.shard_ref).await;
                survivors.extend(found.map_err(|error| error.to_string())?);
            }
        }
        let audit = Audit {
            drill: &drilled,
            survivors: &survivors,
            lost: &lost,
        };
        let context =
            |error: String| format!("drill {} with {lost:?} lost: {error}", self.report.drills);
        for (key, writes) in &self.history {
            let live = self.shard.entry(key).await.map_err(|e| e.to_string())?;
            let coded = live.and_then(|entry| entry.object?.coded);
            audit
                .check(&self.shard_ref, &self.holders, key, writes, coded.as_ref())
                .await
                .map_err(context)?;
        }
        audit.check_install(&self.shard_ref).map_err(context)?;
        self.tally(&drilled, &survivors);
        Ok(())
    }

    fn tally(&mut self, drill: &Drill, survivors: &[FoundFragment]) {
        let report = &mut self.report;
        report.drills += 1;
        report.with_snapshot += usize::from(drill.report.snapshot.is_some());
        report.restored += drill.restored.len();
        report.unrecoverable += drill
            .report
            .lost
            .iter()
            .filter(|l| l.coded.is_some())
            .count();
        report.lost += drill
            .report
            .lost
            .iter()
            .filter(|l| l.coded.is_none())
            .count();
        let from_ms = drill.report.window.from_ms;
        for (key, entry) in &drill.index.entries {
            let writes = &self.history[key];
            let Some(object) = &entry.object else {
                continue;
            };
            let put = writes.iter().find(
                |done| matches!(&done.write, Write::Put { etag, .. } if *etag == object.local_etag),
            );
            if put.is_some_and(|put| from_ms.is_none_or(|from| put.acked_ms >= from)) {
                report.written_after += 1;
            }
            let at_snapshot = from_ms.and_then(|from| state(writes, Some(from)));
            let snapshot_tags = at_snapshot
                .filter(|state| *state.etag == object.local_etag)
                .map(|state| state.tagged_at);
            let from_header = survivors.iter().any(|found| {
                found.header.key == *key
                    && found.header.object.tags == object.tags
                    && snapshot_tags.is_none_or(|at| read_at(&found.header) > at)
            });
            if from_header && snapshot_tags.is_some() {
                report.retags_from_headers += 1;
            }
        }
    }
}

/// The holders of `latest`'s first stripe to lose with the member so that
/// the stripe keeps fewer than `k` fragments, chosen so that each stripe of
/// `previous`, the key's previous layout, loses as few as can be.
fn holders_to_lose(
    latest: &skys3_index::Coded,
    previous: Option<&skys3_index::Coded>,
) -> Vec<NodeId> {
    let stripe = &latest.stripes[0];
    let member = node(0);
    let holders: Vec<&NodeId> = stripe
        .fragments()
        .iter()
        .map(|location| &location.node)
        .filter(|holder| **holder != member)
        .collect();
    let needed = stripe.geometry().parity_fragments() + 1;
    let needed = needed - usize::from(holders.len() < stripe.fragments().len());
    let harm = |chosen: &[&NodeId]| {
        previous
            .iter()
            .flat_map(|coded| &coded.stripes)
            .map(|stripe| {
                stripe
                    .fragments()
                    .iter()
                    .filter(|l| l.node == member || chosen.contains(&&l.node))
                    .count()
            })
            .max()
            .unwrap_or(0)
    };
    let mut best: Option<(usize, Vec<&NodeId>)> = None;
    for mask in 0u32..(1 << holders.len()) {
        if mask.count_ones() as usize != needed {
            continue;
        }
        let chosen: Vec<&NodeId> = (0..holders.len())
            .filter(|bit| mask & (1 << bit) != 0)
            .map(|bit| holders[bit])
            .collect();
        let harmed = harm(&chosen);
        if best.as_ref().is_none_or(|(least, _)| harmed < *least) {
            best = Some((harmed, chosen));
        }
    }
    best.map(|(_, chosen)| chosen.into_iter().cloned().collect())
        .unwrap_or_default()
}

/// The position at which an attempt read the entry its header describes:
/// the version, or a later retag, which its write identity then names.
fn read_at(header: &FragmentHeader) -> EpochSeq {
    header.version.max(header.object.identity)
}

/// A drill's result, checked against the history.
struct Audit<'a> {
    drill: &'a Drill,
    survivors: &'a [FoundFragment],
    lost: &'a BTreeSet<NodeId>,
}

impl Audit<'_> {
    /// Checks what the drill says of `key`, whose history is `writes` and
    /// whose live entry has the coded layout `coded`, if any.
    async fn check(
        &self,
        shard: &ShardRef,
        holders: &Arc<Holders>,
        key: &str,
        writes: &[Done],
        coded: Option<&skys3_index::Coded>,
    ) -> Result<(), String> {
        let report = &self.drill.report;
        let restored = self.drill.index.entries.get(key);
        let lost = report.lost.iter().find(|lost| lost.key == key);
        let from_ms = report.window.from_ms;
        let at_snapshot = from_ms.and_then(|from| state(writes, Some(from)));
        let last = state(writes, None);
        let put_of = |etag: &ETag| {
            writes.iter().find(
                |done| matches!(&done.write, Write::Put { etag: written, .. } if written == etag),
            )
        };
        if let Some(entry) = restored {
            let object = entry.object.as_ref().ok_or("restored without an object")?;
            let put = put_of(&object.local_etag)
                .ok_or_else(|| format!("{key} is restored with an ETag never written"))?;
            let Write::Put { data, .. } = &put.write else {
                unreachable!("put_of finds PUTs")
            };
            self.read_back(shard, holders, key, entry, data).await?;
            // No delete or overwrite before the snapshot is undone.
            let before = from_ms.and_then(|from| {
                writes
                    .iter()
                    .filter(|done| {
                        done.acked_ms < from && !matches!(done.write, Write::Retag { .. })
                    })
                    .last()
            });
            if let Some(before) = before
                && before.position > put.position
            {
                return Err(format!(
                    "{key} is restored with the version at {}, older than its write at {} \
                     before the snapshot",
                    put.position, before.position
                ));
            }
            // Nor a version written after the snapshot whose fragments
            // survive.
            if let Some(newer) = self.newest_after_snapshot(key, writes)
                && newer.position > put.position
            {
                return Err(format!(
                    "{key} is restored with the version at {}, but the version at {} was \
                     written after the snapshot and its fragments survive",
                    put.position, newer.position
                ));
            }
            let expected = self.expected_tags(key, &object.local_etag, at_snapshot.as_ref());
            if Some(&object.tags) != expected {
                return Err(format!(
                    "{key} is restored with the tags {:?}, not {expected:?}",
                    object.tags
                ));
            }
        }
        // A last write coded, whose stripes each keep `k` fragments, is
        // restored however recently it was written.
        if let (Some(last), Some(coded)) = (&last, coded)
            && self.recoverable(key, last.etag, coded)
            && restored
                .and_then(|e| e.object.as_ref())
                .map(|o| &o.local_etag)
                != Some(last.etag)
        {
            return Err(format!(
                "{key}'s last version, coded and recoverable, is not restored: {restored:?}, \
                 reported lost: {lost:?}"
            ));
        }
        if let Some(lost) = lost
            && let Some(object) = &lost.object
            && put_of(&object.etag).is_none()
        {
            return Err(format!("{key} is reported lost with an ETag never written"));
        }
        let in_window = from_ms.is_none_or(|from| writes.iter().any(|d| d.acked_ms >= from));
        if in_window {
            return Ok(());
        }
        match (last, coded) {
            (None, _) => {
                if restored.is_some() || lost.is_some() {
                    return Err(format!(
                        "{key} was deleted before the snapshot, but is restored ({}) or \
                         reported lost ({lost:?})",
                        restored.is_some()
                    ));
                }
            }
            (Some(last), Some(coded)) if !self.recoverable(key, last.etag, coded) => {
                let reported = lost
                    .filter(|l| l.coded.is_some())
                    .and_then(|l| l.object.as_ref());
                if reported.map(|o| &o.etag) != Some(last.etag) || restored.is_some() {
                    return Err(format!(
                        "{key}'s coded version cannot be rebuilt and must be reported lost: \
                         {lost:?}, restored {}",
                        restored.is_some()
                    ));
                }
            }
            (Some(_), Some(_)) => {}
            (Some(last), None) => {
                // Lost as replicated, or as coded if an encoding left
                // headers that cannot be rebuilt.
                let reported = lost.and_then(|l| l.object.as_ref());
                if reported.map(|o| &o.etag) != Some(last.etag) || restored.is_some() {
                    return Err(format!(
                        "{key}'s replicated version {} must be reported lost: {lost:?}, \
                         restored {}",
                        last.put.position,
                        restored.is_some()
                    ));
                }
            }
        }
        Ok(())
    }

    /// Reads `entry`'s object back over the surviving nodes.
    async fn read_back(
        &self,
        shard: &ShardRef,
        holders: &Arc<Holders>,
        key: &str,
        entry: &skys3_index::Entry,
        data: &Bytes,
    ) -> Result<(), String> {
        let object = entry.object.as_ref().ok_or("no object")?;
        let coded = object.coded.as_ref().ok_or("restored without a layout")?;
        let survivors = Arc::new(Holders {
            servers: holders.servers.clone(),
            silent: Mutex::new(self.lost.clone()),
        });
        let read = CodedRead {
            shard: shard.clone(),
            key: key.to_owned(),
            version: coded.version,
            etag: object.local_etag.clone(),
            size: object.size,
            stripes: coded.stripes.clone(),
            range: Range {
                start: 0,
                end: object.size,
            },
        };
        let mut body = read_coded(survivors as Arc<dyn FragmentSource>, read)
            .await
            .map_err(|error| format!("{key} does not read back: {error}"))?;
        let mut bytes = Vec::new();
        while let Some(piece) = body.recv().await {
            bytes.extend_from_slice(&piece.map_err(|e| format!("{key} breaks off: {e}"))?);
        }
        if bytes != data[..] {
            return Err(format!(
                "{key} reads back other bytes than it was written with"
            ));
        }
        Ok(())
    }

    /// The latest `PUT` of `key` written after the snapshot's position
    /// whose version some surviving header names.
    fn newest_after_snapshot<'w>(&self, key: &str, writes: &'w [Done]) -> Option<&'w Done> {
        let position = self.drill.report.snapshot.map(|s| s.position);
        writes
            .iter()
            .filter(|done| position.is_none_or(|at| done.position > at))
            .filter(|done| match &done.write {
                Write::Put { etag, .. } => self
                    .survivors
                    .iter()
                    .any(|found| found.header.key == key && found.header.object.etag == *etag),
                _ => false,
            })
            .last()
    }

    /// The tags the newest evidence gives the object of `key` with `etag`:
    /// the key's state at the snapshot, if it is that object, or a
    /// surviving header of it read at a later retag.
    fn expected_tags<'s>(
        &'s self,
        key: &str,
        etag: &ETag,
        at_snapshot: Option<&'s State<'s>>,
    ) -> Option<&'s TagSet> {
        let mut best = at_snapshot
            .filter(|state| state.etag == etag)
            .map(|state| (state.tagged_at, state.tags));
        for found in self.survivors {
            let header = &found.header;
            if header.key == key && header.object.etag == *etag {
                let at = read_at(header);
                if best.is_none_or(|(known, _)| at > known) {
                    best = Some((at, &header.object.tags));
                }
            }
        }
        best.map(|(_, tags)| tags)
    }

    /// Whether every stripe of `coded`, the layout of `key`'s object with
    /// `etag`, keeps `k` distinct fragments on the surviving nodes.
    fn recoverable(&self, key: &str, etag: &ETag, coded: &skys3_index::Coded) -> bool {
        coded.stripes.iter().all(|stripe| {
            let mut copies = BTreeMap::<u8, BTreeSet<&NodeId>>::new();
            for found in self.survivors {
                let h = &found.header;
                if h.key == key
                    && h.version == coded.version
                    && h.object.etag == *etag
                    && h.stripe.number == stripe.number()
                    && h.stripe.offset == stripe.offset()
                    && h.stripe.data_len == stripe.data_len()
                {
                    copies
                        .entry(h.index)
                        .or_default()
                        .insert(&found.location.node);
                }
            }
            let copies: Vec<&BTreeSet<&NodeId>> = copies.values().collect();
            most_on_distinct_nodes(&copies, &mut BTreeSet::new())
                >= stripe.geometry().data_fragments()
        })
    }

    /// Installs the restored index as a learner installs a snapshot, on a
    /// new disk, and checks that it reads back the same entries.
    fn check_install(&self, shard: &ShardRef) -> Result<(), String> {
        let restored = &self.drill.index;
        let disk = skys3_io::SimDisk::new(0);
        let index = Index::open_sim(&disk.mount(), "index.redb", &IndexConfig::default())
            .map_err(|e| e.to_string())?;
        let config = ShardConfig {
            bucket_id: shard.bucket.clone(),
            shard: shard.shard,
            epoch: restored.epoch,
            primary: node(NODES),
            members: vec![node(NODES)],
            learners: Vec::new(),
            min_write_replicas: 1,
            replicas: 1,
            proposal_id: ProposalId::new("p-restored").map_err(|e| e.to_string())?,
        };
        let marker = EpochSeq::new(restored.epoch, Seq::MAX);
        index
            .begin_install(shard, marker)
            .map_err(|e| e.to_string())?;
        for (table, rows) in restored.rows().map_err(|e| e.to_string())? {
            index
                .install_rows(shard, table, &rows)
                .map_err(|e| format!("the restored rows do not install: {e}"))?;
        }
        index
            .finish_install(&config, restored.applied)
            .map_err(|e| e.to_string())?;
        let reader = index.read().map_err(|e| e.to_string())?;
        for (key, entry) in &restored.entries {
            let read = reader.entry(shard, key).map_err(|e| e.to_string())?;
            if read.as_ref() != Some(entry) {
                return Err(format!(
                    "{key}'s restored entry does not read back installed"
                ));
            }
        }
        if reader.applied(shard).map_err(|e| e.to_string())? != Some(restored.applied) {
            return Err("the restored index is not at its applied position".to_owned());
        }
        Ok(())
    }
}

/// The most of the fragments whose copies are on `copies` (one set of
/// nodes per fragment) that can be read from distinct nodes, none of
/// `used`: a search over every choice, which a stripe's few fragments
/// keep small.
fn most_on_distinct_nodes<'a>(
    copies: &[&BTreeSet<&'a NodeId>],
    used: &mut BTreeSet<&'a NodeId>,
) -> usize {
    let Some((first, rest)) = copies.split_first() else {
        return 0;
    };
    let mut best = most_on_distinct_nodes(rest, used);
    for node in first.iter() {
        if used.insert(node) {
            best = best.max(1 + most_on_distinct_nodes(rest, used));
            used.remove(node);
        }
    }
    best
}

fn store_config() -> FragmentStoreConfig {
    FragmentStoreConfig {
        disk: 0,
        segment_bytes: 128 * 1024,
        group_commit_max_bytes: 64 * 1024,
        max_fragment_bytes: 1 << 20,
    }
}

/// A planner over every node, each a failure domain of its own, coding
/// stripes as 3+2.
fn planner() -> PlannerSource {
    Arc::new(|| {
        let candidates = (0..NODES).map(|n| Candidate {
            node: node(n),
            zone: None,
            rack: None,
            capacity_bytes: 1 << 40,
            state: NodeState::Live,
            shards: 0,
            primaries: 0,
        });
        let policy = EcConfig {
            min_eligible_nodes: 5,
            max_data_fragments: 3,
            ..EcConfig::default()
        };
        let policy = GeometryPolicy::from_config(&policy).expect("a valid policy");
        FragmentPlanner::new(Topology::new(FailureDomain::Node, candidates), policy)
    })
}
