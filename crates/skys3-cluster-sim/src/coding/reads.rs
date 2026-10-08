//! Coded reads through the gateway (plan M5-06, design §8.5): once every
//! object is coded and its replicas are dropped, clients read whole
//! objects and ranges through the gateways of every node while up to `m`
//! fragment holders crash, lose power, lose their fragment disk for good,
//! or send corrupted fragment bytes, and every byte a GET returns must be
//! the version's that its ETag names. Some requests copy a coded object to
//! a new key with CopyObject, which reads the source from its fragments,
//! and read the copy back; others retag a coded object, which moves its
//! entry's version past the one its fragments were written for, and read
//! it back.
//!
//! Each node runs a gateway whose shards route to the shard's primary
//! ([`RoutedShards`], over the node's transport), whose bucket comes from
//! an in-memory control store, and whose GETs of coded objects read
//! fragments through the node's [`FragmentReadClient`]. The faults fall
//! only on the run's *lossy* nodes, at most `m` of them, so no stripe ever
//! has more than `m` fragments unreadable: every GET may fail with `503`
//! or break off while plans or fragments are unavailable, but none may
//! return other bytes.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http::{Request, StatusCode};
use http_body_util::Full;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use skys3::storage::Storage;
use skys3_control::{
    Expected, MemoryControlStore, ProposalIds, ProposalOutcome, RetryPolicy, TypedKey, bootstrap,
    propose_document,
};
use skys3_ec::read::ReadFuture;
use skys3_ec::read::seeded::ReadBug;
use skys3_ec::{
    FragmentBytes, FragmentReadClient, FragmentRequest, FragmentServer, FragmentSource,
};
use skys3_gateway::routing::{ForwardServer, RoutedShards, ShardMap};
use skys3_gateway::{Gateway, GatewayConfig, HotCache, IdSource, LocalShards, TrustAll};
use skys3_io::SimMount;
use skys3_net::{Transport, TurmoilNetwork};
use skys3_types::{BucketDocument, BucketMode, BucketName, ClusterId, ShardConfig, ShardCount};

use super::{CrashKind, Driver, MEMBERS, NODES, READ_WINDOW, World, lock};
use crate::node::BoxError;
use crate::s3::{self, NoAnswer};

/// The name of the coded bucket, `b-coded`, in S3 requests.
const BUCKET: &str = "coded";

/// How long a client waits for a GET's whole answer.
const GET_TIMEOUT: Duration = Duration::from_secs(10);

/// How long after the read window's faults the nodes have restarted and
/// serve again, and how many times a client then GETs an object it
/// retagged, 250 ms apart, before it judges the object unreadable.
const SETTLE_AFTER_FAULTS: Duration = Duration::from_secs(2);
const READ_BACK_ATTEMPTS: usize = 40;

/// The room of each gateway's hot cache: whole objects of up to an eighth
/// of it are kept, so some reads fill it and later ones hit it.
const HOT_CACHE_BYTES: u64 = 256 * 1024;

/// Reads through the gateways while fragment holders fail.
#[derive(Debug, Clone, Copy)]
pub struct ReadConfig {
    /// How many clients read, each from gateways drawn per GET.
    pub clients: usize,
    /// How many GETs each client sends.
    pub gets: usize,
    /// How many nodes fail, at most `m` (2), drawn from `n1` to `n5`; the
    /// primary `n0` stays up to plan the reads.
    pub lossy: usize,
    /// Whether the lossy nodes only send corrupted fragment bytes, rather
    /// than also crashing and losing disks.
    pub corrupt_only: bool,
    /// A bug seeded into every coded read.
    pub bug: Option<ReadBug>,
}

/// What the readers saw.
#[derive(Debug, Clone, Default)]
pub struct ReadReport {
    /// GETs answered with the version's bytes.
    pub served: u64,
    /// GETs answered with `503`, or with no answer.
    pub unavailable: u64,
    /// GETs whose body broke off after the response began.
    pub broken: u64,
    /// CopyObjects of a coded object answered `200`; each copy is then
    /// read back as a GET is.
    pub copied: u64,
    /// PutObjectTaggings of a coded object answered `200`, each followed
    /// by a GET of the whole object.
    pub retagged: u64,
    /// Fragment reads of a parity fragment, which only degraded pieces
    /// make.
    pub parity_reads: u64,
    /// Fragment reads that failed, corrupted ones included.
    pub failed_fragment_reads: u64,
    /// The faults the lossy nodes suffered, as `(node, fault)`.
    pub faults: Vec<(String, ReadFault)>,
}

/// A fault of a lossy node during the reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadFault {
    /// The node is killed, or loses power, and restarts after `down`.
    Crash {
        /// A power loss rather than a kill.
        power: bool,
        /// How long it stays down.
        down: Duration,
    },
    /// The node's fragment disk fails at once, and the node restarts after
    /// `down` with an empty one: its fragments are gone for good.
    DiskLoss {
        /// How long until it restarts.
        down: Duration,
    },
    /// For `lasting`, every fragment byte range the node sends arrives
    /// with a byte changed, under the CRC32C of the right bytes.
    Corrupt {
        /// How long.
        lasting: Duration,
    },
}

/// The read side of the world: the control store the gateways read, the
/// bytes of every version, the lossy nodes' state, and the report.
pub(super) struct Reads {
    pub(super) config: ReadConfig,
    cluster: ClusterId,
    store: MemoryControlStore,
    /// Each version's bytes, by ETag.
    versions: Mutex<BTreeMap<String, Vec<u8>>>,
    /// Whether each node's fragment bytes arrive corrupted.
    corrupt: Vec<AtomicBool>,
    clients_done: AtomicU64,
    parity_reads: AtomicU64,
    failed_fragment_reads: AtomicU64,
    report: Mutex<ReadReport>,
    /// GETs that returned other bytes than their version's.
    wrong: Mutex<Vec<String>>,
}

impl Reads {
    /// The read side of a run of `config` in cluster `cluster` of `nodes`
    /// nodes, with the coded bucket and its shard, `shard`, registered in a
    /// fresh control store.
    pub(super) fn new(
        config: ReadConfig,
        cluster: ClusterId,
        shard: &ShardConfig,
        seed: u64,
        nodes: usize,
    ) -> Result<Self, BoxError> {
        let store = MemoryControlStore::new();
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        runtime.block_on(register(&store, &cluster, shard, seed))?;
        Ok(Self {
            config,
            cluster,
            store,
            versions: Mutex::default(),
            corrupt: (0..nodes).map(|_| AtomicBool::new(false)).collect(),
            clients_done: AtomicU64::new(0),
            parity_reads: AtomicU64::new(0),
            failed_fragment_reads: AtomicU64::new(0),
            report: Mutex::default(),
            wrong: Mutex::default(),
        })
    }

    /// Records the bytes of a version written.
    pub(super) fn written(&self, etag: &str, data: &[u8]) {
        lock(&self.versions).insert(etag.to_owned(), data.to_vec());
    }

    /// Whether every client is done.
    pub(super) fn done(&self) -> bool {
        self.clients_done.load(Ordering::SeqCst) == self.config.clients as u64
    }

    /// The report, or the first GET that returned wrong bytes.
    pub(super) fn report(&self) -> Result<ReadReport, String> {
        if let Some(wrong) = lock(&self.wrong).first() {
            return Err(wrong.clone());
        }
        let mut report = lock(&self.report).clone();
        report.parity_reads = self.parity_reads.load(Ordering::SeqCst);
        report.failed_fragment_reads = self.failed_fragment_reads.load(Ordering::SeqCst);
        Ok(report)
    }
}

/// Writes `cluster.json`, the coded bucket's register, and its shard's.
async fn register(
    store: &MemoryControlStore,
    cluster: &ClusterId,
    shard: &ShardConfig,
    seed: u64,
) -> Result<(), BoxError> {
    let retry = RetryPolicy::default();
    let mut ids = ProposalIds::seeded(seed);
    bootstrap(store, cluster, ids.next_id(), &retry).await?;
    let bucket = BucketDocument {
        bucket_id: shard.bucket_id.clone(),
        name: BucketName::new(BUCKET)?,
        mode: BucketMode::Local,
        shards: ShardCount::new(1)?,
        replicas: MEMBERS as u8,
        min_write_replicas: 1,
        clean_copies: MEMBERS as u8,
        target: None,
        created_unix_ms: 0,
        lifecycle: None,
        proposal_id: ids.next_id(),
    };
    let proposals = [
        propose_document(
            store,
            &TypedKey::bucket(&bucket.name),
            Expected::Absent,
            &bucket,
            &retry,
        )
        .await?,
        propose_document(
            store,
            &TypedKey::shard(&bucket.bucket_id, shard.shard),
            Expected::Absent,
            shard,
            &retry,
        )
        .await?,
    ];
    if proposals
        .iter()
        .any(|outcome| !matches!(outcome, ProposalOutcome::Accepted(_)))
    {
        return Err("a register exists in a fresh control store".into());
    }
    Ok(())
}

/// The forwarded requests a node serves for other gateways.
pub(super) type Forwards = ForwardServer<LocalShards<SimMount>, SimMount>;

/// Starts node `index`'s gateway on its S3 port: its shards route to the
/// shard's primary, and its coded GETs read fragments through
/// `transport`, this node's own from `fragments`. Returns the server of
/// the requests other gateways forward to this node.
pub(super) async fn start_gateway(
    world: &Arc<World>,
    index: usize,
    storage: &Storage<SimMount>,
    transport: &Transport<TurmoilNetwork>,
    fragments: &FragmentServer<SimMount>,
) -> Result<Forwards, BoxError> {
    let reads = world.reads.as_ref().ok_or("no reads in this run")?;
    let node = world.nodes[index].id.clone();
    let map = ShardMap::in_memory();
    map.learn(world.config.clone()).await;
    let local = storage.shards.clone();
    let routed = RoutedShards::new(
        node.clone(),
        local.clone(),
        local.set().clone(),
        map,
        transport.clone(),
        world.peers.clone(),
        reads.store.clone(),
    );
    let forwards = routed.server().clone();
    let config = format!(
        "[cluster]\ncluster_id = \"{}\"\n\
         [control_store]\netcd_endpoints = [\"https://etcd.sim.internal:2379\"]\n",
        reads.cluster
    )
    .parse::<skys3_config::Config>()?;
    let mut gateway_config = GatewayConfig::new(&config);
    gateway_config.hot_cache = HotCache::new(HOT_CACHE_BYTES);
    // The shard's log takes inline bodies only up to its own bound, so the
    // copies the gateway writes must keep to it.
    gateway_config.inline_max_bytes = super::log_config().inline_max_bytes;
    let client = FragmentReadClient::new(node, transport.clone(), world.peers.clone())
        .with_local(fragments.clone());
    gateway_config.fragments = Some(Arc::new(Observed {
        client,
        world: Arc::clone(world),
    }));
    let seed = world.seed ^ (index as u64).rotate_left(40);
    let gateway = Gateway::new(
        gateway_config,
        reads.store.clone(),
        routed,
        IdSource::seeded(seed),
        TrustAll,
    )
    .await?;
    let listener = s3::bind().await?;
    tokio::spawn(async move {
        if let Err(error) = s3::serve(listener, gateway).await {
            tracing::debug!(%error, "the S3 listener stopped");
        }
    });
    Ok(forwards)
}

/// A node's fragment reads, counted, and corrupted while a fault says so.
struct Observed {
    client: FragmentReadClient<TurmoilNetwork, SimMount>,
    world: Arc<World>,
}

impl std::fmt::Debug for Observed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Observed").finish_non_exhaustive()
    }
}

impl FragmentSource for Observed {
    fn read(&self, request: FragmentRequest) -> ReadFuture<'_> {
        Box::pin(async move {
            let reads = self.world.reads.as_ref().expect("reads in this run");
            if usize::from(request.identity.index)
                >= request.identity.stripe.geometry.data_fragments()
            {
                reads.parity_reads.fetch_add(1, Ordering::SeqCst);
            }
            let node = self.world.position(&request.node);
            let mut read = self.client.read(request).await;
            if let Ok(FragmentBytes { data, .. }) = &mut read
                && node.is_some_and(|n| reads.corrupt[n].load(Ordering::SeqCst))
                && !data.is_empty()
            {
                let mut changed = data.to_vec();
                changed[0] ^= 0x01;
                *data = Bytes::from(changed);
            }
            let corrupted = node.is_some_and(|n| reads.corrupt[n].load(Ordering::SeqCst));
            if read.is_err() || corrupted {
                reads.failed_fragment_reads.fetch_add(1, Ordering::SeqCst);
            }
            read
        })
    }
}

/// Draws the lossy nodes and their faults over `window` from `now`.
pub(super) fn plan_faults(
    world: &World,
    now: Duration,
    window: Duration,
) -> Vec<(Duration, usize, ReadFault)> {
    let Some(reads) = &world.reads else {
        return Vec::new();
    };
    let mut rng = StdRng::seed_from_u64(world.seed ^ 0x4c4f_5353);
    let mut candidates: Vec<usize> = (1..NODES).collect();
    candidates.shuffle(&mut rng);
    let lossy = reads.config.lossy.min(2);
    let mut faults = Vec::new();
    for &node in &candidates[..lossy] {
        let mut at = now + Duration::from_millis(rng.random_range(50..400));
        while at < now + window {
            let span = Duration::from_millis(rng.random_range(300..1500));
            let fault = match rng.random_range(0..4) {
                _ if reads.config.corrupt_only => ReadFault::Corrupt { lasting: span },
                0 => ReadFault::Crash {
                    power: false,
                    down: span,
                },
                1 => ReadFault::Crash {
                    power: true,
                    down: span,
                },
                2 => ReadFault::DiskLoss { down: span },
                _ => ReadFault::Corrupt { lasting: span },
            };
            faults.push((at, node, fault));
            at += span + Duration::from_millis(rng.random_range(100..600));
        }
    }
    faults.sort_by_key(|(at, node, _)| (*at, *node));
    faults
}

impl Driver<'_> {
    /// Starts the read faults that are due, and ends corruption whose
    /// time is up.
    pub(super) fn read_faults(&mut self, sim: &mut turmoil::Sim<'_>, now: Duration) {
        let Some(reads) = &self.world.reads else {
            return;
        };
        let mut due = Vec::new();
        self.read_faults.retain(|(at, node, fault)| {
            let ready = *at <= now;
            if ready {
                due.push((*node, *fault));
            }
            !ready
        });
        for (node, fault) in due {
            let slot = &self.world.nodes[node];
            lock(&reads.report)
                .faults
                .push((slot.id.to_string(), fault));
            match fault {
                ReadFault::Crash { power, down } => self.crash(sim, now, node, power, down),
                ReadFault::DiskLoss { down } => {
                    // The disk fails under the running node, which reads
                    // its fragments as damaged until it restarts on a new
                    // one.
                    self.lose_disk(node);
                    self.read_faults.push((
                        now + down,
                        node,
                        ReadFault::Crash {
                            power: true,
                            down: Duration::from_millis(50),
                        },
                    ));
                    self.report
                        .crashes
                        .push((slot.id.to_string(), CrashKind::PowerLoss));
                }
                ReadFault::Corrupt { lasting } => {
                    reads.corrupt[node].store(true, Ordering::SeqCst);
                    self.healing.push((now + lasting, node));
                }
            }
        }
        self.healing.retain(|(at, node)| {
            let healed = *at <= now;
            if healed {
                reads.corrupt[*node].store(false, Ordering::SeqCst);
            }
            !healed
        });
    }

    /// Starts the readers.
    pub(super) fn start_readers(&self, sim: &mut turmoil::Sim<'_>, world: &Arc<World>) {
        let Some(reads) = &world.reads else {
            return;
        };
        for n in 0..reads.config.clients {
            let world = Arc::clone(world);
            sim.client(format!("reader-{n}"), async move {
                reader(&world, n).await;
                let reads = world.reads.as_ref().expect("reads in this run");
                reads.clients_done.fetch_add(1, Ordering::SeqCst);
                Ok(())
            });
        }
    }
}

/// One client's requests, each of a drawn object through a drawn node's
/// gateway: GETs of the whole object or a drawn range, and, one request in
/// eight each, a CopyObject to a new key whose copy is then read back, and
/// a PutObjectTagging followed by a GET of the whole object.
async fn reader(world: &World, n: usize) {
    let reads = world.reads.as_ref().expect("reads in this run");
    let started = tokio::time::Instant::now();
    let mut rng = StdRng::seed_from_u64(world.seed ^ 0x5245_4144 ^ (n as u64) << 20);
    let mut retagged = BTreeSet::new();
    let keys: Vec<(String, usize)> = lock(&world.expected)
        .iter()
        .map(|(key, data)| (key.clone(), data.len()))
        .collect();
    for i in 0..reads.config.gets {
        let (key, size) = keys[rng.random_range(0..keys.len())].clone();
        // A client never picks a node lost for good: no gateway answers
        // there.
        let mut host = rng.random_range(0..NODES);
        while world.lost[host].load(Ordering::SeqCst) {
            host = rng.random_range(0..NODES);
        }
        let host = format!("n{host}");
        let range = match rng.random_range(0..8) {
            0 => {
                // The copy keeps the source's ETag, so its bytes are
                // checked against the source version's.
                let target = format!("copy-{n}-{i}");
                let copied = copy(&host, &key, &target).await;
                if reads.wrote(Write::Copy, &key, &host, copied) {
                    let outcome = get(&host, &target, None).await;
                    reads.count(&target, &host, None, outcome);
                }
                tokio::time::sleep(Duration::from_millis(rng.random_range(0..20))).await;
                continue;
            }
            1 => {
                // The retag moves the entry's version past its coded
                // layout's; the GET after it must still read the fragments.
                let tagged = retag(&host, &key, &format!("{n}-{i}")).await;
                if reads.wrote(Write::Retag, &key, &host, tagged) {
                    retagged.insert(key.clone());
                }
                None
            }
            2..=3 => None,
            _ => {
                let (a, b) = (rng.random_range(0..size), rng.random_range(0..size));
                Some((a.min(b), a.max(b)))
            }
        };
        let outcome = get(&host, &key, range).await;
        reads.count(&key, &host, range, outcome);
        tokio::time::sleep(Duration::from_millis(rng.random_range(0..20))).await;
    }
    // Once the faults are over, every object this client retagged must be
    // served again: a GET may answer `503` under faults, but a read that
    // named the entry's version rather than the fragments' would answer it
    // for good.
    tokio::time::sleep_until(started + READ_WINDOW + SETTLE_AFTER_FAULTS).await;
    for key in retagged {
        let mut served = false;
        for _ in 0..READ_BACK_ATTEMPTS {
            let outcome = get("n0", &key, None).await;
            served = matches!(outcome, Got::Bytes { .. });
            reads.count(&key, "n0", None, outcome);
            if served {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        if !served {
            lock(&reads.wrong).push(format!(
                "{key}, retagged, was not served again once the faults were over"
            ));
        }
    }
}

/// A write a client sends besides its GETs.
#[derive(Debug, Clone, Copy)]
enum Write {
    Copy,
    Retag,
}

impl Reads {
    /// Counts what `write` of `key` through `host` got, and whether it
    /// was done.
    fn wrote(&self, write: Write, key: &str, host: &str, outcome: Result<bool, String>) -> bool {
        let mut report = lock(&self.report);
        match outcome {
            Ok(true) => {
                match write {
                    Write::Copy => report.copied += 1,
                    Write::Retag => report.retagged += 1,
                }
                true
            }
            Ok(false) => {
                report.unavailable += 1;
                false
            }
            Err(what) => {
                lock(&self.wrong).push(format!("a {write:?} of {key} through {host}: {what}"));
                false
            }
        }
    }

    /// Counts what a GET of `key` through `host` got, and records it if
    /// its bytes are not its version's.
    fn count(&self, key: &str, host: &str, range: Option<(usize, usize)>, outcome: Got) {
        let mut report = lock(&self.report);
        match outcome {
            Got::Bytes { etag, body } => {
                let versions = lock(&self.versions);
                let expected = versions.get(&etag).map(|data| match range {
                    Some((a, b)) => &data[a..=b],
                    None => &data[..],
                });
                if expected != Some(&body[..]) {
                    lock(&self.wrong).push(format!(
                        "a GET of {key} {range:?} through {host} returned {} bytes that are \
                         not those of version {etag}",
                        body.len()
                    ));
                }
                report.served += 1;
            }
            Got::Unavailable => report.unavailable += 1,
            Got::Broken => report.broken += 1,
            Got::Unexpected(what) => {
                lock(&self.wrong).push(format!("a GET of {key} through {host}: {what}"));
            }
        }
    }
}

/// What a GET got.
enum Got {
    Bytes { etag: String, body: Bytes },
    Unavailable,
    Broken,
    Unexpected(String),
}

/// Copies `key` to `target` through `host`'s gateway: whether it copied,
/// or what was wrong with the answer.
async fn copy(host: &str, key: &str, target: &str) -> Result<bool, String> {
    let request = Request::put(format!("/{BUCKET}/{target}"))
        .header("x-amz-copy-source", format!("{BUCKET}/{key}"))
        .body(Full::new(Bytes::new()))
        .map_err(|error| format!("a request that does not build: {error}"))?;
    write(host, request).await
}

/// Sets `key`'s tags to one, `reader` = `value`, through `host`'s gateway:
/// whether it was set, or what was wrong with the answer.
pub(super) async fn retag(host: &str, key: &str, value: &str) -> Result<bool, String> {
    let body = format!(
        "<Tagging><TagSet><Tag><Key>reader</Key><Value>{value}</Value></Tag></TagSet></Tagging>"
    );
    let request = Request::put(format!("/{BUCKET}/{key}?tagging"))
        .body(Full::new(Bytes::from(body)))
        .map_err(|error| format!("a request that does not build: {error}"))?;
    write(host, request).await
}

/// Sends `request`, a write, through `host`'s gateway: whether it was
/// done, or what was wrong with the answer.
async fn write(host: &str, request: Request<Full<Bytes>>) -> Result<bool, String> {
    let answer = match s3::connect(host, GET_TIMEOUT).await {
        Ok(connection) => connection.send(request, GET_TIMEOUT).await,
        Err(error) => Err(error),
    };
    match answer {
        Ok(response) if response.status() == StatusCode::OK => Ok(true),
        Ok(response) if response.status().is_server_error() => Ok(false),
        Ok(response) => Err(format!("status {}", response.status())),
        Err(_) => Ok(false),
    }
}

/// GETs `key`, or bytes `a` to `b` of it, through `host`'s gateway.
async fn get(host: &str, key: &str, range: Option<(usize, usize)>) -> Got {
    let mut request = Request::get(format!("/{BUCKET}/{key}"));
    if let Some((a, b)) = range {
        request = request.header("range", format!("bytes={a}-{b}"));
    }
    let Ok(request) = request.body(Full::new(Bytes::new())) else {
        return Got::Unexpected("a request that does not build".to_owned());
    };
    let answer = match s3::connect(host, GET_TIMEOUT).await {
        Ok(connection) => connection.send(request, GET_TIMEOUT).await,
        Err(error) => Err(error),
    };
    match answer {
        Ok(response) => {
            let expected = if range.is_some() {
                StatusCode::PARTIAL_CONTENT
            } else {
                StatusCode::OK
            };
            match response.status() {
                status if status == expected => {
                    let etag = response
                        .headers()
                        .get("etag")
                        .and_then(|etag| etag.to_str().ok())
                        .map(|etag| etag.trim_matches('"').to_owned());
                    match etag {
                        Some(etag) => Got::Bytes {
                            etag,
                            body: response.into_body(),
                        },
                        None => Got::Unexpected("an answer without an ETag".to_owned()),
                    }
                }
                status if status.is_server_error() => Got::Unavailable,
                status => Got::Unexpected(format!("status {status}")),
            }
        }
        Err(NoAnswer::Broken(_)) => Got::Broken,
        Err(_) => Got::Unavailable,
    }
}

impl Driver<'_> {
    /// Whether the reads keep the run going: the first time the objects
    /// settle it starts the readers and plans the faults, and the reads go
    /// on until every reader is done.
    pub(super) fn reads_pending(
        &mut self,
        sim: &mut turmoil::Sim<'_>,
        world: &Arc<World>,
        now: Duration,
    ) -> bool {
        let Some(reads) = &world.reads else {
            return false;
        };
        if !self.reading {
            self.reading = true;
            self.read_faults = plan_faults(world, now, READ_WINDOW);
            self.start_readers(sim, world);
            return true;
        }
        !reads.done()
    }
}
