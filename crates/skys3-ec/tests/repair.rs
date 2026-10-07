//! Repair on a shard alone (§8.6): which fragments count as lost, the
//! order stripes are repaired in, what an `EC_RELOCATE` moves, and how a
//! repair fails.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use skys3_config::{EcConfig, FailureDomain};
use skys3_coord::{Candidate, FragmentPlanner, GeometryPolicy, NodeState, Topology};
use skys3_ec::fragment::FragmentHeader;
use skys3_ec::read::ReadFuture;
use skys3_ec::repair::RepairBug;
use skys3_ec::{
    Attempts, Encoded, Encoder, EncoderSettings, FragmentBytes, FragmentId, FragmentReadError,
    FragmentRequest, FragmentSource, FragmentWriter, PlannerSource, RepairEvent, RepairMetrics,
    RepairReport, RepairSettings, RepairStep, Repairer, TransferError, codec,
};
use skys3_index::{Index, IndexConfig};
use skys3_io::{BlockingPool, ManualWallClock, MonotonicClock, SimDisk, SimMount};
use skys3_log::record::{Extent, Put, PutData};
use skys3_log::{LogConfig, RecordBody, SegmentLog, ShardRef};
use skys3_shard::Shard;
use skys3_types::{
    BucketId, CodedStripe, ETag, Epoch, EpochSeq, NodeId, ProposalId, ShardConfig, ShardId,
};
use tokio::sync::watch;

const WRITTEN_MS: u64 = 1_800_000_000_000;

fn node(n: usize) -> NodeId {
    format!("n{n}").parse().unwrap()
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
}

fn sample(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed) ^ (i >> 8) as u8)
        .collect()
}

async fn open_shard(disk: &SimDisk) -> Shard<SimMount> {
    let config = LogConfig {
        inline_max_bytes: 512,
        segment_bytes: 1 << 20,
        group_commit_max_delay: Duration::ZERO,
        group_commit_max_bytes: 256 * 1024,
    };
    let clock = Arc::new(MonotonicClock::new());
    let (log, _) = SegmentLog::open(disk.mount(), config, clock).await.unwrap();
    let index = Index::open_sim(&disk.mount(), "index.redb", &IndexConfig::default()).unwrap();
    let pool = BlockingPool::new("index", NonZeroUsize::MIN).unwrap();
    let shard = ShardRef::new(BucketId::new("b-ec").unwrap(), ShardId::new(0));
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
    Shard::open(&config, log, Arc::new(index), pool)
        .await
        .unwrap()
}

async fn write(shard: &Shard<SimMount>, key: &str, data: &[u8], tag: u64) -> EpochSeq {
    let mut extents = Vec::new();
    for (n, chunk) in data.chunks(4000).enumerate() {
        let extent = Extent {
            key: key.to_owned(),
            offset: n as u64 * 4000,
            data: Bytes::copy_from_slice(chunk),
        };
        extents.push(shard.append_extent(extent).await.unwrap());
    }
    let body = RecordBody::Put(Put {
        key: key.to_owned(),
        size: data.len() as u64,
        last_modified_ms: WRITTEN_MS,
        etag: ETag::new(format!("{tag:032x}")).unwrap(),
        inherited_identity: None,
        metadata: BTreeMap::new(),
        tags: BTreeMap::new(),
        checksums: BTreeMap::new(),
        copy_source: None,
        data: PutData::Extents(extents),
    });
    shard.commit(body).await.unwrap().position
}

/// The fragment nodes, in memory: what they hold, which do not answer,
/// which lost everything, which refuse writes, and a gate that holds
/// writes until it opens.
#[derive(Debug, Default)]
struct Nodes {
    fragments: Mutex<BTreeMap<(NodeId, FragmentId), (FragmentHeader, Bytes)>>,
    silent: Mutex<BTreeSet<NodeId>>,
    failing: Mutex<BTreeSet<NodeId>>,
    /// How many more writes fail, whatever their node.
    failures: Mutex<usize>,
    gate: Mutex<Option<watch::Receiver<bool>>>,
    next: Mutex<u128>,
    reads: Mutex<u64>,
}

impl Nodes {
    fn silence(&self, n: usize) {
        self.silent.lock().unwrap().insert(node(n));
    }

    /// Node `n` loses every fragment, as with a new disk.
    fn wipe(&self, n: usize) {
        self.fragments
            .lock()
            .unwrap()
            .retain(|(holder, _), _| *holder != node(n));
    }

    fn hold(&self) -> watch::Sender<bool> {
        let (sender, receiver) = watch::channel(false);
        *self.gate.lock().unwrap() = Some(receiver);
        sender
    }

    fn get(&self, holder: &NodeId, id: FragmentId) -> Option<(FragmentHeader, Bytes)> {
        self.fragments
            .lock()
            .unwrap()
            .get(&(holder.clone(), id))
            .cloned()
    }
}

impl FragmentWriter for Nodes {
    async fn write(
        &self,
        holder: &NodeId,
        header: &FragmentHeader,
        data: Bytes,
    ) -> Result<FragmentId, TransferError> {
        let gate = self.gate.lock().unwrap().clone();
        if let Some(mut gate) = gate {
            gate.wait_for(|open| *open).await.unwrap();
        }
        let failure = {
            let mut failures = self.failures.lock().unwrap();
            let fails = *failures > 0;
            *failures = failures.saturating_sub(1);
            fails
        };
        if failure
            || self.failing.lock().unwrap().contains(holder)
            || self.silent.lock().unwrap().contains(holder)
        {
            return Err(TransferError::NoAnswer(holder.clone()));
        }
        let mut next = self.next.lock().unwrap();
        *next += 1;
        let id = FragmentId::new(*next);
        self.fragments
            .lock()
            .unwrap()
            .insert((holder.clone(), id), (header.clone(), data));
        Ok(id)
    }
}

impl FragmentSource for Nodes {
    fn read(&self, request: FragmentRequest) -> ReadFuture<'_> {
        Box::pin(async move {
            if self.silent.lock().unwrap().contains(&request.node) {
                return Err(FragmentReadError::Unreachable {
                    node: request.node,
                    reason: "silent".into(),
                });
            }
            let held = self.get(&request.node, request.fragment);
            let Some((_, bytes)) = held.filter(|(h, _)| request.identity.matches(h)) else {
                return Err(FragmentReadError::NotHeld {
                    node: request.node,
                    reason: "no such fragment".into(),
                });
            };
            *self.reads.lock().unwrap() += 1;
            let data = bytes.slice(request.range.start as usize..request.range.end as usize);
            Ok(FragmentBytes {
                crc32c: crc32c::crc32c(&data),
                data,
            })
        })
    }
}

/// A planner over nodes `0..nodes`, each a domain of its own, under
/// `policy`; the nodes in `departing` are departing.
fn planner(nodes: usize, policy: EcConfig, departing: Vec<usize>) -> PlannerSource {
    growing(Arc::new(AtomicUsize::new(nodes)), policy, departing)
}

/// A planner over as many nodes as `nodes` holds when it is asked.
fn growing(nodes: Arc<AtomicUsize>, policy: EcConfig, departing: Vec<usize>) -> PlannerSource {
    Arc::new(move || {
        let count = nodes.load(Ordering::SeqCst);
        let candidates = (0..count).map(|n| Candidate {
            node: node(n),
            zone: None,
            rack: None,
            capacity_bytes: 1 << 40,
            state: if departing.contains(&n) {
                NodeState::Departing
            } else {
                NodeState::Live
            },
            shards: 0,
            primaries: 0,
        });
        let topology = Topology::new(FailureDomain::Node, candidates);
        FragmentPlanner::new(topology, GeometryPolicy::from_config(&policy).unwrap())
    })
}

/// 2+2 stripes from four nodes on.
fn two_plus_two() -> EcConfig {
    EcConfig {
        min_eligible_nodes: 4,
        max_data_fragments: 2,
        ..EcConfig::default()
    }
}

const SETTINGS: RepairSettings = RepairSettings {
    interval: Duration::from_millis(10),
    lost_after: Duration::from_millis(200),
    checks_per_node: 2,
    replans: 2,
    bytes_per_second: 1 << 40,
};

struct Fixture {
    _disk: SimDisk,
    shard: Shard<SimMount>,
    nodes: Arc<Nodes>,
    attempts: Attempts,
    metrics: RepairMetrics,
    events: Arc<Mutex<Vec<RepairEvent>>>,
    /// The bytes of each object.
    objects: BTreeMap<String, Vec<u8>>,
}

impl Fixture {
    /// A shard whose `objects` objects of 10,000 bytes, in stripes of
    /// 4096, are coded over `nodes` nodes under `policy`.
    async fn coded(objects: usize, nodes: usize, policy: EcConfig) -> Self {
        let disk = SimDisk::new(11);
        let shard = open_shard(&disk).await;
        let fixture = Self {
            _disk: disk,
            shard,
            nodes: Arc::default(),
            attempts: Attempts::default(),
            metrics: RepairMetrics::default(),
            events: Arc::default(),
            objects: (0..objects)
                .map(|n| (format!("object-{n}"), sample(10_000, n as u8)))
                .collect(),
        };
        let encoder = Encoder::new(
            fixture.shard.clone(),
            Arc::clone(&fixture.nodes),
            planner(nodes, policy, Vec::new()),
            Arc::new(ManualWallClock::new(Duration::from_millis(
                WRITTEN_MS + 61_000,
            ))),
            EncoderSettings {
                min_object_bytes: 1000,
                stripe_data_bytes: 4096,
                after: Duration::from_secs(60),
                replans: 3,
                after_backup: false,
            },
        )
        .with_attempts(fixture.attempts.clone());
        for (n, (key, data)) in fixture.objects.iter().enumerate() {
            write(&fixture.shard, key, data, n as u64).await;
            assert!(matches!(
                encoder.encode(key).await.unwrap(),
                Encoded::Published(_)
            ));
        }
        fixture
    }

    fn repairer(
        &self,
        planner: PlannerSource,
        settings: RepairSettings,
    ) -> Repairer<SimMount, Nodes> {
        let events = Arc::clone(&self.events);
        Repairer::new(
            self.shard.clone(),
            Arc::clone(&self.nodes) as Arc<dyn FragmentSource>,
            Arc::clone(&self.nodes),
            planner,
            BlockingPool::inline("repair"),
            settings,
        )
        .with_attempts(self.attempts.clone())
        .with_metrics(self.metrics.clone())
        .with_observer(Arc::new(move |event| {
            events.lock().unwrap().push(event.clone());
        }))
    }

    async fn layouts(&self) -> BTreeMap<String, Vec<CodedStripe>> {
        let mut layouts = BTreeMap::new();
        for key in self.objects.keys() {
            let entry = self.shard.entry(key).await.unwrap().unwrap();
            layouts.insert(key.clone(), entry.object.unwrap().coded.unwrap().stripes);
        }
        layouts
    }

    /// Checks that every fragment each layout names is held by its node,
    /// with a header of its stripe and index, and decodes to the object.
    async fn check_whole(&self) {
        for (key, stripes) in self.layouts().await {
            let data = &self.objects[&key];
            for stripe in stripes {
                let mut slots = Vec::new();
                for (index, location) in stripe.fragments().iter().enumerate() {
                    let (header, bytes) = self
                        .nodes
                        .get(&location.node, location.fragment)
                        .unwrap_or_else(|| panic!("{key}: {location:?} is not held"));
                    assert_eq!(header.key, key);
                    assert_eq!(usize::from(header.index), index);
                    assert_eq!(header.stripe.number, stripe.number());
                    slots.push(Some(bytes));
                }
                // The parity alone, with one data fragment, still decodes.
                slots[0] = None;
                let slots: Vec<Option<&[u8]>> = slots.iter().map(|s| s.as_deref()).collect();
                let decoded = codec(stripe.codec())
                    .unwrap()
                    .decode(stripe.geometry(), stripe.data_len(), &slots)
                    .unwrap();
                assert_eq!(
                    decoded,
                    data[stripe.offset() as usize..stripe.end() as usize]
                );
            }
        }
    }

    /// How many fragments the layouts place on node `n`.
    async fn on(&self, n: usize) -> usize {
        let layouts = self.layouts().await;
        layouts
            .values()
            .flatten()
            .flat_map(CodedStripe::fragments)
            .filter(|location| location.node == node(n))
            .count()
    }

    fn steps(&self) -> Vec<RepairStep> {
        let events = self.events.lock().unwrap();
        events.iter().map(|event| event.step.clone()).collect()
    }
}

#[test]
fn a_silent_node_is_repaired_once_it_stays_silent() {
    runtime().block_on(async {
        let f = Fixture::coded(2, 6, EcConfig::default()).await;
        let lost = f.on(3).await;
        assert!(lost > 0);
        f.nodes.silence(3);
        let repairer = f.repairer(planner(6, EcConfig::default(), Vec::new()), SETTINGS);

        // Silent, but not for long enough: nothing is lost yet.
        let report = repairer.pass().await;
        assert_eq!((report.nodes, report.lost, report.repaired), (6, 0, 0));
        tokio::time::sleep(SETTINGS.lost_after).await;
        let report = repairer.pass().await;
        assert_eq!(report.lost, lost);
        assert_eq!(
            report.repaired, lost,
            "one fragment of each stripe: {report:?}"
        );
        assert_eq!(f.on(3).await, 0);
        f.check_whole().await;
        assert_eq!(f.metrics.repaired(), lost as u64);
        assert_eq!(f.metrics.unrepaired(), 0);
        assert!(f.metrics.bytes() > 0);
        assert!(f.attempts.in_progress().is_empty());

        // The steps of one repair, in order.
        let steps = f.steps();
        assert!(matches!(&steps[0], RepairStep::Started { pass: 2, lost } if lost.len() == 1));
        let reads = steps[1..4]
            .iter()
            .filter(|s| matches!(s, RepairStep::Read { .. }));
        assert_eq!(reads.count(), 3);
        assert!(matches!(&steps[4], RepairStep::Placed { nodes } if nodes.len() == 1));
        assert!(matches!(&steps[5], RepairStep::Written { .. }));
        assert_eq!(steps[6], RepairStep::Appended);
        assert!(matches!(
            steps[7],
            RepairStep::Committed {
                relocated: true,
                ..
            }
        ));

        // Nothing is left to repair.
        // Nothing is left to repair, and the silent node holds nothing.
        assert_eq!(
            repairer.pass().await,
            RepairReport {
                nodes: 5,
                ..RepairReport::default()
            }
        );
    });
}

#[test]
fn a_node_that_lost_its_disk_is_repaired_at_once() {
    runtime().block_on(async {
        let f = Fixture::coded(3, 6, EcConfig::default()).await;
        let lost = f.on(2).await;
        assert!(lost > SETTINGS.checks_per_node, "more than one pass checks");
        f.nodes.wipe(2);
        let repairer = f.repairer(planner(6, EcConfig::default(), Vec::new()), SETTINGS);
        // One check finds a fragment missing, so every other one of the
        // node is checked too, and all are repaired in that pass. The node
        // answers, so it may take fragments back.
        let report = repairer.pass().await;
        assert_eq!((report.lost, report.repaired), (lost, lost));
        f.check_whole().await;
    });
}

#[test]
fn a_departing_node_is_repaired_without_checks() {
    runtime().block_on(async {
        let f = Fixture::coded(1, 6, EcConfig::default()).await;
        let lost = f.on(4).await;
        let repairer = f.repairer(planner(6, EcConfig::default(), vec![4]), SETTINGS);
        let report = repairer.pass().await;
        assert_eq!((report.lost, report.repaired), (lost, lost));
        assert_eq!(f.on(4).await, 0);
        f.check_whole().await;

        // A topology that lists no node yet says nothing of any.
        let empty: PlannerSource = Arc::new(|| {
            FragmentPlanner::new(
                Topology::new(FailureDomain::Node, Vec::new()),
                GeometryPolicy::from_config(&EcConfig::default()).unwrap(),
            )
        });
        let report = f.repairer(empty, SETTINGS).pass().await;
        assert_eq!(report.lost, 0);
    });
}

#[test]
fn stripes_that_lost_two_fragments_go_first() {
    runtime().block_on(async {
        // 2+2 stripes on eight nodes: losing two nodes leaves some stripes
        // with two fragments lost and room to rebuild every one.
        let f = Fixture::coded(4, 8, two_plus_two()).await;
        let layouts = f.layouts().await;
        let stripes: Vec<&CodedStripe> = layouts.values().flatten().collect();
        let lost = |a: usize, b: usize, stripe: &CodedStripe| {
            let nodes: BTreeSet<&NodeId> = stripe.fragments().iter().map(|l| &l.node).collect();
            usize::from(nodes.contains(&node(a))) + usize::from(nodes.contains(&node(b)))
        };
        let count = |a, b, n| stripes.iter().filter(|s| lost(a, b, s) == n).count();
        // Two nodes that share some stripes and not others.
        let (a, b) = (0..8)
            .flat_map(|a| (a + 1..8).map(move |b| (a, b)))
            .find(|&(a, b)| count(a, b, 2) > 0 && count(a, b, 1) > 0)
            .expect("two nodes share some stripes");
        let (doubles, singles) = (count(a, b, 2), count(a, b, 1));
        f.nodes.wipe(a);
        f.nodes.wipe(b);
        let settings = RepairSettings {
            checks_per_node: 64,
            ..SETTINGS
        };
        let repairer = f.repairer(planner(8, two_plus_two(), Vec::new()), settings);
        let report = repairer.pass().await;
        assert_eq!(report.repaired, doubles + singles, "{report:?}");
        let lost: Vec<usize> = f
            .steps()
            .iter()
            .filter_map(|step| match step {
                RepairStep::Started { lost, .. } => Some(lost.len()),
                _ => None,
            })
            .collect();
        let mut sorted = lost.clone();
        sorted.sort_by(|a, b| b.cmp(a));
        assert_eq!(lost, sorted);
        assert_eq!(lost.first(), Some(&2));
        f.check_whole().await;

        // With the priority ignored, they go in key order.
        let g = Fixture::coded(4, 8, two_plus_two()).await;
        g.nodes.wipe(a);
        g.nodes.wipe(b);
        let repairer = g
            .repairer(planner(8, two_plus_two(), Vec::new()), settings)
            .with_bug(Some(RepairBug::IgnorePriority));
        repairer.pass().await;
        let lost: Vec<usize> = g
            .steps()
            .iter()
            .filter_map(|step| match step {
                RepairStep::Started { lost, .. } => Some(lost.len()),
                _ => None,
            })
            .collect();
        assert!(lost.windows(2).any(|pair| pair[0] < pair[1]), "{lost:?}");
    });
}

#[test]
fn a_stripe_that_lost_too_much_is_reported_and_one_without_room_waits() {
    runtime().block_on(async {
        // 3+2 on five nodes: no spare node to rebuild onto once one is
        // silent.
        let f = Fixture::coded(1, 5, EcConfig::default()).await;
        f.nodes.silence(4);
        let nodes = Arc::new(AtomicUsize::new(5));
        let repairer = f.repairer(
            growing(Arc::clone(&nodes), EcConfig::default(), Vec::new()),
            SETTINGS,
        );
        repairer.pass().await;
        tokio::time::sleep(SETTINGS.lost_after).await;
        let report = repairer.pass().await;
        assert_eq!((report.lost, report.failed, report.repaired), (3, 3, 0));
        assert_eq!(f.metrics.unrepaired(), 3);
        assert!(f.attempts.in_progress().is_empty());
        assert!(f.steps().contains(&RepairStep::Abandoned));
        // A sixth node joins: the stripes are rebuilt on it.
        nodes.store(6, Ordering::SeqCst);
        let report = repairer.pass().await;
        assert_eq!((report.failed, report.repaired), (0, 3));
        assert_eq!(f.on(5).await, 3);
        assert_eq!(f.metrics.unrepaired(), 0);
        f.check_whole().await;

        // Three of five lost: more than the parity rebuilds.
        let g = Fixture::coded(1, 5, EcConfig::default()).await;
        for n in [1, 2, 3] {
            g.nodes.wipe(n);
        }
        let report = g
            .repairer(planner(7, EcConfig::default(), Vec::new()), SETTINGS)
            .pass()
            .await;
        assert_eq!(report.unrecoverable, 3);
        assert_eq!(report.repaired, 0);
    });
}

#[test]
fn a_failed_write_is_placed_again_elsewhere() {
    runtime().block_on(async {
        let f = Fixture::coded(1, 8, EcConfig::default()).await;
        let lost = f.on(1).await;
        f.nodes.wipe(1);
        // The first write fails, wherever it goes.
        *f.nodes.failures.lock().unwrap() = 1;
        let repairer = f.repairer(planner(8, EcConfig::default(), Vec::new()), SETTINGS);
        let report = repairer.pass().await;
        assert_eq!((report.failed, report.repaired), (0, lost), "{report:?}");
        let placed = f
            .steps()
            .iter()
            .filter(|step| matches!(step, RepairStep::Placed { .. }))
            .count();
        assert_eq!(placed, lost + 1);
        f.check_whole().await;

        // With every possible node failing, the stripe waits.
        let g = Fixture::coded(1, 6, EcConfig::default()).await;
        g.nodes.wipe(1);
        g.nodes.failing.lock().unwrap().extend((0..6).map(node));
        let report = g
            .repairer(planner(6, EcConfig::default(), Vec::new()), SETTINGS)
            .pass()
            .await;
        assert!(report.failed > 0 && report.repaired == 0);
    });
}

#[test]
fn an_attempt_abandoned_by_the_fence_never_relocates() {
    runtime().block_on(async {
        let f = Fixture::coded(1, 6, EcConfig::default()).await;
        let before = f.layouts().await;
        f.nodes.wipe(5);
        let gate = f.nodes.hold();
        let repairer = Arc::new(f.repairer(planner(6, EcConfig::default(), Vec::new()), SETTINGS));
        let pass = tokio::spawn({
            let repairer = Arc::clone(&repairer);
            async move { repairer.pass().await }
        });
        // The repair writes: it is in progress, and a judge in its epoch
        // leaves it be; abandoning it while it writes stops it.
        let attempt = loop {
            if let Some(&attempt) = f.attempts.in_progress().first() {
                break attempt;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        };
        assert!(!f.attempts.orphaned(attempt, Epoch::new(1)));
        assert!(f.attempts.abandon(attempt));
        gate.send(true).unwrap();
        let report = pass.await.unwrap();
        assert_eq!(report.failed, 1, "{report:?}");
        // The abandoned attempt's stripe keeps its layout.
        let events = f.events.lock().unwrap().clone();
        let stripe = events.iter().find(|e| e.attempt == attempt).unwrap().stripe;
        assert!(
            events
                .iter()
                .any(|e| e.attempt == attempt && e.step == RepairStep::Abandoned)
        );
        let after = f.layouts().await;
        assert_eq!(
            after["object-0"][stripe as usize],
            before["object-0"][stripe as usize]
        );
    });
}

#[test]
fn a_repair_of_a_superseded_version_is_rejected() {
    runtime().block_on(async {
        let f = Fixture::coded(1, 6, EcConfig::default()).await;
        f.nodes.wipe(5);
        let gate = f.nodes.hold();
        let repairer = Arc::new(f.repairer(planner(6, EcConfig::default(), Vec::new()), SETTINGS));
        let pass = tokio::spawn({
            let repairer = Arc::clone(&repairer);
            async move { repairer.pass().await }
        });
        while f.attempts.in_progress().is_empty() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        // The object is overwritten while its stripe is rebuilt.
        write(&f.shard, "object-0", &sample(10_000, 9), 99).await;
        gate.send(true).unwrap();
        let report = pass.await.unwrap();
        assert!(report.rejected > 0 || report.failed > 0, "{report:?}");
        assert_eq!(report.repaired, 0);
        let entry = f.shard.entry("object-0").await.unwrap().unwrap();
        assert!(entry.object.unwrap().coded.is_none());
    });
}

#[test]
fn repairs_keep_to_the_bandwidth_cap() {
    runtime().block_on(async {
        let f = Fixture::coded(1, 6, EcConfig::default()).await;
        let lost = f.on(3).await;
        // A repair reads three fragments of its stripe and writes one.
        let lens: Vec<u64> = f
            .layouts()
            .await
            .values()
            .flatten()
            .filter(|stripe| stripe.fragments().iter().any(|l| l.node == node(3)))
            .map(|stripe| {
                let codec = codec(stripe.codec()).unwrap();
                codec
                    .fragment_len(stripe.geometry(), stripe.data_len())
                    .unwrap()
            })
            .collect();
        f.nodes.wipe(3);
        let settings = RepairSettings {
            bytes_per_second: 20_000,
            ..SETTINGS
        };
        let repairer = f.repairer(planner(6, EcConfig::default(), Vec::new()), settings);
        let started = tokio::time::Instant::now();
        let report = repairer.pass().await;
        assert_eq!(report.repaired, lost);
        let bytes = f.metrics.bytes();
        assert_eq!(bytes, lens.iter().map(|len| 4 * len).sum::<u64>());
        // All but the last transfer's bytes passed at the cap.
        let last = lens.iter().max().unwrap();
        let least = Duration::from_secs_f64((bytes - last) as f64 / 20_000.0);
        assert!(
            started.elapsed() >= least,
            "{:?} < {least:?}",
            started.elapsed()
        );
        f.check_whole().await;
    });
}
