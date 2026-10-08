//! The encoder on a shard alone (§8.2, §8.4): what qualifies, how stripes
//! are cut, written, and published, and how attempts fail.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use skys3_config::{EcConfig, FailureDomain};
use skys3_coord::{Candidate, FragmentPlanner, GeometryPolicy, NodeState, Topology};
use skys3_ec::fragment::FragmentHeader;
use skys3_ec::{
    AttemptState, EncodeError, EncodeEvent, EncodeStep, Encoded, Encoder, EncoderSettings,
    FragmentId, FragmentWriter, PlannerSource, Skip, TransferError, codec,
};
use skys3_index::{Index, IndexConfig};
use skys3_io::{BlockingPool, ManualWallClock, MonotonicClock, SimDisk, SimMount};
use skys3_log::record::{Delete, Extent, Flushed, Put, PutData};
use skys3_log::{LogConfig, RecordBody, SegmentLog, ShardRef};
use skys3_shard::{Rejection, Shard};
use skys3_types::{
    AttemptId, BucketId, ETag, Epoch, EpochSeq, NodeId, ProposalId, ShardConfig, ShardId,
};
use tokio::sync::watch;

/// When every object is written, in milliseconds since the Unix epoch.
const WRITTEN_MS: u64 = 1_800_000_000_000;

fn node(n: usize) -> NodeId {
    format!("n{n}").parse().unwrap()
}

fn shard_ref() -> ShardRef {
    ShardRef::new(BucketId::new("b-ec").unwrap(), ShardId::new(0))
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
}

/// Deterministic bytes.
fn sample(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed) ^ (i >> 8) as u8)
        .collect()
}

/// A shard alone in epoch 1, on its own disk.
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
    let shard = shard_ref();
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

/// Writes `data` as `key`, in extents of at most 4000 bytes.
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
        tags: BTreeMap::from([("team".to_owned(), "a".to_owned())]),
        checksums: BTreeMap::new(),
        copy_source: None,
        data: PutData::Extents(extents),
    });
    shard.commit(body).await.unwrap().position
}

/// Fragments kept in memory by node, with nodes that fail every write
/// and a gate that holds writes until it opens.
#[derive(Default)]
struct MemoryWriter {
    fragments: Mutex<BTreeMap<(NodeId, FragmentId), (FragmentHeader, Bytes)>>,
    failing: Mutex<BTreeSet<NodeId>>,
    gate: Mutex<Option<watch::Receiver<bool>>>,
}

impl MemoryWriter {
    fn fail(&self, nodes: impl IntoIterator<Item = NodeId>) {
        self.failing.lock().unwrap().extend(nodes);
    }

    /// Holds writes until the returned sender sends `true`.
    fn hold(&self) -> watch::Sender<bool> {
        let (sender, receiver) = watch::channel(false);
        *self.gate.lock().unwrap() = Some(receiver);
        sender
    }

    fn get(&self, node: &NodeId, id: FragmentId) -> (FragmentHeader, Bytes) {
        self.fragments.lock().unwrap()[&(node.clone(), id)].clone()
    }

    fn nodes(&self) -> BTreeSet<NodeId> {
        let fragments = self.fragments.lock().unwrap();
        fragments.keys().map(|(node, _)| node.clone()).collect()
    }
}

impl FragmentWriter for MemoryWriter {
    async fn write(
        &self,
        node: &NodeId,
        header: &FragmentHeader,
        data: Bytes,
    ) -> Result<FragmentId, TransferError> {
        let gate = self.gate.lock().unwrap().clone();
        if let Some(mut gate) = gate {
            gate.wait_for(|open| *open).await.unwrap();
        }
        if self.failing.lock().unwrap().contains(node) {
            return Err(TransferError::NoAnswer(node.clone()));
        }
        let mut fragments = self.fragments.lock().unwrap();
        let id = FragmentId::new(fragments.len() as u128 + 1);
        fragments.insert((node.clone(), id), (header.clone(), data));
        Ok(id)
    }
}

/// A planner over `nodes` nodes, each a domain of its own, with the
/// default policy: 3+2 from 5 nodes on.
fn planner(nodes: usize) -> PlannerSource {
    Arc::new(move || {
        let candidates = (0..nodes).map(|n| Candidate {
            node: node(n),
            zone: None,
            rack: None,
            capacity_bytes: 1 << 40,
            state: NodeState::Live,
            shards: 0,
            primaries: 0,
        });
        let topology = Topology::new(FailureDomain::Node, candidates);
        FragmentPlanner::new(
            topology,
            GeometryPolicy::from_config(&EcConfig::default()).unwrap(),
        )
    })
}

const SETTINGS: EncoderSettings = EncoderSettings {
    min_object_bytes: 1000,
    stripe_data_bytes: 4096,
    after: Duration::from_secs(60),
    replans: 3,
    after_backup: false,
};

struct Fixture {
    disk: SimDisk,
    shard: Shard<SimMount>,
    writer: Arc<MemoryWriter>,
    clock: ManualWallClock,
    events: Arc<Mutex<Vec<EncodeEvent>>>,
}

impl Fixture {
    async fn new() -> Self {
        let disk = SimDisk::new(7);
        let shard = open_shard(&disk).await;
        Self {
            disk,
            shard,
            writer: Arc::default(),
            clock: ManualWallClock::new(Duration::from_millis(WRITTEN_MS + 61_000)),
            events: Arc::default(),
        }
    }

    fn encoder(&self, nodes: usize) -> Encoder<SimMount, MemoryWriter> {
        let events = Arc::clone(&self.events);
        Encoder::new(
            self.shard.clone(),
            Arc::clone(&self.writer),
            planner(nodes),
            Arc::new(self.clock.clone()),
            SETTINGS,
        )
        .with_observer(Arc::new(move |event| {
            events.lock().unwrap().push(event.clone());
        }))
    }

    fn steps(&self) -> Vec<EncodeStep> {
        let events = self.events.lock().unwrap();
        events.iter().map(|event| event.step.clone()).collect()
    }
}

#[test]
fn an_object_is_encoded_stripe_by_stripe_and_published() {
    runtime().block_on(async {
        let f = Fixture::new().await;
        let data = sample(10_000, 1);
        let version = write(&f.shard, "photo", &data, 1).await;
        let encoder = f.encoder(6);

        let Encoded::Published(position) = encoder.encode("photo").await.unwrap() else {
            panic!("not published");
        };
        let entry = f.shard.entry("photo").await.unwrap().unwrap();
        assert_eq!(entry.version, version);
        let coded = entry.object.unwrap().coded.unwrap();
        assert_eq!(coded.publish, position);
        assert_eq!(coded.version, version);
        assert_eq!(coded.attempt, AttemptId::new(Epoch::new(1), 0));
        let lens: Vec<_> = coded.stripes.iter().map(|s| s.data_len()).collect();
        assert_eq!(lens, [4096, 4096, 1808]);

        // Each stripe decodes from its fragments to the object's bytes,
        // and every header names the version, its stripe, and the attempt.
        for stripe in &coded.stripes {
            assert_eq!(stripe.geometry().total_fragments(), 5);
            let mut slots = Vec::new();
            for (index, location) in stripe.fragments().iter().enumerate() {
                let (header, bytes) = f.writer.get(&location.node, location.fragment);
                assert_eq!(header.version, version);
                assert_eq!(header.attempt, coded.attempt);
                assert_eq!(header.index as usize, index);
                assert_eq!(header.stripe.number, stripe.number());
                assert_eq!(header.stripe.count, 3);
                assert_eq!(header.object.size, 10_000);
                assert_eq!(header.object.tags["team"], "a");
                slots.push(Some(bytes));
            }
            // Any two may be lost.
            slots[0] = None;
            slots[3] = None;
            let slots: Vec<Option<&[u8]>> = slots.iter().map(|s| s.as_deref()).collect();
            let decoded = codec(stripe.codec())
                .unwrap()
                .decode(stripe.geometry(), stripe.data_len(), &slots)
                .unwrap();
            let range = stripe.offset() as usize..stripe.end() as usize;
            assert_eq!(decoded, data[range]);
        }

        // The steps, in order.
        let steps = f.steps();
        assert_eq!(steps[0], EncodeStep::Started { stripes: 3 });
        assert!(matches!(
            steps[1],
            EncodeStep::StripeStarted { stripe: 0, .. }
        ));
        assert_eq!(
            steps
                .iter()
                .filter(|s| matches!(s, EncodeStep::FragmentWritten { .. }))
                .count(),
            15
        );
        let tail = &steps[steps.len() - 3..];
        assert_eq!(tail[0], EncodeStep::StripeWritten { stripe: 2 });
        assert_eq!(tail[1], EncodeStep::Appended);
        assert_eq!(
            tail[2],
            EncodeStep::Committed {
                position,
                published: true
            }
        );
        assert!(encoder.attempts().in_progress().is_empty());

        // A coded version is not encoded again.
        assert_eq!(
            encoder.encode("photo").await.unwrap(),
            Encoded::Skipped(Skip::Coded)
        );
    });
}

#[test]
fn a_backed_up_bucket_encodes_only_what_its_backup_holds() {
    runtime().block_on(async {
        let f = Fixture::new().await;
        let version = write(&f.shard, "photo", &sample(5000, 1), 1).await;
        let encoder = Encoder::new(
            f.shard.clone(),
            Arc::clone(&f.writer),
            planner(6),
            Arc::new(f.clock.clone()),
            EncoderSettings {
                after_backup: true,
                ..SETTINGS
            },
        );
        // The flusher sends a version from the replica encoding drops, so
        // the version waits until its backup holds it (§8.9).
        assert_eq!(
            encoder.encode("photo").await.unwrap(),
            Encoded::Skipped(Skip::NotBackedUp)
        );
        assert!(f.writer.nodes().is_empty());
        let flushed = Flushed {
            key: "photo".to_owned(),
            seq: version.seq,
            remote_etag: Some(ETag::new(format!("{:032x}", 1)).unwrap()),
            remote_version_id: None,
        };
        f.shard.commit(RecordBody::Flushed(flushed)).await.unwrap();
        assert!(matches!(
            encoder.encode("photo").await.unwrap(),
            Encoded::Published(_)
        ));
    });
}

#[test]
fn objects_that_do_not_qualify_stay_replicated() {
    runtime().block_on(async {
        let f = Fixture::new().await;
        write(&f.shard, "small", &sample(999, 2), 2).await;
        write(&f.shard, "large", &sample(30_000, 3), 3).await;
        write(&f.shard, "gone", &sample(5000, 4), 4).await;
        f.shard
            .commit(RecordBody::Delete(Delete {
                key: "gone".to_owned(),
            }))
            .await
            .unwrap();
        let encoder = f.encoder(6);
        let skipped = Encoded::Skipped;
        assert_eq!(
            encoder.encode("absent").await.unwrap(),
            skipped(Skip::Absent)
        );
        assert_eq!(encoder.encode("gone").await.unwrap(), skipped(Skip::Absent));
        assert_eq!(
            encoder.encode("small").await.unwrap(),
            skipped(Skip::TooSmall)
        );

        // Too recent until `ec_after` has passed since Last-Modified.
        f.clock.set(Duration::from_millis(WRITTEN_MS + 59_999));
        assert_eq!(
            encoder.encode("large").await.unwrap(),
            skipped(Skip::TooRecent)
        );
        f.clock.set(Duration::from_millis(WRITTEN_MS + 60_000));

        // Four nodes support no geometry: encoding pauses.
        let paused = f.encoder(4).encode("large").await.unwrap();
        assert!(
            matches!(paused, Encoded::Skipped(Skip::Paused(_))),
            "{paused:?}"
        );
        let report = f.encoder(4).scan().await;
        assert!(report.paused.is_some());
        assert_eq!(report.published, 0);

        // More stripes than one record can name.
        let tiny = Encoder::new(
            f.shard.clone(),
            Arc::clone(&f.writer),
            planner(6),
            Arc::new(f.clock.clone()),
            EncoderSettings {
                stripe_data_bytes: 1,
                ..SETTINGS
            },
        );
        assert_eq!(tiny.encode("large").await.unwrap(), skipped(Skip::TooLarge));
        assert!(f.writer.nodes().is_empty());

        // A scan encodes only what qualifies.
        let report = encoder.scan().await;
        assert_eq!(
            (report.published, report.rejected, report.failed),
            (1, 0, 0)
        );
        assert!(report.paused.is_none());
        for (key, coded) in [("large", true), ("small", false)] {
            let entry = f.shard.entry(key).await.unwrap().unwrap();
            assert_eq!(entry.object.unwrap().coded.is_some(), coded, "{key}");
        }
    });
}

#[test]
fn failed_writes_are_planned_again_without_their_nodes() {
    runtime().block_on(async {
        let f = Fixture::new().await;
        write(&f.shard, "k", &sample(5000, 5), 5).await;
        f.writer.fail([node(2)]);
        let encoder = f.encoder(7);
        assert!(matches!(
            encoder.encode("k").await.unwrap(),
            Encoded::Published(_)
        ));
        let coded = f
            .shard
            .entry("k")
            .await
            .unwrap()
            .unwrap()
            .object
            .unwrap()
            .coded
            .unwrap();
        for stripe in &coded.stripes {
            assert!(stripe.fragments().iter().all(|l| l.node != node(2)));
        }
    });
}

#[test]
fn an_attempt_whose_fragments_cannot_all_be_written_is_abandoned() {
    runtime().block_on(async {
        let f = Fixture::new().await;
        write(&f.shard, "k", &sample(5000, 6), 6).await;
        f.writer.fail([node(0), node(1)]);
        let encoder = f.encoder(6);
        let error = encoder.encode("k").await.unwrap_err();
        assert!(matches!(error, EncodeError::NoGeometry(_)), "{error}");
        // With room to re-plan, but every re-plan failing too.
        let f2 = Fixture::new().await;
        write(&f2.shard, "k", &sample(5000, 6), 6).await;
        f2.writer.fail((0..20).map(node));
        let encoder = Encoder::new(
            f2.shard.clone(),
            Arc::clone(&f2.writer),
            planner(20),
            Arc::new(f2.clock.clone()),
            EncoderSettings {
                replans: 1,
                ..SETTINGS
            },
        );
        let error = encoder.encode("k").await.unwrap_err();
        assert!(
            matches!(error, EncodeError::Fragments { stripe: 0, .. }),
            "{error}"
        );
        assert!(encoder.attempts().in_progress().is_empty());
        assert_eq!(f.steps().last(), Some(&EncodeStep::Abandoned));
        let entry = f.shard.entry("k").await.unwrap().unwrap();
        assert!(entry.object.unwrap().coded.is_none());
    });
}

#[test]
fn a_version_overwritten_while_it_is_encoded_is_not_coded() {
    runtime().block_on(async {
        let f = Fixture::new().await;
        write(&f.shard, "k", &sample(5000, 7), 7).await;
        let open = f.writer.hold();
        let encoder = Arc::new(f.encoder(6));
        let encoding = tokio::spawn({
            let encoder = Arc::clone(&encoder);
            async move { encoder.encode("k").await }
        });
        while f.steps().is_empty() {
            tokio::task::yield_now().await;
        }
        let attempt = f.events.lock().unwrap()[0].attempt;
        assert_eq!(
            encoder.attempts().state(attempt),
            Some(AttemptState::Writing)
        );
        // An overwrite of the same size and the same ETag still supersedes.
        let newer = write(&f.shard, "k", &sample(5000, 8), 7).await;
        open.send(true).unwrap();
        assert_eq!(
            encoding.await.unwrap().unwrap(),
            Encoded::Rejected(Rejection::Superseded)
        );
        let entry = f.shard.entry("k").await.unwrap().unwrap();
        assert_eq!(entry.version, newer);
        assert!(entry.object.unwrap().coded.is_none());
        assert!(matches!(
            f.steps().last(),
            Some(EncodeStep::Committed {
                published: false,
                ..
            })
        ));
    });
}

#[test]
fn an_abandoned_attempt_never_publishes() {
    runtime().block_on(async {
        let f = Fixture::new().await;
        write(&f.shard, "k", &sample(5000, 9), 9).await;
        let open = f.writer.hold();
        let encoder = Arc::new(f.encoder(6));
        let encoding = tokio::spawn({
            let encoder = Arc::clone(&encoder);
            async move { encoder.encode("k").await }
        });
        while f.steps().is_empty() {
            tokio::task::yield_now().await;
        }
        let attempt = f.events.lock().unwrap()[0].attempt;
        // Orphan reclamation's fence abandons it while it writes.
        assert!(encoder.attempts().abandon(attempt));
        let applied = f.shard.applied();
        open.send(true).unwrap();
        let error = encoding.await.unwrap().unwrap_err();
        assert!(
            matches!(error, EncodeError::Abandoned(a) if a == attempt),
            "{error}"
        );
        assert_eq!(f.shard.applied(), applied);
        assert_eq!(f.steps().last(), Some(&EncodeStep::Abandoned));
        assert_eq!(encoder.attempts().state(attempt), None);
    });
}

#[test]
fn attempt_numbers_are_never_reused_across_restarts() {
    runtime().block_on(async {
        let f = Fixture::new().await;
        write(&f.shard, "a", &sample(5000, 10), 10).await;
        write(&f.shard, "b", &sample(5000, 11), 11).await;
        f.encoder(6).encode("a").await.unwrap();
        let first = f.events.lock().unwrap()[0].attempt;
        assert_eq!(first, AttemptId::new(Epoch::new(1), 0));
        // A new encoder, as after a restart, reserves a new block.
        f.events.lock().unwrap().clear();
        f.encoder(6).encode("b").await.unwrap();
        let second = f.events.lock().unwrap()[0].attempt;
        assert_eq!(second, AttemptId::new(Epoch::new(1), 64));
        drop(f.disk);
    });
}
