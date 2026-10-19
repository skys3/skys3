//! Fragment release (§8.7): a node keeps a released fragment for the
//! release delay and while reads hold it; a shard primary tells nodes what
//! writes released and records it with `EC_RELEASE`; orphans take the same
//! path; coded reads hold their fragments while they stream; and releases
//! and holds travel between nodes.

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use prost::Message;
use skys3_config::{EcConfig, FailureDomain, StorageConfig};
use skys3_coord::{Candidate, FragmentPlanner, GeometryPolicy, NodeState, Topology};
use skys3_ec::read::{HoldFuture, ReadFuture};
use skys3_ec::release::{FragmentNoted, FragmentRelease, ReleasedBody};
use skys3_ec::{
    CodecId, CodedRead, CodedStripe, FragmentBytes, FragmentHolds, FragmentId, FragmentLocation,
    FragmentReadClient, FragmentRequest, FragmentServer, FragmentSource, FragmentStore, Geometry,
    HoldSettings, OrphanConfirmer, OrphanReclaimer, PlannerSource, ReleaseClient, ReleaseMetrics,
    ReleaseSettings, ReleaseSink, ReleasedFragment, Releaser, Suspect, Verdict, codec, read_coded,
};
use skys3_index::{Index, IndexConfig};
use skys3_io::{BlockingPool, Clock, MonotonicClock, SimDisk, SimMount};
use skys3_log::record::{EcPublish, Extent, Put, PutData, ShardRef};
use skys3_log::{LogConfig, RecordBody, SegmentLog};
use skys3_net::{Frame, Header, MessageKind, TokioNetwork};
use skys3_obs::MetricsRegistry;
use skys3_shard::Shard;
use skys3_types::{
    AttemptId, BucketId, ETag, Epoch, EpochSeq, NodeAddress, NodeId, ProposalId, ShardConfig,
    ShardId,
};
use support::pki::Pki;
use support::{fragment, header, position, sample, shard, small_config};

const DELAY: Duration = Duration::from_secs(60);
const TTL: Duration = Duration::from_secs(30);

fn paused_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap()
}

fn node(n: usize) -> NodeId {
    format!("n{n}").parse().unwrap()
}

fn holds() -> FragmentHolds {
    let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::new());
    FragmentHolds::new(
        clock,
        HoldSettings {
            release_delay: DELAY,
            registration_ttl: TTL,
        },
    )
}

async fn store(seed: u64) -> FragmentStore<SimMount> {
    FragmentStore::open(SimDisk::new(seed).mount(), small_config())
        .await
        .unwrap()
        .0
}

async fn put(store: &FragmentStore<SimMount>, key: &str, seed: u64) -> FragmentId {
    let (header, payload) = fragment(key, 4096, seed);
    store.write(&header, payload).await.unwrap()
}

#[test]
fn settings_come_from_the_configuration() {
    let settings = HoldSettings::from_config(&EcConfig::default(), &StorageConfig::default());
    assert_eq!(settings.release_delay, Duration::from_secs(60));
    assert_eq!(settings.registration_ttl, Duration::from_secs(30));
    assert_eq!(
        ReleaseSettings::default().interval,
        ReleaseSettings::INTERVAL
    );
}

#[test]
fn released_fragments_wait_for_the_delay_and_for_the_reads_that_hold_them() {
    paused_runtime().block_on(async {
        let registry = MetricsRegistry::new();
        let metrics = ReleaseMetrics::register(&registry);
        let holds = holds().with_metrics(metrics.clone());
        let store = store(1).await;
        let (a, b, c) = (
            put(&store, "a", 1).await,
            put(&store, "b", 2).await,
            put(&store, "c", 3).await,
        );
        let stores = vec![store.clone()];
        assert!(holds.release(a, 4096));
        assert!(holds.release(b, 4096));
        assert!(!holds.release(b, 4096), "released once");
        assert!(holds.is_released(a) && !holds.is_released(c));
        holds.hold(&node(9), 1, [b]);
        assert!(holds.is_held(b) && !holds.is_held(a));
        assert_eq!(metrics.releasing(), 8192);

        // Within the delay, nothing goes.
        tokio::time::advance(Duration::from_secs(25)).await;
        holds.hold(&node(9), 1, [c]);
        let report = holds.sweep(&stores, 0.5).await;
        assert_eq!((report.reclaimed, report.kept), (0, 2));

        // Past it, what no read holds goes; a renewed hold keeps b.
        tokio::time::advance(Duration::from_secs(25)).await;
        holds.hold(&node(9), 1, []);
        tokio::time::advance(Duration::from_secs(11)).await;
        let report = holds.sweep(&stores, 0.5).await;
        assert_eq!((report.reclaimed, report.kept), (1, 1), "{report:?}");
        assert_eq!(report.reclaimed_bytes, 4096);
        assert!(store.len(a).is_none() && store.len(b).is_some());
        assert!(!holds.is_released(a));

        // Another reader's hold, or another registration, is its own.
        holds.end(&node(8), 1);
        holds.end(&node(9), 2);
        assert!(holds.is_held(b));
        holds.end(&node(9), 1);
        assert!(!holds.is_held(b));
        let report = holds.sweep(&stores, 0.5).await;
        assert_eq!((report.reclaimed, report.kept), (1, 0));
        assert!(store.len(b).is_none() && store.len(c).is_some());
        assert_eq!(
            (metrics.released(), metrics.releasing(), metrics.reclaimed()),
            (8192, 0, 8192)
        );

        // A hold its reader stops renewing lapses, and then holds nothing.
        holds.hold(&node(7), 1, [c]);
        assert!(holds.release(c, 4096));
        tokio::time::advance(DELAY + TTL).await;
        assert!(!holds.is_held(c));
        holds.hold(&node(7), 1, []);
        assert!(!holds.is_held(c), "a lapsed hold starts over");
        assert_eq!(holds.sweep(&stores, 0.5).await.reclaimed, 1);

        // A fragment no store holds any more is forgotten, uncounted.
        let gone = FragmentId::new(12345);
        holds.release(gone, 10);
        tokio::time::advance(DELAY).await;
        assert_eq!(holds.sweep(&stores, 0.5).await.reclaimed, 0);
        assert!(!holds.is_released(gone));
        assert_eq!(metrics.releasing(), 0);
        assert!(format!("{holds:?}").contains("FragmentHolds"));
        let text = registry.encode().unwrap();
        assert!(
            text.contains("skys3_fragment_reclaimed_bytes_total 12288"),
            "{text}"
        );
    });
}

#[test]
fn sweeps_compact_the_space_reclaimed() {
    paused_runtime().block_on(async {
        let holds = holds();
        let store = store(2).await;
        // 64 KiB fragments, three to a segment.
        let mut ids = Vec::new();
        for n in 0..7 {
            let (header, payload) = fragment(&format!("k{n}"), 65_536, n);
            ids.push(store.write(&header, payload).await.unwrap());
        }
        assert_eq!(store.segments().len(), 3);
        for n in [1, 2, 4, 5] {
            holds.release(ids[n], 65_536);
        }
        tokio::time::advance(DELAY).await;
        let report = holds.sweep(std::slice::from_ref(&store), 0.5).await;
        assert_eq!(report.reclaimed, 4);
        assert_eq!(
            (report.compaction.segments, report.compaction.copied),
            (2, 2),
            "{report:?}"
        );
        assert_eq!(report.failed, 0);
        assert_eq!(store.ids(), [ids[0], ids[3], ids[6]]);

        // The background sweep does the same.
        let task = tokio::spawn(holds.clone().run(vec![store.clone()], DELAY, 0.5));
        for n in [0, 3, 6] {
            holds.release(ids[n], 65_536);
        }
        tokio::time::sleep(DELAY * 3).await;
        task.abort();
        assert!(store.ids().is_empty());
    });
}

#[test]
fn nodes_release_only_fragments_whose_header_names_the_release() {
    paused_runtime().block_on(async {
        let store = store(3).await;
        let id = put(&store, "k", 1).await;
        let other = put(&store, "j", 2).await;
        let named = header("k", 4096, 1);
        let released = |key: &str, version: EpochSeq, fragment| ReleasedFragment {
            fragment,
            key: key.into(),
            version,
            stripe: 0,
            index: 1,
        };
        let plain = FragmentServer::new(vec![store.clone()]);
        assert!(plain.release(&shard(), &[]).await.is_err());
        assert!(plain.hold(&node(1), 1, &[id], false).is_err());

        let holds = holds();
        let server = FragmentServer::new(vec![store.clone()]).with_holds(holds.clone());
        assert!(server.holds().is_some());
        let wrong = vec![
            released("other", named.version, id),
            released("k", position(4, 99), id),
            released("k", named.version, FragmentId::new(1)),
            ReleasedFragment {
                stripe: 1,
                ..released("k", named.version, id)
            },
            ReleasedFragment {
                index: 2,
                ..released("k", named.version, id)
            },
        ];
        assert_eq!(server.release(&shard(), &wrong).await.unwrap(), (0, 5));
        let elsewhere = ShardRef::new(BucketId::new("b-other").unwrap(), ShardId::new(3));
        let right = vec![released("k", named.version, id)];
        assert_eq!(server.release(&elsewhere, &right).await.unwrap(), (0, 1));
        assert!(!holds.is_released(id));
        assert_eq!(server.release(&shard(), &right).await.unwrap(), (1, 0));
        assert!(holds.is_released(id) && !holds.is_released(other));

        assert_eq!(
            server
                .hold(&node(1), 1, &[id, FragmentId::new(1)], false)
                .unwrap(),
            (1, 1)
        );
        assert!(holds.is_held(id));
        assert_eq!(server.hold(&node(1), 1, &[], true).unwrap(), (0, 0));
        assert!(!holds.is_held(id));
    });
}

/// The fragment nodes a primary tells, in memory: which answer, and what
/// each was told.
#[derive(Default)]
struct Script {
    silent: Mutex<BTreeSet<NodeId>>,
    told: Mutex<BTreeMap<NodeId, Vec<ReleasedFragment>>>,
}

impl ReleaseSink for Script {
    async fn release(
        &self,
        node: &NodeId,
        _shard: &ShardRef,
        fragments: &[ReleasedFragment],
    ) -> Result<(), String> {
        if self.silent.lock().unwrap().contains(node) {
            return Err("silent".into());
        }
        self.told
            .lock()
            .unwrap()
            .entry(node.clone())
            .or_default()
            .extend_from_slice(fragments);
        Ok(())
    }
}

/// A planner whose topology lists nodes `nodes`.
fn planner(nodes: Vec<usize>) -> PlannerSource {
    Arc::new(move || {
        let candidates = nodes.iter().map(|&n| Candidate {
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

/// Puts `len` bytes at `key` with ETag `tag`, returning its position.
async fn write(shard: &Shard<SimMount>, key: &str, len: usize, tag: u64) -> EpochSeq {
    let data = sample(len, tag);
    let extent = Extent {
        key: key.to_owned(),
        offset: 0,
        data: Bytes::from(data),
    };
    let extent = shard.append_extent(extent).await.unwrap();
    let body = RecordBody::Put(Put {
        key: key.to_owned(),
        size: len as u64,
        last_modified_ms: 1,
        etag: ETag::new(format!("{tag:032x}")).unwrap(),
        inherited_identity: None,
        metadata: BTreeMap::new(),
        tags: BTreeMap::new(),
        checksums: BTreeMap::new(),
        copy_source: None,
        data: PutData::Extents(vec![extent]),
    });
    shard.commit(body).await.unwrap().position
}

/// Publishes a one-stripe 3+2 layout of `key`'s version at `version` with
/// fragment `i` on node `i + 1`, IDs from `first`.
async fn publish(
    shard: &Shard<SimMount>,
    key: &str,
    version: EpochSeq,
    len: u64,
    tag: u64,
    first: u128,
) {
    let fragments = (0..5)
        .map(|i| FragmentLocation {
            node: node(i + 1),
            fragment: FragmentId::new(first + i as u128),
        })
        .collect();
    let stripe = CodedStripe::new(
        0,
        0,
        len,
        Geometry::new(3, 2).unwrap(),
        CodecId::CURRENT,
        fragments,
    )
    .unwrap();
    let body = RecordBody::EcPublish(EcPublish {
        key: key.into(),
        version,
        etag: ETag::new(format!("{tag:032x}")).unwrap(),
        attempt: AttemptId::new(Epoch::new(1), 1),
        size: len,
        stripes: vec![stripe],
    });
    shard.commit(body).await.unwrap();
}

fn pending(shard: &Shard<SimMount>) -> usize {
    shard
        .index()
        .read()
        .unwrap()
        .released(shard.shard(), None, usize::MAX)
        .unwrap()
        .len()
}

#[test]
fn a_primary_tells_nodes_what_writes_released_and_records_it() {
    support::runtime().block_on(async {
        let disk = SimDisk::new(4);
        let shard = alone(&disk).await;
        let version = write(&shard, "k", 300, 1).await;
        publish(&shard, "k", version, 300, 1, 100).await;
        let script = Arc::new(Script::default());
        let pool = BlockingPool::new("release", NonZeroUsize::MIN).unwrap();
        // Node 5 is gone from the cluster; node 1 does not answer.
        let releaser = Releaser::new(
            shard.clone(),
            Arc::clone(&script),
            planner(vec![0, 1, 2, 3, 4]),
            pool,
            ReleaseSettings::default(),
        );
        assert_eq!(releaser.pass().await.pending, 0);

        write(&shard, "k", 10, 2).await;
        assert_eq!(pending(&shard), 5);
        script.silent.lock().unwrap().insert(node(1));
        let report = releaser.pass().await;
        assert_eq!(
            (
                report.pending,
                report.told,
                report.forgotten,
                report.unanswered
            ),
            (5, 3, 1, 1),
            "{report:?}"
        );
        assert_eq!((report.records, report.removed), (1, 4));
        assert_eq!(pending(&shard), 1);
        let told = script.told.lock().unwrap().clone();
        assert_eq!(
            told.keys().cloned().collect::<Vec<_>>(),
            [node(2), node(3), node(4)]
        );
        assert_eq!(
            told[&node(3)],
            [ReleasedFragment {
                fragment: FragmentId::new(102),
                key: "k".into(),
                version,
                stripe: 0,
                index: 2,
            }]
        );

        // Once node 1 answers, the rest goes.
        script.silent.lock().unwrap().clear();
        let report = releaser.pass().await;
        assert_eq!((report.told, report.records, report.removed), (1, 1, 1));
        assert_eq!(pending(&shard), 0);
        assert_eq!(releaser.pass().await.pending, 0);
    });
}

/// A primary that answers every orphan query with an orphan verdict.
struct Orphans;

impl OrphanConfirmer for Orphans {
    async fn confirm(
        &self,
        _shard: &ShardRef,
        suspects: &[Suspect],
    ) -> Result<Vec<Verdict>, skys3_ec::orphans::Unanswered> {
        Ok(vec![Verdict::Orphan; suspects.len()])
    }
}

#[test]
fn orphans_leave_like_released_fragments() {
    paused_runtime().block_on(async {
        let store = store(5).await;
        let id = put(&store, "k", 1).await;
        let holds = holds();
        let primary = Orphans;
        let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::new());
        let after = Duration::from_secs(10);
        let reclaimer = OrphanReclaimer::new(node(1), vec![store.clone()], primary, clock, after)
            .with_holds(holds.clone());
        reclaimer.sweep().await;
        tokio::time::advance(after).await;
        let report = reclaimer.sweep().await;
        assert_eq!(report.reclaimed, 1);
        // Released, not reclaimed: a read may still hold it.
        assert!(store.len(id).is_some() && holds.is_released(id));
        holds.hold(&node(2), 1, [id]);
        tokio::time::advance(DELAY).await;
        // Not asked about again while released.
        assert_eq!(reclaimer.sweep().await.asked, 0);
        holds.hold(&node(2), 1, [id]);
        assert_eq!(
            holds
                .sweep(std::slice::from_ref(&store), 0.5)
                .await
                .reclaimed,
            0
        );
        holds.end(&node(2), 1);
        assert_eq!(
            holds
                .sweep(std::slice::from_ref(&store), 0.5)
                .await
                .reclaimed,
            1
        );
        assert!(store.len(id).is_none());
    });
}

/// Fragments in memory, read through a source that takes holds and
/// records every hold it is asked for, in order with the reads.
#[derive(Debug, Default)]
struct Held {
    fragments: BTreeMap<FragmentId, Bytes>,
    log: Mutex<Vec<String>>,
}

impl FragmentSource for Held {
    fn read(&self, request: FragmentRequest) -> ReadFuture<'_> {
        Box::pin(async move {
            self.log
                .lock()
                .unwrap()
                .push(format!("read {}", request.node));
            let data = self.fragments[&request.fragment]
                .slice(request.range.start as usize..request.range.end as usize);
            Ok(FragmentBytes {
                crc32c: crc32c::crc32c(&data),
                data,
            })
        })
    }

    fn register(&self) -> Option<(u64, Duration)> {
        Some((7, Duration::from_secs(10)))
    }

    fn hold(
        &self,
        node: NodeId,
        registration: u64,
        fragments: Vec<FragmentId>,
        end: bool,
    ) -> HoldFuture<'_> {
        assert_eq!(registration, 7);
        let verb = if end { "end" } else { "hold" };
        self.log
            .lock()
            .unwrap()
            .push(format!("{verb} {node} {}", fragments.len()));
        Box::pin(std::future::ready(if node == self::node(3) {
            Err("refused".to_owned())
        } else {
            Ok(())
        }))
    }
}

#[test]
fn coded_reads_hold_their_fragments_while_they_stream() {
    paused_runtime().block_on(async {
        // One 2+1 stripe of 3 MiB over nodes 1 to 3: three pieces.
        let geometry = Geometry::new(2, 1).unwrap();
        let data = sample(3 << 20, 5);
        let encoded = codec(CodecId::CURRENT)
            .unwrap()
            .encode(geometry, &data)
            .unwrap();
        let mut source = Held::default();
        let mut locations = Vec::new();
        for (i, fragment) in encoded.into_iter().enumerate() {
            let id = FragmentId::new(i as u128 + 1);
            source.fragments.insert(id, Bytes::from(fragment));
            locations.push(FragmentLocation {
                node: node(i + 1),
                fragment: id,
            });
        }
        let stripe = CodedStripe::new(
            0,
            0,
            data.len() as u64,
            geometry,
            CodecId::CURRENT,
            locations,
        )
        .unwrap();
        let source = Arc::new(source);
        let read = CodedRead {
            shard: shard(),
            key: "k".into(),
            version: position(1, 1),
            etag: ETag::new("e").unwrap(),
            size: data.len() as u64,
            stripes: vec![stripe],
            range: 0..data.len() as u64,
        };
        let mut body = read_coded(Arc::clone(&source) as Arc<dyn FragmentSource>, read)
            .await
            .unwrap();
        let log = || source.log.lock().unwrap().clone();
        // Every node of the stripe is held before the first fetch.
        let first = log();
        assert_eq!(first.len(), 4, "{first:?}");
        assert!(first[..3].iter().all(|entry| entry.starts_with("hold")));
        assert_eq!(first[3], "read n1");

        // A consumer that dawdles: the holds are renewed meanwhile.
        let mut read_back = Vec::new();
        read_back.extend_from_slice(&body.recv().await.unwrap().unwrap());
        tokio::time::sleep(Duration::from_secs(25)).await;
        let renewals = log()
            .iter()
            .filter(|entry| entry.starts_with("hold"))
            .count();
        assert_eq!(renewals, 3 + 2 * 3);
        while let Some(piece) = body.recv().await {
            read_back.extend_from_slice(&piece.unwrap());
        }
        assert_eq!(read_back, data);
        // Once the stream ends, the holds end.
        tokio::time::sleep(Duration::from_secs(1)).await;
        let ends: BTreeSet<String> = log()
            .into_iter()
            .filter(|entry| entry.starts_with("end"))
            .collect();
        assert_eq!(ends.len(), 3, "{:?}", log());
        tokio::time::sleep(Duration::from_secs(60)).await;
        let after = log();
        tokio::time::sleep(Duration::from_secs(60)).await;
        assert_eq!(log(), after, "no renewal after the end");
    });
}

const WAIT: Duration = Duration::from_secs(30);

async fn timed<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(WAIT, future).await.expect("in time")
}

fn address(addr: SocketAddr) -> NodeAddress {
    addr.to_string().parse().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn releases_and_holds_travel_between_nodes() {
    let pki = Pki::new();
    let fragment_store = store(6).await;
    let id = put(&fragment_store, "k", 1).await;
    let holds = holds();
    let server = FragmentServer::new(vec![fragment_store]).with_holds(holds.clone());
    let listener = pki
        .transport(&node(2))
        .bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let serving = server.clone();
    tokio::spawn(async move { serving.serve_listener(listener).await });
    let peers = BTreeMap::from([(node(2), address(addr))]);

    // A read's hold, its renewal, and its end, on the read connections.
    let reader =
        FragmentReadClient::<_, SimMount>::new(node(1), pki.transport(&node(1)), peers.clone())
            .with_holds(Duration::from_secs(10));
    let (registration, renew) = reader.register().unwrap();
    assert_eq!(renew, Duration::from_secs(10));
    assert_ne!(reader.register().unwrap().0, registration);
    timed(reader.hold(node(2), registration, vec![id], false))
        .await
        .unwrap();
    assert!(holds.is_held(id));
    timed(reader.hold(node(2), registration, Vec::new(), true))
        .await
        .unwrap();
    assert!(!holds.is_held(id));
    let unknown = timed(reader.hold(node(4), registration, vec![id], false)).await;
    assert!(unknown.unwrap_err().contains("no address"));
    let plain =
        FragmentReadClient::<_, SimMount>::new(node(1), pki.transport(&node(1)), peers.clone());
    assert!(plain.register().is_none());

    // A release: the wrong version is not held, the right one is noted.
    let client = ReleaseClient::<_, SimMount>::new(node(1), pki.transport(&node(1)), peers, None);
    let named = header("k", 4096, 1);
    let wrong = ReleasedFragment {
        fragment: id,
        key: "k".into(),
        version: position(9, 9),
        stripe: 0,
        index: 1,
    };
    timed(client.release(&node(2), &shard(), std::slice::from_ref(&wrong)))
        .await
        .unwrap();
    assert!(!holds.is_released(id));
    let right = ReleasedFragment {
        version: named.version,
        ..wrong.clone()
    };
    timed(client.release(&node(2), &shard(), std::slice::from_ref(&right)))
        .await
        .unwrap();
    assert!(holds.is_released(id));
    let unknown = timed(client.release(&node(4), &shard(), std::slice::from_ref(&right))).await;
    assert!(unknown.unwrap_err().contains("no address"));

    // The node's own fragments need no connection.
    let local_store = store(7).await;
    let local_id = put(&local_store, "k", 1).await;
    let local_holds = self::holds();
    let local = FragmentServer::new(vec![local_store]).with_holds(local_holds.clone());
    let own = ReleaseClient::<TokioNetwork, _>::new(
        node(1),
        pki.transport(&node(1)),
        BTreeMap::new(),
        Some(local.clone()),
    );
    let mine = ReleasedFragment {
        fragment: local_id,
        ..right.clone()
    };
    own.release(&node(1), &shard(), &[mine]).await.unwrap();
    assert!(local_holds.is_released(local_id));
    let own_reader = FragmentReadClient::new(node(1), pki.transport(&node(1)), BTreeMap::new())
        .with_local(local);
    own_reader
        .hold(node(1), 3, vec![local_id], false)
        .await
        .unwrap();
    assert!(local_holds.is_held(local_id));

    // Malformed releases are refused with a reason.
    let mut connection = timed(pki.transport(&node(1)).connect(&node(2), &address(addr)))
        .await
        .unwrap();
    let bad = [
        FragmentRelease {
            bucket: "Not A Bucket".into(),
            shard: 3,
            fragments: Vec::new(),
        },
        FragmentRelease {
            fragments: Vec::new(),
            ..FragmentRelease::new(&shard(), std::slice::from_ref(&right))
        },
        FragmentRelease {
            fragments: vec![ReleasedBody {
                id: vec![1, 2, 3],
                key: "k".into(),
                version_epoch: 1,
                version_seq: 1,
                stripe: 0,
                index: 0,
            }],
            ..FragmentRelease::new(&shard(), std::slice::from_ref(&right))
        },
        FragmentRelease {
            fragments: vec![ReleasedBody {
                id: id.get().to_le_bytes().to_vec(),
                key: String::new(),
                version_epoch: 1,
                version_seq: 1,
                stripe: 0,
                index: 0,
            }],
            ..FragmentRelease::new(&shard(), std::slice::from_ref(&right))
        },
        FragmentRelease {
            fragments: vec![ReleasedBody {
                id: id.get().to_le_bytes().to_vec(),
                key: "k".into(),
                version_epoch: 1,
                version_seq: 1,
                stripe: 0,
                index: 255,
            }],
            ..FragmentRelease::new(&shard(), std::slice::from_ref(&right))
        },
        FragmentRelease {
            shard: 300,
            ..FragmentRelease::new(&shard(), &[right])
        },
    ];
    for body in bad {
        let frame = Frame::new(
            Header::new(MessageKind::FragmentRelease),
            body.encode_to_vec(),
        );
        timed(connection.send(&frame)).await.unwrap();
        let answer = timed(connection.recv()).await.unwrap().unwrap();
        assert_eq!(answer.header.kind, MessageKind::FragmentNoted);
        let noted = FragmentNoted::decode(answer.payload.as_ref()).unwrap();
        assert!(noted.parse().is_err(), "{body:?}");
    }
    let garbage = Frame::new(Header::new(MessageKind::FragmentRelease), vec![0xff; 8]);
    timed(connection.send(&garbage)).await.unwrap();
    let answer = timed(connection.recv()).await.unwrap().unwrap();
    let noted = FragmentNoted::decode(answer.payload.as_ref()).unwrap();
    assert!(noted.parse().unwrap_err().contains("malformed"));
}

/// A source that only reads.
#[derive(Debug)]
struct Plain;

impl FragmentSource for Plain {
    fn read(&self, request: FragmentRequest) -> ReadFuture<'_> {
        Box::pin(std::future::ready(Err(
            skys3_ec::FragmentReadError::Unreachable {
                node: request.node,
                reason: "plain".into(),
            },
        )))
    }
}

#[test]
fn a_source_without_holds_registers_nothing() {
    support::runtime().block_on(async {
        assert!(Plain.register().is_none());
        let error = Plain.hold(node(1), 1, Vec::new(), false).await;
        assert!(error.is_err());
    });
}
