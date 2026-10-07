//! Orphan reclamation (§8.4): a fragment node's reclaimer asks about its
//! fragments only once they are old enough and reclaims the orphans, a
//! store removes segments whose fragments are all reclaimed, and orphan
//! queries travel between nodes, malformed ones refused.

mod support;

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use proptest::prelude::*;
use prost::Message;
use skys3_ec::orphans::{
    MAX_SUSPECTS, OrphanQuery, OrphanVerdicts, PrimariesOf, Reclaimed, SuspectBody, Unanswered,
};
use skys3_ec::{
    Attempts, FragmentId, FragmentStore, OrphanClient, OrphanConfirmer, OrphanJudge,
    OrphanReclaimer, OrphanServer, Suspect, Verdict,
};
use skys3_index::{Index, IndexConfig};
use skys3_io::{BlockingPool, Clock, Disk, MonotonicClock, SimDisk, SimMount};
use skys3_log::{LogConfig, SegmentLog, ShardRef};
use skys3_net::{Frame, Header, MessageKind, TokioNetwork};
use skys3_shard::Shard;
use skys3_types::{
    AttemptId, BucketId, Epoch, NodeAddress, NodeId, ProposalId, ShardConfig, ShardId,
};
use support::pki::Pki;
use support::{fragment, runtime, shard, small_config};

/// Whom the reclaimer asks: verdicts scripted by fragment, every query
/// recorded, and no answer at all while `silent`.
#[derive(Clone, Default)]
struct Script {
    verdicts: Arc<Mutex<BTreeMap<FragmentId, Verdict>>>,
    asked: Arc<Mutex<Vec<Vec<FragmentId>>>>,
    silent: Arc<AtomicBool>,
}

impl Script {
    fn set(&self, id: FragmentId, verdict: Verdict) {
        self.verdicts.lock().unwrap().insert(id, verdict);
    }

    /// The fragments asked about since the last call, in order.
    fn asked(&self) -> Vec<FragmentId> {
        let mut asked: Vec<_> = self.asked.lock().unwrap().drain(..).flatten().collect();
        asked.sort();
        asked
    }
}

impl OrphanConfirmer for Script {
    async fn confirm(
        &self,
        shard: &ShardRef,
        suspects: &[Suspect],
    ) -> Result<Vec<Verdict>, Unanswered> {
        self.asked
            .lock()
            .unwrap()
            .push(suspects.iter().map(|s| s.id).collect());
        if self.silent.load(Ordering::SeqCst) {
            return Err(Unanswered {
                shard: shard.clone(),
                reason: "silent".to_owned(),
            });
        }
        let verdicts = self.verdicts.lock().unwrap();
        Ok(suspects
            .iter()
            .map(|s| verdicts.get(&s.id).copied().unwrap_or(Verdict::InProgress))
            .collect())
    }
}

fn paused_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap()
}

fn node(n: u8) -> NodeId {
    format!("node-{n}").parse().unwrap()
}

async fn put(store: &FragmentStore<SimMount>, seed: u64) -> FragmentId {
    let (header, payload) = fragment("photo", 4096, seed);
    store.write(&header, payload).await.unwrap()
}

#[test]
fn fragments_are_asked_about_once_old_enough_and_orphans_are_reclaimed() {
    paused_runtime().block_on(async {
        let disk = SimDisk::new(1);
        let (store, _) = FragmentStore::open(disk.mount(), small_config())
            .await
            .unwrap();
        let (a, b, c) = (
            put(&store, 1).await,
            put(&store, 2).await,
            put(&store, 3).await,
        );
        let script = Script::default();
        let reclaimed = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&reclaimed);
        let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::new());
        let after = Duration::from_secs(60);
        let reclaimer =
            OrphanReclaimer::new(node(1), vec![store.clone()], script.clone(), clock, after)
                .with_observer(Arc::new(move |r: &Reclaimed| {
                    seen.lock().unwrap().push(r.clone());
                }));

        // Too young to ask about.
        assert_eq!(reclaimer.sweep().await.asked, 0);
        tokio::time::advance(Duration::from_secs(30)).await;
        let d = put(&store, 4).await;
        assert_eq!(reclaimer.sweep().await.asked, 0);

        // The first three are old enough; the fourth is not.
        tokio::time::advance(Duration::from_secs(31)).await;
        script.set(a, Verdict::Orphan);
        script.set(b, Verdict::Referenced);
        let report = reclaimer.sweep().await;
        assert_eq!(script.asked(), [a, b, c]);
        assert_eq!(
            (report.reclaimed, report.referenced, report.in_progress),
            (1, 1, 1)
        );
        assert_eq!(store.len(a), None);
        assert!(store.len(b).is_some() && store.len(c).is_some());
        let observed = reclaimed.lock().unwrap().clone();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].node, node(1));
        assert_eq!(observed[0].shard, shard());
        assert_eq!(observed[0].suspect.id, a);
        assert_eq!(observed[0].suspect.key, "photo");
        assert_eq!(
            observed[0].suspect.attempt,
            AttemptId::new(Epoch::new(4), 1)
        );

        // A referenced fragment is not asked about again; one in progress
        // is, after another wait, with the fourth.
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(reclaimer.sweep().await.asked, 0);
        tokio::time::advance(Duration::from_secs(60)).await;
        script.silent.store(true, Ordering::SeqCst);
        let report = reclaimer.sweep().await;
        assert_eq!(script.asked(), [c, d]);
        assert_eq!(report.unanswered, 2);

        // Unanswered fragments are asked about again at the next sweep.
        script.silent.store(false, Ordering::SeqCst);
        script.set(c, Verdict::Orphan);
        let report = reclaimer.sweep().await;
        assert_eq!(script.asked(), [c, d]);
        assert_eq!((report.reclaimed, report.in_progress), (1, 1));
        assert_eq!(store.ids(), [b, d]);
    });
}

#[test]
fn a_segment_is_removed_once_all_its_fragments_are_reclaimed() {
    runtime().block_on(async {
        let disk = SimDisk::new(2);
        let (store, _) = FragmentStore::open(disk.mount(), small_config())
            .await
            .unwrap();
        // Three fragments of 80 KiB to each 256 KiB segment.
        let mut ids = Vec::new();
        for seed in 0..7 {
            let (header, payload) = fragment("photo", 80 * 1024, seed);
            ids.push(store.write(&header, payload).await.unwrap());
        }
        let segments: Vec<u64> = store.segments().iter().map(|s| s.id).collect();
        assert_eq!(segments.len(), 3);

        // Two of the first segment's three: it stays.
        assert!(store.reclaim(ids[0]).await.unwrap());
        assert!(store.reclaim(ids[1]).await.unwrap());
        assert!(!store.reclaim(ids[1]).await.unwrap());
        assert_eq!(store.segments().len(), 3);
        // The third: it is removed.
        assert!(store.reclaim(ids[2]).await.unwrap());
        let left: Vec<u64> = store.segments().iter().map(|s| s.id).collect();
        assert_eq!(left, segments[1..]);
        assert!(
            !disk
                .mount()
                .list()
                .await
                .unwrap()
                .iter()
                .any(|name| { name.contains(&format!("{:016x}", segments[0])) })
        );
        // The last segment stays, though all of it is reclaimed.
        assert!(store.reclaim(ids[6]).await.unwrap());
        assert_eq!(store.segments().len(), 2);
        assert_eq!(store.ids(), ids[3..6]);

        // Reclaiming is not recorded: recovery finds the fragments of the
        // segments that remain, and every acknowledged one still reads.
        drop(store);
        let (store, report) = FragmentStore::open(disk.mount(), small_config())
            .await
            .unwrap();
        assert_eq!(report.fragments, 4);
        assert_eq!(store.ids(), ids[3..]);
        for id in &ids[3..] {
            store.read(*id, 0..80 * 1024).await.unwrap();
        }
    });
}

fn suspect_strategy() -> impl Strategy<Value = Suspect> {
    (
        any::<u128>(),
        "[a-z0-9/._-]{1,64}",
        any::<u64>(),
        any::<u64>(),
    )
        .prop_map(|(id, key, epoch, number)| Suspect {
            id: FragmentId::new(id),
            key,
            attempt: AttemptId::new(Epoch::new(epoch), number),
        })
}

proptest! {
    #[test]
    fn queries_round_trip(
        suspects in prop::collection::vec(suspect_strategy(), 1..=MAX_SUSPECTS),
        number in any::<u8>(),
    ) {
        let shard = ShardRef::new(BucketId::new("b-7f3a").unwrap(), ShardId::new(number));
        let bytes = OrphanQuery::new(&shard, &suspects).encode_to_vec();
        let parsed = OrphanQuery::decode(bytes.as_slice()).unwrap().parse().unwrap();
        prop_assert_eq!(parsed, (shard, suspects));
    }

    #[test]
    fn verdicts_round_trip(verdicts in prop::collection::vec(1u8..=3, 0..64)) {
        let answer = OrphanVerdicts { verdicts: verdicts.clone(), error: String::new() };
        let bytes = answer.encode_to_vec();
        let parsed = OrphanVerdicts::decode(bytes.as_slice()).unwrap().parse(verdicts.len());
        let expected: Vec<Verdict> =
            verdicts.iter().map(|&b| Verdict::from_byte(b).unwrap()).collect();
        prop_assert_eq!(parsed, Ok(expected));
    }
}

#[test]
fn malformed_queries_and_answers_are_refused() {
    let one = SuspectBody {
        id: vec![0; 16],
        key: "photo".to_owned(),
        attempt_epoch: 1,
        attempt_number: 2,
    };
    let query = |bucket: &str, shard: u32, suspects: Vec<SuspectBody>| OrphanQuery {
        bucket: bucket.to_owned(),
        shard,
        suspects,
    };
    assert!(query("b-1", 3, vec![one.clone()]).parse().is_ok());
    let refused = [
        query("B 1", 3, vec![one.clone()]),
        query("b-1", 256, vec![one.clone()]),
        query("b-1", 3, Vec::new()),
        query("b-1", 3, vec![one.clone(); MAX_SUSPECTS + 1]),
        query(
            "b-1",
            3,
            vec![SuspectBody {
                id: vec![0; 15],
                ..one.clone()
            }],
        ),
        query(
            "b-1",
            3,
            vec![SuspectBody {
                key: String::new(),
                ..one.clone()
            }],
        ),
        query(
            "b-1",
            3,
            vec![SuspectBody {
                key: "k".repeat(1025),
                ..one
            }],
        ),
    ];
    for query in refused {
        assert!(query.parse().is_err(), "{query:?}");
    }

    let answer = |verdicts: Vec<u8>, error: &str| OrphanVerdicts {
        verdicts,
        error: error.to_owned(),
    };
    assert_eq!(
        answer(vec![1, 2, 3], "").parse(3),
        Ok(vec![
            Verdict::Referenced,
            Verdict::InProgress,
            Verdict::Orphan
        ])
    );
    assert_eq!(
        answer(Vec::new(), "not here").parse(1),
        Err("not here".to_owned())
    );
    assert!(answer(vec![1, 2], "").parse(3).is_err());
    assert!(answer(vec![0], "").parse(1).is_err());
    assert!(answer(vec![4], "").parse(1).is_err());
}

/// A shard alone in epoch 1 on node 2, with nothing written.
async fn alone_shard(disk: &SimDisk) -> Shard<SimMount> {
    let clock = Arc::new(MonotonicClock::new());
    let (log, _) = SegmentLog::open(disk.mount(), LogConfig::default(), clock)
        .await
        .unwrap();
    let index = Index::open_sim(&disk.mount(), "index.redb", &IndexConfig::default()).unwrap();
    let pool = BlockingPool::new("index", NonZeroUsize::MIN).unwrap();
    let config = ShardConfig {
        bucket_id: shard().bucket.clone(),
        shard: shard().shard,
        epoch: Epoch::new(1),
        primary: node(2),
        members: vec![node(2)],
        learners: Vec::new(),
        min_write_replicas: 1,
        replicas: 1,
        proposal_id: ProposalId::new("p-1").unwrap(),
    };
    Shard::open(&config, log, Arc::new(index), pool)
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queries_reach_the_primary_over_the_transport() {
    let pki = Pki::new();
    // Node 2 leads the shard; node 3 has no replica of it.
    let disk = SimDisk::new(3);
    let primary = alone_shard(&disk).await;
    let judges = OrphanServer::default();
    let pool = BlockingPool::new("judge", NonZeroUsize::MIN).unwrap();
    judges.insert(OrphanJudge::new(primary, Attempts::default(), pool));
    let mut peers = BTreeMap::new();
    for (n, server) in [(2, judges.clone()), (3, OrphanServer::default())] {
        let listener = pki
            .transport(&node(n))
            .bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let address: NodeAddress = listener.local_addr().unwrap().to_string().parse().unwrap();
        peers.insert(node(n), address);
        tokio::spawn(async move {
            loop {
                let Ok(incoming) = listener.accept().await else {
                    continue;
                };
                let server = server.clone();
                tokio::spawn(async move {
                    let connection = incoming.handshake().await.unwrap();
                    let (mut receiver, sender) = connection.into_split();
                    let first = receiver.recv().await.unwrap().unwrap();
                    server.serve((receiver, sender), first).await;
                });
            }
        });
    }
    // Node 4 is unknown, node 3 refuses, node 2 answers.
    let order = vec![node(4), node(3), node(2)];
    let primaries: PrimariesOf = Arc::new(move |_| order.clone());
    let client = OrphanClient::<TokioNetwork, SimMount>::new(
        node(1),
        pki.transport(&node(1)),
        peers.clone(),
        primaries,
        OrphanServer::default(),
    );
    let suspects = [
        Suspect {
            id: FragmentId::new(7),
            key: "photo".to_owned(),
            attempt: AttemptId::new(Epoch::new(1), 3),
        },
        Suspect {
            id: FragmentId::new(8),
            key: "photo".to_owned(),
            attempt: AttemptId::new(Epoch::new(2), 0),
        },
    ];
    let verdicts = client.confirm(&shard(), &suspects).await.unwrap();
    // Nothing references the first; the second is a later epoch's.
    assert_eq!(verdicts, [Verdict::Orphan, Verdict::InProgress]);

    // A shard no node leads is unanswered, with every node's reason.
    let other = ShardRef::new(shard().bucket.clone(), ShardId::new(9));
    let error = client.confirm(&other, &suspects).await.unwrap_err();
    assert_eq!(error.shard, other);
    assert!(
        error.reason.contains("no address for node node-4"),
        "{error}"
    );
    assert!(error.reason.contains("no replica of shard"), "{error}");

    // A node asks its own judge without a connection.
    let order = vec![node(2)];
    let local = OrphanClient::<TokioNetwork, SimMount>::new(
        node(2),
        pki.transport(&node(2)),
        BTreeMap::new(),
        Arc::new(move |_| order.clone()),
        judges,
    );
    let verdicts = local.confirm(&shard(), &suspects[..1]).await.unwrap();
    assert_eq!(verdicts, [Verdict::Orphan]);

    // A frame that is not a well-formed query is refused.
    let mut connection = pki
        .transport(&node(1))
        .connect(&node(2), &peers[&node(2)])
        .await
        .unwrap();
    let frame = Frame::new(
        Header::new(MessageKind::OrphanQuery),
        Bytes::from_static(b"\xff\xff"),
    );
    connection.send(&frame).await.unwrap();
    let answer = connection.recv().await.unwrap().unwrap();
    assert_eq!(answer.header.kind, MessageKind::OrphanVerdicts);
    let answer = OrphanVerdicts::decode(answer.payload.as_ref()).unwrap();
    assert!(answer.error.contains("malformed"), "{answer:?}");
}
