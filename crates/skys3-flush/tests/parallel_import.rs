//! The namespace import listing key ranges in parallel (design §7.7,
//! §9.1): ranges discovered from the remote's names, a checkpoint per
//! range that a restart resumes, one rate limit over every stream, and an
//! import rate that grows with the stream count against a remote 100 ms
//! away.

mod support;

use std::collections::BTreeSet;
use std::time::Duration;

use skys3_flush::{FlushMetrics, FlushService, FlushSettings, ImportStatus};
use skys3_index::{ImportCheckpoint, ImportRanges};
use skys3_io::SimMount;
use skys3_obs::MetricsRegistry;
use skys3_remote::probe::ConditionalProbe;
use skys3_remote::{ObjectStore, PutObject};
use skys3_sim::SimS3;
use skys3_sim::s3::{SimS3Config, SimS3Faults};
use skys3_types::{
    BucketDocument, BucketId, BucketMode, BucketName, ProposalId, RemoteTarget, ShardCount,
};
use support::{Node, Patience, cluster, runtime, settings, shard_ref};
use tokio::time::Instant;

const PREFIX: &str = "team/";

/// A round trip of 100 ms: 50 ms each way.
const FAR: SimS3Faults = SimS3Faults {
    min_delay: Duration::from_millis(50),
    max_delay: Duration::from_millis(50),
    ..SimS3Faults::NONE
};

fn bucket() -> BucketDocument {
    BucketDocument {
        bucket_id: shard_ref().bucket,
        name: BucketName::new("photos").unwrap(),
        mode: BucketMode::WriteBack,
        shards: ShardCount::new(1).unwrap(),
        replicas: 1,
        min_write_replicas: 1,
        clean_copies: 1,
        target: Some(RemoteTarget {
            endpoint: "https://s3.example".to_owned(),
            bucket: "remote".to_owned(),
            prefix: Some(PREFIX.to_owned()),
        }),
        created_unix_ms: 0,
        lifecycle: None,
        proposal_id: ProposalId::new("p-1").unwrap(),
    }
}

/// A service whose imports list pages of `page_keys` keys with `streams`
/// streams, at most `keys_per_second` keys a second.
fn service(
    store: &SimS3,
    page_keys: u32,
    keys_per_second: u64,
    streams: usize,
) -> FlushService<SimS3, SimMount> {
    let store = store.clone();
    let settings = FlushSettings {
        import_page_keys: page_keys,
        import_keys_per_second: keys_per_second,
        import_streams: streams,
        ..settings()
    };
    FlushService::new(
        cluster(),
        settings,
        Box::new(move |_: &RemoteTarget| store.clone()),
        FlushMetrics::register(&MetricsRegistry::new()),
    )
}

/// The keys of `dirs` folders of `per_dir` keys each, in order.
fn folders(dirs: u32, per_dir: u32) -> Vec<String> {
    (0..dirs)
        .flat_map(|dir| (0..per_dir).map(move |n| format!("d{dir:02}/k{n:03}")))
        .collect()
}

/// A remote holding `keys` under the prefix.
async fn remote(seed: u64, keys: &[String]) -> SimS3 {
    let store = SimS3::new(seed, SimS3Config::default());
    for key in keys {
        let request = PutObject::new(format!("{PREFIX}{key}"), "body");
        store.put_object(request).await.unwrap();
    }
    store
}

/// The bucket's import status.
fn status(service: &FlushService<SimS3, SimMount>) -> ImportStatus {
    service.status(&bucket().bucket_id).unwrap().import.unwrap()
}

/// Follows the bucket until its import is done, and returns its status.
async fn import(service: &FlushService<SimS3, SimMount>, node: &Node) -> ImportStatus {
    let patience = Patience::new();
    loop {
        service.reconcile(&[bucket()], &node.set).await;
        let status = status(service);
        if status.checkpoint == ImportCheckpoint::Done {
            return status;
        }
        assert!(
            !patience.is_exhausted(),
            "the import never finished: {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// The keys the index holds.
async fn indexed(node: &Node) -> Vec<String> {
    let entries = node.shard.entries(None, usize::MAX).await.unwrap();
    entries.into_iter().map(|(key, _)| key).collect()
}

/// Whether every range has imported a page and none is done.
fn every_range_running(ranges: &ImportRanges) -> bool {
    ranges.ranges().iter().all(|range| {
        matches!(
            &range.checkpoint,
            ImportCheckpoint::Running { after: Some(_) }
        )
    })
}

#[test]
fn a_parallel_import_lists_its_ranges_together_and_imports_every_key_once() {
    runtime().block_on(async {
        let node = Node::open(91).await;
        let keys = folders(6, 25);
        let store = remote(91, &keys).await;
        store.set_faults(FAR);
        let service = service(&store, 10, 1_000_000, 4);
        service.reconcile(&[bucket()], &node.set).await;
        // Every range makes progress before any is done.
        let patience = Patience::new();
        loop {
            let status = status(&service);
            if status.ranges.ranges().len() == 4 && every_range_running(&status.ranges) {
                let stored = node.set.import_ranges(&bucket().bucket_id).await.unwrap();
                let stored = stored.unwrap();
                assert_eq!(stored.ranges().len(), 4);
                assert!(every_range_running(&stored), "{stored:?}");
                break;
            }
            assert!(status.checkpoint != ImportCheckpoint::Done, "{status:?}");
            assert!(!patience.is_exhausted(), "{status:?}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let status = import(&service, &node).await;
        assert_eq!((status.imported, status.error), (150, None));
        assert_eq!(status.ranges, ImportCheckpoint::Done.into());
        assert_eq!(indexed(&node).await, keys);
        let stored = node.set.import_ranges(&bucket().bucket_id).await.unwrap();
        assert_eq!(stored, Some(ImportCheckpoint::Done.into()));
        service.shutdown().await;
    });
}

#[test]
fn a_restart_mid_import_resumes_every_range() {
    runtime().block_on(async {
        let node = Node::open_inline(92).await;
        let keys = folders(4, 40);
        let store = remote(92, &keys).await;
        // Four streams from the bucket's settings, at 40 keys a second.
        let config: skys3_config::Config = "[cluster]\ncluster_id = \"c-test\"\n\
             [control_store]\netcd_endpoints = [\"https://etcd.invalid:2379\"]\n\
             [buckets.photos]\nimport_parallel_streams = 4"
            .parse()
            .unwrap();
        let first = service(&store, 10, 40, 1).with_buckets(config.buckets().clone());
        first.reconcile(&[bucket()], &node.set).await;
        let patience = Patience::new();
        while !every_range_running(&status(&first).ranges) {
            assert!(!patience.is_exhausted(), "{:?}", status(&first));
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        first.shutdown().await;
        let stored = node.set.import_ranges(&bucket().bucket_id).await.unwrap();
        let stored = stored.unwrap();
        assert_eq!(stored.ranges().len(), 4);
        assert!(every_range_running(&stored), "{stored:?}");
        let passed = keys.iter().filter(|key| stored.passed(key)).count();
        assert!(passed < keys.len());
        let indexed_before = indexed(&node).await.len();
        assert!(indexed_before >= passed);

        // One stream now: the stored ranges resume as they are, each from
        // its checkpoint, and no key they passed is listed again.
        let restarted = service(&store, 10, 1_000_000, 1);
        let status = import(&restarted, &node).await;
        assert_eq!(status.imported as usize, keys.len() - passed);
        assert_eq!(indexed(&node).await, keys);
        restarted.shutdown().await;
    });
}

#[test]
fn every_stream_shares_one_rate_limit() {
    runtime().block_on(async {
        let node = Node::open(93).await;
        let keys = folders(4, 10);
        let store = remote(93, &keys).await;
        // 40 keys at 20 a second, in four ranges of pages of ten keys.
        let service = service(&store, 10, 20, 4);
        let started = Instant::now();
        let patience = Patience::new();
        loop {
            service.reconcile(&[bucket()], &node.set).await;
            let imported = indexed(&node).await.len();
            let elapsed = started.elapsed().as_secs_f64();
            assert!(
                imported as f64 <= 20.0 * elapsed + 1e-6,
                "{imported} keys after {elapsed} s"
            );
            if status(&service).checkpoint == ImportCheckpoint::Done {
                assert_eq!(imported, 40);
                break;
            }
            assert!(!patience.is_exhausted(), "{:?}", status(&service));
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(started.elapsed() >= Duration::from_secs(2));
        assert_eq!(status(&service).ranges.ranges().len(), 1);
        service.shutdown().await;
    });
}

#[test]
fn the_import_rate_scales_with_the_stream_count_against_a_100_ms_target() {
    let keys = folders(8, 500);
    let mut seconds = Vec::new();
    for streams in [1, 2, 4, 8] {
        let elapsed = runtime().block_on(async {
            // The index works on the runtime's thread, so the paused clock
            // moves only for the remote's delays.
            let node = Node::open_inline(94).await;
            let store = remote(94, &keys).await;
            store.set_faults(FAR);
            let service = service(&store, 100, 1_000_000, streams);
            let started = Instant::now();
            let status = import(&service, &node).await;
            let elapsed = started.elapsed().as_secs_f64();
            assert_eq!(status.imported, 4000);
            let indexed: BTreeSet<String> = indexed(&node).await.into_iter().collect();
            assert_eq!(indexed.len(), 4000);
            service.shutdown().await;
            elapsed
        });
        seconds.push(elapsed);
    }
    // One stream lists 40 pages, a round trip each.
    assert!(seconds[0] >= 4.0, "{seconds:?}");
    // Doubling the streams nearly halves the time; discovery costs two
    // round trips, and each range a last page.
    assert!(seconds[1] <= seconds[0] * 0.75, "{seconds:?}");
    assert!(seconds[2] <= seconds[0] * 0.45, "{seconds:?}");
    assert!(seconds[3] <= seconds[0] * 0.3, "{seconds:?}");
}

#[test]
fn shutdown_waits_for_a_checkpoint_write_in_flight() {
    runtime().block_on(async {
        // A real pool thread, which finishes a queued job whether or not
        // anyone still waits for it.
        let node = Node::open(95).await;
        // Keys under the probe's scratch prefix are listed but never
        // imported, so a page uses the index only to store its
        // checkpoint, and a bucket with no shard open here has no flusher
        // that could use it. Far more pages than the test lets through.
        let scratch = ConditionalProbe::SCRATCH_DIR;
        let keys: Vec<String> = (0..990).map(|n| format!("{scratch}k{n:03}")).collect();
        let store = remote(95, &keys).await;
        let bucket = BucketDocument {
            bucket_id: BucketId::new("b-elsewhere").unwrap(),
            ..bucket()
        };
        let checkpoint = |service: &FlushService<SimS3, SimMount>| {
            service
                .status(&bucket.bucket_id)
                .unwrap()
                .import
                .unwrap()
                .checkpoint
        };
        // About a page of ten keys a second, on the paused clock.
        let service = service(&store, 10, 10, 1);
        service
            .reconcile(std::slice::from_ref(&bucket), &node.set)
            .await;
        let patience = Patience::new();
        while checkpoint(&service) == (ImportCheckpoint::Running { after: None }) {
            assert!(!patience.is_exhausted(), "{:?}", checkpoint(&service));
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Hold the index's pool with a job that runs once every job queued
        // before it is done, and then blocks until the gate opens.
        let (started, holding) = std::sync::mpsc::channel::<()>();
        let (open, gate) = std::sync::mpsc::channel::<()>();
        drop(node.pool.run(move || {
            started.send(()).unwrap();
            let _ = gate.recv();
        }));
        let patience = Patience::new();
        while holding.try_recv().is_err() {
            assert!(!patience.is_exhausted(), "the pool never ran the hold");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        // Long enough for the stream to show what was stored before the
        // hold and to queue its next page's checkpoint behind it, where it
        // stays: the stream goes no further until that write is done.
        tokio::time::sleep(Duration::from_secs(3)).await;
        let ImportCheckpoint::Running {
            after: Some(before),
        } = checkpoint(&service)
        else {
            panic!("the import ended: {:?}", checkpoint(&service));
        };
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(
            checkpoint(&service),
            ImportCheckpoint::Running {
                after: Some(before.clone())
            },
            "a checkpoint was stored while the pool was held"
        );

        let shutdown = service.shutdown();
        tokio::pin!(shutdown);
        let early = tokio::time::timeout(Duration::from_secs(5), &mut shutdown).await;
        assert!(
            early.is_err(),
            "shutdown returned with a checkpoint write still queued"
        );
        open.send(()).unwrap();
        // The pool thread runs on the real clock, which the paused one
        // does not wait for.
        let patience = Patience::new();
        while tokio::time::timeout(Duration::from_millis(10), &mut shutdown)
            .await
            .is_err()
        {
            assert!(!patience.is_exhausted(), "shutdown never finished");
        }
        // The queued write, of the page after `before`, landed before
        // shutdown returned, and nothing came after it.
        let page = |key: &str| -> usize { key[key.len() - 3..].parse().unwrap() };
        let next = format!("{scratch}k{:03}", page(&before) + 10);
        let stored = node.set.import_ranges(&bucket.bucket_id).await.unwrap();
        assert_eq!(
            stored.map(|ranges| ranges.position()),
            Some(ImportCheckpoint::Running { after: Some(next) })
        );
    });
}

#[test]
fn a_bucket_followed_again_resumes_its_ranges() {
    runtime().block_on(async {
        let node = Node::open_inline(96).await;
        let keys = folders(4, 30);
        let store = remote(96, &keys).await;
        let service = service(&store, 10, 40, 4);
        service.reconcile(&[bucket()], &node.set).await;
        let patience = Patience::new();
        while !every_range_running(&status(&service).ranges) {
            assert!(!patience.is_exhausted(), "{:?}", status(&service));
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // The bucket is no longer followed, then followed again: the new
        // import waits for the old one to stop, and resumes its ranges.
        service.reconcile(&[], &node.set).await;
        assert!(service.status(&bucket().bucket_id).is_none());
        let stored = node.set.import_ranges(&bucket().bucket_id).await.unwrap();
        let stored = stored.unwrap();
        assert_eq!(stored.ranges().len(), 4);
        let passed = keys.iter().filter(|key| stored.passed(key)).count();
        let status = import(&service, &node).await;
        assert!(status.imported as usize <= keys.len() - passed);
        assert_eq!(indexed(&node).await, keys);
        service.shutdown().await;
    });
}
