//! Tests of the flush service: following a node's buckets and shards, the
//! capability probe, status, and metrics.

mod support;

use std::sync::Arc;
use std::time::Duration;

use skys3_flush::{
    DEFAULT_CREDENTIALS, DirtyBudget, FlushMetrics, FlushService, FlushSettings, ProbeStatus,
    profile_credentials,
};
use skys3_io::{ManualWallClock, SimMount, WallClock};
use skys3_obs::MetricsRegistry;
use skys3_remote::probe::ConditionalOperation;
use skys3_remote::{ObjectStore, PutObject};
use skys3_sim::SimS3;
use skys3_sim::s3::{Conditionals, Fault, Operation, SimS3Config, SimS3Faults};
use skys3_types::{
    BucketDocument, BucketMode, BucketName, Epoch, ProposalId, RemoteTarget, ShardConfig,
    ShardCount,
};
use support::{Node, Patience, cluster, identity, runtime, settings, shard_ref};

fn bucket(mode: BucketMode) -> BucketDocument {
    BucketDocument {
        bucket_id: shard_ref().bucket,
        name: BucketName::new("photos").unwrap(),
        mode,
        shards: ShardCount::new(1).unwrap(),
        replicas: 1,
        min_write_replicas: 1,
        clean_copies: 1,
        target: (mode == BucketMode::WriteBack).then(|| RemoteTarget {
            endpoint: "https://s3.example".to_owned(),
            bucket: "remote".to_owned(),
            prefix: Some("team/".to_owned()),
        }),
        created_unix_ms: 0,
        lifecycle: None,
        proposal_id: ProposalId::new("p-1").unwrap(),
    }
}

fn service(store: &SimS3, registry: &MetricsRegistry) -> FlushService<SimS3, SimMount> {
    let store = store.clone();
    // Multipart uploads stream, so a completion is counted in the
    // streaming-overlap histogram (§7.3).
    let settings = FlushSettings {
        streaming: true,
        ..settings()
    };
    FlushService::new(
        cluster(),
        settings,
        Box::new(move |target: &RemoteTarget| {
            assert_eq!(target.bucket, "remote");
            store.clone()
        }),
        FlushMetrics::register(registry),
    )
}

/// Reconciles until the bucket's probe is done and its flusher runs.
async fn reconcile_until_probed(
    service: &FlushService<SimS3, SimMount>,
    node: &Node,
    bucket: &BucketDocument,
) -> ProbeStatus {
    let patience = Patience::new();
    while !patience.is_exhausted() {
        service
            .reconcile(std::slice::from_ref(bucket), &node.set)
            .await;
        let status = service.status(&bucket.bucket_id).unwrap();
        if matches!(status.probe, ProbeStatus::Done { .. }) && !status.shards.is_empty() {
            return status.probe;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the probe never finished");
}

#[test]
fn flushes_write_back_buckets_under_their_prefix() {
    runtime().block_on(async {
        let node = Node::open(20).await;
        let config = SimS3Config {
            min_part_size: 1,
            ..SimS3Config::default()
        };
        let store = SimS3::new(20, config);
        let registry = MetricsRegistry::new();
        let wall = Arc::new(ManualWallClock::new(Duration::from_secs(1_700_000_100)));
        let service = service(&store, &registry).with_wall_clock(wall.clone());
        let bucket = bucket(BucketMode::WriteBack);

        let seq = node.put("cat.jpg", "meow").await;
        let probe = reconcile_until_probed(&service, &node, &bucket).await;
        assert_eq!(
            probe,
            ProbeStatus::Done {
                unprotected: Vec::new()
            }
        );
        let patience = Patience::new();
        while !patience.is_exhausted() {
            if store.object("team/cat.jpg").is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let object = store.object("team/cat.jpg").unwrap();
        assert_eq!(
            object.info.metadata.write_identity(),
            Some(identity(seq).as_str())
        );
        // The probe cleaned up after itself.
        assert_eq!(store.keys(), ["team/cat.jpg"]);

        // A conflict shows in the status and the metrics; a multipart
        // object is flushed.
        let mut metadata = skys3_remote::UserMetadata::new();
        metadata.insert("writer", "other").unwrap();
        skys3_remote::ObjectStore::put_object(
            &store,
            skys3_remote::PutObject::new("team/dog.jpg", "woof").with_metadata(metadata),
        )
        .await
        .unwrap();
        node.put("dog.jpg", "bark").await;
        node.multipart("clip.mp4", &["ab", "cd"]).await;
        let status = loop {
            // Once the object is there, a status without dirty keys has
            // seen its flush end.
            let flushed = store.object("team/clip.mp4").is_some();
            let status = service.status(&bucket.bucket_id).unwrap();
            let shard = &status.shards[0].1;
            if shard.conflicts.len() == 1 && shard.dirty == 0 && flushed {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        assert_eq!(status.name, "photos");
        assert_eq!(status.shards[0].1.conflicts[0].key, "dog.jpg");
        wall.advance(Duration::from_secs(100));
        let gauges = status.gauges(wall.now());
        assert_eq!(gauges.conflicted_keys, 1);
        assert_eq!(gauges.orphaned_uploads, 0);
        assert_eq!(
            gauges.dirty_budget,
            u64::MAX,
            "the default budget is unlimited"
        );
        assert_eq!(gauges.dirty_bytes, 4);
        assert_eq!(gauges.flush_lag, 0.0);
        assert_eq!(gauges.oldest_dirty_age, 100.0);
        service.refresh_metrics();
        let text = registry.encode().unwrap();
        for line in [
            "skys3_dirty_bytes{bucket=\"photos\"} 4",
            "skys3_dirty_budget_bytes{bucket=\"photos\"} 9223372036854775807",
            "skys3_conflicted_keys{bucket=\"photos\"} 1",
            "skys3_flush_orphaned_uploads{bucket=\"photos\"} 0",
            "skys3_flush_conflicts_total{bucket=\"photos\"} 1",
            "skys3_flushes_total{bucket=\"photos\"} 2",
            "skys3_oldest_dirty_age_seconds{bucket=\"photos\"} 100.0",
            "skys3_flush_lag_seconds{bucket=\"photos\"} 0.0",
            "skys3_flush_streaming_overlap_ratio_count{bucket=\"photos\"} 1",
        ] {
            assert!(text.contains(line), "{line} is not in {text}");
        }

        // A bucket that is gone is no longer flushed.
        service.reconcile(&[], &node.set).await;
        assert!(service.status(&bucket.bucket_id).is_none());
        service.refresh_metrics();
        assert!(!registry.encode().unwrap().contains("skys3_dirty_bytes{"));
    });
}

#[test]
fn the_probe_finds_unprotected_operations_and_retries() {
    runtime().block_on(async {
        let node = Node::open(21).await;
        let config = SimS3Config {
            conditionals: Conditionals::R2,
            ..SimS3Config::default()
        };
        let store = SimS3::new(21, config);
        store.set_faults(SimS3Faults::OUTAGE);
        let registry = MetricsRegistry::new();
        let service = service(&store, &registry);
        let bucket = bucket(BucketMode::WriteBack);
        node.put("cat.jpg", "meow").await;

        service
            .reconcile(std::slice::from_ref(&bucket), &node.set)
            .await;
        let failed = loop {
            let status = service.status(&bucket.bucket_id).unwrap();
            if let ProbeStatus::Running { error: Some(error) } = status.probe {
                break error;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        };
        assert!(failed.contains("500"), "{failed}");
        // The shard's dirty keys are tracked while the probe fails, so
        // they count against the dirty budget during the outage.
        let tracked = |service: &FlushService<SimS3, SimMount>| {
            let status = service.status(&bucket.bucket_id).unwrap();
            status.shards.len() == 1 && status.shards[0].1.dirty_bytes == 4
        };
        while !tracked(&service) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let usage = service.budget().usage(&bucket.bucket_id).unwrap();
        assert_eq!(usage.dirty, 4);
        assert_eq!(usage.share, u64::MAX, "the default budget is unlimited");
        assert!(store.object("team/cat.jpg").is_none());

        store.set_faults(SimS3Faults::NONE);
        let probe = reconcile_until_probed(&service, &node, &bucket).await;
        assert_eq!(
            probe,
            ProbeStatus::Done {
                unprotected: vec![
                    ConditionalOperation::CompleteMultipartUpload,
                    ConditionalOperation::DeleteObject
                ]
            }
        );
        // Once the probe is done, the tracked key is flushed.
        let patience = Patience::new();
        while service.budget().cluster_usage().dirty > 0 {
            assert!(!patience.is_exhausted(), "the key was never flushed");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(store.object("team/cat.jpg").is_some());
        service.shutdown().await;
        assert!(service.status(&bucket.bucket_id).is_none());
    });
}

#[test]
fn only_a_shards_primary_flushes_it() {
    runtime().block_on(async {
        let node = Node::open(23).await;
        let store = SimS3::new(23, SimS3Config::default());
        let service = service(&store, &MetricsRegistry::new());
        let bucket = bucket(BucketMode::WriteBack);
        // The node's replica becomes a member of a configuration whose
        // primary is another node.
        node.set.remove(&shard_ref()).await.unwrap();
        let member = ShardConfig {
            epoch: Epoch::new(2),
            primary: "node-2".parse().unwrap(),
            members: vec!["node-2".parse().unwrap(), "node-1".parse().unwrap()],
            replicas: 2,
            ..support::config()
        };
        node.set
            .open_replica(&member, &"node-1".parse().unwrap())
            .await
            .unwrap();
        service
            .reconcile(std::slice::from_ref(&bucket), &node.set)
            .await;
        assert!(service.status(&bucket.bucket_id).unwrap().shards.is_empty());
    });
}

#[test]
fn other_buckets_and_closed_shards_are_not_flushed() {
    runtime().block_on(async {
        let node = Node::open(22).await;
        let store = SimS3::new(22, SimS3Config::default());
        let registry = MetricsRegistry::new();
        let budget = Arc::new(DirtyBudget::new(1000));
        let service = service(&store, &registry).with_budget(Arc::clone(&budget));
        for mode in [BucketMode::Local, BucketMode::ReadOnly] {
            service.reconcile(&[bucket(mode)], &node.set).await;
            assert!(service.status(&shard_ref().bucket).is_none());
        }
        let bucket = bucket(BucketMode::WriteBack);
        reconcile_until_probed(&service, &node, &bucket).await;
        node.shard.close().await.unwrap();
        service
            .reconcile(std::slice::from_ref(&bucket), &node.set)
            .await;
        assert!(service.status(&bucket.bucket_id).unwrap().shards.is_empty());
        // Without a flusher here, the bucket's writes are not limited, and
        // its shard still counts in the cluster's share.
        assert_eq!(budget.check(&bucket.bucket_id), Ok(()));
        assert_eq!(budget.usage(&bucket.bucket_id), None);
        assert_eq!(budget.cluster_usage().share, 0);
        let other = BucketDocument {
            bucket_id: "b-away".parse().unwrap(),
            name: BucketName::new("away").unwrap(),
            ..bucket.clone()
        };
        service.reconcile(&[bucket, other.clone()], &node.set).await;
        assert_eq!(budget.check(&other.bucket_id), Ok(()));
        assert!(format!("{service:?}").contains("FlushService"));
    });
}

/// Waits up to a minute of real time until `done`, polling every 10 ms of
/// simulated time.
async fn wait_until(what: &str, done: impl Fn() -> bool) {
    let patience = Patience::new();
    while !done() {
        assert!(!patience.is_exhausted(), "never saw {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A remote upload that a failed flush left open, and whose abort failed,
/// is aborted even though no later multipart flush comes to abort it
/// first: the key's next version is a single `PUT`.
#[test]
fn uploads_left_open_are_aborted_without_another_multipart_flush() {
    runtime().block_on(async {
        let node = Node::open(25).await;
        let store = support::remote(25, false);
        let target_store = store.clone();
        // A failed flush is retried a second later.
        let settings = FlushSettings {
            min_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(1),
            ..settings()
        };
        let service = FlushService::new(
            cluster(),
            settings,
            Box::new(move |_: &RemoteTarget| target_store.clone()),
            FlushMetrics::register(&MetricsRegistry::new()),
        );
        let bucket = bucket(BucketMode::WriteBack);
        reconcile_until_probed(&service, &node, &bucket).await;
        let status = || service.status(&bucket.bucket_id).unwrap();

        // The flush's part fails, and so does its abort; before the flush
        // is retried, a single `PUT` replaces the version.
        store.inject(Operation::UploadPart, Fault::InternalError);
        store.inject(Operation::AbortMultipartUpload, Fault::SlowDown);
        node.multipart("clip.mp4", &["ab", "cd"]).await;
        wait_until("an orphaned upload", || status().orphaned_uploads == 1).await;
        node.put("clip.mp4", "small").await;
        wait_until("the single PUT flushed", || {
            store
                .object("team/clip.mp4")
                .is_some_and(|object| &object.body[..] == b"small")
        })
        .await;

        wait_until("no upload left", || {
            status().orphaned_uploads == 0 && store.uploads().is_empty()
        })
        .await;
    });
}

/// A `read_only` bucket over an origin in `remote`, under `team/`.
fn read_only() -> BucketDocument {
    BucketDocument {
        target: Some(RemoteTarget {
            endpoint: "https://s3.example".to_owned(),
            bucket: "remote".to_owned(),
            prefix: Some("team/".to_owned()),
        }),
        ..bucket(BucketMode::ReadOnly)
    }
}

#[test]
fn read_only_buckets_have_an_origin_and_no_flushers() {
    runtime().block_on(async {
        let node = Node::open(24).await;
        let store = SimS3::new(24, SimS3Config::default());
        let service = service(&store, &MetricsRegistry::new());
        let bucket = read_only();
        service
            .reconcile(std::slice::from_ref(&bucket), &node.set)
            .await;
        // Nothing is flushed to an origin, and it has no import.
        assert!(service.status(&bucket.bucket_id).is_none());
        assert!(service.remote(&bucket.bucket_id).is_none());
        assert_eq!(service.target_of(&bucket), None);
        let origin = service.origin(&bucket.bucket_id).unwrap();
        assert_eq!(origin.target(), bucket.target.as_ref().unwrap());
        assert_eq!(origin.credentials(), DEFAULT_CREDENTIALS);
        assert!(service.filler(&bucket.bucket_id).is_some());
        // The origin is read under the target's prefix.
        store
            .put_object(PutObject::new("team/k", "origin"))
            .await
            .unwrap();
        let object = origin.remote().head("k").await.unwrap().unwrap();
        assert_eq!(object.object.size, 6);
        assert!(format!("{origin:?}").contains("Origin"));
        // A bucket that is gone loses its origin, and so does a service
        // that stops.
        service.reconcile(&[], &node.set).await;
        assert!(service.origin(&bucket.bucket_id).is_none());
        assert!(service.filler(&bucket.bucket_id).is_none());
        service
            .reconcile(std::slice::from_ref(&bucket), &node.set)
            .await;
        assert!(service.origin(&bucket.bucket_id).is_some());
        service.shutdown().await;
        assert!(service.origin(&bucket.bucket_id).is_none());
    });
}

#[test]
fn origins_are_read_with_their_buckets_profiles() {
    runtime().block_on(async {
        let node = Node::open(25).await;
        let store = SimS3::new(25, SimS3Config::default());
        let config: skys3_config::Config = "[cluster]\ncluster_id = \"c-test\"\n\
             [control_store]\netcd_endpoints = [\"https://etcd.invalid:2379\"]\n\
             [buckets.photos]\nmode = \"read_only\"\norigin_profile = \"reader\"\n"
            .parse()
            .unwrap();
        let profiles = Arc::new(std::sync::Mutex::new(Vec::new()));
        let connect = {
            let (store, profiles) = (store.clone(), Arc::clone(&profiles));
            move |_: &RemoteTarget, profile: Option<&str>| {
                profiles.lock().unwrap().push(profile.map(str::to_owned));
                store.with_credentials(profile.unwrap_or_default())
            }
        };
        let service = service(&store, &MetricsRegistry::new())
            .with_buckets(config.buckets().clone())
            .with_origin_connect(Box::new(connect));
        let bucket = read_only();
        for _ in 0..2 {
            service
                .reconcile(std::slice::from_ref(&bucket), &node.set)
                .await;
        }
        let origin = service.origin(&bucket.bucket_id).unwrap();
        assert_eq!(origin.credentials(), profile_credentials("reader"));
        assert_eq!(origin.credentials(), "profile:reader");
        // The origin's store is built once, with the bucket's profile.
        assert_eq!(*profiles.lock().unwrap(), [Some("reader".to_owned())]);
        // The profile's credentials are the ones the origin is read with.
        store
            .put_object(PutObject::new("team/k", "origin"))
            .await
            .unwrap();
        store.deny("reader", "team/k", true);
        let error = origin.remote().head("k").await.unwrap_err();
        assert_eq!(error.status(), Some(403));
    });
}
