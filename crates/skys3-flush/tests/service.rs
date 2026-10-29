//! Tests of the flush service: following a node's buckets and shards, the
//! capability probe, status, and metrics.

mod support;

use std::sync::Arc;
use std::time::Duration;

use skys3_flush::{FlushMetrics, FlushService, ProbeStatus};
use skys3_io::{ManualWallClock, SimMount, WallClock};
use skys3_obs::MetricsRegistry;
use skys3_remote::probe::ConditionalOperation;
use skys3_sim::SimS3;
use skys3_sim::s3::{Conditionals, SimS3Config, SimS3Faults};
use skys3_types::{BucketDocument, BucketMode, BucketName, ProposalId, RemoteTarget, ShardCount};
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
        proposal_id: ProposalId::new("p-1").unwrap(),
    }
}

fn service(store: &SimS3, registry: &MetricsRegistry) -> FlushService<SimS3, SimMount> {
    let store = store.clone();
    FlushService::new(
        cluster(),
        settings(),
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
        let store = SimS3::new(20, SimS3Config::default());
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

        // A conflict and a multipart object show in the status and the
        // metrics.
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
            let status = service.status(&bucket.bucket_id).unwrap();
            let shard = &status.shards[0].1;
            if shard.conflicts.len() == 1 && shard.awaiting_multipart.len() == 1 {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        assert_eq!(status.name, "photos");
        assert_eq!(status.shards[0].1.conflicts[0].key, "dog.jpg");
        wall.advance(Duration::from_secs(100));
        let gauges = status.gauges(wall.now());
        assert_eq!(gauges.conflicted_keys, 1);
        assert_eq!(gauges.awaiting_multipart_keys, 1);
        assert_eq!(gauges.dirty_bytes, 8);
        assert_eq!(gauges.flush_lag, 0.0);
        assert_eq!(gauges.oldest_dirty_age, 100.0);
        service.refresh_metrics();
        let text = registry.encode().unwrap();
        for line in [
            "skys3_dirty_bytes{bucket=\"photos\"} 8",
            "skys3_conflicted_keys{bucket=\"photos\"} 1",
            "skys3_awaiting_multipart_flush_keys{bucket=\"photos\"} 1",
            "skys3_flush_conflicts_total{bucket=\"photos\"} 1",
            "skys3_flushes_total{bucket=\"photos\"} 1",
            "skys3_oldest_dirty_age_seconds{bucket=\"photos\"} 100.0",
            "skys3_flush_lag_seconds{bucket=\"photos\"} 0.0",
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
        assert!(service.status(&bucket.bucket_id).unwrap().shards.is_empty());

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
        service.shutdown().await;
        assert!(service.status(&bucket.bucket_id).is_none());
    });
}

#[test]
fn other_buckets_and_closed_shards_are_not_flushed() {
    runtime().block_on(async {
        let node = Node::open(22).await;
        let store = SimS3::new(22, SimS3Config::default());
        let registry = MetricsRegistry::new();
        let service = service(&store, &registry);
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
        assert!(format!("{service:?}").contains("FlushService"));
    });
}
