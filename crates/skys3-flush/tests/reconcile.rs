//! Tests of the reconciliation scan (§9.1): out-of-band changes at the
//! remote appear in its report, keys that are dirty or being flushed do
//! not, and the scan changes nothing.

mod support;

use std::sync::Arc;
use std::time::Duration;

use skys3_config::Config;
use skys3_flush::{
    DifferenceKind, FlushMetrics, FlushService, FlushSettings, ReconcileReport, RemoteWriter,
};
use skys3_index::{Entry, EntryState};
use skys3_io::{ManualWallClock, SimMount};
use skys3_log::RecordBody;
use skys3_log::record::Delete;
use skys3_obs::MetricsRegistry;
use skys3_remote::{DeleteObject, ObjectStore, PutObject, UserMetadata};
use skys3_shard::Shard;
use skys3_sim::SimS3;
use skys3_sim::s3::{Operation, SimS3Faults};
use skys3_types::{
    BucketDocument, BucketMode, BucketName, EpochSeq, ProposalId, RemoteTarget, ShardConfig,
    ShardCount, ShardId, shard_for_key,
};
use support::{Node, Patience, cluster, config, put, remote, runtime, settings, shard_ref};

/// The remote prefix the bucket flushes under.
const PREFIX: &str = "team/";

/// The bucket `photos`, with two shards, flushed to `team/` in the remote.
fn bucket() -> BucketDocument {
    BucketDocument {
        bucket_id: shard_ref().bucket,
        name: BucketName::new("photos").unwrap(),
        mode: BucketMode::WriteBack,
        shards: ShardCount::new(2).unwrap(),
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

/// A node with both shards of the bucket open, as their primary.
struct World {
    node: Node,
    shards: Vec<Shard<SimMount>>,
    store: SimS3,
    service: FlushService<SimS3, SimMount>,
    registry: MetricsRegistry,
}

impl World {
    /// A node whose service scans `photos` every `interval` seconds, with
    /// `faults` on every remote request.
    async fn new(seed: u64, interval: u64, faults: SimS3Faults) -> World {
        let node = Node::open(seed).await;
        let second = ShardConfig {
            shard: ShardId::new(1),
            ..config()
        };
        let other = node.set.open(&second).await.unwrap();
        let shards = vec![node.shard.clone(), other];
        let store = remote(seed, false);
        store.set_faults(faults);
        let parsed: Config = format!(
            "[cluster]\ncluster_id = \"c-test\"\n\
             [control_store]\netcd_endpoints = [\"https://etcd.invalid:2379\"]\n\
             [buckets.photos]\nreconcile_interval_seconds = {interval}\n"
        )
        .parse()
        .unwrap();
        let registry = MetricsRegistry::new();
        let connected = store.clone();
        let service = FlushService::new(
            cluster(),
            FlushSettings {
                // Several pages, each compared with both shards.
                reconcile_page_keys: 3,
                reconcile_requests_per_second: 100,
                ..settings()
            },
            Box::new(move |_: &RemoteTarget| connected.clone()),
            FlushMetrics::register(&registry),
        )
        .with_wall_clock(Arc::new(ManualWallClock::new(Duration::from_secs(
            1_700_000_000,
        ))))
        .with_buckets(parsed.buckets().clone());
        World {
            node,
            shards,
            store,
            service,
            registry,
        }
    }

    /// The shard `key` belongs to.
    fn shard_of(&self, key: &str) -> &Shard<SimMount> {
        let bucket = bucket();
        let id = shard_for_key(&bucket.bucket_id, key.as_bytes(), bucket.shards);
        &self.shards[usize::from(id.get())]
    }

    async fn put(&self, key: &str, body: &str) {
        self.shard_of(key).commit(put(key, body)).await.unwrap();
    }

    async fn delete(&self, key: &str) {
        let body = RecordBody::Delete(Delete {
            key: key.to_owned(),
        });
        self.shard_of(key).commit(body).await.unwrap();
    }

    async fn reconcile(&self) {
        self.service.reconcile(&[bucket()], &self.node.set).await;
    }

    /// Every entry of both shards, and each shard's applied position.
    async fn index(&self) -> (Vec<(String, Entry)>, Vec<EpochSeq>) {
        let mut entries = Vec::new();
        let mut applied = Vec::new();
        for shard in &self.shards {
            entries.extend(shard.entries(None, usize::MAX).await.unwrap());
            applied.push(shard.applied());
        }
        (entries, applied)
    }

    /// Follows the bucket until every entry is clean, but for `held` keys,
    /// and the import is done.
    async fn settle(&self, held: usize) {
        let patience = Patience::new();
        loop {
            self.reconcile().await;
            let (entries, _) = self.index().await;
            let unclean = entries
                .iter()
                .filter(|(_, e)| !matches!(e.state, EntryState::Clean | EntryState::Evicted))
                .count();
            let status = self.service.status(&bucket().bucket_id).unwrap();
            let imported = status
                .import
                .is_some_and(|import| import.checkpoint == skys3_index::ImportCheckpoint::Done);
            if unclean == held && imported {
                return;
            }
            assert!(!patience.is_exhausted(), "the flush did not settle");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Follows the bucket until more than `after` scans finished, and
    /// returns the last one's report and the number of scans.
    async fn next_report(&self, after: u64) -> (Arc<ReconcileReport>, u64) {
        let patience = Patience::new();
        loop {
            self.reconcile().await;
            let status = self.service.reconciliation(&bucket().bucket_id).unwrap();
            if let Some(last) = &status.last
                && status.scans > after
            {
                return (Arc::clone(last), status.scans);
            }
            assert!(!patience.is_exhausted(), "no scan finished: {status:?}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

/// Writes `key` at the remote as another writer, with `identity` as its
/// write identity if given.
async fn write_out_of_band(store: &SimS3, key: &str, body: &str, identity: Option<&str>) {
    let mut metadata = UserMetadata::new();
    if let Some(identity) = identity {
        metadata.insert("skys3-wid", identity).unwrap();
    }
    let request = PutObject::new(format!("{PREFIX}{key}"), body.to_owned()).with_metadata(metadata);
    store.put_object(request).await.unwrap();
}

#[test]
fn out_of_band_changes_appear_in_the_report_and_the_scan_changes_nothing() {
    runtime().block_on(async {
        let world = World::new(71, 60, SimS3Faults::NONE).await;
        for n in 0..10 {
            world.put(&format!("k{n}"), &format!("body {n}")).await;
        }
        world.settle(0).await;
        let wall = Duration::from_secs(1_700_000_000).as_millis() as u64;
        // Nothing differs yet.
        let (report, scans) = world.next_report(0).await;
        assert!(report.differences.is_empty(), "{report:?}");
        assert_eq!(report.shards, [ShardId::new(0), ShardId::new(1)]);
        assert_eq!(report.listed, 10);
        assert_eq!(report.compared, 10);
        assert!(report.started_ms >= wall);

        // Another writer adds a key, overwrites one as another SkyS3
        // bucket, deletes one, and adds one with a value that is no
        // identity.
        write_out_of_band(&world.store, "added", "new", None).await;
        write_out_of_band(&world.store, "k1", "other", Some("c-other/b-other/0/1.1")).await;
        world
            .store
            .delete_object(DeleteObject::new(format!("{PREFIX}k2")))
            .await
            .unwrap();
        write_out_of_band(&world.store, "odd", "x", Some("not-an-identity")).await;
        // One overwritten key is written locally too: its flush finds the
        // other write and holds the key in conflict, dirty, which the scan
        // leaves to the flusher.
        write_out_of_band(&world.store, "k3", "theirs", None).await;
        world.put("k3", "ours").await;
        world.settle(1).await;

        let before = world.index().await;
        let writes = world.store.apply_by_times().len();
        let keys = world.store.keys();
        let flushes = world.registry.encode().unwrap();
        let flushes = flushes
            .lines()
            .find(|line| line.starts_with("skys3_flushes_total{"))
            .unwrap()
            .to_owned();

        let (report, _) = world.next_report(scans).await;
        let found: Vec<(&str, DifferenceKind, Option<RemoteWriter>)> = report
            .differences
            .iter()
            .map(|d| (d.key.as_str(), d.kind, d.remote.as_ref().map(|r| r.writer)))
            .collect();
        let mut expected = vec![
            (
                "added",
                DifferenceKind::RemoteOnly,
                Some(RemoteWriter::Outside),
            ),
            (
                "k1",
                DifferenceKind::EtagMismatch,
                Some(RemoteWriter::Foreign),
            ),
            ("k2", DifferenceKind::IndexOnly, None),
            (
                "odd",
                DifferenceKind::RemoteOnly,
                Some(RemoteWriter::Unknown),
            ),
        ];
        // Reported in key order within each page, pages in order.
        expected.sort_by_key(|(key, ..)| *key);
        let mut sorted = found.clone();
        sorted.sort_by_key(|(key, ..)| *key);
        assert_eq!(sorted, expected, "{report:#?}");
        let k1 = report.differences.iter().find(|d| d.key == "k1").unwrap();
        assert_eq!(
            k1.remote.as_ref().unwrap().identity.as_deref(),
            Some("c-other/b-other/0/1.1")
        );
        assert_eq!(k1.index.as_ref().unwrap().state, EntryState::Clean);
        assert_eq!(report.unconfirmed, 0);

        // The scan changed nothing: not the index, not the remote, and it
        // started no flush.
        assert_eq!(world.index().await, before);
        assert_eq!(world.store.apply_by_times().len(), writes);
        assert_eq!(world.store.keys(), keys);
        let text = world.registry.encode().unwrap();
        assert!(text.contains(&flushes), "{flushes} changed:\n{text}");
        for line in [
            "skys3_reconcile_differences{bucket=\"photos\",kind=\"remote_only\"} 2",
            "skys3_reconcile_differences{bucket=\"photos\",kind=\"index_only\"} 1",
            "skys3_reconcile_differences{bucket=\"photos\",kind=\"etag_mismatch\"} 1",
            "skys3_reconcile_differences{bucket=\"photos\",kind=\"size_mismatch\"} 0",
        ] {
            assert!(text.contains(line), "{line} is not in {text}");
        }
        assert!(text.contains("skys3_reconcile_scans_total{bucket=\"photos\"}"));

        // A bucket no longer followed has no scan, and no series.
        world.service.reconcile(&[], &world.node.set).await;
        assert!(world.service.reconciliation(&bucket().bucket_id).is_none());
        let text = world.registry.encode().unwrap();
        assert!(!text.contains("skys3_reconcile_differences{"), "{text}");
        world.service.shutdown().await;
    });
}

#[test]
fn keys_written_and_flushed_during_scans_are_never_reported() {
    runtime().block_on(async {
        // Requests take up to 20 ms each way and some fail, so flushes and
        // the scans' listings and HEADs interleave.
        let faults = SimS3Faults {
            min_delay: Duration::ZERO,
            max_delay: Duration::from_millis(20),
            internal_error_probability: 0.05,
            ..SimS3Faults::NONE
        };
        let world = World::new(72, 1, faults).await;
        world.settle(0).await;
        let mut seen = 0;
        for round in 0..40_u32 {
            for n in 0..4 {
                let key = format!("k{}", (round * 7 + n * 3) % 13);
                if (round + n) % 5 == 0 {
                    world.delete(&key).await;
                } else {
                    world.put(&key, &format!("round {round}")).await;
                }
            }
            world.reconcile().await;
            tokio::time::sleep(Duration::from_millis(150)).await;
            let status = world.service.reconciliation(&bucket().bucket_id).unwrap();
            if let Some(last) = status.last
                && status.scans > seen
            {
                assert!(last.differences.is_empty(), "{last:#?}");
                seen = status.scans;
            }
        }
        assert!(seen >= 3, "only {seen} scans finished");
        // Once flushing settles, a scan compares every key and agrees.
        world.store.set_faults(SimS3Faults::NONE);
        world.settle(0).await;
        let (report, _) = world.next_report(seen).await;
        assert!(report.differences.is_empty(), "{report:#?}");
        assert!(report.compared > 0);
        assert!(
            world.store.applied(Operation::PutObject).len() > 10,
            "the workload flushed"
        );
        world.service.shutdown().await;
    });
}

#[test]
fn only_the_shards_the_node_leads_are_compared() {
    runtime().block_on(async {
        let world = World::new(74, 60, SimS3Faults::NONE).await;
        let keys: Vec<String> = (0..8).map(|n| format!("k{n}")).collect();
        for key in &keys {
            world.put(key, "v").await;
        }
        world.settle(0).await;
        // Another writer deletes every key; then shard 1 stops here, as
        // when another node takes it over.
        for key in &keys {
            let request = DeleteObject::new(format!("{PREFIX}{key}"));
            world.store.delete_object(request).await.unwrap();
        }
        world.shards[1].close().await.unwrap();
        let (report, _) = world.next_report(0).await;
        assert_eq!(report.shards, [ShardId::new(0)]);
        let of_shard_0: Vec<&String> = keys
            .iter()
            .filter(|key| world.shard_of(key).shard().shard == ShardId::new(0))
            .collect();
        assert!(!of_shard_0.is_empty() && of_shard_0.len() < keys.len());
        let reported: Vec<&String> = report.differences.iter().map(|d| &d.key).collect();
        assert_eq!(reported, of_shard_0);
        assert!(
            report
                .differences
                .iter()
                .all(|d| d.kind == DifferenceKind::IndexOnly && d.shard == ShardId::new(0))
        );
        world.service.shutdown().await;
    });
}

#[test]
fn a_bucket_without_an_interval_is_not_scanned() {
    runtime().block_on(async {
        let world = World::new(73, 0, SimS3Faults::NONE).await;
        world.put("k", "v").await;
        world.settle(0).await;
        assert!(world.service.reconciliation(&bucket().bucket_id).is_none());
        world.service.shutdown().await;
    });
}
