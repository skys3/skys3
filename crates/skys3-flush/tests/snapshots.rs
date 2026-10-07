//! Index snapshots and the lost-key report (§6.9, §8.9): what a shard's
//! primary writes to its snapshot target, what a restore reads back, and
//! what the report says is lost.

mod support;

use std::time::Duration;

use bytes::Bytes;
use skys3_config::Config;
use skys3_flush::snapshot::{
    Contents, DurableHome, LostKeyReport, Restored, SnapshotService, SnapshotStatus, latest,
    lost_keys, object_key, parse_object_key, shard_dir,
};
use skys3_flush::test_hooks::{SnapshotBug, seed_snapshot_bug};
use skys3_index::{EntryState, ShardTable};
use skys3_io::SimMount;
use skys3_remote::{ListObjectsV2, ObjectStore, PutObject};
use skys3_sim::SimS3;
use skys3_types::{BucketDocument, BucketMode, BucketName, RemoteTarget, ShardCount};
use support::{Node, Patience, cluster, runtime, shard_ref, target};

/// Where the tests' snapshots go.
const SNAPSHOTS: &str = "snaps/";
/// Where the `write_back` bucket's objects go: the support target's prefix.
const DATA: &str = "";

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
            prefix: Some(DATA.to_owned()),
        }),
        created_unix_ms: 0,
        lifecycle: None,
        proposal_id: "p-1".parse().unwrap(),
    }
}

/// A service that snapshots the bucket `photos`, of `mode`, to `store`
/// every second.
fn service(store: &SimS3, mode: BucketMode) -> SnapshotService<SimS3, SimMount> {
    let mode = match mode {
        BucketMode::Local => "local",
        _ => "write_back",
    };
    let config: Config = format!(
        "[cluster]\ncluster_id = \"c-test\"\n\
         [control_store]\netcd_endpoints = [\"https://etcd.invalid:2379\"]\n\
         [buckets.photos]\nmode = \"{mode}\"\n\
         index_snapshot_interval_seconds = 1\n\
         snapshot_target = \"https://s3.example/remote/{SNAPSHOTS}\"\n"
    )
    .parse()
    .unwrap();
    let store = store.clone();
    SnapshotService::new(Box::new(move |_| store.clone()), config.buckets().clone())
}

/// Starts the writers and waits until they wrote `count` more snapshots.
async fn written(
    service: &SnapshotService<SimS3, SimMount>,
    node: &Node,
    document: &BucketDocument,
    count: u64,
) -> SnapshotStatus {
    let before = status(service, document).written;
    let patience = Patience::new();
    loop {
        service
            .reconcile(std::slice::from_ref(document), &node.set)
            .await;
        let status = status(service, document);
        if status.written >= before + count {
            return status;
        }
        assert!(!patience.is_exhausted(), "no snapshot: {status:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn status(service: &SnapshotService<SimS3, SimMount>, document: &BucketDocument) -> SnapshotStatus {
    service
        .status(&document.bucket_id)
        .into_iter()
        .next()
        .map(|(_, status)| status)
        .unwrap_or_default()
}

async fn restored(store: &SimS3) -> Restored {
    latest(store, SNAPSHOTS, &shard_ref())
        .await
        .unwrap()
        .unwrap()
}

async fn report(store: &SimS3, home: Option<&str>) -> LostKeyReport {
    let restored = latest(store, SNAPSHOTS, &shard_ref()).await.unwrap();
    let cluster = cluster();
    let home = home.map(|prefix| DurableHome {
        store,
        prefix,
        cluster: &cluster,
    });
    lost_keys(&shard_ref(), restored.as_ref(), home, u64::MAX)
        .await
        .unwrap()
}

fn lost(report: &LostKeyReport) -> Vec<&str> {
    report.lost.iter().map(|lost| lost.key.as_str()).collect()
}

/// The snapshot objects the store holds, as chain numbers per chain.
fn objects(store: &SimS3) -> Vec<(u64, u32)> {
    let dir = shard_dir(SNAPSHOTS, &shard_ref());
    store
        .keys()
        .iter()
        .filter_map(|key| parse_object_key(&dir, key))
        .map(|(chain, number)| (chain.base.seq.get(), number))
        .collect()
}

#[test]
fn a_write_back_snapshot_holds_what_the_remote_lacks() {
    runtime().block_on(async {
        let store = support::remote(1, false);
        let node = Node::open_inline(1).await;
        let document = bucket(BucketMode::WriteBack);
        let service = service(&store, BucketMode::WriteBack);
        node.put("flushed", "one").await;
        node.put("gone", "two").await;
        let flusher = node.flusher(&target(&store));
        node.settle(&flusher).await;
        flusher.stop().await;
        node.put("dirty", "three").await;
        node.delete("gone").await;
        let upload = node.create("open").await;
        node.part("open", upload, 1, "part").await;

        let status = written(&service, &node, &document, 1).await;
        let taken = status.last.unwrap();
        assert_eq!(taken.number, 0);
        let snapshot = restored(&store).await;
        assert_eq!(snapshot.contents, Contents::Unflushed);
        assert_eq!(snapshot.position, node.shard.applied());
        let entries = snapshot.entries().unwrap();
        assert_eq!(entries.keys().collect::<Vec<_>>(), ["dirty", "gone"]);
        assert!(entries.values().all(|e| e.state == EntryState::Dirty));
        assert_eq!(snapshot.rows(ShardTable::Parts).count(), 1);

        // The remote has neither the new key nor the delete; the upload is
        // lost with the members.
        let found = report(&store, Some(DATA)).await;
        assert_eq!(lost(&found), ["dirty", "gone"]);
        assert!(found.lost[1].object.is_none());
        assert_eq!(found.uploads.len(), 1);
        assert_eq!(
            (found.uploads[0].key.as_str(), found.uploads[0].parts),
            ("open", 1)
        );
        assert_eq!(found.window.from_ms, Some(taken.taken_ms));
        assert_eq!(found.snapshot.unwrap().position, taken.position);

        // Once flushed after the snapshot, nothing it names is lost.
        let flusher = node.flusher(&target(&store));
        node.settle(&flusher).await;
        flusher.stop().await;
        assert!(report(&store, Some(DATA)).await.lost.is_empty());

        // Each snapshot of a `write_back` bucket is a base, and replaces
        // the previous one.
        node.put("later", "four").await;
        written(&service, &node, &document, 2).await;
        let objects = objects(&store);
        assert_eq!(objects.len(), 1, "{objects:?}");
        let snapshot = restored(&store).await;
        assert_eq!(
            snapshot.entries().unwrap().keys().collect::<Vec<_>>(),
            ["later"]
        );
        service.shutdown().await;
    });
}

#[test]
fn a_local_snapshot_restores_the_whole_index_from_its_deltas() {
    runtime().block_on(async {
        let store = support::remote(2, false);
        let node = Node::open_inline(2).await;
        let document = bucket(BucketMode::Local);
        let service = service(&store, BucketMode::Local);
        for n in 0..8 {
            node.put(&format!("key-{n}"), "body").await;
        }
        written(&service, &node, &document, 1).await;
        node.put("key-0", "changed").await;
        node.delete("key-1").await;
        node.multipart("multi", &["a", "b"]).await;
        let status = written(&service, &node, &document, 1).await;
        assert_eq!(status.last.unwrap().number, 1);

        let snapshot = restored(&store).await;
        assert_eq!(snapshot.contents, Contents::Full);
        assert_eq!(snapshot.number, 1);
        let entries = snapshot.entries().unwrap();
        let all = node.shard.entries(None, usize::MAX).await.unwrap();
        assert_eq!(entries.into_iter().collect::<Vec<_>>(), all);
        assert_eq!(snapshot.rows(ShardTable::Parts).count(), 2);

        // Without a backup, every object the members held is lost; the
        // tombstone of key-1 lost nothing.
        let found = report(&store, None).await;
        let mut expected: Vec<String> = (0..8)
            .filter(|n| *n != 1)
            .map(|n| format!("key-{n}"))
            .collect();
        expected.push("multi".to_owned());
        assert_eq!(lost(&found), expected);

        // The deltas outgrow the base: the next snapshot is a new base, and
        // the older chain goes.
        for n in 0..12 {
            node.put(&format!("more-{n}"), "body").await;
            written(&service, &node, &document, 1).await;
        }
        let objects = objects(&store);
        assert!(
            objects.iter().all(|(base, _)| *base == objects[0].0),
            "{objects:?}"
        );
        let snapshot = restored(&store).await;
        let all = node.shard.entries(None, usize::MAX).await.unwrap();
        assert_eq!(
            snapshot.entries().unwrap().into_iter().collect::<Vec<_>>(),
            all
        );
        service.shutdown().await;
    });
}

#[test]
fn a_restore_stops_at_a_damaged_delta_and_skips_a_damaged_base() {
    runtime().block_on(async {
        let store = support::remote(3, false);
        let node = Node::open_inline(3).await;
        let document = bucket(BucketMode::Local);
        let service = service(&store, BucketMode::Local);
        for n in 0..8 {
            node.put(&format!("key-{n}"), "body").await;
        }
        let base = written(&service, &node, &document, 1).await.last.unwrap();
        node.put("second", "body").await;
        let first = written(&service, &node, &document, 1).await.last.unwrap();
        node.put("third", "body").await;
        written(&service, &node, &document, 1).await;
        service.shutdown().await;

        let dir = shard_dir(SNAPSHOTS, &shard_ref());
        let damage = |number| {
            PutObject::new(
                object_key(&dir, &base.chain, number),
                Bytes::from_static(b"junk"),
            )
        };
        store.put_object(damage(2)).await.unwrap();
        let snapshot = restored(&store).await;
        assert_eq!((snapshot.number, snapshot.taken_ms), (1, first.taken_ms));
        assert!(snapshot.entries().unwrap().contains_key("second"));
        assert!(!snapshot.entries().unwrap().contains_key("third"));

        // A chain without a readable base is passed over.
        store.put_object(damage(0)).await.unwrap();
        assert_eq!(latest(&store, SNAPSHOTS, &shard_ref()).await.unwrap(), None);
        let found = report(&store, None).await;
        assert!(found.snapshot.is_none() && found.lost.is_empty());
        assert_eq!(found.window.from_ms, None);
        // Objects under the directory that are not snapshots are ignored.
        store
            .put_object(PutObject::new(format!("{dir}stray"), Bytes::new()))
            .await
            .unwrap();
        let listed = store
            .list_objects_v2(ListObjectsV2::new(dir.clone()))
            .await
            .unwrap();
        assert!(listed.objects.len() >= 4);
        assert_eq!(latest(&store, SNAPSHOTS, &shard_ref()).await.unwrap(), None);
    });
}

/// Writes keys, takes a base, deletes one and writes another, takes a
/// delta, and returns the report built without a durable home.
async fn local_report(seed: u64) -> (LostKeyReport, Restored) {
    let store = support::remote(seed, false);
    let node = Node::open_inline(seed).await;
    let document = bucket(BucketMode::Local);
    let service = service(&store, BucketMode::Local);
    for n in 0..4 {
        node.put(&format!("key-{n}"), "body").await;
    }
    written(&service, &node, &document, 1).await;
    node.delete("key-0").await;
    node.shard
        .commit(skys3_log::RecordBody::Flushed(skys3_log::record::Flushed {
            key: "key-0".to_owned(),
            seq: node.entry("key-0").await.unwrap().version.seq,
            remote_etag: None,
            remote_version_id: None,
        }))
        .await
        .unwrap();
    node.put("new", "body").await;
    written(&service, &node, &document, 1).await;
    service.shutdown().await;
    (report(&store, None).await, restored(&store).await)
}

#[test]
fn seeded_bugs_change_what_a_restore_sees() {
    runtime().block_on(async {
        let (found, _) = local_report(4).await;
        assert_eq!(lost(&found), ["key-1", "key-2", "key-3", "new"]);

        seed_snapshot_bug(SnapshotBug::KeepsRemoved);
        let (found, _) = local_report(5).await;
        assert!(lost(&found).contains(&"key-0"), "{found:?}");

        seed_snapshot_bug(SnapshotBug::BaseOnly);
        let (found, restored) = local_report(6).await;
        assert_eq!(restored.number, 1);
        assert!(!lost(&found).contains(&"new"), "{found:?}");
        seed_snapshot_bug(SnapshotBug::None);
    });
}

#[test]
fn the_seeded_bug_of_skipping_dirty_entries_hides_them() {
    runtime().block_on(async {
        seed_snapshot_bug(SnapshotBug::SkipsDirty);
        let store = support::remote(7, false);
        let node = Node::open_inline(7).await;
        let document = bucket(BucketMode::WriteBack);
        let service = service(&store, BucketMode::WriteBack);
        node.put("dirty", "body").await;
        written(&service, &node, &document, 1).await;
        service.shutdown().await;
        seed_snapshot_bug(SnapshotBug::None);
        assert!(report(&store, Some(DATA)).await.lost.is_empty());
    });
}

#[test]
fn a_bucket_without_a_target_or_a_primary_is_not_snapshotted() {
    runtime().block_on(async {
        let store = support::remote(8, false);
        let node = Node::open_inline(8).await;
        let service = service(&store, BucketMode::Local);
        let mut other = bucket(BucketMode::Local);
        other.name = BucketName::new("plain").unwrap();
        assert_eq!(service.target_of(&other), None);
        let mut read_only = bucket(BucketMode::Local);
        read_only.mode = BucketMode::ReadOnly;
        assert_eq!(service.target_of(&read_only), None);
        service.reconcile(&[other], &node.set).await;
        assert!(service.status(&shard_ref().bucket).is_empty());
        let document = bucket(BucketMode::Local);
        let (target, contents) = service.target_of(&document).unwrap();
        assert_eq!(
            (target.prefix.as_deref(), contents),
            (Some(SNAPSHOTS), Contents::Full)
        );
        written(&service, &node, &document, 1).await;
        // A shard that is gone stops its writer.
        node.set.close_all().await.unwrap();
        service.reconcile(&[document.clone()], &node.set).await;
        assert!(service.status(&document.bucket_id).is_empty());
    });
}
