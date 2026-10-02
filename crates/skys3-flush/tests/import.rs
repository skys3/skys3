//! The namespace import of a `write_back` bucket (design §9.1) through the
//! flush service: importing on attach, resuming from the checkpoint, the
//! rate limit, writes that race the import, reads of the remote, and the
//! index bytes each imported entry takes.

mod support;

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;
use skys3_flush::{FlushMetrics, FlushService, ImportStatus, RemoteReader, loaded_metadata};
use skys3_index::{EntryState, ImportCheckpoint, Index, IndexConfig, ListItem, Payload};
use skys3_io::{SimDisk, SimMount};
use skys3_obs::MetricsRegistry;
use skys3_remote::{ObjectInfo, ObjectStore, PutObject, UserMetadata};
use skys3_sim::SimS3;
use skys3_sim::s3::SimS3Config;
use skys3_types::{
    BucketDocument, BucketMode, BucketName, ETag, Epoch, EpochSeq, ProposalId, RemoteTarget, Seq,
    ShardCount,
};
use support::{Node, Patience, cluster, md5_etag, runtime, settings, shard_ref};

const PREFIX: &str = "team/";

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
        proposal_id: ProposalId::new("p-1").unwrap(),
    }
}

fn service(store: &SimS3, page_keys: u32, keys_per_second: u64) -> FlushService<SimS3, SimMount> {
    let store = store.clone();
    let settings = skys3_flush::FlushSettings {
        import_page_keys: page_keys,
        import_keys_per_second: keys_per_second,
        ..settings()
    };
    FlushService::new(
        cluster(),
        settings,
        Box::new(move |_: &RemoteTarget| store.clone()),
        FlushMetrics::register(&MetricsRegistry::new()),
    )
}

/// Stores `body` at the remote key `key`.
async fn remote_put(store: &SimS3, key: &str, body: &str) -> ETag {
    let request = PutObject::new(key, body.to_owned()).with_content_type("text/plain");
    store.put_object(request).await.unwrap().etag
}

/// Follows the bucket until its import is done, and returns its status.
async fn import(service: &FlushService<SimS3, SimMount>, node: &Node) -> ImportStatus {
    let patience = Patience::new();
    loop {
        service.reconcile(&[bucket()], &node.set).await;
        let status = service.status(&bucket().bucket_id).unwrap().import;
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

#[test]
fn an_attached_bucket_imports_its_remote_namespace_once() {
    runtime().block_on(async {
        let node = Node::open(81).await;
        let store = SimS3::new(81, SimS3Config::default());
        let mut etags = BTreeMap::new();
        for n in 0..25 {
            let key = format!("k{n:02}");
            etags.insert(
                key.clone(),
                remote_put(&store, &format!("{PREFIX}{key}"), &key).await,
            );
        }
        // Outside the prefix, the prefix itself, and the probe's scratch
        // keys are never imported.
        remote_put(&store, "other/k00", "x").await;
        remote_put(&store, PREFIX, "x").await;
        remote_put(&store, &format!("{PREFIX}.skys3-probe/0/a"), "x").await;

        let service = service(&store, 10, 1_000_000);
        let status = import(&service, &node).await;
        assert_eq!((status.imported, status.error), (25, None));
        let entries = node.shard.entries(None, usize::MAX).await.unwrap();
        let imported: BTreeMap<String, ETag> = entries
            .iter()
            .map(|(key, entry)| {
                assert_eq!(entry.state, EntryState::Evicted);
                let object = entry.object.as_ref().unwrap();
                assert_eq!(object.payload, Payload::None);
                assert_eq!(object.size, 3);
                assert_eq!(entry.remote_etag.as_ref(), Some(&object.local_etag));
                (key.clone(), object.local_etag.clone())
            })
            .collect();
        assert_eq!(imported, etags);
        // The progress is in the shard's log, not the node's own table.
        let (_, progress) = node.shard.import_progress().await.unwrap().unwrap();
        assert_eq!(progress, ImportCheckpoint::Done.into());
        let checkpoint = node.set.import_ranges(&bucket().bucket_id).await;
        assert_eq!(checkpoint.unwrap(), None);

        // A restarted node finds the import done and lists nothing.
        service.shutdown().await;
        let requests = store.stats().requests;
        let restarted = self::service(&store, 10, 1_000_000);
        let status = import(&restarted, &node).await;
        assert_eq!(status.imported, 0);
        restarted.shutdown().await;
        // The probe's requests only.
        let listings = store.stats().requests - requests;
        assert!(listings < 40, "{listings} requests");
    });
}

#[test]
fn an_import_resumes_from_its_checkpoint_at_its_rate() {
    runtime().block_on(async {
        let node = Node::open(82).await;
        let store = SimS3::new(82, SimS3Config::default());
        for n in 0..40 {
            remote_put(&store, &format!("{PREFIX}k{n:02}"), "body").await;
        }
        // A checkpoint a build that kept it in the node's index stored,
        // before progress moved into the log.
        let after = ImportCheckpoint::Running {
            after: Some("k09".to_owned()),
        };
        node.set
            .set_import_ranges(&bucket().bucket_id, Some(after.into()))
            .await
            .unwrap();
        // 30 keys in pages of 10, at 10 keys a second: each page waits.
        let service = service(&store, 10, 10);
        let started = tokio::time::Instant::now();
        let status = import(&service, &node).await;
        assert!(
            started.elapsed() >= Duration::from_secs(3),
            "{:?}",
            started.elapsed()
        );
        assert_eq!(status.imported, 30);
        let entries = node.shard.entries(None, usize::MAX).await.unwrap();
        assert_eq!(entries.len(), 30);
        assert_eq!(entries[0].0, "k10");
        service.shutdown().await;
    });
}

#[test]
fn an_import_of_one_page_keeps_to_its_rate() {
    runtime().block_on(async {
        let node = Node::open(87).await;
        let store = SimS3::new(87, SimS3Config::default());
        for n in 0..3 {
            remote_put(&store, &format!("{PREFIX}k{n}"), "body").await;
        }
        // Three keys fit one page of 1,000, but at one key a second the
        // first commits after a second and the last after three.
        let service = service(&store, 1000, 1);
        let started = tokio::time::Instant::now();
        let patience = Patience::new();
        loop {
            service.reconcile(&[bucket()], &node.set).await;
            let entries = node.shard.entries(None, usize::MAX).await.unwrap();
            let elapsed = started.elapsed();
            assert!(
                entries.len() as u64 <= elapsed.as_secs(),
                "{} keys after {elapsed:?}",
                entries.len()
            );
            let status = service.status(&bucket().bucket_id).unwrap().import;
            if status.checkpoint == ImportCheckpoint::Done {
                assert_eq!(status.imported, 3);
                break;
            }
            assert!(!patience.is_exhausted(), "the import never finished");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(started.elapsed() >= Duration::from_secs(3));
        service.shutdown().await;
    });
}

#[test]
fn writes_that_race_the_import_win_over_the_remote() {
    runtime().block_on(async {
        let node = Node::open(83).await;
        let store = SimS3::new(83, SimS3Config::default());
        for key in ["kept", "written", "deleted"] {
            remote_put(&store, &format!("{PREFIX}{key}"), "remote").await;
        }
        // Written before the import reached the keys: their remote state is
        // unknown when they are committed.
        let mine = node.put("written", "mine").await;
        node.delete("deleted").await;
        node.delete("absent").await;

        let service = service(&store, 1, 1_000_000);
        import(&service, &node).await;
        let patience = Patience::new();
        while !node.unclean().await.is_empty() {
            assert!(!patience.is_exhausted(), "{:?}", node.unclean().await);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let status = service.status(&bucket().bucket_id).unwrap();
        assert!(status.shards[0].1.conflicts.is_empty(), "{status:?}");

        // The local write replaced the remote object, conditioned on the
        // ETag the import recorded.
        let written = node.entry("written").await.unwrap();
        assert_eq!(
            (written.version.seq.get(), written.state),
            (mine, EntryState::Clean)
        );
        let remote = store.object(&format!("{PREFIX}written")).unwrap();
        assert_eq!(remote.body, Bytes::from("mine"));
        // The deletes stayed deleted, and nothing was resurrected.
        for key in ["deleted", "absent"] {
            assert_eq!(node.entry(key).await, None, "{key}");
            assert!(store.object(&format!("{PREFIX}{key}")).is_none(), "{key}");
        }
        assert_eq!(node.entry("kept").await.unwrap().state, EntryState::Evicted);
        service.shutdown().await;
    });
}

#[test]
fn remote_reads_see_the_bucket_under_its_prefix() {
    runtime().block_on(async {
        let node = Node::open(84).await;
        let store = SimS3::new(84, SimS3Config::default());
        let mut metadata = UserMetadata::new();
        metadata.insert("Color", "red").unwrap();
        metadata.insert("skys3-wid", "c/b/0/1.2").unwrap();
        let request = PutObject::new(format!("{PREFIX}a/1"), "hello").with_metadata(metadata);
        let etag = store.put_object(request).await.unwrap().etag;
        for key in ["a/2", "b", "c/1"] {
            remote_put(&store, &format!("{PREFIX}{key}"), "x").await;
        }
        let service = service(&store, 1000, 1_000_000);
        service.reconcile(&[bucket()], &node.set).await;
        let reader: RemoteReader<SimS3> = service.remote(&bucket().bucket_id).unwrap();
        assert!(
            service
                .remote(&skys3_types::BucketId::new("b-none").unwrap())
                .is_none()
        );

        let found = reader.head("a/1").await.unwrap().unwrap();
        assert_eq!(found.object.local_etag, etag);
        assert_eq!(found.object.size, 5);
        assert_eq!(
            found.object.metadata,
            BTreeMap::from([
                ("content-type".to_owned(), "binary/octet-stream".to_owned()),
                ("x-amz-meta-color".to_owned(), "red".to_owned()),
            ]),
            "the default type, and no write identity"
        );
        assert_eq!(reader.head("none").await.unwrap(), None);
        let body = reader.get("a/1", &etag, 1..3).await.unwrap();
        assert_eq!(body, Some(Bytes::from("el")));
        assert_eq!(
            reader.get("a/1", &etag, 2..2).await.unwrap(),
            Some(Bytes::new())
        );
        let other = ETag::new("0123456789abcdef0123456789abcdef").unwrap();
        assert_eq!(reader.get("a/1", &other, 0..5).await.unwrap(), None);
        assert_eq!(reader.get("none", &etag, 0..5).await.unwrap(), None);

        let names = |items: &[ListItem]| -> Vec<String> {
            items.iter().map(|item| item.name().to_owned()).collect()
        };
        let (items, next) = reader.list("", Some("/"), None, None, 2).await.unwrap();
        assert_eq!(names(&items), ["a/", "b"]);
        let (items, next) = reader.list("", Some("/"), None, next, 2).await.unwrap();
        assert_eq!((names(&items), next), (vec!["c/".to_owned()], None));
        let (items, _) = reader
            .list("a/", None, Some("a/1"), None, 10)
            .await
            .unwrap();
        assert_eq!(names(&items), ["a/2"]);
        let ListItem::Object { object, .. } = &items[0] else {
            panic!("{items:?}");
        };
        assert!(object.metadata.is_empty(), "a listing loads no metadata");
        service.shutdown().await;
    });
}

#[test]
fn loaded_metadata_always_has_a_content_type() {
    let info = |content_type: Option<&str>| ObjectInfo {
        etag: ETag::new("e").unwrap(),
        size: 0,
        version_id: None,
        metadata: UserMetadata::new(),
        content_type: content_type.map(str::to_owned),
        last_modified_ms: None,
    };
    assert_eq!(
        loaded_metadata(&info(Some("image/png")))["content-type"],
        "image/png"
    );
    for missing in [None, Some("")] {
        assert_eq!(
            loaded_metadata(&info(missing))["content-type"],
            "binary/octet-stream"
        );
    }
}

/// Index bytes per imported entry (design §19 item 6): the growth of a
/// fresh index's file after importing many stubs, made durable, divided by
/// their number. Keys are 40 bytes, like `photos/2024/06/IMG_20240612_0001.jpg`.
#[test]
fn index_bytes_per_imported_entry() {
    use skys3_log::record::Import;
    use skys3_log::{LogRecord, RecordBody, RecordLocation, SegmentId};
    use skys3_shard::StateMachine;

    const ENTRIES: u64 = 20_000;
    let disk = SimDisk::new(85);
    let mount = disk.mount();
    let index = Index::open_sim(&mount, "index.redb", &IndexConfig::default()).unwrap();
    index.checkpoint(&BTreeMap::new()).unwrap();
    let before = mount.open_block_file("index.redb").unwrap().len().unwrap();
    let shard = shard_ref();
    let location = RecordLocation {
        segment: SegmentId::new(0),
        offset: 0,
        len: 0,
    };
    let mut records = Vec::new();
    for n in 1..=ENTRIES {
        let import = Import {
            key: format!("photos/2024/06/IMG_20240612_{n:08}.jpg"),
            size: 3_500_000 + n,
            last_modified_ms: 1_718_000_000_000 + n,
            etag: md5_etag(&n.to_le_bytes()),
            storage_class: Some("STANDARD".to_owned()),
        };
        let position = EpochSeq::new(Epoch::new(1), Seq::new(n));
        let record = LogRecord {
            shard: shard.clone(),
            position,
            body: RecordBody::Import(import),
        };
        records.push((record, location));
        if records.len() == 1000 {
            index.apply(&StateMachine, &records).unwrap();
            records.clear();
        }
    }
    index.checkpoint(&BTreeMap::new()).unwrap();
    let after = mount.open_block_file("index.redb").unwrap().len().unwrap();
    let per_entry = (after - before) / ENTRIES;
    println!("index bytes per imported entry: {per_entry} ({ENTRIES} entries, 40-byte keys)");
    // An encoded stub is about 120 bytes; redb's B-tree pages roughly
    // double it.
    assert!(
        (100..=600).contains(&per_entry),
        "{per_entry} bytes per entry"
    );
}
