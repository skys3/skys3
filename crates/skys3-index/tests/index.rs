//! Tests of the index's tables, checkpoints, and replay.

mod support;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use skys3_config::StorageConfig;
use skys3_index::{
    Applier, ControlEntry, Entry, EntryState, FORMAT_VERSION, ImportCheckpoint, ImportRanges,
    Index, IndexConfig, IndexError, IndexWriter, LogState, codec,
};
use skys3_io::{SimDisk, SimDiskFaults, SimPower, SyncCut};
use skys3_log::record::{Delete, RecordBody};
use skys3_log::{LogRecord, RecordLocation, SegmentId};
use skys3_types::{
    BucketId, Epoch, EpochSeq, Generation, NodeId, ProposalId, RegisterDocument, Seq, ShardConfig,
    ShardId,
};
use support::{
    TestApplier, Workload, apply, disk_label, disk_label_of, index_config, log_of, open_node,
    open_node_on, pool, runtime, shard,
};

fn position(seq: u64) -> EpochSeq {
    EpochSeq::new(Epoch::new(1), Seq::new(seq))
}

fn delete(shard_no: u8, seq: u64, key: &str) -> (LogRecord, RecordLocation) {
    let record = LogRecord {
        shard: shard(shard_no),
        position: position(seq),
        body: RecordBody::Delete(Delete { key: key.into() }),
    };
    let location = RecordLocation {
        segment: SegmentId::new(0),
        offset: seq * 100,
        len: 100,
    };
    (record, location)
}

fn tombstone(seq: u64) -> Entry {
    Entry {
        version: position(seq),
        state: EntryState::Dirty,
        object: None,
        remote_etag: None,
        remote_version_id: None,
    }
}

#[test]
fn a_real_file_keeps_what_a_checkpoint_made_durable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("index.redb");
    let index = Index::open(&path, &IndexConfig::default()).unwrap();
    let applied = index
        .apply(&TestApplier, &[delete(0, 1, "a"), delete(0, 2, "b")])
        .unwrap();
    assert_eq!(applied, 2);
    let checkpoint = index.checkpoint(&BTreeMap::new()).unwrap();
    assert_eq!(
        checkpoint.applied,
        BTreeMap::from([(shard(0), position(2))])
    );
    assert!(checkpoint.releasable.is_empty());
    drop(index);

    let index = Index::open(&path, &IndexConfig::default()).unwrap();
    let read = index.read().unwrap();
    assert_eq!(read.entry(&shard(0), "b").unwrap(), Some(tombstone(2)));
    assert_eq!(read.applied(&shard(0)).unwrap(), Some(position(2)));
    assert_eq!(read.applied(&shard(1)).unwrap(), None);
    assert!(format!("{index:?}").starts_with("Index"));
}

#[test]
fn removing_a_shard_keeps_every_other_shard() {
    let disk = SimDisk::new(5);
    let index = Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap();
    let extent = |shard_no: u8, seq: u64| {
        let (mut record, location) = delete(shard_no, seq, "big");
        record.body = RecordBody::Extent(skys3_log::record::Extent {
            key: "big".into(),
            offset: 0,
            data: Bytes::from_static(b"bytes"),
        });
        (record, location)
    };
    // Shard 255 is the last of its bucket, so its keys end in 0xff; 253 is
    // in the same bucket, and 254 in another.
    for n in [253, 254, 255] {
        index
            .apply(
                &TestApplier,
                &[delete(n, 1, "a"), extent(n, 2), delete(n, 3, "z")],
            )
            .unwrap();
    }
    index.remove_shard(&shard(255)).unwrap();
    let dump = index.read().unwrap().dump().unwrap();
    for n in [253, 254] {
        assert_eq!(
            dump.entries.keys().filter(|(s, _)| *s == shard(n)).count(),
            2
        );
        assert!(dump.locations.contains_key(&(shard(n), position(2))));
        assert_eq!(dump.applied.get(&shard(n)), Some(&position(3)));
    }
    assert!(dump.entries.keys().all(|(s, _)| *s != shard(255)));
    assert!(dump.locations.keys().all(|(s, _)| *s != shard(255)));
    assert!(!dump.applied.contains_key(&shard(255)));
    // The shard starts afresh.
    assert_eq!(
        index.apply(&TestApplier, &[delete(255, 1, "a")]).unwrap(),
        1
    );
}

#[test]
fn rejects_an_index_of_another_format() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("index.redb");
    {
        let db = redb::Database::create(&path).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let meta: redb::TableDefinition<&str, u64> = redb::TableDefinition::new("meta");
            let mut table = txn.open_table(meta).unwrap();
            table.insert("format_version", FORMAT_VERSION + 1).unwrap();
        }
        txn.commit().unwrap();
    }
    let error = Index::open(&path, &IndexConfig::default()).unwrap_err();
    assert!(
        matches!(
            error,
            IndexError::UnsupportedFormat {
                found,
                supported: FORMAT_VERSION
            } if found == FORMAT_VERSION + 1
        ),
        "{error}"
    );
    let error = Index::open(
        &dir.path().join("missing/index.redb"),
        &IndexConfig::default(),
    )
    .unwrap_err();
    assert!(matches!(error, IndexError::Io(_)), "{error}");
}

#[test]
fn import_checkpoints_are_durable_and_an_older_index_gets_their_table() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("index.redb");
    // A version 2 index, from before the imports table.
    {
        let db = redb::Database::create(&path).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let meta: redb::TableDefinition<&str, u64> = redb::TableDefinition::new("meta");
            txn.open_table(meta)
                .unwrap()
                .insert("format_version", 2)
                .unwrap();
        }
        txn.commit().unwrap();
    }
    let bucket = BucketId::new("b-import").unwrap();
    let running = ImportRanges::from(ImportCheckpoint::Running {
        after: Some("photos/cat.jpg".to_owned()),
    });
    {
        let index = Index::open(&path, &IndexConfig::default()).unwrap();
        assert_eq!(index.import_ranges(&bucket).unwrap(), None);
        index.set_import_ranges(&bucket, Some(&running)).unwrap();
    }
    let index = Index::open(&path, &IndexConfig::default()).unwrap();
    assert_eq!(index.import_ranges(&bucket).unwrap(), Some(running));
    let split = ImportRanges::split(None, ["m".to_owned(), "t".to_owned()]);
    index.set_import_ranges(&bucket, Some(&split)).unwrap();
    assert_eq!(index.import_ranges(&bucket).unwrap(), Some(split));
    let done = ImportRanges::from(ImportCheckpoint::Done);
    index.set_import_ranges(&bucket, Some(&done)).unwrap();
    assert_eq!(index.import_ranges(&bucket).unwrap(), Some(done));
    index.set_import_ranges(&bucket, None).unwrap();
    assert_eq!(index.import_ranges(&bucket).unwrap(), None);
}

#[test]
fn apply_skips_records_already_applied_and_aborts_on_an_error() {
    let disk = SimDisk::new(1);
    let index = Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap();
    assert_eq!(index.apply(&TestApplier, &[delete(0, 1, "a")]).unwrap(), 1);
    // A replayed record, and an older one, change nothing.
    let records = [delete(0, 1, "x"), delete(0, 0, "y"), delete(0, 2, "b")];
    assert_eq!(index.apply(&TestApplier, &records).unwrap(), 1);
    let read = index.read().unwrap();
    assert_eq!(read.entry(&shard(0), "x").unwrap(), None);
    assert_eq!(read.entry(&shard(0), "b").unwrap(), Some(tombstone(2)));

    struct Failing;
    impl Applier for Failing {
        fn apply(
            &self,
            index: &mut IndexWriter<'_>,
            record: &LogRecord,
            location: RecordLocation,
        ) -> Result<(), IndexError> {
            index.put_location(&record.shard, record.position, &location)?;
            if record.position.seq.get() == 4 {
                return Err(IndexError::Io(std::io::Error::other("refused")));
            }
            Ok(())
        }
    }
    let error = index
        .apply(&Failing, &[delete(0, 3, "c"), delete(0, 4, "d")])
        .unwrap_err();
    assert!(matches!(error, IndexError::Io(_)), "{error}");
    let read = index.read().unwrap();
    assert_eq!(read.applied(&shard(0)).unwrap(), Some(position(2)));
    assert_eq!(read.location(&shard(0), position(3)).unwrap(), None);
}

#[test]
fn writers_change_entries_and_locations() {
    struct Edit;
    impl Applier for Edit {
        fn apply(
            &self,
            index: &mut IndexWriter<'_>,
            record: &LogRecord,
            location: RecordLocation,
        ) -> Result<(), IndexError> {
            let shard = &record.shard;
            index.put_entry(shard, "k", &tombstone(record.position.seq.get()))?;
            index.put_location(shard, record.position, &location)?;
            assert_eq!(index.location(shard, record.position)?, Some(location));
            assert_eq!(index.applied(shard)?, None);
            assert!(index.remove_location(shard, record.position)?);
            assert!(!index.remove_location(shard, record.position)?);
            assert!(index.remove_entry(shard, "k")?);
            assert!(!index.remove_entry(shard, "k")?);
            assert_eq!(index.entry(shard, "k")?, None);
            Ok(())
        }
    }
    let disk = SimDisk::new(1);
    let index = Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap();
    assert_eq!(index.apply(&Edit, &[delete(3, 5, "k")]).unwrap(), 1);
    assert_eq!(
        index.read().unwrap().dump().unwrap().entries,
        BTreeMap::new()
    );
}

#[test]
fn lists_a_shards_entries_in_pages() {
    let disk = SimDisk::new(1);
    let index = Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap();
    let records: Vec<_> = ["b", "a", "d", "c"]
        .iter()
        .zip(1..)
        .map(|(key, seq)| delete(0, seq, key))
        .chain([delete(2, 1, "a"), delete(1, 1, "z")])
        .collect();
    index.apply(&TestApplier, &records).unwrap();
    let read = index.read().unwrap();
    let keys = |start_after, limit| -> Vec<String> {
        read.entries(&shard(0), start_after, limit)
            .unwrap()
            .into_iter()
            .map(|(key, _)| key)
            .collect()
    };
    assert_eq!(keys(None, 10), ["a", "b", "c", "d"]);
    assert_eq!(keys(None, 2), ["a", "b"]);
    assert_eq!(keys(Some("b"), 2), ["c", "d"]);
    assert_eq!(keys(Some("bb"), 10), ["c", "d"]);
    assert_eq!(keys(Some("d"), 10), Vec::<String>::new());
    assert_eq!(read.entries(&shard(4), None, 10).unwrap(), Vec::new());
}

#[test]
fn control_state_is_durable_at_once() {
    let disk = SimDisk::new(1);
    let index = Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap();
    let entry = ControlEntry {
        generation: Generation::new(4),
        version: "v1".into(),
        value: b"{}".to_vec(),
    };
    index
        .update_control(|control| {
            assert_eq!(control.get("nodes/n-1.json")?, None);
            control.put("nodes/n-1.json", &entry)?;
            control.put("nodes/n-2.json", &entry)?;
            assert!(control.remove("nodes/n-2.json")?);
            assert!(!control.remove("nodes/n-3.json")?);
            control.set_generation(Generation::new(4))
        })
        .unwrap();
    // A failed update changes nothing.
    let error = index
        .update_control(|control| {
            control.put("nodes/n-9.json", &entry)?;
            Err::<(), _>(IndexError::Io(std::io::Error::other("refused")))
        })
        .unwrap_err();
    assert!(matches!(error, IndexError::Io(_)));
    disk.crash();
    drop(index);

    let index = Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap();
    let read = index.read().unwrap();
    assert_eq!(read.control("nodes/n-1.json").unwrap(), Some(entry.clone()));
    assert_eq!(read.control("nodes/n-9.json").unwrap(), None);
    assert_eq!(read.control_generation().unwrap(), Some(Generation::new(4)));
    let dump = read.dump().unwrap();
    assert_eq!(
        dump.control,
        BTreeMap::from([("nodes/n-1.json".to_owned(), entry.clone())])
    );
    assert_eq!(read.control_entries().unwrap(), dump.control);
    assert_eq!(read.control_synced_at().unwrap(), None);
    drop(read);

    // A sync replaces the whole copy and records when it started.
    index
        .update_control(|control| {
            control.clear()?;
            control.put("buckets/b.json", &entry)?;
            control.set_synced_at(std::time::Duration::from_millis(1_234_567))
        })
        .unwrap();
    let read = index.read().unwrap();
    assert_eq!(
        read.control_entries().unwrap(),
        BTreeMap::from([("buckets/b.json".to_owned(), entry)])
    );
    assert_eq!(
        read.control_synced_at().unwrap(),
        Some(std::time::Duration::from_millis(1_234_567))
    );
}

fn route(shard_no: u8, epoch: u64) -> ShardConfig {
    let node: NodeId = "node-1".parse().unwrap();
    ShardConfig {
        bucket_id: shard(shard_no).bucket,
        shard: ShardId::new(shard_no),
        epoch: Epoch::new(epoch),
        primary: node.clone(),
        members: vec![node],
        learners: Vec::new(),
        min_write_replicas: 1,
        replicas: 1,
        proposal_id: ProposalId::new("p").unwrap(),
    }
}

#[test]
fn the_shard_map_is_durable_at_once() {
    let disk = SimDisk::new(1);
    let index = Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap();
    index.store_route(&route(0, 1)).unwrap();
    index.store_route(&route(0, 2)).unwrap();
    index.store_route(&route(1, 1)).unwrap();
    index.forget_route(&shard(1)).unwrap();
    index.forget_route(&shard(2)).unwrap();
    disk.crash();
    drop(index);

    let index = Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap();
    let map = index.read().unwrap().shard_map().unwrap();
    assert_eq!(map, BTreeMap::from([(shard(0), route(0, 2))]));

    let encoded = codec::encode_route(&route(3, 9)).unwrap();
    assert_eq!(codec::decode_route(&encoded).unwrap(), route(3, 9));
    let mut damaged = encoded;
    damaged.pop();
    assert!(codec::decode_route(&damaged).is_err());
    let mut wrong = codec::encode_route(&route(3, 9)).unwrap();
    let json_start = wrong.len() - route(3, 9).to_json().unwrap().len();
    wrong[json_start] = b'[';
    let error = codec::decode_route(&wrong).unwrap_err();
    assert_eq!(error.field(), "shard_map.config");
}

#[test]
fn step_downs_are_durable_at_once_and_go_with_their_shard() {
    let disk = SimDisk::new(2);
    let index = Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap();
    assert_eq!(index.read().unwrap().step_down(&shard(0)).unwrap(), None);
    index.store_step_down(&shard(0), Epoch::new(3)).unwrap();
    index.store_step_down(&shard(0), Epoch::new(4)).unwrap();
    index.store_step_down(&shard(1), Epoch::new(2)).unwrap();
    disk.crash();
    drop(index);

    let index = Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap();
    let read = index.read().unwrap();
    assert_eq!(read.step_down(&shard(0)).unwrap(), Some(Epoch::new(4)));
    assert_eq!(read.step_down(&shard(1)).unwrap(), Some(Epoch::new(2)));
    drop(read);
    index.remove_shard(&shard(1)).unwrap();
    assert_eq!(index.read().unwrap().step_down(&shard(1)).unwrap(), None);
}

#[test]
fn promotions_are_durable_at_once_and_go_with_their_shard() {
    let disk = SimDisk::new(3);
    let index = Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap();
    assert_eq!(index.read().unwrap().promotion(&shard(0)).unwrap(), None);
    index.store_promotion(&route(0, 3)).unwrap();
    index.store_promotion(&route(0, 4)).unwrap();
    index.store_promotion(&route(1, 2)).unwrap();
    disk.crash();
    drop(index);

    let index = Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap();
    let read = index.read().unwrap();
    assert_eq!(read.promotion(&shard(0)).unwrap(), Some(route(0, 4)));
    assert_eq!(read.promotion(&shard(1)).unwrap(), Some(route(1, 2)));
    drop(read);
    index.remove_shard(&shard(1)).unwrap();
    assert_eq!(index.read().unwrap().promotion(&shard(1)).unwrap(), None);
}

#[test]
fn an_index_from_before_the_shard_map_gains_one() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("index.redb");
    {
        // An index of the current format, written before the shard map.
        let index = Index::open(&path, &IndexConfig::default()).unwrap();
        index.apply(&TestApplier, &[delete(0, 1, "a")]).unwrap();
        index.checkpoint(&BTreeMap::new()).unwrap();
        drop(index);
        let db = redb::Database::create(&path).unwrap();
        let txn = db.begin_write().unwrap();
        let map: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("shard_map");
        assert!(txn.delete_table(map).unwrap());
        txn.commit().unwrap();
    }
    let index = Index::open(&path, &IndexConfig::default()).unwrap();
    assert!(index.read().unwrap().shard_map().unwrap().is_empty());
    assert_eq!(
        index.read().unwrap().entry(&shard(0), "a").unwrap(),
        Some(tombstone(1))
    );
    index.store_route(&route(0, 1)).unwrap();
    assert_eq!(index.read().unwrap().shard_map().unwrap().len(), 1);
}

#[test]
fn takes_its_settings_from_the_storage_section() {
    let storage = StorageConfig {
        index_checkpoint_interval_seconds: 3,
        ..StorageConfig::default()
    };
    let config = IndexConfig::from_storage(&storage);
    assert_eq!(config.checkpoint_interval, Duration::from_secs(3));
    assert_eq!(config.cache_bytes, IndexConfig::DEFAULT_CACHE_BYTES);
    assert_eq!(
        IndexConfig::default().checkpoint_interval,
        Duration::from_secs(10)
    );
}

#[test]
fn checkpoints_release_segments_behind_them() {
    runtime().block_on(async {
        let disk = SimDisk::new(2);
        let pool = pool();
        let node = open_node(&disk.mount(), &pool).await;
        let mut workload = Workload::new(2, 2);
        for _ in 0..30 {
            workload.step(&node).await;
        }
        // A record appended but not yet applied holds back its segment.
        let pending = workload.append(&node).await;
        let checkpoint = node.checkpoint().await.unwrap();
        let released = log_of(&node).released();
        let held = pending[0].1.segment;
        assert!(!released.contains(&held));
        assert_eq!(
            checkpoint.releasable[&disk_label()],
            released.iter().copied().collect::<Vec<_>>()
        );
        assert!(!released.is_empty());
        // The last segment of each class is never released.
        let state = LogState::of(log_of(&node));
        assert!(state.segments.len() > released.len() + 1);

        apply(&node, &pending);
        node.checkpoint().await.unwrap();
        let released_now = log_of(&node).released();
        assert!(released_now.is_superset(&released));
        let last = state.segments.iter().map(|s| s.id).max().unwrap();
        assert!(!released_now.contains(&last));

        // Replay after a clean restart reads only what follows the
        // checkpoint, and skips what the checkpoint released.
        let before = node.index().read().unwrap().dump().unwrap();
        drop(node);
        let node = open_node(&disk.mount(), &pool).await;
        let report = node.replay(Arc::new(TestApplier)).await.unwrap();
        assert_eq!(report.applied, 0);
        for segment in released_now {
            assert!(!report.scanned.contains_key(&(disk_label(), segment)));
        }
        assert_eq!(node.index().read().unwrap().dump().unwrap(), before);
        // Released segments keep their coverage through a later replay.
        let report = node.replay(Arc::new(TestApplier)).await.unwrap();
        assert!(report.scanned.is_empty());
    });
}

#[test]
fn a_failed_checkpoint_stops_checkpoints_and_replay_recovers() {
    runtime().block_on(async {
        let disk = SimDisk::new(3);
        let pool = pool();
        let node = open_node(&disk.mount(), &pool).await;
        let mut workload = Workload::new(3, 2);
        for _ in 0..10 {
            workload.step(&node).await;
        }
        node.checkpoint().await.unwrap();
        for _ in 0..10 {
            workload.step(&node).await;
        }
        let before = node.index().read().unwrap().dump().unwrap();
        disk.fail_next_syncs(1);
        let error = node.checkpoint().await.unwrap_err();
        assert!(matches!(error, IndexError::Storage(_)), "{error}");
        // redb refuses further writes after an I/O error.
        let error = node.run(Duration::from_millis(1)).await;
        assert!(matches!(error, IndexError::Storage(_)), "{error}");

        disk.crash();
        drop(node);
        let node = open_node(&disk.mount(), &pool).await;
        let report = node.replay(Arc::new(TestApplier)).await.unwrap();
        assert!(report.applied > 0);
        assert_eq!(node.index().read().unwrap().dump().unwrap(), before);
    });
}

#[test]
fn checkpoints_run_periodically() {
    runtime().block_on(async {
        let disk = SimDisk::new(4);
        let pool = pool();
        let node = open_node(&disk.mount(), &pool).await;
        let mut workload = Workload::new(4, 1);
        for _ in 0..30 {
            workload.step(&node).await;
        }
        assert!(log_of(&node).released().is_empty());
        let run = tokio::time::timeout(
            Duration::from_millis(50),
            node.run(Duration::from_millis(5)),
        );
        assert!(run.await.is_err(), "checkpoints keep running");
        assert!(!log_of(&node).released().is_empty());
    });
}

#[test]
fn replay_refuses_a_damaged_record() {
    runtime().block_on(async {
        let disk = SimDisk::new(5);
        let pool = pool();
        let node = open_node(&disk.mount(), &pool).await;
        let (record, _) = delete(0, 1, "photos/cat.jpg");
        let mut bytes = record.to_bytes().unwrap().to_vec();
        // Change the key under a valid CRC: the header's key hash no longer
        // matches, which only decoding the body finds.
        let at = bytes.len() - 1;
        bytes[at] = b'x';
        let crc = crc32c::crc32c(&bytes[8..]);
        bytes[4..8].copy_from_slice(&crc.to_le_bytes());
        let location = log_of(&node)
            .append_encoded(Bytes::from(bytes))
            .await
            .unwrap();
        drop(node);

        let node = open_node(&disk.mount(), &pool).await;
        let error = node.replay(Arc::new(TestApplier)).await.unwrap_err();
        match error {
            IndexError::Damaged { location: at, .. } => assert_eq!(at, location),
            error => panic!("{error}"),
        }
    });
}

#[test]
fn replay_orders_a_shards_records_across_disks() {
    runtime().block_on(async {
        let disks = [SimDisk::new(6), SimDisk::new(7)];
        let pool = pool();
        let mounts = disks.each_ref().map(SimDisk::mount);
        let node = open_node_on(&mounts, &pool).await;
        // Shard 0's first record is on the second disk by label, and its
        // second on the first, which replay reads first.
        let (first, _) = delete(0, 1, "first");
        let (second, _) = delete(0, 2, "second");
        let logs = node.logs();
        let at_1 = logs[&disk_label_of(1)].append(&first).await.unwrap();
        let at_0 = logs[&disk_label_of(0)].append(&second).await.unwrap();
        apply(&node, &[(first, at_1), (second, at_0)]);
        let before = node.index().read().unwrap().dump().unwrap();
        for disk in &disks {
            disk.crash();
        }
        drop(node);

        let mounts = disks.each_ref().map(SimDisk::mount);
        let node = open_node_on(&mounts, &pool).await;
        assert!(
            node.index()
                .read()
                .unwrap()
                .dump()
                .unwrap()
                .entries
                .is_empty()
        );
        let report = node.replay(Arc::new(TestApplier)).await.unwrap();
        assert_eq!(report.applied, 2);
        let after = node.index().read().unwrap().dump().unwrap();
        assert_eq!(after, before);
        assert_eq!(after.entries.len(), 2);
    });
}

/// A member reconciled with a new primary (§6.6): it truncated two
/// records of epoch 2 past seq 1, then took the primary's records from
/// seq 2 on, of epoch 3 and later. Replay applies the valid ones in
/// position order, and neither the truncated records nor the `TRUNCATE`,
/// whose position is past records it keeps valid.
#[test]
fn replay_skips_what_a_truncate_invalidates() {
    runtime().block_on(async {
        let disk = SimDisk::new(8);
        let pool = pool();
        let node = open_node(&disk.mount(), &pool).await;
        let at = |epoch: u64, seq: u64| EpochSeq::new(Epoch::new(epoch), Seq::new(seq));
        let record = |position: EpochSeq, body: RecordBody| LogRecord {
            shard: shard(0),
            position,
            body,
        };
        let delete =
            |position, key: &str| record(position, RecordBody::Delete(Delete { key: key.into() }));
        let log = log_of(&node);
        for written in [
            delete(at(1, 1), "kept"),
            delete(at(2, 2), "truncated-2"),
            delete(at(2, 3), "truncated-3"),
            record(at(3, 1), RecordBody::Truncate),
            delete(at(3, 2), "taken-2"),
            delete(at(4, 3), "taken-3"),
            // Another shard's records are not its business.
            LogRecord {
                shard: shard(1),
                ..delete(at(2, 2), "other")
            },
        ] {
            log.append(&written).await.unwrap();
        }
        disk.crash();
        drop(node);

        let node = open_node(&disk.mount(), &pool).await;
        let report = node.replay(Arc::new(TestApplier)).await.unwrap();
        // Three records of shard 0, the TRUNCATE, and the other shard's.
        assert_eq!(report.applied, 5);
        let reader = node.index().read().unwrap();
        let keys: Vec<String> = reader
            .entries(&shard(0), None, 10)
            .unwrap()
            .into_iter()
            .map(|(key, _)| key)
            .collect();
        assert_eq!(keys, ["kept", "taken-2", "taken-3"]);
        assert_eq!(reader.applied(&shard(0)).unwrap(), Some(at(4, 3)));
        assert!(reader.entry(&shard(1), "other").unwrap().is_some());
    });
}

/// redb's magic number, which a complete database file begins with.
const REDB_MAGIC: [u8; 9] = [b'r', b'e', b'd', b'b', 0x1A, 0x0A, 0xA9, 0x0D, 0x0A];

#[test]
fn a_creation_cut_short_by_a_power_loss_is_redone() {
    // redb creates a database with two syncs: the header, then its magic
    // number. The power fails just after the first, or just before the
    // second, with or without a torn write of the magic number.
    for (sync, cut) in [(0, SyncCut::After), (1, SyncCut::Before)] {
        for seed in 0..8 {
            let faults = SimDiskFaults {
                torn_write_probability: 0.5,
                ..SimDiskFaults::default()
            };
            let disk = SimDisk::with_faults(seed, faults);
            let power = SimPower::new();
            disk.set_power(&power);
            power.cut_at_sync(sync, cut);
            Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap_err();
            assert!(power.is_cut());
            let index = Index::open_sim(&disk.mount(), "index.redb", &index_config())
                .unwrap_or_else(|error| panic!("{cut:?} sync {sync}, seed {seed}: {error}"));
            index.apply(&TestApplier, &[delete(0, 1, "a")]).unwrap();
            assert_eq!(
                index.read().unwrap().entry(&shard(0), "a").unwrap(),
                Some(tombstone(1))
            );
        }
    }
}

#[test]
fn a_real_file_without_its_magic_number_is_created_again() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("index.redb");
    drop(Index::open(&path, &IndexConfig::default()).unwrap());
    let written = std::fs::read(&path).unwrap();
    assert_eq!(written[..REDB_MAGIC.len()], REDB_MAGIC);

    // A header without the magic number, whole or torn.
    for prefix in [0, 4, 8] {
        let mut header = vec![0; 4096];
        header[..prefix].copy_from_slice(&REDB_MAGIC[..prefix]);
        std::fs::write(&path, &header).unwrap();
        let index = Index::open(&path, &IndexConfig::default()).unwrap();
        index.apply(&TestApplier, &[delete(0, 1, "a")]).unwrap();
        drop(index);
    }
    std::fs::write(&path, &REDB_MAGIC[..3]).unwrap();
    drop(Index::open(&path, &IndexConfig::default()).unwrap());

    // Anything else is not taken for an unfinished index.
    std::fs::write(&path, b"not an index at all").unwrap();
    let error = Index::open(&path, &IndexConfig::default()).unwrap_err();
    assert!(error.to_string().contains("magic number"), "{error}");
    assert_eq!(std::fs::read(&path).unwrap(), b"not an index at all");
}
