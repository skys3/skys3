//! Tests of the index's tables, checkpoints, and replay.

mod support;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use skys3_config::StorageConfig;
use skys3_index::{
    Applier, ControlEntry, Entry, EntryState, Index, IndexConfig, IndexError, IndexWriter, LogState,
};
use skys3_io::SimDisk;
use skys3_log::record::{Delete, RecordBody};
use skys3_log::{LogRecord, RecordLocation, SegmentId};
use skys3_types::{Epoch, EpochSeq, Generation, Seq};
use support::{
    TestApplier, Workload, apply, disk_label, index_config, log_of, open_node, pool, runtime, shard,
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
            table.insert("format_version", 2).unwrap();
        }
        txn.commit().unwrap();
    }
    let error = Index::open(&path, &IndexConfig::default()).unwrap_err();
    assert!(
        matches!(
            error,
            IndexError::UnsupportedFormat {
                found: 2,
                supported: 1
            }
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
        BTreeMap::from([("nodes/n-1.json".to_owned(), entry)])
    );
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
