//! Tests of the uploads and parts tables, and of opening an index written
//! before they existed.

mod support;

use std::collections::BTreeMap;

use skys3_index::{
    FORMAT_VERSION, Index, IndexError, IndexWriter, Part, Payload, RemotePart, RemoteUpload,
    ShardTable, Upload,
};
use skys3_log::record::{ChecksumAlgorithm, ExtentRef, ShardRef, UploadChecksum};
use skys3_log::{LogRecord, RecordLocation};
use skys3_types::{ETag, Epoch, EpochSeq, Seq};
use support::{index_config, shard};

fn position(seq: u64) -> EpochSeq {
    EpochSeq::new(Epoch::new(1), Seq::new(seq))
}

fn upload(initiated_ms: u64) -> Upload {
    Upload {
        initiated_ms,
        metadata: BTreeMap::from([("content-type".to_owned(), "a/b".to_owned())]),
        tags: BTreeMap::new(),
        checksum: UploadChecksum::of(ChecksumAlgorithm::Crc32),
    }
}

fn part(seq: u64, payload: Payload) -> Part {
    Part {
        position: position(seq),
        size: 3,
        last_modified_ms: 9,
        etag: ETag::new("900150983cd24fb0d6963f7d28e17f72").unwrap(),
        checksums: BTreeMap::new(),
        payload,
    }
}

/// Runs `edit` in one applied write transaction.
fn write(index: &Index, edit: impl Fn(&mut IndexWriter<'_>) -> Result<(), IndexError>) {
    struct Edit<F>(F);
    impl<F: Fn(&mut IndexWriter<'_>) -> Result<(), IndexError>> skys3_index::Applier for Edit<F> {
        fn apply(
            &self,
            index: &mut IndexWriter<'_>,
            _: &LogRecord,
            _: RecordLocation,
        ) -> Result<(), IndexError> {
            (self.0)(index)
        }
    }
    let applied = index.read().unwrap().applied(&shard(9)).unwrap();
    let next = applied.map_or(1, |p| p.seq.get() + 1);
    let record = LogRecord {
        shard: shard(9),
        position: position(next),
        body: skys3_log::RecordBody::Truncate,
    };
    let location = RecordLocation {
        segment: skys3_log::SegmentId::new(0),
        offset: 0,
        len: 1,
    };
    index.apply(&Edit(edit), &[(record, location)]).unwrap();
}

#[test]
fn uploads_and_parts_are_stored_listed_and_removed() {
    let disk = skys3_io::SimDisk::new(1);
    let index = Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap();
    let s = shard(0);
    let other = shard(1);
    write(&index, |w| {
        for (key, seq) in [("a/1", 3), ("a/1", 7), ("a/2", 5), ("b", 4)] {
            w.put_upload(&s, key, position(seq), &upload(seq))?;
        }
        w.put_upload(&other, "a/1", position(2), &upload(2))?;
        w.put_part(&s, position(3), 2, &part(8, Payload::Inline(position(8))))?;
        let extents = vec![ExtentRef {
            position: position(9),
            len: 3,
        }];
        w.put_part(&s, position(3), 1, &part(10, Payload::Extents(extents)))?;
        w.put_part(&s, position(7), 1, &part(11, Payload::Inline(position(11))))?;
        assert_eq!(w.upload(&s, "a/1", position(3))?, Some(upload(3)));
        assert_eq!(w.upload(&s, "a/1", position(4))?, None);
        let numbers: Vec<_> = w.parts(&s, position(3))?.into_iter().map(|p| p.0).collect();
        assert_eq!(numbers, [1, 2]);
        assert_eq!(w.part(&s, position(3), 2)?.unwrap().position, position(8));
        Ok(())
    });

    let read = index.read().unwrap();
    let listed = |prefix: &str, after: Option<(&str, Option<EpochSeq>)>, limit| {
        read.uploads(&s, prefix, after, limit)
            .unwrap()
            .into_iter()
            .map(|(key, upload, _)| (key, upload.seq.get()))
            .collect::<Vec<_>>()
    };
    let all = [("a/1", 3), ("a/1", 7), ("a/2", 5), ("b", 4)].map(|(k, s)| (k.to_owned(), s));
    assert_eq!(listed("", None, 100), all);
    assert_eq!(listed("", None, 2), all[..2]);
    assert_eq!(listed("a/", None, 100), all[..3]);
    assert_eq!(listed("", Some(("a/1", Some(position(3)))), 100), all[1..]);
    assert_eq!(listed("", Some(("a/1", None)), 100), all[2..]);
    // A marker before the prefix starts at the prefix.
    assert_eq!(listed("b", Some(("a", None)), 100), all[3..]);
    assert_eq!(listed("c", None, 100), []);
    assert_eq!(read.upload(&s, "b", position(4)).unwrap(), Some(upload(4)));

    let page = read.parts(&s, position(3), 1, 10).unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].0, 2);
    assert_eq!(read.parts(&s, position(3), u16::MAX, 10).unwrap(), []);
    assert_eq!(read.parts(&s, position(3), 0, 1).unwrap().len(), 1);

    let dump = read.dump().unwrap();
    assert_eq!(dump.uploads.len(), 5);
    assert_eq!(dump.parts.len(), 3);
    drop(read);

    write(&index, |w| {
        assert!(w.remove_upload(&s, "b", position(4))?);
        assert!(!w.remove_upload(&s, "b", position(4))?);
        assert!(w.remove_part(&s, position(3), 2)?);
        assert!(!w.remove_part(&s, position(3), 2)?);
        Ok(())
    });
    index.remove_shard(&s).unwrap();
    let dump = index.read().unwrap().dump().unwrap();
    let keys: Vec<_> = dump.uploads.keys().cloned().collect();
    assert_eq!(keys, [(other.clone(), "a/1".to_owned(), position(2))]);
    assert!(dump.parts.is_empty());
}

#[test]
fn remote_uploads_are_stored_replaced_removed_and_copied() {
    let disk = skys3_io::SimDisk::new(2);
    let index = Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap();
    let (s, other) = (shard(0), shard(1));
    let remote = |key: &str, id: &str| RemoteUpload {
        key: key.to_owned(),
        id: id.to_owned(),
    };
    let sent = |seq| RemotePart {
        position: position(seq),
        etag: ETag::new(format!("e{seq}")).unwrap(),
    };
    write(&index, |w| {
        w.put_remote_upload(&s, position(3), &remote("a", "r1"))?;
        w.put_remote_upload(&s, position(5), &remote("b", "r2"))?;
        w.put_remote_upload(&other, position(3), &remote("a", "r3"))?;
        w.put_remote_part(&s, position(3), 2, &sent(8))?;
        w.put_remote_part(&s, position(3), 1, &sent(7))?;
        w.put_remote_part(&s, position(5), 1, &sent(9))?;
        assert_eq!(w.remote_upload(&s, position(3))?, Some(remote("a", "r1")));
        assert_eq!(w.remote_upload(&s, position(4))?, None);
        Ok(())
    });
    let read = index.read().unwrap();
    assert_eq!(
        read.remote_upload(&s, position(3)).unwrap(),
        Some((remote("a", "r1"), vec![(1, sent(7)), (2, sent(8))]))
    );
    assert_eq!(read.remote_upload(&s, position(4)).unwrap(), None);
    assert_eq!(
        read.remote_uploads(&s).unwrap(),
        [
            (position(3), remote("a", "r1")),
            (position(5), remote("b", "r2"))
        ]
    );
    let dump = read.dump().unwrap();
    assert_eq!(dump.remote_uploads.len(), 3);
    assert_eq!(dump.remote_parts.len(), 3);

    // The rows of a shard copy to a learner whole.
    let learner = Index::open_sim(&disk.mount(), "learner.redb", &index_config()).unwrap();
    learner.begin_install(&s, position(20)).unwrap();
    for table in [ShardTable::RemoteUploads, ShardTable::RemoteParts] {
        assert_eq!(ShardTable::from_code(table.code()), Some(table));
        let rows = read.shard_rows(table, &s, None, 1 << 20).unwrap();
        // Rows of one table do not install in the other.
        let wrong = match table {
            ShardTable::RemoteUploads => ShardTable::RemoteParts,
            _ => ShardTable::RemoteUploads,
        };
        assert!(matches!(
            learner.install_rows(&s, wrong, &rows),
            Err(IndexError::Snapshot(_))
        ));
        learner.install_rows(&s, table, &rows).unwrap();
    }
    let copied = learner.read().unwrap();
    assert_eq!(
        copied.remote_upload(&s, position(3)).unwrap(),
        read.remote_upload(&s, position(3)).unwrap()
    );
    assert_eq!(
        copied.remote_uploads(&s).unwrap(),
        read.remote_uploads(&s).unwrap()
    );
    assert!(copied.remote_uploads(&other).unwrap().is_empty());
    drop((read, copied));

    // A new remote upload replaces the row and drops its parts; removing
    // one removes its parts.
    write(&index, |w| {
        w.put_remote_upload(&s, position(3), &remote("a", "r4"))?;
        assert!(w.remove_remote_upload(&s, position(5))?);
        assert!(!w.remove_remote_upload(&s, position(5))?);
        Ok(())
    });
    let read = index.read().unwrap();
    assert_eq!(
        read.remote_upload(&s, position(3)).unwrap(),
        Some((remote("a", "r4"), Vec::new()))
    );
    assert_eq!(read.dump().unwrap().remote_parts.len(), 0);
    drop(read);
    index.remove_shard(&s).unwrap();
    let dump = index.read().unwrap().dump().unwrap();
    let keys: Vec<_> = dump.remote_uploads.keys().cloned().collect();
    assert_eq!(keys, [(other, position(3))]);
}

#[test]
fn parts_never_hold_parts() {
    let disk = skys3_io::SimDisk::new(1);
    let index = Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap();
    struct Put(ShardRef);
    impl skys3_index::Applier for Put {
        fn apply(
            &self,
            index: &mut IndexWriter<'_>,
            _: &LogRecord,
            _: RecordLocation,
        ) -> Result<(), IndexError> {
            let nested = Payload::Parts {
                upload: position(1),
                parts: Vec::new(),
            };
            index.put_part(&self.0, position(1), 1, &part(2, nested))
        }
    }
    let record = LogRecord {
        shard: shard(0),
        position: position(1),
        body: skys3_log::RecordBody::Truncate,
    };
    let location = RecordLocation {
        segment: skys3_log::SegmentId::new(0),
        offset: 0,
        len: 1,
    };
    let error = index
        .apply(&Put(shard(0)), &[(record, location)])
        .unwrap_err();
    assert!(matches!(error, IndexError::Codec { .. }), "{error}");
}

#[test]
fn an_index_of_format_1_is_opened_and_upgraded() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("index.redb");
    {
        let db = redb::Database::create(&path).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let meta: redb::TableDefinition<&str, u64> = redb::TableDefinition::new("meta");
            let mut table = txn.open_table(meta).unwrap();
            table.insert("format_version", 1).unwrap();
        }
        txn.commit().unwrap();
    }
    let index = Index::open(&path, &index_config()).unwrap();
    assert!(index.read().unwrap().dump().unwrap().uploads.is_empty());
    drop(index);
    let db = redb::Database::open(&path).unwrap();
    let txn = redb::ReadableDatabase::begin_read(&db).unwrap();
    let meta: redb::TableDefinition<&str, u64> = redb::TableDefinition::new("meta");
    let table = txn.open_table(meta).unwrap();
    assert_eq!(
        table.get("format_version").unwrap().unwrap().value(),
        FORMAT_VERSION
    );
}
