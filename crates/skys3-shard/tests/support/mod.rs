//! Shared test support: records, indexes on simulated disks, and nodes
//! with a log.

#![allow(dead_code, reason = "each test target uses part of the support")]

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use skys3_index::{
    Applier, Entry, EntryState, Index, IndexConfig, IndexDump, IndexError, IndexWriter,
};
use skys3_io::{BlockingPool, MonotonicClock, SimDisk, SimMount};
use skys3_log::record::{
    Adopt, CompletedPart, Delete, Extent, ExtentRef, Flushed, Import, MpuAbort, MpuComplete,
    MpuCreate, MpuPart, Put, PutData, Tags, UploadBegin,
};
use skys3_log::{
    LogConfig, LogRecord, RecordBody, RecordLocation, SegmentId, SegmentLog, ShardRef,
};
use skys3_shard::{Outcome, Recorder};
use skys3_types::{BucketId, ETag, Epoch, EpochSeq, NodeId, ProposalId, Seq, ShardConfig, ShardId};

/// Shard `n` of bucket `b-test`.
pub fn shard(n: u8) -> ShardRef {
    ShardRef::new(BucketId::new("b-test").unwrap(), ShardId::new(n))
}

/// The position `seq` in epoch 1.
pub fn at(seq: u64) -> EpochSeq {
    EpochSeq::new(Epoch::new(1), Seq::new(seq))
}

/// An ETag made from `n`.
pub fn etag(n: u64) -> ETag {
    ETag::new(format!("{n:032x}")).unwrap()
}

/// `len` bytes of a fixed pattern.
pub fn fill(len: usize) -> Bytes {
    (0..len).map(|i| (i % 251) as u8).collect::<Vec<_>>().into()
}

/// A `PUT` of `key` with `len` inline bytes and ETag `etag(tag)`.
pub fn put(key: &str, len: usize, tag: u64) -> RecordBody {
    RecordBody::Put(Put {
        key: key.to_owned(),
        size: len as u64,
        last_modified_ms: 1_700_000_000_000 + tag,
        etag: etag(tag),
        inherited_identity: None,
        metadata: BTreeMap::from([("content-type".to_owned(), "text/plain".to_owned())]),
        tags: BTreeMap::new(),
        checksums: BTreeMap::new(),
        copy_source: None,
        data: PutData::Inline(fill(len)),
    })
}

/// A `PUT` of `key` whose bytes are in `extents`.
pub fn put_extents(key: &str, extents: Vec<ExtentRef>, tag: u64) -> RecordBody {
    let RecordBody::Put(mut body) = put(key, 0, tag) else {
        unreachable!()
    };
    body.size = extents.iter().map(|e| u64::from(e.len)).sum();
    body.data = PutData::Extents(extents);
    RecordBody::Put(body)
}

/// An `EXTENT` of `key` at `offset` with `len` bytes.
pub fn extent(key: &str, offset: u64, len: usize) -> Extent {
    Extent {
        key: key.to_owned(),
        offset,
        data: fill(len),
    }
}

/// The `UPLOAD_BEGIN` of a streamed PUT of `key`.
pub fn upload_begin(key: &str) -> RecordBody {
    RecordBody::UploadBegin(UploadBegin {
        key: key.to_owned(),
    })
}

/// `put`, inheriting the write identity of the `UPLOAD_BEGIN` at `begin`.
pub fn streamed(put: RecordBody, begin: EpochSeq) -> RecordBody {
    let RecordBody::Put(mut body) = put else {
        panic!("only a PUT inherits an identity")
    };
    body.inherited_identity = Some(begin);
    RecordBody::Put(body)
}

/// A `DELETE` of `key`.
pub fn delete(key: &str) -> RecordBody {
    RecordBody::Delete(Delete {
        key: key.to_owned(),
    })
}

/// A `TAGS` of `key` with one tag.
pub fn tags(key: &str, value: &str) -> RecordBody {
    RecordBody::Tags(Tags {
        key: key.to_owned(),
        tags: BTreeMap::from([("t".to_owned(), value.to_owned())]),
    })
}

/// A `FLUSHED` of `key` at `seq`, with a remote ETag unless `deleted`.
pub fn flushed(key: &str, seq: u64, deleted: bool) -> RecordBody {
    RecordBody::Flushed(Flushed {
        key: key.to_owned(),
        seq: Seq::new(seq),
        remote_etag: (!deleted).then(|| etag(1000 + seq)),
        remote_version_id: (!deleted).then(|| format!("v{seq}")),
    })
}

/// An `IMPORT` of `key`.
pub fn import(key: &str, tag: u64) -> RecordBody {
    RecordBody::Import(Import {
        key: key.to_owned(),
        size: 42,
        last_modified_ms: 1_600_000_000_000,
        etag: etag(tag),
        storage_class: Some("STANDARD".to_owned()),
    })
}

/// An `ADOPT` of `key`, expecting it clean at `expected_seq`.
pub fn adopt(key: &str, expected_seq: u64, tag: u64) -> RecordBody {
    RecordBody::Adopt(Adopt {
        key: key.to_owned(),
        expected_seq: Seq::new(expected_seq),
        size: 7,
        last_modified_ms: 1_650_000_000_000,
        remote_etag: etag(tag),
        remote_version_id: Some("remote".to_owned()),
        metadata: BTreeMap::from([("content-type".to_owned(), "image/png".to_owned())]),
        checksums: BTreeMap::new(),
    })
}

/// An `MPU_CREATE` of `key`.
pub fn mpu_create(key: &str) -> RecordBody {
    RecordBody::MpuCreate(MpuCreate {
        key: key.to_owned(),
        initiated_ms: 1_700_000_000_000,
        metadata: BTreeMap::from([("content-type".to_owned(), "video/mp4".to_owned())]),
        tags: BTreeMap::new(),
        checksum: None,
    })
}

/// An `MPU_PART` of `key`'s upload at `upload`: part `number`, with `len`
/// inline bytes, or in `extents` if there are any.
pub fn mpu_part(
    key: &str,
    upload: EpochSeq,
    number: u16,
    len: usize,
    extents: Vec<ExtentRef>,
) -> RecordBody {
    let data = if extents.is_empty() {
        PutData::Inline(fill(len))
    } else {
        PutData::Extents(extents)
    };
    let size = match &data {
        PutData::Inline(bytes) => bytes.len() as u64,
        PutData::Extents(extents) => extents.iter().map(|e| u64::from(e.len)).sum(),
    };
    RecordBody::MpuPart(MpuPart {
        key: key.to_owned(),
        upload,
        part_number: number,
        size,
        last_modified_ms: 1_700_000_000_001,
        etag: etag(u64::from(number)),
        checksums: BTreeMap::new(),
        data,
    })
}

/// An `MPU_COMPLETE` of `key`'s upload at `upload` with `parts`, numbers
/// and positions, of `size` bytes in all.
pub fn mpu_complete(
    key: &str,
    upload: EpochSeq,
    parts: &[(u16, EpochSeq)],
    size: u64,
) -> RecordBody {
    RecordBody::MpuComplete(MpuComplete {
        key: key.to_owned(),
        upload,
        last_modified_ms: 1_700_000_000_002,
        size,
        etag: ETag::new(format!("{:032x}-{}", upload.seq.get(), parts.len())).unwrap(),
        checksums: BTreeMap::new(),
        parts: parts
            .iter()
            .map(|&(number, position)| CompletedPart { number, position })
            .collect(),
    })
}

/// An `MPU_ABORT` of `key`'s upload at `upload`.
pub fn mpu_abort(key: &str, upload: EpochSeq) -> RecordBody {
    RecordBody::MpuAbort(MpuAbort {
        key: key.to_owned(),
        upload,
    })
}

/// A record of shard 0 at `position`.
pub fn record(position: EpochSeq, body: RecordBody) -> LogRecord {
    LogRecord {
        shard: shard(0),
        position,
        body,
    }
}

/// A made-up location for the record at `position`.
pub fn location(position: EpochSeq) -> RecordLocation {
    RecordLocation {
        segment: SegmentId::new(position.epoch.get()),
        offset: position.seq.get() * 100,
        len: 100,
    }
}

/// A new index on its own simulated disk. The disk is returned so that
/// tests can crash it.
pub fn new_index() -> (SimDisk, Index) {
    let disk = SimDisk::new(1);
    let index = Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap();
    (disk, index)
}

/// A small page cache, so that redb writes pages out between checkpoints.
pub fn index_config() -> IndexConfig {
    IndexConfig {
        checkpoint_interval: Duration::from_secs(10),
        cache_bytes: 256 * 1024,
    }
}

/// Applies `record` with the state machine and returns its outcome, or
/// `None` if the index skipped it.
pub fn apply(index: &Index, record: &LogRecord) -> Option<Outcome> {
    let recorder = Recorder::default();
    index
        .apply(&recorder, &[(record.clone(), location(record.position))])
        .unwrap();
    recorder.take().pop().map(|(_, outcome)| outcome)
}

/// Returns the entry of `key` in shard 0.
pub fn entry(index: &Index, key: &str) -> Option<Entry> {
    index.read().unwrap().entry(&shard(0), key).unwrap()
}

/// Returns the index's contents.
pub fn dump(index: &Index) -> IndexDump {
    index.read().unwrap().dump().unwrap()
}

/// Sets the state of an entry directly, as the flusher's own transitions
/// (M1-16) will.
struct SetState(String, EntryState);

impl Applier for SetState {
    fn apply(
        &self,
        index: &mut IndexWriter<'_>,
        record: &LogRecord,
        _: RecordLocation,
    ) -> Result<(), IndexError> {
        let mut entry = index.entry(&record.shard, &self.0)?.unwrap();
        entry.state = self.1;
        index.put_entry(&record.shard, &self.0, &entry)
    }
}

/// Moves the entry of `key` to `state`, through a `TRUNCATE` at `position`
/// that carries the change.
pub fn set_state(index: &Index, position: EpochSeq, key: &str, state: EntryState) {
    let carrier = record(position, RecordBody::Truncate);
    index
        .apply(
            &SetState(key.to_owned(), state),
            &[(carrier, location(position))],
        )
        .unwrap();
}

/// A single-member configuration of `shard` in `epoch`.
pub fn config(shard: &ShardRef, epoch: u64) -> ShardConfig {
    let node: NodeId = "node-1".parse().unwrap();
    ShardConfig {
        bucket_id: shard.bucket.clone(),
        shard: shard.shard,
        epoch: Epoch::new(epoch),
        primary: node.clone(),
        members: vec![node],
        learners: Vec::new(),
        min_write_replicas: 1,
        replicas: 1,
        proposal_id: ProposalId::new("p-1").unwrap(),
    }
}

/// Small segments, and group commits that take what is queued without
/// waiting for more.
pub fn log_config() -> LogConfig {
    LogConfig {
        inline_max_bytes: 512,
        segment_bytes: 8192,
        group_commit_max_delay: Duration::ZERO,
        group_commit_max_bytes: 16 * 1024,
    }
}

/// A one-thread pool for index I/O.
pub fn pool() -> BlockingPool {
    BlockingPool::new("index", NonZeroUsize::MIN).unwrap()
}

/// A current-thread runtime with timers.
pub fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
}

/// Opens the log on `mount`.
pub async fn open_log(mount: SimMount) -> SegmentLog<SimMount> {
    let clock = Arc::new(MonotonicClock::new());
    SegmentLog::open(mount, log_config(), clock)
        .await
        .unwrap()
        .0
}
