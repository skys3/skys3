//! Shared test support: a minimal deterministic applier, and a workload
//! that appends records to a log and applies them to an index.
//!
//! The applier stands in for the shard state machine (M1-04). It gives each
//! record kind a simple, deterministic effect so that the tests can check
//! that replay reproduces the index exactly; it does not implement the
//! object states of §4.2.

#![allow(dead_code, reason = "each test target uses part of the support")]

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use skys3_index::{
    Applier, Checkpointer, ControlEntry, Entry, EntryState, Index, IndexConfig, IndexError,
    IndexWriter, ObjectVersion, Payload,
};
use skys3_io::{BlockingPool, Disk, MonotonicClock, SimMount};
use skys3_log::record::{
    Delete, Extent, ExtentRef, Flushed, Put, PutData, RecordBody, ShardRef, Tags,
};
use skys3_log::{LogConfig, LogRecord, RecordLocation, SegmentLog};
use skys3_types::{BucketId, ETag, Epoch, EpochSeq, Generation, Label, Seq, ShardId};

/// The applier the tests use in place of the shard state machine.
#[derive(Debug, Default)]
pub struct TestApplier;

impl Applier for TestApplier {
    fn apply(
        &self,
        index: &mut IndexWriter<'_>,
        record: &LogRecord,
        location: RecordLocation,
    ) -> Result<(), IndexError> {
        let shard = &record.shard;
        let position = record.position;
        match &record.body {
            RecordBody::Put(put) => {
                let payload = match &put.data {
                    PutData::Inline(_) => {
                        index.put_location(shard, position, &location)?;
                        Payload::Inline(position)
                    }
                    PutData::Extents(extents) => Payload::Extents(extents.clone()),
                };
                let prior = index.entry(shard, &put.key)?;
                let entry = Entry {
                    version: position,
                    state: EntryState::Dirty,
                    object: Some(ObjectVersion {
                        size: put.size,
                        last_modified_ms: put.last_modified_ms,
                        local_etag: put.etag.clone(),
                        write_identity: put.inherited_identity,
                        metadata: put.metadata.clone(),
                        tags: put.tags.clone(),
                        checksums: put.checksums.clone(),
                        storage_class: None,
                        copy_source: put.copy_source.clone(),
                        payload,
                    }),
                    remote_etag: prior.as_ref().and_then(|p| p.remote_etag.clone()),
                    remote_version_id: prior.and_then(|p| p.remote_version_id),
                };
                index.put_entry(shard, &put.key, &entry)
            }
            RecordBody::Extent(_) => index.put_location(shard, position, &location),
            RecordBody::Delete(Delete { key }) => {
                let prior = index.entry(shard, key)?;
                let entry = Entry {
                    version: position,
                    state: EntryState::Dirty,
                    object: None,
                    remote_etag: prior.as_ref().and_then(|p| p.remote_etag.clone()),
                    remote_version_id: prior.and_then(|p| p.remote_version_id),
                };
                index.put_entry(shard, key, &entry)
            }
            RecordBody::Tags(Tags { key, tags }) => {
                let Some(mut entry) = index.entry(shard, key)? else {
                    return Ok(());
                };
                if let Some(object) = &mut entry.object {
                    object.tags.clone_from(tags);
                    index.put_entry(shard, key, &entry)?;
                }
                Ok(())
            }
            RecordBody::Flushed(flushed) => {
                let Some(mut entry) = index.entry(shard, &flushed.key)? else {
                    return Ok(());
                };
                if entry.version.seq != flushed.seq {
                    return Ok(());
                }
                if entry.object.is_none() {
                    index.remove_entry(shard, &flushed.key)?;
                    return Ok(());
                }
                entry.state = EntryState::Clean;
                entry.remote_etag.clone_from(&flushed.remote_etag);
                entry
                    .remote_version_id
                    .clone_from(&flushed.remote_version_id);
                index.put_entry(shard, &flushed.key, &entry)
            }
            _ => Ok(()),
        }
    }
}

/// The disk label the tests use.
pub fn disk_label() -> Label {
    disk_label_of(0)
}

/// The label of the node's disk number `n`.
pub fn disk_label_of(n: usize) -> Label {
    Label::new(format!("disk-{n}")).unwrap()
}

/// Shard `n` of one of two buckets.
pub fn shard(n: u8) -> ShardRef {
    let bucket = if n.is_multiple_of(2) {
        "b-even"
    } else {
        "b-odd"
    };
    ShardRef::new(BucketId::new(bucket).unwrap(), ShardId::new(n))
}

/// Small segments, and group commits that do not wait.
pub fn log_config() -> LogConfig {
    LogConfig {
        inline_max_bytes: 512,
        segment_bytes: 2048,
        group_commit_max_delay: Duration::ZERO,
        group_commit_max_bytes: 4096,
    }
}

/// A small page cache, so that redb writes pages out between checkpoints.
pub fn index_config() -> IndexConfig {
    IndexConfig {
        checkpoint_interval: Duration::from_secs(10),
        cache_bytes: 256 * 1024,
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

/// Opens the index and the log on `mount`, as a node does after a restart.
pub async fn open_node(mount: &SimMount, pool: &BlockingPool) -> Checkpointer<SimMount> {
    open_node_on(std::slice::from_ref(mount), pool).await
}

/// Opens a node with a log on each of `mounts`, labelled `disk-0` on, and
/// the index on the first.
pub async fn open_node_on(mounts: &[SimMount], pool: &BlockingPool) -> Checkpointer<SimMount> {
    let mut logs = BTreeMap::new();
    for (n, mount) in mounts.iter().enumerate() {
        logs.insert(disk_label_of(n), open_log(mount.clone()).await);
    }
    let index = Arc::new(Index::open_sim(&mounts[0], "index.redb", &index_config()).unwrap());
    Checkpointer::new(index, logs, pool.clone())
}

/// Returns the log of `node`'s first disk.
pub fn log_of<D: Disk>(node: &Checkpointer<D>) -> &SegmentLog<D> {
    node.logs().get(&disk_label()).unwrap()
}

fn etag(n: u64) -> ETag {
    ETag::new(format!("{n:032x}")).unwrap()
}

/// Generates records for a few shards and appends and applies them.
///
/// It tracks each shard's next position, so a workload can continue across
/// crashes: every record it appended was acknowledged and applied, so the
/// log and the index agree on where each shard is.
#[derive(Debug)]
pub struct Workload {
    rng: SmallRng,
    shards: u8,
    next: BTreeMap<u8, EpochSeq>,
    generation: u64,
}

impl Workload {
    /// A workload over `shards` shards, seeded by `seed`.
    pub fn new(seed: u64, shards: u8) -> Self {
        Self {
            rng: SmallRng::seed_from_u64(seed),
            shards,
            next: BTreeMap::new(),
            generation: 0,
        }
    }

    /// Returns the random generator.
    pub fn rng(&mut self) -> &mut SmallRng {
        &mut self.rng
    }

    fn take_position(&mut self, n: u8) -> EpochSeq {
        let next = self
            .next
            .entry(n)
            .or_insert(EpochSeq::new(Epoch::new(1), Seq::new(1)));
        let position = *next;
        *next = EpochSeq::new(position.epoch, Seq::new(position.seq.get() + 1));
        position
    }

    fn key(&mut self) -> String {
        format!("key-{}", self.rng.random_range(0..6))
    }

    /// Appends one operation's records to `node`'s log and applies each
    /// once it is acknowledged, as the write path does.
    pub async fn step(&mut self, node: &Checkpointer<SimMount>) {
        let records = self.append(node).await;
        apply(node, &records);
    }

    /// Appends one operation's records to the logs of `node`, each to a
    /// disk picked at random, and returns them with their locations once
    /// the logs have acknowledged them. A node with several disks thus has
    /// shards whose records interleave across disks.
    pub async fn append(
        &mut self,
        node: &Checkpointer<SimMount>,
    ) -> Vec<(LogRecord, RecordLocation)> {
        let n = self.rng.random_range(0..self.shards);
        let logs: Vec<_> = node.logs().values().collect();
        let mut appended = Vec::new();
        for record in self.records(node, n) {
            let log = logs[self.rng.random_range(0..logs.len())];
            let location = log.append(&record).await.unwrap();
            appended.push((record, location));
        }
        appended
    }

    fn records(&mut self, node: &Checkpointer<SimMount>, n: u8) -> Vec<LogRecord> {
        let shard = shard(n);
        let record = |position, body| LogRecord {
            shard: shard.clone(),
            position,
            body,
        };
        match self.rng.random_range(0..10) {
            0..=2 => {
                let key = self.key();
                let len = self.rng.random_range(0..300);
                let position = self.take_position(n);
                vec![record(
                    position,
                    RecordBody::Put(put(key, position, PutData::Inline(fill(len)))),
                )]
            }
            3 | 4 => {
                let key = self.key();
                let mut records = Vec::new();
                let mut extents = Vec::new();
                for i in 0..self.rng.random_range(1..=3u64) {
                    let len = self.rng.random_range(1..900);
                    let position = self.take_position(n);
                    extents.push(ExtentRef {
                        position,
                        len: u32::try_from(len).unwrap(),
                    });
                    let body = RecordBody::Extent(Extent {
                        key: key.clone(),
                        offset: i * 1000,
                        data: fill(len),
                    });
                    records.push(record(position, body));
                }
                let position = self.take_position(n);
                let body = RecordBody::Put(put(key, position, PutData::Extents(extents)));
                records.push(record(position, body));
                records
            }
            5 => {
                let key = self.key();
                vec![record(
                    self.take_position(n),
                    RecordBody::Delete(Delete { key }),
                )]
            }
            6 => {
                let key = self.key();
                let tags = BTreeMap::from([("t".to_owned(), self.rng.random::<u8>().to_string())]);
                vec![record(
                    self.take_position(n),
                    RecordBody::Tags(Tags { key, tags }),
                )]
            }
            7 | 8 => {
                let key = self.key();
                let current = node.index().read().unwrap().entry(&shard, &key).unwrap();
                // Sometimes a stale FLUSHED, which the applier drops.
                let seq = current.map_or(Seq::new(1), |entry| entry.version.seq);
                let body = RecordBody::Flushed(Flushed {
                    key,
                    seq,
                    remote_etag: Some(etag(seq.get())),
                    remote_version_id: None,
                });
                vec![record(self.take_position(n), body)]
            }
            _ => {
                // A new epoch, as after a configuration change: a TRUNCATE
                // at the last sequence number of the old one.
                let last = self.take_position(n);
                let epoch = Epoch::new(last.epoch.get() + 1);
                let position = EpochSeq::new(epoch, last.seq);
                self.next
                    .insert(n, EpochSeq::new(epoch, Seq::new(last.seq.get() + 1)));
                vec![record(position, RecordBody::Truncate)]
            }
        }
    }

    /// Changes the node's control-state copy, in one durable commit.
    pub fn control_update(&mut self, index: &Index) {
        self.generation += 1;
        let generation = Generation::new(self.generation);
        let key = format!("buckets/bucket-{}.json", self.rng.random_range(0..3));
        let remove = self.rng.random_bool(0.2);
        index
            .update_control(|control| {
                if remove {
                    control.remove(&key)?;
                } else {
                    let entry = ControlEntry {
                        generation,
                        version: format!("v{}", self.generation),
                        value: format!("{{\"g\":{}}}", self.generation).into_bytes(),
                    };
                    control.put(&key, &entry)?;
                }
                control.set_generation(generation)
            })
            .unwrap();
    }
}

/// Applies acknowledged records to `node`'s index, as the write path does.
pub fn apply(node: &Checkpointer<SimMount>, records: &[(LogRecord, RecordLocation)]) {
    let applied = node.index().apply(&TestApplier, records).unwrap();
    assert_eq!(applied, records.len());
}

fn put(key: String, position: EpochSeq, data: PutData) -> Put {
    let size = match &data {
        PutData::Inline(bytes) => bytes.len() as u64,
        PutData::Extents(extents) => extents.iter().map(|e| u64::from(e.len)).sum(),
    };
    Put {
        key,
        size,
        last_modified_ms: 1_700_000_000_000 + position.seq.get(),
        etag: etag(position.seq.get()),
        inherited_identity: None,
        metadata: BTreeMap::from([("content-type".to_owned(), "text/plain".to_owned())]),
        tags: BTreeMap::new(),
        checksums: BTreeMap::new(),
        copy_source: None,
        data,
    }
}

fn fill(len: usize) -> Bytes {
    (0..len).map(|i| (i % 251) as u8).collect::<Vec<_>>().into()
}
