//! Helpers shared by the fragment store's tests and simulations.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use skys3_ec::fragment::{FragmentHeader, ObjectMeta, PartSize, StripeInfo};
use skys3_ec::{AttemptId, CodecId, FragmentStoreConfig, Geometry};
use skys3_io::disk::SimFile;
use skys3_io::{Disk, SegmentFile, SimDisk, SimMount};
use skys3_log::record::ShardRef;
use skys3_types::checksum::{Checksum, ChecksumAlgorithm};
use skys3_types::{BucketId, ETag, Epoch, EpochSeq, Seq, ShardId};

/// A single-threaded runtime: with the simulated disk, a run replays
/// exactly.
pub fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
}

/// Small segments and groups, so a few fragments span several of each.
pub fn small_config() -> FragmentStoreConfig {
    FragmentStoreConfig {
        disk: 0,
        segment_bytes: 256 * 1024,
        group_commit_max_bytes: 128 * 1024,
        max_fragment_bytes: 1 << 20,
    }
}

/// Deterministic, incompressible bytes.
pub fn sample(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

pub fn position(epoch: u64, seq: u64) -> EpochSeq {
    EpochSeq::new(Epoch::new(epoch), Seq::new(seq))
}

pub fn shard() -> ShardRef {
    ShardRef::new(BucketId::new("b-7f3a").unwrap(), ShardId::new(3))
}

/// Metadata of an object of `size` bytes, with a little of everything.
pub fn object(size: u64) -> ObjectMeta {
    ObjectMeta {
        size,
        last_modified_ms: 1_767_225_600_000,
        etag: ETag::new("9b2cf535f27731c974343645a3985328").unwrap(),
        identity: position(4, 17),
        metadata: BTreeMap::from([
            ("content-type".to_owned(), "image/jpeg".to_owned()),
            ("x-amz-meta-camera".to_owned(), "x100".to_owned()),
        ]),
        tags: BTreeMap::from([("team".to_owned(), "photos".to_owned())]),
        checksums: BTreeMap::from([(
            ChecksumAlgorithm::Crc32c,
            Checksum::full_object(ChecksumAlgorithm::Crc32c, &[1, 2, 3, 4]).unwrap(),
        )]),
        parts: Vec::new(),
    }
}

/// A multipart object of `parts` equal parts of `part` bytes.
pub fn multipart(part: u64, parts: u16) -> ObjectMeta {
    ObjectMeta {
        parts: (1..=parts)
            .map(|number| PartSize { number, size: part })
            .collect(),
        ..object(part * u64::from(parts))
    }
}

/// The header of fragment `index` of a one-stripe object whose fragments
/// are `len` bytes (a multiple of 64) in a 4+2 geometry with codec 1.
pub fn header(key: &str, len: u64, index: u8) -> FragmentHeader {
    let data_len = 4 * len;
    FragmentHeader {
        shard: shard(),
        key: key.to_owned(),
        version: position(4, 18),
        attempt: AttemptId::new(Epoch::new(4), 1),
        stripe: StripeInfo {
            number: 0,
            count: 1,
            offset: 0,
            data_len,
            geometry: Geometry::RS_4_2,
            codec: CodecId::REED_SOLOMON_V1,
        },
        index,
        object: object(data_len),
    }
}

/// A fragment for a store: a header and a payload that fits it.
pub fn fragment(key: &str, len: u64, seed: u64) -> (FragmentHeader, Bytes) {
    let payload = sample(len as usize, seed);
    (header(key, len, (seed % 6) as u8), payload.into())
}

/// A mount that kills the process (keeping the page cache) at a planned
/// sync, numbered over data and directory syncs from 0. The killed sync
/// fails and does not happen.
#[derive(Clone, Debug)]
pub struct KillMount {
    mount: SimMount,
    plan: Arc<KillPlan>,
}

#[derive(Debug)]
struct KillPlan {
    syncs: AtomicU64,
    kill_at: Option<u64>,
}

impl KillPlan {
    fn before_sync(&self, disk: &SimDisk) -> io::Result<()> {
        let number = self.syncs.fetch_add(1, Ordering::Relaxed);
        if Some(number) == self.kill_at {
            disk.kill();
            return Err(io::Error::other("simulated process crash during a sync"));
        }
        Ok(())
    }
}

impl KillMount {
    /// Mounts `disk`, killing the process at sync `kill_at`, if any.
    pub fn new(disk: &SimDisk, kill_at: Option<u64>) -> Self {
        Self {
            mount: disk.mount(),
            plan: Arc::new(KillPlan {
                syncs: AtomicU64::new(0),
                kill_at,
            }),
        }
    }

    /// The syncs so far.
    pub fn syncs(&self) -> u64 {
        self.plan.syncs.load(Ordering::Relaxed)
    }

    fn wrap(&self, file: SimFile) -> KillFile {
        KillFile {
            file,
            disk: self.mount.disk().clone(),
            plan: Arc::clone(&self.plan),
        }
    }
}

impl Disk for KillMount {
    type File = KillFile;

    async fn create(&self, name: &str) -> io::Result<KillFile> {
        self.mount.create(name).await.map(|f| self.wrap(f))
    }

    async fn open(&self, name: &str) -> io::Result<KillFile> {
        self.mount.open(name).await.map(|f| self.wrap(f))
    }

    async fn remove(&self, name: &str) -> io::Result<()> {
        self.mount.remove(name).await
    }

    async fn list(&self) -> io::Result<Vec<String>> {
        self.mount.list().await
    }

    async fn sync_dir(&self) -> io::Result<()> {
        self.plan.before_sync(self.mount.disk())?;
        self.mount.sync_dir().await
    }
}

/// A file on a [`KillMount`].
#[derive(Debug)]
pub struct KillFile {
    file: SimFile,
    disk: SimDisk,
    plan: Arc<KillPlan>,
}

impl SegmentFile for KillFile {
    fn len(&self) -> u64 {
        self.file.len()
    }

    async fn append(&self, data: Bytes) -> io::Result<u64> {
        self.file.append(data).await
    }

    async fn sync_data(&self) -> io::Result<()> {
        self.plan.before_sync(&self.disk)?;
        self.file.sync_data().await
    }

    async fn read_at(&self, offset: u64, len: usize) -> io::Result<Bytes> {
        self.file.read_at(offset, len).await
    }

    async fn truncate(&self, len: u64) -> io::Result<()> {
        self.file.truncate(len).await
    }
}
