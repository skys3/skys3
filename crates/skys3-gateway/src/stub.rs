//! Shards on a simulated disk, for tests.
//!
//! [`MemoryShards`] is [`LocalShards`] over a [`SimDisk`]: the real log,
//! index, and shard state machine, in memory. A test can crash the disk and
//! [`MemoryShards::open`] it again, which recovers the log and replays it as
//! a restarted node does. Helpers commit entries in a given state
//! ([`MemoryShards::put`]), flush a bucket ([`MemoryShards::flush`]), and
//! make every request fail ([`MemoryShards::set_unavailable`]).

use std::collections::BTreeMap;
use std::error::Error;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use skys3_index::{
    Checkpointer, Entry, EntryState as IndexState, Index, IndexConfig, Part, Upload,
};
use skys3_io::{BlockingPool, MonotonicClock, SimDisk, SimMount};
use skys3_log::record::{Delete, Extent, ExtentRef, Flushed, Put, PutData};
use skys3_log::{LogConfig, RecordBody, SegmentLog};
use skys3_shard::{ShardSet, StateMachine};
use skys3_types::{BucketDocument, BucketId, ETag, EpochSeq, Label, NodeId};

use crate::conditions::{ConditionFailed, Precondition};
use crate::local::LocalShards;
use crate::shard::{ShardError, ShardRef, ShardSummary, Shards};

/// The state [`MemoryShards::put`] leaves an entry in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryState {
    /// A committed PUT that has not reached the target.
    Dirty,
    /// A PUT that matches the target.
    Clean,
    /// A committed DELETE that has not reached the target.
    Tombstone,
}

/// The ETag of an empty body.
const EMPTY_ETAG: &str = "d41d8cd98f00b204e9800998ecf8427e";

/// Real shards on a simulated disk. Clones share the shards.
#[derive(Debug, Clone)]
pub struct MemoryShards {
    disk: SimDisk,
    local: LocalShards<SimMount>,
    /// When set, every request fails with [`ShardError::Unavailable`].
    unavailable: Arc<AtomicBool>,
}

impl MemoryShards {
    /// Shards on a fresh simulated disk.
    ///
    /// # Panics
    ///
    /// If the disk cannot be set up, which a fresh one always can.
    pub async fn new() -> Self {
        Self::open(SimDisk::new(1))
            .await
            .expect("a fresh simulated disk opens")
    }

    /// Shards on `disk`, as a node finds them after a restart: the log is
    /// recovered and replayed into the index, and no shard is open yet.
    ///
    /// # Errors
    ///
    /// If recovery, the index, or replay fails.
    pub async fn open(disk: SimDisk) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let mount = disk.mount();
        let clock = Arc::new(MonotonicClock::new());
        let (log, _) = SegmentLog::open(mount.clone(), Self::log_config(), clock).await?;
        let config = IndexConfig {
            checkpoint_interval: Duration::from_secs(60),
            cache_bytes: 1 << 20,
        };
        let index = Arc::new(Index::open_sim(&mount, "index.redb", &config)?);
        let pool = BlockingPool::new("shards", NonZeroUsize::MIN)?;
        let logs = BTreeMap::from([(Label::new("disk-0")?, log.clone())]);
        Checkpointer::new(Arc::clone(&index), logs, pool.clone())
            .replay(Arc::new(StateMachine))
            .await?;
        let node = NodeId::new("node-1")?;
        Ok(Self {
            disk,
            local: LocalShards::new(ShardSet::new(index, log, pool), node),
            unavailable: Arc::default(),
        })
    }

    /// The log's settings: the defaults, with group commits that take what
    /// is queued without waiting for more.
    #[must_use]
    pub fn log_config() -> LogConfig {
        LogConfig {
            group_commit_max_delay: Duration::ZERO,
            ..LogConfig::default()
        }
    }

    /// The simulated disk.
    #[must_use]
    pub fn disk(&self) -> &SimDisk {
        &self.disk
    }

    /// The shards themselves.
    #[must_use]
    pub fn local(&self) -> &LocalShards<SimMount> {
        &self.local
    }

    /// Commits a client write of `key` to its shard of `bucket`, leaving the
    /// entry in `state`.
    ///
    /// # Errors
    ///
    /// As [`Shards::write`].
    pub async fn put(
        &self,
        bucket: &BucketDocument,
        key: &str,
        state: EntryState,
    ) -> Result<ShardRef, ShardError> {
        let shard = ShardRef::for_key(bucket, key);
        let body = if state == EntryState::Tombstone {
            RecordBody::Delete(Delete {
                key: key.to_owned(),
            })
        } else {
            RecordBody::Put(Put {
                key: key.to_owned(),
                size: 0,
                last_modified_ms: 0,
                etag: ETag::new(EMPTY_ETAG).expect("the ETag is valid"),
                inherited_identity: None,
                metadata: BTreeMap::new(),
                tags: BTreeMap::new(),
                checksums: BTreeMap::new(),
                copy_source: None,
                data: PutData::Inline(Bytes::new()),
            })
        };
        let position = self.commit(&shard, body).await?;
        if state == EntryState::Clean {
            self.commit(&shard, flushed(key, position, true)).await?;
        }
        Ok(shard)
    }

    /// Flushes every entry of `bucket`: objects become clean and
    /// tombstones go away. Flushing ignores seals.
    ///
    /// # Panics
    ///
    /// If the index or a shard fails.
    pub async fn flush(&self, bucket: &BucketId) {
        for shard in self.open_shards(bucket).await {
            let local = self.local.set().get(&(&shard).into()).await;
            let local = local.expect("the shard is open");
            let entries = local
                .index()
                .read()
                .and_then(|reader| reader.entries(&(&shard).into(), None, usize::MAX))
                .expect("the index reads");
            for (key, entry) in entries {
                if !matches!(entry.state, IndexState::Clean | IndexState::Evicted) {
                    let body = flushed(&key, entry.version, entry.object.is_some());
                    local.commit(body).await.expect("the flush commits");
                }
            }
        }
    }

    /// The open shards of `bucket`.
    pub async fn open_shards(&self, bucket: &BucketId) -> Vec<ShardRef> {
        self.local
            .set()
            .shards()
            .await
            .into_iter()
            .filter(|shard| shard.bucket == *bucket)
            .map(|shard| ShardRef {
                bucket: shard.bucket,
                shard: shard.shard,
            })
            .collect()
    }

    /// The number of open shards, of every bucket.
    pub async fn len(&self) -> usize {
        self.local.set().shards().await.len()
    }

    /// Whether no shard is open.
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }

    /// Whether `shard` is sealed, or `None` if it is not open.
    pub async fn is_sealed(&self, shard: &ShardRef) -> Option<bool> {
        let local = self.local.set().get(&shard.into()).await?;
        Some(local.is_sealed())
    }

    /// Makes every later request fail with [`ShardError::Unavailable`], or
    /// succeed again.
    pub fn set_unavailable(&self, unavailable: bool) {
        self.unavailable.store(unavailable, Ordering::SeqCst);
    }

    fn check(&self, shard: &ShardRef) -> Result<(), ShardError> {
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(ShardError::Unavailable {
                shard: shard.clone(),
                reason: "the stub is unavailable".to_owned(),
            });
        }
        Ok(())
    }

    async fn commit(&self, shard: &ShardRef, body: RecordBody) -> Result<EpochSeq, ShardError> {
        match self.write(shard, body, Precondition::None).await? {
            Ok(position) => Ok(position),
            Err(failed) => unreachable!("an unconditional write failed its condition: {failed}"),
        }
    }
}

/// A `FLUSHED` of `key` at `position`, of an object or of a tombstone.
fn flushed(key: &str, position: EpochSeq, object: bool) -> RecordBody {
    RecordBody::Flushed(Flushed {
        key: key.to_owned(),
        seq: position.seq,
        remote_etag: object.then(|| ETag::new(EMPTY_ETAG).expect("the ETag is valid")),
        remote_version_id: None,
    })
}

impl Shards for MemoryShards {
    async fn open(&self, shard: &ShardRef, bucket: &BucketDocument) -> Result<(), ShardError> {
        self.check(shard)?;
        self.local.open(shard, bucket).await
    }

    async fn seal(&self, shard: &ShardRef) -> Result<ShardSummary, ShardError> {
        self.check(shard)?;
        self.local.seal(shard).await
    }

    async fn unseal(&self, shard: &ShardRef) -> Result<(), ShardError> {
        self.check(shard)?;
        self.local.unseal(shard).await
    }

    async fn remove(&self, shard: &ShardRef) -> Result<(), ShardError> {
        self.check(shard)?;
        self.local.remove(shard).await
    }

    async fn entry(&self, shard: &ShardRef, key: &str) -> Result<Option<Entry>, ShardError> {
        self.check(shard)?;
        self.local.entry(shard, key).await
    }

    async fn upload(
        &self,
        shard: &ShardRef,
        key: &str,
        upload: EpochSeq,
    ) -> Result<Option<Upload>, ShardError> {
        self.check(shard)?;
        self.local.upload(shard, key, upload).await
    }

    async fn uploads(
        &self,
        shard: &ShardRef,
        prefix: &str,
        after: Option<(String, Option<EpochSeq>)>,
        limit: usize,
    ) -> Result<Vec<(String, EpochSeq, Upload)>, ShardError> {
        self.check(shard)?;
        self.local.uploads(shard, prefix, after, limit).await
    }

    async fn parts(
        &self,
        shard: &ShardRef,
        upload: EpochSeq,
        after: u16,
        limit: usize,
    ) -> Result<Vec<(u16, Part)>, ShardError> {
        self.check(shard)?;
        self.local.parts(shard, upload, after, limit).await
    }

    async fn payload(&self, shard: &ShardRef, position: EpochSeq) -> Result<Bytes, ShardError> {
        self.check(shard)?;
        self.local.payload(shard, position).await
    }

    async fn append_extent(
        &self,
        shard: &ShardRef,
        extent: Extent,
    ) -> Result<ExtentRef, ShardError> {
        self.check(shard)?;
        self.local.append_extent(shard, extent).await
    }

    async fn write(
        &self,
        shard: &ShardRef,
        body: RecordBody,
        condition: Precondition,
    ) -> Result<Result<EpochSeq, ConditionFailed>, ShardError> {
        self.check(shard)?;
        self.local.write(shard, body, condition).await
    }
}
