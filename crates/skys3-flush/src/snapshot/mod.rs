//! Index snapshots and the lost-key report (§6.9, §8.9).
//!
//! Losing every member of a shard loses its index too, so the cluster
//! alone cannot list what was lost. Each shard's primary therefore writes
//! its index, every `index_snapshot_interval_seconds`, to the bucket's
//! `snapshot_target` (by default its backup target), and an operator who
//! lost a shard builds a report from the latest snapshot:
//!
//! - **What a snapshot holds** ([`Contents`]). A `write_back` bucket's
//!   holds only what is not at the remote: entries that are not clean and
//!   the multipart uploads in progress. A `local` bucket's holds the whole
//!   index, incrementally: a base, then deltas of the rows changed or
//!   removed since the previous snapshot. A new base starts once the deltas
//!   wrote as many rows as the base, after 64 deltas, and whenever the
//!   primary changes.
//! - **When.** A pass reads the time, then the shard's rows in one read
//!   transaction, and writes the snapshot only if the replica served reads
//!   both before and after: as the primary holding a lease from every
//!   member (§5.4), so no other primary acknowledged a write meanwhile,
//!   and every write acknowledged before that time is in the rows.
//! - **Across primary changes.** Snapshots are primary-scoped work: a
//!   [`SnapshotService`] runs a writer for each shard its node leads, the
//!   first pass at once. A writer keeps its chain only in memory and
//!   extends it only in the epoch it started it in, so a new primary, a
//!   restarted one, and one that leads again in a later epoch each start a
//!   chain of their own. Chains order by that epoch ([`ChainId`]), so the
//!   latest chain is the current primary's, and a deposed primary's late
//!   writes only extend a chain no reader picks. A new base deletes every
//!   older chain once it is written.
//! - **Objects** ([`Snapshot`]) live under the target's prefix in
//!   [`SNAPSHOT_DIR`], with the rows as the index stores them and an MD5
//!   trailer; [`Snapshot`] gives their layout and their keys.
//! - **Restoring** ([`latest`]) reads the greatest chain whose base
//!   decodes, and applies its deltas in order up to the first missing or
//!   damaged one.
//! - **The report** ([`lost_keys`]): the keys whose latest version, as of
//!   the snapshot, existed only on the lost members, checked against the
//!   bucket's durable home when it has one, the uploads in progress, and
//!   the window from the snapshot to the loss in which any key written may
//!   also be lost. Coded objects are listed apart: re-indexing from their
//!   fragment headers recovers them.
//! - **The restore drill** ([`drill()`]): the operator-run recovery of a
//!   shard whose members are all lost. It combines the latest snapshot
//!   with the fragment headers of every surviving node to re-index the
//!   shard's coded objects (`skys3_ec::reindex`), including those written
//!   after the snapshot, into a [`RestoredIndex`] a new member can install,
//!   and reports what nothing restores.

mod drill;
mod format;
pub(crate) mod hooks;
mod report;
mod restore;
mod writer;

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use skys3_config::BucketsConfig;
use skys3_io::{Disk, SystemWallClock, WallClock};
use skys3_log::ShardRef;
use skys3_remote::{ObjectStore, S3Error};
use skys3_shard::{ShardError, ShardSet};
use skys3_types::{BucketDocument, BucketId, BucketMode, EpochSeq, RemoteTarget, ShardId};
use tokio::task::JoinHandle;

use crate::service::Connect;

pub use drill::{Drill, DrillRequest, RestoredIndex, RestoredKey, drill};
pub use format::{
    ChainId, Contents, FORMAT, FormatError, RowDigest, SNAPSHOT_DIR, Snapshot, object_key,
    parse_object_key, row_digest, shard_dir,
};
pub use report::{
    DurableHome, LossWindow, LostKey, LostKeyReport, LostObject, LostUpload, SnapshotInfo,
    lost_keys,
};
pub use restore::{Restored, latest};

use writer::ShardWriter;

/// Why a snapshot could not be taken, written, or read back.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SnapshotError {
    /// The shard failed to read its index.
    #[error(transparent)]
    Shard(#[from] ShardError),
    /// A request to the snapshot target or the durable home failed.
    #[error("a request to the target failed: {0}")]
    Remote(#[from] S3Error),
    /// A row of the index or of a snapshot does not decode.
    #[error("a row does not decode: {0}")]
    Row(String),
}

/// A snapshot written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Taken {
    /// Its chain.
    pub chain: ChainId,
    /// Its number in the chain: 0 for a base.
    pub number: u32,
    /// The applied position it was taken at.
    pub position: EpochSeq,
    /// When it was taken, in milliseconds since the Unix epoch.
    pub taken_ms: u64,
    /// The rows it wrote, and for a delta the rows it removed.
    pub rows: u64,
}

/// A shard's snapshots on this node.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SnapshotStatus {
    /// The latest snapshot this node's writer wrote.
    pub last: Option<Taken>,
    /// How many it wrote.
    pub written: u64,
    /// Why the latest pass failed, until one succeeds.
    pub error: Option<String>,
}

/// A shard's writer, running.
struct Running {
    task: JoinHandle<()>,
    status: Arc<Mutex<SnapshotStatus>>,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// The index snapshots of the shards a node leads (§8.9).
///
/// [`SnapshotService::reconcile`] keeps a writer on every shard, open on
/// the node as its primary, of each `write_back` or `local` bucket whose
/// settings name a `snapshot_target` (by default its `backup_target`), and
/// stops the others.
pub struct SnapshotService<S, D> {
    connect: Connect<S>,
    buckets: BucketsConfig,
    wall: Arc<dyn WallClock>,
    /// Each target's store, by bucket.
    stores: Mutex<BTreeMap<BucketId, (RemoteTarget, Arc<S>)>>,
    writers: Mutex<BTreeMap<ShardRef, Running>>,
    _disk: std::marker::PhantomData<fn() -> D>,
}

impl<S, D> fmt::Debug for SnapshotService<S, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnapshotService").finish_non_exhaustive()
    }
}

impl<S: ObjectStore, D: Disk> SnapshotService<S, D> {
    /// A service that reads each bucket's snapshot target and interval
    /// from `buckets`, and connects to targets with `connect`.
    pub fn new(connect: Connect<S>, buckets: BucketsConfig) -> Self {
        Self {
            connect,
            buckets,
            wall: Arc::new(SystemWallClock),
            stores: Mutex::default(),
            writers: Mutex::default(),
            _disk: std::marker::PhantomData,
        }
    }

    /// Sets the clock snapshots are timed on.
    #[must_use]
    pub fn with_wall_clock(mut self, wall: Arc<dyn WallClock>) -> Self {
        self.wall = wall;
        self
    }

    /// Where `bucket`'s snapshots go, and what they hold, if it has a
    /// snapshot target: a `read_only` bucket has nothing to snapshot.
    #[must_use]
    pub fn target_of(&self, bucket: &BucketDocument) -> Option<(RemoteTarget, Contents)> {
        let contents = match bucket.mode {
            BucketMode::WriteBack => Contents::Unflushed,
            BucketMode::Local => Contents::Full,
            _ => return None,
        };
        let target = self.buckets.get(&bucket.name).snapshot_target.clone()?;
        Some((target, contents))
    }

    /// Starts and stops writers so that every shard of `buckets` with a
    /// snapshot target that is open in `set` as its primary has one.
    pub async fn reconcile(&self, buckets: &[BucketDocument], set: &ShardSet<D>) {
        let mut wanted = Vec::new();
        for bucket in buckets {
            let Some((target, contents)) = self.target_of(bucket) else {
                continue;
            };
            let interval = self.buckets.get(&bucket.name).index_snapshot_interval();
            for shard in bucket.shards.shards() {
                let shard_ref = ShardRef::new(bucket.bucket_id.clone(), shard);
                if let Some(shard) = set.get(&shard_ref).await
                    && !shard.is_stopped()
                    && !shard.role().follows()
                {
                    wanted.push((shard, target.clone(), contents, interval));
                }
            }
        }
        let mut writers = lock(&self.writers);
        writers.retain(|shard, running| {
            !running.task.is_finished() && wanted.iter().any(|(s, ..)| s.shard() == shard)
        });
        for (shard, target, contents, interval) in wanted {
            let shard_ref = shard.shard().clone();
            if writers.contains_key(&shard_ref) {
                continue;
            }
            let store = self.store(&shard_ref.bucket, &target);
            let dir = shard_dir(target.prefix.as_deref().unwrap_or_default(), &shard_ref);
            let writer = ShardWriter::new(shard, store, dir, contents, Arc::clone(&self.wall));
            let status = Arc::clone(&writer.status);
            let task = tokio::spawn(writer.run(interval));
            writers.insert(shard_ref, Running { task, status });
        }
    }

    /// The store of `bucket`'s snapshot target `target`.
    fn store(&self, bucket: &BucketId, target: &RemoteTarget) -> Arc<S> {
        let mut stores = lock(&self.stores);
        match stores.get(bucket) {
            Some((known, store)) if known == target => Arc::clone(store),
            _ => {
                let store = Arc::new((self.connect)(target));
                stores.insert(bucket.clone(), (target.clone(), Arc::clone(&store)));
                store
            }
        }
    }

    /// The snapshots of `bucket`'s shards that this node writes.
    #[must_use]
    pub fn status(&self, bucket: &BucketId) -> Vec<(ShardId, SnapshotStatus)> {
        lock(&self.writers)
            .iter()
            .filter(|(shard, _)| shard.bucket == *bucket)
            .map(|(shard, running)| (shard.shard, lock(&running.status).clone()))
            .collect()
    }

    /// Stops every writer. A snapshot being written may or may not land.
    pub async fn shutdown(&self) {
        let writers = std::mem::take(&mut *lock(&self.writers));
        for (_, mut running) in writers {
            running.task.abort();
            let _ = (&mut running.task).await;
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
