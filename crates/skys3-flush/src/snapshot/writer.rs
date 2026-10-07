//! The snapshots of one shard on its primary: a pass every interval that
//! reads the shard's rows and writes a base or a delta (§8.9).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use base64::Engine as _;
use md5::{Digest, Md5};
use skys3_index::{EntryState, IndexReader, Payload, ShardRow, ShardTable, codec};
use skys3_io::{Disk, WallClock};
use skys3_remote::{DeleteObject, ListObjectsV2, ObjectStore, PutObject};
use skys3_shard::Shard;
use skys3_types::{Epoch, EpochSeq};

use super::format::{
    ChainId, Contents, RowDigest, Snapshot, object_key, parse_object_key, row_digest, value_digest,
};
use super::hooks::{self, SnapshotBug};
use super::{SnapshotError, SnapshotStatus, Taken};

/// The most deltas a chain gets before the next snapshot is a new base.
pub(crate) const MAX_DELTAS: u32 = 64;

/// How long a writer waits before it looks again at a primary that does
/// not serve, at most.
const NOT_SERVING_RETRY: Duration = Duration::from_secs(1);

/// The shortest and longest wait after a failed pass.
const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// The rows of a pass, each with its table.
type Rows = Vec<(ShardTable, ShardRow)>;

/// What the writer knows of the latest snapshot of its chain: enough to
/// write the next delta.
#[derive(Debug)]
struct Chain {
    id: ChainId,
    /// The number of the next snapshot.
    next: u32,
    base_rows: u64,
    /// Rows the deltas so far wrote or removed.
    delta_rows: u64,
    /// Every row the latest snapshot holds, with the digest of its value.
    rows: BTreeMap<RowDigest, u64>,
}

impl Chain {
    /// Whether the next snapshot should be a base: a chain extends only
    /// in the epoch it started in, and its deltas stay cheaper to read
    /// than a new base.
    fn ends(&self, epoch: Epoch) -> bool {
        self.id.epoch != epoch || self.next > MAX_DELTAS || self.delta_rows >= self.base_rows.max(1)
    }
}

/// The digests of `rows`.
fn digests(rows: &Rows) -> BTreeMap<RowDigest, u64> {
    rows.iter()
        .map(|(table, (key, value))| (row_digest(*table, key), value_digest(value)))
        .collect()
}

/// One shard's snapshots, written by its primary.
pub(crate) struct ShardWriter<S, D: Disk> {
    pub(crate) shard: Shard<D>,
    pub(crate) store: Arc<S>,
    /// The shard's directory at the target ([`super::format::shard_dir`]).
    pub(crate) dir: String,
    pub(crate) contents: Contents,
    pub(crate) wall: Arc<dyn WallClock>,
    pub(crate) status: Arc<Mutex<SnapshotStatus>>,
    chain: Option<Chain>,
    /// A base whose older chains are still to be deleted.
    prune: Option<ChainId>,
}

impl<S: ObjectStore, D: Disk> ShardWriter<S, D> {
    pub(crate) fn new(
        shard: Shard<D>,
        store: Arc<S>,
        dir: String,
        contents: Contents,
        wall: Arc<dyn WallClock>,
    ) -> Self {
        Self {
            shard,
            store,
            dir,
            contents,
            wall,
            status: Arc::default(),
            chain: None,
            prune: None,
        }
    }

    /// Writes a snapshot every `interval` until the shard stops: at once,
    /// then after each one written, sooner after a failure or while the
    /// replica does not serve.
    pub(crate) async fn run(mut self, interval: Duration) {
        let mut failures = 0;
        while !self.shard.is_stopped() {
            let wait = match self.pass().await {
                Ok(Some(taken)) => {
                    failures = 0;
                    let mut status = self.lock();
                    status.last = Some(taken);
                    status.written += 1;
                    status.error = None;
                    interval
                }
                Ok(None) => NOT_SERVING_RETRY,
                Err(error) => {
                    failures += 1;
                    tracing::warn!(shard = %self.shard.shard(), %error,
                        "an index snapshot failed; it is retried");
                    self.lock().error = Some(error.to_string());
                    MIN_BACKOFF
                        .saturating_mul(1 << failures.min(6))
                        .min(MAX_BACKOFF)
                }
            };
            tokio::time::sleep(wait.min(interval)).await;
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, SnapshotStatus> {
        self.status.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Takes one snapshot and writes it, or returns `None` if the replica
    /// is not a primary that serves reads.
    ///
    /// The time is read before the rows, and the replica must serve reads
    /// both before and after it reads them: while it holds a lease from
    /// every member, no other primary acknowledged a write (§5.4), and the
    /// writes it acknowledged are applied, so every write acknowledged
    /// before that time is in the rows.
    pub(crate) async fn pass(&mut self) -> Result<Option<Taken>, SnapshotError> {
        if self.shard.check_readable().is_err() {
            return Ok(None);
        }
        let epoch = self.shard.sequencing();
        let taken_ms = u64::try_from(self.wall.now().as_millis()).unwrap_or(u64::MAX);
        let (reader, position) = self.shard.snapshot().await?;
        let rows = self.read(&reader).await?;
        drop(reader);
        if self.shard.check_readable().is_err() || self.shard.sequencing() != epoch {
            return Ok(None);
        }
        let state = (self.contents == Contents::Full).then(|| digests(&rows));
        let (snapshot, written) = match &self.chain {
            Some(chain) if self.contents == Contents::Full && !chain.ends(epoch) => {
                let state = state.as_ref().unwrap_or(&chain.rows);
                let (rows, removed) = delta(chain, state, rows);
                let written = (rows.len() + removed.len()) as u64;
                let snapshot = Snapshot {
                    shard: self.shard.shard().clone(),
                    contents: self.contents,
                    chain: chain.id,
                    number: chain.next,
                    position,
                    taken_ms,
                    rows,
                    removed,
                };
                (snapshot, written)
            }
            _ => {
                let chain = ChainId {
                    epoch,
                    base: position,
                    taken_ms,
                };
                let written = rows.len() as u64;
                let snapshot = Snapshot {
                    shard: self.shard.shard().clone(),
                    contents: self.contents,
                    chain,
                    number: 0,
                    position,
                    taken_ms,
                    rows,
                    removed: Vec::new(),
                };
                (snapshot, written)
            }
        };
        let body = snapshot.encode();
        let md5 = base64::engine::general_purpose::STANDARD.encode(Md5::digest(&body));
        let key = object_key(&self.dir, &snapshot.chain, snapshot.number);
        self.store
            .put_object(PutObject::new(key, body).with_content_md5(md5))
            .await?;
        // Only what is written moves the chain on: after a failure, the
        // next delta is taken against the last snapshot written, under the
        // same number.
        let base = snapshot.number == 0;
        match (&mut self.chain, state) {
            (Some(chain), Some(state)) if !base => {
                chain.next += 1;
                chain.delta_rows += written;
                chain.rows = state;
            }
            (_, state) => {
                self.chain = state.map(|rows| Chain {
                    id: snapshot.chain,
                    next: 1,
                    base_rows: written,
                    delta_rows: 0,
                    rows,
                });
            }
        }
        if base {
            self.prune = Some(snapshot.chain);
        }
        if let Some(keep) = self.prune
            && self.delete_before(keep).await.is_ok()
        {
            self.prune = None;
        }
        Ok(Some(Taken {
            chain: snapshot.chain,
            number: snapshot.number,
            position,
            taken_ms,
            rows: written,
        }))
    }

    /// Reads every row of the shard's tables from `reader`, or for
    /// [`Contents::Unflushed`] the rows that are not at the remote.
    async fn read(&self, reader: &Arc<IndexReader>) -> Result<Rows, SnapshotError> {
        let mut rows = Vec::new();
        for table in ShardTable::ALL {
            let mut after = None;
            loop {
                let chunk = self
                    .shard
                    .snapshot_rows(reader, table, after.take())
                    .await?;
                let Some((last, _)) = chunk.last() else {
                    break;
                };
                after = Some(last.clone());
                rows.extend(chunk.into_iter().map(|row| (table, row)));
            }
        }
        if hooks::snapshot_bug() == SnapshotBug::SkipsDirty {
            rows.retain(|(table, (_, value))| {
                *table != ShardTable::Namespace
                    || codec::decode_entry(value).is_ok_and(|e| e.state != EntryState::Dirty)
            });
        }
        match self.contents {
            Contents::Full => Ok(rows),
            Contents::Unflushed => unflushed(rows),
        }
    }

    /// Deletes the snapshots of every chain of the shard older than
    /// `keep`.
    async fn delete_before(&self, keep: ChainId) -> Result<(), SnapshotError> {
        let mut token = None;
        let mut old = Vec::new();
        loop {
            let mut request = ListObjectsV2::new(self.dir.clone());
            if let Some(token) = token.take() {
                request = request.with_continuation_token(token);
            }
            let page = self.store.list_objects_v2(request).await?;
            old.extend(page.objects.into_iter().filter_map(|object| {
                let (chain, _) = parse_object_key(&self.dir, &object.key)?;
                (chain < keep).then_some(object.key)
            }));
            match page.next_continuation_token {
                Some(next) if page.is_truncated => token = Some(next),
                _ => break,
            }
        }
        for key in old {
            self.store.delete_object(DeleteObject::new(key)).await?;
        }
        Ok(())
    }
}

/// The rows of `state` that are new or changed since `chain`'s latest
/// snapshot, and the digests of its rows that are gone.
fn delta(chain: &Chain, state: &BTreeMap<RowDigest, u64>, rows: Rows) -> (Rows, Vec<RowDigest>) {
    let changed = rows
        .into_iter()
        .filter(|(table, (key, _))| {
            let digest = row_digest(*table, key);
            chain.rows.get(&digest) != state.get(&digest)
        })
        .collect();
    let removed = if hooks::snapshot_bug() == SnapshotBug::KeepsRemoved {
        Vec::new()
    } else {
        chain
            .rows
            .keys()
            .filter(|digest| !state.contains_key(*digest))
            .copied()
            .collect()
    };
    (changed, removed)
}

/// The rows of a `write_back` shard that are not at the remote: entries
/// that are not clean, open uploads and the parts of those and of unclean
/// multipart objects, and the streaming flush's remote uploads (§8.9).
fn unflushed(rows: Rows) -> Result<Rows, SnapshotError> {
    let invalid = |error: codec::CodecError| SnapshotError::Row(error.to_string());
    // `ShardTable::ALL` reads the namespace and the uploads before the
    // parts.
    let mut uploads = BTreeSet::<EpochSeq>::new();
    let mut kept = Vec::new();
    for (table, (key, value)) in rows {
        let keep = match table {
            ShardTable::Namespace => {
                let entry = codec::decode_entry(&value).map_err(invalid)?;
                let unclean = !matches!(entry.state, EntryState::Clean | EntryState::Evicted);
                if unclean
                    && let Some(Payload::Parts { upload, .. }) =
                        entry.object.as_ref().map(|object| &object.payload)
                {
                    uploads.insert(*upload);
                }
                unclean
            }
            ShardTable::Uploads => {
                let (_, _, upload) = codec::decode_upload_key(&key).map_err(invalid)?;
                uploads.insert(upload);
                true
            }
            ShardTable::Parts => {
                let (_, upload, _) = codec::decode_part_key(&key).map_err(invalid)?;
                uploads.contains(&upload)
            }
            ShardTable::RemoteUploads | ShardTable::RemoteParts => true,
            ShardTable::Fragments => false,
        };
        if keep {
            kept.push((table, (key, value)));
        }
    }
    Ok(kept)
}
