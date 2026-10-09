//! The gateway's part in native replication from peer clusters (design
//! §7.8): relaying staged frames to the shard primaries, publishing what a
//! `COMMIT` names in the shard of its key, and applying the small objects
//! of a `BATCH` together.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use skys3_config::BucketsConfig;
use skys3_index::Entry;
use skys3_io::{SystemWallClock, WallClock};
use skys3_log::RecordBody;
use skys3_log::record::{self, Delete, Extent, ExtentRef, IDENTITY_METADATA, MAX_METADATA_LEN};
use skys3_peer::{
    ApplyError, Commit, CommitSink, ExtentSink, Outcome, Put, PutData, SinkError, StagedObject,
    Write,
};
use skys3_types::{BucketDocument, BucketName, ClusterId};

use crate::buckets::GatewayConfig;
use crate::conditions::{ConditionFailed, PeerCondition, Precondition, current_identity};
use crate::shard::{ShardError, ShardRef, Shards};

/// Looks up a bucket by name, as the gateway's local copy of the bucket
/// registers holds it ([`Gateway::bucket`](crate::Gateway::bucket)).
pub type BucketLookup = Arc<dyn Fn(&BucketName) -> Option<BucketDocument> + Send + Sync>;

/// Stages a source's frames on the primary of each key's shard, through
/// the gateway's [`Shards`]: in process when this node is the primary,
/// otherwise forwarded over the intra-cluster transport
/// ([`RoutedShards`](crate::routing::RoutedShards)). Each frame is an
/// `EXTENT` record of the key, at its piece offset, which the primary
/// commits once every member of the shard has it durably, as it does the
/// extents of a client's upload.
#[derive(Clone)]
pub struct PeerExtents<S> {
    shards: S,
    buckets: BucketLookup,
}

impl<S: fmt::Debug> fmt::Debug for PeerExtents<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerExtents")
            .field("shards", &self.shards)
            .finish_non_exhaustive()
    }
}

impl<S: Shards> PeerExtents<S> {
    /// Stages through `shards`, finding destination buckets with
    /// `buckets`.
    #[must_use]
    pub fn new(shards: S, buckets: BucketLookup) -> Self {
        Self { shards, buckets }
    }
}

impl<S: Shards> ExtentSink for PeerExtents<S> {
    async fn append(
        &self,
        bucket: &BucketName,
        key: &str,
        offset: u64,
        data: Bytes,
    ) -> Result<ExtentRef, SinkError> {
        let Some(document) = (self.buckets)(bucket) else {
            return Err(SinkError::Refused(format!("no bucket {bucket}")));
        };
        let shard = ShardRef::for_key(&document, key);
        let extent = Extent {
            key: key.to_owned(),
            offset,
            data,
        };
        self.shards
            .append_extent(&shard, extent)
            .await
            .map_err(|error| match error {
                // The bucket is being deleted, or the record breaks
                // a rule: sending it again cannot help.
                ShardError::Sealed(_) | ShardError::Invalid { .. } => {
                    SinkError::Refused(error.to_string())
                }
                _ => SinkError::Unavailable(error.to_string()),
            })
    }
}

/// Applies the `COMMIT`s of source clusters in the shard of each key,
/// through the gateway's [`Shards`], so the shard's primary evaluates the
/// precondition in its own log.
///
/// - **One record.** A `PUT` publishes the object, referencing the
///   extents staged for it, and a `DELETE` removes it. The record carries
///   the commit's write identity in its stored metadata
///   (`x-amz-meta-skys3-wid`), as the version on any remote does (§7.2);
///   clients never see it.
/// - **Applied once.** That identity is the stored result: a `COMMIT` whose
///   identity the key's current version carries was applied already, and
///   is answered `committed` with the version's ETag, without writing
///   again, whichever node it reaches and whichever member is primary. The
///   shard's condition refuses such a write too, so two copies of a
///   `COMMIT` racing each other apply once.
/// - **Precondition.** Evaluated against the current version's write
///   identity ([`current_identity`]). When it fails, `APPLIED` names the
///   current identity, or none if the key has no current version.
/// - **Deletes.** A delete of a key that has no current version commits
///   nothing and is answered `committed`: the key is absent, as the delete
///   wants, which is also how a replayed delete finds it.
/// - **Receiving buckets.** A commit applies only to a bucket whose
///   `peer_source` is the commit's source cluster.
/// - **Batches.** The items of a `BATCH` carry their bytes inline and skip
///   staging. The items of each shard are committed together
///   ([`Shards::write_all`]), so they share the primary's group commit. A
///   body up to `inline_max_bytes` goes inline in its `PUT`; a longer one
///   is committed first as `extent_bytes` `EXTENT` records, as a client's
///   upload is.
#[derive(Clone)]
pub struct PeerCommits<S> {
    shards: S,
    buckets: BucketLookup,
    settings: BucketsConfig,
    cluster: ClusterId,
    inline_max_bytes: usize,
    extent_bytes: usize,
    /// The clock apply-by times are checked on.
    wall: Arc<dyn WallClock>,
}

impl<S: fmt::Debug> fmt::Debug for PeerCommits<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerCommits")
            .field("shards", &self.shards)
            .field("cluster", &self.cluster)
            .finish_non_exhaustive()
    }
}

impl<S: Shards> PeerCommits<S> {
    /// Applies commits through `shards`, finding destination buckets with
    /// `buckets` and their sources in `config`.
    #[must_use]
    pub fn new(shards: S, buckets: BucketLookup, config: &GatewayConfig) -> Self {
        Self {
            shards,
            buckets,
            settings: config.buckets.clone(),
            cluster: config.cluster_id.clone(),
            inline_max_bytes: usize::try_from(config.inline_max_bytes).unwrap_or(usize::MAX),
            extent_bytes: usize::try_from(config.extent_bytes)
                .unwrap_or(usize::MAX)
                .max(1),
            wall: Arc::new(SystemWallClock),
        }
    }

    /// The commits, checking apply-by times on `wall` (the system clock by
    /// default). The shards check them again as they sequence each write,
    /// on their own clock.
    #[must_use]
    pub fn with_wall_clock(mut self, wall: Arc<dyn WallClock>) -> Self {
        self.wall = wall;
        self
    }

    /// The condition `commit` writes under in the shard of its key, if its
    /// bucket receives from the commit's source; otherwise the refusal.
    fn condition(&self, commit: &Commit) -> Result<PeerCondition, Outcome> {
        let Some(document) = (self.buckets)(&commit.bucket) else {
            return Err(refused(format!("there is no bucket {}", commit.bucket)));
        };
        let now = u64::try_from(self.wall.now().as_millis()).unwrap_or(u64::MAX);
        if commit.apply_by_ms.is_some_and(|by| now > by) {
            return Err(expired());
        }
        if commit.key == crate::peer_s3::DESCRIPTOR_KEY {
            return Err(refused(format!(
                "the key {} is reserved for the peer descriptor",
                commit.key
            )));
        }
        let source = &commit.identity.cluster;
        if self.settings.get(&commit.bucket).peer_source.as_ref() != Some(source) {
            return Err(refused(format!(
                "bucket {} does not receive from cluster {source}",
                commit.bucket
            )));
        }
        Ok(PeerCondition {
            identity: commit.identity.clone(),
            expected: commit.precondition.clone(),
            cluster: self.cluster.clone(),
            shard: ShardRef::for_key(&document, &commit.key),
            apply_by_ms: commit.apply_by_ms,
        })
    }

    /// Applies `commit`; an early answer is the `Err`.
    async fn publish(
        &self,
        commit: &Commit,
        staged: Option<&StagedObject>,
    ) -> Result<Outcome, Outcome> {
        let condition = self.condition(commit)?;
        let shard = condition.shard.clone();
        // What the key holds now answers a replay, and a precondition that
        // fails already, before the staged bytes are needed.
        let entry = self.entry(&shard, &commit.key).await?;
        if let Err(outcome) = self.unapplied(commit, &condition, entry.as_ref()) {
            return Ok(outcome);
        }
        let (body, etag) = match &commit.write {
            Write::Delete => (
                RecordBody::Delete(Delete {
                    key: commit.key.clone(),
                }),
                None,
            ),
            Write::Put(put) => (
                RecordBody::Put(published(commit, put, staged)?),
                Some(put.etag.clone()),
            ),
        };
        let written = self
            .shards
            .write(&shard, body, Precondition::Peer(condition.clone()));
        match written.await.map_err(failed)? {
            Ok(_) => return Ok(Outcome::Committed { etag }),
            // Not sequenced: it would have been too late.
            Err(ConditionFailed::Expired) => return Ok(expired()),
            Err(_) => {}
        }
        // The condition failed when the shard sequenced the write: another
        // copy of the commit applied it, or the key changed since it was
        // read.
        let entry = self.entry(&shard, &commit.key).await?;
        Ok(self.unmet(commit, &condition, entry.as_ref()))
    }

    /// The result of `commit`, whose condition failed when the shard
    /// sequenced its write, from the key's entry `entry` read since.
    fn unmet(&self, commit: &Commit, condition: &PeerCondition, entry: Option<&Entry>) -> Outcome {
        match self.unapplied(commit, condition, entry) {
            Err(outcome) => outcome,
            Ok(()) => Outcome::Failed {
                error: ApplyError::Unavailable,
                reason: "the key changed while the commit was applied".to_owned(),
            },
        }
    }

    /// Applies the items of a `BATCH`, each shard's items together.
    async fn apply_items(&self, items: &[Commit]) -> Vec<Outcome> {
        let mut outcomes: Vec<Option<Outcome>> = items.iter().map(|_| None).collect();
        let mut by_shard: BTreeMap<ShardRef, Vec<Item>> = BTreeMap::new();
        for (at, commit) in items.iter().enumerate() {
            let checked = match &commit.write {
                Write::Put(Put {
                    data: PutData::Inline(_),
                    ..
                })
                | Write::Delete => self.condition(commit),
                Write::Put(_) => Err(refused("a BATCH item carries its bytes inline")),
            };
            match checked {
                Ok(condition) => by_shard
                    .entry(condition.shard.clone())
                    .or_default()
                    .push(Item {
                        at,
                        commit: commit.clone(),
                        condition,
                    }),
                Err(outcome) => outcomes[at] = Some(outcome),
            }
        }
        let applied = each(by_shard, |(shard, items)| {
            let commits = self.clone();
            async move { commits.apply_shard(&shard, items).await }
        });
        for (at, outcome) in applied.await.into_iter().flatten().flatten() {
            outcomes[at] = Some(outcome);
        }
        outcomes
            .into_iter()
            .map(|outcome| outcome.unwrap_or_else(unanswered))
            .collect()
    }

    /// Applies `items`, the items of a batch in `shard`, and returns each
    /// one's result by its index in the batch.
    async fn apply_shard(&self, shard: &ShardRef, items: Vec<Item>) -> Vec<(usize, Outcome)> {
        let mut answered = Vec::new();
        // A delete of a key that has no current version writes nothing, so
        // a delete reads its key first, as a `COMMIT` does.
        let deletes = items
            .iter()
            .filter(|item| matches!(item.commit.write, Write::Delete));
        let keys: Vec<_> = deletes.map(|item| item.commit.key.clone()).collect();
        let mut read = self.entries(shard, keys).await.into_iter();
        let mut writes = Vec::new();
        for item in items {
            if matches!(item.commit.write, Write::Delete) {
                let early = match read.next().flatten() {
                    Some(Ok(entry)) => {
                        self.unapplied(&item.commit, &item.condition, entry.as_ref())
                    }
                    Some(Err(error)) => Err(failed(error)),
                    None => Err(unanswered()),
                };
                if let Err(outcome) = early {
                    answered.push((item.at, outcome));
                    continue;
                }
            }
            writes.push(item);
        }
        // Bodies too long to go inline are committed as extents first:
        // records of different classes become durable independently, so a
        // `PUT` follows its extents (§10.4).
        let bodies = each(writes, |item| {
            let commits = self.clone();
            let shard = shard.clone();
            async move {
                let body = commits.record(&shard, &item.commit).await;
                (item, body)
            }
        });
        let mut sequenced = Vec::new();
        for (item, body) in bodies.await.into_iter().flatten() {
            match body {
                Ok(body) => sequenced.push((item, body)),
                Err(outcome) => answered.push((item.at, outcome)),
            }
        }
        let (items, writes): (Vec<_>, Vec<_>) = sequenced
            .into_iter()
            .map(|(item, body)| {
                let condition = Precondition::Peer(item.condition.clone());
                (item, (body, condition))
            })
            .unzip();
        let written = self.shards.write_all(shard, writes).await;
        let mut unmet = Vec::new();
        for (item, written) in items.into_iter().zip(written) {
            match written {
                Ok(Ok(_)) => {
                    let etag = match &item.commit.write {
                        Write::Put(put) => Some(put.etag.clone()),
                        Write::Delete => None,
                    };
                    answered.push((item.at, Outcome::Committed { etag }));
                }
                Ok(Err(ConditionFailed::Expired)) => answered.push((item.at, expired())),
                Ok(Err(_)) => unmet.push(item),
                Err(error) => answered.push((item.at, failed(error))),
            }
        }
        // As for a `COMMIT`: another copy applied the item, or the key
        // changed since it was read.
        let keys: Vec<_> = unmet.iter().map(|item| item.commit.key.clone()).collect();
        let read = self.entries(shard, keys).await;
        for (item, entry) in unmet.into_iter().zip(read) {
            let outcome = match entry {
                Some(Ok(entry)) => self.unmet(&item.commit, &item.condition, entry.as_ref()),
                Some(Err(error)) => failed(error),
                None => unanswered(),
            };
            answered.push((item.at, outcome));
        }
        answered
    }

    /// The entries of `keys` in `shard`, read all at once, in order.
    async fn entries(
        &self,
        shard: &ShardRef,
        keys: Vec<String>,
    ) -> Vec<Option<Result<Option<Entry>, ShardError>>> {
        each(keys, |key| {
            let (shards, shard) = (self.shards.clone(), shard.clone());
            async move { shards.entry(&shard, &key).await }
        })
        .await
    }

    /// The record of a batch item in `shard`: a `DELETE`, or a `PUT` with
    /// its bytes inline or, past `inline_max_bytes`, in extents committed
    /// first.
    async fn record(&self, shard: &ShardRef, commit: &Commit) -> Result<RecordBody, Outcome> {
        let put = match &commit.write {
            Write::Delete => {
                return Ok(RecordBody::Delete(Delete {
                    key: commit.key.clone(),
                }));
            }
            Write::Put(put) => put,
        };
        let PutData::Inline(bytes) = &put.data else {
            return Err(refused("a BATCH item carries its bytes inline"));
        };
        let data = if bytes.len() <= self.inline_max_bytes {
            record::PutData::Inline(bytes.clone())
        } else {
            let mut extents = Vec::new();
            for (n, chunk) in bytes.chunks(self.extent_bytes).enumerate() {
                let extent = Extent {
                    key: commit.key.clone(),
                    offset: (n * self.extent_bytes) as u64,
                    data: bytes.slice_ref(chunk),
                };
                let appended = self.shards.append_extent(shard, extent).await;
                extents.push(appended.map_err(failed)?);
            }
            record::PutData::Extents(extents)
        };
        version(commit, put, data).map(RecordBody::Put)
    }

    async fn entry(&self, shard: &ShardRef, key: &str) -> Result<Option<Entry>, Outcome> {
        self.shards.entry(shard, key).await.map_err(failed)
    }

    /// Whether `commit` is still to be applied to the key, whose entry is
    /// `entry`; otherwise its result:
    ///
    /// - `committed` with the version's ETag if the current version is
    ///   the commit's write, applied already;
    /// - `committed` for a delete of a key that has no current version;
    /// - and `precondition failed`, with the current identity, if the
    ///   precondition does not hold.
    fn unapplied(
        &self,
        commit: &Commit,
        condition: &PeerCondition,
        entry: Option<&Entry>,
    ) -> Result<(), Outcome> {
        let current = current_identity(entry, &self.cluster, &condition.shard);
        let object = entry.and_then(|entry| entry.object.as_ref());
        match (&commit.write, current, object) {
            (_, Some(current), Some(object)) if current == commit.identity => {
                Err(Outcome::Committed {
                    etag: Some(object.local_etag.clone()),
                })
            }
            (Write::Delete, None, _) => Err(Outcome::Committed { etag: None }),
            (_, current, _) => match condition.check(entry) {
                Ok(()) => Ok(()),
                Err(_) => Err(Outcome::PreconditionFailed { current }),
            },
        }
    }
}

impl<S: Shards> CommitSink for PeerCommits<S> {
    async fn apply(&self, commit: &Commit, staged: Option<&StagedObject>) -> Outcome {
        match self.publish(commit, staged).await {
            Ok(outcome) | Err(outcome) => outcome,
        }
    }

    async fn apply_batch(&self, items: &[Commit]) -> Vec<Outcome> {
        self.apply_items(items).await
    }
}

/// A batch item on its way to its shard: its index in the batch, and its
/// condition there.
struct Item {
    at: usize,
    commit: Commit,
    condition: PeerCondition,
}

/// Runs `job` on each of `inputs` at once, each on a task of its own, and
/// returns the outputs in order; `None` for a task that panicked.
async fn each<T, F>(
    inputs: impl IntoIterator<Item = T>,
    job: impl Fn(T) -> F,
) -> Vec<Option<F::Output>>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let tasks: Vec<_> = inputs
        .into_iter()
        .map(|input| tokio::spawn(job(input)))
        .collect();
    let mut outputs = Vec::with_capacity(tasks.len());
    for task in tasks {
        outputs.push(task.await.ok());
    }
    outputs
}

/// The result of an item whose request ended without an answer.
fn unanswered() -> Outcome {
    Outcome::Failed {
        error: ApplyError::Unavailable,
        reason: "the shard's request ended without an answer".to_owned(),
    }
}

/// The `PUT` record that publishes `put`, the write of `commit`, from the
/// extents `staged` holds, with the commit's identity in its metadata.
fn published(
    commit: &Commit,
    put: &Put,
    staged: Option<&StagedObject>,
) -> Result<record::Put, Outcome> {
    let data = match &put.data {
        PutData::Staged { .. } if put.size == 0 => record::PutData::Inline(Bytes::new()),
        PutData::Staged { piece } => staged
            .and_then(|staged| staged.extents(*piece, put.size))
            .map(record::PutData::Extents)
            .ok_or_else(|| Outcome::Failed {
                error: ApplyError::Incomplete,
                reason: format!("piece {piece} is not staged whole"),
            })?,
        // Publishing parts needs a `PUT` that keeps their boundaries.
        PutData::Multipart(_) => {
            return Err(Outcome::Failed {
                error: ApplyError::Unavailable,
                reason: "this destination does not publish multipart objects yet".to_owned(),
            });
        }
        PutData::Inline(_) => return Err(refused("only BATCH items carry inline bytes")),
    };
    version(commit, put, data)
}

/// The `PUT` record of `put`, the write of `commit`, whose bytes are
/// `data`, with the commit's identity in its metadata.
fn version(commit: &Commit, put: &Put, data: record::PutData) -> Result<record::Put, Outcome> {
    let mut metadata = put.metadata.clone();
    metadata.insert(IDENTITY_METADATA.to_owned(), commit.identity.to_string());
    let len: usize = metadata
        .iter()
        .map(|(name, value)| name.len() + value.len())
        .sum();
    if len > MAX_METADATA_LEN {
        return Err(refused(format!(
            "the metadata with the write identity is {len} bytes; the limit is {MAX_METADATA_LEN}"
        )));
    }
    Ok(record::Put {
        key: commit.key.clone(),
        size: put.size,
        last_modified_ms: put.last_modified_ms,
        etag: put.etag.clone(),
        inherited_identity: None,
        metadata,
        tags: put.tags.clone(),
        checksums: put.checksums.clone(),
        copy_source: None,
        data,
    })
}

/// The answer to a write that reached its shard after its apply-by time:
/// the source may send it again with a new one.
fn expired() -> Outcome {
    Outcome::Failed {
        error: ApplyError::Unavailable,
        reason: "the COMMIT's apply-by time passed".to_owned(),
    }
}

fn refused(reason: impl Into<String>) -> Outcome {
    Outcome::Failed {
        error: ApplyError::Refused,
        reason: reason.into(),
    }
}

/// The result of a shard request that failed.
fn failed(error: ShardError) -> Outcome {
    let error_kind = match error {
        // The bucket is being deleted, or the record breaks a rule:
        // sending it again cannot help.
        ShardError::Sealed(_) | ShardError::Invalid { .. } => ApplyError::Refused,
        _ => ApplyError::Unavailable,
    };
    Outcome::Failed {
        error: error_kind,
        reason: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use skys3_config::Config;
    use skys3_log::record::Put as PutRecord;
    use skys3_peer::Precondition as Expected;
    use skys3_peer::{BatchBuilder, DEFAULT_BATCH_RECORD_BYTES, Refusal};
    use skys3_types::{
        BucketId, BucketMode, ETag, EpochSeq, ProposalId, ShardCount, WriteIdentity,
    };

    use std::time::Duration;

    use skys3_io::ManualWallClock;

    use super::*;
    use crate::stub::MemoryShards;

    const SOURCE: &str = "prod-us";

    fn document() -> BucketDocument {
        BucketDocument {
            bucket_id: BucketId::new("b-dest").unwrap(),
            name: "archive".parse().unwrap(),
            mode: BucketMode::Local,
            shards: ShardCount::new(4).unwrap(),
            replicas: 1,
            min_write_replicas: 1,
            clean_copies: 0,
            target: None,
            created_unix_ms: 0,
            lifecycle: None,
            proposal_id: ProposalId::new("p").unwrap(),
        }
    }

    fn lookup(document: &BucketDocument) -> BucketLookup {
        let document = document.clone();
        Arc::new(move |wanted: &BucketName| (*wanted == document.name).then(|| document.clone()))
    }

    /// The shards of [`document`], open, and a lookup that finds it.
    async fn opened() -> (MemoryShards, BucketDocument) {
        let shards = MemoryShards::new().await;
        let document = document();
        for shard in ShardRef::all(&document) {
            shards.open(&shard, &document).await.unwrap();
        }
        (shards, document)
    }

    /// The gateway settings of cluster `prod-eu`, where `archive` receives
    /// from `source`.
    fn settings(source: Option<&str>) -> GatewayConfig {
        let config: Config = "[cluster]\ncluster_id = \"prod-eu\"\n[control_store]\n\
                              etcd_endpoints = [\"https://etcd.invalid:2379\"]\n"
            .parse()
            .unwrap();
        let mut config = GatewayConfig::new(&config);
        let mut archive = config.buckets.defaults.clone();
        archive.mode = BucketMode::Local;
        archive.peer_source = source.map(|source| ClusterId::new(source).unwrap());
        config
            .buckets
            .named
            .insert("archive".parse().unwrap(), archive);
        config
    }

    fn identity(seq: u64) -> WriteIdentity {
        format!("{SOURCE}/b-src/5/42.{seq}").parse().unwrap()
    }

    fn etag(n: u8) -> ETag {
        ETag::new(format!("{n:032x}")).unwrap()
    }

    fn put_commit(seq: u64, key: &str, precondition: Expected, size: u64) -> Commit {
        Commit {
            identity: identity(seq),
            bucket: "archive".parse().unwrap(),
            key: key.to_owned(),
            precondition,
            write: Write::Put(Put {
                size,
                etag: etag(u8::try_from(seq).unwrap()),
                last_modified_ms: 1_700_000_000_000 + seq,
                metadata: BTreeMap::from([("content-type".to_owned(), "image/jpeg".to_owned())]),
                tags: BTreeMap::from([("source".to_owned(), "us".to_owned())]),
                checksums: BTreeMap::new(),
                data: PutData::Staged { piece: 7 },
            }),
            apply_by_ms: None,
        }
    }

    fn delete_commit(seq: u64, key: &str, precondition: Expected) -> Commit {
        Commit {
            write: Write::Delete,
            ..put_commit(seq, key, precondition, 0)
        }
    }

    /// Stages `data` for `key` as piece 7 in two extents, as the staging
    /// service does.
    async fn stage(
        shards: &MemoryShards,
        document: &BucketDocument,
        key: &str,
        data: &[u8],
    ) -> StagedObject {
        let sink = PeerExtents::new(shards.clone(), lookup(document));
        let half = data.len() / 2;
        let mut extents = BTreeMap::new();
        for (offset, bytes) in [(0, &data[..half]), (half, &data[half..])] {
            let bytes = Bytes::copy_from_slice(bytes);
            let extent = sink
                .append(&document.name, key, offset as u64, bytes)
                .await
                .unwrap();
            extents.insert(offset as u64, extent);
        }
        StagedObject {
            bucket: document.name.clone(),
            key: key.to_owned(),
            pieces: BTreeMap::from([(7, extents)]),
        }
    }

    /// The number of records the shard of `key` has sequenced.
    async fn sequenced(shards: &MemoryShards, document: &BucketDocument, key: &str) -> u64 {
        let shard = ShardRef::for_key(document, key);
        let local = shards.local().set().get(&(&shard).into()).await.unwrap();
        local.last_sequenced().get()
    }

    #[tokio::test]
    async fn duplicate_commits_apply_once() {
        let (shards, document) = opened().await;
        let commits = PeerCommits::new(shards.clone(), lookup(&document), &settings(Some(SOURCE)));
        assert!(format!("{commits:?}").contains("PeerCommits"));
        let key = "photos/cat.jpg";
        let staged = stage(&shards, &document, key, b"the cat's photo").await;
        // Unconditional, so only the write identity keeps it from applying
        // twice.
        let commit = put_commit(1, key, Expected::Unconditional, 15);

        // Two copies race, as after a reconnect: both are committed, and
        // one record is written.
        let before = sequenced(&shards, &document, key).await;
        let (first, second) = tokio::join!(
            commits.apply(&commit, Some(&staged)),
            commits.apply(&commit, Some(&staged)),
        );
        let committed = Outcome::Committed {
            etag: Some(etag(1)),
        };
        assert_eq!((&first, &second), (&committed, &committed));
        assert_eq!(sequenced(&shards, &document, key).await, before + 1);

        // The object is published with the source's attributes and the
        // write identity, and its bytes are the staged extents.
        let shard = ShardRef::for_key(&document, key);
        let entry = shards.entry(&shard, key).await.unwrap().unwrap();
        let object = entry.object.as_ref().unwrap();
        assert_eq!(object.local_etag, etag(1));
        assert_eq!(object.last_modified_ms, 1_700_000_000_001);
        assert_eq!(object.metadata[IDENTITY_METADATA], identity(1).to_string());
        assert_eq!(object.tags["source"], "us");
        let cluster = ClusterId::new("prod-eu").unwrap();
        assert_eq!(
            current_identity(Some(&entry), &cluster, &shard),
            Some(identity(1))
        );
        let skys3_index::Payload::Extents(extents) = &object.payload else {
            panic!("expected extents, got {:?}", object.payload);
        };
        let mut body = Vec::new();
        for extent in extents {
            body.extend_from_slice(&shards.payload(&shard, extent.position).await.unwrap());
        }
        assert_eq!(body, b"the cat's photo");

        // A replay after the APPLIED was lost, with the staging consumed or
        // on another node, returns the stored result and writes nothing,
        // whatever its precondition.
        for precondition in [Expected::Absent, Expected::Matches(identity(9))] {
            let replay = Commit {
                precondition,
                ..commit.clone()
            };
            assert_eq!(commits.apply(&replay, None).await, committed);
        }
        assert_eq!(sequenced(&shards, &document, key).await, before + 1);
        assert_eq!(shards.entry(&shard, key).await.unwrap(), Some(entry));
    }

    #[tokio::test]
    async fn precondition_failures_return_the_current_write_identity() {
        let (shards, document) = opened().await;
        let commits = PeerCommits::new(shards.clone(), lookup(&document), &settings(Some(SOURCE)));
        let key = "k";
        let staged = stage(&shards, &document, key, b"0123").await;
        let first = put_commit(1, key, Expected::Absent, 4);
        assert!(matches!(
            commits.apply(&first, Some(&staged)).await,
            Outcome::Committed { .. }
        ));

        // The key has a version: neither "absent" nor another identity
        // holds, and APPLIED names the current one.
        let current = Outcome::PreconditionFailed {
            current: Some(identity(1)),
        };
        for precondition in [Expected::Absent, Expected::Matches(identity(9))] {
            let commit = put_commit(2, key, precondition, 4);
            assert_eq!(commits.apply(&commit, Some(&staged)).await, current);
        }
        // The expected identity holds.
        let second = put_commit(2, key, Expected::Matches(identity(1)), 4);
        assert_eq!(
            commits.apply(&second, Some(&staged)).await,
            Outcome::Committed {
                etag: Some(etag(2))
            }
        );

        // A version this cluster's own client wrote is named by its local
        // write identity.
        let shard = ShardRef::for_key(&document, key);
        let local = RecordBody::Put(PutRecord {
            key: key.to_owned(),
            size: 0,
            last_modified_ms: 0,
            etag: etag(0),
            inherited_identity: None,
            metadata: BTreeMap::new(),
            tags: BTreeMap::new(),
            checksums: BTreeMap::new(),
            copy_source: None,
            data: record::PutData::Inline(Bytes::new()),
        });
        let position = shards
            .write(&shard, local, Precondition::None)
            .await
            .unwrap()
            .unwrap();
        let local = WriteIdentity::new(
            ClusterId::new("prod-eu").unwrap(),
            shard.bucket.clone(),
            shard.shard,
            position,
        );
        let third = put_commit(3, key, Expected::Matches(identity(2)), 4);
        assert_eq!(
            commits.apply(&third, Some(&staged)).await,
            Outcome::PreconditionFailed {
                current: Some(local.clone())
            }
        );

        // A delete under the right precondition removes it; a replay, or a
        // delete of a key that has no version, commits nothing more.
        let delete = delete_commit(4, key, Expected::Matches(local));
        let deleted = Outcome::Committed { etag: None };
        assert_eq!(commits.apply(&delete, None).await, deleted);
        let after = sequenced(&shards, &document, key).await;
        assert_eq!(commits.apply(&delete, None).await, deleted);
        let absent = delete_commit(5, "never", Expected::Matches(identity(1)));
        assert_eq!(commits.apply(&absent, None).await, deleted);
        assert_eq!(sequenced(&shards, &document, key).await, after);
        // A delete whose expected version is gone fails, naming the
        // current one.
        assert!(matches!(
            commits
                .apply(&put_commit(6, key, Expected::Absent, 4), Some(&staged))
                .await,
            Outcome::Committed { .. }
        ));
        let stale = delete_commit(7, key, Expected::Matches(identity(2)));
        assert_eq!(
            commits.apply(&stale, None).await,
            Outcome::PreconditionFailed {
                current: Some(identity(6))
            }
        );
        // A PUT that expects a version of a key that has none.
        let missing = put_commit(8, "other", Expected::Matches(identity(1)), 4);
        let staged = stage(&shards, &document, "other", b"0123").await;
        assert_eq!(
            commits.apply(&missing, Some(&staged)).await,
            Outcome::PreconditionFailed { current: None }
        );
    }

    #[tokio::test]
    async fn a_local_tag_change_replaces_the_carried_identity() {
        let (shards, document) = opened().await;
        let commits = PeerCommits::new(shards.clone(), lookup(&document), &settings(Some(SOURCE)));
        let key = "k";
        let staged = stage(&shards, &document, key, b"0123").await;
        let first = put_commit(1, key, Expected::Absent, 4);
        assert!(matches!(
            commits.apply(&first, Some(&staged)).await,
            Outcome::Committed { .. }
        ));

        // A client of this cluster (`peer_local_writes`) changes the tags:
        // a new version, whose identity is the `TAGS` record's, as the
        // flusher sends it (§7.2).
        let shard = ShardRef::for_key(&document, key);
        let tags = RecordBody::Tags(record::Tags {
            key: key.to_owned(),
            tags: BTreeMap::from([("local".to_owned(), "yes".to_owned())]),
        });
        let position = shards
            .write(&shard, tags, Precondition::None)
            .await
            .unwrap()
            .unwrap();
        let entry = shards.entry(&shard, key).await.unwrap().unwrap();
        let object = entry.object.as_ref().unwrap();
        assert!(!object.metadata.contains_key(IDENTITY_METADATA));
        assert_eq!(object.metadata["content-type"], "image/jpeg");
        let local = WriteIdentity::new(
            ClusterId::new("prod-eu").unwrap(),
            shard.bucket.clone(),
            shard.shard,
            position,
        );

        // The source's next version, which expects its own, does not
        // overwrite the tag change, and its replay of the first finds it
        // gone.
        let second = put_commit(2, key, Expected::Matches(identity(1)), 4);
        let changed = Outcome::PreconditionFailed {
            current: Some(local),
        };
        assert_eq!(commits.apply(&second, Some(&staged)).await, changed);
        assert_eq!(commits.apply(&first, Some(&staged)).await, changed);
        assert_eq!(shards.entry(&shard, key).await.unwrap(), Some(entry));
    }

    #[track_caller]
    fn assert_failed(outcome: &Outcome, wanted: ApplyError) {
        assert!(
            matches!(outcome, Outcome::Failed { error, .. } if *error == wanted),
            "expected {wanted:?}, got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn commits_that_cannot_apply_are_refused_or_retried() {
        let (shards, document) = opened().await;
        let key = "k";
        let staged = stage(&shards, &document, key, b"0123").await;
        let commit = put_commit(1, key, Expected::Absent, 4);

        // Only a bucket that receives from the commit's source.
        for source in [None, Some("prod-ap")] {
            let commits = PeerCommits::new(shards.clone(), lookup(&document), &settings(source));
            let outcome = commits.apply(&commit, Some(&staged)).await;
            assert_failed(&outcome, ApplyError::Refused);
        }
        let commits = PeerCommits::new(shards.clone(), lookup(&document), &settings(Some(SOURCE)));
        let unknown = Commit {
            bucket: "unknown".parse().unwrap(),
            ..commit.clone()
        };
        assert_failed(
            &commits.apply(&unknown, Some(&staged)).await,
            ApplyError::Refused,
        );

        // Bytes that are not staged whole.
        assert_failed(&commits.apply(&commit, None).await, ApplyError::Incomplete);
        let longer = put_commit(1, key, Expected::Absent, 5);
        assert_failed(
            &commits.apply(&longer, Some(&staged)).await,
            ApplyError::Incomplete,
        );
        // Multipart objects wait for a later version; inline bytes travel
        // only in BATCH.
        let Write::Put(put) = &commit.write else {
            unreachable!()
        };
        for (data, error) in [
            (PutData::Multipart(Vec::new()), ApplyError::Unavailable),
            (
                PutData::Inline(Bytes::from_static(b"0123")),
                ApplyError::Refused,
            ),
        ] {
            let other = Commit {
                write: Write::Put(Put {
                    data,
                    ..put.clone()
                }),
                ..commit.clone()
            };
            assert_failed(&commits.apply(&other, Some(&staged)).await, error);
        }
        // Metadata with no room left for the write identity.
        let full = Commit {
            write: Write::Put(Put {
                metadata: BTreeMap::from([(
                    "x-amz-meta-big".to_owned(),
                    "x".repeat(MAX_METADATA_LEN - 20),
                )]),
                ..put.clone()
            }),
            ..commit.clone()
        };
        assert_failed(
            &commits.apply(&full, Some(&staged)).await,
            ApplyError::Refused,
        );

        // A bucket being deleted refuses it; shards that are unavailable
        // may take it later.
        let shard = ShardRef::for_key(&document, key);
        shards.seal(&shard).await.unwrap();
        let sealed = commits.apply(&commit, Some(&staged)).await;
        assert_failed(&sealed, ApplyError::Refused);
        shards.unseal(&shard).await.unwrap();
        shards.set_unavailable(true);
        let unavailable = commits.apply(&commit, Some(&staged)).await;
        assert_failed(&unavailable, ApplyError::Unavailable);
        shards.set_unavailable(false);

        // An empty object needs no staging.
        let empty = put_commit(2, "empty", Expected::Absent, 0);
        assert_eq!(
            commits.apply(&empty, None).await,
            Outcome::Committed {
                etag: Some(etag(2))
            }
        );
        // And the original applies at last.
        assert!(matches!(
            commits.apply(&commit, Some(&staged)).await,
            Outcome::Committed { .. }
        ));
    }

    /// A batch item: `put_commit` with `body` inline, or a delete.
    fn item(seq: u64, key: &str, precondition: Expected, body: Option<&[u8]>) -> Commit {
        match body {
            Some(body) => {
                let mut commit = put_commit(seq, key, precondition, body.len() as u64);
                if let Write::Put(put) = &mut commit.write {
                    put.data = PutData::Inline(Bytes::copy_from_slice(body));
                }
                commit
            }
            None => delete_commit(seq, key, precondition),
        }
    }

    /// The bytes of `key`'s current version.
    async fn body(shards: &MemoryShards, document: &BucketDocument, key: &str) -> Vec<u8> {
        let shard = ShardRef::for_key(document, key);
        let entry = shards.entry(&shard, key).await.unwrap().unwrap();
        match &entry.object.as_ref().unwrap().payload {
            skys3_index::Payload::Inline(position) => {
                shards.payload(&shard, *position).await.unwrap().to_vec()
            }
            skys3_index::Payload::Extents(extents) => {
                let mut body = Vec::new();
                for extent in extents {
                    body.extend_from_slice(&shards.payload(&shard, extent.position).await.unwrap());
                }
                body
            }
            other => panic!("unexpected payload {other:?}"),
        }
    }

    /// The group commits of the shards' log so far.
    fn group_commits(shards: &MemoryShards) -> u64 {
        let (_, log) = shards.local().set().logs().next().unwrap();
        log.stats().group_commits
    }

    #[tokio::test]
    async fn a_batch_applies_each_shards_items_together() {
        let (shards, document) = opened().await;
        let mut config = settings(Some(SOURCE));
        config.inline_max_bytes = 1024;
        config.extent_bytes = 1000;
        let commits = PeerCommits::new(shards.clone(), lookup(&document), &config);
        let items: Vec<_> = (1..=40)
            .map(|seq| {
                let body = format!("object {seq}");
                item(
                    seq,
                    &format!("logs/{seq}"),
                    Expected::Absent,
                    Some(body.as_bytes()),
                )
            })
            .collect();
        let before = (group_commits(&shards), records(&shards).await);
        let outcomes = commits.apply_batch(&items).await;
        for (seq, outcome) in (1..).zip(&outcomes) {
            assert_eq!(
                *outcome,
                Outcome::Committed {
                    etag: Some(etag(u8::try_from(seq).unwrap()))
                }
            );
        }
        // Forty records in at most one group commit per shard, not one per
        // object.
        assert_eq!(records(&shards).await, before.1 + 40);
        let groups = group_commits(&shards) - before.0;
        assert!((1..=4).contains(&groups), "{groups} group commits");
        assert_eq!(body(&shards, &document, "logs/7").await, b"object 7");
        let shard = ShardRef::for_key(&document, "logs/7");
        let entry = shards.entry(&shard, "logs/7").await.unwrap().unwrap();
        let object = entry.object.as_ref().unwrap();
        assert_eq!(object.metadata[IDENTITY_METADATA], identity(7).to_string());
        assert_eq!(object.tags["source"], "us");

        // A replay after the APPLIEDs were lost returns the stored results
        // and writes nothing.
        assert_eq!(commits.apply_batch(&items).await, outcomes);
        assert_eq!(records(&shards).await, before.1 + 40);

        // A body past `inline_max_bytes` is committed as extents first.
        let large: Vec<u8> = (0..2500u32).map(|n| (n % 251) as u8).collect();
        let next = [
            item(41, "large", Expected::Absent, Some(&large)),
            item(42, "logs/1", Expected::Matches(identity(1)), None),
            item(43, "logs/2", Expected::Absent, None),
            item(44, "never", Expected::Absent, None),
            item(45, "logs/3", Expected::Absent, Some(b"again")),
        ];
        let outcomes = commits.apply_batch(&next).await;
        assert_eq!(
            outcomes,
            [
                Outcome::Committed {
                    etag: Some(etag(41))
                },
                Outcome::Committed { etag: None },
                Outcome::PreconditionFailed {
                    current: Some(identity(2))
                },
                Outcome::Committed { etag: None },
                Outcome::PreconditionFailed {
                    current: Some(identity(3))
                },
            ]
        );
        assert_eq!(body(&shards, &document, "large").await, large);
        let shard = ShardRef::for_key(&document, "large");
        let entry = shards.entry(&shard, "large").await.unwrap().unwrap();
        let skys3_index::Payload::Extents(extents) = &entry.object.unwrap().payload else {
            panic!("expected extents");
        };
        assert_eq!(extents.len(), 3);
        let shard = ShardRef::for_key(&document, "logs/1");
        let deleted = shards.entry(&shard, "logs/1").await.unwrap().unwrap();
        assert!(deleted.object.is_none());
    }

    /// The records the shards of [`document`] have sequenced.
    async fn records(shards: &MemoryShards) -> u64 {
        let document = document();
        let mut total = 0;
        for shard in ShardRef::all(&document) {
            let local = shards.local().set().get(&(&shard).into()).await.unwrap();
            total += local.last_sequenced().get();
        }
        total
    }

    /// The bytes the shards' log has written so far.
    fn log_bytes(shards: &MemoryShards) -> u64 {
        let (_, log) = shards.local().set().logs().next().unwrap();
        log.stats().bytes
    }

    #[tokio::test]
    async fn a_built_batch_writes_at_most_its_record_budget() {
        let (shards, document) = opened().await;
        let commits = PeerCommits::new(shards.clone(), lookup(&document), &settings(Some(SOURCE)));
        // 1,024 objects of 8 KiB, every eighth with the most metadata: the
        // builder splits them so that each batch's records fit one group
        // commit of the default log.
        let body = vec![5; 8 << 10];
        let mut builder = BatchBuilder::new();
        let mut batches = Vec::new();
        for seq in 1..=1024 {
            let mut item = item(seq % 250, &format!("k{seq}"), Expected::Absent, Some(&body));
            item.identity = identity(seq);
            if seq % 8 == 0
                && let Write::Put(put) = &mut item.write
            {
                put.metadata = BTreeMap::from([(
                    "x-amz-meta-big".to_owned(),
                    "x".repeat(MAX_METADATA_LEN - 200),
                )]);
            }
            if builder.push(&item) == Err(Refusal::Full) {
                batches.extend(builder.take());
                builder.push(&item).unwrap();
            }
        }
        batches.extend(builder.take());
        assert!(batches.len() > 2, "{} batches", batches.len());
        let cap = skys3_log::LogConfig::default().group_commit_max_bytes;
        assert_eq!(cap, DEFAULT_BATCH_RECORD_BYTES);
        for batch in batches {
            let before = log_bytes(&shards);
            let outcomes = commits.apply_batch(&batch.items).await;
            assert!(
                outcomes
                    .iter()
                    .all(|o| matches!(o, Outcome::Committed { .. }))
            );
            let written = log_bytes(&shards) - before;
            assert!(written <= cap, "{written} bytes of records");
        }
    }

    #[tokio::test]
    async fn the_descriptor_key_is_never_written() {
        let (shards, document) = opened().await;
        let key = crate::peer_s3::DESCRIPTOR_KEY;
        let staged = stage(&shards, &document, key, b"0123").await;
        let commits = PeerCommits::new(shards.clone(), lookup(&document), &settings(Some(SOURCE)));
        let outcome = commits
            .apply(&put_commit(1, key, Expected::Absent, 4), Some(&staged))
            .await;
        assert_failed(&outcome, ApplyError::Refused);
        let outcomes = commits
            .apply_batch(&[
                item(2, key, Expected::Absent, Some(b"a")),
                item(3, key, Expected::Unconditional, None),
                item(4, "other", Expected::Absent, Some(b"b")),
            ])
            .await;
        assert_failed(&outcomes[0], ApplyError::Refused);
        assert_failed(&outcomes[1], ApplyError::Refused);
        assert!(matches!(outcomes[2], Outcome::Committed { .. }));
    }

    #[tokio::test]
    async fn batch_items_that_cannot_apply_are_refused_or_retried() {
        let (shards, document) = opened().await;
        let commits = PeerCommits::new(shards.clone(), lookup(&document), &settings(Some(SOURCE)));
        let unknown = Commit {
            bucket: "unknown".parse().unwrap(),
            ..item(1, "a", Expected::Absent, Some(b"a"))
        };
        let staged = put_commit(2, "b", Expected::Absent, 4);
        let outcomes = commits
            .apply_batch(&[unknown, staged, item(3, "c", Expected::Absent, Some(b"c"))])
            .await;
        assert_failed(&outcomes[0], ApplyError::Refused);
        assert_failed(&outcomes[1], ApplyError::Refused);
        assert!(matches!(outcomes[2], Outcome::Committed { .. }));
        let elsewhere = PeerCommits::new(shards.clone(), lookup(&document), &settings(None));
        let outcomes = elsewhere
            .apply_batch(&[item(4, "d", Expected::Absent, Some(b"d"))])
            .await;
        assert_failed(&outcomes[0], ApplyError::Refused);

        // Shards that cannot serve now: the source sends the items again.
        shards.set_unavailable(true);
        let outcomes = commits
            .apply_batch(&[
                item(5, "e", Expected::Absent, Some(b"e")),
                item(6, "c", Expected::Unconditional, None),
            ])
            .await;
        for outcome in &outcomes {
            assert_failed(outcome, ApplyError::Unavailable);
        }
        shards.set_unavailable(false);
        // A bucket being deleted refuses them for good.
        let shard = ShardRef::for_key(&document, "f");
        shards.seal(&shard).await.unwrap();
        let outcomes = commits
            .apply_batch(&[item(7, "f", Expected::Absent, Some(b"f"))])
            .await;
        assert_failed(&outcomes[0], ApplyError::Refused);
        // So does metadata with no room for the write identity.
        shards.unseal(&shard).await.unwrap();
        let mut full = item(8, "g", Expected::Absent, Some(b"g"));
        if let Write::Put(put) = &mut full.write {
            put.metadata = BTreeMap::from([(
                "x-amz-meta-big".to_owned(),
                "x".repeat(MAX_METADATA_LEN - 20),
            )]);
        }
        assert_failed(&commits.apply_batch(&[full]).await[0], ApplyError::Refused);
    }

    /// Shards that take the default [`Shards::write_all`], and whose writes
    /// wait for a permit of the gate, if there is one, before they reach
    /// the shard.
    #[derive(Debug, Clone)]
    struct OneByOne(MemoryShards, Option<Arc<tokio::sync::Semaphore>>);

    impl Shards for OneByOne {
        async fn open(&self, s: &ShardRef, b: &BucketDocument) -> Result<(), ShardError> {
            self.0.open(s, b).await
        }
        async fn seal(&self, s: &ShardRef) -> Result<crate::ShardSummary, ShardError> {
            self.0.seal(s).await
        }
        async fn unseal(&self, s: &ShardRef) -> Result<(), ShardError> {
            self.0.unseal(s).await
        }
        async fn remove(&self, s: &ShardRef) -> Result<(), ShardError> {
            self.0.remove(s).await
        }
        async fn entry(&self, s: &ShardRef, k: &str) -> Result<Option<Entry>, ShardError> {
            self.0.entry(s, k).await
        }
        async fn list(
            &self,
            s: &ShardRef,
            q: &skys3_index::ListQuery,
        ) -> Result<skys3_index::ListPage, ShardError> {
            self.0.list(s, q).await
        }
        async fn upload(
            &self,
            s: &ShardRef,
            k: &str,
            u: EpochSeq,
            a: u16,
            l: usize,
        ) -> Result<Option<crate::UploadParts>, ShardError> {
            self.0.upload(s, k, u, a, l).await
        }
        async fn uploads(
            &self,
            s: &ShardRef,
            p: &str,
            a: Option<(String, Option<EpochSeq>)>,
            l: usize,
        ) -> Result<Vec<(String, EpochSeq, skys3_index::Upload)>, ShardError> {
            self.0.uploads(s, p, a, l).await
        }
        async fn parts(
            &self,
            s: &ShardRef,
            u: EpochSeq,
            a: u16,
            l: usize,
        ) -> Result<Vec<(u16, skys3_index::Part)>, ShardError> {
            self.0.parts(s, u, a, l).await
        }
        async fn payload(&self, s: &ShardRef, p: EpochSeq) -> Result<Bytes, ShardError> {
            self.0.payload(s, p).await
        }
        async fn plan(&self, s: &ShardRef, k: &str) -> Result<skys3_shard::ReadPlan, ShardError> {
            self.0.plan(s, k).await
        }
        async fn register(
            &self,
            s: &ShardRef,
            h: &skys3_types::NodeId,
            k: &str,
            v: EpochSeq,
            l: Vec<ExtentRef>,
        ) -> Result<Option<skys3_shard::Registered>, ShardError> {
            self.0.register(s, h, k, v, l).await
        }
        async fn renew(
            &self,
            s: &ShardRef,
            h: &skys3_types::NodeId,
            r: u64,
        ) -> Result<bool, ShardError> {
            self.0.renew(s, h, r).await
        }
        async fn release(
            &self,
            s: &ShardRef,
            h: &skys3_types::NodeId,
            r: u64,
        ) -> Result<(), ShardError> {
            self.0.release(s, h, r).await
        }
        async fn fetch(
            &self,
            s: &ShardRef,
            h: &skys3_types::NodeId,
            r: u64,
            p: EpochSeq,
        ) -> Result<Bytes, ShardError> {
            self.0.fetch(s, h, r, p).await
        }
        async fn append_extent(&self, s: &ShardRef, e: Extent) -> Result<ExtentRef, ShardError> {
            self.0.append_extent(s, e).await
        }
        async fn announce(
            &self,
            s: &ShardRef,
            b: skys3_shard::StreamedBody,
        ) -> Result<(), ShardError> {
            self.0.announce(s, b).await
        }
        async fn flushed(
            &self,
            s: &ShardRef,
            k: &str,
            v: EpochSeq,
            w: std::time::Duration,
        ) -> Result<skys3_shard::FlushState, ShardError> {
            self.0.flushed(s, k, v, w).await
        }
        async fn write(
            &self,
            s: &ShardRef,
            b: RecordBody,
            c: Precondition,
        ) -> Result<Result<EpochSeq, crate::ConditionFailed>, ShardError> {
            if let Some(gate) = &self.1 {
                drop(gate.acquire().await.unwrap());
            }
            self.0.write(s, b, c).await
        }
    }

    /// What the destination holds now, and the time on its clocks.
    const NOW: Duration = Duration::from_secs(1_800_000_000);

    #[tokio::test]
    async fn a_commit_that_stalls_past_its_apply_by_time_never_applies() {
        let (shards, document) = opened().await;
        let wall = Arc::new(ManualWallClock::new(NOW));
        let shards = shards.with_wall_clock(wall.clone());
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let stalled = OneByOne(shards.clone(), Some(Arc::clone(&gate)));
        let commits = PeerCommits::new(stalled, lookup(&document), &settings(Some(SOURCE)))
            .with_wall_clock(wall.clone());
        let apply_by = |by: Duration| u64::try_from((NOW + by).as_millis()).unwrap();
        let staged = stage(&shards, &document, "k", b"0123").await;
        let mut commit = put_commit(1, "k", Expected::Absent, 4);
        commit.apply_by_ms = Some(apply_by(Duration::from_secs(30)));

        // The shard stalls past the connection's idle timeout and the
        // commit window: the write reaches it too late, and is refused
        // where it would have been sequenced.
        let applying = tokio::spawn({
            let (commits, commit, staged) = (commits.clone(), commit.clone(), staged.clone());
            async move { commits.apply(&commit, Some(&staged)).await }
        });
        tokio::task::yield_now().await;
        wall.advance(Duration::from_secs(60));
        gate.add_permits(1);
        let outcome = applying.await.unwrap();
        assert_failed(&outcome, ApplyError::Unavailable);
        assert!(
            shards
                .entry(&shard_of(&document, "k"), "k")
                .await
                .unwrap()
                .is_none()
        );

        // A COMMIT that arrives after its apply-by time is refused at once.
        assert_failed(
            &commits.apply(&commit, Some(&staged)).await,
            ApplyError::Unavailable,
        );

        // Sent again with a later apply-by time, it applies in time.
        commit.apply_by_ms = Some(apply_by(Duration::from_secs(90)));
        gate.add_permits(1);
        let outcome = commits.apply(&commit, Some(&staged)).await;
        assert!(matches!(outcome, Outcome::Committed { .. }), "{outcome:?}");

        // Batch items too.
        let mut late = item(2, "b", Expected::Absent, Some(b"b"));
        late.apply_by_ms = Some(apply_by(Duration::from_secs(70)));
        let batching = tokio::spawn({
            let (commits, late) = (commits.clone(), late.clone());
            async move { commits.apply_batch(&[late]).await }
        });
        tokio::task::yield_now().await;
        wall.advance(Duration::from_secs(20));
        gate.add_permits(1);
        let outcomes = batching.await.unwrap();
        assert_failed(&outcomes[0], ApplyError::Unavailable);
        assert!(
            shards
                .entry(&shard_of(&document, "b"), "b")
                .await
                .unwrap()
                .is_none()
        );
    }

    fn shard_of(document: &BucketDocument, key: &str) -> ShardRef {
        ShardRef::for_key(document, key)
    }

    #[tokio::test]
    async fn a_shard_that_does_not_batch_writes_each_item_on_its_own() {
        let (shards, document) = opened().await;
        let one_by_one = OneByOne(shards.clone(), None);
        let commits = PeerCommits::new(one_by_one, lookup(&document), &settings(Some(SOURCE)));
        let items: Vec<_> = (1..=8)
            .map(|seq| item(seq, &format!("k{seq}"), Expected::Absent, Some(b"x")))
            .collect();
        let outcomes = commits.apply_batch(&items).await;
        assert!(
            outcomes
                .iter()
                .all(|o| matches!(o, Outcome::Committed { .. }))
        );
        assert_eq!(commits.apply_batch(&items).await, outcomes);
        assert_eq!(body(&shards, &document, "k8").await, b"x");
    }

    #[test]
    fn the_identity_metadata_is_the_write_identitys_header() {
        assert_eq!(
            IDENTITY_METADATA,
            format!("x-amz-meta-{}", WriteIdentity::METADATA_KEY)
        );
    }

    #[tokio::test]
    async fn frames_are_staged_as_extents_in_the_shard_of_their_key() {
        let shards = MemoryShards::new().await;
        let document = BucketDocument {
            bucket_id: BucketId::new("b-dest").unwrap(),
            name: "archive".parse().unwrap(),
            mode: BucketMode::Local,
            shards: ShardCount::new(4).unwrap(),
            replicas: 1,
            min_write_replicas: 1,
            clean_copies: 0,
            target: None,
            created_unix_ms: 0,
            lifecycle: None,
            proposal_id: ProposalId::new("p").unwrap(),
        };
        for shard in ShardRef::all(&document) {
            shards.open(&shard, &document).await.unwrap();
        }
        let name = document.name.clone();
        let lookup = document.clone();
        let sink = PeerExtents::new(
            shards.clone(),
            Arc::new(move |wanted: &BucketName| (*wanted == lookup.name).then(|| lookup.clone())),
        );
        assert!(format!("{sink:?}").contains("PeerExtents"));
        let key = "photos/cat.jpg";
        let data = Bytes::from_static(b"staged bytes");
        let extent = sink.append(&name, key, 4096, data.clone()).await.unwrap();
        assert_eq!(extent.len, 12);
        let shard = ShardRef::for_key(&document, key);
        assert_eq!(shards.payload(&shard, extent.position).await.unwrap(), data);
        // No entry references it, so no client sees it.
        assert_eq!(shards.entry(&shard, key).await.unwrap(), None);

        let unknown = BucketName::new("unknown").unwrap();
        let error = sink.append(&unknown, key, 0, data.clone()).await;
        assert!(matches!(error, Err(SinkError::Refused(_))), "{error:?}");
        // A bucket being deleted refuses staging for good.
        shards.seal(&shard).await.unwrap();
        let error = sink.append(&name, key, 0, data.clone()).await;
        assert!(matches!(error, Err(SinkError::Refused(_))), "{error:?}");
        shards.unseal(&shard).await.unwrap();
        // An unavailable shard may stage it later.
        shards.set_unavailable(true);
        let error = sink.append(&name, key, 0, data).await;
        assert!(matches!(error, Err(SinkError::Unavailable(_))), "{error:?}");
    }
}
