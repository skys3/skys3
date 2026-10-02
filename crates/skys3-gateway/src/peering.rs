//! The gateway's part in native replication from peer clusters (design
//! §7.8): relaying staged frames to the shard primaries, and publishing
//! what a `COMMIT` names in the shard of its key.

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use skys3_config::BucketsConfig;
use skys3_index::Entry;
use skys3_log::RecordBody;
use skys3_log::record::{self, Delete, Extent, ExtentRef, IDENTITY_METADATA, MAX_METADATA_LEN};
use skys3_peer::{
    ApplyError, Commit, CommitSink, ExtentSink, Outcome, Put, PutData, SinkError, StagedObject,
    Write,
};
use skys3_types::{BucketDocument, BucketName, ClusterId};

use crate::buckets::GatewayConfig;
use crate::conditions::{PeerCondition, Precondition, current_identity};
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
#[derive(Clone)]
pub struct PeerCommits<S> {
    shards: S,
    buckets: BucketLookup,
    settings: BucketsConfig,
    cluster: ClusterId,
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
        }
    }

    /// Applies `commit`; an early answer is the `Err`.
    async fn publish(
        &self,
        commit: &Commit,
        staged: Option<&StagedObject>,
    ) -> Result<Outcome, Outcome> {
        let Some(document) = (self.buckets)(&commit.bucket) else {
            return Err(refused(format!("there is no bucket {}", commit.bucket)));
        };
        let source = &commit.identity.cluster;
        if self.settings.get(&commit.bucket).peer_source.as_ref() != Some(source) {
            return Err(refused(format!(
                "bucket {} does not receive from cluster {source}",
                commit.bucket
            )));
        }
        let shard = ShardRef::for_key(&document, &commit.key);
        let condition = PeerCondition {
            identity: commit.identity.clone(),
            expected: commit.precondition.clone(),
            cluster: self.cluster.clone(),
            shard: shard.clone(),
        };
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
        if written.await.map_err(failed)?.is_ok() {
            return Ok(Outcome::Committed { etag });
        }
        // The condition failed when the shard sequenced the write: another
        // copy of the commit applied it, or the key changed since it was
        // read.
        let entry = self.entry(&shard, &commit.key).await?;
        match self.unapplied(commit, &condition, entry.as_ref()) {
            Err(outcome) => Ok(outcome),
            Ok(()) => Err(Outcome::Failed {
                error: ApplyError::Unavailable,
                reason: "the key changed while the commit was applied".to_owned(),
            }),
        }
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
    use skys3_types::{BucketId, BucketMode, ETag, ProposalId, ShardCount, WriteIdentity};

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
