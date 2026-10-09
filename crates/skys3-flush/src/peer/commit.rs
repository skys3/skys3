//! One flush of one key to a native target: the `COMMIT` of the key's
//! latest version under the §7.2 precondition, and what its `APPLIED`
//! decides.

use skys3_config::ConflictPolicy;
use skys3_index::{Entry, ObjectVersion, Payload};
use skys3_io::Disk;
use skys3_log::record::IDENTITY_METADATA;
use skys3_peer::{
    Begin, Commit, Outcome as Applied, Precondition, Put, PutData, Write, skips_staging,
};
use skys3_remote::ObjectStore;
use skys3_types::{ETag, EpochSeq, WriteIdentity};

use super::batch::Unbatched;
use super::hooks::peer_bug;
use super::upload::{Layout, Staged};
use super::{MAX_BOUNDED_BYTES, Native, PIECE, PeerBug};
use crate::attempt::{Attempt, Conflict, Failure, MAX_ROUNDS, Outcome, Remote, read_payload};

/// What a flush sends: a delete, or a version and where its bytes are.
enum Sending<'v> {
    Delete,
    Put(&'v ObjectVersion),
}

impl<S: ObjectStore, D: Disk> Attempt<'_, S, D> {
    /// Flushes the key's latest version, `entry`'s, to the native target
    /// `native` (§7.8). `known` is what an earlier flush or the entry says
    /// the destination holds: its ETag, and the write identity its version
    /// carries. `passed` says whether the import has passed the key.
    pub(crate) async fn native(
        &self,
        native: &Native,
        entry: &Entry,
        known: (Option<ETag>, Option<String>),
        passed: bool,
    ) -> Result<Outcome, Failure> {
        let version = entry.version;
        let (sending, identity) = match &entry.object {
            Some(object) => {
                let written = object.write_identity.unwrap_or(version);
                (
                    Sending::Put(object),
                    self.target.identity(self.shard.shard(), written),
                )
            }
            None => (
                Sending::Delete,
                self.target.identity(self.shard.shard(), version),
            ),
        };
        let mut precondition = match (self.resolution, known) {
            (Some(ConflictPolicy::Overwrite), _) => Precondition::Unconditional,
            (_, (None, _)) => Precondition::Absent,
            // An identity the entry does not know, as for an imported key,
            // is learned from the first failed precondition.
            (_, (Some(_), identity)) => identity
                .and_then(|text| text.parse::<WriteIdentity>().ok())
                .map_or(Precondition::Absent, Precondition::Matches),
        };
        for _ in 0..MAX_ROUNDS {
            let commit = Commit {
                identity: identity.clone(),
                bucket: native.bucket.clone(),
                key: self.remote_key(),
                precondition: precondition.clone(),
                write: Write::Delete,
                apply_by_ms: None,
            };
            let applied = match &sending {
                Sending::Delete => self.commit_small(native, commit).await?,
                Sending::Put(object) => self.commit_put(native, commit, object).await?,
            };
            let current = match applied {
                Applied::Committed { etag } => {
                    let etag = match &sending {
                        Sending::Put(object) => Some(etag.unwrap_or(object.local_etag.clone())),
                        Sending::Delete => None,
                    };
                    return Ok(self.committed(version, etag, &sending, &identity, passed));
                }
                Applied::PreconditionFailed { current } => current,
                Applied::Failed { error, reason } => {
                    return Err(Failure::Peer(format!("{error:?}: {reason}")));
                }
            };
            precondition = match current {
                // The destination answers a commit it applied already with
                // its stored result; a precondition that names the version's
                // own identity means the same.
                Some(current) if current == identity => {
                    let etag = match &sending {
                        Sending::Put(object) => Some(object.local_etag.clone()),
                        Sending::Delete => None,
                    };
                    return Ok(self.committed(version, etag, &sending, &identity, passed));
                }
                // The version the flush conditioned on is gone, perhaps
                // deleted by an earlier flush whose answer was lost.
                None => Precondition::Absent,
                // An earlier write of this shard, which the version
                // supersedes; or, before the import passed the key, a write
                // the local version is newer than (§9.1).
                Some(current) if self.wrote_before(&current, version) || !passed => {
                    Precondition::Matches(current)
                }
                Some(current) => {
                    return Ok(Outcome::Conflict(Conflict {
                        seq: version.seq,
                        remote_etag: None,
                        remote_identity: Some(current.to_string()),
                    }));
                }
            };
        }
        Err(Failure::Changing)
    }

    /// The outcome of a version the destination holds: recorded with the
    /// identity its version carries, so that the key's next version is
    /// conditioned on it. A delete before the import passed its key keeps
    /// its tombstone (§4.2).
    fn committed(
        &self,
        version: EpochSeq,
        etag: Option<ETag>,
        sending: &Sending<'_>,
        identity: &WriteIdentity,
        passed: bool,
    ) -> Outcome {
        let (version_id, record) = match sending {
            Sending::Put(_) => (Some(identity.to_string()), true),
            Sending::Delete => (None, passed),
        };
        Outcome::Flushed {
            version,
            remote: Remote {
                seq: version.seq,
                etag,
                version_id,
            },
            record,
        }
    }

    /// Sends `commit`, its write filled in from `object`: inline in a
    /// batch if it fits one frame and the session accepts batches,
    /// otherwise staged.
    async fn commit_put(
        &self,
        native: &Native,
        mut commit: Commit,
        object: &ObjectVersion,
    ) -> Result<Applied, Failure> {
        let mut metadata = object.metadata.clone();
        // A version that came from a peer carries its identity; this
        // commit's replaces it at the destination.
        metadata.remove(IDENTITY_METADATA);
        let mut put = Put {
            size: object.size,
            etag: object.local_etag.clone(),
            last_modified_ms: object.last_modified_ms,
            metadata,
            tags: object.tags.clone(),
            checksums: object.checksums.clone(),
            data: PutData::Staged { piece: PIECE },
        };
        // A batch's bytes are bounded by its apply-by time (§7.8).
        let small = skips_staging(object.size, native.frame_bytes.min(MAX_BOUNDED_BYTES))
            && !matches!(object.payload, Payload::Parts { .. });
        if small && native.batcher.supported() {
            let _reserved = self.target.reserve(object.size).await;
            put.data =
                PutData::Inline(read_payload(self.shard, &object.payload, object.size).await?);
            commit.write = Write::Put(put.clone());
            match self.batched(native, commit.clone()).await {
                Err(Unbatched::NotSupported) => put.data = PutData::Staged { piece: PIECE },
                Err(Unbatched::Lost(error)) => return Err(Failure::link(error)),
                Ok(applied) => return Ok(applied),
            }
        }
        commit.write = Write::Put(put);
        let layout = Layout::of_version(self.shard, &object.payload, object.size).await?;
        let staged = Staged {
            shard: self.shard,
            target: self.target,
            native,
            begin: Begin {
                identity: commit.identity.clone(),
                bucket: commit.bucket.clone(),
                key: commit.key.clone(),
            },
        };
        staged.commit(&commit, &layout, &|| self.sent()).await
    }

    /// Sends `commit`, a delete, in a batch if the session accepts them,
    /// otherwise alone on a stream of its own.
    async fn commit_small(&self, native: &Native, commit: Commit) -> Result<Applied, Failure> {
        if native.batcher.supported() {
            match self.batched(native, commit.clone()).await {
                Err(Unbatched::NotSupported) => {}
                Err(Unbatched::Lost(error)) => return Err(Failure::link(error)),
                Ok(applied) => return Ok(applied),
            }
        }
        let mut stream = super::link::Stream::open(&*native.link, native.timeout)
            .await
            .map_err(Failure::link)?;
        stream
            .send(&skys3_peer::Message::Commit(native.stamped(&commit)))
            .await
            .map_err(Failure::link)?;
        stream.finish().map_err(Failure::link)?;
        self.sent();
        if peer_bug() == PeerBug::FlushedOnCommit {
            return Ok(super::batch::assumed_committed(&commit));
        }
        match super::upload::applied(&mut stream, &commit.identity)
            .await
            .map_err(Failure::link)?
        {
            super::upload::Ended::Applied(applied) => Ok(applied),
            super::upload::Ended::Aborted(reason, detail) => {
                Err(Failure::Peer(format!("{reason:?}: {detail}")))
            }
        }
    }

    /// Sends `commit` in the target's next batch.
    async fn batched(&self, native: &Native, commit: Commit) -> Result<Applied, Unbatched> {
        let queued = native.batcher.submit(commit)?;
        self.sent();
        queued.applied().await
    }
}
