//! The flush of a completed multipart object (§7.3, §7.4): a remote
//! multipart upload with the client's part boundaries, so that the remote
//! ETag is the local one.
//!
//! **Streamed.** An object whose upload streamed to the remote while the
//! client uploaded (see the `stream` module) completes that remote upload:
//! the attempt sends the parts the stream did not, and completes it with
//! exactly the parts the local completion kept. A stream resumed after a
//! restart or a primary change first lists the remote upload (`ListParts`):
//! a part counts as sent only if the remote holds it with the ETag the
//! stream recorded, or with the part's MD5 (§7.3).
//!
//! **After commit.** Otherwise, an attempt opens a remote upload whose `CreateMultipartUpload` carries
//! the version's write identity (the upload's `MPU_CREATE`, or the `TAGS`
//! that retagged the object), uploads each part from the local log under
//! its own part number with `Content-MD5`, and completes the upload with
//! the §7.2 precondition. A failed precondition is followed as for a
//! `PutObject`: the upload stays open meanwhile, so the next round only
//! completes it again. An upload that does not complete is aborted.
//!
//! **Lost answers.** A `CompleteMultipartUpload` whose answer is lost may
//! have been applied. The attempt then aborts the upload: if the store no
//! longer has it (`404 NoSuchUpload`), it HEADs the key, and its own write
//! identity there means the flush succeeded. Otherwise the next attempt
//! finds it by the identity after its own Complete fails its precondition.
//!
//! **Abandoned uploads.** An abort that fails, and an upload left open by
//! an attempt that was cancelled (its flusher stopped), are kept by the
//! target and aborted at the start of later multipart flushes, and by the
//! bucket's flusher after a backoff, so that no later flush is needed.
//! Uploads whose IDs never reached the flusher (a lost
//! `CreateMultipartUpload` answer) or were lost with the node's memory are
//! left to the remote bucket's abort-incomplete-uploads lifecycle rule
//! (§7.3).

use skys3_index::{ObjectPart, ObjectVersion, Part};
use skys3_io::Disk;
use skys3_log::record::{Metadata, RemoteStep, TagSet};
use skys3_remote::{
    AbortMultipartUpload, CompleteMultipartUpload, CompletedPart, CreateMultipartUpload,
    ObjectInfo, ObjectStore, PutObject, S3ErrorKind, UploadId, WritePrecondition,
};
use skys3_types::{ETag, EpochSeq, WriteIdentity};

use crate::attempt::{
    Attempt, Failure, Found, Outcome, Write, flushed, put_request, request_headers, written,
};
use crate::stream::{self, Claim, Verdict};
use crate::target::Target;

/// How many abandoned uploads one multipart flush tries to abort before it
/// starts its own.
const ORPHANS_PER_FLUSH: usize = 16;

impl<S: ObjectStore, D: Disk> Attempt<'_, S, D> {
    /// Flushes `object`, the multipart version at `version` whose parts
    /// belong to the upload opened at `upload` and are laid out as
    /// `layout`, conditioned on `expected` (§7.2).
    pub(crate) async fn put_parts(
        &self,
        version: EpochSeq,
        object: &ObjectVersion,
        upload: EpochSeq,
        layout: &[ObjectPart],
        identity: &WriteIdentity,
        expected: Option<ETag>,
    ) -> Result<Outcome, Failure> {
        let parts = self.parts(upload, layout).await?;
        // A stream carries the `MPU_CREATE` identity; a `TAGS` version
        // needs its own.
        if object.write_identity == Some(upload)
            && let Some(claim) = self.streams.claim(upload).await
        {
            return self
                .complete_stream(version, upload, &parts, claim, identity, expected)
                .await;
        }
        self.target.abort_orphaned_uploads().await;
        let key = self.remote_key();
        let put = put_request(key.clone(), object, identity)?;
        let create = CreateMultipartUpload {
            key: put.key,
            metadata: put.metadata,
            content_type: put.content_type,
            headers: put.headers,
            tags: put.tags,
        };
        let upload_id = self
            .target
            .store
            .create_multipart_upload(create)
            .await
            .map_err(Failure::Remote)?;
        let open = OpenUpload::new(self.target, key.clone(), upload_id.clone());
        let mut completed = Vec::with_capacity(parts.len());
        for (number, part) in &parts {
            match self.upload_part(&upload_id, *number, part).await {
                Ok(etag) => completed.push(CompletedPart {
                    part_number: u32::from(*number),
                    etag,
                }),
                Err(failure) => {
                    open.abort().await;
                    return Err(failure);
                }
            }
        }
        self.complete_opened(open, key, upload_id, completed, identity, version, expected)
            .await
    }

    /// Flushes `object`, a single PUT at `version` whose body is over
    /// [`MAX_BOUNDED_BYTES`](crate::MAX_BOUNDED_BYTES), to a target whose
    /// S3 writes carry apply-by times (§7.8): as a remote multipart upload
    /// of `flush_part_bytes` parts read from its extents, whose small
    /// completion carries the apply-by time, conditioned on `expected`.
    /// The remote ETag is then a multipart one, as after a streamed PUT
    /// (§7.3).
    pub(crate) async fn put_body_parts(
        &self,
        version: EpochSeq,
        object: &ObjectVersion,
        identity: &WriteIdentity,
        expected: Option<ETag>,
    ) -> Result<Outcome, Failure> {
        let part_bytes = self.target.settings.part_bytes.max(1);
        let count = object.size.div_ceil(part_bytes);
        let extents = crate::single::body_extents(&object.payload, object.size)
            .filter(|_| count <= u64::from(u16::MAX))
            .ok_or_else(|| {
                Failure::Local("the object's body cannot be uploaded in parts".into())
            })?;
        self.target.abort_orphaned_uploads().await;
        let key = self.remote_key();
        let put = put_request(key.clone(), object, identity)?;
        let create = CreateMultipartUpload {
            key: put.key,
            metadata: put.metadata,
            content_type: put.content_type,
            headers: put.headers,
            tags: put.tags,
        };
        let upload_id = self
            .target
            .store
            .create_multipart_upload(create)
            .await
            .map_err(Failure::Remote)?;
        let open = OpenUpload::new(self.target, key.clone(), upload_id.clone());
        let mut completed = Vec::new();
        for number in (1..=u16::MAX).take_while(|n| u64::from(*n) <= count) {
            let (start, end) = stream::part_range(number, part_bytes);
            let range = (start, end.min(object.size));
            let Some(span) = stream::span(&extents, range) else {
                open.abort().await;
                return Err(Failure::Local(format!(
                    "the object's extents do not hold bytes {start} to {}",
                    range.1
                )));
            };
            let part = stream::Piece {
                number,
                range,
                span: &span,
            };
            match stream::upload_span(self.shard, self.target, self.key, &upload_id, part, None)
                .await
            {
                Ok(etag) => completed.push(CompletedPart {
                    part_number: u32::from(number),
                    etag,
                }),
                Err(failure) => {
                    open.abort().await;
                    return Err(failure);
                }
            }
        }
        self.complete_opened(open, key, upload_id, completed, identity, version, expected)
            .await
    }

    /// Completes the remote upload `upload_id` of `key` that this attempt
    /// opened, `open`, with `completed`, as the version at `version`,
    /// conditioned on `expected` (§7.2): a failed precondition is followed
    /// as for a `PutObject`, and an upload that does not complete is
    /// aborted.
    #[expect(clippy::too_many_arguments, reason = "one completion's parts")]
    async fn complete_opened(
        &self,
        open: OpenUpload<'_, S>,
        key: String,
        upload_id: UploadId,
        completed: Vec<CompletedPart>,
        identity: &WriteIdentity,
        version: EpochSeq,
        expected: Option<ETag>,
    ) -> Result<Outcome, Failure> {
        let request = CompleteMultipartUpload {
            key,
            upload_id,
            parts: completed,
            precondition: WritePrecondition::None,
            apply_by_ms: None,
        };
        match self
            .conditional(Write::Complete(&request), identity, version, expected)
            .await
        {
            Ok(Ok(output)) => {
                open.completed();
                Ok(written(version, output))
            }
            Ok(Err(outcome)) => {
                open.abort().await;
                Ok(outcome)
            }
            Err(failure) => {
                // A Complete whose answer was lost may have been applied,
                // which consumed the upload.
                if open.abort().await == Abort::Gone
                    && let Found::Mine(mut info) = self.inspect(identity, version, None).await?
                {
                    self.seeded_etag(&mut info).await;
                    return Ok(flushed(version, Some(info), true));
                }
                Err(failure)
            }
        }
    }

    /// Completes the stream of the upload opened at `upload`, which the
    /// attempt claimed, as the multipart version at `version` made of
    /// `parts`, conditioned on `expected` (§7.2, §7.3). It sends each part
    /// the remote does not hold from the `MPU_PART` the version kept, and
    /// lists exactly those parts.
    async fn complete_stream(
        &self,
        version: EpochSeq,
        upload: EpochSeq,
        parts: &[(u16, Part)],
        mut claim: Claim,
        identity: &WriteIdentity,
        expected: Option<ETag>,
    ) -> Result<Outcome, Failure> {
        let streams = self.streams;
        if let Err(failure) = self.reconcile(upload, &mut claim).await {
            return self.claim_failed(version, upload, failure, identity).await;
        }
        if let Some(at_completion) = &claim.at_completion {
            let total: u64 = parts.iter().map(|(_, part)| part.size).sum();
            let streamed: u64 = parts
                .iter()
                .filter(|(number, part)| at_completion.get(number) == Some(&part.position))
                .map(|(_, part)| part.size)
                .sum();
            if total > 0 {
                #[allow(clippy::cast_precision_loss, reason = "a ratio")]
                let overlap = streamed as f64 / total as f64;
                self.target.counters.streaming_overlap.observe(overlap);
            }
        }
        let mut completed = Vec::with_capacity(parts.len());
        for (number, part) in parts {
            let etag = match claim.flushed.get(number) {
                Some((position, etag)) if *position == part.position => etag.clone(),
                _ => {
                    // The remote may hold the part already: the old
                    // primary's send whose record was lost.
                    let listed = claim.listed.as_ref().and_then(|listed| listed.get(number));
                    let sent = stream::upload_part(
                        self.shard,
                        self.target,
                        self.key,
                        &claim.id,
                        *number,
                        part,
                        listed,
                    )
                    .await;
                    match sent {
                        Ok(etag) => {
                            streams.sent(upload, *number, part.position, etag.clone());
                            let step = RemoteStep::Part {
                                number: *number,
                                position: part.position,
                                remote_etag: etag.clone(),
                            };
                            drop(streams.record(self.shard, self.key, upload, &claim.id, step));
                            etag
                        }
                        Err(failure) => {
                            return self.claim_failed(version, upload, failure, identity).await;
                        }
                    }
                }
            };
            completed.push(CompletedPart {
                part_number: u32::from(*number),
                etag,
            });
        }
        self.complete_claimed(version, upload, &claim.id, completed, identity, expected)
            .await
    }

    /// Completes the remote upload `id` of the stream of `upload`, which
    /// the attempt claimed, with `parts`, as the version at `version`,
    /// conditioned on `expected` (§7.2), and ends the claim.
    pub(crate) async fn complete_claimed(
        &self,
        version: EpochSeq,
        upload: EpochSeq,
        id: &UploadId,
        parts: Vec<CompletedPart>,
        identity: &WriteIdentity,
        expected: Option<ETag>,
    ) -> Result<Outcome, Failure> {
        let streams = self.streams;
        let mut parts = parts;
        if hooks::large_object_bug() == LargeObjectBug::CompletesWithoutLastPart && parts.len() > 1
        {
            parts.pop();
        }
        let request = CompleteMultipartUpload {
            key: self.remote_key(),
            upload_id: id.clone(),
            parts,
            precondition: WritePrecondition::None,
            apply_by_ms: None,
        };
        match self
            .conditional(Write::Complete(&request), identity, version, expected)
            .await
        {
            Ok(Ok(output)) => {
                streams.release(upload, Verdict::Completed);
                let ended = RemoteStep::Ended;
                drop(streams.record(self.shard, self.key, upload, id, ended));
                Ok(written(version, output))
            }
            // The remote holds this version from an earlier flush, or
            // another writer's object: this upload never completes.
            Ok(Err(outcome)) => {
                streams.release(upload, Verdict::Abort);
                Ok(outcome)
            }
            Err(failure) => self.claim_failed(version, upload, failure, identity).await,
        }
    }

    /// Lists the remote upload of a claimed stream that does not know what
    /// the remote holds, because it was resumed from the index or the
    /// remote refused a part it listed (§7.3), and keeps in `claim` only
    /// the parts the remote holds as recorded.
    pub(crate) async fn reconcile(
        &self,
        upload: EpochSeq,
        claim: &mut Claim,
    ) -> Result<(), Failure> {
        if claim.listed.is_none() {
            let remote = stream::list_parts(self.target, self.key, &claim.id).await?;
            self.streams.reconciled(upload, remote.clone());
            claim.reconcile(remote);
        }
        Ok(())
    }

    /// Ends the claim on the stream of `upload` after `failure`, and
    /// returns the attempt's outcome. A remote upload that is gone may have
    /// been consumed by an earlier Complete whose answer was lost: if the
    /// remote holds the version's identity, the version is flushed (§7.2).
    pub(crate) async fn claim_failed(
        &self,
        version: EpochSeq,
        upload: EpochSeq,
        failure: Failure,
        identity: &WriteIdentity,
    ) -> Result<Outcome, Failure> {
        let verdict = verdict_after(&failure);
        self.streams.release(upload, verdict);
        if verdict == Verdict::Abort
            && let Found::Mine(mut info) = self.inspect(identity, version, None).await?
        {
            self.seeded_etag(&mut info).await;
            return Ok(flushed(version, Some(info), true));
        }
        Err(failure)
    }

    /// The seeded bug [`LargeObjectBug::RecordsLocalEtagAfterLostComplete`]:
    /// takes the key's local ETag for the remote one a HEAD found after a
    /// Complete. Does nothing unless the bug is seeded.
    pub(crate) async fn seeded_etag(&self, info: &mut ObjectInfo) {
        if hooks::large_object_bug() == LargeObjectBug::RecordsLocalEtagAfterLostComplete
            && let Ok(Some(entry)) = self.shard.entry(self.key).await
            && let Some(object) = entry.object
        {
            info.etag = object.local_etag;
        }
    }

    /// The parts of the upload opened at `upload`, as the index holds them,
    /// checked against the object's layout. A newer version of the key
    /// removes them, so they may be gone.
    async fn parts(
        &self,
        upload: EpochSeq,
        layout: &[ObjectPart],
    ) -> Result<Vec<(u16, Part)>, Failure> {
        let parts = self.shard.parts(upload, 0, layout.len()).await?;
        let complete = parts.len() == layout.len()
            && parts.iter().zip(layout).all(|((number, part), listed)| {
                *number == listed.number && part.size == listed.size
            });
        if !complete {
            return Err(Failure::Local(format!(
                "the parts of upload {upload} are not in the index as the object lists them"
            )));
        }
        Ok(parts)
    }

    /// Uploads part `number` from the local log, within the target's
    /// in-flight budget, and returns the remote part's ETag.
    async fn upload_part(
        &self,
        upload_id: &UploadId,
        number: u16,
        part: &Part,
    ) -> Result<ETag, Failure> {
        stream::upload_part(
            self.shard,
            self.target,
            self.key,
            upload_id,
            number,
            part,
            None,
        )
        .await
    }
}

/// What a failed streamed completion leaves of its stream: a remote upload
/// that is gone is aborted (which records its end); one that refused a
/// part the completion listed, `400 InvalidPart`, is listed again before
/// the retry, since the remote does not hold what the stream recorded (a
/// late send of an old primary replaced the part); anything else is kept
/// for the retry.
pub(crate) fn verdict_after(failure: &Failure) -> Verdict {
    match failure {
        Failure::Remote(error) => match error.kind() {
            S3ErrorKind::NoSuchUpload => Verdict::Abort,
            S3ErrorKind::InvalidPart => Verdict::Relist,
            _ => Verdict::Keep,
        },
        _ => Verdict::Keep,
    }
}

impl<S: ObjectStore> Target<S> {
    /// Aborts up to 16 of the remote multipart uploads that flushes left
    /// open ([`Target::orphaned_uploads`]), oldest first; those whose abort
    /// fails again are kept. Every multipart flush calls it before it opens
    /// its own upload, and the bucket's flusher after a backoff.
    pub async fn abort_orphaned_uploads(&self) {
        for _ in 0..self.orphaned_uploads().min(ORPHANS_PER_FLUSH) {
            let Some((key, upload_id)) = self.take_orphan() else {
                return;
            };
            OpenUpload::new(self, key, upload_id).abort().await;
        }
    }
}

/// What aborting a remote upload found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Abort {
    /// The upload is discarded.
    Done,
    /// The store has no such upload: it was completed or aborted already.
    Gone,
    /// The abort failed; the target keeps the upload to abort later.
    Failed,
}

/// A remote upload that is open: it is completed or aborted. One dropped
/// while open, because its attempt was cancelled or its abort failed, is
/// handed to the target to abort later.
struct OpenUpload<'a, S> {
    target: &'a Target<S>,
    key: String,
    upload_id: Option<UploadId>,
}

impl<'a, S: ObjectStore> OpenUpload<'a, S> {
    fn new(target: &'a Target<S>, key: String, upload_id: UploadId) -> Self {
        Self {
            target,
            key,
            upload_id: Some(upload_id),
        }
    }

    /// The upload was completed.
    fn completed(mut self) {
        self.upload_id = None;
    }

    /// Aborts the upload.
    async fn abort(mut self) -> Abort {
        let Some(upload_id) = self.upload_id.clone() else {
            return Abort::Gone;
        };
        let request = AbortMultipartUpload {
            key: self.key.clone(),
            upload_id,
        };
        let abort = match self.target.store.abort_multipart_upload(request).await {
            Ok(()) => Abort::Done,
            Err(error) if error.kind() == S3ErrorKind::NoSuchUpload => Abort::Gone,
            Err(error) => {
                tracing::debug!(key = self.key, %error, "a remote upload was not aborted");
                return Abort::Failed;
            }
        };
        self.upload_id = None;
        abort
    }
}

impl<S> Drop for OpenUpload<'_, S> {
    fn drop(&mut self) {
        if let Some(upload_id) = self.upload_id.take() {
            self.target.orphan(std::mem::take(&mut self.key), upload_id);
        }
    }
}

/// The `CreateMultipartUpload` that gives the completed object what a
/// `PutObject` of it would: `metadata` split into user metadata,
/// `Content-Type`, and the other standard headers, `tags`, and the write
/// identity.
pub(crate) fn create_request(
    key: String,
    metadata: &Metadata,
    tags: &TagSet,
    identity: &WriteIdentity,
) -> Result<CreateMultipartUpload, Failure> {
    let put: PutObject = request_headers(key, metadata, tags, identity)?;
    Ok(CreateMultipartUpload {
        key: put.key,
        metadata: put.metadata,
        content_type: put.content_type,
        headers: put.headers,
        tags: put.tags,
    })
}

/// A bug seeded into the large-object flushes of every flusher on this
/// thread, for the simulation that shows its audits catch them (the
/// `test-util` feature exports [`seed_large_object_bug`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum LargeObjectBug {
    /// No bug.
    #[default]
    None,
    /// A streamed completion leaves the last of its parts out of the
    /// `CompleteMultipartUpload`, so that the remote assembles a truncated
    /// object.
    CompletesWithoutLastPart,
    /// A completion whose Complete answer was lost, and that a HEAD then
    /// finds applied, records the key's local ETag as the version's remote
    /// one instead of the ETag the HEAD found: wrong for a streamed single
    /// `PUT`, whose remote ETag is a multipart one.
    RecordsLocalEtagAfterLostComplete,
    /// A stream whose remote upload must be aborted gives up after one
    /// failed `AbortMultipartUpload`, and records the upload as ended
    /// anyway.
    GivesUpFailedAborts,
}

pub(crate) use hooks::large_object_bug;
#[cfg(feature = "test-util")]
pub use hooks::seed_large_object_bug;

mod hooks {
    use std::cell::Cell;

    use super::LargeObjectBug;

    thread_local! {
        static BUG: Cell<LargeObjectBug> = const { Cell::new(LargeObjectBug::None) };
    }

    /// Seeds `bug` into the large-object flushes of every flusher this
    /// thread runs. A deterministic simulation runs every node on its
    /// test's thread, so other tests run the real code.
    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    pub fn seed_large_object_bug(bug: LargeObjectBug) {
        BUG.with(|seeded| seeded.set(bug));
    }

    /// The bug seeded on this thread.
    pub(crate) fn large_object_bug() -> LargeObjectBug {
        BUG.with(Cell::get)
    }
}
