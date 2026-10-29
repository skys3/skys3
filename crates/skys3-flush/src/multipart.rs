//! The flush of a completed multipart object (§7.3, §7.4): a remote
//! multipart upload with the client's part boundaries, so that the remote
//! ETag is the local one.
//!
//! An attempt opens a remote upload whose `CreateMultipartUpload` carries
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
//! target and aborted at the start of later multipart flushes. Uploads
//! whose IDs never reached the flusher (a lost `CreateMultipartUpload`
//! answer) or were lost with the node's memory are left to the remote
//! bucket's abort-incomplete-uploads lifecycle rule (§7.3).

use skys3_index::{ObjectPart, ObjectVersion, Part};
use skys3_io::Disk;
use skys3_remote::{
    AbortMultipartUpload, CompleteMultipartUpload, CompletedPart, CreateMultipartUpload,
    ObjectStore, PutObject, S3ErrorKind, UploadId, UploadPart, WritePrecondition,
};
use skys3_types::{ETag, EpochSeq, WriteIdentity};

use crate::attempt::{
    Attempt, Failure, Found, Outcome, Write, content_md5, flushed, put_request, written,
};
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
        self.target.abort_orphaned_uploads().await;
        let key = self.remote_key();
        let create = create_request(put_request(key.clone(), object, identity)?);
        let upload_id = self
            .target
            .store
            .create_multipart_upload(create)
            .await
            .map_err(Failure::Remote)?;
        let open = OpenUpload::new(self.target, key.clone(), upload_id.clone());
        let mut completed = Vec::with_capacity(parts.len());
        for (number, part) in &parts {
            match self.upload_part(&key, &upload_id, *number, part).await {
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
        let request = CompleteMultipartUpload {
            key,
            upload_id,
            parts: completed,
            precondition: WritePrecondition::None,
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
                    && let Found::Mine(info) = self.inspect(identity, version, None).await?
                {
                    return Ok(flushed(version, Some(info), true));
                }
                Err(failure)
            }
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
        key: &str,
        upload_id: &UploadId,
        number: u16,
        part: &Part,
    ) -> Result<ETag, Failure> {
        let _permit = self.target.reserve(part.size).await;
        let body = self.read(&part.payload, part.size).await?;
        let mut request = UploadPart::new(key, upload_id.clone(), u32::from(number), body);
        request.content_md5 = content_md5(&part.etag);
        self.target
            .store
            .upload_part(request)
            .await
            .map_err(Failure::Remote)
    }
}

impl<S: ObjectStore> Target<S> {
    /// Aborts up to 16 of the remote multipart uploads that flushes left
    /// open ([`Target::orphaned_uploads`]), oldest first; those whose abort
    /// fails again are kept. Every multipart flush calls it before it opens
    /// its own upload.
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
/// `PutObject` of the version would: metadata with the write identity,
/// standard headers, and tags.
fn create_request(put: PutObject) -> CreateMultipartUpload {
    CreateMultipartUpload {
        key: put.key,
        metadata: put.metadata,
        content_type: put.content_type,
        headers: put.headers,
        tags: put.tags,
    }
}
