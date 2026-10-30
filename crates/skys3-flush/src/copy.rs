//! The flush of a copy as a remote server-side `CopyObject` (§7.2, §11).
//!
//! A copy commits as a `PUT` that records its source: the bucket, the key,
//! the version copied, and the source's `remote_etag` if the source was
//! clean, so that the remote held exactly the version copied. When the
//! source's bucket flushes to the same remote bucket as the copy's
//! ([`CopySources`](crate::CopySources)) and the probe found the target
//! supports everything a copy sends ([`CopySupport::is_usable`]), the copy
//! is sent as a `CopyObject` instead of an upload of its bytes:
//!
//! - `x-amz-metadata-directive: REPLACE` with the copy's own metadata,
//!   standard headers, and write identity, and `x-amz-tagging-directive:
//!   REPLACE` with its tags, so the remote object is the copy's version
//!   and the 412 rule of §7.2 recognizes it as the copy's own write;
//! - `x-amz-copy-source-if-match: <source remote_etag>`, so the bytes
//!   copied are the version copied;
//! - the destination precondition of any flush: `If-Match` on the remote
//!   ETag the key is known to have, or `If-None-Match: *`, and none when
//!   an `overwrite` resolution sends the version unconditionally.
//!
//! A `412`, or a `404` for a source or destination that is gone, does not
//! say which precondition failed. The flush HEADs the destination as after
//! any failed precondition: its own write identity means an earlier copy
//! landed and only its answer was lost. If the destination is still what
//! the copy was conditioned on, its precondition held, so the source's
//! failed: the source was overwritten or deleted at the remote since the
//! copy committed. That is no conflict; the copy is then sent as a regular
//! upload of its local bytes, with the same precondition and identity. A
//! target that refuses the copy itself (`501`, or another `400`) gets a
//! regular upload too. Anything else at the destination follows the §7.2
//! rule as for a `PutObject`.
//!
//! The local copy refuses sources over 5 GiB (§11), S3's limit for
//! `CopyObject`, so every copy fits one request; a larger version would be
//! sent as an upload.

use skys3_config::ConflictPolicy;
use skys3_index::ObjectVersion;
use skys3_io::Disk;
use skys3_remote::{
    CopyObject, MetadataDirective, ObjectStore, S3Error, S3ErrorKind, TaggingDirective,
    WritePrecondition,
};
use skys3_types::{ETag, EpochSeq, WriteIdentity};

use crate::attempt::{
    Attempt, Failure, Found, MAX_ROUNDS, Outcome, conflict, failed_precondition, flushed,
    request_headers, written,
};

/// The largest object S3 copies with one `CopyObject`: 5 GiB.
pub(crate) const MAX_COPY_BYTES: u64 = 5 << 30;

/// Where a copy's source is at the remote.
pub(crate) struct Source {
    /// The source's remote key.
    key: String,
    /// The ETag the remote held for the version copied.
    etag: ETag,
}

/// How a server-side copy ended.
pub(crate) enum Copied {
    /// The flush is settled: copied, recognized as copied, or in conflict.
    Done(Outcome),
    /// The remote source changed, or the target refused the copy: send the
    /// version as a regular upload, conditioned on this remote ETag of the
    /// destination (`None`: the key is absent).
    Upload(Option<ETag>),
}

impl<S: ObjectStore, D: Disk> Attempt<'_, S, D> {
    /// The remote source of `object`, if it is a copy that can be sent as
    /// a server-side copy: of a version that was clean when copied, from a
    /// bucket in the same remote bucket, to a target that supports it.
    pub(crate) fn copy_source(&self, object: &ObjectVersion) -> Option<Source> {
        let source = object.copy_source.as_ref()?;
        let etag = source.remote_etag.clone()?;
        // A SkyS3 peer's apply-by time would bound the copy's work at the
        // destination too (§7.8): its copies are sent as objects.
        if !self.target.copy_support().is_usable()
            || object.size > MAX_COPY_BYTES
            || self.target.stamped()
        {
            return None;
        }
        let prefix = self.target.copy_sources.prefix(&source.bucket)?;
        Some(Source {
            key: format!("{prefix}{}", source.key),
            etag,
        })
    }

    /// Sends the copy `object` at `version` as a `CopyObject` of `source`
    /// conditioned on `expected`, and settles a failed precondition (see
    /// the module documentation).
    pub(crate) async fn copy(
        &self,
        version: EpochSeq,
        object: &ObjectVersion,
        source: Source,
        identity: &WriteIdentity,
        mut expected: Option<ETag>,
    ) -> Result<Copied, Failure> {
        let request = self.copy_request(object, &source, identity)?;
        // A copy is used only where the target honors its destination
        // preconditions; `overwrite` sends none.
        let protected = self.resolution != Some(ConflictPolicy::Overwrite);
        for _ in 0..MAX_ROUNDS {
            let precondition = match (&expected, protected) {
                (_, false) => WritePrecondition::None,
                (Some(etag), true) => WritePrecondition::IfMatch(etag.clone()),
                (None, true) => WritePrecondition::IfAbsent,
            };
            let request = request.clone().with_precondition(precondition);
            let error = match self.target.store.copy_object(request).await {
                Ok(output) => {
                    self.target.counters.copies.inc();
                    return Ok(Copied::Done(written(version, output)));
                }
                Err(error) if failed_precondition(&error) => error,
                Err(error) if refused(&error) => {
                    tracing::debug!(key = self.key, %error, "the target refused a copy");
                    return Ok(self.fall_back(expected));
                }
                Err(error) => return Err(Failure::Remote(error)),
            };
            if !protected {
                // Only the source had a precondition.
                return Ok(self.fall_back(expected));
            }
            let found = self.inspect(identity, version, expected.as_ref()).await?;
            let held = match &found {
                Found::Missing => expected.is_none(),
                Found::Supersede(etag) => expected.as_ref() == Some(etag),
                Found::Mine(_) | Found::Foreign(_) => false,
            };
            // A 409 wrote nothing because of a racing write: copy again.
            let raced = error.kind() == S3ErrorKind::ConditionalRequestConflict;
            if held && !raced {
                if hooks::copy_bug() == CopyBug::SourceFailureAsConflict {
                    return Ok(Copied::Done(Outcome::Conflict(conflict(version, None))));
                }
                tracing::debug!(key = self.key, %error, "the remote copy source changed");
                return Ok(self.fall_back(expected));
            }
            match found {
                Found::Mine(info) => return Ok(Copied::Done(flushed(version, Some(info), true))),
                Found::Supersede(etag) => expected = Some(etag),
                Found::Missing => expected = None,
                Found::Foreign(info) => {
                    tracing::debug!(key = self.key, %error, "a copy found a foreign object");
                    return Ok(Copied::Done(Outcome::Conflict(conflict(
                        version,
                        Some(info),
                    ))));
                }
            }
        }
        Err(Failure::Changing)
    }

    /// Gives up on the server-side copy: the version is uploaded instead,
    /// conditioned on `expected`.
    fn fall_back(&self, expected: Option<ETag>) -> Copied {
        self.target.counters.copy_fallbacks.inc();
        Copied::Upload(expected)
    }

    /// The `CopyObject` of `object` from `source`, without its
    /// destination precondition.
    fn copy_request(
        &self,
        object: &ObjectVersion,
        source: &Source,
        identity: &WriteIdentity,
    ) -> Result<CopyObject, Failure> {
        let headers = request_headers(self.remote_key(), &object.metadata, &object.tags, identity)?;
        let mut metadata = headers.metadata;
        if hooks::copy_bug() == CopyBug::WithoutIdentity {
            metadata.remove(WriteIdentity::METADATA_KEY);
        }
        Ok(CopyObject::new(source.key.clone(), headers.key)
            .with_source_if_match(source.etag.clone())
            .with_metadata_directive(MetadataDirective::Replace {
                metadata,
                content_type: headers.content_type,
                headers: headers.headers,
            })
            .with_tagging_directive(TaggingDirective::Replace(headers.tags)))
    }
}

/// Whether the target refused a copy as such, rather than its state or a
/// transient failure: `501 NotImplemented`, or a `400` such as
/// `InvalidRequest`. Nothing was copied, and an upload of the bytes may
/// succeed.
fn refused(error: &S3Error) -> bool {
    match error.kind() {
        S3ErrorKind::NotImplemented
        | S3ErrorKind::InvalidRequest
        | S3ErrorKind::InvalidArgument => true,
        S3ErrorKind::Other => matches!(error.status(), Some(400 | 501)),
        _ => false,
    }
}

/// A bug seeded into the server-side copies of every flusher on this
/// thread, for the simulation that shows its audit catches them (the
/// `test-util` feature exports [`seed_copy_bug`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum CopyBug {
    /// No bug.
    #[default]
    None,
    /// A copy replaces the metadata without the copy's write identity, so
    /// a retry after a lost answer finds an object it cannot tell from
    /// another writer's.
    WithoutIdentity,
    /// A copy whose source changed at the remote is taken for a conflict
    /// at its destination, instead of being sent as an upload.
    SourceFailureAsConflict,
}

#[cfg(feature = "test-util")]
pub use hooks::seed_copy_bug;

mod hooks {
    use std::cell::Cell;

    use super::CopyBug;

    thread_local! {
        static BUG: Cell<CopyBug> = const { Cell::new(CopyBug::None) };
    }

    /// Seeds `bug` into the server-side copies of every flusher this
    /// thread runs. A deterministic simulation runs every node on its
    /// test's thread, so other tests run the real code.
    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    pub fn seed_copy_bug(bug: CopyBug) {
        BUG.with(|seeded| seeded.set(bug));
    }

    /// The bug seeded on this thread.
    pub(super) fn copy_bug() -> CopyBug {
        BUG.with(Cell::get)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refusals_are_501_and_400() {
        for (kind, status, expected) in [
            (S3ErrorKind::NotImplemented, None, true),
            (S3ErrorKind::InvalidRequest, None, true),
            (S3ErrorKind::InvalidArgument, None, true),
            (S3ErrorKind::Other, Some(400), true),
            (S3ErrorKind::Other, Some(501), true),
            (S3ErrorKind::Other, Some(403), false),
            (S3ErrorKind::PreconditionFailed, None, false),
            (S3ErrorKind::InternalError, None, false),
        ] {
            let mut error = S3Error::new(kind, "");
            if let Some(status) = status {
                error = error.with_status(status);
            }
            assert_eq!(refused(&error), expected, "{kind:?} {status:?}");
        }
    }
}
