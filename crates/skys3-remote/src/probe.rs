//! The conditional-write capability probe (design §7.2).
//!
//! Providers differ in which write preconditions they honor. When a remote
//! target is attached, [`ConditionalProbe`] finds out, per operation the
//! flusher makes conditional (`PutObject`, `CompleteMultipartUpload`, and
//! `DeleteObject`) and per header (`If-None-Match: *`, `If-Match`), whether
//! the store:
//!
//! - **honors** it: a precondition that holds lets the write through, and
//!   one that fails is answered with `412 Precondition Failed` and nothing
//!   is written;
//! - **ignores** it: the write is applied although its precondition fails;
//! - or **rejects** it: a write carrying the header fails even though its
//!   precondition holds, with `501 NotImplemented`, a `400`, or a `412`.
//!
//! Only honored headers protect a write, so the flusher sends the others'
//! operations unconditionally and the bucket status lists them as
//! unprotected ([`ConditionalWrites::unprotected`]).
//!
//! # Server-side copies
//!
//! The flusher sends a copy of a clean source as a remote `CopyObject`
//! only if the store supports everything such a copy needs (design §7.2,
//! §11): `x-amz-metadata-directive: REPLACE`, `x-amz-tagging-directive:
//! REPLACE`, `x-amz-copy-source-if-match`, and both destination
//! preconditions. The probe classifies each the same three ways
//! ([`CopySupport`]). It writes a source with one tag and a metadata
//! mark, and copies it:
//!
//! - with each directive set to `REPLACE` in turn, the other one `COPY`,
//!   and HEADs the copy: the directive is honored if the copy has the
//!   replaced mark, or two tags (`x-amz-tagging-count`), ignored if it has
//!   the source's, and rejected if the copy fails;
//! - with `x-amz-copy-source-if-match` naming another ETag and then the
//!   source's, as a precondition;
//! - to a new key with `If-None-Match: *` and `If-Match`, as
//!   `PutObject` is probed.
//!
//! S3 sends `x-amz-tagging-count` only to callers allowed
//! `s3:GetObjectTagging`. Without that permission the tagging directive
//! reads as ignored, and copies are sent as regular uploads, which is
//! always correct.
//!
//! [`ConditionalProbe::run`] probes both. The flush service probes the
//! writes alone first ([`ConditionalProbe::run_writes`]) and then the
//! copies ([`ConditionalProbe::run_copies`]): every run fails on any
//! transient error, and the copy steps more than double the requests of a
//! run, so on a flaky store a combined run would hold flushing back.
//!
//! # Scratch keys
//!
//! The probe writes only under its scratch prefix,
//! `<owned prefix>.skys3-probe/<nonce>/`, where the owned prefix is the key
//! prefix SkyS3 owns in the remote bucket (empty for a whole bucket) and the
//! nonce is 16 hex digits, fresh for every run. The nonce keeps concurrent
//! probes of one target, from several nodes, apart. The keys under it are
//! `put-object`, `complete-multipart-upload`, `delete-object`,
//! `copy-source`, `copy-object`, and `copy-destination`.
//!
//! The probe removes everything it created before it returns, whether or
//! not it succeeded: each version it wrote and each delete marker it added
//! on a versioned bucket, the keys themselves on an unversioned one, and any
//! multipart upload it left open. A write whose response was lost may have
//! been applied: the probe finds the version it may have created with
//! `HeadObject` and deletes it. Two things it cannot find without listing,
//! which the [`ObjectStore`] trait does not offer: the delete marker a lost
//! `DeleteObject` may have added on a versioned bucket, and the upload a
//! lost `CreateMultipartUpload` may have started (the abort-incomplete-
//! uploads lifecycle rule of design §7.3 removes it). What it could not
//! remove, or may have left, is reported in [`ProbeError::leftovers`]. It
//! does not retry: a transient error fails the probe, and the caller probes
//! again.
//!
//! ```
//! use skys3_remote::probe::ConditionalProbe;
//!
//! let probe = ConditionalProbe::new("team-a/", 0x2a);
//! assert_eq!(probe.scratch_prefix(), "team-a/.skys3-probe/000000000000002a/");
//! ```

use std::collections::BTreeMap;
use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::{BuildHasher, Hasher};
use std::time::{SystemTime, UNIX_EPOCH};

use skys3_types::ETag;

use crate::model::{
    AbortMultipartUpload, CompleteMultipartUpload, CompletedPart, CopyObject,
    CreateMultipartUpload, DeleteObject, HeadObject, MetadataDirective, ObjectInfo, PutObject,
    TaggingDirective, UploadId, UploadPart, VersionId, WriteOutput, WritePrecondition,
};
use crate::{ObjectStore, S3Error, S3ErrorKind, S3Result, UserMetadata};

/// How a store treats one write-precondition header on one operation, or
/// one `CopyObject` directive.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PreconditionSupport {
    /// The precondition is evaluated: a failed one is `412` and writes
    /// nothing.
    Honored,
    /// The header is accepted and has no effect: the write is applied even
    /// though its precondition fails.
    Ignored,
    /// Writes carrying the header fail even when the precondition holds,
    /// for example with `501 NotImplemented`.
    Rejected,
}

impl PreconditionSupport {
    /// Whether the header protects the write.
    pub fn is_honored(self) -> bool {
        self == PreconditionSupport::Honored
    }

    fn as_str(self) -> &'static str {
        match self {
            PreconditionSupport::Honored => "honored",
            PreconditionSupport::Ignored => "ignored",
            PreconditionSupport::Rejected => "rejected",
        }
    }
}

impl fmt::Display for PreconditionSupport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A write operation the flusher makes conditional (design §7.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ConditionalOperation {
    /// `PutObject`, with `If-None-Match: *` or `If-Match`.
    PutObject,
    /// `CompleteMultipartUpload`, with `If-None-Match: *` or `If-Match`.
    CompleteMultipartUpload,
    /// `DeleteObject`, with `If-Match`.
    DeleteObject,
}

impl ConditionalOperation {
    /// Every operation, in the order the probe tests them.
    pub const ALL: [ConditionalOperation; 3] = [
        ConditionalOperation::PutObject,
        ConditionalOperation::CompleteMultipartUpload,
        ConditionalOperation::DeleteObject,
    ];

    /// The S3 operation name.
    pub fn as_str(self) -> &'static str {
        match self {
            ConditionalOperation::PutObject => "PutObject",
            ConditionalOperation::CompleteMultipartUpload => "CompleteMultipartUpload",
            ConditionalOperation::DeleteObject => "DeleteObject",
        }
    }
}

impl fmt::Display for ConditionalOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How a store treats the precondition headers of one operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct OperationSupport {
    /// `If-None-Match: *`, or `None` for `DeleteObject`, which does not
    /// take it.
    pub if_none_match: Option<PreconditionSupport>,
    /// `If-Match: <etag>`.
    pub if_match: PreconditionSupport,
}

impl OperationSupport {
    /// Whether every precondition header of the operation is honored, so
    /// the flusher can send it conditionally.
    pub fn is_protected(&self) -> bool {
        self.if_match.is_honored()
            && self
                .if_none_match
                .is_none_or(PreconditionSupport::is_honored)
    }
}

impl fmt::Display for OperationSupport {
    /// Writes, for example, `If-None-Match honored, If-Match ignored`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(support) = self.if_none_match {
            write!(f, "If-None-Match {support}, ")?;
        }
        write!(f, "If-Match {}", self.if_match)
    }
}

/// How a store treats what a server-side copy of a flush sends (design
/// §7.2, §11).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CopySupport {
    /// `x-amz-metadata-directive: REPLACE`: honored if the copy gets the
    /// request's metadata, ignored if it keeps the source's.
    pub metadata_directive: PreconditionSupport,
    /// `x-amz-tagging-directive: REPLACE`: honored if the copy gets the
    /// request's tags, ignored if it keeps the source's.
    pub tagging_directive: PreconditionSupport,
    /// `x-amz-copy-source-if-match`.
    pub source_if_match: PreconditionSupport,
    /// `If-None-Match: *` and `If-Match` on the destination.
    pub destination: OperationSupport,
}

impl CopySupport {
    /// Support for nothing: a store that rejects every copy.
    pub const NONE: CopySupport = CopySupport {
        metadata_directive: PreconditionSupport::Rejected,
        tagging_directive: PreconditionSupport::Rejected,
        source_if_match: PreconditionSupport::Rejected,
        destination: OperationSupport {
            if_none_match: Some(PreconditionSupport::Rejected),
            if_match: PreconditionSupport::Rejected,
        },
    };

    /// Support for everything, as on AWS S3.
    pub const FULL: CopySupport = CopySupport {
        metadata_directive: PreconditionSupport::Honored,
        tagging_directive: PreconditionSupport::Honored,
        source_if_match: PreconditionSupport::Honored,
        destination: OperationSupport {
            if_none_match: Some(PreconditionSupport::Honored),
            if_match: PreconditionSupport::Honored,
        },
    };

    /// Whether the flusher may send copies as `CopyObject`: the store
    /// honors both directives, the source precondition, and both
    /// destination preconditions. Otherwise copies are sent as regular
    /// uploads.
    pub fn is_usable(&self) -> bool {
        self.metadata_directive.is_honored()
            && self.tagging_directive.is_honored()
            && self.source_if_match.is_honored()
            && self.destination.is_protected()
    }
}

impl fmt::Display for CopySupport {
    /// Writes, for example, `x-amz-metadata-directive honored,
    /// x-amz-tagging-directive ignored, x-amz-copy-source-if-match honored,
    /// If-None-Match honored, If-Match honored`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "x-amz-metadata-directive {}, x-amz-tagging-directive {}, \
             x-amz-copy-source-if-match {}, {}",
            self.metadata_directive, self.tagging_directive, self.source_if_match, self.destination
        )
    }
}

/// What the probe found: which write preconditions a store honors, and
/// whether it can take the server-side copies of a flush.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ConditionalWrites {
    /// `PutObject`.
    pub put_object: OperationSupport,
    /// `CompleteMultipartUpload`.
    pub complete_multipart_upload: OperationSupport,
    /// `DeleteObject`.
    pub delete_object: OperationSupport,
    /// `CopyObject`, which the flusher uses only if it is usable
    /// ([`CopySupport::is_usable`]); it is never sent unprotected.
    pub copy_object: CopySupport,
}

impl ConditionalWrites {
    /// The support of one operation.
    pub fn operation(&self, operation: ConditionalOperation) -> &OperationSupport {
        match operation {
            ConditionalOperation::PutObject => &self.put_object,
            ConditionalOperation::CompleteMultipartUpload => &self.complete_multipart_upload,
            ConditionalOperation::DeleteObject => &self.delete_object,
        }
    }

    /// The operations that are not protected, which the flusher sends
    /// unconditionally and the bucket status lists (design §7.2).
    pub fn unprotected(&self) -> Vec<ConditionalOperation> {
        ConditionalOperation::ALL
            .into_iter()
            .filter(|&operation| !self.operation(operation).is_protected())
            .collect()
    }
}

impl fmt::Display for ConditionalWrites {
    /// Writes each operation and its support, separated by `; `.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for operation in ConditionalOperation::ALL {
            write!(f, "{operation}: {}; ", self.operation(operation))?;
        }
        write!(f, "CopyObject: {}", self.copy_object)
    }
}

/// A step of the probe that failed, and why.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("capability probe failed at {step}: {error}{}", Leftovers(&self.leftovers))]
pub struct ProbeError {
    /// The step that failed, such as `PutObject with If-Match with a
    /// different ETag`, or `cleanup` if only the cleanup failed.
    pub step: String,
    /// The store's error.
    pub error: S3Error,
    /// What the probe created and could not remove: keys, versions, and
    /// uploads, one per entry. An entry ending "if the delete was applied"
    /// or "if it was created" names what a request whose response was lost
    /// may have left, which the probe cannot find.
    pub leftovers: Vec<String>,
}

/// Formats a non-empty leftover list as `; left behind: a, b`.
struct Leftovers<'a>(&'a [String]);

impl fmt::Display for Leftovers<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.0.is_empty() {
            write!(f, "; left behind: {}", self.0.join(", "))?;
        }
        Ok(())
    }
}

/// The conditional-write probe of one target. See the [module
/// documentation](self).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConditionalProbe {
    scratch_prefix: String,
}

impl ConditionalProbe {
    /// The directory under the owned prefix that probes write in.
    pub const SCRATCH_DIR: &'static str = ".skys3-probe/";

    /// A probe writing under `<owned_prefix>.skys3-probe/<nonce>/`.
    /// Simulations pass a nonce derived from their seed; real attaches use
    /// [`ConditionalProbe::with_fresh_nonce`].
    pub fn new(owned_prefix: &str, nonce: u64) -> Self {
        ConditionalProbe {
            scratch_prefix: format!("{owned_prefix}{}{nonce:016x}/", Self::SCRATCH_DIR),
        }
    }

    /// A probe with a nonce that no other probe uses: drawn from the
    /// process's randomly keyed hasher and mixed with the time and the
    /// process ID.
    pub fn with_fresh_nonce(owned_prefix: &str) -> Self {
        let mut hasher = RandomState::new().build_hasher();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        hasher.write_u128(now.as_nanos());
        hasher.write_u32(std::process::id());
        Self::new(owned_prefix, hasher.finish())
    }

    /// The prefix every scratch key starts with.
    pub fn scratch_prefix(&self) -> &str {
        &self.scratch_prefix
    }

    /// Probes `store`, writes and server-side copies, and removes the
    /// scratch keys.
    ///
    /// # Errors
    ///
    /// Returns a [`ProbeError`] if a request fails in a way that says
    /// nothing about preconditions, such as a transient error or `403
    /// AccessDenied`, or if the cleanup fails. Either way, the probe has
    /// tried to remove everything it created.
    pub async fn run<S: ObjectStore>(&self, store: &S) -> Result<ConditionalWrites, ProbeError> {
        self.run_part(store, Part::All).await
    }

    /// Probes the writes of `store` alone, as [`ConditionalProbe::run`]
    /// does, and reports no server-side copy support
    /// ([`CopySupport::NONE`]). With [`ConditionalProbe::run_copies`], it
    /// lets a flusher start without waiting for the copy steps, whose
    /// requests a flaky store fails more often than the writes' alone.
    ///
    /// # Errors
    ///
    /// As [`ConditionalProbe::run`].
    pub async fn run_writes<S: ObjectStore>(
        &self,
        store: &S,
    ) -> Result<ConditionalWrites, ProbeError> {
        self.run_part(store, Part::Writes).await
    }

    /// Probes the server-side copy support of `store` alone, as
    /// [`ConditionalProbe::run`] does.
    ///
    /// # Errors
    ///
    /// As [`ConditionalProbe::run`].
    pub async fn run_copies<S: ObjectStore>(&self, store: &S) -> Result<CopySupport, ProbeError> {
        self.run_part(store, Part::Copies)
            .await
            .map(|writes| writes.copy_object)
    }

    /// Probes `part` of what [`ConditionalProbe::run`] does.
    async fn run_part<S: ObjectStore>(
        &self,
        store: &S,
        part: Part,
    ) -> Result<ConditionalWrites, ProbeError> {
        let mut run = Run {
            store,
            prefix: &self.scratch_prefix,
            created: Vec::new(),
        };
        let result = run.probe(part).await;
        let cleanup = run.clean_up().await;
        let ((step, error), leftovers) = match (result, cleanup) {
            (Ok(writes), Ok(())) => return Ok(writes),
            (Ok(_), Err((error, leftovers))) => {
                return Err(ProbeError {
                    step: "cleanup".to_owned(),
                    error,
                    leftovers,
                });
            }
            (Err(failed), Ok(())) => (failed, Vec::new()),
            (Err(failed), Err((_, leftovers))) => (failed, leftovers),
        };
        Err(ProbeError {
            step: step.to_string(),
            error,
            leftovers,
        })
    }
}

/// A step of the probe: an operation and what it tests.
#[derive(Clone, Copy, Debug)]
struct Step(&'static str, &'static str);

impl fmt::Display for Step {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.0, self.1)
    }
}

/// A failed probe step and the store's error.
type StepError = (Step, S3Error);

/// Something the probe created, or may have created, to remove at the end.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Created {
    /// A key the probe wrote, and the version or delete marker the write
    /// created, if the store reported one.
    Version(String, Option<VersionId>),
    /// A multipart upload the probe could not abort.
    Upload(String, UploadId),
    /// A key a write may have created a version of that the probe does not
    /// know, because the write's response was lost.
    Unknown(String),
    /// A key a `DeleteObject` may have added a delete marker to, because
    /// its response was lost. Finding the marker would take
    /// `ListObjectVersions`, so the probe reports it on a versioned bucket,
    /// with the error that lost the response.
    UnknownMarker(String, S3Error),
    /// A key a `CreateMultipartUpload` may have started an upload for,
    /// because its response, and so the upload ID, was lost. Finding the
    /// upload would take `ListMultipartUploads`, so the probe reports it,
    /// with the error that lost the response; the abort-incomplete-uploads
    /// lifecycle rule removes it (design §7.3).
    UnknownUpload(String, S3Error),
}

impl Created {
    /// Whether the cleanup has to find the item, which it does after
    /// removing what it knows.
    fn is_unknown(&self) -> bool {
        !matches!(self, Created::Version(..) | Created::Upload(..))
    }
}

impl fmt::Display for Created {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Created::Version(key, None) => write!(f, "{key}"),
            Created::Version(key, Some(version)) => write!(f, "{key} (version {version})"),
            Created::Upload(key, upload) => write!(f, "{key} (upload {upload})"),
            Created::Unknown(key) => write!(f, "{key} (a version the probe could not find)"),
            Created::UnknownMarker(key, _) => {
                write!(f, "{key} (a delete marker, if the delete was applied)")
            }
            Created::UnknownUpload(key, _) => {
                write!(f, "{key} (an upload, if it was created)")
            }
        }
    }
}

/// The outcome of a write whose precondition fails.
enum Failing<T> {
    /// Answered `412`: honored, if a write whose precondition holds goes
    /// through too.
    Refused,
    /// Applied despite the precondition.
    Applied(T),
    /// Refused as unsupported.
    Rejected,
}

/// An ETag no store assigns: not a hex digest, and not a multipart ETag.
fn mismatch() -> ETag {
    ETag::new("skys3-probe-mismatch").expect("a valid entity tag")
}

/// Whether `error` says the store does not support a header, rather than
/// that a precondition failed or that the request could not be served.
fn is_rejection(error: &S3Error) -> bool {
    match error.kind() {
        S3ErrorKind::NotImplemented
        | S3ErrorKind::InvalidArgument
        | S3ErrorKind::InvalidRequest => true,
        S3ErrorKind::Other => matches!(error.status(), Some(400 | 501)),
        _ => false,
    }
}

/// Classifies the result of a write whose precondition fails.
fn failing<T>(step: Step, result: S3Result<T>) -> Result<Failing<T>, StepError> {
    match result {
        Ok(output) => Ok(Failing::Applied(output)),
        Err(error) if error.kind() == S3ErrorKind::PreconditionFailed => Ok(Failing::Refused),
        Err(error) if is_rejection(&error) => Ok(Failing::Rejected),
        Err(error) => Err((step, error)),
    }
}

/// Classifies the result of a write whose precondition holds: `None` if the
/// store refused it, which rejects the header.
fn passing<T>(step: Step, result: S3Result<T>) -> Result<Option<T>, StepError> {
    match result {
        Ok(output) => Ok(Some(output)),
        Err(error)
            if is_rejection(&error)
                || matches!(
                    error.kind(),
                    S3ErrorKind::PreconditionFailed | S3ErrorKind::NoSuchKey
                ) =>
        {
            Ok(None)
        }
        Err(error) => Err((step, error)),
    }
}

/// An operation that creates objects, which takes both headers.
#[derive(Clone, Copy, Debug)]
enum Creating {
    Put,
    Complete,
    /// `CopyObject` of the copy source ([`COPY_SOURCE`]).
    Copy,
}

impl Creating {
    fn name(self) -> &'static str {
        match self {
            Creating::Put => "PutObject",
            Creating::Complete => "CompleteMultipartUpload",
            Creating::Copy => "CopyObject",
        }
    }

    fn key(self) -> &'static str {
        match self {
            Creating::Put => "put-object",
            Creating::Complete => "complete-multipart-upload",
            Creating::Copy => "copy-destination",
        }
    }

    fn step(self, what: &'static str) -> Step {
        Step(self.name(), what)
    }
}

/// What a run of the probe tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Part {
    /// Writes and server-side copies.
    All,
    /// Writes alone.
    Writes,
    /// Server-side copies alone.
    Copies,
}

/// One run of the probe against a store.
struct Run<'a, S> {
    store: &'a S,
    prefix: &'a str,
    created: Vec<Created>,
}

impl<S: ObjectStore> Run<'_, S> {
    /// Probes `part`; what it leaves out reads as unsupported.
    async fn probe(&mut self, part: Part) -> Result<ConditionalWrites, StepError> {
        let unprobed = OperationSupport {
            if_none_match: Some(PreconditionSupport::Ignored),
            if_match: PreconditionSupport::Ignored,
        };
        let mut writes = ConditionalWrites {
            put_object: unprobed,
            complete_multipart_upload: unprobed,
            delete_object: unprobed,
            copy_object: CopySupport::NONE,
        };
        if part != Part::Copies {
            writes.put_object = self.probe_creating(Creating::Put).await?;
            writes.complete_multipart_upload = self.probe_creating(Creating::Complete).await?;
            writes.delete_object = self.probe_delete().await?;
        }
        if part != Part::Writes {
            writes.copy_object = self.probe_copy().await?;
        }
        Ok(writes)
    }

    /// Probes what a server-side copy of a flush sends (see the module
    /// documentation).
    async fn probe_copy(&mut self) -> Result<CopySupport, StepError> {
        let source = format!("{}{COPY_SOURCE}", self.prefix);
        let step = Step("PutObject", "of the copy source");
        let request = PutObject::new(&source, BODY)
            .with_content_type("text/plain")
            .with_metadata(marked("source"))
            .with_tags(probe_tags(1));
        let result = self.store.put_object(request).await;
        self.record_write(&source, &result);
        let etag = result.map_err(|error| (step, error))?.etag;
        let copied = format!("{}copy-object", self.prefix);

        let step = Step("CopyObject", "with x-amz-metadata-directive: REPLACE");
        let replace = MetadataDirective::Replace {
            metadata: marked("replaced"),
            content_type: Some("text/plain".to_owned()),
            headers: BTreeMap::new(),
        };
        let request = CopyObject::new(&source, &copied).with_metadata_directive(replace);
        let metadata_directive = match passing(step, self.copy(request).await)? {
            Some(_) => {
                let info = self.head(step, &copied).await?;
                replaced(info.metadata.get(MARK) == Some("replaced"))
            }
            None => PreconditionSupport::Rejected,
        };

        let step = Step("CopyObject", "with x-amz-tagging-directive: REPLACE");
        let request = CopyObject::new(&source, &copied)
            .with_tagging_directive(TaggingDirective::Replace(probe_tags(2)));
        let tagging_directive = match passing(step, self.copy(request).await)? {
            Some(_) => replaced(self.head(step, &copied).await?.tag_count == 2),
            None => PreconditionSupport::Rejected,
        };

        let step = Step(
            "CopyObject",
            "with x-amz-copy-source-if-match with a different ETag",
        );
        let request = CopyObject::new(&source, &copied).with_source_if_match(mismatch());
        let source_if_match = match failing(step, self.copy(request).await)? {
            Failing::Refused => {
                let step = Step(
                    "CopyObject",
                    "with x-amz-copy-source-if-match with the source's ETag",
                );
                let request = CopyObject::new(&source, &copied).with_source_if_match(etag);
                match passing(step, self.copy(request).await)? {
                    Some(_) => PreconditionSupport::Honored,
                    None => PreconditionSupport::Rejected,
                }
            }
            Failing::Applied(_) => PreconditionSupport::Ignored,
            Failing::Rejected => PreconditionSupport::Rejected,
        };

        Ok(CopySupport {
            metadata_directive,
            tagging_directive,
            source_if_match,
            destination: self.probe_creating(Creating::Copy).await?,
        })
    }

    /// `CopyObject`, recording the version it created.
    async fn copy(&mut self, request: CopyObject) -> S3Result<WriteOutput> {
        let key = request.key.clone();
        let result = self.store.copy_object(request).await;
        self.record_write(&key, &result);
        result
    }

    /// HEADs `key`, which `step` just wrote.
    async fn head(&self, step: Step, key: &str) -> Result<ObjectInfo, StepError> {
        let request = HeadObject::new(key);
        self.store
            .head_object(request)
            .await
            .map_err(|error| (step, error))
    }

    /// Probes `If-None-Match: *` and then `If-Match` on `PutObject` or
    /// `CompleteMultipartUpload`.
    async fn probe_creating(&mut self, op: Creating) -> Result<OperationSupport, StepError> {
        let key = format!("{}{}", self.prefix, op.key());

        let step = op.step("with If-None-Match: * on a missing key");
        let created = self
            .write(op, step, &key, WritePrecondition::IfAbsent)
            .await?;
        let (if_none_match, current) = match passing(step, created)? {
            Some(output) => {
                let step = op.step("with If-None-Match: * on an existing key");
                let result = self
                    .write(op, step, &key, WritePrecondition::IfAbsent)
                    .await?;
                match failing(step, result)? {
                    Failing::Refused => (PreconditionSupport::Honored, output.etag),
                    Failing::Applied(applied) => (PreconditionSupport::Ignored, applied.etag),
                    Failing::Rejected => (PreconditionSupport::Rejected, output.etag),
                }
            }
            None => {
                // The header is rejected, so create the object without it
                // to probe If-Match.
                let step = Step("PutObject", "without a precondition");
                let result = self.put(&key, WritePrecondition::None).await;
                let output = result.map_err(|error| (step, error))?;
                (PreconditionSupport::Rejected, output.etag)
            }
        };

        let step = op.step("with If-Match with a different ETag");
        let result = self
            .write(op, step, &key, WritePrecondition::IfMatch(mismatch()))
            .await?;
        let if_match = match failing(step, result)? {
            Failing::Refused => {
                let step = op.step("with If-Match with the current ETag");
                let precondition = WritePrecondition::IfMatch(current);
                match passing(step, self.write(op, step, &key, precondition).await?)? {
                    Some(_) => PreconditionSupport::Honored,
                    None => PreconditionSupport::Rejected,
                }
            }
            Failing::Applied(_) => PreconditionSupport::Ignored,
            Failing::Rejected => PreconditionSupport::Rejected,
        };
        Ok(OperationSupport {
            if_none_match: Some(if_none_match),
            if_match,
        })
    }

    /// Probes `If-Match` on `DeleteObject`.
    async fn probe_delete(&mut self) -> Result<OperationSupport, StepError> {
        let key = format!("{}delete-object", self.prefix);
        let step = Step("PutObject", "of the key to delete");
        let result = self.put(&key, WritePrecondition::None).await;
        let current = result.map_err(|error| (step, error))?.etag;

        let step = Step("DeleteObject", "with If-Match with a different ETag");
        let deleted = self.delete(&key, mismatch()).await;
        let if_match = match failing(step, deleted)? {
            Failing::Refused => {
                let step = Step("DeleteObject", "with If-Match with the current ETag");
                match passing(step, self.delete(&key, current).await)? {
                    Some(_) => PreconditionSupport::Honored,
                    None => PreconditionSupport::Rejected,
                }
            }
            Failing::Applied(_) => PreconditionSupport::Ignored,
            Failing::Rejected => PreconditionSupport::Rejected,
        };
        Ok(OperationSupport {
            if_none_match: None,
            if_match,
        })
    }

    /// Writes `key` with `op` and `precondition`. The outer error is a
    /// failure to set the write up, such as the multipart upload a complete
    /// needs; the inner result is the conditional write's.
    async fn write(
        &mut self,
        op: Creating,
        step: Step,
        key: &str,
        precondition: WritePrecondition,
    ) -> Result<S3Result<WriteOutput>, StepError> {
        match op {
            Creating::Put => Ok(self.put(key, precondition).await),
            Creating::Complete => self.complete(step, key, precondition).await,
            Creating::Copy => {
                let source = format!("{}{COPY_SOURCE}", self.prefix);
                let request = CopyObject::new(source, key).with_precondition(precondition);
                Ok(self.copy(request).await)
            }
        }
    }

    async fn put(&mut self, key: &str, precondition: WritePrecondition) -> S3Result<WriteOutput> {
        let request = PutObject::new(key, BODY)
            .with_content_type("text/plain")
            .with_precondition(precondition);
        let result = self.store.put_object(request).await;
        self.record_write(key, &result);
        result
    }

    /// Uploads a one-part multipart upload to `key` and completes it with
    /// `precondition`. An upload that does not complete is aborted.
    async fn complete(
        &mut self,
        step: Step,
        key: &str,
        precondition: WritePrecondition,
    ) -> Result<S3Result<WriteOutput>, StepError> {
        let create = CreateMultipartUpload::new(key).with_content_type("text/plain");
        let upload_id = match self.store.create_multipart_upload(create).await {
            Ok(upload_id) => upload_id,
            Err(error) => {
                if may_have_applied(&error) {
                    let unknown = Created::UnknownUpload(key.to_owned(), error.clone());
                    self.created.push(unknown);
                }
                return Err((step, error));
            }
        };
        let part = UploadPart::new(key, upload_id.clone(), 1, BODY);
        let result = match self.store.upload_part(part).await {
            Ok(etag) => {
                let request = CompleteMultipartUpload {
                    key: key.to_owned(),
                    upload_id: upload_id.clone(),
                    parts: vec![CompletedPart {
                        part_number: 1,
                        etag,
                    }],
                    precondition,
                    apply_by_ms: None,
                };
                let result = self.store.complete_multipart_upload(request).await;
                self.record_write(key, &result);
                Ok(result)
            }
            Err(error) => Err((step, error)),
        };
        if !matches!(result, Ok(Ok(_))) {
            let abort = AbortMultipartUpload {
                key: key.to_owned(),
                upload_id: upload_id.clone(),
            };
            if let Err(error) = self.store.abort_multipart_upload(abort).await
                && error.kind() != S3ErrorKind::NoSuchUpload
            {
                self.created
                    .push(Created::Upload(key.to_owned(), upload_id));
            }
        }
        result
    }

    /// `DeleteObject` with `If-Match`, recording a delete marker it adds.
    async fn delete(&mut self, key: &str, if_match: ETag) -> S3Result<()> {
        let request = DeleteObject::new(key).with_if_match(if_match);
        match self.store.delete_object(request).await {
            Ok(output) => {
                if output.delete_marker && output.version_id.is_some() {
                    let version = output.version_id;
                    self.created.push(Created::Version(key.to_owned(), version));
                }
                Ok(())
            }
            Err(error) => {
                if may_have_applied(&error) {
                    let unknown = Created::UnknownMarker(key.to_owned(), error.clone());
                    self.created.push(unknown);
                }
                Err(error)
            }
        }
    }

    /// Records the version a write created, or its key if the write may
    /// have been applied although it failed.
    fn record_write(&mut self, key: &str, result: &S3Result<WriteOutput>) {
        match result {
            Ok(output) => {
                let version = output.version_id.clone();
                self.created.push(Created::Version(key.to_owned(), version));
            }
            Err(error) if may_have_applied(error) => {
                self.created.push(Created::Unknown(key.to_owned()));
            }
            Err(_) => {}
        }
    }

    /// Removes everything the probe created and returns the first error
    /// and what is left.
    ///
    /// On a versioned bucket, where writes report versions, it deletes each
    /// version and delete marker by ID; deleting a key there would only add
    /// a delete marker. On an unversioned bucket it deletes each key once.
    /// Then it finds what writes with lost responses may have created
    /// ([`Run::remove_unknown`]), and reports what it cannot find.
    async fn clean_up(&mut self) -> Result<(), (S3Error, Vec<String>)> {
        let created = std::mem::take(&mut self.created);
        let versioned = created
            .iter()
            .any(|c| matches!(c, Created::Version(_, Some(_))));
        let mut removals: Vec<Created> = Vec::new();
        for item in created {
            let item = match item {
                // A write that reported no version on a bucket whose other
                // writes did: its version is found like a lost one's.
                Created::Version(key, None) if versioned => Created::Unknown(key),
                item => item,
            };
            if !removals.contains(&item) {
                removals.push(item);
            }
        }
        // Stable, so known items keep their order and come first.
        removals.sort_by_key(Created::is_unknown);

        let mut first_error = None;
        let mut leftovers = Vec::new();
        for removal in &removals {
            let result = match removal {
                Created::Version(key, version) => {
                    let mut request = DeleteObject::new(key.clone());
                    request.version_id = version.clone();
                    self.store.delete_object(request).await.map(drop)
                }
                Created::Upload(key, upload_id) => {
                    let request = AbortMultipartUpload {
                        key: key.clone(),
                        upload_id: upload_id.clone(),
                    };
                    match self.store.abort_multipart_upload(request).await {
                        Err(error) if error.kind() == S3ErrorKind::NoSuchUpload => Ok(()),
                        result => result,
                    }
                }
                Created::Unknown(key) => self.remove_unknown(key, &removals).await,
                // On an unversioned bucket a delete leaves nothing behind.
                Created::UnknownMarker(..) if !versioned => Ok(()),
                Created::UnknownMarker(_, error) | Created::UnknownUpload(_, error) => {
                    Err(error.clone())
                }
            };
            if let Err(error) = result {
                leftovers.push(removal.to_string());
                first_error.get_or_insert(error);
            }
        }
        match first_error {
            None => Ok(()),
            Some(error) => Err((error, leftovers)),
        }
    }

    /// Removes the version a write to `key` with a lost response may have
    /// created, once the versions the cleanup knows are gone (`known`).
    ///
    /// The probe sends nothing to a key after such a write, and nothing
    /// else writes under its scratch prefix, so that version, if the write
    /// was applied, is the key's current object: `HeadObject` names it, and
    /// the cleanup deletes it by ID, or the key on an unversioned bucket.
    /// It repeats until the key has no current object, or its current
    /// object is a version the cleanup already tried to delete: a known one
    /// whose delete failed, which is reported already.
    async fn remove_unknown(&self, key: &str, known: &[Created]) -> S3Result<()> {
        let mut tried: Vec<VersionId> = known
            .iter()
            .filter_map(|item| match item {
                Created::Version(k, Some(version)) if k == key => Some(version.clone()),
                _ => None,
            })
            .collect();
        loop {
            let info = match self.store.head_object(HeadObject::new(key)).await {
                Ok(info) => info,
                Err(error) if error.kind() == S3ErrorKind::NoSuchKey => return Ok(()),
                Err(error) => return Err(error),
            };
            let mut request = DeleteObject::new(key);
            match info.version_id {
                Some(version) if tried.contains(&version) => return Ok(()),
                Some(version) => {
                    request.version_id = Some(version.clone());
                    tried.push(version);
                }
                None => return self.store.delete_object(request).await.map(drop),
            }
            self.store.delete_object(request).await?;
        }
    }
}

/// Whether a failed request may have created something the cleanup has to
/// remove. A store that rejects a header did not apply the request,
/// whatever its status.
fn may_have_applied(error: &S3Error) -> bool {
    error.may_have_applied() && !is_rejection(error)
}

/// The body of every object and part the probe writes.
const BODY: &str = "skys3 capability probe";

/// The scratch key the probe's copies copy.
const COPY_SOURCE: &str = "copy-source";

/// The user-metadata key that tells the copy source's metadata from the
/// metadata a copy replaces it with.
const MARK: &str = "skys3-probe";

/// User metadata with the probe's mark set to `value`.
fn marked(value: &str) -> UserMetadata {
    let mut metadata = UserMetadata::new();
    metadata
        .insert(MARK, value)
        .expect("the probe's mark is valid metadata");
    metadata
}

/// `count` tags, which tell the copy source's tags (one) from those a copy
/// replaces them with (two).
fn probe_tags(count: usize) -> BTreeMap<String, String> {
    (0..count)
        .map(|n| (format!("skys3-probe-{n}"), "probe".to_owned()))
        .collect()
}

/// A directive that a copy applied if `replaced`, and otherwise ignored.
fn replaced(replaced: bool) -> PreconditionSupport {
    if replaced {
        PreconditionSupport::Honored
    } else {
        PreconditionSupport::Ignored
    }
}
