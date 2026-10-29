//! Conditional requests (design §5.1, §9.2): `If-Match`, `If-None-Match`,
//! `If-Modified-Since`, and `If-Unmodified-Since`, evaluated against a key's
//! entry in its shard's index.
//!
//! Reads follow RFC 9110 §13.2.2, which is also how S3 combines the
//! headers:
//!
//! 1. `If-Match`, if present, must match the object's ETag (strong
//!    comparison), or the answer is `412 PreconditionFailed`.
//! 2. Otherwise `If-Unmodified-Since`, if present, must not be before the
//!    object's `Last-Modified`, or the answer is `412`.
//! 3. `If-None-Match`, if present, must not match the ETag (weak
//!    comparison), or the answer is `304 Not Modified`.
//! 4. Otherwise `If-Modified-Since`, if present, must be before
//!    `Last-Modified`, or the answer is `304`.
//!
//! `Last-Modified` is compared in whole seconds, the precision of an HTTP
//! date. A missing key answers `404 NoSuchKey` whatever the conditions.
//!
//! Writes take the [`Precondition`]s S3 supports for PutObject and
//! DeleteObject, which the shard checks when it sequences the write
//! ([`Shards::write`](crate::Shards::write)).

use std::time::{Duration, UNIX_EPOCH};

use http::HeaderMap;
use s3s::dto::{ETag as S3ETag, ETagCondition, Timestamp, TimestampFormat};
use s3s::{S3Error, S3ErrorCode, S3Result, s3_error};
use skys3_index::{Entry, ObjectVersion};

/// What a conditional write requires of its key's current object.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum Precondition {
    /// Nothing: the write is unconditional.
    #[default]
    None,
    /// The key has no object (`If-None-Match: *`). A delete tombstone counts
    /// as no object.
    Absent,
    /// The key has an object (`If-Match: *`).
    Exists,
    /// The key's object has this ETag, without quotes (`If-Match`).
    Matches(String),
}

/// Why a [`Precondition`] did not hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ConditionFailed {
    /// The precondition names an object, and the key has none. S3 answers
    /// `404 NoSuchKey` rather than `412` (design §7.2).
    #[error("the key has no object")]
    NoSuchKey,
    /// The key's object does not satisfy the precondition.
    #[error("the precondition does not hold")]
    PreconditionFailed,
    /// A multipart record names an upload that is not open (`404
    /// NoSuchUpload`).
    #[error("the upload is not open")]
    NoSuchUpload,
    /// A completion names a part that is missing, or was replaced while the
    /// completion was prepared (`400 InvalidPart`).
    #[error("a part is not the one the completion names")]
    InvalidPart,
}

impl Precondition {
    /// The precondition of a write's `If-Match` and `If-None-Match`
    /// headers. S3 accepts only `*` in `If-None-Match` on a write, and not
    /// both headers together.
    ///
    /// # Errors
    ///
    /// `501 NotImplemented` for an `If-None-Match` other than `*`, or for
    /// both headers.
    pub(crate) fn of_write(
        if_match: Option<&ETagCondition>,
        if_none_match: Option<&ETagCondition>,
    ) -> S3Result<Self> {
        match (if_match, if_none_match) {
            (None, None) => Ok(Self::None),
            (None, Some(ETagCondition::Any)) => Ok(Self::Absent),
            (None, Some(_)) => Err(s3_error!(
                NotImplemented,
                "A header you provided implies functionality that is not implemented: \
                 If-None-Match accepts only * on a write"
            )),
            (Some(ETagCondition::Any), None) => Ok(Self::Exists),
            (Some(ETagCondition::ETag(etag)), None) => Ok(Self::Matches(etag.value().to_owned())),
            (Some(_), Some(_)) => Err(s3_error!(
                NotImplemented,
                "A header you provided implies functionality that is not implemented: \
                 If-Match and If-None-Match together on a write"
            )),
        }
    }

    /// Whether the precondition holds of `entry`, the key's entry.
    ///
    /// # Errors
    ///
    /// Why it does not.
    pub fn check(&self, entry: Option<&Entry>) -> Result<(), ConditionFailed> {
        let object = entry.and_then(|entry| entry.object.as_ref());
        match (self, object) {
            (Self::None, _) | (Self::Absent, None) | (Self::Exists, Some(_)) => Ok(()),
            (Self::Absent, Some(_)) => Err(ConditionFailed::PreconditionFailed),
            (Self::Exists | Self::Matches(_), None) => Err(ConditionFailed::NoSuchKey),
            (Self::Matches(etag), Some(object)) if object.local_etag.as_str() == etag => Ok(()),
            (Self::Matches(_), Some(_)) => Err(ConditionFailed::PreconditionFailed),
        }
    }
}

impl From<ConditionFailed> for S3Error {
    fn from(failed: ConditionFailed) -> Self {
        match failed {
            ConditionFailed::NoSuchKey => no_such_key(),
            ConditionFailed::PreconditionFailed => precondition_failed(),
            ConditionFailed::NoSuchUpload => no_such_upload(),
            ConditionFailed::InvalidPart => invalid_part(),
        }
    }
}

/// `404 NoSuchUpload`.
pub(crate) fn no_such_upload() -> S3Error {
    s3_error!(
        NoSuchUpload,
        "The specified upload does not exist. The upload ID may be invalid, or the upload may \
         have been aborted or completed."
    )
}

/// `400 InvalidPart`.
pub(crate) fn invalid_part() -> S3Error {
    s3_error!(
        InvalidPart,
        "One or more of the specified parts could not be found. The part may not have been \
         uploaded, or the specified entity tag may not match the part's entity tag."
    )
}

/// The conditional headers of a read.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ReadConditions<'a> {
    pub(crate) if_match: Option<&'a ETagCondition>,
    pub(crate) if_none_match: Option<&'a ETagCondition>,
    pub(crate) if_modified_since: Option<&'a Timestamp>,
    pub(crate) if_unmodified_since: Option<&'a Timestamp>,
}

impl ReadConditions<'_> {
    /// Checks the conditions against `object`, the key's current object.
    ///
    /// # Errors
    ///
    /// `412 PreconditionFailed`, or `304 Not Modified` with the object's
    /// `ETag` and `Last-Modified`.
    pub(crate) fn check(&self, object: &ObjectVersion) -> S3Result<()> {
        let etag = object.local_etag.as_str();
        let modified = last_modified(object);
        let failed = match (self.if_match, self.if_unmodified_since) {
            (Some(condition), _) => !matches_strongly(condition, etag),
            (None, Some(since)) => modified > *since,
            (None, None) => false,
        };
        if failed {
            return Err(precondition_failed());
        }
        let unchanged = match (self.if_none_match, self.if_modified_since) {
            (Some(condition), _) => matches_weakly(condition, etag),
            (None, Some(since)) => modified <= *since,
            (None, None) => false,
        };
        if unchanged {
            return Err(not_modified(object));
        }
        Ok(())
    }
}

/// An object's `Last-Modified`, in whole seconds.
pub(crate) fn last_modified(object: &ObjectVersion) -> Timestamp {
    Timestamp::from(UNIX_EPOCH + Duration::from_secs(object.last_modified_ms / 1000))
}

/// An object's ETag as S3 returns it.
pub(crate) fn s3_etag(object: &ObjectVersion) -> S3ETag {
    S3ETag::Strong(object.local_etag.as_str().to_owned())
}

fn matches_strongly(condition: &ETagCondition, etag: &str) -> bool {
    match condition {
        ETagCondition::Any => true,
        ETagCondition::ETag(S3ETag::Strong(value)) => value == etag,
        ETagCondition::ETag(S3ETag::Weak(_)) => false,
    }
}

fn matches_weakly(condition: &ETagCondition, etag: &str) -> bool {
    match condition {
        ETagCondition::Any => true,
        ETagCondition::ETag(tag) => tag.value() == etag,
    }
}

fn precondition_failed() -> S3Error {
    s3_error!(
        PreconditionFailed,
        "At least one of the preconditions you specified did not hold."
    )
}

pub(crate) fn no_such_key() -> S3Error {
    s3_error!(NoSuchKey, "The specified key does not exist.")
}

/// `304 Not Modified`, which carries the object's validators and no body.
fn not_modified(object: &ObjectVersion) -> S3Error {
    let mut error = S3Error::new(S3ErrorCode::NotModified);
    let mut headers = HeaderMap::new();
    if let Ok(etag) = s3_etag(object).to_http_header() {
        headers.insert(http::header::ETAG, etag);
    }
    let mut date = Vec::new();
    if last_modified(object)
        .format(TimestampFormat::HttpDate, &mut date)
        .is_ok()
        && let Ok(value) = http::HeaderValue::from_bytes(&date)
    {
        headers.insert(http::header::LAST_MODIFIED, value);
    }
    error.set_headers(headers);
    error
}
