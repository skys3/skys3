//! Errors returned by an [`ObjectStore`](crate::ObjectStore).

use std::fmt;

/// Error codes with which stores, and the proxies and gateways in front of
/// them, ask a client to send less, whatever their status: S3's
/// `SlowDown`, and the throttling codes of other AWS services and
/// S3-compatible providers.
pub const THROTTLE_CODES: [&str; 10] = [
    "SlowDown",
    "Throttling",
    "ThrottlingException",
    "ThrottledException",
    "RequestThrottled",
    "RequestThrottledException",
    "TooManyRequests",
    "TooManyRequestsException",
    "RequestLimitExceeded",
    "BandwidthLimitExceeded",
];

/// The kind of an [`S3Error`]: an S3 error code, or a failure to get a
/// response at all.
///
/// Callers decide what to do from the kind alone. Retries and fault handling
/// depend on two questions, which [`S3ErrorKind::is_transient`] and
/// [`S3ErrorKind::may_have_applied`] answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum S3ErrorKind {
    /// `304 Not Modified`: a read's `If-None-Match` matched the current
    /// ETag. Polling readers, such as the S3 control store watching
    /// `cluster.json` (design §6.1), expect it.
    NotModified,
    /// `400 InvalidArgument`: a request parameter is not valid, such as a
    /// part number outside `1..=10000` or an unknown continuation token.
    InvalidArgument,
    /// `400 InvalidRequest`: the request is malformed as a whole, such as a
    /// `CompleteMultipartUpload` without parts.
    InvalidRequest,
    /// `400 MetadataTooLarge`: the user metadata exceeds the store's limit
    /// ([`UserMetadata::S3_LIMIT`](crate::UserMetadata::S3_LIMIT) on S3).
    MetadataTooLarge,
    /// `400 InvalidPart`: a completed part is not uploaded or its ETag
    /// differs.
    InvalidPart,
    /// `400 InvalidPartOrder`: completed parts are not in ascending order.
    InvalidPartOrder,
    /// `400 EntityTooSmall`: a part other than the last is smaller than the
    /// store's minimum part size.
    EntityTooSmall,
    /// `404 NoSuchKey`: the object does not exist, or its current version is
    /// a delete marker. A write with `If-Match` also gets it when the key
    /// has no current object.
    NoSuchKey,
    /// `404 NoSuchUpload`: the multipart upload does not exist, or was
    /// completed or aborted.
    NoSuchUpload,
    /// `404 NoSuchVersion`: the requested version does not exist.
    NoSuchVersion,
    /// `405 MethodNotAllowed`: a read named a version that is a delete
    /// marker.
    MethodNotAllowed,
    /// `409 ConditionalRequestConflict`: another write to the key was
    /// applied while this conditional write was in progress. Nothing was
    /// written; re-read the key and retry (design §6.1).
    ConditionalRequestConflict,
    /// `412 Precondition Failed`: a precondition did not hold. Nothing was
    /// written.
    PreconditionFailed,
    /// `416 InvalidRange`: the requested range does not overlap the
    /// object.
    InvalidRange,
    /// `500 InternalError`. The request may or may not have been applied.
    InternalError,
    /// `501 NotImplemented`: the store rejects a header it does not support,
    /// such as a precondition on an operation without conditional writes.
    NotImplemented,
    /// `503 SlowDown`: the store is shedding load. Nothing was applied; back
    /// off and reduce concurrency (design §7.7).
    SlowDown,
    /// `503 Service Unavailable`. Nothing was applied.
    ServiceUnavailable,
    /// No response arrived: the request timed out or the connection
    /// dropped. The request may or may not have been applied.
    Timeout,
    /// The client failed before it sent the request, for example because a
    /// parameter cannot be encoded or no endpoint could be resolved. Nothing
    /// was applied, and sending it again fails the same way.
    NotSent,
    /// Any other status, or a response that could not be understood.
    /// [`S3Error::status`] and [`S3Error::code`] give what the store
    /// answered, if anything.
    Other,
}

impl S3ErrorKind {
    /// The S3 error code, as in an error response's `Code` element.
    pub fn code(self) -> &'static str {
        match self {
            S3ErrorKind::NotModified => "NotModified",
            S3ErrorKind::InvalidArgument => "InvalidArgument",
            S3ErrorKind::InvalidRequest => "InvalidRequest",
            S3ErrorKind::MetadataTooLarge => "MetadataTooLarge",
            S3ErrorKind::InvalidPart => "InvalidPart",
            S3ErrorKind::InvalidPartOrder => "InvalidPartOrder",
            S3ErrorKind::EntityTooSmall => "EntityTooSmall",
            S3ErrorKind::NoSuchKey => "NoSuchKey",
            S3ErrorKind::NoSuchUpload => "NoSuchUpload",
            S3ErrorKind::NoSuchVersion => "NoSuchVersion",
            S3ErrorKind::MethodNotAllowed => "MethodNotAllowed",
            S3ErrorKind::ConditionalRequestConflict => "ConditionalRequestConflict",
            S3ErrorKind::PreconditionFailed => "PreconditionFailed",
            S3ErrorKind::InvalidRange => "InvalidRange",
            S3ErrorKind::InternalError => "InternalError",
            S3ErrorKind::NotImplemented => "NotImplemented",
            S3ErrorKind::SlowDown => "SlowDown",
            S3ErrorKind::ServiceUnavailable => "ServiceUnavailable",
            S3ErrorKind::Timeout => "Timeout",
            S3ErrorKind::NotSent => "NotSent",
            S3ErrorKind::Other => "Other",
        }
    }

    /// The HTTP status, or `None` for [`S3ErrorKind::Timeout`],
    /// [`S3ErrorKind::NotSent`], and [`S3ErrorKind::Other`], which have no
    /// fixed status.
    pub fn status(self) -> Option<u16> {
        Some(match self {
            S3ErrorKind::NotModified => 304,
            S3ErrorKind::InvalidArgument
            | S3ErrorKind::InvalidRequest
            | S3ErrorKind::MetadataTooLarge
            | S3ErrorKind::InvalidPart
            | S3ErrorKind::InvalidPartOrder
            | S3ErrorKind::EntityTooSmall => 400,
            S3ErrorKind::NoSuchKey | S3ErrorKind::NoSuchUpload | S3ErrorKind::NoSuchVersion => 404,
            S3ErrorKind::MethodNotAllowed => 405,
            S3ErrorKind::ConditionalRequestConflict => 409,
            S3ErrorKind::PreconditionFailed => 412,
            S3ErrorKind::InvalidRange => 416,
            S3ErrorKind::InternalError => 500,
            S3ErrorKind::NotImplemented => 501,
            S3ErrorKind::SlowDown | S3ErrorKind::ServiceUnavailable => 503,
            S3ErrorKind::Timeout | S3ErrorKind::NotSent | S3ErrorKind::Other => return None,
        })
    }

    /// Whether the same request may succeed if sent again after a backoff:
    /// server errors, throttling, and timeouts.
    ///
    /// A [`S3ErrorKind::ConditionalRequestConflict`] is not transient in
    /// this sense: the caller re-reads the key before it retries.
    pub fn is_transient(self) -> bool {
        matches!(
            self,
            S3ErrorKind::InternalError
                | S3ErrorKind::SlowDown
                | S3ErrorKind::ServiceUnavailable
                | S3ErrorKind::Timeout
        )
    }

    /// Whether a write that failed with this error may nevertheless have
    /// been applied: its response was lost or was a server error. Every
    /// write must be safe to retry after such an error, which is what the
    /// write identity (design §7.2) and the `proposal_id` of control-store
    /// registers (design §6.1) are for.
    pub fn may_have_applied(self) -> bool {
        matches!(
            self,
            S3ErrorKind::InternalError | S3ErrorKind::Timeout | S3ErrorKind::Other
        )
    }
}

impl fmt::Display for S3ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.status() {
            Some(status) => write!(f, "{status} {}", self.code()),
            None => f.write_str(self.code()),
        }
    }
}

/// An error from an [`ObjectStore`](crate::ObjectStore) operation.
///
/// Besides its [`S3ErrorKind`], an error from a real store keeps the HTTP
/// status and error code the store answered with, which may be more precise
/// than the kind: a `403 AccessDenied` is [`S3ErrorKind::Other`] with status
/// 403 and code `AccessDenied`, and a `502` from a proxy is
/// [`S3ErrorKind::InternalError`] with status 502. Callers deciding whether
/// to retry use [`S3Error::is_transient`] and [`S3Error::may_have_applied`],
/// which take the status into account.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{}: {message}", self.describe())]
pub struct S3Error {
    kind: S3ErrorKind,
    message: String,
    status: Option<u16>,
    code: Option<String>,
}

impl S3Error {
    /// Returns an error of `kind` with a human-readable `message`.
    pub fn new(kind: S3ErrorKind, message: impl Into<String>) -> Self {
        S3Error {
            kind,
            message: message.into(),
            status: None,
            code: None,
        }
    }

    /// Records the HTTP status the store answered with.
    pub fn with_status(mut self, status: u16) -> Self {
        self.status = Some(status);
        self
    }

    /// Records the error code the store answered with, such as
    /// `AccessDenied`.
    pub fn with_code(mut self, code: impl Into<String>) -> Self {
        self.code = Some(code.into());
        self
    }

    /// The error's kind.
    pub fn kind(&self) -> S3ErrorKind {
        self.kind
    }

    /// The human-readable message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// The HTTP status the store answered with, or else the kind's
    /// [`S3ErrorKind::status`].
    pub fn status(&self) -> Option<u16> {
        self.status.or_else(|| self.kind.status())
    }

    /// The error code the store answered with, or else the kind's
    /// [`S3ErrorKind::code`].
    pub fn code(&self) -> &str {
        self.code.as_deref().unwrap_or_else(|| self.kind.code())
    }

    /// Whether the same request may succeed if sent again after a backoff:
    /// [`S3ErrorKind::is_transient`], and also an [`S3ErrorKind::Other`]
    /// with a `429 Too Many Requests` or `5xx` status.
    pub fn is_transient(&self) -> bool {
        self.kind.is_transient()
            || (self.kind == S3ErrorKind::Other && matches!(self.status, Some(429 | 500..=599)))
    }

    /// Whether a write that failed with this error may nevertheless have
    /// been applied: [`S3ErrorKind::may_have_applied`], except that an
    /// [`S3ErrorKind::Other`] with a `4xx` status was refused and not
    /// applied.
    pub fn may_have_applied(&self) -> bool {
        match (self.kind, self.status) {
            (S3ErrorKind::Other, Some(400..=499)) => false,
            (kind, _) => kind.may_have_applied(),
        }
    }

    /// Whether the store asked the caller to send less: any `503`
    /// ([`S3ErrorKind::SlowDown`], [`S3ErrorKind::ServiceUnavailable`], or
    /// another code), a `429 Too Many Requests`, or one of the
    /// [`THROTTLE_CODES`] with another status. Nothing was applied. Flush
    /// concurrency shrinks on it (design §7.7).
    pub fn is_throttle(&self) -> bool {
        matches!(
            self.kind,
            S3ErrorKind::SlowDown | S3ErrorKind::ServiceUnavailable
        ) || matches!(self.status(), Some(429 | 503))
            || THROTTLE_CODES.contains(&self.code())
    }

    /// `<status> <code>`, or the code alone without a status.
    fn describe(&self) -> String {
        match self.status() {
            Some(status) => format!("{status} {}", self.code()),
            None => self.code().to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses_codes_and_classes() {
        let error = S3Error::new(S3ErrorKind::PreconditionFailed, "etag differs");
        assert_eq!(error.kind(), S3ErrorKind::PreconditionFailed);
        assert_eq!(error.message(), "etag differs");
        assert_eq!(error.to_string(), "412 PreconditionFailed: etag differs");
        assert_eq!(
            S3Error::new(S3ErrorKind::Timeout, "no response").to_string(),
            "Timeout: no response"
        );

        let kinds = [
            (S3ErrorKind::NotModified, Some(304), false, false),
            (S3ErrorKind::InvalidArgument, Some(400), false, false),
            (S3ErrorKind::InvalidRequest, Some(400), false, false),
            (S3ErrorKind::MetadataTooLarge, Some(400), false, false),
            (S3ErrorKind::InvalidPart, Some(400), false, false),
            (S3ErrorKind::InvalidPartOrder, Some(400), false, false),
            (S3ErrorKind::EntityTooSmall, Some(400), false, false),
            (S3ErrorKind::NoSuchKey, Some(404), false, false),
            (S3ErrorKind::NoSuchUpload, Some(404), false, false),
            (S3ErrorKind::NoSuchVersion, Some(404), false, false),
            (S3ErrorKind::MethodNotAllowed, Some(405), false, false),
            (
                S3ErrorKind::ConditionalRequestConflict,
                Some(409),
                false,
                false,
            ),
            (S3ErrorKind::PreconditionFailed, Some(412), false, false),
            (S3ErrorKind::InvalidRange, Some(416), false, false),
            (S3ErrorKind::InternalError, Some(500), true, true),
            (S3ErrorKind::NotImplemented, Some(501), false, false),
            (S3ErrorKind::SlowDown, Some(503), true, false),
            (S3ErrorKind::ServiceUnavailable, Some(503), true, false),
            (S3ErrorKind::Timeout, None, true, true),
            (S3ErrorKind::NotSent, None, false, false),
            (S3ErrorKind::Other, None, false, true),
        ];
        for (kind, status, transient, applied) in kinds {
            assert_eq!(kind.status(), status, "{kind:?}");
            assert_eq!(kind.is_transient(), transient, "{kind:?}");
            assert_eq!(kind.may_have_applied(), applied, "{kind:?}");
            assert_eq!(format!("{kind:?}"), kind.code());
            let error = S3Error::new(kind, "");
            assert_eq!(error.status(), status, "{kind:?}");
            assert_eq!(error.code(), kind.code());
            assert_eq!(error.is_transient(), transient, "{kind:?}");
            assert_eq!(error.may_have_applied(), applied, "{kind:?}");
        }
    }

    #[test]
    fn responses_refine_the_kind() {
        let denied = S3Error::new(S3ErrorKind::Other, "denied")
            .with_status(403)
            .with_code("AccessDenied");
        assert_eq!(denied.status(), Some(403));
        assert_eq!(denied.code(), "AccessDenied");
        assert_eq!(denied.to_string(), "403 AccessDenied: denied");
        assert!(!denied.is_transient());
        assert!(!denied.may_have_applied());

        let gateway = S3Error::new(S3ErrorKind::Other, "bad gateway").with_status(502);
        assert_eq!(gateway.code(), "Other");
        assert!(gateway.is_transient());
        assert!(gateway.may_have_applied());

        let throttled = S3Error::new(S3ErrorKind::Other, "").with_status(429);
        assert!(throttled.is_transient());
        assert!(!throttled.may_have_applied());

        let proxy = S3Error::new(S3ErrorKind::InternalError, "").with_status(504);
        assert_eq!(proxy.to_string(), "504 InternalError: ");
        assert!(proxy.may_have_applied());
    }

    #[test]
    fn throttles_are_503s_429s_and_throttling_codes() {
        let throttles = [
            S3Error::new(S3ErrorKind::SlowDown, ""),
            S3Error::new(S3ErrorKind::ServiceUnavailable, ""),
            S3Error::new(S3ErrorKind::Other, "").with_status(429),
            S3Error::new(S3ErrorKind::Other, "").with_status(503),
            S3Error::new(S3ErrorKind::Other, "")
                .with_status(400)
                .with_code("RequestLimitExceeded"),
            S3Error::new(S3ErrorKind::Other, "").with_code("Throttling"),
        ];
        for error in throttles {
            assert!(error.is_throttle(), "{error}");
        }
        let others = [
            S3Error::new(S3ErrorKind::InternalError, ""),
            S3Error::new(S3ErrorKind::Timeout, ""),
            S3Error::new(S3ErrorKind::PreconditionFailed, ""),
            S3Error::new(S3ErrorKind::Other, "").with_status(502),
            S3Error::new(S3ErrorKind::Other, "")
                .with_status(403)
                .with_code("AccessDenied"),
        ];
        for error in others {
            assert!(!error.is_throttle(), "{error}");
        }
    }
}
