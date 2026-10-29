//! Maps AWS SDK errors to [`S3Error`].

use std::error::Error;

use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::error::{DisplayErrorContext, ProvideErrorMetadata, SdkError};

use crate::{S3Error, S3ErrorKind};

/// The kinds whose S3 error code is their name, and so is recognized in an
/// error response.
const CODED: [S3ErrorKind; 18] = [
    S3ErrorKind::NotModified,
    S3ErrorKind::InvalidArgument,
    S3ErrorKind::InvalidRequest,
    S3ErrorKind::MetadataTooLarge,
    S3ErrorKind::InvalidPart,
    S3ErrorKind::InvalidPartOrder,
    S3ErrorKind::EntityTooSmall,
    S3ErrorKind::NoSuchKey,
    S3ErrorKind::NoSuchUpload,
    S3ErrorKind::NoSuchVersion,
    S3ErrorKind::MethodNotAllowed,
    S3ErrorKind::ConditionalRequestConflict,
    S3ErrorKind::PreconditionFailed,
    S3ErrorKind::InvalidRange,
    S3ErrorKind::InternalError,
    S3ErrorKind::NotImplemented,
    S3ErrorKind::SlowDown,
    S3ErrorKind::ServiceUnavailable,
];

/// The kind of an error response with `code`, if it has one, and `status`.
///
/// A code decides when it names a kind. Responses without a code, such as
/// every error to a `HEAD` and a `304`, are classified by status, and so
/// are proxies' and load balancers' error pages. A `404` without a code is
/// the SDK's `NotFound` for `HEAD`, a missing key.
fn kind_of(code: Option<&str>, status: u16) -> S3ErrorKind {
    if let Some(code) = code {
        if let Some(kind) = CODED.into_iter().find(|kind| kind.code() == code) {
            return kind;
        }
        match code {
            "NotFound" => return S3ErrorKind::NoSuchKey,
            // RequestTimeout: the store gave up waiting for the body.
            "RequestTimeout" => return S3ErrorKind::Timeout,
            _ => {}
        }
    }
    match (code, status) {
        (_, 304) => S3ErrorKind::NotModified,
        (_, 412) => S3ErrorKind::PreconditionFailed,
        (None, 404) => S3ErrorKind::NoSuchKey,
        (None, 405) => S3ErrorKind::MethodNotAllowed,
        (None, 416) => S3ErrorKind::InvalidRange,
        (None, 429) => S3ErrorKind::SlowDown,
        (None, 501) => S3ErrorKind::NotImplemented,
        (None, 503) => S3ErrorKind::ServiceUnavailable,
        (None, 500 | 502 | 504) => S3ErrorKind::InternalError,
        _ => S3ErrorKind::Other,
    }
}

/// Maps the error of an SDK call of `operation`.
///
/// A service error keeps the store's status, code, and message. A request
/// the client could not build is [`S3ErrorKind::NotSent`]. A request that
/// timed out, or whose connection failed, is [`S3ErrorKind::Timeout`]: the
/// client cannot tell whether the store received it, so it may have been
/// applied. So is a failure to load credentials, which happens while
/// dispatching and usually passes. A response that could not be read is
/// [`S3ErrorKind::Other`] with its status.
pub(super) fn map_sdk_error<E>(operation: &str, error: SdkError<E, HttpResponse>) -> S3Error
where
    E: ProvideErrorMetadata + Error + Send + Sync + 'static,
{
    match &error {
        SdkError::ServiceError(service) => {
            let status = service.raw().status().as_u16();
            let code = service.err().code();
            let kind = kind_of(code, status);
            let message = match service.err().message() {
                Some(message) => format!("{operation}: {message}"),
                None => format!("{operation} failed"),
            };
            let mapped = S3Error::new(kind, message).with_status(status);
            match code {
                Some(code) => mapped.with_code(code),
                None => mapped,
            }
        }
        SdkError::ConstructionFailure(_) => S3Error::new(
            S3ErrorKind::NotSent,
            format!(
                "{operation} could not be built: {}",
                DisplayErrorContext(&error)
            ),
        ),
        SdkError::TimeoutError(_) => {
            S3Error::new(S3ErrorKind::Timeout, format!("{operation} timed out"))
        }
        SdkError::DispatchFailure(failure) if failure.is_user() => S3Error::new(
            S3ErrorKind::NotSent,
            format!(
                "{operation} could not be sent: {}",
                DisplayErrorContext(&error)
            ),
        ),
        SdkError::DispatchFailure(_) => S3Error::new(
            S3ErrorKind::Timeout,
            format!(
                "{operation} got no response: {}",
                DisplayErrorContext(&error)
            ),
        ),
        SdkError::ResponseError(response) => {
            let status = response.raw().status().as_u16();
            let kind = match status {
                200..=299 => S3ErrorKind::Other,
                _ => kind_of(None, status),
            };
            S3Error::new(
                kind,
                format!(
                    "{operation} returned an unreadable response: {}",
                    DisplayErrorContext(&error)
                ),
            )
            .with_status(status)
        }
        _ => S3Error::new(
            S3ErrorKind::Other,
            format!("{operation} failed: {}", DisplayErrorContext(&error)),
        ),
    }
}

/// An error for a successful response that lacks a field SkyS3 needs, or
/// has one it cannot parse. The request was applied.
pub(super) fn malformed(operation: &str, what: impl std::fmt::Display) -> S3Error {
    S3Error::new(
        S3ErrorKind::Other,
        format!("{operation} returned a malformed response: {what}"),
    )
    .with_status(200)
}

#[cfg(test)]
mod tests {
    use aws_sdk_s3::error::{ConnectorError, ErrorMetadata};
    use aws_sdk_s3::operation::get_object::GetObjectError;
    use aws_sdk_s3::primitives::SdkBody;

    use super::*;

    fn response(status: u16) -> HttpResponse {
        HttpResponse::new(status.try_into().unwrap(), SdkBody::empty())
    }

    fn service(status: u16, code: Option<&str>, message: Option<&str>) -> S3Error {
        let mut meta = ErrorMetadata::builder();
        if let Some(code) = code {
            meta = meta.code(code);
        }
        if let Some(message) = message {
            meta = meta.message(message);
        }
        let error = GetObjectError::generic(meta.build());
        map_sdk_error(
            "GetObject",
            SdkError::service_error(error, response(status)),
        )
    }

    #[test]
    fn codes_decide_the_kind() {
        let error = service(412, Some("PreconditionFailed"), Some("At least one failed"));
        assert_eq!(error.kind(), S3ErrorKind::PreconditionFailed);
        assert_eq!(error.status(), Some(412));
        assert_eq!(error.message(), "GetObject: At least one failed");

        for kind in CODED {
            let status = kind.status().unwrap();
            assert_eq!(service(status, Some(kind.code()), None).kind(), kind);
        }
        assert_eq!(
            service(404, Some("NotFound"), None).kind(),
            S3ErrorKind::NoSuchKey
        );
        assert_eq!(
            service(400, Some("RequestTimeout"), None).kind(),
            S3ErrorKind::Timeout
        );

        let bucket = service(404, Some("NoSuchBucket"), None);
        assert_eq!(bucket.kind(), S3ErrorKind::Other);
        assert_eq!(bucket.code(), "NoSuchBucket");
        assert!(!bucket.may_have_applied());
        assert_eq!(bucket.message(), "GetObject failed");
    }

    #[test]
    fn statuses_decide_without_a_code() {
        for (status, kind) in [
            (304, S3ErrorKind::NotModified),
            (404, S3ErrorKind::NoSuchKey),
            (405, S3ErrorKind::MethodNotAllowed),
            (412, S3ErrorKind::PreconditionFailed),
            (416, S3ErrorKind::InvalidRange),
            (429, S3ErrorKind::SlowDown),
            (500, S3ErrorKind::InternalError),
            (501, S3ErrorKind::NotImplemented),
            (502, S3ErrorKind::InternalError),
            (503, S3ErrorKind::ServiceUnavailable),
            (504, S3ErrorKind::InternalError),
            (400, S3ErrorKind::Other),
            (403, S3ErrorKind::Other),
        ] {
            let error = service(status, None, None);
            assert_eq!(error.kind(), kind, "{status}");
            assert_eq!(error.status(), Some(status));
        }
        assert!(service(502, None, None).may_have_applied());
        assert!(!service(403, None, None).may_have_applied());
    }

    #[test]
    fn transport_errors_may_have_applied() {
        let timeout: SdkError<GetObjectError, HttpResponse> = SdkError::timeout_error("slow");
        let error = map_sdk_error("GetObject", timeout);
        assert_eq!(error.kind(), S3ErrorKind::Timeout);
        assert!(error.may_have_applied());

        for connector in [
            ConnectorError::io("reset".into()),
            ConnectorError::timeout("read".into()),
            ConnectorError::other("credentials".into(), None),
        ] {
            let failure: SdkError<GetObjectError, HttpResponse> =
                SdkError::dispatch_failure(connector);
            let error = map_sdk_error("PutObject", failure);
            assert_eq!(error.kind(), S3ErrorKind::Timeout);
            assert!(error.is_transient() && error.may_have_applied());
        }

        let user: SdkError<GetObjectError, HttpResponse> =
            SdkError::dispatch_failure(ConnectorError::user("bad uri".into()));
        assert_eq!(
            map_sdk_error("PutObject", user).kind(),
            S3ErrorKind::NotSent
        );

        let construction: SdkError<GetObjectError, HttpResponse> =
            SdkError::construction_failure("invalid header");
        let error = map_sdk_error("PutObject", construction);
        assert_eq!(error.kind(), S3ErrorKind::NotSent);
        assert!(!error.may_have_applied() && !error.is_transient());
    }

    #[test]
    fn unreadable_responses_keep_their_status() {
        let ok: SdkError<GetObjectError, HttpResponse> =
            SdkError::response_error("truncated XML", response(200));
        let error = map_sdk_error("CompleteMultipartUpload", ok);
        assert_eq!(error.kind(), S3ErrorKind::Other);
        assert_eq!(error.status(), Some(200));
        assert!(error.may_have_applied());

        let gateway: SdkError<GetObjectError, HttpResponse> =
            SdkError::response_error("HTML", response(502));
        assert_eq!(
            map_sdk_error("PutObject", gateway).kind(),
            S3ErrorKind::InternalError
        );

        let error = malformed("PutObject", "no ETag");
        assert_eq!(error.status(), Some(200));
        assert!(error.may_have_applied());
        assert!(error.message().contains("no ETag"));
    }
}
