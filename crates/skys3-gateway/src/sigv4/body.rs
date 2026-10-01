//! What `x-amz-content-sha256` declares about a request body, and the body
//! wrappers that hold the client to it.

use std::borrow::Cow;
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use bytes::Bytes;
use http::header::{self, HeaderMap, HeaderName, HeaderValue};
use http::request::Parts;
use http_body::{Frame, SizeHint};
use s3s::{Body, S3Error, S3ErrorCode, StdError, s3_error};
use skys3_io::BlockingPool;
use skys3_types::checksum::ChecksumAlgorithm;
use skys3_types::limits::MAX_SINGLE_PUT_BYTES;

use crate::checksum::PooledHasher;

use super::chunked::{AwsChunkedBody, ChunkSigner, Decoder, MAX_TRAILERS, Trailers};
use super::params::decode_signature;

/// The header that declares the payload hash.
pub(crate) const CONTENT_SHA256: &str = "x-amz-content-sha256";

/// The header with the length of an `aws-chunked` body once decoded.
const DECODED_LENGTH: &str = "x-amz-decoded-content-length";

/// The header that names an `aws-chunked` body's trailers.
const TRAILER: &str = "x-amz-trailer";

/// What a request's body was signed as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Payload {
    /// `UNSIGNED-PAYLOAD`: anything goes.
    Unsigned,
    /// The body's SHA-256.
    Sha256([u8; 32]),
    /// An `aws-chunked` body, with signed chunks or not, and with trailers
    /// or not.
    Chunked { signed: bool, trailer: bool },
}

impl Payload {
    /// Reads `x-amz-content-sha256`, or `None` if the request has none.
    ///
    /// # Errors
    ///
    /// `InvalidArgument` for a value that is not a payload hash,
    /// `NotImplemented` for SigV4a streaming.
    pub(crate) fn declared(headers: &HeaderMap) -> Result<Option<Self>, S3Error> {
        let mut values = headers.get_all(CONTENT_SHA256).iter();
        let Some(value) = values.next() else {
            return Ok(None);
        };
        if values.next().is_some() {
            return Err(invalid_hash());
        }
        let payload = match value.as_bytes() {
            b"UNSIGNED-PAYLOAD" => Payload::Unsigned,
            b"STREAMING-AWS4-HMAC-SHA256-PAYLOAD" => Payload::Chunked {
                signed: true,
                trailer: false,
            },
            b"STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER" => Payload::Chunked {
                signed: true,
                trailer: true,
            },
            b"STREAMING-UNSIGNED-PAYLOAD-TRAILER" => Payload::Chunked {
                signed: false,
                trailer: true,
            },
            b"STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD"
            | b"STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER" => {
                return Err(super::params::unsupported());
            }
            hash => Payload::Sha256(
                decode_signature(&hash.to_ascii_lowercase()).ok_or_else(invalid_hash)?,
            ),
        };
        Ok(Some(payload))
    }
}

fn invalid_hash() -> S3Error {
    s3_error!(
        InvalidArgument,
        "x-amz-content-sha256 must be UNSIGNED-PAYLOAD, STREAMING-AWS4-HMAC-SHA256-PAYLOAD, \
         STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER, STREAMING-UNSIGNED-PAYLOAD-TRAILER, or a \
         valid sha256 value."
    )
}

/// Wraps `body` so that reading it checks it against `payload`, and
/// rewrites the head of an `aws-chunked` request to describe the decoded
/// body: `Content-Length` becomes the decoded length, `aws-chunked` leaves
/// `Content-Encoding`, and `x-amz-decoded-content-length` and
/// `Transfer-Encoding` are removed. A body with trailers gets a
/// [`Trailers`] extension.
///
/// `signer` checks signed chunks; a request without one cannot have them.
/// A declared SHA-256 is computed on `pool`, or inline without one.
///
/// # Errors
///
/// `MissingContentLength`, `EntityTooLarge`, or `InvalidArgument` for an
/// `aws-chunked` request whose decoded length or declared trailers are
/// missing or invalid, and `AccessDenied` for signed chunks in an unsigned
/// request.
pub(crate) fn wrap(
    parts: &mut Parts,
    body: Body,
    payload: Payload,
    signer: Option<ChunkSigner>,
    pool: Option<&BlockingPool>,
) -> Result<Body, S3Error> {
    let (signed, trailer) = match payload {
        Payload::Unsigned => return Ok(body),
        Payload::Sha256(expected) => {
            return Ok(Body::http_body(HashedBody::new(body, expected, pool)));
        }
        Payload::Chunked { signed, trailer } => (signed, trailer),
    };
    let signer = match (signed, signer) {
        (true, Some(signer)) => Some(signer),
        (true, None) => {
            return Err(s3_error!(
                AccessDenied,
                "A body with signed chunks needs a signed request."
            ));
        }
        (false, _) => None,
    };
    let decoded_length = decoded_length(&parts.headers)?;
    let expected = if trailer {
        declared_trailers(&parts.headers)?
    } else {
        Vec::new()
    };
    let headers = &mut parts.headers;
    headers.insert(header::CONTENT_LENGTH, HeaderValue::from(decoded_length));
    headers.remove(DECODED_LENGTH);
    headers.remove(header::TRANSFER_ENCODING);
    remove_aws_chunked(headers);
    let trailers = Trailers::default();
    if trailer {
        parts.extensions.insert(trailers.clone());
    }
    let decoder = Decoder::new(signer, trailer, expected, decoded_length);
    Ok(Body::http_body(AwsChunkedBody::new(
        body,
        decoder,
        decoded_length,
        trailers,
    )))
}

fn decoded_length(headers: &HeaderMap) -> Result<u64, S3Error> {
    let mut values = headers.get_all(DECODED_LENGTH).iter();
    let (Some(value), None) = (values.next(), values.next()) else {
        return Err(s3_error!(
            MissingContentLength,
            "An aws-chunked body needs one x-amz-decoded-content-length header."
        ));
    };
    let length = (value.len() <= 20 && value.as_bytes().iter().all(u8::is_ascii_digit))
        .then(|| value.to_str().ok()?.parse::<u64>().ok())
        .flatten()
        .ok_or_else(|| {
            s3_error!(
                InvalidArgument,
                "x-amz-decoded-content-length is not a number."
            )
        })?;
    if length > MAX_SINGLE_PUT_BYTES {
        return Err(s3_error!(
            EntityTooLarge,
            "Your proposed upload exceeds the maximum allowed size"
        ));
    }
    Ok(length)
}

/// The trailer names `x-amz-trailer` declares.
fn declared_trailers(headers: &HeaderMap) -> Result<Vec<HeaderName>, S3Error> {
    let mut names: Vec<HeaderName> = Vec::new();
    for value in headers.get_all(TRAILER) {
        for name in value.as_bytes().split(|&b| b == b',') {
            let name = name.trim_ascii();
            let name = HeaderName::from_bytes(&name.to_ascii_lowercase())
                .ok()
                .filter(|name| name != "x-amz-trailer-signature" && !names.contains(name))
                .ok_or_else(|| s3_error!(InvalidArgument, "x-amz-trailer is not valid."))?;
            names.push(name);
        }
    }
    if names.len() > MAX_TRAILERS {
        return Err(s3_error!(
            InvalidArgument,
            "x-amz-trailer declares more than {MAX_TRAILERS} trailers."
        ));
    }
    Ok(names)
}

/// Removes `aws-chunked` from `Content-Encoding`, and the header if
/// nothing else is left, as S3 does before it stores the object's
/// `Content-Encoding`.
fn remove_aws_chunked(headers: &mut HeaderMap) {
    let codings: Vec<Vec<u8>> = headers
        .get_all(header::CONTENT_ENCODING)
        .iter()
        .flat_map(|value| value.as_bytes().split(|&b| b == b','))
        .map(<[u8]>::trim_ascii)
        .filter(|coding| !coding.is_empty() && !coding.eq_ignore_ascii_case(b"aws-chunked"))
        .map(<[u8]>::to_vec)
        .collect();
    headers.remove(header::CONTENT_ENCODING);
    if !codings.is_empty()
        && let Ok(value) = HeaderValue::from_bytes(&codings.join(&b", "[..]))
    {
        headers.insert(header::CONTENT_ENCODING, value);
    }
}

/// The kinds of [`BodyError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BodyErrorKind {
    /// The body's SHA-256 is not the one `x-amz-content-sha256` declared.
    Sha256Mismatch,
    /// A chunk or trailer signature does not match.
    SignatureMismatch,
    /// The `aws-chunked` framing is malformed, or holds more data than
    /// declared.
    Malformed,
    /// The body ended early, or holds less data than declared.
    Incomplete,
    /// The trailers are malformed or are not the declared ones.
    MalformedTrailer,
}

/// Why a request body failed its SigV4 checks while it was read.
///
/// A body the authenticator wrapped yields this error, boxed, from the
/// read that found the problem. [`BodyError::to_s3_error`] is the answer to
/// give; the request must have no effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BodyError {
    kind: BodyErrorKind,
    message: Cow<'static, str>,
}

impl BodyError {
    fn new(kind: BodyErrorKind, message: impl Into<Cow<'static, str>>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub(crate) fn signature() -> Self {
        Self::new(
            BodyErrorKind::SignatureMismatch,
            "a chunk or trailer signature does not match",
        )
    }

    pub(crate) fn malformed(message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(BodyErrorKind::Malformed, message)
    }

    pub(crate) fn incomplete(message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(BodyErrorKind::Incomplete, message)
    }

    pub(crate) fn trailer(message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(BodyErrorKind::MalformedTrailer, message)
    }

    /// What went wrong.
    #[must_use]
    pub fn kind(&self) -> BodyErrorKind {
        self.kind
    }

    /// Finds a `BodyError` in `error` or its sources.
    #[must_use]
    pub fn find<'a>(error: &'a (dyn std::error::Error + 'static)) -> Option<&'a Self> {
        let mut next = Some(error);
        while let Some(error) = next {
            if let Some(found) = error.downcast_ref::<Self>() {
                return Some(found);
            }
            next = error.source();
        }
        None
    }

    /// The S3 error to answer with.
    #[must_use]
    pub fn to_s3_error(&self) -> S3Error {
        let (code, status) = match self.kind {
            BodyErrorKind::Sha256Mismatch => {
                let code = S3ErrorCode::from_bytes(b"XAmzContentSHA256Mismatch");
                (code, Some(http::StatusCode::BAD_REQUEST))
            }
            BodyErrorKind::SignatureMismatch => (Some(S3ErrorCode::SignatureDoesNotMatch), None),
            BodyErrorKind::Malformed => (Some(S3ErrorCode::InvalidRequest), None),
            BodyErrorKind::Incomplete => (Some(S3ErrorCode::IncompleteBody), None),
            BodyErrorKind::MalformedTrailer => {
                let code = S3ErrorCode::from_bytes(b"MalformedTrailerError");
                (code, Some(http::StatusCode::BAD_REQUEST))
            }
        };
        let mut error = S3Error::with_message(
            code.unwrap_or(S3ErrorCode::InvalidRequest),
            self.to_string(),
        );
        if let Some(status) = status {
            error.set_status_code(status);
        }
        error
    }
}

impl fmt::Display for BodyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let prefix = match self.kind {
            BodyErrorKind::Sha256Mismatch => "The body does not match x-amz-content-sha256",
            BodyErrorKind::SignatureMismatch => "The body's signature does not match",
            BodyErrorKind::Malformed => "The aws-chunked body is malformed",
            BodyErrorKind::Incomplete => "The body is incomplete",
            BodyErrorKind::MalformedTrailer => "The body's trailers are malformed",
        };
        write!(f, "{prefix}: {}.", self.message)
    }
}

impl std::error::Error for BodyError {}

/// A body checked against the SHA-256 its request declared, when it ends.
/// The hashing runs on the authenticator's hashing pool, if it has one.
struct HashedBody {
    inner: Body,
    hasher: PooledHasher,
    expected: [u8; 32],
    inner_ended: bool,
    done: bool,
}

impl HashedBody {
    fn new(inner: Body, expected: [u8; 32], pool: Option<&BlockingPool>) -> Self {
        Self {
            inner,
            hasher: PooledHasher::new([ChecksumAlgorithm::Sha256], pool.cloned()),
            expected,
            inner_ended: false,
            done: false,
        }
    }
}

impl http_body::Body for HashedBody {
    type Data = Bytes;
    type Error = StdError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, StdError>>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        if !this.inner_ended {
            if let Err(closed) = ready!(this.hasher.poll_ready(cx)) {
                this.done = true;
                return Poll::Ready(Some(Err(closed.into())));
            }
            match ready!(Pin::new(&mut this.inner).poll_frame(cx)) {
                Some(Ok(frame)) => {
                    if let Some(data) = frame.data_ref() {
                        this.hasher.push(data.clone());
                    }
                    return Poll::Ready(Some(Ok(frame)));
                }
                Some(Err(error)) => {
                    this.done = true;
                    return Poll::Ready(Some(Err(error)));
                }
                None => this.inner_ended = true,
            }
        }
        let digests = ready!(this.hasher.poll_finish(cx));
        this.done = true;
        let matches = digests.map(|digests| {
            digests.get(&ChecksumAlgorithm::Sha256).map(Vec::as_slice) == Some(&this.expected[..])
        });
        match matches {
            Ok(true) => Poll::Ready(None),
            Ok(false) => {
                let error =
                    BodyError::new(BodyErrorKind::Sha256Mismatch, "the computed hash differs");
                Poll::Ready(Some(Err(error.into())))
            }
            Err(closed) => Poll::Ready(Some(Err(closed.into()))),
        }
    }

    fn is_end_stream(&self) -> bool {
        self.done
    }

    fn size_hint(&self) -> SizeHint {
        http_body::Body::size_hint(&self.inner)
    }
}

#[cfg(test)]
mod tests {
    use http::Request;
    use http_body_util::BodyExt;

    use super::*;
    use crate::sigv4::canonical::{EMPTY_SHA256, sha256};

    fn parts(headers: &[(&str, &str)]) -> Parts {
        let mut request = Request::put("/b/k");
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        request.body(()).unwrap().into_parts().0
    }

    fn declared(value: &str) -> Result<Option<Payload>, S3Error> {
        Payload::declared(&parts(&[(CONTENT_SHA256, value)]).headers)
    }

    #[test]
    fn payload_hashes_are_classified() {
        assert_eq!(Payload::declared(&HeaderMap::new()).unwrap(), None);
        assert_eq!(
            declared("UNSIGNED-PAYLOAD").unwrap(),
            Some(Payload::Unsigned)
        );
        assert_eq!(
            declared("STREAMING-UNSIGNED-PAYLOAD-TRAILER").unwrap(),
            Some(Payload::Chunked {
                signed: false,
                trailer: true
            })
        );
        assert_eq!(
            declared(&EMPTY_SHA256.to_uppercase()).unwrap(),
            Some(Payload::Sha256(sha256(b"")))
        );
        let code = |value: &str| declared(value).unwrap_err().code().clone();
        assert_eq!(code("abc"), S3ErrorCode::InvalidArgument);
        assert_eq!(code("unsigned-payload"), S3ErrorCode::InvalidArgument);
        assert_eq!(
            code("STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD"),
            S3ErrorCode::InvalidRequest
        );
        let twice = parts(&[
            (CONTENT_SHA256, EMPTY_SHA256),
            (CONTENT_SHA256, EMPTY_SHA256),
        ]);
        assert!(Payload::declared(&twice.headers).is_err());
    }

    #[test]
    fn chunked_heads_describe_the_decoded_body() {
        let mut head = parts(&[
            ("content-length", "100"),
            ("content-encoding", "aws-chunked, gzip"),
            ("transfer-encoding", "chunked"),
            (DECODED_LENGTH, "5"),
            (TRAILER, "x-amz-checksum-crc32, X-Amz-Checksum-Sha256"),
        ]);
        let payload = Payload::Chunked {
            signed: false,
            trailer: true,
        };
        wrap(&mut head, Body::empty(), payload, None, None).unwrap();
        assert_eq!(head.headers["content-length"], "5");
        assert_eq!(head.headers["content-encoding"], "gzip");
        assert!(!head.headers.contains_key(DECODED_LENGTH));
        assert!(!head.headers.contains_key("transfer-encoding"));
        assert!(head.extensions.get::<Trailers>().is_some());
        let mut only = parts(&[("content-encoding", "AWS-CHUNKED"), (DECODED_LENGTH, "0")]);
        let signed = Payload::Chunked {
            signed: false,
            trailer: false,
        };
        wrap(&mut only, Body::empty(), signed, None, None).unwrap();
        assert!(!only.headers.contains_key("content-encoding"));
        assert!(only.extensions.get::<Trailers>().is_none());
    }

    #[test]
    fn chunked_heads_are_checked() {
        let unsigned = Payload::Chunked {
            signed: false,
            trailer: true,
        };
        let code = |headers: &[(&str, &str)], payload| {
            wrap(&mut parts(headers), Body::empty(), payload, None, None)
                .unwrap_err()
                .code()
                .clone()
        };
        assert_eq!(code(&[], unsigned), S3ErrorCode::MissingContentLength);
        assert_eq!(
            code(&[(DECODED_LENGTH, "1"), (DECODED_LENGTH, "1")], unsigned),
            S3ErrorCode::MissingContentLength
        );
        assert_eq!(
            code(&[(DECODED_LENGTH, "-1")], unsigned),
            S3ErrorCode::InvalidArgument
        );
        let huge = (MAX_SINGLE_PUT_BYTES + 1).to_string();
        assert_eq!(
            code(&[(DECODED_LENGTH, &huge)], unsigned),
            S3ErrorCode::EntityTooLarge
        );
        for bad in ["a b", "x-amz-trailer-signature", "x-a,x-a", "a,,b"] {
            assert_eq!(
                code(&[(DECODED_LENGTH, "1"), (TRAILER, bad)], unsigned),
                S3ErrorCode::InvalidArgument,
                "{bad}"
            );
        }
        let many = (0..=MAX_TRAILERS)
            .map(|n| format!("x-{n}"))
            .collect::<Vec<_>>()
            .join(",");
        assert_eq!(
            code(&[(DECODED_LENGTH, "1"), (TRAILER, &many)], unsigned),
            S3ErrorCode::InvalidArgument
        );
        let signed = Payload::Chunked {
            signed: true,
            trailer: false,
        };
        assert_eq!(
            code(&[(DECODED_LENGTH, "1")], signed),
            S3ErrorCode::AccessDenied
        );
    }

    async fn read(body: Body) -> Result<Bytes, StdError> {
        Ok(body.collect().await?.to_bytes())
    }

    fn pool() -> BlockingPool {
        BlockingPool::new("test-hash", std::num::NonZeroUsize::new(2).unwrap()).unwrap()
    }

    /// A body that yields these frames.
    struct Frames(std::collections::VecDeque<Result<Frame<Bytes>, StdError>>);

    impl http_body::Body for Frames {
        type Data = Bytes;
        type Error = StdError;

        fn poll_frame(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, StdError>>> {
            Poll::Ready(self.get_mut().0.pop_front())
        }
    }

    /// A body of `data` in frames of `frame` bytes.
    fn framed(data: &[u8], frame: usize) -> Body {
        let frames = data
            .chunks(frame)
            .map(|chunk| Ok(Frame::data(Bytes::copy_from_slice(chunk))))
            .collect();
        Body::http_body(Frames(frames))
    }

    #[tokio::test]
    async fn hashed_bodies_are_checked_at_their_end() {
        let pool = pool();
        for pool in [None, Some(&pool)] {
            let mut head = parts(&[]);
            let good = wrap(
                &mut head,
                Body::from(Bytes::from_static(b"abc")),
                Payload::Sha256(sha256(b"abc")),
                None,
                pool,
            )
            .unwrap();
            assert_eq!(read(good).await.unwrap(), "abc");
            let bad = wrap(
                &mut head,
                Body::from(Bytes::from_static(b"abd")),
                Payload::Sha256(sha256(b"abc")),
                None,
                pool,
            )
            .unwrap();
            let error = read(bad).await.unwrap_err();
            let found = BodyError::find(&*error).unwrap();
            assert_eq!(found.kind(), BodyErrorKind::Sha256Mismatch);
            let s3 = found.to_s3_error();
            assert_eq!(s3.code().as_str(), "XAmzContentSHA256Mismatch");
            assert_eq!(s3.status_code(), Some(http::StatusCode::BAD_REQUEST));
            let unsigned = wrap(
                &mut head,
                Body::from(Bytes::from_static(b"x")),
                Payload::Unsigned,
                None,
                pool,
            )
            .unwrap();
            assert_eq!(read(unsigned).await.unwrap(), "x");
        }
    }

    #[tokio::test]
    async fn large_bodies_hash_on_the_pool_in_batches() {
        let pool = pool();
        let data: Vec<u8> = (0..3 * crate::checksum::HASH_BATCH_BYTES + 12_345)
            .map(|i| (i % 251) as u8)
            .collect();
        let mut head = parts(&[]);
        let body = wrap(
            &mut head,
            framed(&data, 10_000),
            Payload::Sha256(sha256(&data)),
            None,
            Some(&pool),
        )
        .unwrap();
        assert_eq!(read(body).await.unwrap(), data);
        let mut corrupt = data.clone();
        corrupt[HASH_LAST] ^= 1;
        let body = wrap(
            &mut head,
            framed(&corrupt, 7_777),
            Payload::Sha256(sha256(&data)),
            None,
            Some(&pool),
        )
        .unwrap();
        let error = read(body).await.unwrap_err();
        assert!(BodyError::find(&*error).is_some());
    }

    const HASH_LAST: usize = 3 * crate::checksum::HASH_BATCH_BYTES + 12_000;

    #[tokio::test]
    async fn a_closed_pool_fails_the_body() {
        let pool = pool();
        pool.shutdown();
        let mut head = parts(&[]);
        let data = vec![1; 2 * crate::checksum::HASH_BATCH_BYTES];
        let body = wrap(
            &mut head,
            framed(&data, 64 * 1024),
            Payload::Sha256(sha256(&data)),
            None,
            Some(&pool),
        )
        .unwrap();
        let error = read(body).await.unwrap_err();
        assert!(
            error.downcast_ref::<skys3_io::PoolClosed>().is_some(),
            "{error}"
        );
        // A body whose source fails ends with that failure.
        let failing: Vec<Result<Frame<Bytes>, StdError>> = vec![Err("lost".into())];
        let body = wrap(
            &mut head,
            Body::http_body(Frames(failing.into())),
            Payload::Sha256(sha256(b"")),
            None,
            None,
        )
        .unwrap();
        assert_eq!(read(body).await.unwrap_err().to_string(), "lost");
    }

    #[test]
    fn body_errors_map_to_s3_errors() {
        for (error, code, status) in [
            (BodyError::signature(), "SignatureDoesNotMatch", 403),
            (BodyError::malformed("x"), "InvalidRequest", 400),
            (BodyError::incomplete("x"), "IncompleteBody", 400),
            (BodyError::trailer("x"), "MalformedTrailerError", 400),
        ] {
            let s3 = error.to_s3_error();
            assert_eq!(s3.code().as_str(), code);
            let actual = s3.status_code().or_else(|| s3.code().status_code());
            assert_eq!(actual.map(|s| s.as_u16()), Some(status), "{code}");
        }
        #[derive(Debug)]
        struct Wrapper(BodyError);
        impl fmt::Display for Wrapper {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("wrapped")
            }
        }
        impl std::error::Error for Wrapper {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.0)
            }
        }
        let nested = Wrapper(BodyError::incomplete("x"));
        assert_eq!(
            BodyError::find(&nested).unwrap().kind(),
            BodyErrorKind::Incomplete
        );
        assert!(BodyError::find(&std::io::Error::other("x")).is_none());
    }
}
