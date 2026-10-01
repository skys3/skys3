//! Why a body failed its integrity checks, and the S3 answer for each case.

use s3s::{S3Error, S3ErrorCode};
use skys3_types::checksum::ChecksumAlgorithm;

/// Why a request's checksums were refused. [`IntegrityError::to_s3_error`]
/// gives the answer; the request must have no effect.
///
/// The [module documentation](super) maps each case to S3's answer.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum IntegrityError {
    /// `Content-MD5` is not the base64 of 16 bytes (`InvalidDigest`).
    #[error("The Content-MD5 you specified is not valid.")]
    InvalidDigest,
    /// A digest does not match the body (`BadDigest`). A checksum value
    /// that is not base64 at all can match nothing, so it gets this answer
    /// too.
    #[error("{}", bad_digest_message(*.0))]
    BadDigest(ChecksumAlgorithm),
    /// An `x-amz-checksum-*` value is base64 of the wrong length
    /// (`InvalidRequest`).
    #[error("Value for {} header is invalid.", .0.header_name())]
    InvalidValue(ChecksumAlgorithm),
    /// The request names more than one `x-amz-checksum-*` value, in headers
    /// and trailers together (`InvalidRequest`).
    #[error("Expecting a single x-amz-checksum- header. Multiple checksum Types are not allowed.")]
    MultipleChecksums,
    /// `x-amz-sdk-checksum-algorithm` names an algorithm the request has
    /// no value for (`InvalidRequest`).
    #[error(
        "x-amz-sdk-checksum-algorithm specified, but no corresponding x-amz-checksum-* or \
         x-amz-trailer headers were found."
    )]
    MissingChecksum,
    /// A checksum algorithm name is not one S3 defines (`InvalidRequest`).
    #[error("{0:?} is not a valid checksum algorithm.")]
    UnknownAlgorithm(String),
    /// An algorithm S3 defines but SkyS3 does not support, such as SHA512
    /// or XXHASH64 (`NotImplemented`).
    #[error("The checksum algorithm {0} is not implemented.")]
    UnsupportedAlgorithm(String),
    /// A trailing checksum is declared, but the body is not an
    /// `aws-chunked` body with trailers (`InvalidRequest`).
    #[error("A trailing checksum needs an aws-chunked body with trailers.")]
    NoTrailers,
    /// The blocking pool was shut down: the node is stopping
    /// (`ServiceUnavailable`).
    #[error("The server is shutting down; please retry.")]
    Unavailable,
}

impl IntegrityError {
    /// The S3 error to answer with.
    #[must_use]
    pub fn to_s3_error(&self) -> S3Error {
        let code = match self {
            Self::InvalidDigest => S3ErrorCode::InvalidDigest,
            Self::BadDigest(_) => S3ErrorCode::BadDigest,
            Self::InvalidValue(_)
            | Self::MultipleChecksums
            | Self::MissingChecksum
            | Self::UnknownAlgorithm(_)
            | Self::NoTrailers => S3ErrorCode::InvalidRequest,
            Self::UnsupportedAlgorithm(_) => S3ErrorCode::NotImplemented,
            Self::Unavailable => S3ErrorCode::ServiceUnavailable,
        };
        S3Error::with_message(code, self.to_string())
    }
}

impl From<IntegrityError> for S3Error {
    fn from(error: IntegrityError) -> Self {
        error.to_s3_error()
    }
}

fn bad_digest_message(algorithm: ChecksumAlgorithm) -> String {
    match algorithm {
        ChecksumAlgorithm::Md5 => {
            "The Content-MD5 you specified did not match what we received.".to_owned()
        }
        other => format!("The {other} you specified did not match the calculated checksum."),
    }
}
