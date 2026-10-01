//! What a request says about its body's checksums, and the validator that
//! holds the body to it.

use bytes::Bytes;
use http::Extensions;
use http::header::{HeaderMap, HeaderValue};
use skys3_io::BlockingPool;
use skys3_types::ETag;
use skys3_types::checksum::{Checksum, ChecksumAlgorithm, Checksums, decode_digest};

use super::error::IntegrityError;
use super::pooled::PooledHasher;
use crate::sigv4::Trailers;

/// The algorithm of the checksum stored with an object uploaded without
/// one, as S3 adds it.
pub const DEFAULT_ALGORITHM: ChecksumAlgorithm = ChecksumAlgorithm::Crc64Nvme;

const CONTENT_MD5: &str = "content-md5";
const SDK_ALGORITHM: &str = "x-amz-sdk-checksum-algorithm";
const TRAILER: &str = "x-amz-trailer";
const CHECKSUM_PREFIX: &str = "x-amz-checksum-";

/// Headers under [`CHECKSUM_PREFIX`] that carry no checksum value.
const CHECKSUM_SETTINGS: [&str; 3] = ["type", "algorithm", "mode"];

/// Algorithms S3 defines that SkyS3 does not support, lowercase.
const UNSUPPORTED: [&str; 5] = ["md5", "sha512", "xxhash3", "xxhash64", "xxhash128"];

/// The checksum values a request supplies for its body.
///
/// [`ExpectedChecksums::from_headers`] reads them from `Content-MD5`, at
/// most one `x-amz-checksum-*` header or trailer that `x-amz-trailer`
/// declares, and `x-amz-sdk-checksum-algorithm`, which must name the same
/// algorithm.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExpectedChecksums {
    content_md5: Option<[u8; 16]>,
    checksum: Option<ExpectedChecksum>,
}

/// Where a request's `x-amz-checksum-*` value is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpectedChecksum {
    /// In a header: the digest.
    Header(ChecksumAlgorithm, Vec<u8>),
    /// In a trailer of an `aws-chunked` body, known once the body ends.
    Trailer(ChecksumAlgorithm),
}

impl ExpectedChecksum {
    /// The algorithm.
    #[must_use]
    pub fn algorithm(&self) -> ChecksumAlgorithm {
        match self {
            Self::Header(algorithm, _) | Self::Trailer(algorithm) => *algorithm,
        }
    }
}

impl ExpectedChecksums {
    /// Reads a request's checksum headers.
    ///
    /// # Errors
    ///
    /// An [`IntegrityError`]: `InvalidDigest` for a malformed `Content-MD5`;
    /// `BadDigest` for a checksum value that is not base64, or that names
    /// another algorithm than `x-amz-sdk-checksum-algorithm`; and the
    /// `InvalidRequest` and `NotImplemented` cases of the [module
    /// documentation](super).
    pub fn from_headers(headers: &HeaderMap) -> Result<Self, IntegrityError> {
        let content_md5 = content_md5(headers)?;
        let mut checksum = None;
        let mut set = |found: ExpectedChecksum| match checksum.replace(found) {
            Some(_) => Err(IntegrityError::MultipleChecksums),
            None => Ok(()),
        };
        for (name, value) in headers {
            let Some(algorithm) = checksum_header(name.as_str())? else {
                continue;
            };
            set(ExpectedChecksum::Header(
                algorithm,
                parse_value(algorithm, value)?,
            ))?;
        }
        for value in headers.get_all(TRAILER) {
            for name in value.as_bytes().split(|&b| b == b',') {
                let name = String::from_utf8_lossy(name.trim_ascii()).to_ascii_lowercase();
                if let Some(algorithm) = checksum_header(&name)? {
                    set(ExpectedChecksum::Trailer(algorithm))?;
                }
            }
        }
        if let Some(named) = sdk_algorithm(headers)? {
            match &checksum {
                None => return Err(IntegrityError::MissingChecksum),
                Some(found) if found.algorithm() != named => {
                    return Err(IntegrityError::BadDigest(found.algorithm()));
                }
                Some(_) => {}
            }
        }
        Ok(Self {
            content_md5,
            checksum,
        })
    }

    /// The `Content-MD5` digest, if the request has one.
    #[must_use]
    pub fn content_md5(&self) -> Option<&[u8; 16]> {
        self.content_md5.as_ref()
    }

    /// The `x-amz-checksum-*` value, if the request has one.
    #[must_use]
    pub fn checksum(&self) -> Option<&ExpectedChecksum> {
        self.checksum.as_ref()
    }

    /// The algorithm of the checksum stored with the body: the request's,
    /// or [`DEFAULT_ALGORITHM`].
    #[must_use]
    pub fn stored_algorithm(&self) -> ChecksumAlgorithm {
        self.checksum
            .as_ref()
            .map_or(DEFAULT_ALGORITHM, ExpectedChecksum::algorithm)
    }
}

fn content_md5(headers: &HeaderMap) -> Result<Option<[u8; 16]>, IntegrityError> {
    let mut values = headers.get_all(CONTENT_MD5).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(IntegrityError::InvalidDigest);
    }
    value
        .to_str()
        .ok()
        .and_then(|text| decode_digest(text).ok())
        .and_then(|digest| <[u8; 16]>::try_from(digest).ok())
        .map(Some)
        .ok_or(IntegrityError::InvalidDigest)
}

/// The algorithm of a checksum header or trailer name, if it is one.
fn checksum_header(name: &str) -> Result<Option<ChecksumAlgorithm>, IntegrityError> {
    let Some(suffix) = name.strip_prefix(CHECKSUM_PREFIX) else {
        return Ok(None);
    };
    if CHECKSUM_SETTINGS.contains(&suffix) {
        return Ok(None);
    }
    if UNSUPPORTED.contains(&suffix) {
        return Err(IntegrityError::UnsupportedAlgorithm(
            suffix.to_ascii_uppercase(),
        ));
    }
    // Other names are not S3 headers; S3 ignores unknown `x-amz-` headers.
    Ok(ChecksumAlgorithm::from_header_name(name))
}

/// The algorithm `x-amz-sdk-checksum-algorithm` names, if any.
fn sdk_algorithm(headers: &HeaderMap) -> Result<Option<ChecksumAlgorithm>, IntegrityError> {
    let mut values = headers.get_all(SDK_ALGORITHM).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    let name = String::from_utf8_lossy(value.as_bytes()).into_owned();
    if values.next().is_some() {
        return Err(IntegrityError::UnknownAlgorithm(name));
    }
    if UNSUPPORTED.iter().any(|u| u.eq_ignore_ascii_case(&name)) {
        return Err(IntegrityError::UnsupportedAlgorithm(
            name.to_ascii_uppercase(),
        ));
    }
    name.parse()
        .map(Some)
        .map_err(|_| IntegrityError::UnknownAlgorithm(name))
}

/// Parses an `x-amz-checksum-*` header or trailer value.
fn parse_value(
    algorithm: ChecksumAlgorithm,
    value: &HeaderValue,
) -> Result<Vec<u8>, IntegrityError> {
    let digest = value
        .to_str()
        .ok()
        .and_then(|text| decode_digest(text).ok())
        .ok_or(IntegrityError::BadDigest(algorithm))?;
    if digest.len() == algorithm.digest_len() {
        Ok(digest)
    } else {
        Err(IntegrityError::InvalidValue(algorithm))
    }
}

/// What a body that passed its checks is: its length, ETag, and the
/// checksums to store with it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedBody {
    /// The body's length, in bytes.
    pub length: u64,
    /// The body's MD5.
    pub md5: [u8; 16],
    /// The ETag S3 gives the body as an object or a part: its MD5 in hex.
    pub etag: ETag,
    /// The checksums to store: the `Content-MD5` digest if the request had
    /// one, and its `x-amz-checksum-*` value, or a computed
    /// [`DEFAULT_ALGORITHM`] checksum if it had none. Each is
    /// `FULL_OBJECT`.
    pub checksums: Checksums,
}

/// Checks a request body against the checksums its request supplies, while
/// the body streams, hashing on a blocking pool.
///
/// Feed every chunk of the decoded body to [`ChecksumValidator::update`] in
/// order, then call [`ChecksumValidator::finish`] once the body has been
/// read to its end without error, which is also when its trailers are
/// known. Nothing may be committed before `finish` succeeds.
///
/// ```
/// use std::num::NonZeroUsize;
///
/// use bytes::Bytes;
/// use skys3_gateway::checksum::{ChecksumValidator, ExpectedChecksums};
/// use skys3_io::BlockingPool;
///
/// # tokio::runtime::Builder::new_current_thread().build()?.block_on(async {
/// let pool = BlockingPool::new("hash", NonZeroUsize::MIN)?;
/// let mut headers = http::HeaderMap::new();
/// headers.insert("x-amz-checksum-crc32", "DUoRhQ==".parse()?);
/// let expected = ExpectedChecksums::from_headers(&headers)?;
/// let mut validator = ChecksumValidator::new(expected, None, &pool)?;
/// validator.update(Bytes::from_static(b"hello ")).await?;
/// validator.update(Bytes::from_static(b"world")).await?;
/// let body = validator.finish().await?;
/// assert_eq!(body.etag.as_str(), "5eb63bbbe01eeed093cb22bb8f5acdc3");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// # })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug)]
pub struct ChecksumValidator {
    expected: ExpectedChecksums,
    trailers: Option<Trailers>,
    hasher: PooledHasher,
    length: u64,
}

impl ChecksumValidator {
    /// A validator for a body whose request supplied `expected`, and whose
    /// trailers, if it has any, arrive in `trailers`.
    ///
    /// # Errors
    ///
    /// [`IntegrityError::NoTrailers`] if a trailing checksum is expected
    /// but the body has no trailers.
    pub fn new(
        expected: ExpectedChecksums,
        trailers: Option<Trailers>,
        pool: &BlockingPool,
    ) -> Result<Self, IntegrityError> {
        if matches!(expected.checksum, Some(ExpectedChecksum::Trailer(_))) && trailers.is_none() {
            return Err(IntegrityError::NoTrailers);
        }
        let algorithms = [ChecksumAlgorithm::Md5, expected.stored_algorithm()];
        Ok(Self {
            hasher: PooledHasher::new(algorithms, Some(pool.clone())),
            expected,
            trailers,
            length: 0,
        })
    }

    /// A validator for a request's body, from its headers and the
    /// [`Trailers`] extension the authenticator adds.
    ///
    /// # Errors
    ///
    /// The errors of [`ExpectedChecksums::from_headers`] and
    /// [`ChecksumValidator::new`].
    pub fn for_request(
        headers: &HeaderMap,
        extensions: &Extensions,
        pool: &BlockingPool,
    ) -> Result<Self, IntegrityError> {
        let expected = ExpectedChecksums::from_headers(headers)?;
        Self::new(expected, extensions.get::<Trailers>().cloned(), pool)
    }

    /// Feeds the next chunk of the body. It may wait for the pool when
    /// the caller reads faster than the pool hashes.
    ///
    /// # Errors
    ///
    /// [`IntegrityError::Unavailable`] if the pool was shut down.
    pub async fn update(&mut self, data: Bytes) -> Result<(), IntegrityError> {
        self.length += data.len() as u64;
        self.hasher
            .update(data)
            .await
            .map_err(|_| IntegrityError::Unavailable)
    }

    /// Finishes hashing and checks every supplied checksum.
    ///
    /// # Errors
    ///
    /// [`IntegrityError::BadDigest`] for a digest that does not match,
    /// [`IntegrityError::InvalidValue`] for a malformed trailing checksum,
    /// [`IntegrityError::NoTrailers`] if the expected trailer has not
    /// arrived because the body was not read to its end, and
    /// [`IntegrityError::Unavailable`] if the pool was shut down.
    pub async fn finish(self) -> Result<VerifiedBody, IntegrityError> {
        let mut digests = self
            .hasher
            .finish()
            .await
            .map_err(|_| IntegrityError::Unavailable)?;
        let md5 = digests
            .remove(&ChecksumAlgorithm::Md5)
            .and_then(|digest| <[u8; 16]>::try_from(digest).ok())
            .ok_or(IntegrityError::Unavailable)?;
        let mut checksums = Checksums::new();
        if let Some(expected) = self.expected.content_md5 {
            if expected != md5 {
                return Err(IntegrityError::BadDigest(ChecksumAlgorithm::Md5));
            }
            checksums.insert(
                ChecksumAlgorithm::Md5,
                full_object(ChecksumAlgorithm::Md5, &md5),
            );
        }
        let algorithm = self.expected.stored_algorithm();
        let computed = digests
            .remove(&algorithm)
            .ok_or(IntegrityError::Unavailable)?;
        let supplied = match &self.expected.checksum {
            None => None,
            Some(ExpectedChecksum::Header(_, digest)) => Some(digest.clone()),
            Some(ExpectedChecksum::Trailer(_)) => {
                let value = self
                    .trailers
                    .as_ref()
                    .and_then(Trailers::get)
                    .and_then(|trailers| trailers.get(algorithm.header_name()))
                    .ok_or(IntegrityError::NoTrailers)?;
                Some(parse_value(algorithm, value)?)
            }
        };
        if supplied.is_some_and(|supplied| supplied != computed) {
            return Err(IntegrityError::BadDigest(algorithm));
        }
        checksums.insert(algorithm, full_object(algorithm, &computed));
        Ok(VerifiedBody {
            length: self.length,
            md5,
            etag: ETag::from_md5(&md5),
            checksums,
        })
    }
}

fn full_object(algorithm: ChecksumAlgorithm, digest: &[u8]) -> Checksum {
    Checksum::full_object(algorithm, digest).expect("a computed digest has its algorithm's length")
}
