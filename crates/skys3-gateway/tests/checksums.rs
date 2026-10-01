//! Checksums at the protocol boundary: requests signed by the AWS SDK's
//! signer pass the SigV4 authenticator, hashing on a pool, and their
//! bodies are checked against the checksums the requests supply.

mod common;

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use common::signing::{NOW, Signing, credentials, sdk_chunked, sdk_signed};
use http::Method;
use http::Request;
use http_body_util::BodyExt;
use s3s::Body;
use skys3_gateway::checksum::{ChecksumValidator, IntegrityError, VerifiedBody, digest};
use skys3_gateway::sigv4::MemoryCredentials;
use skys3_gateway::{Authenticator, BodyError, SigV4Authenticator};
use skys3_io::{BlockingPool, ManualWallClock};
use skys3_types::checksum::{ChecksumAlgorithm, encode_digest};

fn pool() -> BlockingPool {
    BlockingPool::new("hash", NonZeroUsize::new(2).unwrap()).unwrap()
}

fn authenticator(pool: &BlockingPool) -> SigV4Authenticator<MemoryCredentials> {
    let clock = ManualWallClock::new(Duration::from_secs(NOW));
    SigV4Authenticator::new(credentials(), Arc::new(clock)).with_hashing_pool(pool.clone())
}

/// Error from reading the body, or from the checksum checks.
#[derive(Debug)]
enum Failure {
    Body(BodyError),
    Integrity(IntegrityError),
}

/// Authenticates `request`, then reads its body through a validator, as
/// an object operation does.
async fn upload(request: Request<Body>, pool: &BlockingPool) -> Result<VerifiedBody, Failure> {
    let request = authenticator(pool).authenticate(request).await.unwrap();
    let (parts, mut body) = request.into_parts();
    let mut validator = ChecksumValidator::for_request(&parts.headers, &parts.extensions, pool)
        .map_err(Failure::Integrity)?;
    while let Some(frame) = body.frame().await {
        let frame =
            frame.map_err(|error| Failure::Body(BodyError::find(&*error).unwrap().clone()))?;
        if let Ok(data) = frame.into_data() {
            validator.update(data).await.map_err(Failure::Integrity)?;
        }
    }
    validator.finish().await.map_err(Failure::Integrity)
}

#[tokio::test]
async fn signed_chunked_uploads_with_trailing_checksums() {
    let pool = pool();
    let data: Vec<u8> = (0..300_000_u32).map(|n| (n % 251) as u8).collect();
    let crc = encode_digest(&digest(ChecksumAlgorithm::Crc32c, &data));
    let trailer = Some(("x-amz-checksum-crc32c", crc.as_str()));
    let (request, _) = sdk_chunked("/bucket/object", &data, 65_536, trailer);
    let body = upload(request, &pool).await.unwrap();
    assert_eq!(body.length, data.len() as u64);
    assert_eq!(body.checksums[&ChecksumAlgorithm::Crc32c].to_string(), crc);
    assert_eq!(body.etag.md5(), Some(body.md5));

    // A signed trailer with the wrong checksum: the signature holds, the
    // checksum does not.
    let wrong = encode_digest(&digest(ChecksumAlgorithm::Crc32c, b"other"));
    let trailer = Some(("x-amz-checksum-crc32c", wrong.as_str()));
    let (request, _) = sdk_chunked("/bucket/object", &data, 65_536, trailer);
    let Err(Failure::Integrity(error)) = upload(request, &pool).await else {
        panic!("a wrong trailing checksum must fail");
    };
    assert_eq!(error, IntegrityError::BadDigest(ChecksumAlgorithm::Crc32c));
    assert_eq!(error.to_s3_error().code().as_str(), "BadDigest");
}

#[tokio::test]
async fn signed_payloads_with_checksum_headers() {
    let pool = pool();
    let data: &'static [u8] = b"The quick brown fox jumps over the lazy dog";
    let sha1 = encode_digest(&digest(ChecksumAlgorithm::Sha1, data));
    let md5 = encode_digest(&digest(ChecksumAlgorithm::Md5, data));
    let headers = [
        ("x-amz-checksum-sha1", sha1.as_str()),
        ("content-md5", md5.as_str()),
    ];
    let request = sdk_signed(Method::PUT, "/b/k", &headers, data, Signing::default());
    let body = upload(request, &pool).await.unwrap();
    assert_eq!(body.etag.as_str(), "9e107d9d372bb6826bd81d3542a419d6");
    let stored: Vec<_> = body.checksums.keys().copied().collect();
    assert_eq!(stored, [ChecksumAlgorithm::Sha1, ChecksumAlgorithm::Md5]);

    // A tampered body fails the signed payload hash first.
    let request = sdk_signed(Method::PUT, "/b/k", &headers, data, Signing::default());
    let (parts, _) = request.into_parts();
    let tampered = Request::from_parts(parts, Body::from(b"The quick brown fox".to_vec()));
    let Err(Failure::Body(error)) = upload(tampered, &pool).await else {
        panic!("a tampered body must fail");
    };
    assert_eq!(
        error.to_s3_error().code().as_str(),
        "XAmzContentSHA256Mismatch"
    );

    // A wrong Content-MD5 is BadDigest.
    let wrong = encode_digest(&digest(ChecksumAlgorithm::Md5, b"x"));
    let headers = [("content-md5", wrong.as_str())];
    let request = sdk_signed(Method::PUT, "/b/k", &headers, data, Signing::default());
    let Err(Failure::Integrity(error)) = upload(request, &pool).await else {
        panic!("a wrong Content-MD5 must fail");
    };
    assert_eq!(error, IntegrityError::BadDigest(ChecksumAlgorithm::Md5));
}
