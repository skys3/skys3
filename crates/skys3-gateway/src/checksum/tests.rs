use std::num::NonZeroUsize;

use bytes::Bytes;
use http::header::{HeaderMap, HeaderName, HeaderValue};
use http::{Extensions, Request};
use http_body_util::BodyExt;
use proptest::prelude::*;
use s3s::Body;
use skys3_io::BlockingPool;
use skys3_types::checksum::{
    Checksum, ChecksumAlgorithm, ChecksumError, ChecksumType, encode_digest,
};

use super::*;
use crate::sigv4::Trailers;
use crate::sigv4::body::{Payload, wrap};
use crate::sigv4::chunked::encode;

use ChecksumAlgorithm::{Crc32, Crc32c, Crc64Nvme, Md5, Sha1, Sha256};

fn pool() -> BlockingPool {
    BlockingPool::new("test-checksum", NonZeroUsize::new(2).unwrap()).unwrap()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
    pairs
        .iter()
        .map(|(name, value)| {
            (
                HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            )
        })
        .collect()
}

fn expected(pairs: &[(&str, &str)]) -> Result<ExpectedChecksums, IntegrityError> {
    ExpectedChecksums::from_headers(&headers(pairs))
}

/// Published check values: the CRC catalogue's check values over
/// `123456789`, and the FIPS 180 and RFC 1321 examples.
#[test]
fn known_answers() {
    let vectors: [(ChecksumAlgorithm, &[u8], &str); 12] = [
        (Crc32, b"123456789", "cbf43926"),
        (Crc32c, b"123456789", "e3069283"),
        (Crc64Nvme, b"123456789", "ae8b14860a799888"),
        (Crc32, b"", "00000000"),
        (Crc64Nvme, b"", "0000000000000000"),
        (Sha1, b"abc", "a9993e364706816aba3e25717850c26c9cd0d89d"),
        (Sha1, b"", "da39a3ee5e6b4b0d3255bfef95601890afd80709"),
        (
            Sha256,
            b"abc",
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        ),
        (
            Sha256,
            b"",
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        ),
        (Md5, b"abc", "900150983cd24fb0d6963f7d28e17f72"),
        (Md5, b"", "d41d8cd98f00b204e9800998ecf8427e"),
        (
            Md5,
            b"12345678901234567890123456789012345678901234567890123456789012345678901234567890",
            "57edf4a22be3c955ac49da2e2107b67a",
        ),
    ];
    for (algorithm, input, expected) in vectors {
        assert_eq!(hex(&digest(algorithm, input)), expected, "{algorithm}");
        let mut hasher = Hasher::new(algorithm);
        assert_eq!(hasher.algorithm(), algorithm);
        assert_eq!(format!("{hasher:?}"), format!("Hasher({algorithm:?})"));
        for byte in input {
            hasher.update(&[*byte]);
        }
        assert_eq!(hex(&hasher.finish()), expected, "{algorithm} by bytes");
    }
}

/// `n` bytes of `fill`, as the `s3-tests` suite's `FakeWriteFile` makes.
fn filled(n: usize, fill: u8) -> Vec<u8> {
    vec![fill; n]
}

/// Values from the `s3-tests` suite (`test_object_checksum_*`), which
/// S3-compatible stores are tested against.
#[test]
fn s3_tests_single_part_known_answers() {
    let body = filled(1024, b'A');
    for (algorithm, value) in [
        (Sha256, "arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0="),
        (Crc64Nvme, "Qeh8oXvGiSo="),
    ] {
        let checksum = Checksum::parse(algorithm, value).unwrap();
        assert_eq!(digest(algorithm, &body), checksum.digest(), "{algorithm}");
    }
}

/// The three 5 MiB parts of `A`, `B`, and `C` of the `s3-tests` suite
/// (`test_multipart_use_cksum_helper_*`, `test_multipart_reupload_*`):
/// each part's checksum, and the object's.
#[test]
fn s3_tests_multipart_known_answers() {
    const PART: usize = 5 * 1024 * 1024;
    let parts = [filled(PART, b'A'), filled(PART, b'B'), filled(PART, b'C')];
    let cases = [
        (
            Sha256,
            ChecksumType::Composite,
            [
                "275VF5loJr1YYawit0XSHREhkFXYkkPKGuoK0x9VKxI=",
                "mrHwOfjTL5Zwfj74F05HOQGLdUb7E5szdCbxgUSq6NM=",
                "Vw7oB/nKQ5xWb3hNgbyfkvDiivl+U+/Dft48nfJfDow=",
            ],
            "uWBwpe1dxI4Vw8Gf0X9ynOdw/SS6VBzfWm9giiv1sf4=-3",
        ),
        (
            Sha1,
            ChecksumType::Composite,
            [
                "iIaTCGbm+vdVjNqIMF2S0T7ibMk=",
                "LS/TJ32bAVKEwRu+sE3X7awh/lk=",
                "6DDwovUaHwrKNXDMzOGbuvj9kxI=",
            ],
            "sizjvY4eud3MrcHdZM3cQ/ol39o=-3",
        ),
        (
            Crc32,
            ChecksumType::FullObject,
            ["JRTCyQ==", "QoZTGg==", "YAgjqw=="],
            "WgDhBQ==",
        ),
        (
            Crc32c,
            ChecksumType::FullObject,
            ["MDaLrw==", "TH4EZg==", "Z7mBIQ=="],
            "xU+Krw==",
        ),
        (
            Crc64Nvme,
            ChecksumType::FullObject,
            ["L/E4WYn8v98=", "xW1l19VobYM=", "cK5MnNaWrW4="],
            "i+6LR0y3eFo=",
        ),
    ];
    for (algorithm, checksum_type, part_values, object_value) in cases {
        let mut object = MultipartChecksum::new(algorithm, checksum_type).unwrap();
        for (part, value) in parts.iter().zip(part_values) {
            let part_digest = digest(algorithm, part);
            assert_eq!(encode_digest(&part_digest), value, "{algorithm} part");
            object.push(&part_digest, part.len() as u64).unwrap();
        }
        let object = object.finish().unwrap();
        assert_eq!(object.to_string(), object_value, "{algorithm}");
        assert_eq!(object.checksum_type(), checksum_type);
        if checksum_type == ChecksumType::FullObject {
            assert_eq!(object.digest(), digest(algorithm, &parts.concat()));
        }
    }
    let mut etag = MultipartEtag::new();
    for part in &parts {
        let md5: [u8; 16] = digest(Md5, part).try_into().unwrap();
        etag.push(&md5).unwrap();
    }
    assert_eq!(
        etag.finish().unwrap().as_str(),
        "b2add96cc9702bbf4efb0ccdfc6b7747-3"
    );
}

#[test]
fn multipart_builders_refuse_what_s3_does_not_have() {
    assert_eq!(
        MultipartChecksum::new(Crc64Nvme, ChecksumType::Composite).unwrap_err(),
        ChecksumError::NotComposite(Crc64Nvme)
    );
    assert_eq!(
        MultipartChecksum::new(Sha256, ChecksumType::FullObject).unwrap_err(),
        ChecksumError::NotFullObject(Sha256)
    );
    let empty = MultipartChecksum::new(Crc32, ChecksumType::FullObject).unwrap();
    assert_eq!(empty.finish().unwrap_err(), ChecksumError::PartCount(0));
    let mut composite = MultipartChecksum::new(Crc32, ChecksumType::Composite).unwrap();
    assert!(matches!(
        composite.push(&[0; 8], 1),
        Err(ChecksumError::DigestLength { .. })
    ));
    for _ in 0..10_000 {
        composite.push(&[1, 2, 3, 4], 1).unwrap();
    }
    assert_eq!(
        composite.push(&[1, 2, 3, 4], 1),
        Err(ChecksumError::PartCount(10_001))
    );
    assert_eq!(composite.finish().unwrap().parts(), Some(10_000));

    assert_eq!(MultipartEtag::new().finish(), None);
    let mut etag = MultipartEtag::new();
    for _ in 0..10_000 {
        etag.push(&[0; 16]).unwrap();
    }
    assert_eq!(etag.push(&[0; 16]), Err(ChecksumError::PartCount(10_001)));
    // The MD5 of 16 zero bytes, as one part.
    let mut one = MultipartEtag::new();
    one.push(&[0; 16]).unwrap();
    assert_eq!(
        one.finish().unwrap().as_str(),
        "4ae71336e44bf9bf79d2752e234818a5-1"
    );
}

#[test]
fn expected_checksums_are_read_from_headers() {
    let none = expected(&[("content-type", "text/plain")]).unwrap();
    assert_eq!(none, ExpectedChecksums::default());
    assert_eq!(none.stored_algorithm(), DEFAULT_ALGORITHM);

    let md5 = expected(&[("content-md5", "kAFQmDzST7DWlj99KOF/cg==")]).unwrap();
    assert_eq!(
        hex(md5.content_md5().unwrap()),
        "900150983cd24fb0d6963f7d28e17f72"
    );

    let header = expected(&[
        ("x-amz-checksum-crc32", "NSRBwg=="),
        ("x-amz-sdk-checksum-algorithm", "crc32"),
        ("x-amz-checksum-type", "FULL_OBJECT"),
        ("x-amz-checksum-mode", "ENABLED"),
        ("x-amz-checksum-algorithm", "CRC32"),
        ("x-amz-checksum-other", "ignored"),
    ])
    .unwrap();
    assert_eq!(
        header.checksum(),
        Some(&ExpectedChecksum::Header(
            Crc32,
            vec![0x35, 0x24, 0x41, 0xc2]
        ))
    );
    assert_eq!(header.stored_algorithm(), Crc32);

    let trailer = expected(&[
        ("x-amz-trailer", "x-other, X-Amz-Checksum-Crc64nvme"),
        ("x-amz-sdk-checksum-algorithm", "CRC64NVME"),
    ])
    .unwrap();
    assert_eq!(
        trailer.checksum(),
        Some(&ExpectedChecksum::Trailer(Crc64Nvme))
    );
    assert_eq!(trailer.checksum().unwrap().algorithm(), Crc64Nvme);
}

#[test]
fn malformed_checksum_headers_get_s3_answers() {
    let err = |pairs: &[(&str, &str)]| expected(pairs).unwrap_err();
    // `Content-MD5` (the `s3-tests` header cases).
    for value in [
        "YWJyYWNhZGFicmE=",
        "",
        "AWS HAHAHA",
        "kAFQmDzST7DWlj99KOF/cg",
    ] {
        assert_eq!(
            err(&[("content-md5", value)]),
            IntegrityError::InvalidDigest,
            "{value:?}"
        );
    }
    let twice = [
        ("content-md5", "kAFQmDzST7DWlj99KOF/cg=="),
        ("content-md5", "kAFQmDzST7DWlj99KOF/cg=="),
    ];
    assert_eq!(err(&twice), IntegrityError::InvalidDigest);
    // A value that is not base64 can match nothing; one of the wrong
    // length is malformed.
    assert_eq!(
        err(&[("x-amz-checksum-sha256", "bad")]),
        IntegrityError::BadDigest(Sha256)
    );
    let hex_sha256 = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
    assert_eq!(
        err(&[("x-amz-checksum-sha256", hex_sha256)]),
        IntegrityError::InvalidValue(Sha256)
    );
    assert_eq!(
        err(&[
            ("x-amz-checksum-crc32", "NSRBwg=="),
            ("x-amz-checksum-crc32c", "NSRBwg==")
        ]),
        IntegrityError::MultipleChecksums
    );
    assert_eq!(
        err(&[
            ("x-amz-checksum-crc32", "NSRBwg=="),
            ("x-amz-trailer", "x-amz-checksum-crc32")
        ]),
        IntegrityError::MultipleChecksums
    );
    assert_eq!(
        err(&[("x-amz-trailer", "x-amz-checksum-crc32,x-amz-checksum-sha1")]),
        IntegrityError::MultipleChecksums
    );
    assert_eq!(
        err(&[
            ("x-amz-checksum-crc32", "NSRBwg=="),
            ("x-amz-sdk-checksum-algorithm", "SHA256")
        ]),
        IntegrityError::BadDigest(Crc32)
    );
    assert_eq!(
        err(&[("x-amz-sdk-checksum-algorithm", "CRC32")]),
        IntegrityError::MissingChecksum
    );
    assert_eq!(
        err(&[("x-amz-sdk-checksum-algorithm", "CRC16")]),
        IntegrityError::UnknownAlgorithm("CRC16".into())
    );
    assert!(matches!(
        err(&[
            ("x-amz-sdk-checksum-algorithm", "CRC32"),
            ("x-amz-sdk-checksum-algorithm", "CRC32")
        ]),
        IntegrityError::UnknownAlgorithm(_)
    ));
    for (name, value) in [
        ("x-amz-sdk-checksum-algorithm", "sha512"),
        ("x-amz-checksum-xxhash64", "AAAAAAAAAAA="),
        ("x-amz-trailer", "x-amz-checksum-md5"),
    ] {
        assert!(
            matches!(
                err(&[(name, value)]),
                IntegrityError::UnsupportedAlgorithm(_)
            ),
            "{name}"
        );
    }
    let mut non_ascii = HeaderMap::new();
    non_ascii.insert(
        "x-amz-checksum-crc32",
        HeaderValue::from_bytes(b"\xff").unwrap(),
    );
    assert_eq!(
        ExpectedChecksums::from_headers(&non_ascii).unwrap_err(),
        IntegrityError::BadDigest(Crc32)
    );
}

#[test]
fn integrity_errors_map_to_s3_errors() {
    for (error, code, status) in [
        (IntegrityError::InvalidDigest, "InvalidDigest", 400),
        (IntegrityError::BadDigest(Md5), "BadDigest", 400),
        (IntegrityError::BadDigest(Crc32c), "BadDigest", 400),
        (IntegrityError::InvalidValue(Sha1), "InvalidRequest", 400),
        (IntegrityError::MultipleChecksums, "InvalidRequest", 400),
        (IntegrityError::MissingChecksum, "InvalidRequest", 400),
        (
            IntegrityError::UnknownAlgorithm("X".into()),
            "InvalidRequest",
            400,
        ),
        (
            IntegrityError::UnsupportedAlgorithm("SHA512".into()),
            "NotImplemented",
            501,
        ),
        (IntegrityError::NoTrailers, "InvalidRequest", 400),
        (IntegrityError::Unavailable, "ServiceUnavailable", 503),
    ] {
        let s3 = s3s::S3Error::from(error.clone());
        assert_eq!(s3.code().as_str(), code, "{error}");
        let actual = s3.status_code().or_else(|| s3.code().status_code());
        assert_eq!(actual.map(|s| s.as_u16()), Some(status), "{error}");
        assert_eq!(s3.message(), Some(error.to_string().as_str()));
    }
    assert_eq!(
        IntegrityError::BadDigest(Md5).to_string(),
        "The Content-MD5 you specified did not match what we received."
    );
    assert_eq!(
        IntegrityError::BadDigest(Sha256).to_string(),
        "The SHA256 you specified did not match the calculated checksum."
    );
    assert_eq!(
        IntegrityError::InvalidValue(Crc32).to_string(),
        "Value for x-amz-checksum-crc32 header is invalid."
    );
}

/// Feeds `data` to a validator in pieces of `piece` bytes.
async fn validate(
    expected: ExpectedChecksums,
    trailers: Option<Trailers>,
    data: &[u8],
    piece: usize,
    pool: &BlockingPool,
) -> Result<VerifiedBody, IntegrityError> {
    let mut validator = ChecksumValidator::new(expected, trailers, pool)?;
    for chunk in data.chunks(piece) {
        validator.update(Bytes::copy_from_slice(chunk)).await?;
    }
    validator.finish().await
}

#[tokio::test]
async fn header_checksums_are_checked_for_every_algorithm() {
    let pool = pool();
    let data: Vec<u8> = (0..700_000_u32).map(|i| (i * 7 % 256) as u8).collect();
    for algorithm in ChecksumAlgorithm::FLEXIBLE {
        let value = encode_digest(&digest(algorithm, &data));
        let good = expected(&[(algorithm.header_name(), &value)]).unwrap();
        let body = validate(good, None, &data, 65_536, &pool).await.unwrap();
        assert_eq!(body.length, data.len() as u64);
        assert_eq!(body.md5.to_vec(), digest(Md5, &data));
        assert_eq!(body.etag.md5(), Some(body.md5));
        assert_eq!(body.checksums.len(), 1);
        assert_eq!(body.checksums[&algorithm].to_string(), value);
        assert_eq!(
            body.checksums[&algorithm].checksum_type(),
            ChecksumType::FullObject
        );

        let other = encode_digest(&digest(algorithm, b"other bytes"));
        let bad = expected(&[(algorithm.header_name(), &other)]).unwrap();
        assert_eq!(
            validate(bad, None, &data, 100_000, &pool)
                .await
                .unwrap_err(),
            IntegrityError::BadDigest(algorithm),
            "{algorithm}"
        );
    }
}

#[tokio::test]
async fn content_md5_is_checked_and_stored() {
    let pool = pool();
    let data = b"hello world";
    let md5 = encode_digest(&digest(Md5, data));
    let good = expected(&[("content-md5", &md5)]).unwrap();
    let body = validate(good, None, data, 3, &pool).await.unwrap();
    assert_eq!(body.etag.as_str(), "5eb63bbbe01eeed093cb22bb8f5acdc3");
    assert_eq!(body.checksums[&Md5].to_string(), md5);
    // Without an x-amz-checksum value, the default is computed and stored.
    assert_eq!(
        body.checksums[&DEFAULT_ALGORITHM].digest(),
        digest(DEFAULT_ALGORITHM, data)
    );
    // The `s3-tests` value that does not match.
    let bad = expected(&[("content-md5", "rL0Y20xC+Fzt72VPzMSk2A==")]).unwrap();
    assert_eq!(
        validate(bad, None, data, 4, &pool).await.unwrap_err(),
        IntegrityError::BadDigest(Md5)
    );
}

#[tokio::test]
async fn bodies_without_checksums_get_the_default() {
    let pool = pool();
    let body = validate(ExpectedChecksums::default(), None, b"", 1, &pool)
        .await
        .unwrap();
    assert_eq!(body.length, 0);
    assert_eq!(body.etag.as_str(), "d41d8cd98f00b204e9800998ecf8427e");
    let stored: Vec<_> = body.checksums.keys().copied().collect();
    assert_eq!(stored, [Crc64Nvme]);
    assert_eq!(body.checksums[&Crc64Nvme].to_string(), "AAAAAAAAAAA=");
}

/// Decodes an `aws-chunked` body with unsigned trailers, as the
/// authenticator does, and validates it.
async fn validate_chunked(
    head: &[(&str, &str)],
    trailer: Option<(&str, &str)>,
    data: &[u8],
    pool: &BlockingPool,
) -> Result<VerifiedBody, IntegrityError> {
    let mut trailers = HeaderMap::new();
    if let Some((name, value)) = trailer {
        trailers.insert(
            HeaderName::from_bytes(name.as_bytes()).unwrap(),
            HeaderValue::from_str(value).unwrap(),
        );
    }
    let encoded = encode(data, &[1000, 300], None, &trailers);
    let mut request = Request::put("/b/k")
        .header("x-amz-decoded-content-length", data.len())
        .header("content-encoding", "aws-chunked");
    for (name, value) in head {
        request = request.header(*name, *value);
    }
    let (mut parts, ()) = request.body(()).unwrap().into_parts();
    let payload = Payload::Chunked {
        signed: false,
        trailer: true,
    };
    let mut body = wrap(
        &mut parts,
        Body::from(Bytes::from(encoded)),
        payload,
        None,
        None,
    )
    .unwrap();
    let mut validator = ChecksumValidator::for_request(&parts.headers, &parts.extensions, pool)?;
    while let Some(frame) = body.frame().await {
        let frame = frame.expect("the body decodes");
        if let Ok(data) = frame.into_data() {
            validator.update(data).await?;
        }
    }
    validator.finish().await
}

#[tokio::test]
async fn trailing_checksums_are_checked() {
    let pool = pool();
    let data: Vec<u8> = (0..5000_u32).map(|i| (i % 253) as u8).collect();
    for algorithm in ChecksumAlgorithm::FLEXIBLE {
        let name = algorithm.header_name();
        let value = encode_digest(&digest(algorithm, &data));
        let head = [
            ("x-amz-trailer", name),
            ("x-amz-sdk-checksum-algorithm", algorithm.name()),
        ];
        let body = validate_chunked(&head, Some((name, &value)), &data, &pool)
            .await
            .unwrap();
        assert_eq!(body.checksums[&algorithm].to_string(), value);
        assert_eq!(body.length, data.len() as u64);

        let wrong = encode_digest(&digest(algorithm, b"x"));
        assert_eq!(
            validate_chunked(&head, Some((name, &wrong)), &data, &pool)
                .await
                .unwrap_err(),
            IntegrityError::BadDigest(algorithm)
        );
    }
    let head = [("x-amz-trailer", "x-amz-checksum-crc32")];
    let short = Some(("x-amz-checksum-crc32", "AAAA"));
    assert_eq!(
        validate_chunked(&head, short, &data, &pool)
            .await
            .unwrap_err(),
        IntegrityError::InvalidValue(Crc32)
    );
    let garbage = Some(("x-amz-checksum-crc32", "%%%%"));
    assert_eq!(
        validate_chunked(&head, garbage, &data, &pool)
            .await
            .unwrap_err(),
        IntegrityError::BadDigest(Crc32)
    );
}

#[tokio::test]
async fn trailing_checksums_need_trailers() {
    let pool = pool();
    let declared = expected(&[("x-amz-trailer", "x-amz-checksum-crc32c")]).unwrap();
    assert_eq!(
        ChecksumValidator::new(declared.clone(), None, &pool).unwrap_err(),
        IntegrityError::NoTrailers
    );
    // Trailers that have not arrived: the body was not read to its end.
    let pending = Trailers::default();
    assert_eq!(
        validate(declared, Some(pending), b"abc", 1, &pool)
            .await
            .unwrap_err(),
        IntegrityError::NoTrailers
    );
    let mut extensions = Extensions::new();
    extensions.insert(Trailers::default());
    let validator = ChecksumValidator::for_request(&headers(&[]), &extensions, &pool).unwrap();
    assert!(format!("{validator:?}").contains("ChecksumValidator"));
}

#[tokio::test]
async fn a_closed_pool_makes_the_service_unavailable() {
    let pool = pool();
    pool.shutdown();
    assert_eq!(
        validate(ExpectedChecksums::default(), None, b"abc", 1, &pool)
            .await
            .unwrap_err(),
        IntegrityError::Unavailable
    );
    let big = vec![0; 2 * HASH_BATCH_BYTES];
    assert_eq!(
        validate(ExpectedChecksums::default(), None, &big, 100_000, &pool)
            .await
            .unwrap_err(),
        IntegrityError::Unavailable
    );
}

#[tokio::test]
async fn pooled_hashers_hold_at_most_two_batches() {
    let pool = pool();
    let mut hasher = PooledHasher::new(ChecksumAlgorithm::ALL, Some(pool.clone()));
    assert!(format!("{hasher:?}").contains("pending_len"));
    let data: Vec<u8> = (0..1_000_000_u32).map(|i| (i % 249) as u8).collect();
    for chunk in data.chunks(HASH_BATCH_BYTES / 3 + 1) {
        hasher.update(Bytes::copy_from_slice(chunk)).await.unwrap();
        hasher.update(Bytes::new()).await.unwrap();
    }
    let digests = hasher.finish().await.unwrap();
    for algorithm in ChecksumAlgorithm::ALL {
        assert_eq!(digests[&algorithm], digest(algorithm, &data), "{algorithm}");
    }
    let mut hashers = Hashers::new([Crc32, Crc32, Md5]);
    hashers.update(b"abc");
    assert_eq!(hashers.finish().len(), 2);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Feeding a hasher the data in any split gives the one-shot digest.
    #[test]
    fn streaming_in_any_split_equals_one_shot(
        data in proptest::collection::vec(any::<u8>(), 0..4096),
        cuts in proptest::collection::vec(any::<prop::sample::Index>(), 0..12),
    ) {
        let mut cuts: Vec<usize> = cuts.iter().map(|cut| cut.index(data.len() + 1)).collect();
        cuts.sort_unstable();
        for algorithm in ChecksumAlgorithm::ALL {
            let mut hasher = Hasher::new(algorithm);
            let mut start = 0;
            for &cut in cuts.iter().chain([&data.len()]) {
                hasher.update(&data[start..cut]);
                start = cut;
            }
            prop_assert_eq!(hasher.finish(), digest(algorithm, &data));
        }
    }

    /// The same holds on the pool, where batches cross piece boundaries.
    #[test]
    fn pooled_streaming_in_any_split_equals_one_shot(
        data in proptest::collection::vec(any::<u8>(), 0..(HASH_BATCH_BYTES * 2 + 100)),
        pieces in proptest::collection::vec(1_usize..200_000, 1..8),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let pool = pool();
        let digests = runtime.block_on(async {
            let mut hasher = PooledHasher::new(ChecksumAlgorithm::ALL, Some(pool));
            let mut rest = &data[..];
            for &piece in pieces.iter().cycle() {
                if rest.is_empty() {
                    break;
                }
                let (head, tail) = rest.split_at(piece.min(rest.len()));
                hasher.update(Bytes::copy_from_slice(head)).await.unwrap();
                rest = tail;
            }
            hasher.finish().await.unwrap()
        });
        for algorithm in ChecksumAlgorithm::ALL {
            prop_assert_eq!(&digests[&algorithm], &digest(algorithm, &data));
        }
    }

    /// A full-object CRC combined from parts equals the CRC of the whole.
    #[test]
    fn combined_crcs_equal_the_whole(
        parts in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..300), 1..6),
    ) {
        for algorithm in [Crc32, Crc32c, Crc64Nvme] {
            let mut object = MultipartChecksum::new(algorithm, ChecksumType::FullObject).unwrap();
            for part in &parts {
                object.push(&digest(algorithm, part), part.len() as u64).unwrap();
            }
            let object = object.finish().unwrap();
            prop_assert_eq!(object.digest(), &digest(algorithm, &parts.concat())[..]);
        }
    }

    /// Header parsing never panics, whatever the values.
    #[test]
    fn header_parsing_is_total(
        values in proptest::collection::vec(("[a-z0-9-]{0,30}", any::<Vec<u8>>()), 0..6),
    ) {
        let mut map = HeaderMap::new();
        for (suffix, value) in values {
            let name = HeaderName::from_bytes(format!("x-amz-checksum-{suffix}").as_bytes());
            if let (Ok(name), Ok(value)) = (name, HeaderValue::from_bytes(&value)) {
                map.append(name, value);
            }
        }
        let _ = ExpectedChecksums::from_headers(&map);
    }
}
