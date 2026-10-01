//! The SigV4 authenticator: the documented S3 examples, requests signed by
//! the AWS SDK's signer, clock skew, presigned URL lifetimes, session
//! tokens, and `aws-chunked` bodies.

mod common;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::signing::{
    KEY, NOW, SESSION_KEY, SESSION_SECRET, Signing, TOKEN, credentials, request, sdk_chunked,
    sdk_signed,
};
use http::{Method, Request};
use http_body_util::BodyExt;
use s3s::{Body, S3Error, S3ErrorCode};
use skys3_gateway::sigv4::{AuthMethod, MAX_CLOCK_SKEW, MemoryCredentials};
use skys3_gateway::{Authenticated, Authenticator, BodyError, SigV4Authenticator, Trailers};
use skys3_io::ManualWallClock;

/// The credentials of the examples in the S3 documentation.
const DOC_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
const DOC_SECRET: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
/// 2013-05-24T00:00:00Z, the time of the documented examples.
const DOC_TIME: u64 = 1_369_353_600;
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

fn authenticator(now: u64) -> (SigV4Authenticator<MemoryCredentials>, ManualWallClock) {
    let clock = ManualWallClock::new(Duration::from_secs(now));
    let credentials = credentials().with_key(DOC_KEY, DOC_SECRET, None);
    (
        SigV4Authenticator::new(credentials, Arc::new(clock.clone())),
        clock,
    )
}

#[track_caller]
fn assert_code(result: Result<Request<Body>, S3Error>, code: S3ErrorCode) {
    match result {
        Ok(_) => panic!("accepted; expected {code:?}"),
        Err(error) => assert_eq!(*error.code(), code, "{error:?}"),
    }
}

fn principal(request: &Request<Body>) -> &Authenticated<String> {
    request
        .extensions()
        .get::<Authenticated<String>>()
        .expect("the request is authenticated")
}

async fn read(request: Request<Body>) -> Result<Bytes, s3s::StdError> {
    Ok(request.into_body().collect().await?.to_bytes())
}

fn doc_authorization(signed_headers: &str, signature: &str) -> String {
    format!(
        "AWS4-HMAC-SHA256 Credential={DOC_KEY}/20130524/us-east-1/s3/aws4_request, \
         SignedHeaders={signed_headers}, Signature={signature}"
    )
}

/// GET Object, from
/// <https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html>.
fn doc_get_object() -> Request<Body> {
    let authorization = doc_authorization(
        "host;range;x-amz-content-sha256;x-amz-date",
        "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41",
    );
    request(
        Method::GET,
        "/test.txt",
        &[
            ("host", "examplebucket.s3.amazonaws.com"),
            ("range", "bytes=0-9"),
            ("x-amz-content-sha256", EMPTY_SHA256),
            ("x-amz-date", "20130524T000000Z"),
            ("authorization", &authorization),
        ],
        Bytes::new(),
    )
}

#[tokio::test]
async fn the_documented_get_object_request_is_accepted() {
    let (auth, _) = authenticator(DOC_TIME);
    let accepted = auth.authenticate(doc_get_object()).await.unwrap();
    let caller = principal(&accepted);
    assert_eq!(caller.principal, DOC_KEY);
    assert_eq!(caller.access_key_id, DOC_KEY);
    assert_eq!(caller.method, AuthMethod::Header);
    assert!(!accepted.headers().contains_key("authorization"));
    assert_eq!(read(accepted).await.unwrap(), "");
}

#[tokio::test]
async fn the_documented_put_object_request_is_accepted() {
    let (auth, _) = authenticator(DOC_TIME);
    let body_hash = "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072";
    let authorization = doc_authorization(
        "date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class",
        "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd",
    );
    let put = |body: &'static str| {
        request(
            Method::PUT,
            "/test$file.text",
            &[
                ("date", "Fri, 24 May 2013 00:00:00 GMT"),
                ("host", "examplebucket.s3.amazonaws.com"),
                ("x-amz-content-sha256", body_hash),
                ("x-amz-date", "20130524T000000Z"),
                ("x-amz-storage-class", "REDUCED_REDUNDANCY"),
                ("authorization", &authorization),
            ],
            Bytes::from_static(body.as_bytes()),
        )
    };
    let accepted = auth
        .authenticate(put("Welcome to Amazon S3."))
        .await
        .unwrap();
    assert_eq!(read(accepted).await.unwrap(), "Welcome to Amazon S3.");
    // Another body passes the signature check, but not the payload hash.
    let tampered = auth
        .authenticate(put("Welcome to Amazon S4."))
        .await
        .unwrap();
    let error = read(tampered).await.unwrap_err();
    let error = BodyError::find(&*error).unwrap().to_s3_error();
    assert_eq!(error.code().as_str(), "XAmzContentSHA256Mismatch");
}

#[tokio::test]
async fn the_documented_bucket_requests_are_accepted() {
    let (auth, _) = authenticator(DOC_TIME);
    let headers = |signature: &str| {
        [
            ("host", "examplebucket.s3.amazonaws.com".to_owned()),
            ("x-amz-content-sha256", EMPTY_SHA256.to_owned()),
            ("x-amz-date", "20130524T000000Z".to_owned()),
            (
                "authorization",
                doc_authorization("host;x-amz-content-sha256;x-amz-date", signature),
            ),
        ]
    };
    for (uri, signature) in [
        (
            "/?lifecycle",
            "fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543",
        ),
        (
            "/?max-keys=2&prefix=J",
            "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7",
        ),
    ] {
        let headers = headers(signature);
        let headers: Vec<(&str, &str)> = headers.iter().map(|(n, v)| (*n, v.as_str())).collect();
        let accepted = auth
            .authenticate(request(Method::GET, uri, &headers, Bytes::new()))
            .await
            .unwrap();
        assert_eq!(accepted.uri().to_string(), uri);
    }
}

/// From <https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html>.
const DOC_PRESIGNED: &str = "/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256\
    &X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request\
    &X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host\
    &X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404";

fn doc_presigned() -> Request<Body> {
    request(
        Method::GET,
        DOC_PRESIGNED,
        &[("host", "examplebucket.s3.amazonaws.com")],
        Bytes::new(),
    )
}

#[tokio::test]
async fn the_documented_presigned_url_lives_for_its_lifetime() {
    let (auth, clock) = authenticator(DOC_TIME);
    let accepted = auth.authenticate(doc_presigned()).await.unwrap();
    assert_eq!(principal(&accepted).method, AuthMethod::Presigned);
    assert_eq!(accepted.uri(), "/test.txt", "the signature is removed");
    // Valid up to and including its last second.
    clock.set(Duration::from_secs(DOC_TIME + 86_400));
    auth.authenticate(doc_presigned()).await.unwrap();
    clock.advance(Duration::from_secs(1));
    assert_code(
        auth.authenticate(doc_presigned()).await,
        S3ErrorCode::AccessDenied,
    );
    // A URL dated in the future is refused beyond the allowed skew.
    clock.set(Duration::from_secs(DOC_TIME) - MAX_CLOCK_SKEW);
    auth.authenticate(doc_presigned()).await.unwrap();
    clock.set(Duration::from_secs(DOC_TIME - 1) - MAX_CLOCK_SKEW);
    assert_code(
        auth.authenticate(doc_presigned()).await,
        S3ErrorCode::AccessDenied,
    );
}

/// The headers of the streaming examples, from
/// <https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html>
/// and `sigv4-streaming-trailers.html`, with 65,536 and 1,024 bytes of `a`.
fn doc_chunked(trailer: bool) -> Request<Body> {
    let (payload, signed, seed, signatures, trailers): (_, _, _, [&str; 3], &str) = if trailer {
        (
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
            "content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class;x-amz-trailer",
            "106e2a8a18243abcf37539882f36619c00e2dfc72633413f02d3b74544bfeb8e",
            [
                "b474d8862b1487a5145d686f57f013e54db672cee1c953b3010fb58501ef5aa2",
                "1c1344b170168f8e65b41376b44b20fe354e373826ccbbe2c1d40a8cae51e5c7",
                "2ca2aba2005185cf7159c6277faf83795951dd77a3a99e6e65d5c9f85863f992",
            ],
            "x-amz-checksum-crc32c:sOO8/Q==\r\nx-amz-trailer-signature:\
             d81f82fc3505edab99d459891051a732e8730629a2e4a59689829ca17fe2e435\r\n",
        )
    } else {
        (
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
            "content-encoding;content-length;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class",
            "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9",
            [
                "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648",
                "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497",
                "b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9",
            ],
            "",
        )
    };
    let mut body = Vec::new();
    body.extend_from_slice(format!("10000;chunk-signature={}\r\n", signatures[0]).as_bytes());
    body.extend_from_slice(&[b'a'; 65_536]);
    body.extend_from_slice(format!("\r\n400;chunk-signature={}\r\n", signatures[1]).as_bytes());
    body.extend_from_slice(&[b'a'; 1024]);
    body.extend_from_slice(format!("\r\n0;chunk-signature={}\r\n", signatures[2]).as_bytes());
    body.extend_from_slice(trailers.as_bytes());
    body.extend_from_slice(b"\r\n");
    if !trailer {
        assert_eq!(body.len(), 66_824, "the documented Content-Length");
    }
    let length = body.len().to_string();
    let authorization = doc_authorization(signed, seed);
    let mut headers = vec![
        ("content-encoding", "aws-chunked"),
        ("host", "s3.amazonaws.com"),
        ("x-amz-content-sha256", payload),
        ("x-amz-date", "20130524T000000Z"),
        ("x-amz-decoded-content-length", "66560"),
        ("x-amz-storage-class", "REDUCED_REDUNDANCY"),
        ("authorization", &authorization),
        ("content-length", &length),
    ];
    if trailer {
        headers.push(("x-amz-trailer", "x-amz-checksum-crc32c"));
    }
    request(
        Method::PUT,
        "/examplebucket/chunkObject.txt",
        &headers,
        Bytes::from(body),
    )
}

#[tokio::test]
async fn the_documented_chunked_upload_decodes() {
    let (auth, _) = authenticator(DOC_TIME);
    let accepted = auth.authenticate(doc_chunked(false)).await.unwrap();
    assert_eq!(accepted.headers()["content-length"], "66560");
    assert!(!accepted.headers().contains_key("content-encoding"));
    assert!(
        !accepted
            .headers()
            .contains_key("x-amz-decoded-content-length")
    );
    assert!(accepted.extensions().get::<Trailers>().is_none());
    assert_eq!(read(accepted).await.unwrap(), vec![b'a'; 66_560]);
}

#[tokio::test]
async fn the_documented_chunked_upload_with_a_trailer_decodes() {
    let (auth, _) = authenticator(DOC_TIME);
    let accepted = auth.authenticate(doc_chunked(true)).await.unwrap();
    let trailers = accepted.extensions().get::<Trailers>().unwrap().clone();
    assert!(trailers.get().is_none(), "not before the body is read");
    assert_eq!(read(accepted).await.unwrap().len(), 66_560);
    assert_eq!(trailers.get().unwrap()["x-amz-checksum-crc32c"], "sOO8/Q==");
}

#[tokio::test]
async fn header_signatures_must_be_close_to_the_clock() {
    let (auth, clock) = authenticator(DOC_TIME);
    for offset in [MAX_CLOCK_SKEW, Duration::ZERO] {
        clock.set(Duration::from_secs(DOC_TIME) + offset);
        auth.authenticate(doc_get_object()).await.unwrap();
        clock.set(Duration::from_secs(DOC_TIME) - offset);
        auth.authenticate(doc_get_object()).await.unwrap();
    }
    let too_far = MAX_CLOCK_SKEW + Duration::from_secs(1);
    clock.set(Duration::from_secs(DOC_TIME) + too_far);
    assert_code(
        auth.authenticate(doc_get_object()).await,
        S3ErrorCode::RequestTimeTooSkewed,
    );
    clock.set(Duration::from_secs(DOC_TIME) - too_far);
    assert_code(
        auth.authenticate(doc_get_object()).await,
        S3ErrorCode::RequestTimeTooSkewed,
    );
}

#[tokio::test]
async fn sdk_signed_requests_are_accepted() {
    let (auth, _) = authenticator(NOW);
    let cases = [
        (Method::GET, "/bucket/key", &[][..], &b""[..]),
        (
            Method::GET,
            "/bucket?list-type=2&prefix=a%2Fb&delimiter=%2F",
            &[],
            b"",
        ),
        (
            Method::PUT,
            "/bucket/a%20key%2Bwith%24odd%2Fchars~",
            &[("content-type", "text/plain")],
            b"data",
        ),
        (
            Method::PUT,
            "/bucket",
            &[("x-skys3-bucket-mode", "local")],
            b"",
        ),
        (
            Method::DELETE,
            "/bucket/k?versionId=null",
            &[("x-amz-meta-a", "x  y")],
            b"",
        ),
    ];
    for (method, uri, headers, body) in cases {
        let signed = sdk_signed(method.clone(), uri, headers, body, Signing::default());
        let accepted = auth
            .authenticate(signed)
            .await
            .unwrap_or_else(|e| panic!("{method} {uri}: {e:?}"));
        assert_eq!(principal(&accepted).principal, KEY);
        assert_eq!(read(accepted).await.unwrap(), body);
    }
}

#[tokio::test]
async fn sdk_presigned_urls_are_accepted_and_stripped() {
    let (auth, clock) = authenticator(NOW);
    let presigned = Signing {
        presigned: Some(Duration::from_secs(3600)),
        ..Signing::default()
    };
    let make = || sdk_signed(Method::GET, "/bucket/key?partNumber=1", &[], b"", presigned);
    let accepted = auth.authenticate(make()).await.unwrap();
    assert_eq!(principal(&accepted).method, AuthMethod::Presigned);
    assert_eq!(accepted.uri(), "/bucket/key?partNumber=1");
    clock.advance(Duration::from_secs(3601));
    assert_code(auth.authenticate(make()).await, S3ErrorCode::AccessDenied);
    // Seven days is the longest lifetime.
    let week = Signing {
        presigned: Some(Duration::from_secs(7 * 86_400 + 1)),
        ..Signing::default()
    };
    let too_long = sdk_signed(Method::GET, "/bucket/key", &[], b"", week);
    assert_code(
        auth.authenticate(too_long).await,
        S3ErrorCode::AuthorizationQueryParametersError,
    );
}

#[tokio::test]
async fn session_tokens_are_checked() {
    let (auth, _) = authenticator(NOW);
    let session = Signing {
        key: SESSION_KEY,
        secret: SESSION_SECRET,
        token: Some(TOKEN),
        ..Signing::default()
    };
    let signed = sdk_signed(Method::GET, "/bucket/key", &[], b"", session);
    assert!(signed.headers().contains_key("x-amz-security-token"));
    auth.authenticate(signed).await.unwrap();
    let presigned = Signing {
        presigned: Some(Duration::from_secs(60)),
        ..session
    };
    let signed = sdk_signed(Method::GET, "/bucket/key", &[], b"", presigned);
    assert!(
        signed
            .uri()
            .query()
            .unwrap()
            .contains("X-Amz-Security-Token")
    );
    let accepted = auth.authenticate(signed).await.unwrap();
    assert_eq!(accepted.uri(), "/bucket/key");
    let wrong = Signing {
        token: Some("other"),
        ..session
    };
    let signed = sdk_signed(Method::GET, "/bucket/key", &[], b"", wrong);
    assert_code(auth.authenticate(signed).await, S3ErrorCode::InvalidToken);
    let missing = Signing {
        token: None,
        ..session
    };
    let signed = sdk_signed(Method::GET, "/bucket/key", &[], b"", missing);
    assert_code(auth.authenticate(signed).await, S3ErrorCode::InvalidToken);
    // A token added after signing is an unsigned x-amz- header.
    let mut signed = sdk_signed(Method::GET, "/bucket/key", &[], b"", missing);
    signed
        .headers_mut()
        .insert("x-amz-security-token", TOKEN.parse().unwrap());
    assert_code(auth.authenticate(signed).await, S3ErrorCode::AccessDenied);
}

#[tokio::test]
async fn bad_credentials_and_signatures_are_refused() {
    let (auth, _) = authenticator(NOW);
    let unknown = Signing {
        key: "AKIDUNKNOWN",
        ..Signing::default()
    };
    let signed = sdk_signed(Method::GET, "/b/k", &[], b"", unknown);
    assert_code(
        auth.authenticate(signed).await,
        S3ErrorCode::InvalidAccessKeyId,
    );
    let wrong_secret = Signing {
        secret: "wrong",
        ..Signing::default()
    };
    let signed = sdk_signed(Method::GET, "/b/k", &[], b"", wrong_secret);
    assert_code(
        auth.authenticate(signed).await,
        S3ErrorCode::SignatureDoesNotMatch,
    );
    // Changing anything signed breaks the signature: the path, the query,
    // a header, or the method.
    let tamper: [fn(&mut Request<Body>); 4] = [
        |r| *r.uri_mut() = "/b/k2".parse().unwrap(),
        |r| *r.uri_mut() = "/b/k?x=1".parse().unwrap(),
        |r| {
            r.headers_mut().insert("host", "elsewhere".parse().unwrap());
        },
        |r| *r.method_mut() = Method::DELETE,
    ];
    for change in tamper {
        let mut signed = sdk_signed(Method::GET, "/b/k", &[], b"", Signing::default());
        change(&mut signed);
        assert_code(
            auth.authenticate(signed).await,
            S3ErrorCode::SignatureDoesNotMatch,
        );
    }
    // Encoding the same path differently is a different request.
    let mut signed = sdk_signed(Method::GET, "/b/a%2Fb", &[], b"", Signing::default());
    *signed.uri_mut() = "/b/a/b".parse().unwrap();
    assert_code(
        auth.authenticate(signed).await,
        S3ErrorCode::SignatureDoesNotMatch,
    );
}

#[tokio::test]
async fn skys3_and_amz_headers_must_be_signed() {
    let (auth, _) = authenticator(NOW);
    for name in ["x-skys3-bucket-mode", "x-amz-meta-late"] {
        let mut signed = sdk_signed(Method::PUT, "/bucket", &[], b"", Signing::default());
        signed.headers_mut().insert(name, "local".parse().unwrap());
        assert_code(auth.authenticate(signed).await, S3ErrorCode::AccessDenied);
    }
    // Other headers need not be signed.
    let mut signed = sdk_signed(Method::PUT, "/bucket", &[], b"", Signing::default());
    signed
        .headers_mut()
        .insert("user-agent", "x".parse().unwrap());
    auth.authenticate(signed).await.unwrap();
    // An unsigned request cannot set SkyS3 headers at all.
    let anonymous = request(
        Method::PUT,
        "/bucket",
        &[("x-skys3-bucket-mode", "local")],
        Bytes::new(),
    );
    assert_code(
        auth.authenticate(anonymous).await,
        S3ErrorCode::AccessDenied,
    );
    // The host header must be signed.
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={KEY}/20261001/us-east-1/s3/aws4_request, \
         SignedHeaders=x-amz-content-sha256;x-amz-date, Signature={}",
        "0".repeat(64)
    );
    let unhosted = request(
        Method::GET,
        "/b/k",
        &[
            ("authorization", &authorization),
            ("x-amz-date", "20261001T000000Z"),
            ("x-amz-content-sha256", "UNSIGNED-PAYLOAD"),
        ],
        Bytes::new(),
    );
    assert_code(
        auth.authenticate(unhosted).await,
        S3ErrorCode::AuthorizationHeaderMalformed,
    );
}

#[tokio::test]
async fn payload_hashes_are_required_and_enforced() {
    let (auth, _) = authenticator(NOW);
    let mut signed = sdk_signed(Method::GET, "/b/k", &[], b"", Signing::default());
    signed.headers_mut().remove("x-amz-content-sha256");
    assert_code(auth.authenticate(signed).await, S3ErrorCode::InvalidRequest);
    // A presigned URL cannot carry an aws-chunked body.
    let presigned = Signing {
        presigned: Some(Duration::from_secs(60)),
        ..Signing::default()
    };
    let chunked = [("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")];
    let signed = sdk_signed(Method::PUT, "/b/k", &chunked, b"", presigned);
    assert_code(auth.authenticate(signed).await, S3ErrorCode::InvalidRequest);
}

#[tokio::test]
async fn unsigned_requests_pass_unauthenticated() {
    let (auth, _) = authenticator(NOW);
    let anonymous = request(Method::GET, "/b/k", &[], Bytes::new());
    let passed = auth.authenticate(anonymous).await.unwrap();
    assert!(passed.extensions().get::<Authenticated<String>>().is_none());
    // An unsigned aws-chunked body still decodes.
    let body = Bytes::from_static(b"3\r\nabc\r\n0\r\nx-amz-checksum-crc32:NSRBwg==\r\n\r\n");
    let chunked = request(
        Method::PUT,
        "/b/k",
        &[
            ("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
            ("x-amz-decoded-content-length", "3"),
            ("x-amz-trailer", "x-amz-checksum-crc32"),
        ],
        body,
    );
    let passed = auth.authenticate(chunked).await.unwrap();
    let trailers = passed.extensions().get::<Trailers>().unwrap().clone();
    assert_eq!(read(passed).await.unwrap(), "abc");
    assert_eq!(trailers.get().unwrap()["x-amz-checksum-crc32"], "NSRBwg==");
    // Signed chunks need a signed request.
    let signed_chunks = request(
        Method::PUT,
        "/b/k",
        &[
            ("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"),
            ("x-amz-decoded-content-length", "3"),
        ],
        Bytes::new(),
    );
    assert_code(
        auth.authenticate(signed_chunks).await,
        S3ErrorCode::AccessDenied,
    );
}

#[tokio::test]
async fn sdk_signed_chunked_uploads_decode() {
    // The SDK's chunk signer, chained from the SDK's seed signature.
    let (auth, _) = authenticator(NOW);
    let data: Vec<u8> = (0..200_000_u32).map(|n| (n % 251) as u8).collect();
    let crc = Some(("x-amz-checksum-crc32", "AAAAAA=="));
    let (request, _) = sdk_chunked("/bucket/object", &data, 65_536, crc);
    let accepted = auth.authenticate(request).await.unwrap();
    let trailers = accepted.extensions().get::<Trailers>().unwrap().clone();
    assert_eq!(read(accepted).await.unwrap(), data);
    assert_eq!(trailers.get().unwrap()["x-amz-checksum-crc32"], "AAAAAA==");
    // A flipped data byte fails the body, not the head.
    let (request, data_at) = sdk_chunked("/bucket/object", &data, 65_536, crc);
    let (parts, body) = request.into_parts();
    let mut bytes = body.collect().await.unwrap().to_bytes().to_vec();
    bytes[data_at + 70_000] ^= 1;
    let tampered = Request::from_parts(parts, Body::from(bytes));
    let accepted = auth.authenticate(tampered).await.unwrap();
    let error = read(accepted).await.unwrap_err();
    let error = BodyError::find(&*error).unwrap();
    assert_eq!(
        *error.to_s3_error().code(),
        S3ErrorCode::SignatureDoesNotMatch
    );
}
