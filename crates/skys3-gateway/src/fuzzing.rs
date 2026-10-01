//! Entry points for the fuzz targets in the repository's `fuzz/` crate.
//!
//! This module is hidden from the documentation and is not a stable API.
//! [`Harness`] drives arbitrary bytes, as a whole request or as the XML
//! body of a bucket or object operation, through the full request
//! pipeline: limits, rejected features, XML bounds, `s3s` parsing, and the
//! operations, over a fresh in-memory control store and shard stub.
//! [`Harness::sigv4`] drives requests through SigV4 canonicalization and
//! verification, and [`aws_chunked`] drives bodies through the chunk
//! decoder. [`checksums`] reads checksum headers and values.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::header::{AUTHORIZATION, HOST, HeaderMap, HeaderName, HeaderValue};
use http::{Method, Request};
use s3s::Body;
use skys3_config::Config;
use skys3_control::{MemoryControlStore, ProposalIds, RetryPolicy, bootstrap};
use skys3_io::ManualWallClock;
use skys3_types::checksum::{Checksum, ChecksumAlgorithm};

use crate::buckets::{GatewayConfig, IdSource, MODE_HEADER};
use crate::checksum::{ExpectedChecksum, ExpectedChecksums};
use crate::service::{Authenticator, Gateway, Unauthenticated};
use crate::sigv4::body::BodyError;
use crate::sigv4::canonical::{self, Head};
use crate::sigv4::chunked::{ChunkSigner, Decoder, encode};
use crate::sigv4::{MemoryCredentials, SigV4Authenticator};
use crate::stub::MemoryShards;

/// The largest response the gateway may give to any request in the
/// harness. Every response to a bucket operation is small, so a larger one
/// means a limit failed.
pub const MAX_RESPONSE_BYTES: usize = 64 * 1024;

/// The bucket every harness gateway starts with.
pub const BUCKET: &str = "fuzz";

/// Requests whose body the structured mode fills with the input.
const XML_REQUESTS: [(Method, &str); 14] = [
    (Method::PUT, "/fuzz-new"),
    (Method::PUT, "/fuzz?versioning"),
    (Method::PUT, "/fuzz?acl"),
    (Method::PUT, "/fuzz?ownershipControls"),
    (Method::PUT, "/fuzz?object-lock"),
    (Method::PUT, "/fuzz?encryption"),
    (Method::PUT, "/fuzz?tagging"),
    (Method::PUT, "/fuzz?lifecycle"),
    (Method::POST, "/fuzz?delete"),
    (Method::PUT, "/fuzz/key?tagging"),
    (Method::PUT, "/fuzz/key?retention"),
    (Method::PUT, "/fuzz/key?legal-hold"),
    (Method::PUT, "/fuzz/key?acl"),
    (Method::POST, "/fuzz/key?uploadId=u"),
];

const METHODS: [Method; 6] = [
    Method::GET,
    Method::PUT,
    Method::POST,
    Method::DELETE,
    Method::HEAD,
    Method::OPTIONS,
];

/// Runs fuzz inputs through fresh gateways.
#[derive(Debug)]
pub struct Harness {
    runtime: tokio::runtime::Runtime,
}

impl Default for Harness {
    fn default() -> Self {
        Self::new()
    }
}

impl Harness {
    /// A harness with its own single-threaded runtime.
    ///
    /// # Panics
    ///
    /// If the runtime cannot be built.
    #[must_use]
    pub fn new() -> Self {
        Self {
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .expect("a current-thread runtime builds"),
        }
    }

    /// Answers the request `data` encodes, and returns the response's
    /// status code and body length, or `None` for bytes that are not a
    /// valid HTTP request, which hyper would have refused.
    ///
    /// If the first byte has its high bit set, the rest is the XML body of
    /// one of a fixed set of operations, chosen by the low bits. Otherwise
    /// the first byte picks the method, and the rest is the request target,
    /// a newline, `name: value` header lines, an empty line, and the body.
    ///
    /// # Panics
    ///
    /// If the gateway cannot be built, or its response body is longer
    /// than [`MAX_RESPONSE_BYTES`].
    #[must_use]
    pub fn request(&self, data: &[u8]) -> Option<(u16, usize)> {
        let request = decode(data)?;
        self.runtime.block_on(async {
            let gateway = gateway().await;
            let response = gateway.handle(request).await;
            let status = response.status().as_u16();
            let mut body = response.into_body();
            let bytes = body
                .store_all_limited(MAX_RESPONSE_BYTES)
                .await
                .expect("responses are small and complete");
            Some((status, bytes.len()))
        })
    }
}

/// What [`Harness::sigv4`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SigV4Outcome {
    /// The status the request as decoded got from the authenticator, or
    /// `None` if it passed.
    pub raw: Option<u16>,
    /// Whether the request, signed by the harness, passed.
    pub resigned: bool,
    /// Whether the signed request must pass: its query names no other
    /// signature.
    pub must_pass: bool,
}

/// The access key the SigV4 harness signs with.
const FUZZ_KEY: &str = "AKIDFUZZ";
const FUZZ_SECRET: &str = "fuzz-secret";
/// The harness's time, 2026-10-01T00:00:00Z.
const FUZZ_TIME: u64 = 1_790_812_800;
const FUZZ_TIMESTAMP: &str = "20261001T000000Z";
const FUZZ_SCOPE: &str = "20261001/us-east-1/s3/aws4_request";

impl Harness {
    /// Drives the request `data` encodes (as for [`Harness::request`])
    /// through SigV4: its canonical path and query must be stable under
    /// canonicalization; the request as it is must not get a server error;
    /// and the request signed by the harness, with every header signed,
    /// must pass unless its query carries another signature. Returns
    /// `None` for bytes that are not a request.
    ///
    /// # Panics
    ///
    /// If any of those properties fails.
    #[must_use]
    pub fn sigv4(&self, data: &[u8]) -> Option<SigV4Outcome> {
        let (parts, _) = decode(data)?.into_parts();
        let head = Head::new(&parts);
        let mut path = Vec::new();
        canonical::canonical_uri(head.path, &mut path);
        let mut again = Vec::new();
        canonical::canonical_uri(std::str::from_utf8(&path).expect("ASCII"), &mut again);
        assert_eq!(path, again, "canonical paths are stable");
        let mut query = Vec::new();
        canonical::canonical_query(head.query, false, &mut query);
        let mut again = Vec::new();
        canonical::canonical_query(
            std::str::from_utf8(&query).expect("ASCII"),
            false,
            &mut again,
        );
        assert_eq!(query, again, "canonical queries are stable");

        let auth = SigV4Authenticator::new(
            MemoryCredentials::new().with_key(FUZZ_KEY, FUZZ_SECRET, None),
            Arc::new(ManualWallClock::new(Duration::from_secs(FUZZ_TIME))),
        );
        let raw = self.runtime.block_on(auth.authenticate(decode(data)?));
        let raw = raw.err().map(|error| {
            let status = error
                .status_code()
                .or_else(|| error.code().status_code())
                .map_or(0, |status| status.as_u16());
            assert!((400..500).contains(&status) || status == 501, "{error:?}");
            status
        });

        let mut signed = decode(data)?;
        sign(&mut signed);
        let query = signed.uri().query().unwrap_or("");
        let must_pass = !crate::sigv4::params::query_params(query).any(|(_, name, _)| {
            matches!(
                name.as_str(),
                "X-Amz-Algorithm"
                    | "X-Amz-Credential"
                    | "X-Amz-Signature"
                    | "AWSAccessKeyId"
                    | "Signature"
            )
        });
        let resigned = self.runtime.block_on(auth.authenticate(signed));
        if must_pass && let Err(error) = &resigned {
            panic!("a request the harness signed was refused: {error:?}");
        }
        Some(SigV4Outcome {
            raw,
            resigned: resigned.is_ok(),
            must_pass,
        })
    }
}

/// Signs `request` with the harness key, every header included.
fn sign(request: &mut Request<Body>) {
    let headers = request.headers_mut();
    headers.remove(AUTHORIZATION);
    headers.remove("x-amz-security-token");
    headers.insert("x-amz-date", HeaderValue::from_static(FUZZ_TIMESTAMP));
    headers.insert(
        "x-amz-content-sha256",
        HeaderValue::from_static("UNSIGNED-PAYLOAD"),
    );
    if !headers.contains_key(HOST) {
        headers.insert(HOST, HeaderValue::from_static("fuzz.example"));
    }
    let mut names: Vec<&str> = headers.keys().map(HeaderName::as_str).collect();
    names.sort_unstable();
    let signed_headers = names.join(";");
    let (parts, body) = std::mem::take(request).into_parts();
    let canonical_request = canonical::canonical_request(
        &Head::new(&parts),
        &signed_headers,
        "UNSIGNED-PAYLOAD",
        false,
    )
    .expect("every signed header is present");
    let key = canonical::signing_key(FUZZ_SECRET.as_bytes(), "20261001", "us-east-1", "s3");
    let string_to_sign = canonical::string_to_sign(FUZZ_TIMESTAMP, FUZZ_SCOPE, &canonical_request);
    let signature = canonical::hex(&canonical::sign(&key, string_to_sign.as_bytes()));
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={FUZZ_KEY}/{FUZZ_SCOPE}, SignedHeaders={signed_headers}, \
         Signature={signature}"
    );
    *request = Request::from_parts(parts, body);
    request.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::from_str(&authorization).expect("ASCII"),
    );
}

/// What [`aws_chunked`] decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkedOutcome {
    /// The body decoded to this many bytes.
    Decoded(usize),
    /// The body was refused.
    Refused,
}

/// Drives `data` through the `aws-chunked` decoder.
///
/// The first byte picks the form: bit 0 for signed chunks, bit 1 for a
/// trailer, and bit 7 for a round trip. The second picks how the input is
/// split into reads. In a round trip, the rest is data that the harness
/// encodes, in chunk sizes from bits 2 to 6, and decodes again, which must
/// give the data back. Otherwise two bytes give the declared decoded
/// length, and the rest is a body to decode, which must give exactly that
/// many bytes or be refused.
///
/// # Panics
///
/// If a property fails.
#[must_use]
pub fn aws_chunked(data: &[u8]) -> Option<ChunkedOutcome> {
    let (&form, rest) = data.split_first()?;
    let (&split, rest) = rest.split_first()?;
    let (signed, trailer) = (form & 1 != 0, form & 2 != 0);
    let split = usize::from(split).max(1);
    let signer = || {
        let key = canonical::signing_key(FUZZ_SECRET.as_bytes(), "20261001", "us-east-1", "s3");
        signed.then(|| ChunkSigner::new(key, FUZZ_TIMESTAMP, FUZZ_SCOPE, [7; 32]))
    };
    let name = HeaderName::from_static("x-amz-checksum-crc32c");
    let expected = if trailer {
        vec![name.clone()]
    } else {
        Vec::new()
    };
    if form & 0x80 != 0 {
        let mut trailers = HeaderMap::new();
        if trailer {
            trailers.insert(name, HeaderValue::from_static("AAAAAA=="));
        }
        let sizes = [usize::from(form >> 2 & 0x1f) + 1, split];
        let body = encode(rest, &sizes, signer(), &trailers);
        let decoder = Decoder::new(signer(), trailer, expected, rest.len() as u64);
        let (decoded, got) =
            decode_body(decoder, &body, split, rest.len()).expect("an encoded body decodes");
        assert_eq!(decoded, rest, "a round trip gives the data back");
        assert_eq!(got, trailers, "a round trip gives the trailers back");
        return Some(ChunkedOutcome::Decoded(decoded.len()));
    }
    let (length, body) = rest.split_first_chunk::<2>()?;
    let length = usize::from(u16::from_be_bytes(*length));
    let decoder = Decoder::new(signer(), trailer, expected, length as u64);
    Some(match decode_body(decoder, body, split, length) {
        Ok((decoded, _)) => {
            assert_eq!(
                decoded.len(),
                length,
                "a body decodes to its declared length"
            );
            ChunkedOutcome::Decoded(decoded.len())
        }
        Err(_) => ChunkedOutcome::Refused,
    })
}

/// The header names [`checksums`] picks from.
const CHECKSUM_HEADERS: [&str; 12] = [
    "content-md5",
    "x-amz-checksum-crc32",
    "x-amz-checksum-crc32c",
    "x-amz-checksum-crc64nvme",
    "x-amz-checksum-sha1",
    "x-amz-checksum-sha256",
    "x-amz-checksum-sha512",
    "x-amz-checksum-type",
    "x-amz-sdk-checksum-algorithm",
    "x-amz-trailer",
    "x-amz-checksum-",
    "content-type",
];

/// Reads checksum headers and values from untrusted input.
///
/// The input is lines: each line's first byte picks a header name from a
/// fixed list and the rest is its value. The headers are read as
/// [`ExpectedChecksums`]; every line is also parsed as a stored checksum
/// value of every algorithm, which must print back to a value that parses
/// to the same checksum. Returns whether the headers were accepted.
///
/// # Panics
///
/// If a property fails.
#[must_use]
pub fn checksums(data: &[u8]) -> bool {
    let mut headers = HeaderMap::new();
    for line in data.split(|&b| b == b'\n') {
        let Some((&pick, value)) = line.split_first() else {
            continue;
        };
        if let Ok(text) = std::str::from_utf8(value) {
            for algorithm in ChecksumAlgorithm::ALL {
                if let Ok(checksum) = Checksum::parse(algorithm, text) {
                    let printed = checksum.to_string();
                    assert_eq!(Checksum::parse(algorithm, &printed), Ok(checksum.clone()));
                    assert!(checksum.check(algorithm).is_ok());
                }
            }
        }
        let name = CHECKSUM_HEADERS[usize::from(pick) % CHECKSUM_HEADERS.len()];
        if let Ok(value) = HeaderValue::from_bytes(value) {
            headers.append(HeaderName::from_static(name), value);
        }
    }
    match ExpectedChecksums::from_headers(&headers) {
        Ok(expected) => {
            if let Some(ExpectedChecksum::Header(algorithm, digest)) = expected.checksum() {
                assert_eq!(digest.len(), algorithm.digest_len());
            }
            true
        }
        Err(error) => {
            // Every refusal has an S3 answer.
            let _ = error.to_s3_error();
            false
        }
    }
}

/// Decodes `body` delivered in reads of `split` bytes, checking that no
/// more than `declared` bytes come out.
fn decode_body(
    mut decoder: Decoder,
    body: &[u8],
    split: usize,
    declared: usize,
) -> Result<(Vec<u8>, HeaderMap), BodyError> {
    let mut out = Vec::new();
    for piece in body.chunks(split) {
        let mut input = Bytes::copy_from_slice(piece);
        while let Some(data) = decoder.decode(&mut input)? {
            assert!(out.len() + data.len() <= declared, "no more than declared");
            out.extend_from_slice(&data);
        }
    }
    let trailers = decoder.finish()?;
    Ok((out, trailers))
}

/// A gateway with bucket [`BUCKET`] over fresh in-memory state.
async fn gateway() -> Gateway<Unauthenticated> {
    let config: Config = "[cluster]\ncluster_id = \"fuzz\"\n[control_store]\netcd_endpoints = [\"https://etcd.invalid:2379\"]"
        .parse()
        .expect("the configuration is valid");
    let config = GatewayConfig::new(&config);
    let store = MemoryControlStore::new();
    let mut ids = ProposalIds::seeded(0);
    bootstrap(
        &store,
        &config.cluster_id,
        ids.next_id(),
        &RetryPolicy::default(),
    )
    .await
    .expect("the memory store bootstraps");
    let gateway = Gateway::new(
        config,
        store,
        MemoryShards::new(),
        IdSource::seeded(0),
        Unauthenticated,
    )
    .await
    .expect("the gateway loads");
    let create = Request::put(format!("/{BUCKET}"))
        .header(MODE_HEADER, "local")
        .body(Body::empty())
        .expect("the request is valid");
    assert!(gateway.handle(create).await.status().is_success());
    gateway
}

fn decode(data: &[u8]) -> Option<Request<Body>> {
    let (&first, rest) = data.split_first()?;
    if first & 0x80 != 0 {
        let (method, target) = &XML_REQUESTS[usize::from(first & 0x7f) % XML_REQUESTS.len()];
        return Request::builder()
            .method(method)
            .uri(*target)
            .header(http::header::CONTENT_LENGTH, rest.len())
            .body(Body::from(rest.to_vec()))
            .ok();
    }
    let method = &METHODS[usize::from(first) % METHODS.len()];
    let (head, body) = match rest.windows(2).position(|w| w == b"\n\n") {
        Some(end) => (&rest[..end], &rest[end + 2..]),
        None => (rest, &[][..]),
    };
    let mut lines = head.split(|&b| b == b'\n');
    let target = lines.next()?;
    let mut request = Request::builder().method(method).uri(target);
    for line in lines {
        let colon = line.iter().position(|&b| b == b':')?;
        let value = line[colon + 1..].trim_ascii();
        request = request.header(&line[..colon], value);
    }
    request.body(Body::from(body.to_vec())).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inputs_decode_into_requests() {
        let harness = Harness::default();
        assert_eq!(harness.request(b""), None);
        let (status, len) = harness.request(b"\x00/").unwrap();
        assert_eq!(status, 200, "ListBuckets");
        assert!(len > 0);
        assert_eq!(harness.request(b"\x04/fuzz").unwrap().0, 200, "HeadBucket");
        let versioning = b"\x01/fuzz?versioning\nx-amz-acl: private\n\n<VersioningConfiguration/>";
        assert_eq!(harness.request(versioning).unwrap().0, 501);
        assert_eq!(harness.request(b"\x00/\nno colon"), None);
        let enable =
            b"\x81<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>";
        assert_eq!(harness.request(enable).unwrap().0, 501);
        assert_eq!(harness.request(b"\x88<Delete><Object>").unwrap().0, 400);
    }

    #[test]
    fn sigv4_inputs_are_signed_and_verified() {
        let harness = Harness::default();
        assert_eq!(harness.sigv4(b""), None);
        let plain = harness
            .sigv4(b"\x00/b/k%2f?a=1&b\nx-amz-meta-a: x  y\n\nbody")
            .unwrap();
        assert_eq!(
            plain,
            SigV4Outcome {
                raw: None,
                resigned: true,
                must_pass: true
            }
        );
        let unsupported = harness
            .sigv4(b"\x00/b?Signature=x&AWSAccessKeyId=y")
            .unwrap();
        assert_eq!(unsupported.raw, Some(400));
        assert!(!unsupported.must_pass);
        let forged = harness
            .sigv4(
                b"\x00/\nauthorization: AWS4-HMAC-SHA256 Credential=AKIDFUZZ/20261001/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=0000000000000000000000000000000000000000000000000000000000000000\nx-amz-date: 20261001T000000Z\nhost: h\nx-amz-content-sha256: UNSIGNED-PAYLOAD",
            )
            .unwrap();
        assert_eq!(forged.raw, Some(403));
    }

    #[test]
    fn checksum_inputs_are_read_or_refused() {
        assert!(checksums(b""));
        assert!(checksums(b"\x01NSRBwg==\n\x08crc32"));
        assert!(!checksums(b"\x01NSRBwg==\n\x02NSRBwg=="));
        assert!(!checksums(b"\x00short"));
        assert!(checksums(b"\x0bignored\n\x0bMDaLrw==-3"));
    }

    #[test]
    fn chunked_inputs_round_trip_or_are_refused() {
        assert_eq!(aws_chunked(b"\x83"), None);
        assert_eq!(
            aws_chunked(b"\x83\x07hello, world"),
            Some(ChunkedOutcome::Decoded(12))
        );
        assert_eq!(aws_chunked(b"\x80\x01"), Some(ChunkedOutcome::Decoded(0)));
        assert_eq!(aws_chunked(b"\x00\x03\x00"), None);
        assert_eq!(
            aws_chunked(b"\x00\x02\x00\x033\r\nabc\r\n0\r\n\r\n"),
            Some(ChunkedOutcome::Decoded(3))
        );
        assert_eq!(
            aws_chunked(b"\x01\x02\x00\x033\r\nabc\r\n0\r\n\r\n"),
            Some(ChunkedOutcome::Refused)
        );
    }
}
