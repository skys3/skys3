//! Requests signed by the AWS SDK's own signer (`aws-sigv4`), an
//! implementation independent of the gateway's.

use std::time::{Duration, UNIX_EPOCH};

use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    PayloadChecksumKind, PercentEncodingMode, SignableBody, SignableRequest, SignatureLocation,
    SigningSettings, UriPathNormalizationMode, sign,
};
use aws_sigv4::sign::v4;
use bytes::Bytes;
use http::{Method, Request};
use s3s::Body;
use skys3_gateway::sigv4::MemoryCredentials;

pub const KEY: &str = "AKIDTEST";
pub const SECRET: &str = "test-secret";
pub const SESSION_KEY: &str = "ASIATEST";
pub const SESSION_SECRET: &str = "session-secret";
pub const TOKEN: &str = "session/token+with=base64";
/// 2026-10-01T00:00:00Z.
pub const NOW: u64 = 1_790_812_800;
/// The `host` header every signed request carries.
pub const HOST: &str = "localhost:9000";

/// The test keys: [`KEY`], and [`SESSION_KEY`] with [`TOKEN`].
pub fn credentials() -> MemoryCredentials {
    MemoryCredentials::new()
        .with_key(KEY, SECRET, None)
        .with_key(SESSION_KEY, SESSION_SECRET, Some(TOKEN))
}

pub fn request(method: Method, uri: &str, headers: &[(&str, &str)], body: Bytes) -> Request<Body> {
    let mut request = Request::builder().method(method).uri(uri);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    request.body(Body::from(body)).unwrap()
}

/// Options for [`sdk_signed`].
#[derive(Clone, Copy)]
pub struct Signing<'a> {
    pub key: &'a str,
    pub secret: &'a str,
    pub token: Option<&'a str>,
    pub time: u64,
    pub presigned: Option<Duration>,
}

impl Default for Signing<'_> {
    fn default() -> Self {
        Self {
            key: KEY,
            secret: SECRET,
            token: None,
            time: NOW,
            presigned: None,
        }
    }
}

/// S3's signing settings: no path normalization, single encoding.
fn settings() -> SigningSettings {
    let mut settings = SigningSettings::default();
    settings.percent_encoding_mode = PercentEncodingMode::Single;
    settings.uri_path_normalization_mode = UriPathNormalizationMode::Disabled;
    settings
}

/// A request with a `host` header, signed by the SDK's signer as an SDK
/// signs for S3: with `x-amz-content-sha256`, or as a presigned URL.
pub fn sdk_signed(
    method: Method,
    uri: &str,
    headers: &[(&str, &str)],
    body: &'static [u8],
    signing: Signing<'_>,
) -> Request<Body> {
    let identity = Credentials::new(
        signing.key,
        signing.secret,
        signing.token.map(str::to_owned),
        None,
        "test",
    )
    .into();
    let mut settings = settings();
    let signable_body = if let Some(expires) = signing.presigned {
        settings.signature_location = SignatureLocation::QueryParams;
        settings.expires_in = Some(expires);
        SignableBody::UnsignedPayload
    } else {
        settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
        SignableBody::Bytes(body)
    };
    let params = v4::SigningParams::builder()
        .identity(&identity)
        .region("us-east-1")
        .name("s3")
        .time(UNIX_EPOCH + Duration::from_secs(signing.time))
        .settings(settings)
        .build()
        .unwrap()
        .into();
    let mut all = vec![("host", HOST)];
    all.extend_from_slice(headers);
    let signable =
        SignableRequest::new(method.as_str(), uri, all.iter().copied(), signable_body).unwrap();
    let (instructions, _) = sign(signable, &params).unwrap().into_parts();
    let mut request = request(method, uri, &all, Bytes::from_static(body));
    instructions.apply_to_request_http1x(&mut request);
    request
}

/// A PUT of `data` as an `aws-chunked` body in signed chunks of `size`
/// bytes, with a signed `trailer` if given, signed by the SDK's signer at
/// [`NOW`] with [`KEY`]. Also returns where the first chunk's data starts
/// in the body.
pub fn sdk_chunked(
    uri: &str,
    data: &[u8],
    size: usize,
    trailer: Option<(&str, &str)>,
) -> (Request<Body>, usize) {
    let identity = Credentials::new(KEY, SECRET, None, None, "test").into();
    let mut settings = settings();
    settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
    let time = UNIX_EPOCH + Duration::from_secs(NOW);
    let params = v4::SigningParams::builder()
        .identity(&identity)
        .region("us-east-1")
        .name("s3")
        .time(time)
        .settings(settings)
        .build()
        .unwrap();
    let decoded = data.len().to_string();
    let mut headers = vec![
        ("host", HOST),
        ("content-encoding", "aws-chunked"),
        ("x-amz-decoded-content-length", decoded.as_str()),
    ];
    let body_kind = match trailer {
        Some((name, _)) => {
            headers.push(("x-amz-trailer", name));
            SignableBody::StreamingSignedPayloadTrailer
        }
        None => SignableBody::Precomputed("STREAMING-AWS4-HMAC-SHA256-PAYLOAD".to_owned()),
    };
    let signable = SignableRequest::new("PUT", uri, headers.iter().copied(), body_kind).unwrap();
    let (instructions, seed) = sign(signable, &params.into()).unwrap().into_parts();
    let chunk_params = v4::SigningParams::builder()
        .identity(&identity)
        .region("us-east-1")
        .name("s3")
        .time(time)
        .settings(())
        .build()
        .unwrap();
    let mut body = Vec::new();
    let mut previous = seed;
    let mut data_at = None;
    for chunk in data.chunks(size.max(1)).chain([&[][..]]) {
        let signature = v4::sign_chunk(&Bytes::copy_from_slice(chunk), &previous, &chunk_params)
            .unwrap()
            .into_parts()
            .1;
        body.extend_from_slice(
            format!("{:X};chunk-signature={signature}\r\n", chunk.len()).as_bytes(),
        );
        data_at.get_or_insert(body.len());
        if !chunk.is_empty() {
            body.extend_from_slice(chunk);
            body.extend_from_slice(b"\r\n");
        }
        previous = signature;
    }
    if let Some((name, value)) = trailer {
        // The trailer string to sign, as the S3 documentation gives it.
        let line = format!("{name}:{value}\n");
        let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, line.as_bytes());
        let hash: String = digest.as_ref().iter().map(|b| format!("{b:02x}")).collect();
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256-TRAILER\n20261001T000000Z\n20261001/us-east-1/s3/aws4_request\n\
             {previous}\n{hash}"
        );
        let key = v4::generate_signing_key(SECRET, time, "us-east-1", "s3");
        let signature = v4::calculate_signature(key, string_to_sign.as_bytes());
        body.extend_from_slice(
            format!("{name}:{value}\r\nx-amz-trailer-signature:{signature}\r\n").as_bytes(),
        );
    }
    body.extend_from_slice(b"\r\n");
    let length = body.len().to_string();
    headers.push(("content-length", &length));
    let mut request = request(Method::PUT, uri, &headers, Bytes::from(body));
    instructions.apply_to_request_http1x(&mut request);
    (request, data_at.unwrap_or(0))
}
