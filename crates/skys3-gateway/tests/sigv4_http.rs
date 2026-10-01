//! SigV4 end to end: signed requests over HTTP/1.1, through the gateway's
//! listener and its whole pipeline, from the AWS SDK for Rust and from the
//! SDK's signer by hand (for `aws-chunked` bodies).

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ChecksumMode;
use aws_smithy_http_client::tls::{self, rustls_provider::CryptoMode};
use bytes::Bytes;
use common::signing::{KEY, NOW, SECRET, Signing, credentials, sdk_chunked, sdk_signed};
use common::{config, gateway_with};
use http::{Method, Request, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use s3s::Body;
use skys3_gateway::sigv4::MemoryCredentials;
use skys3_gateway::{GatewayListener, MODE_HEADER, SigV4Authenticator};
use skys3_io::{ManualWallClock, SystemWallClock, WallClock};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// A listening gateway that authenticates with [`credentials`], and new
/// buckets default to `local`.
struct Server {
    addr: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    serving: JoinHandle<()>,
}

impl Server {
    async fn start(clock: Arc<dyn WallClock>) -> Self {
        let auth = SigV4Authenticator::new(credentials(), clock);
        let gateway = gateway_with::<SigV4Authenticator<MemoryCredentials>>(
            config("[buckets.defaults]\nmode = \"local\"\n"),
            auth,
        )
        .await;
        let listener = GatewayListener::bind("127.0.0.1:0".parse().unwrap(), gateway)
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop, stopped) = oneshot::channel::<()>();
        let serving = tokio::spawn(listener.serve(async {
            let _ = stopped.await;
        }));
        Self {
            addr,
            stop: Some(stop),
            serving,
        }
    }

    async fn stop(mut self) {
        let _ = self.stop.take().unwrap().send(());
        self.serving.await.unwrap();
    }

    /// Sends `request` on a new connection; returns the status and body.
    async fn send(&self, request: Request<Body>) -> (StatusCode, String) {
        let (parts, body) = request.into_parts();
        let body = body.collect().await.unwrap().to_bytes();
        let request = Request::from_parts(parts, Full::new(body));
        let stream = TcpStream::connect(self.addr).await.unwrap();
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .unwrap();
        tokio::spawn(connection);
        let response = sender.send_request(request).await.unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    fn client(&self, secret: &str) -> aws_sdk_s3::Client {
        let http = aws_smithy_http_client::Builder::new()
            .tls_provider(tls::Provider::Rustls(CryptoMode::AwsLc))
            .build_https();
        let config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .http_client(http)
            .endpoint_url(format!("http://{}", self.addr))
            .force_path_style(true)
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new(KEY, secret, None, None, "test"))
            .retry_config(RetryConfig::disabled())
            .build();
        aws_sdk_s3::Client::from_conf(config)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_aws_sdk_manages_buckets_with_sigv4() {
    let server = Server::start(Arc::new(SystemWallClock)).await;
    let client = server.client(SECRET);
    client
        .create_bucket()
        .bucket("photos")
        .customize()
        .mutate_request(|request| {
            request.headers_mut().insert(MODE_HEADER, "local");
        })
        .send()
        .await
        .unwrap();
    client.head_bucket().bucket("photos").send().await.unwrap();
    let listed = client.list_buckets().send().await.unwrap();
    let names: Vec<_> = listed.buckets().iter().filter_map(|b| b.name()).collect();
    assert_eq!(names, ["photos"]);
    client
        .get_bucket_location()
        .bucket("photos")
        .send()
        .await
        .unwrap();

    // A presigned GetObject, sent without the SDK. It passes
    // authentication and finds no object.
    let presigned = client
        .get_object()
        .bucket("photos")
        .key("a key")
        .presigned(PresigningConfig::expires_in(Duration::from_secs(60)).unwrap())
        .await
        .unwrap();
    let target = presigned
        .uri()
        .strip_prefix(&format!("http://{}", server.addr))
        .unwrap();
    assert!(target.contains("X-Amz-Signature="), "{target}");
    let mut request = Request::builder().method(presigned.method()).uri(target);
    for (name, value) in presigned.headers() {
        request = request.header(name, value);
    }
    let request = request
        .header("host", server.addr.to_string())
        .body(Body::empty())
        .unwrap();
    let (status, body) = server.send(request).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert!(body.contains("<Code>NoSuchKey</Code>"), "{body}");

    // The wrong secret is refused.
    let error = server
        .client("wrong-secret")
        .list_buckets()
        .send()
        .await
        .unwrap_err();
    assert_eq!(error.code(), Some("SignatureDoesNotMatch"), "{error:?}");

    client
        .delete_bucket()
        .bucket("photos")
        .send()
        .await
        .unwrap();
    server.stop().await;
}

const CREATE: &[u8] = b"<CreateBucketConfiguration><LocationConstraint>eu-west-1\
    </LocationConstraint></CreateBucketConfiguration>";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signed_requests_pass_the_pipeline() {
    let server = Server::start(Arc::new(ManualWallClock::new(Duration::from_secs(NOW)))).await;
    let head = |bucket: &str| {
        sdk_signed(
            Method::HEAD,
            &format!("/{bucket}"),
            &[],
            b"",
            Signing::default(),
        )
    };

    // An aws-chunked XML body, decoded before the gateway bounds and parses
    // it. New buckets default to `local` here, so it needs no mode header.
    let (request, _) = sdk_chunked("/chunked", CREATE, 16, None);
    let (status, body) = server.send(request).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(server.send(head("chunked")).await.0, StatusCode::OK);

    // A tampered chunk fails the request, which has no effect.
    let (request, data_at) = sdk_chunked("/tampered", CREATE, 16, None);
    let request = flip_body_byte(request, data_at).await;
    let (status, body) = server.send(request).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert!(
        body.contains("<Code>SignatureDoesNotMatch</Code>"),
        "{body}"
    );
    assert_eq!(server.send(head("tampered")).await.0, StatusCode::NOT_FOUND);

    // A body that does not match its signed hash.
    let request = sdk_signed(Method::PUT, "/hashed", &[], CREATE, Signing::default());
    let request = flip_body_byte(request, 30).await;
    let (status, body) = server.send(request).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body.contains("<Code>XAmzContentSHA256Mismatch</Code>"),
        "{body}"
    );

    // SkyS3's headers must be signed.
    let mut request = sdk_signed(Method::PUT, "/unsigned", &[], b"", Signing::default());
    request
        .headers_mut()
        .insert(MODE_HEADER, "local".parse().unwrap());
    let (status, body) = server.send(request).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert!(body.contains("<Code>AccessDenied</Code>"), "{body}");

    // A presigned HeadBucket.
    let presigned = Signing {
        presigned: Some(Duration::from_secs(60)),
        ..Signing::default()
    };
    let request = sdk_signed(Method::HEAD, "/chunked", &[], b"", presigned);
    assert_eq!(server.send(request).await.0, StatusCode::OK);

    // A signature an hour old is refused.
    let stale = Signing {
        time: NOW - 3600,
        ..Signing::default()
    };
    let request = sdk_signed(Method::GET, "/", &[], b"", stale);
    let (status, body) = server.send(request).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert!(body.contains("<Code>RequestTimeTooSkewed</Code>"), "{body}");

    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_aws_sdk_reads_and_writes_objects() {
    let server = Server::start(Arc::new(SystemWallClock)).await;
    let client = server.client(SECRET);
    client
        .create_bucket()
        .bucket("photos")
        .send()
        .await
        .unwrap();

    // A small object, inline, and a large one in extents. The SDK sends
    // both as aws-chunked bodies with a trailing CRC32 checksum.
    let small = Bytes::from_static(b"a small object");
    let large: Bytes = (0..3 * 1024 * 1024 + 17)
        .map(|i: usize| (i % 251) as u8)
        .collect();
    for (key, data) in [("small", &small), ("large", &large)] {
        let put = client
            .put_object()
            .bucket("photos")
            .key(key)
            .content_type("application/octet-stream")
            .metadata("owner", "test")
            .body(ByteStream::from(data.clone()))
            .send()
            .await
            .unwrap();
        assert!(put.checksum_crc32().is_some(), "{put:?}");
        let got = client
            .get_object()
            .bucket("photos")
            .key(key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .unwrap();
        assert_eq!(got.e_tag(), put.e_tag());
        assert_eq!(got.checksum_crc32(), put.checksum_crc32());
        assert_eq!(got.metadata().unwrap()["owner"], "test");
        let body = got.body.collect().await.unwrap().into_bytes();
        assert_eq!(body, *data, "{key}");
    }

    let head = client
        .head_object()
        .bucket("photos")
        .key("large")
        .send()
        .await
        .unwrap();
    assert_eq!(head.content_length(), i64::try_from(large.len()).ok());
    assert_eq!(head.content_type(), Some("application/octet-stream"));

    // A range across the end of the first extent.
    let ranged = client
        .get_object()
        .bucket("photos")
        .key("large")
        .range("bytes=1048570-1048585")
        .send()
        .await
        .unwrap();
    let expected = format!("bytes 1048570-1048585/{}", large.len());
    assert_eq!(ranged.content_range(), Some(expected.as_str()));
    let body = ranged.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, large[1_048_570..1_048_586]);

    // Response header overrides, signed into the query, replace the stored
    // headers on GetObject and HeadObject alike.
    let got = client
        .get_object()
        .bucket("photos")
        .key("small")
        .response_content_type("text/plain")
        .response_content_disposition("attachment; filename=\"small.txt\"")
        .response_cache_control("no-cache")
        .send()
        .await
        .unwrap();
    assert_eq!(got.content_type(), Some("text/plain"));
    assert_eq!(
        got.content_disposition(),
        Some("attachment; filename=\"small.txt\"")
    );
    assert_eq!(got.cache_control(), Some("no-cache"));
    assert_eq!(got.storage_class(), None);
    let body = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(body, small);
    let head = client
        .head_object()
        .bucket("photos")
        .key("small")
        .response_content_language("fr")
        .send()
        .await
        .unwrap();
    assert_eq!(head.content_language(), Some("fr"));
    assert_eq!(head.content_type(), Some("application/octet-stream"));

    // A conditional PUT must not overwrite, and may create.
    let error = client
        .put_object()
        .bucket("photos")
        .key("small")
        .if_none_match("*")
        .body(ByteStream::from_static(b"replaced"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(error.code(), Some("PreconditionFailed"), "{error:?}");
    client
        .put_object()
        .bucket("photos")
        .key("new")
        .if_none_match("*")
        .body(ByteStream::from_static(b"created"))
        .send()
        .await
        .unwrap();

    for key in ["small", "large", "new"] {
        client
            .delete_object()
            .bucket("photos")
            .key(key)
            .send()
            .await
            .unwrap();
    }
    let error = client
        .head_object()
        .bucket("photos")
        .key("small")
        .send()
        .await
        .unwrap_err();
    assert!(
        error.as_service_error().unwrap().is_not_found(),
        "{error:?}"
    );
    client
        .delete_bucket()
        .bucket("photos")
        .send()
        .await
        .unwrap();
    server.stop().await;
}

/// An `aws-chunked` object body whose last chunk was tampered with is
/// refused, and nothing is stored, although its first extents were written
/// while it arrived.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tampered_object_body_stores_nothing() {
    let server = Server::start(Arc::new(ManualWallClock::new(Duration::from_secs(NOW)))).await;
    let create = sdk_signed(Method::PUT, "/photos", &[], b"", Signing::default());
    assert_eq!(server.send(create).await.0, StatusCode::OK);
    let head = || sdk_signed(Method::HEAD, "/photos/key", &[], b"", Signing::default());
    let data: Vec<u8> = (0..2_621_440).map(|i: usize| (i % 251) as u8).collect();

    let (request, _) = sdk_chunked("/photos/key", &data, 64 * 1024, None);
    let length = request.headers()["content-length"].to_str().unwrap();
    let at = length.parse::<usize>().unwrap() - 200;
    let request = flip_body_byte(request, at).await;
    let (status, body) = server.send(request).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert!(
        body.contains("<Code>SignatureDoesNotMatch</Code>"),
        "{body}"
    );
    assert_eq!(server.send(head()).await.0, StatusCode::NOT_FOUND);

    // The same body untampered is stored.
    let (request, _) = sdk_chunked("/photos/key", &data, 64 * 1024, None);
    assert_eq!(server.send(request).await.0, StatusCode::OK);
    assert_eq!(server.send(head()).await.0, StatusCode::OK);
    server.stop().await;
}

/// `request` with one byte of its body changed.
async fn flip_body_byte(request: Request<Body>, at: usize) -> Request<Body> {
    let (parts, body) = request.into_parts();
    let mut bytes = body.collect().await.unwrap().to_bytes().to_vec();
    bytes[at] ^= 0x20;
    Request::from_parts(parts, Body::from(Bytes::from(bytes)))
}
