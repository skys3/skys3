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
    // authentication; object operations arrive with plan M1-09.
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
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{body}");
    assert!(!body.contains("authentication"), "{body}");

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

/// `request` with one byte of its body changed.
async fn flip_body_byte(request: Request<Body>, at: usize) -> Request<Body> {
    let (parts, body) = request.into_parts();
    let mut bytes = body.collect().await.unwrap().to_bytes().to_vec();
    bytes[at] ^= 0x20;
    Request::from_parts(parts, Body::from(Bytes::from(bytes)))
}
