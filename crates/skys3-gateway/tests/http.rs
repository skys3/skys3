//! Bucket operations end to end over HTTP/1.1, through the gateway's
//! listener.

mod common;

use std::net::SocketAddr;

use bytes::Bytes;
use common::setup;
use http::{Method, Request, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use skys3_gateway::{GatewayListener, MODE_HEADER, TARGET_HEADER};
use tokio::net::TcpStream;

/// Sends one request on a new connection, and returns the status and body.
async fn send(addr: SocketAddr, request: Request<Full<Bytes>>) -> (StatusCode, String) {
    let stream = TcpStream::connect(addr).await.unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .unwrap();
    tokio::spawn(connection);
    let response = sender.send_request(request).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8(body.to_vec()).unwrap())
}

fn request(
    method: Method,
    uri: &str,
    headers: &[(&str, &str)],
    body: &'static str,
) -> Request<Full<Bytes>> {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header("host", "localhost");
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    request
        .body(Full::new(Bytes::from_static(body.as_bytes())))
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bucket_operations_over_http() {
    let setup = setup("").await;
    let listener = GatewayListener::bind("127.0.0.1:0".parse().unwrap(), setup.gateway.clone())
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    assert!(format!("{listener:?}").contains("GatewayListener"));
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let serving = tokio::spawn(listener.serve(async {
        let _ = stopped.await;
    }));

    let create = request(
        Method::PUT,
        "/photos",
        &[
            (MODE_HEADER, "write_back"),
            (TARGET_HEADER, "https://s3.example.com/photos-remote"),
        ],
        "<CreateBucketConfiguration><LocationConstraint>eu-west-1</LocationConstraint>\
         </CreateBucketConfiguration>",
    );
    let (status, body) = send(addr, create).await;
    assert_eq!(status, 200, "{body}");
    let (status, _) = send(addr, request(Method::HEAD, "/photos", &[], "")).await;
    assert_eq!(status, 200);
    let (status, body) = send(addr, request(Method::GET, "/", &[], "")).await;
    assert_eq!(status, 200);
    assert!(body.contains("<Name>photos</Name>"), "{body}");
    let (status, body) = send(addr, request(Method::GET, "/photos?location", &[], "")).await;
    assert_eq!(status, 200);
    assert!(body.contains("LocationConstraint"), "{body}");

    // A rejected feature, with an object body the gateway never reads.
    let sse = request(
        Method::PUT,
        "/photos/key",
        &[("x-amz-server-side-encryption", "AES256")],
        "object data",
    );
    let (status, body) = send(addr, sse).await;
    assert_eq!(status, 501);
    assert!(body.contains("<Code>NotImplemented</Code>"), "{body}");

    // hyper refuses a head over the header-count limit before the gateway
    // sees it.
    let names: Vec<String> = (0..120).map(|n| format!("x-many-{n}")).collect();
    let many: Vec<(&str, &str)> = names.iter().map(|name| (name.as_str(), "v")).collect();
    let (status, _) = send(addr, request(Method::GET, "/", &many, "")).await;
    assert_eq!(status, StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE);

    let (status, body) = send(addr, request(Method::DELETE, "/photos", &[], "")).await;
    assert_eq!(status, 204, "{body}");
    let (status, _) = send(addr, request(Method::HEAD, "/photos", &[], "")).await;
    assert_eq!(status, 404);

    stop.send(()).unwrap();
    serving.await.unwrap();
    assert!(TcpStream::connect(addr).await.is_err(), "still listening");
}
