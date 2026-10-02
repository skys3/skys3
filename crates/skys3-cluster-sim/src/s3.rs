//! S3 over the simulated network: each node serves its gateway on a
//! `turmoil` listener, and clients send plain HTTP/1.1 requests over
//! `turmoil` TCP, one connection per request.

use std::convert::Infallible;
use std::io;
use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use bytes::Bytes;
use http::{Request, Response};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use skys3_gateway::{Authenticator, Gateway};

/// The port every node serves S3 on.
pub const S3_PORT: u16 = 9000;

/// Binds the node's S3 listener on [`S3_PORT`].
pub(crate) async fn bind() -> io::Result<turmoil::net::TcpListener> {
    turmoil::net::TcpListener::bind((IpAddr::V4(Ipv4Addr::UNSPECIFIED), S3_PORT)).await
}

/// Serves `gateway` on `listener` until accepting fails.
///
/// # Errors
///
/// The listener's error.
pub(crate) async fn serve<A: Authenticator>(
    listener: turmoil::net::TcpListener,
    gateway: Gateway<A>,
) -> io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let gateway = gateway.clone();
        tokio::spawn(async move {
            let service = service_fn(move |request: Request<Incoming>| {
                let gateway = gateway.clone();
                async move { Ok::<_, Infallible>(gateway.handle(request.map(s3s::Body::from)).await) }
            });
            // A broken connection only ends this request.
            let _ = http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await;
        });
    }
}

/// Why a request got no answer.
#[derive(Debug)]
pub(crate) enum NoAnswer {
    /// The connection failed or broke.
    Io(String),
    /// The answer did not arrive in time.
    Timeout,
}

impl std::fmt::Display for NoAnswer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NoAnswer::Io(error) => write!(f, "the connection failed: {error}"),
            NoAnswer::Timeout => f.write_str("no answer in time"),
        }
    }
}

/// A connection a node's S3 listener accepted, ready for one request.
pub(crate) struct Connection {
    sender: hyper::client::conn::http1::SendRequest<Full<Bytes>>,
}

/// Connects to `host` within `timeout`. Once this returns, the life of the
/// node that accepted the connection is the only one that can receive a
/// request sent on it: a later life has no such connection.
pub(crate) async fn connect(host: &str, timeout: Duration) -> Result<Connection, NoAnswer> {
    let connecting = async {
        let stream = turmoil::net::TcpStream::connect((host, S3_PORT))
            .await
            .map_err(|e| NoAnswer::Io(e.to_string()))?;
        let (sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .map_err(|e| NoAnswer::Io(e.to_string()))?;
        tokio::spawn(connection);
        Ok(Connection { sender })
    };
    tokio::time::timeout(timeout, connecting)
        .await
        .unwrap_or(Err(NoAnswer::Timeout))
}

impl Connection {
    /// Sends `request` and waits up to `timeout` for the whole answer.
    pub(crate) async fn send(
        mut self,
        request: Request<Full<Bytes>>,
        timeout: Duration,
    ) -> Result<Response<Bytes>, NoAnswer> {
        let exchange = async {
            let io = |error: &dyn std::fmt::Display| NoAnswer::Io(error.to_string());
            let response = self
                .sender
                .send_request(request)
                .await
                .map_err(|e| io(&e))?;
            let (parts, body) = response.into_parts();
            let body = body.collect().await.map_err(|e| io(&e))?.to_bytes();
            Ok(Response::from_parts(parts, body))
        };
        tokio::time::timeout(timeout, exchange)
            .await
            .unwrap_or(Err(NoAnswer::Timeout))
    }
}
