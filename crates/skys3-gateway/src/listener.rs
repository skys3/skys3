//! The gateway's HTTP/1.1 listener.

use std::convert::Infallible;
use std::fmt;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::server::graceful::GracefulShutdown;
use s3s::Body;
use tokio::net::TcpListener;
use tokio::task::JoinSet;

use crate::service::{Authenticator, Gateway};

/// How long a client may take to send a request's head.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// How long in-flight requests get to finish once shutdown starts.
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// The pause after a failed `accept`, such as running out of file
/// descriptors.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

/// hyper's smallest read buffer.
const MIN_BUFFER: usize = 8 * 1024;

/// A bound gateway listener, ready to serve.
pub struct GatewayListener<A> {
    listener: TcpListener,
    gateway: Gateway<A>,
}

impl<A: Authenticator> GatewayListener<A> {
    /// Binds `listen` for `gateway`.
    ///
    /// # Errors
    ///
    /// The operating system's error if binding fails.
    pub async fn bind(listen: SocketAddr, gateway: Gateway<A>) -> io::Result<Self> {
        Ok(Self {
            listener: TcpListener::bind(listen).await?,
            gateway,
        })
    }

    /// The bound address, which tells the port when the requested port is
    /// 0.
    ///
    /// # Errors
    ///
    /// The operating system's error if the address is unavailable.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Serves connections until `shutdown` completes, then stops accepting
    /// and gives in-flight requests the drain deadline to finish.
    ///
    /// hyper refuses a request head with more header fields than
    /// [`RequestLimits::max_header_count`](crate::RequestLimits) or more
    /// bytes than its buffer, sized from the header and URI limits, before
    /// the gateway sees it.
    pub async fn serve(self, shutdown: impl Future<Output = ()>) {
        let Self { listener, gateway } = self;
        let limits = *gateway.limits();
        let mut builder = http1::Builder::new();
        builder
            .timer(TokioTimer::new())
            .header_read_timeout(HEADER_READ_TIMEOUT)
            .max_headers(limits.max_header_count)
            .max_buf_size(MIN_BUFFER.max(limits.max_header_bytes + limits.max_uri_bytes + 1024));
        let graceful = GracefulShutdown::new();
        let mut connections = JoinSet::new();
        tokio::pin!(shutdown);
        loop {
            while connections.try_join_next().is_some() {}
            let (stream, peer) = tokio::select! {
                () = &mut shutdown => break,
                accepted = listener.accept() => match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        tracing::warn!(%error, "the gateway cannot accept a connection");
                        tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
                        continue;
                    }
                },
            };
            let gateway = gateway.clone();
            let service = service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
                let gateway = gateway.clone();
                async move { Ok::<_, Infallible>(gateway.handle(request.map(Body::from)).await) }
            });
            let connection =
                graceful.watch(builder.serve_connection(TokioIo::new(stream), service));
            connections.spawn(async move {
                if let Err(error) = connection.await {
                    tracing::debug!(%peer, %error, "a gateway connection ended with an error");
                }
            });
        }
        drop(listener);
        tokio::select! {
            () = graceful.shutdown() => {}
            () = tokio::time::sleep(SHUTDOWN_DRAIN_TIMEOUT) => {
                tracing::warn!(
                    connections = connections.len(),
                    "the gateway dropped connections still open at the drain deadline"
                );
            }
        }
        connections.shutdown().await;
    }
}

impl<A> fmt::Debug for GatewayListener<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GatewayListener")
            .field("local_addr", &self.listener.local_addr().ok())
            .finish_non_exhaustive()
    }
}
