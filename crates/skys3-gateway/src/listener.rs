//! The gateway's HTTP/1.1 listener, over TCP or TLS.

use std::convert::Infallible;
use std::fmt;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::server::graceful::{GracefulShutdown, Watcher};
use s3s::Body;
use skys3_obs::{HandshakeLimit, HandshakeOutcome};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio_rustls::TlsAcceptor;

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

/// The TLS handshakes a listener runs at once unless
/// [`GatewayListener::with_handshake_limit`] sets another cap.
const DEFAULT_MAX_HANDSHAKES: u32 = 256;

/// A bound gateway listener, ready to serve.
pub struct GatewayListener<A> {
    listener: TcpListener,
    gateway: Gateway<A>,
    tls: Option<TlsAcceptor>,
    handshakes: HandshakeLimit,
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
            tls: None,
            handshakes: HandshakeLimit::new(DEFAULT_MAX_HANDSHAKES),
        })
    }

    /// Serves HTTPS with `config` instead of plain HTTP. The handshake runs
    /// on the connection's task, within the time a client has to send a
    /// request head, so a slow client never delays others.
    #[must_use]
    pub fn with_tls(mut self, config: Arc<rustls::ServerConfig>) -> Self {
        self.tls = Some(TlsAcceptor::from(config));
        self
    }

    /// Caps the TLS handshakes the listener runs at once with `limit`
    /// (design §12; `[gateway] max_inflight_handshakes`): a connection
    /// gets a task only once a handshake slot is free, and the listener
    /// accepts no other meanwhile, so further connections wait in the
    /// kernel's accept queue, which holds no task or TLS state. Without
    /// TLS there is no handshake to cap.
    #[must_use]
    pub fn with_handshake_limit(mut self, limit: HandshakeLimit) -> Self {
        self.handshakes = limit;
        self
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
        let Self {
            listener,
            gateway,
            tls,
            handshakes,
        } = self;
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
            // With TLS, the connection waits for a handshake slot before
            // it gets a task, and the listener accepts nothing meanwhile.
            let handshake = match &tls {
                Some(tls) => tokio::select! {
                    () = &mut shutdown => break,
                    permit = handshakes.acquire() => Some((tls.clone(), permit)),
                },
                None => None,
            };
            let gateway = gateway.clone();
            let service = service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
                let gateway = gateway.clone();
                async move { Ok::<_, Infallible>(gateway.handle(request.map(Body::from)).await) }
            });
            let (builder, watcher) = (builder.clone(), graceful.watcher());
            connections.spawn(async move {
                let Some((tls, permit)) = handshake else {
                    return serve_connection(&builder, watcher, stream, service, peer).await;
                };
                match tokio::time::timeout(HEADER_READ_TIMEOUT, tls.accept(stream)).await {
                    Ok(Ok(stream)) => {
                        permit.finish(HandshakeOutcome::Completed);
                        serve_connection(&builder, watcher, stream, service, peer).await;
                    }
                    Ok(Err(error)) => {
                        permit.finish(HandshakeOutcome::Failed);
                        tracing::debug!(%peer, %error, "a TLS handshake with the gateway failed");
                    }
                    Err(_) => {
                        permit.finish(HandshakeOutcome::TimedOut);
                        tracing::debug!(%peer, "a TLS handshake with the gateway timed out");
                    }
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

/// Serves one connection until it closes or shutdown drains it.
async fn serve_connection<I, S>(
    builder: &http1::Builder,
    watcher: Watcher,
    io: I,
    service: S,
    peer: SocketAddr,
) where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    S: hyper::service::Service<
            hyper::Request<hyper::body::Incoming>,
            Response = http::Response<Body>,
            Error = Infallible,
        > + Send
        + 'static,
    S::Future: Send + 'static,
{
    let connection = watcher.watch(builder.serve_connection(TokioIo::new(io), service));
    if let Err(error) = connection.await {
        tracing::debug!(%peer, %error, "a gateway connection ended with an error");
    }
}

impl<A> fmt::Debug for GatewayListener<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GatewayListener")
            .field("local_addr", &self.listener.local_addr().ok())
            .field("tls", &self.tls.is_some())
            .field("handshakes", &self.handshakes)
            .finish_non_exhaustive()
    }
}
