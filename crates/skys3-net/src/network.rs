//! The TCP layer under the transport: real sockets, or `turmoil`'s
//! simulated network in tests.

use std::io;
use std::net::SocketAddr;

use skys3_types::{Host, NodeAddress};
use tokio::io::{AsyncRead, AsyncWrite};

/// A TCP implementation: binds listeners, accepts, and connects.
///
/// [`TokioNetwork`] uses the operating system's sockets. With the
/// `turmoil` feature, `TurmoilNetwork` uses `turmoil`'s simulated
/// network, so the same transport runs in deterministic simulations.
pub trait Network: Clone + Send + Sync + 'static {
    /// A connected TCP stream.
    type Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static;
    /// A bound listener.
    type Listener: Send + Sync + 'static;

    /// Binds a listener to `addr`.
    fn bind(&self, addr: SocketAddr) -> impl Future<Output = io::Result<Self::Listener>> + Send;

    /// Accepts the next connection on `listener`, with the remote address.
    fn accept(
        listener: &Self::Listener,
    ) -> impl Future<Output = io::Result<(Self::Stream, SocketAddr)>> + Send;

    /// The address `listener` is bound to.
    ///
    /// # Errors
    ///
    /// The error of the underlying socket.
    fn local_addr(listener: &Self::Listener) -> io::Result<SocketAddr>;

    /// Connects to `addr`, resolving a DNS name first.
    fn connect(&self, addr: &NodeAddress) -> impl Future<Output = io::Result<Self::Stream>> + Send;
}

/// The operating system's TCP, through Tokio. Streams have `TCP_NODELAY`
/// set, since frames are written whole and small acknowledgements must not
/// wait for more data.
#[derive(Debug, Clone, Copy, Default)]
pub struct TokioNetwork;

impl Network for TokioNetwork {
    type Stream = tokio::net::TcpStream;
    type Listener = tokio::net::TcpListener;

    async fn bind(&self, addr: SocketAddr) -> io::Result<Self::Listener> {
        tokio::net::TcpListener::bind(addr).await
    }

    async fn accept(listener: &Self::Listener) -> io::Result<(Self::Stream, SocketAddr)> {
        let (stream, remote) = listener.accept().await?;
        stream.set_nodelay(true)?;
        Ok((stream, remote))
    }

    fn local_addr(listener: &Self::Listener) -> io::Result<SocketAddr> {
        listener.local_addr()
    }

    async fn connect(&self, addr: &NodeAddress) -> io::Result<Self::Stream> {
        let stream = match addr.host() {
            Host::Dns(name) => tokio::net::TcpStream::connect((name.as_str(), addr.port())).await,
            Host::Ipv4(ip) => tokio::net::TcpStream::connect((*ip, addr.port())).await,
            Host::Ipv6(ip) => tokio::net::TcpStream::connect((*ip, addr.port())).await,
        }?;
        stream.set_nodelay(true)?;
        Ok(stream)
    }
}

/// `turmoil`'s simulated TCP. DNS names resolve to simulated hosts; every
/// call must run inside a host or client of a `turmoil` simulation.
#[cfg(feature = "turmoil")]
#[derive(Debug, Clone, Copy, Default)]
pub struct TurmoilNetwork;

#[cfg(feature = "turmoil")]
impl Network for TurmoilNetwork {
    type Stream = turmoil::net::TcpStream;
    type Listener = turmoil::net::TcpListener;

    async fn bind(&self, addr: SocketAddr) -> io::Result<Self::Listener> {
        turmoil::net::TcpListener::bind(addr).await
    }

    async fn accept(listener: &Self::Listener) -> io::Result<(Self::Stream, SocketAddr)> {
        listener.accept().await
    }

    fn local_addr(listener: &Self::Listener) -> io::Result<SocketAddr> {
        listener.local_addr()
    }

    async fn connect(&self, addr: &NodeAddress) -> io::Result<Self::Stream> {
        match addr.host() {
            Host::Dns(name) => turmoil::net::TcpStream::connect((name.as_str(), addr.port())).await,
            Host::Ipv4(ip) => turmoil::net::TcpStream::connect((*ip, addr.port())).await,
            Host::Ipv6(ip) => turmoil::net::TcpStream::connect((*ip, addr.port())).await,
        }
    }
}
