//! The QUIC endpoint of the peer transport: one UDP socket that accepts
//! connections from source clusters and opens connections to destination
//! clusters (design §7.8, §12).
//!
//! A connection is usable once both ends have verified each other's
//! certificates and exchanged `HELLO`s. The source opens the connection's
//! first stream for its `HELLO`; the destination answers on it, and both
//! finish it. Each end then derives the [`Session`] with [`negotiate`],
//! and the `HELLO`'s cluster must be the one the certificate names.

use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use quinn::congestion::{BbrConfig, CubicConfig, NewRenoConfig};
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::{IdleTimeout, TransportConfig, VarInt};
use rustls::pki_types::CertificateDer;
use skys3_config::{CongestionControl, PeeringConfig};
use skys3_types::{ClusterId, NodeId};
use tokio::time::{Instant, timeout};

use crate::message::{Hello, Message};
use crate::negotiation::{
    Capabilities, NegotiationError, SUPPORTED_VERSIONS, Session, Side, negotiate,
};
use crate::stream::{InboundStream, MessageStream, STREAM_REFUSED, StreamCount, StreamError};
use crate::tls::{PeerTls, peer_of};
use crate::trust::PeerTrust;

/// How long a connection may go without any packet before it is lost.
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// How often an otherwise quiet connection sends a packet, so that it
/// outlives [`IDLE_TIMEOUT`] and NAT bindings.
pub const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(5);

/// The most streams a source may have open on one connection: one per
/// object or batch in flight.
pub const MAX_STREAMS_PER_CONNECTION: u32 = 256;

/// The flow-control window of a new connection, and the smallest one: the
/// bandwidth-delay product of a 300 Mbit/s path with a 200 ms round trip,
/// at most `peer_max_inflight_bytes`.
pub const INITIAL_WINDOW: u64 = 8 << 20;

/// How often a connection's windows are sized again from its measured
/// bandwidth-delay product.
pub const WINDOW_INTERVAL: Duration = Duration::from_secs(1);

/// Application close codes of a peer connection.
const CLOSE_NORMAL: VarInt = VarInt::from_u32(0);
const CLOSE_PROTOCOL: VarInt = VarInt::from_u32(1);
const CLOSE_NO_COMMON_VERSION: VarInt = VarInt::from_u32(2);
const CLOSE_WRONG_CLUSTER: VarInt = VarInt::from_u32(3);

/// How an endpoint's connections behave, from `[peering]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointSettings {
    /// `congestion_control`: the controller of every connection.
    pub congestion_control: CongestionControl,
    /// `peer_connect_timeout_ms`: how long connecting, or accepting a
    /// connection, may take, handshake and `HELLO`s included.
    pub connect_timeout: Duration,
    /// `peer_max_inflight_bytes`: the cap on stream and connection
    /// windows.
    pub max_inflight_bytes: u64,
    /// The capabilities this end announces in its `HELLO`.
    pub capabilities: Capabilities,
}

impl EndpointSettings {
    /// The settings of the `[peering]` section, announcing every known
    /// capability.
    #[must_use]
    pub fn from_config(config: &PeeringConfig) -> Self {
        Self {
            congestion_control: config.congestion_control,
            connect_timeout: config.peer_connect_timeout(),
            max_inflight_bytes: config.peer_max_inflight_bytes,
            capabilities: Capabilities::KNOWN,
        }
    }

    /// The QUIC transport parameters of every connection.
    fn transport(&self) -> TransportConfig {
        let cap = self.max_inflight_bytes;
        let initial = cap.min(INITIAL_WINDOW);
        let mut transport = TransportConfig::default();
        transport
            .max_idle_timeout(Some(
                IdleTimeout::try_from(IDLE_TIMEOUT).expect("the idle timeout fits a VarInt"),
            ))
            .keep_alive_interval(Some(KEEP_ALIVE_INTERVAL))
            .max_concurrent_bidi_streams(MAX_STREAMS_PER_CONNECTION.into())
            .max_concurrent_uni_streams(VarInt::from_u32(0))
            .datagram_receive_buffer_size(None)
            // A stream may use the whole connection window, which is the
            // bound sized from the bandwidth-delay product.
            .stream_receive_window(varint(cap))
            .receive_window(varint(initial))
            .send_window(initial);
        match self.congestion_control {
            CongestionControl::Cubic => {
                transport.congestion_controller_factory(Arc::new(CubicConfig::default()))
            }
            CongestionControl::NewReno => {
                transport.congestion_controller_factory(Arc::new(NewRenoConfig::default()))
            }
            CongestionControl::Bbr => {
                transport.congestion_controller_factory(Arc::new(BbrConfig::default()))
            }
        };
        transport
    }
}

/// `value` as a QUIC variable-length integer, saturating.
fn varint(value: u64) -> VarInt {
    VarInt::from_u64(value).unwrap_or(VarInt::MAX)
}

/// The flow-control window of a connection: twice its bandwidth-delay
/// product, between [`INITIAL_WINDOW`] and `cap`, both at most `cap`.
///
/// The product is the larger of the congestion window and the bytes
/// delivered in `interval` scaled to one round trip. While flow control
/// limits a connection, its delivery fills the window and the window
/// doubles each interval; once the path limits it, the window settles at
/// twice what the path holds.
#[must_use]
pub fn window_size(rtt: Duration, delivered: u64, interval: Duration, cwnd: u64, cap: u64) -> u64 {
    let interval = interval.as_micros().max(1);
    let delivered_per_rtt = u128::from(delivered) * rtt.as_micros() / interval;
    let bdp = u64::try_from(delivered_per_rtt)
        .unwrap_or(u64::MAX)
        .max(cwnd);
    bdp.saturating_mul(2).clamp(cap.min(INITIAL_WINDOW), cap)
}

/// A destination: a peer cluster, and the address of one of its gateways.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Destination {
    /// The peer cluster, which its gateway's certificate must name.
    pub cluster: ClusterId,
    /// The gateway's UDP address.
    pub address: SocketAddr,
}

/// Why a peer connection could not be set up.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConnectError {
    /// The connection could not be started, for example because the
    /// endpoint is closing.
    #[error("cannot connect: {0}")]
    Connect(#[from] quinn::ConnectError),
    /// The handshake failed or the connection was lost: an untrusted
    /// certificate on either side ends here, as a TLS alert.
    #[error("connection failed: {0}")]
    Connection(#[from] quinn::ConnectionError),
    /// The handshake and `HELLO`s took longer than the connect timeout.
    #[error("the peer did not finish connecting within {0:?}")]
    Timeout(Duration),
    /// The peer's verified certificate names no node of a cluster.
    #[error("the peer presented no usable certificate")]
    NoIdentity,
    /// The `HELLO` exchange failed.
    #[error("HELLO exchange failed: {0}")]
    Hello(#[from] StreamError),
    /// The first message was not a `HELLO`.
    #[error("the peer sent {0} before HELLO")]
    NotHello(&'static str),
    /// The peers speak no common protocol version.
    #[error(transparent)]
    Negotiation(#[from] NegotiationError),
    /// The `HELLO` names another cluster than the certificate.
    #[error("the peer's certificate names cluster {certificate}, its HELLO {hello}")]
    WrongCluster {
        /// The cluster of the verified certificate.
        certificate: ClusterId,
        /// The cluster of the `HELLO`.
        hello: ClusterId,
    },
}

/// A peer endpoint: one UDP socket that serves connections from source
/// clusters and opens connections to destination clusters. Cloning it
/// gives another handle to the same endpoint.
#[derive(Debug, Clone)]
pub struct PeerEndpoint {
    inner: Arc<EndpointInner>,
}

#[derive(Debug)]
struct EndpointInner {
    endpoint: quinn::Endpoint,
    client: quinn::ClientConfig,
    hello: Hello,
    trust: Arc<PeerTrust>,
    settings: EndpointSettings,
}

impl PeerEndpoint {
    /// Binds the endpoint to `address` (`[peering] quic_listen`). It must
    /// run inside a Tokio runtime.
    ///
    /// # Errors
    ///
    /// The I/O error of binding the socket.
    pub fn bind(
        address: SocketAddr,
        tls: &PeerTls,
        settings: EndpointSettings,
    ) -> io::Result<Self> {
        let transport = Arc::new(settings.transport());
        let server_tls = QuicServerConfig::try_from(tls.server_config())
            .expect("the aws-lc-rs provider has QUIC's initial cipher suite");
        let mut server = quinn::ServerConfig::with_crypto(Arc::new(server_tls));
        server.transport_config(transport.clone());
        let client_tls = QuicClientConfig::try_from(tls.client_config())
            .expect("the aws-lc-rs provider has QUIC's initial cipher suite");
        let mut client = quinn::ClientConfig::new(Arc::new(client_tls));
        client.transport_config(transport);
        let endpoint = quinn::Endpoint::server(server, address)?;
        let hello = Hello {
            cluster: tls.cluster().clone(),
            versions: SUPPORTED_VERSIONS,
            capabilities: settings.capabilities,
        };
        Ok(Self {
            inner: Arc::new(EndpointInner {
                endpoint,
                client,
                hello,
                trust: tls.trust().clone(),
                settings,
            }),
        })
    }

    /// The address the endpoint is bound to.
    ///
    /// # Errors
    ///
    /// The I/O error of reading the socket's address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.endpoint.local_addr()
    }

    /// The settings the endpoint was bound with.
    #[must_use]
    pub fn settings(&self) -> &EndpointSettings {
        &self.inner.settings
    }

    /// Connects to `destination` as a source: a handshake that verifies
    /// the gateway's certificate against the peer's trust bundle and its
    /// cluster, then the `HELLO`s, all within the connect timeout.
    ///
    /// # Errors
    ///
    /// [`ConnectError`]. A destination that refuses this node's
    /// certificate fails the handshake on its side; the refusal arrives
    /// here as [`ConnectError::Connection`].
    pub async fn connect(&self, destination: &Destination) -> Result<PeerConnection, ConnectError> {
        let inner = &self.inner;
        let connecting = inner.endpoint.connect_with(
            inner.client.clone(),
            destination.address,
            destination.cluster.as_str(),
        )?;
        let limit = inner.settings.connect_timeout;
        timeout(limit, async {
            let connection = connecting.await?;
            self.establish(connection, Side::Source).await
        })
        .await
        .map_err(|_| ConnectError::Timeout(limit))?
    }

    /// Waits for the next connection from a source, or `None` once the
    /// endpoint is closed. The handshake runs in [`Incoming::establish`],
    /// so a caller can establish each connection on its own task.
    pub async fn accept(&self) -> Option<Incoming> {
        let incoming = self.inner.endpoint.accept().await?;
        Some(Incoming {
            incoming,
            endpoint: self.clone(),
        })
    }

    /// Closes every connection and stops accepting new ones.
    pub fn close(&self) {
        self.inner.endpoint.close(CLOSE_NORMAL, b"endpoint closed");
    }

    /// Waits until every connection is closed and its close is delivered,
    /// or the peers' idle timeouts pass.
    pub async fn wait_idle(&self) {
        self.inner.endpoint.wait_idle().await;
    }

    /// Identifies the peer of a handshaken connection, exchanges `HELLO`s,
    /// and starts sizing its windows.
    async fn establish(
        &self,
        connection: quinn::Connection,
        side: Side,
    ) -> Result<PeerConnection, ConnectError> {
        let inner = &self.inner;
        let (cluster, node) = connection
            .peer_identity()
            .and_then(|identity| identity.downcast::<Vec<CertificateDer<'static>>>().ok())
            .and_then(|chain| chain.first().and_then(peer_of))
            .ok_or(ConnectError::NoIdentity)?;
        let remote = match exchange_hello(&connection, side, &inner.hello).await {
            Ok(remote) => remote,
            Err(error) => {
                connection.close(CLOSE_PROTOCOL, b"HELLO exchange failed");
                return Err(error);
            }
        };
        if remote.cluster != cluster {
            connection.close(CLOSE_WRONG_CLUSTER, b"HELLO names another cluster");
            return Err(ConnectError::WrongCluster {
                certificate: cluster,
                hello: remote.cluster,
            });
        }
        let session = negotiate(&inner.hello, &remote).inspect_err(|_| {
            connection.close(CLOSE_NO_COMMON_VERSION, b"no common protocol version");
        })?;
        let shared = Arc::new(Shared {
            connection,
            node,
            session: Arc::new(session),
            side,
            trust: inner.trust.clone(),
            streams: Arc::new(AtomicUsize::new(0)),
        });
        spawn_window_sizing(Arc::downgrade(&shared), inner.settings.max_inflight_bytes);
        Ok(PeerConnection { shared })
    }
}

/// A connection a source is opening to this endpoint.
#[derive(Debug)]
pub struct Incoming {
    incoming: quinn::Incoming,
    endpoint: PeerEndpoint,
}

impl Incoming {
    /// The address the connection comes from.
    #[must_use]
    pub fn remote_address(&self) -> SocketAddr {
        self.incoming.remote_address()
    }

    /// Completes the handshake, which verifies the source's certificate
    /// against the trust bundle of the cluster it names, and the `HELLO`s,
    /// all within the connect timeout.
    ///
    /// # Errors
    ///
    /// [`ConnectError`]; an untrusted source fails the handshake with
    /// [`ConnectError::Connection`].
    pub async fn establish(self) -> Result<PeerConnection, ConnectError> {
        let limit = self.endpoint.inner.settings.connect_timeout;
        timeout(limit, async {
            let connection = self.incoming.await?;
            self.endpoint.establish(connection, Side::Destination).await
        })
        .await
        .map_err(|_| ConnectError::Timeout(limit))?
    }
}

/// Sends this end's `HELLO` and receives the peer's on the connection's
/// first stream, which the source opens.
async fn exchange_hello(
    connection: &quinn::Connection,
    side: Side,
    hello: &Hello,
) -> Result<Hello, ConnectError> {
    let ours = Message::Hello(hello.clone())
        .encode()
        .map_err(StreamError::from)?;
    let (mut send, mut recv) = match side {
        Side::Source => connection.open_bi().await?,
        Side::Destination => connection.accept_bi().await?,
    };
    if side == Side::Source {
        send.write_all(&ours).await.map_err(StreamError::from)?;
        send.finish().map_err(StreamError::from)?;
    }
    let theirs = match crate::stream::read_message(&mut recv).await? {
        Some(Message::Hello(theirs)) => theirs,
        Some(other) => return Err(ConnectError::NotHello(other.name())),
        None => return Err(StreamError::Truncated.into()),
    };
    if side == Side::Destination {
        send.write_all(&ours).await.map_err(StreamError::from)?;
        send.finish().map_err(StreamError::from)?;
    }
    Ok(theirs)
}

/// An established connection to a peer cluster. Cloning it gives another
/// handle to the same connection, which closes once every handle and
/// stream is dropped.
#[derive(Debug, Clone)]
pub struct PeerConnection {
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    connection: quinn::Connection,
    node: NodeId,
    session: Arc<Session>,
    side: Side,
    trust: Arc<PeerTrust>,
    streams: Arc<AtomicUsize>,
}

impl PeerConnection {
    /// The peer cluster, as its certificate and `HELLO` name it.
    #[must_use]
    pub fn peer(&self) -> &ClusterId {
        &self.shared.session.peer
    }

    /// The peer node whose certificate the connection verified.
    #[must_use]
    pub fn peer_node(&self) -> &NodeId {
        &self.shared.node
    }

    /// What the two `HELLO`s agreed on.
    #[must_use]
    pub fn session(&self) -> &Session {
        &self.shared.session
    }

    /// This end's side of the connection.
    #[must_use]
    pub fn side(&self) -> Side {
        self.shared.side
    }

    /// The peer's address.
    #[must_use]
    pub fn remote_address(&self) -> SocketAddr {
        self.shared.connection.remote_address()
    }

    /// The connection's current round-trip estimate.
    #[must_use]
    pub fn rtt(&self) -> Duration {
        self.shared.connection.rtt()
    }

    /// The connection's transport statistics.
    #[must_use]
    pub fn stats(&self) -> quinn::ConnectionStats {
        self.shared.connection.stats()
    }

    /// Why the connection closed, or `None` while it is open.
    #[must_use]
    pub fn close_reason(&self) -> Option<quinn::ConnectionError> {
        self.shared.connection.close_reason()
    }

    /// The streams of this connection that are open on this side.
    #[must_use]
    pub fn open_streams(&self) -> usize {
        self.shared.streams.load(Ordering::Relaxed)
    }

    /// Opens a stream for one object or batch.
    ///
    /// # Errors
    ///
    /// [`StreamError::Connection`] if the connection is lost.
    pub async fn open_stream(&self) -> Result<MessageStream, StreamError> {
        self.open_reserved(self.reserve_stream()).await
    }

    /// Counts a stream as open before it is, so that callers choosing a
    /// connection while the stream is being opened see it. Dropping the
    /// reservation, as a failed or cancelled open does, releases it.
    pub(crate) fn reserve_stream(&self) -> StreamCount {
        StreamCount::new(&self.shared.streams)
    }

    /// Opens a stream for a reservation of this connection.
    pub(crate) async fn open_reserved(
        &self,
        reservation: StreamCount,
    ) -> Result<MessageStream, StreamError> {
        let shared = &self.shared;
        let streams = shared.connection.open_bi().await?;
        Ok(MessageStream::new(
            streams,
            shared.session.clone(),
            shared.side,
            reservation,
        ))
    }

    /// Accepts the next stream a source opens, on the destination's side.
    /// Its messages are authorized against the peer's bucket pairs.
    ///
    /// # Errors
    ///
    /// [`StreamError::Connection`] once the connection is closed or lost,
    /// and [`StreamError::ZeroRtt`] for a stream opened in 0-RTT, which
    /// is stopped.
    pub async fn accept_stream(&self) -> Result<InboundStream, StreamError> {
        let shared = &self.shared;
        let (send, mut recv) = shared.connection.accept_bi().await?;
        if recv.is_0rtt() {
            let _ = recv.stop(STREAM_REFUSED);
            return Err(StreamError::ZeroRtt);
        }
        let stream = MessageStream::new(
            (send, recv),
            shared.session.clone(),
            shared.side,
            StreamCount::new(&shared.streams),
        );
        Ok(InboundStream::new(
            stream,
            self.peer().clone(),
            shared.trust.clone(),
        ))
    }

    /// Closes the connection. Streams still open are reset.
    pub fn close(&self) {
        self.shared.connection.close(CLOSE_NORMAL, b"closed");
    }

    /// Waits until the connection is closed, and returns why.
    pub async fn closed(&self) -> quinn::ConnectionError {
        self.shared.connection.closed().await
    }
}

/// Sizes a connection's windows from its bandwidth-delay product every
/// [`WINDOW_INTERVAL`], until the connection closes or every handle to it
/// is dropped. The task holds the connection only while it sizes it, so
/// it never keeps an unused connection open.
fn spawn_window_sizing(shared: Weak<Shared>, cap: u64) {
    tokio::spawn(async move {
        let mut last: Option<(Instant, u64, u64)> = None;
        loop {
            tokio::time::sleep(WINDOW_INTERVAL).await;
            let Some(shared) = shared.upgrade() else {
                return;
            };
            let connection = &shared.connection;
            if connection.close_reason().is_some() {
                return;
            }
            let stats = connection.stats();
            let now = Instant::now();
            let (tx, rx) = (stats.udp_tx.bytes, stats.udp_rx.bytes);
            if let Some((at, last_tx, last_rx)) = last {
                let delivered = tx.saturating_sub(last_tx).max(rx.saturating_sub(last_rx));
                let window = window_size(stats.path.rtt, delivered, now - at, stats.path.cwnd, cap);
                connection.set_receive_window(varint(window));
                connection.set_send_window(window);
            }
            last = Some((now, tx, rx));
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1 << 20;

    #[test]
    fn windows_follow_the_bandwidth_delay_product() {
        let second = Duration::from_secs(1);
        let rtt = Duration::from_millis(200);
        // 100 MiB/s for 200 ms is 20 MiB in flight; the window is twice it.
        assert_eq!(window_size(rtt, 100 * MIB, second, 0, 256 * MIB), 40 * MIB);
        // The congestion window counts when it is larger.
        assert_eq!(window_size(rtt, 0, second, 30 * MIB, 256 * MIB), 60 * MIB);
        // Never below the initial window, never above the cap.
        assert_eq!(window_size(rtt, 0, second, 0, 256 * MIB), INITIAL_WINDOW);
        assert_eq!(window_size(rtt, u64::MAX, second, 0, 256 * MIB), 256 * MIB);
        assert_eq!(window_size(rtt, 0, second, 0, MIB), MIB);
        assert_eq!(window_size(rtt, MIB, Duration::ZERO, 0, 64 * MIB), 64 * MIB);
    }

    #[test]
    fn settings_come_from_the_peering_section() {
        let config = PeeringConfig {
            congestion_control: CongestionControl::Bbr,
            ..PeeringConfig::default()
        };
        let settings = EndpointSettings::from_config(&config);
        assert_eq!(settings.congestion_control, CongestionControl::Bbr);
        assert_eq!(settings.connect_timeout, Duration::from_secs(3));
        assert_eq!(settings.max_inflight_bytes, 256 * MIB);
        assert_eq!(settings.capabilities, Capabilities::KNOWN);
        for congestion_control in [
            CongestionControl::Cubic,
            CongestionControl::NewReno,
            CongestionControl::Bbr,
        ] {
            let settings = EndpointSettings {
                congestion_control,
                ..settings.clone()
            };
            // Builds without panicking, whatever the controller.
            let _ = settings.transport();
        }
        assert_eq!(varint(u64::MAX), VarInt::MAX);
    }
}
