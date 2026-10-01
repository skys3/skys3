//! Authenticated connections: listening, connecting, and exchanging
//! frames that the peer's role allows.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::ServerName;
use skys3_types::{ClusterId, NodeAddress, NodeId};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio_rustls::{TlsAcceptor, TlsConnector, TlsStream};

use crate::frame::{Frame, FrameError, read_frame, write_frame};
use crate::identity::{IdentityError, PeerIdentity, Role};
use crate::message::MessageKind;
use crate::network::Network;
use crate::pki::{Credentials, identity_of};

/// The ALPN protocol both ends must agree on. Its version names the frame
/// format and the meaning of message kinds; a node that speaks another
/// version fails the handshake instead of misreading frames.
pub const ALPN_PROTOCOL: &[u8] = b"skys3-cluster/1";

/// How long connecting, including the TLS handshake, or accepting a
/// connection's handshake may take.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// The server name given to TLS. Peers are authenticated by the SPIFFE ID
/// in their certificates, so this name is neither sent nor checked.
const SERVER_NAME: &str = "skys3-node";

/// Why a connection could not be set up or used. After any error other
/// than [`TransportError::Unauthorized`] from a local send, the connection
/// is unusable and should be dropped.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TransportError {
    /// Binding, accepting, or connecting at the TCP level failed.
    #[error("network error: {0}")]
    Io(#[source] io::Error),
    /// The TLS handshake failed; [`TransportError::tls_error`] gives the
    /// reason, such as an unknown CA or an expired certificate.
    #[error("TLS handshake failed: {0}")]
    Handshake(#[source] io::Error),
    /// The connection or its handshake took longer than
    /// [`HANDSHAKE_TIMEOUT`].
    #[error("the handshake did not finish within {HANDSHAKE_TIMEOUT:?}")]
    HandshakeTimeout,
    /// The peer did not negotiate [`ALPN_PROTOCOL`].
    #[error("the peer does not speak {}", String::from_utf8_lossy(ALPN_PROTOCOL))]
    WrongProtocol,
    /// The peer's verified certificate does not carry a usable identity.
    #[error("the peer's identity is not usable: {0}")]
    Identity(#[source] IdentityError),
    /// The peer is a node of the cluster, but not the one connected to.
    #[error("connected to {found}, expected node {expected}")]
    UnexpectedPeer {
        /// The node the caller meant to reach.
        expected: NodeId,
        /// The identity the peer proved.
        found: PeerIdentity,
    },
    /// Only nodes accept connections.
    #[error("credentials with role {0} cannot accept connections; only nodes serve")]
    NotANode(Role),
    /// A message kind the sender's role may not send: received from a
    /// peer, or about to be sent by this side.
    #[error("{role} may not send {kind:?} messages")]
    Unauthorized {
        /// The sender's role.
        role: Role,
        /// The kind of message.
        kind: MessageKind,
    },
    /// Reading or writing a frame failed.
    #[error(transparent)]
    Frame(#[from] FrameError),
}

impl TransportError {
    /// The TLS error behind a failed handshake or a failed read, if any.
    #[must_use]
    pub fn tls_error(&self) -> Option<&rustls::Error> {
        let io = match self {
            Self::Handshake(error) | Self::Frame(FrameError::Io(error)) => error,
            _ => return None,
        };
        io.get_ref()?.downcast_ref()
    }
}

/// The intra-cluster transport of one holder of credentials: a node, or an
/// operator tool that only connects.
#[derive(Clone)]
pub struct Transport<N> {
    network: N,
    identity: PeerIdentity,
    cluster: ClusterId,
    acceptor: Option<TlsAcceptor>,
    connector: TlsConnector,
}

impl<N: Network> Transport<N> {
    /// A transport over `network` with `credentials`.
    #[must_use]
    pub fn new(network: N, credentials: &Credentials) -> Self {
        Self {
            network,
            identity: credentials.identity().clone(),
            cluster: credentials.cluster().clone(),
            acceptor: credentials.server_config().map(TlsAcceptor::from),
            connector: TlsConnector::from(credentials.client_config()),
        }
    }

    /// This side's identity.
    #[must_use]
    pub fn identity(&self) -> &PeerIdentity {
        &self.identity
    }

    /// Binds a listener to `addr`.
    ///
    /// # Errors
    ///
    /// [`TransportError::NotANode`] for an operator tool's credentials, and
    /// [`TransportError::Io`] if the address cannot be bound.
    pub async fn bind(&self, addr: SocketAddr) -> Result<Listener<N>, TransportError> {
        let acceptor = self
            .acceptor
            .clone()
            .ok_or(TransportError::NotANode(self.identity.role()))?;
        let listener = self.network.bind(addr).await.map_err(TransportError::Io)?;
        Ok(Listener {
            listener,
            acceptor,
            cluster: self.cluster.clone(),
            role: self.identity.role(),
        })
    }

    /// Connects to `node` at `addr`, completing the handshake within
    /// [`HANDSHAKE_TIMEOUT`]. The peer must prove that it is `node`.
    ///
    /// In TLS 1.3 the server checks this side's certificate after the
    /// client has finished its part of the handshake, so a server that
    /// refuses this side shows up as an error on the first receive.
    ///
    /// # Errors
    ///
    /// [`TransportError`] if the connection fails, the handshake fails or
    /// times out, or the peer is not `node`.
    pub async fn connect(
        &self,
        node: &NodeId,
        addr: &NodeAddress,
    ) -> Result<Connection<N::Stream>, TransportError> {
        let connecting = async {
            let stream = self
                .network
                .connect(addr)
                .await
                .map_err(TransportError::Io)?;
            let name = ServerName::try_from(SERVER_NAME).expect("a valid DNS name");
            let stream = self
                .connector
                .connect(name, stream)
                .await
                .map_err(TransportError::Handshake)?;
            Connection::established(
                TlsStream::Client(stream),
                &self.cluster,
                self.identity.role(),
            )
        };
        let connection = tokio::time::timeout(HANDSHAKE_TIMEOUT, connecting)
            .await
            .map_err(|_| TransportError::HandshakeTimeout)??;
        match connection.peer() {
            PeerIdentity::Node(id) if id == node => Ok(connection),
            found => Err(TransportError::UnexpectedPeer {
                expected: node.clone(),
                found: found.clone(),
            }),
        }
    }
}

/// A bound listener of a node's transport.
pub struct Listener<N: Network> {
    listener: N::Listener,
    acceptor: TlsAcceptor,
    cluster: ClusterId,
    role: Role,
}

impl<N: Network> Listener<N> {
    /// The address the listener is bound to.
    ///
    /// # Errors
    ///
    /// The error of the underlying socket.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        N::local_addr(&self.listener)
    }

    /// Accepts the next TCP connection. Its handshake has not run yet: run
    /// [`Incoming::handshake`] on a task of its own, so a slow or hostile
    /// client cannot hold up the accept loop.
    ///
    /// # Errors
    ///
    /// The error of the underlying socket.
    pub async fn accept(&self) -> io::Result<Incoming<N::Stream>> {
        let (stream, remote) = N::accept(&self.listener).await?;
        Ok(Incoming {
            stream,
            remote,
            acceptor: self.acceptor.clone(),
            cluster: self.cluster.clone(),
            role: self.role,
        })
    }
}

/// An accepted TCP connection whose TLS handshake has not run yet.
pub struct Incoming<S> {
    stream: S,
    remote: SocketAddr,
    acceptor: TlsAcceptor,
    cluster: ClusterId,
    role: Role,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Incoming<S> {
    /// The remote address, for logs. Peers are authenticated by their
    /// certificates, never by address.
    #[must_use]
    pub fn remote_addr(&self) -> SocketAddr {
        self.remote
    }

    /// Runs the TLS handshake within [`HANDSHAKE_TIMEOUT`]: the client must
    /// present a certificate that verifies against the cluster's CA and
    /// carries one of its identities.
    ///
    /// # Errors
    ///
    /// [`TransportError`] if the handshake fails or times out.
    pub async fn handshake(self) -> Result<Connection<S>, TransportError> {
        let accepting = async {
            let stream = self
                .acceptor
                .accept(self.stream)
                .await
                .map_err(TransportError::Handshake)?;
            Connection::established(TlsStream::Server(stream), &self.cluster, self.role)
        };
        tokio::time::timeout(HANDSHAKE_TIMEOUT, accepting)
            .await
            .map_err(|_| TransportError::HandshakeTimeout)?
    }
}

/// An authenticated connection to a peer.
///
/// [`Connection::send`] returns only once the frame is written to the
/// socket, and a frame larger than the socket buffers is written only as
/// the peer reads it. So a task that sends large frames must not be the
/// one that reads the peer's frames, or two peers that send at once wait
/// for each other forever: split the connection with
/// [`Connection::into_split`] and receive on a task of its own. Calling
/// `send` and `recv` from one task suits request and reply exchanges
/// where only one side writes at a time.
pub struct Connection<S> {
    receiver: Receiver<S>,
    sender: Sender<S>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Connection<S> {
    /// Checks the negotiated protocol and reads the peer's identity from
    /// its verified certificate.
    fn established(
        stream: TlsStream<S>,
        cluster: &ClusterId,
        role: Role,
    ) -> Result<Self, TransportError> {
        let (_, session) = stream.get_ref();
        if session.alpn_protocol() != Some(ALPN_PROTOCOL) {
            return Err(TransportError::WrongProtocol);
        }
        let leaf = session
            .peer_certificates()
            .and_then(<[_]>::first)
            .ok_or(TransportError::Identity(IdentityError::Missing))?;
        let peer = identity_of(leaf, cluster).map_err(|error| {
            // The verifier accepted this certificate, so it parses.
            TransportError::Handshake(io::Error::new(io::ErrorKind::InvalidData, error))
        })?;
        let peer = Arc::new(peer);
        let (reader, writer) = tokio::io::split(stream);
        Ok(Self {
            receiver: Receiver {
                reader,
                peer: peer.clone(),
            },
            sender: Sender { writer, role, peer },
        })
    }

    /// The peer's authenticated identity.
    #[must_use]
    pub fn peer(&self) -> &PeerIdentity {
        &self.receiver.peer
    }

    /// Sends a frame; see [`Sender::send`].
    ///
    /// # Errors
    ///
    /// As [`Sender::send`].
    pub async fn send(&mut self, frame: &Frame) -> Result<(), TransportError> {
        self.sender.send(frame).await
    }

    /// Receives a frame; see [`Receiver::recv`].
    ///
    /// # Errors
    ///
    /// As [`Receiver::recv`].
    pub async fn recv(&mut self) -> Result<Option<Frame>, TransportError> {
        self.receiver.recv().await
    }

    /// Closes the sending direction; see [`Sender::close`].
    ///
    /// # Errors
    ///
    /// As [`Sender::close`].
    pub async fn close(&mut self) -> Result<(), TransportError> {
        self.sender.close().await
    }

    /// Splits the connection so frames can be received and sent from
    /// different tasks.
    #[must_use]
    pub fn into_split(self) -> (Receiver<S>, Sender<S>) {
        (self.receiver, self.sender)
    }
}

/// The receiving half of a [`Connection`].
pub struct Receiver<S> {
    reader: ReadHalf<TlsStream<S>>,
    peer: Arc<PeerIdentity>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Receiver<S> {
    /// The peer's authenticated identity.
    #[must_use]
    pub fn peer(&self) -> &PeerIdentity {
        &self.peer
    }

    /// Receives the next frame, or `None` once the peer has closed the
    /// connection.
    ///
    /// # Errors
    ///
    /// [`TransportError::Unauthorized`] for a frame of a kind the peer's
    /// role may not send, and [`TransportError::Frame`] for an invalid
    /// frame or a broken connection.
    pub async fn recv(&mut self) -> Result<Option<Frame>, TransportError> {
        let Some(frame) = read_frame(&mut self.reader).await? else {
            return Ok(None);
        };
        let role = self.peer.role();
        if !role.may_send_kind(frame.header.kind) {
            return Err(TransportError::Unauthorized {
                role,
                kind: frame.header.kind,
            });
        }
        Ok(Some(frame))
    }
}

/// The sending half of a [`Connection`].
pub struct Sender<S> {
    writer: WriteHalf<TlsStream<S>>,
    role: Role,
    peer: Arc<PeerIdentity>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Sender<S> {
    /// The peer's authenticated identity.
    #[must_use]
    pub fn peer(&self) -> &PeerIdentity {
        &self.peer
    }

    /// Sends a frame and flushes it to the network.
    ///
    /// # Errors
    ///
    /// [`TransportError::Unauthorized`] if this side's role may not send
    /// the frame's kind (nothing is sent, and the connection stays
    /// usable), and [`TransportError::Frame`] for a frame over the limits
    /// or a broken connection.
    pub async fn send(&mut self, frame: &Frame) -> Result<(), TransportError> {
        let kind = frame.header.kind;
        if !self.role.may_send_kind(kind) {
            return Err(TransportError::Unauthorized {
                role: self.role,
                kind,
            });
        }
        write_frame(&mut self.writer, frame).await?;
        Ok(())
    }

    /// Closes the sending direction with a TLS `close_notify`; the peer's
    /// receiver then returns `None`.
    ///
    /// # Errors
    ///
    /// [`TransportError::Frame`] if the connection is broken.
    pub async fn close(&mut self) -> Result<(), TransportError> {
        self.writer
            .shutdown()
            .await
            .map_err(|error| TransportError::Frame(error.into()))
    }
}
