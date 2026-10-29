#![forbid(unsafe_code)]
//! SkyS3 intra-cluster transport: TCP with mutual TLS, node identities from
//! the operator's PKI, and frames whose kinds are authorized per role
//! (design §12, §15).
//!
//! Section numbers (§) refer to the [SkyS3 design].
//!
//! [SkyS3 design]: https://github.com/skys3/skys3/blob/main/docs/skys3-design.md
//!
//! - **Identity.** Every certificate names its holder with one SPIFFE ID,
//!   `spiffe://<cluster-id>/<role>/<name>`: a node (`node/<node-id>`) or an
//!   operator tool (`admin/<name>`). See [`PeerIdentity`].
//! - **Handshake.** Both ends present certificates and verify the other's
//!   chain against the cluster's CA bundle, its validity period, and its
//!   identity, inside a TLS 1.3 handshake that negotiates
//!   [`ALPN_PROTOCOL`]. A client also checks that the server is the node
//!   it meant to reach. See [`Credentials`] and [`Transport`].
//! - **Frames.** A length-prefixed `prost` header and a raw payload, with
//!   both lengths bounded before anything is allocated. See [`Frame`].
//! - **Authorization.** Each [`MessageKind`] belongs to a [`MessageClass`],
//!   and a connection refuses a frame whose class the sender's [`Role`]
//!   may not send. Shard-level roles (which node is the primary of an
//!   epoch) are checked by the protocol layers, which read the peer's node
//!   ID from [`Connection::peer`].
//! - **Network.** [`Transport`] runs over any [`Network`]: the operating
//!   system's TCP ([`TokioNetwork`]) or, with the `turmoil` feature,
//!   `turmoil`'s simulated network.
//!
//! ```no_run
//! use skys3_net::{Credentials, Frame, Header, MessageKind, TokioNetwork, Transport};
//! use skys3_types::{ClusterId, NodeId};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let credentials = Credentials::load(
//!     ClusterId::new("prod-a")?,
//!     "/etc/skys3/node.crt".as_ref(),
//!     "/etc/skys3/node.key".as_ref(),
//!     "/etc/skys3/cluster-ca.crt".as_ref(),
//! )?;
//! let transport = Transport::new(TokioNetwork, &credentials);
//! let peer = NodeId::new("node-2")?;
//! let mut connection = transport
//!     .connect(&peer, &"node-2.example.internal:7400".parse()?)
//!     .await?;
//! connection
//!     .send(&Frame::new(Header::new(MessageKind::Beacon).with_request_id(1), ""))
//!     .await?;
//! let reply = connection.recv().await?;
//! # Ok(())
//! # }
//! ```

mod frame;
mod identity;
mod message;
mod network;
mod pki;
mod transport;

pub use frame::{
    Frame, FrameError, Header, MAX_HEADER_LEN, MAX_PAYLOAD_LEN, PREFIX_LEN, read_frame, write_frame,
};
pub use identity::{IdentityError, PeerIdentity, Role};
pub use message::{MessageClass, MessageKind};
#[cfg(feature = "turmoil")]
pub use network::TurmoilNetwork;
pub use network::{Network, TokioNetwork};
pub use pki::{Credentials, PkiError};
/// The DER types [`Credentials::new`] takes, re-exported from `rustls`.
pub use rustls::pki_types::{CertificateDer, PrivateKeyDer};
pub use transport::{
    ALPN_PROTOCOL, Connection, HANDSHAKE_TIMEOUT, Incoming, Listener, Receiver, Sender, Transport,
    TransportError,
};
