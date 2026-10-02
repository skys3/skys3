#![forbid(unsafe_code)]
//! SkyS3's native peer protocol between clusters (design §7.8): its
//! messages, their encoding and limits, the negotiation of protocol
//! versions and capabilities, and the QUIC endpoint and connection pool
//! that carry them.
//!
//! Section numbers (§) refer to the [SkyS3 design].
//!
//! [SkyS3 design]: https://github.com/skys3/skys3/blob/main/docs/skys3-design.md
//!
//! A source cluster's flusher sends objects to a destination cluster over
//! QUIC. This crate holds the protocol's vocabulary, as pure functions,
//! and the QUIC endpoint and connection pool that carry it; staging and
//! the flusher are built on them (plan M6-03 to M6-07).
//!
//! ```text
//! source                                           destination
//!   HELLO(cluster, versions, capabilities)  <->    HELLO           once per connection
//!
//!   BEGIN(identity, bucket, key)            ->                     one stream per object
//!                                           <-     RESUME(identity, durable ranges)
//!   DATA(piece, offset) + bytes             ->
//!                                           <-     DURABLE(identity, ranges)   cumulative
//!   COMMIT(identity, precondition, write)   ->
//!                                           <-     APPLIED(identity, outcome)
//!
//!   BATCH(commits) + inline bytes           ->                     one stream per batch
//!                                           <-     APPLIED per item
//!
//!   ABORT(identity, reason)                 <->    staging discarded
//! ```
//!
//! - **Messages** ([`Message`]): one type per message of the design, with
//!   [`Message::validate`] checking every limit. A *piece* is a staged
//!   byte sequence that a `COMMIT` publishes: the body of a single PUT, or
//!   one part of a multipart upload.
//! - **Frames** ([`Message::encode`], [`Message::decode`]): a
//!   length-prefixed `prost` header and a raw payload, both bounded before
//!   anything is allocated, as on intra-cluster connections. `DATA` and
//!   `BATCH` carry a CRC32C of their payload, checked when they are
//!   decoded.
//! - **Negotiation** ([`negotiate`], [`Session`]): the highest version
//!   both ends speak and the capabilities both have, and which messages
//!   each side may send in the session.
//! - **Durable ranges** ([`ByteRanges`]): what a destination holds of a
//!   piece, and what a source resends after a reconnect.
//! - **Trust** ([`PeerTrust`], [`PeerTls`]): mutual TLS 1.3 between
//!   clusters. Each end presents its node certificate, and the other
//!   verifies it against the CA bundle of the peer cluster the
//!   certificate names. Session resumption and 0-RTT are off, so a
//!   replayed peer message can never apply a mutation. Each peer may write
//!   only the bucket pairs it is authorized for.
//! - **Endpoint** ([`PeerEndpoint`], [`PeerConnection`]): QUIC with the
//!   configured congestion controller, flow-control windows sized from
//!   each connection's bandwidth-delay product and capped by
//!   `peer_max_inflight_bytes`, and a `HELLO` exchange that must name the
//!   certificate's cluster. A destination authorizes every message a
//!   source sends on a stream ([`InboundStream`]) and answers a refused
//!   one with the protocol's refusal.
//! - **Pool** ([`ConnectionPool`]): the connections of every shard a node
//!   hosts, pooled per destination, with a count that adapts like REST
//!   flush concurrency ([`AdaptiveLimit`]).
//!
//! ```no_run
//! use std::sync::Arc;
//!
//! use skys3_config::Config;
//! use skys3_net::Credentials;
//! use skys3_peer::{
//!     ConnectionPool, Destination, EndpointSettings, PeerEndpoint, PeerTls, PeerTrust,
//! };
//! use skys3_types::ClusterId;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = Config::load("/etc/skys3/skys3.toml")?;
//! let files = config.transport().tls_files().ok_or("no node certificate")?;
//! let credentials = Credentials::load(
//!     config.cluster().cluster_id.clone(),
//!     files.cert,
//!     files.key,
//!     files.ca,
//! )?;
//! let trust = Arc::new(PeerTrust::load(config.peering())?);
//! let tls = PeerTls::new(&credentials, trust).ok_or("not a node certificate")?;
//! let endpoint = PeerEndpoint::bind(
//!     config.peering().quic_listen,
//!     &tls,
//!     EndpointSettings::from_config(config.peering()),
//! )?;
//! let pool = ConnectionPool::new(endpoint, config.peering().peer_connections_per_shard);
//! let shard = pool.attach(Destination {
//!     cluster: ClusterId::new("skys3-prod-eu")?,
//!     address: "198.51.100.7:7443".parse()?,
//! });
//! let mut stream = shard.open_stream().await?;
//! # Ok(())
//! # }
//! ```

mod endpoint;
mod error;
mod frame;
mod message;
mod negotiation;
mod pool;
mod ranges;
mod stream;
mod tls;
mod trust;
mod wire;

pub use endpoint::{
    ConnectError, Destination, EndpointSettings, IDLE_TIMEOUT, INITIAL_WINDOW, Incoming,
    KEEP_ALIVE_INTERVAL, MAX_STREAMS_PER_CONNECTION, PeerConnection, PeerEndpoint, WINDOW_INTERVAL,
    window_size,
};

pub use error::MessageError;
pub use frame::{MAX_HEADER_LEN, MAX_PAYLOAD_LEN, PREFIX_LEN, parse_prefix};
pub use message::{
    Abort, AbortReason, Applied, ApplyError, Batch, Begin, Commit, Data, Hello, MAX_BATCH_ITEMS,
    MAX_OBJECT_BYTES, MAX_PIECE_BYTES, MAX_REASON_LEN, MAX_REPORTED_PIECES, MAX_REPORTED_RANGES,
    Message, Outcome, Precondition, Put, PutData, StagedPart, StagedRanges, Write,
};
pub use negotiation::{
    Capabilities, NegotiationError, PROTOCOL_VERSION, ProtocolError, SUPPORTED_VERSIONS, Session,
    Side, VersionRange, negotiate,
};
pub use pool::{
    AdaptiveLimit, ConnectionPool, GROWTH_THRESHOLD, LATENCY_SLACK, LATENCY_TOLERANCE, Measurement,
    Meter, PoolError, PoolStats, Sample, ShardLease, TransportMeter,
};
pub use ranges::ByteRanges;
pub use stream::{
    InboundStream, MessageReceiver, MessageSender, MessageStream, STREAM_REFUSED, StreamError,
};
pub use tls::{ALPN, PeerTls};
pub use trust::{PeerTrust, TrustError, Unauthorized};
