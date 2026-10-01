#![forbid(unsafe_code)]
//! SkyS3's native peer protocol between clusters (design §7.8): its
//! messages, their encoding and limits, and the negotiation of protocol
//! versions and capabilities.
//!
//! Section numbers (§) refer to the [SkyS3 design].
//!
//! [SkyS3 design]: https://github.com/skys3/skys3/blob/main/docs/skys3-design.md
//!
//! A source cluster's flusher sends objects to a destination cluster over
//! QUIC. This crate is the protocol's vocabulary, as pure functions; the
//! QUIC endpoint, staging, and the flusher are built on it (plan M6-02 to
//! M6-07).
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

mod error;
mod frame;
mod message;
mod negotiation;
mod ranges;
mod wire;

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
pub use ranges::ByteRanges;
