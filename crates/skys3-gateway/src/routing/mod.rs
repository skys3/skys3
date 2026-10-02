//! Routing requests to shard primaries across nodes (design §5.1 step 1,
//! §6.2).
//!
//! - [`ShardMap`]: the configuration, with its epoch, of every shard the
//!   gateway routes to, kept in the node-local index so a restart keeps
//!   it. A configuration only replaces one of an older epoch.
//! - [`RoutedShards`]: the gateway's [`Shards`](crate::Shards) on a
//!   replicated node. Each call goes to the primary the map names, with the
//!   epoch the map knows: in process if that is this node, otherwise over
//!   the intra-cluster transport. A replica that is not the current
//!   primary serves nothing and answers with its configuration as a
//!   redirect hint; the gateway keeps it if it is newer and asks again.
//!   If the primary does not answer, the gateway asks the configuration's
//!   other members, which redirect it too, and it reads the shard's
//!   register from the control store only when none of them answers.
//! - [`ForwardServer`]: a node's end of forwarded requests. It hands each
//!   request to the node's replica of the shard, which serves it only as
//!   the serving primary of its configuration (see
//!   [`Shard::check_readable`](skys3_shard::Shard::check_readable)), and
//!   answers with the replica's configuration when it does not.
//!   [`serve_peers`] accepts the intra-cluster connections of a node and
//!   passes forwarded requests to it and replication links to the
//!   replication service.
//!
//! A GET's bytes do not go through the primary (§9.2): its read plan
//! names the holders, and the gateway registers the read with one of them
//! and fetches from it directly ([`Request::is_holder`]). Those requests
//! go to the node named, are served by any replica of the shard there
//! that holds the bytes, member or primary, and are never redirected.
//!
//! A request whose answer is lost after it was sent is not sent again
//! unless it only reads: a write may have been applied, so the client
//! sees it fail (`503`), which means not acknowledged (§5.2).

mod client;
mod map;
mod server;
#[cfg(test)]
mod tests;
pub mod wire;

use bytes::Bytes;
use skys3_index::{Entry, ListPage, ListQuery, Part, Upload};
use skys3_log::RecordBody;
use skys3_log::record::{Extent, ExtentRef};
use skys3_shard::{ReadId, ReadPlan, Registered};
use skys3_types::{Epoch, EpochSeq, ShardConfig};

pub use client::{RoutedShards, RoutingConfig, RoutingStats};
pub use map::ShardMap;
pub use server::{ForwardServer, Served, serve_peers};

use crate::conditions::{ConditionFailed, Precondition};
use crate::shard::{ShardError, ShardSummary, UploadParts};

/// One call of the gateway's shard interface, as it is forwarded.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
#[expect(
    clippy::large_enum_variant,
    reason = "one value per request, moved a few times; boxing would allocate for each"
)]
pub enum Request {
    /// [`Shards::entry`](crate::Shards::entry).
    Entry {
        /// The key.
        key: String,
    },
    /// [`Shards::list`](crate::Shards::list).
    List(ListQuery),
    /// [`Shards::upload`](crate::Shards::upload).
    Upload {
        /// The key.
        key: String,
        /// The upload.
        upload: EpochSeq,
        /// The part number to list after.
        after: u16,
        /// The most parts.
        limit: usize,
    },
    /// [`Shards::uploads`](crate::Shards::uploads).
    Uploads {
        /// The key prefix.
        prefix: String,
        /// The key and upload to list after.
        after: Option<(String, Option<EpochSeq>)>,
        /// The most uploads.
        limit: usize,
    },
    /// [`Shards::parts`](crate::Shards::parts).
    Parts {
        /// The upload.
        upload: EpochSeq,
        /// The part number to list after.
        after: u16,
        /// The most parts.
        limit: usize,
    },
    /// [`Shards::payload`](crate::Shards::payload).
    Payload(EpochSeq),
    /// [`Shards::plan`](crate::Shards::plan).
    Plan {
        /// The key.
        key: String,
    },
    /// [`Shards::register`](crate::Shards::register), to the holder.
    Register {
        /// The key.
        key: String,
        /// The version the plan named.
        version: EpochSeq,
        /// The version's layout in the plan.
        layout: Vec<ExtentRef>,
    },
    /// [`Shards::renew`](crate::Shards::renew), to the holder.
    Renew(ReadId),
    /// [`Shards::release`](crate::Shards::release), to the holder.
    Release(ReadId),
    /// [`Shards::fetch`](crate::Shards::fetch), to the holder.
    Fetch {
        /// The registered read.
        read: ReadId,
        /// The position of the payload.
        position: EpochSeq,
    },
    /// [`Shards::append_extent`](crate::Shards::append_extent).
    AppendExtent(Extent),
    /// [`Shards::write`](crate::Shards::write).
    Write {
        /// The record.
        body: RecordBody,
        /// Its precondition.
        condition: Precondition,
    },
    /// [`Shards::seal`](crate::Shards::seal).
    Seal,
    /// [`Shards::unseal`](crate::Shards::unseal).
    Unseal,
}

impl Request {
    /// Whether the request only reads, so that sending it again after its
    /// answer was lost changes nothing.
    #[must_use]
    pub fn is_read(&self) -> bool {
        !matches!(
            self,
            Self::AppendExtent(_) | Self::Write { .. } | Self::Seal | Self::Unseal
        )
    }

    /// Whether the request goes to a holder of read payload, whatever its
    /// role, rather than to the shard's primary: a registration, a renewal
    /// or release of one, or a fetch under one (§8.7).
    #[must_use]
    pub fn is_holder(&self) -> bool {
        matches!(
            self,
            Self::Register { .. } | Self::Renew(_) | Self::Release(_) | Self::Fetch { .. }
        )
    }
}

/// The result of a served [`Request`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Response {
    /// The entry of [`Request::Entry`].
    Entry(Option<Entry>),
    /// The page of [`Request::List`].
    List(ListPage),
    /// The upload and parts of [`Request::Upload`].
    Upload(Option<UploadParts>),
    /// The uploads of [`Request::Uploads`].
    Uploads(Vec<(String, EpochSeq, Upload)>),
    /// The parts of [`Request::Parts`].
    Parts(Vec<(u16, Part)>),
    /// The bytes of [`Request::Payload`] or [`Request::Fetch`].
    Payload(Bytes),
    /// The read plan of [`Request::Plan`].
    Plan(ReadPlan),
    /// The registration of [`Request::Register`], or `None` if the holder
    /// does not hold the version.
    Registered(Option<Registered>),
    /// Whether [`Request::Renew`] renewed the registration, which it does
    /// not once it lapsed.
    Renewed(bool),
    /// [`Request::Release`] released the registration.
    Released,
    /// The extent of [`Request::AppendExtent`].
    Extent(ExtentRef),
    /// The outcome of [`Request::Write`].
    Written(Result<EpochSeq, ConditionFailed>),
    /// The summary of [`Request::Seal`].
    Sealed(ShardSummary),
    /// [`Request::Unseal`] lifted a seal.
    Unsealed,
}

/// What a replica answers a forwarded request.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
#[expect(
    clippy::large_enum_variant,
    reason = "one value per request, moved a few times; boxing would allocate for each"
)]
pub enum Reply {
    /// The replica served the request as the primary of its configuration
    /// in `epoch`. `hint` is that configuration if it is newer than the
    /// one the request named.
    Served {
        /// The result.
        response: Response,
        /// The epoch the replica served in.
        epoch: Epoch,
        /// The replica's configuration, if newer than the gateway's.
        hint: Option<ShardConfig>,
    },
    /// The replica is not the primary of its configuration, the redirect
    /// hint, and served nothing.
    Redirect(ShardConfig),
    /// The replica's configuration is older than the gateway's, in this
    /// epoch, so it served nothing: it has not adopted the newer one yet.
    Behind(Epoch),
    /// The replica refused the request; [`ShardError::NotFound`] means the
    /// shard is not open on its node.
    Refused(ShardError),
}
