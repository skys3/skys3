//! Fragment release (design §8.7, §10.3): fragments that no layout names
//! any more leave their nodes, but only once no read can still need them.
//!
//! A write that replaces a coded version (an overwrite, a delete, an
//! `ADOPT`, a lifecycle expiration), and an `EC_RELOCATE` that moves a
//! fragment away, leave fragments that the key's layout no longer names.
//! Applying such a record moves them from the shard's index from node to
//! fragments to its index of *released* fragments, on every replica. Then:
//!
//! 1. The shard primary's [`Releaser`] tells each fragment's node, in
//!    batches ([`ReleaseSink`], [`ReleaseClient`] over the cluster
//!    transport), and once a node has answered, or the cluster's topology
//!    no longer lists it, commits an `EC_RELEASE` that removes those
//!    fragments from the index. A new primary takes up whatever its
//!    predecessor did not finish, from the same index.
//! 2. The node checks that each fragment's header names the shard, key,
//!    and version the primary says, and notes it released in its
//!    [`FragmentHolds`]. It keeps the fragment for
//!    `fragment_release_delay_seconds`, which covers a read plan issued
//!    before the release whose gateway has not registered it yet, and
//!    then for as long as a read's hold names it.
//! 3. A gateway registers a hold on every fragment a coded read may fetch
//!    from a node before its first fetch, renews it every
//!    `read_registration_renew_interval_seconds` while it streams, and
//!    ends it when the stream ends (see [`read`](crate::read)); a hold its
//!    gateway stopped renewing lapses after
//!    `read_registration_ttl_seconds`.
//! 4. [`FragmentHolds::sweep`] reclaims released fragments that waited the
//!    delay and that no hold names, and compacts the node's fragment
//!    segments ([`FragmentStore::compact`]), which gives their space back.
//!
//! Orphan reclamation (§8.4) releases the fragments it judges orphans the
//! same way when given the node's holds, so a read that holds an old plan
//! finishes, or fails cleanly once its hold lapses, whichever path a
//! fragment leaves by. A read never returns another fragment's bytes: a
//! node serves a fragment only under the header the read expects (§8.5).
//!
//! [`FragmentStore::compact`]: crate::FragmentStore::compact

mod holds;
mod metrics;
mod releaser;
mod wire;

use std::future::Future;

use skys3_log::ShardRef;
use skys3_types::{EpochSeq, FragmentId, NodeId};

#[cfg(feature = "test-util")]
#[doc(hidden)]
pub use holds::HoldsBug;
pub use holds::{FragmentHolds, HoldSettings, SweepReport};
pub use metrics::ReleaseMetrics;
pub use releaser::{ReleaseReport, ReleaseSettings, Releaser};
pub use wire::{
    FragmentHold, FragmentNoted, FragmentRelease, MAX_HOLDS, MAX_RELEASES, ReleaseClient,
    ReleasedBody,
};

/// A fragment a shard primary tells its node to release: its ID on that
/// node and what its header must name. The stripe and index matter where
/// an ID was assigned again, on a disk that was replaced: the version may
/// have placed another of its fragments on the node since.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleasedFragment {
    /// The fragment's ID on its node.
    pub fragment: FragmentId,
    /// The object key whose layout named it.
    pub key: String,
    /// The version it was written for, as its header names it.
    pub version: EpochSeq,
    /// Its stripe's number, as its header names it.
    pub stripe: u32,
    /// Its index within the stripe, as its header names it.
    pub index: u8,
}

/// What a shard primary tells fragment nodes through: [`ReleaseClient`]
/// over the cluster transport, or a test's stand-in.
pub trait ReleaseSink: Send + Sync + 'static {
    /// Tells `node` that `fragments` of `shard` are released. Succeeds
    /// once the node has noted them, or answered that it does not hold
    /// them.
    fn release(
        &self,
        node: &NodeId,
        shard: &ShardRef,
        fragments: &[ReleasedFragment],
    ) -> impl Future<Output = Result<(), String>> + Send;
}
