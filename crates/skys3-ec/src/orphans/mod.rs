//! Orphan fragment reclamation (design §8.4): fragments that no committed
//! stripe layout references, and none ever will, are removed from the
//! nodes that hold them.
//!
//! A fragment-writing attempt (an encoding, and later a repair or a move)
//! that crashes, fails, or is superseded leaves fragments that its
//! publishing record (`EC_PUBLISH`, later `EC_RELOCATE`) never names. Only
//! the shard's primary can tell such an orphan from a fragment whose
//! record is still to come, so reclamation is a question and an answer:
//!
//! 1. A fragment node's [`OrphanReclaimer`] notes when it first sees each
//!    fragment in its stores. Once a fragment has been there for
//!    `fragment_orphan_after_seconds`, the reclaimer reads its header and
//!    asks the primary of the header's shard about it, in batches, through
//!    an [`OrphanConfirmer`] ([`OrphanClient`] over the cluster transport).
//! 2. The shard primary's [`OrphanJudge`] gives each fragment a
//!    [`Verdict`]: referenced by the key's committed layout, written by an
//!    attempt still in progress, or an orphan.
//! 3. The reclaimer reclaims the orphans ([`FragmentStore::reclaim`]),
//!    keeps the referenced ones without asking again in this life, and
//!    asks about the others again after another
//!    `fragment_orphan_after_seconds`.
//!
//! # The fence
//!
//! A verdict of [`Verdict::Orphan`] is final: the attempt that wrote the
//! fragment never gets a publishing record committed afterwards. The judge
//! makes it so (plan M5-05, design §8.4):
//!
//! - It answers only while its replica leads the shard and serves, that is
//!   after reconciliation (§6.6) committed every record a predecessor
//!   appended, publishing records included.
//! - An attempt of a later epoch than the one the replica sequences in was
//!   started by a newer primary, so this one is deposed and does not know
//!   it: its fragments are [`Verdict::InProgress`].
//! - Otherwise it asks its [`Attempts`] whether the attempt can still
//!   publish, and marks it abandoned in the same step if not
//!   ([`Attempts::orphaned`]). An attempt that is writing in the replica's
//!   epoch, or publishing, is in progress, however long it takes. A
//!   primary appends an attempt's record only while it sequences in the
//!   attempt's epoch ([`Shard::commit_in`](skys3_shard::Shard::commit_in))
//!   and its life lasts, so an attempt still writing from an earlier
//!   epoch, or one the tracker does not know (started by an earlier
//!   primary, or in an earlier life of this one, or finished) never
//!   appends one again.
//! - Then the fragment is [`Verdict::Referenced`] if the key's layout in
//!   the replica's index names it on the asking node, and an orphan if
//!   not. The index holds every committed record the replica applied; a
//!   record committed after it could only be one the tracker still shows.
//!
//! A [`Verdict::InProgress`] is not final, though: a primary deposed
//! without knowing it may hold its own unfinished attempt in progress for
//! good. So a fragment node asks every replica that may lead, and an
//! orphan verdict from any of them wins over one in progress
//! ([`OrphanClient`]).
//!
//! Every attempt that writes fragments on a node shares that node's
//! [`Attempts`] for the shard with the judge: the [`Encoder`] here, and
//! repair (M5-08) and moves (M5-09) later.
//!
//! [`FragmentStore::reclaim`]: crate::FragmentStore::reclaim
//! [`Encoder`]: crate::Encoder

mod judge;
mod reclaimer;
mod wire;

use std::future::Future;

use skys3_log::ShardRef;
use skys3_types::{AttemptId, FragmentId};

pub use judge::{JudgeError, OrphanJudge};
pub use reclaimer::{OrphanReclaimer, ReclaimObserver, Reclaimed, SweepReport};
pub use wire::{
    MAX_SUSPECTS, OrphanClient, OrphanQuery, OrphanServer, OrphanVerdicts, PrimariesOf, SuspectBody,
};

#[cfg(feature = "test-util")]
#[doc(hidden)]
pub use judge::JudgeBug;

#[cfg(doc)]
use crate::Attempts;

/// A fragment a node asks its shard's primary about: what its header says
/// of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Suspect {
    /// The fragment's ID in the asking node's store.
    pub id: FragmentId,
    /// The key of the object it belongs to.
    pub key: String,
    /// The attempt that wrote it.
    pub attempt: AttemptId,
}

/// A shard primary's answer about one fragment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Verdict {
    /// A committed stripe layout names the fragment on the asking node:
    /// keep it.
    Referenced,
    /// The attempt that wrote it may still publish it, or the primary
    /// cannot tell yet: ask again later.
    InProgress,
    /// No committed layout names it, and none ever will: reclaim it.
    Orphan,
}

impl Verdict {
    /// The verdict's byte on the wire.
    #[must_use]
    pub const fn to_byte(self) -> u8 {
        match self {
            Self::Referenced => 1,
            Self::InProgress => 2,
            Self::Orphan => 3,
        }
    }

    /// The verdict a byte on the wire stands for, if any.
    #[must_use]
    pub const fn from_byte(byte: u8) -> Option<Self> {
        Some(match byte {
            1 => Self::Referenced,
            2 => Self::InProgress,
            3 => Self::Orphan,
            _ => return None,
        })
    }
}

/// Why no shard primary answered an orphan query.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("no primary of shard {shard} answered: {reason}")]
pub struct Unanswered {
    /// The shard.
    pub shard: ShardRef,
    /// What the nodes asked said, or why they could not be asked.
    pub reason: String,
}

/// What a fragment node asks shard primaries through: [`OrphanClient`]
/// over the cluster transport, or a test's stand-in.
pub trait OrphanConfirmer: Send + Sync + 'static {
    /// The verdicts of the primary of `shard` on `suspects`, fragments of
    /// this node, one per suspect and in their order.
    fn confirm(
        &self,
        shard: &ShardRef,
        suspects: &[Suspect],
    ) -> impl Future<Output = Result<Vec<Verdict>, Unanswered>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verdicts_round_trip_through_their_bytes() {
        for verdict in [Verdict::Referenced, Verdict::InProgress, Verdict::Orphan] {
            assert_eq!(Verdict::from_byte(verdict.to_byte()), Some(verdict));
        }
        for byte in [0, 4, u8::MAX] {
            assert_eq!(Verdict::from_byte(byte), None);
        }
    }
}
