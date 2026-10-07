//! Conflict policies and their administration (§7.2).
//!
//! A flush that finds a write SkyS3 did not make at the remote puts its key
//! in **conflict**, and the bucket's `flush_conflict_policy` decides what
//! happens next:
//!
//! - `hold` (the default) keeps the key dirty and unflushed until an
//!   operator resolves it through the admin API
//!   ([`FlushService::resolve`](crate::FlushService::resolve)).
//! - `overwrite` resolves it at once: the key returns to dirty and its
//!   next flush is sent without a precondition, so the local version
//!   replaces the out-of-band write.
//! - `discard_local` resolves it at once too: the key returns to dirty, and
//!   its next flush HEADs the key and commits an `ADOPT` of the remote's
//!   write, naming the dirty version, which drops it. It loses
//!   acknowledged writes, so only a `write_back` bucket whose own
//!   `[buckets.<name>]` table names it may use it ([`effective_policy`]),
//!   automatically or through the admin API.
//!
//! Resolving moves the key from Conflict back to Dirty (§4.2), never to
//! Clean: the resolution is carried out by the key's next flush, with the
//! usual retries, ordering, and write identity, and lasts until a flush of
//! the key succeeds. Like the Conflict state itself, a resolution lives in
//! the primary flusher's memory: a flusher started after a restart or on a
//! new primary finds the conflict again by flushing, and resolves it again
//! by the bucket's policy, or holds it for the operator again.
//!
//! A backup target (§8.9) is not the system of record of its `local`
//! bucket, so its flushers hold or overwrite, but never discard: adopting
//! the backup's object would replace acknowledged data in the cluster with
//! a write that no read path serves.

use std::fmt;

use skys3_config::ConflictPolicy;

/// Why a held conflict was not resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unresolved {
    /// The bucket is not flushed on this node.
    NotFlushed,
    /// No flusher on this node holds the key in conflict: it is clean,
    /// dirty, or being flushed, or its shard's primary is another node.
    NotHeld,
    /// `discard_local` was asked of a bucket that did not opt into it.
    NotOptedIn,
    /// The flusher that held the key stopped.
    Stopped,
}

impl fmt::Display for Unresolved {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::NotFlushed => "the bucket is not flushed on this node",
            Self::NotHeld => "the key is not held in conflict on this node",
            Self::NotOptedIn => {
                "discard_local loses acknowledged writes, and the bucket's \
                 [buckets.<name>] table does not opt into it"
            }
            Self::Stopped => "the key's flusher stopped",
        })
    }
}

impl std::error::Error for Unresolved {}

/// The policy a bucket's flushers apply to conflicts: its own
/// `flush_conflict_policy`, except that `discard_local` holds instead on a
/// backup target. `policy` is what the bucket's settings say and `backup`
/// whether the target is a backup.
pub(crate) fn effective_policy(policy: ConflictPolicy, backup: bool) -> ConflictPolicy {
    if hooks::conflict_bug() == ConflictBug::DiscardsWithoutOptIn {
        return ConflictPolicy::DiscardLocal;
    }
    match policy {
        ConflictPolicy::DiscardLocal if backup => ConflictPolicy::Hold,
        policy => policy,
    }
}

/// Whether an operator may resolve the bucket's conflicts with
/// `discard_local`: only where its flushers would apply it themselves.
pub(crate) fn may_discard(policy: ConflictPolicy, backup: bool) -> bool {
    effective_policy(policy, backup) == ConflictPolicy::DiscardLocal
}

/// A bug seeded into the conflict handling of every flusher on this
/// thread, for the simulation that shows its audits catch them (the
/// `test-util` feature exports [`seed_conflict_bug`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConflictBug {
    /// No bug.
    #[default]
    None,
    /// Every bucket discards its conflicts, whatever its policy and
    /// whether or not it opted in.
    DiscardsWithoutOptIn,
    /// Resolving a held conflict records the local version as flushed,
    /// making the key clean, instead of returning it to dirty: the remote
    /// keeps the out-of-band write.
    ResolvesClean,
}

pub(crate) use hooks::conflict_bug;
#[cfg(feature = "test-util")]
pub use hooks::seed_conflict_bug;

mod hooks {
    use std::cell::Cell;

    use super::ConflictBug;

    thread_local! {
        static BUG: Cell<ConflictBug> = const { Cell::new(ConflictBug::None) };
    }

    /// Seeds `bug` into the conflict handling of every flusher this thread
    /// runs. A deterministic simulation runs every node on its test's
    /// thread, so other tests run the real code.
    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    pub fn seed_conflict_bug(bug: ConflictBug) {
        BUG.with(|seeded| seeded.set(bug));
    }

    /// The bug seeded on this thread.
    pub(crate) fn conflict_bug() -> ConflictBug {
        BUG.with(Cell::get)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_backup_never_discards() {
        for policy in [ConflictPolicy::Hold, ConflictPolicy::Overwrite] {
            assert_eq!(effective_policy(policy, false), policy);
            assert_eq!(effective_policy(policy, true), policy);
        }
        assert_eq!(
            effective_policy(ConflictPolicy::DiscardLocal, false),
            ConflictPolicy::DiscardLocal
        );
        assert_eq!(
            effective_policy(ConflictPolicy::DiscardLocal, true),
            ConflictPolicy::Hold
        );
        assert!(may_discard(ConflictPolicy::DiscardLocal, false));
        assert!(!may_discard(ConflictPolicy::DiscardLocal, true));
        assert!(!may_discard(ConflictPolicy::Overwrite, false));
    }

    #[test]
    fn every_refusal_says_why() {
        for refusal in [
            Unresolved::NotFlushed,
            Unresolved::NotHeld,
            Unresolved::NotOptedIn,
            Unresolved::Stopped,
        ] {
            assert!(!refusal.to_string().is_empty());
        }
    }
}
