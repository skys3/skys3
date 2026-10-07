//! Seeded bugs of index snapshots, for the simulation that shows its
//! restore drill catches them (the `test-util` feature exports them).

use std::cell::Cell;

/// A bug seeded into the snapshots this thread writes and restores.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum SnapshotBug {
    /// No bug.
    #[default]
    None,
    /// A snapshot leaves out dirty entries, keeping only those being
    /// flushed, in conflict, or clean.
    SkipsDirty,
    /// A delta lists no removed rows, so a deleted key stays in the
    /// restored index.
    KeepsRemoved,
    /// The restore applies the base alone, but dates the result as the
    /// latest delta: the window starts after the state it restored.
    BaseOnly,
}

thread_local! {
    static BUG: Cell<SnapshotBug> = const { Cell::new(SnapshotBug::None) };
}

/// Seeds `bug` into every snapshot writer and restore this thread runs. A
/// deterministic simulation runs every node on its test's thread, so other
/// tests run the real code.
#[cfg(feature = "test-util")]
#[doc(hidden)]
pub fn seed_snapshot_bug(bug: SnapshotBug) {
    BUG.with(|seeded| seeded.set(bug));
}

/// The bug seeded on this thread.
pub(crate) fn snapshot_bug() -> SnapshotBug {
    BUG.with(Cell::get)
}
