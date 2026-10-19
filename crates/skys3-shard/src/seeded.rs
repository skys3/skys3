//! Bugs a simulation seeds into the shards it runs, to show that its
//! checkers catch them (test support, behind the `test-util` feature).
//!
//! A seeded bug holds for the thread that seeds it: a deterministic
//! simulation runs every simulated node on its test's thread, so the
//! simulations of other tests, on other threads, run the real code.

/// A bug of the erasure-coding steps (§8.4) that a simulation can seed.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeededBug {
    /// A replica drops a coded version's replicated bytes once it has
    /// applied the version's `EC_PUBLISH`, without waiting until it knows
    /// the record committed.
    DropBeforeCommit,
    /// The state machine applies an `EC_PUBLISH` whose version is no longer
    /// the key's current one, to the current one.
    PublishSuperseded,
}

#[cfg(feature = "test-util")]
thread_local! {
    static SEEDED: std::cell::Cell<Option<SeededBug>> = const { std::cell::Cell::new(None) };
}

/// Seeds `bug` into every shard this thread runs from now on, or removes
/// the seeded bug with `None`.
#[cfg(feature = "test-util")]
#[doc(hidden)]
pub fn seed_bug(bug: Option<SeededBug>) {
    SEEDED.with(|seeded| seeded.set(bug));
}

/// Whether `bug` is seeded on this thread.
pub(crate) fn is_seeded(bug: SeededBug) -> bool {
    #[cfg(feature = "test-util")]
    {
        SEEDED.with(std::cell::Cell::get) == Some(bug)
    }
    #[cfg(not(feature = "test-util"))]
    {
        let _ = bug;
        false
    }
}
