//! Wall-clock time for token lifetimes and key caching.
//!
//! JWT lifetimes (`exp`, `nbf`, `iat`) are Unix timestamps set by the
//! issuer, so they are compared with the node's wall clock, not with the
//! monotonic `skys3_io::Clock` used for leases. The validator reads time
//! only through [`WallClock`], so tests and the simulation control it. Key
//! cache ages use the same clock: they are hours long, and a clock step
//! only makes a refresh happen early.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A source of wall-clock time, as a duration since the Unix epoch.
///
/// Readings may jump backwards or forwards when the system clock is set.
/// Callers treat a reading earlier than a previous one as "a long time has
/// passed", never as a negative interval.
pub trait WallClock: fmt::Debug + Send + Sync + 'static {
    /// Returns the time elapsed since 1970-01-01T00:00:00Z.
    fn now(&self) -> Duration;
}

/// The operating system's wall clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl WallClock for SystemClock {
    fn now(&self) -> Duration {
        // A clock set before 1970 reads as the epoch, which makes every
        // token look expired: a safe failure.
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
    }
}

/// A wall clock that moves only when told to, for tests and simulation.
///
/// Clones share the same reading.
#[derive(Clone, Debug, Default)]
pub struct ManualClock {
    nanos: Arc<AtomicU64>,
}

impl ManualClock {
    /// Returns a clock that reads `since_epoch`.
    pub fn new(since_epoch: Duration) -> Self {
        let clock = Self::default();
        clock.set(since_epoch);
        clock
    }

    /// Sets the reading to `since_epoch`, saturating at about the year 2554.
    pub fn set(&self, since_epoch: Duration) {
        let nanos = u64::try_from(since_epoch.as_nanos()).unwrap_or(u64::MAX);
        self.nanos.store(nanos, Ordering::SeqCst);
    }

    /// Moves the reading forward by `by`.
    pub fn advance(&self, by: Duration) {
        self.set(self.now().saturating_add(by));
    }
}

impl WallClock for ManualClock {
    fn now(&self) -> Duration {
        Duration::from_nanos(self.nanos.load(Ordering::SeqCst))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_clock_is_after_2020() {
        assert!(SystemClock.now() > Duration::from_secs(1_577_836_800));
    }

    #[test]
    fn manual_clock_moves_when_told() {
        let clock = ManualClock::new(Duration::from_secs(100));
        let shared = clock.clone();
        clock.advance(Duration::from_secs(5));
        assert_eq!(shared.now(), Duration::from_secs(105));
        shared.set(Duration::from_secs(7));
        assert_eq!(clock.now(), Duration::from_secs(7));
        clock.set(Duration::MAX);
        assert_eq!(clock.now(), Duration::from_nanos(u64::MAX));
        clock.advance(Duration::MAX);
        assert_eq!(clock.now(), Duration::from_nanos(u64::MAX));
    }
}
