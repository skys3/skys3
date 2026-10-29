//! Monotonic node clocks with bounded rate drift.
//!
//! The design assumes monotonic clocks whose rate drift is bounded by `ρ`
//! (`replication.assumed_clock_drift`, 1% by default) and never compares
//! readings taken on different nodes (design §2.3, §5.4). Leases, grace
//! periods, and every other timer read time through the [`Clock`] trait, so
//! the simulation can give each node its own rate and origin.
//!
//! [`MonotonicClock`] follows the Tokio runtime clock. In production that is
//! the operating system's monotonic clock. Under `turmoil`, and in tests with
//! paused Tokio time, it is the runtime's simulated clock, and
//! [`MonotonicClock::drifting`] adds a per-node rate drift and origin on top.

use std::fmt;
use std::future::IntoFuture;
use std::ops::{Add, Sub};
use std::time::Duration;

use rand::Rng;
use tokio::time::{Instant, Sleep, Timeout};

/// A reading of one node's monotonic clock, in nanoseconds since that
/// clock's origin.
///
/// Readings from different clocks are not comparable: each node's clock has
/// its own origin and rate.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MonoTime(u64);

impl MonoTime {
    /// The clock's origin.
    pub const ZERO: MonoTime = MonoTime(0);

    /// The largest representable reading, about 584 years after the origin.
    pub const MAX: MonoTime = MonoTime(u64::MAX);

    /// Returns the reading `nanos` nanoseconds after the origin.
    pub const fn from_nanos(nanos: u64) -> Self {
        MonoTime(nanos)
    }

    /// Returns the nanoseconds since the origin.
    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    /// Returns the time elapsed from `earlier` to `self`, or zero if
    /// `earlier` is later.
    pub fn saturating_duration_since(self, earlier: MonoTime) -> Duration {
        Duration::from_nanos(self.0.saturating_sub(earlier.0))
    }

    /// Returns the time elapsed from `earlier` to `self`, or `None` if
    /// `earlier` is later.
    pub fn checked_duration_since(self, earlier: MonoTime) -> Option<Duration> {
        self.0.checked_sub(earlier.0).map(Duration::from_nanos)
    }

    /// Returns `self + duration`, or `None` on overflow.
    pub fn checked_add(self, duration: Duration) -> Option<MonoTime> {
        let nanos = u64::try_from(duration.as_nanos()).ok()?;
        self.0.checked_add(nanos).map(MonoTime)
    }

    /// Returns `self + duration`, saturating at [`MonoTime::MAX`].
    pub fn saturating_add(self, duration: Duration) -> MonoTime {
        self.checked_add(duration).unwrap_or(MonoTime::MAX)
    }
}

impl Add<Duration> for MonoTime {
    type Output = MonoTime;

    /// # Panics
    ///
    /// Panics on overflow, like [`std::time::Instant`].
    fn add(self, duration: Duration) -> MonoTime {
        self.checked_add(duration)
            .expect("overflow when adding a duration to a MonoTime")
    }
}

impl Sub for MonoTime {
    type Output = Duration;

    /// Saturates at zero, like [`std::time::Instant`].
    fn sub(self, earlier: MonoTime) -> Duration {
        self.saturating_duration_since(earlier)
    }
}

/// A clock's rate error in parts per million: a clock with drift `d` advances
/// `1 + d / 1_000_000` seconds per real second.
///
/// The design's bound `ρ` is itself a drift: the default 1% is
/// `Drift::from_ppm(10_000)`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Drift(i32);

impl Drift {
    /// No drift: the clock runs at the real rate.
    pub const NONE: Drift = Drift(0);

    /// The largest supported magnitude, ±50%. Real clocks are within a few
    /// hundred ppm; larger values exist to test drift far beyond `ρ`.
    pub const MAX_PPM: i32 = 500_000;

    /// Returns the drift of `ppm` parts per million, or `None` if its
    /// magnitude exceeds [`Drift::MAX_PPM`].
    pub const fn from_ppm(ppm: i32) -> Option<Drift> {
        if ppm >= -Self::MAX_PPM && ppm <= Self::MAX_PPM {
            Some(Drift(ppm))
        } else {
            None
        }
    }

    /// Returns the drift for a rate error given as a fraction, such as the
    /// configured `assumed_clock_drift = 0.01`, rounded to whole ppm. Returns
    /// `None` if it is not finite or its magnitude exceeds
    /// [`Drift::MAX_PPM`].
    pub fn from_fraction(fraction: f64) -> Option<Drift> {
        let ppm = (fraction * 1e6).round();
        if ppm.is_finite() && ppm.abs() <= f64::from(Self::MAX_PPM) {
            // In range, so the cast is exact.
            Self::from_ppm(ppm as i32)
        } else {
            None
        }
    }

    /// Returns the drift in parts per million.
    pub const fn ppm(self) -> i32 {
        self.0
    }

    /// Returns a drift drawn uniformly from `[-|bound|, +|bound|]`, as the
    /// simulation does for each node with `bound = ρ`.
    pub fn random_within<R: Rng + ?Sized>(rng: &mut R, bound: Drift) -> Drift {
        let max = bound.0.abs();
        Drift(rng.random_range(-max..=max))
    }

    /// Local nanoseconds per real nanosecond, scaled by one million.
    fn rate(self) -> u128 {
        // At most 1_500_000 and at least 500_000: never zero.
        (1_000_000 + i64::from(self.0)) as u128
    }
}

impl fmt::Display for Drift {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:+} ppm", self.0)
    }
}

/// A node's monotonic clock: the only source of time for leases, grace
/// periods, and timers.
///
/// Timers are Tokio timers, so they run under the simulated time of
/// `turmoil` and of paused Tokio runtimes. An implementation converts its own
/// readings to the runtime's clock in [`Clock::runtime_deadline`], and the
/// provided methods build sleeps and timeouts from that.
pub trait Clock: fmt::Debug + Send + Sync + 'static {
    /// Returns the current reading. Successive readings never decrease.
    fn now(&self) -> MonoTime;

    /// Returns the earliest runtime instant at which [`Clock::now`] reads at
    /// least `deadline`.
    fn runtime_deadline(&self, deadline: MonoTime) -> Instant;

    /// Returns a future that completes once this clock reads at least
    /// `deadline`.
    fn sleep_until(&self, deadline: MonoTime) -> Sleep {
        tokio::time::sleep_until(self.runtime_deadline(deadline))
    }

    /// Returns a future that completes once `duration` has passed on this
    /// clock.
    fn sleep(&self, duration: Duration) -> Sleep {
        self.sleep_until(self.now().saturating_add(duration))
    }
}

/// Runs `future` until it completes or `duration` passes on `clock`,
/// whichever comes first.
pub fn timeout<C, F>(clock: &C, duration: Duration, future: F) -> Timeout<F::IntoFuture>
where
    C: Clock + ?Sized,
    F: IntoFuture,
{
    let deadline = clock.runtime_deadline(clock.now().saturating_add(duration));
    tokio::time::timeout_at(deadline, future)
}

/// How far in the future a deadline that does not fit an [`Instant`] is
/// placed. Tokio uses the same horizon for its own "never" deadlines.
const FAR_FUTURE: Duration = Duration::from_secs(86_400 * 365 * 30);

/// A [`Clock`] on the Tokio runtime's monotonic clock, optionally with a rate
/// drift and an origin offset.
///
/// A clock follows the time of the runtime it was created on, so under
/// `turmoil` each node creates its clock inside its host software. Readings
/// start at the given origin when the clock is created.
#[derive(Clone, Debug)]
pub struct MonotonicClock {
    /// The runtime instant at which this clock read `start`.
    base: Instant,
    start: MonoTime,
    drift: Drift,
}

impl MonotonicClock {
    /// Returns a clock with no drift that reads [`MonoTime::ZERO`] now.
    pub fn new() -> Self {
        Self::drifting(Drift::NONE, MonoTime::ZERO)
    }

    /// Returns a clock that runs at the rate given by `drift` and reads
    /// `start` now. The simulation gives every node a distinct `start`, so
    /// code that compares readings across nodes fails visibly.
    pub fn drifting(drift: Drift, start: MonoTime) -> Self {
        MonotonicClock {
            base: Instant::now(),
            start,
            drift,
        }
    }

    /// Returns this clock's drift.
    pub fn drift(&self) -> Drift {
        self.drift
    }
}

impl Default for MonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for MonotonicClock {
    fn now(&self) -> MonoTime {
        let real = Instant::now()
            .saturating_duration_since(self.base)
            .as_nanos();
        let local = real * self.drift.rate() / 1_000_000;
        let local = u64::try_from(local).unwrap_or(u64::MAX);
        MonoTime(self.start.0.saturating_add(local))
    }

    fn runtime_deadline(&self, deadline: MonoTime) -> Instant {
        let local = u128::from(deadline.0.saturating_sub(self.start.0));
        // Round up, so the clock reads at least `deadline` at the instant.
        let real = (local * 1_000_000).div_ceil(self.drift.rate());
        u64::try_from(real)
            .ok()
            .and_then(|nanos| self.base.checked_add(Duration::from_nanos(nanos)))
            .unwrap_or_else(|| self.base + FAR_FUTURE)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rand::SeedableRng;
    use rand::rngs::SmallRng;

    use super::*;

    #[test]
    fn mono_time_arithmetic() {
        let t = MonoTime::from_nanos(1_000);
        assert_eq!(t.as_nanos(), 1_000);
        assert_eq!((t + Duration::from_nanos(500)).as_nanos(), 1_500);
        assert_eq!(t - MonoTime::from_nanos(400), Duration::from_nanos(600));
        assert_eq!(MonoTime::from_nanos(400) - t, Duration::ZERO);
        assert_eq!(
            t.checked_duration_since(MonoTime::ZERO),
            Some(Duration::from_nanos(1_000))
        );
        assert_eq!(MonoTime::ZERO.checked_duration_since(t), None);
        assert_eq!(MonoTime::MAX.checked_add(Duration::from_nanos(1)), None);
        assert_eq!(t.checked_add(Duration::MAX), None);
        assert_eq!(
            MonoTime::MAX.saturating_add(Duration::from_secs(1)),
            MonoTime::MAX
        );
        assert_eq!(MonoTime::default(), MonoTime::ZERO);
    }

    #[test]
    #[should_panic(expected = "overflow")]
    fn mono_time_add_panics_on_overflow() {
        let _ = MonoTime::MAX + Duration::from_nanos(1);
    }

    #[test]
    fn drift_construction() {
        assert_eq!(Drift::from_ppm(10_000).map(Drift::ppm), Some(10_000));
        assert_eq!(
            Drift::from_ppm(-Drift::MAX_PPM).map(Drift::ppm),
            Some(-500_000)
        );
        assert_eq!(Drift::from_ppm(Drift::MAX_PPM + 1), None);
        assert_eq!(Drift::from_ppm(-Drift::MAX_PPM - 1), None);
        assert_eq!(Drift::from_fraction(0.01), Drift::from_ppm(10_000));
        assert_eq!(Drift::from_fraction(-0.000_25), Drift::from_ppm(-250));
        assert_eq!(Drift::from_fraction(0.6), None);
        assert_eq!(Drift::from_fraction(f64::NAN), None);
        assert_eq!(Drift::from_fraction(f64::INFINITY), None);
        assert_eq!(Drift::from_ppm(-42).unwrap().to_string(), "-42 ppm");
        assert_eq!(Drift::NONE.to_string(), "+0 ppm");
    }

    #[test]
    fn random_drift_stays_within_bound_and_is_seeded() {
        let bound = Drift::from_ppm(10_000).unwrap();
        let mut rng = SmallRng::seed_from_u64(7);
        let drifts: Vec<Drift> = (0..1_000)
            .map(|_| Drift::random_within(&mut rng, bound))
            .collect();
        assert!(drifts.iter().all(|d| d.ppm().abs() <= 10_000));
        assert!(drifts.iter().any(|d| d.ppm() > 5_000));
        assert!(drifts.iter().any(|d| d.ppm() < -5_000));

        let mut again = SmallRng::seed_from_u64(7);
        let negated_bound = Drift::from_ppm(-10_000).unwrap();
        assert_eq!(Drift::random_within(&mut again, negated_bound), drifts[0]);
    }

    #[tokio::test(start_paused = true)]
    async fn clock_without_drift_follows_runtime_time() {
        let clock = MonotonicClock::default();
        assert_eq!(clock.drift(), Drift::NONE);
        assert_eq!(clock.now(), MonoTime::ZERO);
        tokio::time::advance(Duration::from_millis(1_500)).await;
        assert_eq!(clock.now(), MonoTime::from_nanos(1_500_000_000));
    }

    #[tokio::test(start_paused = true)]
    async fn drifting_clock_runs_fast_or_slow() {
        let start = MonoTime::from_nanos(5_000_000_000);
        let fast = MonotonicClock::drifting(Drift::from_ppm(10_000).unwrap(), start);
        let slow = MonotonicClock::drifting(Drift::from_ppm(-10_000).unwrap(), start);
        assert_eq!(fast.now(), start);

        tokio::time::advance(Duration::from_secs(100)).await;
        assert_eq!(fast.now() - start, Duration::from_secs(101));
        assert_eq!(slow.now() - start, Duration::from_secs(99));
    }

    #[tokio::test(start_paused = true)]
    async fn sleep_waits_for_local_time() {
        for ppm in [-Drift::MAX_PPM, -10_000, -1, 0, 1, 10_000, Drift::MAX_PPM] {
            let clock = MonotonicClock::drifting(Drift::from_ppm(ppm).unwrap(), MonoTime::ZERO);
            let before = clock.now();
            let real_before = Instant::now();
            clock.sleep(Duration::from_millis(1_000)).await;
            let local = clock.now() - before;
            let real = Instant::now() - real_before;
            assert!(
                local >= Duration::from_millis(1_000),
                "{ppm} ppm: slept {local:?}"
            );
            // Tokio timers have millisecond resolution.
            assert!(
                local <= Duration::from_millis(1_002),
                "{ppm} ppm: slept {local:?}"
            );
            let expected_real = 1_000_000_000_f64 / (1.0 + f64::from(ppm) / 1e6);
            assert!(
                (real.as_nanos() as f64 - expected_real).abs() < 2e6,
                "{ppm} ppm: {real:?}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn deadlines_in_the_past_or_far_future() {
        let clock = MonotonicClock::drifting(Drift::NONE, MonoTime::from_nanos(1_000));
        let now = Instant::now();
        assert_eq!(clock.runtime_deadline(MonoTime::ZERO), now);
        clock.sleep_until(MonoTime::ZERO).await;
        let far = clock.runtime_deadline(MonoTime::MAX);
        assert!(far > now + Duration::from_secs(86_400 * 365));
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_uses_local_time() {
        let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::drifting(
            Drift::from_ppm(Drift::MAX_PPM).unwrap(),
            MonoTime::ZERO,
        ));
        let real_before = Instant::now();
        let never = std::future::pending::<()>();
        assert!(
            timeout(&*clock, Duration::from_secs(3), never)
                .await
                .is_err()
        );
        // At +50%, three local seconds take two real seconds.
        assert_eq!(Instant::now() - real_before, Duration::from_secs(2));

        let ready = timeout(&*clock, Duration::from_secs(3), async { 7 }).await;
        assert_eq!(ready.ok(), Some(7));
    }
}
