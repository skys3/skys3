//! The network path between a [`SimS3`](super::SimS3) and its callers, and
//! the store's request-rate limit: what an adaptive flush concurrency
//! controller (design §7.7) measures and must respect.
//!
//! The path is one bottleneck queue each way. A request body is sent once
//! the bodies sent before it have left, at the link's bandwidth, and then
//! travels half the round trip; a response body comes back the same way.
//! With fewer requests in flight than the bandwidth-delay product, nothing
//! waits and every request takes its uncongested time, so throughput grows
//! with concurrency. With more, bodies queue, throughput stays at the
//! bandwidth, and latency grows with every request added. That knee is
//! what the controller is to find.
//!
//! Times are read from the caller's Tokio clock, so a simulation on a
//! paused runtime replays them exactly. Every caller of one store must
//! share that clock: one runtime, not separate `turmoil` hosts.

use std::num::{NonZeroU32, NonZeroU64};
use std::time::Duration;

use tokio::time::Instant;

/// The network path to a simulated store, and the store's request-rate
/// limit. The default is an instant path with no limit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SimLink {
    /// The round-trip time without any body: half of it passes before the
    /// store sees a request, and half before the caller sees the answer.
    pub round_trip: Duration,
    /// The bandwidth in bytes per second, in each direction, or `None` for
    /// unlimited. Request bodies (`PutObject`, `UploadPart`) share the way
    /// up and response bodies (`GetObject`) the way down, each body sent
    /// whole after the ones before it.
    pub bandwidth: Option<NonZeroU64>,
    /// The request rate the store accepts, if limited: a request arriving
    /// beyond it is answered `503 SlowDown` and not applied.
    pub rate_limit: Option<RateLimit>,
}

impl SimLink {
    /// A path with round trip `round_trip` and no bandwidth or rate limit.
    pub const fn with_round_trip(round_trip: Duration) -> SimLink {
        SimLink {
            round_trip,
            bandwidth: None,
            rate_limit: None,
        }
    }

    /// The bandwidth-delay product in bytes: how many bytes must be in
    /// flight to keep the link busy for a round trip. `None` without a
    /// bandwidth.
    pub fn bandwidth_delay_product(&self) -> Option<u64> {
        let bandwidth = self.bandwidth?.get();
        let bytes = u128::from(bandwidth) * self.round_trip.as_nanos() / NANOS_PER_SECOND;
        Some(u64::try_from(bytes).unwrap_or(u64::MAX))
    }

    /// How long `bytes` take to send at the link's bandwidth.
    pub fn transfer_time(&self, bytes: u64) -> Duration {
        match self.bandwidth {
            Some(bandwidth) if bytes > 0 => {
                let nanos = u128::from(bytes) * NANOS_PER_SECOND / u128::from(bandwidth.get());
                Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
            }
            _ => Duration::ZERO,
        }
    }
}

/// A store's request-rate limit, as a token bucket: `requests_per_second`
/// on average, and up to `burst` more at once after a quiet spell.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RateLimit {
    /// The sustained rate.
    pub requests_per_second: NonZeroU32,
    /// How many requests beyond the sustained rate are accepted at once.
    pub burst: u32,
}

const NANOS_PER_SECOND: u128 = 1_000_000_000;

/// The state of a store's link: when each direction is next free, and the
/// rate limiter's theoretical arrival time.
#[derive(Clone, Debug, Default)]
pub(super) struct Link {
    pub(super) config: SimLink,
    up_free: Option<Instant>,
    down_free: Option<Instant>,
    /// The generic cell rate algorithm's theoretical arrival time: when
    /// the limiter would be back to an empty bucket.
    theoretical_arrival: Option<Instant>,
}

impl Link {
    /// When a request sent at `now` with a body of `bytes` reaches the
    /// store.
    pub(super) fn arrival(&mut self, now: Instant, bytes: u64) -> Instant {
        let sent = transmit(&self.config, &mut self.up_free, now, bytes);
        sent + self.config.round_trip / 2
    }

    /// When an answer with a body of `bytes` that leaves the store at `now`
    /// reaches the caller.
    pub(super) fn delivery(&mut self, now: Instant, bytes: u64) -> Instant {
        let sent = transmit(&self.config, &mut self.down_free, now, bytes);
        sent + (self.config.round_trip - self.config.round_trip / 2)
    }

    /// Whether the rate limit admits a request arriving at `now`, and if
    /// so counts it.
    pub(super) fn admit(&mut self, now: Instant) -> bool {
        let Some(limit) = self.config.rate_limit else {
            return true;
        };
        let interval = Duration::from_nanos(
            u64::try_from(NANOS_PER_SECOND / u128::from(limit.requests_per_second.get()))
                .unwrap_or(u64::MAX),
        );
        let tolerance = interval.saturating_mul(limit.burst);
        let arrival = self.theoretical_arrival.map_or(now, |at| at.max(now));
        if arrival > now + tolerance {
            return false;
        }
        self.theoretical_arrival = Some(arrival + interval);
        true
    }
}

/// Sends `bytes` at `now` on the direction that is next free at `free`,
/// and returns when the last byte has left.
fn transmit(config: &SimLink, free: &mut Option<Instant>, now: Instant, bytes: u64) -> Instant {
    if bytes == 0 || config.bandwidth.is_none() {
        return now;
    }
    let start = free.map_or(now, |free| free.max(now));
    let end = start + config.transfer_time(bytes);
    *free = Some(end);
    end
}

/// The body bytes of an answer, which come back over the link.
pub(super) trait BodyBytes {
    /// The size of the body, 0 for an answer without one.
    fn body_bytes(&self) -> u64 {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn link(config: SimLink) -> Link {
        Link {
            config,
            ..Link::default()
        }
    }

    #[test]
    fn bodies_queue_behind_each_other_at_the_bandwidth() {
        let config = SimLink {
            round_trip: Duration::from_millis(10),
            bandwidth: NonZeroU64::new(1_000_000),
            rate_limit: None,
        };
        assert_eq!(config.bandwidth_delay_product(), Some(10_000));
        assert_eq!(config.transfer_time(1_000), Duration::from_millis(1));
        let mut link = link(config);
        let now = Instant::now();
        // The first body leaves after 1 ms, the second waits for it.
        assert_eq!(link.arrival(now, 1_000), now + Duration::from_millis(6));
        assert_eq!(link.arrival(now, 1_000), now + Duration::from_millis(7));
        // A request without a body does not wait.
        assert_eq!(link.arrival(now, 0), now + Duration::from_millis(5));
        // Once the link is free again, nothing waits.
        let later = now + Duration::from_secs(1);
        assert_eq!(link.arrival(later, 1_000), later + Duration::from_millis(6));
        // The way down is separate.
        assert_eq!(link.delivery(now, 2_000), now + Duration::from_millis(7));
        assert_eq!(SimLink::default().bandwidth_delay_product(), None);
        assert_eq!(SimLink::default().transfer_time(1 << 30), Duration::ZERO);
    }

    #[test]
    fn an_instant_link_adds_nothing() {
        let mut link = link(SimLink::default());
        let now = Instant::now();
        assert_eq!(link.arrival(now, 1 << 20), now);
        assert_eq!(link.delivery(now, 1 << 20), now);
        assert!((0..1000).all(|_| link.admit(now)));
    }

    #[test]
    fn the_rate_limit_admits_its_rate_and_burst() {
        let mut link = link(SimLink {
            rate_limit: Some(RateLimit {
                requests_per_second: NonZeroU32::new(100).unwrap(),
                burst: 4,
            }),
            ..SimLink::with_round_trip(Duration::from_millis(1))
        });
        let now = Instant::now();
        // The burst and the request that fits the rate, then nothing.
        let admitted = (0..10).filter(|_| link.admit(now)).count();
        assert_eq!(admitted, 5);
        // One more each 10 ms.
        assert!(!link.admit(now + Duration::from_millis(9)));
        assert!(link.admit(now + Duration::from_millis(10)));
        assert!(!link.admit(now + Duration::from_millis(10)));
        // Over a second, the rate.
        let start = now + Duration::from_secs(10);
        let admitted = (0..1000)
            .filter(|n| link.admit(start + Duration::from_millis(*n)))
            .count();
        assert_eq!(admitted, 100 + 4);
    }
}
