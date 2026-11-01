//! The repair bandwidth cap of a node (`repair_bytes_per_second_per_node`,
//! §8.6).

use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::time::Instant;

/// Paces the bytes a node's repairs read and write, over every shard it
/// leads, to at most `bytes_per_second` on average.
///
/// Each transfer takes its bytes before it starts ([`RepairBandwidth::take`])
/// and starts no earlier than the transfers before it, at the cap, would
/// have ended: transfer `j` starts at least `(b_i + … + b_{j−1}) / rate`
/// after transfer `i` does. So the bytes started in any window of `t`
/// seconds are at most `rate × t` plus the last transfer's, and a burst
/// after an idle spell is never larger than one transfer. Clones share
/// the cap.
#[derive(Clone)]
pub struct RepairBandwidth {
    bytes_per_second: u64,
    /// When the next transfer may start.
    next: Arc<Mutex<Option<Instant>>>,
}

impl fmt::Debug for RepairBandwidth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RepairBandwidth")
            .field("bytes_per_second", &self.bytes_per_second)
            .finish_non_exhaustive()
    }
}

impl RepairBandwidth {
    /// A cap of `bytes_per_second`, at least one byte a second.
    #[must_use]
    pub fn new(bytes_per_second: u64) -> Self {
        Self {
            bytes_per_second: bytes_per_second.max(1),
            next: Arc::default(),
        }
    }

    /// The cap, in bytes a second.
    #[must_use]
    pub fn bytes_per_second(&self) -> u64 {
        self.bytes_per_second
    }

    /// How long `bytes` take at the cap.
    #[must_use]
    pub fn duration_of(&self, bytes: u64) -> Duration {
        let nanos = u128::from(bytes) * 1_000_000_000 / u128::from(self.bytes_per_second);
        Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
    }

    /// Waits until a transfer of `bytes` may start, and counts it.
    pub async fn take(&self, bytes: u64) {
        let start = {
            let mut next = self.next.lock().unwrap_or_else(PoisonError::into_inner);
            let now = Instant::now();
            let start = next.map_or(now, |next| next.max(now));
            *next = Some(start + self.duration_of(bytes));
            start
        };
        tokio::time::sleep_until(start).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn transfers_start_no_sooner_than_the_cap_allows() {
        let cap = RepairBandwidth::new(1000);
        assert_eq!(cap.bytes_per_second(), 1000);
        let begin = Instant::now();
        // The first transfer starts at once, whatever its size.
        cap.take(500).await;
        assert_eq!(begin.elapsed(), Duration::ZERO);
        // The next waits for the first's bytes at the cap, and clones
        // share it.
        let shared = cap.clone();
        shared.take(2000).await;
        assert_eq!(begin.elapsed(), Duration::from_millis(500));
        cap.take(1).await;
        assert_eq!(begin.elapsed(), Duration::from_millis(2500));
        // An idle spell earns no burst beyond the next transfer.
        tokio::time::sleep(Duration::from_secs(10)).await;
        let idle = Instant::now();
        cap.take(1000).await;
        cap.take(1000).await;
        assert_eq!(idle.elapsed(), Duration::from_secs(1));
        assert_eq!(RepairBandwidth::new(0).bytes_per_second(), 1);
        assert_eq!(
            RepairBandwidth::new(3).duration_of(1),
            Duration::from_nanos(333_333_333)
        );
    }
}
