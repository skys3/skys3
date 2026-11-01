//! The repair bandwidth cap of a node (`repair_bytes_per_second_per_node`,
//! §8.6), which fragment moves share, after repairs.

use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::Instant;

/// Paces the bytes a node's repairs and fragment moves read and write,
/// over every shard it leads, to at most `bytes_per_second` on average.
///
/// Each transfer takes its bytes before it starts ([`RepairBandwidth::take`])
/// and starts no earlier than the transfers before it, at the cap, would
/// have ended: transfer `j` starts at least `(b_i + … + b_{j−1}) / rate`
/// after transfer `i` does. So the bytes started in any window of `t`
/// seconds are at most `rate × t` plus the last transfer's, and a burst
/// after an idle spell is never larger than one transfer. Clones share
/// the cap.
///
/// Repairs come first: while any repairer of the node holds a
/// [`Repairing`] guard, a move's transfer waits to take its bytes
/// ([`RepairBandwidth::take_after_repairs`]). A repair then waits for at
/// most the move transfers that had already taken theirs.
#[derive(Clone)]
pub struct RepairBandwidth {
    bytes_per_second: u64,
    /// When the next transfer may start.
    next: Arc<Mutex<Option<Instant>>>,
    /// How many repairers have repairs to make.
    repairing: Arc<watch::Sender<usize>>,
}

impl fmt::Debug for RepairBandwidth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RepairBandwidth")
            .field("bytes_per_second", &self.bytes_per_second)
            .field("repairing", &*self.repairing.borrow())
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
            repairing: Arc::new(watch::Sender::new(0)),
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

    /// Waits until no repairer holds a [`Repairing`] guard, then as
    /// [`RepairBandwidth::take`] does: a fragment move's transfer.
    pub async fn take_after_repairs(&self, bytes: u64) {
        let mut repairing = self.repairing.subscribe();
        // The sender lives as long as `self`, so the wait cannot fail.
        let _ = repairing.wait_for(|repairing| *repairing == 0).await;
        self.take(bytes).await;
    }

    /// Marks repairs to make until the guard is dropped: moves wait for
    /// them.
    #[must_use]
    pub fn repairing(&self) -> Repairing {
        self.repairing.send_modify(|repairing| *repairing += 1);
        Repairing {
            repairing: Arc::clone(&self.repairing),
        }
    }

    /// Whether a repairer holds a [`Repairing`] guard.
    #[must_use]
    pub fn has_repairs(&self) -> bool {
        *self.repairing.borrow() > 0
    }
}

/// Repairs a repairer has to make, from [`RepairBandwidth::repairing`]:
/// fragment moves wait for the cap until every such guard is dropped.
#[derive(Debug)]
pub struct Repairing {
    repairing: Arc<watch::Sender<usize>>,
}

impl Drop for Repairing {
    fn drop(&mut self) {
        self.repairing.send_modify(|repairing| *repairing -= 1);
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

    #[tokio::test(start_paused = true)]
    async fn moves_wait_for_repairs() {
        let cap = RepairBandwidth::new(1000);
        let begin = Instant::now();
        // With no repair to make, a move takes the cap as a repair does.
        cap.take_after_repairs(1000).await;
        assert_eq!(begin.elapsed(), Duration::ZERO);
        let repairing = cap.repairing();
        let other = cap.repairing();
        assert!(cap.has_repairs());
        let shared = cap.clone();
        let mover = tokio::spawn(async move {
            shared.take_after_repairs(1000).await;
            Instant::now()
        });
        // Repairs keep taking the cap while the move waits for every one.
        cap.take(500).await;
        tokio::time::sleep(Duration::from_secs(5)).await;
        drop(repairing);
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(!mover.is_finished());
        drop(other);
        assert!(!cap.has_repairs());
        let moved = mover.await.unwrap();
        assert_eq!(moved - begin, Duration::from_secs(7));
        assert!(format!("{cap:?}").contains("repairing: 0"));
    }
}
