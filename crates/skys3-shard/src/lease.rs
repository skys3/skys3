//! Primary leases and member grace (§5.4).
//!
//! A primary serves strongly consistent reads (GET, HEAD, LIST, and
//! conditional checks) only while it holds a lease from every member, and a
//! member proposes no new primary until `primary_grace` has passed since it
//! last granted one. Both sides time their part on their own monotonic
//! clock; no reading crosses from one node's clock to another's.
//!
//! - **Stamps.** Every append and beacon the primary sends carries a stamp:
//!   its clock reading when it sent it. A member's acknowledgement echoes
//!   the latest stamp it received in the session. That grants the primary
//!   a lease until the stamp plus `primary_lease`, on the primary's clock
//!   ([`Leases`]).
//! - **Grace.** A member restarts its grace each time it sends an
//!   acknowledgement that grants a lease, after it received the stamp, and
//!   counts opening its replica as a grant, so a restart is one too
//!   ([`Grace`]).
//!
//! The lease starts when the primary sent the beacon and the grace when the
//! member acknowledged it, so any delay between the two only shortens the
//! lease. With rates within `1 ± ρ`, a lease of `primary_lease` lasts at
//! most `primary_lease / (1−ρ)` real time, and a grace of `primary_grace`
//! at least `primary_grace / (1+ρ)`. Configuration loading enforces
//! `primary_grace ≥ primary_lease × (1+ρ)/(1−ρ) + margin`, so every lease a
//! member granted has expired before its grace passes.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use skys3_io::{Clock, MonoTime};
use skys3_types::{Epoch, NodeId};
use tokio::sync::watch;

/// The leases a primary holds from its members, timed on its own clock.
///
/// Until [`Leases::start`] gives it a clock, it holds none.
#[derive(Default)]
pub(crate) struct Leases {
    /// The primary's clock and `primary_lease`.
    timing: Option<(Arc<dyn Clock>, Duration)>,
    /// When the lease from each member ends, on the primary's clock.
    until: BTreeMap<NodeId, MonoTime>,
}

impl fmt::Debug for Leases {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Leases")
            .field("until", &self.until)
            .finish_non_exhaustive()
    }
}

impl Leases {
    /// Times leases of `primary_lease` on `clock` from now on.
    pub(crate) fn start(&mut self, clock: Arc<dyn Clock>, primary_lease: Duration) {
        self.timing = Some((clock, primary_lease));
    }

    /// The stamp for a beacon sent now, or `None` before [`Leases::start`].
    pub(crate) fn stamp(&self) -> Option<MonoTime> {
        self.timing.as_ref().map(|(clock, _)| clock.now())
    }

    /// Records that `member` acknowledged the beacon stamped `stamp`. A
    /// stamp from the future cannot be one this primary sent, so it counts
    /// as sent now.
    pub(crate) fn granted(&mut self, member: &NodeId, stamp: MonoTime) {
        let Some((clock, lease)) = &self.timing else {
            return;
        };
        let until = stamp.min(clock.now()).saturating_add(*lease);
        let current = self.until.entry(member.clone()).or_insert(until);
        *current = (*current).max(until);
    }

    /// When the lease from `member` ends, if it ever granted one.
    pub(crate) fn until(&self, member: &NodeId) -> Option<MonoTime> {
        self.until.get(member).copied()
    }

    /// Whether the lease from every one of `members` is valid now.
    pub(crate) fn held(&self, members: &[NodeId]) -> bool {
        let Some((clock, _)) = &self.timing else {
            return false;
        };
        let now = clock.now();
        members
            .iter()
            .all(|member| self.until(member).is_some_and(|until| now < until))
    }
}

/// A member's grace for one shard: when it last granted the primary a
/// lease, on its own clock (§5.4).
///
/// A member proposes no new primary until [`Grace::has_passed`]. The grace
/// counts from the grace's creation as if a lease had been granted then,
/// because a node that restarts does not know when it last granted one.
///
/// A candidate stops granting, and acknowledging, before it proposes
/// itself (rule R1, §6.3): [`Grace::stop_if_passed`] decides that under the
/// same lock as every grant, so no grant slips in between. The grace then
/// holds the epoch the candidate proposes over, so that a session of that
/// epoch, or an older one, cannot resume it ([`Grace::resume_for`]) while
/// the proposal may still land, and a candidate that was resumed proposes
/// nothing more ([`Grace::propose_over`]).
pub struct Grace {
    clock: Arc<dyn Clock>,
    grace: Duration,
    granted: watch::Sender<MonoTime>,
    /// The epoch the member proposes over, once it stopped granting to
    /// propose itself.
    stopped: Mutex<Option<Epoch>>,
}

impl fmt::Debug for Grace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Grace")
            .field("grace", &self.grace)
            .field("last_granted", &self.last_granted())
            .finish_non_exhaustive()
    }
}

impl Grace {
    /// A grace of `primary_grace` on `clock`, counting from now.
    #[must_use]
    pub fn new(clock: Arc<dyn Clock>, primary_grace: Duration) -> Self {
        let now = clock.now();
        Self {
            clock,
            grace: primary_grace,
            granted: watch::Sender::new(now),
            stopped: Mutex::new(None),
        }
    }

    /// Records a lease granted now: call it after the stamp it echoes
    /// arrived, and send the acknowledgement that grants it only if this
    /// returns `true`. A member that stopped granting grants nothing.
    pub fn grant(&self) -> bool {
        let stopped = self.lock();
        if stopped.is_some() {
            return false;
        }
        self.record_grant();
        true
    }

    fn record_grant(&self) {
        let now = self.clock.now();
        self.granted.send_if_modified(|last| {
            let later = now > *last;
            *last = (*last).max(now);
            later
        });
    }

    /// Stops granting leases, and acknowledging appends, if the grace has
    /// passed: the member may then propose itself as primary over its
    /// configuration in `epoch` (R1). Returns whether it is stopped.
    pub fn stop_if_passed(&self, epoch: Epoch) -> bool {
        let mut stopped = self.lock();
        if stopped.is_none() && self.has_passed() {
            *stopped = Some(epoch);
        }
        stopped.is_some()
    }

    /// Whether the member stopped granting to propose itself.
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.lock().is_some()
    }

    /// Records that a stopped member proposes over its configuration in
    /// `epoch` now. Returns `false` if it was resumed in the meantime: it
    /// may have granted since, so it must not propose.
    pub fn propose_over(&self, epoch: Epoch) -> bool {
        match self.lock().as_mut() {
            Some(over) => {
                *over = (*over).max(epoch);
                true
            }
            None => false,
        }
    }

    /// Grants again for a session of a configuration in `epoch`, which a
    /// stopped member follows only if it is newer than the one it proposes
    /// over: the register has then moved past that, so the proposal cannot
    /// land. Returns whether the member grants.
    pub fn resume_for(&self, epoch: Epoch) -> bool {
        let mut stopped = self.lock();
        if stopped.is_some_and(|over| epoch <= over) {
            return false;
        }
        if stopped.take().is_some() {
            self.record_grant();
        }
        true
    }

    /// Grants again after a proposal lost: the member follows the
    /// configuration that won, and counts this as a grant, so that it
    /// gives the new primary a whole grace to reach it before it proposes
    /// again.
    pub fn resume(&self) {
        if self.lock().take().is_some() {
            self.record_grant();
        }
    }

    /// Waits until the grace has passed.
    pub async fn passed(&self) {
        while !self.has_passed() {
            self.clock.sleep_until(self.passes_at()).await;
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Option<Epoch>> {
        self.stopped.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// When the member last granted a lease, or the grace started.
    #[must_use]
    pub fn last_granted(&self) -> MonoTime {
        *self.granted.borrow()
    }

    /// The time since the member last granted a lease.
    #[must_use]
    pub fn since_granted(&self) -> Duration {
        self.clock
            .now()
            .saturating_duration_since(self.last_granted())
    }

    /// When the grace passes, unless the member grants another lease first.
    #[must_use]
    pub fn passes_at(&self) -> MonoTime {
        self.last_granted().saturating_add(self.grace)
    }

    /// Whether `primary_grace` has passed since the member last granted a
    /// lease: from then on, no lease it granted is still valid.
    #[must_use]
    pub fn has_passed(&self) -> bool {
        self.clock.now() >= self.passes_at()
    }

    /// A receiver that sees [`Grace::last_granted`] as it changes.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<MonoTime> {
        self.granted.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use skys3_io::{Drift, MonotonicClock};

    use super::*;

    fn node(n: u8) -> NodeId {
        format!("node-{n}").parse().unwrap()
    }

    fn ms(ms: u64) -> Duration {
        Duration::from_millis(ms)
    }

    #[tokio::test(start_paused = true)]
    async fn leases_last_from_the_stamp_on_the_primarys_clock() {
        let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::drifting(
            Drift::NONE,
            MonoTime::from_nanos(1_000_000_000),
        ));
        let members = [node(2), node(3)];
        let mut leases = Leases::default();
        // Without a clock, nothing is stamped or granted.
        assert_eq!(leases.stamp(), None);
        leases.granted(&node(2), clock.now());
        assert_eq!(leases.until(&node(2)), None);
        assert!(!leases.held(&members));

        leases.start(Arc::clone(&clock), ms(400));
        let stamp = leases.stamp().unwrap();
        tokio::time::advance(ms(100)).await;
        leases.granted(&node(2), stamp);
        assert_eq!(leases.until(&node(2)), Some(stamp + ms(400)));
        // One member's lease is not enough.
        assert!(!leases.held(&members));
        // A stamp from the future counts as sent now.
        leases.granted(&node(3), clock.now() + ms(10_000));
        assert_eq!(leases.until(&node(3)), Some(clock.now() + ms(400)));
        assert!(leases.held(&members));
        // An older stamp never shortens a lease.
        leases.granted(&node(3), stamp);
        assert_eq!(leases.until(&node(3)), Some(clock.now() + ms(400)));

        // The lease from node 2 counts from its stamp, not its arrival.
        tokio::time::advance(ms(299)).await;
        assert!(leases.held(&members));
        tokio::time::advance(ms(1)).await;
        assert!(!leases.held(&members));
        assert!(leases.held(&members[1..]));
    }

    #[tokio::test(start_paused = true)]
    async fn grace_counts_from_the_last_grant() {
        let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::new());
        let grace = Grace::new(Arc::clone(&clock), ms(600));
        let mut granted = grace.subscribe();
        // Creation counts as a grant.
        assert_eq!(grace.last_granted(), MonoTime::ZERO);
        assert_eq!(grace.passes_at(), MonoTime::ZERO + ms(600));
        tokio::time::advance(ms(500)).await;
        assert_eq!(grace.since_granted(), ms(500));
        assert!(!grace.has_passed());
        assert!(grace.grant());
        assert!(granted.has_changed().unwrap());
        assert_eq!(*granted.borrow_and_update(), MonoTime::ZERO + ms(500));
        // A second grant at the same reading changes nothing.
        assert!(grace.grant());
        assert!(!granted.has_changed().unwrap());
        // Too early to stop granting.
        assert!(!grace.stop_if_passed(Epoch::new(3)));
        tokio::time::advance(ms(599)).await;
        assert!(!grace.has_passed());
        let started = tokio::time::Instant::now();
        grace.passed().await;
        assert_eq!(started.elapsed(), ms(1));
        assert!(grace.has_passed());
        assert!(format!("{grace:?}").contains("last_granted"));

        // A candidate stops granting (R1) until it resumes, which counts as
        // a grant.
        let epoch = Epoch::new;
        assert!(grace.stop_if_passed(epoch(3)));
        assert!(grace.is_stopped());
        assert!(!grace.grant());
        assert!(!granted.has_changed().unwrap());
        // It proposes over epoch 4 next: sessions up to that epoch do not
        // resume it, a newer one does.
        assert!(grace.propose_over(epoch(4)));
        assert!(!grace.resume_for(epoch(4)));
        assert!(grace.is_stopped());
        assert!(grace.resume_for(epoch(5)));
        assert!(!grace.is_stopped());
        assert!(granted.has_changed().unwrap());
        assert!(!grace.has_passed());
        // Resumed, it proposes nothing more until its grace passes again.
        assert!(!grace.propose_over(epoch(5)));
        assert!(!grace.stop_if_passed(epoch(5)));
        assert!(grace.resume_for(epoch(1)));
        tokio::time::advance(ms(600)).await;
        assert!(grace.stop_if_passed(epoch(5)));
        grace.resume();
        assert!(!grace.is_stopped());
        // Resuming a grace that is not stopped changes nothing.
        granted.borrow_and_update();
        grace.resume();
        assert!(!granted.has_changed().unwrap());
    }
}
