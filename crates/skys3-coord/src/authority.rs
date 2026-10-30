//! Who may ask a node to hand a shard off (design §12, threat T6).
//!
//! A [`Handoff`](crate::Handoff) request is advice: the handoff checks the
//! register, and every configuration it leads to is a compare-and-swap, so
//! a forged request cannot break a shard. It can still stall the shard's
//! writes for the time a handoff takes, over and over. So a node starts
//! the handoffs only the coordinator or an operator asks for: the holder
//! of `coordinator.lease`, or a peer with an `admin` certificate (an
//! operator tool). Any other node of the cluster is refused, also one
//! whose certificate is valid.
//!
//! [`CoordinatorOrAdmin`] reads the lease from the control store to tell
//! who holds it, at most once per interval whoever asks, so a flood of
//! requests costs the store at most one read per interval.

use std::sync::Arc;
use std::time::Duration;

use skys3_control::{ControlStore, TypedKey, read};
use skys3_io::{Clock, MonoTime};
use skys3_net::PeerIdentity;
use skys3_types::NodeId;
use tokio::sync::Mutex;

use crate::handoff::HandoffFuture;

/// Decides whether a peer may ask this node for a handoff: the extension
/// point between the admin endpoint and who the cluster trusts to move
/// primaries ([`AdminEndpoint::with_handoffs_from`](crate::AdminEndpoint::with_handoffs_from)).
pub trait HandoffAuthority: Send + Sync + 'static {
    /// `Ok` if `peer` may ask for a handoff, or why not.
    fn permits<'a>(&'a self, peer: &'a PeerIdentity) -> HandoffFuture<'a>;
}

/// The lease holder as last read, and when.
#[derive(Debug, Clone)]
struct Reading {
    holder: Option<NodeId>,
    at: MonoTime,
}

/// The authority of threat T6: an `admin` certificate, or the node that
/// holds `coordinator.lease` in the control store.
///
/// The holder is read from the store when a node asks and the last
/// reading is older than the interval, so at most one read per interval;
/// within it, the last reading answers. A coordinator that lost the lease
/// since may therefore still be admitted for up to an interval, which
/// costs at most a handoff: the request is advice (design §6.7).
pub struct CoordinatorOrAdmin<S> {
    store: S,
    clock: Arc<dyn Clock>,
    interval: Duration,
    last: Mutex<Option<Reading>>,
}

impl<S> std::fmt::Debug for CoordinatorOrAdmin<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoordinatorOrAdmin")
            .field("interval", &self.interval)
            .finish_non_exhaustive()
    }
}

impl<S: ControlStore> CoordinatorOrAdmin<S> {
    /// The default interval between two reads of the lease.
    pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(1);

    /// Admits `admin` certificates, and the node that holds the lease in
    /// `store`, read at most once per [`Self::DEFAULT_INTERVAL`] on
    /// `clock`.
    pub fn new(store: S, clock: Arc<dyn Clock>) -> Self {
        Self {
            store,
            clock,
            interval: Self::DEFAULT_INTERVAL,
            last: Mutex::new(None),
        }
    }

    /// Reads the lease at most once per `interval`.
    #[must_use]
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// The node that holds the lease: as last read if that was within the
    /// interval, or read again.
    async fn holder(&self) -> Result<Option<NodeId>, String> {
        let mut last = self.last.lock().await;
        let now = self.clock.now();
        if let Some(reading) = last
            .as_ref()
            .filter(|reading| now.saturating_duration_since(reading.at) < self.interval)
        {
            return Ok(reading.holder.clone());
        }
        let holder = read(&self.store, &TypedKey::coordinator_lease())
            .await
            .map_err(|error| format!("cannot read the coordinator lease: {error}"))?
            .map(|lease| lease.value.holder);
        // Timed from when the read returned: a read slower than the
        // interval would otherwise land expired, and every request queued
        // behind the lock would read again, one after another.
        *last = Some(Reading {
            holder: holder.clone(),
            at: self.clock.now(),
        });
        Ok(holder)
    }

    async fn check(&self, peer: &PeerIdentity) -> Result<(), String> {
        let node = match peer {
            PeerIdentity::Admin(_) => return Ok(()),
            PeerIdentity::Node(node) => node,
        };
        match self.holder().await? {
            Some(holder) if holder == *node => Ok(()),
            Some(holder) => Err(format!(
                "{node} does not hold the coordinator lease ({holder} does); only the \
                 coordinator or an admin certificate may ask for a handoff"
            )),
            None => Err(format!(
                "no node holds the coordinator lease, so {node} is not coordinator; only the \
                 coordinator or an admin certificate may ask for a handoff"
            )),
        }
    }
}

impl<S: ControlStore> HandoffAuthority for CoordinatorOrAdmin<S> {
    fn permits<'a>(&'a self, peer: &'a PeerIdentity) -> HandoffFuture<'a> {
        Box::pin(self.check(peer))
    }
}

#[cfg(test)]
mod tests {
    use skys3_control::faults::{Fault, FaultyStore};
    use skys3_control::{Expected, MemoryControlStore, PutOutcome};
    use skys3_io::MonotonicClock;
    use skys3_types::{CoordinatorLease, Label, ProposalId, RegisterDocument};

    use super::*;

    fn node(name: &str) -> PeerIdentity {
        PeerIdentity::Node(NodeId::new(name).unwrap())
    }

    async fn lease(store: &MemoryControlStore, holder: &str, proposal: &str) {
        let key = TypedKey::coordinator_lease();
        let expected = match read(store, &key).await.unwrap() {
            Some(current) => Expected::Version(current.version),
            None => Expected::Absent,
        };
        let lease = CoordinatorLease {
            holder: NodeId::new(holder).unwrap(),
            proposal_id: ProposalId::new(proposal).unwrap(),
        };
        let put = store
            .put_if(key.key(), expected, lease.to_json().unwrap().into())
            .await
            .unwrap();
        assert!(matches!(put, PutOutcome::Written(_)), "{put:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn the_lease_holder_and_admin_certificates_may_ask_and_no_other_node() {
        let store = FaultyStore::new(MemoryControlStore::new());
        let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::new());
        let authority = CoordinatorOrAdmin::new(store.clone(), clock);
        let admin = PeerIdentity::Admin(Label::new("ops").unwrap());

        // No lease: no node may ask, an operator tool may.
        let refused = authority.permits(&node("n1")).await.unwrap_err();
        assert!(refused.contains("no node holds"), "{refused}");
        authority.permits(&admin).await.unwrap();

        // Once n1 holds the lease, it may ask and n2 may not. Within the
        // interval, the reading of no holder still stands.
        lease(store.inner(), "n1", "p1").await;
        assert!(authority.permits(&node("n1")).await.is_err());
        tokio::time::advance(CoordinatorOrAdmin::<MemoryControlStore>::DEFAULT_INTERVAL).await;
        authority.permits(&node("n1")).await.unwrap();
        let refused = authority.permits(&node("n2")).await.unwrap_err();
        assert!(refused.contains("n1 does"), "{refused}");

        // The lease moves to n2.
        lease(store.inner(), "n2", "p2").await;
        tokio::time::advance(Duration::from_secs(1)).await;
        authority.permits(&node("n2")).await.unwrap();
        assert!(authority.permits(&node("n1")).await.is_err());

        // A flood of requests within an interval costs one read.
        tokio::time::advance(Duration::from_secs(1)).await;
        let before = store.requests();
        for _ in 0..100 {
            assert!(authority.permits(&node("n3")).await.is_err());
        }
        assert_eq!(store.requests(), before + 1);

        // A store that does not answer admits no node, and an operator
        // tool still.
        tokio::time::advance(Duration::from_secs(1)).await;
        store.script([Fault::Unavailable]);
        let refused = authority.permits(&node("n2")).await.unwrap_err();
        assert!(refused.contains("cannot read"), "{refused}");
        authority.permits(&admin).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_read_still_answers_for_an_interval() {
        let store = FaultyStore::new(MemoryControlStore::new());
        let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::new());
        let authority = CoordinatorOrAdmin::new(store.clone(), clock);
        lease(store.inner(), "n1", "p1").await;
        // The read takes longer than the interval.
        let slow = Fault::before(|| tokio::time::sleep(Duration::from_millis(1500)));
        store.script([slow]);
        let before = store.requests();
        let n2 = node("n2");
        let (first, second) = tokio::join!(authority.permits(&n2), authority.permits(&n2));
        assert!(first.is_err() && second.is_err());
        for _ in 0..10 {
            assert!(authority.permits(&node("n2")).await.is_err());
        }
        assert_eq!(store.requests(), before + 1);
    }

    #[tokio::test(start_paused = true)]
    async fn the_interval_is_configurable() {
        let store = MemoryControlStore::new();
        let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::new());
        let authority =
            CoordinatorOrAdmin::new(store.clone(), clock).with_interval(Duration::from_millis(10));
        assert!(format!("{authority:?}").contains("10ms"));
        assert!(authority.permits(&node("n1")).await.is_err());
        lease(&store, "n1", "p1").await;
        tokio::time::advance(Duration::from_millis(10)).await;
        authority.permits(&node("n1")).await.unwrap();
    }
}
