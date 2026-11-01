//! Caps on the TLS handshakes a listener runs at once (design §12).
//!
//! A host that floods a listener with connections and never finishes a
//! handshake would otherwise get a task and TLS state per connection for
//! the whole handshake timeout, so memory would grow with the rate of the
//! flood. A [`HandshakeLimit`] bounds that: a listener takes a slot
//! ([`HandshakeLimit::acquire`], or [`HandshakeLimit::try_acquire`] where
//! a refusal is cheap) before it starts a handshake, and gives it back
//! when the handshake ends, however it ends.
//!
//! [`HandshakeMetrics`] exports what every limit of a node does, by
//! listener:
//!
//! | Metric | Type | Meaning |
//! |---|---|---|
//! | `skys3_handshakes_in_flight{listener}` | gauge | Handshakes running now; never above the listener's cap |
//! | `skys3_handshakes_total{listener,outcome}` | counter | Handshakes ended, by `outcome`: `completed`, `failed`, `timed_out`, or `refused` (turned away at the cap) |
//!
//! ```
//! use skys3_obs::{HandshakeLimit, HandshakeMetrics, HandshakeOutcome, MetricsRegistry};
//!
//! # tokio::runtime::Builder::new_current_thread().build()?.block_on(async {
//! let registry = MetricsRegistry::new();
//! let metrics = HandshakeMetrics::register(&registry);
//! let limit = HandshakeLimit::new(2).with_metrics(&metrics, "transport");
//! let first = limit.acquire().await;
//! let second = limit.try_acquire().expect("a slot is free");
//! assert!(limit.try_acquire().is_none(), "both slots are taken");
//! assert_eq!(limit.in_flight(), 2);
//! first.finish(HandshakeOutcome::Completed);
//! second.finish(HandshakeOutcome::TimedOut);
//! assert_eq!(limit.in_flight(), 0);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # })?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::fmt;
use std::sync::Arc;

use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::metrics::MetricsRegistry;

/// How a handshake ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HandshakeOutcome {
    /// The peer was authenticated, and the connection is served.
    Completed,
    /// The handshake failed: an untrusted certificate, a protocol error,
    /// or a connection closed early.
    Failed,
    /// The handshake did not finish within the listener's timeout.
    TimedOut,
    /// The connection was turned away because every slot was taken.
    Refused,
}

impl HandshakeOutcome {
    /// The value of the `outcome` label.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::TimedOut => "timed_out",
            Self::Refused => "refused",
        }
    }
}

/// The label set of a listener's gauge.
type ListenerLabels = [(&'static str, &'static str); 1];
/// The label set of a listener's outcome counter.
type OutcomeLabels = [(&'static str, &'static str); 2];

/// The handshake metrics of a node's listeners, registered once and shared
/// by every [`HandshakeLimit`] of the node. Clones share the families.
#[derive(Debug, Clone, Default)]
pub struct HandshakeMetrics {
    in_flight: Family<ListenerLabels, Gauge>,
    ended: Family<OutcomeLabels, Counter>,
}

impl HandshakeMetrics {
    /// Registers `skys3_handshakes_in_flight` and `skys3_handshakes_total`
    /// in `registry`.
    #[must_use]
    pub fn register(registry: &MetricsRegistry) -> Self {
        let metrics = Self::default();
        registry.register(
            "handshakes_in_flight",
            "TLS handshakes a listener runs now, by listener; at most its \
             max_inflight_handshakes.",
            metrics.in_flight.clone(),
        );
        registry.register(
            "handshakes",
            "TLS handshakes a listener ended, by listener and outcome: completed, failed, \
             timed_out, or refused at the cap.",
            metrics.ended.clone(),
        );
        metrics
    }

    /// The handshakes `listener` runs now, as exported.
    #[must_use]
    pub fn in_flight(&self, listener: &'static str) -> i64 {
        self.in_flight
            .get_or_create(&[("listener", listener)])
            .get()
    }

    /// The handshakes of `listener` that ended with `outcome`, as
    /// exported.
    #[must_use]
    pub fn ended(&self, listener: &'static str, outcome: HandshakeOutcome) -> u64 {
        self.ended
            .get_or_create(&[("listener", listener), ("outcome", outcome.as_str())])
            .get()
    }
}

/// One listener's metrics.
#[derive(Clone)]
struct ListenerMetrics {
    listener: &'static str,
    in_flight: Gauge,
    metrics: HandshakeMetrics,
}

impl ListenerMetrics {
    fn record(&self, outcome: HandshakeOutcome) {
        self.metrics
            .ended
            .get_or_create(&[("listener", self.listener), ("outcome", outcome.as_str())])
            .inc();
    }
}

/// The cap on one listener's in-flight handshakes. Clones share the slots.
#[derive(Clone)]
pub struct HandshakeLimit {
    slots: Arc<Semaphore>,
    cap: usize,
    metrics: Option<ListenerMetrics>,
}

impl fmt::Debug for HandshakeLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HandshakeLimit")
            .field("cap", &self.cap)
            .field("in_flight", &self.in_flight())
            .field("listener", &self.metrics.as_ref().map(|m| m.listener))
            .finish()
    }
}

impl HandshakeLimit {
    /// A limit of `cap` handshakes at once; at least one.
    #[must_use]
    pub fn new(cap: u32) -> Self {
        let cap = usize::try_from(cap.max(1)).unwrap_or(usize::MAX);
        Self {
            slots: Arc::new(Semaphore::new(cap)),
            cap,
            metrics: None,
        }
    }

    /// Exports the limit's handshakes in `metrics` as those of `listener`,
    /// such as `"transport"`, `"gateway"`, or `"peer"`.
    #[must_use]
    pub fn with_metrics(mut self, metrics: &HandshakeMetrics, listener: &'static str) -> Self {
        self.metrics = Some(ListenerMetrics {
            listener,
            in_flight: metrics
                .in_flight
                .get_or_create(&[("listener", listener)])
                .clone(),
            metrics: metrics.clone(),
        });
        self
    }

    /// The most handshakes that run at once.
    #[must_use]
    pub fn cap(&self) -> usize {
        self.cap
    }

    /// The handshakes running now.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.cap.saturating_sub(self.slots.available_permits())
    }

    /// Waits for a free slot. A listener that waits before it accepts the
    /// next connection leaves further connections in the kernel's accept
    /// queue, which holds no task or TLS state.
    pub async fn acquire(&self) -> HandshakePermit {
        let permit = Arc::clone(&self.slots)
            .acquire_owned()
            .await
            .expect("the semaphore of a handshake limit is never closed");
        self.permit(permit)
    }

    /// Takes a free slot, or returns `None` at the cap, which counts as a
    /// [`HandshakeOutcome::Refused`] handshake: the caller turns the
    /// connection away.
    #[must_use]
    pub fn try_acquire(&self) -> Option<HandshakePermit> {
        match Arc::clone(&self.slots).try_acquire_owned() {
            Ok(permit) => Some(self.permit(permit)),
            Err(_) => {
                if let Some(metrics) = &self.metrics {
                    metrics.record(HandshakeOutcome::Refused);
                }
                None
            }
        }
    }

    fn permit(&self, permit: OwnedSemaphorePermit) -> HandshakePermit {
        if let Some(metrics) = &self.metrics {
            metrics.in_flight.inc();
        }
        HandshakePermit {
            _permit: permit,
            metrics: self.metrics.clone(),
        }
    }
}

/// A slot of a [`HandshakeLimit`], held while one handshake runs and
/// given back when it is dropped or [finished](HandshakePermit::finish).
#[must_use = "the slot is given back as soon as the permit is dropped"]
pub struct HandshakePermit {
    _permit: OwnedSemaphorePermit,
    metrics: Option<ListenerMetrics>,
}

impl fmt::Debug for HandshakePermit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HandshakePermit").finish_non_exhaustive()
    }
}

impl HandshakePermit {
    /// Records how the handshake ended, and gives the slot back.
    pub fn finish(self, outcome: HandshakeOutcome) {
        if let Some(metrics) = &self.metrics {
            metrics.record(outcome);
        }
    }
}

impl Drop for HandshakePermit {
    fn drop(&mut self) {
        if let Some(metrics) = &self.metrics {
            metrics.in_flight.dec();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn slots_are_capped_counted_and_given_back() {
        let registry = MetricsRegistry::new();
        let metrics = HandshakeMetrics::register(&registry);
        let limit = HandshakeLimit::new(2).with_metrics(&metrics, "gateway");
        assert_eq!(limit.cap(), 2);
        let first = limit.acquire().await;
        let second = limit.acquire().await;
        assert_eq!((limit.in_flight(), metrics.in_flight("gateway")), (2, 2));
        assert!(limit.try_acquire().is_none());
        assert_eq!(metrics.ended("gateway", HandshakeOutcome::Refused), 1);

        // A waiter gets the slot the first handshake gives back.
        let waiter = {
            let limit = limit.clone();
            tokio::spawn(async move { limit.acquire().await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished(), "every slot is taken");
        first.finish(HandshakeOutcome::Completed);
        let third = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("the slot was given back")
            .unwrap();
        assert_eq!(metrics.in_flight("gateway"), 2);
        // A dropped permit gives its slot back without an outcome.
        drop(second);
        third.finish(HandshakeOutcome::Failed);
        assert_eq!((limit.in_flight(), metrics.in_flight("gateway")), (0, 0));
        assert_eq!(metrics.ended("gateway", HandshakeOutcome::Completed), 1);
        assert_eq!(metrics.ended("gateway", HandshakeOutcome::Failed), 1);
        assert_eq!(metrics.ended("gateway", HandshakeOutcome::TimedOut), 0);
        // Another listener's series are its own.
        assert_eq!(metrics.in_flight("peer"), 0);

        let text = registry.encode().unwrap();
        assert!(
            text.contains(r#"skys3_handshakes_total{listener="gateway",outcome="completed"} 1"#),
            "{text}"
        );
        assert!(
            text.contains(r#"skys3_handshakes_in_flight{listener="gateway"} 0"#),
            "{text}"
        );
    }

    #[test]
    fn a_limit_without_metrics_still_caps() {
        let limit = HandshakeLimit::new(0);
        assert_eq!(limit.cap(), 1, "at least one slot");
        let permit = limit.try_acquire().unwrap();
        assert!(limit.try_acquire().is_none());
        assert!(format!("{limit:?}").contains("in_flight: 1"));
        assert_eq!(format!("{permit:?}"), "HandshakePermit { .. }");
        permit.finish(HandshakeOutcome::Completed);
        assert_eq!(limit.in_flight(), 0);
        let outcomes = [
            HandshakeOutcome::Completed,
            HandshakeOutcome::Failed,
            HandshakeOutcome::TimedOut,
            HandshakeOutcome::Refused,
        ];
        let names: Vec<_> = outcomes.iter().map(|o| o.as_str()).collect();
        assert_eq!(names, ["completed", "failed", "timed_out", "refused"]);
    }
}
