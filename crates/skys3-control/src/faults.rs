//! Fault injection for any [`ControlStore`], for tests and simulation.
//!
//! [`FaultyStore`] wraps a backend and decides, per request, whether to
//! pass it on or to inject a fault: a request lost before or after it was
//! applied, one applied late, a `409`-style conflict, or an outage. Faults
//! come from a script, for unit tests that need an exact sequence, and
//! otherwise from seeded random [`FaultRates`], for simulation. Random
//! delays before and after each request let concurrent callers interleave.
//!
//! The wrapped store's change stream is passed through without faults.

use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use skys3_types::Generation;

use crate::key::{KeyPrefix, RegisterKey};
use crate::store::{ControlError, ControlStore, Expected, PutOutcome, Version, Versioned};

type HookFn = dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync;

/// An action a scripted [`Fault`] runs while a request is in flight, such
/// as another writer's request.
#[derive(Clone)]
pub struct Hook(Arc<HookFn>);

impl Hook {
    /// Wraps an async action.
    pub fn new<F, Fut>(action: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        Self(Arc::new(move || Box::pin(action())))
    }

    async fn run(&self) {
        (self.0)().await;
    }
}

impl fmt::Debug for Hook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Hook")
    }
}

/// What happens to one request.
///
/// Reads suffer the same faults as writes, except that a conflict on a read
/// is reported as [`ControlError::Unavailable`], and a late read is simply
/// lost.
#[derive(Debug, Clone)]
pub enum Fault {
    /// The request is passed on unchanged.
    Pass,
    /// The request is lost before it is applied: nothing is written, and
    /// the caller gets [`ControlError::Indeterminate`].
    LoseRequest,
    /// The request is applied and its answer is lost: the caller gets
    /// [`ControlError::Indeterminate`].
    LoseResponse,
    /// The caller gets [`ControlError::Indeterminate`] at once, and the
    /// request is applied after the delay, like a timed-out request still
    /// in flight.
    LateRequest(Duration),
    /// The store reports a concurrent conditional write
    /// ([`ControlError::Conflict`]); nothing is written.
    Conflict,
    /// The store is unavailable ([`ControlError::Unavailable`]); nothing is
    /// written.
    Unavailable,
    /// The request fails with an error that is not retried
    /// ([`ControlError::Io`]); nothing is written.
    Fail,
    /// The hook runs, and then the request is passed on.
    Before(Hook),
    /// The request is applied, the hook runs, and the answer is lost.
    LoseResponseAfter(Hook),
}

impl Fault {
    /// [`Fault::Before`] with an async action.
    pub fn before<F, Fut>(action: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        Self::Before(Hook::new(action))
    }

    /// [`Fault::LoseResponseAfter`] with an async action.
    pub fn lose_response_after<F, Fut>(action: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        Self::LoseResponseAfter(Hook::new(action))
    }
}

/// The probability of each random fault per request, and the delays.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct FaultRates {
    /// [`Fault::LoseRequest`].
    pub lose_request: f64,
    /// [`Fault::LoseResponse`].
    pub lose_response: f64,
    /// [`Fault::LateRequest`], delayed by up to four times `max_delay`.
    pub late_request: f64,
    /// [`Fault::Conflict`].
    pub conflict: f64,
    /// [`Fault::Unavailable`].
    pub unavailable: f64,
    /// The longest random delay before a request is applied, and again
    /// before its answer is returned.
    pub max_delay: Duration,
}

/// Counts of the requests a [`FaultyStore`] saw and the faults it
/// injected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FaultStats {
    /// Every request.
    pub requests: u64,
    /// [`Fault::LoseRequest`]s.
    pub lost_requests: u64,
    /// [`Fault::LoseResponse`]s and [`Fault::LoseResponseAfter`]s.
    pub lost_responses: u64,
    /// [`Fault::LateRequest`]s.
    pub late_requests: u64,
    /// [`Fault::Conflict`]s.
    pub conflicts: u64,
    /// [`Fault::Unavailable`]s.
    pub unavailable: u64,
}

/// A [`ControlStore`] that injects faults into the requests it passes to
/// another store. Clones share the script, the random generator, and the
/// counts.
#[derive(Debug, Clone)]
pub struct FaultyStore<S> {
    inner: S,
    state: Arc<Mutex<FaultState>>,
}

#[derive(Debug)]
struct FaultState {
    script: VecDeque<Fault>,
    rates: FaultRates,
    rng: SmallRng,
    stats: FaultStats,
}

/// A request's plan: delays around it, and its fault.
struct Plan {
    before: Duration,
    after: Duration,
    fault: Fault,
}

impl<S: ControlStore> FaultyStore<S> {
    /// Wraps `inner` without random faults; scripted faults can be added.
    pub fn new(inner: S) -> Self {
        Self::seeded(inner, 0, FaultRates::default())
    }

    /// Wraps `inner` with random faults drawn from `seed`.
    pub fn seeded(inner: S, seed: u64, rates: FaultRates) -> Self {
        Self {
            inner,
            state: Arc::new(Mutex::new(FaultState {
                script: VecDeque::new(),
                rates,
                rng: SmallRng::seed_from_u64(seed),
                stats: FaultStats::default(),
            })),
        }
    }

    fn state(&self) -> MutexGuard<'_, FaultState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The wrapped store.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Appends faults for the next requests, which take them in order
    /// before any random fault.
    pub fn script(&self, faults: impl IntoIterator<Item = Fault>) {
        self.state().script.extend(faults);
    }

    /// Replaces the random fault rates.
    pub fn set_rates(&self, rates: FaultRates) {
        self.state().rates = rates;
    }

    /// The counts so far.
    pub fn stats(&self) -> FaultStats {
        self.state().stats
    }

    /// The number of requests so far.
    pub fn requests(&self) -> u64 {
        self.stats().requests
    }

    /// Draws the next request's plan and counts it.
    fn plan(&self) -> Plan {
        let mut state = self.state();
        let FaultState {
            script,
            rates,
            rng,
            stats,
        } = &mut *state;
        let mut delay = || {
            if rates.max_delay.is_zero() {
                Duration::ZERO
            } else {
                rng.random_range(Duration::ZERO..=rates.max_delay)
            }
        };
        let (before, after) = (delay(), delay());
        let fault = match script.pop_front() {
            Some(fault) => fault,
            None => {
                let roll: f64 = rng.random();
                let late =
                    rates.max_delay.max(Duration::from_millis(1)) * rng.random_range(1..=4_u32);
                [
                    (rates.lose_request, Fault::LoseRequest),
                    (rates.lose_response, Fault::LoseResponse),
                    (rates.late_request, Fault::LateRequest(late)),
                    (rates.conflict, Fault::Conflict),
                    (rates.unavailable, Fault::Unavailable),
                ]
                .into_iter()
                .scan(0.0, |total, (rate, fault)| {
                    *total += rate;
                    Some((*total, fault))
                })
                .find(|(total, _)| roll < *total)
                .map_or(Fault::Pass, |(_, fault)| fault)
            }
        };
        stats.requests += 1;
        match fault {
            Fault::LoseRequest => stats.lost_requests += 1,
            Fault::LoseResponse | Fault::LoseResponseAfter(_) => stats.lost_responses += 1,
            Fault::LateRequest(_) => stats.late_requests += 1,
            Fault::Conflict => stats.conflicts += 1,
            Fault::Unavailable => stats.unavailable += 1,
            Fault::Pass | Fault::Fail | Fault::Before(_) => {}
        }
        Plan {
            before,
            after,
            fault,
        }
    }

    /// Runs one request under the next plan. `request` sends it to the
    /// wrapped store; `late` sends it in the background.
    async fn inject<T, F>(
        &self,
        key: Option<&RegisterKey>,
        request: impl FnOnce() -> F,
        late: impl FnOnce(Duration),
    ) -> Result<T, ControlError>
    where
        F: Future<Output = Result<T, ControlError>>,
    {
        let plan = self.plan();
        tokio::time::sleep(plan.before).await;
        let lost = || ControlError::Indeterminate("injected lost answer".to_owned());
        let result = match plan.fault {
            Fault::Pass => request().await,
            Fault::Before(hook) => {
                hook.run().await;
                request().await
            }
            Fault::LoseRequest => Err(lost()),
            Fault::LoseResponse => request().await.and_then(|_| Err(lost())),
            Fault::LoseResponseAfter(hook) => {
                let result = request().await;
                hook.run().await;
                result.and_then(|_| Err(lost()))
            }
            Fault::LateRequest(delay) => {
                late(delay);
                Err(lost())
            }
            Fault::Conflict => Err(match key {
                Some(key) => ControlError::Conflict(key.clone()),
                None => ControlError::Unavailable("injected conflict".to_owned()),
            }),
            Fault::Unavailable => Err(ControlError::Unavailable("injected outage".to_owned())),
            Fault::Fail => Err(ControlError::Io(io::Error::other("injected failure"))),
        };
        tokio::time::sleep(plan.after).await;
        result
    }
}

impl<S: ControlStore> ControlStore for FaultyStore<S> {
    type Changes = S::Changes;

    async fn get(&self, key: &RegisterKey) -> Result<Option<Versioned>, ControlError> {
        self.inject(None, || self.inner.get(key), drop).await
    }

    async fn put_if(
        &self,
        key: &RegisterKey,
        expected: Expected,
        value: Bytes,
    ) -> Result<PutOutcome, ControlError> {
        let late = |delay| {
            let (inner, key, expected, value) = (
                self.inner.clone(),
                key.clone(),
                expected.clone(),
                value.clone(),
            );
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                // Nobody waits for the answer of a timed-out request.
                let _ = inner.put_if(&key, expected, value).await;
            });
        };
        self.inject(
            Some(key),
            || self.inner.put_if(key, expected.clone(), value.clone()),
            late,
        )
        .await
    }

    async fn list(&self, prefix: &KeyPrefix) -> Result<Vec<(RegisterKey, Version)>, ControlError> {
        self.inject(None, || self.inner.list(prefix), drop).await
    }

    async fn changes(&self, after: Generation) -> Result<Self::Changes, ControlError> {
        self.inner.changes(after).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryControlStore;

    fn value(text: &'static str) -> Bytes {
        Bytes::from_static(text.as_bytes())
    }

    #[tokio::test(start_paused = true)]
    async fn scripted_faults_apply_in_order() {
        let store = FaultyStore::new(MemoryControlStore::new());
        let key = RegisterKey::new("k").unwrap();
        store.script([
            Fault::LoseRequest,
            Fault::LoseResponse,
            Fault::Conflict,
            Fault::Unavailable,
            Fault::Fail,
            Fault::Conflict,
            Fault::LateRequest(Duration::from_secs(1)),
        ]);
        let put = |text| store.put_if(&key, Expected::Absent, value(text));
        assert!(matches!(
            put("a").await,
            Err(ControlError::Indeterminate(_))
        ));
        assert!(store.inner().get(&key).await.unwrap().is_none());
        assert!(matches!(
            put("b").await,
            Err(ControlError::Indeterminate(_))
        ));
        let stored = store.inner().get(&key).await.unwrap().unwrap();
        assert_eq!(stored.value, value("b"));
        assert!(matches!(put("c").await, Err(ControlError::Conflict(_))));
        assert!(matches!(put("c").await, Err(ControlError::Unavailable(_))));
        assert!(matches!(put("c").await, Err(ControlError::Io(_))));
        assert!(matches!(
            store.get(&key).await,
            Err(ControlError::Unavailable(_))
        ));
        let next = Expected::Version(stored.version);
        let late = store.put_if(&key, next, value("late")).await;
        assert!(matches!(late, Err(ControlError::Indeterminate(_))));
        assert_eq!(store.get(&key).await.unwrap().unwrap().value, value("b"));
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(store.get(&key).await.unwrap().unwrap().value, value("late"));
        let stats = store.stats();
        assert_eq!(
            stats,
            FaultStats {
                requests: 9,
                lost_requests: 1,
                lost_responses: 1,
                late_requests: 1,
                conflicts: 2,
                unavailable: 1,
            }
        );
        assert_eq!(format!("{:?}", Fault::before(|| async {})), "Before(Hook)");
    }

    #[tokio::test(start_paused = true)]
    async fn random_faults_are_seeded() {
        let rates = FaultRates {
            lose_request: 0.1,
            lose_response: 0.1,
            late_request: 0.1,
            conflict: 0.1,
            unavailable: 0.1,
            max_delay: Duration::from_millis(5),
        };
        let run = |seed| async move {
            let store = FaultyStore::seeded(MemoryControlStore::new(), seed, rates);
            let mut outcomes = Vec::new();
            for n in 0..200 {
                let key = RegisterKey::new(format!("k{n}")).unwrap();
                let result = store.put_if(&key, Expected::Absent, value("x")).await;
                outcomes.push(format!("{result:?}"));
                let listed = store.list(&KeyPrefix::root()).await.map(|keys| keys.len());
                outcomes.push(format!("{listed:?}"));
            }
            (outcomes, store.stats())
        };
        let (first, stats) = run(7).await;
        assert_eq!(run(7).await.0, first);
        assert_ne!(run(8).await.0, first);
        assert!(stats.lost_requests > 0 && stats.lost_responses > 0 && stats.late_requests > 0);
        assert!(stats.conflicts > 0 && stats.unavailable > 0);
        let store = FaultyStore::seeded(MemoryControlStore::new(), 1, rates);
        store.set_rates(FaultRates::default());
        let key = RegisterKey::new("k").unwrap();
        assert!(
            store
                .put_if(&key, Expected::Absent, value("x"))
                .await
                .is_ok()
        );
    }
}
