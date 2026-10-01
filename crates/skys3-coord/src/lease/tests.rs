use std::time::Duration;

use bytes::Bytes;
use skys3_control::faults::{Fault, FaultRates, FaultyStore};
use skys3_control::{MemoryControlStore, PutOutcome, RegisterKey, Versioned};
use skys3_io::MonotonicClock;
use skys3_types::RegisterDocument;

use super::*;

/// Every wait in these tests, in paused (virtual) time.
const WAIT: Duration = Duration::from_secs(600);

fn config() -> LeaseConfig {
    LeaseConfig::new(Duration::from_secs(9), 0.01)
        .unwrap()
        .with_retry(RetryPolicy {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(100),
        })
}

fn node(n: u8) -> NodeId {
    NodeId::new(format!("node-{n}")).unwrap()
}

/// One clock for every elector of a test, so their readings compare.
fn clock() -> Arc<dyn Clock> {
    Arc::new(MonotonicClock::new())
}

fn elector<S: ControlStore>(store: S, n: u8, clock: &Arc<dyn Clock>) -> Elector<S> {
    let clock = Arc::clone(clock);
    Elector::new(
        store,
        node(n),
        clock,
        config(),
        ProposalIds::seeded(n.into()),
    )
}

async fn lease<S: ControlStore>(store: &S) -> Option<Versioned<CoordinatorLease>> {
    read(store, &TypedKey::coordinator_lease()).await.unwrap()
}

/// Runs rounds of `elector` until it is coordinator, and returns when.
async fn until_coordinator<S: ControlStore>(elector: &mut Elector<S>) -> MonoTime {
    tokio::time::timeout(WAIT, async {
        loop {
            let wake = elector.round().await;
            let now = elector.clock.now();
            if elector.leadership().is_coordinator_at(now) {
                return now;
            }
            elector.clock.sleep_until(wake).await;
        }
    })
    .await
    .expect("the elector became coordinator")
}

/// Runs rounds of `elector` for `duration`, checking after each that it is
/// not coordinator.
async fn never_coordinator<S: ControlStore>(elector: &mut Elector<S>, duration: Duration) {
    let end = elector.clock.now().saturating_add(duration);
    while elector.clock.now() < end {
        let wake = elector.round().await;
        assert!(
            !elector.leadership().is_coordinator_at(elector.clock.now()),
            "{:?}",
            elector.leadership()
        );
        elector.clock.sleep_until(wake.min(end)).await;
    }
}

#[test]
fn timings_follow_the_lease_and_the_drift_bound() {
    let config = LeaseConfig::new(Duration::from_secs(9), 0.01).unwrap();
    assert_eq!(config.lease(), Duration::from_secs(9));
    assert!((config.drift() - 0.01).abs() < f64::EPSILON);
    assert_eq!(config.renew_interval(), Duration::from_secs(3));
    assert_eq!(config.takeover_after(), Duration::from_millis(9090));
    assert_eq!(config.tenure(), Duration::from_millis(8910));
    assert_eq!(config.retry, RetryPolicy::default());
    assert_eq!(
        LeaseConfig::new(Duration::ZERO, 0.01),
        Err(LeaseConfigError::ZeroLease)
    );
    for drift in [-0.1, 1.0, f64::NAN] {
        let error = LeaseConfig::new(Duration::from_secs(1), drift).unwrap_err();
        assert!(matches!(error, LeaseConfigError::Drift(_)), "{error}");
    }
    assert_eq!(
        LeaseConfigError::Drift(1.0).to_string(),
        "the clock drift bound must be at least 0 and less than 1, not 1"
    );

    let text = "[cluster]\ncluster_id = \"prod\"\n\
                [control_store]\netcd_endpoints = [\"https://etcd:2379\"]\n";
    let config: skys3_config::Config = text.parse().unwrap();
    let lease = LeaseConfig::from_config(&config).unwrap();
    assert_eq!(lease.lease(), Duration::from_secs(10));
    assert!((lease.drift() - 0.01).abs() < f64::EPSILON);
}

#[test]
fn leadership_ends_at_its_tenure() {
    let at = |nanos| MonoTime::from_nanos(nanos);
    let leader = Leadership::Coordinator {
        version: Version::new("1"),
        until: at(10),
    };
    assert!(leader.is_coordinator_at(at(9)));
    assert!(!leader.is_coordinator_at(at(10)));
    assert_eq!(leader.until(), Some(at(10)));
    let follower = Leadership::Follower { holder: None };
    assert!(!follower.is_coordinator_at(at(0)));
    assert_eq!(follower.until(), None);
}

#[tokio::test(start_paused = true)]
async fn the_first_candidate_takes_a_free_lease_and_renews_it_by_if_match() {
    let clock = clock();
    let store = FaultyStore::new(MemoryControlStore::new());
    let mut first = elector(store.clone(), 1, &clock);
    let mut changes = first.subscribe();
    let renew_at = first.round().await;
    let taken = lease(&store).await.unwrap();
    assert_eq!(taken.value.holder, node(1));
    let Leadership::Coordinator { version, until } = first.leadership() else {
        panic!("{:?}", first.leadership());
    };
    assert_eq!(version, taken.version);
    let start = first.clock.now();
    assert!(until > start && until <= start.saturating_add(config().tenure()));
    assert!(renew_at <= start.saturating_add(config().renew_interval()));
    assert!(changes.has_changed().unwrap());
    changes.mark_unchanged();

    // Early rounds send nothing.
    let requests = store.requests();
    assert_eq!(first.round().await, renew_at);
    assert_eq!(store.requests(), requests);

    // Each renewal writes a fresh proposal under If-Match, so the version
    // changes although the holder does not.
    let mut versions = vec![taken.version];
    for _ in 0..3 {
        let wake = first.round().await;
        first.clock.sleep_until(wake).await;
        first.round().await;
        let renewed = lease(&store).await.unwrap();
        assert_eq!(renewed.value.holder, node(1));
        assert!(!versions.contains(&renewed.version));
        versions.push(renewed.version);
        assert!(first.leadership().is_coordinator_at(first.clock.now()));
        assert!(changes.has_changed().unwrap());
        changes.mark_unchanged();
    }
}

#[tokio::test(start_paused = true)]
async fn a_candidate_waits_out_an_unchanged_lease_and_never_a_renewed_one() {
    let clock = clock();
    let store = MemoryControlStore::new();
    let mut first = elector(store.clone(), 1, &clock);
    let mut second = elector(store.clone(), 2, &clock);
    first.round().await;
    let leadership = first.subscribe();
    let running = tokio::spawn(first.run());

    // While the holder renews, the candidate never takes over.
    never_coordinator(&mut second, config().lease() * 5).await;
    assert_eq!(
        second.leadership(),
        Leadership::Follower {
            holder: Some(node(1))
        }
    );

    // Once it stops, the candidate takes over after observing the last
    // version for longer than `coordinator_lease × (1+ρ)`, and not before
    // the holder's tenure has ended.
    running.abort();
    let stopped = second.clock.now();
    let last_tenure = leadership.borrow().until().unwrap();
    let taken = until_coordinator(&mut second).await;
    // The last renewal was written at most one interval before the stop.
    let earliest = stopped
        .saturating_add(config().takeover_after())
        .saturating_duration_since(MonoTime::ZERO)
        - config().renew_interval();
    let taken_at = taken.saturating_duration_since(MonoTime::ZERO);
    assert!(taken_at >= earliest, "{taken_at:?} < {earliest:?}");
    assert!(
        taken
            <= stopped
                .saturating_add(config().takeover_after())
                .saturating_add(config().renew_interval())
    );
    assert!(last_tenure < taken);
    assert_eq!(lease(&store).await.unwrap().value.holder, node(2));
}

#[tokio::test(start_paused = true)]
async fn a_holder_that_cannot_renew_steps_down_before_anyone_takes_over() {
    let clock = clock();
    let memory = MemoryControlStore::new();
    let cut = FaultyStore::new(memory.clone());
    let mut first = elector(cut.clone(), 1, &clock);
    let mut second = elector(memory.clone(), 2, &clock);
    first.round().await;
    let leadership = first.subscribe();
    cut.set_rates(FaultRates {
        unavailable: 1.0,
        ..FaultRates::default()
    });
    let running = tokio::spawn(first.run());
    let taken = until_coordinator(&mut second).await;
    // The same clock times both electors, so their readings compare.
    assert!(!leadership.borrow().is_coordinator_at(clock.now()));
    assert!(matches!(*leadership.borrow(), Leadership::Follower { .. }));
    assert!(clock.now() >= taken);

    // Once the store answers again, the old holder follows the new one.
    cut.set_rates(FaultRates::default());
    clock.sleep(config().lease()).await;
    assert_eq!(
        *leadership.borrow(),
        Leadership::Follower {
            holder: Some(node(2))
        }
    );
    running.abort();
}

#[tokio::test(start_paused = true)]
async fn a_lapsed_holder_takes_its_lease_back_once_the_store_answers() {
    let clock = clock();
    let store = FaultyStore::new(MemoryControlStore::new());
    let mut first = elector(store.clone(), 1, &clock);
    first.round().await;
    let held = lease(&store).await.unwrap().version;
    store.set_rates(FaultRates {
        unavailable: 1.0,
        ..FaultRates::default()
    });
    let end = clock.now().saturating_add(config().lease());
    while clock.now() < end {
        let wake = first.round().await;
        clock.sleep_until(wake.min(end)).await;
    }
    first.round().await;
    assert_eq!(
        first.leadership(),
        Leadership::Follower {
            holder: Some(node(1))
        }
    );

    // Nobody took over, so its next renewal lands, without waiting out
    // the lease as a stranger's.
    store.set_rates(FaultRates::default());
    let lapsed = clock.now();
    let back = until_coordinator(&mut first).await;
    assert!(back.saturating_duration_since(lapsed) <= config().renew_interval());
    assert_ne!(lease(&store).await.unwrap().version, held);
}

#[tokio::test(start_paused = true)]
async fn a_rejected_renewal_steps_down_at_once() {
    let clock = clock();
    let store = MemoryControlStore::new();
    let mut first = elector(store.clone(), 1, &clock);
    first.round().await;
    // Another node writes the lease over the holder's version, as one
    // that took over would.
    let current = lease(&store).await.unwrap();
    let theirs = CoordinatorLease {
        holder: node(2),
        proposal_id: ProposalIds::seeded(99).next_id(),
    };
    let written = store
        .put_if(
            &RegisterKey::coordinator_lease(),
            Expected::Version(current.version),
            Bytes::from(theirs.to_json().unwrap()),
        )
        .await
        .unwrap();
    assert!(matches!(written, PutOutcome::Written(_)));

    let wake = first.round().await;
    first.clock.sleep_until(wake).await;
    let next = first.round().await;
    assert_eq!(first.leadership(), Leadership::Follower { holder: None });
    assert_eq!(next, first.clock.now());
    first.round().await;
    assert_eq!(
        first.leadership(),
        Leadership::Follower {
            holder: Some(node(2))
        }
    );
}

#[tokio::test(start_paused = true)]
async fn a_lease_written_with_a_lost_answer_is_adopted_on_the_next_read() {
    let clock = clock();
    let store = FaultyStore::new(MemoryControlStore::new());
    let mut first = elector(store.clone(), 1, &clock);
    first.config.retry.max_attempts = 1;
    // The read passes; the write lands and its answer is lost.
    store.script([Fault::Pass, Fault::LoseResponse]);
    let sent = first.clock.now();
    let retry_at = first.round().await;
    assert_eq!(first.leadership(), Leadership::Follower { holder: None });
    assert_eq!(lease(&store).await.unwrap().value.holder, node(1));

    // The next round finds its own proposal and holds the lease, with the
    // tenure counted from the first attempt.
    first.clock.sleep_until(retry_at).await;
    first.round().await;
    assert_eq!(
        first.leadership().until(),
        Some(sent.saturating_add(config().tenure()))
    );
}

#[tokio::test(start_paused = true)]
async fn a_late_adopted_lease_is_renewed_before_the_node_acts() {
    let clock = clock();
    let store = FaultyStore::new(MemoryControlStore::new());
    let mut first = elector(store.clone(), 1, &clock);
    first.config.retry.max_attempts = 1;
    store.script([Fault::Pass, Fault::LoseResponse]);
    first.round().await;
    // The node learns of the write only after its tenure would have ended.
    first.clock.sleep(config().lease() * 2).await;
    let renew_at = first.round().await;
    assert_eq!(
        first.leadership(),
        Leadership::Follower {
            holder: Some(node(1))
        }
    );
    assert!(renew_at <= first.clock.now());
    first.round().await;
    assert!(first.leadership().is_coordinator_at(first.clock.now()));
}

#[tokio::test(start_paused = true)]
async fn of_two_candidates_one_takes_over_and_the_other_follows_it() {
    let clock = clock();
    let store = MemoryControlStore::new();
    let mut old = elector(store.clone(), 1, &clock);
    old.round().await;
    drop(old);
    let mut second = elector(store.clone(), 2, &clock);
    let mut third = elector(store.clone(), 3, &clock);
    // Both observe the same version now, so both are due together.
    second.round().await;
    third.round().await;
    clock
        .sleep(config().takeover_after() + Duration::from_millis(1))
        .await;
    second.round().await;
    third.round().await;
    let winner = lease(&store).await.unwrap().value.holder;
    let (won, lost) = if winner == node(2) {
        (&second, &mut third)
    } else {
        (&third, &mut second)
    };
    assert!(won.leadership().is_coordinator_at(won.clock.now()));
    assert!(!lost.leadership().is_coordinator_at(lost.clock.now()));
    lost.round().await;
    assert_eq!(
        lost.leadership(),
        Leadership::Follower {
            holder: Some(winner)
        }
    );
}

#[tokio::test(start_paused = true)]
async fn an_unreadable_lease_is_read_again_later() {
    let clock = clock();
    let store = FaultyStore::new(MemoryControlStore::new());
    let mut first = elector(store.clone(), 1, &clock);
    store.script([Fault::Unavailable]);
    let start = first.clock.now();
    let retry_at = first.round().await;
    assert_eq!(
        retry_at,
        start.saturating_add(config().renew_interval() / 4)
    );
    assert_eq!(first.leadership(), Leadership::Follower { holder: None });

    // A takeover that gets no answer is retried later too.
    store.script(
        [Fault::Pass]
            .into_iter()
            .chain(std::iter::repeat_n(Fault::Unavailable, 3)),
    );
    first.clock.sleep_until(retry_at).await;
    let retry_at = first.round().await;
    assert!(retry_at > first.clock.now());
    assert!(lease(&store).await.is_none());
    first.clock.sleep_until(retry_at).await;
    first.round().await;
    assert!(first.leadership().is_coordinator_at(first.clock.now()));
}
