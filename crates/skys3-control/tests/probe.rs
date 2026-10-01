//! The startup probe (design §6.1): it passes on stores that honor
//! conditional writes and deletes and read their own writes, also under
//! delays, conflicts, and lost responses, and refuses simulated stores with
//! ignored or rejected preconditions or stale reads.

use std::time::Duration;

use bytes::Bytes;
use skys3_control::faults::{FaultRates, FaultyStore};
use skys3_control::{
    ControlError, ControlProbe, ControlStore, DeleteOutcome, Expected, KeyPrefix,
    MemoryControlStore, ProbeFailure, PutOutcome, RegisterKey, RetryPolicy, S3ControlStore,
    S3StoreConfig, Version, Versioned,
};
use skys3_sim::SimS3;
use skys3_sim::s3::{ConditionalSupport, Conditionals, SimS3Config, SimS3Faults};
use skys3_types::Generation;

const WRITERS: usize = 3;

fn s3(config: SimS3Config, faults: SimS3Faults) -> S3ControlStore<SimS3> {
    let objects = SimS3::new(11, config);
    objects.set_faults(faults);
    let config = S3StoreConfig {
        prefix: "skys3-prod-a/".to_owned(),
        poll_interval: Duration::from_secs(30),
    };
    S3ControlStore::new(objects, config).unwrap()
}

fn with(conditionals: Conditionals) -> S3ControlStore<SimS3> {
    let config = SimS3Config {
        conditionals,
        ..SimS3Config::default()
    };
    s3(config, SimS3Faults::NONE)
}

/// One handle per writer, as racing nodes have.
fn writers<S: Clone>(store: &S) -> Vec<S> {
    vec![store.clone(); WRITERS]
}

/// Enough patience for a store with faults on many requests.
fn patient() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 200,
        initial_backoff: Duration::from_millis(2),
        max_backoff: Duration::from_millis(50),
    }
}

#[tokio::test(start_paused = true)]
async fn an_aws_like_store_passes_and_is_left_clean() {
    for versioning in [false, true] {
        let store = s3(
            SimS3Config {
                versioning,
                ..SimS3Config::default()
            },
            SimS3Faults::NONE,
        );
        ControlProbe::new(1).run(&writers(&store)).await.unwrap();
        assert!(
            store.objects().keys().is_empty(),
            "{:?}",
            store.objects().keys()
        );
        // Three writes per round, plus each loser's read, plus the deletes.
        assert!(store.objects().stats().requests > 500);
    }
}

#[tokio::test(start_paused = true)]
async fn racing_writers_pass_through_conflicts_and_lost_responses() {
    let faults = SimS3Faults {
        max_delay: Duration::from_millis(5),
        internal_error_probability: 0.03,
        slow_down_probability: 0.03,
        lost_request_probability: 0.03,
        lost_response_probability: 0.05,
        ..SimS3Faults::NONE
    };
    let store = s3(SimS3Config::default(), faults);
    let probe = ControlProbe::new(2).with_policy(patient());
    probe.run(&writers(&store)).await.unwrap();
    let stats = store.objects().stats();
    assert!(stats.conflicts > 0, "{stats:?}");
    assert!(stats.lost_responses > 0, "{stats:?}");
    store.objects().set_faults(SimS3Faults::NONE);
    assert!(store.objects().keys().is_empty());
}

#[tokio::test(start_paused = true)]
async fn the_probe_runs_on_any_backend_with_faults_per_writer() {
    let memory = MemoryControlStore::new();
    let rates = FaultRates {
        lose_request: 0.04,
        lose_response: 0.06,
        late_request: 0.03,
        conflict: 0.04,
        unavailable: 0.04,
        max_delay: Duration::from_millis(3),
    };
    let handles: Vec<_> = (0..4)
        .map(|seed| FaultyStore::seeded(memory.clone(), seed, rates))
        .collect();
    let probe = ControlProbe::new(3).with_policy(patient());
    probe.run(&handles).await.unwrap();
    // Late requests are all fenced by now.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let lost: u64 = handles.iter().map(|h| h.stats().lost_responses).sum();
    assert!(lost > 0);
    assert!(memory.registers().is_empty(), "{:?}", memory.registers());
}

#[tokio::test(start_paused = true)]
async fn ignored_conditional_writes_are_refused() {
    let store = with(Conditionals::all(ConditionalSupport::Ignored));
    let error = ControlProbe::new(4)
        .run(&writers(&store))
        .await
        .unwrap_err();
    assert!(
        matches!(error.failure, ProbeFailure::Winners(n) if n == WRITERS),
        "{error}"
    );
    assert!(error.refuses_store());
    assert!(
        error.step.starts_with("round 0 on probe/"),
        "{}",
        error.step
    );
    assert!(error.leftovers.is_empty());
    assert!(store.objects().keys().is_empty());
}

#[tokio::test(start_paused = true)]
async fn rejected_conditional_writes_are_refused() {
    let store = with(Conditionals::all(ConditionalSupport::Rejected));
    let error = ControlProbe::new(5)
        .run(&writers(&store))
        .await
        .unwrap_err();
    assert!(
        matches!(
            error.failure,
            ProbeFailure::Store(ControlError::Rejected(_))
        ),
        "{error}"
    );
    assert!(error.refuses_store());
}

#[tokio::test(start_paused = true)]
async fn ignored_conditional_deletes_are_refused() {
    // R2 as design §7.2 describes it: PutObject honors preconditions,
    // DeleteObject ignores them.
    let store = with(Conditionals::R2);
    let error = ControlProbe::new(6)
        .run(&writers(&store))
        .await
        .unwrap_err();
    assert!(
        matches!(error.failure, ProbeFailure::DeleteIgnored),
        "{error}"
    );
    assert!(error.refuses_store());
    assert!(
        error.step.starts_with("the deletion of probe/"),
        "{}",
        error.step
    );
    assert!(store.objects().keys().is_empty());
}

#[tokio::test(start_paused = true)]
async fn rejected_conditional_deletes_are_refused_and_leftovers_reported() {
    let store = with(Conditionals {
        delete_object: ConditionalSupport::Rejected,
        ..Conditionals::AWS_S3
    });
    let probe = ControlProbe::new(7).with_rounds(8, 2);
    let error = probe.run(&writers(&store)).await.unwrap_err();
    assert!(
        matches!(
            error.failure,
            ProbeFailure::Store(ControlError::Rejected(_))
        ),
        "{error}"
    );
    assert!(error.refuses_store());
    let prefix = probe.scratch_prefix();
    let expected: Vec<_> = (0..2)
        .map(|n| RegisterKey::new(format!("{prefix}{n}.json")).unwrap())
        .collect();
    assert_eq!(error.leftovers, expected);
    let message = error.to_string();
    assert!(
        message.contains(&format!("left behind: {}, {}", expected[0], expected[1])),
        "{message}"
    );
    assert_eq!(store.objects().keys().len(), 2);
}

#[tokio::test(start_paused = true)]
async fn stale_reads_are_refused() {
    let faults = SimS3Faults {
        stale_read_probability: 0.3,
        ..SimS3Faults::NONE
    };
    let store = s3(SimS3Config::default(), faults);
    let error = ControlProbe::new(8)
        .run(&writers(&store))
        .await
        .unwrap_err();
    assert!(
        matches!(
            error.failure,
            ProbeFailure::StaleRead { .. } | ProbeFailure::DeletedStillRead
        ),
        "{error}"
    );
    assert!(error.refuses_store());
    // The cleanup reads through the stale reads.
    store.objects().set_faults(SimS3Faults::NONE);
    assert!(store.objects().keys().is_empty());
}

#[tokio::test(start_paused = true)]
async fn an_unreachable_store_is_not_refused() {
    let store = s3(SimS3Config::default(), SimS3Faults::OUTAGE);
    let probe = ControlProbe::new(9).with_policy(RetryPolicy {
        max_attempts: 3,
        ..RetryPolicy::default()
    });
    let error = probe.run(&writers(&store)).await.unwrap_err();
    assert!(
        matches!(
            error.failure,
            ProbeFailure::Store(ControlError::RetriesExhausted { .. })
        ),
        "{error}"
    );
    assert!(!error.refuses_store(), "an outage refused the store");
    assert_eq!(error.leftovers.len(), ControlProbe::KEYS as usize);
}

/// A store that refuses every conditional write or every conditional
/// delete with a failed precondition, and otherwise behaves.
#[derive(Debug, Clone)]
struct Refuses {
    inner: MemoryControlStore,
    writes: bool,
}

impl ControlStore for Refuses {
    type Changes = <MemoryControlStore as ControlStore>::Changes;

    async fn get(&self, key: &RegisterKey) -> Result<Option<Versioned>, ControlError> {
        self.inner.get(key).await
    }

    async fn put_if(
        &self,
        key: &RegisterKey,
        expected: Expected,
        value: Bytes,
    ) -> Result<PutOutcome, ControlError> {
        if self.writes {
            return Ok(PutOutcome::PreconditionFailed);
        }
        self.inner.put_if(key, expected, value).await
    }

    async fn delete_if(
        &self,
        key: &RegisterKey,
        expected: &Version,
    ) -> Result<DeleteOutcome, ControlError> {
        if !self.writes {
            return Ok(DeleteOutcome::PreconditionFailed);
        }
        self.inner.delete_if(key, expected).await
    }

    async fn list(&self, prefix: &KeyPrefix) -> Result<Vec<(RegisterKey, Version)>, ControlError> {
        self.inner.list(prefix).await
    }

    async fn changes(&self, after: Generation) -> Result<Self::Changes, ControlError> {
        self.inner.changes(after).await
    }
}

#[tokio::test(start_paused = true)]
async fn a_round_without_a_winner_is_refused() {
    let store = Refuses {
        inner: MemoryControlStore::new(),
        writes: true,
    };
    let error = ControlProbe::new(10)
        .run(&writers(&store))
        .await
        .unwrap_err();
    assert!(matches!(error.failure, ProbeFailure::Winners(0)), "{error}");
    assert_eq!(
        error.to_string(),
        format!(
            "control-store probe failed at {}: 0 racing conditional writers won, not exactly one",
            error.step
        )
    );
}

#[tokio::test(start_paused = true)]
async fn a_store_that_refuses_deletes_is_refused_and_its_registers_reported() {
    let store = Refuses {
        inner: MemoryControlStore::new(),
        writes: false,
    };
    let probe = ControlProbe::new(12).with_rounds(4, 2);
    let error = probe.run(&writers(&store)).await.unwrap_err();
    assert!(
        matches!(error.failure, ProbeFailure::DeleteRefused),
        "{error}"
    );
    assert!(error.refuses_store());
    assert_eq!(error.leftovers.len(), 2);
    assert_eq!(store.inner.registers().len(), 2);
}

#[test]
fn probes_use_fresh_nonces_and_scratch_keys() {
    let (a, b) = (
        ControlProbe::with_fresh_nonce(),
        ControlProbe::with_fresh_nonce(),
    );
    assert_ne!(a.scratch_prefix(), b.scratch_prefix());
    let probe = ControlProbe::new(u64::MAX).with_rounds(0, 0);
    assert_eq!(probe.scratch_prefix().as_str(), "probe/ffffffffffffffff/");
    assert_eq!(probe, ControlProbe::new(u64::MAX).with_rounds(1, 1));
}

#[tokio::test]
#[should_panic(expected = "at least two writers")]
async fn a_race_needs_two_writers() {
    let _ = ControlProbe::new(0).run(&[MemoryControlStore::new()]).await;
}
