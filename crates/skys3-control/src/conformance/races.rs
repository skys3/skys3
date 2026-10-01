//! The checks that race several handles to one store: one writer, node, or
//! change stream per handle.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use skys3_types::{ClusterId, Generation, ProposalId};
use tokio::task::JoinSet;

use super::history::{self, Event, Op, Tick, WriteResult};
use super::{counter_of, key, next, value};
use crate::cluster::{Bootstrap, FIRST_GENERATION, bootstrap, bump_generation, read_cluster};
use crate::faults::{FaultRates, FaultStats, FaultyStore};
use crate::key::{KeyPrefix, RegisterKey};
use crate::probe::ControlProbe;
use crate::propose::{
    ProposalIds, ProposalOutcome, RetryPolicy, get_with_retries, proposal_id_of, propose,
};
use crate::store::{ControlError, ControlStore, Expected, PutOutcome, Version};

/// The random faults of [`increments_survive_random_faults`] and
/// [`the_probe_passes_under_faults`]: about one request in five.
const FAULTS: FaultRates = FaultRates {
    lose_request: 0.04,
    lose_response: 0.05,
    late_request: 0.03,
    conflict: 0.05,
    unavailable: 0.03,
    max_delay: Duration::from_millis(3),
};

/// Longer than any late request [`FAULTS`] delays (four times its
/// `max_delay`) takes to land on a remote store.
const LATE_REQUESTS: Duration = Duration::from_secs(2);

/// Enough attempts for a request to get through [`FAULTS`] on every
/// attempt's request and re-read.
fn patient() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 100,
        initial_backoff: Duration::from_millis(2),
        max_backoff: Duration::from_millis(50),
    }
}

/// Each handle with its own seeded random faults.
fn faulty<S: ControlStore>(handles: &[S], seed: u64) -> Vec<FaultyStore<S>> {
    handles
        .iter()
        .zip(seed..)
        .map(|(handle, seed)| FaultyStore::seeded(handle.clone(), seed, FAULTS))
        .collect()
}

/// The faults injected across `handles`, which must include some.
fn injected<S: ControlStore>(handles: &[FaultyStore<S>]) -> FaultStats {
    let total = handles
        .iter()
        .map(FaultyStore::stats)
        .fold(FaultStats::default(), |a, b| FaultStats {
            requests: a.requests + b.requests,
            lost_requests: a.lost_requests + b.lost_requests,
            lost_responses: a.lost_responses + b.lost_responses,
            late_requests: a.late_requests + b.late_requests,
            conflicts: a.conflicts + b.conflicts,
            unavailable: a.unavailable + b.unavailable,
        });
    assert!(
        total.lost_requests + total.lost_responses + total.late_requests > 0,
        "no fault was injected: {total:?}"
    );
    total
}

/// One proposal per handle races to create one register: exactly one is
/// accepted, and the register holds it.
pub async fn racing_creates_have_one_winner<S: ControlStore>(handles: &[S]) {
    let register = key("coordinator.lease");
    let mut tasks = JoinSet::new();
    for (writer, store) in (0..).zip(handles) {
        let (store, register) = (store.clone(), register.clone());
        tasks.spawn(async move {
            let proposal = ProposalIds::seeded(100 + writer).next_id();
            let outcome = propose(
                &store,
                &register,
                Expected::Absent,
                value(&proposal, writer),
                &proposal,
                &RetryPolicy::default(),
            )
            .await;
            (proposal, outcome.unwrap())
        });
    }
    let winners: Vec<_> = tasks
        .join_all()
        .await
        .into_iter()
        .filter(|(_, outcome)| matches!(outcome, ProposalOutcome::Accepted(_)))
        .collect();
    assert_eq!(
        winners.len(),
        1,
        "racing creates had {} winners",
        winners.len()
    );
    let stored = handles[0]
        .get(&register)
        .await
        .unwrap()
        .expect("the register exists");
    assert_eq!(proposal_id_of(&stored.value).as_ref(), Some(&winners[0].0));
}

/// One writer per handle increments a counter register `increments` times
/// by read and `put_if`: no increment is lost or applied twice.
pub async fn increments_are_linearizable<S: ControlStore>(handles: &[S], increments: u64) {
    let register = key("shards/b-1/0.json");
    let created = handles[0].put_if(
        &register,
        Expected::Absent,
        value(&ProposalId::from_u128(0), 0),
    );
    assert!(matches!(created.await.unwrap(), PutOutcome::Written(_)));
    let mut tasks = JoinSet::new();
    for (writer, store) in (0..).zip(handles) {
        let (store, register) = (store.clone(), register.clone());
        tasks.spawn(async move {
            let mut ids = ProposalIds::seeded(200 + writer);
            let mut applied = Vec::new();
            while (applied.len() as u64) < increments {
                let current = store
                    .get(&register)
                    .await
                    .unwrap()
                    .expect("the counter exists");
                // Let other writers read the same version.
                tokio::task::yield_now().await;
                let next = counter_of(&current) + 1;
                let proposal = ids.next_id();
                let outcome = propose(
                    &store,
                    &register,
                    Expected::Version(current.version),
                    value(&proposal, next),
                    &proposal,
                    &RetryPolicy::default(),
                )
                .await;
                if let ProposalOutcome::Accepted(_) = outcome.unwrap() {
                    applied.push(next);
                }
                tokio::task::yield_now().await;
            }
            applied
        });
    }
    let applied: Vec<u64> = tasks.join_all().await.into_iter().flatten().collect();
    let distinct: BTreeSet<u64> = applied.iter().copied().collect();
    let total = handles.len() as u64 * increments;
    assert_eq!(
        distinct.len() as u64,
        total,
        "an increment was applied twice: {applied:?}"
    );
    assert_eq!(
        distinct,
        (1..=total).collect(),
        "increments are not contiguous"
    );
    let current = handles[0]
        .get(&register)
        .await
        .unwrap()
        .expect("the counter exists");
    assert_eq!(counter_of(&current), total, "the counter lost an increment");
}

/// The registers [`histories_are_linearizable`] spreads its operations
/// over.
const HISTORY_REGISTERS: usize = 3;

/// One writer per handle runs `operations` random operations on a few
/// registers, `get`s and `put_if`s at the latest version it saw, an older
/// one, or `Absent`, recording when each was called and returned. Each
/// register's history, closed by a read after every operation returned,
/// must be linearizable: its successful writes form one chain of versions,
/// and every read, write, and failed precondition fits that chain in real
/// time. An unanswered write counts if a read saw its value.
///
/// # Panics
///
/// If a history is not linearizable, or a request fails with an error
/// that is not retried.
pub async fn histories_are_linearizable<S: ControlStore>(handles: &[S], operations: u64) {
    let clock = Arc::new(AtomicU64::new(0));
    let tick = |clock: &AtomicU64| -> Tick { clock.fetch_add(1, Ordering::SeqCst) };
    let mut tasks = JoinSet::new();
    for (writer, store) in (0..).zip(handles) {
        let (store, clock) = (store.clone(), clock.clone());
        tasks.spawn(async move {
            let mut rng = SmallRng::seed_from_u64(400 + writer);
            let mut ids = ProposalIds::seeded(400 + writer);
            // The versions this writer has seen, per register, in order.
            let mut seen: Vec<Vec<Version>> = vec![Vec::new(); HISTORY_REGISTERS];
            let mut events = Vec::new();
            for _ in 0..operations {
                let register = rng.random_range(0..HISTORY_REGISTERS);
                let key = history_key(register);
                let seen = &mut seen[register];
                let roll: f64 = rng.random();
                let call = tick(&clock);
                let op = if roll < 0.35 {
                    match store.get(&key).await {
                        Ok(read) => {
                            seen.extend(read.as_ref().map(|r| r.version.clone()));
                            Op::Read(read)
                        }
                        Err(error) if error.is_retryable() => continue,
                        Err(error) => panic!("reading {key} failed: {error}"),
                    }
                } else {
                    let base = if roll < 0.85 {
                        seen.last()
                    } else {
                        // An older version, or none.
                        seen.get(rng.random_range(0..=seen.len()))
                    };
                    let expected = base.cloned().map_or(Expected::Absent, Expected::Version);
                    let value = value(&ids.next_id(), writer);
                    let answer = store.put_if(&key, expected.clone(), value.clone()).await;
                    let result = match answer {
                        Ok(PutOutcome::Written(version)) => {
                            seen.push(version.clone());
                            WriteResult::Written(version)
                        }
                        Ok(PutOutcome::PreconditionFailed) => WriteResult::PreconditionFailed,
                        Err(error) if error.may_have_applied() => WriteResult::Unknown,
                        Err(error) if error.is_retryable() => continue,
                        Err(error) => panic!("writing {key} failed: {error}"),
                    };
                    Op::Write {
                        expected,
                        value,
                        result,
                    }
                };
                let ret = match op {
                    Op::Write {
                        result: WriteResult::Unknown,
                        ..
                    } => Tick::MAX,
                    _ => tick(&clock),
                };
                events.push((register, Event { call, ret, op }));
                tokio::task::yield_now().await;
            }
            events
        });
    }
    let mut histories = vec![Vec::new(); HISTORY_REGISTERS];
    for (register, event) in tasks.join_all().await.into_iter().flatten() {
        histories[register].push(event);
    }
    let mut written = 0;
    for (register, history) in histories.iter_mut().enumerate() {
        let key = history_key(register);
        let call = tick(&clock);
        let read = get_with_retries(&handles[0], &key, &RetryPolicy::default()).await;
        history.push(Event {
            call,
            ret: tick(&clock),
            op: Op::Read(read.unwrap()),
        });
        if let Err(violation) = history::check(history) {
            panic!("the history of {key} is not linearizable: {violation}");
        }
        written += history
            .iter()
            .filter(|e| {
                matches!(
                    e.op,
                    Op::Write {
                        result: WriteResult::Written(_),
                        ..
                    }
                )
            })
            .count();
    }
    assert!(written > 0, "no write succeeded");
}

fn history_key(register: usize) -> RegisterKey {
    key(&format!("shards/history/{register}.json"))
}

/// What a writer of [`increments_survive_random_faults`] was told.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Answer {
    Accepted,
    /// Rejected: lost a race, or landed and was overwritten before the
    /// re-read of the lost-response rule.
    Rejected,
    /// Gave up, and may have applied.
    Unknown,
    /// Gave up without applying.
    NotApplied,
}

/// One writer per handle increments a counter `increments` times through
/// [`propose`], each over its handle with random lost requests, lost
/// responses, late requests, `409` conflicts, and outages: the
/// lost-response rule at scale. No two accepted proposals share a counter,
/// no value appears that no proposal could have written, and the
/// register's final value is not one its proposer was told was rejected.
pub async fn increments_survive_random_faults<S: ControlStore>(handles: &[S], increments: u64) {
    let register = key("shards/b-2/0.json");
    let created = handles[0].put_if(
        &register,
        Expected::Absent,
        value(&ProposalId::from_u128(0), 0),
    );
    assert!(matches!(created.await.unwrap(), PutOutcome::Written(_)));
    let faulty = faulty(handles, 500);
    let mut tasks = JoinSet::new();
    for (writer, store) in (0..).zip(&faulty) {
        let (store, register) = (store.clone(), register.clone());
        tasks.spawn(async move {
            let policy = patient();
            let mut ids = ProposalIds::seeded(600 + writer);
            let mut proposals = Vec::new();
            let mut accepted = 0;
            while accepted < increments {
                assert!(
                    proposals.len() < 100 * increments as usize,
                    "writer {writer} made no progress"
                );
                let current = get_with_retries(&store, &register, &policy).await;
                let current = current.unwrap().expect("the counter exists");
                let next = counter_of(&current) + 1;
                let proposal = ids.next_id();
                let expected = Expected::Version(current.version);
                let value = value(&proposal, next);
                let outcome = propose(&store, &register, expected, value, &proposal, &policy);
                let answer = match outcome.await {
                    Ok(ProposalOutcome::Accepted(_)) => {
                        accepted += 1;
                        Answer::Accepted
                    }
                    Ok(ProposalOutcome::Rejected) => Answer::Rejected,
                    Err(error) if error.may_have_applied() => Answer::Unknown,
                    Err(ControlError::RetriesExhausted { .. }) => Answer::NotApplied,
                    Err(error) => panic!("proposing failed: {error}"),
                };
                proposals.push((proposal, next, answer));
                tokio::task::yield_now().await;
            }
            proposals
        });
    }
    let proposals: Vec<_> = tasks.join_all().await.into_iter().flatten().collect();
    injected(&faulty);
    // Late requests either land or fail their preconditions by now.
    tokio::time::sleep(LATE_REQUESTS).await;
    let current = handles[0].get(&register).await.unwrap();
    let current = current.expect("the counter exists");
    let last = counter_of(&current);

    let mut accepted = BTreeSet::new();
    for (proposal, counter, answer) in &proposals {
        if *answer == Answer::Accepted {
            assert!(
                accepted.insert(*counter),
                "two accepted proposals wrote counter {counter}"
            );
            assert!(*counter <= last, "accepted {proposal} at {counter} is lost");
        }
    }
    assert_eq!(accepted.len() as u64, handles.len() as u64 * increments);
    for counter in 1..=last {
        let possible = proposals
            .iter()
            .any(|(_, c, answer)| *c == counter && *answer != Answer::NotApplied);
        assert!(possible, "counter {counter} was written by no proposal");
    }
    let holder = proposal_id_of(&current.value).expect("the counter has a proposal");
    let answer = proposals
        .iter()
        .find(|(proposal, _, _)| *proposal == holder)
        .map(|(_, _, answer)| *answer);
    assert!(
        matches!(answer, Some(Answer::Accepted | Answer::Unknown)),
        "the counter holds {holder}, whose proposer was told {answer:?}"
    );
}

/// One node per handle bootstraps the same cluster at once and then
/// increments its generation: exactly one creates `cluster.json`, all
/// agree on it, and the increments count up from it.
pub async fn bootstrap_races_agree<S: ControlStore>(handles: &[S]) {
    let cluster = ClusterId::new("conformance").expect("valid");
    let mut tasks = JoinSet::new();
    for (node, store) in (0..).zip(handles) {
        let (store, cluster) = (store.clone(), cluster.clone());
        tasks.spawn(async move {
            let mut ids = ProposalIds::seeded(300 + node);
            let policy = RetryPolicy::default();
            let outcome = bootstrap(&store, &cluster, ids.next_id(), &policy)
                .await
                .unwrap();
            let generation = bump_generation(&store, &cluster, &mut ids, &policy)
                .await
                .unwrap();
            (outcome, generation)
        });
    }
    let results = tasks.join_all().await;
    let created: Vec<_> = results
        .iter()
        .filter_map(|(outcome, _)| match outcome {
            Bootstrap::Created(cluster) => Some(cluster),
            Bootstrap::Existing(_) => None,
        })
        .collect();
    assert_eq!(
        created.len(),
        1,
        "{} nodes created cluster.json",
        created.len()
    );
    // Nodes that read cluster.json saw the created document, or a later
    // increment of it.
    for (outcome, _) in &results {
        let found = &outcome.cluster().value;
        assert_eq!(found.cluster_id, cluster);
        if found.generation == FIRST_GENERATION {
            assert_eq!(found, &created[0].value, "nodes disagree");
        }
    }
    let policy = RetryPolicy::default();
    let current = read_cluster(&handles[0], &cluster, &policy)
        .await
        .unwrap()
        .value
        .generation;
    let nodes = handles.len() as u64;
    assert!(
        current > FIRST_GENERATION && current.get() <= 1 + nodes,
        "{current}"
    );
    for (_, generation) in &results {
        assert!(
            *generation > FIRST_GENERATION && *generation <= current,
            "{generation}"
        );
    }
    let other = ClusterId::new("other").expect("valid");
    let mismatch = bootstrap(&handles[0], &other, ProposalId::from_u128(1), &policy).await;
    assert!(
        mismatch.is_err(),
        "another cluster bootstrapped over this one"
    );
}

/// The prefix of every probe's scratch registers.
fn probe_prefix() -> KeyPrefix {
    KeyPrefix::new("probe/").expect("valid")
}

/// The startup probe repeated at scale: in each of `runs` runs, one node
/// per handle probes the store at once, with `rounds` rounds, racing three
/// writers over its own handle and the next ones. Every probe passes and
/// leaves no scratch register behind.
pub async fn probes_pass_repeatedly<S: ControlStore>(handles: &[S], runs: u32, rounds: u32) {
    assert!(handles.len() >= 2, "the probe races at least two writers");
    let racers = handles.len().min(3);
    for run in 0..runs {
        let mut probes = JoinSet::new();
        for node in 0..handles.len() {
            let writers: Vec<S> = (node..node + racers)
                .map(|n| handles[n % handles.len()].clone())
                .collect();
            let nonce = 0xc0f0_0000 + (u64::from(run) << 8) + node as u64;
            let probe = ControlProbe::new(nonce).with_rounds(rounds, ControlProbe::KEYS);
            probes.spawn(async move { (node, probe.run(&writers).await) });
        }
        for (node, result) in probes.join_all().await {
            if let Err(error) = result {
                panic!("run {run}: the probe of node {node} failed: {error}");
            }
        }
    }
    let left = handles[0].list(&probe_prefix()).await.unwrap();
    assert!(left.is_empty(), "the probes left {left:?}");
}

/// The startup probe, with `rounds` rounds and one writer per handle, each
/// with random faults: lost requests and responses, late requests, `409`
/// conflicts, and outages. It passes, and leaves no scratch register
/// behind once late requests are done.
pub async fn the_probe_passes_under_faults<S: ControlStore>(handles: &[S], rounds: u32) {
    let faulty = faulty(handles, 700);
    let probe = ControlProbe::new(0xfa17)
        .with_rounds(rounds, ControlProbe::KEYS)
        .with_policy(patient());
    if let Err(error) = probe.run(&faulty).await {
        panic!("the probe failed under faults: {error}");
    }
    injected(&faulty);
    // Late requests are fenced by their preconditions.
    tokio::time::sleep(LATE_REQUESTS).await;
    let left = handles[0].list(&probe_prefix()).await.unwrap();
    assert!(left.is_empty(), "the probe left {left:?}");
}

/// One change stream per handle, opened before any write, while one
/// writer per handle writes its own bucket register `rounds` times and
/// increments the generation after each write. Every stream reports
/// strictly increasing generations, a snapshot first, and reaches the
/// registers' final versions.
pub async fn changes_reach_every_watcher<S: ControlStore>(handles: &[S], rounds: u64) {
    let cluster = ClusterId::new("conformance").expect("valid");
    let policy = RetryPolicy::default();
    let mut ids = ProposalIds::seeded(800);
    bootstrap(&handles[0], &cluster, ids.next_id(), &policy)
        .await
        .unwrap();

    // The generation that ends the watch; set once the writers are done.
    let target = Arc::new(AtomicU64::new(u64::MAX));
    let mut watchers = JoinSet::new();
    for (watcher, store) in handles.iter().enumerate() {
        let mut changes = store.changes(Generation::ZERO).await.unwrap();
        let target = target.clone();
        watchers.spawn(async move {
            let mut registers = BTreeMap::new();
            let mut last: Option<Generation> = None;
            loop {
                let change = next(&mut changes).await;
                match last {
                    None => assert!(change.snapshot, "watcher {watcher}: {change:?} came first"),
                    Some(last) => assert!(
                        change.generation > last,
                        "watcher {watcher}: {change:?} came after generation {last}"
                    ),
                }
                if change.snapshot {
                    registers.clear();
                }
                for (key, version) in change.registers {
                    match version {
                        Some(version) => registers.insert(key, version),
                        None => registers.remove(&key),
                    };
                }
                last = Some(change.generation);
                if change.generation.get() >= target.load(Ordering::SeqCst) {
                    return (watcher, registers);
                }
            }
        });
    }

    let mut writers = JoinSet::new();
    for (writer, store) in (0..).zip(handles) {
        let (store, cluster, policy) = (store.clone(), cluster.clone(), policy);
        writers.spawn(async move {
            let register = key(&format!("buckets/watched-{writer}.json"));
            let mut ids = ProposalIds::seeded(900 + writer);
            let mut expected = Expected::Absent;
            for round in 0..rounds {
                let proposal = ids.next_id();
                let value = value(&proposal, round);
                let outcome = propose(&store, &register, expected, value, &proposal, &policy);
                let ProposalOutcome::Accepted(version) = outcome.await.unwrap() else {
                    panic!("the only writer of {register} lost a race");
                };
                expected = Expected::Version(version);
                bump_generation(&store, &cluster, &mut ids, &policy)
                    .await
                    .unwrap();
            }
            let Expected::Version(version) = expected else {
                unreachable!("every round writes");
            };
            (register, version)
        });
    }
    let written: BTreeMap<RegisterKey, Version> = writers.join_all().await.into_iter().collect();
    let generation = read_cluster(&handles[0], &cluster, &policy)
        .await
        .unwrap()
        .value
        .generation;
    target.store(generation.get() + 1, Ordering::SeqCst);
    let last = bump_generation(&handles[0], &cluster, &mut ids, &policy)
        .await
        .unwrap();
    assert_eq!(last.get(), generation.get() + 1);
    for (watcher, registers) in watchers.join_all().await {
        assert_eq!(registers, written, "watcher {watcher} missed a change");
    }
}
