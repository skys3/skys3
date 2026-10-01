//! The conformance suite every [`ControlStore`] backend must pass (design
//! §16.1, plan M2-06).
//!
//! A backend's tests implement [`Backend`], which makes fresh, empty
//! stores, and call [`run`]. Each check is also public, so a backend can
//! run one alone or at a larger scale. A check panics with a message naming
//! what failed.
//!
//! The checks cover linearizable `put_if` under concurrency, listing, the
//! lost-response rule with faults injected by
//! [`FaultyStore`] around the backend, cluster
//! bootstrap and the generation, and change delivery. A backend's own
//! transport faults, such as a simulated S3 store's, come on top of these.
//!
//! The checks wait on real or simulated time: backoffs between retries,
//! and a late request in flight. A backend whose requests go to blocking
//! threads must run them without Tokio's paused clock, which would jump
//! ahead while the runtime waits for those threads.

use std::collections::BTreeSet;
use std::future::Future;
use std::time::Duration;

use bytes::Bytes;
use skys3_types::{ClusterId, Generation, ProposalId};
use tokio::task::JoinSet;

use crate::cluster::{Bootstrap, FIRST_GENERATION, bootstrap, bump_generation, read_cluster};
use crate::faults::{Fault, FaultyStore};
use crate::key::{KeyPrefix, RegisterKey};
use crate::propose::{ProposalIds, ProposalOutcome, RetryPolicy, proposal_id_of, propose};
use crate::store::{Change, ChangeStream, ControlStore, Expected, PutOutcome, Versioned};

/// Makes the stores a conformance run tests.
pub trait Backend {
    /// The store type.
    type Store: ControlStore;

    /// Returns a new, empty store, independent of every earlier one.
    fn store(&mut self) -> impl Future<Output = Self::Store>;
}

/// How long a change stream may take to report an announced write. Polling
/// backends report within their poll interval.
const REPORT_TIMEOUT: Duration = Duration::from_secs(60);

/// Runs every check, each on a fresh store.
pub async fn run<B: Backend>(backend: &mut B) {
    conditional_writes(&backend.store().await).await;
    listing(&backend.store().await).await;
    racing_creates_have_one_winner(&backend.store().await, 8).await;
    increments_are_linearizable(&backend.store().await, 4, 8).await;
    lost_responses_resolve(&backend.store().await).await;
    bootstrap_races_agree(&backend.store().await, 6).await;
    changes_follow_the_generation(&backend.store().await).await;
}

/// A register value carrying `proposal` and a counter.
fn value(proposal: &ProposalId, counter: u64) -> Bytes {
    Bytes::from(format!(
        r#"{{"counter":{counter},"proposal_id":"{proposal}"}}"#
    ))
}

fn counter_of(register: &Versioned) -> u64 {
    let json: serde_json::Value =
        serde_json::from_slice(&register.value).expect("a counter register holds JSON");
    json["counter"]
        .as_u64()
        .expect("a counter register holds a counter")
}

fn key(key: &str) -> RegisterKey {
    RegisterKey::new(key).expect("the suite's keys are valid")
}

/// `get` and `put_if` on one register: creation with `Absent`, updates
/// with the current version only, and a new version for every write.
pub async fn conditional_writes<S: ControlStore>(store: &S) {
    let register = key("buckets/conformance.json");
    let mut ids = ProposalIds::seeded(1);
    assert_eq!(
        store.get(&register).await.unwrap(),
        None,
        "a new store is empty"
    );

    let first = value(&ids.next_id(), 1);
    let PutOutcome::Written(v1) = store
        .put_if(&register, Expected::Absent, first.clone())
        .await
        .unwrap()
    else {
        panic!("creating an absent register failed its precondition");
    };
    let read = store.get(&register).await.unwrap();
    assert_eq!(
        read,
        Some(Versioned {
            value: first,
            version: v1.clone()
        })
    );

    let again = store
        .put_if(&register, Expected::Absent, value(&ids.next_id(), 2))
        .await
        .unwrap();
    assert_eq!(again, PutOutcome::PreconditionFailed, "created twice");

    let second = value(&ids.next_id(), 2);
    let PutOutcome::Written(v2) = store
        .put_if(&register, Expected::Version(v1.clone()), second.clone())
        .await
        .unwrap()
    else {
        panic!("an update at the current version failed its precondition");
    };
    assert_ne!(v1, v2, "an update kept its version");
    let stale = store
        .put_if(&register, Expected::Version(v1), value(&ids.next_id(), 3))
        .await
        .unwrap();
    assert_eq!(
        stale,
        PutOutcome::PreconditionFailed,
        "a stale update was applied"
    );
    let missing = store
        .put_if(
            &key("buckets/other.json"),
            Expected::Version(v2.clone()),
            second.clone(),
        )
        .await
        .unwrap();
    assert_eq!(
        missing,
        PutOutcome::PreconditionFailed,
        "an update created a register"
    );
    let read = store.get(&register).await.unwrap();
    assert_eq!(
        read,
        Some(Versioned {
            value: second,
            version: v2
        })
    );
}

/// `list` returns exactly the registers under a prefix, in key order,
/// with the versions `get` returns.
pub async fn listing<S: ControlStore>(store: &S) {
    let keys = [
        "buckets-old/x.json",
        "buckets/a.json",
        "buckets/b.json",
        "cluster.json",
        "nodes/node-1.json",
        "shards/b-1/0.json",
        "shards/b-1/1.json",
        "shards/b-10/0.json",
    ];
    let mut ids = ProposalIds::seeded(2);
    // Written out of order; listed in byte order, where '-' sorts before '/'.
    for k in keys.iter().rev() {
        let written = store
            .put_if(&key(k), Expected::Absent, value(&ids.next_id(), 0))
            .await;
        assert!(matches!(written.unwrap(), PutOutcome::Written(_)), "{k}");
    }
    let cases = [
        (KeyPrefix::root(), keys.to_vec()),
        (
            KeyPrefix::buckets(),
            vec!["buckets/a.json", "buckets/b.json"],
        ),
        (KeyPrefix::nodes(), vec!["nodes/node-1.json"]),
        (
            KeyPrefix::new("shards/b-1/").unwrap(),
            vec!["shards/b-1/0.json", "shards/b-1/1.json"],
        ),
        (KeyPrefix::identity(), vec![]),
    ];
    for (prefix, expected) in cases {
        let listed = store.list(&prefix).await.unwrap();
        let names: Vec<_> = listed.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(names, expected, "listing {prefix:?}");
        for (k, version) in listed {
            let current = store
                .get(&k)
                .await
                .unwrap()
                .expect("a listed register exists");
            assert_eq!(current.version, version, "{k} lists another version");
        }
    }
}

/// `writers` proposals race to create one register: exactly one is
/// accepted, and the register holds it.
pub async fn racing_creates_have_one_winner<S: ControlStore>(store: &S, writers: u64) {
    let register = key("coordinator.lease");
    let mut tasks = JoinSet::new();
    for writer in 0..writers {
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
    let stored = store
        .get(&register)
        .await
        .unwrap()
        .expect("the register exists");
    assert_eq!(proposal_id_of(&stored.value).as_ref(), Some(&winners[0].0));
}

/// `writers` writers each increment a counter register `increments` times
/// by read and `put_if`: no increment is lost or applied twice.
pub async fn increments_are_linearizable<S: ControlStore>(
    store: &S,
    writers: u64,
    increments: u64,
) {
    let register = key("shards/b-1/0.json");
    let created = store.put_if(
        &register,
        Expected::Absent,
        value(&ProposalId::from_u128(0), 0),
    );
    assert!(matches!(created.await.unwrap(), PutOutcome::Written(_)));
    let mut tasks = JoinSet::new();
    for writer in 0..writers {
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
    let total = writers * increments;
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
    let current = store
        .get(&register)
        .await
        .unwrap()
        .expect("the counter exists");
    assert_eq!(counter_of(&current), total, "the counter lost an increment");
}

/// The lost-response rule and retries, with faults injected around the
/// backend: every write the proposer is told landed did land, and every
/// write it is told was rejected is not the register's value.
pub async fn lost_responses_resolve<S: ControlStore>(store: &S) {
    let faulty = FaultyStore::new(store.clone());
    let register = key("buckets/lost.json");
    let mut ids = ProposalIds::seeded(3);
    let policy = RetryPolicy::default();

    let mut write = async |faults: Vec<Fault>, expected: Expected| {
        faulty.script(faults);
        let proposal = ids.next_id();
        let outcome = propose(
            &faulty,
            &register,
            expected,
            value(&proposal, 0),
            &proposal,
            &policy,
        );
        let outcome = outcome.await.unwrap();
        let stored = store
            .get(&register)
            .await
            .unwrap()
            .expect("the register exists");
        let holds = proposal_id_of(&stored.value) == Some(proposal);
        (outcome, stored, holds)
    };

    let cases = [
        vec![Fault::LoseResponse],
        vec![Fault::LoseRequest],
        vec![Fault::Conflict, Fault::Unavailable],
        vec![Fault::LoseResponse, Fault::LoseResponse, Fault::LoseRequest],
        vec![
            Fault::LateRequest(Duration::from_millis(5)),
            Fault::Pass,
            Fault::Unavailable,
        ],
        vec![Fault::LateRequest(Duration::from_millis(200))],
    ];
    let mut expected = Expected::Absent;
    for faults in cases {
        let description = format!("{faults:?}");
        let (outcome, stored, holds) = write(faults, expected).await;
        assert!(
            holds,
            "{description}: the register does not hold the proposal"
        );
        assert_eq!(
            outcome,
            ProposalOutcome::Accepted(stored.version.clone()),
            "{description}"
        );
        expected = Expected::Version(stored.version);
    }
    // A late request never lands over a newer value.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let current = store
        .get(&register)
        .await
        .unwrap()
        .expect("the register exists");
    assert_eq!(
        Expected::Version(current.version.clone()),
        expected,
        "a late request landed"
    );

    // Our write lands and another writer replaces it before we learn of it.
    let (other, register_) = (store.clone(), register.clone());
    let theirs = ProposalId::new("theirs").expect("valid");
    let their_value = value(&theirs, 1);
    let overwrite = Fault::lose_response_after(move || {
        let (other, register, their_value) =
            (other.clone(), register_.clone(), their_value.clone());
        async move {
            let current = other
                .get(&register)
                .await
                .unwrap()
                .expect("the register exists");
            let written = other.put_if(&register, Expected::Version(current.version), their_value);
            assert!(matches!(written.await.unwrap(), PutOutcome::Written(_)));
        }
    });
    let (outcome, stored, _) = write(vec![overwrite], expected).await;
    assert_eq!(
        outcome,
        ProposalOutcome::Rejected,
        "an overwritten proposal was accepted"
    );
    assert_eq!(proposal_id_of(&stored.value), Some(theirs));
}

/// `nodes` nodes bootstrap the same cluster at once and then increment
/// its generation: exactly one creates `cluster.json`, all agree on it,
/// and the increments count up from it.
pub async fn bootstrap_races_agree<S: ControlStore>(store: &S, nodes: u64) {
    let cluster = ClusterId::new("conformance").expect("valid");
    let mut tasks = JoinSet::new();
    for node in 0..nodes {
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
    let current = read_cluster(store, &cluster, &policy)
        .await
        .unwrap()
        .value
        .generation;
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
    let mismatch = bootstrap(store, &other, ProposalId::from_u128(1), &policy).await;
    assert!(
        mismatch.is_err(),
        "another cluster bootstrapped over this one"
    );
}

async fn next<C: ChangeStream>(changes: &mut C) -> Change {
    tokio::time::timeout(REPORT_TIMEOUT, changes.next())
        .await
        .expect("the change stream reports an announced write")
        .expect("the change stream reads the store")
}

/// Change streams report announced writes as the [`ChangeStream`]
/// contract says: a snapshot first, then coalesced differences, and never
/// `cluster.json`, `coordinator.lease`, or keys outside the layout.
pub async fn changes_follow_the_generation<S: ControlStore>(store: &S) {
    let cluster = ClusterId::new("conformance").expect("valid");
    let mut ids = ProposalIds::seeded(4);
    let policy = RetryPolicy::default();
    bootstrap(store, &cluster, ids.next_id(), &policy)
        .await
        .unwrap();
    let mut changes = store.changes(Generation::ZERO).await.unwrap();
    let first = next(&mut changes).await;
    assert!(first.snapshot && first.registers.is_empty(), "{first:?}");
    assert_eq!(first.generation, FIRST_GENERATION);

    let mut put = async |k: &str| {
        let register = key(k);
        let expected = match store.get(&register).await.unwrap() {
            Some(current) => Expected::Version(current.version),
            None => Expected::Absent,
        };
        match store
            .put_if(&register, expected, value(&ids.next_id(), 0))
            .await
            .unwrap()
        {
            PutOutcome::Written(version) => version,
            PutOutcome::PreconditionFailed => panic!("a lone writer lost a race"),
        }
    };
    let node = put("nodes/node-1.json").await;
    put("coordinator.lease").await;
    put("probe/0").await;
    put("buckets/b.json").await;
    let bucket = put("buckets/b.json").await;
    let generation = bump_generation(store, &cluster, &mut ProposalIds::seeded(5), &policy)
        .await
        .unwrap();
    let mut change = next(&mut changes).await;
    // A write may be reported before its announcement.
    while change.generation < generation {
        change = next(&mut changes).await;
    }
    assert!(!change.snapshot, "{change:?}");
    let reported: Vec<_> = change.registers.iter().map(|(k, _)| k.as_str()).collect();
    for (k, version) in [("buckets/b.json", &bucket), ("nodes/node-1.json", &node)] {
        let entry = change.registers.iter().find(|(key, _)| key.as_str() == k);
        assert_eq!(
            entry.and_then(|(_, v)| v.as_ref()),
            Some(version),
            "{k} in {reported:?}"
        );
    }
    assert_eq!(
        reported,
        ["buckets/b.json", "nodes/node-1.json"],
        "unexpected registers"
    );

    // A stream opened at the current generation reports a snapshot at the
    // next increment.
    let mut late = store.changes(generation).await.unwrap();
    let shard = put("shards/b-1/0.json").await;
    let next_generation = bump_generation(store, &cluster, &mut ProposalIds::seeded(6), &policy)
        .await
        .unwrap();
    let snapshot = next(&mut late).await;
    assert!(
        snapshot.snapshot && snapshot.generation == next_generation,
        "{snapshot:?}"
    );
    let listed: Vec<_> = snapshot
        .registers
        .iter()
        .map(|(k, v)| (k.as_str(), v.clone()))
        .collect();
    assert_eq!(
        listed,
        [
            ("buckets/b.json", Some(bucket)),
            ("nodes/node-1.json", Some(node)),
            ("shards/b-1/0.json", Some(shard.clone())),
        ]
    );
    let change = next(&mut changes).await;
    assert_eq!(change.registers, [(key("shards/b-1/0.json"), Some(shard))]);
}
