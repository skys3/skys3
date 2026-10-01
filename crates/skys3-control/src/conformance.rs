//! The conformance suite every [`ControlStore`] backend must pass (design
//! §16.1, plan M2-06).
//!
//! # Plugging in a backend
//!
//! A backend's tests implement [`Backend`], which makes fresh, empty
//! stores and, if the backend has connections, more handles to a store's
//! registers, and call [`run`] or [`run_at`]:
//!
//! ```
//! use skys3_control::MemoryControlStore;
//! use skys3_control::conformance::{self, Backend, Scale};
//!
//! struct Memory;
//!
//! impl Backend for Memory {
//!     type Store = MemoryControlStore;
//!
//!     async fn store(&mut self) -> MemoryControlStore {
//!         MemoryControlStore::new()
//!     }
//! }
//!
//! # let runtime = tokio::runtime::Builder::new_current_thread()
//! #     .enable_time()
//! #     .start_paused(true)
//! #     .build()?;
//! # runtime.block_on(async {
//! conformance::run_at(&mut Memory, Scale::SMALL).await;
//! # });
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! Every check is also public, so a backend can run one alone, at another
//! scale, or under faults of its own, such as a simulated S3 store's. A
//! check panics with a message naming what failed, and [`run_at`] names
//! each check on standard error before it starts it.
//!
//! # What the suite checks
//!
//! - One register at a time: [`conditional_writes`],
//!   [`conditional_deletes`], and [`listing`].
//! - Linearizable `put_if` under concurrency, with one writer per handle:
//!   [`racing_creates_have_one_winner`], [`increments_are_linearizable`],
//!   and [`histories_are_linearizable`], which records concurrent reads
//!   and conditional writes and checks each register's history against
//!   real time.
//! - Retries and the lost-response rule, with faults injected by
//!   [`FaultyStore`] around the backend: scripted in
//!   [`lost_responses_resolve`] and [`conflicts_are_retried`] (`409`
//!   answers), random in [`increments_survive_random_faults`].
//! - Cluster bootstrap and the generation: [`bootstrap_races_agree`].
//! - The startup probe at scale: [`probes_pass_repeatedly`], with several
//!   nodes probing at once, run after run, and
//!   [`the_probe_passes_under_faults`].
//! - Change delivery, by watch or by poll: [`changes_follow_the_generation`]
//!   and [`changes_reach_every_watcher`].
//!
//! # Time
//!
//! The checks wait on real or simulated time: backoffs between retries,
//! and a late request in flight. A backend whose requests go to blocking
//! threads must run them without Tokio's paused clock, which would jump
//! ahead while the runtime waits for those threads.

mod history;
mod races;

use std::future::Future;
use std::time::Duration;

use bytes::Bytes;
use skys3_types::{ClusterId, Generation, ProposalId};

pub use self::races::{
    bootstrap_races_agree, changes_reach_every_watcher, histories_are_linearizable,
    increments_are_linearizable, increments_survive_random_faults, probes_pass_repeatedly,
    racing_creates_have_one_winner, the_probe_passes_under_faults,
};
use crate::cluster::{FIRST_GENERATION, bootstrap, bump_generation};
use crate::faults::{Fault, FaultyStore};
use crate::key::{KeyPrefix, RegisterKey};
use crate::probe::ControlProbe;
use crate::propose::{
    DeletionOutcome, ProposalIds, ProposalOutcome, RetryPolicy, proposal_id_of, propose,
    propose_delete,
};
use crate::store::{
    Change, ChangeStream, ControlError, ControlStore, DeleteOutcome, Expected, PutOutcome, Version,
    Versioned,
};

/// Makes the stores a conformance run tests.
pub trait Backend {
    /// The store type.
    type Store: ControlStore;

    /// Returns a new, empty store, independent of every earlier one.
    fn store(&mut self) -> impl Future<Output = Self::Store>;

    /// Returns another handle to `store`'s registers, as another node
    /// would open it. Racing writers each use their own handle. The
    /// default clones `store`; a backend whose clones share a connection,
    /// such as etcd's, opens a new one.
    fn connect(&mut self, store: &Self::Store) -> impl Future<Output = Self::Store> {
        std::future::ready(store.clone())
    }
}

/// How hard [`run_at`] pushes a backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Scale {
    /// Handles to one store, each with its own racing writer, probing
    /// node, or change stream. At least 2.
    pub writers: u64,
    /// Operations per writer in the concurrent checks. At least 8, so
    /// that every check gets some.
    pub operations: u64,
    /// How many times every node runs the startup probe.
    pub probe_runs: u32,
    /// The rounds of each probe.
    pub probe_rounds: u32,
}

impl Scale {
    /// A brief run, for in-process stand-ins on real time such as the
    /// file store and the fake etcd.
    pub const SMALL: Self = Self {
        writers: 3,
        operations: 8,
        probe_runs: 1,
        probe_rounds: 20,
    };

    /// [`run`]'s scale, also for stores billed per request: full probes.
    pub const DEFAULT: Self = Self {
        writers: 4,
        operations: 16,
        probe_runs: 2,
        probe_rounds: ControlProbe::ROUNDS,
    };

    /// For simulated stores and a local etcd: more writers, longer
    /// histories, and more probes.
    pub const LARGE: Self = Self {
        writers: 8,
        operations: 48,
        probe_runs: 4,
        probe_rounds: ControlProbe::ROUNDS,
    };
}

impl Default for Scale {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// How long a change stream may take to report an announced write. Polling
/// backends report within their poll interval.
const REPORT_TIMEOUT: Duration = Duration::from_secs(60);

/// Runs every check at [`Scale::DEFAULT`].
pub async fn run<B: Backend>(backend: &mut B) {
    run_at(backend, Scale::DEFAULT).await;
}

/// Runs every check at `scale`, each on a fresh store.
///
/// # Panics
///
/// If a check fails, or `scale` is below its minimums.
pub async fn run_at<B: Backend>(backend: &mut B, scale: Scale) {
    assert!(scale.writers >= 2, "the suite races at least two writers");
    assert!(
        scale.operations >= 8,
        "the suite needs 8 operations per writer"
    );
    let Scale {
        writers,
        operations,
        probe_runs,
        probe_rounds,
    } = scale;

    announce("conditional_writes");
    conditional_writes(&backend.store().await).await;
    announce("conditional_deletes");
    conditional_deletes(&backend.store().await).await;
    announce("listing");
    listing(&backend.store().await).await;
    announce("conflicts_are_retried");
    conflicts_are_retried(&backend.store().await).await;
    announce("lost_responses_resolve");
    lost_responses_resolve(&backend.store().await).await;

    announce("racing_creates_have_one_winner");
    racing_creates_have_one_winner(&handles(backend, writers).await).await;
    announce("increments_are_linearizable");
    increments_are_linearizable(&handles(backend, writers).await, operations / 2).await;
    announce("histories_are_linearizable");
    histories_are_linearizable(&handles(backend, writers).await, operations).await;
    announce("increments_survive_random_faults");
    increments_survive_random_faults(&handles(backend, writers).await, operations / 4).await;
    announce("bootstrap_races_agree");
    bootstrap_races_agree(&handles(backend, writers).await).await;

    announce("probes_pass_repeatedly");
    probes_pass_repeatedly(&handles(backend, writers).await, probe_runs, probe_rounds).await;
    announce("the_probe_passes_under_faults");
    the_probe_passes_under_faults(&handles(backend, writers).await, probe_rounds).await;

    announce("changes_follow_the_generation");
    changes_follow_the_generation(&backend.store().await).await;
    announce("changes_reach_every_watcher");
    changes_reach_every_watcher(&handles(backend, writers).await, (operations / 8).max(2)).await;
}

fn announce(check: &str) {
    eprintln!("conformance: {check}");
}

/// A fresh store and `count - 1` more handles to it.
async fn handles<B: Backend>(backend: &mut B, count: u64) -> Vec<B::Store> {
    let mut handles = vec![backend.store().await];
    for _ in 1..count {
        let handle = backend.connect(&handles[0]).await;
        handles.push(handle);
    }
    handles
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

/// `delete_if` removes a register only at its current version; a register
/// created again gets a version no stale or late delete matches; lost
/// delete answers resolve by re-reading; and change streams report the
/// removal.
pub async fn conditional_deletes<S: ControlStore>(store: &S) {
    let register = key("buckets/deleted.json");
    let mut ids = ProposalIds::seeded(6);
    let create = async |ids: &mut ProposalIds| -> Version {
        match store
            .put_if(&register, Expected::Absent, value(&ids.next_id(), 0))
            .await
            .unwrap()
        {
            PutOutcome::Written(version) => version,
            PutOutcome::PreconditionFailed => panic!("creating an absent register failed"),
        }
    };
    let absent = store.delete_if(&register, &Version::new("none")).await;
    assert_eq!(
        absent.unwrap(),
        DeleteOutcome::PreconditionFailed,
        "deleted an absent register"
    );
    let v1 = create(&mut ids).await;
    let PutOutcome::Written(v2) = store
        .put_if(
            &register,
            Expected::Version(v1.clone()),
            value(&ids.next_id(), 1),
        )
        .await
        .unwrap()
    else {
        panic!("an update at the current version failed its precondition");
    };
    let stale = store.delete_if(&register, &v1).await.unwrap();
    assert_eq!(
        stale,
        DeleteOutcome::PreconditionFailed,
        "a stale delete applied"
    );
    let deleted = store.delete_if(&register, &v2).await.unwrap();
    assert_eq!(deleted, DeleteOutcome::Deleted);
    assert_eq!(store.get(&register).await.unwrap(), None, "still readable");
    assert!(
        store.list(&KeyPrefix::buckets()).await.unwrap().is_empty(),
        "a deleted register is listed"
    );
    let again = store.delete_if(&register, &v2).await.unwrap();
    assert_eq!(again, DeleteOutcome::PreconditionFailed, "deleted twice");

    // A register created again is out of reach of every earlier version.
    let v3 = create(&mut ids).await;
    for old in [&v1, &v2] {
        assert_ne!(&v3, old, "a recreated register reused a version");
        let late = store.delete_if(&register, old).await.unwrap();
        assert_eq!(
            late,
            DeleteOutcome::PreconditionFailed,
            "a late delete applied"
        );
    }

    // Lost and late answers.
    let faulty = FaultyStore::new(store.clone());
    let policy = RetryPolicy::default();
    faulty.script([Fault::LoseResponse]);
    let outcome = propose_delete(&faulty, &register, &v3, &policy).await;
    assert_eq!(outcome.unwrap(), DeletionOutcome::Deleted, "lost answer");
    let v4 = create(&mut ids).await;
    faulty.script([Fault::LateRequest(Duration::from_millis(50))]);
    let outcome = propose_delete(&faulty, &register, &v4, &policy).await;
    assert_eq!(outcome.unwrap(), DeletionOutcome::Deleted, "late request");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(store.get(&register).await.unwrap(), None);

    // Change streams report the removal.
    let cluster = ClusterId::new("conformance").expect("valid");
    bootstrap(store, &cluster, ids.next_id(), &policy)
        .await
        .unwrap();
    let v5 = create(&mut ids).await;
    let generation = bump_generation(store, &cluster, &mut ids, &policy)
        .await
        .unwrap();
    let mut changes = store.changes(Generation::ZERO).await.unwrap();
    let first = next(&mut changes).await;
    assert_eq!(first.registers, [(register.clone(), Some(v5.clone()))]);
    let deleted = store.delete_if(&register, &v5).await.unwrap();
    assert_eq!(deleted, DeleteOutcome::Deleted);
    let after = bump_generation(store, &cluster, &mut ids, &policy)
        .await
        .unwrap();
    let mut change = next(&mut changes).await;
    while change.generation < after {
        change = next(&mut changes).await;
    }
    assert!(after > generation);
    assert_eq!(change.registers, [(register, None)], "removal not reported");
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

/// `409 ConditionalRequestConflict` answers to creates, updates, and
/// deletes are sent again until they land, as design §6.1 says, and are
/// never taken for a lost race or a lost response; a write whose every
/// attempt conflicted reports that it applied nothing, and did not.
pub async fn conflicts_are_retried<S: ControlStore>(store: &S) {
    let faulty = FaultyStore::new(store.clone());
    let register = key("buckets/conflicted.json");
    let mut ids = ProposalIds::seeded(7);
    let policy = RetryPolicy::default();
    let conflicts = |n| vec![Fault::Conflict; n];

    let mut write = async |faults: Vec<Fault>, expected: Expected, policy: &RetryPolicy| {
        faulty.script(faults);
        let proposal = ids.next_id();
        let value = value(&proposal, 0);
        let outcome = propose(
            &faulty,
            &register,
            expected,
            value.clone(),
            &proposal,
            policy,
        );
        (outcome.await, value)
    };
    let (created, value1) = write(conflicts(3), Expected::Absent, &policy).await;
    let Ok(ProposalOutcome::Accepted(v1)) = created else {
        panic!("a create that conflicted three times: {created:?}");
    };
    let (updated, value2) = write(conflicts(2), Expected::Version(v1.clone()), &policy).await;
    let Ok(ProposalOutcome::Accepted(v2)) = updated else {
        panic!("an update that conflicted twice: {updated:?}");
    };
    assert_ne!(v1, v2);
    let current = store.get(&register).await.unwrap();
    assert_eq!(
        current,
        Some(Versioned {
            value: value2,
            version: v2.clone()
        }),
        "conflicting writes landed elsewhere (the first wrote {value1:?})"
    );

    let short = RetryPolicy {
        max_attempts: 3,
        ..policy
    };
    let (exhausted, _) = write(conflicts(3), Expected::Version(v2.clone()), &short).await;
    match exhausted {
        Err(ControlError::RetriesExhausted {
            attempts: 3,
            may_have_applied: false,
            ..
        }) => {}
        other => panic!("three conflicts with three attempts: {other:?}"),
    }
    let unchanged = store.get(&register).await.unwrap().map(|r| r.version);
    assert_eq!(unchanged, Some(v2.clone()), "a conflicting write applied");

    faulty.script(conflicts(2));
    let deleted = propose_delete(&faulty, &register, &v2, &policy).await;
    assert_eq!(deleted.unwrap(), DeletionOutcome::Deleted);
    assert_eq!(store.get(&register).await.unwrap(), None);
    let stats = faulty.stats();
    assert_eq!(
        (stats.conflicts, stats.lost_responses),
        (10, 0),
        "{stats:?}"
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
