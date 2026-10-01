//! Simulation scenarios for the control store, run by CI's simulation job
//! with a larger seed set.
//!
//! Several nodes share one control store, and every request may fail:
//!
//! - In the **memory** scenario, nodes share an in-memory store, each
//!   through its own seeded [`FaultyStore`]: requests are lost before or
//!   after they apply, applied late, refused with conflicts and outages,
//!   and delayed so that nodes interleave.
//! - In the **S3** scenario, nodes share an [`S3ControlStore`] over one
//!   simulated bucket that injects delays, `500`s, `503 SlowDown`, lost
//!   requests, and lost responses, and answers conditional writes that race
//!   with `409 ConditionalRequestConflict`. Every node first runs the
//!   startup probe, concurrently, and the store must pass it.
//!
//! In both:
//!
//! - **Bootstrap race.** Every node bootstraps the same cluster at once.
//!   One `cluster.json` results, every node agrees on it, and at most one
//!   node reports creating it.
//! - **Contended increments.** Every node then increments one shared
//!   counter register with [`propose`], and announces each increment by
//!   bumping the generation. No counter value is acknowledged to two nodes,
//!   no rejected proposal is the register's value, and each node's
//!   generations only grow.
//! - **Change delivery.** A reader follows a change stream and ends with
//!   the store's final versions.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::rc::Rc;
use std::time::Duration;

use bytes::Bytes;
use skys3_control::faults::{FaultRates, FaultStats, FaultyStore};
use skys3_control::{
    Bootstrap, ChangeStream, ControlError, ControlProbe, ControlStore, Expected, KeyPrefix,
    MemoryControlStore, ProposalIds, ProposalOutcome, RegisterKey, RetryPolicy, S3ControlStore,
    S3StoreConfig, Version, Versioned, bootstrap, bump_generation, proposal_id_of, propose,
    read_cluster,
};
use skys3_sim::s3::{SimS3Config, SimS3Faults, SimS3Stats};
use skys3_sim::{Runner, SeedSet, SimContext, SimS3};
use skys3_types::{ClusterDocument, ClusterId, Generation};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

const NODES: u64 = 5;
const INCREMENTS: u64 = 4;

/// Faults on every node's requests, often enough to show up in every seed.
const RATES: FaultRates = FaultRates {
    lose_request: 0.05,
    lose_response: 0.08,
    late_request: 0.04,
    conflict: 0.05,
    unavailable: 0.05,
    max_delay: Duration::from_millis(5),
};

/// Faults of the simulated S3 bucket, on every request.
const S3_FAULTS: SimS3Faults = SimS3Faults {
    min_delay: Duration::ZERO,
    max_delay: Duration::from_millis(5),
    internal_error_probability: 0.02,
    slow_down_probability: 0.02,
    lost_request_probability: 0.02,
    lost_response_probability: 0.03,
    stale_read_probability: 0.0,
    stale_list_probability: 0.0,
};

/// The control prefix in the simulated bucket.
const PREFIX: &str = "sim/";

/// Enough attempts that no proposal gives up in practice; one that does
/// fails the seed.
const POLICY: RetryPolicy = RetryPolicy {
    max_attempts: 1_000,
    initial_backoff: Duration::from_millis(2),
    max_backoff: Duration::from_millis(50),
};

fn cluster_id() -> ClusterId {
    ClusterId::new("sim").unwrap()
}

fn counter_key() -> RegisterKey {
    RegisterKey::new("shards/b-1/0.json").unwrap()
}

fn counter_value(counter: u64, proposal: &skys3_types::ProposalId) -> Bytes {
    Bytes::from(format!(
        r#"{{"counter":{counter},"proposal_id":"{proposal}"}}"#
    ))
}

fn counter_of(value: &[u8]) -> TestResult<u64> {
    let json: serde_json::Value = serde_json::from_slice(value)?;
    json["counter"]
        .as_u64()
        .ok_or_else(|| "the counter register has no counter".into())
}

/// The control store a scenario's nodes share.
trait World: 'static {
    /// A node's handle on the store.
    type Store: ControlStore;

    /// Whether nodes run the startup probe before they bootstrap.
    const PROBES: bool;

    /// A new node's handle, with its faults.
    fn handle(&self, context: &mut SimContext) -> Self::Store;

    /// Every register, read without faults.
    fn registers(&self) -> Vec<(RegisterKey, Versioned)>;

    /// One register, read without faults.
    fn register(&self, key: &RegisterKey) -> Option<Versioned> {
        self.registers()
            .into_iter()
            .find(|(k, _)| k == key)
            .map(|(_, register)| register)
    }
}

/// An in-memory store behind one [`FaultyStore`] per node.
struct Memory {
    memory: MemoryControlStore,
    handles: RefCell<Vec<FaultyStore<MemoryControlStore>>>,
}

impl World for Memory {
    type Store = FaultyStore<MemoryControlStore>;
    const PROBES: bool = false;

    fn handle(&self, context: &mut SimContext) -> Self::Store {
        let store = FaultyStore::seeded(self.memory.clone(), context.fork_seed(), RATES);
        self.handles.borrow_mut().push(store.clone());
        store
    }

    fn registers(&self) -> Vec<(RegisterKey, Versioned)> {
        self.memory.registers()
    }
}

impl Memory {
    fn stats(&self) -> FaultStats {
        let mut stats = FaultStats::default();
        for store in self.handles.borrow().iter() {
            let node = store.stats();
            stats.requests += node.requests;
            stats.lost_requests += node.lost_requests;
            stats.lost_responses += node.lost_responses;
            stats.late_requests += node.late_requests;
            stats.conflicts += node.conflicts;
            stats.unavailable += node.unavailable;
        }
        stats
    }
}

/// One simulated S3 bucket with faults, shared by every node's
/// [`S3ControlStore`].
struct S3 {
    objects: SimS3,
}

impl World for S3 {
    type Store = S3ControlStore<SimS3>;
    const PROBES: bool = true;

    fn handle(&self, _: &mut SimContext) -> Self::Store {
        let config = S3StoreConfig {
            prefix: PREFIX.to_owned(),
            poll_interval: Duration::from_millis(20),
        };
        S3ControlStore::new(self.objects.clone(), config).unwrap()
    }

    fn registers(&self) -> Vec<(RegisterKey, Versioned)> {
        self.objects
            .keys()
            .into_iter()
            .filter_map(|key| {
                let register = RegisterKey::new(key.strip_prefix(PREFIX)?).ok()?;
                let object = self.objects.object(&key)?;
                let version = Version::new(object.info.etag.as_str());
                Some((
                    register,
                    Versioned {
                        value: object.body,
                        version,
                    },
                ))
            })
            .collect()
    }
}

/// Retries a read until the faulty store answers.
async fn read_until_answered<T, F>(mut request: impl FnMut() -> F) -> TestResult<T>
where
    F: Future<Output = Result<T, ControlError>>,
{
    for _ in 0..POLICY.max_attempts {
        match request().await {
            Ok(value) => return Ok(value),
            Err(error) if error.is_retryable() => tokio::time::sleep(POLICY.initial_backoff).await,
            Err(error) => return Err(error.into()),
        }
    }
    Err("a read never got an answer".into())
}

/// What one node did.
#[derive(Debug, Default)]
struct NodeLog {
    bootstrap: Option<Bootstrap>,
    /// The counter values this node was told it wrote.
    acknowledged: Vec<u64>,
    /// The generations its increments returned, in order.
    generations: Vec<Generation>,
}

/// Runs the startup probe with three racing writers until it passes. A
/// probe that only failed to get answers is run again; a refusal fails the
/// seed.
async fn probe<S: ControlStore>(store: &S, seed: u64) -> TestResult {
    let probe = ControlProbe::new(seed)
        .with_rounds(20, 3)
        .with_policy(POLICY);
    let writers = [store.clone(), store.clone(), store.clone()];
    loop {
        match probe.run(&writers).await {
            Ok(()) => return Ok(()),
            Err(error) if !error.refuses_store() => {}
            Err(error) => return Err(error.into()),
        }
    }
}

async fn node<W: World>(
    world: Rc<W>,
    store: W::Store,
    seed: u64,
    log: Rc<RefCell<NodeLog>>,
) -> TestResult {
    if W::PROBES {
        probe(&store, seed).await?;
    }
    let mut ids = ProposalIds::seeded(seed);
    // A retried bootstrap keeps its proposal, so a creation whose answer
    // was lost is still reported as one.
    let proposal = ids.next_id();
    let outcome = bootstrap(&store, &cluster_id(), proposal, &POLICY).await?;
    log.borrow_mut().bootstrap = Some(outcome);

    let key = counter_key();
    while (log.borrow().acknowledged.len() as u64) < INCREMENTS {
        let current = match read_until_answered(|| store.get(&key)).await? {
            Some(current) => current,
            None => {
                let created = store
                    .put_if(&key, Expected::Absent, counter_value(0, &ids.next_id()))
                    .await;
                if let Err(error) = created
                    && !error.is_retryable()
                {
                    return Err(error.into());
                }
                continue;
            }
        };
        let next = counter_of(&current.value)? + 1;
        let proposal = ids.next_id();
        let expected = Expected::Version(current.version);
        let value = counter_value(next, &proposal);
        match propose(&store, &key, expected, value, &proposal, &POLICY).await? {
            ProposalOutcome::Accepted(_) => {
                log.borrow_mut().acknowledged.push(next);
                let generation = bump_generation(&store, &cluster_id(), &mut ids, &POLICY).await?;
                log.borrow_mut().generations.push(generation);
            }
            ProposalOutcome::Rejected => {
                // The lost-response rule never rejects a write that is
                // the register's value.
                let stored = world.register(&key).ok_or("the counter is gone")?;
                if proposal_id_of(&stored.value) == Some(proposal) {
                    return Err(format!("counter {next} was rejected but is stored").into());
                }
            }
        }
    }
    Ok(())
}

/// Follows a change stream until every node has finished and the stream
/// has reported the final generation, and returns the versions it holds.
async fn reader<W: World>(
    world: Rc<W>,
    store: W::Store,
    finished: Rc<Cell<u64>>,
) -> TestResult<BTreeMap<RegisterKey, Version>> {
    // Wait for a node to bootstrap the cluster.
    while world.register(&RegisterKey::cluster()).is_none() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let mut changes = read_until_answered(|| store.changes(Generation::ZERO)).await?;
    let mut copies = BTreeMap::new();
    let mut generation = Generation::ZERO;
    loop {
        if let Ok(change) = tokio::time::timeout(Duration::from_millis(50), changes.next()).await {
            let change = match change {
                Ok(change) => change,
                // The stream keeps its state; the next call observes again.
                Err(error) if error.is_retryable() => continue,
                Err(error) => return Err(error.into()),
            };
            if change.generation <= generation {
                return Err(format!("generation {} after {generation}", change.generation).into());
            }
            generation = change.generation;
            if change.snapshot {
                copies.clear();
            }
            for (key, version) in change.registers {
                match version {
                    Some(version) => copies.insert(key, version),
                    None => copies.remove(&key),
                };
            }
        }
        if finished.get() == NODES
            && read_cluster(&store, &cluster_id(), &POLICY)
                .await?
                .value
                .generation
                == generation
        {
            return Ok(copies);
        }
    }
}

/// The outcome of one run, to compare replays.
#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    cluster: ClusterDocument,
    counter: u64,
    acknowledged: Vec<Vec<u64>>,
    elapsed: Duration,
}

fn race<W: World>(context: &mut SimContext, world: W) -> TestResult<(Outcome, Rc<W>)> {
    let world = Rc::new(world);
    let mut builder = context.builder();
    builder.simulation_duration(Duration::from_secs(600));
    let mut sim = builder.build();

    let finished = Rc::new(Cell::new(0));
    let mut logs = Vec::new();
    for n in 0..NODES {
        let store = world.handle(context);
        let log = Rc::new(RefCell::new(NodeLog::default()));
        let (seed, finished) = (context.fork_seed(), Rc::clone(&finished));
        let (task_world, task_log) = (Rc::clone(&world), Rc::clone(&log));
        sim.client(format!("node-{n}"), async move {
            node(task_world, store, seed, task_log).await?;
            finished.set(finished.get() + 1);
            Ok(())
        });
        logs.push(log);
    }
    let copies = Rc::new(RefCell::new(BTreeMap::new()));
    let (reader_world, reader_copies) = (Rc::clone(&world), Rc::clone(&copies));
    let reader_store = world.handle(context);
    sim.client("reader", async move {
        *reader_copies.borrow_mut() = reader(reader_world, reader_store, finished).await?;
        Ok(())
    });
    sim.run()?;

    // At most one node knows it created cluster.json, and every node
    // agrees on it. A creator whose answer was lost, and whose document
    // was replaced by an increment before it re-read, cannot tell its own
    // creation apart and reports the cluster as existing.
    let outcomes: Vec<Bootstrap> = logs
        .iter()
        .map(|log| {
            log.borrow()
                .bootstrap
                .clone()
                .ok_or("a node did not bootstrap")
        })
        .collect::<Result<_, _>>()?;
    let created: Vec<_> = outcomes
        .iter()
        .filter_map(|outcome| match outcome {
            Bootstrap::Created(cluster) => Some(&cluster.value),
            Bootstrap::Existing(_) => None,
        })
        .collect();
    if created.len() > 1 {
        return Err(format!("{} nodes created cluster.json", created.len()).into());
    }
    let mut first = created.first().copied();
    for outcome in &outcomes {
        let found = &outcome.cluster().value;
        if found.cluster_id != cluster_id() {
            return Err(format!("a node bootstrapped {found:?}").into());
        }
        if found.generation == skys3_control::FIRST_GENERATION {
            match first {
                Some(first) if first != found => {
                    return Err(format!("nodes disagree: {found:?} and {first:?}").into());
                }
                _ => first = Some(found),
            }
        }
    }

    // No counter value was acknowledged twice, and each node's
    // generations grew.
    let mut acknowledged = BTreeSet::new();
    for log in &logs {
        let log = log.borrow();
        for value in &log.acknowledged {
            if !acknowledged.insert(*value) {
                return Err(format!("counter {value} was acknowledged twice").into());
            }
        }
        if !log.generations.is_sorted_by(|a, b| a < b) {
            return Err(format!("generations went back: {:?}", log.generations).into());
        }
    }
    let stored = world
        .register(&counter_key())
        .ok_or("the counter does not exist")?;
    let counter = counter_of(&stored.value)?;
    let highest = acknowledged.last().copied().unwrap_or(0);
    if counter < highest || (acknowledged.len() as u64) != NODES * INCREMENTS {
        return Err(format!("counter {counter} after acknowledging {acknowledged:?}").into());
    }

    // The reader's copies match the store, and the probes left nothing
    // behind.
    let registers = world.registers();
    let expected: BTreeMap<_, _> = registers
        .iter()
        .filter(|(key, _)| key.starts_with(&KeyPrefix::shards()))
        .map(|(key, register)| (key.clone(), register.version.clone()))
        .collect();
    if *copies.borrow() != expected {
        return Err(format!("the reader holds {:?}, not {expected:?}", copies.borrow()).into());
    }
    let keys: Vec<_> = registers.iter().map(|(key, _)| key.as_str()).collect();
    if keys != ["cluster.json", "shards/b-1/0.json"] {
        return Err(format!("the store holds {keys:?}").into());
    }

    let cluster = world
        .register(&RegisterKey::cluster())
        .ok_or("cluster.json does not exist")?;
    let outcome = Outcome {
        cluster: skys3_types::RegisterDocument::from_json(&cluster.value)?,
        counter,
        acknowledged: logs
            .iter()
            .map(|log| log.borrow().acknowledged.clone())
            .collect(),
        elapsed: sim.elapsed(),
    };
    Ok((outcome, world))
}

fn memory_race(context: &mut SimContext) -> TestResult<(Outcome, FaultStats)> {
    let world = Memory {
        memory: MemoryControlStore::new(),
        handles: RefCell::default(),
    };
    let (outcome, world) = race(context, world)?;
    Ok((outcome, world.stats()))
}

fn s3_race(context: &mut SimContext) -> TestResult<(Outcome, SimS3Stats)> {
    let objects = SimS3::new(context.fork_seed(), SimS3Config::default());
    objects.set_faults(S3_FAULTS);
    let (outcome, world) = race(context, S3 { objects })?;
    Ok((outcome, world.objects.stats()))
}

#[test]
fn racing_nodes_agree_on_one_cluster_and_never_share_an_increment() {
    Runner::new().run(|context| memory_race(context).map(drop));
}

#[test]
fn racing_nodes_probe_and_share_an_s3_control_store() {
    Runner::new().run(|context| s3_race(context).map(drop));
}

#[test]
fn the_scenario_injects_every_fault() {
    let mut total = FaultStats::default();
    let mut s3 = SimS3Stats::default();
    Runner::with_seeds(SeedSet::Range(0..4)).run(|context| {
        let (_, stats) = memory_race(context)?;
        total.lost_requests += stats.lost_requests;
        total.lost_responses += stats.lost_responses;
        total.late_requests += stats.late_requests;
        total.conflicts += stats.conflicts;
        total.unavailable += stats.unavailable;
        let (_, stats) = s3_race(context)?;
        s3.internal_errors += stats.internal_errors;
        s3.slow_downs += stats.slow_downs;
        s3.lost_requests += stats.lost_requests;
        s3.lost_responses += stats.lost_responses;
        s3.conflicts += stats.conflicts;
        Ok(())
    });
    let counts = [
        total.lost_requests,
        total.lost_responses,
        total.late_requests,
        total.conflicts,
        total.unavailable,
        s3.internal_errors,
        s3.slow_downs,
        s3.lost_requests,
        s3.lost_responses,
        s3.conflicts,
    ];
    assert!(counts.iter().all(|&count| count > 0), "{total:?} {s3:?}");
}

#[test]
fn the_scenario_replays_exactly() {
    let run = |seed| memory_race(&mut SimContext::new(seed)).unwrap();
    let first = run(1);
    assert_eq!(run(1), first);
    assert_ne!(run(2), first);
    let run = |seed| s3_race(&mut SimContext::new(seed)).unwrap();
    let first = run(1);
    assert_eq!(run(1), first);
    assert_ne!(run(2), first);
}
