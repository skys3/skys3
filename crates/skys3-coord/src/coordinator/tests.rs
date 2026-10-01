use std::sync::Mutex;

use skys3_control::faults::{Fault, FaultRates, FaultyStore};
use skys3_control::{MemoryControlStore, TypedKey, Version, bootstrap, read, read_cluster};
use skys3_io::MonotonicClock;
use skys3_types::{BucketId, Epoch, Generation, NodeId, ShardConfig, ShardId};

use super::*;
use crate::lease::{Elector, LeaseConfig};
use crate::push::ControlHints;

/// Every wait in these tests, in paused (virtual) time.
const WAIT: Duration = Duration::from_secs(600);

fn cluster() -> ClusterId {
    ClusterId::new("prod").unwrap()
}

fn node(n: u8) -> NodeId {
    NodeId::new(format!("node-{n}")).unwrap()
}

fn key() -> TypedKey<ShardConfig> {
    TypedKey::shard(&BucketId::new("b-1").unwrap(), ShardId::new(0))
}

fn retry() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 3,
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(100),
    }
}

fn config() -> CoordinatorConfig {
    CoordinatorConfig {
        cluster: cluster(),
        retry: retry(),
        idle: Duration::from_millis(100),
    }
}

/// An accepted write of [`Epochs`]: who wrote which epoch.
type Ledger = Arc<Mutex<Vec<(NodeId, u64)>>>;

/// A placement that moves one shard register to its next epoch on every
/// other plan, naming its node as primary, and records what it got.
struct Epochs {
    node: NodeId,
    rest: bool,
    planned: u64,
    ledger: Ledger,
    rejected: Arc<Mutex<usize>>,
}

impl Epochs {
    fn new(node: NodeId, ledger: &Ledger) -> Self {
        Self {
            node,
            rest: false,
            planned: 0,
            ledger: Arc::clone(ledger),
            rejected: Arc::default(),
        }
    }
}

impl Placement for Epochs {
    async fn plan<S: ControlStore>(
        &mut self,
        store: &S,
        proposals: &mut ProposalIds,
    ) -> Result<Option<ChangeSet>, ControlError> {
        self.rest = !self.rest;
        if !self.rest {
            return Ok(None);
        }
        let current = read(store, &key()).await?;
        self.planned = current.as_ref().map_or(1, |c| c.value.epoch.get() + 1);
        let document = ShardConfig {
            bucket_id: BucketId::new("b-1").unwrap(),
            shard: ShardId::new(0),
            epoch: Epoch::new(self.planned),
            primary: self.node.clone(),
            members: vec![self.node.clone()],
            learners: Vec::new(),
            min_write_replicas: 1,
            replicas: 1,
            proposal_id: proposals.next_id(),
        };
        let change = match current {
            None => ChangeSet::new().create(&key(), &document),
            Some(current) => ChangeSet::new().update(&key(), &current.version, &document),
        };
        Ok(Some(change.unwrap()))
    }

    fn applied(&mut self, _change: &ChangeSet, applied: &Applied) {
        if applied.is_complete() {
            let mut ledger = self.ledger.lock().unwrap();
            ledger.push((self.node.clone(), self.planned));
        } else {
            *self.rejected.lock().unwrap() += 1;
        }
    }
}

fn clock() -> Arc<dyn Clock> {
    Arc::new(MonotonicClock::new())
}

/// Leadership that says "coordinator" for good, as a node that wrongly
/// believes it holds the lease would.
fn forged() -> (watch::Sender<Leadership>, watch::Receiver<Leadership>) {
    watch::channel(Leadership::Coordinator {
        version: Version::new("forged"),
        until: MonoTime::MAX,
    })
}

async fn store() -> MemoryControlStore {
    let store = MemoryControlStore::new();
    let id = ProposalIds::seeded(0).next_id();
    bootstrap(&store, &cluster(), id, &retry()).await.unwrap();
    store
}

async fn generation(store: &impl ControlStore) -> Generation {
    read_cluster(store, &cluster(), &retry())
        .await
        .unwrap()
        .value
        .generation
}

#[tokio::test(start_paused = true)]
async fn a_coordinator_changes_only_during_its_tenure_and_announces_each_change() {
    let store = store().await;
    let clock = clock();
    let (leadership, follower) = watch::channel(Leadership::Follower { holder: None });
    let ledger = Ledger::default();
    let hints = ControlHints::new();
    let coordinator = Coordinator::new(
        store.clone(),
        Arc::clone(&clock),
        follower,
        Epochs::new(node(1), &ledger),
        hints.clone(),
        config(),
        ProposalIds::seeded(1),
    );
    assert!(format!("{coordinator:?}").starts_with("Coordinator"));
    let waker = coordinator.waker();
    let running = tokio::spawn(coordinator.run());
    clock.sleep(Duration::from_secs(5)).await;
    assert!(ledger.lock().unwrap().is_empty());

    // A tenure of two seconds.
    let until = clock.now().saturating_add(Duration::from_secs(2));
    leadership.send_replace(Leadership::Coordinator {
        version: Version::new("1"),
        until,
    });
    clock.sleep(Duration::from_secs(5)).await;
    let made = ledger.lock().unwrap().len();
    assert!((5..=20).contains(&made), "{made} changes");
    // Every change is announced by its own generation, and pushed.
    assert_eq!(generation(&store).await, Generation::new(1 + made as u64));
    assert_eq!(hints.latest(), Generation::new(1 + made as u64));
    let current = read(&store, &key()).await.unwrap().unwrap();
    assert_eq!(current.value.epoch, Epoch::new(made as u64));

    // Waking a coordinator out of tenure changes nothing.
    waker.notify_one();
    clock.sleep(Duration::from_secs(1)).await;
    assert_eq!(ledger.lock().unwrap().len(), made);

    // The coordinator stops when its elector does.
    drop(leadership);
    tokio::time::timeout(WAIT, running).await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn two_nodes_that_both_believe_they_coordinate_only_compete() {
    let store = store().await;
    let ledger = Ledger::default();
    let mut tasks = Vec::new();
    let mut rejected = Vec::new();
    let mut senders = Vec::new();
    for n in 1..=2 {
        let (sender, leadership) = forged();
        senders.push(sender);
        let placement = Epochs::new(node(n), &ledger);
        rejected.push(Arc::clone(&placement.rejected));
        // Requests take a few milliseconds, so the two interleave.
        let delayed = FaultyStore::seeded(
            store.clone(),
            n.into(),
            FaultRates {
                max_delay: Duration::from_millis(5),
                ..FaultRates::default()
            },
        );
        let coordinator = Coordinator::new(
            delayed,
            clock(),
            leadership,
            placement,
            ControlHints::new(),
            CoordinatorConfig {
                // Both plan at the same moments, so they race.
                idle: Duration::from_millis(50),
                ..config()
            },
            ProposalIds::seeded(n.into()),
        );
        tasks.push(tokio::spawn(coordinator.run()));
    }
    tokio::time::sleep(Duration::from_secs(10)).await;
    // Both stop believing, and finish the change they are making.
    for sender in &senders {
        sender.send_replace(Leadership::Follower { holder: None });
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    for task in &tasks {
        task.abort();
    }
    let ledger = ledger.lock().unwrap().clone();
    // Each epoch was written once, by one of them: no change was lost or
    // overwritten unseen.
    let mut epochs: Vec<u64> = ledger.iter().map(|(_, epoch)| *epoch).collect();
    epochs.sort_unstable();
    assert_eq!(epochs, (1..=ledger.len() as u64).collect::<Vec<_>>());
    let final_value = read(&store, &key()).await.unwrap().unwrap().value;
    let last = ledger.iter().max_by_key(|(_, epoch)| *epoch).unwrap();
    assert_eq!(final_value.epoch, Epoch::new(last.1));
    assert_eq!(final_value.primary, last.0);
    assert!(ledger.iter().any(|(n, _)| *n == node(1)));
    assert!(ledger.iter().any(|(n, _)| *n == node(2)));
    let lost: usize = rejected.iter().map(|r| *r.lock().unwrap()).sum();
    assert!(lost > 0, "the coordinators never raced");
    assert!(generation(&store).await >= Generation::new(1 + ledger.len() as u64 / 2));
}

#[tokio::test(start_paused = true)]
async fn placement_work_moves_with_the_lease() {
    let store = store().await;
    let clock = clock();
    let lease = LeaseConfig::new(Duration::from_secs(3), 0.01)
        .unwrap()
        .with_retry(retry());
    let ledger = Ledger::default();
    let mut nodes = Vec::new();
    for n in 1..=2 {
        let elector = Elector::new(
            store.clone(),
            node(n),
            Arc::clone(&clock),
            lease,
            ProposalIds::seeded(n.into()),
        );
        let coordinator = Coordinator::new(
            store.clone(),
            Arc::clone(&clock),
            elector.subscribe(),
            Epochs::new(node(n), &ledger),
            ControlHints::new(),
            config(),
            ProposalIds::seeded(10 + u64::from(n)),
        );
        nodes.push((tokio::spawn(elector.run()), tokio::spawn(coordinator.run())));
        // Node 1 takes the free lease first.
        clock.sleep(Duration::from_millis(10)).await;
    }
    clock.sleep(Duration::from_secs(10)).await;
    let before = ledger.lock().unwrap().clone();
    assert!(!before.is_empty());
    assert!(before.iter().all(|(n, _)| *n == node(1)), "{before:?}");

    // Node 1 stops; node 2 takes over within about the takeover wait plus
    // one read interval, and placement resumes there.
    let (elector, coordinator) = nodes.remove(0);
    elector.abort();
    coordinator.abort();
    let stopped = clock.now();
    let resumed = tokio::time::timeout(WAIT, async {
        loop {
            clock.sleep(Duration::from_millis(50)).await;
            if ledger.lock().unwrap().iter().any(|(n, _)| *n == node(2)) {
                return clock.now();
            }
        }
    })
    .await
    .unwrap();
    let delay = resumed.saturating_duration_since(stopped);
    assert!(
        delay <= lease.takeover_after() + lease.renew_interval() + Duration::from_millis(500),
        "{delay:?}"
    );
    let ledger = ledger.lock().unwrap().clone();
    let mut epochs: Vec<u64> = ledger.iter().map(|(_, epoch)| *epoch).collect();
    epochs.sort_unstable();
    epochs.dedup();
    assert_eq!(epochs.len(), ledger.len());
}

#[tokio::test(start_paused = true)]
async fn failed_plans_and_changes_are_tried_again_later() {
    let memory = store().await;
    let store = FaultyStore::new(memory.clone());
    let clock = clock();
    let (_leadership, believer) = forged();
    let ledger = Ledger::default();
    let coordinator = Coordinator::new(
        store.clone(),
        Arc::clone(&clock),
        believer,
        Epochs::new(node(1), &ledger),
        ControlHints::new(),
        config(),
        ProposalIds::seeded(1),
    );
    // The plan's read fails, then the change's writes do.
    store.script(
        [Fault::Unavailable]
            .into_iter()
            .chain([Fault::Pass])
            .chain(std::iter::repeat_n(Fault::Unavailable, 3)),
    );
    let running = tokio::spawn(coordinator.run());
    tokio::time::timeout(WAIT, async {
        while ledger.lock().unwrap().is_empty() {
            clock.sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(store.stats().unavailable >= 4);
    running.abort();

    // A coordinator with no placement holds its tenure and changes
    // nothing.
    let (_leadership, believer) = forged();
    let idle = Coordinator::new(
        memory.clone(),
        Arc::clone(&clock),
        believer,
        NoPlacement,
        ControlHints::new(),
        config(),
        ProposalIds::seeded(2),
    );
    let before = generation(&memory).await;
    let running = tokio::spawn(idle.run());
    clock.sleep(Duration::from_secs(2)).await;
    assert_eq!(generation(&memory).await, before);
    running.abort();
}

#[tokio::test(start_paused = true)]
async fn a_change_planned_as_the_tenure_ends_is_dropped() {
    /// Plans a change that takes longer than the tenure to plan.
    struct Slow(Arc<dyn Clock>);
    impl Placement for Slow {
        async fn plan<S: ControlStore>(
            &mut self,
            _store: &S,
            proposals: &mut ProposalIds,
        ) -> Result<Option<ChangeSet>, ControlError> {
            self.0.sleep(Duration::from_secs(2)).await;
            let document = ShardConfig {
                bucket_id: BucketId::new("b-1").unwrap(),
                shard: ShardId::new(0),
                epoch: Epoch::new(1),
                primary: node(1),
                members: vec![node(1)],
                learners: Vec::new(),
                min_write_replicas: 1,
                replicas: 1,
                proposal_id: proposals.next_id(),
            };
            Ok(Some(ChangeSet::new().create(&key(), &document).unwrap()))
        }
    }
    let store = store().await;
    let clock = clock();
    let (_leadership, believer) = watch::channel(Leadership::Coordinator {
        version: Version::new("1"),
        until: clock.now().saturating_add(Duration::from_secs(1)),
    });
    let coordinator = Coordinator::new(
        store.clone(),
        Arc::clone(&clock),
        believer,
        Slow(Arc::clone(&clock)),
        ControlHints::new(),
        config(),
        ProposalIds::seeded(1),
    );
    let running = tokio::spawn(coordinator.run());
    clock.sleep(Duration::from_secs(5)).await;
    assert!(read(&store, &key()).await.unwrap().is_none());
    assert_eq!(generation(&store).await, Generation::new(1));
    running.abort();
}
