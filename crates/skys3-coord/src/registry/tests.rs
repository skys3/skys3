use std::sync::Mutex;

use skys3_control::faults::FaultyStore;
use skys3_control::{Expected, MemoryControlStore, RetryPolicy, bootstrap, propose_document};
use skys3_io::MonotonicClock;
use skys3_types::{BucketId, ClusterId, DiskInfo, Epoch, Label, ProposalId, ShardId};
use tokio::sync::watch;

use super::*;
use crate::change::apply;
use crate::coordinator::{Coordinator, CoordinatorConfig, NoPlacement};
use crate::join::{NodeProfile, register};
use crate::lease::Leadership;
use crate::push::ControlHints;

/// Every wait in these tests, in paused (virtual) time.
const WAIT: Duration = Duration::from_secs(3600);

const SUSPECT: Duration = Duration::from_secs(10);
const FORGET: Duration = Duration::from_secs(60);

fn cluster() -> ClusterId {
    ClusterId::new("prod").unwrap()
}

fn node(n: u8) -> NodeId {
    NodeId::new(format!("node-{n}")).unwrap()
}

fn retry() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 3,
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(10),
    }
}

fn profile(n: u8) -> NodeProfile {
    NodeProfile {
        node: node(n),
        address: format!("10.0.0.{n}:7400").parse().unwrap(),
        zone: None,
        rack: Some(Label::new(format!("rack-{}", n % 2)).unwrap()),
        disks: vec![DiskInfo {
            disk_id: Label::new("nvme0").unwrap(),
            capacity_bytes: 1 << 40,
        }],
    }
}

fn config() -> RegistryConfig {
    RegistryConfig {
        suspect_after: SUSPECT,
        forget_after: FORGET,
    }
}

fn registry() -> NodeRegistry {
    NodeRegistry::new(Arc::new(MonotonicClock::new()), config())
}

/// A bootstrapped store in which nodes `nodes` are registered.
async fn store(nodes: &[u8]) -> MemoryControlStore {
    let store = MemoryControlStore::new();
    let mut ids = ProposalIds::seeded(1);
    bootstrap(&store, &cluster(), ids.next_id(), &retry())
        .await
        .unwrap();
    for n in nodes {
        register(&store, &cluster(), &profile(*n), &mut ids, &retry())
            .await
            .unwrap();
    }
    store
}

/// Writes shard 0 of bucket `b-1` with `members`, the first as primary.
async fn shard(store: &MemoryControlStore, members: &[u8]) {
    let key = TypedKey::shard(&BucketId::new("b-1").unwrap(), ShardId::new(0));
    let current = read(store, &key).await.unwrap();
    let members: Vec<NodeId> = members.iter().map(|n| node(*n)).collect();
    let document = ShardConfig {
        bucket_id: BucketId::new("b-1").unwrap(),
        shard: ShardId::new(0),
        epoch: Epoch::new(current.as_ref().map_or(1, |c| c.value.epoch.get() + 1)),
        primary: members[0].clone(),
        members,
        learners: Vec::new(),
        min_write_replicas: 1,
        replicas: 1,
        proposal_id: ProposalIds::from_os_rng().next_id(),
    };
    let expected = current.map_or(Expected::Absent, |c| Expected::Version(c.version));
    let outcome = propose_document(store, &key, expected, &document, &retry())
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        skys3_control::ProposalOutcome::Accepted(_)
    ));
}

fn states(registry: &NodeRegistry) -> Vec<(NodeId, NodeState)> {
    registry
        .entries()
        .into_iter()
        .map(|entry| (entry.registration.node_id, entry.state))
        .collect()
}

#[tokio::test(start_paused = true)]
async fn health_follows_heartbeats_and_is_judged_afresh_each_tenure() {
    let store = store(&[1, 2]).await;
    let registry = registry();
    registry.begin_tenure();
    registry.refresh(&store).await.unwrap();
    assert_eq!(
        states(&registry),
        [(node(1), NodeState::Live), (node(2), NodeState::Live)]
    );
    let entry = registry.get(&node(1)).unwrap();
    assert!(profile(1).describes(&entry.registration));
    assert_eq!(
        registry.peers(),
        BTreeMap::from([(node(1), profile(1).address), (node(2), profile(2).address)])
    );

    tokio::time::advance(SUSPECT).await;
    assert!(registry.heard(&node(1)));
    assert_eq!(
        states(&registry),
        [(node(1), NodeState::Live), (node(2), NodeState::Suspect)]
    );
    assert_eq!(registry.get(&node(2)).unwrap().silent_for, SUSPECT);

    tokio::time::advance(FORGET).await;
    assert_eq!(
        states(&registry),
        [
            (node(1), NodeState::Departing),
            (node(2), NodeState::Departing)
        ]
    );
    assert_eq!(
        registry
            .silent()
            .into_iter()
            .map(|entry| entry.registration.node_id)
            .collect::<Vec<_>>(),
        [node(1), node(2)]
    );

    // A new tenure has heard nothing yet, so it suspects no one.
    registry.begin_tenure();
    assert_eq!(
        states(&registry),
        [(node(1), NodeState::Live), (node(2), NodeState::Live)]
    );
    assert!(registry.get(&node(3)).is_none());
}

#[tokio::test(start_paused = true)]
async fn a_node_is_told_it_is_unregistered_only_after_a_listing_in_the_tenure() {
    let store = store(&[1]).await;
    let registry = registry();
    let wake = Arc::new(Notify::new());
    registry.set_waker(Arc::clone(&wake));

    // Nothing listed yet: every node counts as registered.
    assert!(registry.heard(&node(2)));
    registry.begin_tenure();
    registry.refresh(&store).await.unwrap();
    assert!(registry.heard(&node(1)));
    assert!(!registry.heard(&node(2)));
    tokio::time::timeout(WAIT, wake.notified()).await.unwrap();

    // Once it registers, the next listing finds it, and a new
    // registration counts as hearing from the node.
    tokio::time::advance(SUSPECT * 2).await;
    register(
        &store,
        &cluster(),
        &profile(2),
        &mut ProposalIds::seeded(2),
        &retry(),
    )
    .await
    .unwrap();
    registry.refresh(&store).await.unwrap();
    assert!(registry.heard(&node(2)));
    assert_eq!(registry.get(&node(2)).unwrap().state, NodeState::Live);

    // A new tenure lists again before it tells anyone.
    registry.begin_tenure();
    assert!(registry.heard(&node(5)));
}

#[tokio::test(start_paused = true)]
async fn a_listing_skips_registers_that_do_not_parse_or_name_another_node() {
    let store = store(&[1]).await;
    let bad = RegisterKey::new("nodes/node-7.json").unwrap();
    let _ = store
        .put_if(&bad, Expected::Absent, bytes::Bytes::from_static(b"{}"))
        .await
        .unwrap();
    let misnamed = profile(1).document(ProposalId::from_u128(9));
    let _ = store
        .put_if(
            &RegisterKey::new("nodes/node-8.json").unwrap(),
            Expected::Absent,
            misnamed.to_json().unwrap().into(),
        )
        .await
        .unwrap();
    let registry = registry();
    registry.refresh(&store).await.unwrap();
    assert_eq!(states(&registry), [(node(1), NodeState::Live)]);
}

#[tokio::test(start_paused = true)]
async fn a_refresh_reads_only_registrations_that_changed() {
    let store = FaultyStore::new(store(&[1, 2, 3]).await);
    let registry = registry();
    registry.refresh(&store).await.unwrap();
    let before = store.requests();
    registry.refresh(&store).await.unwrap();
    assert_eq!(store.requests() - before, 1, "only the listing");

    let mut moved = profile(2);
    moved.address = "10.0.9.2:7400".parse().unwrap();
    register(
        store.inner(),
        &cluster(),
        &moved,
        &mut ProposalIds::seeded(3),
        &retry(),
    )
    .await
    .unwrap();
    let before = store.requests();
    registry.refresh(&store).await.unwrap();
    assert_eq!(store.requests() - before, 2, "the listing and one read");
    assert_eq!(registry.peers()[&node(2)], moved.address);
}

/// Records every peer set it is given.
#[derive(Clone, Default)]
struct Peers(Arc<Mutex<Vec<BTreeMap<NodeId, NodeAddress>>>>);

impl PeerSink for Peers {
    fn set_peers(&self, peers: BTreeMap<NodeId, NodeAddress>) {
        self.0.lock().unwrap().push(peers);
    }
}

/// What one lifecycle round did to a node's registration.
#[derive(Debug, PartialEq, Eq)]
enum Did {
    Marked(NodeId),
    Forgot(NodeId),
}

/// Plans with `lifecycle` and applies what it planned, as the coordinator
/// does. Returns what the round did to node registrations.
async fn step<P: Placement, R: Rehoming>(
    lifecycle: &mut Lifecycle<P, R>,
    store: &MemoryControlStore,
) -> Vec<Did> {
    let mut ids = ProposalIds::seeded(4);
    let Some(change) = lifecycle.plan(store, &mut ids).await.unwrap() else {
        return Vec::new();
    };
    let applied = apply(store, &cluster(), &change, &mut ids, &retry())
        .await
        .unwrap();
    lifecycle.applied(&change, &applied);
    applied
        .written
        .into_iter()
        .filter_map(|(key, version)| match (key.kind(), version) {
            (RegisterKind::Node(node), None) => Some(Did::Forgot(node)),
            (RegisterKind::Node(node), Some(_)) => Some(Did::Marked(node)),
            _ => None,
        })
        .collect()
}

/// Whether node `n`'s registration exists and carries the mark.
async fn marked(store: &MemoryControlStore, n: u8) -> Option<bool> {
    read(store, &TypedKey::node(&node(n)))
        .await
        .unwrap()
        .map(|current| current.value.departing)
}

#[tokio::test(start_paused = true)]
async fn a_silent_node_is_marked_then_forgotten_once_no_shard_names_it() {
    let store = store(&[1, 2, 3]).await;
    shard(&store, &[1, 3]).await;
    let peers = Peers::default();
    let mut lifecycle = Lifecycle::new(registry(), NoPlacement).with_peers(peers.clone());
    lifecycle.begin_tenure();
    assert!(step(&mut lifecycle, &store).await.is_empty());
    let last_peers = || {
        peers
            .0
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .keys()
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(last_peers(), [node(1), node(2), node(3)]);

    // Node 3 keeps sending heartbeats; nodes 1 and 2 fall silent, and are
    // marked one round each, before anything is deleted.
    for _ in 0..7 {
        tokio::time::advance(FORGET / 6).await;
        lifecycle.registry().heard(&node(3));
    }
    assert_eq!(step(&mut lifecycle, &store).await, [Did::Marked(node(1))]);
    assert_eq!(step(&mut lifecycle, &store).await, [Did::Marked(node(2))]);
    assert_eq!(marked(&store, 2).await, Some(true));
    assert_eq!(marked(&store, 3).await, Some(false));
    // The mark is not news from the node: it stays departing.
    assert_eq!(step(&mut lifecycle, &store).await, [Did::Forgot(node(2))]);
    assert_eq!(marked(&store, 2).await, None);
    assert!(lifecycle.registry().get(&node(2)).is_none());
    assert_eq!(last_peers().len(), 3, "peers are set before the change");

    // Node 1 is marked too, but shard 0 still names it.
    assert_eq!(
        lifecycle.registry().get(&node(1)).unwrap().state,
        NodeState::Departing
    );
    assert!(step(&mut lifecycle, &store).await.is_empty());
    assert_eq!(last_peers(), [node(1), node(3)]);
    // Once placement re-homes the shard, node 1 is forgotten as well.
    shard(&store, &[3]).await;
    assert_eq!(step(&mut lifecycle, &store).await, [Did::Forgot(node(1))]);
    assert!(step(&mut lifecycle, &store).await.is_empty());
    assert_eq!(
        states(&lifecycle.registry().clone()),
        [(node(3), NodeState::Live)]
    );
}

/// A competing coordinator's placement, as one that briefly overlaps this
/// coordinator would run it: it reads node 2's registration when it plans
/// and, unless the registration is marked `departing`, adds node 2 to
/// shard 0. It runs right after this coordinator's rehoming scan, the
/// worst moment for it.
struct Competitor {
    scan: ShardScan,
    store: MemoryControlStore,
    assigned: bool,
}

impl Rehoming for Competitor {
    async fn still_needed<S: ControlStore>(
        &mut self,
        store: &S,
        departing: &BTreeSet<NodeId>,
    ) -> Result<BTreeSet<NodeId>, ControlError> {
        let needed = self.scan.still_needed(store, departing).await?;
        if marked(&self.store, 2).await == Some(false) {
            shard(&self.store, &[3, 2]).await;
            self.assigned = true;
        }
        Ok(needed)
    }
}

#[tokio::test(start_paused = true)]
async fn a_competing_placement_never_assigns_a_node_being_forgotten() {
    let store = store(&[2, 3]).await;
    shard(&store, &[3]).await;
    let competitor = Competitor {
        scan: ShardScan::default(),
        store: store.clone(),
        assigned: false,
    };
    let mut lifecycle = Lifecycle::new(registry(), NoPlacement).with_rehoming(competitor);
    lifecycle.begin_tenure();
    assert!(step(&mut lifecycle, &store).await.is_empty());
    for _ in 0..7 {
        tokio::time::advance(FORGET / 6).await;
        lifecycle.registry().heard(&node(3));
    }
    for _ in 0..4 {
        let _ = step(&mut lifecycle, &store).await;
    }
    // The registration was marked before any scan, so the competitor saw
    // the mark and assigned nothing, and node 2 was forgotten. Forgetting
    // it in the round of the scan would have let the competitor add it to
    // shard 0 just before its registration was deleted.
    assert!(!lifecycle.rehoming.assigned);
    assert_eq!(marked(&store, 2).await, None);
    let key = TypedKey::shard(&BucketId::new("b-1").unwrap(), ShardId::new(0));
    let shard = read(&store, &key).await.unwrap().unwrap().value;
    for named in names(shard) {
        assert!(
            read(&store, &TypedKey::node(&named))
                .await
                .unwrap()
                .is_some(),
            "shard 0 names {named}, which is not registered"
        );
    }
}

/// A rehoming that needs no node.
struct Unneeded;

impl Rehoming for Unneeded {
    async fn still_needed<S: ControlStore>(
        &mut self,
        _store: &S,
        _departing: &BTreeSet<NodeId>,
    ) -> Result<BTreeSet<NodeId>, ControlError> {
        Ok(BTreeSet::new())
    }
}

#[tokio::test(start_paused = true)]
async fn a_node_that_registers_again_is_not_forgotten() {
    let store = store(&[1]).await;
    let mut lifecycle = Lifecycle::new(registry(), NoPlacement).with_rehoming(Unneeded);
    lifecycle.begin_tenure();
    assert!(step(&mut lifecycle, &store).await.is_empty());
    tokio::time::advance(FORGET).await;
    let mut ids = ProposalIds::seeded(5);
    let change = lifecycle.plan(&store, &mut ids).await.unwrap().unwrap();
    // The node restarts elsewhere between the plan and the mark.
    let mut moved = profile(1);
    moved.address = "10.0.9.1:7400".parse().unwrap();
    register(&store, &cluster(), &moved, &mut ids, &retry())
        .await
        .unwrap();
    let applied = apply(&store, &cluster(), &change, &mut ids, &retry())
        .await
        .unwrap();
    assert!(!applied.is_complete());
    lifecycle.applied(&change, &applied);
    // The next listing reads the new registration, which counts as
    // hearing from the node.
    assert!(step(&mut lifecycle, &store).await.is_empty());
    let entry = lifecycle.registry().get(&node(1)).unwrap();
    assert_eq!(
        (entry.state, entry.registration.address.clone()),
        (NodeState::Live, moved.address.clone())
    );

    // Silent again, it is marked; it then sends a heartbeat, is told to
    // register again, and its registration clears the mark, so it is not
    // forgotten.
    tokio::time::advance(FORGET).await;
    assert_eq!(step(&mut lifecycle, &store).await, [Did::Marked(node(1))]);
    lifecycle.registry().refresh(&store).await.unwrap();
    assert!(!lifecycle.registry().heard(&node(1)));
    let again = register(&store, &cluster(), &moved, &mut ids, &retry())
        .await
        .unwrap();
    assert_eq!(again.registration, crate::join::Registration::Updated);
    assert!(step(&mut lifecycle, &store).await.is_empty());
    assert_eq!(marked(&store, 1).await, Some(false));
    assert!(lifecycle.registry().heard(&node(1)));
    assert_eq!(
        lifecycle.registry().get(&node(1)).unwrap().state,
        NodeState::Live
    );
}

#[tokio::test(start_paused = true)]
async fn a_shard_scan_reads_only_changed_registers_and_keeps_nodes_it_cannot_judge() {
    let store = FaultyStore::new(store(&[]).await);
    shard(store.inner(), &[1, 2]).await;
    let departing = BTreeSet::from([node(2), node(3)]);
    let mut scan = ShardScan::default();
    assert_eq!(
        scan.still_needed(&store, &departing).await.unwrap(),
        BTreeSet::from([node(2)])
    );
    let before = store.requests();
    assert_eq!(
        scan.still_needed(&store, &departing).await.unwrap(),
        BTreeSet::from([node(2)])
    );
    assert_eq!(store.requests() - before, 1, "only the listing");

    // A shard register that does not parse might name anyone.
    let key = RegisterKey::new("shards/b-2/0.json").unwrap();
    let _ = store
        .inner()
        .put_if(&key, Expected::Absent, bytes::Bytes::from_static(b"{}"))
        .await
        .unwrap();
    assert_eq!(
        scan.still_needed(&store, &departing).await.unwrap(),
        departing
    );
}

#[tokio::test(start_paused = true)]
async fn the_coordinator_runs_the_lifecycle_and_starts_each_tenure_afresh() {
    let store = store(&[1, 2]).await;
    shard(&store, &[1]).await;
    let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::new());
    let registry = NodeRegistry::new(Arc::clone(&clock), config());
    let lifecycle = Lifecycle::new(registry.clone(), NoPlacement);
    let (leader, leadership) = watch::channel(Leadership::Follower { holder: None });
    let coordinator = Coordinator::new(
        store.clone(),
        Arc::clone(&clock),
        leadership,
        lifecycle,
        ControlHints::new(),
        CoordinatorConfig {
            cluster: cluster(),
            retry: retry(),
            idle: Duration::from_secs(1),
        },
        ProposalIds::seeded(6),
    );
    registry.set_waker(coordinator.waker());
    let task = tokio::spawn(coordinator.run());

    // Silence before the tenure does not count.
    tokio::time::advance(FORGET * 2).await;
    leader.send_replace(Leadership::Coordinator {
        version: Version::new("held"),
        until: MonoTime::MAX,
    });
    tokio::time::sleep(SUSPECT / 2).await;
    assert_eq!(
        states(&registry),
        [(node(1), NodeState::Live), (node(2), NodeState::Live)]
    );

    // Node 2 is forgotten once it has been silent for the whole of
    // `node_forget_after` in this tenure; node 1 holds a shard.
    let forgotten = async {
        while read(&store, &TypedKey::node(&node(2)))
            .await
            .unwrap()
            .is_some()
        {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    };
    tokio::time::timeout(WAIT, forgotten).await.unwrap();
    assert!(
        read(&store, &TypedKey::node(&node(1)))
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(states(&registry), [(node(1), NodeState::Departing)]);
    leader.send_replace(Leadership::Follower { holder: None });
    drop(leader);
    tokio::time::timeout(WAIT, task).await.unwrap().unwrap();
}

#[test]
fn the_configuration_follows_node_forget_after() {
    let config = RegistryConfig::new(Duration::from_secs(3));
    assert_eq!(config.suspect_after, Duration::from_secs(3));
    let text = "[cluster]\ncluster_id = \"prod\"\n\
                [control_store]\netcd_endpoints = [\"https://etcd:2379\"]\n\
                [replication]\nnode_forget_after_hours = 2\n";
    let config: skys3_config::Config = text.parse().unwrap();
    let config = RegistryConfig::from_config(&config);
    assert_eq!(config.forget_after, Duration::from_secs(2 * 3600));
    assert_eq!(config.suspect_after, RegistryConfig::DEFAULT_SUSPECT_AFTER);
}
