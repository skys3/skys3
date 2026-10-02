use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Mutex;

use skys3_control::{
    Expected, MemoryControlStore, ProposalOutcome, RetryPolicy, bootstrap, propose_document, read,
};
use skys3_io::MonotonicClock;
use skys3_types::{BucketId, ClusterId, DiskInfo, Label, NodeAddress, NodeRegistration, ShardId};

use super::*;
use crate::change::apply;
use crate::handoff::HandoffError;
use crate::join::{NodeProfile, register};
use crate::registry::RegistryConfig;
use crate::replace::{Replacement, ReplacementConfig};

const SUSPECT: Duration = Duration::from_secs(10);

fn cluster() -> ClusterId {
    ClusterId::new("prod").unwrap()
}

fn node(n: u8) -> NodeId {
    NodeId::new(format!("node-{n}")).unwrap()
}

fn nodes(ns: &[u8]) -> Vec<NodeId> {
    ns.iter().map(|n| node(*n)).collect()
}

fn retry() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 3,
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(10),
    }
}

fn bucket() -> BucketId {
    BucketId::new("b-1").unwrap()
}

fn key(shard: u8) -> TypedKey<ShardConfig> {
    TypedKey::shard(&bucket(), ShardId::new(shard))
}

/// How a test node registers.
#[derive(Clone, Copy)]
struct Shape {
    rack: Option<u8>,
    capacity: u64,
}

const PLAIN: Shape = Shape {
    rack: None,
    capacity: 1 << 40,
};

async fn bootstrapped() -> MemoryControlStore {
    let store = MemoryControlStore::new();
    bootstrap(
        &store,
        &cluster(),
        ProposalIds::seeded(1).next_id(),
        &retry(),
    )
    .await
    .unwrap();
    store
}

/// Registers node `n`.
async fn join(store: &MemoryControlStore, n: u8, shape: Shape) {
    let profile = NodeProfile {
        node: node(n),
        address: format!("10.0.0.{n}:7400").parse().unwrap(),
        zone: None,
        rack: shape
            .rack
            .map(|rack| Label::new(format!("rack-{rack}")).unwrap()),
        disks: vec![DiskInfo {
            disk_id: Label::new("nvme0").unwrap(),
            capacity_bytes: shape.capacity,
        }],
    };
    register(
        store,
        &cluster(),
        &profile,
        &mut ProposalIds::from_os_rng(),
        &retry(),
    )
    .await
    .unwrap();
}

/// A store in which nodes `0..count` are registered alike.
async fn store(count: u8) -> MemoryControlStore {
    let store = bootstrapped().await;
    for n in 0..count {
        join(&store, n, PLAIN).await;
    }
    store
}

/// Writes `config` over whatever the register of its shard holds.
async fn put(store: &MemoryControlStore, config: &ShardConfig) {
    let key = key(config.shard.get());
    let current = read(store, &key).await.unwrap();
    let expected = current.map_or(Expected::Absent, |c| Expected::Version(c.version));
    let outcome = propose_document(store, &key, expected, config, &retry())
        .await
        .unwrap();
    assert!(matches!(outcome, ProposalOutcome::Accepted(_)));
}

/// Writes shard `shard` with `members`, the first as primary, and
/// `learners`, in the next epoch.
async fn write_shard(
    store: &MemoryControlStore,
    shard: u8,
    members: &[u8],
    learners: &[u8],
) -> ShardConfig {
    let current = read(store, &key(shard)).await.unwrap();
    let config = ShardConfig {
        bucket_id: bucket(),
        shard: ShardId::new(shard),
        epoch: Epoch::new(current.as_ref().map_or(1, |c| c.value.epoch.get() + 1)),
        primary: node(members[0]),
        members: nodes(members),
        learners: nodes(learners),
        min_write_replicas: 1,
        replicas: 3,
        proposal_id: ProposalIds::from_os_rng().next_id(),
    };
    put(store, &config).await;
    config
}

async fn shard_register(store: &MemoryControlStore, shard: u8) -> ShardConfig {
    read(store, &key(shard)).await.unwrap().unwrap().value
}

async fn shards(store: &MemoryControlStore, count: u8) -> Vec<ShardConfig> {
    let mut all = Vec::new();
    for shard in 0..count {
        all.push(shard_register(store, shard).await);
    }
    all
}

/// Marks node `n`'s registration `departing`, as the lifecycle does.
async fn depart(store: &MemoryControlStore, n: u8) {
    let key = TypedKey::<NodeRegistration>::node(&node(n));
    let current = read(store, &key).await.unwrap().unwrap();
    let mut marked = current.value.clone();
    marked.departing = true;
    marked.proposal_id = ProposalIds::from_os_rng().next_id();
    let outcome = propose_document(
        store,
        &key,
        Expected::Version(current.version),
        &marked,
        &retry(),
    )
    .await
    .unwrap();
    assert!(matches!(outcome, ProposalOutcome::Accepted(_)));
}

/// The handoffs rebalancing asked for, answered at once.
#[derive(Clone, Default)]
struct Asked(Arc<Mutex<Vec<(NodeId, NodeAddress, Handoff)>>>);

impl Asked {
    fn take(&self) -> Vec<(NodeId, Handoff)> {
        std::mem::take(&mut *self.0.lock().unwrap())
            .into_iter()
            .map(|(primary, _, handoff)| (primary, handoff))
            .collect()
    }
}

impl RequestHandoff for Asked {
    fn request(
        &self,
        primary: NodeId,
        address: NodeAddress,
        handoff: Handoff,
    ) -> impl Future<Output = Result<(), HandoffError>> + Send + 'static {
        self.0.lock().unwrap().push((primary, address, handoff));
        async { Err(HandoffError::Refused("a test answers nothing".to_owned())) }
    }
}

/// A coordinator whose placement is rebalancing, inside replacement as in
/// a full coordinator.
struct Coordinator {
    registry: NodeRegistry,
    placement: Replacement<Rebalancing<Asked>>,
    asked: Asked,
    ids: ProposalIds,
    count: u8,
}

impl Coordinator {
    fn new(level: FailureDomain, config: RebalanceConfig) -> Self {
        let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::new());
        let registry = NodeRegistry::new(
            Arc::clone(&clock),
            RegistryConfig {
                suspect_after: SUSPECT,
                forget_after: Duration::from_secs(3600),
            },
        );
        let asked = Asked::default();
        let rebalancing = Rebalancing::new(
            registry.clone(),
            level,
            Arc::clone(&clock),
            config,
            asked.clone(),
        );
        let placement = Replacement::new(
            registry.clone(),
            level,
            clock,
            ReplacementConfig {
                removal_interval: Duration::ZERO,
                ..ReplacementConfig::default()
            },
        )
        .with_placement(rebalancing);
        Self {
            registry,
            placement,
            asked,
            ids: ProposalIds::seeded(5),
            count: 0,
        }
    }

    fn nodes() -> Self {
        Self::new(FailureDomain::Node, RebalanceConfig::default())
    }

    /// Hears from nodes `0..count`, so that none is suspect.
    fn hear(&self, count: u8) {
        for n in 0..count {
            self.registry.heard(&node(n));
        }
    }

    /// One round: lists the nodes and plans.
    async fn plan(&mut self, store: &MemoryControlStore) -> Option<ChangeSet> {
        self.registry.refresh(store).await.unwrap();
        self.placement.plan(store, &mut self.ids).await.unwrap()
    }

    /// One round, applying what it plans. Returns whether it changed
    /// anything.
    async fn round(&mut self, store: &MemoryControlStore) -> bool {
        let Some(change) = self.plan(store).await else {
            return false;
        };
        let applied = apply(store, &cluster(), &change, &mut self.ids, &retry())
            .await
            .unwrap();
        assert!(applied.is_complete(), "{applied:?}");
        self.placement.applied(&change, &applied);
        true
    }
}

/// What the primaries do: promote every learner, as once it caught up
/// (rule R3), and take the handoffs asked for, as the member named
/// proposes itself without the old primary (§5.4).
async fn primaries(store: &MemoryControlStore, count: u8, handoffs: &[(NodeId, Handoff)]) {
    for mut config in shards(store, count).await {
        if config.learners.is_empty() {
            continue;
        }
        config.epoch = config.epoch.checked_next().unwrap();
        let learners = std::mem::take(&mut config.learners);
        config.members.extend(learners);
        config.proposal_id = ProposalIds::from_os_rng().next_id();
        put(store, &config).await;
    }
    for (primary, handoff) in handoffs {
        let mut config = shard_register(store, handoff.shard.get()).await;
        assert_eq!(config.primary, *primary);
        if config.epoch != handoff.epoch || !config.is_member(&handoff.to) {
            continue;
        }
        config.epoch = config.epoch.checked_next().unwrap();
        config.primary = handoff.to.clone();
        config.members.retain(|member| member != primary);
        config.proposal_id = ProposalIds::from_os_rng().next_id();
        put(store, &config).await;
    }
}

/// Runs coordinator rounds and the primaries' steps until nothing changes
/// for a while, advancing time past the handoff interval each round.
async fn settle(coordinator: &mut Coordinator, store: &MemoryControlStore, nodes: u8, count: u8) {
    let mut quiet = 0;
    for _ in 0..200 {
        coordinator.hear(nodes);
        let changed = coordinator.round(store).await;
        let handoffs = coordinator.asked.take();
        coordinator.count += u8::try_from(handoffs.len()).unwrap();
        primaries(store, count, &handoffs).await;
        tokio::time::advance(Duration::from_secs(3)).await;
        quiet = if changed || !handoffs.is_empty() {
            0
        } else {
            quiet + 1
        };
        if quiet == 5 {
            return;
        }
    }
    panic!(
        "rebalancing never settled: {:?}",
        shards(store, count).await
    );
}

/// Each node's members and primaries over `configs`.
fn loads(configs: &[ShardConfig]) -> BTreeMap<NodeId, (usize, usize)> {
    let mut loads: BTreeMap<NodeId, (usize, usize)> = BTreeMap::new();
    for config in configs {
        for member in &config.members {
            loads.entry(member.clone()).or_default().0 += 1;
        }
        loads.entry(config.primary.clone()).or_default().1 += 1;
    }
    loads
}

fn assert_whole(configs: &[ShardConfig]) {
    for config in configs {
        assert_eq!(config.members.len(), 3, "{config:?}");
        assert!(config.learners.is_empty(), "{config:?}");
        assert!(config.is_member(&config.primary), "{config:?}");
        let distinct: BTreeSet<&NodeId> = config.members.iter().collect();
        assert_eq!(distinct.len(), 3, "{config:?}");
    }
}

/// Eight shards of three members on nodes `0..4`, two primaries each.
async fn four_nodes_loaded(store: &MemoryControlStore) {
    for shard in 0..8u8 {
        let first = shard % 4;
        let members: Vec<u8> = (0..3).map(|n| (first + n) % 4).collect();
        write_shard(store, shard, &members, &[]).await;
    }
}

#[tokio::test(start_paused = true)]
async fn a_new_node_receives_its_share_of_shards_and_primaries() {
    let store = store(4).await;
    four_nodes_loaded(&store).await;
    let mut coordinator = Coordinator::nodes();
    coordinator.hear(4);
    // Balanced: nothing moves.
    assert!(coordinator.plan(&store).await.is_none());
    assert!(coordinator.asked.take().is_empty());

    join(&store, 4, PLAIN).await;
    settle(&mut coordinator, &store, 5, 8).await;
    let configs = shards(&store, 8).await;
    assert_whole(&configs);
    let loads = loads(&configs);
    // 24 members over five nodes: four or five each; eight primaries:
    // one or two each.
    for (node, (members, primaries)) in &loads {
        assert!((4..=5).contains(members), "{node}: {loads:?}");
        assert!((1..=2).contains(primaries), "{node}: {loads:?}");
    }
    assert!(coordinator.count > 0, "the primaries moved by handoff");
}

#[tokio::test(start_paused = true)]
async fn a_batch_moves_at_most_max_moves_shards_and_waits_for_them() {
    let store = store(4).await;
    four_nodes_loaded(&store).await;
    join(&store, 4, PLAIN).await;
    let mut coordinator = Coordinator::new(
        FailureDomain::Node,
        RebalanceConfig {
            max_moves: 2,
            ..RebalanceConfig::default()
        },
    );
    coordinator.hear(5);
    assert!(coordinator.round(&store).await);
    let configs = shards(&store, 8).await;
    let learning: Vec<&ShardConfig> = configs.iter().filter(|c| !c.learners.is_empty()).collect();
    assert_eq!(learning.len(), 2, "{configs:?}");
    for config in &learning {
        assert_eq!(config.learners, vec![node(4)]);
    }
    // The learners are not promoted yet: no new batch.
    coordinator.hear(5);
    assert!(!coordinator.round(&store).await);
    assert!(coordinator.asked.take().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_promoted_learner_replaces_the_member_it_was_added_for() {
    let store = store(4).await;
    four_nodes_loaded(&store).await;
    join(&store, 4, PLAIN).await;
    let mut coordinator = Coordinator::new(
        FailureDomain::Node,
        RebalanceConfig {
            max_moves: 1,
            ..RebalanceConfig::default()
        },
    );
    coordinator.hear(5);
    assert!(coordinator.round(&store).await);
    let before = shards(&store, 8).await;
    let moving = before.iter().find(|c| !c.learners.is_empty()).unwrap();
    primaries(&store, 8, &[]).await;
    let promoted = shard_register(&store, moving.shard.get()).await;
    assert_eq!(promoted.members.len(), 4);

    coordinator.hear(5);
    coordinator.round(&store).await;
    let asked = coordinator.asked.take();
    let after = shard_register(&store, moving.shard.get()).await;
    if asked.is_empty() {
        // A member other than the primary left by a compare-and-swap.
        assert_eq!(after.members.len(), 3, "{after:?}");
        assert!(after.is_member(&node(4)));
        assert_eq!(after.primary, moving.primary);
    } else {
        // The primary left: it is asked to hand off to the new member.
        assert_eq!(asked.len(), 1);
        assert_eq!(asked[0].0, moving.primary);
        assert_eq!(asked[0].1.epoch, promoted.epoch);
        assert_eq!(after, promoted);
    }
}

#[tokio::test(start_paused = true)]
async fn a_surplus_member_left_by_another_coordinator_is_removed() {
    let store = store(5).await;
    // Node 0 holds three shards, the others fewer.
    write_shard(&store, 0, &[1, 0, 2, 3], &[]).await;
    write_shard(&store, 1, &[0, 1, 4], &[]).await;
    write_shard(&store, 2, &[0, 2, 3], &[]).await;
    let mut coordinator = Coordinator::nodes();
    coordinator.hear(5);
    assert!(coordinator.round(&store).await);
    let after = shard_register(&store, 0).await;
    // The member on the most loaded node leaves; the primary stays.
    assert_eq!(after.members, nodes(&[1, 2, 3]));
    assert_eq!(after.primary, node(1));
    assert!(coordinator.asked.take().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_surplus_primary_hands_the_shard_off() {
    let store = store(5).await;
    // Node 0 leads the surplus shard and is the most loaded.
    write_shard(&store, 0, &[0, 1, 2, 3], &[]).await;
    write_shard(&store, 1, &[0, 1, 4], &[]).await;
    write_shard(&store, 2, &[0, 2, 4], &[]).await;
    let before = shard_register(&store, 0).await;
    let mut coordinator = Coordinator::nodes();
    coordinator.hear(5);
    assert!(!coordinator.round(&store).await);
    let asked = coordinator.asked.take();
    assert_eq!(asked.len(), 1, "{asked:?}");
    let (primary, handoff) = &asked[0];
    assert_eq!(*primary, node(0));
    assert_eq!(handoff.epoch, before.epoch);
    assert_eq!(handoff.shard, ShardId::new(0));
    // A member that leads nothing, the first by node ID.
    assert_eq!(handoff.to, node(1));

    // Not asked again in the same epoch, nor within the interval.
    coordinator.hear(5);
    assert!(!coordinator.round(&store).await);
    assert!(coordinator.asked.take().is_empty());
    tokio::time::advance(Duration::from_secs(11)).await;
    coordinator.hear(5);
    coordinator.round(&store).await;
    assert_eq!(
        coordinator.asked.take().len(),
        1,
        "asked again after the retry"
    );
}

#[tokio::test(start_paused = true)]
async fn a_departing_primary_is_handed_off_once_replaced() {
    let store = store(4).await;
    write_shard(&store, 0, &[0, 1, 2], &[]).await;
    depart(&store, 0).await;
    let mut coordinator = Coordinator::nodes();
    coordinator.hear(4);
    // Replacement adds a learner for the primary, which does not count.
    assert!(coordinator.round(&store).await);
    assert_eq!(shard_register(&store, 0).await.learners, nodes(&[3]));
    assert!(coordinator.asked.take().is_empty());
    primaries(&store, 1, &[]).await;

    coordinator.hear(4);
    assert!(!coordinator.round(&store).await);
    let asked = coordinator.asked.take();
    assert_eq!(asked.len(), 1, "{asked:?}");
    assert_eq!(asked[0].0, node(0));
    assert!(nodes(&[1, 2, 3]).contains(&asked[0].1.to));
    primaries(&store, 1, &asked).await;
    let after = shard_register(&store, 0).await;
    assert!(!after.is_member(&node(0)));
    assert_eq!(after.members.len(), 3);
}

#[tokio::test(start_paused = true)]
async fn nothing_moves_while_a_node_is_suspect_or_a_shard_is_moving() {
    let store = store(4).await;
    four_nodes_loaded(&store).await;
    join(&store, 4, PLAIN).await;
    let mut coordinator = Coordinator::nodes();
    // Nodes 0..4 heard from recently, node 4 never: suspect once
    // `suspect_after` passes.
    coordinator.registry.refresh(&store).await.unwrap();
    tokio::time::advance(SUSPECT + Duration::from_secs(1)).await;
    coordinator.hear(4);
    assert!(coordinator.plan(&store).await.is_none());

    // A learner replacement added holds the next batch back.
    coordinator.hear(5);
    let config = shard_register(&store, 0).await;
    let mut learning = config.clone();
    learning.epoch = config.epoch.checked_next().unwrap();
    learning.learners = nodes(&[4]);
    put(&store, &learning).await;
    assert!(coordinator.plan(&store).await.is_none());
}

#[tokio::test(start_paused = true)]
async fn moves_keep_members_in_separate_racks() {
    let store = bootstrapped().await;
    // Racks 0, 1, 2, and the new node 3 in rack 0.
    for n in 0..3 {
        join(
            &store,
            n,
            Shape {
                rack: Some(n),
                ..PLAIN
            },
        )
        .await;
    }
    for shard in 0..3 {
        write_shard(
            &store,
            shard,
            &[shard, (shard + 1) % 3, (shard + 2) % 3],
            &[],
        )
        .await;
    }
    join(
        &store,
        3,
        Shape {
            rack: Some(0),
            ..PLAIN
        },
    )
    .await;
    let mut coordinator = Coordinator::new(FailureDomain::Rack, RebalanceConfig::default());
    settle(&mut coordinator, &store, 4, 3).await;
    let configs = shards(&store, 3).await;
    assert_whole(&configs);
    for config in &configs {
        // Node 3 only ever replaces node 0, in the same rack.
        assert!(
            !(config.is_member(&node(0)) && config.is_member(&node(3))),
            "{config:?}"
        );
    }
    let loads = loads(&configs);
    assert!(loads.get(&node(3)).is_some_and(|l| l.0 >= 1), "{loads:?}");
}

#[tokio::test(start_paused = true)]
async fn shares_follow_capacity() {
    let store = bootstrapped().await;
    for n in 0..4 {
        join(&store, n, PLAIN).await;
    }
    for shard in 0..8u8 {
        let first = shard % 4;
        let members: Vec<u8> = (0..3).map(|n| (first + n) % 4).collect();
        write_shard(&store, shard, &members, &[]).await;
    }
    // A node with three times the capacity of the others.
    join(
        &store,
        4,
        Shape {
            capacity: 3 << 40,
            ..PLAIN
        },
    )
    .await;
    let mut coordinator = Coordinator::nodes();
    settle(&mut coordinator, &store, 5, 8).await;
    let configs = shards(&store, 8).await;
    assert_whole(&configs);
    let loads = loads(&configs);
    // 24 members over a capacity of 7: about 10 for the large node, but
    // at most one per shard.
    assert_eq!(loads[&node(4)].0, 8, "{loads:?}");
    for n in 0..4 {
        assert_eq!(loads[&node(n)].0, 4, "{loads:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn primaries_move_alone_once_shards_are_balanced() {
    let store = store(3).await;
    // Every shard on every node, all led by node 0.
    for shard in 0..4 {
        write_shard(&store, shard, &[0, 1, 2], &[]).await;
    }
    join(&store, 3, PLAIN).await;
    let mut coordinator = Coordinator::nodes();
    settle(&mut coordinator, &store, 4, 4).await;
    let configs = shards(&store, 4).await;
    assert_whole(&configs);
    let loads = loads(&configs);
    for (node, (_, primaries)) in &loads {
        assert_eq!(*primaries, 1, "{node}: {loads:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_lost_learner_compare_and_swap_starts_no_move() {
    let store = store(4).await;
    four_nodes_loaded(&store).await;
    join(&store, 4, PLAIN).await;
    let mut coordinator = Coordinator::new(
        FailureDomain::Node,
        RebalanceConfig {
            max_moves: 1,
            ..RebalanceConfig::default()
        },
    );
    coordinator.hear(5);
    let change = coordinator.plan(&store).await.unwrap();
    // A primary changes the register first.
    let moving = change.writes()[0].key().clone();
    let shard = shards(&store, 8)
        .await
        .into_iter()
        .find(|c| key(c.shard.get()).key() == &moving)
        .unwrap();
    let mut raced = shard.clone();
    raced.epoch = shard.epoch.checked_next().unwrap();
    put(&store, &raced).await;
    let applied = apply(&store, &cluster(), &change, &mut coordinator.ids, &retry())
        .await
        .unwrap();
    assert_eq!(applied.rejected.as_ref(), Some(&moving));
    coordinator.placement.applied(&change, &applied);
    assert!(format!("{:?}", coordinator.placement).contains("Replacement"));
    // The next round starts the move again from the register.
    coordinator.hear(5);
    assert!(coordinator.round(&store).await);
}
