use std::time::Duration;

use skys3_control::{
    Expected, MemoryControlStore, ProposalOutcome, RetryPolicy, bootstrap, propose_document, read,
    read_cluster,
};
use skys3_io::MonotonicClock;
use skys3_types::{BucketId, ClusterId, DiskInfo, Epoch, Label, NodeRegistration, ShardId};

use super::*;
use crate::change::apply;
use crate::join::{NodeProfile, register};
use crate::registry::RegistryConfig;

const SUSPECT: Duration = Duration::from_secs(10);
const INTERVAL: Duration = Duration::from_secs(30);

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

/// A store in which nodes `0..count` are registered, node `n` in rack
/// `rack(n)` if `rack` is given.
async fn store_with(count: u8, rack: Option<fn(u8) -> u8>) -> MemoryControlStore {
    let store = MemoryControlStore::new();
    let mut ids = ProposalIds::seeded(1);
    bootstrap(&store, &cluster(), ids.next_id(), &retry())
        .await
        .unwrap();
    for n in 0..count {
        let profile = NodeProfile {
            node: node(n),
            address: format!("10.0.0.{n}:7400").parse().unwrap(),
            zone: None,
            rack: rack.map(|rack| Label::new(format!("rack-{}", rack(n))).unwrap()),
            disks: vec![DiskInfo {
                disk_id: Label::new("nvme0").unwrap(),
                capacity_bytes: 1 << 40,
            }],
        };
        register(&store, &cluster(), &profile, &mut ids, &retry())
            .await
            .unwrap();
    }
    store
}

async fn store(count: u8) -> MemoryControlStore {
    store_with(count, None).await
}

/// Writes shard `shard` with `members`, the first as primary, and
/// `learners`, over whatever the register holds; returns the new config.
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
    let expected = current.map_or(Expected::Absent, |c| Expected::Version(c.version));
    let outcome = propose_document(store, &key(shard), expected, &config, &retry())
        .await
        .unwrap();
    assert!(matches!(outcome, ProposalOutcome::Accepted(_)));
    config
}

async fn shard_register(store: &MemoryControlStore, shard: u8) -> ShardConfig {
    read(store, &key(shard)).await.unwrap().unwrap().value
}

async fn generation(store: &MemoryControlStore) -> skys3_types::Generation {
    read_cluster(store, &cluster(), &retry())
        .await
        .unwrap()
        .value
        .generation
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

struct Coordinator {
    registry: NodeRegistry,
    replacement: Replacement,
    ids: ProposalIds,
}

impl Coordinator {
    fn new(level: FailureDomain, config: ReplacementConfig) -> Self {
        let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::new());
        let registry = NodeRegistry::new(
            Arc::clone(&clock),
            RegistryConfig {
                suspect_after: SUSPECT,
                forget_after: Duration::from_secs(3600),
            },
        );
        let replacement = Replacement::new(registry.clone(), level, clock, config);
        Self {
            registry,
            replacement,
            ids: ProposalIds::seeded(5),
        }
    }

    fn nodes() -> Self {
        Self::new(
            FailureDomain::Node,
            ReplacementConfig {
                removal_interval: INTERVAL,
                ..ReplacementConfig::default()
            },
        )
    }

    /// One round: lists the nodes and plans.
    async fn plan(&mut self, store: &MemoryControlStore) -> Option<ChangeSet> {
        self.registry.refresh(store).await.unwrap();
        self.replacement.plan(store, &mut self.ids).await.unwrap()
    }

    /// One round that must make a change, applied in full.
    async fn step(&mut self, store: &MemoryControlStore) {
        let applied = self.round(store).await.expect("a change");
        assert!(applied.is_complete(), "{applied:?}");
    }

    /// One round, applying what it plans.
    async fn round(&mut self, store: &MemoryControlStore) -> Option<Applied> {
        let change = self.plan(store).await?;
        let applied = apply(store, &cluster(), &change, &mut self.ids, &retry())
            .await
            .unwrap();
        self.replacement.applied(&change, &applied);
        Some(applied)
    }
}

#[tokio::test(start_paused = true)]
async fn a_shard_that_lost_a_member_gets_a_learner_on_a_spare_node_in_an_announced_change() {
    let store = store(4).await;
    // The primary removed node-2: two members left.
    write_shard(&store, 0, &[0, 1, 2], &[]).await;
    let before = write_shard(&store, 0, &[0, 1], &[]).await;
    let announced = generation(&store).await;

    let mut coordinator = Coordinator::nodes();
    let applied = coordinator.round(&store).await.unwrap();
    assert!(applied.is_complete());
    assert!(applied.generation.unwrap() > announced);
    let after = shard_register(&store, 0).await;
    assert_eq!(after.epoch, before.epoch.checked_next().unwrap());
    assert_eq!(
        (&after.primary, &after.members),
        (&before.primary, &before.members)
    );
    // node-2 holds no shard now, as node-3 does not: the hash breaks the
    // tie, and either rejoins as a learner.
    assert_eq!(after.learners.len(), 1);
    assert!([node(2), node(3)].contains(&after.learners[0]), "{after:?}");
    assert_ne!(after.proposal_id, before.proposal_id);

    // The learner counts while it catches up: nothing more to do.
    assert!(coordinator.round(&store).await.is_none());
}

#[tokio::test(start_paused = true)]
async fn nothing_is_planned_before_the_registry_lists_the_nodes_and_the_wrapped_placement_plans() {
    struct Fixed(Option<ChangeSet>, usize);
    impl Placement for Fixed {
        async fn plan<S: ControlStore>(
            &mut self,
            _store: &S,
            _proposals: &mut ProposalIds,
        ) -> Result<Option<ChangeSet>, ControlError> {
            Ok(self.0.clone())
        }
        fn applied(&mut self, _change: &ChangeSet, _applied: &Applied) {
            self.1 += 1;
        }
    }
    let store = store(4).await;
    write_shard(&store, 0, &[0, 1], &[]).await;
    let theirs = ChangeSet::new()
        .create(&key(9), &shard_register(&store, 0).await)
        .unwrap();
    let mut coordinator = Coordinator::nodes();
    let mut replacement = std::mem::replace(
        &mut coordinator.replacement,
        Replacement::new(
            coordinator.registry.clone(),
            FailureDomain::Node,
            Arc::new(MonotonicClock::new()),
            ReplacementConfig::default(),
        ),
    )
    .with_placement(Fixed(Some(theirs.clone()), 0));
    assert!(format!("{replacement:?}").contains("Replacement"));
    replacement.begin_tenure();
    let mut ids = ProposalIds::seeded(1);

    // Not listed yet: the wrapped placement plans.
    let planned = replacement.plan(&store, &mut ids).await.unwrap();
    assert_eq!(planned.as_ref(), Some(&theirs));
    replacement.applied(
        &theirs,
        &apply(&store, &cluster(), &theirs, &mut ids, &retry())
            .await
            .unwrap(),
    );
    assert_eq!(replacement.placement.1, 1);

    // Listed: the shard short of members comes first, and its change is
    // not reported to the wrapped placement.
    coordinator.registry.refresh(&store).await.unwrap();
    let ours = replacement.plan(&store, &mut ids).await.unwrap().unwrap();
    assert_ne!(ours, theirs);
    let applied = apply(&store, &cluster(), &ours, &mut ids, &retry())
        .await
        .unwrap();
    replacement.applied(&ours, &applied);
    assert_eq!(replacement.placement.1, 1);
}

#[tokio::test(start_paused = true)]
async fn no_eligible_node_means_no_learner_and_never_a_departing_one() {
    let store = store(3).await;
    write_shard(&store, 0, &[0, 1], &[]).await;
    depart(&store, 2).await;
    let mut coordinator = Coordinator::nodes();
    assert!(coordinator.round(&store).await.is_none());
    assert!(shard_register(&store, 0).await.learners.is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_lost_node_rejoins_as_a_learner_when_it_is_the_only_one_left() {
    // Three nodes: node-2 was removed and is suspect, and is the only node
    // outside the shard.
    let store = store(3).await;
    write_shard(&store, 0, &[0, 1], &[]).await;
    let mut coordinator = Coordinator::nodes();
    coordinator.registry.refresh(&store).await.unwrap();
    tokio::time::advance(SUSPECT).await;
    coordinator.registry.heard(&node(0));
    coordinator.registry.heard(&node(1));
    coordinator.step(&store).await;
    assert_eq!(shard_register(&store, 0).await.learners, [node(2)]);
    // Suspect, but no live node could take its place: it stays.
    assert!(coordinator.round(&store).await.is_none());
}

#[tokio::test(start_paused = true)]
async fn live_nodes_are_preferred_and_a_learner_on_a_suspect_node_is_swapped() {
    let store = store(5).await;
    write_shard(&store, 0, &[0, 1], &[3]).await;
    let mut coordinator = Coordinator::nodes();
    coordinator.registry.refresh(&store).await.unwrap();
    tokio::time::advance(SUSPECT).await;
    for n in [0, 1, 4] {
        coordinator.registry.heard(&node(n));
    }
    // node-3, the learner, and node-2 are suspect; node-4 is live.
    coordinator.step(&store).await;
    let after = shard_register(&store, 0).await;
    assert_eq!(after.learners, [node(4)], "{after:?}");
    assert_eq!(after.members, nodes(&[0, 1]));
    assert!(coordinator.round(&store).await.is_none());
}

#[tokio::test(start_paused = true)]
async fn learners_that_cannot_count_are_dropped_and_replaced_in_one_change() {
    let store = store(5).await;
    write_shard(&store, 0, &[0, 1], &[2]).await;
    depart(&store, 2).await;
    let mut coordinator = Coordinator::nodes();
    coordinator.step(&store).await;
    let after = shard_register(&store, 0).await;
    assert_eq!(after.learners.len(), 1);
    assert!([node(3), node(4)].contains(&after.learners[0]), "{after:?}");

    // A learner on a node that is not registered at all goes too.
    write_shard(&store, 1, &[0, 1, 3], &[7]).await;
    coordinator.step(&store).await;
    assert!(shard_register(&store, 1).await.learners.is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_departing_member_is_removed_only_once_its_replacement_is_a_member() {
    let store = store(5).await;
    write_shard(&store, 0, &[0, 1, 2], &[]).await;
    depart(&store, 2).await;
    let mut coordinator = Coordinator::nodes();
    // First a learner: the members that count are two.
    coordinator.step(&store).await;
    let with_learner = shard_register(&store, 0).await;
    assert_eq!(with_learner.members, nodes(&[0, 1, 2]));
    let learner = with_learner.learners[0].clone();
    // The primary has not promoted it yet: nothing is removed.
    assert!(coordinator.round(&store).await.is_none());

    // The primary promotes it; then the departing member goes.
    let n = learner
        .as_str()
        .trim_start_matches("node-")
        .parse()
        .unwrap();
    write_shard(&store, 0, &[0, 1, 2, n], &[]).await;
    coordinator.step(&store).await;
    let after = shard_register(&store, 0).await;
    assert_eq!(after.members, nodes(&[0, 1, n]));
    assert_eq!(after.primary, node(0));
    assert!(coordinator.round(&store).await.is_none());
}

#[tokio::test(start_paused = true)]
async fn the_primary_is_never_removed_and_removals_are_rate_limited() {
    let store = store(6).await;
    // Shard 0's primary departs; shards 1 and 2 each have a departing
    // member, already replaced.
    write_shard(&store, 0, &[2, 0, 1, 3], &[]).await;
    write_shard(&store, 1, &[0, 1, 3, 2], &[]).await;
    write_shard(&store, 2, &[1, 3, 4, 2], &[]).await;
    depart(&store, 2).await;
    let mut coordinator = Coordinator::nodes();
    let applied = coordinator.round(&store).await.unwrap();
    assert_eq!(applied.written.len(), 1, "{applied:?}");
    assert!(coordinator.round(&store).await.is_none());
    tokio::time::advance(INTERVAL).await;
    coordinator.step(&store).await;
    assert!(coordinator.round(&store).await.is_none());
    tokio::time::advance(INTERVAL).await;
    // Only the primary is left on node-2.
    assert!(coordinator.round(&store).await.is_none());
    assert_eq!(
        shard_register(&store, 0).await.members,
        nodes(&[2, 0, 1, 3])
    );
    assert_eq!(shard_register(&store, 1).await.members, nodes(&[0, 1, 3]));
    assert_eq!(shard_register(&store, 2).await.members, nodes(&[1, 3, 4]));
}

#[tokio::test(start_paused = true)]
async fn co_located_members_are_separated_at_the_rack_level() {
    // Racks: node-0 and node-1 in rack 0, node-2 in rack 1, node-3 in
    // rack 2.
    let store = store_with(4, Some(|n| n.saturating_sub(1))).await;
    write_shard(&store, 0, &[1, 0, 2], &[]).await;
    let mut coordinator = Coordinator::new(FailureDomain::Rack, ReplacementConfig::default());
    coordinator.step(&store).await;
    assert_eq!(shard_register(&store, 0).await.learners, [node(3)]);
    write_shard(&store, 0, &[1, 0, 2, 3], &[]).await;
    coordinator.step(&store).await;
    // node-0 shares rack 0 with the primary, node-1, and goes.
    let after = shard_register(&store, 0).await;
    assert_eq!(after.members, nodes(&[1, 2, 3]));
    assert!(coordinator.round(&store).await.is_none());
}

#[tokio::test(start_paused = true)]
async fn the_shards_with_fewest_members_go_first_within_the_batch_and_learner_limits() {
    let store = store(6).await;
    write_shard(&store, 0, &[0, 1], &[]).await;
    write_shard(&store, 1, &[2], &[]).await;
    write_shard(&store, 2, &[3, 4], &[]).await;
    let mut coordinator = Coordinator::new(
        FailureDomain::Node,
        ReplacementConfig {
            max_learners: 3,
            batch: 1,
            removal_interval: INTERVAL,
        },
    );
    // Shard 1, with one member, gets both its learners first.
    coordinator.step(&store).await;
    assert_eq!(shard_register(&store, 1).await.learners.len(), 2);
    // Then one more learner fits, for shard 0.
    coordinator.step(&store).await;
    assert_eq!(shard_register(&store, 0).await.learners.len(), 1);
    assert!(coordinator.round(&store).await.is_none());
    assert!(shard_register(&store, 2).await.learners.is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_change_that_loses_to_the_primary_is_planned_again_from_the_register() {
    let store = store(5).await;
    write_shard(&store, 0, &[0, 1, 2], &[]).await;
    let mut coordinator = Coordinator::nodes();
    // The coordinator plans over the register as it reads it.
    let stale = write_shard(&store, 0, &[0, 1], &[]).await;
    let change = coordinator.plan(&store).await.unwrap();
    // The primary removes node-1 before the change lands.
    write_shard(&store, 0, &[0], &[]).await;
    let primary_read = read(&store, &key(0)).await.unwrap().unwrap();
    let applied = apply(&store, &cluster(), &change, &mut coordinator.ids, &retry())
        .await
        .unwrap();
    assert_eq!(applied.rejected.as_ref(), Some(key(0).key()));
    assert!(applied.generation.is_none());
    coordinator.replacement.applied(&change, &applied);
    // The next round plans over the primary's change: two learners now.
    coordinator.step(&store).await;
    let after = shard_register(&store, 0).await;
    assert_eq!(after.members, nodes(&[0]));
    assert_eq!(after.learners.len(), 2);
    assert_eq!(after.epoch.get(), stale.epoch.get() + 2);

    // A change the primary planned over what it read before loses in turn:
    // the compare-and-swap decides, and the primary reads the register.
    let mut late = primary_read.value.clone();
    late.epoch = after.epoch;
    late.proposal_id = ProposalIds::from_os_rng().next_id();
    let outcome = propose_document(
        &store,
        &key(0),
        Expected::Version(primary_read.version),
        &late,
        &retry(),
    )
    .await
    .unwrap();
    assert!(matches!(outcome, ProposalOutcome::Rejected));
    assert_eq!(shard_register(&store, 0).await, after);
}

#[tokio::test(start_paused = true)]
async fn a_member_on_a_node_that_has_not_registered_is_left_to_its_primary() {
    // node-7 has not registered (yet): nothing shows it lost.
    let store = store(5).await;
    write_shard(&store, 0, &[0, 1, 7], &[]).await;
    write_shard(&store, 1, &[0, 1, 2, 7], &[]).await;
    let mut coordinator = Coordinator::nodes();
    assert!(coordinator.round(&store).await.is_none());
    assert_eq!(shard_register(&store, 0).await.members, nodes(&[0, 1, 7]));
    assert_eq!(
        shard_register(&store, 1).await.members,
        nodes(&[0, 1, 2, 7])
    );

    // Its primary removes it as unresponsive: the shard is short now.
    write_shard(&store, 0, &[0, 1], &[]).await;
    coordinator.step(&store).await;
    assert_eq!(shard_register(&store, 0).await.learners.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_learner_in_a_members_domain_counts_in_its_place() {
    // Racks: node-0 and node-1 in rack 0, node-2 in rack 1, node-3 in
    // rack 2. Rebalancing moves the shard from node-0 to node-1.
    let store = store_with(4, Some(|n| n.saturating_sub(1))).await;
    write_shard(&store, 0, &[2, 0, 3], &[1]).await;
    let mut coordinator = Coordinator::new(FailureDomain::Rack, ReplacementConfig::default());
    // The learner is kept, and nothing is added.
    assert!(coordinator.round(&store).await.is_none());
    // Once it is promoted, the member it replaces goes.
    write_shard(&store, 0, &[2, 0, 3, 1], &[]).await;
    coordinator.step(&store).await;
    assert_eq!(shard_register(&store, 0).await.members, nodes(&[2, 3, 1]));
    assert!(coordinator.round(&store).await.is_none());
}
