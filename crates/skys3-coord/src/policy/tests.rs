use std::time::Duration;

use skys3_control::faults::{Fault, FaultyStore};
use skys3_control::{
    Expected, MemoryControlStore, ProposalOutcome, PutOutcome, RetryPolicy, TypedKey, bootstrap,
    propose_document,
};
use skys3_io::MonotonicClock;
use skys3_types::{
    BucketMode, ClusterId, DiskInfo, Epoch, Label, NodeRegistration, ProposalId, ShardCount,
};

use super::*;
use crate::join::{NodeProfile, register};
use crate::place::Candidate;
use crate::registry::RegistryConfig;

const TB: u64 = 1 << 40;

fn node(n: usize) -> NodeId {
    NodeId::new(format!("node-{n}")).unwrap()
}

fn rack(n: usize) -> Label {
    Label::new(format!("rack-{n}")).unwrap()
}

fn bucket_id(b: usize) -> BucketId {
    BucketId::new(format!("b-{b}")).unwrap()
}

/// Live node `n` in rack `rack`.
fn candidate(n: usize, rack_of: usize) -> Candidate {
    Candidate {
        node: node(n),
        zone: None,
        rack: Some(rack(rack_of)),
        capacity_bytes: TB,
        state: NodeState::Live,
        shards: 0,
        primaries: 0,
    }
}

/// A `local` bucket `b` of `shards` shards and `replicas` replicas.
fn bucket(b: usize, shards: u32, replicas: u8) -> BucketDocument {
    BucketDocument {
        bucket_id: bucket_id(b),
        name: BucketName::new(format!("bucket-{b}")).unwrap(),
        mode: BucketMode::Local,
        shards: ShardCount::new(shards).unwrap(),
        replicas,
        min_write_replicas: 1,
        clean_copies: 0,
        target: None,
        created_unix_ms: 0,
        proposal_id: ProposalId::new(format!("p-b{b}")).unwrap(),
    }
}

/// Shard `s` of bucket `b` with `members`, the first as primary.
fn shard(b: usize, s: u8, members: &[usize]) -> ShardConfig {
    let members: Vec<NodeId> = members.iter().map(|n| node(*n)).collect();
    ShardConfig {
        bucket_id: bucket_id(b),
        shard: ShardId::new(s),
        epoch: Epoch::new(1),
        primary: members[0].clone(),
        members,
        learners: Vec::new(),
        min_write_replicas: 1,
        replicas: 2,
        proposal_id: ProposalId::new(format!("p-b{b}-s{s}")).unwrap(),
    }
}

#[test]
fn a_satisfied_cluster_reports_nothing() {
    let topology = Topology::new(
        FailureDomain::Rack,
        [candidate(0, 0), candidate(1, 1), candidate(2, 2)],
    );
    let buckets = [bucket(1, 2, 2)];
    let shards = [
        shard(1, 0, &[0, 1]),
        shard(1, 1, &[1, 2]),
        shard(9, 0, &[0]),
    ];
    let report = report(&topology, &buckets, &shards);
    assert!(report.is_satisfied(), "{report:?}");
    assert_eq!(report.failure_domain, FailureDomain::Rack);
    assert_eq!((report.eligible_nodes, report.domains), (3, 3));
    assert!(report.unlabeled.is_empty());
}

#[test]
fn losing_a_rack_leaves_buckets_unsatisfied_without_co_location() {
    let mut lost = candidate(2, 2);
    lost.state = NodeState::Departing;
    let mut unlabeled = candidate(3, 0);
    unlabeled.rack = None;
    // Node 4 shares rack 1 with node 1.
    let topology = Topology::new(
        FailureDomain::Rack,
        [
            candidate(0, 0),
            candidate(1, 1),
            lost,
            unlabeled,
            candidate(4, 1),
        ],
    );
    let buckets = [bucket(2, 2, 3), bucket(1, 3, 2)];
    let shards = [
        // Bucket 1: whole, missing, and co-located (relabeled nodes).
        shard(1, 0, &[0, 1]),
        shard(1, 2, &[0, 1, 4]),
        // Bucket 2: three members, one departing; and one member left.
        shard(2, 0, &[0, 1, 2]),
        shard(2, 1, &[0]),
    ];
    let report = report(&topology, &buckets, &shards);
    assert!(!report.is_satisfied());
    assert_eq!((report.eligible_nodes, report.domains), (3, 2));
    assert_eq!(report.unlabeled, [node(3)]);
    assert_eq!(report.unsatisfied.len(), 2);

    let first = &report.unsatisfied[0];
    assert_eq!(first.bucket_id, bucket_id(1));
    assert!(first.placeable, "two racks are left for two replicas");
    assert_eq!(
        first.short,
        [ShortShard {
            shard: ShardId::new(1),
            members: Vec::new(),
            domains: 0
        }]
    );
    assert_eq!(
        first.co_located,
        [CoLocated {
            shard: ShardId::new(2),
            domain: Domain::Rack(rack(1)),
            members: vec![node(1), node(4)],
        }]
    );

    let second = &report.unsatisfied[1];
    assert_eq!(second.name.as_str(), "bucket-2");
    assert_eq!(second.replicas, 3);
    assert!(!second.placeable);
    let short: Vec<_> = second.short.iter().map(|s| (s.shard, s.domains)).collect();
    assert_eq!(short, [(ShardId::new(0), 2), (ShardId::new(1), 1)]);
    assert!(second.co_located.is_empty());
}

#[test]
fn unregistered_members_count_as_domains_of_their_own() {
    let topology = Topology::new(FailureDomain::Node, [candidate(0, 0), candidate(1, 0)]);
    let buckets = [bucket(1, 1, 3)];
    let report = report(&topology, &buckets, &[shard(1, 0, &[0, 7, 8])]);
    assert_eq!(report.unsatisfied.len(), 1);
    let bucket = &report.unsatisfied[0];
    assert!(!bucket.placeable);
    assert!(bucket.short.is_empty(), "{bucket:?}");
}

#[test]
fn the_health_handle_keeps_the_latest_report() {
    let health = PlacementHealth::new();
    assert_eq!(health.latest(), None);
    let topology = Topology::new(FailureDomain::Node, [candidate(0, 0)]);
    let first = report(&topology, &[], &[]);
    assert_eq!(health.clone().publish(Some(first.clone())), None);
    assert_eq!(health.latest(), Some(first.clone()));
    assert_eq!(health.publish(None), Some(first));
}

fn cluster() -> ClusterId {
    ClusterId::new("prod").unwrap()
}

fn retry() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 3,
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(10),
    }
}

fn registry() -> NodeRegistry {
    NodeRegistry::new(
        Arc::new(MonotonicClock::new()),
        RegistryConfig::new(Duration::from_secs(60)),
    )
}

/// A store with nodes 0 to 2 registered in racks 0 to 2.
async fn store() -> MemoryControlStore {
    let store = MemoryControlStore::new();
    let mut ids = ProposalIds::seeded(7);
    bootstrap(&store, &cluster(), ids.next_id(), &retry())
        .await
        .unwrap();
    for n in 0..3 {
        let profile = NodeProfile {
            node: node(n),
            address: format!("10.0.0.{n}:7400").parse().unwrap(),
            zone: None,
            rack: Some(rack(n)),
            disks: vec![DiskInfo {
                disk_id: Label::new("nvme0").unwrap(),
                capacity_bytes: TB,
            }],
        };
        register(&store, &cluster(), &profile, &mut ids, &retry())
            .await
            .unwrap();
    }
    store
}

async fn put<D: RegisterDocument>(
    store: &MemoryControlStore,
    key: &TypedKey<D>,
    expected: Expected,
    document: &D,
) {
    let outcome = propose_document(store, key, expected, document, &retry())
        .await
        .unwrap();
    assert!(
        matches!(outcome, ProposalOutcome::Accepted(_)),
        "{outcome:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn the_watch_publishes_what_the_registers_say() {
    let store = store().await;
    let registry = registry();
    registry.begin_tenure();
    registry.refresh(&store).await.unwrap();
    let health = PlacementHealth::new();
    let mut watch = PolicyWatch::new(registry.clone(), FailureDomain::Rack)
        .with_placement(NoPlacement)
        .with_health(health.clone());
    assert!(format!("{watch:?}").contains("PolicyWatch"));
    let mut proposals = ProposalIds::seeded(9);

    // One satisfied bucket and one that needs four racks.
    put(
        &store,
        &TypedKey::bucket(&bucket(1, 1, 2).name),
        Expected::Absent,
        &bucket(1, 1, 2),
    )
    .await;
    put(
        &store,
        &TypedKey::bucket(&bucket(2, 1, 4).name),
        Expected::Absent,
        &bucket(2, 1, 4),
    )
    .await;
    let first = shard(1, 0, &[0, 1]);
    let key = TypedKey::shard(&bucket_id(1), ShardId::new(0));
    put(&store, &key, Expected::Absent, &first).await;
    // A register that does not parse is left out.
    let garbage = store
        .put_if(
            &RegisterKey::new("shards/b-2/0.json").unwrap(),
            Expected::Absent,
            bytes::Bytes::from_static(b"{"),
        )
        .await
        .unwrap();
    assert!(matches!(garbage, PutOutcome::Written(_)), "{garbage:?}");

    let planned = watch.plan(&store, &mut proposals).await.unwrap();
    assert!(planned.is_none());
    let report = health.latest().unwrap();
    assert_eq!(report.domains, 3);
    let names: Vec<_> = report.unsatisfied.iter().map(|b| b.name.as_str()).collect();
    assert_eq!(names, ["bucket-2"]);
    assert_eq!(watch.health().latest(), Some(report));

    // Moving shard 0 of bucket 1 into one rack is reported, and moving it
    // back clears the report. Unchanged registers are not read again.
    let current = skys3_control::read(&store, &key).await.unwrap().unwrap();
    let mut moved = shard(1, 0, &[0, 0]);
    moved.members = vec![node(0)];
    moved.epoch = Epoch::new(2);
    put(&store, &key, Expected::Version(current.version), &moved).await;
    watch.plan(&store, &mut proposals).await.unwrap();
    let report = health.latest().unwrap();
    let names: Vec<_> = report.unsatisfied.iter().map(|b| b.name.as_str()).collect();
    assert_eq!(names, ["bucket-1", "bucket-2"]);

    let current = skys3_control::read(&store, &key).await.unwrap().unwrap();
    let mut back = first.clone();
    back.epoch = Epoch::new(3);
    put(&store, &key, Expected::Version(current.version), &back).await;
    watch.plan(&store, &mut proposals).await.unwrap();
    let names: Vec<_> = health
        .latest()
        .unwrap()
        .unsatisfied
        .iter()
        .map(|b| b.bucket_id.clone())
        .collect();
    assert_eq!(names, [bucket_id(2)]);

    // A new tenure clears the stale report until the next plan.
    watch.begin_tenure();
    assert_eq!(health.latest(), None);
    let change = ChangeSet::new();
    let applied = Applied {
        written: Vec::new(),
        rejected: None,
        generation: None,
    };
    watch.applied(&change, &applied);
}

#[tokio::test(start_paused = true)]
async fn a_failed_scan_keeps_the_previous_view() {
    let store = FaultyStore::new(store().await);
    let registry = registry();
    registry.refresh(&store).await.unwrap();
    let mut watch = PolicyWatch::new(registry, FailureDomain::Node);
    let mut proposals = ProposalIds::seeded(3);
    let bucket = bucket(1, 1, 2);
    put(
        store.inner(),
        &TypedKey::bucket(&bucket.name),
        Expected::Absent,
        &bucket,
    )
    .await;
    watch.plan(&store, &mut proposals).await.unwrap();
    let report = watch.health().latest().unwrap();
    assert_eq!(report.unsatisfied[0].short.len(), 1, "{report:?}");

    // Unchanged registers are not read again: only the two listings.
    let before = store.requests();
    watch.plan(&store, &mut proposals).await.unwrap();
    assert_eq!(store.requests() - before, 2);

    store.script([Fault::Fail]);
    assert!(watch.plan(&store, &mut proposals).await.is_err());
    assert_eq!(watch.health().latest(), Some(report));
}

#[test]
fn registrations_feed_a_topology_without_a_registry() {
    let registration = NodeRegistration {
        node_id: node(1),
        address: "10.0.0.1:7400".parse().unwrap(),
        zone: None,
        rack: None,
        disks: Vec::new(),
        proposal_id: ProposalId::new("p1").unwrap(),
    };
    let topology = Topology::new(
        FailureDomain::Node,
        [Candidate::from_registration(&registration)],
    );
    // A node with no disks offers no capacity.
    assert!(topology.check(1).is_err());
}
