use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use proptest::prelude::*;
use skys3_io::MonotonicClock;
use skys3_types::{DiskInfo, Epoch, NodeAddress, ProposalId};

use super::*;
use crate::registry::RegistryConfig;

const TB: u64 = 1 << 40;

fn node(n: usize) -> NodeId {
    NodeId::new(format!("node-{n}")).unwrap()
}

fn label(text: String) -> Label {
    Label::new(text).unwrap()
}

fn bucket() -> BucketId {
    BucketId::new("b-1").unwrap()
}

/// A live node `n` in `zone` and `rack` with one terabyte.
fn candidate(n: usize, zone: Option<&str>, rack: Option<&str>) -> Candidate {
    Candidate {
        node: node(n),
        zone: zone.map(|z| label(z.to_owned())),
        rack: rack.map(|r| label(r.to_owned())),
        capacity_bytes: TB,
        state: NodeState::Live,
        shards: 0,
        primaries: 0,
    }
}

/// `racks` racks of `per_rack` nodes, rack `r` in zone `r % zones`.
fn racked(racks: usize, per_rack: usize, zones: usize) -> Vec<Candidate> {
    (0..racks * per_rack)
        .map(|n| {
            let rack = n / per_rack;
            candidate(
                n,
                Some(&format!("zone-{}", rack % zones)),
                Some(&format!("rack-{rack}")),
            )
        })
        .collect()
}

fn domains_of(topology: &Topology, nodes: &[NodeId]) -> Vec<Domain> {
    nodes.iter().map(|n| topology.domain(n).unwrap()).collect()
}

fn distinct(domains: &[Domain]) -> bool {
    domains.iter().collect::<BTreeSet<_>>().len() == domains.len()
}

#[test]
fn domains_follow_the_level_and_need_its_label() {
    let labeled = candidate(1, Some("z"), Some("r"));
    let bare = candidate(2, None, None);
    assert_eq!(
        labeled.domain(FailureDomain::Node),
        Some(Domain::Node(node(1)))
    );
    assert_eq!(
        labeled.domain(FailureDomain::Rack),
        Some(Domain::Rack(label("r".into())))
    );
    assert_eq!(
        labeled.domain(FailureDomain::Zone),
        Some(Domain::Zone(label("z".into())))
    );
    assert_eq!(
        bare.domain(FailureDomain::Node),
        Some(Domain::Node(node(2)))
    );
    assert_eq!(bare.domain(FailureDomain::Rack), None);
    assert_eq!(bare.domain(FailureDomain::Zone), None);
    assert!(bare.is_eligible(FailureDomain::Node));
    assert!(!bare.is_eligible(FailureDomain::Rack));

    assert_eq!(Domain::Node(node(1)).to_string(), "node node-1");
    assert_eq!(Domain::Rack(label("r".into())).to_string(), "rack r");
    assert_eq!(Domain::Zone(label("z".into())).to_string(), "zone z");
}

#[test]
fn candidates_come_from_registrations_and_registry_entries() {
    let registration = NodeRegistration {
        node_id: node(1),
        address: "10.0.0.1:7400".parse::<NodeAddress>().unwrap(),
        zone: Some(label("z".into())),
        rack: None,
        disks: vec![
            DiskInfo {
                disk_id: label("a".into()),
                capacity_bytes: u64::MAX,
            },
            DiskInfo {
                disk_id: label("b".into()),
                capacity_bytes: 5,
            },
        ],
        departing: false,
        proposal_id: ProposalId::new("p1").unwrap(),
    };
    let candidate = Candidate::from_registration(&registration);
    assert_eq!(candidate.node, node(1));
    assert_eq!(candidate.capacity_bytes, u64::MAX, "capacity saturates");
    assert_eq!(candidate.state, NodeState::Live);
    assert_eq!((candidate.shards, candidate.primaries), (0, 0));

    let mut entry = NodeEntry {
        registration,
        version: skys3_control::Version::new("v1"),
        state: NodeState::Suspect,
        silent_for: Duration::from_secs(20),
    };
    let candidate = Candidate::from_entry(&entry);
    assert_eq!(candidate.state, NodeState::Suspect);
    assert_eq!(candidate.zone, Some(label("z".into())));

    // A registration marked departing is departing, from either source.
    entry.registration.departing = true;
    assert_eq!(
        Candidate::from_registration(&entry.registration).state,
        NodeState::Departing
    );
    assert_eq!(Candidate::from_entry(&entry).state, NodeState::Departing);
}

#[test]
fn a_node_marked_departing_is_never_chosen() {
    // Three nodes in three racks; the coordinator marked node-2 departing.
    let registrations: Vec<NodeRegistration> = (0..3)
        .map(|n| NodeRegistration {
            node_id: node(n),
            address: format!("10.0.0.{n}:7400").parse::<NodeAddress>().unwrap(),
            zone: None,
            rack: Some(label(format!("rack-{n}"))),
            disks: vec![DiskInfo {
                disk_id: label("nvme0".into()),
                capacity_bytes: TB,
            }],
            departing: n == 2,
            proposal_id: ProposalId::new(format!("p{n}")).unwrap(),
        })
        .collect();
    let topology = Topology::new(
        FailureDomain::Rack,
        registrations.iter().map(Candidate::from_registration),
    );
    assert!(
        !topology
            .get(&node(2))
            .unwrap()
            .is_eligible(FailureDomain::Rack)
    );
    assert_eq!(topology.eligible_domains().len(), 2);
    assert!(topology.unlabeled().is_empty());
    // Three replicas no longer fit: the marked node does not count.
    assert_eq!(topology.check(3).unwrap_err().domains, 2);
    for shard in 0..32 {
        let placed = topology.place(&ShardRequest::new(&bucket(), ShardId::new(shard), 3));
        assert!(!placed.members.contains(&node(2)), "{placed:?}");
        assert_eq!(placed.short, 1);
    }
    let placed = topology
        .place_bucket(&bucket(), ShardCount::new(16).unwrap(), 2)
        .unwrap();
    assert!(placed.iter().all(|shard| !shard.members.contains(&node(2))));
}

#[test]
fn an_empty_registry_has_no_domains() {
    let registry = NodeRegistry::new(
        Arc::new(MonotonicClock::new()),
        RegistryConfig::new(Duration::from_secs(60)),
    );
    let topology = Topology::from_registry(FailureDomain::Rack, &registry);
    assert_eq!(topology.level(), FailureDomain::Rack);
    assert_eq!(topology.candidates().count(), 0);
    assert!(topology.eligible_domains().is_empty());
    assert_eq!(topology.check(0), Ok(()));
    let error = topology.check(1).unwrap_err();
    assert_eq!(
        error,
        Unsatisfiable {
            level: FailureDomain::Rack,
            replicas: 1,
            domains: 0
        }
    );
    assert!(error.to_string().contains("different rack"), "{error}");
}

#[test]
fn creation_is_rejected_when_the_cluster_has_too_few_domains() {
    // Six nodes in two racks: three replicas fit by node, not by rack.
    let nodes = racked(2, 3, 1);
    let by_node = Topology::new(FailureDomain::Node, nodes.clone());
    let by_rack = Topology::new(FailureDomain::Rack, nodes.clone());
    let by_zone = Topology::new(FailureDomain::Zone, nodes);
    let shards = ShardCount::new(8).unwrap();
    assert!(by_node.place_bucket(&bucket(), shards, 3).is_ok());
    assert_eq!(
        by_rack.place_bucket(&bucket(), shards, 3),
        Err(Unsatisfiable {
            level: FailureDomain::Rack,
            replicas: 3,
            domains: 2
        })
    );
    assert!(by_rack.place_bucket(&bucket(), shards, 2).is_ok());
    assert_eq!(by_zone.check(2).unwrap_err().domains, 1);

    let mut settings = skys3_config::Config::from_toml_str(
        "[cluster]\ncluster_id = \"c\"\n[control_store]\nbackend = \"file\"\n[buckets.defaults]\nreplicas = 3\n",
    )
    .unwrap()
    .buckets()
    .defaults
    .clone();
    assert!(by_node.check_bucket(&settings).is_ok());
    assert!(by_rack.check_bucket(&settings).is_err());
    settings.replication.replicas = 2;
    settings.replication.min_write_replicas = 1;
    assert!(by_rack.check_bucket(&settings).is_ok());
}

#[test]
fn ineligible_nodes_are_never_chosen() {
    let mut departing = candidate(1, None, Some("r1"));
    departing.state = NodeState::Departing;
    let mut empty = candidate(2, None, Some("r2"));
    empty.capacity_bytes = 0;
    let unlabeled = candidate(3, None, None);
    let good = candidate(4, None, Some("r4"));
    let topology = Topology::new(
        FailureDomain::Rack,
        [departing, empty, unlabeled, good.clone()],
    );
    assert_eq!(topology.unlabeled(), [node(3)]);
    assert_eq!(
        topology
            .eligible()
            .map(|c| c.node.clone())
            .collect::<Vec<_>>(),
        [node(4)]
    );
    let placed = topology.place(&ShardRequest::new(&bucket(), ShardId::new(0), 3));
    assert_eq!(placed.members, [node(4)]);
    assert_eq!(placed.short, 2);
    assert!(!placed.is_complete());
}

#[test]
fn a_shortfall_never_co_locates() {
    // Three racks, one of them lost: a shard keeping its member in rack 0
    // gets one more member, in rack 1, and stays one short.
    let mut nodes = racked(3, 2, 1);
    for lost in &mut nodes[4..] {
        lost.state = NodeState::Departing;
    }
    let topology = Topology::new(FailureDomain::Rack, nodes);
    assert!(topology.check(3).is_err());
    let keep = [node(0), node(4)];
    let avoid = [node(4)];
    let placed = topology.place(
        &ShardRequest::new(&bucket(), ShardId::new(3), 3)
            .keep(&keep)
            .avoid(&avoid),
    );
    assert_eq!(placed.added.len(), 1);
    assert_eq!(
        topology.domain(&placed.added[0]),
        Some(Domain::Rack(label("rack-1".into())))
    );
    assert!(placed.is_complete(), "the departing member is still kept");

    // Dropping the departing member leaves the shard one short.
    let placed = topology.place(
        &ShardRequest::new(&bucket(), ShardId::new(3), 3)
            .keep(&keep[..1])
            .avoid(&avoid),
    );
    assert_eq!(placed.members.len(), 2);
    assert_eq!(placed.short, 1);
}

#[test]
fn kept_nodes_are_kept_once_and_take_their_domains() {
    let topology = Topology::new(FailureDomain::Rack, racked(3, 2, 1));
    // node-0 and node-1 share rack 0; node-9 is unknown and takes no domain.
    let keep = [node(0), node(1), node(0), node(9)];
    let placed = topology.place(&ShardRequest::new(&bucket(), ShardId::new(0), 5).keep(&keep));
    assert_eq!(&placed.members[..3], [node(0), node(1), node(9)]);
    let added = domains_of(&topology, &placed.added);
    assert_eq!(
        added.iter().collect::<BTreeSet<_>>(),
        [
            &Domain::Rack(label("rack-1".into())),
            &Domain::Rack(label("rack-2".into()))
        ]
        .into_iter()
        .collect()
    );
    assert!(placed.is_complete());

    // More kept nodes than replicas: nothing is added or removed.
    let placed = topology.place(&ShardRequest::new(&bucket(), ShardId::new(0), 1).keep(&keep));
    assert!(placed.added.is_empty());
    assert_eq!(placed.short, 0);
}

#[test]
fn live_nodes_are_preferred_to_suspect_ones() {
    let mut suspect = candidate(0, None, None);
    suspect.state = NodeState::Suspect;
    let topology = Topology::new(
        FailureDomain::Node,
        [suspect, candidate(1, None, None), candidate(2, None, None)],
    );
    for shard in 0..32 {
        let placed = topology.place(&ShardRequest::new(&bucket(), ShardId::new(shard), 2));
        assert!(!placed.members.contains(&node(0)), "{placed:?}");
    }
    let placed = topology.place(&ShardRequest::new(&bucket(), ShardId::new(0), 3));
    assert_eq!(placed.members.last(), Some(&node(0)));
    let primary = topology.choose_primary(&bucket(), ShardId::new(0), &placed.members);
    assert_ne!(primary, Some(node(0)));
    assert_eq!(
        topology.choose_primary(&bucket(), ShardId::new(0), &[node(7)]),
        None
    );
}

#[test]
fn capacity_and_load_steer_placement() {
    let mut big = candidate(0, None, None);
    big.capacity_bytes = 4 * TB;
    let mut loaded = candidate(1, None, None);
    loaded.shards = 10;
    let topology = Topology::new(FailureDomain::Node, [big, loaded, candidate(2, None, None)]);
    let placed = topology.place(&ShardRequest::new(&bucket(), ShardId::new(0), 2));
    assert_eq!(placed.added, [node(0), node(2)]);

    // Recording shards counts members, learners, and the primary.
    let mut topology = topology;
    topology.record(&ShardConfig {
        bucket_id: bucket(),
        shard: ShardId::new(0),
        epoch: Epoch::new(1),
        primary: node(2),
        members: vec![node(2), node(0)],
        learners: vec![node(2), node(7)],
        min_write_replicas: 1,
        replicas: 2,
        proposal_id: ProposalId::new("p1").unwrap(),
    });
    let shards: Vec<_> = topology
        .candidates()
        .map(|c| (c.shards, c.primaries))
        .collect();
    assert_eq!(shards, [(1, 0), (10, 0), (1, 1)]);
}

#[test]
fn spread_prefers_other_zones_and_racks_below_the_level() {
    // Node level, two zones: a shard of two goes to both zones.
    let nodes = racked(4, 2, 2);
    let topology = Topology::new(FailureDomain::Node, nodes.clone());
    for shard in 0..16 {
        let placed = topology.place(&ShardRequest::new(&bucket(), ShardId::new(shard), 2));
        let zones: BTreeSet<_> = placed
            .members
            .iter()
            .map(|n| topology.get(n).unwrap().zone.clone())
            .collect();
        assert_eq!(zones.len(), 2, "{placed:?}");
        let placed = topology.place(&ShardRequest::new(&bucket(), ShardId::new(shard), 4));
        let racks: BTreeSet<_> = placed
            .members
            .iter()
            .map(|n| topology.get(n).unwrap().rack.clone())
            .collect();
        assert_eq!(racks.len(), 4, "{placed:?}");
    }
    // Rack level: two racks out of four, in different zones.
    let topology = Topology::new(FailureDomain::Rack, nodes);
    for shard in 0..16 {
        let placed = topology.place(&ShardRequest::new(&bucket(), ShardId::new(shard), 2));
        let zones: BTreeSet<_> = placed
            .members
            .iter()
            .map(|n| topology.get(n).unwrap().zone.clone())
            .collect();
        assert_eq!(zones.len(), 2, "{placed:?}");
    }
}

#[test]
fn a_bucket_spreads_members_and_primaries_evenly() {
    // Nine equal nodes, 3 replicas, 255 shards: loads stay within one.
    let topology = Topology::new(
        FailureDomain::Node,
        (0..9).map(|n| candidate(n, None, None)),
    );
    let placed = topology
        .place_bucket(&bucket(), ShardCount::new(255).unwrap(), 3)
        .unwrap();
    assert_eq!(placed.len(), 255);
    let mut members: BTreeMap<&NodeId, u32> = BTreeMap::new();
    let mut primaries: BTreeMap<&NodeId, u32> = BTreeMap::new();
    for shard in &placed {
        assert!(shard.members.contains(&shard.primary));
        for member in &shard.members {
            *members.entry(member).or_default() += 1;
        }
        *primaries.entry(&shard.primary).or_default() += 1;
    }
    let spread = |counts: &BTreeMap<&NodeId, u32>| {
        let max = counts.values().max().unwrap();
        let min = counts.values().min().unwrap();
        (counts.len(), max - min)
    };
    assert_eq!(spread(&members), (9, 0));
    assert_eq!(spread(&primaries), (9, 1), "255 primaries over 9 nodes");
    // The topology is left as it was.
    assert!(topology.candidates().all(|c| c.shards == 0));
    // Replicas of 0 count as 1.
    let single = topology
        .place_bucket(&bucket(), ShardCount::new(2).unwrap(), 0)
        .unwrap();
    assert!(single.iter().all(|shard| shard.members.len() == 1));
}

#[test]
fn placement_does_not_depend_on_listing_order() {
    let nodes = racked(5, 3, 3);
    let forward = Topology::new(FailureDomain::Rack, nodes.clone());
    let backward = Topology::new(FailureDomain::Rack, nodes.into_iter().rev());
    let shards = ShardCount::new(16).unwrap();
    assert_eq!(
        forward.place_bucket(&bucket(), shards, 3),
        backward.place_bucket(&bucket(), shards, 3)
    );
}

#[test]
fn equal_nodes_share_new_shards_by_hash() {
    // With no load, which nodes a shard gets depends on the shard.
    let topology = Topology::new(
        FailureDomain::Node,
        (0..12).map(|n| candidate(n, None, None)),
    );
    let firsts: BTreeSet<_> = (0..64)
        .map(|shard| {
            topology
                .place(&ShardRequest::new(&bucket(), ShardId::new(shard), 1))
                .added[0]
                .clone()
        })
        .collect();
    assert!(firsts.len() > 6, "{firsts:?}");
}

/// A random node: an optional zone and rack from a few, some capacity
/// (sometimes none), a state, and a load.
fn arb_candidate(n: usize) -> impl Strategy<Value = Candidate> {
    (
        proptest::option::weighted(0.9, 0..4_usize),
        proptest::option::weighted(0.9, 0..8_usize),
        prop_oneof![9 => 1..=16_u64, 1 => Just(0_u64)],
        prop_oneof![
            6 => Just(NodeState::Live),
            2 => Just(NodeState::Suspect),
            1 => Just(NodeState::Departing)
        ],
        0..40_u32,
    )
        .prop_map(move |(zone, rack, capacity, state, shards)| Candidate {
            node: node(n),
            zone: zone.map(|z| label(format!("zone-{z}"))),
            rack: rack.map(|r| label(format!("rack-{r}"))),
            capacity_bytes: capacity * TB,
            state,
            shards,
            primaries: shards / 3,
        })
}

fn arb_topology() -> impl Strategy<Value = Topology> {
    (
        prop_oneof![
            Just(FailureDomain::Node),
            Just(FailureDomain::Rack),
            Just(FailureDomain::Zone)
        ],
        0..24_usize,
    )
        .prop_flat_map(|(level, count)| {
            (0..count)
                .map(arb_candidate)
                .collect::<Vec<_>>()
                .prop_map(move |nodes| Topology::new(level, nodes))
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// The engine's rule: new members never share a domain with each
    /// other or with a kept node, are eligible, and fall short only when
    /// no eligible domain is left.
    #[test]
    fn placement_never_puts_two_members_in_one_domain(
        topology in arb_topology(),
        replicas in 0..6_u8,
        shard in any::<u8>(),
        keep in proptest::collection::vec(0..26_usize, 0..4),
        avoid in proptest::collection::vec(0..26_usize, 0..4),
    ) {
        let keep: Vec<NodeId> = keep.into_iter().map(node).collect();
        let avoid: Vec<NodeId> = avoid.into_iter().map(node).collect();
        let bucket = bucket();
        let request = ShardRequest::new(&bucket, ShardId::new(shard), replicas)
            .keep(&keep)
            .avoid(&avoid);
        let placed = topology.place(&request);
        prop_assert_eq!(&placed, &topology.place(&request), "deterministic");

        let kept: BTreeSet<Domain> = keep.iter().filter_map(|n| topology.domain(n)).collect();
        let added = domains_of(&topology, &placed.added);
        prop_assert!(distinct(&added), "{:?}", placed);
        prop_assert!(added.iter().all(|d| !kept.contains(d)), "{:?}", placed);
        for node in &placed.added {
            prop_assert!(topology.get(node).unwrap().is_eligible(topology.level()));
            prop_assert!(!avoid.contains(node) && !keep.contains(node));
        }
        let unique_keep = keep.iter().collect::<BTreeSet<_>>().len();
        prop_assert_eq!(placed.members.len(), unique_keep + placed.added.len());
        prop_assert_eq!(
            usize::from(placed.short),
            usize::from(replicas).saturating_sub(placed.members.len())
        );
        if !placed.is_complete() {
            // Short only because every eligible domain is taken.
            let taken: BTreeSet<Domain> = kept.into_iter().chain(added).collect();
            let free = topology
                .eligible()
                .filter(|c| !avoid.contains(&c.node) && !keep.contains(&c.node))
                .filter_map(|c| c.domain(topology.level()))
                .any(|d| !taken.contains(&d));
            prop_assert!(!free, "{:?}", placed);
        }
    }

    /// A bucket is placed exactly when its policy is satisfiable, and then
    /// every shard has `replicas` members in separate domains.
    #[test]
    fn buckets_are_placed_whole_or_rejected(
        topology in arb_topology(),
        replicas in 1..6_u8,
        shards in 1..=32_u32,
    ) {
        let shards = ShardCount::new(shards).unwrap();
        let placed = topology.place_bucket(&bucket(), shards, replicas);
        let domains = topology.eligible_domains().len();
        match placed {
            Err(error) => {
                prop_assert!(domains < usize::from(replicas));
                prop_assert_eq!(error.domains, domains);
                prop_assert_eq!(Err(error), topology.check(replicas));
            }
            Ok(placed) => {
                prop_assert!(domains >= usize::from(replicas));
                prop_assert_eq!(placed.len(), usize::try_from(shards.get()).unwrap());
                for (shard, new) in shards.shards().zip(&placed) {
                    prop_assert_eq!(new.shard, shard);
                    prop_assert_eq!(new.members.len(), usize::from(replicas));
                    prop_assert!(distinct(&domains_of(&topology, &new.members)));
                    prop_assert!(new.members.contains(&new.primary));
                    for member in &new.members {
                        prop_assert!(topology.get(member).unwrap().is_eligible(topology.level()));
                    }
                }
            }
        }
    }

    /// Nodes depart one after another, and each round re-homes the shards
    /// that named them the way replacement will (plan M3-05): the shard
    /// keeps its other members and avoids the departing ones. However
    /// short the cluster runs, no shard ever has two members in a domain.
    #[test]
    fn re_homing_after_losses_never_co_locates(
        topology in arb_topology(),
        replicas in 1..5_u8,
        losses in proptest::collection::vec(0..24_usize, 1..8),
    ) {
        let bucket = bucket();
        let Ok(placed) = topology.place_bucket(&bucket, ShardCount::new(16).unwrap(), replicas)
        else {
            return Ok(());
        };
        let mut topology = topology;
        let mut shards: Vec<(ShardId, Vec<NodeId>)> = placed
            .into_iter()
            .map(|shard| (shard.shard, shard.members))
            .collect();
        for lost in losses {
            let lost = node(lost);
            let Some(candidate) = topology.nodes.get_mut(&lost) else {
                continue;
            };
            candidate.state = NodeState::Departing;
            for (shard, members) in &mut shards {
                if !members.contains(&lost) {
                    continue;
                }
                let keep: Vec<NodeId> = members.iter().filter(|n| **n != lost).cloned().collect();
                let avoid = [lost.clone()];
                let placed = topology.place(
                    &ShardRequest::new(&bucket, *shard, replicas).keep(&keep).avoid(&avoid),
                );
                prop_assert!(placed.members.len() <= usize::from(replicas));
                *members = placed.members;
            }
            for (_, members) in &shards {
                prop_assert!(distinct(&domains_of(&topology, members)), "{:?}", members);
            }
        }
    }
}
