use std::collections::{BTreeMap, BTreeSet};

use proptest::prelude::*;

use super::*;
use crate::NodeState;

pub(super) const TB: u64 = 1 << 40;
pub(super) const MIB: u64 = 1 << 20;

pub(super) fn node(n: usize) -> NodeId {
    NodeId::new(format!("node-{n}")).unwrap()
}

fn label(text: String) -> skys3_types::Label {
    skys3_types::Label::new(text).unwrap()
}

pub(super) fn bucket() -> BucketId {
    BucketId::new("b-1").unwrap()
}

pub(super) fn defaults() -> GeometryPolicy {
    GeometryPolicy::from_config(&EcConfig::default()).unwrap()
}

fn geometry(k: usize, m: usize) -> Geometry {
    Geometry::new(k, m).unwrap()
}

/// A live node `n` with one terabyte, in `rack` and `zone`.
pub(super) fn candidate(n: usize, zone: Option<usize>, rack: Option<usize>) -> Candidate {
    Candidate {
        node: node(n),
        zone: zone.map(|z| label(format!("zone-{z}"))),
        rack: rack.map(|r| label(format!("rack-{r}"))),
        capacity_bytes: TB,
        state: NodeState::Live,
        shards: 0,
        primaries: 0,
    }
}

/// Live nodes, `sizes[r]` of them in rack `r`.
pub(super) fn racks(sizes: &[usize]) -> Vec<Candidate> {
    let mut nodes = Vec::new();
    for (rack, &size) in sizes.iter().enumerate() {
        for _ in 0..size {
            nodes.push(candidate(nodes.len(), Some(rack % 2), Some(rack)));
        }
    }
    nodes
}

/// `n` live nodes without labels, so no soft spread below the `node`
/// level steers their choice.
pub(super) fn flat(n: usize) -> Vec<Candidate> {
    (0..n).map(|i| candidate(i, None, None)).collect()
}

pub(super) fn planner_for(level: FailureDomain, nodes: Vec<Candidate>) -> FragmentPlanner {
    FragmentPlanner::new(Topology::new(level, nodes), defaults())
}

fn request<'a>(bucket: &'a BucketId, stripe: u32) -> StripeRequest<'a> {
    StripeRequest::new(bucket, ShardId::new(3), "videos/cat.mp4", stripe, 64 * MIB)
}

/// The §8.3 table, for `failure_domain = "node"` and the default policy.
fn design_table(nodes: usize) -> Option<Geometry> {
    match nodes {
        0..=4 => None,
        5 | 6 => Some(Geometry::RS_3_2),
        7 | 8 => Some(Geometry::RS_4_2),
        9 | 10 => Some(Geometry::RS_6_2),
        _ => Some(Geometry::RS_8_2),
    }
}

/// The table's "nodes left outside a stripe" column.
fn spare_nodes(nodes: usize) -> std::ops::RangeInclusive<usize> {
    match nodes {
        5 | 6 => 0..=1,
        7..=10 => 1..=2,
        _ => 1..=usize::MAX,
    }
}

/// The widest default geometry `domains` domains allow, at most `m = 2`
/// fragments in each: a stripe of `k+m` needs `⌈(k+m)/m⌉` (§8.3).
fn domain_bound(domains: usize) -> Option<Geometry> {
    Geometry::DESIGN_TABLE
        .into_iter()
        .rev()
        .find(|g| g.total_fragments().div_ceil(2) <= domains)
}

/// Checks the two rules on `plan`, and that its nodes are eligible and
/// not avoided.
fn check_plan(topology: &Topology, plan: &StripePlan, avoid: &[NodeId]) {
    let m = plan.geometry.parity_fragments();
    assert_eq!(plan.nodes.len(), plan.geometry.total_fragments());
    let distinct: BTreeSet<&NodeId> = plan.nodes.iter().collect();
    assert_eq!(distinct.len(), plan.nodes.len(), "one per node: {plan:?}");
    let mut per_domain: BTreeMap<Domain, usize> = BTreeMap::new();
    for node in &plan.nodes {
        let candidate = topology.get(node).unwrap();
        assert!(candidate.is_eligible(topology.level()), "{node} eligible");
        assert!(!avoid.contains(node), "{node} avoided");
        *per_domain
            .entry(topology.domain(node).unwrap())
            .or_default() += 1;
    }
    assert!(
        per_domain.values().all(|&count| count <= m),
        "at most {m} per domain: {per_domain:?}"
    );
    assert!(per_domain.len() >= plan.geometry.total_fragments().div_ceil(m));
}

#[test]
fn the_default_ladder_is_the_design_table() {
    let policy = defaults();
    assert_eq!(policy.geometries(), Geometry::DESIGN_TABLE);
    assert_eq!(policy.narrowest(), Geometry::RS_3_2);
    assert_eq!(policy.widest(), Geometry::RS_8_2);
    assert_eq!(policy.parity_fragments(), 2);
    assert_eq!(policy.min_eligible_nodes(), 5);
}

#[test]
fn ladders_grow_in_steps_of_m() {
    let ladder = |m, k, n| {
        GeometryPolicy::new(m, k, n)
            .unwrap()
            .geometries()
            .into_iter()
            .map(|g| g.to_string())
            .collect::<Vec<_>>()
    };
    // The design's example configuration (§14).
    assert_eq!(ladder(3, 6, 7), ["4+3", "6+3"]);
    assert_eq!(ladder(1, 4, 3), ["2+1", "3+1", "4+1"]);
    // A widest `k` off the steps is still offered.
    assert_eq!(ladder(2, 7, 5), ["3+2", "4+2", "6+2", "7+2"]);
    // `min_eligible_nodes` no greater than `m` still leaves one data
    // fragment.
    assert_eq!(ladder(2, 8, 2), ["1+2", "2+2", "4+2", "6+2", "8+2"]);
    // A `min_eligible_nodes` past the widest stripe needs spares.
    assert_eq!(ladder(2, 3, 12), ["3+2"]);
    assert_eq!(ladder(4, 1, 5), ["1+4"]);

    let picky = GeometryPolicy::new(2, 3, 12).unwrap();
    let room = |n: usize| StripeRoom {
        nodes: n,
        domains: n,
        fragments: n,
    };
    assert_eq!(picky.choose(&room(11)), None);
    assert_eq!(picky.choose(&room(12)), Some(Geometry::RS_3_2));
}

#[test]
fn policies_need_valid_geometries() {
    assert!(GeometryPolicy::new(0, 8, 5).is_err());
    assert!(GeometryPolicy::new(2, 0, 5).is_err());
    assert!(GeometryPolicy::new(2, 254, 5).is_err());
    let widest = GeometryPolicy::new(2, 253, 5).unwrap();
    assert_eq!(widest.widest().total_fragments(), Geometry::MAX_FRAGMENTS);
    let config = EcConfig {
        parity_fragments: 3,
        max_data_fragments: 6,
        min_eligible_nodes: 7,
        ..EcConfig::default()
    };
    let policy = GeometryPolicy::from_config(&config).unwrap();
    assert_eq!(policy.narrowest(), geometry(4, 3));
}

/// §8.3: the table of geometries by eligible node count.
#[test]
fn node_counts_reproduce_the_design_table() {
    let bucket = bucket();
    for n in 0..=24 {
        let mut planner = planner_for(FailureDomain::Node, flat(n));
        let expected = design_table(n);
        assert_eq!(planner.geometry().ok(), expected, "{n} nodes");
        match planner.plan(&request(&bucket, 0)) {
            Ok(plan) => {
                assert_eq!(Some(plan.geometry), expected);
                assert!(spare_nodes(n).contains(&(n - plan.nodes.len())), "{n}");
                check_plan(planner.topology(), &plan, &[]);
            }
            Err(error) => {
                assert_eq!(expected, None, "{n} nodes: {error}");
                assert_eq!(error.room.nodes, n);
            }
        }
    }
}

/// §8.3's example: an 11-node cluster in three racks uses 4+2, not 8+2.
#[test]
fn eleven_nodes_in_three_racks_use_four_plus_two() {
    let bucket = bucket();
    let mut planner = planner_for(FailureDomain::Rack, racks(&[4, 4, 3]));
    assert_eq!(
        planner.room(&[]),
        StripeRoom {
            nodes: 11,
            domains: 3,
            fragments: 6,
        }
    );
    let plan = planner.plan(&request(&bucket, 0)).unwrap();
    assert_eq!(plan.geometry, Geometry::RS_4_2);
    check_plan(planner.topology(), &plan, &[]);

    // At the node level the same cluster uses 8+2; with more racks the
    // domain count stops binding.
    assert_eq!(
        planner_geometry(FailureDomain::Node, &[4, 4, 3]),
        Some(Geometry::RS_8_2)
    );
    assert_eq!(
        planner_geometry(FailureDomain::Rack, &[3, 3, 3, 2]),
        Some(Geometry::RS_6_2)
    );
    assert_eq!(
        planner_geometry(FailureDomain::Rack, &[3, 2, 2, 2, 2]),
        Some(Geometry::RS_8_2)
    );
    assert_eq!(planner_geometry(FailureDomain::Rack, &[6, 5]), None);
}

fn planner_geometry(level: FailureDomain, sizes: &[usize]) -> Option<Geometry> {
    planner_for(level, racks(sizes)).geometry().ok()
}

/// Three domains are not enough when they cannot each hold `m` fragments:
/// the design's `⌈(k+m)/m⌉` domains are needed, not always enough.
#[test]
fn uneven_domains_hold_fewer_fragments() {
    let planner = planner_for(FailureDomain::Rack, racks(&[9, 1, 1]));
    let error = planner.geometry().unwrap_err();
    assert_eq!(
        error.room,
        StripeRoom {
            nodes: 11,
            domains: 3,
            fragments: 4,
        }
    );
    assert_eq!(
        error.to_string(),
        "no stripe geometry fits: the narrowest, 3+2, needs 5 eligible nodes with room for 5 \
         fragments at most 2 per rack (failure_domain = \"rack\"), and the cluster has 11 \
         eligible nodes in 3 domains with room for 4"
    );
    assert_eq!(
        planner_geometry(FailureDomain::Rack, &[9, 2, 1]),
        Some(Geometry::RS_3_2)
    );
}

#[test]
fn only_eligible_nodes_count() {
    let mut nodes = flat(7);
    nodes[0].state = NodeState::Departing;
    nodes[1].capacity_bytes = 0;
    nodes[2].state = NodeState::Suspect;
    let planner = planner_for(FailureDomain::Node, nodes.clone());
    assert_eq!(planner.geometry(), Ok(Geometry::RS_3_2));

    // Unlabeled nodes count only at the node level.
    nodes = racks(&[3, 2, 2]);
    nodes[0].rack = None;
    nodes[1].zone = None;
    assert_eq!(
        planner_for(FailureDomain::Node, nodes.clone()).geometry(),
        Ok(Geometry::RS_4_2)
    );
    let rack = planner_for(FailureDomain::Rack, nodes.clone());
    assert_eq!(rack.room(&[]).nodes, 6);
    assert_eq!(planner_for(FailureDomain::Zone, nodes).room(&[]).nodes, 6);
}

#[test]
fn live_nodes_come_first_and_avoided_ones_never() {
    let bucket = bucket();
    let mut nodes = flat(12);
    nodes[4].state = NodeState::Suspect;
    nodes[7].state = NodeState::Suspect;
    let mut planner = planner_for(FailureDomain::Node, nodes);
    for stripe in 0..20 {
        let plan = planner.plan(&request(&bucket, stripe)).unwrap();
        assert_eq!(plan.geometry, Geometry::RS_8_2);
        // Ten fragments, ten live nodes: the suspect ones get none.
        assert!(!plan.nodes.contains(&node(4)) && !plan.nodes.contains(&node(7)));
    }

    // Avoiding nodes narrows the geometry rather than breaking a rule.
    let avoid = [node(0), node(1), node(2)];
    let plan = planner.plan(&request(&bucket, 0).avoid(&avoid)).unwrap();
    assert_eq!(plan.geometry, Geometry::RS_6_2);
    check_plan(planner.topology(), &plan, &avoid);
    // Suspect nodes count, and are used once no live node is left.
    assert!(plan.nodes.contains(&node(4)) || plan.nodes.contains(&node(7)));
}

#[test]
fn stripes_spread_evenly_over_equal_nodes() {
    let bucket = bucket();
    let mut planner = planner_for(FailureDomain::Node, flat(11));
    let mut counts: BTreeMap<NodeId, usize> = BTreeMap::new();
    for stripe in 0..110 {
        let plan = planner.plan(&request(&bucket, stripe)).unwrap();
        for node in plan.nodes {
            *counts.entry(node).or_default() += 1;
        }
        let max = counts.values().max().unwrap();
        let min = if counts.len() == 11 {
            *counts.values().min().unwrap()
        } else {
            0
        };
        assert!(max - min <= 1, "after stripe {stripe}: {counts:?}");
    }
    assert!(counts.values().all(|&count| count == 100));
    assert_eq!(planner.held(&node(0)), 100 * (64 * MIB).div_ceil(8));
}

#[test]
fn fuller_nodes_are_left_outside() {
    let bucket = bucket();
    let mut planner = planner_for(FailureDomain::Node, flat(7));
    planner.hold(&node(3), TB / 2);
    let plan = planner.plan(&request(&bucket, 0)).unwrap();
    assert_eq!(plan.geometry, Geometry::RS_4_2);
    assert!(!plan.nodes.contains(&node(3)));

    // A smaller disk fills faster.
    let mut nodes = flat(7);
    nodes[5].capacity_bytes = TB / 4;
    let mut planner = planner_for(FailureDomain::Node, nodes);
    planner.hold(&node(0), TB / 8);
    planner.hold(&node(5), TB / 8);
    let plan = planner.plan(&request(&bucket, 0)).unwrap();
    assert!(!plan.nodes.contains(&node(5)), "{plan:?}");
}

#[test]
fn stripes_spread_over_domains_before_filling_them() {
    let bucket = bucket();
    // Six racks of two, at the rack level: 8+2 puts ten fragments in six
    // racks, never three in one, and uses every rack.
    let mut planner = planner_for(FailureDomain::Rack, racks(&[2, 2, 2, 2, 2, 2]));
    for stripe in 0..10 {
        let plan = planner.plan(&request(&bucket, stripe)).unwrap();
        assert_eq!(plan.geometry, Geometry::RS_8_2);
        check_plan(planner.topology(), &plan, &[]);
        let racks: BTreeSet<Domain> = plan
            .nodes
            .iter()
            .map(|n| planner.topology().domain(n).unwrap())
            .collect();
        assert_eq!(racks.len(), 6);
    }

    // At the node level, a 6+2 stripe in five racks of two takes a node in
    // every rack (the soft spread below the required level).
    let mut planner = planner_for(FailureDomain::Node, racks(&[2, 2, 2, 2, 2]));
    let plan = planner.plan(&request(&bucket, 0)).unwrap();
    assert_eq!(plan.geometry, Geometry::RS_6_2);
    let racks: BTreeSet<_> = plan
        .nodes
        .iter()
        .map(|n| planner.topology().get(n).unwrap().rack.clone())
        .collect();
    assert_eq!(racks.len(), 5);
}

#[test]
fn plans_become_coded_stripes_that_keep_their_geometry() {
    let bucket = bucket();
    let mut planner = planner_for(FailureDomain::Rack, racks(&[3, 3, 2]));
    let plan = planner.plan(&request(&bucket, 1)).unwrap();
    assert_eq!(plan.geometry, Geometry::RS_4_2);
    let ids: Vec<FragmentId> = (0..6).map(|i| FragmentId::new(100 + i)).collect();
    let stripe = plan
        .locate(1, 64 * MIB, 64 * MIB, CodecId::CURRENT, &ids)
        .unwrap();
    assert_eq!(stripe.geometry(), Geometry::RS_4_2);
    for (index, location) in stripe.fragments().iter().enumerate() {
        assert_eq!(location.node, plan.nodes[index]);
        assert_eq!(location.fragment, ids[index]);
    }

    // The cluster grows: new stripes are wider, the recorded one is not.
    let mut grown = planner_for(FailureDomain::Rack, racks(&[3, 3, 2, 2, 2]));
    assert_eq!(
        grown.plan(&request(&bucket, 2)).unwrap().geometry,
        Geometry::RS_8_2
    );
    assert_eq!(stripe.geometry(), Geometry::RS_4_2);

    assert_eq!(
        plan.locate(1, 0, 1, CodecId::CURRENT, &ids[..5])
            .unwrap_err()
            .to_string(),
        "stripe 1 is 4+2 but locates 5 fragments"
    );
    assert!(plan.locate(1, 0, 0, CodecId::CURRENT, &ids).is_err());
}

#[test]
fn node_level_errors_name_the_level() {
    let error = planner_for(FailureDomain::Node, flat(4))
        .geometry()
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "no stripe geometry fits: the narrowest, 3+2, needs 5 eligible nodes with room for 5 \
         fragments at most 2 per node (failure_domain = \"node\"), and the cluster has 4 \
         eligible nodes in 4 domains with room for 4"
    );
}

pub(super) fn arb_state() -> impl Strategy<Value = NodeState> {
    prop_oneof![
        6 => Just(NodeState::Live),
        2 => Just(NodeState::Suspect),
        1 => Just(NodeState::Departing),
    ]
}

pub(super) fn arb_level() -> impl Strategy<Value = FailureDomain> {
    prop_oneof![
        Just(FailureDomain::Node),
        Just(FailureDomain::Rack),
        Just(FailureDomain::Zone),
    ]
}

/// A random cluster: up to 30 nodes in up to 8 racks and 4 zones, some
/// unlabeled, suspect, departing, or without capacity.
fn arb_topology() -> impl Strategy<Value = Topology> {
    let node = (
        proptest::option::weighted(0.9, 0..4_usize),
        proptest::option::weighted(0.9, 0..8_usize),
        arb_state(),
        prop_oneof![1 => Just(0_u64), 9 => 1..=TB],
    );
    (arb_level(), proptest::collection::vec(node, 0..30)).prop_map(|(level, nodes)| {
        let nodes =
            nodes
                .into_iter()
                .enumerate()
                .map(|(n, (zone, rack, state, capacity_bytes))| Candidate {
                    state,
                    capacity_bytes,
                    ..candidate(n, zone, rack)
                });
        Topology::new(level, nodes)
    })
}

/// A random valid policy.
fn arb_policy() -> impl Strategy<Value = GeometryPolicy> {
    (1..=4_usize, 1..=12_usize, 0..=12_usize)
        .prop_map(|(m, k, extra)| GeometryPolicy::new(m, k, m + 1 + extra).unwrap())
}

/// A [`ReplaceRequest`] for `count` fragments of `plan`'s stripe 0 that
/// keeps the fragments of `plan` at the indices `keep`.
fn replacing<'a>(
    bucket: &'a BucketId,
    plan: &StripePlan,
    keep: &'a [NodeId],
    count: usize,
    avoid: &'a [NodeId],
) -> ReplaceRequest<'a> {
    ReplaceRequest {
        bucket,
        shard: ShardId::new(0),
        key: "key",
        stripe: 0,
        geometry: plan.geometry,
        data_len: 64 * MIB,
        keep,
        count,
        avoid,
    }
}

#[test]
fn repairs_place_fragments_around_the_ones_a_stripe_keeps() {
    // Seven flat nodes: 4+2 with one spare node.
    let mut planner = planner_for(FailureDomain::Node, flat(7));
    let bucket = bucket();
    let plan = planner.plan(&request(&bucket, 0)).unwrap();
    assert_eq!(plan.geometry, Geometry::RS_4_2);
    let spare = (0..7).map(node).find(|n| !plan.nodes.contains(n)).unwrap();
    // One lost: it goes to the spare node, never to a kept or lost one.
    let lost = plan.nodes[2].clone();
    let keep: Vec<NodeId> = plan.nodes.iter().filter(|n| **n != lost).cloned().collect();
    let avoid = [lost.clone()];
    let placed = planner
        .replace(&replacing(&bucket, &plan, &keep, 1, &avoid))
        .unwrap();
    assert_eq!(placed, std::slice::from_ref(&spare));
    assert!(planner.held(&spare) >= 16 * MIB);
    // Two lost and one spare: no room until a node joins.
    let keep = &plan.nodes[2..];
    let avoid = [plan.nodes[0].clone(), plan.nodes[1].clone()];
    assert_eq!(
        planner.replace(&replacing(&bucket, &plan, keep, 2, &avoid)),
        None
    );
    // A lost fragment's node may take it back when nothing forbids it, as
    // after its disk was replaced.
    let keep = &plan.nodes[1..];
    let placed = planner
        .replace(&replacing(&bucket, &plan, keep, 2, &[]))
        .unwrap();
    let placed: BTreeSet<&NodeId> = placed.iter().collect();
    assert_eq!(placed, BTreeSet::from([&plan.nodes[0], &spare]));
}

#[test]
fn repairs_keep_the_per_domain_cap() {
    // Three racks of four: 4+2, two fragments per rack. The kept
    // fragments fill two racks, so the lost ones go to the third.
    let mut planner = planner_for(FailureDomain::Rack, racks(&[4, 4, 4]));
    let bucket = bucket();
    let plan = planner.plan(&request(&bucket, 0)).unwrap();
    let topology = planner.topology().clone();
    let rack = |n: &NodeId| topology.domain(n).unwrap();
    let third = rack(&plan.nodes[0]);
    let keep: Vec<NodeId> = plan
        .nodes
        .iter()
        .filter(|n| rack(n) != third)
        .cloned()
        .collect();
    assert_eq!(keep.len(), 4);
    let lost: Vec<NodeId> = plan
        .nodes
        .iter()
        .filter(|n| rack(n) == third)
        .cloned()
        .collect();
    let placed = planner
        .replace(&replacing(&bucket, &plan, &keep, 2, &lost))
        .unwrap();
    assert_eq!(placed.len(), 2);
    assert!(placed.iter().all(|n| rack(n) == third && !lost.contains(n)));
    // Only one node left in that rack: no room.
    let avoid: Vec<NodeId> = lost
        .iter()
        .cloned()
        .chain(placed.iter().take(1).cloned())
        .collect();
    assert_eq!(
        planner.replace(&replacing(&bucket, &plan, &keep, 2, &avoid)),
        None
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// On any cluster and policy: a chosen geometry is supported (enough
    /// eligible nodes and domains, room under the caps, a spare unless it
    /// is the narrowest) and is the widest supported; none is chosen when
    /// no placement could keep the rules; and every plan keeps them.
    #[test]
    fn geometry_and_placement_keep_the_rules(
        topology in arb_topology(),
        policy in arb_policy(),
        avoid in proptest::collection::vec(0..30_usize, 0..3),
        stripes in 1..4_u32,
    ) {
        let avoid: Vec<NodeId> = avoid.into_iter().map(node).collect();
        let m = policy.parity_fragments();

        // What the eligible nodes offer, counted from first principles.
        let mut per_domain: BTreeMap<Domain, usize> = BTreeMap::new();
        for candidate in topology.candidates() {
            let eligible = candidate.state != NodeState::Departing
                && candidate.capacity_bytes > 0
                && !avoid.contains(&candidate.node);
            if let (true, Some(domain)) = (eligible, candidate.domain(topology.level())) {
                *per_domain.entry(domain).or_default() += 1;
            }
        }
        let nodes: usize = per_domain.values().sum();
        let room: usize = per_domain.values().map(|&n| n.min(m)).sum();
        let mut planner = FragmentPlanner::new(topology.clone(), policy);
        prop_assert_eq!(
            planner.room(&avoid),
            StripeRoom { nodes, domains: per_domain.len(), fragments: room }
        );

        let supported = |g: Geometry| {
            let total = g.total_fragments();
            nodes >= policy.min_eligible_nodes()
                && total <= room
                && per_domain.len() >= total.div_ceil(m)
                && (g == policy.narrowest() || nodes > total)
        };
        let chosen = policy.choose(&planner.room(&avoid));
        let ladder = policy.geometries();
        match chosen {
            Some(g) => {
                prop_assert!(supported(g));
                prop_assert!(ladder.contains(&g));
                prop_assert!(ladder.iter().filter(|w| **w > g).all(|w| !supported(*w)));
            }
            None => {
                // No placement can exist: some domain would hold more
                // than `m`, or too few nodes are eligible.
                let narrowest = policy.narrowest().total_fragments();
                prop_assert!(nodes < policy.min_eligible_nodes() || room < narrowest);
                prop_assert!(ladder.iter().all(|g| !supported(*g)));
            }
        }

        let bucket = bucket();
        for stripe in 0..stripes {
            let request = request(&bucket, stripe).avoid(&avoid);
            let before = planner.clone();
            match planner.plan(&request) {
                Ok(plan) => {
                    prop_assert_eq!(Some(plan.geometry), chosen);
                    check_plan(&topology, &plan, &avoid);
                    let mut again = before;
                    prop_assert_eq!(again.plan(&request).unwrap(), plan, "deterministic");
                }
                Err(error) => {
                    prop_assert_eq!(chosen, None);
                    prop_assert_eq!(error.room.fragments, room);
                }
            }
        }
    }

    /// §8.3's table at the node level, whatever the labels, suspicions,
    /// and ineligible nodes around the eligible ones.
    #[test]
    fn the_table_holds_for_any_node_level_cluster(
        topology in arb_topology(),
    ) {
        let topology = Topology::new(FailureDomain::Node, topology.candidates().cloned());
        let eligible = topology.eligible().count();
        let mut planner = FragmentPlanner::new(topology, defaults());
        let expected = design_table(eligible);
        prop_assert_eq!(planner.geometry().ok(), expected);
        let bucket = bucket();
        if let Ok(plan) = planner.plan(&request(&bucket, 0)) {
            prop_assert!(spare_nodes(eligible).contains(&(eligible - plan.nodes.len())));
        }
    }

    /// The geometry is the widest that both the node count (the table) and
    /// the domain count allow, when each domain has room for `m`
    /// fragments: an 11-node cluster in any three racks uses 4+2.
    #[test]
    fn nodes_and_domains_both_bound_the_geometry(
        extra in proptest::collection::vec(0..4_usize, 1..8),
        three_racks in proptest::collection::vec(0..6_usize, 3),
    ) {
        let sizes: Vec<usize> = extra.iter().map(|e| e + 2).collect();
        let n: usize = sizes.iter().sum();
        let expected = design_table(n).zip(domain_bound(sizes.len())).map(|(a, b)| a.min(b));
        let mut planner = planner_for(FailureDomain::Rack, racks(&sizes));
        prop_assert_eq!(planner.geometry().ok(), expected);
        let bucket = bucket();
        if let Ok(plan) = planner.plan(&request(&bucket, 0)) {
            check_plan(planner.topology(), &plan, &[]);
        }

        // Eleven nodes, at least two in each of three racks.
        let mut sizes = vec![2, 2, 2];
        for (i, pick) in three_racks.iter().enumerate().take(5) {
            sizes[(pick + i) % 3] += 1;
        }
        sizes[0] += 11 - sizes.iter().sum::<usize>();
        prop_assert_eq!(sizes.iter().sum::<usize>(), 11);
        let mut planner = planner_for(FailureDomain::Rack, racks(&sizes));
        let plan = planner.plan(&request(&bucket, 0)).unwrap();
        prop_assert_eq!(plan.geometry, Geometry::RS_4_2);
        check_plan(planner.topology(), &plan, &[]);
    }

    /// Whatever a stripe keeps, the fragments placed around it keep both
    /// rules, never land on a kept or avoided node, and fail only when the
    /// caps leave too few nodes.
    #[test]
    fn replaced_stripes_keep_both_rules(
        sizes in proptest::collection::vec(1..5_usize, 3..7),
        lost in proptest::collection::btree_set(0..6_usize, 1..=2),
        avoided in 0..3_usize,
    ) {
        let mut planner = planner_for(FailureDomain::Rack, racks(&sizes));
        let bucket = bucket();
        let Ok(plan) = planner.plan(&request(&bucket, 0)) else {
            return Ok(());
        };
        let total = plan.geometry.total_fragments();
        let lost: Vec<usize> = lost.into_iter().filter(|i| *i < total).collect();
        prop_assume!(!lost.is_empty());
        let keep: Vec<NodeId> = (0..total)
            .filter(|i| !lost.contains(i))
            .map(|i| plan.nodes[i].clone())
            .collect();
        let mut avoid: Vec<NodeId> = lost.iter().map(|&i| plan.nodes[i].clone()).collect();
        let spare: Vec<NodeId> = planner
            .topology()
            .candidates()
            .map(|c| c.node.clone())
            .filter(|n| !plan.nodes.contains(n))
            .collect();
        avoid.extend(spare.iter().take(avoided).cloned());
        let topology = planner.topology().clone();
        match planner.replace(&replacing(&bucket, &plan, &keep, lost.len(), &avoid)) {
            Some(placed) => {
                prop_assert_eq!(placed.len(), lost.len());
                let mut nodes = plan.nodes.clone();
                for (&i, node) in lost.iter().zip(&placed) {
                    prop_assert!(!keep.contains(node));
                    nodes[i] = node.clone();
                }
                let replaced = StripePlan { geometry: plan.geometry, nodes };
                check_plan(&topology, &replaced, &[]);
                prop_assert!(placed.iter().all(|n| !avoid.contains(n)));
            }
            None => {
                // No eligible node outside the kept and avoided ones has
                // room under the per-domain cap.
                let m = plan.geometry.parity_fragments();
                let mut used: BTreeMap<Domain, usize> = BTreeMap::new();
                for node in &keep {
                    *used.entry(topology.domain(node).unwrap()).or_default() += 1;
                }
                let mut room = 0;
                let mut free: BTreeMap<Domain, usize> = BTreeMap::new();
                for candidate in topology.eligible() {
                    if keep.contains(&candidate.node) || avoid.contains(&candidate.node) {
                        continue;
                    }
                    *free.entry(candidate.domain(topology.level()).unwrap()).or_default() += 1;
                }
                for (domain, nodes) in free {
                    room += nodes.min(m - used.get(&domain).copied().unwrap_or(0).min(m));
                }
                prop_assert!(room < lost.len(), "room {} for {}", room, lost.len());
            }
        }
    }
}
