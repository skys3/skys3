use std::collections::{BTreeMap, BTreeSet};

use proptest::prelude::*;
use skys3_config::FailureDomain;
use skys3_types::{CodecId, FragmentId, Label};

use super::super::tests::{
    MIB, TB, arb_level, arb_state, bucket, candidate, defaults, flat, node, racks,
};
use super::*;
use crate::{GeometryPolicy, StripeRequest};

/// `count` stripes of 3 MiB each, planned over `nodes` at `level`, with
/// distinct fragment IDs.
fn stripes(level: FailureDomain, nodes: Vec<Candidate>, count: u32) -> Vec<(String, CodedStripe)> {
    let mut planner = FragmentPlanner::new(Topology::new(level, nodes), defaults());
    let bucket = bucket();
    let mut next = 0_u128;
    (0..count)
        .map(|n| {
            let key = format!("key-{}", n / 3);
            let request = StripeRequest::new(&bucket, ShardId::new(0), &key, n % 3, 3 * MIB);
            let plan = planner.plan(&request).unwrap();
            let ids: Vec<FragmentId> = plan
                .nodes
                .iter()
                .map(|_| {
                    next += 1;
                    FragmentId::new(next)
                })
                .collect();
            let stripe = plan
                .locate(
                    n % 3,
                    u64::from(n % 3) * 3 * MIB,
                    3 * MIB,
                    CodecId::CURRENT,
                    &ids,
                )
                .unwrap();
            (key, stripe)
        })
        .collect()
}

/// The moves `planner` plans for `stripes`, avoiding `avoid`, at most
/// `limit`.
fn plan(
    planner: &FragmentPlanner,
    stripes: &[(String, CodedStripe)],
    avoid: &[NodeId],
    limit: usize,
) -> Vec<PlannedMove> {
    let bucket = bucket();
    let shard_stripes: Vec<ShardStripe<'_>> = stripes
        .iter()
        .map(|(key, stripe)| ShardStripe { key, stripe })
        .collect();
    planner.moves(&MoveRequest {
        bucket: &bucket,
        shard: ShardId::new(0),
        stripes: &shard_stripes,
        avoid,
        limit,
    })
}

/// Applies `moves` to `stripes`, giving each moved fragment a new ID.
fn apply(stripes: &mut [(String, CodedStripe)], moves: &[PlannedMove]) {
    for (n, moved) in moves.iter().enumerate() {
        let (_, stripe) = stripes
            .iter_mut()
            .find(|(key, stripe)| *key == moved.key && stripe.number() == moved.stripe)
            .unwrap();
        let mut fragments = stripe.fragments().to_vec();
        assert_eq!(fragments[usize::from(moved.index)], moved.from);
        fragments[usize::from(moved.index)] = FragmentLocation {
            node: moved.to.clone(),
            fragment: FragmentId::new(1_000_000 + n as u128),
        };
        *stripe = CodedStripe::new(
            stripe.number(),
            stripe.offset(),
            stripe.data_len(),
            stripe.geometry(),
            stripe.codec(),
            fragments,
        )
        .expect("a move keeps the stripe's fragments on distinct nodes");
    }
}

/// How many fragments of `stripes` each node holds.
fn counts(stripes: &[(String, CodedStripe)]) -> BTreeMap<NodeId, usize> {
    let mut counts = BTreeMap::new();
    for (_, stripe) in stripes {
        for location in stripe.fragments() {
            *counts.entry(location.node.clone()).or_default() += 1;
        }
    }
    counts
}

/// Plans and applies moves until none is left, at most `rounds` times, and
/// returns every move.
fn settle(
    planner: &FragmentPlanner,
    stripes: &mut [(String, CodedStripe)],
    avoid: &[NodeId],
    limit: usize,
    rounds: usize,
) -> Vec<PlannedMove> {
    let mut all = Vec::new();
    for _ in 0..rounds {
        let moves = plan(planner, stripes, avoid, limit);
        if moves.is_empty() {
            return all;
        }
        assert!(moves.len() <= limit);
        apply(stripes, &moves);
        all.extend(moves);
    }
    panic!("moves did not settle in {rounds} rounds");
}

#[test]
fn joined_nodes_receive_their_share() {
    // Twelve 3+2 stripes on six nodes: ten fragments each.
    let mut stripes = stripes(FailureDomain::Node, flat(6), 12);
    assert!(counts(&stripes).values().all(|&n| n == 10));
    let planner = FragmentPlanner::new(Topology::new(FailureDomain::Node, flat(8)), defaults());
    // A batch of four, then the rest.
    let first = plan(&planner, &stripes, &[], 4);
    assert_eq!(first.len(), 4);
    let moves = settle(&planner, &mut stripes, &[], 4, 10);
    assert!(moves.len() >= 14, "{}", moves.len());
    assert!(moves.iter().all(|m| m.reason == MoveReason::Balance));
    assert!(moves.iter().all(|m| m.to == node(6) || m.to == node(7)));
    // Sixty fragments on eight nodes: seven or eight each.
    let counts = counts(&stripes);
    assert_eq!(counts.len(), 8);
    assert!(counts.values().all(|&n| (7..=8).contains(&n)), "{counts:?}");
}

#[test]
fn balanced_nodes_move_nothing() {
    let stripes = stripes(FailureDomain::Node, flat(7), 7);
    let planner = FragmentPlanner::new(Topology::new(FailureDomain::Node, flat(7)), defaults());
    // Within one fragment of each other, nothing moves.
    let counts = counts(&stripes);
    let (min, max) = (counts.values().min(), counts.values().max());
    assert!(max.unwrap() - min.unwrap() <= 1, "{counts:?}");
    assert!(plan(&planner, &stripes, &[], 16).is_empty());
    // Nor does an empty request, or a topology that lists no node.
    assert!(plan(&planner, &[], &[], 16).is_empty());
    let empty = FragmentPlanner::new(Topology::new(FailureDomain::Node, Vec::new()), defaults());
    assert!(plan(&empty, &stripes, &[], 16).is_empty());
}

#[test]
fn a_departing_node_is_drained_while_others_are_avoided() {
    // 4+2 stripes on seven nodes, and an eighth that is suspect.
    let mut stripes = stripes(FailureDomain::Node, flat(7), 6);
    let held = counts(&stripes)[&node(2)];
    assert!(held > 0);
    let mut nodes = flat(8);
    nodes[2].state = NodeState::Departing;
    // A suspect node receives nothing.
    nodes[7].state = NodeState::Suspect;
    let planner = FragmentPlanner::new(Topology::new(FailureDomain::Node, nodes), defaults());

    // Avoided, as when it is silent: its fragments may be lost, which is
    // repair's to find.
    let avoid = [node(2)];
    assert!(
        plan(&planner, &stripes, &avoid, 16)
            .iter()
            .all(|m| m.reason == MoveReason::Balance && m.from.node != node(2))
    );

    let moves = plan(&planner, &stripes, &[], 2 * held);
    let drains: Vec<&PlannedMove> = moves
        .iter()
        .filter(|m| m.reason == MoveReason::Drain)
        .collect();
    assert_eq!(drains.len(), held);
    assert!(drains.iter().all(|m| m.from.node == node(2)));
    // Drains come before balancing.
    assert!(moves[..held].iter().all(|m| m.reason == MoveReason::Drain));
    assert!(moves.iter().all(|m| m.to != node(2) && m.to != node(7)));
    settle(&planner, &mut stripes, &[], 16, 10);
    assert!(!counts(&stripes).contains_key(&node(2)));
    assert!(!counts(&stripes).contains_key(&node(7)));
}

#[test]
fn a_drain_without_room_waits_and_unlisted_nodes_are_repairs() {
    // Five nodes hold 3+2 stripes with no spare: a departing node has
    // nowhere to send its fragments.
    let stripes = stripes(FailureDomain::Node, flat(5), 3);
    let mut nodes = flat(5);
    nodes[0].state = NodeState::Departing;
    let planner = FragmentPlanner::new(Topology::new(FailureDomain::Node, nodes), defaults());
    assert!(plan(&planner, &stripes, &[], 16).is_empty());

    // A node the topology no longer lists lost its fragments: nothing
    // moves from it, or to it.
    let planner = FragmentPlanner::new(Topology::new(FailureDomain::Node, flat(4)), defaults());
    let moves = plan(&planner, &stripes, &[], 16);
    assert!(
        moves
            .iter()
            .all(|m| m.from.node != node(4) && m.to != node(4))
    );
}

#[test]
fn a_stripe_over_a_domain_s_cap_is_spread_again() {
    // Three racks of three: 3+2 stripes, at most two fragments per rack.
    let mut stripes = stripes(FailureDomain::Rack, racks(&[3, 3, 3]), 6);
    // A node of rack 1 that holds a fragment is relabeled into rack 0,
    // so some stripe now holds three there.
    let mut nodes = racks(&[3, 3, 3]);
    let topology = Topology::new(FailureDomain::Rack, nodes.clone());
    let rack_of = |n: &NodeId| topology.domain(n).unwrap();
    let over = stripes
        .iter()
        .find_map(|(_, stripe)| {
            let in_rack = |rack: usize| {
                stripe
                    .fragments()
                    .iter()
                    .filter(|l| rack_of(&l.node) == rack_of(&node(rack * 3)))
                    .map(|l| l.node.clone())
                    .collect::<Vec<_>>()
            };
            (in_rack(0).len() == 2)
                .then(|| in_rack(1).first().cloned())
                .flatten()
        })
        .expect("a stripe with two fragments in rack 0 and one in rack 1");
    let moved = nodes.iter().position(|c| c.node == over).unwrap();
    nodes[moved].rack = Some(Label::new("rack-0").unwrap());
    let planner = FragmentPlanner::new(Topology::new(FailureDomain::Rack, nodes), defaults());
    let moves = plan(&planner, &stripes, &[], 1);
    assert_eq!(moves.len(), 1);
    assert_eq!(moves[0].reason, MoveReason::Domain);
    let topology = planner.topology().clone();
    settle(&planner, &mut stripes, &[], 16, 10);
    for (_, stripe) in &stripes {
        let mut per_rack: BTreeMap<Domain, usize> = BTreeMap::new();
        for location in stripe.fragments() {
            *per_rack
                .entry(topology.domain(&location.node).unwrap())
                .or_default() += 1;
        }
        assert!(per_rack.values().all(|&n| n <= 2), "{per_rack:?}");
    }
}

#[test]
fn shares_follow_capacity() {
    // Twelve 3+2 stripes on six nodes, and a seventh joins with twice the
    // capacity of the others: its share is fifteen of the sixty fragments,
    // and it can hold one of each stripe.
    let mut stripes = stripes(FailureDomain::Node, flat(6), 12);
    let mut nodes = flat(7);
    nodes[6].capacity_bytes = 2 * TB;
    let planner = FragmentPlanner::new(Topology::new(FailureDomain::Node, nodes), defaults());
    let moves = settle(&planner, &mut stripes, &[], 8, 20);
    assert!(moves.iter().all(|m| m.to == node(6)));
    let counts = counts(&stripes);
    assert_eq!(counts[&node(6)], 12, "{counts:?}");
    assert!(
        counts.values().filter(|&&c| c != 12).all(|&c| c == 8),
        "{counts:?}"
    );
}

/// A cluster to move to: the nodes the stripes were planned over, with
/// some changed and some added.
#[derive(Debug, Clone)]
struct Change {
    level: FailureDomain,
    before: Vec<Candidate>,
    after: Vec<Candidate>,
    avoid: Vec<NodeId>,
}

fn arb_change() -> impl Strategy<Value = Change> {
    let labels = (0..3_usize, 0..5_usize);
    (
        arb_level(),
        proptest::collection::vec(labels.clone(), 5..12),
        proptest::collection::vec((arb_state(), labels, 1..=4_u64, any::<bool>()), 12),
        proptest::collection::vec(0..14_usize, 0..2),
        0..3_usize,
    )
        .prop_map(|(level, before, changes, avoid, joined)| {
            let before: Vec<Candidate> = before
                .into_iter()
                .enumerate()
                .map(|(n, (zone, rack))| candidate(n, Some(zone), Some(rack)))
                .collect();
            let mut after = before.clone();
            for n in 0..joined {
                after.push(candidate(before.len() + n, Some(n % 3), Some(n)));
            }
            for (candidate, (state, (zone, rack), capacity, relabel)) in
                after.iter_mut().zip(changes)
            {
                candidate.state = state;
                candidate.capacity_bytes = capacity * TB;
                if relabel {
                    candidate.zone = Some(Label::new(format!("zone-{zone}")).unwrap());
                    candidate.rack = Some(Label::new(format!("rack-{rack}")).unwrap());
                }
            }
            Change {
                level,
                before,
                after,
                avoid: avoid.into_iter().map(node).collect(),
            }
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// On any cluster that changed under its stripes: every planned move
    /// leaves an avoided or unlisted node alone, goes to a live eligible
    /// node outside its stripe, never moves a fragment twice, and keeps
    /// every domain within its cap unless it was over it before; and
    /// moves settle, with no fragment left on an ineligible node that has
    /// somewhere to go.
    #[test]
    fn moves_keep_the_rules_and_settle(change in arb_change(), limit in 1..8_usize) {
        let policy = GeometryPolicy::new(2, 4, 5).unwrap();
        let before = FragmentPlanner::new(Topology::new(change.level, change.before.clone()), policy);
        let Ok(_) = before.geometry() else {
            return Ok(());
        };
        let mut stripes = {
            let mut planner = before.clone();
            let bucket = bucket();
            let mut out = Vec::new();
            for n in 0..6_u32 {
                let key = format!("key-{n}");
                let request = StripeRequest::new(&bucket, ShardId::new(0), &key, 0, 1 + u64::from(n) * MIB);
                let plan = planner.plan(&request).unwrap();
                let ids: Vec<FragmentId> = (0..plan.nodes.len())
                    .map(|i| FragmentId::new(u128::from(n) * 100 + i as u128))
                    .collect();
                out.push((key, plan.locate(0, 0, 1 + u64::from(n) * MIB, CodecId::CURRENT, &ids).unwrap()));
            }
            out
        };
        let planner = FragmentPlanner::new(Topology::new(change.level, change.after.clone()), policy);
        let topology = planner.topology().clone();
        let level = topology.level();
        let over = |stripe: &CodedStripe| {
            let mut per: BTreeMap<Domain, usize> = BTreeMap::new();
            for location in stripe.fragments() {
                if let Some(domain) = topology.domain(&location.node) {
                    *per.entry(domain).or_default() += 1;
                }
            }
            per.into_iter()
                .filter(|(_, n)| *n > stripe.geometry().parity_fragments())
                .map(|(d, _)| d)
                .collect::<BTreeSet<_>>()
        };
        for _ in 0..300 {
            let moves = plan(&planner, &stripes, &change.avoid, limit);
            if moves.is_empty() {
                break;
            }
            prop_assert!(moves.len() <= limit);
            let mut seen = BTreeSet::new();
            for moved in &moves {
                prop_assert!(seen.insert((moved.key.clone(), moved.stripe, moved.index)));
                prop_assert!(!change.avoid.contains(&moved.from.node));
                prop_assert!(!change.avoid.contains(&moved.to));
                prop_assert!(topology.get(&moved.from.node).is_some());
                let target = topology.get(&moved.to).unwrap();
                prop_assert!(target.is_eligible(level) && target.state == NodeState::Live);
            }
            let overs: Vec<BTreeSet<Domain>> = stripes.iter().map(|(_, s)| over(s)).collect();
            apply(&mut stripes, &moves);
            for ((_, stripe), was) in stripes.iter().zip(&overs) {
                prop_assert!(over(stripe).is_subset(was), "a move broke a domain's cap");
            }
        }
        prop_assert!(plan(&planner, &stripes, &change.avoid, usize::MAX).is_empty(), "moves settle");
    }
}
