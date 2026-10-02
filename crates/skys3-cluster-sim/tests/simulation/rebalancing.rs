//! Rebalancing (plan M3-06, design §6.7): nodes join a loaded cluster
//! while clients write, and the coordinator, with no operator, gives each
//! its share of shards and primaries. Shards move by add-learner, promote,
//! and remove steps; primaries move by planned handoffs the coordinator
//! asks the primaries for.
//!
//! After every step the commit audit, the R3 audit, the lease audit (no
//! read served once a member could have taken over, a step-down
//! included), and the routing audit (only the current primary serves) run.
//! Every client write is timed end to end: the only writes that wait are
//! those in flight while a handoff of their shard is under way, and they
//! wait no longer than the handoff. The numbers are reported (§16.3).

use std::collections::BTreeMap;
use std::time::Duration;

use skys3_cluster_sim::{
    Cluster, ClusterConfig, CoordinatedServices, CoordinationConfig, Fault, FaultPlan, HandoffTime,
    ReplicatedServices, RoutedServices, View, Workload, WriteTiming,
};
use skys3_coord::{RebalanceConfig, ReplacementConfig};
use skys3_gateway::ShardRef;
use skys3_gateway::routing::RoutingConfig;
use skys3_shard::replication::ReplicationConfig;
use skys3_sim::history::Outcome;
use skys3_sim::{Runner, SimContext};
use skys3_types::{NodeId, ShardConfig};

use crate::COST;

type Services = CoordinatedServices<RoutedServices>;

/// When the first new node joins, after the clients start; a second one
/// joins `JOIN_EVERY` later.
const JOIN_AT: Duration = Duration::from_secs(3);
const JOIN_EVERY: Duration = Duration::from_secs(8);

/// How long the moves may go on after the last join.
const MOVES: Duration = Duration::from_secs(15);

/// How long a write may take when no handoff of its shard is under way. A
/// stall would be a write waiting for a membership change, such as for
/// `member_suspect_after` (3 s) or `primary_grace` (6 s). Without any
/// move, writes here take up to about 1.5 s, when control-store faults
/// delay a gateway's register read.
const SLOW: Duration = Duration::from_secs(2);

/// How long after a handoff's new primary serves a gateway may still be
/// learning of it: its register poll, a redirect, and a retry.
const ROUTING_SLACK: Duration = Duration::from_millis(800);

/// Four loaded nodes and `joining` that join: two buckets, one
/// `write_back`, of four shards with three members each.
fn config(joining: usize) -> ClusterConfig {
    ClusterConfig {
        nodes: 4 + joining,
        joining,
        replicas: 3,
        write_back_buckets: 1,
        every_member_durable: true,
        ..ClusterConfig::default()
    }
}

/// Clients that write to any node for well past the moves.
fn workload(context: &SimContext) -> Workload {
    Workload {
        clients: 4,
        operations: 700 * context.scale() as usize,
        keys: 8,
        think_time: Duration::from_millis(120),
        timeout: Duration::from_secs(8),
        any_gateway: true,
        ..Workload::default()
    }
}

/// Replicated services behind routing gateways, whose coordinator replaces
/// lost members and rebalances.
fn services() -> Services {
    let replicated = ReplicatedServices::new(ReplicationConfig::default())
        .with_takeover()
        .following_registers();
    let routing = RoutingConfig {
        connect_timeout: Duration::from_millis(300),
        request_timeout: Duration::from_secs(4),
        register_interval: Duration::from_millis(200),
        ..RoutingConfig::default()
    };
    let coordination = CoordinationConfig {
        replacement: Some(ReplacementConfig::default()),
        rebalancing: Some(RebalanceConfig::default()),
        ..CoordinationConfig::default()
    };
    CoordinatedServices::new(RoutedServices::new(replicated, routing), coordination)
}

/// The audits of §6.8 and the routing audit after every step, and the
/// samples the measurements need.
fn cluster(config: ClusterConfig, services: &Services) -> Cluster<Services> {
    Cluster::with_services(config, services.clone()).invariant(|view: &View<'_, Services>| {
        let routed = view.services.inner();
        let replicated = routed.replicated();
        replicated.check_commits()?;
        replicated.check_members_hold_commits()?;
        replicated.check_reads()?;
        routed.check_served()?;
        replicated.sample_durability(view.elapsed)?;
        replicated.sample_serving(view.elapsed)
    })
}

/// Each node's members and primaries.
fn loads(registers: &BTreeMap<ShardRef, ShardConfig>) -> BTreeMap<NodeId, (usize, usize)> {
    let mut loads: BTreeMap<NodeId, (usize, usize)> = BTreeMap::new();
    for config in registers.values() {
        for member in &config.members {
            loads.entry(member.clone()).or_default().0 += 1;
        }
        loads.entry(config.primary.clone()).or_default().1 += 1;
    }
    loads
}

/// Whether `write` was in flight while a handoff of its shard was under
/// way, until gateways could have learned of the new primary.
fn meets_handoff(write: &WriteTiming, handoffs: &[HandoffTime]) -> bool {
    handoffs.iter().any(|handoff| {
        let end = handoff.began + handoff.took.unwrap_or(Duration::MAX / 4) + ROUTING_SLACK;
        handoff.shard == write.shard && write.sent < end && handoff.began < write.sent + write.took
    })
}

/// The slowest of `writes` and how many got no definite answer.
fn summary(writes: &[&WriteTiming]) -> (usize, Duration, usize) {
    let slowest = writes.iter().map(|w| w.took).max().unwrap_or_default();
    let unanswered = writes.iter().filter(|w| !w.answered).count();
    (writes.len(), slowest, unanswered)
}

/// Runs `joining` nodes joining four loaded ones, and checks that every
/// node ends with its share and that writes stall only for handoffs.
fn joins(context: &mut SimContext, joining: usize) -> Result<(), Box<dyn std::error::Error>> {
    let mut plan = FaultPlan::none();
    let mut at = JOIN_AT;
    for node in 4..4 + joining {
        plan.push(at, Fault::Join { node });
        at += JOIN_EVERY;
    }
    let last_join = at - JOIN_EVERY;
    let services = services();
    let report = cluster(config(joining), &services).run(context, &workload(context), &plan)?;
    let replicated = services.inner().replicated();

    // Every node's share: 24 members, and eight primaries, as even as they
    // can be.
    let registers = replicated.shard_registers();
    assert_eq!(registers.len(), 8);
    let loads = loads(&registers);
    eprintln!("members and primaries by node: {loads:?}");
    for config in registers.values() {
        assert_eq!(config.members.len(), 3, "{config:?}");
        assert!(config.learners.is_empty(), "{config:?}");
    }
    let nodes = 4 + joining;
    assert_eq!(loads.len(), nodes, "every node holds members: {loads:?}");
    let share = |total: usize| (total / nodes)..=total.div_ceil(nodes);
    for (node, (members, primaries)) in &loads {
        assert!(share(24).contains(members), "{node}: {loads:?}");
        assert!(share(8).contains(primaries), "{node}: {loads:?}");
    }

    // The moves: learners promoted, and handoffs asked for and taken.
    let learners = replicated.learners();
    let handoffs = replicated.handoff_times();
    let windows = replicated.durability_windows();
    eprintln!(
        "{learners:?}; {} handoffs: {handoffs:?}; durability windows: {windows:?}",
        handoffs.len()
    );
    assert!(learners.promoted >= 4 * joining, "{learners:?}");
    assert!(!handoffs.is_empty());
    let took: Vec<Duration> = handoffs.iter().filter_map(|h| h.took).collect();
    assert_eq!(
        took.len(),
        handoffs.len(),
        "a handoff never ended: {handoffs:?}"
    );
    let longest_handoff = took.iter().copied().max().unwrap_or_default();
    let mean_handoff = took.iter().sum::<Duration>() / u32::try_from(took.len())?;
    // No handoff fell back to the members' grace.
    assert!(longest_handoff < ReplicationConfig::default().primary_grace);

    // Write latency while the moves go on, from the first join until
    // `MOVES` after the last, and before and after them.
    let (from, until) = (report.started + JOIN_AT, report.started + last_join + MOVES);
    let (during, outside): (Vec<&WriteTiming>, Vec<&WriteTiming>) = report
        .writes
        .iter()
        .partition(|write| (from..until).contains(&write.sent));
    let (stalled, away): (Vec<&WriteTiming>, Vec<&WriteTiming>) = during
        .iter()
        .partition(|write| meets_handoff(write, &handoffs));
    eprintln!(
        "writes while shards moved, meeting no handoff (count, slowest, unanswered): {:?}; \
         meeting one: {:?}; before and after the moves: {:?}; handoffs took at most \
         {longest_handoff:?}, on average {mean_handoff:?}",
        summary(&away),
        summary(&stalled),
        summary(&outside),
    );
    assert!(!away.is_empty());
    for write in &away {
        assert!(
            write.took < SLOW,
            "a write met no handoff of its shard, but stalled: {write:?}"
        );
    }
    for write in &stalled {
        assert!(
            write.took < longest_handoff + ROUTING_SLACK + SLOW,
            "a write stalled longer than a handoff: {write:?}"
        );
    }
    assert!(report.count(|o| *o == Outcome::Done) > 0);
    Ok(())
}

/// A node joins four loaded nodes: it receives its share of shards and
/// primaries, and the only writes that stall are those that meet a
/// handoff of their shard, for no longer than the handoff takes.
#[test]
fn a_new_node_receives_its_share_of_shards_and_primaries() {
    Runner::with_cost(3, COST).run(|context| joins(context, 1));
}

/// Two nodes join, one after the other: the second batch of moves starts
/// once the first completed, and both nodes end with their share.
#[test]
fn nodes_that_join_one_after_another_each_receive_their_share() {
    Runner::with_cost(2, COST).run(|context| joins(context, 2));
}
