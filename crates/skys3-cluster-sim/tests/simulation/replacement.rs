//! Replacement (plan M3-05, design §6.4, §6.7): a node is lost, the
//! primaries of its shards (or the members that take over from it) remove
//! it, and the coordinator, with no driver and no operator, adds a learner
//! to every shard left with fewer than `replicas` members, on a node
//! placement chooses. The primaries backfill and promote the learners, and
//! every affected shard returns to `replicas` members.
//!
//! After every step the commit audit and the R3 audit run, and the
//! durability audit samples how long each shard had fewer than `replicas`
//! copies: until new writes had them again, until all data did, and until
//! the shard had `replicas` members again. The times are reported (§16.3).

use std::time::Duration;

use skys3_cluster_sim::{
    Cluster, ClusterConfig, CoordinatedServices, CoordinationConfig, DurabilityWindows, Fault,
    FaultPlan, ReplicatedServices, RoutedServices, View, Workload,
};
use skys3_coord::{RegistryConfig, ReplacementConfig};
use skys3_gateway::routing::RoutingConfig;
use skys3_shard::replication::ReplicationConfig;
use skys3_sim::history::Outcome;
use skys3_sim::{Runner, SimContext};
use skys3_types::NodeId;

use crate::COST;

/// What one seed of these scenarios costs, in typical seeds: a whole
/// cluster under the coordinator, writing for about 19 s of simulated time
/// with a node lost, costs about twenty typical seeds' run time, so CI's
/// fixed seed set runs one seed of each, and larger seed sets more.
const REPLACEMENT_COST: u64 = 8 * COST;

/// Replicated, routed services under a coordinator that replaces lost
/// members.
pub(crate) type Services = CoordinatedServices<RoutedServices>;

/// `nodes` nodes, every shard on three of them.
pub(crate) fn config(nodes: usize) -> ClusterConfig {
    ClusterConfig {
        nodes,
        replicas: 3,
        every_member_durable: true,
        ..ClusterConfig::default()
    }
}

/// Clients that keep writing for well over ten seconds, to any node, and
/// wait out a takeover.
pub(crate) fn workload(context: &SimContext) -> Workload {
    Workload {
        clients: 4,
        operations: 400 * context.scale() as usize,
        keys: 8,
        think_time: Duration::from_millis(120),
        timeout: Duration::from_secs(8),
        any_gateway: true,
        ..Workload::default()
    }
}

/// [`workload`], for long enough that a lost primary is taken over from,
/// about 7 s after the loss, and its shard replaced, and the lost node
/// forgotten, before the clients stop: they write for about 19 s, and the
/// last shard is back to three members by about 12.5 s.
fn long_workload(context: &SimContext) -> Workload {
    Workload {
        operations: 500 * context.scale() as usize,
        ..workload(context)
    }
}

/// Services whose members take over from a lost primary, behind gateways
/// that route to the current primary, whose nodes follow the shard
/// registers, and whose coordinator replaces lost members, judging node
/// health by `registry`.
pub(crate) fn services_with(registry: RegistryConfig) -> Services {
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
        registry,
        ..CoordinationConfig::default()
    };
    CoordinatedServices::new(RoutedServices::new(replicated, routing), coordination)
}

/// [`services_with`] the default registry of the simulation: nodes are
/// suspect after 600 ms of silence and never forgotten.
pub(crate) fn services() -> Services {
    services_with(CoordinationConfig::default().registry)
}

/// A cluster checked against the commit rule and R3, and sampled for its
/// durability windows, after every step.
pub(crate) fn cluster(config: ClusterConfig, services: &Services) -> Cluster<Services> {
    Cluster::with_services(config, services.clone()).invariant(|view: &View<'_, Services>| {
        let replicated = view.services.inner().replicated();
        replicated.check_commits()?;
        replicated.check_members_hold_commits()?;
        replicated.sample_durability(view.elapsed)
    })
}

/// Checks that every shard that lost a member regained `replicas` copies,
/// new writes first, and `replicas` members, and reports how long each
/// took.
pub(crate) fn check_windows(windows: &DurabilityWindows) {
    eprintln!(
        "durability windows: {} opened; new writes regained replicas copies after at most \
         {:?}, all data after at most {:?}, and the shards replicas members after at most \
         {:?} ({windows:?})",
        windows.opened,
        DurabilityWindows::longest(&windows.new_writes),
        DurabilityWindows::longest(&windows.all_data),
        DurabilityWindows::longest(&windows.restored),
    );
    assert!(windows.opened > 0, "{windows:?}");
    assert_eq!(windows.restored.len(), windows.opened, "{windows:?}");
    for ((new_writes, all_data), restored) in windows
        .new_writes
        .iter()
        .zip(&windows.all_data)
        .zip(&windows.restored)
    {
        assert!(new_writes <= all_data, "{windows:?}");
        assert!(all_data <= restored, "{windows:?}");
    }
}

/// The node at `position`.
fn node(position: usize) -> NodeId {
    NodeId::new(format!("node-{}", position + 1)).unwrap()
}

/// Five nodes and two buckets, one `write_back`: the first node, usually
/// the first coordinator too, is lost for good while writes go on. Every
/// shard it held returns to three members, on the nodes the coordinator
/// chose, with no driver and no operator.
#[test]
fn after_a_node_loss_every_affected_shard_returns_to_replicas_members() {
    Runner::with_cost(2, REPLACEMENT_COST).run(|context| {
        let crash = Fault::Crash {
            node: 0,
            power_loss: false,
            downtime: Duration::from_secs(120),
        };
        let plan = FaultPlan::none().with(Duration::from_secs(2), crash);
        let config = ClusterConfig {
            write_back_buckets: 1,
            ..config(5)
        };
        let services = services();
        let report = cluster(config, &services).run(context, &long_workload(context), &plan)?;
        let windows = services.inner().replicated().durability_windows();
        check_windows(&windows);
        // The learner joins the acknowledgement set at its first session.
        assert!(DurabilityWindows::longest(&windows.new_writes) < Duration::from_secs(5));
        assert!(services.inner().replicated().learners().promoted >= windows.opened);
        assert!(services.shard_writes() >= windows.opened);
        eprintln!(
            "{} shard registers written by coordinators {:?}",
            services.shard_writes(),
            services.coordinators()
        );
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        Ok(())
    });
}

/// A node stays silent past `node_forget_after`: its shards' primaries
/// remove it, the coordinator replaces it, marks it departing, and forgets
/// it once no shard names it, with no operator action.
#[test]
fn a_lost_node_is_replaced_then_forgotten() {
    Runner::with_cost(2, REPLACEMENT_COST).run(|context| {
        let crash = Fault::Crash {
            node: 1,
            power_loss: true,
            downtime: Duration::from_secs(120),
        };
        let plan = FaultPlan::none().with(Duration::from_secs(2), crash);
        let services = services_with(RegistryConfig {
            suspect_after: Duration::from_millis(600),
            forget_after: Duration::from_secs(5),
        });
        let report = cluster(config(4), &services).run(context, &long_workload(context), &plan)?;
        check_windows(&services.inner().replicated().durability_windows());
        let forgotten = services.forgotten();
        eprintln!("forgotten: {forgotten:?}");
        assert!(forgotten.iter().any(|f| f.node == node(1)), "{forgotten:?}");
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        Ok(())
    });
}
