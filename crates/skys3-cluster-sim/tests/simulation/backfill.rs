//! Backfill and re-admission (plan M2-15, design §6.4, §6.7, §16.3): a
//! node is lost, its shards' primaries (or the members that take over
//! from it) remove it, and a test driver adds a learner to each shard left
//! with fewer than `replicas` members: a spare node outside the shard if
//! there is one, and otherwise the lost node itself once it returns, with
//! whatever its log still holds. The learner gets a snapshot of the
//! primary's index or keeps a log the primary verifies, joins the
//! acknowledgement set, backfills its payload, and is promoted.
//!
//! After every step the commit audit and the R3 audit run, and the
//! durability audit samples how long each shard had fewer than `replicas`
//! copies: until new writes had them again, and until all data did. Both
//! times are reported (§16.3). At the end, every acknowledged write must
//! be durable on every member of the final configurations.

use std::time::Duration;

use skys3_cluster_sim::{
    Cluster, ClusterConfig, DurabilityWindows, Fault, FaultPlan, FaultProfile,
    ReplicatedServices, RoutedServices, View, Workload,
};
use skys3_gateway::routing::RoutingConfig;
use skys3_shard::replication::ReplicationConfig;
use skys3_sim::history::Outcome;
use skys3_sim::{Runner, SimContext};

use crate::COST;

/// `nodes` nodes, every shard on three of them.
fn config(nodes: usize) -> ClusterConfig {
    ClusterConfig {
        nodes,
        replicas: 3,
        every_member_durable: true,
        ..ClusterConfig::default()
    }
}

/// Clients that keep writing for well over ten seconds, to any node, and
/// wait out a takeover.
fn workload(context: &SimContext) -> Workload {
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

/// Services that replace lost members, whose members take over from a
/// lost primary too, behind gateways that route to the current primary.
fn services() -> RoutedServices {
    let replicated = ReplicatedServices::new(ReplicationConfig::default())
        .with_takeover()
        .replacing_lost_members(Duration::ZERO);
    let routing = RoutingConfig {
        connect_timeout: Duration::from_millis(300),
        request_timeout: Duration::from_secs(4),
        register_interval: Duration::from_millis(200),
        ..RoutingConfig::default()
    };
    RoutedServices::new(replicated, routing)
}

/// A cluster checked against the commit rule and R3, and sampled for its
/// durability windows, after every step.
fn cluster(nodes: usize, services: &RoutedServices) -> Cluster<RoutedServices> {
    Cluster::with_services(config(nodes), services.clone()).invariant(
        |view: &View<'_, RoutedServices>| {
            let replicated = view.services.replicated();
            replicated.check_commits()?;
            replicated.check_members_hold_commits()?;
            replicated.sample_durability(view.elapsed)
        },
    )
}

/// Checks that every shard that lost a member regained `replicas` copies,
/// new writes first, and reports how long each took.
fn check_windows(windows: &DurabilityWindows) {
    eprintln!(
        "durability windows: {} opened; new writes regained replicas copies after at most \
         {:?}, all data after at most {:?} ({windows:?})",
        windows.opened,
        DurabilityWindows::longest(&windows.new_writes),
        DurabilityWindows::longest(&windows.all_data),
    );
    assert!(windows.opened > 0, "{windows:?}");
    assert_eq!(windows.new_writes.len(), windows.opened, "{windows:?}");
    for (new_writes, all_data) in windows.new_writes.iter().zip(&windows.all_data) {
        assert!(new_writes <= all_data, "{windows:?}");
    }
}

/// A node is lost for good while writes go on. Each shard it held gets
/// the spare node as a learner: new writes have three copies again within
/// seconds of the removal, and all data once the learner's backfill ends.
#[test]
fn a_lost_member_is_replaced_and_its_shards_regain_their_copies() {
    Runner::with_cost(3, COST).run(|context| {
        let crash = Fault::Crash {
            node: 0,
            power_loss: false,
            downtime: Duration::from_secs(60),
        };
        let plan = FaultPlan::none().with(Duration::from_secs(2), crash);
        let services = services();
        let report = cluster(4, &services).run(context, &workload(context), &plan)?;
        let windows = services.replicated().durability_windows();
        check_windows(&windows);
        // The learner joins the acknowledgement set at its first session.
        let longest = DurabilityWindows::longest(&windows.new_writes);
        assert!(longest < Duration::from_secs(5), "{windows:?}");
        assert!(services.replicated().learners().promoted >= windows.opened);
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        Ok(())
    });
}

/// In a cluster of three, a node loses power and stays down for a while:
/// its shards' primaries remove it, and the driver adds it back as a
/// learner, the only node left. It returns with its old log, which seeds
/// its catch-up if the primary verifies it, and is discarded for a
/// snapshot otherwise.
#[test]
fn a_removed_node_is_readmitted_as_a_learner() {
    Runner::with_cost(3, COST).run(|context| {
        let crash = Fault::Crash {
            node: 2,
            power_loss: true,
            downtime: Duration::from_secs(6),
        };
        let plan = FaultPlan::none().with(Duration::from_secs(2), crash);
        let services = services();
        let report = cluster(3, &services).run(context, &workload(context), &plan)?;
        check_windows(&services.replicated().durability_windows());
        assert!(services.replicated().learners().promoted > 0);
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        Ok(())
    });
}

/// Lost members replaced under crashes, power loss, partitions, message
/// loss, and control-store faults: learners are added, dropped, given
/// snapshots again, and promoted, and R3 holds throughout.
#[test]
fn lost_members_are_replaced_under_random_faults() {
    Runner::with_cost(4, COST / 2).run(|context| {
        let workload = Workload {
            operations: 200 * context.scale() as usize,
            ..workload(context)
        };
        let profile = FaultProfile {
            end: Duration::from_secs(10) * context.scale(),
            crashes: 3,
            partitions: 2,
            message_loss: 1,
            control: 1,
            max_duration: Duration::from_secs(5),
            ..FaultProfile::default()
        };
        let config = config(4);
        let plan = FaultPlan::random(
            context.rng(),
            &profile,
            config.nodes,
            config.disks_per_node,
            workload.clients,
        );
        let services = services();
        let report = cluster(4, &services).run(context, &workload, &plan)?;
        eprintln!("{:?} {:?}", services.replicated().learners(), services.replicated().durability_windows());
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        Ok(())
    });
}
