//! Planned handoff (plan M2-13, design §5.4, §16.1): primaries hand their
//! shards off to members now and then, while clients read and write
//! through gateways whose shard maps start stale and go stale again with
//! every handoff.
//!
//! A primary that steps down stops serving and renewing its leases, and
//! the member it steps down to proposes itself without waiting for its
//! grace, so the lease audit also checks every read against the step-downs
//! members received: no read is served once a member could have taken
//! over (`check_reads`). The commit audit checks one committing primary per
//! epoch, and the routing audit that only the current primary serves
//! (`check_served`). Every history is checked for linearizability per key,
//! which a stale read would break.

use std::time::Duration;

use skys3_cluster_sim::{
    Cluster, ClusterConfig, Drift, Fault, FaultPlan, FaultProfile, ReplicatedServices,
    RoutedServices, RunError, View, Workload,
};
use skys3_gateway::routing::RoutingConfig;
use skys3_shard::replication::ReplicationConfig;
use skys3_sim::history::Outcome;
use skys3_sim::{Runner, SimContext};

use crate::COST;

/// Four nodes, every shard on all of them, so each shard can be handed
/// off three times, with a placement in epoch 3 that gateways know only
/// from epoch 2.
fn config() -> ClusterConfig {
    ClusterConfig {
        nodes: 4,
        replicas: 4,
        placement_epoch: 3,
        every_member_durable: true,
        ..ClusterConfig::default()
    }
}

/// The takeover scenarios' timings (plan M2-12): a primary suspects a
/// member before the member's grace passes, within the lease inequality
/// for `ρ` = 1%.
fn fast() -> ReplicationConfig {
    ReplicationConfig {
        lease_renew_interval: Duration::from_millis(200),
        primary_lease: Duration::from_millis(800),
        primary_grace: Duration::from_millis(1400),
        member_suspect_after: Duration::from_millis(700),
        ..ReplicationConfig::default()
    }
}

/// Read-heavy clients that send each request to any node, with few keys,
/// so reads of a key land on both sides of each handoff.
fn workload(context: &SimContext, operations: usize) -> Workload {
    Workload {
        clients: 6,
        operations: operations * context.scale() as usize,
        keys: 4,
        think_time: Duration::from_millis(60),
        timeout: Duration::from_secs(6),
        any_gateway: true,
        ..Workload::default()
    }
}

/// Primaries that hand a shard off every `every` for the first `until` of
/// the run, behind gateways that start from stale maps.
fn services(replicated: ReplicatedServices, every: Duration, until: Duration) -> RoutedServices {
    let routing = RoutingConfig {
        connect_timeout: Duration::from_millis(300),
        request_timeout: Duration::from_secs(4),
        register_interval: Duration::from_millis(200),
        ..RoutingConfig::default()
    };
    RoutedServices::new(replicated.with_handoffs(every, until), routing)
}

/// A cluster checked against the three invariants of §6.8 after every
/// step.
fn cluster(config: ClusterConfig, services: &RoutedServices) -> Cluster<RoutedServices> {
    Cluster::with_services(config, services.clone()).invariant(|view: &View<'_, RoutedServices>| {
        let replicated = view.services.replicated();
        replicated.check_commits()?;
        replicated.check_reads()?;
        view.services.check_served()
    })
}

/// The scenario of §16.1: handoffs race reads that gateways with stale
/// maps send to old primaries, and no read is stale.
#[test]
fn handoffs_racing_reads_through_stale_gateways() {
    Runner::with_cost(3, COST).run(|context| {
        let every = Duration::from_millis(400 + 50 * (context.seed() % 6));
        let services = services(
            ReplicatedServices::new(fast()),
            every,
            Duration::from_secs(12),
        );
        let report = cluster(config(), &services).run(
            context,
            &workload(context, 60),
            &FaultPlan::none(),
        )?;
        let handoffs = services.replicated().handoffs();
        eprintln!("{handoffs:?}");
        assert!(handoffs.sent > 0, "{handoffs:?}");
        assert!(handoffs.received > 0, "{handoffs:?}");
        let leases = services.replicated().leases();
        assert!(leases.served > 0, "{leases:?}");
        assert_eq!(leases.stale, 0);
        let (_, stats) = services.stats();
        assert!(stats.redirects > 0, "{stats:?}");
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        Ok(())
    });
}

/// Handoffs under message loss, partitions, crashes, and clock drift
/// within `ρ`: step-downs are lost or delayed, so candidates fall back to
/// their grace, and old primaries restart after they stepped down.
#[test]
fn handoffs_under_random_faults_and_drift() {
    Runner::with_cost(4, COST / 2).run(|context| {
        let config = ClusterConfig {
            drift: Drift::from_ppm(10_000).unwrap(),
            ..config()
        };
        let workload = workload(context, 50);
        let profile = FaultProfile {
            end: Duration::from_secs(8) * context.scale(),
            crashes: 2,
            partitions: 2,
            message_loss: 2,
            max_duration: Duration::from_secs(3),
            ..FaultProfile::default()
        };
        let plan = FaultPlan::random(
            context.rng(),
            &profile,
            config.nodes,
            config.disks_per_node,
            workload.clients,
        );
        let services = services(
            ReplicatedServices::new(fast()),
            Duration::from_millis(600),
            Duration::from_secs(10),
        );
        let report = cluster(config, &services).run(context, &workload, &plan)?;
        let handoffs = services.replicated().handoffs();
        eprintln!("{handoffs:?}");
        assert!(handoffs.begun > 0);
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        Ok(())
    });
}

/// Message loss through every handoff: some step-downs never arrive, and
/// their shards are taken over once the members' grace passes.
#[test]
fn lost_step_downs_fall_back_to_the_grace() {
    Runner::with_cost(2, COST).run(|context| {
        let loss = Fault::MessageLoss {
            rate: 0.05,
            duration: Duration::from_secs(10),
        };
        let plan = FaultPlan::none().with(Duration::ZERO, loss);
        let services = services(
            ReplicatedServices::new(fast()),
            Duration::from_millis(500),
            Duration::from_secs(10),
        );
        let report = cluster(config(), &services).run(context, &workload(context, 50), &plan)?;
        let handoffs = services.replicated().handoffs();
        eprintln!("{handoffs:?}");
        assert!(handoffs.begun > 0);
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        Ok(())
    });
}

/// The seeded bug: a primary that stepped down still answers reads. The
/// lease audit catches the first one served after the member it stepped
/// down to received the step-down.
#[test]
fn the_audit_catches_a_stepped_down_primary_that_serves_reads() {
    Runner::with_cost(1, COST).run(|context| {
        let services = services(
            ReplicatedServices::new(fast()).stepped_down_serving_reads(),
            Duration::from_millis(400),
            Duration::from_secs(12),
        );
        let outcome =
            cluster(config(), &services).run(context, &workload(context, 60), &FaultPlan::none());
        match outcome {
            Err(RunError::Simulation(error)) => {
                assert!(error.contains("stepped down to"), "{error}");
                Ok(())
            }
            other => Err(
                format!("a stepped-down primary serving reads went unnoticed: {other:?}").into(),
            ),
        }
    });
}
