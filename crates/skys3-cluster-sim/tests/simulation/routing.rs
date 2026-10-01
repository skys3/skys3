//! Routing (plan M2-08): clients send each request to any node, whose
//! gateway starts from a stale shard map, is redirected by members, reads
//! the shard register when no member it knows answers, and is never
//! served by a replica other than the current primary.

use std::time::Duration;

use skys3_cluster_sim::{
    Cluster, ClusterConfig, Fault, FaultPlan, FaultProfile, FaultRates, ReplicatedServices,
    RoutedServices, RunError, View, Workload,
};
use skys3_gateway::routing::RoutingConfig;
use skys3_sim::Runner;
use skys3_sim::history::Outcome;

use crate::COST;

/// Four nodes, so each shard has a node outside its three members, with
/// a placement in epoch 3 that gateways know only from epoch 2, and a
/// `write_back` bucket that only primaries flush.
fn config() -> ClusterConfig {
    ClusterConfig {
        nodes: 4,
        replicas: 3,
        placement_epoch: 3,
        write_back_buckets: 1,
        every_member_durable: true,
        ..ClusterConfig::default()
    }
}

/// Clients that wait out a gateway's forwarding timeout.
fn workload() -> Workload {
    Workload {
        any_gateway: true,
        timeout: Duration::from_secs(6),
        ..Workload::default()
    }
}

/// Gateways that give up on a request within a client's timeout. A write
/// to a `write_back` shard may wait out a lazy `FLUSHED` record on its
/// primary for up to a second, so a forwarded request gets a few.
fn services(replicated: ReplicatedServices) -> RoutedServices {
    let routing = RoutingConfig {
        connect_timeout: Duration::from_millis(300),
        request_timeout: Duration::from_secs(4),
        register_interval: Duration::from_millis(200),
        ..RoutingConfig::default()
    };
    RoutedServices::new(replicated, routing)
}

/// A cluster whose commits and served requests are audited after every
/// step.
fn cluster(config: ClusterConfig, services: &RoutedServices) -> Cluster<RoutedServices> {
    Cluster::with_services(config, services.clone()).invariant(|view: &View<'_, RoutedServices>| {
        view.services.replicated().check_commits()?;
        view.services.check_served()
    })
}

#[test]
fn stale_gateways_are_redirected_to_the_primary() {
    Runner::with_cost(2, COST).run(|context| {
        let config = ClusterConfig {
            control_rates: FaultRates::default(),
            ..config()
        };
        let services = services(ReplicatedServices::default());
        let report = cluster(config, &services).run(context, &workload(), &FaultPlan::none())?;
        // No write fails (a failed write is recorded as unknown). A read
        // may: a `GET` whose object is replaced between its entry and its
        // payload answers a retryable `503`, which forwarding makes
        // likelier.
        assert_eq!(report.count(|o| *o == Outcome::Unknown), 0);
        let failed = report.count(|o| *o == Outcome::Failed);
        let done = report.count(|o| *o == Outcome::Done);
        assert!(
            failed * 20 <= done,
            "{failed} reads failed, {done} writes done"
        );
        let (served, stats) = services.stats();
        assert!(served > 0);
        // Most requests reach a node that is not their shard's primary.
        assert!(stats.forwarded > 0, "{stats:?}");
        assert!(stats.redirects > 0, "{stats:?}");
        assert!(stats.register_reads > 0, "{stats:?}");
        assert!(report.flushed > 0, "{report:?}");
        Ok(())
    });
}

#[test]
fn routing_under_crashes_partitions_and_message_loss() {
    Runner::with_cost(4, COST / 2).run(|context| {
        let config = config();
        let workload = workload();
        let profile = FaultProfile {
            end: Duration::from_secs(8) * context.scale(),
            crashes: 3,
            message_loss: 2,
            ..FaultProfile::default()
        };
        let plan = FaultPlan::random(
            context.rng(),
            &profile,
            config.nodes,
            config.disks_per_node,
            workload.clients,
        );
        let services = services(ReplicatedServices::default());
        let report = cluster(config, &services).run(context, &workload, &plan)?;
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        Ok(())
    });
}

#[test]
fn routing_while_a_primary_restarts() {
    Runner::with_cost(2, COST).run(|context| {
        let config = config();
        let workload = workload();
        // Every node is the primary of some shards and restarts once, so
        // gateways meet primaries that are down, then reconciling, and
        // connections the restart broke.
        let mut plan = FaultPlan::none();
        for node in 0..config.nodes {
            plan.push(
                Duration::from_millis(600 + 1000 * node as u64),
                Fault::Crash {
                    node,
                    power_loss: node % 2 == 0,
                    downtime: Duration::from_millis(400),
                },
            );
        }
        let services = services(ReplicatedServices::default()).fresh();
        let report = cluster(config, &services).run(context, &workload, &plan)?;
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        assert!(report.lives >= 8, "{}", report.lives);
        Ok(())
    });
}

#[test]
fn the_audit_catches_a_member_that_serves_reads() {
    Runner::with_cost(1, COST).run(|context| {
        let config = ClusterConfig {
            control_rates: FaultRates::default(),
            ..config()
        };
        let workload = Workload {
            clients: 2,
            operations: 30,
            ..workload()
        };
        // Every gateway asks a member first, which serves a read where it
        // should redirect.
        let services = services(ReplicatedServices::members_serving_reads()).members_first();
        let outcome = cluster(config, &services).run(context, &workload, &FaultPlan::none());
        match outcome {
            Err(RunError::Simulation(error)) => {
                assert!(error.contains("but its primary is"), "{error}");
                Ok(())
            }
            other => Err(format!("a member serving reads went unnoticed: {other:?}").into()),
        }
    });
}
