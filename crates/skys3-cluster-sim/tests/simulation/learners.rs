//! Learners (plan M2-14, design §6.3, §6.7): a test driver adds a node
//! outside each shard's placement as a learner by a compare-and-swap of
//! the shard's register, the primary streams its log to it, takes it into
//! the acknowledgement set once it keeps up, and promotes it to member by
//! another compare-and-swap once it is durable up to the commit watermark.
//!
//! After every step the commit audit checks that no primary committed a
//! record a member did not hold (`check_commits`), and the R3 audit that
//! every member the register names, a promoted learner included, holds
//! every record any primary of the shard committed
//! (`check_members_hold_commits`). At the end, every acknowledged write
//! must be durable on every member of the final configurations.

use std::time::Duration;

use skys3_cluster_sim::{
    Cluster, ClusterConfig, Fault, FaultPlan, FaultProfile, ReplicatedServices, RunError, View,
    Workload,
};
use skys3_shard::replication::ReplicationConfig;
use skys3_sim::history::Outcome;
use skys3_sim::{Runner, SimContext};

use crate::COST;

/// Four nodes, every shard on three of them, so each has a spare to learn
/// it.
fn config() -> ClusterConfig {
    ClusterConfig {
        nodes: 4,
        replicas: 3,
        every_member_durable: true,
        ..ClusterConfig::default()
    }
}

/// Clients that keep writing through the promotions.
fn workload(context: &SimContext, operations: usize) -> Workload {
    Workload {
        clients: 6,
        operations: operations * context.scale() as usize,
        keys: 8,
        think_time: Duration::from_millis(60),
        timeout: Duration::from_secs(8),
        ..Workload::default()
    }
}

/// Services whose driver adds learners from 1 s on.
fn services(config: ReplicationConfig) -> ReplicatedServices {
    ReplicatedServices::new(config).with_learners(Duration::from_secs(1))
}

/// A cluster checked against R3 after every step.
fn cluster(services: &ReplicatedServices) -> Cluster<ReplicatedServices> {
    Cluster::with_services(config(), services.clone()).invariant(
        |view: &View<'_, ReplicatedServices>| {
            view.services.check_commits()?;
            view.services.check_members_hold_commits()
        },
    )
}

/// Every promotion's compare-and-swap takes over a second, as control-store
/// round trips are slow, and no write in flight meanwhile waits for it:
/// promotion adds no write stall.
#[test]
fn learners_are_promoted_without_stalling_writes() {
    Runner::with_cost(3, COST).run(|context| {
        let latency = Fault::ControlLatency {
            min: Duration::from_millis(500),
            duration: Duration::from_secs(30),
        };
        let plan = FaultPlan::none().with(Duration::ZERO, latency);
        let services = services(ReplicationConfig::default());
        // Long enough for two slow compare-and-swaps of each shard.
        let workload = Workload {
            operations: 150 * context.scale() as usize,
            think_time: Duration::from_millis(120),
            ..workload(context, 1)
        };
        let report = cluster(&services).run(context, &workload, &plan)?;
        let learners = services.learners();
        eprintln!("{learners:?}");
        assert!(learners.added > 0, "{learners:?}");
        assert!(learners.promoted > 0, "{learners:?}");
        assert!(
            learners.slowest_promotion >= Duration::from_secs(1),
            "{learners:?}"
        );
        assert!(
            learners.slowest_write_during < Duration::from_millis(500),
            "a write stalled during a promotion: {learners:?}"
        );
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        Ok(())
    });
}

/// Learners under crashes, power loss, partitions, message loss, and lost
/// compare-and-swap answers: learners drop out of the acknowledgement set
/// and rejoin, promotions are proposed again or lose, and primaries
/// restart with a promotion outstanding. R3 holds throughout.
#[test]
fn learners_under_random_faults() {
    Runner::with_cost(4, COST / 2).run(|context| {
        let workload = workload(context, 50);
        let profile = FaultProfile {
            end: Duration::from_secs(8) * context.scale(),
            crashes: 2,
            partitions: 2,
            message_loss: 1,
            control: 2,
            max_duration: Duration::from_secs(3),
            ..FaultProfile::default()
        };
        let config = config();
        let mut plan = FaultPlan::random(
            context.rng(),
            &profile,
            config.nodes,
            config.disks_per_node,
            workload.clients,
        );
        plan.push(
            Duration::from_millis(1500),
            Fault::LostCasResponses {
                probability: 0.5,
                duration: Duration::from_secs(4),
            },
        );
        let services = services(ReplicationConfig::default());
        let report = cluster(&services).run(context, &workload, &plan)?;
        eprintln!("{:?}", services.learners());
        assert!(services.learners().added > 0);
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        Ok(())
    });
}

/// The seeded bug: the driver promotes each learner as soon as it adds
/// it, before the learner holds anything. The R3 audit catches the member
/// that misses committed records.
#[test]
fn the_audit_catches_a_learner_promoted_before_it_caught_up() {
    Runner::with_cost(1, COST).run(|context| {
        let services = services(ReplicationConfig::default()).promoting_early();
        let outcome = cluster(&services).run(context, &workload(context, 60), &FaultPlan::none());
        match outcome {
            Err(RunError::Simulation(error)) => {
                assert!(
                    error.contains("held only seq") || error.contains("holds only seq"),
                    "{error}"
                );
                Ok(())
            }
            other => Err(format!("an early promotion went unnoticed: {other:?}").into()),
        }
    });
}
