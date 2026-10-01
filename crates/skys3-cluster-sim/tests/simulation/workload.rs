//! The M1 workload under faults, with both checkers.

use std::time::Duration;

use skys3_cluster_sim::{
    Cluster, ClusterConfig, Endpoint, Fault, FaultPlan, FaultProfile, FaultRates, Workload,
};
use skys3_sim::history::Outcome;
use skys3_sim::{Runner, SimContext};

use crate::COST;

/// The cluster most scenarios run.
fn config() -> ClusterConfig {
    ClusterConfig::default()
}

/// The workload most scenarios run, longer at a larger scale.
fn workload(context: &SimContext) -> Workload {
    let base = Workload::default();
    Workload {
        operations: base.operations * context.scale() as usize,
        ..base
    }
}

#[test]
fn m1_workload_without_faults() {
    Runner::with_cost(2, COST).run(|context| {
        let workload = workload(context);
        let config = ClusterConfig {
            control_rates: FaultRates::default(),
            ..config()
        };
        let report = Cluster::new(config).run(context, &workload, &FaultPlan::none())?;
        // Every request is answered, and the final reads come on top.
        assert_eq!(report.count(|o| *o == Outcome::Unknown), 0);
        assert_eq!(report.count(|o| *o == Outcome::Failed), 0);
        assert!(report.history.len() >= workload.clients * workload.operations);
        assert_eq!(report.lives, 3);
        assert_eq!(report.faults, 0);
        Ok(())
    });
}

#[test]
fn m1_workload_under_random_faults() {
    Runner::with_cost(4, COST / 2).run(|context| {
        let config = config();
        let workload = workload(context);
        let profile = FaultProfile {
            end: Duration::from_secs(8) * context.scale(),
            ..FaultProfile::default()
        };
        let plan = FaultPlan::random(
            context.rng(),
            &profile,
            config.nodes,
            config.disks_per_node,
            workload.clients,
        );
        let report = Cluster::new(config).run(context, &workload, &plan)?;
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        Ok(())
    });
}

#[test]
fn whole_cluster_restart_while_the_control_store_is_unreachable() {
    Runner::with_cost(2, COST).run(|context| {
        let config = config();
        let workload = workload(context);
        // The store goes away, then every node restarts, with or without
        // power, and resumes from its local copy of control state.
        let mut plan = FaultPlan::none().with(
            Duration::from_secs(1),
            Fault::ControlOutage {
                duration: Duration::from_secs(4),
            },
        );
        for node in 0..config.nodes {
            plan.push(
                Duration::from_millis(1500 + 300 * node as u64),
                Fault::Crash {
                    node,
                    power_loss: node % 2 == 0,
                    downtime: Duration::from_millis(400),
                },
            );
        }
        let report = Cluster::new(config).run(context, &workload, &plan)?;
        // A node may also fail a start on a control-store fault and be
        // restarted by its supervisor.
        assert!(report.lives >= 6, "{}", report.lives);
        Ok(())
    });
}

#[test]
fn slow_and_lossy_control_store_with_partitions() {
    Runner::with_cost(2, COST).run(|context| {
        let config = config();
        let workload = workload(context);
        let plan = FaultPlan::none()
            .with(
                Duration::from_millis(500),
                Fault::ControlLatency {
                    min: Duration::from_millis(100),
                    duration: Duration::from_secs(3),
                },
            )
            .with(
                Duration::from_millis(600),
                Fault::LostCasResponses {
                    probability: 0.5,
                    duration: Duration::from_secs(3),
                },
            )
            .with(
                Duration::from_millis(700),
                Fault::Crash {
                    node: 1,
                    power_loss: true,
                    downtime: Duration::from_millis(300),
                },
            )
            .with(
                Duration::from_millis(800),
                Fault::Partition {
                    a: Endpoint::Node(0),
                    b: Endpoint::Client(1),
                    duration: Duration::from_secs(1),
                },
            )
            .with(
                Duration::from_millis(900),
                Fault::Hold {
                    a: Endpoint::Node(2),
                    b: Endpoint::Client(0),
                    duration: Duration::from_millis(800),
                },
            )
            .with(
                Duration::from_millis(1000),
                Fault::MessageLoss {
                    rate: 0.01,
                    duration: Duration::from_millis(500),
                },
            )
            .with(
                Duration::from_millis(1200),
                Fault::FailSync { node: 0, disk: 1 },
            );
        let report = Cluster::new(config).run(context, &workload, &plan)?;
        assert_eq!(report.faults, 7);
        assert!(report.lives >= 5, "{}", report.lives);
        Ok(())
    });
}
