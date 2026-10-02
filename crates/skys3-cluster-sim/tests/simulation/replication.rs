//! Replication (plan M2-07): every shard has three members under a static
//! placement, writes commit on all of them, and crashes and message loss
//! lose nothing committed.

use std::time::Duration;

use skys3_cluster_sim::{
    Cluster, ClusterConfig, Endpoint, Fault, FaultPlan, FaultProfile, FaultRates, IoCounts,
    LocalServices, ReplicatedServices, RunError, View, Workload,
};
use skys3_sim::history::Outcome;
use skys3_sim::{Runner, SimContext};

use crate::COST;

/// Three nodes, every shard on all of them, and every member checked for
/// every acknowledged write after recovery.
fn config() -> ClusterConfig {
    ClusterConfig {
        replicas: 3,
        every_member_durable: true,
        ..ClusterConfig::default()
    }
}

fn workload(context: &SimContext) -> Workload {
    let base = Workload::default();
    Workload {
        operations: base.operations * context.scale() as usize,
        ..base
    }
}

/// A cluster of replicated nodes whose commits are audited after every
/// step: no primary commits or acknowledges what a member does not hold
/// durably.
fn cluster(config: ClusterConfig, services: &ReplicatedServices) -> Cluster<ReplicatedServices> {
    Cluster::with_services(config, services.clone())
        .invariant(|view: &View<'_, ReplicatedServices>| view.services.check_commits())
}

/// Acknowledged writes in a run.
fn acknowledged(report: &skys3_cluster_sim::Report) -> usize {
    report
        .history
        .iter()
        .filter(|op| op.call.is_write() && op.outcome == Outcome::Done)
        .count()
}

#[test]
fn replicated_writes_without_faults() {
    Runner::with_cost(2, COST).run(|context| {
        let workload = workload(context);
        let config = ClusterConfig {
            control_rates: FaultRates::default(),
            ..config()
        };
        let services = ReplicatedServices::default();
        let report = cluster(config, &services).run(context, &workload, &FaultPlan::none())?;
        assert_eq!(report.count(|o| *o == Outcome::Unknown), 0);
        assert_eq!(report.count(|o| *o == Outcome::Failed), 0);
        assert!(report.history.len() >= workload.clients * workload.operations);
        Ok(())
    });
}

/// Design section 5.3's counts for small PUTs, over every replica: group
/// commits (each one sync of each file written) per acknowledged write,
/// and records per group commit, with many clients writing small bodies at
/// once. A write costs one record on each of the three members, all synced
/// in parallel, so at most three syncs, and fewer as group commits batch.
#[test]
fn replicated_write_io() {
    let (mut writes, mut io) = (0, IoCounts::default());
    Runner::with_cost(2, COST).run(|context| {
        let workload = Workload {
            clients: 24,
            operations: 20,
            keys: 64,
            max_body: 256,
            think_time: Duration::from_millis(2),
            ..Workload::default()
        };
        let config = ClusterConfig {
            control_rates: FaultRates::default(),
            ..config()
        };
        let services = ReplicatedServices::default();
        let report = cluster(config, &services).run(context, &workload, &FaultPlan::none())?;
        assert_eq!(report.count(|o| *o == Outcome::Failed), 0);
        writes += acknowledged(&report);
        let run = services.io();
        io.group_commits += run.group_commits;
        io.records += run.records;
        Ok(())
    });
    if writes == 0 {
        return;
    }
    // Multipart uploads write three records per acknowledged completion,
    // so the counts sit a little above one record per write and replica.
    let per_write = io.group_commits as f64 / writes as f64;
    let records_per_write = io.records as f64 / writes as f64;
    let per_commit = io.records as f64 / io.group_commits as f64;
    eprintln!(
        "{writes} acknowledged writes: {per_write:.2} group commits per write over all replicas, {records_per_write:.2} records per write, {per_commit:.2} records per group commit"
    );
    assert!(per_write < 3.0, "{per_write} group commits per write");
    assert!(per_commit > 1.0, "{per_commit} records per group commit");
}

#[test]
fn replicated_writes_under_crashes_and_message_loss() {
    Runner::with_cost(4, COST / 2).run(|context| {
        let config = config();
        let workload = workload(context);
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
        let services = ReplicatedServices::default();
        let report = cluster(config, &services).run(context, &workload, &plan)?;
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        Ok(())
    });
}

#[test]
fn primaries_lose_power_while_members_hold_their_tail() {
    Runner::with_cost(2, COST).run(|context| {
        let config = config();
        let workload = workload(context);
        // Every node is the primary of some shards. Each loses power in
        // turn, so its members may hold records it lost and roll them
        // forward to it; message loss between primaries and members drops
        // links mid-stream.
        let mut plan = FaultPlan::none().with(
            Duration::from_millis(300),
            Fault::MessageLoss {
                rate: 0.02,
                duration: Duration::from_secs(2),
            },
        );
        for node in 0..config.nodes {
            plan.push(
                Duration::from_millis(700 + 900 * node as u64),
                Fault::Crash {
                    node,
                    power_loss: true,
                    downtime: Duration::from_millis(300),
                },
            );
        }
        plan.push(
            Duration::from_millis(1200),
            Fault::Partition {
                a: Endpoint::Node(0),
                b: Endpoint::Node(1),
                duration: Duration::from_millis(800),
            },
        );
        plan.push(
            Duration::from_millis(1500),
            Fault::FailSync { node: 2, disk: 0 },
        );
        let services = ReplicatedServices::default();
        let report = cluster(config, &services).run(context, &workload, &plan)?;
        assert!(report.lives >= 6, "{}", report.lives);
        Ok(())
    });
}

#[test]
fn a_replicated_seed_replays_exactly() {
    let run = |seed| {
        let mut context = SimContext::new(seed);
        let config = ClusterConfig {
            buckets: 1,
            shards_per_bucket: 2,
            ..config()
        };
        let workload = Workload {
            clients: 2,
            operations: 15,
            ..Workload::default()
        };
        let plan = FaultPlan::random(
            context.rng(),
            &FaultProfile::default(),
            config.nodes,
            config.disks_per_node,
            workload.clients,
        );
        cluster(config, &ReplicatedServices::default())
            .run(&mut context, &workload, &plan)
            .unwrap()
            .history
    };
    Runner::with_cost(1, COST).run(|context| {
        let (first, second) = (run(context.seed()), run(context.seed()));
        if let Some(at) =
            (0..first.len().max(second.len())).find(|&i| first.get(i) != second.get(i))
        {
            return Err(format!(
                "the runs part at operation {at}: {:?} and {:?}",
                first.get(at),
                second.get(at)
            )
            .into());
        }
        Ok(())
    });
}

#[test]
fn the_audit_catches_acknowledgements_before_the_members_hold_the_write() {
    Runner::with_cost(1, COST).run(|context| {
        let workload = Workload {
            clients: 2,
            operations: 20,
            ..Workload::default()
        };
        let config = ClusterConfig {
            every_member_durable: false,
            ..config()
        };
        let services = ReplicatedServices::committing_alone();
        let outcome = cluster(config, &services).run(context, &workload, &FaultPlan::none());
        match outcome {
            Err(RunError::Simulation(error)) => {
                assert!(error.contains("acknowledged the write"), "{error}");
                Ok(())
            }
            other => Err(format!("early acknowledgements went unnoticed: {other:?}").into()),
        }
    });
}

#[test]
fn the_every_member_check_catches_writes_the_primary_kept_alone() {
    Runner::with_cost(1, COST).run(|context| {
        // M1 nodes keep each write on the primary only.
        let workload = Workload {
            clients: 2,
            operations: 30,
            ..Workload::default()
        };
        let outcome = Cluster::with_services(config(), LocalServices).run(
            context,
            &workload,
            &FaultPlan::none(),
        );
        match outcome {
            Err(RunError::Check(violation)) => {
                assert!(violation.to_string().contains("is lost"), "{violation}");
                Ok(())
            }
            other => Err(format!("writes on one member went unnoticed: {other:?}").into()),
        }
    });
}
