//! Leases and strong reads (plan M2-09, design §5.4): a primary serves
//! reads only while every member grants it a lease, and each member's
//! `primary_grace` outlasts every lease it granted while clocks drift
//! within `ρ`. The lease audit of [`ReplicatedServices`] checks every read
//! a primary serves against its members' grace, in simulated time.
//!
//! Static placements have no takeover yet (plan M2-12), so a read can only
//! be stale in principle: the audit flags a read served once some member
//! could have proposed itself, which is the moment a takeover could start
//! acknowledging writes the read misses.

use std::time::Duration;

use skys3_cluster_sim::{
    Cluster, ClusterConfig, Drift, Endpoint, Fault, FaultPlan, FaultProfile, ReplicatedServices,
    RunError, View, Workload,
};
use skys3_shard::replication::ReplicationConfig;
use skys3_sim::history::Outcome;
use skys3_sim::{Runner, SimContext};

use crate::COST;

/// The lease timings of these scenarios: shorter than the defaults so that
/// partitions outlast leases and grace quickly, and within the design's
/// inequality for `ρ` = 1% ([`timings_keep_the_lease_inequality`]).
fn replication() -> ReplicationConfig {
    ReplicationConfig {
        lease_renew_interval: Duration::from_millis(200),
        primary_lease: Duration::from_millis(800),
        primary_grace: Duration::from_millis(1400),
        ..ReplicationConfig::default()
    }
}

/// The drift bound `ρ` = 1%.
fn rho() -> Drift {
    Drift::from_ppm(10_000).unwrap()
}

/// Three nodes, every shard on all of them.
fn config() -> ClusterConfig {
    ClusterConfig {
        replicas: 3,
        every_member_durable: true,
        ..ClusterConfig::default()
    }
}

/// A busy workload: many clients with short pauses, so reads land in every
/// window a partition opens. A partition stalls the writes of every shard
/// it cuts a member from, so clients give up on an answer soon and move
/// on, rather than all waiting out the partition.
fn workload(context: &SimContext) -> Workload {
    Workload {
        clients: 8,
        operations: 120 * context.scale() as usize,
        keys: 6,
        think_time: Duration::from_millis(60),
        timeout: Duration::from_millis(300),
        ..Workload::default()
    }
}

/// `count` partitions of `duration`, one after another, between node `a`
/// and node `b`, the first at `first`, each `every` after the last began.
fn partitions(
    plan: &mut FaultPlan,
    (a, b): (usize, usize),
    first: Duration,
    every: Duration,
    count: u32,
    duration: Duration,
) {
    for n in 0..count {
        plan.push(
            first + every * n,
            Fault::Partition {
                a: Endpoint::Node(a),
                b: Endpoint::Node(b),
                duration,
            },
        );
    }
}

#[test]
fn timings_keep_the_lease_inequality() {
    let timings = replication();
    let config = skys3_config::ReplicationConfig {
        lease_renew_interval_ms: 200,
        primary_lease_ms: 800,
        primary_grace_ms: 1400,
        assumed_clock_drift: 0.01,
        ..skys3_config::ReplicationConfig::default()
    };
    assert_eq!(config.primary_lease(), timings.primary_lease);
    assert_eq!(config.primary_grace(), timings.primary_grace);
    assert!(config.min_primary_grace_ms() <= 1400);
    assert!(timings.beacon_interval <= timings.lease_renew_interval);
    assert!(timings.lease_renew_interval < timings.primary_lease);
}

/// Partitions between every pair of nodes, each longer than a member's
/// grace, with crashes, held links, and message loss on top, and every
/// node's clock drifting within `ρ`: primaries lose their leases and refuse
/// reads, and no read is served after a member's grace has passed.
#[test]
fn no_stale_read_under_partitions_and_drift_within_rho() {
    Runner::with_cost(3, COST).run(|context| {
        // The worst case within the bound: node 1 as slow and node 2 as
        // fast as `ρ` allows, in every life. Node 3 draws its drift.
        let config = ClusterConfig {
            drift: rho(),
            node_drifts: vec![Drift::from_ppm(-rho().ppm()).unwrap(), rho()],
            ..config()
        };
        let workload = workload(context);
        let profile = FaultProfile {
            end: Duration::from_secs(8) * context.scale(),
            crashes: 2,
            partitions: 3,
            message_loss: 1,
            sync_failures: 0,
            control: 0,
            max_duration: Duration::from_secs(2),
            ..FaultProfile::default()
        };
        let mut plan = FaultPlan::random(
            context.rng(),
            &profile,
            config.nodes,
            config.disks_per_node,
            workload.clients,
        );
        let every = Duration::from_millis(2500);
        let duration = Duration::from_millis(2000);
        for (n, pair) in [(0, 1), (1, 2), (2, 0)].into_iter().enumerate() {
            let first = Duration::from_millis(300) + every * n as u32;
            partitions(&mut plan, pair, first, every * 3, 1, duration);
        }
        let services = ReplicatedServices::new(replication());
        let report = Cluster::with_services(config, services.clone())
            .invariant(|view: &View<'_, ReplicatedServices>| view.services.check_commits())
            .invariant(|view: &View<'_, ReplicatedServices>| view.services.check_reads())
            .run(context, &workload, &plan)?;
        let leases = services.leases();
        assert_eq!(leases.stale, 0);
        assert!(leases.served > 0, "{leases:?}");
        // Every partition outlasts the leases, so some reads were refused.
        assert!(leases.refused > 0, "{leases:?}");
        assert!(report.count(|o| matches!(o, Outcome::Read(_))) > 0);
        Ok(())
    });
}

/// Drift beyond `ρ` (§13): node 1's clock runs 40% slow and node 2's 40%
/// fast, so while they are partitioned, node 2's grace for node 1's shards
/// passes before node 1's lease from it expires. Node 1 then serves reads
/// that a takeover by node 2 would make stale: about 1.33 s of lease
/// against 1 s of grace in real time. Writes stay safe all the same: no
/// commit runs ahead of the members, and the history is linearizable with
/// every acknowledged write on every member.
#[test]
fn drift_beyond_rho_risks_stale_reads_but_writes_stay_safe() {
    let mut stale = 0;
    Runner::with_cost(2, COST).run(|context| {
        let config = ClusterConfig {
            drift: Drift::NONE,
            node_drifts: vec![
                Drift::from_ppm(-400_000).unwrap(),
                Drift::from_ppm(400_000).unwrap(),
            ],
            ..config()
        };
        let workload = workload(context);
        let mut plan = FaultPlan::none();
        let every = Duration::from_millis(3000);
        partitions(
            &mut plan,
            (0, 1),
            Duration::from_millis(300),
            every,
            3,
            Duration::from_millis(2000),
        );
        let services = ReplicatedServices::new(replication());
        let outcome = Cluster::with_services(config, services.clone())
            .invariant(|view: &View<'_, ReplicatedServices>| view.services.check_commits())
            .run(context, &workload, &plan);
        let report = match outcome {
            Ok(report) => report,
            Err(RunError::Check(violation)) => {
                return Err(format!("drift broke write safety: {violation}").into());
            }
            Err(error) => return Err(error.into()),
        };
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        let leases = services.leases();
        stale += leases.stale;
        if leases.stale > 0 {
            let error = services.check_reads().unwrap_err();
            assert!(error.contains("primary_grace of node-2"), "{error}");
            assert!(error.contains("node-1 served"), "{error}");
        }
        Ok(())
    });
    eprintln!("drift beyond ρ: {stale} reads served after a member's grace had passed");
    assert!(
        stale > 0,
        "drift beyond ρ let no read past a member's grace"
    );
}
