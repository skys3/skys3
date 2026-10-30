//! Conflict policies (plan M4-06, design §7.2): an out-of-band writer puts
//! objects into the remote store of a `write_back` bucket while clients
//! write the same keys through any node, primaries crash and are taken
//! over, and the remote fails some requests. The final remote state is
//! audited against the bucket's `flush_conflict_policy`
//! (`ClusterConfig::conflicts`): `overwrite` leaves every key with its
//! final write, `discard_local` with the out-of-band write its final write
//! met, adopted by every member, and `hold` keeps those keys until an
//! operator resolves each through the flush service, under `overwrite` or
//! `discard_local`.

use std::time::Duration;

use skys3_cluster_sim::{
    Cluster, ClusterConfig, ConflictBug, Conflicts, FaultPlan, FaultProfile, ReplicatedServices,
    Report, RoutedServices, RunError, Workload,
};
use skys3_config::ConflictPolicy;
use skys3_gateway::routing::RoutingConfig;
use skys3_shard::CacheSettings;
use skys3_shard::replication::ReplicationConfig;
use skys3_sim::history::Outcome;
use skys3_sim::s3::SimS3Faults;
use skys3_sim::{Runner, SimContext};

use crate::COST;

/// Out-of-band writes while the clients run.
const WRITES: usize = 16;

/// Three nodes, every shard on all of them, a `local` bucket and a
/// `write_back` one under `conflicts`, a remote store whose requests take
/// 2 to 15 ms each way and, if `faulty`, fail or lose 2% of them, and a
/// clean cache that holds the whole workload, so that reads of a version
/// a discard adopted fill it from the remote.
fn config(conflicts: Conflicts, faulty: bool) -> ClusterConfig {
    let errors = if faulty { 0.02 } else { 0.0 };
    ClusterConfig {
        replicas: 3,
        every_member_durable: true,
        write_back_buckets: 1,
        remote_faults: SimS3Faults {
            min_delay: Duration::from_millis(2),
            max_delay: Duration::from_millis(15),
            internal_error_probability: errors,
            slow_down_probability: errors,
            lost_request_probability: errors / 2.0,
            lost_response_probability: errors,
            ..SimS3Faults::NONE
        },
        clean_cache: Some(CacheSettings {
            max_bytes: 1 << 20,
            reserve_fraction: 0.0,
        }),
        conflicts: Some(conflicts),
        ..ClusterConfig::default()
    }
}

/// Clients that send each request to any node and wait out a takeover.
fn workload(context: &SimContext, operations: usize) -> Workload {
    Workload {
        clients: 4,
        operations: operations * context.scale() as usize,
        keys: 6,
        think_time: Duration::from_millis(150),
        timeout: Duration::from_secs(6),
        any_gateway: true,
        ..Workload::default()
    }
}

/// Routing over replicated shards whose members take over from a primary
/// that is down, with the timings of the takeover scenarios.
fn services() -> RoutedServices {
    let replication = ReplicationConfig {
        lease_renew_interval: Duration::from_millis(200),
        primary_lease: Duration::from_millis(800),
        primary_grace: Duration::from_millis(1400),
        member_suspect_after: Duration::from_millis(700),
        ..ReplicationConfig::default()
    };
    let routing = RoutingConfig {
        connect_timeout: Duration::from_millis(300),
        request_timeout: Duration::from_secs(4),
        register_interval: Duration::from_millis(200),
        ..RoutingConfig::default()
    };
    RoutedServices::new(
        ReplicatedServices::new(replication).with_takeover(),
        routing,
    )
}

/// Crashes with and without power loss, which move primaries, partitions,
/// message loss, and control-store faults.
fn faults(context: &mut SimContext, config: &ClusterConfig, workload: &Workload) -> FaultPlan {
    let profile = FaultProfile {
        end: Duration::from_secs(8) * context.scale(),
        crashes: 2,
        partitions: 1,
        message_loss: 1,
        sync_failures: 0,
        control: 1,
        max_duration: Duration::from_secs(3),
        ..FaultProfile::default()
    };
    FaultPlan::random(
        context.rng(),
        &profile,
        config.nodes,
        config.disks_per_node,
        workload.clients,
    )
}

/// Runs `policy` under faults and returns the report, which the audit
/// passed.
fn run_under_faults(context: &mut SimContext, policy: ConflictPolicy) -> Result<Report, RunError> {
    let config = config(Conflicts::new(policy, WRITES), true);
    let workload = workload(context, 30);
    let plan = faults(context, &config, &workload);
    let report = Cluster::with_services(config, services()).run(context, &workload, &plan)?;
    assert!(report.count(|o| *o == Outcome::Done) > 0);
    assert!(report.conflicts.burst > 0, "{:?}", report.conflicts);
    assert!(report.conflicts.out_of_band > 0, "{:?}", report.conflicts);
    Ok(report)
}

/// Under `overwrite`, the final write of every key replaces whatever
/// another writer left, through takeovers and remote faults.
#[test]
fn overwrite_leaves_every_key_with_its_final_write() {
    Runner::with_cost(4, COST).run(|context| {
        let report = run_under_faults(context, ConflictPolicy::Overwrite)?;
        assert_eq!(report.conflicts.adopted, 0, "{:?}", report.conflicts);
        assert_eq!(report.conflicts.held, 0, "{:?}", report.conflicts);
        Ok(())
    });
}

/// Under `discard_local`, a final write that meets an out-of-band write is
/// dropped for it, on every member, and the others reach the remote.
#[test]
fn discard_local_adopts_the_out_of_band_writes_it_meets() {
    Runner::with_cost(4, COST).run(|context| {
        let report = run_under_faults(context, ConflictPolicy::DiscardLocal)?;
        assert!(
            report.conflicts.adopted >= report.conflicts.burst,
            "{:?}",
            report.conflicts
        );
        assert_eq!(report.conflicts.held, 0, "{:?}", report.conflicts);
        Ok(())
    });
}

/// Under `hold`, a final write that meets an out-of-band write is held,
/// and the remote keeps the other writer's object, until an operator
/// resolves the key: Conflict → Dirty, then flushed by the resolution.
#[test]
fn hold_keeps_conflicts_until_an_operator_resolves_them() {
    Runner::with_cost(4, COST).run(|context| {
        let report = run_under_faults(context, ConflictPolicy::Hold)?;
        assert!(
            report.conflicts.held >= report.conflicts.burst,
            "{:?}",
            report.conflicts
        );
        Ok(())
    });
}

/// Runs `conflicts`, with a seeded bug, without faults, and returns what
/// the checkers found.
fn seeded(context: &mut SimContext, conflicts: Conflicts) -> Result<Report, RunError> {
    let config = config(conflicts, false);
    let workload = workload(context, 20);
    Cluster::with_services(config, services()).run(context, &workload, &FaultPlan::none())
}

/// The seeded bug of discarding in a bucket that did not opt in: under
/// `overwrite`, a key ends with an out-of-band write.
#[test]
fn the_audit_catches_discards_without_the_opt_in() {
    Runner::with_cost(2, COST).run(|context| {
        let conflicts = Conflicts::new(ConflictPolicy::Overwrite, WRITES)
            .with_bug(ConflictBug::DiscardsWithoutOptIn);
        match seeded(context, conflicts) {
            Err(RunError::Check(violation)) => {
                assert!(violation.reason.contains("overwrite"), "{violation}");
                Ok(())
            }
            other => Err(format!("the bug went unnoticed: {other:?}").into()),
        }
    });
}

/// The seeded bug of a resolution that records the key as flushed,
/// returning it to clean instead of dirty: the remote keeps the other
/// writer's object while the members hold the final write.
#[test]
fn the_audit_catches_a_resolution_that_returns_the_key_to_clean() {
    Runner::with_cost(2, COST).run(|context| {
        let conflicts =
            Conflicts::new(ConflictPolicy::Hold, WRITES).with_bug(ConflictBug::ResolvesClean);
        match seeded(context, conflicts) {
            Err(RunError::Check(violation)) => {
                assert!(violation.reason.contains("resolved with"), "{violation}");
                Ok(())
            }
            other => Err(format!("the bug went unnoticed: {other:?}").into()),
        }
    });
}
