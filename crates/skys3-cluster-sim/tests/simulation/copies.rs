//! Remote server-side copies (plan M4-07, design §7.2, §11): a copier
//! copies clean objects of two `write_back` buckets, which flush to one
//! remote bucket, within and across the buckets through any node, and
//! sometimes writes a copy's source again right after copying it, while
//! the workload runs, primaries crash and are taken over, and the remote
//! fails or loses some requests. The copies flush as remote `CopyObject`
//! requests where their source is clean, and as uploads where the source
//! changed at the remote first. The audit (`ClusterConfig::copies`)
//! checks that every copier key ends with the members' version at the
//! remote, bytes, metadata, tags, and write identity, and that no conflict
//! is reported for the cluster's own writes.

use std::time::Duration;

use skys3_cluster_sim::{
    Cluster, ClusterConfig, Copies, CopyBug, FaultPlan, FaultProfile, ReplicatedServices, Report,
    RoutedServices, RunError, Workload,
};
use skys3_gateway::routing::RoutingConfig;
use skys3_shard::replication::ReplicationConfig;
use skys3_sim::history::Outcome;
use skys3_sim::s3::SimS3Faults;
use skys3_sim::{Runner, SimContext};

use crate::COST;

/// Copies the copier sends.
const COPIES: usize = 24;

/// Three nodes, every shard on all of them, two `write_back` buckets, and
/// a remote store whose requests take 2 to 15 ms each way, `errors` of
/// them failing with `500` and as many with `503`, and, if `lost`, half as
/// many lost and as many with their answer lost.
fn config(copies: Copies, errors: f64, lost: bool) -> ClusterConfig {
    let lost = if lost { errors } else { 0.0 };
    ClusterConfig {
        replicas: 3,
        every_member_durable: true,
        buckets: 2,
        write_back_buckets: 2,
        remote_faults: SimS3Faults {
            min_delay: Duration::from_millis(2),
            max_delay: Duration::from_millis(15),
            internal_error_probability: errors,
            slow_down_probability: errors,
            lost_request_probability: lost / 2.0,
            lost_response_probability: lost,
            ..SimS3Faults::NONE
        },
        copies: Some(copies),
        ..ClusterConfig::default()
    }
}

/// Clients that send each request to any node and wait out a takeover.
fn workload(context: &SimContext, operations: usize) -> Workload {
    Workload {
        clients: 3,
        operations: operations * context.scale() as usize,
        keys: 4,
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

/// Under crashes with and without power loss, which move primaries,
/// partitions, message loss, control-store faults, and remote failures,
/// copies of clean sources flush as server-side copies, the others as
/// uploads, and every copier key ends at the remote exactly as the members
/// hold it, with no conflict.
#[test]
fn copies_flush_server_side_and_are_never_conflicts() {
    Runner::with_cost(4, 2 * COST).run(|context| {
        let config = config(Copies::new(COPIES), 0.02, true);
        let workload = workload(context, 24);
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
        let plan = FaultPlan::random(
            context.rng(),
            &profile,
            config.nodes,
            config.disks_per_node,
            workload.clients,
        );
        let report = Cluster::with_services(config, services()).run(context, &workload, &plan)?;
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        let copies = &report.copies;
        assert!(copies.acknowledged > 0, "{copies:?}");
        assert!(copies.server_side > 0, "{copies:?}");
        assert!(copies.rewritten > 0, "{copies:?}");
        assert!(copies.audited > 0, "{copies:?}");
        Ok(())
    });
}

/// Runs the copier with `copies`, a seeded bug, without faults, and
/// returns what the checkers found.
fn seeded(context: &mut SimContext, copies: Copies) -> Result<Report, RunError> {
    let config = config(copies, 0.0, false);
    let workload = workload(context, 12);
    Cluster::with_services(config, services()).run(context, &workload, &FaultPlan::none())
}

/// The seeded bug of copies flushed without their own write identity: the
/// remote object does not name the copy's record.
#[test]
fn the_audit_catches_copies_without_their_identity() {
    Runner::with_cost(2, 2 * COST).run(|context| {
        let copies = Copies::new(COPIES / 2)
            .with_rewrites(0.0, 0.0)
            .with_bug(CopyBug::WithoutIdentity);
        match seeded(context, copies) {
            Err(RunError::Check(violation)) => {
                assert!(violation.reason.contains("write identity"), "{violation}");
                Ok(())
            }
            other => Err(format!("the bug went unnoticed: {other:?}").into()),
        }
    });
}

/// The seeded bug of a copy whose source changed at the remote taken for
/// a conflict at its destination: the cluster's own copy is held. A
/// source written again right after its copy usually flushes after it;
/// a failed first `CopyObject` holds the copy back for a retry, while its
/// source's new version lands.
#[test]
fn the_audit_catches_a_changed_source_taken_for_a_conflict() {
    Runner::with_cost(2, 2 * COST).run(|context| {
        let copies = Copies::new(COPIES / 2)
            .with_rewrites(1.0, 1.0)
            .with_bug(CopyBug::SourceFailureAsConflict);
        match seeded(context, copies) {
            Err(RunError::Check(violation)) => {
                let reason = &violation.reason;
                assert!(reason.starts_with("a conflict was reported"), "{violation}");
                Ok(())
            }
            other => Err(format!("the bug went unnoticed: {other:?}").into()),
        }
    });
}
