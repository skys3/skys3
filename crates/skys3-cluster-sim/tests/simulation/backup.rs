//! Backup targets of `local` buckets (plan M4-09, design §8.9): every
//! committed change of a `local` bucket reaches its backup target, and
//! with `backup_ack = "write_through"` a write is acknowledged only once
//! the backup has it.
//!
//! Each scenario runs three replicated nodes with takeovers, clients that
//! send each request to any node, two `local` buckets backed up to a slow
//! and faulty remote store, a clean cache smaller than the workload, and
//! compaction. The audits (`ClusterConfig::backup`): once every fault
//! healed and the backup flushers are idle, the backup holds exactly what
//! every member holds of each key, deletes included; every member still
//! holds the bytes of each acknowledged write, also once the backup made
//! its entry clean; and with `backup_ack = "write_through"`, the backup
//! alone holds each acknowledged write when its client records it, and
//! every one once the clients are done, as if every node's disks were
//! destroyed.

use std::time::Duration;

use skys3_cluster_sim::{
    Backup, Cluster, ClusterConfig, FaultPlan, FaultProfile, ReadRegistration, ReplicatedServices,
    Report, RoutedServices, RunError, Workload,
};
use skys3_gateway::routing::RoutingConfig;
use skys3_shard::replication::ReplicationConfig;
use skys3_shard::{CacheSettings, CompactionSettings};
use skys3_sim::history::Outcome;
use skys3_sim::s3::SimS3Faults;
use skys3_sim::{Runner, SimContext};

use crate::COST;

/// Three nodes, every shard on all of them, two `local` buckets backed up
/// as `backup` says to a remote store whose requests take 2 to 15 ms each
/// way (longer than a message between nodes) and, if `faulty`, fail or
/// lose 2% of them; a clean cache of a few bodies, and compaction with
/// short delays, so that whatever could drop a backed-up payload runs.
fn config(backup: Backup, faulty: bool) -> ClusterConfig {
    let errors = if faulty { 0.02 } else { 0.0 };
    ClusterConfig {
        replicas: 3,
        every_member_durable: true,
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
            max_bytes: 2048,
            reserve_fraction: 0.1,
        }),
        compaction: Some(CompactionSettings {
            live_threshold: 0.6,
            unreferenced_ttl: Duration::from_secs(1),
        }),
        read_registration: ReadRegistration {
            release_delay: Duration::from_secs(1),
            ..ReadRegistration::default()
        },
        backup,
        ..ClusterConfig::default()
    }
}

/// Clients that send each request to any node and wait out a takeover and
/// a write's flush.
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
        partitions: 2,
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

/// What every run of a correct cluster shows.
fn check(report: &Report) {
    assert!(report.count(|o| *o == Outcome::Done) > 0);
    assert!(report.backups_drained, "{report:?}");
    // The backup holds an object, unless the clients deleted every key
    // last.
    let present = report.history.iter().any(|operation| {
        operation.process == "verifier" && matches!(operation.outcome, Outcome::Read(Some(_)))
    });
    assert!(report.backed_up > 0 || !present, "{report:?}");
}

/// Under crashes, takeovers, partitions, message loss, and remote faults,
/// the backup ends up with every committed change, and no member drops
/// the bytes of a backed-up object.
#[test]
fn every_committed_change_reaches_the_backup_under_faults() {
    Runner::with_cost(4, COST).run(|context| {
        let config = config(Backup::Asynchronous, true);
        let workload = workload(context, 40);
        let plan = faults(context, &config, &workload);
        let report = Cluster::with_services(config, services()).run(context, &workload, &plan)?;
        check(&report);
        Ok(())
    });
}

/// With `backup_ack = "write_through"`, no acknowledged write is missing
/// from the backup, at its acknowledgement or once the clients are done,
/// under the same faults.
#[test]
fn losing_every_node_after_a_backup_acknowledgement_loses_no_write() {
    Runner::with_cost(4, COST).run(|context| {
        let config = config(Backup::WriteThrough, true);
        let workload = workload(context, 40);
        let plan = faults(context, &config, &workload);
        let report = Cluster::with_services(config, services()).run(context, &workload, &plan)?;
        check(&report);
        assert!(report.remote_checked > 0, "{report:?}");
        Ok(())
    });
}

/// Runs a seeded bug without faults, and returns what the checkers found.
///
/// A local eviction runs with a cache that holds the whole workload and no
/// compaction, so that only members beyond `clean_copies` drop their
/// copies and every read is still served by its primary: the member audit,
/// not a failed read, finds the loss. A forgotten delete shows only if it
/// is the last change of a key the backup held, as a later write replaces
/// the object: more keys and operations make that likely at every seed.
fn seeded(context: &mut SimContext, bug: Backup) -> Result<Report, RunError> {
    let mut config = config(bug, false);
    let mut workload = workload(context, 20);
    match bug {
        Backup::EvictsLocal => {
            config.clean_cache = Some(CacheSettings {
                max_bytes: 1 << 20,
                reserve_fraction: 0.0,
            });
            config.compaction = None;
        }
        Backup::ForgetsDeletes => {
            workload.keys = 16;
            workload.operations *= 2;
        }
        _ => {}
    }
    Cluster::with_services(config, services()).run(context, &workload, &FaultPlan::none())
}

/// The seeded bug of acknowledging a write after its local commit only:
/// the audit at acknowledgement finds a write the backup does not hold.
#[test]
fn the_audit_catches_writes_acknowledged_before_the_backup_has_them() {
    Runner::with_cost(2, COST).run(|context| match seeded(context, Backup::AckedLocally) {
        Err(RunError::Check(violation)) => {
            assert!(violation.key.starts_with("bucket-"), "{violation}");
            assert!(violation.reason.contains("is lost"), "{violation}");
            Ok(())
        }
        other => Err(format!("the bug went unnoticed: {other:?}").into()),
    });
}

/// The seeded bug of evicting backed-up payload as a `write_back` bucket's
/// cache does: members beyond `clean_copies` drop their copies once the
/// backup has them, which the member audit finds.
#[test]
fn the_checkers_catch_a_local_eviction_of_backed_up_data() {
    Runner::with_cost(2, COST).run(|context| match seeded(context, Backup::EvictsLocal) {
        Err(RunError::Check(violation)) => {
            assert!(violation.key.starts_with("bucket-"), "{violation}");
            assert!(violation.reason.contains("is lost"), "{violation}");
            Ok(())
        }
        other => Err(format!("the bug went unnoticed: {other:?}").into()),
    });
}

/// The seeded bug of recording a delete as flushed without deleting the
/// key at the backup: the backup keeps an object the members deleted.
#[test]
fn the_audit_catches_a_delete_that_never_reaches_the_backup() {
    Runner::with_cost(2, COST).run(|context| match seeded(context, Backup::ForgetsDeletes) {
        Err(RunError::Check(violation)) => {
            assert!(
                violation.reason.contains("backup target holds"),
                "{violation}"
            );
            Ok(())
        }
        other => Err(format!("the bug went unnoticed: {other:?}").into()),
    });
}
