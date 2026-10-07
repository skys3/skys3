//! Index snapshots and the lost-key report (plan M4-11, design §6.9,
//! §8.9): the restore drill of a shard whose members are all lost.
//!
//! Each scenario runs three replicated nodes with takeovers, clients that
//! send each request to any node, two `local` buckets and a `write_back`
//! one, with a snapshot of every shard every second to a remote store
//! whose requests take 2 to 15 ms each way. At three random moments of the
//! workload the drill assumes every member of every shard lost, keeping
//! the remote store as it is then, and after the run it builds each
//! shard's lost-key report from the latest snapshot and checks it against
//! the clients' history (`ClusterConfig::snapshots`): every key whose last
//! value existed only on the members is reported, with that value; no
//! other key is; and every key left out was written in the report's
//! window. With backup targets the `local` buckets' snapshots go to them
//! and the report reads them as durable homes; without, every object of a
//! `local` bucket is lost with its members.

use std::time::Duration;

use skys3_cluster_sim::{
    Backup, Cluster, ClusterConfig, FaultPlan, FaultProfile, ReplicatedServices, Report,
    RoutedServices, RunError, SnapshotBug, Snapshots, Workload,
};
use skys3_gateway::routing::RoutingConfig;
use skys3_shard::replication::ReplicationConfig;
use skys3_sim::history::Outcome;
use skys3_sim::s3::SimS3Faults;
use skys3_sim::{Runner, SimContext};

use crate::COST;

/// Three nodes, every shard on all of them; buckets 0 and 1 `local`,
/// backed up as `backup` says, and bucket 2 `write_back`; snapshots with
/// `bug` seeded.
fn config(backup: Backup, bug: SnapshotBug, faulty: bool) -> ClusterConfig {
    let errors = if faulty { 0.02 } else { 0.0 };
    ClusterConfig {
        replicas: 3,
        buckets: 3,
        write_back_buckets: 1,
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
        backup,
        snapshots: Some(Snapshots {
            bug,
            ..Snapshots::default()
        }),
        ..ClusterConfig::default()
    }
}

/// Clients that send each request to any node and wait out a takeover.
fn workload(context: &SimContext) -> Workload {
    Workload {
        clients: 4,
        operations: 40 * context.scale() as usize,
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

/// Crashes with and without power loss, which move primaries and so start
/// new chains, partitions, message loss, and control-store faults.
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

/// What every drill of a correct cluster shows.
fn check(report: &Report) {
    assert!(report.count(|o| *o == Outcome::Done) > 0);
    let audit = &report.snapshots;
    assert_eq!(audit.losses, 3, "{audit:?}");
    // Three buckets of four shards, at each loss.
    assert_eq!(audit.reports, 36, "{audit:?}");
    assert!(audit.from_snapshots > 0, "{audit:?}");
    assert!(audit.checked > 0, "{audit:?}");
}

/// With backup targets, under crashes, takeovers, partitions, message
/// loss, and remote faults, each report matches the history.
#[test]
fn the_lost_key_report_matches_the_history_with_backups() {
    Runner::with_cost(4, COST).run(|context| {
        let config = config(Backup::Asynchronous, SnapshotBug::None, true);
        let workload = workload(context);
        let plan = faults(context, &config, &workload);
        let report = Cluster::with_services(config, services()).run(context, &workload, &plan)?;
        check(&report);
        Ok(())
    });
}

/// Without backup targets every object of a `local` bucket is lost with
/// its members, and the report, from a base and its deltas, names each.
#[test]
fn the_lost_key_report_matches_the_history_without_backups() {
    Runner::with_cost(4, COST).run(|context| {
        let config = config(Backup::Off, SnapshotBug::None, true);
        let workload = workload(context);
        let plan = faults(context, &config, &workload);
        let report = Cluster::with_services(config, services()).run(context, &workload, &plan)?;
        check(&report);
        assert!(report.snapshots.lost > 0, "{:?}", report.snapshots);
        assert!(report.snapshots.from_deltas > 0, "{:?}", report.snapshots);
        Ok(())
    });
}

/// Runs a seeded bug without faults, with `local` buckets that are not
/// backed up, and returns what the drill found. A forgotten delete shows
/// only if the key was deleted between two snapshots of a chain and not
/// written again before the loss: that bug runs more keys, more
/// operations, and more losses.
fn seeded(context: &mut SimContext, bug: SnapshotBug) -> Result<Report, RunError> {
    let mut config = config(Backup::Off, bug, false);
    let mut workload = workload(context);
    if bug == SnapshotBug::KeepsRemoved {
        workload.keys = 16;
        workload.operations *= 2;
        if let Some(snapshots) = &mut config.snapshots {
            snapshots.losses = 6;
            snapshots.latest = Duration::from_secs(14);
        }
    }
    Cluster::with_services(config, services()).run(context, &workload, &FaultPlan::none())
}

/// Expects the drill to have caught a seeded bug, with one of `reasons`
/// in its finding.
fn caught(
    found: Result<Report, RunError>,
    reasons: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    match found {
        Err(RunError::Simulation(error)) if reasons.iter().any(|reason| error.contains(reason)) => {
            Ok(())
        }
        other => Err(format!("the bug went unnoticed: {other:?}").into()),
    }
}

/// A snapshot that leaves out dirty entries: a key held only by the
/// members, written before the window, is missing from the report.
#[test]
fn the_drill_catches_a_snapshot_without_dirty_entries() {
    Runner::with_cost(2, COST).run(|context| {
        let found = seeded(context, SnapshotBug::SkipsDirty);
        caught(found, &["is not reported"])
    });
}

/// Deltas that forget removed rows: a key deleted before the window is
/// still in the restored index, and reported lost. A seed runs twice the
/// operations of the others, and costs twice as much.
#[test]
fn the_drill_catches_deltas_that_forget_deletes() {
    Runner::with_cost(2, 2 * COST).run(|context| {
        let found = seeded(context, SnapshotBug::KeepsRemoved);
        caught(found, &["is reported lost"])
    });
}

/// A restore that applies the base alone but dates it as the latest
/// delta: its window starts after the state it restored, so a key written
/// or deleted in between is reported with its older value, reported though
/// it was deleted, or not reported at all.
#[test]
fn the_drill_catches_a_window_that_starts_after_the_restored_state() {
    Runner::with_cost(2, COST).run(|context| {
        let found = seeded(context, SnapshotBug::BaseOnly);
        caught(found, &["is not reported", "is reported lost"])
    });
}
