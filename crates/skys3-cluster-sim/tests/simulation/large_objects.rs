//! Large objects under faults (plan M4-13, the M4 exit criterion, design
//! §17): no partial remote object is ever visible, and ETags match.
//!
//! Each scenario runs three replicated nodes with takeovers and clients
//! that send each request to any node, with the large-object mix
//! (`ClusterConfig::large_objects`): streamed single `PUT`s, some of
//! whose bodies fail their checks after streaming; multipart uploads of
//! two to four parts with re-uploaded parts, UploadPartCopy, completions
//! that leave a part out, aborts, and abandoned uploads; small `PUT`s,
//! reads, and deletes. One scenario per kind of target: `write_back`
//! buckets, `write_through` ones, and `local` buckets with a backup
//! target, acknowledged after their local commit or by the backup.
//!
//! The faults: crashes with and without power loss, of primaries and
//! members alike, which move primaries; crashes aimed at a node right
//! after the remote store applied one of its multipart steps or
//! `PutObject`s; partitions, held links, and message loss between nodes;
//! a node's link to the remote store dropped; and control-store faults.
//! The remote store fails each multipart step and `PutObject` with `500`
//! or `503 SlowDown`, and loses requests and answers, more often than
//! other requests. A quarter of the `write_back` seeds flush without
//! streaming, so that multipart versions go as the flusher's own uploads
//! after their commit.
//!
//! The audit checks every object the remote store holds as it appears and
//! once more against the final logs (see the harness's `large_objects`
//! module); the history checkers, with the write-through and backup
//! audits, check that no acknowledged write is lost. Three seeded bugs of
//! the flusher show that the audit catches partial objects, wrong
//! recorded ETags, and abandoned uploads left open.

use std::time::Duration;

use rand::Rng;
use skys3_cluster_sim::{
    Backup, Cluster, ClusterConfig, Fault, FaultPlan, FaultProfile, LargeObjectBug, LargeObjects,
    ReplicatedServices, Report, RoutedServices, RunError, Workload, WriteThrough,
};
use skys3_gateway::routing::RoutingConfig;
use skys3_shard::replication::ReplicationConfig;
use skys3_sim::history::Outcome;
use skys3_sim::s3::{Operation, SimS3Faults};
use skys3_sim::{Runner, SimContext};

use crate::COST;

/// The kind of remote target the buckets flush to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Target {
    /// Two `write_back` buckets, acknowledged after the local commit.
    WriteBack,
    /// Two `write_back` buckets with `ack_policy = "write_through"`.
    WriteThrough,
    /// Two `local` buckets backed up as this says.
    Backup(Backup),
}

/// A remote store whose requests take 2 to 15 ms each way.
const DELAYS: SimS3Faults = SimS3Faults {
    min_delay: Duration::from_millis(2),
    max_delay: Duration::from_millis(15),
    ..SimS3Faults::NONE
};

/// Three nodes, every shard on all of them, two buckets flushing to
/// `target` in a remote store whose requests take 2 to 15 ms each way
/// and fail or lose 1% of them, and every multipart step and `PutObject`
/// as `large` says.
fn config(target: Target, large: LargeObjects) -> ClusterConfig {
    let (write_back_buckets, write_through, backup) = match target {
        Target::WriteBack => (2, WriteThrough::Off, Backup::Off),
        Target::WriteThrough => (2, WriteThrough::On, Backup::Off),
        Target::Backup(backup) => (0, WriteThrough::Off, backup),
    };
    ClusterConfig {
        replicas: 3,
        every_member_durable: true,
        buckets: 2,
        write_back_buckets,
        remote_faults: SimS3Faults {
            internal_error_probability: 0.01,
            slow_down_probability: 0.01,
            lost_request_probability: 0.005,
            lost_response_probability: 0.01,
            ..DELAYS
        },
        write_through,
        backup,
        large_objects: Some(large),
        ..ClusterConfig::default()
    }
}

/// Clients that send each request to any node and wait out a takeover and
/// a write's flush.
fn workload(context: &SimContext) -> Workload {
    Workload {
        clients: 4,
        operations: 30 * context.scale() as usize,
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

/// Two crashes with or without power loss, two partitions or held links,
/// a window of message loss, and a control-store fault, within the first
/// eight seconds.
fn profile(context: &SimContext) -> FaultProfile {
    FaultProfile {
        end: Duration::from_secs(8) * context.scale(),
        crashes: 2,
        partitions: 2,
        message_loss: 1,
        sync_failures: 0,
        control: 1,
        max_duration: Duration::from_secs(3),
        ..FaultProfile::default()
    }
}

/// The faults of `profile`, and up to two drops of a node's link to the
/// remote store.
fn faults(
    context: &mut SimContext,
    config: &ClusterConfig,
    workload: &Workload,
    profile: &FaultProfile,
) -> FaultPlan {
    let mut plan = FaultPlan::random(
        context.rng(),
        profile,
        config.nodes,
        config.disks_per_node,
        workload.clients,
    );
    let rng = context.rng();
    for _ in 0..rng.random_range(0..=2) {
        let at = rng.random_range(profile.start..=profile.end);
        let fault = Fault::RemoteLink {
            node: rng.random_range(0..config.nodes),
            duration: rng.random_range(Duration::from_millis(200)..=profile.max_duration),
        };
        plan.push(at, fault);
    }
    plan
}

/// Runs `target` under every fault with the large-object mix.
fn run(context: &mut SimContext, target: Target, large: LargeObjects) -> Result<Report, RunError> {
    let config = config(target, large);
    let workload = workload(context);
    let plan = faults(context, &config, &workload, &profile(context));
    Cluster::with_services(config, services()).run(context, &workload, &plan)
}

/// What every run of a correct cluster shows: acknowledged writes, and
/// remote objects audited as they appeared.
fn check(report: &Report) {
    assert!(report.count(|o| *o == Outcome::Done) > 0, "{report:?}");
    let audit = &report.large_objects;
    assert!(
        audit.single + audit.streamed + audit.multipart > 0,
        "{audit:?}"
    );
}

/// `write_back` buckets: every remote object is a complete version with
/// its ETag, through crashes at every step, failovers, remote errors and
/// lost answers, and dropped links.
#[test]
fn write_back_large_objects_are_never_partial_at_the_remote() {
    Runner::with_cost(4, COST).run(|context| {
        let large = LargeObjects {
            // The flusher's own multipart uploads, after the commit.
            streaming: context.seed() % 4 != 3,
            ..LargeObjects::default()
        };
        check(&run(context, Target::WriteBack, large)?);
        Ok(())
    });
}

/// `write_through` buckets: the same, and every acknowledged write is at
/// the remote when it is acknowledged.
#[test]
fn write_through_large_objects_are_never_partial_at_the_remote() {
    Runner::with_cost(4, COST).run(|context| {
        let report = run(context, Target::WriteThrough, LargeObjects::default())?;
        check(&report);
        assert!(report.remote_checked > 0, "{report:?}");
        Ok(())
    });
}

/// `local` buckets with a backup target, acknowledged after the local
/// commit on even seeds and by the backup on odd ones: the same, and the
/// backup holds what the members hold once the flushers are done.
#[test]
fn backed_up_large_objects_are_never_partial_at_the_backup() {
    Runner::with_cost(4, COST).run(|context| {
        let backup = if context.seed() % 2 == 0 {
            Backup::Asynchronous
        } else {
            Backup::WriteThrough
        };
        let report = run(context, Target::Backup(backup), LargeObjects::default())?;
        check(&report);
        assert!(report.backups_drained, "{report:?}");
        Ok(())
    });
}

/// Runs `target` with `bug` seeded, without node faults and with only
/// `step_faults` at the remote store, and expects the audit to catch the
/// bug with a reason that contains `reason`.
fn caught(
    context: &mut SimContext,
    target: Target,
    bug: LargeObjectBug,
    step_faults: Vec<(Operation, SimS3Faults)>,
    reason: &str,
) -> turmoil::Result {
    let large = LargeObjects {
        step_faults,
        step_crash_probability: 0.0,
        bug,
        ..LargeObjects::default()
    };
    let config = ClusterConfig {
        remote_faults: DELAYS,
        ..config(target, large)
    };
    let workload = workload(context);
    let cluster = Cluster::with_services(config, services());
    match cluster.run(context, &workload, &FaultPlan::none()) {
        Err(RunError::Check(violation)) if violation.reason.contains(reason) => Ok(()),
        other => Err(format!("{bug:?} went unnoticed: {other:?}").into()),
    }
}

/// A streamed completion that leaves its last part out assembles a
/// truncated object at the remote, which the audit sees as it appears.
#[test]
fn the_audit_catches_a_completion_without_its_last_part() {
    Runner::with_cost(2, COST).run(|context| {
        caught(
            context,
            Target::WriteBack,
            LargeObjectBug::CompletesWithoutLastPart,
            Vec::new(),
            "partial object",
        )
    });
}

/// Recording the local ETag after a lost Complete answer leaves a
/// `FLUSHED` whose ETag no remote object of its version had: the MD5 of a
/// streamed body, not the multipart ETag of its parts.
#[test]
fn the_audit_catches_a_wrong_etag_recorded_after_a_lost_complete() {
    Runner::with_cost(2, COST).run(|context| {
        let lost = SimS3Faults {
            lost_response_probability: 0.4,
            ..DELAYS
        };
        caught(
            context,
            Target::WriteBack,
            LargeObjectBug::RecordsLocalEtagAfterLostComplete,
            vec![(Operation::CompleteMultipartUpload, lost)],
            "records the remote ETag",
        )
    });
}

/// Giving up a failed abort leaves a remote upload open whose ID a log
/// recorded.
#[test]
fn the_audit_catches_remote_uploads_left_open() {
    Runner::with_cost(2, COST).run(|context| {
        let failing = SimS3Faults {
            internal_error_probability: 0.25,
            ..DELAYS
        };
        caught(
            context,
            Target::Backup(Backup::Asynchronous),
            LargeObjectBug::GivesUpFailedAborts,
            vec![(Operation::AbortMultipartUpload, failing)],
            "was never completed or aborted",
        )
    });
}

/// A seed of the large-object mix replays exactly, with every step's
/// faults, the remote store's, and dropped links to it. Node faults are
/// left out: a crash drops the node's tasks in an order tokio takes from
/// task IDs, which the whole test process shares, and tokio wakes some
/// waiters in an order it draws at random unless built with
/// `tokio_unstable`.
#[test]
fn a_large_object_seed_replays_exactly() {
    let run = |seed| {
        let mut context = SimContext::new(seed);
        let large = LargeObjects {
            step_crash_probability: 0.0,
            ..LargeObjects::default()
        };
        let config = config(Target::WriteBack, large);
        let workload = workload(&context);
        let links = FaultProfile {
            crashes: 0,
            partitions: 0,
            message_loss: 0,
            control: 0,
            ..profile(&context)
        };
        let plan = faults(&mut context, &config, &workload, &links);
        Cluster::with_services(config, services()).run(&mut context, &workload, &plan)
    };
    Runner::with_cost(1, 4 * COST).run(|context| {
        let seed = context.seed();
        assert_eq!(run(seed)?, run(seed)?);
        Ok(())
    });
}
