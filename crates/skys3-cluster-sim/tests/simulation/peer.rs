//! Flushing to a SkyS3 peer over the native protocol (plan M6-06, design
//! §7.8): a source cluster's `write_back` buckets and the backups of its
//! `local` ones flush to a destination cluster's bucket, which stages
//! their frames and applies their `COMMIT`s in its own shard log.
//!
//! Each scenario runs three replicated nodes with takeovers, clients that
//! send each request to any node, and a destination of one node that the
//! nodes reach over TCP, one connection per stream. Faults cut the links
//! between the nodes and the destination, hold them, lose messages, crash
//! source nodes so that new primaries take over their flushes, and
//! restart the destination, with and without power loss. The audits
//! (`ClusterConfig::peer`): no flusher records a key flushed unless the
//! destination applied that exact write identity; with `write_through`
//! acknowledgements, the destination holds each acknowledged write when
//! its client records it, and every one once the clients are done, as if
//! every source node were lost; and once everything healed and the
//! flushers are idle, after a power loss of the destination too, it holds
//! exactly what every member holds of each key, each write identity
//! applied once.

use std::time::Duration;

use skys3_cluster_sim::{
    Backup, Cluster, ClusterConfig, FaultPlan, FaultProfile, Peer, PeerBug, ReplicatedServices,
    Report, RoutedServices, RunError, Workload, WriteThrough,
};
use skys3_gateway::routing::RoutingConfig;
use skys3_shard::replication::ReplicationConfig;
use skys3_sim::history::Outcome;
use skys3_sim::{Runner, SimContext};

use crate::COST;

/// Three nodes, every shard on all of them, a `local` bucket backed up as
/// `backup` says and a `write_back` bucket, both flushed to the peer, with
/// write-through acknowledgements if `through`.
fn config(backup: Backup, through: bool, bug: PeerBug) -> ClusterConfig {
    ClusterConfig {
        replicas: 3,
        every_member_durable: true,
        buckets: 2,
        write_back_buckets: 1,
        backup,
        write_through: if through {
            WriteThrough::On
        } else {
            WriteThrough::Off
        },
        peer: Some(Peer {
            bug,
            ..Peer::default()
        }),
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

/// Crashes with and without power loss, which move primaries and their
/// flushes, partitions, message loss, a control-store fault, and links to
/// the peer cut or held and restarts of the peer.
fn faults(context: &mut SimContext, config: &ClusterConfig, workload: &Workload) -> FaultPlan {
    let profile = FaultProfile {
        end: Duration::from_secs(8) * context.scale(),
        crashes: 2,
        partitions: 2,
        message_loss: 1,
        sync_failures: 0,
        control: 1,
        max_duration: Duration::from_secs(3),
        peer: 4,
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
    assert!(report.peer.flushed > 0, "{report:?}");
    // The peer holds an object, unless the clients deleted every key
    // last.
    let present = report.history.iter().any(|operation| {
        operation.process == "verifier" && matches!(operation.outcome, Outcome::Read(Some(_)))
    });
    assert!(report.peer.objects > 0 || !present, "{report:?}");
}

/// Under link faults, source crashes and takeovers, and peer restarts,
/// every committed change reaches the peer, each write once, and no key
/// is recorded flushed before the peer applied it.
#[test]
fn every_committed_change_reaches_the_peer_under_faults() {
    Runner::with_cost(4, COST).run(|context| {
        let config = config(Backup::Asynchronous, false, PeerBug::None);
        let workload = workload(context, 40);
        let plan = faults(context, &config, &workload);
        let report = Cluster::with_services(config, services()).run(context, &workload, &plan)?;
        check(&report);
        Ok(())
    });
}

/// A seed with the peer and its faults replays exactly, on a cluster
/// whose shards have one member: what the peer does, and the native
/// transport, are deterministic too. A seed runs twice.
#[test]
fn a_seed_with_a_peer_replays_exactly() {
    let run = |seed| {
        let mut context = SimContext::new(seed);
        let config = ClusterConfig {
            nodes: 2,
            shards_per_bucket: 2,
            replicas: 1,
            every_member_durable: false,
            ..config(Backup::WriteThrough, true, PeerBug::None)
        };
        let workload = Workload {
            clients: 2,
            operations: 20,
            ..Workload::default()
        };
        let plan = faults(&mut context, &config, &workload);
        Cluster::new(config)
            .run(&mut context, &workload, &plan)
            .unwrap()
    };
    Runner::with_cost(1, 2 * COST).run(|context| {
        assert_eq!(run(context.seed()), run(context.seed()));
        Ok(())
    });
}

/// With `ack_policy` and `backup_ack` both `write_through`, no write is
/// acknowledged before the peer applied it, under the same faults.
#[test]
fn write_through_writes_are_acknowledged_once_the_peer_applied_them() {
    Runner::with_cost(4, COST).run(|context| {
        let config = config(Backup::WriteThrough, true, PeerBug::None);
        let workload = workload(context, 40);
        let plan = faults(context, &config, &workload);
        let report = Cluster::with_services(config, services()).run(context, &workload, &plan)?;
        check(&report);
        assert!(report.remote_checked > 0, "{report:?}");
        Ok(())
    });
}

/// The seeded bug of recording `FLUSHED` once the `COMMIT` is sent: the
/// audit of each `FLUSHED` finds an identity the peer never applied.
#[test]
fn the_audit_catches_a_key_recorded_flushed_before_applied() {
    Runner::with_cost(2, COST).run(|context| {
        let config = config(Backup::Asynchronous, false, PeerBug::FlushedOnCommit);
        let workload = workload(context, 20);
        let run =
            Cluster::with_services(config, services()).run(context, &workload, &FaultPlan::none());
        match run {
            Err(RunError::Check(violation)) => {
                assert!(
                    violation.reason.contains("the peer never applied"),
                    "{violation}"
                );
                Ok(())
            }
            other => Err(format!("the bug went unnoticed: {other:?}").into()),
        }
    });
}

/// The seeded bug of answering write-through writes once their `COMMIT`
/// is sent: the clients have their answers before the `COMMIT` reaches the
/// peer, further away than the nodes.
#[test]
fn the_audit_catches_writes_acknowledged_before_applied() {
    Runner::with_cost(2, COST).run(|context| {
        let config = config(Backup::WriteThrough, true, PeerBug::AnsweredOnCommit);
        let workload = workload(context, 20);
        let run =
            Cluster::with_services(config, services()).run(context, &workload, &FaultPlan::none());
        match run {
            Err(RunError::Check(violation)) => {
                assert!(violation.key.starts_with("bucket-"), "{violation}");
                assert!(violation.reason.contains("is lost"), "{violation}");
                Ok(())
            }
            other => Err(format!("the bug went unnoticed: {other:?}").into()),
        }
    });
}
