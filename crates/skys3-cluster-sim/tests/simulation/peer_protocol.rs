//! The native peer protocol under its own faults (plan M6-08, design
//! §16.1): lost `DURABLE` and `APPLIED` messages and lost source messages,
//! reconnects in the middle of a transfer, duplicate `COMMIT`s, and
//! staging that expires before its `COMMIT` arrives.
//!
//! Each scenario runs three replicated nodes with takeovers, flushing a
//! `write_back` bucket and a `local` bucket's backups to a destination of
//! one node over real QUIC on the simulated network, under the crashes,
//! partitions, held links, and peer restarts of the peer scenarios. The
//! destination adds faults of the protocol ([`ProtocolFaults`]): it loses
//! messages either way, and drops connections as a `DATA` frame arrives,
//! after a `DURABLE`, and with an `APPLIED` in flight. Faults aimed at
//! `COMMIT`s hold their sender's link past the staging's TTL, or depose
//! their sender while its `COMMIT` waits, so that its successor sends the
//! same `COMMIT` too.
//!
//! Besides the M2-03 checkers and the peer audits (no key recorded flushed
//! before the destination applied it, every committed change applied there
//! once), the destination's taps check that no `DATA` resends what its
//! stream's `RESUME` reported durable, that every copy of a `COMMIT` whose
//! write the key holds is answered `committed` with its ETag, that a
//! `COMMIT` whose stream covered its object is never answered
//! `incomplete`, and that all staging left at the end expires with its
//! whole quota. Seeded bugs of the source show that the new audits catch
//! durable ranges sent again and expired staging taken for a commit.

use std::time::Duration;

use skys3_cluster_sim::{
    AimScope, AimedBlocks, Backup, Cluster, ClusterConfig, FaultPlan, FaultProfile, Peer, PeerBug,
    ProtocolFaults, ReplicatedServices, Report, RoutedServices, RunError, Workload,
};
use skys3_gateway::routing::RoutingConfig;
use skys3_shard::replication::ReplicationConfig;
use skys3_sim::history::Outcome;
use skys3_sim::{Runner, SimContext};

use crate::COST;

/// Three nodes, every shard on all of them, a `local` bucket backed up to
/// the peer and a `write_back` bucket whose target is the peer.
fn config(peer: Peer) -> ClusterConfig {
    ClusterConfig {
        replicas: 3,
        every_member_durable: true,
        buckets: 2,
        write_back_buckets: 1,
        backup: Backup::Asynchronous,
        peer: Some(peer),
        ..ClusterConfig::default()
    }
}

/// The peer with `faults`, and `bug` seeded into the flushers.
fn peer(faults: ProtocolFaults, bug: PeerBug) -> Peer {
    Peer {
        faults,
        bug,
        ..Peer::default()
    }
}

/// Clients that send each request to any node and wait out a takeover and
/// a write's flush. Bodies of up to 2 KiB take up to eight 256-byte
/// frames; those of one frame or less travel in batches.
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
/// that is down or cut off, with the timings of the peer scenarios.
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

/// The faults of the peer scenarios: crashes with and without power loss,
/// which move primaries and their flushes, partitions, message loss, a
/// control-store fault, and links to the peer cut or held, and restarts of
/// the peer.
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
    let present = report.history.iter().any(|operation| {
        operation.process == "verifier" && matches!(operation.outcome, Outcome::Read(Some(_)))
    });
    assert!(report.peer.objects > 0 || !present, "{report:?}");
}

/// Runs `config` under the peer scenarios' faults and checks the run.
fn run(context: &mut SimContext, config: ClusterConfig, operations: usize) -> Report {
    let workload = workload(context, operations);
    let plan = faults(context, &config, &workload);
    let report = Cluster::with_services(config, services())
        .run(context, &workload, &plan)
        .unwrap_or_else(|error| panic!("{error}"));
    check(&report);
    report
}

/// Lost messages either way, `DURABLE`s and `APPLIED`s, `BEGIN`s,
/// `DATA`, `COMMIT`s, and `BATCH`es, for staged objects and batches alike:
/// sources stage again what a lost frame left out, replay what a lost
/// answer left unknown, and every write applies once, answered alike each
/// time.
#[test]
fn lost_messages_are_retried_and_each_write_applies_once() {
    Runner::with_cost(4, COST).run(|context| {
        let faults = ProtocolFaults {
            loss_per_mille: 40,
            ..ProtocolFaults::default()
        };
        let report = run(context, config(peer(faults, PeerBug::None)), 40);
        let audit = &report.peer;
        assert!(audit.lost > 0, "{audit:?}");
        assert!(audit.replayed > 0, "{audit:?}");
        Ok(())
    });
}

/// Connections dropped as `DATA` arrives, between a `DURABLE` and the
/// `COMMIT`, and with an `APPLIED` in flight: each source reconnects,
/// resumes from the `RESUME`, and sends no byte it reported durable.
#[test]
fn reconnects_mid_transfer_resend_only_what_is_not_durable() {
    Runner::with_cost(4, COST).run(|context| {
        let faults = ProtocolFaults {
            drop_per_mille: 30,
            ..ProtocolFaults::default()
        };
        let report = run(context, config(peer(faults, PeerBug::None)), 40);
        let audit = &report.peer;
        assert!(audit.dropped > 0, "{audit:?}");
        assert!(audit.resumes > 0 && audit.resumed_bytes > 0, "{audit:?}");
        Ok(())
    });
}

/// `COMMIT`s replayed after their `APPLIED` was lost, and the same
/// `COMMIT` sent by a deposed primary, held on its way, and by its
/// successor: each applies once and every copy is answered alike.
#[test]
fn duplicate_commits_apply_once_and_are_answered_alike() {
    Runner::with_cost(4, COST).run(|context| {
        let peer = Peer {
            aimed: AimedBlocks {
                count: 3,
                hold: Duration::from_millis(2500),
                every: Duration::from_secs(6),
                scope: AimScope::Deposed {
                    isolated: Duration::from_secs(3),
                },
            },
            ..peer(
                ProtocolFaults {
                    loss_per_mille: 20,
                    ..ProtocolFaults::default()
                },
                PeerBug::None,
            )
        };
        let report = run(context, config(peer), 40);
        assert!(report.peer.replayed > 0, "{:?}", report.peer);
        Ok(())
    });
}

/// Staging with a short TTL, and `COMMIT`s held on their way past it:
/// the destination answers them `incomplete`, the sources stage again,
/// and nothing is recorded flushed that the destination did not apply.
/// Bodies abandoned by crashes expire too, and at the end every staging
/// left expires with its whole quota.
#[test]
fn staging_that_expires_before_its_commit_is_staged_again() {
    Runner::with_cost(4, COST).run(|context| {
        let report = run(context, config(expiring(PeerBug::None)), 40);
        assert!(
            report.peer.late > 0 && report.peer.incomplete + report.peer.expired > 0,
            "{:?}",
            report.peer
        );
        Ok(())
    });
}

/// A peer whose staging expires after 400 ms: one `COMMIT` in two
/// arrives a second late, and those of six flushes are held on their
/// sender's link for 1.2 s, each within the 3 s a source waits for an
/// answer.
fn expiring(bug: PeerBug) -> Peer {
    Peer {
        staging_ttl: Duration::from_millis(400),
        timeout: Duration::from_secs(3),
        aimed: AimedBlocks {
            count: 6,
            hold: Duration::from_millis(1200),
            every: Duration::from_secs(2),
            scope: AimScope::Sender,
        },
        ..peer(
            ProtocolFaults {
                late_per_mille: 500,
                late_by: Duration::from_secs(1),
                ..ProtocolFaults::default()
            },
            bug,
        )
    }
}

/// A seed with the protocol's faults, QUIC included, replays exactly, on a
/// cluster whose shards have one member: QUIC's draws come from the seed,
/// and TLS's own change bytes but not sizes. A seed runs twice.
#[test]
fn a_seed_with_protocol_faults_replays_exactly() {
    let run = |seed| {
        let mut context = SimContext::new(seed);
        let protocol = ProtocolFaults {
            loss_per_mille: 20,
            drop_per_mille: 20,
            late_per_mille: 100,
            late_by: Duration::from_secs(1),
        };
        let config = ClusterConfig {
            nodes: 2,
            shards_per_bucket: 2,
            replicas: 1,
            every_member_durable: false,
            ..config(expiring(PeerBug::None))
        };
        let config = ClusterConfig {
            peer: config.peer.map(|peer| Peer {
                faults: protocol,
                ..peer
            }),
            ..config
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

/// The seeded bug of resending every byte after a `RESUME`: a `DATA`
/// frame repeats bytes its stream's `RESUME` reported durable, as soon as
/// a streamed body or a dropped connection left some.
#[test]
fn the_audit_catches_durable_ranges_sent_again() {
    Runner::with_cost(2, COST).run(|context| {
        let faults = ProtocolFaults {
            drop_per_mille: 30,
            ..ProtocolFaults::default()
        };
        let config = config(peer(faults, PeerBug::ResendDurable));
        let workload = workload(context, 20);
        let run =
            Cluster::with_services(config, services()).run(context, &workload, &FaultPlan::none());
        match run {
            Err(RunError::Check(violation)) => {
                assert!(
                    violation.reason.contains("RESUME reported durable"),
                    "{violation}"
                );
                Ok(())
            }
            other => Err(format!("the bug went unnoticed: {other:?}").into()),
        }
    });
}

/// The seeded bug of taking expired staging for a commit: a key whose
/// `COMMIT` was answered `incomplete` is recorded flushed, which the
/// audit of each `FLUSHED` finds the destination never applied.
#[test]
fn the_audit_catches_expired_staging_taken_for_a_commit() {
    Runner::with_cost(2, COST).run(|context| {
        let config = config(expiring(PeerBug::ExpiredAsCommitted));
        let workload = workload(context, 30);
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
