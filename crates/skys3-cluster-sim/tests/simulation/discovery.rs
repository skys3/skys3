//! Discovery and fallback (plan M6-07, design §7.8): a source cluster
//! whose buckets flush to a SkyS3 peer with `target_transport = "auto"`
//! reads the peer's signed descriptor through the peer's S3 gateway, uses
//! QUIC while a handshake succeeds within `peer_connect_timeout`, falls
//! back to S3 REST through that same gateway while UDP is blocked, and
//! returns to QUIC once it is allowed again.
//!
//! Each scenario runs three replicated nodes with takeovers and clients
//! that write throughout. UDP is blocked between every node and the peer,
//! twice, as partitions or held links that the S3 path does not cross,
//! while the nodes crash and take over each other's flushes and the peer
//! restarts. The audits (`ClusterConfig::peer`): every committed change
//! reaches the peer exactly once, over either transport, after a power
//! loss of the peer too; no native `COMMIT` reaches the peer after an S3
//! write of its key that was acknowledged after it was sent; the transport
//! metric and status of every live node follow each switch; every target
//! is back on QUIC once the faults healed; and no `COMMIT` reaches a peer
//! whose descriptor is forged or expired. Seeded bugs (an unverified
//! descriptor used, flushes over S3 without the quarantine) are caught.

use std::time::Duration;

use rand::Rng;
use skys3_cluster_sim::{
    AimScope, AimedBlocks, Backup, Cluster, ClusterConfig, DescriptorKind, Fault, FaultPlan,
    FaultProfile, Peer, PeerBug, ReplicatedServices, Report, RoutedServices, RunError, Workload,
};
use skys3_config::TargetTransport;
use skys3_gateway::routing::RoutingConfig;
use skys3_shard::replication::ReplicationConfig;
use skys3_sim::history::Outcome;
use skys3_sim::{Runner, SimContext};

use crate::COST;

/// A peer reached with `target_transport = "auto"`, whose streams time out
/// after a second, so that the quarantine (twice that and the peer's idle
/// timeout) is short enough for a run to flush over S3 REST.
fn peer(descriptor: DescriptorKind, bug: PeerBug) -> Peer {
    Peer {
        timeout: Duration::from_secs(1),
        idle: Duration::from_secs(3),
        transport: TargetTransport::Auto,
        descriptor,
        bug,
        ..Peer::default()
    }
}

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

/// Clients that send each request to any node, and write for longer than
/// UDP stays blocked.
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
/// that is down, with the timings of the peer scenarios.
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

/// UDP blocked between every node and the peer, `blocks` times within the
/// first `end`, for `min` to `max` each: each block cuts or holds every
/// link at once, as a firewall would.
fn block_udp(
    context: &mut SimContext,
    plan: &mut FaultPlan,
    blocks: usize,
    end: Duration,
    (min, max): (Duration, Duration),
) {
    for _ in 0..blocks {
        let rng = context.rng();
        let at = rng.random_range(Duration::from_millis(500)..=end);
        let duration = rng.random_range(min..=max);
        let hold = rng.random_bool(0.5);
        plan.push(at, Fault::UdpBlock { hold, duration });
    }
}

/// Crashes with and without power loss, which move primaries and their
/// flushes, partitions between nodes, message loss, a control-store fault,
/// restarts of the peer, and UDP blocked twice for longer than the
/// quarantine.
fn faults(context: &mut SimContext, config: &ClusterConfig, workload: &Workload) -> FaultPlan {
    let end = Duration::from_secs(6) * context.scale();
    let profile = FaultProfile {
        end,
        crashes: 2,
        partitions: 1,
        message_loss: 1,
        sync_failures: 0,
        control: 1,
        max_duration: Duration::from_secs(3),
        peer: 2,
        ..FaultProfile::default()
    };
    let mut plan = FaultPlan::random(
        context.rng(),
        &profile,
        config.nodes,
        config.disks_per_node,
        workload.clients,
    );
    // The blocks start early enough to heal well before the clients stop
    // writing, so that the flushers carry writes over QUIC again.
    let blocked = (Duration::from_secs(8), Duration::from_secs(12));
    block_udp(context, &mut plan, 2, end / 2, blocked);
    plan
}

/// What every run of a correct cluster shows.
fn check(report: &Report) {
    assert!(report.count(|o| *o == Outcome::Done) > 0);
    assert!(report.backups_drained, "{report:?}");
    // The peer holds an object, unless the clients deleted every key
    // last.
    let present = report.history.iter().any(|operation| {
        operation.process == "verifier" && matches!(operation.outcome, Outcome::Read(Some(_)))
    });
    assert!(report.peer.objects > 0 || !present, "{report:?}");
}

/// With UDP blocked mid-run, under takeovers, crashes, and peer restarts,
/// the flushers fall back to S3 REST through the peer's gateway and
/// return to QUIC once UDP is allowed again: every committed change
/// reaches the peer exactly once, no `COMMIT` outlives the quarantine, and
/// the transport metric and status follow every switch.
#[test]
fn with_udp_blocked_flushing_goes_on_over_s3_and_returns_to_quic() {
    // Writes for about 25 s, longer than the blocks and their aftermath,
    // and twice as long as a typical peer scenario.
    Runner::with_cost(4, 2 * COST).run(|context| {
        // Besides the blocks the plan draws, UDP blocked twice as a
        // `COMMIT` is on its way, for a while that ends within the peer's
        // idle timeout or after it.
        let hold = Duration::from_millis(context.rng().random_range(1000..=6000));
        let config = config(Peer {
            aimed: AimedBlocks {
                count: 2,
                hold,
                every: Duration::from_secs(8),
                scope: AimScope::Udp,
                late: false,
            },
            ..peer(DescriptorKind::Signed, PeerBug::None)
        });
        let workload = workload(context, 200);
        let plan = faults(context, &config, &workload);
        let report = Cluster::with_services(config, services()).run(context, &workload, &plan)?;
        check(&report);
        // Both transports carried writes, and the nodes switched.
        assert!(report.peer.commits > 0, "{:?}", report.peer);
        assert!(report.peer.s3_writes > 0, "{:?}", report.peer);
        assert!(
            report.peer.on_s3 > 0 && report.peer.on_quic > 0,
            "{:?}",
            report.peer
        );
        assert!(report.peer.switches >= 2, "{:?}", report.peer);
        Ok(())
    });
}

/// A seed with UDP blocked replays exactly: the discovery, the fallback,
/// and the peer's gateway are deterministic too. A seed runs twice.
#[test]
fn a_seed_with_udp_blocked_replays_exactly() {
    let run = |seed| {
        let mut context = SimContext::new(seed);
        let config = ClusterConfig {
            nodes: 2,
            shards_per_bucket: 2,
            replicas: 1,
            every_member_durable: false,
            ..config(peer(DescriptorKind::Signed, PeerBug::None))
        };
        let workload = Workload {
            clients: 2,
            operations: 120,
            think_time: Duration::from_millis(200),
            ..Workload::default()
        };
        let mut plan = FaultPlan::none();
        let blocked = (Duration::from_secs(8), Duration::from_secs(10));
        block_udp(&mut context, &mut plan, 1, Duration::from_secs(2), blocked);
        Cluster::new(config)
            .run(&mut context, &workload, &plan)
            .unwrap()
    };
    Runner::with_cost(1, 2 * COST).run(|context| {
        let first = run(context.seed());
        assert!(first.peer.s3_writes > 0, "{:?}", first.peer);
        assert_eq!(first, run(context.seed()));
        Ok(())
    });
}

/// A descriptor forged with a CA the source does not trust, or one that
/// expired, is refused: the target is flushed as a plain S3 store through
/// the peer's gateway, no `COMMIT` reaches the peer, and every committed
/// change still does, once.
#[test]
fn forged_and_expired_descriptors_are_refused() {
    Runner::with_cost(4, COST).run(|context| {
        let descriptor = if context.seed() % 2 == 0 {
            DescriptorKind::Forged
        } else {
            DescriptorKind::Expired
        };
        let config = config(peer(descriptor, PeerBug::None));
        let workload = workload(context, 40);
        let plan = faults(context, &config, &workload);
        let report = Cluster::with_services(config, services()).run(context, &workload, &plan)?;
        check(&report);
        assert_eq!(report.peer.commits, 0, "{:?}", report.peer);
        assert!(report.peer.s3_writes > 0, "{:?}", report.peer);
        assert_eq!((report.peer.on_quic, report.peer.on_s3), (0, 0));
        Ok(())
    });
}

/// The seeded bug of using a descriptor without verifying it: the nodes
/// flush over QUIC to a peer whose descriptor is forged.
#[test]
fn the_audit_catches_an_unverified_descriptor() {
    Runner::with_cost(2, COST).run(|context| {
        let config = config(peer(DescriptorKind::Forged, PeerBug::UnverifiedDescriptor));
        let workload = workload(context, 20);
        let run =
            Cluster::with_services(config, services()).run(context, &workload, &FaultPlan::none());
        match run {
            Err(RunError::Check(violation)) => {
                assert!(violation.reason.contains("could not verify"), "{violation}");
                Ok(())
            }
            other => Err(format!("the bug went unnoticed: {other:?}").into()),
        }
    });
}

/// The seeded bug of flushing over S3 REST at once, without the
/// quarantine: a `COMMIT` held while UDP is blocked reaches the peer after
/// the same key was written over S3.
#[test]
fn the_audit_catches_a_key_flushed_over_s3_while_its_commit_is_outstanding() {
    Runner::with_cost(2, 2 * COST).run(|context| {
        // UDP blocked as `COMMIT`s are on their way, for longer than it
        // takes to fall back but not than the peer's idle timeout. Each
        // `COMMIT` reached the peer, which applies it once the block
        // ends: over QUIC, a held `COMMIT` dies with the connection its
        // node closes as it falls back, unless another target keeps it.
        let peer = Peer {
            idle: Duration::from_secs(8),
            aimed: AimedBlocks {
                count: 3,
                hold: Duration::from_secs(5),
                every: Duration::from_secs(10),
                scope: AimScope::Udp,
                late: true,
            },
            ..peer(DescriptorKind::Signed, PeerBug::NoQuarantine)
        };
        let config = config(peer);
        let workload = workload(context, 160);
        let plan = FaultPlan::none();
        let run = Cluster::with_services(config, services()).run(context, &workload, &plan);
        match run {
            Err(RunError::Check(violation)) => {
                assert!(
                    violation
                        .reason
                        .contains("while its COMMIT was outstanding"),
                    "{violation}"
                );
                Ok(())
            }
            other => Err(format!("the bug went unnoticed: {other:?}").into()),
        }
    });
}
