//! Write-through buckets (plan M4-08, design §7.5): a write to a
//! `write_through` bucket is acknowledged only once the remote store holds
//! it, so losing the whole cluster after an acknowledgement loses no
//! acknowledged write.
//!
//! Each scenario runs replicated shards with takeovers, clients that send
//! each request to any node, and a slow and faulty remote store. The
//! audit (`ClusterConfig::write_through`) treats every node's disks as
//! destroyed twice: when a client records an acknowledged write of a
//! `write_back` bucket, the remote store alone must hold it or a write
//! that may have come after it; and once the clients are done, before any
//! fault heals, the same holds of every key.

use std::time::Duration;

use skys3_cluster_sim::{
    Cluster, ClusterConfig, FaultPlan, FaultProfile, ReplicatedServices, RoutedServices, RunError,
    Workload, WriteThrough,
};
use skys3_gateway::routing::RoutingConfig;
use skys3_shard::replication::ReplicationConfig;
use skys3_sim::history::Outcome;
use skys3_sim::s3::SimS3Faults;
use skys3_sim::{Runner, SimContext};

use crate::COST;

/// Three nodes, every shard on all of them, a `local` bucket and a
/// `write_back` one, and a remote store whose requests take 2 to 15 ms
/// each way: longer than a message between nodes, so a flush ends after
/// the local commit's answer would have reached its client.
fn config(write_through: WriteThrough, faulty: bool) -> ClusterConfig {
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
        write_through,
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
/// message loss, and control-store faults, while the remote store fails
/// some requests and loses some answers: no acknowledged write of the
/// `write_through` bucket is missing from the remote store, at its
/// acknowledgement or once the clients are done.
#[test]
fn losing_every_node_after_an_acknowledgement_loses_no_write() {
    Runner::with_cost(4, COST).run(|context| {
        let config = config(WriteThrough::On, true);
        let workload = workload(context, 40);
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
        let plan = FaultPlan::random(
            context.rng(),
            &profile,
            config.nodes,
            config.disks_per_node,
            workload.clients,
        );
        let report = Cluster::with_services(config, services()).run(context, &workload, &plan)?;
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        assert!(report.remote_checked > 0, "{report:?}");
        assert!(report.flushed > 0, "{report:?}");
        Ok(())
    });
}

/// The seeded bug: the gateways acknowledge every write after its local
/// commit, as `ack_policy = "local"` does. The audit finds an
/// acknowledged write the remote store does not hold yet.
#[test]
fn the_audit_catches_writes_acknowledged_after_their_local_commit() {
    Runner::with_cost(2, COST).run(|context| {
        let config = config(WriteThrough::AckedLocally, false);
        let outcome = Cluster::with_services(config, services()).run(
            context,
            &workload(context, 20),
            &FaultPlan::none(),
        );
        match outcome {
            Err(RunError::Check(violation)) => {
                assert!(violation.key.starts_with("bucket-1/"), "{violation}");
                assert!(violation.reason.contains("is lost"), "{violation}");
                Ok(())
            }
            other => Err(format!("the bug went unnoticed: {other:?}").into()),
        }
    });
}
