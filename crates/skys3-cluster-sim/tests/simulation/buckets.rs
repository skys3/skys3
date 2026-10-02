//! Automatic bucket and shard creation (plan M3-04, design §4.1, §6.1):
//! each bucket is created through a different node's gateway, which
//! places its shards on the registered nodes, and clients then read and
//! write through every node's gateway.

use std::time::Duration;

use skys3_cluster_sim::{
    Cluster, ClusterConfig, CoordinatedServices, CoordinationConfig, FaultPlan, FaultProfile,
    FaultRates, ReplicatedServices, RoutedServices, View, Workload,
};
use skys3_config::FailureDomain;
use skys3_gateway::routing::RoutingConfig;
use skys3_sim::history::Outcome;
use skys3_sim::{Runner, SimContext};

use crate::COST;

type Services = CoordinatedServices<RoutedServices>;

/// Four nodes, so a shard of three replicas leaves one out, and two
/// buckets, one `write_back`, created through the S3 API.
fn config() -> ClusterConfig {
    ClusterConfig {
        nodes: 4,
        replicas: 3,
        buckets: 2,
        write_back_buckets: 1,
        every_member_durable: true,
        create_buckets: true,
        ..ClusterConfig::default()
    }
}

/// Replicated, routed services under a coordinator that completes
/// creations cut short.
fn services() -> Services {
    let routing = RoutingConfig {
        connect_timeout: Duration::from_millis(300),
        request_timeout: Duration::from_secs(4),
        register_interval: Duration::from_millis(200),
        ..RoutingConfig::default()
    };
    let routed = RoutedServices::new(ReplicatedServices::default(), routing).fresh();
    let coordination = CoordinationConfig {
        bucket_shards: Some(FailureDomain::Node),
        ..CoordinationConfig::default()
    };
    CoordinatedServices::new(routed, coordination)
}

/// Clients that send each request to any node and wait out a gateway's
/// forwarding timeout.
fn workload(context: &SimContext) -> Workload {
    Workload {
        any_gateway: true,
        timeout: Duration::from_secs(6),
        operations: 40 * context.scale() as usize,
        ..Workload::default()
    }
}

/// A cluster whose commits and served requests are audited after every
/// step.
fn cluster(config: ClusterConfig, services: &Services) -> Cluster<Services> {
    Cluster::with_services(config, services.clone()).invariant(|view: &View<'_, Services>| {
        view.services.inner().replicated().check_commits()?;
        view.services.inner().check_served()
    })
}

/// Without faults, every write through every gateway succeeds on the
/// created buckets, and requests reach primaries on other nodes.
#[test]
fn buckets_created_through_a_gateway_serve_reads_and_writes_on_every_node() {
    Runner::with_cost(2, COST).run(|context| {
        let config = ClusterConfig {
            control_rates: FaultRates::default(),
            ..config()
        };
        let services = services();
        let report =
            cluster(config, &services).run(context, &workload(context), &FaultPlan::none())?;
        // No write fails (a failed write is recorded as unknown). A read
        // may, when its object is replaced between its entry and its
        // payload.
        assert_eq!(report.count(|o| *o == Outcome::Unknown), 0);
        let failed = report.count(|o| *o == Outcome::Failed);
        let done = report.count(|o| *o == Outcome::Done);
        assert!(done > 0);
        assert!(
            failed * 20 <= done,
            "{failed} reads failed, {done} writes done"
        );
        let (served, stats) = services.inner().stats();
        assert!(served > 0);
        assert!(stats.forwarded > 0, "{stats:?}");
        assert!(report.flushed > 0, "{report:?}");
        Ok(())
    });
}

/// Creation through the faults of the control store (lost answers,
/// conflicts, outages), and the workload under crashes and message loss:
/// the history stays linearizable and every acknowledged write durable.
#[test]
fn buckets_created_under_control_store_faults_survive_crashes() {
    Runner::with_cost(3, COST / 2).run(|context| {
        let config = config();
        let workload = workload(context);
        let profile = FaultProfile {
            end: Duration::from_secs(8) * context.scale(),
            crashes: 2,
            message_loss: 1,
            ..FaultProfile::default()
        };
        let plan = FaultPlan::random(
            context.rng(),
            &profile,
            config.nodes,
            config.disks_per_node,
            workload.clients,
        );
        let services = services();
        let report = cluster(config, &services).run(context, &workload, &plan)?;
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        Ok(())
    });
}
