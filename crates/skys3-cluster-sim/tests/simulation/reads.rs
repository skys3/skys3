//! Read plans and holder fetch (plan M2-18): clients `GET` through any
//! node, whose gateway asks the shard's primary for a read plan and reads
//! the bytes from a holder it names, its own node first, while writes
//! replace the keys and the clean cache evicts them. Holders register each
//! read after a delay, so writes and evictions land between plans and
//! their registrations. No `GET` returns bytes of another version than
//! its headers name, and the history stays linearizable. With
//! registrations that lapse before a stream ends, `GET`s fail mid-stream,
//! and no holder serves a fetch under a lapsed registration.

use std::time::Duration;

use skys3_cluster_sim::{
    Cluster, ClusterConfig, FaultPlan, FaultProfile, FaultRates, HolderFaults, ReadRegistration,
    ReplicatedServices, RoutedServices, RunError, View, Workload,
};
use skys3_gateway::routing::RoutingConfig;
use skys3_shard::CacheSettings;
use skys3_sim::{Runner, SimContext};

use crate::COST;

/// Four nodes, every shard on three of them, a `write_back` bucket whose
/// clean copies stay on two members, and caches of a few bodies, so
/// flushes and evictions keep changing which replica holds what.
fn config() -> ClusterConfig {
    ClusterConfig {
        nodes: 4,
        replicas: 3,
        write_back_buckets: 1,
        every_member_durable: true,
        clean_copies: 2,
        clean_cache: Some(CacheSettings {
            max_bytes: 4096,
            reserve_fraction: 0.1,
        }),
        control_rates: FaultRates::default(),
        ..ClusterConfig::default()
    }
}

/// Clients that send each request to any node, on few keys, with bodies
/// of up to four extents.
fn workload(context: &SimContext) -> Workload {
    Workload {
        keys: 3,
        operations: 40 * context.scale() as usize,
        any_gateway: true,
        timeout: Duration::from_secs(6),
        ..Workload::default()
    }
}

/// Holders that register each read up to 200 ms after its plan.
fn racing() -> HolderFaults {
    HolderFaults {
        register_delay: Duration::from_millis(200),
        ..HolderFaults::default()
    }
}

fn services(holders: HolderFaults) -> RoutedServices {
    let routing = RoutingConfig {
        connect_timeout: Duration::from_millis(300),
        request_timeout: Duration::from_secs(4),
        register_interval: Duration::from_millis(200),
        ..RoutingConfig::default()
    };
    RoutedServices::new(ReplicatedServices::default().with_holders(holders), routing).fresh()
}

/// A cluster whose commits, served requests, and holders are audited
/// after every step.
fn cluster(config: ClusterConfig, services: &RoutedServices) -> Cluster<RoutedServices> {
    Cluster::with_services(config, services.clone()).invariant(|view: &View<'_, RoutedServices>| {
        let replicated = view.services.replicated();
        replicated.check_commits()?;
        replicated.check_holders()?;
        view.services.check_served()
    })
}

#[test]
fn reads_racing_overwrites_and_evictions_return_the_planned_version() {
    let mut counts = Vec::new();
    Runner::with_cost(2, COST).run(|context| {
        let services = services(racing());
        let report =
            cluster(config(), &services).run(context, &workload(context), &FaultPlan::none())?;
        assert!(report.flushed > 0, "{report:?}");
        counts.push(services.replicated().holder_counts());
        Ok(())
    });
    // Members serve reads, and some reads were of versions their holder
    // had already replaced or evicted when it registered them.
    let members: u64 = counts.iter().map(|c| c.by_member).sum();
    let superseded: u64 = counts.iter().map(|c| c.superseded).sum();
    assert!(members > 0, "{counts:?}");
    assert!(superseded > 0, "{counts:?}");
}

#[test]
fn reads_under_crashes_and_message_loss_return_the_planned_version() {
    Runner::with_cost(2, COST).run(|context| {
        let config = config();
        let workload = workload(context);
        let profile = FaultProfile {
            end: Duration::from_secs(8) * context.scale(),
            crashes: 2,
            message_loss: 2,
            ..FaultProfile::default()
        };
        let plan = FaultPlan::random(
            context.rng(),
            &profile,
            config.nodes,
            config.disks_per_node,
            workload.clients,
        );
        cluster(config, &services(racing())).run(context, &workload, &plan)?;
        Ok(())
    });
}

/// Registrations that last 100 ms, which gateways renew only every
/// second, and holders that take up to 80 ms per fetch: a `GET` of
/// several extents outlives its registration.
fn lapsing() -> (ClusterConfig, HolderFaults) {
    let config = ClusterConfig {
        read_registration: ReadRegistration {
            ttl: Duration::from_millis(100),
            renew_every: Duration::from_secs(1),
            ..ReadRegistration::default()
        },
        ..config()
    };
    let holders = HolderFaults {
        fetch_delay: Duration::from_millis(80),
        ..HolderFaults::default()
    };
    (config, holders)
}

#[test]
fn a_lapsed_registration_fails_the_get_mid_stream() {
    Runner::with_cost(2, COST).run(|context| {
        let (config, holders) = lapsing();
        let services = services(holders);
        let report =
            cluster(config, &services).run(context, &workload(context), &FaultPlan::none())?;
        let counts = services.replicated().holder_counts();
        assert!(counts.lapsed > 0, "{counts:?}");
        assert!(report.broken_reads > 0, "{counts:?}");
        Ok(())
    });
}

/// The seeded bug: a holder registers the copy it holds now, whatever
/// version the plan named, so a read racing an overwrite gets the new
/// bytes under the old version's headers. Bodies are no longer than their
/// prefix, so a later write is seldom shorter than the version it replaces
/// and the gateway streams the planned size of its bytes rather than
/// finding them short; and holders register up to half a second after the
/// plan.
#[test]
fn a_holder_that_ignores_the_version_is_caught() {
    Runner::with_cost(1, COST).run(|context| {
        let holders = HolderFaults {
            ignore_version: true,
            register_delay: Duration::from_millis(500),
            ..HolderFaults::default()
        };
        let workload = Workload {
            max_body: 0,
            ..workload(context)
        };
        let outcome =
            cluster(config(), &services(holders)).run(context, &workload, &FaultPlan::none());
        match outcome {
            Err(RunError::Simulation(error)) => {
                assert!(error.contains("does not match"), "{error}");
                eprintln!("seed {}: caught: {error}", context.seed());
                Ok(())
            }
            Err(RunError::Check(error)) => {
                eprintln!("seed {}: caught by the check: {error:?}", context.seed());
                Ok(())
            }
            other => Err(format!("a holder ignoring the version went unnoticed: {other:?}").into()),
        }
    });
}

/// The seeded bug: a holder serves fetches under registrations that
/// lapsed, whose payload compaction may have reclaimed.
#[test]
fn a_holder_that_serves_lapsed_registrations_is_caught() {
    Runner::with_cost(1, COST).run(|context| {
        let (config, faults) = lapsing();
        let holders = HolderFaults {
            ignore_lapse: true,
            ..faults
        };
        let outcome = cluster(config, &services(holders)).run(
            context,
            &workload(context),
            &FaultPlan::none(),
        );
        match outcome {
            Err(RunError::Simulation(error)) => {
                assert!(error.contains("had lapsed"), "{error}");
                eprintln!("seed {}: caught: {error}", context.seed());
                Ok(())
            }
            other => Err(format!("a holder serving lapsed reads went unnoticed: {other:?}").into()),
        }
    });
}
