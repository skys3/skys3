//! The hot cache (plan M2-19): every node's gateway keeps the objects it
//! read from other nodes, and serves later `GET`s of the same version from
//! them. `GET`s of one hot object, sent to any node, are then served by
//! every node rather than by the shard's holders alone. Overwrites,
//! evictions, and compaction race the reads, with and without crashes and
//! message loss, and no `GET` returns bytes of another version than its
//! headers name. A cache that ignores the version is caught.

use std::collections::BTreeMap;
use std::time::Duration;

use skys3_cluster_sim::{
    Cluster, ClusterConfig, FaultPlan, FaultProfile, FaultRates, HolderFaults, HotCaches,
    ReadRegistration, ReplicatedServices, Report, RoutedServices, RunError, View, Workload,
};
use skys3_gateway::routing::RoutingConfig;
use skys3_shard::{CacheSettings, CompactionSettings};
use skys3_sim::history::Outcome;
use skys3_sim::{Runner, SimContext};
use skys3_types::NodeId;

use crate::COST;

/// Hot caches of 16 KiB: objects of up to 2 KiB, the workload's largest,
/// and a few of them per node, so the caches evict too.
const HOT_CACHE_BYTES: u64 = 16 * 1024;

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

/// Which node served each `GET` that got bytes: a holder's replicas, or a
/// gateway's hot cache.
fn served(report: &Report, services: &RoutedServices) -> BTreeMap<NodeId, u64> {
    let mut served = services.replicated().holder_reads();
    for (node, hits) in &report.hot_cache_hits {
        *served.entry(node.clone()).or_default() += hits;
    }
    served
}

/// Five nodes and one object, on one node only: a `local` bucket of one
/// shard with one replica.
fn one_holder(hot_cache_bytes: u64) -> ClusterConfig {
    ClusterConfig {
        nodes: 5,
        buckets: 1,
        shards_per_bucket: 1,
        replicas: 1,
        control_rates: FaultRates::default(),
        hot_cache: HotCaches {
            bytes: hot_cache_bytes,
            ..HotCaches::default()
        },
        ..ClusterConfig::default()
    }
}

/// Clients that send almost only `GET`s of one key, each to any node.
fn hot_reads(context: &SimContext) -> Workload {
    Workload {
        keys: 1,
        operations: 100 * context.scale() as usize,
        any_gateway: true,
        get_percent: 95,
        timeout: Duration::from_secs(6),
        ..Workload::default()
    }
}

#[test]
fn gets_of_a_hot_object_spread_across_gateways() {
    Runner::with_cost(2, COST).run(|context| {
        let workload = hot_reads(context);
        let mut shares = Vec::new();
        // The same seed twice: without hot caches, then with them.
        for bytes in [0, HOT_CACHE_BYTES] {
            let services = services(HolderFaults::default());
            let mut context = SimContext::with_scale(context.seed(), context.scale());
            let report = cluster(one_holder(bytes), &services).run(
                &mut context,
                &workload,
                &FaultPlan::none(),
            )?;
            let served = served(&report, &services);
            let gets: u64 = served.values().sum();
            let most = served.values().copied().max().unwrap_or_default();
            eprintln!(
                "seed {}, hot cache of {bytes} bytes: GETs served by {served:?}",
                context.seed()
            );
            assert!(gets > 0, "{served:?}");
            shares.push((served.len(), most as f64 / gets as f64));
        }
        // Without hot caches, the one holder serves every GET.
        assert_eq!(shares[0], (1, 1.0), "{shares:?}");
        // With them, every node serves some, and the holder a minority.
        let (nodes, busiest) = shares[1];
        assert_eq!(nodes, 5, "{shares:?}");
        assert!(busiest < 0.5, "{shares:?}");
        Ok(())
    });
}

/// Four nodes, every shard on three of them, a `write_back` bucket whose
/// clean copies stay on two members, small clean caches, compaction with
/// short delays, and hot caches: writes, flushes, evictions, and
/// compaction keep changing which node holds which version.
fn racing_config() -> ClusterConfig {
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
        compaction: Some(CompactionSettings {
            live_threshold: 0.6,
            unreferenced_ttl: Duration::from_secs(2),
        }),
        read_registration: ReadRegistration {
            release_delay: Duration::from_secs(2),
            ..ReadRegistration::default()
        },
        control_rates: FaultRates::default(),
        hot_cache: HotCaches {
            bytes: HOT_CACHE_BYTES,
            ..HotCaches::default()
        },
        ..ClusterConfig::default()
    }
}

/// Clients that send each request to any node, on few keys, half of the
/// operations `GET`s.
fn racing_workload(context: &SimContext) -> Workload {
    Workload {
        keys: 3,
        operations: 40 * context.scale() as usize,
        any_gateway: true,
        get_percent: 30,
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

#[test]
fn hot_caches_racing_overwrites_evictions_and_compaction_serve_the_planned_version() {
    let mut hits = 0;
    let mut compacted = 0;
    Runner::with_cost(2, COST).run(|context| {
        let services = services(racing());
        let report = cluster(racing_config(), &services).run(
            context,
            &racing_workload(context),
            &FaultPlan::none(),
        )?;
        assert!(report.flushed > 0, "{report:?}");
        hits += report.hot_cache_hits.values().sum::<u64>();
        compacted += report.compacted;
        Ok(())
    });
    assert!(hits > 0, "no GET was served from a hot cache");
    assert!(compacted > 0, "nothing was compacted");
}

#[test]
fn hot_caches_under_crashes_and_message_loss_serve_the_planned_version() {
    Runner::with_cost(2, COST).run(|context| {
        let config = racing_config();
        let workload = racing_workload(context);
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
        let report = cluster(config, &services(racing())).run(context, &workload, &plan)?;
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        Ok(())
    });
}

/// The seeded bug: a hot cache that serves whatever version of a key it
/// holds, so a `GET` after an overwrite gets the old bytes under the new
/// version's headers.
#[test]
fn a_hot_cache_that_ignores_the_version_is_caught() {
    Runner::with_cost(1, COST).run(|context| {
        let config = ClusterConfig {
            hot_cache: HotCaches {
                bytes: HOT_CACHE_BYTES,
                ignore_version: true,
            },
            ..racing_config()
        };
        let outcome = cluster(config, &services(HolderFaults::default())).run(
            context,
            &racing_workload(context),
            &FaultPlan::none(),
        );
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
            other => Err(format!("a hot cache ignoring the version went unnoticed: {other:?}").into()),
        }
    });
}
