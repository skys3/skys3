//! Read-only origin buckets (plan M4-12, design §9.5): two `read_only`
//! buckets over one origin, read with two credential scopes, while an
//! out-of-band writer changes the origin's objects and refuses keys to one
//! scope, and clients read through every node's gateway. Every answer must
//! be one the origin gave, as the reader's credentials see it, during the
//! read under `revalidate`, and at most the TTL before it under `ttl`.
//! Seeded bugs show that the check catches a read served from the cache
//! without asking the origin, and answers kept without their credential
//! scope. See `skys3_cluster_sim::origin` (the `Origin` configuration).

use std::time::Duration;

use skys3_cluster_sim::{
    Cluster, ClusterConfig, FaultPlan, FaultProfile, FaultRates, Origin, OriginBug,
    OriginFreshness, ReplicatedServices, Report, RoutedServices, RunError, Workload,
};
use skys3_gateway::routing::RoutingConfig;
use skys3_shard::CacheSettings;
use skys3_sim::{Runner, SimContext};

use crate::COST;

fn services() -> RoutedServices {
    let routing = RoutingConfig {
        connect_timeout: Duration::from_millis(300),
        request_timeout: Duration::from_secs(4),
        register_interval: Duration::from_millis(200),
        ..RoutingConfig::default()
    };
    RoutedServices::new(ReplicatedServices::default(), routing).fresh()
}

/// Three nodes, every shard on all of them, and only the origin's two
/// buckets, with a clean cache small enough to evict what fills keep.
fn config(origin: Origin) -> ClusterConfig {
    ClusterConfig {
        nodes: 3,
        replicas: 3,
        buckets: 0,
        shards_per_bucket: 2,
        control_rates: FaultRates::default(),
        clean_cache: Some(CacheSettings {
            max_bytes: 8 * 1024,
            reserve_fraction: 0.1,
        }),
        origin: Some(origin),
        ..ClusterConfig::default()
    }
}

/// No workload of the cluster's own buckets: the origin's clients are the
/// run.
fn workload() -> Workload {
    Workload {
        clients: 0,
        ..Workload::default()
    }
}

fn run(context: &mut SimContext, origin: Origin, plan: &FaultPlan) -> Result<Report, RunError> {
    Cluster::with_services(config(origin), services()).run(context, &workload(), plan)
}

fn ttl() -> Origin {
    Origin {
        freshness: OriginFreshness::Ttl(1),
        ..Origin::default()
    }
}

#[test]
fn under_revalidate_every_get_sees_the_origin_as_it_is() {
    let mut denied = 0;
    Runner::with_cost(2, COST).run(|context| {
        let report = run(context, Origin::default(), &FaultPlan::none())?;
        let audit = report.origin;
        eprintln!("seed {}: {audit:?}", context.seed());
        if audit.served == 0 || audit.missing + audit.denied == 0 {
            return Err(format!("the reads saw too little: {audit:?}").into());
        }
        // Nothing a read served was out of date when the read began.
        if audit.stale != 0 {
            return Err(format!("stale reads under revalidate: {audit:?}").into());
        }
        denied += audit.denied;
        Ok(())
    });
    assert!(denied > 0, "no read was refused");
}

#[test]
fn under_a_ttl_reads_are_at_most_the_ttl_stale() {
    let mut stale = 0;
    Runner::with_cost(2, COST).run(|context| {
        let report = run(context, ttl(), &FaultPlan::none())?;
        let audit = report.origin;
        eprintln!("seed {}: {audit:?}", context.seed());
        if audit.served == 0 {
            return Err(format!("no read was served: {audit:?}").into());
        }
        stale += audit.stale;
        Ok(())
    });
    // The TTL was used: some reads served what the origin had replaced.
    assert!(stale > 0, "no read used the TTL");
}

#[test]
fn a_seed_of_origin_reads_replays_exactly() {
    Runner::with_cost(1, 2 * COST).run(|context| {
        let mut replay = SimContext::with_scale(context.seed(), context.scale());
        let first = run(&mut replay, ttl(), &FaultPlan::none())?.origin;
        let second = run(context, ttl(), &FaultPlan::none())?.origin;
        if first != second {
            return Err(format!("two runs differ: {first:?} and {second:?}").into());
        }
        Ok(())
    });
}

#[test]
fn origin_reads_under_crashes_and_message_loss() {
    Runner::with_cost(2, COST).run(|context| {
        let origin = if context.seed() % 2 == 0 {
            Origin::default()
        } else {
            ttl()
        };
        let profile = FaultProfile {
            end: Duration::from_secs(8),
            crashes: 2,
            message_loss: 2,
            ..FaultProfile::default()
        };
        let plan = FaultPlan::random(context.rng(), &profile, 3, 2, 0);
        let report = run(context, origin, &plan)?;
        eprintln!("seed {}: {:?}", context.seed(), report.origin);
        Ok(())
    });
}

/// Runs `origin` clean, then with `bug`, which the audit must catch.
fn caught(context: &mut SimContext, origin: Origin, bug: OriginBug) -> turmoil::Result {
    let mut replay = SimContext::with_scale(context.seed(), context.scale());
    run(&mut replay, origin.clone(), &FaultPlan::none())
        .map_err(|error| format!("without the bug: {error}"))?;
    let bugged = Origin {
        bug: Some(bug),
        ..origin
    };
    match run(context, bugged, &FaultPlan::none()) {
        Err(RunError::Simulation(error)) if error.contains("which the origin never gave") => {
            eprintln!("seed {}: {bug:?} caught: {error}", context.seed());
            Ok(())
        }
        Err(error) => Err(format!("{bug:?} caught for another reason: {error}").into()),
        Ok(report) => Err(format!("{bug:?} went unnoticed: {:?}", report.origin).into()),
    }
}

#[test]
fn serving_a_cached_copy_without_revalidating_is_caught() {
    Runner::with_cost(2, 2 * COST)
        .run(|context| caught(context, Origin::default(), OriginBug::SkipRevalidation));
}

#[test]
fn answers_kept_without_their_credential_scope_are_caught() {
    Runner::with_cost(2, 2 * COST)
        .run(|context| caught(context, ttl(), OriginBug::IgnoreCredentials));
}
