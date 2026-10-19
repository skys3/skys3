//! The clean cache (plan M1-21): replicated `write_back` buckets whose
//! nodes keep a cache smaller than the workload. Members beyond
//! `clean_copies` drop their clean copies as soon as a flush lands,
//! primaries evict the least recently used, and reads fill evicted keys
//! again. Every acknowledged write must survive on every member as a dirty
//! copy that still holds its bytes, or at the remote.

use std::time::Duration;

use skys3_cluster_sim::{
    Cluster, ClusterConfig, FaultPlan, FaultProfile, FaultRates, ReplicatedServices, View, Workload,
};
use skys3_shard::CacheSettings;
use skys3_sim::history::Outcome;
use skys3_sim::{Runner, SimContext};

use crate::COST;

/// Three nodes, every shard on all of them and of a `write_back` bucket,
/// and a cache of a few bodies on each node.
fn config() -> ClusterConfig {
    ClusterConfig {
        replicas: 3,
        every_member_durable: true,
        write_back_buckets: 2,
        clean_cache: Some(CacheSettings {
            max_bytes: 4096,
            reserve_fraction: 0.1,
        }),
        ..ClusterConfig::default()
    }
}

/// More keys than the caches hold, and bodies up to the size of one.
fn workload(context: &SimContext) -> Workload {
    Workload {
        keys: 12,
        operations: 40 * context.scale() as usize,
        ..Workload::default()
    }
}

fn cluster(config: ClusterConfig, services: &ReplicatedServices) -> Cluster<ReplicatedServices> {
    Cluster::with_services(config, services.clone())
        .invariant(|view: &View<'_, ReplicatedServices>| view.services.check_commits())
}

#[test]
fn a_workload_larger_than_the_cache_refills_what_it_evicts() {
    let mut evicted = 0;
    Runner::with_cost(2, COST).run(|context| {
        let config = ClusterConfig {
            control_rates: FaultRates::default(),
            ..config()
        };
        let services = ReplicatedServices::default();
        let report =
            cluster(config, &services).run(context, &workload(context), &FaultPlan::none())?;
        // Every read is answered: evicted keys are filled again.
        assert_eq!(report.count(|o| *o == Outcome::Failed), 0);
        assert_eq!(report.count(|o| *o == Outcome::Unknown), 0);
        assert!(report.flushed > 0);
        evicted += report.evicted;
        Ok(())
    });
    assert!(evicted > 0, "nothing was evicted");
}

#[test]
fn eviction_keeps_every_dirty_byte_under_crashes() {
    Runner::with_cost(4, COST).run(|context| {
        let config = config();
        let workload = workload(context);
        let profile = FaultProfile {
            end: Duration::from_secs(8) * context.scale(),
            crashes: 3,
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
        let services = ReplicatedServices::default();
        let report = cluster(config, &services).run(context, &workload, &plan)?;
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        Ok(())
    });
}
