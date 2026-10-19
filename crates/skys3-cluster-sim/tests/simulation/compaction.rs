//! Segment compaction (plan M1-22) on replicated shards: every node
//! reclaims its segments after each checkpoint interval while the workload
//! overwrites a few keys, under crashes, message loss, and power losses at
//! sync boundaries, some of them inside a compaction pass. Every
//! acknowledged write must survive on every member, as a dirty copy whose
//! bytes are in a segment the node still has, or at the remote; and every
//! node's log must hold each shard's latest `CONFIG` record.

use std::time::Duration;

use rand::Rng;
use skys3_cluster_sim::{
    Cluster, ClusterConfig, FaultPlan, FaultProfile, FaultRates, ReadRegistration,
    ReplicatedServices, RunError, View, Workload,
};
use skys3_io::SyncCut;
use skys3_shard::{CacheSettings, CompactionSettings};
use skys3_sim::history::Outcome;
use skys3_sim::{Runner, SimContext};

use crate::COST;

/// Three nodes, every shard on all of them, a `write_back` bucket beside a
/// `local` one, a small clean cache, and compaction with a short TTL for
/// extents that nothing names and a short release delay for payload.
///
/// An overwritten body's `EXTENT` records wait out the TTL from their
/// segment's seal and then the release delay from the first pass after
/// it, so a pass every checkpoint interval (1 s) reclaims their segment
/// about TTL + delay + 1 to 2 s after it sealed. The workload's writes end
/// about 4 s in, so both are 1 s: with 2 s each, no segment was reclaimed
/// in most runs whose shards never stalled.
fn config() -> ClusterConfig {
    ClusterConfig {
        replicas: 3,
        every_member_durable: true,
        write_back_buckets: 1,
        clean_cache: Some(CacheSettings {
            max_bytes: 8192,
            reserve_fraction: 0.1,
        }),
        compaction: Some(CompactionSettings {
            live_threshold: 0.6,
            unreferenced_ttl: Duration::from_secs(1),
        }),
        read_registration: ReadRegistration {
            release_delay: Duration::from_secs(1),
            ..ReadRegistration::default()
        },
        ..ClusterConfig::default()
    }
}

/// Few keys and many writes, so segments fill with overwritten versions.
fn workload(context: &SimContext) -> Workload {
    Workload {
        keys: 8,
        operations: 60 * context.scale() as usize,
        think_time: Duration::from_millis(100),
        ..Workload::default()
    }
}

fn cluster(config: ClusterConfig, services: &ReplicatedServices) -> Cluster<ReplicatedServices> {
    Cluster::with_services(config, services.clone())
        .invariant(|view: &View<'_, ReplicatedServices>| view.services.check_commits())
}

#[test]
fn replicated_compaction_loses_nothing_under_crashes() {
    let mut compacted = 0;
    Runner::with_cost(4, COST).run(|context| {
        let config = config();
        let workload = workload(context);
        let profile = FaultProfile {
            end: Duration::from_secs(8) * context.scale(),
            crashes: 3,
            message_loss: 2,
            partitions: 1,
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
        compacted += report.compacted;
        Ok(())
    });
    assert!(compacted > 0, "no segment was reclaimed");
}

/// Power losses of the first node at sync boundaries drawn from the second
/// half of a run, where most compaction passes are: each run replays the
/// seed, which replays exactly, and cuts the power at one of them.
#[test]
fn replicated_compaction_survives_power_loss_at_its_syncs() {
    const CUTS: usize = 4;
    let mut compacted = 0;
    Runner::with_cost(2, COST * CUTS as u64).run(|context| {
        let (seed, scale) = (context.seed(), context.scale());
        let fresh = || SimContext::with_scale(seed, scale);
        let config = ClusterConfig {
            control_rates: FaultRates::default(),
            ..config()
        };
        let workload = workload(context);
        let base = cluster(config.clone(), &ReplicatedServices::default()).run(
            &mut fresh(),
            &workload,
            &FaultPlan::none(),
        )?;
        compacted += base.compacted;
        let syncs = base.syncs[0];
        for _ in 0..CUTS {
            let sync = context.rng().random_range(syncs / 2..syncs);
            let cut = if context.rng().random_bool(0.5) {
                SyncCut::Before
            } else {
                SyncCut::After
            };
            let at = format!("power loss {cut:?} sync {sync} of {syncs}");
            let report = cluster(config.clone(), &ReplicatedServices::default())
                .power_loss_at_sync(0, sync, cut)
                .run(&mut fresh(), &workload, &FaultPlan::none())
                .map_err(|error| RunError::Simulation(format!("{at}: {error}")))?;
            assert_eq!(report.power_cuts, 1, "{at}");
            compacted += report.compacted;
        }
        Ok(())
    });
    assert!(compacted > 0, "no segment was reclaimed");
}
