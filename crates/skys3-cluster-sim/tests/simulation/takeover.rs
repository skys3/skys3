//! Primary takeover and reconciliation (plan M2-12, design §6.5, §6.6): a
//! member whose primary stays silent for `primary_grace` proposes itself
//! by a compare-and-swap of the shard's register, reconciles the other
//! members with its log, and serves; the old primary, once back, is
//! deposed and redirects to it.
//!
//! Clients send each request to any node, whose gateway follows redirects
//! and the register. After every step the audits check the three
//! invariants of §6.8: no record is committed, nor any write acknowledged,
//! before every member holds it, and one node acknowledges writes per
//! epoch (`check_commits`); no read is served once a member could have
//! taken over (`check_reads`); and only the current primary serves
//! (`check_served`). After every run, every key reads back what was
//! acknowledged, on every member of the final configuration.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use skys3_cluster_sim::{
    Cluster, ClusterConfig, Drift, Endpoint, Fault, FaultPlan, FaultProfile, ReplicatedServices,
    RoutedServices, View, Workload,
};
use skys3_gateway::routing::RoutingConfig;
use skys3_shard::replication::ReplicationConfig;
use skys3_sim::history::Outcome;
use skys3_sim::{Runner, SimContext};

use crate::COST;

/// Three nodes, every shard on all of them.
fn config() -> ClusterConfig {
    ClusterConfig {
        replicas: 3,
        every_member_durable: true,
        ..ClusterConfig::default()
    }
}

/// Timings shorter than the defaults, in the same order: a primary
/// suspects a member before the member's grace passes, so a primary that
/// reaches the control store removes its members before they could take
/// over (§6.4), and a member takes over from a primary that crashed, or
/// that is cut off from the control store too. Within the lease
/// inequality for `ρ` = 1%.
fn fast() -> ReplicationConfig {
    ReplicationConfig {
        lease_renew_interval: Duration::from_millis(200),
        primary_lease: Duration::from_millis(800),
        primary_grace: Duration::from_millis(1400),
        member_suspect_after: Duration::from_millis(700),
        ..ReplicationConfig::default()
    }
}

/// Clients that send each request to any node and wait out a takeover.
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

/// Routing over `replicated` with takeovers, whose gateways read the
/// register often and give up on a request within a client's timeout.
fn services(replicated: ReplicatedServices) -> RoutedServices {
    let routing = RoutingConfig {
        connect_timeout: Duration::from_millis(300),
        request_timeout: Duration::from_secs(4),
        register_interval: Duration::from_millis(200),
        ..RoutingConfig::default()
    };
    RoutedServices::new(replicated.with_takeover(), routing)
}

/// A cluster checked against the three invariants of §6.8 after every
/// step.
fn cluster(config: ClusterConfig, services: &RoutedServices) -> Cluster<RoutedServices> {
    Cluster::with_services(config, services.clone()).invariant(|view: &View<'_, RoutedServices>| {
        let replicated = view.services.replicated();
        replicated.check_commits()?;
        replicated.check_reads()?;
        view.services.check_served()
    })
}

/// What [`Takeovers`] saw over a run.
#[derive(Debug, Default)]
struct Seen {
    /// When node 0 first went down.
    crashed: Option<Duration>,
    /// When the most shards were served by a node other than their
    /// first primary, and how many.
    taken: Option<(Duration, usize)>,
}

/// An invariant that records when node 0 went down and when its shards
/// were served again by new primaries.
fn takeovers(
    seen: &Arc<Mutex<Seen>>,
) -> impl FnMut(&View<'_, RoutedServices>) -> Result<(), String> + 'static {
    let seen = Arc::clone(seen);
    move |view| {
        let mut seen = seen.lock().unwrap_or_else(PoisonError::into_inner);
        if !view.up[0] && seen.crashed.is_none() {
            seen.crashed = Some(view.elapsed);
        }
        let taken = view.services.replicated().taken_over().len();
        if taken > seen.taken.map_or(0, |(_, most)| most) {
            seen.taken = Some((view.elapsed, taken));
        }
        Ok(())
    }
}

fn crash(node: usize, power_loss: bool, downtime: Duration) -> Fault {
    Fault::Crash {
        node,
        power_loss,
        downtime,
    }
}

/// With the default timings, the shards of a primary that crashes are
/// served by new primaries in under 10 s (§13): its members' grace of
/// 6 s, the takeover delay, the compare-and-swap, and reconciliation.
#[test]
fn a_crashed_primary_fails_over_within_ten_seconds() {
    Runner::with_cost(2, COST).run(|context| {
        let services = services(ReplicatedServices::new(ReplicationConfig::default()));
        let seen = Arc::new(Mutex::new(Seen::default()));
        let at = Duration::from_millis(1000 + 100 * (context.seed() % 10));
        let plan = FaultPlan::none().with(
            at,
            crash(0, context.seed() % 2 == 0, Duration::from_secs(20)),
        );
        let report = cluster(config(), &services)
            .invariant(takeovers(&seen))
            .run(context, &workload(context, 40), &plan)?;
        let seen = seen.lock().unwrap_or_else(PoisonError::into_inner);
        let (crashed, (taken, shards)) = (seen.crashed.unwrap(), seen.taken.unwrap());
        let failover = taken - crashed;
        eprintln!("{shards} shards failed over in {failover:?}");
        let defaults = ReplicationConfig::default();
        assert!(failover >= defaults.primary_grace - defaults.lease_renew_interval);
        assert!(
            failover < Duration::from_secs(10),
            "failover took {failover:?}"
        );
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        Ok(())
    });
}

/// Every member of a crashed primary's shards proposes at about the same
/// time, with little delay to tell them apart, while the members are cut
/// off from each other for a while: the register picks one per shard,
/// the loser follows it, or is removed by the winner if it cannot reach
/// it. The old primary comes back deposed.
#[test]
fn competing_candidates_across_a_partition() {
    Runner::with_cost(3, COST).run(|context| {
        let timings = ReplicationConfig {
            takeover_delay: Duration::from_millis(5),
            ..fast()
        };
        let services = services(ReplicatedServices::new(timings));
        let mut plan = FaultPlan::none().with(
            Duration::from_millis(500),
            crash(0, false, Duration::from_secs(4)),
        );
        let partition = Fault::Partition {
            a: Endpoint::Node(1),
            b: Endpoint::Node(2),
            duration: Duration::from_millis(500 * (1 + context.seed() % 6)),
        };
        plan.push(Duration::from_millis(1600), partition);
        let seen = Arc::new(Mutex::new(Seen::default()));
        let report = cluster(config(), &services)
            .invariant(takeovers(&seen))
            .run(context, &workload(context, 30), &plan)?;
        let seen = seen.lock().unwrap_or_else(PoisonError::into_inner);
        assert!(seen.taken.is_some(), "no shard was taken over");
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        Ok(())
    });
}

/// The first new primary loses power while it takes over and reconciles,
/// at a point that varies with the seed: the last member takes over from
/// it in turn, and records it held past the new primary's log, or it
/// restarts and goes on.
#[test]
fn a_new_primary_crashes_while_it_reconciles() {
    Runner::with_cost(4, COST).run(|context| {
        let services = services(ReplicatedServices::new(fast()));
        let mut plan = FaultPlan::none().with(
            Duration::from_millis(500),
            crash(0, true, Duration::from_secs(6)),
        );
        // The members' grace passes about 1.9 s in; each proposes within
        // the takeover delay after.
        let offset = Duration::from_millis(1800 + 60 * (context.seed() % 12));
        let node = 1 + (context.seed() / 12) as usize % 2;
        plan.push(offset, crash(node, true, Duration::from_secs(2)));
        let seen = Arc::new(Mutex::new(Seen::default()));
        let report = cluster(config(), &services)
            .invariant(takeovers(&seen))
            .run(context, &workload(context, 30), &plan)?;
        let seen = seen.lock().unwrap_or_else(PoisonError::into_inner);
        assert!(seen.taken.is_some(), "no shard was taken over");
        assert!(report.lives > 3, "{}", report.lives);
        Ok(())
    });
}

/// Random crashes, partitions, control store faults, and message loss,
/// with every node's clock drifting within `ρ`: primaries are removed,
/// taken over, and deposed in every order, and the invariants hold.
#[test]
fn takeovers_under_random_faults_and_drift() {
    Runner::with_cost(4, COST / 2).run(|context| {
        let config = ClusterConfig {
            drift: Drift::from_ppm(10_000).unwrap(),
            ..config()
        };
        let workload = workload(context, 40);
        let profile = FaultProfile {
            end: Duration::from_secs(8) * context.scale(),
            crashes: 3,
            partitions: 3,
            message_loss: 1,
            control: 2,
            max_duration: Duration::from_secs(4),
            ..FaultProfile::default()
        };
        let plan = FaultPlan::random(
            context.rng(),
            &profile,
            config.nodes,
            config.disks_per_node,
            workload.clients,
        );
        let services = services(ReplicatedServices::new(fast()));
        let report = cluster(config, &services).run(context, &workload, &plan)?;
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        Ok(())
    });
}

/// A shard with two members: when its primary crashes for good, the
/// other member takes over alone, and serves what was committed.
#[test]
fn a_single_survivor_takes_over() {
    Runner::with_cost(2, COST).run(|context| {
        let config = ClusterConfig {
            nodes: 2,
            replicas: 2,
            ..config()
        };
        let services = services(ReplicatedServices::new(fast()));
        let seen = Arc::new(Mutex::new(Seen::default()));
        let plan = FaultPlan::none().with(
            Duration::from_millis(800),
            crash(0, true, Duration::from_secs(3600)),
        );
        let report = cluster(config, &services).invariant(takeovers(&seen)).run(
            context,
            &workload(context, 20),
            &plan,
        )?;
        let seen = seen.lock().unwrap_or_else(PoisonError::into_inner);
        assert!(seen.taken.is_some(), "no shard was taken over");
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        Ok(())
    });
}
