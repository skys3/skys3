//! The coordinator lease and change propagation (plan M3-01, design §6.2,
//! §6.7): every node contends for the lease beside its data path, and the
//! coordinator moves shard registers of a bucket no gateway serves through
//! their epochs, as placement will. Every change is a compare-and-swap
//! announced by a new generation, and that generation is pushed to every
//! node.

use std::time::Duration;

use skys3_cluster_sim::{
    Cluster, ClusterConfig, CoordinatedServices, CoordinationConfig, FaultPlan, LocalServices,
    View, Workload,
};
use skys3_sim::history::Outcome;
use skys3_sim::{Runner, SimContext};

use crate::COST;

type Services = CoordinatedServices<LocalServices>;

/// Pushes reach a node well within the 500 ms the nodes poll at.
const PUSH_BOUND: Duration = Duration::from_millis(300);

/// How long a restarted node may take to serve pushes again.
const RESTART: Duration = Duration::from_secs(1);

fn workload(context: &SimContext) -> Workload {
    Workload {
        clients: 4,
        operations: 100 * context.scale() as usize,
        think_time: Duration::from_millis(150),
        ..Workload::default()
    }
}

/// The checks every coordinator scenario passes: no epoch written twice,
/// no two tenures at once, and every push delivered in time.
fn checked(services: &Services) -> Result<(), String> {
    services.check_changes()?;
    services.check_tenures()?;
    let pushes = services.check_pushes(PUSH_BOUND, RESTART)?;
    assert!(pushes.checked > 0, "no push was checked");
    assert!(pushes.slowest <= PUSH_BOUND);
    Ok(())
}

/// A node acts as coordinator throughout without the lease, beside the
/// node that holds it, and both change the same registers as fast as they
/// can: one of them loses whenever they race, and no change is lost or
/// overwritten unseen. The data path is unaffected.
#[test]
fn a_node_that_wrongly_believes_it_coordinates_only_competes() {
    let (mut races, mut changes) = (0, 0);
    Runner::with_cost(3, COST).run(|context| {
        let services = Services::new(
            LocalServices,
            CoordinationConfig {
                registers: 2,
                interval: Duration::from_millis(20),
                stale: Some(0),
                ..CoordinationConfig::default()
            },
        );
        let report = Cluster::with_services(ClusterConfig::default(), services.clone())
            .invariant(|view: &View<'_, Services>| view.services.observe(view))
            .invariant(|view: &View<'_, Services>| view.services.check_changes())
            .run(context, &workload(context), &FaultPlan::none())?;
        checked(&services)?;
        let made = services.changes();
        assert!(made.iter().any(|change| change.stale), "the stale node changed nothing");
        assert!(
            made.iter().any(|change| !change.stale),
            "the lease holder changed nothing"
        );
        assert!(services.announcements() > 0);
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        races += services.rejected();
        changes += made.len();
        Ok(())
    });
    eprintln!("{changes} changes made, {races} lost a race");
    assert!(races > 0, "the two coordinators never raced");
}

/// The coordinator is cut off from the control store while clients run.
/// Another node takes the lease once the old one has gone unchanged for
/// `coordinator_lease × (1+ρ)`, and placement resumes there. Clients see
/// nothing: no request touches the control store, and every one succeeds.
#[test]
fn coordinator_failover_delays_only_placement_work() {
    let mut pauses = Vec::new();
    Runner::with_cost(3, COST).run(|context| {
        let config = CoordinationConfig {
            isolate: Some((Duration::from_secs(2), Duration::from_secs(3))),
            ..CoordinationConfig::default()
        };
        let lease = config.lease;
        let interval = config.interval;
        // No control-store faults: the isolation is the only one.
        let cluster = ClusterConfig {
            control_rates: config.rates,
            ..ClusterConfig::default()
        };
        let services = Services::new(LocalServices, config);
        let report = Cluster::with_services(cluster, services.clone())
            .invariant(|view: &View<'_, Services>| view.services.observe(view))
            .run(context, &workload(context), &FaultPlan::none())?;
        checked(&services)?;

        let (isolated, at) = services
            .isolated()
            .ok_or("no node was coordinator when the isolation began")?;
        let after: Vec<_> = services
            .changes()
            .into_iter()
            .filter(|change| change.at > at)
            .collect();
        assert!(
            after.iter().any(|change| change.node != isolated),
            "no other node took over placement"
        );
        assert!(services.coordinators().len() >= 2);
        let pause = services.longest_pause(at.saturating_sub(interval * 2));
        let bound = lease.takeover_after() + lease.renew_interval() * 2 + interval * 3;
        assert!(pause <= bound, "placement paused for {pause:?}, over {bound:?}");
        pauses.push(pause);

        let failed = report.count(|o| matches!(o, Outcome::Failed | Outcome::Unknown));
        assert_eq!(failed, 0, "client requests failed during the failover");
        Ok(())
    });
    eprintln!("placement paused for at most {:?}", pauses.iter().max());
}

/// The tenure audit catches a node that takes over too early: a seeded bug
/// gives one node a lease a quarter as long as the others', so it takes
/// over while the holder still acts. Every change stays a compare-and-swap,
/// so the change audit still passes.
#[test]
fn the_checks_catch_a_node_that_takes_over_too_early() {
    let (mut caught, mut seeds) = (0, 0);
    Runner::with_cost(3, COST).run(|context| {
        seeds += 1;
        let services = Services::new(
            LocalServices,
            CoordinationConfig {
                hasty: Some(0),
                ..CoordinationConfig::default()
            },
        );
        Cluster::with_services(ClusterConfig::default(), services.clone())
            .invariant(|view: &View<'_, Services>| view.services.observe(view))
            .run(context, &workload(context), &FaultPlan::none())?;
        services.check_changes()?;
        if services.check_tenures().is_err() {
            caught += 1;
        }
        Ok(())
    });
    eprintln!("an early takeover: caught in {caught} of {seeds} seeds");
    assert!(caught > 0, "no early takeover was caught");
}
