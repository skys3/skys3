//! Backfill and re-admission (plan M2-15, design §6.4, §6.7): a node is
//! lost, its shards' primaries (or the members that take over from it)
//! remove it, and the coordinator adds a learner to each shard left with
//! fewer than `replicas` members (plan M3-05): a spare node outside the
//! shard if there is one, and otherwise the lost node itself, which
//! returns with whatever its log still holds. The learner gets a snapshot
//! of the primary's index or keeps a log the primary verifies, joins the
//! acknowledgement set, backfills its payload, and is promoted.
//!
//! After every step the commit audit and the R3 audit run, and the
//! durability audit samples how long each shard had fewer than `replicas`
//! copies: until new writes had them again, and until all data did. Both
//! times are reported (§16.3). At the end, every acknowledged write must
//! be durable on every member of the final configurations.

use std::time::Duration;

use skys3_cluster_sim::{DurabilityWindows, Fault, FaultPlan, FaultProfile, Workload};
use skys3_sim::Runner;
use skys3_sim::history::Outcome;

use crate::COST;
use crate::replacement::{check_windows, cluster, config, services, workload};

/// A node is lost for good while writes go on. Each shard it held gets
/// the spare node as a learner: new writes have three copies again within
/// seconds of the removal, and all data once the learner's backfill ends.
#[test]
fn a_lost_member_is_replaced_and_its_shards_regain_their_copies() {
    Runner::with_cost(3, COST).run(|context| {
        let crash = Fault::Crash {
            node: 0,
            power_loss: false,
            downtime: Duration::from_secs(60),
        };
        let plan = FaultPlan::none().with(Duration::from_secs(2), crash);
        let services = services();
        let report = cluster(config(4), &services).run(context, &workload(context), &plan)?;
        let windows = services.inner().replicated().durability_windows();
        check_windows(&windows);
        // The learner joins the acknowledgement set at its first session.
        let longest = DurabilityWindows::longest(&windows.new_writes);
        assert!(longest < Duration::from_secs(5), "{windows:?}");
        assert!(services.inner().replicated().learners().promoted >= windows.opened);
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        Ok(())
    });
}

/// In a cluster of three, a node loses power and stays down for a while:
/// its shards' primaries remove it, and the coordinator adds it back as a
/// learner, the only node left. It returns with its old log, which seeds
/// its catch-up if the primary verifies it, and is discarded for a
/// snapshot otherwise.
#[test]
fn a_removed_node_is_readmitted_as_a_learner() {
    Runner::with_cost(3, COST).run(|context| {
        let crash = Fault::Crash {
            node: 2,
            power_loss: true,
            downtime: Duration::from_secs(6),
        };
        let plan = FaultPlan::none().with(Duration::from_secs(2), crash);
        let services = services();
        let report = cluster(config(3), &services).run(context, &workload(context), &plan)?;
        check_windows(&services.inner().replicated().durability_windows());
        assert!(services.inner().replicated().learners().promoted > 0);
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        Ok(())
    });
}

/// Lost members replaced under crashes, power loss, partitions, message
/// loss, and control-store faults: learners are added, dropped, given
/// snapshots again, and promoted, and R3 holds throughout.
#[test]
fn lost_members_are_replaced_under_random_faults() {
    Runner::with_cost(4, COST / 2).run(|context| {
        let workload = Workload {
            operations: 200 * context.scale() as usize,
            ..workload(context)
        };
        let profile = FaultProfile {
            end: Duration::from_secs(10) * context.scale(),
            crashes: 3,
            partitions: 2,
            message_loss: 1,
            control: 1,
            max_duration: Duration::from_secs(5),
            ..FaultProfile::default()
        };
        let config = config(4);
        let plan = FaultPlan::random(
            context.rng(),
            &profile,
            config.nodes,
            config.disks_per_node,
            workload.clients,
        );
        let services = services();
        let report = cluster(config, &services).run(context, &workload, &plan)?;
        eprintln!(
            "{:?} {:?}",
            services.inner().replicated().learners(),
            services.inner().replicated().durability_windows()
        );
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        Ok(())
    });
}
