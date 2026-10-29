//! Member removal (plan M2-11, design §6.4): a primary removes a member
//! that stays unresponsive for `member_suspect_after` by a compare-and-swap
//! of the shard's register, through the node's faulty control store, and
//! the records the member held back commit under the new epoch.
//!
//! With the defaults (3 s suspicion, a 5 s wait-through timeout), a member
//! cut off from its primaries stalls their writes for about
//! `member_suspect_after` plus the compare-and-swap, and the writes in
//! flight then complete without errors. In fail-fast mode they fail, and
//! later writes succeed once the member is gone. Linearizability and
//! durability on every member of the final configurations are checked
//! after every run.

use std::time::Duration;

use skys3_cluster_sim::{
    Cluster, ClusterConfig, Endpoint, Fault, FaultPlan, ReplicatedServices, View, Workload,
};
use skys3_shard::AckTimeout;
use skys3_shard::replication::ReplicationConfig;
use skys3_sim::history::{Operation, Outcome};
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

/// The design's defaults, with `ack`.
fn replication(ack: AckTimeout) -> ReplicationConfig {
    ReplicationConfig {
        ack_timeout: ack,
        ..ReplicationConfig::default()
    }
}

/// Clients that wait longer than the acknowledgement timeout, so that a
/// stalled write is answered rather than given up on.
fn workload(context: &SimContext) -> Workload {
    Workload {
        clients: 6,
        operations: 40 * context.scale() as usize,
        keys: 8,
        think_time: Duration::from_millis(100),
        timeout: Duration::from_secs(8),
        ..Workload::default()
    }
}

/// Node 3 is cut off from the other nodes for `duration`, from 1 s on:
/// the primaries of its shards remove it, and it removes the members of
/// the shards it leads. Clients still reach every node.
fn isolate_node_3(duration: Duration) -> FaultPlan {
    let mut plan = FaultPlan::none();
    for other in [0, 1] {
        plan.push(
            Duration::from_secs(1),
            Fault::Partition {
                a: Endpoint::Node(2),
                b: Endpoint::Node(other),
                duration,
            },
        );
    }
    plan
}

/// Writes in `history` that failed or got no answer.
fn failed_writes(history: &[Operation]) -> usize {
    history
        .iter()
        .filter(|op| op.call.is_write() && matches!(op.outcome, Outcome::Failed | Outcome::Unknown))
        .count()
}

/// Wait-through mode (the default): no write fails, and the slowest one
/// waits about `member_suspect_after` plus the removal's compare-and-swap.
#[test]
fn a_failing_member_stalls_writes_until_its_removal() {
    let defaults = ReplicationConfig::default();
    let suspect = defaults.member_suspect_after;
    Runner::with_cost(3, COST).run(|context| {
        let services = ReplicatedServices::new(defaults);
        let report = Cluster::with_services(config(), services.clone())
            .invariant(|view: &View<'_, ReplicatedServices>| view.services.check_commits())
            .run(
                context,
                &workload(context),
                &isolate_node_3(Duration::from_secs(8)),
            )?;
        assert!(services.reconfigured() > 0, "no member was removed");
        let failed = failed_writes(&report.history);
        assert_eq!(failed, 0, "writes failed while a member was removed");
        let slowest = services.slowest_write();
        eprintln!("the slowest write took {slowest:?}");
        assert!(
            slowest >= suspect / 2,
            "no write waited for the removal: {slowest:?}"
        );
        assert!(
            slowest < suspect + Duration::from_secs(1),
            "a write stalled {slowest:?}"
        );
        Ok(())
    });
}

/// Fail-fast mode: writes fail once a member is late, and later ones
/// succeed once it is removed. Writes that failed after they got a
/// position commit under the new epoch.
#[test]
fn a_failing_member_fails_writes_fast_until_its_removal() {
    Runner::with_cost(3, COST).run(|context| {
        let ack = AckTimeout::fail_fast(Duration::from_secs(2));
        let services = ReplicatedServices::new(replication(ack));
        let report = Cluster::with_services(config(), services.clone())
            .invariant(|view: &View<'_, ReplicatedServices>| view.services.check_commits())
            .run(
                context,
                &workload(context),
                &isolate_node_3(Duration::from_secs(8)),
            )?;
        assert!(services.reconfigured() > 0, "no member was removed");
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        let late = services.late_writes();
        eprintln!("{late:?}");
        assert!(late.committed > 0, "no failed write committed: {late:?}");
        Ok(())
    });
}

/// While the control store is unreachable no member can be removed, so
/// writes fail after the timeout in wait-through mode too (§5.2); once it
/// is back, the member is removed and writes go on.
#[test]
fn without_the_control_store_writes_fail_until_it_returns() {
    Runner::with_cost(2, COST).run(|context| {
        let mut plan = isolate_node_3(Duration::from_secs(12));
        plan.push(
            Duration::from_millis(500),
            Fault::ControlOutage {
                duration: Duration::from_secs(8),
            },
        );
        let services = ReplicatedServices::default();
        let report = Cluster::with_services(config(), services.clone())
            .invariant(|view: &View<'_, ReplicatedServices>| view.services.check_commits())
            .run(context, &workload(context), &plan)?;
        assert!(services.reconfigured() > 0, "no member was removed");
        assert!(failed_writes(&report.history) > 0);
        Ok(())
    });
}

/// A primary that loses power soon after removing a member restarts in
/// the register's configuration, from a log that may not hold the new
/// epoch's `CONFIG` record yet.
#[test]
fn a_primary_restarts_after_removing_a_member() {
    Runner::with_cost(3, COST).run(|context| {
        let mut plan = isolate_node_3(Duration::from_secs(10));
        let at = Duration::from_millis(4000 + 100 * (context.seed() % 10));
        plan.push(
            at,
            Fault::Crash {
                node: 0,
                power_loss: true,
                downtime: Duration::from_millis(500),
            },
        );
        let services = ReplicatedServices::default();
        let report = Cluster::with_services(config(), services.clone())
            .invariant(|view: &View<'_, ReplicatedServices>| view.services.check_commits())
            .run(context, &workload(context), &plan)?;
        assert!(services.reconfigured() > 0, "no member was removed");
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        Ok(())
    });
}
