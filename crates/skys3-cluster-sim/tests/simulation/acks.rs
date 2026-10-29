//! Acknowledgement timeouts (plan M2-10, design §5.2): while a member is
//! cut off from its primary, writes fail with `503 SlowDown` once
//! `replica_ack_timeout` passes, in both modes. A failed write is not
//! acknowledged, but it keeps its position: it commits once the member is
//! back, so a later read may see it. It never takes effect over a write
//! sent after its failure, which the history checkers verify: such a write
//! is ordered after it (linearizability), and survives recovery
//! (durability).

use std::time::Duration;

use skys3_cluster_sim::{
    Cluster, ClusterConfig, Endpoint, Fault, FaultPlan, LateWrites, ReplicatedServices, RunError,
    View, Workload,
};
use skys3_shard::AckTimeout;
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

/// Replication with `ack`.
fn replication(ack: AckTimeout) -> ReplicationConfig {
    ReplicationConfig {
        ack_timeout: ack,
        ..ReplicationConfig::default()
    }
}

/// A busy workload, so failed writes are followed by later writes and reads
/// of the same keys. Clients wait the default 2 s for an answer, longer
/// than either timeout here, so a stalled write is answered with a `503`
/// rather than given up on.
fn workload(context: &SimContext) -> Workload {
    Workload {
        clients: 6,
        operations: 60 * context.scale() as usize,
        keys: 8,
        think_time: Duration::from_millis(80),
        ..Workload::default()
    }
}

/// Partitions between each pair of nodes in turn, 1.5 s each, longer than
/// either timeout here: every primary has each of its members cut off
/// once.
fn plan() -> FaultPlan {
    let mut plan = FaultPlan::none();
    for (n, (a, b)) in [(0, 1), (1, 2), (2, 0), (0, 2), (1, 0), (2, 1)]
        .into_iter()
        .enumerate()
    {
        plan.push(
            Duration::from_millis(400 + 2200 * n as u64),
            Fault::Partition {
                a: Endpoint::Node(a),
                b: Endpoint::Node(b),
                duration: Duration::from_millis(1500),
            },
        );
    }
    plan
}

/// Runs the scenario with `ack`: in every seed some writes fail after
/// they got a position and are committed later, and the checkers find none
/// taking effect over a write sent after its failure.
fn failed_writes_never_resurface(ack: AckTimeout) {
    let (mut late, mut failed) = (LateWrites::default(), 0);
    Runner::with_cost(3, COST).run(|context| {
        let services = ReplicatedServices::new(replication(ack));
        let report = Cluster::with_services(config(), services.clone())
            .invariant(|view: &View<'_, ReplicatedServices>| view.services.check_commits())
            .run(context, &workload(context), &plan())?;
        let run = services.late_writes();
        // Late writes from the last partitions may still wait for a
        // member when the run ends.
        assert!(run.committed > 0, "no failed write was applied: {run:?}");
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        late.sequenced += run.sequenced;
        late.committed += run.committed;
        failed += report.count(|o| *o == Outcome::Failed);
        Ok(())
    });
    eprintln!("{ack:?}: {failed} operations failed; {late:?}");
}

/// Wait-through mode: writes wait out the timeout, then fail. The timeout
/// is far below the design's minimum for this mode, which leaves time to
/// remove the member (plan M2-11); nothing removes members here, so every
/// partition outlasts it either way.
#[test]
fn failed_writes_never_resurface_waiting_through() {
    failed_writes_never_resurface(AckTimeout::wait_through(Duration::from_millis(600)));
}

/// Fail-fast mode: once a write timed out, the shard refuses writes at once
/// until the member is back and the late record is applied.
#[test]
fn failed_writes_never_resurface_failing_fast() {
    failed_writes_never_resurface(AckTimeout::fail_fast(Duration::from_millis(300)));
}

/// The checks catch a failed write that takes effect over a later one: a
/// seeded bug sends every failed write again a second later, with a new
/// position.
#[test]
fn the_checks_catch_a_failed_write_resubmitted_later() {
    let (mut caught, mut seeds) = (0, 0);
    Runner::with_cost(3, COST).run(|context| {
        seeds += 1;
        let services = ReplicatedServices::resubmitting_failed_writes(replication(
            AckTimeout::fail_fast(Duration::from_millis(300)),
        ));
        let outcome =
            Cluster::with_services(config(), services).run(context, &workload(context), &plan());
        match outcome {
            Ok(_) => {}
            Err(RunError::Check(_)) => caught += 1,
            Err(error) => return Err(error.into()),
        }
        Ok(())
    });
    eprintln!("resubmitted failed writes: caught in {caught} of {seeds} seeds");
    assert!(caught > 0, "no resubmitted write was caught");
}
