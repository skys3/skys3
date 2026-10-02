//! Primary-scoped work across failover (plan M2-17, design §7.1, §7.2,
//! §9.1): the flusher and the namespace import run on primaries only, and
//! a new primary resumes them from its log.
//!
//! A `write_back` bucket flushes to a remote store that fails, loses
//! requests and answers, delays them, and keeps every version. Primaries
//! fail over in the middle of flushes and of the import, by crashes and by
//! planned handoffs. After every step the audits check one committing
//! primary per epoch (`check_commits`), reads under leases
//! (`check_reads`), that only current primaries serve (`check_served`),
//! and rule R3 (`check_members_hold_commits`); every history is checked
//! for linearizability per key, with the remote's objects as the keys'
//! first values. After the run settles, the cluster checks that nothing
//! of the work was lost ([`ClusterConfig::settle`]): every acknowledged
//! write is at the remote and clean on every member, a deleted key is
//! neither at the remote nor in any index, every remote object no client
//! touched is imported on every member, every shard's import is done, and
//! no write identity is on two remote versions, so every flush repeated
//! after a failover was recognized by its identity.

use std::time::{Duration, Instant};

use skys3_cluster_sim::{
    Cluster, ClusterConfig, Fault, FaultPlan, ReplicatedServices, RoutedServices, View, Workload,
};
use skys3_gateway::routing::RoutingConfig;
use skys3_shard::replication::ReplicationConfig;
use skys3_sim::history::Outcome;
use skys3_sim::s3::SimS3Faults;
use skys3_sim::{Runner, SimContext};

use crate::COST;

/// A remote store that fails about one request in twelve: errors, lost
/// requests, and lost answers, and delays up to 40 ms each way. Each
/// node's capability probe must get its twenty-odd requests through in
/// one run before its flushers start, so faults much more frequent would
/// mostly test the probe's retries.
fn faulty_remote() -> SimS3Faults {
    SimS3Faults {
        min_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(40),
        internal_error_probability: 0.02,
        slow_down_probability: 0.01,
        lost_request_probability: 0.02,
        lost_response_probability: 0.04,
        ..SimS3Faults::NONE
    }
}

/// `nodes` nodes, every shard on all of them, and one `write_back` bucket
/// of four shards over the faulty, versioned remote, holding
/// `remote_objects` objects besides the workload's keys before the start.
fn config(nodes: usize, remote_objects: usize) -> ClusterConfig {
    ClusterConfig {
        nodes,
        replicas: nodes,
        buckets: 1,
        write_back_buckets: 1,
        every_member_durable: true,
        remote_faults: faulty_remote(),
        remote_versioning: true,
        remote_objects,
        // Pages of three keys, about ten a second.
        import_keys_per_second: Some(30),
        settle: Duration::from_secs(30),
        ..ClusterConfig::default()
    }
}

/// Timings shorter than the defaults, in the same order and within the
/// lease inequality for `ρ` = 1%. A member's acknowledgement can wait up
/// to a second for a `FLUSHED` record to be synced (§7.1), so a primary
/// suspects a member only after longer than that: with the takeover
/// scenarios' 700 ms, every primary that flushes removes its members.
fn fast() -> ReplicationConfig {
    ReplicationConfig {
        lease_renew_interval: Duration::from_millis(250),
        primary_lease: Duration::from_millis(1500),
        primary_grace: Duration::from_millis(2200),
        member_suspect_after: Duration::from_millis(1600),
        ..ReplicationConfig::default()
    }
}

/// Clients that send each request to any node and wait out a takeover.
fn workload(context: &SimContext, operations: usize) -> Workload {
    Workload {
        clients: 4,
        operations: operations * context.scale() as usize,
        keys: 6,
        think_time: Duration::from_millis(120),
        timeout: Duration::from_secs(6),
        any_gateway: true,
        ..Workload::default()
    }
}

/// Routing over `replicated`, whose gateways read the register often and
/// give up on a request within a client's timeout.
fn services(replicated: ReplicatedServices) -> RoutedServices {
    let routing = RoutingConfig {
        connect_timeout: Duration::from_millis(300),
        request_timeout: Duration::from_secs(4),
        register_interval: Duration::from_millis(200),
        ..RoutingConfig::default()
    };
    RoutedServices::new(replicated, routing)
}

/// A cluster checked against the protocol's audits after every step.
fn cluster(config: ClusterConfig, services: &RoutedServices) -> Cluster<RoutedServices> {
    Cluster::with_services(config, services.clone()).invariant(|view: &View<'_, RoutedServices>| {
        let replicated = view.services.replicated();
        replicated.check_commits()?;
        replicated.check_reads()?;
        replicated.check_members_hold_commits()?;
        view.services.check_served()
    })
}

fn crash(node: usize, power_loss: bool, downtime: Duration) -> Fault {
    Fault::Crash {
        node,
        power_loss,
        downtime,
    }
}

/// Prints what a seed did and how long it took.
fn report(name: &str, seed: u64, started: Instant, services: &RoutedServices, done: usize) {
    let taken = services.replicated().taken_over().len();
    let handoffs = services.replicated().handoffs();
    eprintln!(
        "{name} seed {seed}: {done} writes acknowledged, {taken} shards taken over, \
         {} handoffs, {:?} of real time",
        handoffs.received,
        started.elapsed()
    );
}

/// A node crashes while its primaries flush to the faulty remote, and
/// stays down long enough for members to take its shards over; it comes
/// back as a replica of nothing it led. The new primaries flush what the
/// old one had not recorded as flushed, find what it flushed by its write
/// identity, and every write reaches the remote once.
#[test]
fn a_primary_failing_over_mid_flush_loses_no_flush() {
    Runner::with_cost(3, COST).run(|context| {
        let started = Instant::now();
        let services = services(ReplicatedServices::new(fast()).with_takeover());
        let seed = context.seed();
        let node = usize::try_from(seed % 3)?;
        let at = Duration::from_millis(2000 + 150 * (seed % 8));
        let plan = FaultPlan::none().with(at, crash(node, seed % 2 == 0, Duration::from_secs(6)));
        let report_ =
            cluster(config(3, 0), &services).run(context, &workload(context, 40), &plan)?;
        let done = report_.count(|o| *o == Outcome::Done);
        report("flush", seed, started, &services, done);
        assert!(!services.replicated().taken_over().is_empty());
        assert!(report_.flushed > 0, "{report_:?}");
        Ok(())
    });
}

/// The owner of the bucket's import, the primary of shard 0, crashes in
/// the middle of the import while clients delete and overwrite keys the
/// import has not reached yet. The member that takes shard 0 over resumes
/// the import from the progress in its log, every import record of a key
/// the progress has passed is dropped, and no deleted key comes back.
#[test]
fn an_import_owner_failing_over_mid_import_resurrects_no_key() {
    Runner::with_cost(3, COST).run(|context| {
        let started = Instant::now();
        let services = services(ReplicatedServices::new(fast()).with_takeover());
        let seed = context.seed();
        // Shard 0 of the only bucket has its primary on node 0.
        let at = Duration::from_millis(1000 + 200 * (seed % 8));
        let plan = FaultPlan::none().with(at, crash(0, seed % 2 == 1, Duration::from_secs(6)));
        let report_ =
            cluster(config(3, 120), &services).run(context, &workload(context, 40), &plan)?;
        let done = report_.count(|o| *o == Outcome::Done);
        report("import", seed, started, &services, done);
        assert!(!services.replicated().taken_over().is_empty());
        Ok(())
    });
}

/// Primaries hand their shards off to members again and again while they
/// flush and while the import runs: each handoff stops the old primary's
/// flusher, and its import if it led shard 0, and starts them on the new
/// primary.
#[test]
fn handoffs_move_flushes_and_the_import() {
    Runner::with_cost(3, COST).run(|context| {
        let started = Instant::now();
        let seed = context.seed();
        let every = Duration::from_millis(500 + 50 * (seed % 6));
        let replicated =
            ReplicatedServices::new(fast()).with_handoffs(every, Duration::from_secs(8));
        let services = services(replicated);
        let report_ = cluster(config(4, 80), &services).run(
            context,
            &workload(context, 40),
            &FaultPlan::none(),
        )?;
        let done = report_.count(|o| *o == Outcome::Done);
        report("handoff", seed, started, &services, done);
        assert!(services.replicated().handoffs().received > 0);
        Ok(())
    });
}
