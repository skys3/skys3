//! Whole-cluster restarts while the control store is unreachable (plan
//! M2-16, design §6.2, §16.1): every node resumes each replica from the
//! configuration of its shard's latest `CONFIG` record. A shard whose
//! membership did not change serves again without the control store; a
//! node whose shards changed while it was down holds stale configurations,
//! which epochs keep fenced until it can read the registers.
//!
//! Node 3 of four goes down first. While it is down, its primaries' shards
//! are taken over by their members and the primaries of its other shards
//! remove it. Then the control store becomes unreachable, every node
//! restarts, and node 3 comes back with configurations that are no longer
//! current. Two shards never had node 3 as a member.
//!
//! After every step the audits of the replication scenarios run: commits
//! (`check_commits`), reads under leases (`check_reads`), and that only
//! the current primary serves (`check_served`). During the outage, after
//! the restart, every serving replica must be in its register's
//! configuration, and every shard must be served by a primary in it.
//! Clients that send requests to node 3 wait while its gateway reads the
//! registers its stale map needs, which the outage makes slow, so the
//! audit checks serving on the replicas, and only that some writes were
//! acknowledged.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use skys3_cluster_sim::{
    Cluster, ClusterConfig, Fault, FaultPlan, ReplicatedServices, RoutedServices, View, Workload,
};
use skys3_gateway::ShardRef;
use skys3_gateway::routing::RoutingConfig;
use skys3_shard::replication::ReplicationConfig;
use skys3_sim::history::Outcome;
use skys3_sim::{Runner, SimContext};

use crate::COST;

/// The node that is down while its shards change.
const AWAY: usize = 3;

/// Four nodes, every shard on three of them: two shards without node 3.
fn config() -> ClusterConfig {
    ClusterConfig {
        nodes: 4,
        replicas: 3,
        every_member_durable: true,
        ..ClusterConfig::default()
    }
}

/// The takeover scenarios' timings: members take over within about 2 s,
/// and primaries remove a silent member within about 1 s.
fn fast() -> ReplicationConfig {
    ReplicationConfig {
        lease_renew_interval: Duration::from_millis(200),
        primary_lease: Duration::from_millis(800),
        primary_grace: Duration::from_millis(1400),
        member_suspect_after: Duration::from_millis(700),
        ..ReplicationConfig::default()
    }
}

/// Clients that send each request to any node, with keys in every shard,
/// for longer than the outage lasts.
fn workload(context: &SimContext) -> Workload {
    Workload {
        clients: 4,
        operations: 240 * context.scale() as usize,
        keys: 16,
        think_time: Duration::from_millis(150),
        timeout: Duration::from_secs(6),
        any_gateway: true,
        ..Workload::default()
    }
}

/// Routing over replication with takeovers and local copies, whose
/// gateways read the register often and give up on a request within a
/// client's timeout.
fn services() -> RoutedServices {
    let routing = RoutingConfig {
        connect_timeout: Duration::from_millis(300),
        request_timeout: Duration::from_secs(4),
        register_interval: Duration::from_millis(200),
        ..RoutingConfig::default()
    };
    let replicated = ReplicatedServices::new(fast())
        .with_takeover()
        .with_local_copies();
    RoutedServices::new(replicated, routing)
}

/// What the restart audit saw over a run.
#[derive(Debug, Default)]
struct Seen {
    /// The nodes seen down so far.
    down: BTreeSet<usize>,
    /// When every node was up again after each had been down.
    restarted: Option<Duration>,
    /// The shards seen served, during the window, by a primary in their
    /// register's configuration that had brought its members up to its
    /// log.
    serving: BTreeSet<ShardRef>,
    /// The shards with a write acknowledged in their register's epoch
    /// between the restart and the end of the window.
    acknowledged: BTreeSet<ShardRef>,
    /// The replicas seen open in a configuration older than their
    /// register's during the window, by node.
    stale: BTreeMap<usize, BTreeSet<ShardRef>>,
    /// Whether the window has passed.
    done: bool,
}

/// How long after the restart the window opens: the nodes resume their
/// shards, primaries reconcile their members, and the clients' requests
/// that the crashes cut off time out.
const SETTLE: Duration = Duration::from_secs(5);
/// How long the window lasts, within the outage.
const WINDOW: Duration = Duration::from_secs(5);

/// The restart audit: from `SETTLE` after the whole-cluster restart, for
/// `WINDOW`, while the control store is still unreachable, every replica
/// that serves holds its register's configuration; the shards served in
/// it, the replicas in stale configurations, and the shards whose writes
/// were acknowledged since the restart are noted.
fn restart_audit(
    seen: &Arc<Mutex<Seen>>,
) -> impl FnMut(&View<'_, RoutedServices>) -> Result<(), String> + 'static {
    let seen = Arc::clone(seen);
    move |view| {
        let mut seen = seen.lock().unwrap_or_else(PoisonError::into_inner);
        for (node, up) in view.up.iter().enumerate() {
            if !up {
                seen.down.insert(node);
            }
        }
        if seen.restarted.is_none()
            && seen.down.len() == view.up.len()
            && view.up.iter().all(|up| *up)
        {
            seen.restarted = Some(view.elapsed);
        }
        let Some(restarted) = seen.restarted else {
            return Ok(());
        };
        let (start, end) = (restarted + SETTLE, restarted + SETTLE + WINDOW);
        if view.elapsed < start || seen.done {
            return Ok(());
        }
        let replicated = view.services.replicated();
        let replicas = replicated.replicas_now(view.up);
        let registers: BTreeMap<_, _> = replicas
            .iter()
            .filter_map(|replica| Some((replica.shard.clone(), replica.register.clone()?)))
            .collect();
        for (shard, (at, epoch)) in replicated.acknowledged() {
            if at >= restarted && registers.get(&shard).is_some_and(|r| r.epoch == epoch) {
                seen.acknowledged.insert(shard);
            }
        }
        if view.elapsed >= end {
            seen.done = true;
            return Ok(());
        }
        for replica in replicas {
            let Some(register) = &replica.register else {
                continue;
            };
            if replica.config.epoch < register.epoch {
                let node = position(&replica.node);
                seen.stale
                    .entry(node)
                    .or_default()
                    .insert(replica.shard.clone());
            }
            if replica.serving && replica.config != *register {
                return Err(format!(
                    "{} serves shard {} in epoch {} while its register holds epoch {}",
                    replica.node, replica.shard, replica.config.epoch, register.epoch
                ));
            }
            if replica.serving {
                seen.serving.insert(replica.shard.clone());
            }
        }
        Ok(())
    }
}

/// The position of a simulated node, from 0; its ID counts from 1.
fn position(node: &skys3_types::NodeId) -> usize {
    node.as_str()
        .strip_prefix("node-")
        .and_then(|n| n.parse::<usize>().ok())
        .and_then(|n| n.checked_sub(1))
        .expect("a simulated node's ID")
}

/// The plan: node 3 goes down at 0.5 s for 5 s; the store is unreachable
/// from 3.5 s for 14 s; nodes 0 to 2 go down one after another from 4 s,
/// with or without power as the seed draws, and come back within 2 s, as
/// node 3 does.
fn plan(context: &SimContext) -> FaultPlan {
    let seed = context.seed();
    let mut plan = FaultPlan::none().with(
        Duration::from_millis(500),
        Fault::Crash {
            node: AWAY,
            power_loss: seed % 2 == 0,
            downtime: Duration::from_millis(5000),
        },
    );
    plan.push(
        Duration::from_millis(3500),
        Fault::ControlOutage {
            duration: Duration::from_millis(14_000),
        },
    );
    for node in 0..AWAY {
        let step = node as u64;
        plan.push(
            Duration::from_millis(4000 + 200 * step),
            Fault::Crash {
                node,
                power_loss: (seed >> (step + 1)) % 2 == 0,
                downtime: Duration::from_millis(1200 + 100 * ((seed >> 4) % 4) + 100 * step),
            },
        );
    }
    plan
}

/// The cluster with the replication audits after every step.
fn cluster(services: &RoutedServices, seen: &Arc<Mutex<Seen>>) -> Cluster<RoutedServices> {
    Cluster::with_services(config(), services.clone())
        .invariant(|view: &View<'_, RoutedServices>| {
            let replicated = view.services.replicated();
            replicated.check_commits()?;
            replicated.check_reads()?;
            view.services.check_served()
        })
        .invariant(restart_audit(seen))
}

/// The plan's "Done when" (§16.1): after a whole-cluster restart with the
/// control store unreachable, every shard whose membership did not change
/// serves again, and so does every shard whose surviving members hold its
/// current configuration, while the configurations node 3 kept stay
/// fenced. Once the store answers, node 3 learns that its shards moved
/// on, and every acknowledged write reads back.
#[test]
fn a_whole_cluster_restart_while_the_control_store_is_unreachable() {
    Runner::with_cost(3, COST).run(|context| {
        let services = services();
        let seen = Arc::new(Mutex::new(Seen::default()));
        let plan = plan(context);
        let report = cluster(&services, &seen).run(context, &workload(context), &plan)?;
        let seen = seen.lock().unwrap_or_else(PoisonError::into_inner);
        assert!(seen.done, "the window never passed: {seen:?}");
        // Every shard served again in its current configuration, the two
        // that node 3 never belonged to among them, and writes were
        // acknowledged.
        let shards = config().buckets * config().shards_per_bucket as usize;
        assert_eq!(seen.serving.len(), shards, "{seen:?}");
        assert!(!seen.acknowledged.is_empty(), "{seen:?}");
        // Node 3 resumed shards whose membership changed while it was down,
        // and they stayed fenced.
        let stale = seen.stale.get(&AWAY).map_or(0, BTreeSet::len);
        assert!(stale > 0, "node 3 resumed no stale configuration: {seen:?}");
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        Ok(())
    });
}
