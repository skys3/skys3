//! Rebuilding a lost control store (plan M3-07, design §6.2, §6.9): the
//! drill. The control bucket loses every register while the cluster runs;
//! the nodes serve on from their local copies, and one restarts in the
//! meantime; then an operator stops every node, exports each, and rebuilds
//! the store from the exports; the nodes start again on it, and membership
//! changes resume: once node 3 goes down, primaries remove it and members
//! take over its shards, by compare-and-swap on the rebuilt registers.
//!
//! After every step the audits of the replication scenarios run: commits
//! (`check_commits`), reads under leases (`check_reads`), and that only
//! the current primary serves (`check_served`). The drill's own audit
//! notes the writes acknowledged while the store was lost, and, in a
//! window after the rebuilt cluster settled and before node 3 goes down,
//! that every shard is served by a primary whose configuration is its
//! register's: the rebuild wrote each shard's newest configuration as it
//! was, so no replica had to adopt anything.

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
use skys3_types::{Epoch, NodeId};

use crate::COST;

/// The node that goes down once the store is rebuilt.
const AWAY: usize = 3;

/// When the store is lost, after the clients start.
const LOST_AT: Duration = Duration::from_millis(1000);
/// When the operator stops the nodes and rebuilds the store.
const REBUILD_AT: Duration = Duration::from_millis(4000);
/// How long the nodes stay down for the rebuild.
const REBUILD_DOWNTIME: Duration = Duration::from_millis(1000);
/// When node 3 goes down, after the window.
const AWAY_AT: Duration = Duration::from_millis(13_000);
/// How soon after node 3 goes down a register moves past its rebuilt
/// epoch: `member_suspect_after` and a compare-and-swap, with room for
/// the store's injected faults.
const MEMBERSHIP_BOUND: Duration = Duration::from_secs(2);

/// Four nodes, every shard on three of them.
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
/// for longer than the drill lasts.
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

/// Routing over replication with takeovers and local copies.
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

/// What the drill's audit saw over a run.
#[derive(Debug, Default)]
struct Seen {
    /// When a step first saw no shard register.
    lost: Option<Duration>,
    /// The nodes seen down while the store was lost.
    down_while_lost: BTreeSet<usize>,
    /// The nodes with replicas open at the last step before the rebuild
    /// stopped them, while the store was lost: a node that restarted
    /// meanwhile resumed its replicas from its kept configurations.
    open_while_lost: BTreeSet<usize>,
    /// When every node was down at once: the rebuild.
    stopped: Option<Duration>,
    /// When every node was up again after the rebuild.
    restarted: Option<Duration>,
    /// The shards with a write acknowledged while the store was lost.
    acknowledged_while_lost: BTreeSet<ShardRef>,
    /// The shards served, during the window, by a primary in its
    /// register's configuration.
    serving: BTreeSet<ShardRef>,
    /// Whether the window has passed.
    done: bool,
    /// Each shard's register epoch in the window: the rebuilt one.
    rebuilt: BTreeMap<ShardRef, Epoch>,
    /// When node 3 was first seen down after the window.
    away: Option<Duration>,
    /// When a register was first seen past its rebuilt epoch after that:
    /// the first membership change on the rebuilt store.
    changed: Option<Duration>,
}

/// How long after the rebuilt cluster restarted the window opens: the
/// nodes resume their shards, primaries reconcile their members, and the
/// requests the stop cut off time out.
const SETTLE: Duration = Duration::from_secs(5);
/// How long the window lasts, before node 3 goes down.
const WINDOW: Duration = Duration::from_secs(2);

/// The drill's audit; see the module documentation.
fn rebuild_audit(
    seen: &Arc<Mutex<Seen>>,
) -> impl FnMut(&View<'_, RoutedServices>) -> Result<(), String> + 'static {
    let seen = Arc::clone(seen);
    move |view| {
        let mut seen = seen.lock().unwrap_or_else(PoisonError::into_inner);
        let replicated = view.services.replicated();
        let replicas = replicated.replicas_now(view.up);
        if seen.lost.is_none()
            && !replicas.is_empty()
            && replicas.iter().all(|replica| replica.register.is_none())
        {
            seen.lost = Some(view.elapsed);
        }
        if seen.stopped.is_none() && view.up.iter().all(|up| !up) {
            seen.stopped = Some(view.elapsed);
        }
        if let (Some(lost), None) = (seen.lost, seen.stopped) {
            for (shard, (at, _)) in replicated.acknowledged() {
                if at >= lost {
                    seen.acknowledged_while_lost.insert(shard);
                }
            }
            for (node, up) in view.up.iter().enumerate() {
                if !up {
                    seen.down_while_lost.insert(node);
                }
            }
            seen.open_while_lost = replicas
                .iter()
                .map(|replica| position(&replica.node))
                .collect();
        }
        if seen.stopped.is_some() && seen.restarted.is_none() && view.up.iter().all(|up| *up) {
            seen.restarted = Some(view.elapsed);
        }
        let Some(restarted) = seen.restarted else {
            return Ok(());
        };
        let (start, end) = (restarted + SETTLE, restarted + SETTLE + WINDOW);
        if view.elapsed < start {
            return Ok(());
        }
        if seen.done {
            if seen.away.is_none() && !view.up[AWAY] {
                seen.away = Some(view.elapsed);
            }
            let moved = replicas.iter().any(|replica| {
                let rebuilt = seen.rebuilt.get(&replica.shard);
                replica
                    .register
                    .as_ref()
                    .zip(rebuilt)
                    .is_some_and(|(r, e)| r.epoch > *e)
            });
            if seen.away.is_some() && seen.changed.is_none() && moved {
                seen.changed = Some(view.elapsed);
            }
            return Ok(());
        }
        if view.elapsed >= end {
            seen.done = true;
            return Ok(());
        }
        for replica in replicas {
            let Some(register) = &replica.register else {
                return Err(format!(
                    "shard {} has no register after the rebuild",
                    replica.shard
                ));
            };
            if replica.serving && replica.config != *register {
                return Err(format!(
                    "{} serves shard {} in epoch {} while its rebuilt register holds epoch {}",
                    replica.node, replica.shard, replica.config.epoch, register.epoch
                ));
            }
            if replica.serving {
                seen.serving.insert(replica.shard.clone());
            }
            seen.rebuilt.insert(replica.shard.clone(), register.epoch);
        }
        Ok(())
    }
}

/// The position of a simulated node, from 0; its ID counts from 1.
fn position(node: &NodeId) -> usize {
    node.as_str()
        .strip_prefix("node-")
        .and_then(|n| n.parse::<usize>().ok())
        .and_then(|n| n.checked_sub(1))
        .expect("a simulated node's ID")
}

/// The plan: the store is lost at 1 s; one of nodes 0 to 2 restarts while
/// it is lost; the operator rebuilds it at 4 s, with the nodes down for
/// 1 s; node 3 goes down at 13 s for 6 s, and its shards change
/// membership on the rebuilt store. Power losses as the seed draws.
fn plan(context: &SimContext) -> FaultPlan {
    let seed = context.seed();
    FaultPlan::none()
        .with(LOST_AT, Fault::LoseControlStore)
        .with(
            Duration::from_millis(1500),
            Fault::Crash {
                node: (seed % 3) as usize,
                power_loss: seed & 4 == 0,
                downtime: Duration::from_millis(1000),
            },
        )
        .with(
            REBUILD_AT,
            Fault::RebuildControlStore {
                power_loss: seed & 8 == 0,
                downtime: REBUILD_DOWNTIME,
            },
        )
        .with(
            AWAY_AT,
            Fault::Crash {
                node: AWAY,
                power_loss: seed & 16 == 0,
                downtime: Duration::from_secs(6),
            },
        )
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
        .invariant(rebuild_audit(seen))
}

/// The plan's "Done when": a drill deletes the simulated control bucket,
/// rebuilds it, and membership changes resume. The data path runs on
/// while the store is lost, the rebuilt registers are each shard's newest
/// configuration, and every shard node 3 belonged to moves on without it,
/// past the rebuilt epoch. The histories stay linearizable, and every
/// acknowledged write is on every member.
#[test]
fn a_lost_control_store_is_rebuilt_and_membership_changes_resume() {
    // About twice a typical cluster seed: CI's fixed set runs two.
    Runner::with_cost(3, 4 * COST).run(|context| {
        let services = services();
        let seen = Arc::new(Mutex::new(Seen::default()));
        let plan = plan(context);
        let report = cluster(&services, &seen).run(context, &workload(context), &plan)?;
        let seen = seen.lock().unwrap_or_else(PoisonError::into_inner);
        let shards = config().buckets * config().shards_per_bucket as usize;

        // The data path ran on while the store was lost, and a node that
        // restarted meanwhile resumed its replicas from its local copies.
        assert!(
            seen.lost.is_some(),
            "the store was never seen lost: {seen:?}"
        );
        assert!(!seen.acknowledged_while_lost.is_empty(), "{seen:?}");
        assert!(!seen.down_while_lost.is_empty(), "{seen:?}");
        assert!(
            seen.down_while_lost.is_subset(&seen.open_while_lost),
            "{seen:?}"
        );

        // One rebuild, of every shard's register.
        assert_eq!(report.rebuilds.len(), 1, "{:?}", report.rebuilds);
        let rebuild = &report.rebuilds[0];
        let rebuilt: BTreeMap<ShardRef, _> = rebuild
            .plan
            .shard_configs()
            .map(|config| {
                let shard = ShardRef {
                    bucket: config.bucket_id.clone(),
                    shard: config.shard,
                };
                (shard, config)
            })
            .collect();
        assert_eq!(rebuilt.len(), shards, "{:?}", rebuild.plan.notes());
        assert_eq!(
            rebuild.applied.written + rebuild.applied.present,
            rebuild.plan.registers().len() + 1
        );

        // The rebuilt cluster served every shard as its register says.
        assert!(seen.done, "the window never passed: {seen:?}");
        assert_eq!(seen.serving.len(), shards, "{seen:?}");

        // Membership changes resumed on the rebuilt store: every shard of
        // node 3 moved on without it.
        let away = NodeId::new(format!("node-{}", AWAY + 1)).expect("a node ID");
        let mut changed = 0;
        for (shard, config) in &rebuilt {
            let now = &report.registers[shard];
            assert!(
                now.epoch >= config.epoch,
                "{shard}: {now:?} after {config:?}"
            );
            if config.is_member(&away) {
                assert!(
                    now.epoch > config.epoch && !now.is_member(&away),
                    "{shard} still names {away} after the rebuild: {now:?}"
                );
                changed += 1;
            }
        }
        assert!(changed > 0, "node 3 held no shard: {rebuilt:?}");
        // The first change came about as fast as a member's removal.
        let (Some(away), Some(first)) = (seen.away, seen.changed) else {
            panic!("no membership change after node 3 went down: {seen:?}");
        };
        let after = first - away;
        assert!(after < MEMBERSHIP_BOUND, "the first change took {after:?}");
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        Ok(())
    });
}
