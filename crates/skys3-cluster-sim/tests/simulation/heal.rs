//! Heal tests (plan M3-08, the M3 exit criterion; design §6.4, §6.7,
//! §17): a cluster whose coordinator runs everything placement has (bucket
//! and shard upkeep, replacement, rebalancing, and forgetting silent
//! nodes) loses a node, gains one, loses a whole rack under
//! `failure_domain = "rack"`, or loses its coordinator in the middle of a
//! change, while clients write. No driver and no operator acts: the
//! cluster must heal by itself before the clients stop.
//!
//! Healed means, for the shard registers: every shard has `replicas`
//! members and no learner, every member is on a node that is still in the
//! cluster, and no two members share a domain at the `failure_domain`
//! level; where rebalancing can even the load, every node also holds its
//! share of members and primaries. A [`HealWatch`] samples the registers
//! every [`SAMPLE_EVERY`] of simulated time while the lost nodes stay
//! down, and the scenario checks that the last sample was healed, and
//! reports how long after the fault the cluster was healed for good. At
//! the end the harness checks the history, linearizable and every
//! acknowledged write durable on every member of the final
//! configurations, and the commit and R3 audits run after every step.
//!
//! The simpler cases have their own scenarios: a node lost for good in
//! [`replacement`](crate::replacement) and a node joining in
//! [`rebalancing`](crate::rebalancing). These run the whole lifecycle: a
//! lost node replaced, forgotten, and succeeded by a new one, which ends
//! with its share.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use skys3_cluster_sim::{
    ChangeStage, Cluster, ClusterConfig, CoordinatedServices, CoordinationConfig, CoordinatorLoss,
    Fault, FaultPlan, ReplicatedServices, RoutedServices, View, Workload,
};
use skys3_config::FailureDomain;
use skys3_coord::{RebalanceConfig, RegistryConfig, ReplacementConfig};
use skys3_gateway::ShardRef;
use skys3_gateway::routing::RoutingConfig;
use skys3_shard::replication::ReplicationConfig;
use skys3_sim::history::Outcome;
use skys3_sim::{Runner, SimContext};
use skys3_types::{NodeId, ShardConfig};

use crate::COST;
use crate::replacement::check_windows;

type Services = CoordinatedServices<RoutedServices>;

/// What one seed of these scenarios costs, in typical seeds: a cluster of
/// five or six nodes under the coordinator, writing for about half a
/// minute of simulated time, takes 30 to 60 s in a debug build, so CI's
/// fixed seed set runs one seed of each, and larger seed sets more.
const HEAL_COST: u64 = 8 * COST;

/// How often the [`HealWatch`] samples the shard registers.
const SAMPLE_EVERY: Duration = Duration::from_millis(100);

/// How long a node stays down when it is lost: for good, as far as the
/// workload is concerned. The driver restarts it once the clients stop.
const FOR_GOOD: Duration = Duration::from_secs(600);

/// `node_forget_after`: a lost node is forgotten once it has been silent
/// this long and no shard names it, which lets rebalancing, which moves
/// shards only while no node is suspect, even the load out again.
const FORGET_AFTER: Duration = Duration::from_secs(5);

/// How long a client waits for a connection to a node.
const CONNECT_TIMEOUT: Duration = Duration::from_millis(300);

/// The node at `position`.
fn node(position: usize) -> NodeId {
    NodeId::new(format!("node-{}", position + 1)).unwrap()
}

/// Replicated services behind routing gateways, whose members take over
/// from a lost primary, whose nodes follow the shard registers, and whose
/// coordinator keeps bucket registers matched by shard registers,
/// replaces lost members, and rebalances, at `level`, judging node health
/// with a `node_forget_after` of [`FORGET_AFTER`].
fn services(level: FailureDomain, racks: usize, loss: Option<CoordinatorLoss>) -> Services {
    let replicated = ReplicatedServices::new(ReplicationConfig::default())
        .with_takeover()
        .following_registers();
    let routing = RoutingConfig {
        connect_timeout: Duration::from_millis(300),
        request_timeout: Duration::from_secs(4),
        register_interval: Duration::from_millis(200),
        ..RoutingConfig::default()
    };
    let defaults = CoordinationConfig::default();
    let coordination = CoordinationConfig {
        bucket_shards: Some(level),
        replacement: Some(ReplacementConfig::default()),
        rebalancing: Some(RebalanceConfig::default()),
        registry: RegistryConfig {
            forget_after: FORGET_AFTER,
            ..defaults.registry
        },
        racks,
        lose_coordinator: loss,
        ..defaults
    };
    CoordinatedServices::new(RoutedServices::new(replicated, routing), coordination)
}

/// `nodes` nodes, `joining` of which join later, and two buckets, one
/// `write_back`, of four shards with three members each.
fn config(nodes: usize, joining: usize) -> ClusterConfig {
    ClusterConfig {
        nodes,
        joining,
        replicas: 3,
        write_back_buckets: 1,
        every_member_durable: true,
        ..ClusterConfig::default()
    }
}

/// Four clients that send `operations` operations each to any of the
/// initial nodes and wait out a takeover. They give up on a connection
/// after [`CONNECT_TIMEOUT`], as clients behind a load balancer would
/// skip a node that is down; otherwise each request drawn for a lost node
/// would wait out the whole timeout, and the clients would write little.
fn workload(context: &SimContext, operations: usize) -> Workload {
    Workload {
        clients: 4,
        operations: operations * context.scale() as usize,
        keys: 8,
        think_time: Duration::from_millis(120),
        timeout: Duration::from_secs(8),
        connect_timeout: Some(CONNECT_TIMEOUT),
        any_gateway: true,
        ..Workload::default()
    }
}

/// What the cluster must look like once healed.
#[derive(Clone, Debug)]
struct Healed {
    /// Members per shard.
    replicas: usize,
    /// Each node's domain at the `failure_domain` level.
    domains: BTreeMap<NodeId, String>,
    /// Whether every node in the cluster must hold its share of members
    /// and primaries: where rebalancing can even the load out.
    balanced: bool,
}

impl Healed {
    /// `nodes` nodes, every one its own domain, among which rebalancing
    /// evens the load out.
    fn by_node(nodes: usize) -> Self {
        let domains = (0..nodes).map(|p| (node(p), node(p).to_string())).collect();
        Self {
            replicas: 3,
            domains,
            balanced: true,
        }
    }

    /// `nodes` nodes in `racks` racks, the node at position `p` in rack
    /// `p mod racks`, as the harness labels them.
    fn by_rack(nodes: usize, racks: usize) -> Self {
        let domains = (0..nodes)
            .map(|p| (node(p), format!("rack-{}", p % racks)))
            .collect();
        Self {
            replicas: 3,
            domains,
            balanced: false,
        }
    }

    /// Checks that `registers` describe a healed cluster of the nodes
    /// `cluster`.
    fn check(
        &self,
        registers: &BTreeMap<ShardRef, ShardConfig>,
        cluster: &BTreeSet<NodeId>,
    ) -> Result<(), String> {
        if registers.is_empty() {
            return Err("no shard register yet".to_owned());
        }
        let mut loads: BTreeMap<&NodeId, (usize, usize)> =
            cluster.iter().map(|node| (node, (0, 0))).collect();
        for (shard, config) in registers {
            if config.members.len() != self.replicas || !config.learners.is_empty() {
                return Err(format!("{shard:?} is not whole: {config:?}"));
            }
            let mut domains = BTreeSet::new();
            for member in &config.members {
                let Some(load) = loads.get_mut(member) else {
                    return Err(format!("{shard:?} names {member}, not in the cluster"));
                };
                load.0 += 1;
                if !domains.insert(&self.domains[member]) {
                    return Err(format!(
                        "{shard:?} has two members in {}: {config:?}",
                        self.domains[member]
                    ));
                }
            }
            if let Some(load) = loads.get_mut(&config.primary) {
                load.1 += 1;
            }
        }
        if self.balanced {
            let share = |total: usize| (total / cluster.len())..=total.div_ceil(cluster.len());
            let members = share(registers.len() * self.replicas);
            let primaries = share(registers.len());
            if loads
                .values()
                .any(|(m, p)| !members.contains(m) || !primaries.contains(p))
            {
                return Err(format!(
                    "the load is uneven (members, primaries): {loads:?}"
                ));
            }
        }
        Ok(())
    }
}

/// Samples the shard registers while the lost nodes stay down, and
/// records whether the cluster was healed and since when.
#[derive(Debug)]
struct HealWatch {
    healed: Healed,
    /// The nodes in the cluster once it healed: the initial ones and those
    /// that joined, without the lost ones.
    cluster: BTreeSet<NodeId>,
    /// The positions of the nodes lost for good.
    lost: BTreeSet<usize>,
    /// When the fault struck.
    fault: Option<Duration>,
    /// When the next sample is due.
    next: Duration,
    /// The verdict of the latest sample taken while the lost nodes were
    /// down, and when it was taken.
    last: Option<(Duration, Result<(), String>)>,
    /// Since when every sample was healed, if the latest was.
    since: Option<Duration>,
    /// Whether a lost node came back, which ends the sampling.
    returned: bool,
}

impl HealWatch {
    fn new(healed: Healed, cluster: BTreeSet<NodeId>) -> Self {
        Self {
            healed,
            cluster,
            lost: BTreeSet::new(),
            fault: None,
            next: Duration::ZERO,
            last: None,
            since: None,
            returned: false,
        }
    }

    /// Records that the nodes at `lost` are lost for good, at `at`.
    fn lose(&mut self, lost: impl IntoIterator<Item = usize>, at: Duration) {
        for position in lost {
            self.lost.insert(position);
            self.cluster.remove(&node(position));
        }
        self.fault.get_or_insert(at);
    }

    /// Takes a sample, if one is due and the lost nodes are still down:
    /// once the driver restarts them at the end, the cluster may take them
    /// back.
    fn sample(&mut self, view: &View<'_, Services>) {
        self.returned |= self.lost.iter().any(|position| view.up[*position]);
        if view.elapsed < self.next || self.returned {
            return;
        }
        self.next = view.elapsed + SAMPLE_EVERY;
        let registers = view.services.inner().replicated().shard_registers();
        let verdict = self.healed.check(&registers, &self.cluster);
        let after_fault = self.fault.is_some_and(|fault| view.elapsed >= fault);
        self.since = match (&verdict, self.since) {
            (Ok(()), Some(since)) => Some(since),
            (Ok(()), None) if after_fault => Some(view.elapsed),
            _ => None,
        };
        self.last = Some((view.elapsed, verdict));
    }

    /// Checks that the cluster was healed at the latest sample, and
    /// returns how long after the fault it had been healed for good.
    fn check(&self) -> Duration {
        let fault = self.fault.expect("the fault struck");
        let Some((at, verdict)) = &self.last else {
            panic!("the registers were never sampled");
        };
        if let Err(error) = verdict {
            panic!("the cluster had not healed at {at:?}, the fault struck at {fault:?}: {error}");
        }
        let since = self.since.expect("a healed sample has a start");
        since.saturating_sub(fault)
    }
}

/// A cluster checked against the commit rule and R3 after every step,
/// sampled for its durability windows, and watched by `watch`, which
/// `lost` tells about lost nodes as they are lost.
fn cluster(
    config: ClusterConfig,
    services: &Services,
    watch: Arc<Mutex<HealWatch>>,
    mut lost: impl FnMut(&View<'_, Services>) -> Option<(Vec<usize>, Duration)> + 'static,
) -> Cluster<Services> {
    Cluster::with_services(config, services.clone()).invariant(move |view: &View<'_, Services>| {
        let replicated = view.services.inner().replicated();
        replicated.check_commits()?;
        replicated.check_members_hold_commits()?;
        replicated.sample_durability(view.elapsed)?;
        let mut watch = watch.lock().unwrap();
        if let Some((positions, at)) = lost(view) {
            watch.lose(positions, at);
        }
        watch.sample(view);
        Ok(())
    })
}

/// A shared [`HealWatch`].
fn watch(healed: Healed, cluster: BTreeSet<NodeId>) -> Arc<Mutex<HealWatch>> {
    Arc::new(Mutex::new(HealWatch::new(healed, cluster)))
}

/// A loss the fault plan schedules: the nodes at `positions`, recorded
/// the first time every one of them is seen down once the clients started
/// (a node may restart during startup, after a control-store fault).
fn crashed(
    positions: Vec<usize>,
) -> impl FnMut(&View<'_, Services>) -> Option<(Vec<usize>, Duration)> + 'static {
    let mut seen = false;
    move |view| {
        if seen || view.history.is_empty() || positions.iter().any(|position| view.up[*position]) {
            return None;
        }
        seen = true;
        Some((positions.clone(), view.elapsed))
    }
}

/// The lifecycle of a node: one of four loaded nodes is lost for good
/// while writes go on, and a new node joins a little later, as
/// replacement hardware would. The lost node's shards are replaced, many
/// onto the new node, the lost node is forgotten once no shard names it,
/// and rebalancing then gives the new node its full share: every shard
/// ends with three members on the four nodes left, each holding its share
/// of members and primaries.
#[test]
fn a_lost_node_is_replaced_forgotten_and_succeeded_by_a_new_one() {
    Runner::with_cost(1, HEAL_COST).run(|context| {
        let plan = FaultPlan::none()
            .with(
                Duration::from_secs(2),
                Fault::Crash {
                    node: 1,
                    power_loss: true,
                    downtime: FOR_GOOD,
                },
            )
            .with(Duration::from_secs(4), Fault::Join { node: 4 });
        let services = services(FailureDomain::Node, 2, None);
        let healed = Healed::by_node(5);
        let watch = watch(healed, (0..5).map(node).collect());
        let report = cluster(config(5, 1), &services, watch.clone(), crashed(vec![1])).run(
            context,
            &workload(context, 200),
            &plan,
        )?;
        let healed_after = watch.lock().unwrap().check();
        let replicated = services.inner().replicated();
        let windows = replicated.durability_windows();
        check_windows(&windows);
        let forgotten = services.forgotten();
        eprintln!(
            "healed {healed_after:?} after the loss; forgotten: {forgotten:?}; {:?}; {} shard \
             registers written by coordinators {:?}",
            replicated.learners(),
            services.shard_writes(),
            services.coordinators(),
        );
        assert!(forgotten.iter().any(|f| f.node == node(1)), "{forgotten:?}");
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        Ok(())
    });
}

/// `failure_domain = "rack"`: six nodes in four racks (`rack-0` and
/// `rack-1` with two nodes each), and buckets created through the S3 API,
/// whose gateways keep each shard's members in three different racks.
/// Both nodes of `rack-0` are lost at once, for good. Every shard that had
/// a member there gets a learner in the one rack it does not use, never
/// beside another member, and returns to three members in three racks.
#[test]
fn a_lost_rack_is_replaced_across_the_remaining_racks() {
    Runner::with_cost(1, HEAL_COST).run(|context| {
        const NODES: usize = 6;
        const RACKS: usize = 4;
        let mut plan = FaultPlan::none();
        let rack: Vec<usize> = (0..NODES).filter(|p| p % RACKS == 0).collect();
        for &position in &rack {
            let crash = Fault::Crash {
                node: position,
                power_loss: false,
                downtime: FOR_GOOD,
            };
            plan.push(Duration::from_secs(2), crash);
        }
        let config = ClusterConfig {
            create_buckets: true,
            failure_domain: FailureDomain::Rack,
            ..config(NODES, 0)
        };
        let services = services(FailureDomain::Rack, RACKS, None);
        let watch = watch(
            Healed::by_rack(NODES, RACKS),
            (0..NODES).map(node).collect(),
        );
        let report = cluster(config, &services, watch.clone(), crashed(rack)).run(
            context,
            &workload(context, 200),
            &plan,
        )?;
        let healed_after = watch.lock().unwrap().check();
        let replicated = services.inner().replicated();
        let windows = replicated.durability_windows();
        check_windows(&windows);
        eprintln!(
            "healed {healed_after:?} after the rack loss; {:?}; {} shard registers written by \
             coordinators {:?}",
            replicated.learners(),
            services.shard_writes(),
            services.coordinators(),
        );
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        Ok(())
    });
}

/// A node joins four loaded nodes, and the coordinator is lost in the
/// middle of the first change of the moves it plans: right after some of
/// its writes landed, learners added whose promotion and removal steps
/// are still to come (even seeds, so CI's single seed runs this case), or
/// right after planning it, while its writes are in flight or before the
/// generation increment that announces them (odd seeds). It loses power
/// and stays down for 2 s, longer than the lease takes to move (seeds
/// `4k` and `4k + 1`), or for good (seeds `4k + 2` and `4k + 3`), when
/// its shards are replaced and it is forgotten. Another node takes the
/// lease, finds the moves under way in the registers, completes them, and
/// the cluster ends balanced.
#[test]
fn a_coordinator_lost_in_the_middle_of_a_change_is_succeeded() {
    Runner::with_cost(2, HEAL_COST).run(|context| {
        let seed = context.seed();
        let stage = if seed % 2 == 0 {
            ChangeStage::Applied
        } else {
            ChangeStage::Planned
        };
        let for_good = seed % 4 >= 2;
        let downtime = if for_good {
            FOR_GOOD
        } else {
            Duration::from_secs(2)
        };
        let loss = CoordinatorLoss {
            stage,
            power_loss: true,
            downtime,
        };
        let plan = FaultPlan::none().with(Duration::from_secs(3), Fault::Join { node: 4 });
        let services = services(FailureDomain::Node, 2, Some(loss));
        let watch = watch(Healed::by_node(5), (0..5).map(node).collect());
        let lost = move |view: &View<'_, Services>| {
            let lost = view.services.lost_coordinator()?;
            let positions = if for_good {
                vec![lost.position]
            } else {
                Vec::new()
            };
            Some((positions, lost.at))
        };
        let report = cluster(config(5, 1), &services, watch.clone(), lost).run(
            context,
            &workload(context, 300),
            &plan,
        )?;
        let healed_after = watch.lock().unwrap().check();
        let lost = services
            .lost_coordinator()
            .expect("the coordinator was lost");
        let coordinators = services.coordinators();
        eprintln!(
            "lost {lost:?}; healed {healed_after:?} after the loss; coordinators {coordinators:?}; \
             forgotten: {:?}; {:?}",
            services.forgotten(),
            services.inner().replicated().learners(),
        );
        // Another node took over and finished the work.
        assert!(
            coordinators.iter().any(|c| *c != lost.node),
            "{coordinators:?}"
        );
        if for_good {
            let forgotten = services.forgotten();
            assert!(
                forgotten.iter().any(|f| f.node == lost.node),
                "{forgotten:?}"
            );
        }
        assert!(report.count(|o| *o == Outcome::Done) > 0);
        Ok(())
    });
}
