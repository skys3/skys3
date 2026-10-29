//! The node registry and lifecycle (plan M3-02, design §6.7): nodes join
//! with nothing but their credentials, and the coordinator forgets a node
//! silent for `node_forget_after` only once no shard names it.

use std::time::Duration;

use skys3_cluster_sim::{
    Cluster, ClusterConfig, CoordinatedServices, CoordinationConfig, Fault, FaultPlan,
    LocalServices, View, Workload,
};
use skys3_coord::{NodeState, RegistryConfig};
use skys3_sim::{Runner, SimContext};
use skys3_types::NodeId;

use crate::COST;

type Services = CoordinatedServices<LocalServices>;

/// Pushes reach a node well within the 500 ms the nodes poll at.
const PUSH_BOUND: Duration = Duration::from_millis(300);

/// How long a restarted node may take to register and be listed again.
const RESTART: Duration = Duration::from_secs(1);

/// The node at `position`.
fn node(position: usize) -> NodeId {
    NodeId::new(format!("node-{}", position + 1)).unwrap()
}

fn workload(context: &SimContext, think_time: Duration) -> Workload {
    Workload {
        clients: 4,
        operations: 100 * context.scale() as usize,
        think_time,
        ..Workload::default()
    }
}

/// Every node registers when it starts, and the coordinator's pushes,
/// which start with no node to go to, reach every node it then lists. The
/// registry holds what each node registered, and judges every node live.
#[test]
fn a_node_with_valid_credentials_joins_with_no_other_action() {
    Runner::with_cost(3, COST).run(|context| {
        let services = Services::new(LocalServices, CoordinationConfig::default());
        let cluster = ClusterConfig {
            nodes: 4,
            ..ClusterConfig::default()
        };
        Cluster::with_services(cluster, services.clone())
            .invariant(|view: &View<'_, Services>| view.services.observe(view))
            .run(
                context,
                &workload(context, Duration::from_millis(150)),
                &FaultPlan::none(),
            )?;
        services.check_changes()?;
        let pushes = services.check_pushes(PUSH_BOUND, RESTART)?;
        assert!(pushes.checked > 0, "no push was checked");

        let view = services.registry_view().ok_or("no coordinator planned")?;
        let listed: Vec<NodeId> = view
            .nodes
            .iter()
            .map(|entry| entry.registration.node_id.clone())
            .collect();
        assert_eq!(listed, (0..4).map(node).collect::<Vec<_>>());
        for (position, entry) in view.nodes.iter().enumerate() {
            let registration = &entry.registration;
            assert_eq!(
                registration
                    .rack
                    .as_ref()
                    .map(|rack| rack.as_str().to_owned()),
                Some(format!("rack-{}", position % 2))
            );
            assert_eq!(registration.disks.len(), cluster_disks());
            assert_eq!(entry.state, NodeState::Live, "{entry:?}");
        }
        assert!(services.forgotten().is_empty());
        Ok(())
    });
}

/// The disks every node has in the default cluster.
fn cluster_disks() -> usize {
    ClusterConfig::default().disks_per_node
}

/// Two nodes go down for longer than `node_forget_after`: node 3, which
/// no data shard names, and node 0, which holds shards of the static
/// placement. The coordinator forgets node 3 after its silence, and never
/// node 0, whose shards nothing re-homes. Node 3 registers again when it
/// returns, and pushes reach it again.
#[test]
fn a_silent_node_is_forgotten_only_once_no_shard_names_it() {
    let mut delays = Vec::new();
    Runner::with_cost(3, COST).run(|context| {
        let forget_after = Duration::from_secs(2);
        let config = CoordinationConfig {
            registry: RegistryConfig {
                suspect_after: Duration::from_millis(600),
                forget_after,
            },
            ..CoordinationConfig::default()
        };
        // One bucket of three shards on four nodes: node 3 holds none.
        let cluster = ClusterConfig {
            nodes: 4,
            buckets: 1,
            shards_per_bucket: 3,
            ..ClusterConfig::default()
        };
        let services = Services::new(LocalServices, config);
        // Long enough for the worst case: node 3 was coordinator, the lease
        // moves, and the new coordinator re-homes the stand-in registers
        // that name node 3 while each change waits out the push timeout
        // of the two nodes that are down.
        let (down_at, downtime) = (Duration::from_secs(1), Duration::from_secs(9));
        let plan = FaultPlan::none()
            .with(
                down_at,
                Fault::Crash {
                    node: 3,
                    power_loss: false,
                    downtime,
                },
            )
            .with(
                down_at,
                Fault::Crash {
                    node: 0,
                    power_loss: false,
                    downtime,
                },
            );
        Cluster::with_services(cluster, services.clone())
            .invariant(|view: &View<'_, Services>| view.services.observe(view))
            .run(
                context,
                &workload(context, Duration::from_millis(300)),
                &plan,
            )?;
        services.check_changes()?;
        services.check_pushes(PUSH_BOUND, RESTART)?;

        let forgotten = services.forgotten();
        assert!(
            forgotten.iter().all(|forgot| forgot.node == node(3)),
            "a node that holds shards was forgotten: {forgotten:?}"
        );
        let forgot = forgotten.first().ok_or("node 3 was never forgotten")?;
        let (crashed, _) = *services
            .downtime(3)
            .first()
            .ok_or("node 3 was never seen down")?;
        // The harness sees the crash at its next step, a little after it.
        let silent = forgot.at.saturating_sub(crashed);
        assert!(
            silent >= forget_after.mul_f64(0.99),
            "node 3 was forgotten after {silent:?} of silence"
        );
        delays.push(silent);

        // Node 3 came back and registered again, and the last coordinator
        // to plan lists every node.
        let registered = services.registrations(&node(3));
        assert!(
            registered.iter().any(|at| *at > forgot.at),
            "node 3 did not register again: {registered:?}"
        );
        let view = services.registry_view().ok_or("no coordinator planned")?;
        assert_eq!(view.nodes.len(), 4, "{view:?}");
        Ok(())
    });
    eprintln!(
        "node 3 was forgotten after {:?} of silence at most",
        delays.iter().max()
    );
}
