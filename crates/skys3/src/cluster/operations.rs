//! The cluster routes of the admin API (design §6.7, §12; plan M8-06),
//! served by every node of a cluster:
//!
//! - `GET /v1/cluster`: who holds `coordinator.lease`, every registered
//!   node with its labels, its drain and departure marks, the shards that
//!   name it, and, on the coordinator, its health from heartbeats, and the
//!   coordinator's latest placement judgement.
//! - `POST /v1/shards/<bucket>/<n>/handoff`: a planned handoff of the
//!   shard's primary to the member the body names, or to the member that
//!   leads the fewest shards. Only the shard's primary starts a handoff,
//!   and it takes the request only from the coordinator or an `admin`
//!   certificate (threat T6). So the node that serves the call starts it
//!   itself if it is the primary, asks the primary for it if it is the
//!   coordinator, and otherwise answers `409` naming both.
//! - `POST` and `DELETE /v1/nodes/<id>/drain`: marks the node's
//!   registration `draining`, or clears the mark, by a compare-and-swap
//!   ([`set_draining`]). The coordinator then empties the node as it does
//!   a departing one, and with erasure coding the shard primaries move its
//!   fragments off (design §8.3). Any node serves it: the control store's
//!   credentials, which every node holds, are what allows it, as for every
//!   registration. A node reports in `GET /v1/cluster` the fragments its
//!   own stores hold, so the drained node tells when it is empty.
//!
//! The answers are JSON whose fields are only ever added.

use std::collections::BTreeMap;

use http::StatusCode;
use serde_json::{Value, json};
use skys3_cluster::Handoffs;
use skys3_config::FailureDomain;
use skys3_control::{
    ControlError, ControlStore, KeyPrefix, ProposalIds, RegisterKind, RetryPolicy, TypedKey,
    read_cluster, read_with_retries,
};
use skys3_coord::{
    ClusterScan, Domain, DrainError, Handoff, HandoffClient, HandoffError, HandoffSink, NodeEntry,
    NodeState, PolicyReport, set_draining,
};
use skys3_io::RealDisk;
use skys3_net::{TokioNetwork, Transport};
use skys3_types::{BucketDocument, ClusterId, NodeId, NodeRegistration, ShardConfig, ShardId};

use super::coding::CodingServices;
use super::coordinator::{CoordinatorView, HANDOFF_TIMEOUT};
use crate::admin::{ClusterApi, ClusterFuture};
use crate::control::backend::Backend;

/// An answer of a cluster route: its status and JSON body.
type Answer = (StatusCode, Value);

fn error(status: StatusCode, message: impl Into<String>) -> Answer {
    (status, json!({ "error": message.into() }))
}

/// The answer to a control store that failed.
fn unavailable(error: &ControlError) -> Answer {
    self::error(
        StatusCode::SERVICE_UNAVAILABLE,
        format!("the control store: {error}"),
    )
}

/// The cluster routes of a node of several.
pub(crate) struct ClusterOperations {
    node: NodeId,
    cluster: ClusterId,
    store: Backend,
    retry: RetryPolicy,
    coordinator: CoordinatorView,
    /// Starts handoffs on this node's primaries.
    handoffs: Handoffs<TokioNetwork, RealDisk>,
    /// Asks other nodes' primaries for handoffs, as the coordinator.
    client: HandoffClient<TokioNetwork>,
    /// The node's fragment stores, whose holdings it reports.
    coding: CodingServices,
}

impl std::fmt::Debug for ClusterOperations {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterOperations")
            .field("node", &self.node)
            .finish_non_exhaustive()
    }
}

/// Every registration under `nodes/`, by node ID. One that does not parse
/// is left out.
async fn registrations<S: ControlStore>(
    store: &S,
    retry: &RetryPolicy,
) -> Result<BTreeMap<NodeId, NodeRegistration>, ControlError> {
    let mut nodes = BTreeMap::new();
    for (key, _) in store.list(&KeyPrefix::nodes()).await? {
        let RegisterKind::Node(node) = key.kind() else {
            continue;
        };
        match read_with_retries(store, &TypedKey::node(&node), retry).await {
            Ok(Some(current)) => {
                nodes.insert(node, current.value);
            }
            Ok(None) | Err(ControlError::InvalidRegister { .. }) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(nodes)
}

/// How many shards name each node as member, learner, and primary.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Holdings {
    members: usize,
    learners: usize,
    primaries: usize,
}

fn holdings<'a>(shards: impl Iterator<Item = &'a ShardConfig>) -> BTreeMap<NodeId, Holdings> {
    let mut held: BTreeMap<NodeId, Holdings> = BTreeMap::new();
    for shard in shards {
        held.entry(shard.primary.clone()).or_default().primaries += 1;
        for member in &shard.members {
            held.entry(member.clone()).or_default().members += 1;
        }
        for learner in &shard.learners {
            held.entry(learner.clone()).or_default().learners += 1;
        }
    }
    held
}

/// The member of `config` other than its primary that a handoff goes to
/// when the caller names none: one whose node is registered, neither
/// draining nor departing, and judged live by `judged` (the coordinator's
/// registry; a node it does not judge is not chosen, since a handoff to a
/// silent member stops the shard until another's takeover), leading the
/// fewest shards, the lowest node ID among equals.
fn choose_target(
    config: &ShardConfig,
    nodes: &BTreeMap<NodeId, NodeRegistration>,
    led: &BTreeMap<NodeId, Holdings>,
    judged: impl Fn(&NodeId) -> Option<NodeState>,
) -> Option<NodeId> {
    config
        .members
        .iter()
        .filter(|member| **member != config.primary)
        .filter(|member| nodes.get(*member).is_some_and(|node| !node.is_leaving()))
        .filter(|member| judged(member) == Some(NodeState::Live))
        .min_by_key(|member| (led.get(*member).map_or(0, |held| held.primaries), *member))
        .cloned()
}

fn state_name(state: NodeState) -> &'static str {
    match state {
        NodeState::Live => "live",
        NodeState::Suspect => "suspect",
        NodeState::Departing => "departing",
    }
}

fn level_name(level: FailureDomain) -> &'static str {
    match level {
        FailureDomain::Node => "node",
        FailureDomain::Rack => "rack",
        FailureDomain::Zone => "zone",
    }
}

fn domain_name(domain: &Domain) -> &str {
    match domain {
        Domain::Node(node) => node.as_str(),
        Domain::Rack(label) | Domain::Zone(label) => label.as_str(),
    }
}

fn names(nodes: &[NodeId]) -> Vec<&str> {
    nodes.iter().map(NodeId::as_str).collect()
}

/// The placement judgement of design §12, as `GET /v1/health` and
/// `GET /v1/cluster` report it.
pub(crate) fn placement_json(report: &PolicyReport) -> Value {
    json!({
        "failure_domain": level_name(report.failure_domain),
        "eligible_nodes": report.eligible_nodes,
        "domains": report.domains,
        "unlabeled": names(&report.unlabeled),
        "unsatisfied": report.unsatisfied.iter().map(|bucket| json!({
            "name": bucket.name.as_str(),
            "bucket_id": bucket.bucket_id.as_str(),
            "replicas": bucket.replicas,
            "placeable": bucket.placeable,
            "short": bucket.short.iter().map(|short| json!({
                "shard": short.shard.get(),
                "members": names(&short.members),
                "domains": short.domains,
            })).collect::<Vec<_>>(),
            "co_located": bucket.co_located.iter().map(|co| json!({
                "shard": co.shard.get(),
                "domain": domain_name(&co.domain),
                "members": names(&co.members),
            })).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
    })
}

impl ClusterOperations {
    /// The cluster routes of node `node` of `cluster`, over `store` (the
    /// control store's own client), starting handoffs through `handoffs`,
    /// reaching other primaries through `transport`, and reporting what
    /// `coding`'s fragment stores hold.
    pub(crate) fn new(
        (node, cluster): (NodeId, ClusterId),
        store: Backend,
        coordinator: CoordinatorView,
        (handoffs, coding): (Handoffs<TokioNetwork, RealDisk>, CodingServices),
        transport: Transport<TokioNetwork>,
    ) -> Self {
        Self {
            node,
            cluster,
            store,
            retry: RetryPolicy::default(),
            coordinator,
            handoffs,
            client: HandoffClient::new(transport, HANDOFF_TIMEOUT),
            coding,
        }
    }

    /// What this node's fragment stores hold.
    fn fragments(&self) -> Value {
        let held = self.coding.held();
        json!({ "held": held.fragments, "unreleased": held.unreleased })
    }

    /// The coordinator's latest placement judgement, while this node is
    /// coordinator.
    fn judgement(&self) -> Option<Value> {
        if !self.coordinator.is_coordinator() {
            return None;
        }
        self.coordinator
            .placement
            .latest()
            .map(|report| placement_json(&report))
    }

    /// How the coordinator's registry judges `node`, while this node is
    /// coordinator.
    fn judged(&self, node: &NodeId) -> Option<NodeEntry> {
        self.coordinator
            .is_coordinator()
            .then(|| self.coordinator.registry.get(node))
            .flatten()
    }

    async fn cluster(&self) -> Answer {
        let lease =
            match read_with_retries(&self.store, &TypedKey::coordinator_lease(), &self.retry).await
            {
                Ok(lease) => lease,
                Err(error) => return unavailable(&error),
            };
        let generation = match read_cluster(&self.store, &self.cluster, &self.retry).await {
            Ok(current) => current.value.generation.get(),
            Err(error) => return unavailable(&error),
        };
        let nodes = match registrations(&self.store, &self.retry).await {
            Ok(nodes) => nodes,
            Err(error) => return unavailable(&error),
        };
        let mut scan = ClusterScan::new();
        if let Err(error) = scan.refresh(&self.store).await {
            return unavailable(&error);
        }
        let held = holdings(scan.shards());
        let nodes: Vec<Value> = nodes
            .values()
            .map(|registration| {
                let node = &registration.node_id;
                let held = held.get(node).copied().unwrap_or_default();
                let judged = self.judged(node);
                json!({
                    "node_id": node.as_str(),
                    "address": registration.address.to_string(),
                    "zone": registration.zone.as_ref().map(|zone| zone.as_str()),
                    "rack": registration.rack.as_ref().map(|rack| rack.as_str()),
                    "capacity_bytes": registration.disks.iter()
                        .fold(0_u64, |sum, disk| sum.saturating_add(disk.capacity_bytes)),
                    "draining": registration.draining,
                    "departing": registration.departing,
                    "state": judged.as_ref().map(|entry| state_name(entry.state)),
                    "silent_seconds": judged.as_ref().map(|entry| entry.silent_for.as_secs_f64()),
                    "primaries": held.primaries,
                    "members": held.members,
                    "learners": held.learners,
                })
            })
            .collect();
        let answer = json!({
            "node_id": self.node.as_str(),
            "generation": generation,
            "coordinator": {
                "holder": lease.as_ref().map(|lease| lease.value.holder.as_str()),
                "lease_version": lease.as_ref().map(|lease| lease.version.as_str()),
                "this_node": self.coordinator.is_coordinator(),
                // Every node of a cluster may be elected since plan M8-09;
                // kept, as answers only ever gain fields (§12).
                "candidate": true,
            },
            "nodes": nodes,
            "shards": scan.shards().count(),
            "placement": self.judgement(),
            "fragments": self.fragments(),
        });
        (StatusCode::OK, answer)
    }

    /// The refusal of a node that neither leads the shard nor coordinates,
    /// naming both.
    async fn bystander(&self, config: &ShardConfig) -> Answer {
        let holder = read_with_retries(&self.store, &TypedKey::coordinator_lease(), &self.retry)
            .await
            .ok()
            .flatten()
            .map(|lease| lease.value.holder);
        let primary = &config.primary;
        let mut answer = json!({
            "error": format!(
                "this node neither leads the shard nor is coordinator: ask the primary's \
                 ({primary}) or the coordinator's admin API"
            ),
            "primary": primary.as_str(),
            "coordinator": holder.as_ref().map(NodeId::as_str),
        });
        answer["epoch"] = json!(config.epoch.get());
        (StatusCode::CONFLICT, answer)
    }

    async fn hand_off(
        &self,
        bucket: &BucketDocument,
        shard: ShardId,
        to: Option<NodeId>,
    ) -> Answer {
        if u32::from(shard.get()) >= bucket.shards.get() {
            return error(
                StatusCode::NOT_FOUND,
                format!("bucket {} has {} shards", bucket.name, bucket.shards.get()),
            );
        }
        let key = TypedKey::shard(&bucket.bucket_id, shard);
        let config = match read_with_retries(&self.store, &key, &self.retry).await {
            Ok(Some(current)) => current.value,
            Ok(None) => {
                return error(
                    StatusCode::NOT_FOUND,
                    format!("shard {shard} of bucket {} has no register", bucket.name),
                );
            }
            Err(error) => return unavailable(&error),
        };
        let nodes = match registrations(&self.store, &self.retry).await {
            Ok(nodes) => nodes,
            Err(error) => return unavailable(&error),
        };
        let to = match to {
            Some(to) if to == config.primary => {
                return error(
                    StatusCode::CONFLICT,
                    format!("{to} already leads the shard in epoch {}", config.epoch),
                );
            }
            Some(to) if !config.is_member(&to) => {
                return error(
                    StatusCode::CONFLICT,
                    format!(
                        "{to} is not a member of the shard in epoch {}",
                        config.epoch
                    ),
                );
            }
            Some(to) => to,
            None if !self.coordinator.is_coordinator() => {
                // Only the coordinator's registry knows which members are
                // live: its primary asks the caller to name the member.
                if config.primary != self.node {
                    return self.bystander(&config).await;
                }
                return error(
                    StatusCode::CONFLICT,
                    "only the coordinator, which knows which members are live, chooses the \
                     member: name one ({\"to\": \"<node-id>\"}) or ask the coordinator",
                );
            }
            None => {
                let mut scan = ClusterScan::new();
                if let Err(error) = scan.refresh(&self.store).await {
                    return unavailable(&error);
                }
                let led = holdings(scan.shards());
                let judged = |node: &NodeId| self.judged(node).map(|entry| entry.state);
                let Some(to) = choose_target(&config, &nodes, &led, judged) else {
                    return error(
                        StatusCode::CONFLICT,
                        format!(
                            "the shard has no other live member to hand off to in epoch {}",
                            config.epoch
                        ),
                    );
                };
                to
            }
        };
        let handoff = Handoff {
            bucket: bucket.bucket_id.clone(),
            shard,
            epoch: config.epoch,
            to: to.clone(),
        };
        let primary = config.primary.clone();
        let via = if primary == self.node {
            if let Err(reason) = self.handoffs.hand_off(handoff).await {
                return error(StatusCode::CONFLICT, reason);
            }
            "primary"
        } else if self.coordinator.is_coordinator() {
            let Some(address) = nodes.get(&primary).map(|node| node.address.clone()) else {
                return error(
                    StatusCode::CONFLICT,
                    format!("the primary, {primary}, is not registered"),
                );
            };
            match self.client.send(&primary, &address, &handoff).await {
                Ok(()) => "coordinator",
                Err(HandoffError::Refused(reason)) => return error(StatusCode::CONFLICT, reason),
                Err(failed) => {
                    return error(StatusCode::SERVICE_UNAVAILABLE, failed.to_string());
                }
            }
        } else {
            return self.bystander(&config).await;
        };
        tracing::info!(bucket = %bucket.name, %shard, %primary, %to, via, "started a handoff an operator asked for");
        let answer = json!({
            "bucket": bucket.name.as_str(),
            "bucket_id": bucket.bucket_id.as_str(),
            "shard": shard.get(),
            "epoch": config.epoch.get(),
            "from": primary.as_str(),
            "to": to.as_str(),
            "started": true,
            "via": via,
        });
        (StatusCode::ACCEPTED, answer)
    }

    async fn drain(&self, node: &NodeId, draining: bool) -> Answer {
        let mut proposals = ProposalIds::from_os_rng();
        let drained = set_draining(
            &self.store,
            &self.cluster,
            node,
            draining,
            &mut proposals,
            &self.retry,
        )
        .await;
        match drained {
            Ok(drained) => {
                if drained.changed {
                    tracing::info!(%node, draining, "an operator changed the node's drain mark");
                }
                let answer = json!({
                    "node_id": node.as_str(),
                    "draining": drained.registration.draining,
                    "changed": drained.changed,
                    "generation": drained.generation.map(|generation| generation.get()),
                });
                (StatusCode::OK, answer)
            }
            Err(failed @ DrainError::Unregistered(_)) => {
                error(StatusCode::NOT_FOUND, failed.to_string())
            }
            Err(failed @ DrainError::Contended(_)) => {
                error(StatusCode::CONFLICT, failed.to_string())
            }
            Err(failed) => error(StatusCode::SERVICE_UNAVAILABLE, failed.to_string()),
        }
    }
}

impl ClusterApi for ClusterOperations {
    fn cluster(&self) -> ClusterFuture<'_> {
        Box::pin(self.cluster())
    }

    fn hand_off<'a>(
        &'a self,
        bucket: &'a BucketDocument,
        shard: ShardId,
        to: Option<NodeId>,
    ) -> ClusterFuture<'a> {
        Box::pin(self.hand_off(bucket, shard, to))
    }

    fn drain<'a>(&'a self, node: &'a NodeId, draining: bool) -> ClusterFuture<'a> {
        Box::pin(self.drain(node, draining))
    }

    fn placement(&self) -> Option<Value> {
        self.judgement()
    }
}

#[cfg(test)]
mod tests {
    use skys3_types::{BucketId, DiskInfo, Epoch, Label, ProposalId};

    use super::*;

    fn node(name: &str) -> NodeId {
        NodeId::new(name).unwrap()
    }

    fn config(primary: &str, members: &[&str]) -> ShardConfig {
        ShardConfig {
            bucket_id: BucketId::new("b-1").unwrap(),
            shard: ShardId::new(0),
            epoch: Epoch::new(4),
            primary: node(primary),
            members: members.iter().map(|name| node(name)).collect(),
            learners: Vec::new(),
            min_write_replicas: 2,
            replicas: 3,
            proposal_id: ProposalId::new("p").unwrap(),
        }
    }

    fn registration(name: &str) -> NodeRegistration {
        NodeRegistration {
            node_id: node(name),
            address: "127.0.0.1:7400".parse().unwrap(),
            zone: None,
            rack: None,
            disks: vec![DiskInfo {
                disk_id: Label::new("d").unwrap(),
                capacity_bytes: 1,
            }],
            instance_id: None,
            departing: false,
            draining: false,
            proposal_id: ProposalId::new("p").unwrap(),
        }
    }

    #[test]
    fn a_handoff_goes_to_the_member_that_leads_the_fewest_shards() {
        let shard = config("a", &["a", "b", "c", "d"]);
        let mut nodes: BTreeMap<NodeId, NodeRegistration> = ["a", "b", "c", "d"]
            .into_iter()
            .map(|name| (node(name), registration(name)))
            .collect();
        let shards = [
            config("b", &["b", "c"]),
            config("b", &["b", "c"]),
            config("c", &["c", "d"]),
            config("d", &["d", "a"]),
        ];
        let led = holdings(shards.iter());
        assert_eq!(
            led[&node("c")],
            Holdings {
                members: 3,
                learners: 0,
                primaries: 1
            }
        );
        // c and d lead one shard each; c sorts first.
        let live = |_: &NodeId| Some(NodeState::Live);
        assert_eq!(choose_target(&shard, &nodes, &led, live), Some(node("c")));
        // Without the coordinator's judgement, no member is chosen.
        assert_eq!(choose_target(&shard, &nodes, &led, |_| None), None);
        // A draining node, one the coordinator suspects, and one that is
        // not registered are passed over.
        nodes.get_mut(&node("c")).unwrap().draining = true;
        assert_eq!(choose_target(&shard, &nodes, &led, live), Some(node("d")));
        let suspect = |name: &NodeId| {
            Some(if *name == node("d") {
                NodeState::Suspect
            } else {
                NodeState::Live
            })
        };
        assert_eq!(
            choose_target(&shard, &nodes, &led, suspect),
            Some(node("b"))
        );
        nodes.remove(&node("b"));
        assert_eq!(choose_target(&shard, &nodes, &led, suspect), None);
        // A shard led alone has nobody to hand off to.
        assert_eq!(
            choose_target(&config("a", &["a"]), &nodes, &led, live),
            None
        );
    }

    #[test]
    fn the_placement_judgement_is_reported_as_the_design_names_it() {
        let report = PolicyReport {
            failure_domain: FailureDomain::Rack,
            eligible_nodes: 2,
            domains: 1,
            unlabeled: vec![node("u")],
            unsatisfied: vec![skys3_coord::BucketPolicy {
                bucket_id: BucketId::new("b-1").unwrap(),
                name: "photos".parse().unwrap(),
                replicas: 3,
                placeable: false,
                short: vec![skys3_coord::ShortShard {
                    shard: ShardId::new(1),
                    members: vec![node("a")],
                    domains: 1,
                }],
                co_located: vec![skys3_coord::CoLocated {
                    shard: ShardId::new(2),
                    domain: Domain::Rack(Label::new("r1").unwrap()),
                    members: vec![node("a"), node("b")],
                }],
            }],
        };
        assert_eq!(
            placement_json(&report),
            json!({
                "failure_domain": "rack",
                "eligible_nodes": 2,
                "domains": 1,
                "unlabeled": ["u"],
                "unsatisfied": [{
                    "name": "photos",
                    "bucket_id": "b-1",
                    "replicas": 3,
                    "placeable": false,
                    "short": [{"shard": 1, "members": ["a"], "domains": 1}],
                    "co_located": [{"shard": 2, "domain": "r1", "members": ["a", "b"]}],
                }],
            })
        );
        for (state, name) in [
            (NodeState::Live, "live"),
            (NodeState::Suspect, "suspect"),
            (NodeState::Departing, "departing"),
        ] {
            assert_eq!(state_name(state), name);
        }
        assert_eq!(level_name(FailureDomain::Node), "node");
        assert_eq!(level_name(FailureDomain::Zone), "zone");
        assert_eq!(domain_name(&Domain::Node(node("n"))), "n");
    }
}
