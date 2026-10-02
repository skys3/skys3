//! The coordinator's node registry and the node lifecycle (design §6.7).
//!
//! The registry is the coordinator's view of the nodes: every registration
//! under `nodes/`, read from the control store, and how long each node has
//! been silent, from the heartbeats it sends the coordinator. Health is
//! advice only. It may steer placement away from a node, but it is never
//! taken as proof that a node is dead: shard failover never depends on it
//! (design §6.3, §6.4), and a node is forgotten only once nothing the
//! cluster stores still names it.
//!
//! ```text
//!   registers ──> Live ──(silent for suspect_after)──> Suspect
//!                  ^                                       │
//!                  └──────────────(heard)──────────────────┤
//!                                                (silent for node_forget_after)
//!                                                          v
//!                          Departing <──(mark: CAS departing = true)
//!                              │
//!   forgotten <──(a later round finds no shard naming it: delete by If-Match)
//! ```
//!
//! Silence is counted on the coordinator's own clock, from the latest of
//! the node's last heartbeat, the registry first listing its current
//! registration, and the start of the coordinator's tenure: a coordinator
//! that has just taken over has heard nothing yet, and so suspects no one
//! until it has listened for `suspect_after`.
//!
//! [`Lifecycle`] runs the registry inside the coordinator: it wraps the
//! coordinator's [`Placement`], refreshes the registry before each plan,
//! and hands the nodes' addresses to the [`Pusher`] ([`PeerSink`]).
//! Forgetting takes two changes. The first marks the registration
//! `departing`, by a compare-and-swap on it, which every placement that
//! reads the registry afterwards sees: it assigns the node nothing more,
//! and re-homes what the node holds (plan M3-03, M3-05). Only a later
//! round, whose [`Rehoming`] scan starts after the mark landed, deletes
//! the registration, under `If-Match` on the marked version, and only if
//! no shard names the node. A shard assignment and the delete touch
//! different registers, so no single compare-and-swap orders them: the
//! mark makes a planner that read the node as assignable one that started
//! before the mark, and the rescan catches every assignment that landed
//! before it.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use skys3_control::{
    ControlError, ControlStore, KeyPrefix, ProposalIds, RegisterKey, RegisterKind, TypedKey,
    Version, read,
};
use skys3_io::{Clock, MonoTime};
use skys3_net::Network;
use skys3_types::{NodeAddress, NodeId, NodeRegistration, RegisterDocument, ShardConfig};
use tokio::sync::Notify;

use crate::change::{Applied, ChangeError, ChangeSet};
use crate::coordinator::Placement;
use crate::push::Pusher;

/// How the coordinator judges node health.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegistryConfig {
    /// How long a node may stay silent before it is [`NodeState::Suspect`].
    pub suspect_after: Duration,
    /// `node_forget_after`: how long a node may stay silent before it is
    /// [`NodeState::Departing`] and its shards are re-homed.
    pub forget_after: Duration,
}

impl RegistryConfig {
    /// The default `suspect_after`: a few missed heartbeats.
    pub const DEFAULT_SUSPECT_AFTER: Duration = Duration::from_secs(10);

    /// A configuration that forgets nodes after `forget_after`, and
    /// suspects them after [`RegistryConfig::DEFAULT_SUSPECT_AFTER`] or
    /// `forget_after`, whichever is shorter.
    #[must_use]
    pub fn new(forget_after: Duration) -> Self {
        Self {
            suspect_after: Self::DEFAULT_SUSPECT_AFTER.min(forget_after),
            forget_after,
        }
    }

    /// The configuration `node_forget_after_hours` gives.
    #[must_use]
    pub fn from_config(config: &skys3_config::Config) -> Self {
        Self::new(config.replication().node_forget_after())
    }
}

/// Where a registered node is in its lifecycle, as the coordinator judges
/// it from heartbeats. Advice only (design §6.7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NodeState {
    /// Heard from within `suspect_after`.
    Live,
    /// Silent for `suspect_after`. Placement may prefer other nodes, but
    /// nothing is taken from this one.
    Suspect,
    /// Silent for `node_forget_after`, or its registration carries the
    /// `departing` mark. Placement assigns it nothing and re-homes its
    /// shards, and the node is forgotten once no shard names it.
    Departing,
}

/// A registered node, as [`NodeRegistry::entries`] reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeEntry {
    /// The node's registration.
    pub registration: NodeRegistration,
    /// The version the registry read it at.
    pub version: Version,
    /// The node's lifecycle state.
    pub state: NodeState,
    /// How long the node has been silent, as far as this coordinator knows.
    pub silent_for: Duration,
}

/// A registration the registry listed.
#[derive(Debug, Clone)]
struct Known {
    registration: NodeRegistration,
    version: Version,
    /// When the registry first saw this version: a new registration counts
    /// as hearing from the node.
    seen: MonoTime,
}

#[derive(Debug, Default)]
struct State {
    nodes: BTreeMap<NodeId, Known>,
    /// When each node last sent a heartbeat.
    heard: BTreeMap<NodeId, MonoTime>,
    /// When this node's current tenure as coordinator began.
    tenure: Option<MonoTime>,
    /// Whether `nodes` comes from a listing made in the current tenure.
    listed: bool,
    /// Wakes the coordinator when an unknown node sends a heartbeat.
    wake: Option<Arc<Notify>>,
}

/// The coordinator's registry of nodes: their registrations and their
/// health. Clones share it, so the heartbeat endpoint
/// ([`AdminEndpoint`](crate::AdminEndpoint)) records into the registry the
/// coordinator's [`Lifecycle`] refreshes, and placement reads it.
#[derive(Clone)]
pub struct NodeRegistry {
    clock: Arc<dyn Clock>,
    config: RegistryConfig,
    state: Arc<Mutex<State>>,
}

impl std::fmt::Debug for NodeRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeRegistry")
            .field("config", &self.config)
            .field("state", &*self.state())
            .finish_non_exhaustive()
    }
}

impl NodeRegistry {
    /// An empty registry that judges health by `config` on `clock`.
    #[must_use]
    pub fn new(clock: Arc<dyn Clock>, config: RegistryConfig) -> Self {
        Self {
            clock,
            config,
            state: Arc::default(),
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// How the registry judges health.
    #[must_use]
    pub fn config(&self) -> RegistryConfig {
        self.config
    }

    /// Wakes `coordinator` whenever a node the registry does not list
    /// sends a heartbeat, so that it lists the nodes again before its idle
    /// wait ends ([`Coordinator::waker`](crate::Coordinator::waker)).
    pub fn set_waker(&self, coordinator: Arc<Notify>) {
        self.state().wake = Some(coordinator);
    }

    /// Records a heartbeat from `node`. Returns `false` if the registry's
    /// latest listing, made in this tenure, has no registration for the
    /// node, or one marked `departing`: the node must register again,
    /// which clears the mark. Before that listing, every node counts as
    /// registered.
    pub fn heard(&self, node: &NodeId) -> bool {
        let now = self.clock.now();
        let mut state = self.state();
        state.heard.insert(node.clone(), now);
        let registered = !state.listed
            || state
                .nodes
                .get(node)
                .is_some_and(|known| !known.registration.departing);
        if !registered && let Some(wake) = &state.wake {
            wake.notify_one();
        }
        registered
    }

    /// Whether the registry's nodes come from a listing made in this
    /// node's current tenure as coordinator. Before that, a node it does not
    /// list may still be registered.
    #[must_use]
    pub fn is_listed(&self) -> bool {
        self.state().listed
    }

    /// Every registered node, with its state, by node ID.
    #[must_use]
    pub fn entries(&self) -> Vec<NodeEntry> {
        let now = self.clock.now();
        let state = self.state();
        state
            .nodes
            .iter()
            .map(|(node, known)| self.entry(&state, node, known, now))
            .collect()
    }

    /// The registered node `node`, if the registry lists it.
    #[must_use]
    pub fn get(&self, node: &NodeId) -> Option<NodeEntry> {
        let now = self.clock.now();
        let state = self.state();
        let known = state.nodes.get(node)?;
        Some(self.entry(&state, node, known, now))
    }

    /// Every registered node's transport address.
    #[must_use]
    pub fn peers(&self) -> BTreeMap<NodeId, NodeAddress> {
        self.state()
            .nodes
            .iter()
            .map(|(node, known)| (node.clone(), known.registration.address.clone()))
            .collect()
    }

    fn entry(&self, state: &State, node: &NodeId, known: &Known, now: MonoTime) -> NodeEntry {
        let since = [state.heard.get(node).copied(), state.tenure]
            .into_iter()
            .flatten()
            .fold(known.seen, MonoTime::max);
        let silent_for = now.saturating_duration_since(since);
        let state = if silent_for >= self.config.forget_after || known.registration.departing {
            NodeState::Departing
        } else if silent_for >= self.config.suspect_after {
            NodeState::Suspect
        } else {
            NodeState::Live
        };
        NodeEntry {
            registration: known.registration.clone(),
            version: known.version.clone(),
            state,
            silent_for,
        }
    }

    /// Starts a tenure as coordinator: health is judged afresh, since
    /// heartbeats went to another coordinator before, and the nodes are
    /// listed again before any node is told it is not registered.
    pub(crate) fn begin_tenure(&self) {
        let now = self.clock.now();
        let mut state = self.state();
        state.tenure = Some(now);
        state.listed = false;
    }

    /// Lists `nodes/` and reads every registration whose version changed
    /// since the last refresh. A registration that does not parse is left
    /// out, and logged.
    ///
    /// # Errors
    ///
    /// The store's errors.
    pub(crate) async fn refresh<S: ControlStore>(&self, store: &S) -> Result<(), ControlError> {
        let listed = store.list(&KeyPrefix::nodes()).await?;
        let mut nodes = BTreeMap::new();
        for (key, version) in listed {
            let RegisterKind::Node(node) = key.kind() else {
                continue;
            };
            let cached = self
                .state()
                .nodes
                .get(&node)
                .filter(|known| known.version == version)
                .cloned();
            if let Some(known) = cached {
                nodes.insert(node, known);
                continue;
            }
            match read(store, &TypedKey::node(&node)).await {
                Ok(Some(current)) if current.value.node_id == node => {
                    // A new registration counts as hearing from the node,
                    // but a coordinator's `departing` mark does not.
                    let seen = self
                        .state()
                        .nodes
                        .get(&node)
                        .filter(|_| current.value.departing)
                        .map_or_else(|| self.clock.now(), |known| known.seen);
                    let known = Known {
                        registration: current.value,
                        version: current.version,
                        seen,
                    };
                    nodes.insert(node, known);
                }
                Ok(Some(current)) => tracing::warn!(
                    %key,
                    registered = %current.value.node_id,
                    "a node register names another node; ignoring it"
                ),
                // Deleted since the listing.
                Ok(None) => {}
                Err(error @ ControlError::InvalidRegister { .. }) => {
                    tracing::warn!(%error, "ignoring a node register that does not parse");
                }
                Err(error) => return Err(error),
            }
        }
        let mut state = self.state();
        state.nodes = nodes;
        state.listed = true;
        let State { nodes, heard, .. } = &mut *state;
        heard.retain(|node, _| nodes.contains_key(node));
        Ok(())
    }

    /// The nodes silent for `node_forget_after`.
    fn silent(&self) -> Vec<NodeEntry> {
        self.entries()
            .into_iter()
            .filter(|entry| entry.silent_for >= self.config.forget_after)
            .collect()
    }

    /// Drops `node` once its registration, at `version`, is deleted.
    fn forgot(&self, node: &NodeId, version: &Version) {
        let mut state = self.state();
        if state
            .nodes
            .get(node)
            .is_some_and(|known| known.version == *version)
        {
            state.nodes.remove(node);
            state.heard.remove(node);
        }
    }
}

/// Where the coordinator's pushes go: kept current with every registered
/// node's address.
pub trait PeerSink: Send + Sync + 'static {
    /// Replaces the nodes pushes go to.
    fn set_peers(&self, peers: BTreeMap<NodeId, NodeAddress>);
}

impl<N: Network> PeerSink for Pusher<N> {
    fn set_peers(&self, peers: BTreeMap<NodeId, NodeAddress>) {
        Pusher::set_peers(self, peers);
    }
}

/// Decides when a departing node's shards are re-homed, so that it can be
/// forgotten: the hook between the node lifecycle and placement.
pub trait Rehoming: Send + 'static {
    /// The nodes of `departing` that something the cluster stores still
    /// names, so that forgetting them would lose where it lives.
    ///
    /// # Errors
    ///
    /// The store's errors. The coordinator forgets no one this round.
    fn still_needed<S: ControlStore>(
        &mut self,
        store: &S,
        departing: &BTreeSet<NodeId>,
    ) -> impl Future<Output = Result<BTreeSet<NodeId>, ControlError>> + Send;
}

/// The [`Rehoming`] of shard replicas: a node is still needed while any
/// shard register names it as primary, member, or learner.
///
/// It lists `shards/` and reads only the registers whose version changed
/// since its last scan. A register that does not parse keeps every
/// departing node: what it names is unknown. Erasure-coded fragments
/// (design §8) add their own check when they arrive.
#[derive(Debug, Default)]
pub struct ShardScan {
    /// Each shard register's version, and the nodes it names.
    named: BTreeMap<RegisterKey, (Version, Vec<NodeId>)>,
}

impl Rehoming for ShardScan {
    async fn still_needed<S: ControlStore>(
        &mut self,
        store: &S,
        departing: &BTreeSet<NodeId>,
    ) -> Result<BTreeSet<NodeId>, ControlError> {
        let listed = store.list(&KeyPrefix::shards()).await?;
        let mut named = BTreeMap::new();
        let mut needed = BTreeSet::new();
        for (key, version) in listed {
            if !matches!(key.kind(), RegisterKind::Shard(..)) {
                continue;
            }
            let entry = match self.named.remove(&key) {
                Some(cached) if cached.0 == version => cached,
                _ => {
                    let Some(current) = store.get(&key).await? else {
                        continue;
                    };
                    match ShardConfig::from_json(&current.value) {
                        Ok(config) => (current.version, names(config)),
                        Err(error) => {
                            tracing::warn!(%key, %error, "a shard register does not parse; forgetting no node");
                            return Ok(departing.clone());
                        }
                    }
                }
            };
            needed.extend(
                entry
                    .1
                    .iter()
                    .filter(|node| departing.contains(*node))
                    .cloned(),
            );
            named.insert(key, entry);
        }
        self.named = named;
        Ok(needed)
    }
}

/// Every node a shard configuration names.
fn names(config: ShardConfig) -> Vec<NodeId> {
    let mut names: Vec<NodeId> = std::iter::once(config.primary)
        .chain(config.members)
        .chain(config.learners)
        .collect();
    names.sort();
    names.dedup();
    names
}

/// The node lifecycle as a [`Placement`]: before each plan of the
/// placement it wraps, it refreshes the [`NodeRegistry`], hands every
/// registered node's address to its [`PeerSink`], and forgets nodes silent
/// for `node_forget_after`, in two changes made in different rounds:
///
/// 1. It marks the node's registration `departing`, by a compare-and-swap
///    on the version it read. From then on every placement that reads the
///    registry assigns the node nothing, and re-homes what it holds.
/// 2. In a later round, whose listing read the mark, so that its
///    [`Rehoming`] scan starts after the mark landed, it deletes the
///    registration under `If-Match` on the marked version, once no shard
///    names the node.
///
/// A node that returns clears the mark by registering again (its next
/// heartbeat's answer tells it to), which also changes the version, so it
/// is not deleted. One forgotten while it still runs, for example after a
/// long partition, learns it the same way and registers again.
pub struct Lifecycle<P, R = ShardScan> {
    registry: NodeRegistry,
    placement: P,
    rehoming: R,
    peers: Option<Box<dyn PeerSink>>,
    /// The lifecycle change being made, if the change is one.
    step: Option<Step>,
}

/// A change the lifecycle makes.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Step {
    /// Marks the node's registration `departing`.
    Mark(NodeId),
    /// Deletes the node's registration, at the marked version.
    Forget(NodeId, Version),
}

impl<P, R> std::fmt::Debug for Lifecycle<P, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lifecycle")
            .field("registry", &self.registry)
            .field("step", &self.step)
            .finish_non_exhaustive()
    }
}

impl<P: Placement> Lifecycle<P> {
    /// The lifecycle of the nodes in `registry`, around `placement`,
    /// forgetting a node once no shard register names it ([`ShardScan`]).
    pub fn new(registry: NodeRegistry, placement: P) -> Self {
        Self {
            registry,
            placement,
            rehoming: ShardScan::default(),
            peers: None,
            step: None,
        }
    }
}

impl<P: Placement, R: Rehoming> Lifecycle<P, R> {
    /// Decides with `rehoming` when a departing node can be forgotten.
    pub fn with_rehoming<T: Rehoming>(self, rehoming: T) -> Lifecycle<P, T> {
        Lifecycle {
            registry: self.registry,
            placement: self.placement,
            rehoming,
            peers: self.peers,
            step: self.step,
        }
    }

    /// Keeps `peers` current with every registered node's address.
    #[must_use]
    pub fn with_peers(mut self, peers: impl PeerSink) -> Self {
        self.peers = Some(Box::new(peers));
        self
    }

    /// The registry.
    #[must_use]
    pub fn registry(&self) -> &NodeRegistry {
        &self.registry
    }

    /// The next change of forgetting a silent node, if one is due: a mark
    /// first, and a delete only in a later round.
    async fn forget<S: ControlStore>(
        &mut self,
        store: &S,
        proposals: &mut ProposalIds,
    ) -> Result<Option<ChangeSet>, ControlError> {
        let silent = self.registry.silent();
        let invalid = |error: ChangeError| ControlError::Rejected(error.to_string());
        if let Some(entry) = silent.iter().find(|entry| !entry.registration.departing) {
            let node = entry.registration.node_id.clone();
            let mut marked = entry.registration.clone();
            marked.departing = true;
            marked.proposal_id = proposals.next_id();
            let change = ChangeSet::new()
                .update(&TypedKey::node(&node), &entry.version, &marked)
                .map_err(invalid)?;
            tracing::info!(%node, "marking a silent node departing");
            self.step = Some(Step::Mark(node));
            return Ok(Some(change));
        }
        // This round's listing read every mark below, so each landed
        // before the scan starts.
        let marked: BTreeMap<NodeId, Version> = silent
            .into_iter()
            .map(|entry| (entry.registration.node_id, entry.version))
            .collect();
        if marked.is_empty() {
            return Ok(None);
        }
        let nodes = marked.keys().cloned().collect();
        let needed = self.rehoming.still_needed(store, &nodes).await?;
        let Some((node, version)) = marked.into_iter().find(|(node, _)| !needed.contains(node))
        else {
            return Ok(None);
        };
        let change = ChangeSet::new()
            .delete(&TypedKey::node(&node), &version)
            .map_err(invalid)?;
        tracing::info!(%node, "forgetting a departing node that holds no shard");
        self.step = Some(Step::Forget(node, version));
        Ok(Some(change))
    }
}

impl<P: Placement, R: Rehoming> Placement for Lifecycle<P, R> {
    fn begin_tenure(&mut self) {
        self.registry.begin_tenure();
        self.placement.begin_tenure();
    }

    async fn plan<S: ControlStore>(
        &mut self,
        store: &S,
        proposals: &mut ProposalIds,
    ) -> Result<Option<ChangeSet>, ControlError> {
        self.step = None;
        self.registry.refresh(store).await?;
        if let Some(peers) = &self.peers {
            peers.set_peers(self.registry.peers());
        }
        if let Some(change) = self.forget(store, proposals).await? {
            return Ok(Some(change));
        }
        self.placement.plan(store, proposals).await
    }

    fn applied(&mut self, change: &ChangeSet, applied: &Applied) {
        match self.step.take() {
            None => self.placement.applied(change, applied),
            Some(Step::Mark(node) | Step::Forget(node, _)) if !applied.is_complete() => {
                tracing::info!(%node, "a node's registration changed under its departure");
            }
            Some(Step::Forget(node, version)) => {
                tracing::info!(%node, "forgot a node");
                self.registry.forgot(&node, &version);
            }
            // The next listing reads the mark.
            Some(Step::Mark(_)) => {}
        }
    }
}

#[cfg(test)]
mod tests;
