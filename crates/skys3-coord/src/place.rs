//! The placement engine (design §6.7): a pure function from the nodes,
//! their labels and capacity, and a bucket's policy to the members of its
//! shards.
//!
//! A [`Topology`] is the cluster as placement sees it: every registered
//! node as a [`Candidate`], with its `zone` and `rack` labels, the bytes
//! its disks offer, its health, and the shards it already holds, and the
//! `failure_domain` level the cluster keeps members apart at. Each node
//! belongs to one [`Domain`] at that level. The engine keeps one rule
//! without exception:
//!
//! - **At most one member of a shard per domain.** A shard that cannot get
//!   `replicas` members in separate domains gets fewer ([`Placed::short`]);
//!   it is never co-located to make up the difference.
//!
//! Within that rule it prefers, in order: live nodes over suspect ones,
//! nodes holding fewer shards for their capacity, nodes whose zone and
//! rack hold fewer of the shard's members (a soft spread below the
//! required level), and finally a hash of the shard and the node, so that
//! equal nodes share new shards evenly and the choice never depends on
//! the order nodes were listed in.
//!
//! A node is **eligible** for new members when it is not departing, offers
//! some capacity, and has the label its level needs: a node without a
//! `rack` label cannot be shown to be apart from any rack, so at the
//! `rack` level it is never chosen (and likewise for `zone`). Rack labels
//! are names across the whole cluster: two racks that share a name in
//! different zones count as one domain, which can only make placement
//! more cautious.
//!
//! How later tasks use it:
//!
//! - Creating a bucket (plan M3-04) first runs [`Topology::check_bucket`],
//!   which rejects a policy the cluster cannot satisfy, then
//!   [`Topology::place_bucket`] for the members and primary of every shard.
//! - Replacing a lost member (plan M3-05, [`Replacement`](crate::Replacement))
//!   calls [`Topology::place`] with the shard's members and learners that
//!   count in [`ShardRequest::keep`] and every other node the shard names
//!   in [`ShardRequest::avoid`]. Once no shard names a departing node, the
//!   node lifecycle's [`Rehoming`](crate::Rehoming) check lets the
//!   coordinator forget it.
//! - Fragment placement ([`FragmentPlanner`](crate::FragmentPlanner))
//!   reuses the same domains and eligibility, with its own per-domain cap
//!   of `m` fragments of a stripe.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use skys3_config::{BucketSettings, FailureDomain};
use skys3_types::{BucketId, Label, NodeId, NodeRegistration, ShardConfig, ShardCount, ShardId};

use crate::registry::{NodeEntry, NodeRegistry, NodeState};

/// The configuration spelling of a failure-domain level.
pub(crate) const fn level_name(level: FailureDomain) -> &'static str {
    match level {
        FailureDomain::Node => "node",
        FailureDomain::Rack => "rack",
        FailureDomain::Zone => "zone",
    }
}

/// A failure domain at the `failure_domain` level: what at most one member
/// of a shard may live in.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Domain {
    /// A node, at the `node` level.
    Node(NodeId),
    /// A rack, by its label, at the `rack` level.
    Rack(Label),
    /// A zone, by its label, at the `zone` level.
    Zone(Label),
}

impl Domain {
    /// The domain at `level` of the node `node` with the labels `zone` and
    /// `rack`, or `None` if the node lacks the label the level needs.
    #[must_use]
    pub fn of(
        level: FailureDomain,
        node: &NodeId,
        zone: Option<&Label>,
        rack: Option<&Label>,
    ) -> Option<Self> {
        match level {
            FailureDomain::Node => Some(Self::Node(node.clone())),
            FailureDomain::Rack => rack.cloned().map(Self::Rack),
            FailureDomain::Zone => zone.cloned().map(Self::Zone),
        }
    }
}

impl fmt::Display for Domain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Node(node) => write!(f, "node {node}"),
            Self::Rack(rack) => write!(f, "rack {rack}"),
            Self::Zone(zone) => write!(f, "zone {zone}"),
        }
    }
}

/// A node as placement sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    /// The node.
    pub node: NodeId,
    /// Its zone label, if it has one.
    pub zone: Option<Label>,
    /// Its rack label, if it has one.
    pub rack: Option<Label>,
    /// The bytes its disks offer. A node that offers none is never chosen.
    pub capacity_bytes: u64,
    /// Its health, as the coordinator judges it. Departing nodes are never
    /// chosen, and suspect ones only when no live node will do.
    pub state: NodeState,
    /// The shards that name it as a member or learner.
    pub shards: u32,
    /// The shards that name it as primary.
    pub primaries: u32,
}

impl Candidate {
    /// A node that holds no shard yet, as `registration` describes it: for
    /// a node that judges placement without a registry, such as a gateway
    /// checking a new bucket's policy. It is live, unless the coordinator
    /// has marked the registration `departing`: then it is
    /// [`NodeState::Departing`], as the registry judges it too, and
    /// receives nothing.
    #[must_use]
    pub fn from_registration(registration: &NodeRegistration) -> Self {
        let state = if registration.departing {
            NodeState::Departing
        } else {
            NodeState::Live
        };
        Self {
            node: registration.node_id.clone(),
            zone: registration.zone.clone(),
            rack: registration.rack.clone(),
            capacity_bytes: registration
                .disks
                .iter()
                .fold(0, |sum: u64, disk| sum.saturating_add(disk.capacity_bytes)),
            state,
            shards: 0,
            primaries: 0,
        }
    }

    /// A node the coordinator's registry lists, in the state it judges. A
    /// registration marked `departing` is departing whatever the entry
    /// says (the registry judges it so as well).
    #[must_use]
    pub fn from_entry(entry: &NodeEntry) -> Self {
        let candidate = Self::from_registration(&entry.registration);
        Self {
            state: candidate.state.max(entry.state),
            ..candidate
        }
    }

    /// The node's domain at `level`, if it has the label the level needs.
    #[must_use]
    pub fn domain(&self, level: FailureDomain) -> Option<Domain> {
        Domain::of(level, &self.node, self.zone.as_ref(), self.rack.as_ref())
    }

    /// Whether the node may receive new members at `level`.
    #[must_use]
    pub fn is_eligible(&self, level: FailureDomain) -> bool {
        self.state != NodeState::Departing
            && self.capacity_bytes > 0
            && self.domain(level).is_some()
    }
}

/// Why a bucket's policy cannot be placed: the cluster has fewer eligible
/// domains than the bucket needs members.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub struct Unsatisfiable {
    /// The level members are kept apart at.
    pub level: FailureDomain,
    /// The members each shard needs.
    pub replicas: u8,
    /// The eligible domains the cluster has.
    pub domains: usize,
}

impl fmt::Display for Unsatisfiable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let level = level_name(self.level);
        write!(
            f,
            "the bucket needs {} replicas, each in a different {level} (failure_domain = \"{level}\"), \
             and the cluster has {} eligible",
            self.replicas, self.domains,
        )
    }
}

/// What [`Topology::place`] is asked to place: one shard, with the
/// members it keeps.
#[derive(Debug, Clone, Copy)]
pub struct ShardRequest<'a> {
    /// The shard's bucket.
    pub bucket: &'a BucketId,
    /// The shard.
    pub shard: ShardId,
    /// How many members it needs.
    pub replicas: u8,
    /// The nodes it keeps, members or learners: their domains are taken.
    pub keep: &'a [NodeId],
    /// Nodes it must not be given, such as one being removed.
    pub avoid: &'a [NodeId],
}

impl<'a> ShardRequest<'a> {
    /// A request for `replicas` members of a new shard.
    #[must_use]
    pub fn new(bucket: &'a BucketId, shard: ShardId, replicas: u8) -> Self {
        Self {
            bucket,
            shard,
            replicas,
            keep: &[],
            avoid: &[],
        }
    }

    /// Keeps `nodes`, adding only the members still missing.
    #[must_use]
    pub fn keep(mut self, nodes: &'a [NodeId]) -> Self {
        self.keep = nodes;
        self
    }

    /// Never chooses `nodes`.
    #[must_use]
    pub fn avoid(mut self, nodes: &'a [NodeId]) -> Self {
        self.avoid = nodes;
        self
    }
}

/// What [`Topology::place`] chose for a shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placed {
    /// The shard's nodes: those it keeps, then those added.
    pub members: Vec<NodeId>,
    /// The nodes added, best first.
    pub added: Vec<NodeId>,
    /// How many members are still missing because no eligible domain is
    /// left. The shard runs with fewer members until capacity returns.
    pub short: u8,
}

impl Placed {
    /// Whether the shard has every member it needs.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.short == 0
    }
}

/// A new shard's placement, from [`Topology::place_bucket`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewShard {
    /// The shard.
    pub shard: ShardId,
    /// Its first primary, one of the members.
    pub primary: NodeId,
    /// Its members, each in a domain of its own.
    pub members: Vec<NodeId>,
}

/// The cluster as placement sees it: the nodes and the level their
/// domains are drawn at.
///
/// Placement never puts two members of a shard in one [`Domain`]: a
/// shard that cannot get `replicas` members in separate domains gets
/// fewer ([`Placed::short`]). A node receives members only when it is
/// [eligible](Candidate::is_eligible): not departing, with some capacity,
/// and with the label the level needs. Among eligible nodes, placement
/// prefers live ones, then those holding fewer shards for their capacity,
/// then those whose zone and rack hold fewer of the shard's members, then
/// a stable hash of the shard and the node.
///
/// ```
/// use skys3_config::FailureDomain;
/// use skys3_coord::{Candidate, NodeState, Topology};
/// use skys3_types::{BucketId, Label, ShardCount};
///
/// // Four nodes in two racks.
/// let nodes = (0..4).map(|n| Candidate {
///     node: format!("node-{n}").parse().unwrap(),
///     zone: None,
///     rack: Some(Label::new(format!("rack-{}", n % 2)).unwrap()),
///     capacity_bytes: 1 << 40,
///     state: NodeState::Live,
///     shards: 0,
///     primaries: 0,
/// });
/// let topology = Topology::new(FailureDomain::Rack, nodes);
/// let bucket = BucketId::new("b-1")?;
/// // Three replicas need three racks: the bucket is rejected.
/// assert!(topology.place_bucket(&bucket, ShardCount::new(8)?, 3).is_err());
/// // Two fit, one in each rack.
/// let shards = topology.place_bucket(&bucket, ShardCount::new(8)?, 2)?;
/// for shard in &shards {
///     assert_ne!(topology.domain(&shard.members[0]), topology.domain(&shard.members[1]));
/// }
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Topology {
    level: FailureDomain,
    nodes: BTreeMap<NodeId, Candidate>,
}

impl Topology {
    /// The topology of `nodes` at `level`. A node listed twice keeps its
    /// last entry.
    #[must_use]
    pub fn new(level: FailureDomain, nodes: impl IntoIterator<Item = Candidate>) -> Self {
        Self {
            level,
            nodes: nodes
                .into_iter()
                .map(|candidate| (candidate.node.clone(), candidate))
                .collect(),
        }
    }

    /// The topology of the nodes `registry` lists, in the states it
    /// judges, holding no shards until [`Topology::record`] counts them.
    #[must_use]
    pub fn from_registry(level: FailureDomain, registry: &NodeRegistry) -> Self {
        Self::new(level, registry.entries().iter().map(Candidate::from_entry))
    }

    /// The level members are kept apart at.
    #[must_use]
    pub fn level(&self) -> FailureDomain {
        self.level
    }

    /// Every node, by node ID.
    pub fn candidates(&self) -> impl Iterator<Item = &Candidate> {
        self.nodes.values()
    }

    /// The node `node`, if the topology has it.
    #[must_use]
    pub fn get(&self, node: &NodeId) -> Option<&Candidate> {
        self.nodes.get(node)
    }

    /// The domain of `node`, if the topology has it and it has the label
    /// the level needs.
    #[must_use]
    pub fn domain(&self, node: &NodeId) -> Option<Domain> {
        self.nodes.get(node)?.domain(self.level)
    }

    /// Counts the shard `config` describes into its nodes' loads.
    pub fn record(&mut self, config: &ShardConfig) {
        let named: BTreeSet<&NodeId> = config.members.iter().chain(&config.learners).collect();
        for node in named {
            if let Some(candidate) = self.nodes.get_mut(node) {
                candidate.shards = candidate.shards.saturating_add(1);
            }
        }
        if let Some(candidate) = self.nodes.get_mut(&config.primary) {
            candidate.primaries = candidate.primaries.saturating_add(1);
        }
    }

    /// Counts one more shard into `node`'s load, as when it is given a
    /// learner.
    pub fn assign(&mut self, node: &NodeId) {
        if let Some(candidate) = self.nodes.get_mut(node) {
            candidate.shards = candidate.shards.saturating_add(1);
        }
    }

    /// The nodes that may receive new members.
    pub fn eligible(&self) -> impl Iterator<Item = &Candidate> {
        self.nodes
            .values()
            .filter(|candidate| candidate.is_eligible(self.level))
    }

    /// The domains of the eligible nodes.
    #[must_use]
    pub fn eligible_domains(&self) -> BTreeSet<Domain> {
        self.eligible()
            .filter_map(|candidate| candidate.domain(self.level))
            .collect()
    }

    /// The nodes that are not departing but lack the label the level
    /// needs, so placement never chooses them: most likely a configuration
    /// mistake.
    #[must_use]
    pub fn unlabeled(&self) -> Vec<NodeId> {
        self.nodes
            .values()
            .filter(|candidate| {
                candidate.state != NodeState::Departing && candidate.domain(self.level).is_none()
            })
            .map(|candidate| candidate.node.clone())
            .collect()
    }

    /// Checks that every shard can get `replicas` members, each in an
    /// eligible domain of its own.
    ///
    /// # Errors
    ///
    /// [`Unsatisfiable`] if the cluster has fewer eligible domains.
    pub fn check(&self, replicas: u8) -> Result<(), Unsatisfiable> {
        let domains = self.eligible_domains().len();
        if domains < usize::from(replicas) {
            return Err(Unsatisfiable {
                level: self.level,
                replicas,
                domains,
            });
        }
        Ok(())
    }

    /// Checks a new bucket's policy: what [`Topology::check`] checks for
    /// its `replicas`.
    ///
    /// # Errors
    ///
    /// [`Unsatisfiable`] if the cluster cannot satisfy it.
    pub fn check_bucket(&self, settings: &BucketSettings) -> Result<(), Unsatisfiable> {
        self.check(settings.replication.replicas)
    }

    /// Chooses the members `request` is missing: never two in one domain,
    /// never one in a domain a kept node is in, and fewer than asked
    /// ([`Placed::short`]) rather than either.
    ///
    /// A kept node the topology does not have, or that lacks the label the
    /// level needs, takes no domain: nothing is known to share one with it.
    #[must_use]
    pub fn place(&self, request: &ShardRequest<'_>) -> Placed {
        let mut members: Vec<NodeId> = Vec::new();
        for node in request.keep {
            if !members.contains(node) {
                members.push(node.clone());
            }
        }
        let mut taken: BTreeSet<Domain> = members.iter().filter_map(|n| self.domain(n)).collect();
        let mut chosen: Vec<&Candidate> = members.iter().filter_map(|n| self.get(n)).collect();
        let wanted = usize::from(request.replicas);
        let seed = Seed::new(request.bucket, request.shard);
        let mut added = Vec::new();
        while members.len() < wanted {
            let best = self
                .eligible()
                .filter(|candidate| {
                    !members.contains(&candidate.node) && !request.avoid.contains(&candidate.node)
                })
                .filter_map(|candidate| {
                    let domain = candidate.domain(self.level)?;
                    (!taken.contains(&domain)).then_some((candidate, domain))
                })
                .min_by(|(a, _), (b, _)| self.rank(a, b, &chosen, seed));
            let Some((candidate, domain)) = best else {
                break;
            };
            taken.insert(domain);
            chosen.push(candidate);
            members.push(candidate.node.clone());
            added.push(candidate.node.clone());
        }
        let short = wanted.saturating_sub(members.len());
        Placed {
            members,
            added,
            short: u8::try_from(short).unwrap_or(u8::MAX),
        }
    }

    /// The member of `members` best placed to be a shard's primary: a live
    /// node before a suspect one, then the one that leads the fewest
    /// shards. `None` if the topology has none of them.
    #[must_use]
    pub fn choose_primary(
        &self,
        bucket: &BucketId,
        shard: ShardId,
        members: &[NodeId],
    ) -> Option<NodeId> {
        let seed = Seed::new(bucket, shard);
        members
            .iter()
            .filter_map(|node| self.get(node))
            .min_by(|a, b| {
                a.state
                    .cmp(&b.state)
                    .then(a.primaries.cmp(&b.primaries))
                    .then_with(|| seed.hash(&a.node).cmp(&seed.hash(&b.node)))
                    .then_with(|| a.node.cmp(&b.node))
            })
            .map(|candidate| candidate.node.clone())
    }

    /// Places every shard of a new bucket: `shards` shards of `replicas`
    /// members, each in a domain of its own, with a primary each. Each
    /// shard counts toward the loads the next one is placed by, so a
    /// bucket's shards spread over the cluster. The topology itself is
    /// left as it was.
    ///
    /// `replicas` is at least 1; 0, which configuration validation
    /// rejects, counts as 1.
    ///
    /// # Errors
    ///
    /// [`Unsatisfiable`] if the cluster cannot satisfy the policy: the
    /// bucket must then not be created.
    pub fn place_bucket(
        &self,
        bucket: &BucketId,
        shards: ShardCount,
        replicas: u8,
    ) -> Result<Vec<NewShard>, Unsatisfiable> {
        let replicas = replicas.max(1);
        self.check(replicas)?;
        let mut topology = self.clone();
        let mut placed = Vec::with_capacity(usize::try_from(shards.get()).unwrap_or(0));
        for shard in shards.shards() {
            let members = topology
                .place(&ShardRequest::new(bucket, shard, replicas))
                .members;
            let primary = topology
                .choose_primary(bucket, shard, &members)
                .expect("a checked policy places every member on a known node");
            let config = NewShard {
                shard,
                primary,
                members,
            };
            topology.record_new(&config);
            placed.push(config);
        }
        Ok(placed)
    }

    /// Counts a shard [`Topology::place_bucket`] just placed.
    fn record_new(&mut self, shard: &NewShard) {
        for node in &shard.members {
            if let Some(candidate) = self.nodes.get_mut(node) {
                candidate.shards = candidate.shards.saturating_add(1);
            }
        }
        if let Some(candidate) = self.nodes.get_mut(&shard.primary) {
            candidate.primaries = candidate.primaries.saturating_add(1);
        }
    }

    /// Orders two candidates for a shard that already has `chosen`: the
    /// better one first.
    fn rank(&self, a: &Candidate, b: &Candidate, chosen: &[&Candidate], seed: Seed) -> Ordering {
        a.state
            .cmp(&b.state)
            .then_with(|| load(a).cmp(&load(b)))
            .then_with(|| self.spread(a, chosen).cmp(&self.spread(b, chosen)))
            .then_with(|| seed.hash(&a.node).cmp(&seed.hash(&b.node)))
            .then_with(|| a.node.cmp(&b.node))
    }

    /// How many of `chosen` share `candidate`'s zone, then its rack, at the
    /// levels below the required one: fewer is better.
    pub(crate) fn spread(&self, candidate: &Candidate, chosen: &[&Candidate]) -> (usize, usize) {
        let shared = |label: fn(&Candidate) -> Option<&Label>| {
            label(candidate).map_or(0, |mine| {
                chosen
                    .iter()
                    .filter(|other| label(other) == Some(mine))
                    .count()
            })
        };
        let zone = shared(|c| c.zone.as_ref());
        let rack = shared(|c| c.rack.as_ref());
        match self.level {
            FailureDomain::Node => (zone, rack),
            FailureDomain::Rack => (zone, 0),
            FailureDomain::Zone => (0, 0),
        }
    }
}

/// A candidate's load after it takes one more shard, for its capacity:
/// compared without division, as `(shards + 1) / capacity_bytes`.
fn load(candidate: &Candidate) -> Load {
    Load {
        shards: u128::from(candidate.shards) + 1,
        capacity: u128::from(candidate.capacity_bytes.max(1)),
    }
}

/// A ratio of shards to capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Load {
    shards: u128,
    capacity: u128,
}

impl Ord for Load {
    fn cmp(&self, other: &Self) -> Ordering {
        // A u32 count times a u64 capacity cannot overflow a u128.
        (self.shards * other.capacity).cmp(&(other.shards * self.capacity))
    }
}

impl PartialOrd for Load {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// The shard a tie between equal candidates is broken for: each shard
/// ranks equal nodes in its own order (rendezvous hashing), so new shards
/// spread over them.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Seed(u64);

impl Seed {
    pub(crate) fn new(bucket: &BucketId, shard: ShardId) -> Self {
        let mut hash = fnv(FNV_OFFSET, bucket.as_str().as_bytes());
        hash = fnv(hash, &[0, shard.get(), 0]);
        Self(hash)
    }

    /// The seed of something within the shard that `bytes` name, such as
    /// one stripe of an object, so that each ranks equal nodes in its own
    /// order.
    pub(crate) fn within(self, bytes: &[u8]) -> Self {
        Self(fnv(self.0, bytes))
    }

    /// The rank of `node` for this shard. Stable across builds and
    /// platforms: FNV-1a with a SplitMix64 finish.
    pub(crate) fn hash(self, node: &NodeId) -> u64 {
        let mut z = fnv(self.0, node.as_str().as_bytes());
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;

fn fnv(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests;
