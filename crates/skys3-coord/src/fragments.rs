//! Stripe geometry and fragment placement (design §6.7, §8.3).
//!
//! An erasure-coded stripe of `k` data and `m` parity fragments puts its
//! `k + m` fragments on nodes the placement chooses, which keeps two rules
//! at the `failure_domain` level without exception:
//!
//! - **at most one fragment of a stripe per node**, and
//! - **at most `m` fragments of a stripe per domain**, so that losing a
//!   whole domain loses no more than the stripe can rebuild.
//!
//! Fragments may land on any eligible node, not only the shard's members.
//! A node is eligible for fragments exactly when it is for shard members
//! ([`Candidate::is_eligible`]): not departing, offering some capacity, and
//! carrying the label its level needs.
//!
//! **Geometry.** The two rules bound how wide a stripe the cluster can
//! take. Its eligible nodes can hold at most `Σ min(n_D, m)` fragments of
//! one stripe, over the eligible domains `D` with `n_D` eligible nodes each
//! ([`StripeRoom::fragments`]), so a stripe of `k + m` fragments needs at
//! least `⌈(k+m)/m⌉` eligible domains, and that many is enough when each
//! holds at least `m` eligible nodes. A [`GeometryPolicy`] turns
//! `parity_fragments`, `max_data_fragments`, and `min_eligible_nodes` into
//! a ladder of geometries and picks the widest one the cluster supports
//! ([`GeometryPolicy::choose`]); with the defaults that reproduces the
//! §8.3 table, and an 11-node cluster in three racks uses 4+2.
//!
//! **Recording.** A [`FragmentPlanner`] plans one stripe at a time: its
//! geometry and the node of each fragment ([`StripePlan`]). Once every
//! fragment is durable, [`StripePlan::locate`] turns the plan and the
//! fragment IDs the nodes acknowledged into the [`CodedStripe`] that the
//! `EC_PUBLISH` record holds (plan M5-04). The geometry and locations are
//! recorded per stripe and never recomputed: a stripe keeps its geometry
//! when the cluster grows or shrinks, and only new stripes see the change.
//! Repair (M5-08) and fragment moves (M5-09) keep the same two rules:
//! [`FragmentPlanner::replace`] places fragments of a stripe again around
//! the ones it keeps.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use skys3_config::{EcConfig, FailureDomain};
use skys3_types::{
    BucketId, CodecId, CodedStripe, CodedStripeError, FragmentId, FragmentLocation, Geometry,
    GeometryError, NodeId, ShardId,
};

use crate::place::{Candidate, Domain, Seed, Topology, level_name};

/// How new stripes are shaped (design §8.3): the `[ec]` keys
/// `parity_fragments` (`m`), `max_data_fragments` (the widest `k`), and
/// `min_eligible_nodes`.
///
/// The policy offers a ladder of geometries, narrowest first
/// ([`GeometryPolicy::geometries`]):
///
/// - The **narrowest** has `k = min_eligible_nodes − m` (at least 1, at
///   most `max_data_fragments`), so a cluster of exactly
///   `min_eligible_nodes` eligible nodes can encode, with no spare node.
///   Repair then waits for a replacement node (§8.3).
/// - Wider ones grow `k` in steps of `m`: every multiple of `m` above the
///   narrowest `k` and below `max_data_fragments`, then
///   `max_data_fragments` itself. A `k` between two multiples of `m` needs
///   as many domains as the next multiple, `⌈(k+m)/m⌉`, so the steps lose
///   nothing at the `rack` and `zone` levels, and at the `node` level they
///   leave between one and `m` nodes outside each stripe until the widest
///   geometry is reached.
///
/// A cluster supports a geometry ([`GeometryPolicy::supports`]) when it
/// has at least `min_eligible_nodes` eligible nodes, room for the
/// stripe's `k + m` fragments under the per-domain cap, and, for any
/// geometry wider than the narrowest, at least one eligible node more
/// than the stripe needs, so that a stripe that loses a node can be
/// repaired onto another at once.
///
/// ```
/// use skys3_config::EcConfig;
/// use skys3_coord::{GeometryPolicy, StripeRoom};
/// use skys3_types::Geometry;
///
/// let policy = GeometryPolicy::from_config(&EcConfig::default())?;
/// assert_eq!(policy.geometries(), Geometry::DESIGN_TABLE);
/// // Seven nodes, each a domain of its own (`failure_domain = "node"`).
/// let room = StripeRoom { nodes: 7, domains: 7, fragments: 7 };
/// assert_eq!(policy.choose(&room), Some(Geometry::RS_4_2));
/// // Eleven nodes in three racks (`failure_domain = "rack"`): at most two
/// // fragments per rack.
/// let room = StripeRoom { nodes: 11, domains: 3, fragments: 6 };
/// assert_eq!(policy.choose(&room), Some(Geometry::RS_4_2));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GeometryPolicy {
    widest: Geometry,
    min_eligible_nodes: usize,
}

impl GeometryPolicy {
    /// The policy of `m = parity_fragments` parity fragments, data
    /// fragments up to `max_data_fragments`, and encoding from
    /// `min_eligible_nodes` eligible nodes on.
    ///
    /// # Errors
    ///
    /// [`GeometryError`] if either fragment count is zero or the widest
    /// stripe would have more than [`Geometry::MAX_FRAGMENTS`] fragments,
    /// which configuration validation rejects.
    pub fn new(
        parity_fragments: usize,
        max_data_fragments: usize,
        min_eligible_nodes: usize,
    ) -> Result<Self, GeometryError> {
        Ok(Self {
            widest: Geometry::new(max_data_fragments, parity_fragments)?,
            min_eligible_nodes,
        })
    }

    /// The policy that the validated `[ec]` section configures.
    ///
    /// # Errors
    ///
    /// [`GeometryError`] as for [`GeometryPolicy::new`]; never for a
    /// configuration that passed validation.
    pub fn from_config(config: &EcConfig) -> Result<Self, GeometryError> {
        let count = |value: u32| usize::try_from(value).unwrap_or(usize::MAX);
        Self::new(
            count(config.parity_fragments),
            count(config.max_data_fragments),
            count(config.min_eligible_nodes),
        )
    }

    /// `m`, the parity fragments of every geometry.
    #[must_use]
    pub fn parity_fragments(&self) -> usize {
        self.widest.parity_fragments()
    }

    /// The fewest eligible nodes for which objects are encoded at all.
    #[must_use]
    pub fn min_eligible_nodes(&self) -> usize {
        self.min_eligible_nodes
    }

    /// The widest geometry: `max_data_fragments + m`.
    #[must_use]
    pub fn widest(&self) -> Geometry {
        self.widest
    }

    /// The narrowest geometry, which needs `min_eligible_nodes` eligible
    /// nodes and no spare.
    #[must_use]
    pub fn narrowest(&self) -> Geometry {
        let m = self.parity_fragments();
        let k = self
            .min_eligible_nodes
            .saturating_sub(m)
            .clamp(1, self.widest.data_fragments());
        self.geometry(k)
    }

    /// Every geometry the policy may choose, narrowest first.
    #[must_use]
    pub fn geometries(&self) -> Vec<Geometry> {
        let m = self.parity_fragments();
        let narrowest = self.narrowest().data_fragments();
        let widest = self.widest.data_fragments();
        let mut ladder = vec![self.narrowest()];
        let mut k = (narrowest / m + 1) * m;
        while k < widest {
            ladder.push(self.geometry(k));
            k += m;
        }
        if widest > narrowest {
            ladder.push(self.widest);
        }
        ladder
    }

    /// Whether a cluster with `room` supports new stripes of `geometry`:
    /// it has at least `min_eligible_nodes` eligible nodes, room for every
    /// fragment at most one per node and `m` per domain, and a spare
    /// eligible node unless `geometry` is the narrowest.
    #[must_use]
    pub fn supports(&self, geometry: Geometry, room: &StripeRoom) -> bool {
        let total = geometry.total_fragments();
        room.nodes >= self.min_eligible_nodes
            && total <= room.fragments
            && total <= room.nodes
            && (geometry == self.narrowest() || room.nodes > total)
    }

    /// The widest geometry of the ladder that a cluster with `room`
    /// supports, or `None` if it supports none: then objects stay
    /// replicated and encoding pauses until capacity returns (§6.7).
    #[must_use]
    pub fn choose(&self, room: &StripeRoom) -> Option<Geometry> {
        self.geometries()
            .into_iter()
            .rev()
            .find(|geometry| self.supports(*geometry, room))
    }

    /// `k + m`, for a `k` the policy has bounded already.
    fn geometry(&self, k: usize) -> Geometry {
        Geometry::new(k, self.parity_fragments()).unwrap_or(self.widest)
    }
}

/// What the eligible nodes of a cluster offer one new stripe, as
/// [`Topology::stripe_room`] counts them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StripeRoom {
    /// The eligible nodes.
    pub nodes: usize,
    /// The domains they span at the `failure_domain` level.
    pub domains: usize,
    /// The most fragments of one stripe they can hold: at most one per
    /// node and `m` per domain, so `Σ min(n_D, m)` over the domains.
    pub fragments: usize,
}

impl Topology {
    /// What the eligible nodes other than `avoid` offer a stripe with `m =
    /// parity_fragments` parity fragments.
    #[must_use]
    pub fn stripe_room(&self, parity_fragments: usize, avoid: &[NodeId]) -> StripeRoom {
        let mut per_domain: BTreeMap<Domain, usize> = BTreeMap::new();
        for candidate in self.eligible() {
            if avoid.contains(&candidate.node) {
                continue;
            }
            if let Some(domain) = candidate.domain(self.level()) {
                *per_domain.entry(domain).or_default() += 1;
            }
        }
        StripeRoom {
            nodes: per_domain.values().sum(),
            domains: per_domain.len(),
            fragments: per_domain
                .values()
                .map(|&nodes| nodes.min(parity_fragments))
                .sum(),
        }
    }
}

/// Why no new stripe can be placed: the cluster supports no geometry of
/// the policy. Large objects then stay replicated, and cluster health
/// reports the shortfall until capacity returns (§6.7).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub struct NoGeometry {
    /// The level fragments are kept apart at.
    pub level: FailureDomain,
    /// What the cluster offers a stripe.
    pub room: StripeRoom,
    /// The narrowest geometry of the policy.
    pub narrowest: Geometry,
    /// The policy's `min_eligible_nodes`.
    pub min_eligible_nodes: usize,
}

impl fmt::Display for NoGeometry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let level = level_name(self.level);
        let total = self.narrowest.total_fragments();
        write!(
            f,
            "no stripe geometry fits: the narrowest, {}, needs {} eligible nodes with room for \
             {total} fragments at most {} per {level} (failure_domain = \"{level}\"), and the \
             cluster has {} eligible nodes in {} domains with room for {}",
            self.narrowest,
            self.min_eligible_nodes.max(total),
            self.narrowest.parity_fragments(),
            self.room.nodes,
            self.room.domains,
            self.room.fragments,
        )
    }
}

/// One stripe to place, as [`FragmentPlanner::plan`] takes it.
#[derive(Debug, Clone, Copy)]
pub struct StripeRequest<'a> {
    /// The object's bucket.
    pub bucket: &'a BucketId,
    /// The object's shard.
    pub shard: ShardId,
    /// The object key.
    pub key: &'a str,
    /// The stripe's number within the object.
    pub stripe: u32,
    /// The stripe's data length, which sizes its fragments.
    pub data_len: u64,
    /// Nodes the stripe must not use, such as one that failed a fragment
    /// write of this attempt. The geometry is chosen without them.
    pub avoid: &'a [NodeId],
}

impl<'a> StripeRequest<'a> {
    /// Stripe `stripe`, of `data_len` bytes, of the object `key`.
    #[must_use]
    pub fn new(
        bucket: &'a BucketId,
        shard: ShardId,
        key: &'a str,
        stripe: u32,
        data_len: u64,
    ) -> Self {
        Self {
            bucket,
            shard,
            key,
            stripe,
            data_len,
            avoid: &[],
        }
    }

    /// Never places a fragment on `nodes`.
    #[must_use]
    pub fn avoid(mut self, nodes: &'a [NodeId]) -> Self {
        self.avoid = nodes;
        self
    }
}

/// Fragments of a recorded stripe to place again, around the ones it keeps,
/// as [`FragmentPlanner::replace`] takes them: what repair rebuilds (§8.6).
#[derive(Debug, Clone, Copy)]
pub struct ReplaceRequest<'a> {
    /// The object's bucket.
    pub bucket: &'a BucketId,
    /// The object's shard.
    pub shard: ShardId,
    /// The object key.
    pub key: &'a str,
    /// The stripe's number within the object.
    pub stripe: u32,
    /// The stripe's recorded geometry, which the fragments keep.
    pub geometry: Geometry,
    /// The stripe's data length, which sizes its fragments.
    pub data_len: u64,
    /// The nodes of the fragments the stripe keeps where they are. They
    /// count toward the caps, and no new fragment goes to them.
    pub keep: &'a [NodeId],
    /// How many fragments to place.
    pub count: usize,
    /// Nodes that must receive none, such as one whose fragment was lost
    /// or whose write failed.
    pub avoid: &'a [NodeId],
}

/// Where a new stripe goes: its geometry and the node of each fragment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StripePlan {
    /// The stripe's geometry, recorded with it and never recomputed.
    pub geometry: Geometry,
    /// The node of each fragment, by index: `k` data fragments, then `m`
    /// parity fragments. Distinct, and at most `m` in one domain.
    pub nodes: Vec<NodeId>,
}

impl StripePlan {
    /// The stripe as `EC_PUBLISH` records it, once fragment `i` is durable
    /// on `nodes[i]` under the ID `fragments[i]`: stripe `number` of the
    /// object, `data_len` bytes from `offset`, encoded by `codec`.
    ///
    /// # Errors
    ///
    /// [`CodedStripeError`] if `fragments` does not name one fragment per
    /// node of the plan, or the stripe is empty or ends past `u64::MAX`.
    pub fn locate(
        &self,
        number: u32,
        offset: u64,
        data_len: u64,
        codec: CodecId,
        fragments: &[FragmentId],
    ) -> Result<CodedStripe, CodedStripeError> {
        if fragments.len() != self.nodes.len() {
            return Err(CodedStripeError::FragmentCount {
                stripe: number,
                geometry: self.geometry,
                located: fragments.len(),
            });
        }
        let locations = self
            .nodes
            .iter()
            .zip(fragments)
            .map(|(node, fragment)| FragmentLocation {
                node: node.clone(),
                fragment: *fragment,
            })
            .collect();
        CodedStripe::new(number, offset, data_len, self.geometry, codec, locations)
    }
}

/// Plans new stripes over a [`Topology`]: the widest geometry the cluster
/// supports under a [`GeometryPolicy`], and a node for each fragment.
///
/// It keeps the two rules of fragment placement without exception (at
/// most one fragment of a stripe per node and `m` per domain) and never
/// returns a plan it cannot complete. Within them it prefers, in order:
///
/// 1. live nodes over suspect ones,
/// 2. domains holding fewer of the stripe's fragments, so a stripe spreads
///    over as many domains as it can,
/// 3. nodes whose zone and rack hold fewer of them, below the required
///    level,
/// 4. nodes with the most free space: the fewest fragment bytes held, the
///    stripe's included, for the capacity their disks offer,
/// 5. a hash of the stripe and the node, so equal nodes share stripes
///    evenly and the result never depends on listing order.
///
/// Fragments are chosen in index order, so the data fragments, which
/// every healthy read fetches (§8.5), get the best nodes. Each planned
/// stripe counts toward the bytes its nodes hold, so the stripes of an
/// object, placed independently, spread over the cluster.
///
/// ```
/// use skys3_config::{EcConfig, FailureDomain};
/// use skys3_coord::{Candidate, FragmentPlanner, GeometryPolicy, NodeState, StripeRequest, Topology};
/// use skys3_types::{BucketId, Geometry, Label, ShardId};
///
/// // Eleven nodes in three racks.
/// let nodes = (0..11).map(|n| Candidate {
///     node: format!("node-{n}").parse().unwrap(),
///     zone: None,
///     rack: Some(Label::new(format!("rack-{}", n % 3)).unwrap()),
///     capacity_bytes: 1 << 40,
///     state: NodeState::Live,
///     shards: 0,
///     primaries: 0,
/// });
/// let topology = Topology::new(FailureDomain::Rack, nodes);
/// let policy = GeometryPolicy::from_config(&EcConfig::default())?;
/// let mut planner = FragmentPlanner::new(topology, policy);
/// let bucket = BucketId::new("b-1")?;
/// let plan = planner.plan(&StripeRequest::new(&bucket, ShardId::new(0), "video.mp4", 0, 64 << 20))?;
/// assert_eq!(plan.geometry, Geometry::RS_4_2);
/// assert_eq!(plan.nodes.len(), 6);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone)]
pub struct FragmentPlanner {
    topology: Topology,
    policy: GeometryPolicy,
    held: BTreeMap<NodeId, u64>,
}

impl FragmentPlanner {
    /// A planner over `topology` under `policy`, counting no fragment
    /// bytes held yet.
    #[must_use]
    pub fn new(topology: Topology, policy: GeometryPolicy) -> Self {
        Self {
            topology,
            policy,
            held: BTreeMap::new(),
        }
    }

    /// Counts `bytes` more of fragments held by `node`, such as those the
    /// node reports it stores, so placement prefers nodes with more room.
    pub fn hold(&mut self, node: &NodeId, bytes: u64) {
        let held = self.held.entry(node.clone()).or_default();
        *held = held.saturating_add(bytes);
    }

    /// The fragment bytes counted for `node`: held, and planned since.
    #[must_use]
    pub fn held(&self, node: &NodeId) -> u64 {
        self.held.get(node).copied().unwrap_or(0)
    }

    /// The topology planned over.
    #[must_use]
    pub fn topology(&self) -> &Topology {
        &self.topology
    }

    /// The policy planned under.
    #[must_use]
    pub fn policy(&self) -> GeometryPolicy {
        self.policy
    }

    /// What the eligible nodes other than `avoid` offer a new stripe.
    #[must_use]
    pub fn room(&self, avoid: &[NodeId]) -> StripeRoom {
        self.topology
            .stripe_room(self.policy.parity_fragments(), avoid)
    }

    /// The geometry of new stripes in the cluster as it is: what decides
    /// whether objects are encoded at all (§8.2).
    ///
    /// # Errors
    ///
    /// [`NoGeometry`] if the cluster supports none: encoding pauses.
    pub fn geometry(&self) -> Result<Geometry, NoGeometry> {
        let room = self.room(&[]);
        self.policy
            .choose(&room)
            .ok_or_else(|| self.no_geometry(room))
    }

    /// Plans the stripe `request` names: the widest geometry the eligible
    /// nodes other than `request.avoid` support, and a node for each of
    /// its fragments. The plan's fragment bytes count toward its nodes.
    ///
    /// # Errors
    ///
    /// [`NoGeometry`] if those nodes support no geometry of the policy.
    pub fn plan(&mut self, request: &StripeRequest<'_>) -> Result<StripePlan, NoGeometry> {
        let room = self.room(request.avoid);
        let geometry = self
            .policy
            .choose(&room)
            .ok_or_else(|| self.no_geometry(room))?;
        let fragment_bytes = request.data_len.div_ceil(geometry.data_fragments() as u64);
        let seed = Seed::new(request.bucket, request.shard)
            .within(request.key.as_bytes())
            .within(&[0])
            .within(&request.stripe.to_be_bytes());
        let Some(nodes) = self.choose_nodes(
            geometry,
            seed,
            fragment_bytes,
            request.avoid,
            &[],
            geometry.total_fragments(),
        ) else {
            // The geometry fits the room, and the room is exactly what the
            // per-node and per-domain caps leave, so this is unreachable.
            return Err(self.no_geometry(room));
        };
        for node in &nodes {
            self.hold(node, fragment_bytes);
        }
        Ok(StripePlan { geometry, nodes })
    }

    /// Places `request.count` fragments of a recorded stripe again: on
    /// eligible nodes other than `request.avoid` and the nodes it keeps,
    /// with the kept fragments counted toward the per-domain cap of the
    /// stripe's geometry, and preferred as a new stripe's are. The nodes
    /// count the new fragments' bytes.
    ///
    /// Returns `None` if the caps leave too few nodes, as in a cluster
    /// with no spare node outside the stripe: repair then waits for one
    /// (§8.3).
    pub fn replace(&mut self, request: &ReplaceRequest<'_>) -> Option<Vec<NodeId>> {
        let geometry = request.geometry;
        let fragment_bytes = request.data_len.div_ceil(geometry.data_fragments() as u64);
        let seed = Seed::new(request.bucket, request.shard)
            .within(request.key.as_bytes())
            .within(&[1])
            .within(&request.stripe.to_be_bytes());
        let nodes = self.choose_nodes(
            geometry,
            seed,
            fragment_bytes,
            request.avoid,
            request.keep,
            request.count,
        )?;
        for node in &nodes {
            self.hold(node, fragment_bytes);
        }
        Some(nodes)
    }

    /// Chooses the node of each of `count` fragments of a `geometry`
    /// stripe that keeps fragments on `kept`, in index order, or `None`
    /// if the caps leave too few.
    fn choose_nodes(
        &self,
        geometry: Geometry,
        seed: Seed,
        fragment_bytes: u64,
        avoid: &[NodeId],
        kept: &[NodeId],
        count: usize,
    ) -> Option<Vec<NodeId>> {
        let level = self.topology.level();
        let cap = geometry.parity_fragments();
        let mut chosen: Vec<&Candidate> = Vec::with_capacity(geometry.total_fragments());
        let mut taken: BTreeSet<&NodeId> = kept.iter().collect();
        let mut in_domain: BTreeMap<Domain, usize> = BTreeMap::new();
        for candidate in kept.iter().filter_map(|node| self.topology.get(node)) {
            if let Some(domain) = candidate.domain(level) {
                *in_domain.entry(domain).or_default() += 1;
            }
            chosen.push(candidate);
        }
        let first = chosen.len();
        for _ in 0..count {
            let (candidate, domain, _) = self
                .topology
                .eligible()
                .filter(|c| !avoid.contains(&c.node) && !taken.contains(&c.node))
                .filter_map(|c| {
                    let domain = c.domain(level)?;
                    let used = in_domain.get(&domain).copied().unwrap_or(0);
                    (used < cap).then_some((c, domain, used))
                })
                .min_by(|(a, _, used_a), (b, _, used_b)| {
                    a.state
                        .cmp(&b.state)
                        .then(used_a.cmp(used_b))
                        .then_with(|| {
                            let spread_a = self.topology.spread(a, &chosen);
                            spread_a.cmp(&self.topology.spread(b, &chosen))
                        })
                        .then_with(|| self.fullness(a, b, fragment_bytes))
                        .then_with(|| seed.hash(&a.node).cmp(&seed.hash(&b.node)))
                        .then_with(|| a.node.cmp(&b.node))
                })?;
            *in_domain.entry(domain).or_default() += 1;
            taken.insert(&candidate.node);
            chosen.push(candidate);
        }
        Some(chosen[first..].iter().map(|c| c.node.clone()).collect())
    }

    /// Orders `a` and `b` by the share of their capacity their fragments
    /// would fill with `fragment_bytes` more: the emptier first. Compared
    /// without division.
    fn fullness(&self, a: &Candidate, b: &Candidate, fragment_bytes: u64) -> Ordering {
        let filled = |candidate: &Candidate| {
            u128::from(self.held(&candidate.node).saturating_add(fragment_bytes))
        };
        let capacity = |candidate: &Candidate| u128::from(candidate.capacity_bytes.max(1));
        // Two u64 factors cannot overflow a u128.
        (filled(a) * capacity(b)).cmp(&(filled(b) * capacity(a)))
    }

    fn no_geometry(&self, room: StripeRoom) -> NoGeometry {
        NoGeometry {
            level: self.topology.level(),
            room,
            narrowest: self.policy.narrowest(),
            min_eligible_nodes: self.policy.min_eligible_nodes(),
        }
    }
}

#[cfg(test)]
mod tests;
