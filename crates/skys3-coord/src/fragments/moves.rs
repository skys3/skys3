//! Fragment moves (design §8.3, plan M5-09): which fragments of a shard's
//! recorded stripes its primary moves to other nodes, and where.
//!
//! A stripe's geometry and locations are never recomputed, but its
//! fragments may move one at a time, each to a node that keeps the two
//! placement rules for the stripe. A shard primary plans moves for three
//! reasons, in this order:
//!
//! 1. **Domain.** A stripe holds more than `m` fragments in one failure
//!    domain, as after a node was relabeled: losing that domain would lose
//!    more than the stripe can rebuild.
//! 2. **Drain.** A fragment's node is still listed but no longer eligible:
//!    departing (its registration is marked, as when it is decommissioned),
//!    offering no capacity, or without the label its level needs. Its
//!    fragments move away while it still answers, at one read each, rather
//!    than being rebuilt from `k` reads once it is gone. A node the topology
//!    no longer lists has lost its fragments, which is repair's to rebuild,
//!    not a move's.
//! 3. **Balance.** Among the live eligible nodes, each holds a share of the
//!    shard's fragment bytes proportional to the capacity its disks offer,
//!    so a node that joins receives fragments. A fragment of `f` bytes moves
//!    from node `a` to node `t` only when that brings both closer to their
//!    shares: `(b_a − f) / c_a ≥ (b_t + f) / c_t` for `b` bytes on capacity
//!    `c`. The sum of `b² / c` then falls with every move, so balancing
//!    ends, and never moves a fragment back.
//!
//! The shares are each shard's own: the primary knows its fragments
//! exactly from its index, and shards that each spread their own
//! fragments in proportion to capacity spread the cluster's, to within a
//! fragment per shard and node.
//!
//! A target is a live eligible node outside the stripe, not one the caller
//! avoids (a node silent at its last check, say), whose domain holds fewer
//! than `m` of the stripe's other fragments. Among those, the emptiest for
//! its capacity is chosen, then by a hash of the stripe and the node. A
//! fragment on a node the caller avoids never moves: it may be lost, which
//! is repair's to find.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use skys3_types::{BucketId, CodedStripe, FragmentLocation, NodeId, ShardId};

use super::FragmentPlanner;
use crate::NodeState;
use crate::place::{Candidate, Domain, Seed, Topology};

/// Why a fragment moves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MoveReason {
    /// Its stripe holds more than `m` fragments in the node's failure
    /// domain.
    Domain,
    /// Its node is listed but no longer eligible: departing, offering no
    /// capacity, or without the label its level needs.
    Drain,
    /// Its node holds more than its share of the shard's fragment bytes.
    Balance,
}

/// One recorded stripe of a shard, as move planning takes it.
#[derive(Debug, Clone, Copy)]
pub struct ShardStripe<'a> {
    /// The object key.
    pub key: &'a str,
    /// The stripe as its layout records it.
    pub stripe: &'a CodedStripe,
}

/// The stripes of one shard to plan moves for, as
/// [`FragmentPlanner::moves`] takes them.
#[derive(Debug, Clone, Copy)]
pub struct MoveRequest<'a> {
    /// The shard's bucket.
    pub bucket: &'a BucketId,
    /// The shard.
    pub shard: ShardId,
    /// Every coded stripe of the shard, which the shares count.
    pub stripes: &'a [ShardStripe<'a>],
    /// Nodes no fragment moves from or to, such as those silent at their
    /// last check.
    pub avoid: &'a [NodeId],
    /// The most moves to plan.
    pub limit: usize,
}

/// One fragment to move: from where its stripe's layout locates it to a
/// node, which will hold it under a new fragment ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedMove {
    /// The object key.
    pub key: String,
    /// The stripe's number within the object.
    pub stripe: u32,
    /// The fragment's index within the stripe.
    pub index: u8,
    /// Where the layout locates the fragment now.
    pub from: FragmentLocation,
    /// The node it moves to.
    pub to: NodeId,
    /// Why it moves.
    pub reason: MoveReason,
}

impl FragmentPlanner {
    /// Plans up to `request.limit` fragment moves of the shard's stripes,
    /// in this order: fragments of a stripe over a failure domain's cap
    /// ([`MoveReason::Domain`]), fragments on a listed node that is no
    /// longer eligible ([`MoveReason::Drain`]), and fragments whose move
    /// brings both nodes closer to their shares of the shard's fragment
    /// bytes, `(b_a − f) / c_a ≥ (b_t + f) / c_t` ([`MoveReason::Balance`]).
    /// Each goes to a node that keeps the stripe's placement rules,
    /// counting the moves planned before it. A fragment moves at most once
    /// in a plan. A topology that lists no node plans nothing.
    #[must_use]
    pub fn moves(&self, request: &MoveRequest<'_>) -> Vec<PlannedMove> {
        if self.topology().candidates().next().is_none() {
            return Vec::new();
        }
        let mut planning = Planning::new(self.topology(), request);
        planning.fix(MoveReason::Domain);
        planning.fix(MoveReason::Drain);
        planning.balance();
        planning.moves
    }
}

/// Moves planned so far, and the stripes and loads they leave.
struct Planning<'a> {
    topology: &'a Topology,
    request: &'a MoveRequest<'a>,
    /// Each stripe's nodes, by index, with the planned moves made.
    nodes: Vec<Vec<NodeId>>,
    /// The fragments planned to move, by stripe position and index.
    moving: BTreeSet<(usize, usize)>,
    /// The shard's fragment bytes on each node, with the planned moves
    /// made.
    load: BTreeMap<NodeId, u64>,
    moves: Vec<PlannedMove>,
}

impl<'a> Planning<'a> {
    fn new(topology: &'a Topology, request: &'a MoveRequest<'a>) -> Self {
        let nodes: Vec<Vec<NodeId>> = request
            .stripes
            .iter()
            .map(|s| {
                s.stripe
                    .fragments()
                    .iter()
                    .map(|l| l.node.clone())
                    .collect()
            })
            .collect();
        let mut load: BTreeMap<NodeId, u64> = BTreeMap::new();
        for (position, stripe_nodes) in nodes.iter().enumerate() {
            let bytes = fragment_bytes(request.stripes[position].stripe);
            for node in stripe_nodes {
                *load.entry(node.clone()).or_default() += bytes;
            }
        }
        Self {
            topology,
            request,
            nodes,
            moving: BTreeSet::new(),
            load,
            moves: Vec::new(),
        }
    }

    fn full(&self) -> bool {
        self.moves.len() >= self.request.limit
    }

    /// Plans a move of every fragment that breaks the rule `reason` names
    /// and has somewhere to go.
    fn fix(&mut self, reason: MoveReason) {
        let level = self.topology.level();
        for position in 0..self.nodes.len() {
            for index in 0..self.nodes[position].len() {
                if self.full() {
                    return;
                }
                let node = &self.nodes[position][index];
                if self.request.avoid.contains(node) || self.moving.contains(&(position, index)) {
                    continue;
                }
                // A node the topology does not list lost its fragments:
                // repair's, not a move's.
                let Some(candidate) = self.topology.get(node) else {
                    continue;
                };
                let breaks = match reason {
                    MoveReason::Domain => candidate.domain(level).is_some_and(|domain| {
                        let cap = self.request.stripes[position]
                            .stripe
                            .geometry()
                            .parity_fragments();
                        self.in_domain(position, &domain, None) > cap
                    }),
                    MoveReason::Drain => !candidate.is_eligible(level),
                    MoveReason::Balance => false,
                };
                if breaks && let Some(target) = self.target(position, index) {
                    self.plan(position, index, target, reason);
                }
            }
        }
    }

    /// Plans balancing moves, one at a time from the fullest node that has
    /// one, until none is left or the plan is full.
    fn balance(&mut self) {
        while !self.full() {
            let mut sources: Vec<&Candidate> = self
                .topology
                .eligible()
                .filter(|c| c.state == NodeState::Live && !self.request.avoid.contains(&c.node))
                .collect();
            sources.sort_by(|a, b| {
                self.ratio(b, 0)
                    .cmp(&self.ratio(a, 0))
                    .then_with(|| a.node.cmp(&b.node))
            });
            let found = sources.iter().find_map(|source| {
                (0..self.nodes.len()).find_map(|position| {
                    let index = self.nodes[position]
                        .iter()
                        .position(|node| *node == source.node)?;
                    if self.moving.contains(&(position, index)) {
                        return None;
                    }
                    let target = self.target(position, index)?;
                    let bytes = fragment_bytes(self.request.stripes[position].stripe);
                    self.closer(source, self.topology.get(&target)?, bytes)
                        .then_some((position, index, target))
                })
            });
            let Some((position, index, target)) = found else {
                return;
            };
            self.plan(position, index, target, MoveReason::Balance);
        }
    }

    /// The node fragment `index` of the stripe at `position` may move to:
    /// a live eligible node outside the stripe and not avoided, in a domain
    /// holding fewer than `m` of its other fragments, the emptiest for its
    /// capacity first.
    fn target(&self, position: usize, index: usize) -> Option<NodeId> {
        let level = self.topology.level();
        let ShardStripe { key, stripe } = self.request.stripes[position];
        let cap = stripe.geometry().parity_fragments();
        let bytes = fragment_bytes(stripe);
        let nodes = &self.nodes[position];
        let seed = Seed::new(self.request.bucket, self.request.shard)
            .within(key.as_bytes())
            .within(&[2])
            .within(&stripe.number().to_be_bytes());
        self.topology
            .eligible()
            .filter(|c| {
                c.state == NodeState::Live
                    && !self.request.avoid.contains(&c.node)
                    && !nodes.contains(&c.node)
            })
            .filter(|c| {
                c.domain(level)
                    .is_some_and(|domain| self.in_domain(position, &domain, Some(index)) < cap)
            })
            .min_by(|a, b| {
                self.ratio(a, bytes)
                    .cmp(&self.ratio(b, bytes))
                    .then_with(|| seed.hash(&a.node).cmp(&seed.hash(&b.node)))
                    .then_with(|| a.node.cmp(&b.node))
            })
            .map(|c| c.node.clone())
    }

    /// How many fragments of the stripe at `position`, but the one at
    /// `except`, are on listed nodes of `domain`.
    fn in_domain(&self, position: usize, domain: &Domain, except: Option<usize>) -> usize {
        self.nodes[position]
            .iter()
            .enumerate()
            .filter(|(index, node)| {
                Some(*index) != except && self.topology.domain(node).as_ref() == Some(domain)
            })
            .count()
    }

    /// Whether moving `bytes` from `source` to `target` brings both closer
    /// to their shares: `(b_s − f) / c_s ≥ (b_t + f) / c_t`.
    fn closer(&self, source: &Candidate, target: &Candidate, bytes: u64) -> bool {
        let held = |c: &Candidate| self.load.get(&c.node).copied().unwrap_or(0);
        let capacity = |c: &Candidate| u128::from(c.capacity_bytes.max(1));
        let after_source = u128::from(held(source).saturating_sub(bytes));
        let after_target = u128::from(held(target).saturating_add(bytes));
        after_source * capacity(target) >= after_target * capacity(source)
    }

    /// `candidate`'s share of the shard's fragment bytes with `extra` more,
    /// for its capacity.
    fn ratio(&self, candidate: &Candidate, extra: u64) -> Ratio {
        let held = self.load.get(&candidate.node).copied().unwrap_or(0);
        Ratio {
            bytes: u128::from(held.saturating_add(extra)),
            capacity: u128::from(candidate.capacity_bytes.max(1)),
        }
    }

    fn plan(&mut self, position: usize, index: usize, to: NodeId, reason: MoveReason) {
        let ShardStripe { key, stripe } = self.request.stripes[position];
        let bytes = fragment_bytes(stripe);
        let from = stripe.fragments()[index].clone();
        if let Some(load) = self.load.get_mut(&from.node) {
            *load = load.saturating_sub(bytes);
        }
        *self.load.entry(to.clone()).or_default() += bytes;
        self.nodes[position][index] = to.clone();
        self.moving.insert((position, index));
        self.moves.push(PlannedMove {
            key: key.to_owned(),
            stripe: stripe.number(),
            // A stripe holds at most 255 fragments.
            index: index as u8,
            from,
            to,
            reason,
        });
    }
}

/// A share of bytes for a capacity, compared without division: two are
/// equal when they are the same fraction.
#[derive(Debug, Clone, Copy)]
struct Ratio {
    bytes: u128,
    capacity: u128,
}

impl PartialEq for Ratio {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Ratio {}

impl Ord for Ratio {
    fn cmp(&self, other: &Self) -> Ordering {
        // Two u64 factors cannot overflow a u128.
        (self.bytes * other.capacity).cmp(&(other.bytes * self.capacity))
    }
}

impl PartialOrd for Ratio {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// The bytes one fragment of `stripe` holds, as placement counts them.
fn fragment_bytes(stripe: &CodedStripe) -> u64 {
    stripe
        .data_len()
        .div_ceil(stripe.geometry().data_fragments() as u64)
}

#[cfg(test)]
mod tests;
