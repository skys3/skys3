//! Every combination of lost fragments, and of lost holders, read through
//! the gateways (plan M5-12, design §8.5, §16.1): once every object is
//! coded and its replicas are dropped, a client reads every object whole
//! through one node's gateway per round, while each round makes fragments
//! unreadable at the gateways' fragment sources:
//!
//! - the same fragment indices of every stripe: no index lost, then every
//!   set of 1 to `m + 1` of the `k + m` indices; and
//! - every fragment some nodes hold: every set of 1 to `m + 1` of the six
//!   first nodes, which costs each stripe the fragments those nodes hold
//!   of it.
//!
//! The combinations are enumerated in a fixed order, smaller sets first,
//! so every run reads all of them. A GET of an object none of whose
//! stripes lost more than `m` fragments must return its bytes; one of an
//! object a stripe of which lost more must fail, with `503` or a body that
//! breaks off, and never return bytes. The gateways' hot caches are off,
//! so every GET reads fragments.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use skys3_ec::{FragmentReadError, FragmentRequest};
use skys3_types::NodeId;

use super::reads::{Got, get};
use super::{Driver, NODES, World, lock};

/// How many times a GET that must succeed is sent before it is judged
/// failed.
const ATTEMPTS: usize = 3;

/// What a round makes unreadable.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Lost {
    /// These fragment indices of every stripe.
    Fragments(BTreeSet<u8>),
    /// Every fragment these nodes hold.
    Holders(BTreeSet<NodeId>),
}

impl Lost {
    /// How many fragments of a stripe whose fragments are on `nodes`, in
    /// index order, the round makes unreadable.
    fn of_stripe(&self, nodes: &[NodeId]) -> usize {
        match self {
            Lost::Fragments(indices) => (0..nodes.len())
                .filter(|index| u8::try_from(*index).is_ok_and(|i| indices.contains(&i)))
                .count(),
            Lost::Holders(holders) => nodes.iter().filter(|node| holders.contains(node)).count(),
        }
    }
}

/// What the reads of the loss combinations saw.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CombinationReport {
    /// The stripes' data and parity fragment counts, `k` and `m`.
    pub geometry: (usize, usize),
    /// Sets of lost fragment indices read, the empty one included.
    pub fragment_sets: usize,
    /// Sets of lost holders read.
    pub holder_sets: usize,
    /// GETs answered with the object's bytes.
    pub served: u64,
    /// GETs that failed cleanly, of objects a stripe of which lost more
    /// than `m` fragments.
    pub refused: u64,
}

/// The loss combinations' state, which the gateways' fragment sources and
/// the reading client share.
#[derive(Debug, Default)]
pub(super) struct Combinations {
    /// What the current round makes unreadable, if a round is on.
    lost: Mutex<Option<Lost>>,
    done: AtomicBool,
    report: Mutex<CombinationReport>,
    /// What went wrong: a GET that returned other bytes, or that failed
    /// with no more than `m` fragments of each stripe lost.
    wrong: Mutex<Vec<String>>,
}

impl Combinations {
    /// The error a fragment read gets in the current round, if the round
    /// makes the fragment unreadable.
    pub(super) fn refuses(&self, request: &FragmentRequest) -> Option<FragmentReadError> {
        let node = request.node.clone();
        match lock(&self.lost).as_ref()? {
            Lost::Fragments(indices) if indices.contains(&request.identity.index) => {
                Some(FragmentReadError::NotHeld {
                    node,
                    reason: "the fragment is lost in this combination".to_owned(),
                })
            }
            Lost::Holders(holders) if holders.contains(&node) => {
                Some(FragmentReadError::Unreachable {
                    node,
                    reason: "the holder is lost in this combination".to_owned(),
                })
            }
            _ => None,
        }
    }
}

/// Every set of 1 to `most` of `count` items, smaller sets first and each
/// size in lexicographic order.
fn combinations(count: usize, most: usize) -> Vec<Vec<usize>> {
    let mut sets = Vec::new();
    for size in 1..=most.min(count) {
        let mut set: Vec<usize> = (0..size).collect();
        loop {
            sets.push(set.clone());
            // The last position that can still move right.
            let Some(at) = (0..size).rev().find(|&at| set[at] < count - size + at) else {
                break;
            };
            set[at] += 1;
            for next in at + 1..size {
                set[next] = set[next - 1] + 1;
            }
        }
    }
    sets
}

/// One object to read: its key, its bytes, and its stripes' fragment
/// nodes in index order.
struct Object {
    key: String,
    data: Vec<u8>,
    stripes: Vec<Vec<NodeId>>,
}

/// The rounds of a run whose stripes are `k + m`, in order.
fn rounds(total: usize, parity: usize) -> Vec<Lost> {
    let mut rounds = vec![Lost::Fragments(BTreeSet::new())];
    for set in combinations(total, parity + 1) {
        let indices = set.into_iter().filter_map(|i| u8::try_from(i).ok());
        rounds.push(Lost::Fragments(indices.collect()));
    }
    for set in combinations(NODES, parity + 1) {
        let holders = set
            .into_iter()
            .map(|n| format!("n{n}").parse().expect("a node ID"));
        rounds.push(Lost::Holders(holders.collect()));
    }
    rounds
}

/// Phase state of the driver.
#[derive(Debug, Default)]
pub(super) struct Phase {
    started: bool,
}

impl Driver<'_> {
    /// Whether the loss combinations keep the run going: the first time
    /// the objects settle it starts their reads, and the run goes on until
    /// they are done.
    pub(super) fn combinations_pending(
        &mut self,
        sim: &mut turmoil::Sim<'_>,
        world: &Arc<World>,
    ) -> Result<bool, String> {
        let Some(combinations) = &world.combinations else {
            return Ok(false);
        };
        if self.combinations.started {
            return Ok(!combinations.done.load(Ordering::SeqCst));
        }
        self.combinations.started = true;
        let (objects, geometry) = self.objects()?;
        lock(&combinations.report).geometry = geometry;
        let world = Arc::clone(world);
        sim.client("combinations", async move {
            read_rounds(&world, &objects, geometry).await;
            Ok(())
        });
        Ok(true)
    }

    /// Every object with its bytes and its stripes' nodes, as the first
    /// member's layouts give them, and the stripes' geometry, which must be
    /// the same for all.
    fn objects(&self) -> Result<(Vec<Object>, (usize, usize)), String> {
        let views = self.views();
        let view = views[0].as_ref().ok_or("the primary is down")?;
        let reader = view.index.read().map_err(|e| e.to_string())?;
        let mut geometries = BTreeSet::new();
        let mut objects = Vec::new();
        for (key, data) in lock(&self.world.expected).iter() {
            let entry = reader
                .entry(&self.world.shard, key)
                .map_err(|e| e.to_string())?;
            let coded = entry
                .and_then(|entry| entry.object)
                .and_then(|object| object.coded)
                .ok_or_else(|| format!("{key} is not coded"))?;
            let mut stripes = Vec::new();
            for stripe in &coded.stripes {
                let geometry = stripe.geometry();
                geometries.insert((geometry.data_fragments(), geometry.parity_fragments()));
                stripes.push(stripe.fragments().iter().map(|l| l.node.clone()).collect());
            }
            objects.push(Object {
                key: key.clone(),
                data: data.clone(),
                stripes,
            });
        }
        match geometries.into_iter().collect::<Vec<_>>()[..] {
            [geometry] => Ok((objects, geometry)),
            ref found => Err(format!("the stripes have geometries {found:?}, not one")),
        }
    }

    /// The report of the loss combinations, or the first thing that went
    /// wrong in them.
    pub(super) fn combination_outcome(&self) -> Result<Option<CombinationReport>, String> {
        let Some(combinations) = &self.world.combinations else {
            return Ok(None);
        };
        if let Some(wrong) = lock(&combinations.wrong).first() {
            return Err(wrong.clone());
        }
        Ok(Some(lock(&combinations.report).clone()))
    }
}

/// Reads every object in every round, through the gateway of node `round
/// % 6`, and judges each answer.
async fn read_rounds(world: &World, objects: &[Object], (data, parity): (usize, usize)) {
    let combinations = world
        .combinations
        .as_ref()
        .expect("combinations in this run");
    for (round, lost) in rounds(data + parity, parity).into_iter().enumerate() {
        *lock(&combinations.lost) = Some(lost.clone());
        let host = format!("n{}", round % NODES);
        for object in objects {
            let most = object
                .stripes
                .iter()
                .map(|nodes| lost.of_stripe(nodes))
                .max()
                .unwrap_or(0);
            let readable = most <= parity;
            let mut outcome = get(&host, &object.key, None).await;
            for _ in 1..ATTEMPTS {
                if !readable || matches!(outcome, Got::Bytes { .. }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                outcome = get(&host, &object.key, None).await;
            }
            let wrong = match (outcome, readable) {
                (Got::Bytes { body, .. }, _) if body != object.data => Some(format!(
                    "a GET of {} through {host} with {lost:?} lost returned {} bytes that are \
                     not those of the version written",
                    object.key,
                    body.len()
                )),
                (Got::Bytes { .. }, true) => {
                    lock(&combinations.report).served += 1;
                    None
                }
                (Got::Bytes { .. }, false) => Some(format!(
                    "a GET of {} through {host} was served with {lost:?} lost, {most} fragments \
                     of a stripe of {data}+{parity}",
                    object.key
                )),
                (Got::Unexpected(what), _) => {
                    Some(format!("a GET of {} through {host}: {what}", object.key))
                }
                (_, true) => Some(format!(
                    "a GET of {} through {host} failed with only {lost:?} lost, at most {most} \
                     fragments of each stripe of {data}+{parity}",
                    object.key
                )),
                (_, false) => {
                    lock(&combinations.report).refused += 1;
                    None
                }
            };
            if let Some(wrong) = wrong {
                lock(&combinations.wrong).push(wrong);
            }
        }
        let mut report = lock(&combinations.report);
        match lost {
            Lost::Fragments(_) => report.fragment_sets += 1,
            Lost::Holders(_) => report.holder_sets += 1,
        }
    }
    *lock(&combinations.lost) = None;
    combinations.done.store(true, Ordering::SeqCst);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn combinations_come_smallest_first_in_order() {
        assert_eq!(
            combinations(4, 2),
            vec![
                vec![0],
                vec![1],
                vec![2],
                vec![3],
                vec![0, 1],
                vec![0, 2],
                vec![0, 3],
                vec![1, 2],
                vec![1, 3],
                vec![2, 3],
            ]
        );
        assert_eq!(combinations(6, 3).len(), 6 + 15 + 20);
        assert_eq!(combinations(2, 5).len(), 3);
    }

    #[test]
    fn rounds_cover_every_set_of_indices_and_holders() {
        // 3+2: the empty set, 25 sets of indices, and 41 of holders.
        let rounds = rounds(5, 2);
        assert_eq!(rounds.len(), 1 + 25 + 41);
        let holders: BTreeSet<NodeId> = ["n1", "n4"].iter().map(|n| n.parse().unwrap()).collect();
        assert!(rounds.contains(&Lost::Holders(holders.clone())));
        let nodes: Vec<NodeId> = ["n0", "n1", "n2", "n3", "n4"]
            .iter()
            .map(|n| n.parse().unwrap())
            .collect();
        assert_eq!(Lost::Holders(holders).of_stripe(&nodes), 2);
        assert_eq!(Lost::Fragments([0, 4, 5].into()).of_stripe(&nodes), 2);
    }
}
