//! Checkers for histories of operations on keys (design §16.1).
//!
//! - [`check_linearizable`]: every key's operations, as one primary
//!   answered them, are linearizable for a register that holds a value or
//!   nothing.
//! - [`check_durable`]: every acknowledged write is still present on a
//!   surviving copy of its key, or flushed to the remote store, unless a
//!   later write may have superseded it, or it was reported lost. In a
//!   `write_back` bucket a copy counts only while it is dirty: a clean
//!   copy says the remote store holds its value, so the remote must.
//!
//! Both take the operations of a [`History`](crate::history::History). A
//! failure is a [`Violation`] that names the key and lists its operations.
//!
//! Together they enforce the crash-consistency rule of design §5.2: every
//! acknowledged write survives recovery unless a later write superseded
//! it, and no unacknowledged write resurfaces over a later acknowledged
//! one. The second half needs to know when an unanswered write stopped
//! being able to take effect, which a server's crash decides
//! ([`History::crashed`](crate::history::History::crashed)): a write that
//! may take effect at any time could always explain a resurfaced value.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;

use crate::history::{Call, Condition, Operation, Outcome};

/// The most search states [`check_linearizable`] visits for one key before
/// it gives up, which keeps a pathological history from hanging a
/// simulation. Histories of a few hundred operations with a handful of
/// clients stay far below it.
pub const MAX_SEARCH_STATES: usize = 1 << 20;

/// The interned value of every value no read returns and no `If-Match`
/// names.
const UNOBSERVED: u32 = u32::MAX;

/// A history that breaks a checked property.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Violation {
    /// The key.
    pub key: String,
    /// What is wrong.
    pub reason: String,
    /// The key's operations, as reports print them.
    pub operations: Vec<String>,
}

impl Violation {
    fn new(key: &str, reason: String, operations: &[&Operation]) -> Self {
        Self {
            key: key.to_owned(),
            reason,
            operations: operations.iter().map(ToString::to_string).collect(),
        }
    }
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "key {:?}: {}", self.key, self.reason)?;
        for operation in &self.operations {
            write!(f, "\n  {operation}")?;
        }
        Ok(())
    }
}

impl std::error::Error for Violation {}

/// Groups `operations` by key.
fn by_key(operations: &[Operation]) -> BTreeMap<&str, Vec<&Operation>> {
    let mut keys: BTreeMap<&str, Vec<&Operation>> = BTreeMap::new();
    for operation in operations {
        keys.entry(&operation.key).or_default().push(operation);
    }
    keys
}

/// Checks that each key's operations are linearizable: some order of them,
/// consistent with real time, explains every answer when the key starts
/// without a value.
///
/// Writes that failed or got no answer may be left out of the order, since
/// they may not have taken effect; a failed one, if it is in the order,
/// lies before its answer (§5.2: a failed write never takes effect over a
/// later one). Reads without an answer constrain nothing and are ignored.
///
/// # Errors
///
/// The first key whose operations no order explains, or whose search
/// exceeds [`MAX_SEARCH_STATES`].
pub fn check_linearizable(operations: &[Operation]) -> Result<(), Violation> {
    check_linearizable_within(operations, MAX_SEARCH_STATES)
}

/// [`check_linearizable`], giving up on a key after `max_states` search
/// states.
///
/// # Errors
///
/// As [`check_linearizable`].
pub fn check_linearizable_within(
    operations: &[Operation],
    max_states: usize,
) -> Result<(), Violation> {
    for (key, operations) in by_key(operations) {
        let relevant: Vec<&Operation> = operations
            .into_iter()
            .filter(|operation| {
                operation.call.is_write()
                    || !matches!(operation.outcome, Outcome::Failed | Outcome::Unknown)
            })
            .collect();
        let mut search = Search::new(&relevant, max_states);
        match search.run() {
            Some(true) => {}
            Some(false) => {
                let reason = "no order of the operations explains every answer".to_owned();
                return Err(Violation::new(key, reason, &relevant));
            }
            None => {
                let reason =
                    format!("the search gave up after {max_states} states; shorten the history");
                return Err(Violation::new(key, reason, &relevant));
            }
        }
    }
    Ok(())
}

/// The search for a linearization of one key's operations, after Wing and
/// Gong, with visited states remembered as in Lowe's refinement.
struct Search<'a> {
    operations: &'a [&'a Operation],
    /// Each operation's value, interned, for cheap states.
    values: Vec<Option<u32>>,
    /// Each `If-Match` condition's value, interned.
    matches: Vec<Option<u32>>,
    /// For a write without an answer, the previous one with the same
    /// effect: it must come first (see [`Search::new`]).
    before: Vec<Option<usize>>,
    visited: HashSet<(Vec<u64>, Option<u32>)>,
    max_states: usize,
    exhausted: bool,
}

impl<'a> Search<'a> {
    /// Prepares the search, with two reductions that keep histories with
    /// many unanswered writes tractable:
    ///
    /// - Values that no read returns and no `If-Match` names are only ever
    ///   compared with "no value", so they all become one value.
    /// - Writes without an answer may come at any time after their call.
    ///   Of two with the same effect, the one called first can stand in
    ///   for the other wherever the other could go, so the search only
    ///   ever tries the earliest one not yet placed.
    fn new(operations: &'a [&'a Operation], max_states: usize) -> Self {
        let mut observed: HashSet<&str> = HashSet::new();
        for operation in operations {
            if let Outcome::Read(Some(value)) = &operation.outcome {
                observed.insert(value);
            }
            if let Call::Put {
                condition: Condition::IfMatch(expected),
                ..
            } = &operation.call
            {
                observed.insert(expected);
            }
        }
        let mut interned: HashMap<&str, u32> = HashMap::new();
        let mut intern = |value: &'a str| {
            if !observed.contains(value) {
                return UNOBSERVED;
            }
            let next = u32::try_from(interned.len()).expect("fewer than 2^32 values");
            *interned.entry(value).or_insert(next)
        };
        let mut values = Vec::new();
        let mut matches = Vec::new();
        for operation in operations {
            let value = match (&operation.call, &operation.outcome) {
                (Call::Put { value, .. }, _) | (_, Outcome::Read(Some(value))) => {
                    Some(intern(value))
                }
                _ => None,
            };
            values.push(value);
            matches.push(match &operation.call {
                Call::Put {
                    condition: Condition::IfMatch(expected),
                    ..
                } => Some(intern(expected)),
                _ => None,
            });
        }
        let mut last: HashMap<(u8, Option<u32>, Option<u32>), usize> = HashMap::new();
        let mut order: Vec<usize> = (0..operations.len()).collect();
        order.sort_by_key(|&index| operations[index].called);
        let mut before = vec![None; operations.len()];
        for index in order {
            let operation = operations[index];
            if operation.outcome != Outcome::Unknown {
                continue;
            }
            let kind = match &operation.call {
                Call::Get => continue,
                Call::Delete => 0,
                Call::Put { condition, .. } => match condition {
                    Condition::None => 1,
                    Condition::IfAbsent => 2,
                    Condition::IfMatch(_) => 3,
                },
            };
            before[index] = last.insert((kind, values[index], matches[index]), index);
        }
        Self {
            operations,
            values,
            matches,
            before,
            visited: HashSet::new(),
            max_states,
            exhausted: false,
        }
    }

    /// `Some(found)`, or `None` if the search gave up.
    fn run(&mut self) -> Option<bool> {
        let words = self.operations.len().div_ceil(64);
        let found = self.visit(&mut vec![0; words], None);
        (!self.exhausted).then_some(found)
    }

    fn answered(&self, index: usize) -> u64 {
        self.operations[index].answered.unwrap_or(u64::MAX)
    }

    /// Whether the operation must be in the order: one that surely took
    /// effect, or a read or refused write whose answer must be explained.
    fn required(&self, index: usize) -> bool {
        !matches!(
            self.operations[index].outcome,
            Outcome::Failed | Outcome::Unknown
        )
    }

    /// The state after the operation at `index`, if it can come next from
    /// `state` and give its answer.
    fn step(&self, index: usize, state: Option<u32>) -> Option<Option<u32>> {
        let operation = self.operations[index];
        let value = self.values[index];
        let refused = operation.outcome == Outcome::ConditionFailed;
        match &operation.call {
            Call::Get => (value == state).then_some(state),
            Call::Delete => Some(None),
            Call::Put { condition, .. } => {
                let holds = match condition {
                    Condition::None => true,
                    Condition::IfAbsent => state.is_none(),
                    Condition::IfMatch(_) => state.is_some() && state == self.matches[index],
                };
                match (holds, refused) {
                    (true, false) => Some(value),
                    (false, true) => Some(state),
                    _ => None,
                }
            }
        }
    }

    fn visit(&mut self, done: &mut Vec<u64>, state: Option<u32>) -> bool {
        let pending: Vec<usize> = (0..self.operations.len())
            .filter(|&index| done[index / 64] & (1 << (index % 64)) == 0)
            .collect();
        if pending.iter().all(|&index| !self.required(index)) {
            return true;
        }
        if self.exhausted || !self.visited.insert((done.clone(), state)) {
            return false;
        }
        if self.visited.len() > self.max_states {
            self.exhausted = true;
            return false;
        }
        // Only operations called before every pending one was answered can
        // come next.
        let horizon = pending
            .iter()
            .map(|&index| self.answered(index))
            .min()
            .unwrap_or(u64::MAX);
        let is_pending = |done: &[u64], index: usize| done[index / 64] & (1 << (index % 64)) == 0;
        for &index in &pending {
            let waits = self.before[index].is_some_and(|first| is_pending(done, first));
            if self.operations[index].called > horizon || waits {
                continue;
            }
            let bit = 1 << (index % 64);
            done[index / 64] |= bit;
            let next = self.step(index, state);
            // A write that may not have taken effect can also be dropped;
            // one without an answer never limits the horizon, so dropping
            // it is never needed.
            let droppable = !self.required(index) && self.operations[index].answered.is_some();
            let found = next.is_some_and(|next| self.visit(done, next))
                || (droppable && self.visit(done, state));
            done[index / 64] &= !bit;
            if found {
                return true;
            }
        }
        false
    }
}

/// What a key holds where it survives, for [`check_durable`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Survivors {
    /// The key's value on each surviving copy, such as each member of its
    /// shard whose disks survived, after recovery; `None` where the copy
    /// holds no value.
    pub copies: Vec<Option<String>>,
    /// For a `write_back` bucket, whether each copy, by position in
    /// `copies`, is clean: its entry says the remote store holds its value.
    /// A clean copy keeps no write by itself; only the remote store
    /// ([`Survivors::flushed`]) counts for it. Copies past the end of this
    /// list are dirty, as every copy in a `local` bucket is.
    pub clean: Vec<bool>,
    /// The key's value in the remote store, if the bucket flushes to one:
    /// `Some(None)` if the remote store holds no object.
    pub flushed: Option<Option<String>>,
    /// Whether the key was reported lost.
    pub reported_lost: bool,
}

/// Checks that every acknowledged write survived: for each key, a dirty
/// surviving copy or the remote store holds the write's value or the
/// value of a write that may have come after it, or the key was reported
/// lost. A write *may come after* an acknowledged write unless it was
/// answered before the acknowledged one was called, or its server crashed
/// first. A key missing from `survivors` has no surviving copy.
///
/// In a `write_back` bucket this is the rule that every acknowledged write
/// is either flushed or still dirty locally: a copy marked clean in
/// [`Survivors::clean`] does not count.
///
/// # Errors
///
/// The first key with an acknowledged write that is neither present,
/// superseded, nor reported lost.
pub fn check_durable(
    operations: &[Operation],
    survivors: &BTreeMap<String, Survivors>,
) -> Result<(), Violation> {
    let none = Survivors::default();
    for (key, operations) in by_key(operations) {
        let survivor = survivors.get(key).unwrap_or(&none);
        if survivor.reported_lost {
            continue;
        }
        let dirty = survivor
            .copies
            .iter()
            .enumerate()
            .filter(|(position, _)| !survivor.clean.get(*position).copied().unwrap_or(false))
            .map(|(_, value)| value);
        let held: Vec<&Option<String>> = dirty.chain(survivor.flushed.as_ref()).collect();
        let writes: Vec<&Operation> = operations
            .iter()
            .copied()
            .filter(|operation| operation.may_have_written())
            .collect();
        for acknowledged in writes.iter().filter(|w| w.outcome == Outcome::Done) {
            let survives = held.iter().any(|value| {
                writes.iter().any(|write| {
                    write.written() == value.as_deref() && !write.precedes(acknowledged)
                })
            });
            if !survives {
                let reason = format!(
                    "the acknowledged write {acknowledged} is lost: the survivors hold {held:?}"
                );
                return Err(Violation::new(key, reason, &operations));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
