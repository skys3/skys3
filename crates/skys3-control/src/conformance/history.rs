//! A linearizability check for the history of one compare-and-swap
//! register, for [`histories_are_linearizable`](super::histories_are_linearizable).
//!
//! Every value in a history is unique and every successful write names the
//! version it replaced, so the successful writes must form one chain from
//! the absent register: the chain is the only order a linearization can
//! give them. That leaves each operation one position in the chain, or for
//! a failed write a lower bound, and real time must not contradict those
//! positions. No search is needed, unlike for plain registers.
//!
//! Each operation `op` takes effect at one point between its call and its
//! return. Let `before(op)` and `after(op)` be the chain positions just
//! before and just after that point:
//!
//! - a read that returned position `p`: `before = after = p`;
//! - a write that succeeded at position `q`: `before = q - 1`, `after = q`;
//! - a write that failed its precondition at the version of position `p`:
//!   the register had been at `p` before the call (the writer read it), so
//!   it had moved past `p`: `after >= p + 1`, and `before` is unbounded.
//!
//! If `a` returned before `b` was called, `a`'s point precedes `b`'s, so
//! `after(a) <= before(b)`. And the write that brought the register to an
//! operation's position must have been called before the operation
//! returned. These conditions are necessary for linearizability, and they
//! catch what a broken store does: stale reads, lost or forked writes, and
//! writes that apply out of real-time order.

use std::collections::HashMap;
use std::fmt;

use bytes::Bytes;

use crate::store::{Expected, Version, Versioned};

/// A logical time: the value of a counter every operation increments when
/// it is called and when it returns.
pub(super) type Tick = u64;

/// What one operation did.
#[derive(Debug, Clone)]
pub(super) enum Op {
    /// A `get` and what it returned.
    Read(Option<Versioned>),
    /// A `put_if` of a unique `value`.
    Write {
        expected: Expected,
        value: Bytes,
        result: WriteResult,
    },
}

/// What a `put_if` answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum WriteResult {
    /// Written at this version.
    Written(Version),
    /// The precondition failed.
    PreconditionFailed,
    /// No answer: the write may have applied, at any time after its call.
    Unknown,
}

/// One operation on the register.
#[derive(Debug, Clone)]
pub(super) struct Event {
    /// When it was called.
    pub call: Tick,
    /// When it returned. An unanswered write never returned.
    pub ret: Tick,
    /// What it did.
    pub op: Op,
}

/// Why a history is not linearizable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Violation(String);

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

fn violation<T>(message: impl Into<String>) -> Result<T, Violation> {
    Err(Violation(message.into()))
}

/// An operation's place in the chain.
#[derive(Debug, Clone, Copy)]
struct Bounds {
    call: Tick,
    ret: Tick,
    /// The position just before its point, if bounded.
    before: Option<usize>,
    /// The least position just after its point.
    after: usize,
    /// The event, for reports.
    event: usize,
}

/// Checks the history of one register. `events` holds every operation on
/// it, including a final read after every other operation returned.
pub(super) fn check(events: &[Event]) -> Result<(), Violation> {
    let written = resolve(events)?;
    let chain = chain(events, &written)?;
    let position: HashMap<&Version, usize> = chain
        .iter()
        .enumerate()
        .map(|(n, &event)| (&written[&event], n + 1))
        .collect();
    let position_of = |version: Option<&Version>| match version {
        None => Some(0),
        Some(version) => position.get(version).copied(),
    };

    let mut bounds = Vec::new();
    for (event, e) in events.iter().enumerate() {
        let (before, after) = match &e.op {
            Op::Read(read) => {
                let p = position_of(read.as_ref().map(|r| &r.version))
                    .expect("resolve placed every value read");
                (Some(p), p)
            }
            Op::Write { .. } => match written.get(&event) {
                Some(version) => {
                    let q = position[version];
                    (Some(q - 1), q)
                }
                None => match &e.op {
                    Op::Write {
                        expected,
                        result: WriteResult::PreconditionFailed,
                        ..
                    } => {
                        let expected = match expected {
                            Expected::Absent => None,
                            Expected::Version(version) => Some(version),
                        };
                        // A version the register never held fails without
                        // telling anything.
                        let Some(p) = position_of(expected) else {
                            continue;
                        };
                        (None, p + 1)
                    }
                    // An unanswered write that nothing observed.
                    _ => continue,
                },
            },
        };
        bounds.push(Bounds {
            call: e.call,
            ret: e.ret,
            before,
            after,
            event,
        });
    }

    // The write that brought the register to an operation's position was
    // called before the operation returned.
    for b in &bounds {
        if b.after == 0 {
            continue;
        }
        let Some(&writer) = chain.get(b.after - 1) else {
            return violation(format!(
                "{:?} failed its precondition, but no write ever replaced the version it named",
                events[b.event]
            ));
        };
        if events[writer].call > b.ret {
            return violation(format!(
                "{:?} observed {:?}, which was called after it returned",
                events[b.event], events[writer]
            ));
        }
    }

    // Real time: an operation that returned before another was called took
    // effect at an earlier position. Sweep the operations in call order,
    // keeping the latest position among those that have returned.
    let mut by_ret: Vec<&Bounds> = bounds.iter().collect();
    by_ret.sort_by_key(|b| b.ret);
    let mut by_call: Vec<&Bounds> = bounds.iter().collect();
    by_call.sort_by_key(|b| b.call);
    let mut returned = by_ret.iter().peekable();
    let mut latest: Option<&Bounds> = None;
    for b in by_call {
        while let Some(a) = returned.next_if(|a| a.ret < b.call) {
            if latest.is_none_or(|l| a.after > l.after) {
                latest = Some(a);
            }
        }
        if let (Some(a), Some(before)) = (latest, b.before)
            && a.after > before
        {
            return violation(format!(
                "{:?} returned before {:?} was called, but took effect after it",
                events[a.event], events[b.event]
            ));
        }
    }
    Ok(())
}

/// The version each applied write wrote, by event: the answered ones, and
/// unanswered ones whose value a read returned. Checks that reads return
/// values some write wrote, at the version it wrote them.
fn resolve(events: &[Event]) -> Result<HashMap<usize, Version>, Violation> {
    let mut by_value = HashMap::new();
    let mut written = HashMap::new();
    for (n, event) in events.iter().enumerate() {
        if let Op::Write { value, result, .. } = &event.op {
            let earlier = by_value.insert(value.clone(), n);
            assert!(earlier.is_none(), "the history's values are not unique");
            if let WriteResult::Written(version) = result {
                written.insert(n, version.clone());
            }
        }
    }
    for event in events {
        let Op::Read(Some(read)) = &event.op else {
            continue;
        };
        let Some(&writer) = by_value.get(&read.value) else {
            return violation(format!("{event:?} read a value no write wrote"));
        };
        let Op::Write { result, .. } = &events[writer].op else {
            unreachable!("by_value holds writes");
        };
        match result {
            WriteResult::PreconditionFailed => {
                return violation(format!(
                    "{event:?} read the value of {:?}, which failed its precondition",
                    events[writer]
                ));
            }
            WriteResult::Written(_) | WriteResult::Unknown => {
                let version = written
                    .entry(writer)
                    .or_insert_with(|| read.version.clone());
                if *version != read.version {
                    return violation(format!(
                        "{event:?} read the value of {:?} at another version",
                        events[writer]
                    ));
                }
            }
        }
    }
    Ok(written)
}

/// The applied writes in the order they replaced each other, from the
/// absent register. Checks that no two replaced the same version, that
/// every one replaced a version the register held, and that no two share a
/// version.
fn chain(events: &[Event], written: &HashMap<usize, Version>) -> Result<Vec<usize>, Violation> {
    let mut successor: HashMap<Option<&Version>, usize> = HashMap::new();
    let mut versions: HashMap<&Version, usize> = HashMap::new();
    let mut applied: Vec<usize> = written.keys().copied().collect();
    applied.sort_unstable();
    for &n in &applied {
        let Op::Write { expected, .. } = &events[n].op else {
            unreachable!("only writes are written");
        };
        let from = match expected {
            Expected::Absent => None,
            Expected::Version(version) => Some(version),
        };
        if let Some(other) = successor.insert(from, n) {
            let replaced =
                from.map_or("the absent register".to_owned(), |v| format!("version {v}"));
            return violation(format!(
                "{:?} and {:?} both replaced {replaced}",
                events[other], events[n]
            ));
        }
        if let Some(other) = versions.insert(&written[&n], n) {
            return violation(format!(
                "{:?} and {:?} wrote different values at one version",
                events[other], events[n]
            ));
        }
    }
    let mut chain = Vec::with_capacity(applied.len());
    let mut current = None;
    while let Some(&n) = successor.get(&current) {
        chain.push(n);
        current = Some(&written[&n]);
    }
    if chain.len() < applied.len() {
        let stray = applied
            .iter()
            .find(|n| !chain.contains(n))
            .expect("a write is off the chain");
        return violation(format!(
            "{:?} replaced a version the register never held",
            events[*stray]
        ));
    }
    Ok(chain)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn version(v: &str) -> Version {
        Version::new(v)
    }

    fn value(v: &str) -> Bytes {
        Bytes::from(format!("value-{v}"))
    }

    fn read(call: Tick, ret: Tick, v: Option<&str>) -> Event {
        Event {
            call,
            ret,
            op: Op::Read(v.map(|v| Versioned {
                value: value(v),
                version: version(v),
            })),
        }
    }

    /// A write of value `v`, at version `v` if it was written.
    fn write(call: Tick, ret: Tick, from: Option<&str>, v: &str, result: &str) -> Event {
        Event {
            call,
            ret,
            op: Op::Write {
                expected: from.map_or(Expected::Absent, |f| Expected::Version(version(f))),
                value: value(v),
                result: match result {
                    "ok" => WriteResult::Written(version(v)),
                    "412" => WriteResult::PreconditionFailed,
                    _ => WriteResult::Unknown,
                },
            },
        }
    }

    fn fails(events: &[Event], expected: &str) {
        let error = check(events).unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
    }

    #[test]
    fn sequential_and_overlapping_histories_pass() {
        let history = [
            read(0, 1, None),
            write(2, 3, None, "a", "ok"),
            write(4, 9, Some("a"), "b", "ok"),
            // Overlaps b: it may read before or after it.
            read(5, 6, Some("a")),
            read(7, 10, Some("b")),
            write(8, 11, Some("a"), "c", "412"),
            write(12, 13, None, "d", "412"),
            read(14, 15, Some("b")),
        ];
        check(&history).unwrap();
    }

    #[test]
    fn unanswered_writes_count_once_observed() {
        let history = [
            write(0, 1, None, "a", "ok"),
            // Never answered, and seen by the next read.
            write(2, Tick::MAX, Some("a"), "b", "?"),
            // Never answered, and never seen.
            write(3, Tick::MAX, Some("a"), "c", "?"),
            read(4, 5, Some("b")),
            write(6, 7, Some("b"), "d", "ok"),
            read(8, 9, Some("d")),
        ];
        check(&history).unwrap();
    }

    #[test]
    fn a_stale_read_fails() {
        let history = [
            write(0, 1, None, "a", "ok"),
            write(2, 3, Some("a"), "b", "ok"),
            read(4, 5, Some("a")),
        ];
        fails(&history, "returned before");
    }

    #[test]
    fn reads_must_not_go_back() {
        let history = [
            write(0, 1, None, "a", "ok"),
            write(2, 20, Some("a"), "b", "ok"),
            read(3, 4, Some("b")),
            read(5, 6, Some("a")),
        ];
        fails(&history, "returned before");
    }

    #[test]
    fn a_write_after_a_failure_must_not_land_before_it() {
        // The failure at a says the register had moved past a by tick 3;
        // the only write that could move it was called after.
        let history = [
            write(0, 1, None, "a", "ok"),
            write(2, 3, Some("a"), "x", "412"),
            write(4, 5, Some("a"), "b", "ok"),
        ];
        fails(&history, "which was called after it returned");
    }

    #[test]
    fn a_failure_needs_a_write_that_replaced_its_version() {
        let history = [
            write(0, 1, None, "a", "ok"),
            write(2, 3, Some("a"), "x", "412"),
        ];
        fails(&history, "no write ever replaced");
    }

    #[test]
    fn a_read_from_the_future_fails() {
        let history = [read(0, 1, Some("a")), write(2, 3, None, "a", "ok")];
        fails(&history, "which was called after it returned");
    }

    #[test]
    fn writes_must_not_fork() {
        let history = [
            write(0, 1, None, "a", "ok"),
            write(2, 3, Some("a"), "b", "ok"),
            write(2, 4, Some("a"), "c", "ok"),
        ];
        fails(&history, "both replaced");
    }

    #[test]
    fn writes_must_follow_real_time() {
        let history = [
            write(0, 1, None, "a", "ok"),
            write(2, 3, Some("b"), "c", "ok"),
            write(4, 5, Some("a"), "b", "ok"),
        ];
        fails(&history, "returned before");
    }

    #[test]
    fn writes_must_replace_a_held_version() {
        let history = [
            write(0, 1, None, "a", "ok"),
            write(2, 3, Some("zz"), "b", "ok"),
        ];
        fails(&history, "never held");
    }

    #[test]
    fn values_must_come_from_writes() {
        fails(&[read(0, 1, Some("a"))], "no write wrote");
        let failed_then_read = [write(0, 1, None, "a", "412"), read(2, 3, Some("a"))];
        fails(&failed_then_read, "which failed its precondition");
        let mut moved = read(2, 3, Some("a"));
        if let Op::Read(Some(read)) = &mut moved.op {
            read.version = version("elsewhere");
        }
        fails(&[write(0, 1, None, "a", "ok"), moved], "at another version");
        let shared = Event {
            op: Op::Write {
                expected: Expected::Version(version("a")),
                value: value("b"),
                result: WriteResult::Written(version("a")),
            },
            ..write(2, 3, None, "unused", "ok")
        };
        fails(&[write(0, 1, None, "a", "ok"), shared], "at one version");
    }

    #[test]
    fn failures_at_unknown_versions_tell_nothing() {
        let history = [
            write(0, 1, None, "a", "ok"),
            write(2, 3, Some("never"), "x", "412"),
            read(4, 5, Some("a")),
        ];
        check(&history).unwrap();
    }
}
