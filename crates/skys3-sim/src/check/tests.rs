use super::*;
use crate::history::{Call, Condition, History, Outcome};

/// Builds histories with explicit moments: each operation is
/// `(called, answered)`.
struct Builder(Vec<Operation>);

impl Builder {
    fn new() -> Self {
        Self(Vec::new())
    }

    fn op(mut self, key: &str, call: Call, at: (u64, Option<u64>), outcome: Outcome) -> Self {
        self.0.push(Operation {
            process: format!("c{}", self.0.len()),
            key: key.to_owned(),
            call,
            called: at.0,
            answered: at.1,
            outcome,
        });
        self
    }

    fn put(self, value: &str, at: (u64, Option<u64>), outcome: Outcome) -> Self {
        self.put_if(value, Condition::None, at, outcome)
    }

    fn put_if(
        self,
        value: &str,
        condition: Condition,
        at: (u64, Option<u64>),
        outcome: Outcome,
    ) -> Self {
        let call = Call::Put {
            value: value.to_owned(),
            condition,
        };
        self.op("k", call, at, outcome)
    }

    fn get(self, found: Option<&str>, at: (u64, Option<u64>)) -> Self {
        let outcome = Outcome::Read(found.map(str::to_owned));
        self.op("k", Call::Get, at, outcome)
    }

    fn delete(self, at: (u64, Option<u64>), outcome: Outcome) -> Self {
        self.op("k", Call::Delete, at, outcome)
    }

    fn linearizable(&self) -> bool {
        check_linearizable(&self.0).is_ok()
    }
}

#[test]
fn sequential_histories() {
    let history = Builder::new()
        .get(None, (1, Some(2)))
        .put("a", (3, Some(4)), Outcome::Done)
        .get(Some("a"), (5, Some(6)))
        .delete((7, Some(8)), Outcome::Done)
        .get(None, (9, Some(10)));
    assert!(history.linearizable());

    let stale = Builder::new()
        .put("a", (1, Some(2)), Outcome::Done)
        .put("b", (3, Some(4)), Outcome::Done)
        .get(Some("a"), (5, Some(6)));
    let violation = check_linearizable(&stale.0).unwrap_err();
    assert_eq!(violation.key, "k");
    assert_eq!(violation.operations.len(), 3);
    let report = violation.to_string();
    assert!(report.starts_with("key \"k\": no order"), "{report}");
    assert!(
        report.contains("[5..6] c2 Get -> Read(Some(\"a\"))"),
        "{report}"
    );
}

#[test]
fn concurrent_operations_may_take_either_order() {
    // The read overlaps both writes, so it may see either.
    for seen in ["a", "b"] {
        let history = Builder::new()
            .put("a", (1, Some(4)), Outcome::Done)
            .put("b", (2, Some(5)), Outcome::Done)
            .get(Some(seen), (3, Some(6)));
        assert!(history.linearizable(), "{seen}");
    }
    // Two reads after both writes must agree.
    let history = Builder::new()
        .put("a", (1, Some(3)), Outcome::Done)
        .put("b", (2, Some(4)), Outcome::Done)
        .get(Some("a"), (5, Some(6)))
        .get(Some("b"), (7, Some(8)));
    assert!(!history.linearizable());
}

#[test]
fn unanswered_writes_may_apply_late_or_never() {
    let never = Builder::new()
        .put("a", (1, None), Outcome::Unknown)
        .get(None, (2, Some(3)));
    assert!(never.linearizable());
    let late = Builder::new()
        .put("a", (1, None), Outcome::Unknown)
        .put("b", (2, Some(3)), Outcome::Done)
        .get(Some("b"), (4, Some(5)))
        .get(Some("a"), (6, Some(7)));
    assert!(late.linearizable());
    // A value nobody wrote is never explained.
    let invented = Builder::new().get(Some("z"), (1, Some(2)));
    assert!(!invented.linearizable());
}

#[test]
fn failed_writes_never_apply_over_a_later_write() {
    // Seen before its failure was answered: fine.
    let history = Builder::new()
        .put("a", (1, Some(4)), Outcome::Failed)
        .get(Some("a"), (2, Some(3)));
    assert!(history.linearizable());
    // Dropped: fine.
    let history = Builder::new()
        .put("a", (1, Some(2)), Outcome::Failed)
        .get(None, (3, Some(4)));
    assert!(history.linearizable());
    // Committed after its failure was answered, with no later write: fine,
    // also after a read that missed it.
    let history = Builder::new()
        .put("a", (1, Some(2)), Outcome::Failed)
        .get(None, (3, Some(4)))
        .get(Some("a"), (5, Some(6)));
    assert!(history.linearizable());
    // Resurfacing over a later acknowledged write breaks §5.2, a PUT or a
    // DELETE.
    let history = Builder::new()
        .put("a", (1, Some(2)), Outcome::Failed)
        .put("b", (3, Some(4)), Outcome::Done)
        .get(Some("a"), (5, Some(6)));
    assert!(!history.linearizable());
    let history = Builder::new()
        .put("a", (1, Some(2)), Outcome::Failed)
        .op("k", Call::Delete, (3, Some(4)), Outcome::Done)
        .get(Some("a"), (5, Some(6)));
    assert!(!history.linearizable());
    // A conditional write refused on a read may miss it too.
    let history = Builder::new()
        .put("b", (1, Some(2)), Outcome::Done)
        .op("k", Call::Delete, (3, Some(4)), Outcome::Failed)
        .put_if(
            "c",
            Condition::IfAbsent,
            (5, Some(6)),
            Outcome::ConditionFailed,
        )
        .get(None, (7, Some(8)));
    assert!(history.linearizable());
    // So does resurfacing over a later failed write seen to take effect.
    let history = Builder::new()
        .put("a", (1, Some(2)), Outcome::Failed)
        .put("b", (3, Some(4)), Outcome::Failed)
        .get(Some("b"), (5, Some(6)))
        .get(Some("a"), (7, Some(8)));
    assert!(!history.linearizable());
    // Failed and unanswered reads constrain nothing.
    let history = Builder::new()
        .put("a", (1, Some(2)), Outcome::Done)
        .op("k", Call::Get, (3, Some(4)), Outcome::Failed)
        .op("k", Call::Get, (5, None), Outcome::Unknown);
    assert!(history.linearizable());
}

#[test]
fn conditional_writes() {
    let history = Builder::new()
        .put_if("a", Condition::IfAbsent, (1, Some(2)), Outcome::Done)
        .put_if(
            "b",
            Condition::IfAbsent,
            (3, Some(4)),
            Outcome::ConditionFailed,
        )
        .put_if(
            "c",
            Condition::IfMatch("a".into()),
            (5, Some(6)),
            Outcome::Done,
        )
        .put_if(
            "d",
            Condition::IfMatch("a".into()),
            (7, Some(8)),
            Outcome::ConditionFailed,
        )
        .get(Some("c"), (9, Some(10)));
    assert!(history.linearizable());

    // Two racing creations cannot both win.
    let history = Builder::new()
        .put_if("a", Condition::IfAbsent, (1, Some(3)), Outcome::Done)
        .put_if("b", Condition::IfAbsent, (2, Some(4)), Outcome::Done);
    assert!(!history.linearizable());
    // A refusal needs the condition to be false.
    let history = Builder::new().put_if(
        "a",
        Condition::IfAbsent,
        (1, Some(2)),
        Outcome::ConditionFailed,
    );
    assert!(!history.linearizable());
    let history = Builder::new().put("a", (1, Some(2)), Outcome::Done).put_if(
        "b",
        Condition::IfMatch("a".into()),
        (3, Some(4)),
        Outcome::ConditionFailed,
    );
    assert!(!history.linearizable());
    // An If-Match on an empty key never holds, and a write of unknown
    // outcome whose condition fails is dropped.
    let history = Builder::new()
        .put_if(
            "b",
            Condition::IfMatch("a".into()),
            (1, Some(2)),
            Outcome::ConditionFailed,
        )
        .put_if(
            "c",
            Condition::IfMatch("a".into()),
            (3, None),
            Outcome::Unknown,
        )
        .get(None, (4, Some(5)));
    assert!(history.linearizable());
}

#[test]
fn values_nobody_read_still_count_as_values() {
    // Neither "a" nor "b" is ever read, yet each explains a refusal.
    let history = Builder::new()
        .put("a", (1, None), Outcome::Unknown)
        .put_if(
            "b",
            Condition::IfAbsent,
            (2, Some(3)),
            Outcome::ConditionFailed,
        )
        .delete((4, None), Outcome::Unknown)
        .delete((5, None), Outcome::Unknown)
        .put_if("c", Condition::IfAbsent, (6, Some(7)), Outcome::Done)
        .get(Some("c"), (8, Some(9)));
    assert!(history.linearizable());
    // A refusal with nothing written before it is still unexplained.
    let history = Builder::new()
        .put_if(
            "b",
            Condition::IfAbsent,
            (1, Some(2)),
            Outcome::ConditionFailed,
        )
        .put("a", (3, None), Outcome::Unknown);
    assert!(!history.linearizable());
}

#[test]
fn keys_are_checked_separately() {
    let mut history = Builder::new().put("a", (1, Some(2)), Outcome::Done);
    history.0.push(Operation {
        process: "c".into(),
        key: "other".into(),
        call: Call::Get,
        called: 3,
        answered: Some(4),
        outcome: Outcome::Read(None),
    });
    assert!(history.linearizable());
}

#[test]
fn many_concurrent_unknown_writes_stay_tractable() {
    let mut history = Builder::new();
    for n in 0..40 {
        history = history.put(&format!("v{n}"), (n, None), Outcome::Unknown);
    }
    let history = history
        .put("last", (100, Some(101)), Outcome::Done)
        .get(Some("v7"), (102, Some(103)))
        .get(Some("v7"), (104, Some(105)));
    assert!(history.linearizable());
}

#[test]
fn the_search_gives_up_on_a_huge_history() {
    // Many overlapping reads of a value that never appears force the search
    // through every order of the writes.
    let mut history = Builder::new();
    for n in 0..24 {
        history = history.put(&format!("v{n}"), (n, Some(1000 + n)), Outcome::Done);
    }
    let history = history.get(Some("missing"), (500, Some(2000)));
    let violation = check_linearizable_within(&history.0, 10_000).unwrap_err();
    assert!(
        violation.reason.contains("gave up after 10000"),
        "{violation}"
    );
}

fn survivors(copies: &[Option<&str>]) -> Survivors {
    Survivors {
        copies: copies.iter().map(|c| c.map(str::to_owned)).collect(),
        ..Survivors::default()
    }
}

fn durable(history: &Builder, survivor: Survivors) -> Result<(), Violation> {
    check_durable(&history.0, &BTreeMap::from([("k".to_owned(), survivor)]))
}

#[test]
fn acknowledged_writes_must_survive() {
    let history =
        Builder::new()
            .put("a", (1, Some(2)), Outcome::Done)
            .put("b", (3, Some(4)), Outcome::Done);
    assert!(durable(&history, survivors(&[Some("b")])).is_ok());
    assert!(durable(&history, survivors(&[None, Some("b")])).is_ok());
    let violation = durable(&history, survivors(&[Some("a")])).unwrap_err();
    assert!(violation.reason.contains("is lost"), "{violation}");
    assert!(durable(&history, survivors(&[None])).is_err());
    assert!(check_durable(&history.0, &BTreeMap::new()).is_err());

    // Flushed, or reported lost.
    let flushed = Survivors {
        flushed: Some(Some("b".into())),
        ..survivors(&[Some("a")])
    };
    assert!(durable(&history, flushed).is_ok());
    let lost = Survivors {
        reported_lost: true,
        ..Survivors::default()
    };
    assert!(durable(&history, lost).is_ok());
}

#[test]
fn later_writes_supersede() {
    // A concurrent write of unknown outcome may follow the acknowledged one.
    let history = Builder::new()
        .put("a", (1, Some(3)), Outcome::Done)
        .put("b", (2, None), Outcome::Unknown)
        .delete((4, Some(5)), Outcome::Failed);
    assert!(durable(&history, survivors(&[Some("a")])).is_ok());
    assert!(durable(&history, survivors(&[Some("b")])).is_ok());
    assert!(durable(&history, survivors(&[None])).is_ok());
    // A write answered before the acknowledged one was called cannot.
    let history = Builder::new()
        .put("old", (1, Some(2)), Outcome::Failed)
        .put("a", (3, Some(4)), Outcome::Done)
        .put_if(
            "x",
            Condition::IfAbsent,
            (5, Some(6)),
            Outcome::ConditionFailed,
        );
    assert!(durable(&history, survivors(&[Some("old")])).is_err());
    assert!(durable(&history, survivors(&[Some("x")])).is_err());
    // An acknowledged delete survives as absence.
    let history = Builder::new()
        .put("a", (1, Some(2)), Outcome::Done)
        .delete((3, Some(4)), Outcome::Done);
    assert!(durable(&history, survivors(&[None])).is_ok());
    assert!(durable(&history, survivors(&[Some("a")])).is_err());
}

#[test]
fn recorded_histories_check() {
    let history = History::new();
    assert!(history.is_empty());
    let put = history.call(
        "c1",
        "k",
        Call::Put {
            value: "a".into(),
            condition: Condition::None,
        },
    );
    let get = history.call("c2", "k", Call::Get);
    history.answer(put, Outcome::Done);
    history.answer(get, Outcome::Read(Some("a".into())));
    let lost = history.call("c1", "k", Call::Delete);
    history.answer(lost, Outcome::Unknown);
    let operations = history.operations();
    assert_eq!(history.len(), 3);
    assert_eq!((operations[0].called, operations[0].answered), (1, Some(3)));
    assert_eq!((operations[2].called, operations[2].answered), (5, None));
    assert!(operations[2].may_have_written());
    assert!(operations[0].precedes(&operations[2]));
    assert!(!operations[2].precedes(&operations[0]));
    assert_eq!(operations[1].written(), None);
    check_linearizable(&operations).unwrap();
    check_durable(
        &operations,
        &BTreeMap::from([("k".into(), survivors(&[None]))]),
    )
    .unwrap();
}

#[test]
fn clean_copies_count_only_through_the_remote() {
    // A write_back bucket: an acknowledged write is flushed or still dirty.
    let history = Builder::new().put("a", (1, Some(2)), Outcome::Done);
    let copy = |clean: bool, flushed: Option<&str>| Survivors {
        clean: vec![clean],
        flushed: Some(flushed.map(str::to_owned)),
        ..survivors(&[Some("a")])
    };
    assert!(durable(&history, copy(false, None)).is_ok());
    assert!(durable(&history, copy(true, Some("a"))).is_ok());
    // Clean, yet the remote never got it: eviction would lose it.
    let violation = durable(&history, copy(true, None)).unwrap_err();
    assert!(violation.reason.contains("is lost"), "{violation}");
    // A second, dirty copy keeps it.
    let two = Survivors {
        clean: vec![true],
        flushed: Some(None),
        ..survivors(&[Some("a"), Some("a")])
    };
    assert!(durable(&history, two).is_ok());
}

fn put_call(value: &str) -> Call {
    Call::Put {
        value: value.into(),
        condition: Condition::None,
    }
}

#[test]
fn a_crash_ends_the_operations_sent_to_the_server() {
    let history = History::new();
    let lost = history.call_to("c1", "node", "k", put_call("old"));
    let elsewhere = history.call_to("c2", "other", "k", Call::Get);
    let unnamed = history.call("c3", "k", Call::Delete);
    let acked = history.call_to("c4", "node", "k", put_call("a"));
    history.crashed("node");
    // Answers that arrive after the crash were sent before it.
    history.answer(lost, Outcome::Unknown);
    history.answer(acked, Outcome::Done);
    history.answer(elsewhere, Outcome::Unknown);
    history.answer(unnamed, Outcome::Unknown);
    let operations = history.operations();
    assert_eq!(operations[0].outcome, Outcome::Failed);
    assert_eq!(operations[0].answered, Some(5));
    assert_eq!(operations[3].outcome, Outcome::Done);
    assert_eq!(operations[3].answered, Some(5));
    assert_eq!(operations[1].answered, None);
    assert_eq!(operations[2].answered, None);
}

#[test]
fn an_unacknowledged_write_must_not_resurface_over_a_later_one() {
    // A PUT is cut off by a crash; after the restart a later PUT is
    // acknowledged, and then the first one's value comes back.
    let history = History::new();
    let cut = history.call_to("c1", "node", "k", put_call("old"));
    history.crashed("node");
    history.answer(cut, Outcome::Unknown);
    let later = history.call_to("c1", "node", "k", put_call("new"));
    history.answer(later, Outcome::Done);
    let read = history.call_to("c2", "node", "k", Call::Get);
    history.answer(read, Outcome::Read(Some("old".into())));
    let operations = history.operations();
    assert!(check_linearizable(&operations).is_err());
    let survivor = BTreeMap::from([("k".to_owned(), survivors(&[Some("old")]))]);
    assert!(check_durable(&operations[..2], &survivor).is_err());

    // Without the crash, the first PUT might still be on its way, and the
    // same history is fine.
    let history = History::new();
    let open = history.call("c1", "k", put_call("old"));
    history.answer(open, Outcome::Unknown);
    let later = history.call("c1", "k", put_call("new"));
    history.answer(later, Outcome::Done);
    let read = history.call("c2", "k", Call::Get);
    history.answer(read, Outcome::Read(Some("old".into())));
    let operations = history.operations();
    check_linearizable(&operations).unwrap();
    check_durable(&operations, &survivor).unwrap();
}
