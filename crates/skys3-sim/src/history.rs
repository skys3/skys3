//! Histories of client operations on keys, for the checkers in
//! [`crate::check`].
//!
//! A [`History`] records each operation twice: when a client calls it and
//! when the answer arrives. Both events take the next value of one counter,
//! so the order of moments is the order in which events happened in the
//! simulation, whichever host they happened on. An operation that never
//! gets an answer stays [`Outcome::Unknown`].
//!
//! An operation sent to a named server ([`History::call_to`]) can also be
//! closed by that server's crash ([`History::crashed`]): a request that
//! reached a process can take effect only while the process lives, so an
//! operation still unanswered when it dies ends then, as
//! [`Outcome::Failed`] unless an answer it sent before dying arrives
//! later. This is what keeps an unacknowledged write from resurfacing over
//! a later acknowledged one unnoticed (design §5.2).
//!
//! Values are opaque strings that identify the write that stored them, such
//! as the ETag of a body no other write uses.

use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

/// The condition of a conditional write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Condition {
    /// Unconditional.
    None,
    /// `If-None-Match: *`: only if the key holds no value.
    IfAbsent,
    /// `If-Match`: only if the key holds this value.
    IfMatch(String),
}

/// What a client asked for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Call {
    /// Store `value` under the key, if `condition` holds.
    Put {
        /// The value written.
        value: String,
        /// The write's condition.
        condition: Condition,
    },
    /// Remove the key's value.
    Delete,
    /// Read the key's value.
    Get,
}

impl Call {
    /// Whether the call may change the key.
    #[must_use]
    pub fn is_write(&self) -> bool {
        !matches!(self, Call::Get)
    }
}

/// How an operation ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The write was acknowledged: it took effect once, between its call
    /// and its answer.
    Done,
    /// The conditional write was refused because its condition did not
    /// hold; it took no effect.
    ConditionFailed,
    /// The read found this value, or `None` if the key held none.
    Read(Option<String>),
    /// The operation failed with an answer that does not rule out an
    /// effect, such as a `503`. A write then took effect at most once,
    /// after its call and before every write called after its answer: a
    /// node sequences a write before it answers, never after it reported
    /// failure, and applies writes in sequence (design §5.2). The write
    /// may commit after its answer, so a read called after the answer, or
    /// a conditional write refused on such a read, may still miss it.
    Failed,
    /// No answer arrived, for example because the connection broke or the
    /// client timed out. A write then took effect at most once, at any time
    /// after its call.
    Unknown,
}

/// One operation of a history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Operation {
    /// Who called it, for reports.
    pub process: String,
    /// The key.
    pub key: String,
    /// What was asked.
    pub call: Call,
    /// When it was called.
    pub called: u64,
    /// When its answer arrived, or `None` for [`Outcome::Unknown`].
    pub answered: Option<u64>,
    /// How it ended.
    pub outcome: Outcome,
}

impl Operation {
    /// Whether the operation may have changed its key: a write that was
    /// not refused for its condition.
    #[must_use]
    pub fn may_have_written(&self) -> bool {
        self.call.is_write() && self.outcome != Outcome::ConditionFailed
    }

    /// Whether the operation surely ended before `other` was called.
    #[must_use]
    pub fn precedes(&self, other: &Operation) -> bool {
        self.answered
            .is_some_and(|answered| answered < other.called)
    }

    /// The value the operation leaves if it writes: its value for a `PUT`,
    /// `None` for a `DELETE`.
    #[must_use]
    pub fn written(&self) -> Option<&str> {
        match &self.call {
            Call::Put { value, .. } => Some(value),
            Call::Delete | Call::Get => None,
        }
    }
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let answered = self
            .answered
            .map_or_else(|| "-".to_owned(), |answered| answered.to_string());
        write!(
            f,
            "[{}..{answered}] {} {:?} -> {:?}",
            self.called, self.process, self.call, self.outcome
        )
    }
}

/// A recorder of operations. Clones share the history.
#[derive(Clone, Debug, Default)]
pub struct History {
    state: Arc<Mutex<State>>,
}

#[derive(Debug, Default)]
struct State {
    clock: u64,
    operations: Vec<Operation>,
    /// The server each operation was sent to, if the caller named one.
    servers: Vec<Option<String>>,
}

/// An operation that was called and has not been answered yet.
#[derive(Debug)]
#[must_use = "an operation left pending stays unknown"]
pub struct Pending {
    index: usize,
}

impl History {
    /// An empty history.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records that `process` called `call` on `key`, now. Until
    /// [`History::answer`] records its outcome, it is unknown.
    pub fn call(&self, process: &str, key: &str, call: Call) -> Pending {
        self.record(process, None, key, call)
    }

    /// Records that `process` called `call` on `key`, now, in a request
    /// that only the current life of `server` can receive: one on a
    /// connection that life accepted, for example, or one whose sender
    /// gives up before the server restarts. [`History::crashed`] ends the
    /// operation if `server` dies first.
    pub fn call_to(&self, process: &str, server: &str, key: &str, call: Call) -> Pending {
        self.record(process, Some(server), key, call)
    }

    fn record(&self, process: &str, server: Option<&str>, key: &str, call: Call) -> Pending {
        let mut state = self.state();
        state.clock += 1;
        let called = state.clock;
        state.operations.push(Operation {
            process: process.to_owned(),
            key: key.to_owned(),
            call,
            called,
            answered: None,
            outcome: Outcome::Unknown,
        });
        state.servers.push(server.map(str::to_owned));
        Pending {
            index: state.operations.len() - 1,
        }
    }

    /// Records that the process serving as `server` died, now: every
    /// operation sent to it with [`History::call_to`] and not yet answered
    /// ends now, as [`Outcome::Failed`]. A write among them took effect
    /// before this moment or never.
    pub fn crashed(&self, server: &str) {
        let mut state = self.state();
        state.clock += 1;
        let now = state.clock;
        let State {
            operations,
            servers,
            ..
        } = &mut *state;
        for (operation, sent_to) in operations.iter_mut().zip(servers.iter()) {
            if operation.answered.is_none() && sent_to.as_deref() == Some(server) {
                operation.answered = Some(now);
                operation.outcome = Outcome::Failed;
            }
        }
    }

    /// Records the outcome of `pending`, now. [`Outcome::Unknown`] leaves
    /// the operation without an answer.
    ///
    /// If its server's crash ended the operation already, the operation
    /// keeps that end: an answer that arrives after the crash was sent
    /// before it. A definite outcome (an acknowledgement, a refused
    /// condition, or a read) replaces [`Outcome::Failed`]; a failure or no
    /// answer leaves it.
    pub fn answer(&self, pending: Pending, outcome: Outcome) {
        let mut state = self.state();
        state.clock += 1;
        let answered = state.clock;
        let operation = &mut state.operations[pending.index];
        if operation.answered.is_some() {
            if !matches!(outcome, Outcome::Failed | Outcome::Unknown) {
                operation.outcome = outcome;
            }
            return;
        }
        operation.answered = (outcome != Outcome::Unknown).then_some(answered);
        operation.outcome = outcome;
    }

    /// The moment of the latest event recorded so far: every operation
    /// called or answered from now on gets a later one.
    #[must_use]
    pub fn now(&self) -> u64 {
        self.state().clock
    }

    /// Every operation so far, in the order they were called.
    #[must_use]
    pub fn operations(&self) -> Vec<Operation> {
        self.state().operations.clone()
    }

    /// The number of operations so far.
    #[must_use]
    pub fn len(&self) -> usize {
        self.state().operations.len()
    }

    /// Whether no operation was called yet.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
