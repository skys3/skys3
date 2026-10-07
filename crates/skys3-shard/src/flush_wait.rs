//! Waits for a committed version to reach the remote target, for the
//! acknowledgement of writes to `write_through` buckets (§7.5).
//!
//! The gateway that commits such a write asks the shard's primary
//! ([`Shard::await_flush`](crate::Shard::await_flush)), which passes a
//! [`FlushWaiter`] to its flusher in order with the changes it applied
//! ([`Change::Awaited`](crate::Change::Awaited)). The flusher answers once
//! the remote holds the version or a later one of its key, or once the key
//! is held in conflict; the gateway reads the answer from the matching
//! [`FlushWait`]. Nothing is logged: a waiter lives in the primary's memory
//! only, and a primary change or a flusher restart ends it unanswered, so
//! the gateway asks again.

use std::fmt;

use skys3_types::EpochSeq;
use tokio::sync::mpsc;

/// Where a version a write-through write waits for is (§7.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FlushState {
    /// The remote target holds the version, or a later version of its key,
    /// which supersedes it there.
    Flushed,
    /// The key is held in conflict (§7.2): the remote holds a write SkyS3
    /// did not make, and the version will not be flushed until the conflict
    /// is resolved.
    Conflict,
    /// Neither yet, within the time the caller waited.
    Pending,
}

/// A write-through write waiting for its version to reach the remote, as
/// the shard passes it to the flusher. The flusher answers it once, with
/// [`FlushWaiter::answer`]; a waiter dropped unanswered ends its
/// [`FlushWait`] without an answer.
#[derive(Clone)]
pub struct FlushWaiter {
    key: String,
    version: EpochSeq,
    reply: mpsc::UnboundedSender<FlushState>,
}

impl FlushWaiter {
    /// A waiter for `version` of `key`, and the wait it answers.
    #[must_use]
    pub fn new(key: String, version: EpochSeq) -> (Self, FlushWait) {
        let (reply, receiver) = mpsc::unbounded_channel();
        let waiter = Self {
            key,
            version,
            reply,
        };
        (waiter, FlushWait { receiver })
    }

    /// The key written.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// The position of the version waited for.
    #[must_use]
    pub fn version(&self) -> EpochSeq {
        self.version
    }

    /// Whether the wait is still being waited on: once its [`FlushWait`]
    /// is dropped, answering it does nothing.
    #[must_use]
    pub fn is_waited(&self) -> bool {
        !self.reply.is_closed()
    }

    /// Answers the wait: the caller takes the first answer.
    pub fn answer(&self, state: FlushState) {
        // A caller that stopped waiting needs no answer.
        let _ = self.reply.send(state);
    }
}

impl fmt::Debug for FlushWaiter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlushWaiter")
            .field("key", &self.key)
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

impl PartialEq for FlushWaiter {
    /// Waiters are equal if they wait for the same version of the same key
    /// on the same wait.
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
            && self.version == other.version
            && self.reply.same_channel(&other.reply)
    }
}

impl Eq for FlushWaiter {}

/// The caller's end of a [`FlushWaiter`].
#[derive(Debug)]
pub struct FlushWait {
    receiver: mpsc::UnboundedReceiver<FlushState>,
}

impl FlushWait {
    /// The flusher's answer, or `None` if the waiter was dropped
    /// unanswered: the flusher stopped, or the shard did.
    pub async fn answered(&mut self) -> Option<FlushState> {
        self.receiver.recv().await
    }
}

#[cfg(test)]
mod tests {
    use skys3_types::{Epoch, Seq};

    use super::*;

    fn at(seq: u64) -> EpochSeq {
        EpochSeq::new(Epoch::new(1), Seq::new(seq))
    }

    #[tokio::test]
    async fn a_wait_takes_its_answer_and_ends_with_its_waiter() {
        let (waiter, mut wait) = FlushWaiter::new("k".into(), at(3));
        assert_eq!((waiter.key(), waiter.version()), ("k", at(3)));
        assert!(waiter.is_waited());
        let copy = waiter.clone();
        assert_eq!(copy, waiter);
        waiter.answer(FlushState::Flushed);
        copy.answer(FlushState::Conflict);
        assert_eq!(wait.answered().await, Some(FlushState::Flushed));

        // Dropped unanswered, a waiter ends its wait.
        let (waiter, mut wait) = FlushWaiter::new("k".into(), at(4));
        drop(waiter);
        assert_eq!(wait.answered().await, None);

        let (waiter, wait) = FlushWaiter::new("k".into(), at(3));
        let (other, _other_wait) = FlushWaiter::new("k".into(), at(3));
        assert_ne!(waiter, other, "another wait");
        assert!(format!("{waiter:?}").contains("version"));
        drop(wait);
        assert!(!waiter.is_waited());
        waiter.answer(FlushState::Flushed);
    }
}
