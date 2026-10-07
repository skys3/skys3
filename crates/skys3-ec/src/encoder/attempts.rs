//! The fragment-writing attempts a shard primary has in progress (§8.4),
//! and what every kind of attempt (encoding, repair, moves) shares: their
//! IDs, and the commit of their publishing record.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use skys3_index::IndexError;
use skys3_io::Disk;
use skys3_log::RecordBody;
use skys3_shard::{Committed, Shard, ShardError};
use skys3_types::{AttemptId, Epoch, EpochSeq};

/// How many attempt numbers one durable reservation takes.
const ATTEMPT_BLOCK: u64 = 64;

/// Where an attempt that writes fragments is (§8.4, plan M5-04).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptState {
    /// Writing fragments; it may still be abandoned.
    Writing,
    /// Its publishing record is appended, or about to be. An appended
    /// record cannot be retracted, so the attempt stays in progress until
    /// the record commits: at `position`, once known.
    Publishing {
        /// The publishing record's position, once the shard sequenced it.
        position: Option<EpochSeq>,
    },
    /// Abandoned before its publishing record was appended. It never
    /// appends one, so its fragments are orphans.
    Abandoned,
}

/// The fragment-writing attempts a shard primary has in progress, shared
/// with whatever must not race them.
///
/// Orphan reclamation ([`crate::orphans`]) confirms that a fragment is an
/// orphan only for an attempt that is not in progress, marking it
/// abandoned in the same step ([`Attempts::orphaned`]); the encoder never
/// appends a publishing record for an abandoned attempt. An attempt that
/// finished (published, superseded, or abandoned by the encoder itself) is
/// forgotten: it is not in progress. One tracker serves every attempt of a
/// shard on a node: encodings, and later repairs (M5-08) and moves
/// (M5-09).
#[derive(Debug, Clone, Default)]
pub struct Attempts {
    states: Arc<Mutex<HashMap<AttemptId, AttemptState>>>,
}

impl Attempts {
    /// The state of attempt `id`, or `None` if it is not tracked: never
    /// started here, or finished.
    #[must_use]
    pub fn state(&self, id: AttemptId) -> Option<AttemptState> {
        self.lock().get(&id).copied()
    }

    /// The attempts in progress: writing or publishing.
    #[must_use]
    pub fn in_progress(&self) -> Vec<AttemptId> {
        let mut ids: Vec<_> = self
            .lock()
            .iter()
            .filter(|(_, state)| **state != AttemptState::Abandoned)
            .map(|(id, _)| *id)
            .collect();
        ids.sort_unstable();
        ids
    }

    /// Abandons attempt `id` unless it is publishing: true if it is
    /// abandoned now or not in progress at all, false if its publishing
    /// record may commit.
    pub fn abandon(&self, id: AttemptId) -> bool {
        match self.lock().get_mut(&id) {
            Some(AttemptState::Publishing { .. }) => false,
            Some(state) => {
                *state = AttemptState::Abandoned;
                true
            }
            None => true,
        }
    }

    /// Whether attempt `id` can no longer publish, as the primary of a
    /// shard sequencing in `epoch` judges it for orphan reclamation (§8.4).
    /// If so, it is marked abandoned in the same step, under the tracker's
    /// lock, so it never starts publishing afterwards.
    ///
    /// - An attempt of a later epoch was started by a newer primary: it is
    ///   in progress, as far as this one can tell.
    /// - A publishing attempt is in progress until its record commits.
    /// - A writing attempt of `epoch` is in progress. One of an earlier
    ///   epoch is not: it publishes only in its own epoch
    ///   ([`Shard::commit_in`](skys3_shard::Shard::commit_in)), so it is
    ///   abandoned.
    /// - An attempt the tracker does not know was started by an earlier
    ///   primary or life, or finished: it is not in progress.
    #[must_use]
    pub fn orphaned(&self, id: AttemptId, epoch: Epoch) -> bool {
        if id.epoch > epoch {
            return false;
        }
        match self.lock().get_mut(&id) {
            Some(AttemptState::Publishing { .. }) => false,
            Some(AttemptState::Writing) if id.epoch == epoch => false,
            Some(state) => {
                *state = AttemptState::Abandoned;
                true
            }
            None => true,
        }
    }

    /// Starts tracking `id` as writing.
    pub(crate) fn begin(&self, id: AttemptId) {
        self.lock().insert(id, AttemptState::Writing);
    }

    /// Moves `id` from writing to publishing, or returns false if it was
    /// abandoned.
    pub(crate) fn publish(&self, id: AttemptId) -> bool {
        match self.lock().get_mut(&id) {
            Some(state @ AttemptState::Writing) => {
                *state = AttemptState::Publishing { position: None };
                true
            }
            _ => false,
        }
    }

    /// Records where `id`'s publishing record was sequenced, when its
    /// commit was not confirmed.
    pub(crate) fn sequenced(&self, id: AttemptId, position: Option<EpochSeq>) {
        if let Some(AttemptState::Publishing { position: at }) = self.lock().get_mut(&id) {
            *at = position;
        }
    }

    /// Forgets the publishing attempts whose record is at or before
    /// `applied`: it committed, since a primary applies only committed
    /// records.
    pub(crate) fn settle(&self, applied: EpochSeq) {
        self.lock().retain(|_, state| {
            !matches!(state, AttemptState::Publishing { position: Some(at) } if *at <= applied)
        });
    }

    /// Forgets `id`: it published, or will never publish.
    pub(crate) fn finish(&self, id: AttemptId) {
        self.lock().remove(&id);
    }

    /// Forgets `id` unless it is publishing: its writer stopped before it
    /// appended a record, as when the writer's future is dropped.
    pub(crate) fn release(&self, id: AttemptId) {
        let mut states = self.lock();
        if !matches!(states.get(&id), Some(AttemptState::Publishing { .. })) {
            states.remove(&id);
        }
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<AttemptId, AttemptState>> {
        self.states.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Draws attempt IDs for one kind of attempt of a shard on this node:
/// the shard's epoch and a number from blocks the shard's index reserves
/// durably ([`Index::reserve_attempts`](skys3_index::Index::reserve_attempts)),
/// so no number is used twice on the node, across restarts and kinds too.
#[derive(Debug, Default)]
pub(crate) struct AttemptNumbers {
    numbers: Mutex<Range<u64>>,
}

impl AttemptNumbers {
    /// A new attempt ID for `shard`, in the epoch it sequences in.
    pub(crate) fn next<D: Disk>(&self, shard: &Shard<D>) -> Result<AttemptId, IndexError> {
        let mut numbers = self.numbers.lock().unwrap_or_else(PoisonError::into_inner);
        if numbers.is_empty() {
            // A rare durable commit: one per `ATTEMPT_BLOCK` attempts.
            *numbers = shard
                .index()
                .reserve_attempts(shard.shard(), ATTEMPT_BLOCK)?;
        }
        let number = numbers.next().expect("a reserved block is not empty");
        Ok(AttemptId::new(shard.sequencing(), number))
    }
}

/// An attempt being written: dropped before the attempt publishes, it
/// forgets the attempt, which then never publishes.
pub(crate) struct Writing<'a> {
    pub(crate) attempts: &'a Attempts,
    pub(crate) attempt: AttemptId,
}

impl Drop for Writing<'_> {
    fn drop(&mut self) {
        self.attempts.release(self.attempt);
    }
}

/// What became of an attempt's publishing record.
pub(crate) enum Publication {
    /// It committed, and was applied or rejected as the outcome says.
    Committed(Committed),
    /// The attempt was abandoned, by orphan reclamation's fence, before
    /// the record was appended.
    Abandoned,
    /// Nothing was appended: the attempt is over.
    NotAppended(ShardError),
    /// The record may be appended, but its commit was not confirmed. The
    /// attempt stays in progress until the record commits.
    Unconfirmed(ShardError),
}

/// Commits `body`, the publishing record of `attempt`, whose fragments
/// are all durable: only if `attempts` has not abandoned the attempt, and
/// only while `shard` still sequences in the attempt's epoch (§8.4). Calls
/// `appended` once the record is sequenced and before it commits.
///
/// The commit runs on a task of its own, so the record is appended even if
/// this future is dropped. The attempt is finished in `attempts` unless
/// its record may yet commit.
pub(crate) async fn publish<D: Disk>(
    shard: &Shard<D>,
    attempts: &Attempts,
    attempt: AttemptId,
    body: RecordBody,
    appended: impl FnOnce(),
) -> Publication {
    if !attempts.publish(attempt) {
        attempts.finish(attempt);
        return Publication::Abandoned;
    }
    let before = shard.last_sequenced();
    let committer = shard.clone();
    // Only in the attempt's epoch: a later primary may have judged its
    // fragments orphans (§8.4).
    let commit = tokio::spawn(async move { committer.commit_in(attempt.epoch, body).await });
    while shard.last_sequenced() == before && !commit.is_finished() {
        tokio::task::yield_now().await;
    }
    let sequenced = shard.last_sequenced() != before;
    if sequenced {
        appended();
    }
    match commit.await {
        Ok(Ok(committed)) => {
            attempts.finish(attempt);
            Publication::Committed(committed)
        }
        Ok(Err(error)) => {
            let position = match &error {
                ShardError::NotAcknowledged { position, .. } => *position,
                _ => None,
            };
            if sequenced || position.is_some() {
                // The record may commit yet: the attempt stays in progress
                // until it does.
                attempts.sequenced(attempt, position);
                Publication::Unconfirmed(error)
            } else {
                attempts.finish(attempt);
                Publication::NotAppended(error)
            }
        }
        Err(error) => Publication::Unconfirmed(ShardError::Unavailable {
            shard: shard.shard().clone(),
            reason: error.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use skys3_types::{Epoch, Seq};

    use super::*;

    fn id(number: u64) -> AttemptId {
        AttemptId::new(Epoch::new(2), number)
    }

    #[test]
    fn a_publishing_attempt_cannot_be_abandoned_until_it_commits() {
        let attempts = Attempts::default();
        attempts.begin(id(1));
        attempts.begin(id(2));
        assert_eq!(attempts.in_progress(), [id(1), id(2)]);

        // Abandoned while writing: it never publishes.
        assert!(attempts.abandon(id(1)));
        assert_eq!(attempts.state(id(1)), Some(AttemptState::Abandoned));
        assert!(!attempts.publish(id(1)));
        assert_eq!(attempts.in_progress(), [id(2)]);

        // Publishing: it stays in progress until its record commits.
        assert!(attempts.publish(id(2)));
        assert!(!attempts.abandon(id(2)));
        let at = EpochSeq::new(Epoch::new(2), Seq::new(9));
        attempts.sequenced(id(2), Some(at));
        attempts.settle(EpochSeq::new(Epoch::new(2), Seq::new(8)));
        assert_eq!(
            attempts.state(id(2)),
            Some(AttemptState::Publishing { position: Some(at) })
        );
        attempts.settle(at);
        assert_eq!(attempts.state(id(2)), None);

        // Finished or unknown attempts are not in progress.
        attempts.finish(id(1));
        assert!(attempts.abandon(id(3)));
        assert!(attempts.in_progress().is_empty());
    }

    #[test]
    fn only_attempts_that_can_no_longer_publish_are_orphaned() {
        let attempts = Attempts::default();
        let (now, before) = (Epoch::new(2), Epoch::new(1));
        let old = AttemptId::new(before, 7);
        let later = AttemptId::new(Epoch::new(3), 1);
        attempts.begin(id(1));
        attempts.begin(id(2));
        attempts.begin(old);
        assert!(attempts.publish(id(2)));

        // Writing in the judge's epoch, or publishing: in progress.
        assert!(!attempts.orphaned(id(1), now));
        assert!(!attempts.orphaned(id(2), now));
        assert_eq!(attempts.in_progress(), [old, id(1), id(2)]);
        // A newer primary's attempt: in progress as far as this one knows.
        assert!(!attempts.orphaned(later, now));

        // Writing in an earlier epoch: it can never publish, and is
        // abandoned in the same step.
        assert!(attempts.orphaned(old, now));
        assert_eq!(attempts.state(old), Some(AttemptState::Abandoned));
        assert!(!attempts.publish(old));
        assert!(attempts.orphaned(old, now));
        // Unknown: started by an earlier primary or life, or finished.
        assert!(attempts.orphaned(id(9), now));

        // A writer that stops forgets its attempt, unless it publishes.
        attempts.release(id(1));
        attempts.release(id(2));
        assert_eq!(attempts.state(id(1)), None);
        assert!(attempts.orphaned(id(1), now));
        assert_eq!(attempts.in_progress(), [id(2)]);
    }
}
