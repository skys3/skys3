//! The fragment-writing attempts a shard primary has in progress (§8.4).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use skys3_types::{AttemptId, EpochSeq};

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

/// The attempts a shard primary's encoder has in progress, shared with
/// whatever must not race them.
///
/// Orphan reclamation (plan M5-05) confirms that a fragment is an orphan
/// only for an attempt that is not in progress, marking it abandoned in
/// the same step ([`Attempts::abandon`]); the encoder never appends a
/// publishing record for an abandoned attempt. An attempt that finished
/// (published, superseded, or abandoned by the encoder itself) is
/// forgotten: it is not in progress.
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

    fn lock(&self) -> MutexGuard<'_, HashMap<AttemptId, AttemptState>> {
        self.states.lock().unwrap_or_else(PoisonError::into_inner)
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
}
