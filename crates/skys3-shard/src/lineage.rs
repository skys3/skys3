//! A replica's lineage: the epochs of the records its log holds, which
//! reconciliation compares by `(epoch, seq)` (§6.6).
//!
//! One primary per epoch assigns each `seq` once, and a record keeps its
//! epoch when a later primary rolls it forward, so a record is identified
//! by its `(epoch, seq)`. Every replica's log is a prefix of the log of the
//! primary of the epoch it sequences in, and the records of one epoch form
//! one run in every log that holds them, so two replicas whose logs both
//! reach `seq` `a` and `b` in epoch `e` hold the same records up to the
//! smaller of the two. A lineage records, for each epoch, the last `seq`
//! the log holds in it: enough to find the longest prefix two logs share
//! without reading them.
//!
//! A replica knows its lineage from the position it opened at: the index
//! keeps only the applied position, so the epochs of the records before it
//! are unknown, and a comparison that needs them is undecided.

use std::fmt;

use skys3_types::{Epoch, EpochSeq, Seq};

/// The most runs a lineage from a peer may have.
pub const MAX_RUNS: usize = 4096;

/// One run of a lineage: the log holds records of `epoch` up to `last`,
/// after the previous run's last `seq`. A run may hold no record: the
/// replica adopted the epoch at `last` with a `CONFIG` record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Run {
    /// The epoch.
    pub epoch: Epoch,
    /// The last `seq` the log holds in it.
    pub last: Seq,
}

/// The epochs of a replica's log after a known point: see the
/// [module](self) docs.
#[derive(Clone, PartialEq, Eq)]
pub struct Lineage {
    /// The log's epochs are known for the records after this `seq`; the log
    /// up to it is a prefix of the first run's epoch.
    known_after: Seq,
    /// Never empty; epochs increase strictly and last `seq`s never
    /// decrease.
    runs: Vec<Run>,
}

impl fmt::Debug for Lineage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "after {}:", self.known_after)?;
        for run in &self.runs {
            write!(f, " ({}, {})", run.epoch, run.last)?;
        }
        Ok(())
    }
}

/// What a member does with its log when a primary opens a session (§6.6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reconcile {
    /// Its log is a prefix of the primary's: nothing.
    Keep,
    /// It holds the primary's whole log and records past it that the
    /// primary sequenced and lost in a crash: it sends them back (§5.1).
    RollForward,
    /// It holds records the primary does not: it invalidates those after
    /// `after` with a `TRUNCATE` at `(epoch, after)`.
    Truncate {
        /// The last `seq` that stays valid.
        after: Seq,
        /// The `TRUNCATE`'s epoch: newer than every record it invalidates,
        /// and no newer than any the primary sends next.
        epoch: Epoch,
    },
    /// It cannot reconcile in place: why.
    Diverged(String),
}

impl Lineage {
    /// The lineage of a log whose last record, or `CONFIG` record, is at
    /// `at`.
    #[must_use]
    pub fn new(at: EpochSeq) -> Self {
        Self {
            known_after: at.seq,
            runs: vec![Run {
                epoch: at.epoch,
                last: at.seq,
            }],
        }
    }

    /// A lineage as a peer sent it, checked.
    ///
    /// # Errors
    ///
    /// Why it is not a lineage: no run, too many, epochs that do not
    /// increase, or `seq`s that decrease.
    pub fn from_parts(known_after: Seq, runs: Vec<Run>) -> Result<Self, String> {
        let Some(first) = runs.first() else {
            return Err("a lineage has no run".to_owned());
        };
        if runs.len() > MAX_RUNS {
            return Err(format!("a lineage has {} runs", runs.len()));
        }
        if known_after > first.last {
            return Err("a lineage is known after its first run".to_owned());
        }
        if runs
            .windows(2)
            .any(|pair| pair[0].epoch >= pair[1].epoch || pair[0].last > pair[1].last)
        {
            return Err("a lineage's runs are out of order".to_owned());
        }
        Ok(Self { known_after, runs })
    }

    /// The `seq` after which the log's epochs are known.
    #[must_use]
    pub fn known_after(&self) -> Seq {
        self.known_after
    }

    /// The runs, oldest first.
    #[must_use]
    pub fn runs(&self) -> &[Run] {
        &self.runs
    }

    /// The position of the log's last record or `CONFIG` record.
    #[must_use]
    pub fn last(&self) -> EpochSeq {
        let run = self.last_run();
        EpochSeq::new(run.epoch, run.last)
    }

    fn last_run(&self) -> &Run {
        self.runs.last().expect("a lineage has a run")
    }

    /// Records that the log now holds a record, or a `CONFIG` record, at
    /// `at`, which follows every position it held.
    pub fn push(&mut self, at: EpochSeq) {
        let run = self.runs.last_mut().expect("a lineage has a run");
        debug_assert!(
            at.epoch >= run.epoch && at.seq >= run.last,
            "{at} after {run:?}"
        );
        if at.epoch == run.epoch {
            run.last = run.last.max(at.seq);
        } else {
            self.runs.push(Run {
                epoch: at.epoch,
                last: at.seq,
            });
        }
    }

    /// Records that the log invalidated its records after `after`, which
    /// is no earlier than [`Lineage::known_after`].
    pub fn truncate(&mut self, after: Seq) {
        // A run whose records are all gone proves nothing any more.
        while self.runs.len() > 1 && self.runs[self.runs.len() - 2].last >= after {
            self.runs.pop();
        }
        let run = self.runs.last_mut().expect("a lineage has a run");
        run.last = run.last.min(after);
        self.known_after = self.known_after.min(after);
    }

    /// The epoch of the record at `seq`, if it is known and held.
    #[must_use]
    pub fn epoch_at(&self, seq: Seq) -> Option<Epoch> {
        if seq <= self.known_after {
            return None;
        }
        self.runs
            .iter()
            .find(|run| run.last >= seq)
            .map(|run| run.epoch)
    }

    /// The last `seq` up to which this log and `theirs` surely hold the
    /// same records, or `None` if they share no epoch to tell by.
    #[must_use]
    pub fn matched(&self, theirs: &Self) -> Option<Seq> {
        self.runs
            .iter()
            .filter_map(|ours| {
                let other = theirs.runs.iter().find(|run| run.epoch == ours.epoch)?;
                Some(ours.last.min(other.last))
            })
            .max()
    }

    /// What a member whose log has this lineage, and whose records up to
    /// `applied` are applied, does when the primary of a configuration in
    /// `epoch`, whose log has lineage `primary`, opens a session (§6.6).
    ///
    /// The member keeps the longest prefix it shares with the primary. Past
    /// it, records of the primary's own epochs that the primary lost in a
    /// crash go back to it; anything else is truncated, which needs the
    /// records to be unapplied, and a `TRUNCATE` epoch that tells them from
    /// what the primary sends next. A member that cannot meet those is
    /// diverged: its primary removes it, and it rejoins as a learner.
    #[must_use]
    pub fn reconcile(&self, primary: &Self, epoch: Epoch, applied: Seq) -> Reconcile {
        let last = self.last().seq;
        // A log that holds no record, a new learner's, is a prefix of any.
        if last == Seq::ZERO {
            return Reconcile::Keep;
        }
        let Some(matched) = self.matched(primary) else {
            return Reconcile::Diverged(format!(
                "its log ({self:?}) shares no known epoch with the primary's ({primary:?})"
            ));
        };
        if matched >= last {
            return Reconcile::Keep;
        }
        // The epochs of the records past the matched prefix. Their first
        // run is the one that holds `matched + 1`.
        let tail: Vec<Epoch> = self
            .runs
            .iter()
            .filter(|run| run.last > matched)
            .map(|run| run.epoch)
            .collect();
        let (oldest, newest) = (tail[0], tail[tail.len() - 1]);
        let theirs = primary.last();
        if matched == theirs.seq && oldest >= theirs.epoch {
            return Reconcile::RollForward;
        }
        if matched < applied {
            return Reconcile::Diverged(format!(
                "it applied records up to seq {applied} past the prefix it shares with the \
                 primary, which ends at seq {matched}"
            ));
        }
        let next = if matched == theirs.seq {
            Some(epoch)
        } else {
            primary.epoch_at(Seq::new(matched.get().saturating_add(1)))
        };
        match next {
            Some(next) if newest < next => Reconcile::Truncate {
                after: matched,
                epoch: next,
            },
            _ => Reconcile::Diverged(format!(
                "its records past seq {matched} cannot be told from the primary's ({self:?} \
                 against {primary:?})"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(epoch: u64, seq: u64) -> EpochSeq {
        EpochSeq::new(Epoch::new(epoch), Seq::new(seq))
    }

    /// A lineage known from `start`, holding each of `then` in turn.
    fn lineage(start: (u64, u64), then: &[(u64, u64)]) -> Lineage {
        let mut lineage = Lineage::new(at(start.0, start.1));
        for &(epoch, seq) in then {
            lineage.push(at(epoch, seq));
        }
        lineage
    }

    fn truncate(after: u64, epoch: u64) -> Reconcile {
        Reconcile::Truncate {
            after: Seq::new(after),
            epoch: Epoch::new(epoch),
        }
    }

    #[test]
    fn runs_grow_and_shrink() {
        // Opened at seq 10 in epoch 3, then records of epoch 3, a CONFIG
        // record of epoch 4 at seq 12, and records of epoch 4.
        let mut lineage = lineage((3, 10), &[(3, 11), (3, 12), (4, 12), (4, 13), (4, 14)]);
        assert_eq!(lineage.last(), at(4, 14));
        assert_eq!(lineage.epoch_at(Seq::new(10)), None);
        assert_eq!(lineage.epoch_at(Seq::new(12)), Some(Epoch::new(3)));
        assert_eq!(lineage.epoch_at(Seq::new(13)), Some(Epoch::new(4)));
        assert_eq!(lineage.epoch_at(Seq::new(15)), None);
        assert_eq!(format!("{lineage:?}"), "after 10: (3, 12) (4, 14)");
        lineage.truncate(Seq::new(13));
        assert_eq!(lineage.runs().len(), 2);
        lineage.truncate(Seq::new(12));
        assert_eq!(
            lineage.runs(),
            [Run {
                epoch: Epoch::new(3),
                last: Seq::new(12)
            }]
        );
        lineage.truncate(Seq::new(10));
        assert_eq!(lineage.last(), at(3, 10));
        assert_eq!(lineage.known_after(), Seq::new(10));
    }

    #[test]
    fn lineages_from_peers_are_checked() {
        let run = |epoch, last| Run {
            epoch: Epoch::new(epoch),
            last: Seq::new(last),
        };
        assert!(Lineage::from_parts(Seq::new(1), vec![]).is_err());
        assert!(Lineage::from_parts(Seq::new(5), vec![run(1, 4)]).is_err());
        assert!(Lineage::from_parts(Seq::new(1), vec![run(2, 4), run(2, 5)]).is_err());
        assert!(Lineage::from_parts(Seq::new(1), vec![run(1, 4), run(2, 3)]).is_err());
        let many = (1..=MAX_RUNS as u64 + 1).map(|e| run(e, 9)).collect();
        assert!(Lineage::from_parts(Seq::new(1), many).is_err());
        let lineage = Lineage::from_parts(Seq::new(1), vec![run(1, 4), run(2, 4)]).unwrap();
        assert_eq!(lineage.last(), at(2, 4));
    }

    #[test]
    fn prefixes_match_up_to_the_shorter_run_of_a_shared_epoch() {
        let ours = lineage((2, 5), &[(2, 9)]);
        let theirs = lineage((2, 3), &[(2, 7), (3, 7), (3, 12)]);
        assert_eq!(ours.matched(&theirs), Some(Seq::new(7)));
        assert_eq!(theirs.matched(&ours), Some(Seq::new(7)));
        assert_eq!(ours.matched(&lineage((4, 20), &[])), None);
    }

    #[test]
    fn a_member_keeps_a_prefix_and_rolls_its_primarys_records_forward() {
        let primary = lineage((2, 3), &[(2, 7), (3, 7), (3, 12)]);
        // Behind the primary.
        let behind = lineage((2, 5), &[(2, 7), (3, 7), (3, 10)]);
        assert_eq!(
            behind.reconcile(&primary, Epoch::new(3), Seq::new(5)),
            Reconcile::Keep
        );
        // Ahead of a primary that lost records of its epoch in a crash.
        let ahead = lineage((2, 5), &[(2, 7), (3, 7), (3, 14)]);
        assert_eq!(
            ahead.reconcile(&primary, Epoch::new(3), Seq::new(5)),
            Reconcile::RollForward
        );
    }

    #[test]
    fn a_log_that_holds_nothing_is_a_prefix_of_any() {
        let primary = lineage((4, 20), &[(4, 22)]);
        let empty = Lineage::new(at(0, 0));
        assert_eq!(empty.matched(&primary), None);
        assert_eq!(
            empty.reconcile(&primary, Epoch::new(5), Seq::ZERO),
            Reconcile::Keep
        );
    }

    #[test]
    fn a_member_truncates_what_a_new_primary_lacks() {
        // The new primary of epoch 4 took over with its log at seq 7.
        let primary = lineage((2, 3), &[(2, 7), (4, 7)]);
        let ahead = lineage((2, 5), &[(2, 9)]);
        assert_eq!(
            ahead.reconcile(&primary, Epoch::new(4), Seq::new(5)),
            truncate(7, 4)
        );
        // Records the member applied cannot be truncated.
        assert!(matches!(
            ahead.reconcile(&primary, Epoch::new(4), Seq::new(8)),
            Reconcile::Diverged(_)
        ));
        // A member that followed another candidate's epoch 3 holds records
        // the primary does not, at seqs the primary holds too, which the
        // primary took in epoch 4: they are older than the primary's
        // records there.
        let primary = lineage((2, 3), &[(2, 7), (4, 7), (4, 8), (5, 8)]);
        let other = lineage((2, 5), &[(2, 7), (3, 7), (3, 9)]);
        assert_eq!(
            other.reconcile(&primary, Epoch::new(5), Seq::new(5)),
            truncate(7, 4)
        );
    }

    #[test]
    fn a_member_that_cannot_tell_is_diverged() {
        let primary = lineage((4, 20), &[(4, 22)]);
        let unknown = lineage((2, 5), &[(2, 9)]);
        assert!(matches!(
            unknown.reconcile(&primary, Epoch::new(5), Seq::new(5)),
            Reconcile::Diverged(_)
        ));
        // The member's records past the shared prefix are of a newer epoch
        // than the primary's next record.
        let primary = lineage((2, 3), &[(2, 7), (3, 7), (3, 9), (5, 9)]);
        let newer = lineage((2, 5), &[(2, 7), (4, 7), (4, 8)]);
        assert!(matches!(
            newer.reconcile(&primary, Epoch::new(5), Seq::new(5)),
            Reconcile::Diverged(_)
        ));
        // The primary's epoch at the first seq past the prefix is unknown:
        // it opened later.
        let primary = lineage((2, 12), &[(5, 12)]);
        let member = lineage((2, 5), &[(2, 9), (4, 10)]);
        assert_eq!(member.matched(&primary), Some(Seq::new(9)));
        assert!(matches!(
            member.reconcile(&primary, Epoch::new(5), Seq::new(5)),
            Reconcile::Diverged(_)
        ));
    }
}
