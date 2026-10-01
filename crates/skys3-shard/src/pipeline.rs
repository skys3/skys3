//! The commit pipeline's queue: records in position order, released for
//! applying only once every earlier record is durable.
//!
//! A shard's records become durable out of order: extents go to bulk
//! segments and other records to hot ones, and the two classes sync
//! independently (§10.1). Applying must follow position order, because an
//! applied position means that every earlier record of the shard was
//! applied (§10.2). The queue holds each record from the moment it gets its
//! position until it is durable and every record before it has been
//! released, so a small `PUT` is never applied while the extents of an
//! earlier upload are still in flight.
//!
//! On a replicated shard a durable record must also be committed before it
//! is applied: the queue then releases records only up to a commit limit,
//! the `seq` every other member has acknowledged as durable on the primary,
//! or the commit watermark a member learned from its primary (§5.1).

use std::collections::VecDeque;

use skys3_log::{LogRecord, RecordLocation};
use skys3_types::{EpochSeq, Seq};

/// A record that is durable, with where it is stored. Boxed, as records
/// are large next to the rest of a queue slot.
pub(crate) type Durable = Box<(LogRecord, RecordLocation)>;

/// One place in the queue.
#[derive(Debug)]
enum Slot<W, B> {
    /// A record, by position, with what to tell its writer.
    Write {
        position: EpochSeq,
        reply: W,
        result: Option<Result<Durable, String>>,
    },
    /// Released once every record queued before it is released.
    Barrier(B),
}

/// What the queue releases, in queue order.
#[derive(Debug)]
pub(crate) enum Ready<W, B> {
    /// Durable records, in position order, to apply together.
    Apply(Vec<(Durable, W)>),
    /// A record that did not become durable, and why.
    Failed(W, String),
    /// A barrier: everything before it was released.
    Barrier(B),
}

/// The queue. `W` is what a record's writer waits on, and `B` what a
/// barrier's.
#[derive(Debug)]
pub(crate) struct Pipeline<W, B> {
    slots: VecDeque<Slot<W, B>>,
    /// The last `seq` a durable record may have to be released, or `None`
    /// if every durable record may be.
    limit: Option<Seq>,
}

impl<W, B> Default for Pipeline<W, B> {
    fn default() -> Self {
        Self {
            slots: VecDeque::new(),
            limit: None,
        }
    }
}

impl<W, B> Pipeline<W, B> {
    /// Queues the record at `position`, which follows every position queued
    /// before it.
    pub(crate) fn sequenced(&mut self, position: EpochSeq, reply: W) {
        debug_assert!(
            self.slots.iter().rev().find_map(|slot| match slot {
                Slot::Write { position, .. } => Some(*position),
                Slot::Barrier(_) => None,
            }) < Some(position),
            "positions are queued in order"
        );
        self.slots.push_back(Slot::Write {
            position,
            reply,
            result: None,
        });
    }

    /// Queues a barrier.
    pub(crate) fn barrier(&mut self, reply: B) {
        self.slots.push_back(Slot::Barrier(reply));
    }

    /// Records whether the record at `position` became durable. A position
    /// that is not queued is ignored.
    pub(crate) fn resolved(&mut self, position: EpochSeq, result: Result<Durable, String>) {
        let slot = self.slots.iter_mut().find_map(|slot| match slot {
            Slot::Write {
                position: queued,
                result,
                ..
            } if *queued == position => Some(result),
            _ => None,
        });
        if let Some(slot) = slot {
            *slot = Some(result);
        }
    }

    /// Raises the commit limit to `limit`; a lower limit changes nothing.
    pub(crate) fn commit_through(&mut self, limit: Seq) {
        self.limit = Some(self.limit.map_or(limit, |old| old.max(limit)));
    }

    /// Makes every durable record wait for the commit limit, which starts
    /// at `limit`.
    pub(crate) fn require_commit(&mut self, limit: Seq) {
        self.limit = Some(limit);
    }

    /// The position of the last record of the longest run of durable
    /// records at the front of the queue, committed or not, if the queue
    /// starts with one.
    pub(crate) fn durable_through(&self) -> Option<EpochSeq> {
        self.slots
            .iter()
            .filter(|slot| !matches!(slot, Slot::Barrier(_)))
            .map_while(|slot| match slot {
                Slot::Write {
                    position,
                    result: Some(Ok(_)),
                    ..
                } => Some(*position),
                _ => None,
            })
            .last()
    }

    /// Releases what is ready at the front of the queue: a barrier, a
    /// failed record, or the longest run of durable, committed records.
    /// Returns `None` while the front record is still in flight or waits
    /// for the commit limit.
    pub(crate) fn next(&mut self) -> Option<Ready<W, B>> {
        match self.slots.pop_front()? {
            Slot::Barrier(reply) => Some(Ready::Barrier(reply)),
            Slot::Write {
                reply,
                result: Some(Err(error)),
                ..
            } => Some(Ready::Failed(reply, error)),
            Slot::Write {
                position,
                reply,
                result: Some(Ok(durable)),
            } if self.committed(position) => {
                let mut batch = vec![(durable, reply)];
                batch.extend(std::iter::from_fn(|| self.pop_durable()));
                Some(Ready::Apply(batch))
            }
            pending @ Slot::Write { .. } => {
                self.slots.push_front(pending);
                None
            }
        }
    }

    /// Empties the queue without releasing anything for applying, and
    /// returns what the writers and the barriers wait on, in queue order.
    /// Answers for the records it held are ignored from then on.
    pub(crate) fn abandon(&mut self) -> (Vec<W>, Vec<B>) {
        let (mut writes, mut barriers) = (Vec::new(), Vec::new());
        for slot in self.slots.drain(..) {
            match slot {
                Slot::Write { reply, .. } => writes.push(reply),
                Slot::Barrier(reply) => barriers.push(reply),
            }
        }
        (writes, barriers)
    }

    /// Drops the records after `after`, which a member truncated to
    /// reconcile with a new primary (§6.6), without releasing or answering
    /// them, and lowers the commit limit to `after`: a watermark of the
    /// earlier primary says nothing of the records that take their place.
    pub(crate) fn truncate(&mut self, after: Seq) {
        self.slots.retain(|slot| match slot {
            Slot::Write { position, .. } => position.seq <= after,
            Slot::Barrier(_) => true,
        });
        self.limit = self.limit.map(|limit| limit.min(after));
    }

    fn committed(&self, position: EpochSeq) -> bool {
        self.limit.is_none_or(|limit| position.seq <= limit)
    }

    /// Takes the front slot if it is a durable, committed record.
    fn pop_durable(&mut self) -> Option<(Durable, W)> {
        match self.slots.pop_front()? {
            Slot::Write {
                position,
                reply,
                result: Some(Ok(durable)),
            } if self.committed(position) => Some((durable, reply)),
            other => {
                self.slots.push_front(other);
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use skys3_log::record::Delete;
    use skys3_log::{RecordBody, SegmentId, ShardRef};
    use skys3_types::{BucketId, Epoch, Seq, ShardId};

    use super::*;

    fn at(seq: u64) -> EpochSeq {
        EpochSeq::new(Epoch::new(1), Seq::new(seq))
    }

    fn durable(seq: u64) -> Result<Durable, String> {
        let record = LogRecord {
            shard: ShardRef::new(BucketId::new("b-1").unwrap(), ShardId::new(0)),
            position: at(seq),
            body: RecordBody::Delete(Delete { key: "k".into() }),
        };
        let location = RecordLocation {
            segment: SegmentId::new(0),
            offset: seq * 100,
            len: 100,
        };
        Ok(Box::new((record, location)))
    }

    fn applied(ready: Option<Ready<u64, &str>>) -> Vec<u64> {
        match ready {
            Some(Ready::Apply(batch)) => batch.into_iter().map(|(_, reply)| reply).collect(),
            other => panic!("expected records to apply, got {other:?}"),
        }
    }

    #[test]
    fn a_truncated_tail_is_dropped_and_waits_for_a_new_watermark() {
        let mut pipeline = Pipeline::<u64, &str>::default();
        pipeline.require_commit(Seq::new(3));
        for seq in 1..=3 {
            pipeline.sequenced(at(seq), seq);
            pipeline.resolved(at(seq), durable(seq));
        }
        pipeline.barrier("after");
        pipeline.truncate(Seq::new(1));
        assert_eq!(applied(pipeline.next()), [1]);
        assert!(matches!(pipeline.next(), Some(Ready::Barrier("after"))));
        // A record taken in place of a truncated one waits for a watermark
        // that covers it.
        pipeline.sequenced(at(2), 20);
        pipeline.resolved(at(2), durable(2));
        assert!(pipeline.next().is_none());
        pipeline.commit_through(Seq::new(2));
        assert_eq!(applied(pipeline.next()), [20]);
    }

    #[test]
    fn records_are_released_in_position_order() {
        let mut pipeline = Pipeline::<u64, &str>::default();
        for seq in 1..=4 {
            pipeline.sequenced(at(seq), seq);
        }
        // The last two become durable first: nothing is released.
        pipeline.resolved(at(3), durable(3));
        pipeline.resolved(at(4), durable(4));
        assert!(pipeline.next().is_none());
        pipeline.resolved(at(1), durable(1));
        assert_eq!(applied(pipeline.next()), [1]);
        assert!(pipeline.next().is_none());
        pipeline.resolved(at(2), durable(2));
        assert_eq!(applied(pipeline.next()), [2, 3, 4]);
        assert!(pipeline.next().is_none());
    }

    #[test]
    fn a_barrier_waits_for_every_earlier_record() {
        let mut pipeline = Pipeline::<u64, &str>::default();
        pipeline.barrier("first");
        pipeline.sequenced(at(1), 1);
        pipeline.barrier("second");
        pipeline.sequenced(at(2), 2);
        assert!(matches!(pipeline.next(), Some(Ready::Barrier("first"))));
        assert!(pipeline.next().is_none());
        pipeline.resolved(at(2), durable(2));
        assert!(pipeline.next().is_none());
        pipeline.resolved(at(1), durable(1));
        assert_eq!(applied(pipeline.next()), [1]);
        assert!(matches!(pipeline.next(), Some(Ready::Barrier("second"))));
        assert_eq!(applied(pipeline.next()), [2]);
    }

    #[test]
    fn durable_records_wait_for_the_commit_limit() {
        let mut pipeline = Pipeline::<u64, &str>::default();
        pipeline.require_commit(Seq::ZERO);
        for seq in 1..=3 {
            pipeline.sequenced(at(seq), seq);
        }
        assert_eq!(pipeline.durable_through(), None);
        pipeline.resolved(at(1), durable(1));
        pipeline.resolved(at(2), durable(2));
        assert_eq!(pipeline.durable_through(), Some(at(2)));
        assert!(pipeline.next().is_none());
        pipeline.commit_through(Seq::new(1));
        assert_eq!(applied(pipeline.next()), [1]);
        assert!(pipeline.next().is_none());
        // A lower limit changes nothing.
        pipeline.commit_through(Seq::ZERO);
        assert!(pipeline.next().is_none());
        pipeline.barrier("after");
        pipeline.commit_through(Seq::new(3));
        assert_eq!(applied(pipeline.next()), [2]);
        assert!(pipeline.next().is_none());
        pipeline.resolved(at(3), durable(3));
        assert_eq!(pipeline.durable_through(), Some(at(3)));
        assert_eq!(applied(pipeline.next()), [3]);
        assert!(matches!(pipeline.next(), Some(Ready::Barrier("after"))));
    }

    #[test]
    fn abandoning_hands_back_every_waiter_and_releases_nothing() {
        let mut pipeline = Pipeline::<u64, &str>::default();
        pipeline.require_commit(Seq::ZERO);
        pipeline.sequenced(at(1), 1);
        pipeline.barrier("first");
        pipeline.sequenced(at(2), 2);
        pipeline.resolved(at(1), durable(1));
        assert!(pipeline.next().is_none());
        assert_eq!(pipeline.abandon(), (vec![1, 2], vec!["first"]));
        // Later answers and commits find nothing to release.
        pipeline.resolved(at(2), durable(2));
        pipeline.commit_through(Seq::new(2));
        assert!(pipeline.next().is_none());
        assert_eq!(pipeline.durable_through(), None);
    }

    #[test]
    fn a_failed_record_is_released_alone() {
        let mut pipeline = Pipeline::<u64, &str>::default();
        for seq in 1..=3 {
            pipeline.sequenced(at(seq), seq);
        }
        pipeline.resolved(at(1), durable(1));
        pipeline.resolved(at(2), Err("disk out of service".into()));
        pipeline.resolved(at(3), durable(3));
        // Unknown positions are ignored.
        pipeline.resolved(at(9), durable(9));
        assert_eq!(applied(pipeline.next()), [1]);
        match pipeline.next() {
            Some(Ready::Failed(2, error)) => assert_eq!(error, "disk out of service"),
            other => panic!("expected the failed record, got {other:?}"),
        }
        assert_eq!(applied(pipeline.next()), [3]);
        assert!(pipeline.next().is_none());
    }
}
