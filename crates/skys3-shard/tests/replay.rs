//! Replay is deterministic: whatever records a shard's log holds, and
//! whichever checkpoint the index reverts to after a crash, replaying the
//! log reproduces the index the live path built.

mod support;

use std::collections::BTreeMap;

use proptest::prelude::*;
use skys3_index::{Index, IndexDump};
use skys3_io::SimDisk;
use skys3_log::record::{Extent, ExtentRef};
use skys3_log::{LogRecord, RecordBody};
use skys3_shard::StateMachine;
use skys3_types::{Epoch, EpochSeq, Seq};
use support::{
    adopt, delete, fill, flushed, import, index_config, location, put, put_extents, record, tags,
};

/// One step of a shard's history.
#[derive(Debug, Clone)]
enum Op {
    /// An inline `PUT`, or one with this many extents before it.
    Put {
        key: u8,
        extents: u8,
    },
    Delete {
        key: u8,
    },
    Tags {
        key: u8,
    },
    /// A `FLUSHED` of one of the key's versions, picked by `pick`.
    Flushed {
        key: u8,
        pick: u8,
        deleted: bool,
    },
    Import {
        key: u8,
    },
    /// An `ADOPT` naming one of the key's versions, picked by `pick`.
    Adopt {
        key: u8,
        pick: u8,
    },
    /// A new epoch, which starts with a `TRUNCATE`.
    NewEpoch,
}

fn op() -> impl Strategy<Value = Op> {
    let key = 0..4u8;
    prop_oneof![
        4 => (key.clone(), 0..3u8).prop_map(|(key, extents)| Op::Put { key, extents }),
        2 => key.clone().prop_map(|key| Op::Delete { key }),
        1 => key.clone().prop_map(|key| Op::Tags { key }),
        4 => (key.clone(), any::<u8>(), any::<bool>())
            .prop_map(|(key, pick, deleted)| Op::Flushed { key, pick, deleted }),
        2 => key.clone().prop_map(|key| Op::Import { key }),
        2 => (key, any::<u8>()).prop_map(|(key, pick)| Op::Adopt { key, pick }),
        1 => Just(Op::NewEpoch),
    ]
}

/// Turns operations into records with positions. `FLUSHED` and `ADOPT`
/// name versions the key had, usually its latest, so that the records mix
/// accepted and rejected transitions.
fn records(ops: &[Op]) -> Vec<LogRecord> {
    let mut next = EpochSeq::new(Epoch::new(1), Seq::new(1));
    let take = |next: &mut EpochSeq| {
        let position = *next;
        next.seq = next.seq.checked_next().unwrap();
        position
    };
    let mut versions: BTreeMap<u8, Vec<(u64, bool)>> = BTreeMap::new();
    let pick = |versions: &BTreeMap<u8, Vec<(u64, bool)>>, key: u8, pick: u8| {
        let list = versions.get(&key).map_or(&[][..], Vec::as_slice);
        match (list.last(), pick % 4) {
            (Some(&latest), 0..=1) => latest,
            (Some(_), 2) => list[usize::from(pick) % list.len()],
            _ => (u64::from(pick % 8), false),
        }
    };
    let mut out = Vec::new();
    for op in ops {
        let name = |key: &u8| format!("key-{key}");
        match op {
            Op::Put { key, extents } => {
                let refs: Vec<_> = (0..*extents)
                    .map(|i| {
                        let position = take(&mut next);
                        let body = RecordBody::Extent(Extent {
                            key: name(key),
                            offset: u64::from(i) * 300,
                            data: fill(300),
                        });
                        out.push(record(position, body));
                        ExtentRef { position, len: 300 }
                    })
                    .collect();
                let position = take(&mut next);
                let tag = position.seq.get();
                let body = if refs.is_empty() {
                    put(&name(key), usize::from(*key) * 3, tag)
                } else {
                    put_extents(&name(key), refs, tag)
                };
                out.push(record(position, body));
                versions.entry(*key).or_default().push((tag, false));
            }
            Op::Delete { key } => {
                let position = take(&mut next);
                out.push(record(position, delete(&name(key))));
                versions
                    .entry(*key)
                    .or_default()
                    .push((position.seq.get(), true));
            }
            Op::Tags { key } => {
                let position = take(&mut next);
                out.push(record(position, tags(&name(key), "x")));
                versions
                    .entry(*key)
                    .or_default()
                    .push((position.seq.get(), false));
            }
            Op::Flushed {
                key,
                pick: which,
                deleted,
            } => {
                let (seq, tombstone) = pick(&versions, *key, *which);
                let body = flushed(&name(key), seq, tombstone || *deleted && seq % 2 == 0);
                out.push(record(take(&mut next), body));
            }
            Op::Import { key } => {
                let position = take(&mut next);
                out.push(record(position, import(&name(key), 9)));
                versions
                    .entry(*key)
                    .or_default()
                    .push((position.seq.get(), false));
            }
            Op::Adopt { key, pick: which } => {
                let (seq, _) = pick(&versions, *key, *which);
                let position = take(&mut next);
                out.push(record(position, adopt(&name(key), seq, position.seq.get())));
                versions
                    .entry(*key)
                    .or_default()
                    .push((position.seq.get(), false));
            }
            Op::NewEpoch => {
                let position = take(&mut next);
                let epoch = Epoch::new(position.epoch.get() + 1);
                out.push(record(
                    EpochSeq::new(epoch, position.seq),
                    RecordBody::Truncate,
                ));
                next = EpochSeq::new(epoch, position.seq.checked_next().unwrap());
            }
        }
    }
    out
}

/// Applies `records` in batches of `batch`, as the write path or replay
/// would, and returns how many the index applied.
fn apply(index: &Index, records: &[LogRecord], batch: usize) -> usize {
    records
        .chunks(batch)
        .map(|chunk| {
            let chunk: Vec<_> = chunk
                .iter()
                .map(|record| (record.clone(), location(record.position)))
                .collect();
            index.apply(&StateMachine, &chunk).unwrap()
        })
        .sum()
}

fn dump(index: &Index) -> IndexDump {
    index.read().unwrap().dump().unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    #[test]
    fn replay_from_any_checkpoint_reproduces_the_index(
        ops in prop::collection::vec(op(), 1..48),
        checkpoint in any::<prop::sample::Index>(),
        lost in any::<prop::sample::Index>(),
        live_batch in 1..6usize,
        replay_batch in 1..9usize,
    ) {
        let records = records(&ops);

        // The live path applies every record.
        let live_disk = SimDisk::new(1);
        let live = Index::open_sim(&live_disk.mount(), "index.redb", &index_config()).unwrap();
        prop_assert_eq!(apply(&live, &records, live_batch), records.len());
        let expected = dump(&live);

        // Another node checkpoints after `at` records, applies some more,
        // and loses power.
        let at = checkpoint.index(records.len() + 1);
        let until = at + lost.index(records.len() - at + 1);
        let disk = SimDisk::new(2);
        let index = Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap();
        apply(&index, &records[..at], live_batch);
        index.checkpoint(&BTreeMap::new()).unwrap();
        let durable = dump(&index);
        apply(&index, &records[at..until], live_batch);
        disk.crash();
        drop(index);

        // It reverts to the checkpoint, and replaying the whole log skips
        // what the checkpoint holds and reproduces the live index.
        let index = Index::open_sim(&disk.mount(), "index.redb", &index_config()).unwrap();
        prop_assert_eq!(&dump(&index), &durable);
        prop_assert_eq!(apply(&index, &records, replay_batch), records.len() - at);
        prop_assert_eq!(&dump(&index), &expected);

        // Replaying again changes nothing.
        prop_assert_eq!(apply(&index, &records, replay_batch), 0);
        prop_assert_eq!(&dump(&index), &expected);
    }
}
