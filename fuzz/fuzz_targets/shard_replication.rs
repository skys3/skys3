//! Fuzzes the replication messages a member or a primary decodes from an
//! authenticated but untrusted peer (design §5.1, §6.6, §12): every body,
//! the configuration and the lineage a `Sync` carries, the record of an
//! `Append` or a `SyncAck`, a planned handoff's `StepDown` (§5.4), and a
//! backfill's chunks, rows, and requests (§6.7).

#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use skys3_index::ShardTable;
use skys3_log::LogRecord;
use skys3_net::{Frame, Header, MessageKind};
use skys3_shard::replication::wire::{
    self, Append, AppendAck, Backfill, BackfillAck, Beacon, SnapshotRows, StepDown, Sync, SyncAck,
};
use skys3_shard::lineage::Lineage;
use skys3_types::{Epoch, EpochSeq, RegisterDocument, Seq};

fuzz_target!(|data: &[u8]| {
    // The bodies alone, as a header carries them.
    for kind in [
        MessageKind::Sync,
        MessageKind::SyncAck,
        MessageKind::Append,
        MessageKind::AppendAck,
        MessageKind::Beacon,
        MessageKind::StepDown,
        MessageKind::Backfill,
        MessageKind::BackfillAck,
    ] {
        let mut frame = Frame::new(Header::new(kind).with_body(data.to_vec()), data.to_vec());
        check(&frame);
        frame.payload = Default::default();
        check(&frame);
    }
    // Whole frames, as they arrive.
    if let Ok(Some((frame, _))) = Frame::decode(data) {
        check(&frame);
    }
});

/// Decoding never panics, and what decodes is what was sent.
fn check(frame: &Frame) {
    match frame.header.kind {
        MessageKind::Sync => {
            let Ok(sync) = wire::body::<Sync>(frame, MessageKind::Sync) else {
                return;
            };
            if let Ok(config) = sync.configuration() {
                let again = Sync {
                    config: config.to_json().expect("a valid register encodes"),
                    ..sync.clone()
                };
                assert_eq!(again.configuration(), Ok(config));
            }
            if let Ok(lineage) = sync.lineage() {
                // What decodes encodes the same, and reconciles with any
                // member's lineage without a panic (§6.6).
                let again = Sync::default().with_lineage(&lineage);
                assert_eq!(again.lineage(), Ok(lineage.clone()));
                let member = Lineage::new(EpochSeq {
                    epoch: Epoch::new(sync.sequencing),
                    seq: Seq::new(sync.primary_last),
                });
                let epoch = Epoch::new(sync.sequencing);
                let _ = member.reconcile(&lineage, epoch, Seq::ZERO);
                let _ = lineage.reconcile(&member, epoch, lineage.known_after());
                assert_eq!(lineage.matched(&lineage), Some(lineage.last().seq));
            }
        }
        MessageKind::SyncAck => {
            if wire::body::<SyncAck>(frame, MessageKind::SyncAck).is_ok() {
                record(frame);
            }
        }
        MessageKind::Append => {
            if wire::body::<Append>(frame, MessageKind::Append).is_ok() {
                record(frame);
            }
        }
        MessageKind::AppendAck => {
            let _ = wire::body::<AppendAck>(frame, MessageKind::AppendAck);
        }
        MessageKind::Beacon => {
            let _ = wire::body::<Beacon>(frame, MessageKind::Beacon);
        }
        MessageKind::StepDown => {
            if let Ok(step_down) = wire::body::<StepDown>(frame, MessageKind::StepDown) {
                let again = wire::frame(MessageKind::StepDown, &step_down, Default::default());
                assert_eq!(wire::body(&again, MessageKind::StepDown), Ok(step_down));
            }
        }
        MessageKind::Backfill => {
            if let Ok(backfill) = wire::body::<Backfill>(frame, MessageKind::Backfill) {
                let _ = backfill.position();
                if let Ok(chunk) = SnapshotRows::from_payload(&frame.payload) {
                    for row in &chunk.rows {
                        let _ = ShardTable::from_code(row.table);
                    }
                    let again = SnapshotRows::from_payload(&chunk.to_payload());
                    assert_eq!(again.as_ref(), Ok(&chunk));
                }
                record(frame);
            }
        }
        MessageKind::BackfillAck => {
            if let Ok(ack) = wire::body::<BackfillAck>(frame, MessageKind::BackfillAck) {
                assert_eq!(ack.positions().len(), ack.needs.len());
                let _ = ack.applied();
            }
        }
        _ => assert!(wire::body::<Beacon>(frame, MessageKind::Beacon).is_err()),
    }
}

/// The record a frame carries decodes within its payload.
fn record(frame: &Frame) {
    if let Ok((record, len)) = LogRecord::decode(&frame.payload) {
        assert!(len <= frame.payload.len());
        assert_eq!(LogRecord::decode(&record.to_bytes().unwrap()).unwrap().0, record);
    }
}
