//! Property tests of recovery on arbitrary segment contents: it never
//! panics, keeps every whole record before the first bad byte, and either
//! cuts the tail or refuses to start.

mod harness;

use bytes::Bytes;
use harness::{delete, extent, inline_put, open, runtime, small_config};
use proptest::collection::vec;
use proptest::prelude::*;
use skys3_io::{Disk, SegmentFile, SimDisk};
use skys3_log::record::ErrorClass;
use skys3_log::segment::file_name;
use skys3_log::{LogRecord, RecoveryError, SegmentClass, SegmentId};

fn records() -> impl Strategy<Value = Vec<LogRecord>> {
    vec(
        (0..3_u8, 0..1000_u64, 0..600_usize).prop_map(|(kind, seq, len)| match kind {
            0 => delete(0, seq),
            1 => inline_put(0, seq, len % 500),
            _ => extent(0, seq, len + 1),
        }),
        0..6,
    )
}

/// Bytes after the valid records: random bytes, a prefix of a record, or a
/// record with one byte changed (and, optionally, its CRC recomputed).
fn tail() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        vec(any::<u8>(), 0..200),
        (0..1000_u64, any::<prop::sample::Index>()).prop_map(|(seq, cut)| {
            let bytes = delete(1, seq).to_bytes().unwrap();
            bytes[..cut.index(bytes.len())].to_vec()
        }),
        (
            0..1000_u64,
            any::<prop::sample::Index>(),
            1..=255_u8,
            any::<bool>()
        )
            .prop_map(|(seq, at, flip, reseal)| {
                let mut bytes = inline_put(1, seq, 30).to_bytes().unwrap().to_vec();
                let at = at.index(bytes.len());
                bytes[at] ^= flip;
                if reseal && at >= 8 {
                    let crc = crc32c::crc32c(&bytes[8..]);
                    bytes[4..8].copy_from_slice(&crc.to_le_bytes());
                }
                bytes
            }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn recovery_keeps_whole_records_and_cuts_or_refuses_the_rest(
        records in records(),
        tail in tail(),
    ) {
        runtime().block_on(async {
            let disk = SimDisk::new(0);
            let name = file_name(SegmentClass::Hot, SegmentId::new(0));
            let mut valid = Vec::new();
            for record in &records {
                record.encode(&mut valid).unwrap();
            }
            let file = disk.mount().create(&name).await.unwrap();
            file.append(Bytes::from(valid.clone())).await.unwrap();
            file.append(Bytes::from(tail.clone())).await.unwrap();
            drop(file);

            match open(disk.mount(), small_config()).await {
                Ok((log, report)) => {
                    let mut scanner = log.scan(SegmentId::new(0)).unwrap();
                    let mut found = Vec::new();
                    while let Some(scanned) = scanner.next().await.unwrap() {
                        found.push(scanned);
                    }
                    // The valid records survive. Anything kept after them is
                    // a record whose header and CRC verify; replay decodes
                    // its body.
                    prop_assert!(found.len() >= records.len());
                    for (scanned, record) in found.iter().zip(&records) {
                        prop_assert_eq!(&scanned.decode().unwrap(), record);
                    }
                    let len = report.segments[0].len;
                    prop_assert!(len >= valid.len() as u64);
                    prop_assert!(len <= (valid.len() + tail.len()) as u64);
                    for torn in &report.torn_tails {
                        prop_assert!(matches!(
                            torn.reason.class(),
                            ErrorClass::Incomplete | ErrorClass::Corrupt
                        ));
                        prop_assert_eq!(torn.valid_len, len);
                    }
                }
                Err(RecoveryError::Unreadable { offset, source, .. }) => {
                    prop_assert_eq!(offset, valid.len() as u64);
                    prop_assert!(matches!(
                        source.class(),
                        ErrorClass::Unsupported | ErrorClass::Invalid
                    ));
                    // Nothing was cut.
                    let info = disk.file_info(&name).unwrap();
                    prop_assert_eq!(info.written, (valid.len() + tail.len()) as u64);
                }
                Err(error) => prop_assert!(false, "unexpected error: {error}"),
            }
            Ok(())
        })?;
    }
}
