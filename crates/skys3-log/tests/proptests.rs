//! Property tests: encode and decode round trips, canonical encoding, and a
//! total decoder.

mod support;

use proptest::collection::vec;
use proptest::prelude::*;

use skys3_log::record::{ErrorClass, LogRecord, RecordHeader};
use support::{Frame, record, reseal};

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Every valid record encodes, decodes to an equal record, and reports
    /// its exact length.
    #[test]
    fn records_round_trip(record in record()) {
        let bytes = record.to_bytes().unwrap();
        record.check().unwrap();
        let (decoded, len) = LogRecord::decode(&bytes).unwrap();
        prop_assert_eq!(len, bytes.len());
        prop_assert_eq!(&decoded, &record);
        prop_assert_eq!(decoded.to_bytes().unwrap(), bytes.clone());

        let header = RecordHeader::decode(&bytes).unwrap();
        prop_assert_eq!(header.kind, record.kind());
        prop_assert_eq!(&header.shard, &record.shard);
        prop_assert_eq!(header.position, record.position);
        prop_assert_eq!(header.key_hash, record.key_hash());
        prop_assert_eq!(header.record_len(), bytes.len());
        prop_assert_eq!(RecordHeader::peek_len(&bytes).unwrap(), bytes.len());
    }

    /// Records decode one after another from a shared buffer, as they sit in
    /// a segment, and the bytes after a record are never read.
    #[test]
    fn records_decode_back_to_back(records in vec(record(), 1..4), tail in vec(any::<u8>(), 0..16)) {
        let mut buf = Vec::new();
        for record in &records {
            record.encode(&mut buf).unwrap();
        }
        buf.extend_from_slice(&tail);
        let mut rest = &buf[..];
        for record in &records {
            let (decoded, len) = LogRecord::decode(rest).unwrap();
            prop_assert_eq!(&decoded, record);
            rest = &rest[len..];
        }
        prop_assert_eq!(rest, &tail[..]);
    }

    /// Every proper prefix of a record, as a torn write leaves it, is
    /// rejected as incomplete.
    #[test]
    fn torn_records_are_incomplete(record in record(), cut in any::<prop::sample::Index>()) {
        let bytes = record.to_bytes().unwrap();
        let prefix = &bytes[..cut.index(bytes.len())];
        let error = LogRecord::decode(prefix).unwrap_err();
        prop_assert_eq!(error.class(), ErrorClass::Incomplete, "{}", error);
    }

    /// Changing any one byte is always detected: by the magic check, a
    /// length check, or the CRC.
    #[test]
    fn damaged_records_never_decode(
        record in record(),
        at in any::<prop::sample::Index>(),
        flip in 1..=u8::MAX,
    ) {
        let mut bytes = record.to_bytes().unwrap().to_vec();
        let at = at.index(bytes.len());
        bytes[at] ^= flip;
        let error = LogRecord::decode(&bytes).unwrap_err();
        prop_assert!(
            matches!(error.class(), ErrorClass::Corrupt | ErrorClass::Incomplete | ErrorClass::Unsupported),
            "byte {at}: {error}"
        );
    }

    /// Arbitrary bytes never panic the decoder, and anything it accepts
    /// re-encodes to exactly the bytes it read.
    #[test]
    fn decoding_arbitrary_bytes_is_total(bytes in vec(any::<u8>(), 0..400)) {
        assert_canonical(&bytes)?;
    }

    /// The same holds behind a valid fixed header and CRC, which random
    /// bytes almost never have, so the kind-specific parsers are reached.
    #[test]
    fn decoding_arbitrary_bodies_is_total(
        kind in 0u16..20,
        body in vec(any::<u8>(), 0..300),
        payload in vec(any::<u8>(), 0..8),
        keyed in any::<bool>(),
    ) {
        let mut frame = Frame::new(kind);
        if keyed {
            frame = frame.keyed("k");
        }
        assert_canonical(&frame.build(&body, &payload))?;
    }

    /// Mutating a valid record's kind-specific header, and resealing it so
    /// the CRC passes, never panics the decoder and never yields a record
    /// that encodes differently.
    #[test]
    fn decoding_mutated_bodies_is_total(
        record in record(),
        at in any::<prop::sample::Index>(),
        value in any::<u8>(),
    ) {
        let mut bytes = record.to_bytes().unwrap().to_vec();
        if bytes.len() > RecordHeader::LEN {
            let at = RecordHeader::LEN + at.index(bytes.len() - RecordHeader::LEN);
            bytes[at] = value;
            reseal(&mut bytes);
        }
        assert_canonical(&bytes)?;
    }
}

fn assert_canonical(bytes: &[u8]) -> Result<(), TestCaseError> {
    if let Ok((record, len)) = LogRecord::decode(bytes) {
        prop_assert_eq!(&record.to_bytes().unwrap()[..], &bytes[..len]);
    }
    Ok(())
}
