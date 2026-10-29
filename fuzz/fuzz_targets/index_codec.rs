//! Fuzzes the index's key and value decoders, which read back what the
//! index stored on disk (design §10.2).

#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use skys3_index::codec::{self, VALUE_FORMAT};

fuzz_target!(|data: &[u8]| {
    check(data);
    // Most random bytes fail at the format byte; also try them behind a
    // valid one, to reach the field decoders.
    let mut value = Vec::with_capacity(data.len() + 1);
    value.push(VALUE_FORMAT);
    value.extend_from_slice(data);
    check(&value);
});

/// Decoding must never panic, and whatever decodes re-encodes to exactly
/// the bytes it was read from: every key and value has one encoding.
fn check(data: &[u8]) {
    if let Ok(entry) = codec::decode_entry(data) {
        assert_eq!(codec::encode_entry(&entry).expect("a decoded entry encodes"), data);
    }
    if let Ok(summary) = codec::decode_summary(data) {
        assert_eq!(codec::encode_summary(&summary).expect("a decoded summary encodes"), data);
    }
    if let Ok(control) = codec::decode_control(data) {
        assert_eq!(codec::encode_control(&control).expect("a decoded copy encodes"), data);
    }
    if let Ok(location) = codec::decode_location(data) {
        assert_eq!(codec::encode_location(&location), data);
    }
    if let Ok(applied) = codec::decode_applied(data) {
        assert_eq!(codec::encode_applied(applied), data);
    }
    if let Ok((shard, key)) = codec::decode_entry_key(data) {
        assert_eq!(codec::entry_key(&shard, &key), data);
    }
    if let Ok(shard) = codec::decode_shard_key(data) {
        assert_eq!(codec::shard_key(&shard), data);
    }
    if let Ok((shard, position)) = codec::decode_location_key(data) {
        assert_eq!(codec::location_key(&shard, position), data);
    }
    if let Ok((disk, segment)) = codec::decode_coverage_key(data) {
        assert_eq!(codec::coverage_key(&disk, segment), data);
    }
}
