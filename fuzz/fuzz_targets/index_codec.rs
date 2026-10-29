//! Fuzzes the index's key and value decoders, which read back what the
//! index stored on disk (design §10.2).

#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use skys3_index::codec::{self, MIN_VALUE_FORMAT, VALUE_FORMAT};

fuzz_target!(|data: &[u8]| {
    check(data);
    // Most random bytes fail at the format byte; also try them behind each
    // format this build reads, to reach the field decoders.
    for format in MIN_VALUE_FORMAT..=VALUE_FORMAT {
        let mut value = Vec::with_capacity(data.len() + 1);
        value.push(format);
        value.extend_from_slice(data);
        check(&value);
    }
});

/// Asserts that `decoded`, read from `data`, re-encodes to exactly `data`
/// if `data` is in the current value format, and otherwise to a value that
/// decodes to the same thing.
fn same<T: PartialEq + std::fmt::Debug>(
    data: &[u8],
    encoded: &[u8],
    decoded: &T,
    decode: impl Fn(&[u8]) -> Option<T>,
) {
    if data.first() == Some(&VALUE_FORMAT) {
        assert_eq!(encoded, data);
    } else {
        assert_eq!(encoded.first(), Some(&VALUE_FORMAT));
        assert_eq!(decode(encoded).as_ref(), Some(decoded));
    }
}

/// Decoding must never panic, and whatever decodes re-encodes to exactly
/// the bytes it was read from: every key, and every value of the current
/// format, has one encoding. A value of an older format re-encodes in the
/// current one.
fn check(data: &[u8]) {
    if let Ok(entry) = codec::decode_entry(data) {
        let encoded = codec::encode_entry(&entry).expect("a decoded entry encodes");
        same(data, &encoded, &entry, |b| codec::decode_entry(b).ok());
    }
    if let Ok(summary) = codec::decode_summary(data) {
        let encoded = codec::encode_summary(&summary).expect("a decoded summary encodes");
        same(data, &encoded, &summary, |b| codec::decode_summary(b).ok());
    }
    if let Ok(control) = codec::decode_control(data) {
        let encoded = codec::encode_control(&control).expect("a decoded copy encodes");
        same(data, &encoded, &control, |b| codec::decode_control(b).ok());
    }
    if let Ok(location) = codec::decode_location(data) {
        let encoded = codec::encode_location(&location);
        same(data, &encoded, &location, |b| codec::decode_location(b).ok());
    }
    if let Ok(applied) = codec::decode_applied(data) {
        let encoded = codec::encode_applied(applied);
        same(data, &encoded, &applied, |b| codec::decode_applied(b).ok());
    }
    if let Ok(upload) = codec::decode_upload(data) {
        let encoded = codec::encode_upload(&upload).expect("a decoded upload encodes");
        same(data, &encoded, &upload, |b| codec::decode_upload(b).ok());
    }
    if let Ok(part) = codec::decode_part(data) {
        let encoded = codec::encode_part(&part).expect("a decoded part encodes");
        same(data, &encoded, &part, |b| codec::decode_part(b).ok());
    }
    if let Ok((shard, key, upload)) = codec::decode_upload_key(data) {
        assert_eq!(codec::upload_key(&shard, &key, upload), data);
    }
    if let Ok((shard, upload, number)) = codec::decode_part_key(data) {
        assert_eq!(codec::part_key(&shard, upload, number), data);
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
