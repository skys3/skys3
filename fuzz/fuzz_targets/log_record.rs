//! Fuzzes the log record decoder, which reads records back from disk and,
//! later, from peers (design §10.1).

#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use skys3_log::record::{LogRecord, RecordHeader};

fuzz_target!(|data: &[u8]| {
    check(data);
    // Random bytes almost never carry a valid CRC, so decode a copy with
    // the CRC recomputed as well. That lets the fuzzer reach the
    // kind-specific parsers behind the checksum.
    if let Ok(len) = RecordHeader::peek_len(data)
        && let Some(record) = data.get(..len)
    {
        let mut sealed = record.to_vec();
        let crc = crc32c::crc32c(&sealed[8..]);
        sealed[4..8].copy_from_slice(&crc.to_le_bytes());
        check(&sealed);
    }
});

/// Decoding must never panic. A record that decodes re-encodes to exactly
/// the bytes it was read from, and its fixed header agrees with it.
fn check(data: &[u8]) {
    let Ok((record, len)) = LogRecord::decode(data) else {
        return;
    };
    let encoded = record.to_bytes().expect("a decoded record encodes");
    assert_eq!(&encoded[..], &data[..len]);
    let header = RecordHeader::decode(data).expect("a decoded record has a valid header");
    assert_eq!(header.kind, record.kind());
    assert_eq!(header.position, record.position);
    assert_eq!(header.key_hash, record.key_hash());
    assert_eq!(header.record_len(), len);
}
