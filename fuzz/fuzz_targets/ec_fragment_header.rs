//! Fuzzes the fragment record decoder (`skys3-ec`), which reads fragment
//! headers back from disk at recovery and on every read, and from other
//! nodes when a primary re-indexes coded objects (design §8.4).

#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use skys3_ec::fragment::{BLOCK_LEN, FIXED_LEN, FragmentRecord};

fuzz_target!(|data: &[u8]| {
    check(data);
    // Random bytes almost never carry valid checksums, so decode a copy
    // with the payload's block checksums and the header's CRC recomputed
    // as well. That lets the fuzzer reach the field parsers behind them.
    if let Some(sealed) = seal(data) {
        check(&sealed);
    }
});

/// Decoding must never panic, and a record that decodes re-encodes to
/// exactly the bytes it was read from.
fn check(data: &[u8]) {
    let Ok((record, len)) = FragmentRecord::decode(data) else {
        return;
    };
    let encoded = record.to_bytes().expect("a decoded record encodes");
    assert_eq!(&encoded[..], &data[..len]);
    assert_eq!(record.payload.len() as u64 + header_len(data) as u64, len as u64);
}

fn header_len(data: &[u8]) -> usize {
    u32::from_le_bytes(data[12..16].try_into().unwrap()) as usize
}

/// The record at the start of `data`, if its lengths fit, with valid
/// checksums: each payload block's in the last bytes of the header, then
/// the header's.
fn seal(data: &[u8]) -> Option<Vec<u8>> {
    let fixed = data.get(..FIXED_LEN)?;
    let header_len = header_len(fixed);
    let payload_len = usize::try_from(u64::from_le_bytes(fixed[16..24].try_into().unwrap())).ok()?;
    let blocks = payload_len.div_ceil(BLOCK_LEN as usize);
    let table = header_len.checked_sub(4 * blocks).filter(|&t| t >= FIXED_LEN)?;
    let end = header_len.checked_add(payload_len)?;
    let mut sealed = data.get(..end)?.to_vec();
    for (n, block) in data[header_len..end].chunks(BLOCK_LEN as usize).enumerate() {
        let at = table + 4 * n;
        sealed[at..at + 4].copy_from_slice(&crc32c::crc32c(block).to_le_bytes());
    }
    let crc = crc32c::crc32c(&sealed[8..header_len]);
    sealed[4..8].copy_from_slice(&crc.to_le_bytes());
    Some(sealed)
}
