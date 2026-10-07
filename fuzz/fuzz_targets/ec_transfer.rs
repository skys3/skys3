//! Fuzzes what a fragment node decodes from a shard primary's fragment
//! write, and what the primary decodes from the answer (design §8.4): the
//! `FragmentWrite` and `FragmentWritten` bodies, and the fragment header
//! that starts a write's bytes.

#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use prost::Message;
use skys3_ec::fragment::FragmentHeader;
use skys3_ec::{FragmentWrite, FragmentWritten};

fuzz_target!(|data: &[u8]| {
    // Decoding must never panic, and a header that decodes re-encodes to
    // exactly the bytes it was read from.
    if let Ok(header) = FragmentHeader::from_bytes(data) {
        let encoded = header.to_bytes().expect("a decoded header encodes");
        assert_eq!(encoded, data);
    }
    if let Ok(write) = FragmentWrite::decode(data) {
        let again = FragmentWrite::decode(write.encode_to_vec().as_slice());
        assert_eq!(again.ok(), Some(write));
    }
    if let Ok(written) = FragmentWritten::decode(data) {
        let again = FragmentWritten::decode(written.encode_to_vec().as_slice());
        assert_eq!(again.ok(), Some(written));
    }
});
