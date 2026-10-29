//! Fuzzes parsing of the JSON Web Key Sets fetched from identity providers,
//! including key material handed to `aws-lc-rs`.

#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use skys3_sts::fuzzing::parse_jwks;

fuzz_target!(|data: &[u8]| {
    // Parsing must never panic, and never keep more keys than the limit;
    // `parse_jwks` asserts that.
    parse_jwks(data);
});
