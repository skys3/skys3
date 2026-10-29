//! Fuzzes parsing of the OIDC tokens STS receives in `WebIdentityToken`:
//! compact JWS structure, the header, and the registered claims.

#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use skys3_sts::fuzzing::parse_token;

fuzz_target!(|data: &[u8]| {
    // Parsing must never panic, and an accepted token's signing input must
    // be exactly its first two parts; `parse_token` asserts that.
    parse_token(data);
});
