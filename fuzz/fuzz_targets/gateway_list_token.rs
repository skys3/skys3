//! Fuzzes the decoding of ListObjectsV2 continuation tokens, which clients
//! send back verbatim. Only a token the gateway's keys sealed for the same
//! listing may open, every item must survive its own token, and a token
//! changed in any character must be refused.

#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use skys3_gateway::fuzzing::list_token;

fuzz_target!(|data: &[u8]| {
    // The harness panics when a property fails.
    let _ = list_token(data);
});
