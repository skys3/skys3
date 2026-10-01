//! Fuzzes the reading of checksum headers (`Content-MD5`,
//! `x-amz-checksum-*`, `x-amz-trailer`, `x-amz-sdk-checksum-algorithm`) and
//! of stored checksum values, which also arrive from remote responses.
//! Accepted values must print back to values that parse to the same
//! checksum, and every refusal must have an S3 answer.

#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use skys3_gateway::fuzzing::checksums;

fuzz_target!(|data: &[u8]| {
    // The harness panics when a property fails.
    let _ = checksums(data);
});
