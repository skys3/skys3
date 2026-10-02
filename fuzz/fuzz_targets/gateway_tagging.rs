//! Fuzzes the parsing of `x-amz-tagging` headers, which PutObject and
//! CopyObject take from clients. An accepted tag set must keep S3's limits,
//! survive being encoded again, and be accepted as a PutObjectTagging body.

#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use skys3_gateway::fuzzing::tagging;

fuzz_target!(|data: &[u8]| {
    // The harness panics when a property fails.
    let _ = tagging(data);
});
