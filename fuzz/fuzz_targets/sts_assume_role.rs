//! Fuzzes parsing of `AssumeRoleWithWebIdentity` requests, which STS takes
//! from unauthenticated callers: the form-encoded query string and body.
//!
//! The input is the query string, a newline, and the body; without a
//! newline, all of it is the body.

#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use skys3_sts::fuzzing::parse_assume_role;

fuzz_target!(|data: &[u8]| {
    // Parsing must never panic, and an accepted request must satisfy the
    // rules the parser enforces; `parse_assume_role` asserts them.
    parse_assume_role(data);
});
