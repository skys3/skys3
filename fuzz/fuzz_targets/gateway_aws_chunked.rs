//! Fuzzes the gateway's `aws-chunked` decoder: arbitrary bodies in every
//! form (signed chunks, signed and unsigned trailers), split into reads at
//! arbitrary points, must decode to exactly their declared length or be
//! refused, and data the harness encodes must decode back to itself.
//!
//! The decoder buffers only chunk header lines and trailers, both bounded,
//! so memory stays small whatever lengths an input declares.

#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use skys3_gateway::fuzzing::aws_chunked;

fuzz_target!(|data: &[u8]| {
    // The harness panics when a property fails.
    let _ = aws_chunked(data);
});
