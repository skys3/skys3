//! Fuzzes the parsing of `x-amz-copy-source-range` headers, which
//! UploadPartCopy takes from clients. An accepted range must start at or
//! before its end, survive being written again, and select exactly the
//! bytes it names of any source that can satisfy it.

#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use skys3_gateway::fuzzing::copy_source_range;

fuzz_target!(|data: &[u8]| {
    // The harness panics when a property fails.
    let _ = copy_source_range(data);
});
