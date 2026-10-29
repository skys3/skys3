//! Fuzzes the gateway's request pipeline with arbitrary requests and
//! arbitrary XML bodies for the bucket and object operations that take
//! one: request limits, rejected features, XML bounds, `s3s` parsing, and
//! the bucket operations, over in-memory state.
//!
//! Run with a memory bound, for example `-rss_limit_mb=512`: every body
//! the gateway buffers is bounded by its limits.

#![no_main]
#![forbid(unsafe_code)]

use std::sync::LazyLock;

use libfuzzer_sys::fuzz_target;
use skys3_gateway::fuzzing::{Harness, MAX_RESPONSE_BYTES};

static HARNESS: LazyLock<Harness> = LazyLock::new(Harness::new);

fuzz_target!(|data: &[u8]| {
    // The harness panics on a response longer than MAX_RESPONSE_BYTES.
    if let Some((status, len)) = HARNESS.request(data) {
        // No input is an internal error: every failure is the client's.
        assert_ne!(status, 500, "an internal error answered {data:?}");
        assert!((200..600).contains(&status));
        assert!(len <= MAX_RESPONSE_BYTES);
    }
});
