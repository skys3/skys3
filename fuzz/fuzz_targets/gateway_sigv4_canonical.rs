//! Fuzzes SigV4 canonicalization and verification with arbitrary
//! requests: canonical paths and queries must be stable when canonicalized
//! again, the authenticator must answer any request with a client error or
//! let it through, never fail inside, and a request the harness signs with
//! every header included must pass.

#![no_main]
#![forbid(unsafe_code)]

use std::sync::LazyLock;

use libfuzzer_sys::fuzz_target;
use skys3_gateway::fuzzing::Harness;

static HARNESS: LazyLock<Harness> = LazyLock::new(Harness::new);

fuzz_target!(|data: &[u8]| {
    // The harness panics when a property fails.
    let _ = HARNESS.sigv4(data);
});
