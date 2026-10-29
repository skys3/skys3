//! Fuzzes the admin listener's `Authorization: Bearer` parsing and the
//! token check built on it, with arbitrary header bytes.
//!
//! `fuzz_target!`'s `#[no_mangle]` export comes from an external macro, which
//! the `unsafe_code` lint does not report, so this target forbids unsafe code
//! like every workspace crate.

#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use skys3_obs::fuzzing::{TOKEN, authorize, bearer_credentials};

fuzz_target!(|value: &[u8]| {
    if let Some(credentials) = bearer_credentials(value) {
        assert!(value[..7].eq_ignore_ascii_case(b"bearer "));
        assert!(!credentials.is_empty());
        assert_eq!(credentials, credentials.trim_ascii());
    }
    // Only a header whose credentials are exactly the token is authorized.
    if let Some(authorized) = authorize(value) {
        assert_eq!(
            authorized,
            bearer_credentials(value) == Some(TOKEN.as_bytes())
        );
    }
});
