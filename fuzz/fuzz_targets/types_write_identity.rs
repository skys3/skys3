//! Fuzzes `WriteIdentity` parsing, which reads values back from remote
//! object metadata.
//!
//! No `#![forbid(unsafe_code)]`: `fuzz_target!` expands to a `#[no_mangle]`
//! export, which the `unsafe_code` lint flags.
#![no_main]

use libfuzzer_sys::fuzz_target;
use skys3_types::WriteIdentity;

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    // Parsing must never panic. Anything accepted is already canonical: it
    // prints back as the input, matches it, and re-parses equal.
    if let Ok(wid) = text.parse::<WriteIdentity>() {
        let canonical = wid.to_string();
        assert_eq!(canonical, text);
        assert!(canonical.len() <= WriteIdentity::MAX_LEN);
        assert!(wid.matches(text));
        assert_eq!(canonical.parse::<WriteIdentity>().ok(), Some(wid));
    }
});
