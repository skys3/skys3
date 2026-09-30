//! Fuzzes `NodeAddress` parsing, which reads node registrations from the
//! control store.

#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use skys3_types::{Host, NodeAddress};

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    // Parsing must never panic. Anything accepted prints in canonical form,
    // which re-parses equal. Only IPv6 literals are normalized; every other
    // accepted address is already canonical.
    if let Ok(address) = text.parse::<NodeAddress>() {
        let canonical = address.to_string();
        assert!(canonical.len() <= NodeAddress::MAX_LEN);
        assert_eq!(canonical.parse::<NodeAddress>().ok(), Some(address.clone()));
        if !matches!(address.host(), Host::Ipv6(_)) {
            assert_eq!(canonical, text);
        }
    }
});
