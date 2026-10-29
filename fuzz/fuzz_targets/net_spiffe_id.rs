//! Fuzzes the parser of the SPIFFE IDs that name peers in their
//! certificates (design §12).

#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use skys3_net::PeerIdentity;
use skys3_types::ClusterId;

fuzz_target!(|data: &[u8]| {
    let Ok(uri) = std::str::from_utf8(data) else {
        return;
    };
    let cluster = ClusterId::new("fuzz").expect("a valid cluster ID");
    // An accepted ID is exactly the canonical form of what it parses to.
    if let Ok(identity) = PeerIdentity::from_spiffe_id(uri, &cluster) {
        assert_eq!(identity.spiffe_id(&cluster), uri);
    }
});
