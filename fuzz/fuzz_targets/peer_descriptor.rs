//! Fuzzes the peer descriptor decoder and verifier, which read what a
//! target's S3 endpoint answers before the source trusts it (design §7.8).

#![no_main]
#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use libfuzzer_sys::fuzz_target;
use skys3_config::MAX_ADVERTISED_ADDRESSES;
use skys3_peer::{DescriptorVerifier, Expected, PeerDescriptor, PeerTrust};
use skys3_types::{BucketName, ClusterId};

fuzz_target!(|data: &[u8]| {
    if let Ok(descriptor) = PeerDescriptor::decode_unverified(data) {
        assert!((1..=MAX_ADVERTISED_ADDRESSES).contains(&descriptor.addresses.len()));
        assert!(descriptor.issued < descriptor.expires);
        assert!(descriptor.versions.min() <= descriptor.versions.max());
    }
    // A node that trusts no cluster accepts nothing.
    let verifier = DescriptorVerifier::new(Arc::new(PeerTrust::new()));
    let (bucket, source) = (
        BucketName::new("archive").unwrap(),
        ClusterId::new("prod-us").unwrap(),
    );
    let expected = Expected {
        bucket: &bucket,
        source: &source,
    };
    assert!(
        verifier
            .verify(data, Duration::from_secs(1_800_000_000), expected)
            .is_err()
    );
});
