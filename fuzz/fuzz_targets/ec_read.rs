//! Fuzzes what a fragment node decodes from a gateway's fragment read, and
//! what the gateway decodes from the node's answer (design §8.5): the
//! `FragmentRead` and `FragmentData` bodies and their checks.

#![no_main]
#![forbid(unsafe_code)]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use prost::Message;
use skys3_ec::FragmentRequest;
use skys3_ec::read::{FragmentData, FragmentRead, MAX_READ_LEN};
use skys3_types::NodeId;

fuzz_target!(|data: &[u8]| {
    // Decoding and checking must never panic, and a read that checks out
    // names a bounded range and is built again from what it names.
    if let Ok(read) = FragmentRead::decode(data)
        && let Ok((fragment, identity, range)) = read.parse()
    {
        assert!(range.start < range.end && range.end - range.start <= MAX_READ_LEN);
        let node = NodeId::new("fuzz").expect("a valid node ID");
        let request = FragmentRequest {
            node,
            fragment,
            identity: identity.clone(),
            range: range.clone(),
        };
        assert_eq!(FragmentRead::new(&request).parse(), Ok((fragment, identity, range)));
    }
    if let Ok(answer) = FragmentData::decode(data) {
        let node = NodeId::new("fuzz").expect("a valid node ID");
        let served = answer.parse(&node, Bytes::copy_from_slice(data));
        assert_eq!(served.is_ok(), answer.failure == 0);
    }
});
