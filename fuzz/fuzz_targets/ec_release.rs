//! Fuzzes what a fragment node decodes from a shard primary's release and
//! from a reader's hold, and what both decode from the node's answer
//! (design §8.7): the `FragmentRelease`, `FragmentHold`, and
//! `FragmentNoted` bodies and their checks.

#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use prost::Message;
use skys3_ec::release::{FragmentHold, FragmentNoted, FragmentRelease};

fuzz_target!(|data: &[u8]| {
    // Decoding and checking must never panic, and a body that checks out
    // is built again from what it names.
    if let Ok(release) = FragmentRelease::decode(data)
        && let Ok((shard, fragments)) = release.parse()
    {
        let again = FragmentRelease::new(&shard, &fragments).parse();
        assert_eq!(again, Ok((shard, fragments)));
    }
    if let Ok(hold) = FragmentHold::decode(data)
        && let Ok(fragments) = hold.parse()
    {
        let again = FragmentHold::new(hold.registration, &fragments, hold.end).parse();
        assert_eq!(again, Ok(fragments));
    }
    if let Ok(noted) = FragmentNoted::decode(data)
        && noted.parse().is_ok()
    {
        assert!(noted.error.is_empty());
    }
});
