//! Fuzzes the decoding of index snapshot objects and their keys, which a
//! restore reads from a snapshot target (design §8.9).

#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use skys3_flush::snapshot::{Snapshot, object_key, parse_object_key};

fuzz_target!(|data: &[u8]| {
    // Decoding must never panic. A snapshot that decodes encodes back to
    // one that decodes equal.
    if let Ok(snapshot) = Snapshot::decode(data) {
        assert_eq!(Snapshot::decode(&snapshot.encode()).ok(), Some(snapshot));
    }
    // A key that parses names the key it came from.
    if let Ok(name) = std::str::from_utf8(data) {
        let dir = "p/.skys3-snapshots/b/0/";
        let key = format!("{dir}{name}");
        if let Some((chain, number)) = parse_object_key(dir, &key) {
            assert_eq!(object_key(dir, &chain, number), key);
        }
    }
});
