//! Fuzzes the etcd backend's reading of what etcd sends: gRPC message
//! framing, the Protobuf responses, and the status trailers (design
//! §6.1). etcd is trusted to answer correctly, but a broken or hostile
//! endpoint must not crash a node or make it buffer without bound.

#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use skys3_control::fuzzing::{ETCD_PREFIX_LEN, etcd_frames, etcd_status};

fuzz_target!(|data: &[u8]| {
    let Some((&chunk, body)) = data.split_first() else {
        return;
    };
    // However the stream is split into reads, the same messages come out.
    let whole = etcd_frames(body, body.len());
    let pieces = etcd_frames(body, usize::from(chunk));
    assert_eq!(whole.messages, pieces.messages);
    assert_eq!(whole.refused, pieces.refused);
    let framed: usize = whole
        .messages
        .iter()
        .map(|message| ETCD_PREFIX_LEN + message.len())
        .sum();
    assert!(framed <= body.len());

    // A status from two header values split at the chunk byte.
    let split = usize::from(chunk).min(body.len());
    let (code, message) = body.split_at(split);
    if let Some((_, text)) = etcd_status(code, message) {
        assert!(text.len() <= message.len() * 3);
    }
});
