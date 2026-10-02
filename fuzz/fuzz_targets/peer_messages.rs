//! Fuzzes the peer protocol decoder, which reads messages from another
//! SkyS3 cluster over an authenticated but untrusted connection (design
//! §7.8, §12).

#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use skys3_peer::{MAX_HEADER_LEN, MAX_PAYLOAD_LEN, Message, PREFIX_LEN, parse_prefix};

fuzz_target!(|data: &[u8]| {
    // Whole frames, as they arrive.
    if let Ok(Some((message, len))) = Message::decode(data) {
        assert!(len <= data.len());
        check(&message);
        // A reader that splits the frame at its prefix decodes the same.
        let prefix = data.first_chunk::<PREFIX_LEN>().expect("a whole frame");
        let (header_len, payload_len) = parse_prefix(prefix).expect("a valid prefix");
        assert_eq!(PREFIX_LEN + header_len + payload_len, len);
        let header = &data[PREFIX_LEN..PREFIX_LEN + header_len];
        let payload = data[PREFIX_LEN + header_len..len].to_vec().into();
        assert_eq!(Message::decode_parts(header, payload).unwrap(), message);
    }
    // The input as a header alone, with and without a payload, so that
    // mutations reach the message fields without a valid prefix.
    for split in [data.len(), data.len() / 2] {
        let (header, payload) = data.split_at(split);
        if let Ok(message) = Message::decode_parts(header, payload.to_vec().into()) {
            check(&message);
        }
    }
});

/// A decoded message is valid, and re-encodes to a frame within the limits
/// that decodes back to it.
fn check(message: &Message) {
    message.validate().expect("a decoded message is valid");
    let encoded = message.encode().expect("a decoded message encodes");
    assert!(encoded.len() <= PREFIX_LEN + MAX_HEADER_LEN as usize + MAX_PAYLOAD_LEN as usize);
    assert_eq!(
        Message::decode(&encoded).unwrap(),
        Some((message.clone(), encoded.len()))
    );
}
