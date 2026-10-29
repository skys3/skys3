//! Fuzzes the push of a new generation, which a node decodes from an
//! authenticated but untrusted peer (design §6.2, §12): the body alone, as
//! a header carries it, and whole frames as they arrive.

#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use skys3_coord::ControlChanged;
use skys3_net::{Frame, Header, MessageKind};

fuzz_target!(|data: &[u8]| {
    let body = Frame::new(
        Header::new(MessageKind::ControlChanged).with_body(data.to_vec()),
        Vec::new(),
    );
    check(&body);
    if let Ok(Some((frame, _))) = Frame::decode(data) {
        check(&frame);
    }
});

/// Decoding never panics, and a decoded generation is announced again as
/// itself.
fn check(frame: &Frame) {
    if let Ok(generation) = ControlChanged::from_frame(frame) {
        assert!(generation.get() > 0);
        let again = ControlChanged::frame(generation, frame.header.request_id);
        assert_eq!(ControlChanged::from_frame(&again), Ok(generation));
    }
}
