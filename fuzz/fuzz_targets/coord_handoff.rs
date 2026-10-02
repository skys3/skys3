//! Fuzzes requests to hand a shard off and their answers, which a node and
//! the coordinator decode from authenticated but untrusted peers (design
//! §6.7, §12): the bodies alone, as a header carries them, and whole
//! frames as they arrive.

#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use skys3_coord::{Handoff, HandoffAck};
use skys3_net::{Frame, Header, MessageKind};

fuzz_target!(|data: &[u8]| {
    for kind in [MessageKind::Handoff, MessageKind::AdminReply] {
        let frame = Frame::new(
            Header::new(kind).with_request_id(7).with_body(data.to_vec()),
            Vec::new(),
        );
        check(&frame);
    }
    if let Ok(Some((frame, _))) = Frame::decode(data) {
        check(&frame);
    }
});

/// Decoding never panics, and a decoded request or answer is sent again as
/// itself.
fn check(frame: &Frame) {
    let request = frame.header.request_id;
    if let Ok(handoff) = Handoff::from_frame(frame) {
        assert!(handoff.epoch.get() > 0);
        assert_eq!(Handoff::from_frame(&handoff.frame(request)), Ok(handoff));
    }
    if let Ok(ack) = HandoffAck::from_frame(frame, request) {
        assert_eq!(HandoffAck::from_frame(&ack.frame(request), request), Ok(ack));
    }
}
