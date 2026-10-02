//! Fuzzes heartbeats and their answers, which the coordinator and the
//! nodes decode from authenticated but untrusted peers (design §6.7, §12):
//! the bodies alone, as a header carries them, and whole frames as they
//! arrive.

#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use skys3_coord::{Heartbeat, HeartbeatAck};
use skys3_net::{Frame, Header, MessageKind};

fuzz_target!(|data: &[u8]| {
    for kind in [MessageKind::NodeHeartbeat, MessageKind::AdminReply] {
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

/// Decoding never panics, and a decoded answer is sent again as itself.
fn check(frame: &Frame) {
    let _ = Heartbeat::from_frame(frame);
    let request = frame.header.request_id;
    if let Ok(ack) = HeartbeatAck::from_frame(frame, request) {
        assert_eq!(HeartbeatAck::from_frame(&ack.frame(request), request), Ok(ack));
    }
}
