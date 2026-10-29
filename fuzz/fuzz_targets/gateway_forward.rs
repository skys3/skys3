//! Fuzzes the forwarding messages a replica or a gateway decodes from an
//! authenticated but untrusted peer (design §5.1, §12): requests with the
//! record they carry, and replies with their answer and redirect hint.

#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use skys3_gateway::ShardRef;
use skys3_gateway::routing::wire;
use skys3_net::{Frame, Header, MessageKind};
use skys3_types::{BucketId, ShardId};

fuzz_target!(|data: &[u8]| {
    // A body and a payload split from the input, as one frame carries them.
    let split = data.first().map_or(0, |n| usize::from(*n)).min(data.len());
    let (body, payload) = data.split_at(split);
    for kind in [MessageKind::Forward, MessageKind::ForwardReply] {
        let frame = Frame::new(Header::new(kind).with_body(body.to_vec()), payload.to_vec());
        check(&frame);
    }
    // Whole frames, as they arrive.
    if let Ok(Some((frame, _))) = Frame::decode(data) {
        check(&frame);
    }
});

/// Decoding never panics, and what decodes encodes to what decodes alike.
fn check(frame: &Frame) {
    if let Ok((shard, epoch, request)) = wire::decode_request(frame) {
        let again = wire::request_frame(&shard, epoch, &request).expect("a decoded request encodes");
        assert_eq!(wire::decode_request(&again), Ok((shard, epoch, request)));
    }
    let shard = ShardRef {
        bucket: BucketId::new("b-fuzz").expect("a valid bucket ID"),
        shard: ShardId::new(0),
    };
    if let Ok(reply) = wire::decode_reply(frame, &shard) {
        let again = wire::reply_frame(&reply, frame.header.request_id);
        assert!(wire::decode_reply(&again, &shard).is_ok());
    }
}
