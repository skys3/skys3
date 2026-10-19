use std::net::SocketAddr;
use std::sync::Mutex;

use proptest::prelude::*;
use skys3_net::{Listener, TokioNetwork};

use super::*;
use crate::admin::AdminEndpoint;
use crate::push::{ControlHints, PushError};
use crate::testing::Pki;

/// Every wait in these tests.
const WAIT: Duration = Duration::from_secs(30);

fn node(n: u8) -> NodeId {
    NodeId::new(format!("node-{n}")).unwrap()
}

fn handoff() -> Handoff {
    Handoff {
        bucket: BucketId::new("b-1").unwrap(),
        shard: ShardId::new(3),
        epoch: Epoch::new(42),
        to: node(2),
    }
}

fn body_frame(body: &HandoffBody) -> Frame {
    Frame::new(
        Header::new(MessageKind::Handoff)
            .with_request_id(9)
            .with_body(body.encode_to_vec()),
        Bytes::new(),
    )
}

fn valid_body() -> HandoffBody {
    HandoffBody {
        bucket_id: "b-1".to_owned(),
        shard: 3,
        epoch: 42,
        to: "node-2".to_owned(),
    }
}

#[test]
fn requests_carry_the_shard_the_epoch_and_the_member() {
    let frame = handoff().frame(5);
    assert_eq!(frame.header.kind, MessageKind::Handoff);
    assert_eq!(frame.header.request_id, 5);
    assert!(frame.payload.is_empty());
    assert_eq!(Handoff::from_frame(&frame), Ok(handoff()));
    assert_eq!(
        Handoff::from_frame(&body_frame(&valid_body())),
        Ok(handoff())
    );
}

#[test]
fn malformed_requests_are_refused() {
    let mut kind = handoff().frame(1);
    kind.header.kind = MessageKind::NodeHeartbeat;
    assert_eq!(
        Handoff::from_frame(&kind),
        Err(HandoffError::Kind {
            expected: MessageKind::Handoff,
            found: MessageKind::NodeHeartbeat,
        })
    );
    let mut payload = handoff().frame(1);
    payload.payload = Bytes::from_static(b"xy");
    assert_eq!(Handoff::from_frame(&payload), Err(HandoffError::Payload(2)));
    let mut malformed = handoff().frame(1);
    malformed.header.body = Bytes::from_static(&[0x08]);
    assert!(matches!(
        Handoff::from_frame(&malformed),
        Err(HandoffError::Malformed(_))
    ));

    let field = |body: HandoffBody| match Handoff::from_frame(&body_frame(&body)) {
        Err(HandoffError::Invalid { field, .. }) => field,
        other => panic!("{other:?}"),
    };
    assert_eq!(
        field(HandoffBody {
            bucket_id: String::new(),
            ..valid_body()
        }),
        "bucket"
    );
    assert_eq!(
        field(HandoffBody {
            shard: 256,
            ..valid_body()
        }),
        "shard"
    );
    assert_eq!(
        field(HandoffBody {
            epoch: 0,
            ..valid_body()
        }),
        "epoch"
    );
    assert_eq!(
        field(HandoffBody {
            to: "Not A Node".to_owned(),
            ..valid_body()
        }),
        "to"
    );
}

#[test]
fn answers_say_whether_the_handoff_started() {
    let started = HandoffAck::started();
    let frame = started.frame(7);
    assert_eq!(frame.header.kind, MessageKind::AdminReply);
    assert_eq!(HandoffAck::from_frame(&frame, 7), Ok(started));
    let refused = HandoffAck::refused("a promotion is outstanding");
    assert_eq!(
        HandoffAck::from_frame(&refused.frame(8), 8),
        Ok(refused.clone())
    );
    assert!(!refused.started);
    assert_eq!(
        HandoffAck::from_frame(&refused.frame(8), 9),
        Err(HandoffError::RequestId {
            expected: 9,
            found: 8
        })
    );
    assert!(matches!(
        HandoffAck::from_frame(&handoff().frame(8), 8),
        Err(HandoffError::Kind { .. })
    ));
}

proptest! {
    #[test]
    fn decoding_any_body_never_panics_and_requests_round_trip(
        body in proptest::collection::vec(any::<u8>(), 0..96),
        request in any::<u64>(),
    ) {
        for kind in [MessageKind::Handoff, MessageKind::AdminReply] {
            let frame = Frame::new(
                Header::new(kind).with_request_id(request).with_body(body.clone()),
                Bytes::new(),
            );
            if let Ok(handoff) = Handoff::from_frame(&frame) {
                prop_assert_eq!(Handoff::from_frame(&handoff.frame(request)), Ok(handoff));
            }
            if let Ok(ack) = HandoffAck::from_frame(&frame, request) {
                prop_assert_eq!(HandoffAck::from_frame(&ack.frame(request), request), Ok(ack));
            }
        }
    }

    #[test]
    fn valid_requests_round_trip(
        bucket in "b-[a-z0-9]{1,20}",
        shard in any::<u8>(),
        epoch in 1..u64::MAX,
        to in 1..1000u32,
        request in any::<u64>(),
    ) {
        let handoff = Handoff {
            bucket: BucketId::new(bucket).unwrap(),
            shard: ShardId::new(shard),
            epoch: Epoch::new(epoch),
            to: NodeId::new(format!("node-{to}")).unwrap(),
        };
        prop_assert_eq!(Handoff::from_frame(&handoff.frame(request)), Ok(handoff));
    }
}

/// A sink that records what it is asked, and refuses an epoch other than
/// 42.
#[derive(Default)]
struct Recording {
    asked: Mutex<Vec<Handoff>>,
}

impl HandoffSink for Recording {
    fn hand_off(&self, handoff: Handoff) -> HandoffFuture<'_> {
        Box::pin(async move {
            let epoch = handoff.epoch;
            self.asked.lock().unwrap().push(handoff);
            if epoch == Epoch::new(42) {
                Ok(())
            } else {
                Err(format!("the replica is in epoch 42, not {epoch}"))
            }
        })
    }
}

fn address(addr: SocketAddr) -> NodeAddress {
    addr.to_string().parse().unwrap()
}

/// Node `n` serving admin messages on a loopback port, starting handoffs
/// through `sink` if one is given.
async fn serving(pki: &Pki, n: u8, sink: Option<Arc<Recording>>) -> NodeAddress {
    let listener: Listener<TokioNetwork> = pki
        .transport(&node(n))
        .bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr = address(listener.local_addr().unwrap());
    let mut endpoint = AdminEndpoint::new(ControlHints::new());
    if let Some(sink) = sink {
        endpoint = endpoint.with_handoffs(sink);
    }
    tokio::spawn(async move { endpoint.serve(listener).await });
    addr
}

#[tokio::test]
async fn the_coordinator_asks_a_primary_to_hand_a_shard_off() {
    let pki = Pki::new();
    let sink = Arc::new(Recording::default());
    let addr = serving(&pki, 1, Some(Arc::clone(&sink))).await;
    let client = HandoffClient::new(pki.transport(&node(9)), WAIT);
    assert!(format!("{client:?}").contains("HandoffClient"));

    client.send(&node(1), &addr, &handoff()).await.unwrap();
    // Through the trait, as rebalancing sends it.
    let stale = Handoff {
        epoch: Epoch::new(41),
        ..handoff()
    };
    let refused = client.request(node(1), addr.clone(), stale.clone()).await;
    assert_eq!(
        refused,
        Err(HandoffError::Refused(
            "the replica is in epoch 42, not 41".to_owned()
        ))
    );
    assert_eq!(*sink.asked.lock().unwrap(), vec![handoff(), stale]);

    // An operator tool may ask too.
    let tool = HandoffClient::new(pki.tool("ops"), WAIT);
    tool.send(&node(1), &addr, &handoff()).await.unwrap();
    assert_eq!(sink.asked.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn a_node_without_a_sink_refuses_handoffs() {
    let pki = Pki::new();
    let addr = serving(&pki, 1, None).await;
    let client = HandoffClient::new(pki.transport(&node(9)), WAIT);
    let refused = client.send(&node(1), &addr, &handoff()).await;
    assert!(
        matches!(&refused, Err(HandoffError::Refused(reason)) if reason.contains("no handoffs")),
        "{refused:?}"
    );
}

#[tokio::test]
async fn requests_go_to_the_admin_port_when_one_is_set() {
    let pki = Pki::new();
    let sink = Arc::new(Recording::default());
    let addr = serving(&pki, 1, Some(sink)).await;
    let port = NonZeroU16::new(addr.port()).unwrap();
    // The registered address names another port.
    let registered = NodeAddress::new(addr.host().clone(), NonZeroU16::new(1).unwrap());
    let client = HandoffClient::new(pki.transport(&node(9)), WAIT).with_admin_port(port);
    client
        .send(&node(1), &registered, &handoff())
        .await
        .unwrap();
}

#[tokio::test]
async fn a_request_nobody_answers_fails() {
    let pki = Pki::new();
    // Nothing listens there once the listener is gone.
    let listener: Listener<TokioNetwork> = pki
        .transport(&node(1))
        .bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr = address(listener.local_addr().unwrap());
    drop(listener);
    let client = HandoffClient::new(pki.transport(&node(9)), WAIT);
    let failed = client.send(&node(1), &addr, &handoff()).await;
    assert!(
        matches!(failed, Err(HandoffError::Unanswered(_))),
        "{failed:?}"
    );

    // A listener that never answers: the request times out.
    let silent: Listener<TokioNetwork> = pki
        .transport(&node(1))
        .bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr = address(silent.local_addr().unwrap());
    let client = HandoffClient::new(pki.transport(&node(9)), Duration::from_millis(200));
    let failed = client.send(&node(1), &addr, &handoff()).await;
    assert!(
        matches!(&failed, Err(HandoffError::Unanswered(reason)) if reason.contains("no answer")),
        "{failed:?}"
    );
    drop(silent);
}

#[tokio::test]
async fn a_malformed_request_ends_the_connection() {
    let pki = Pki::new();
    let sink = Arc::new(Recording::default());
    let listener: Listener<TokioNetwork> = pki
        .transport(&node(1))
        .bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr = address(listener.local_addr().unwrap());
    let endpoint = AdminEndpoint::new(ControlHints::new()).with_handoffs(sink.clone());
    let served = tokio::spawn(async move {
        let incoming = listener.accept().await.unwrap();
        let connection = incoming.handshake().await.unwrap();
        endpoint.serve_connection(connection).await
    });
    let mut connection = pki
        .transport(&node(9))
        .connect(&node(1), &addr)
        .await
        .unwrap();
    connection
        .send(&body_frame(&HandoffBody {
            epoch: 0,
            ..valid_body()
        }))
        .await
        .unwrap();
    let ended = tokio::time::timeout(WAIT, served).await.unwrap().unwrap();
    assert!(
        matches!(
            ended,
            Err(PushError::Handoff(HandoffError::Invalid {
                field: "epoch",
                ..
            }))
        ),
        "{ended:?}"
    );
    assert!(sink.asked.lock().unwrap().is_empty());
}
