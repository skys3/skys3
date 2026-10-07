//! The `EC_PUBLISH` body, byte for byte, and every way one is rejected.

mod support;

use skys3_log::record::{
    DecodeError, EcPublish, FieldError, LogRecord, MAX_STRIPES, Problem, RecordBody, RecordKind,
    ShardRef,
};
use skys3_types::{
    AttemptId, BucketId, CodecId, CodedStripe, ETag, Epoch, EpochSeq, FragmentId,
    FragmentLocation, Geometry, KeyHash, NodeId, Seq, ShardId,
};
use support::{Body, Frame};

const EC_PUBLISH: u16 = 14;

fn shard() -> ShardRef {
    ShardRef::new(BucketId::new("b1").unwrap(), ShardId::new(3))
}

fn position(epoch: u64, seq: u64) -> EpochSeq {
    EpochSeq::new(Epoch::new(epoch), Seq::new(seq))
}

fn record(body: RecordBody) -> LogRecord {
    LogRecord {
        shard: shard(),
        position: position(5, 9),
        body,
    }
}

fn malformed(bytes: &[u8]) -> FieldError {
    match LogRecord::decode(bytes).unwrap_err() {
        DecodeError::Malformed(error) => error,
        other => panic!("expected a malformed record, got {other:?}"),
    }
}

fn encode_err(publish: EcPublish) -> FieldError {
    let record = record(RecordBody::EcPublish(publish));
    let error = record.to_bytes().unwrap_err();
    assert_eq!(record.check().unwrap_err(), error);
    error.0
}

fn stripe(number: u32, offset: u64, data_len: u64, nodes: &[&str]) -> CodedStripe {
    let fragments = nodes
        .iter()
        .zip(1..)
        .map(|(node, id)| FragmentLocation {
            node: NodeId::new(*node).unwrap(),
            fragment: FragmentId::new(id << 64 | u128::from(number)),
        })
        .collect();
    CodedStripe::new(
        number,
        offset,
        data_len,
        Geometry::new(2, 1).unwrap(),
        CodecId::CURRENT,
        fragments,
    )
    .unwrap()
}

fn publish() -> EcPublish {
    EcPublish {
        key: "k".into(),
        version: position(5, 2),
        etag: ETag::new("e").unwrap(),
        attempt: AttemptId::new(Epoch::new(5), 7),
        size: 300,
        stripes: vec![
            stripe(0, 0, 200, &["n1", "n2", "n3"]),
            stripe(1, 200, 100, &["n2", "n3", "n4"]),
        ],
    }
}

/// The body of [`publish`], written from the documented layout.
fn body() -> Body {
    let mut body = Body::default()
        .str16("k")
        .u64(5)
        .u64(2) // the version's position
        .str16("e")
        .u64(5)
        .u64(7) // the attempt
        .u64(300)
        .u32(2); // stripes
    for (number, data_len, nodes) in [(0u128, 200, ["n1", "n2", "n3"]), (1, 100, ["n2", "n3", "n4"])]
    {
        body = body.u64(data_len).u8(2).u8(1).u16(1);
        for (node, id) in nodes.iter().zip(1u128..) {
            body = body.str8(node).raw(&(id << 64 | number).to_le_bytes());
        }
    }
    body
}

#[test]
fn ec_publish_records_follow_the_documented_layout() {
    assert!(RecordKind::EcPublish.is_defined());
    let expected = Frame::new(EC_PUBLISH).keyed("k").build(&body().0, &[]);
    let bytes = record(RecordBody::EcPublish(publish())).to_bytes().unwrap();
    assert_eq!(bytes, expected);
    let (decoded, len) = LogRecord::decode(&bytes).unwrap();
    assert_eq!(len, bytes.len());
    assert_eq!(decoded.key(), Some("k"));
    assert_eq!(decoded.key_hash(), Some(KeyHash::of(&shard().bucket, b"k")));
    assert_eq!(decoded.body, RecordBody::EcPublish(publish()));
}

#[test]
fn the_encoder_rejects_what_the_decoder_would() {
    let cases: Vec<(EcPublish, &str)> = vec![
        (
            EcPublish {
                key: String::new(),
                ..publish()
            },
            "ec_publish.key",
        ),
        (
            EcPublish {
                version: position(5, 9),
                ..publish()
            },
            "ec_publish.version",
        ),
        (
            EcPublish {
                stripes: Vec::new(),
                size: 0,
                ..publish()
            },
            "ec_publish.stripes",
        ),
        (
            EcPublish {
                size: 301,
                ..publish()
            },
            "ec_publish.size",
        ),
        (
            // Stripes out of order.
            EcPublish {
                stripes: publish().stripes.into_iter().rev().collect(),
                ..publish()
            },
            "ec_publish.stripes",
        ),
        (
            // A gap between the stripes.
            EcPublish {
                stripes: vec![
                    stripe(0, 0, 200, &["n1", "n2", "n3"]),
                    stripe(1, 201, 99, &["n2", "n3", "n4"]),
                ],
                ..publish()
            },
            "ec_publish.stripes",
        ),
        (
            EcPublish {
                stripes: vec![
                    CodedStripe::new(
                        0,
                        0,
                        300,
                        Geometry::new(2, 1).unwrap(),
                        CodecId::new(0),
                        stripe(0, 0, 300, &["n1", "n2", "n3"]).fragments().to_vec(),
                    )
                    .unwrap(),
                ],
                ..publish()
            },
            "ec_publish.codec",
        ),
    ];
    for (publish, field) in cases {
        assert_eq!(encode_err(publish).field, field);
    }
}

#[test]
fn malformed_bodies_are_rejected() {
    let build = |body: &Body| Frame::new(EC_PUBLISH).keyed("k").build(&body.0, &[]);
    let prefix = |stripes: u32| {
        Body::default()
            .str16("k")
            .u64(5)
            .u64(2)
            .str16("e")
            .u64(5)
            .u64(7)
            .u64(10)
            .u32(stripes)
    };
    let fragment = |body: Body, node: &str| body.str8(node).raw(&[0; 16]);

    // No stripe, too many, or more than the bytes can hold.
    assert_eq!(malformed(&build(&prefix(0))).problem, Problem::Empty);
    let too_many = u32::try_from(MAX_STRIPES + 1).unwrap();
    assert!(matches!(
        malformed(&build(&prefix(too_many))).problem,
        Problem::TooLong { .. }
    ));
    assert_eq!(malformed(&build(&prefix(1000))).problem, Problem::Truncated);

    // One stripe of 10 bytes in 1+1, then each way it can break.
    let good = fragment(fragment(prefix(1).u64(10).u8(1).u8(1).u16(1), "a"), "b");
    assert!(LogRecord::decode(&build(&good)).is_ok());
    let invalid = |body: Body, field: &str| {
        let error = malformed(&build(&body));
        assert_eq!(error.field, field, "{error}");
    };
    invalid(
        fragment(fragment(prefix(1).u64(10).u8(0).u8(1).u16(1), "a"), "b"),
        "ec_publish.geometry",
    );
    invalid(
        fragment(fragment(prefix(1).u64(10).u8(1).u8(1).u16(0), "a"), "b"),
        "ec_publish.codec",
    );
    invalid(
        fragment(fragment(prefix(1).u64(10).u8(1).u8(1).u16(1), "a"), "a"),
        "ec_publish.stripes",
    );
    invalid(
        fragment(fragment(prefix(1).u64(0).u8(1).u8(1).u16(1), "a"), "b"),
        "ec_publish.stripes",
    );
    invalid(
        fragment(fragment(prefix(1).u64(9).u8(1).u8(1).u16(1), "a"), "b"),
        "ec_publish.size",
    );
    invalid(
        fragment(fragment(prefix(1).u64(10).u8(1).u8(1).u16(1), "a"), "B"),
        "ec_publish.node",
    );
    invalid(
        fragment(fragment(prefix(1).u64(10).u8(2).u8(1).u16(1), "a"), "b"),
        "ec_publish.fragments",
    );
    invalid(good.u8(0), "body");
}
