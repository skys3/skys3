//! The `EC_RELOCATE` body, byte for byte, and every way one is rejected.

mod support;

use proptest::prelude::*;
use skys3_log::record::{
    DecodeError, EcRelocate, FieldError, FragmentMove, LogRecord, MAX_MOVES, Problem, RecordBody,
    RecordKind, ShardRef,
};
use skys3_types::{
    AttemptId, BucketId, ETag, Epoch, EpochSeq, FragmentId, FragmentLocation, KeyHash, NodeId, Seq,
    ShardId,
};
use support::{Body, Frame};

const EC_RELOCATE: u16 = 15;

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

fn location(node: &str, id: u128) -> FragmentLocation {
    FragmentLocation {
        node: NodeId::new(node).unwrap(),
        fragment: FragmentId::new(id),
    }
}

fn moved(stripe: u32, index: u8, from: &str, to: &str) -> FragmentMove {
    FragmentMove {
        stripe,
        index,
        from: location(from, u128::from(stripe) << 8 | u128::from(index)),
        to: location(to, 1 << 100 | u128::from(index)),
    }
}

fn relocate() -> EcRelocate {
    EcRelocate {
        key: "k".into(),
        version: position(5, 2),
        etag: ETag::new("e").unwrap(),
        attempt: AttemptId::new(Epoch::new(5), 8),
        moves: vec![moved(0, 3, "n1", "n6"), moved(2, 0, "n1", "n7")],
    }
}

/// The body of [`relocate`], written from the documented layout.
fn body() -> Body {
    let mut body = Body::default()
        .str16("k")
        .u64(5)
        .u64(2) // the version's position
        .str16("e")
        .u64(5)
        .u64(8) // the attempt
        .u32(2); // moves
    for (stripe, index, to) in [(0u32, 3u8, "n6"), (2, 0, "n7")] {
        let from = u128::from(stripe) << 8 | u128::from(index);
        body = body
            .u32(stripe)
            .u8(index)
            .str8("n1")
            .raw(&from.to_le_bytes())
            .str8(to)
            .raw(&(1u128 << 100 | u128::from(index)).to_le_bytes());
    }
    body
}

fn malformed(bytes: &[u8]) -> FieldError {
    match LogRecord::decode(bytes).unwrap_err() {
        DecodeError::Malformed(error) => error,
        other => panic!("expected a malformed record, got {other:?}"),
    }
}

#[test]
fn ec_relocate_records_follow_the_documented_layout() {
    assert!(RecordKind::EcRelocate.is_defined());
    assert_eq!(RecordKind::EcRelocate.code(), EC_RELOCATE);
    let expected = Frame::new(EC_RELOCATE).keyed("k").build(&body().0, &[]);
    let bytes = record(RecordBody::EcRelocate(relocate()))
        .to_bytes()
        .unwrap();
    assert_eq!(bytes, expected);
    let (decoded, len) = LogRecord::decode(&bytes).unwrap();
    assert_eq!(len, bytes.len());
    assert_eq!(decoded.key(), Some("k"));
    assert_eq!(decoded.key_hash(), Some(KeyHash::of(&shard().bucket, b"k")));
    assert_eq!(decoded.body, RecordBody::EcRelocate(relocate()));
}

#[test]
fn the_encoder_rejects_what_the_decoder_would() {
    let cases: Vec<(EcRelocate, &str)> = vec![
        (
            EcRelocate {
                key: String::new(),
                ..relocate()
            },
            "ec_relocate.key",
        ),
        (
            EcRelocate {
                version: position(5, 9),
                ..relocate()
            },
            "ec_relocate.version",
        ),
        (
            EcRelocate {
                moves: Vec::new(),
                ..relocate()
            },
            "ec_relocate.moves",
        ),
        (
            EcRelocate {
                moves: relocate().moves.into_iter().rev().collect(),
                ..relocate()
            },
            "ec_relocate.moves",
        ),
        (
            EcRelocate {
                moves: vec![moved(1, 1, "n1", "n2"), moved(1, 1, "n3", "n4")],
                ..relocate()
            },
            "ec_relocate.moves",
        ),
        (
            EcRelocate {
                moves: vec![FragmentMove {
                    to: location("n1", 1 << 8 | 1),
                    ..moved(1, 1, "n1", "n2")
                }],
                ..relocate()
            },
            "ec_relocate.to",
        ),
    ];
    for (relocate, field) in cases {
        let record = record(RecordBody::EcRelocate(relocate));
        let error = record.to_bytes().unwrap_err();
        assert_eq!(record.check().unwrap_err(), error);
        assert_eq!(error.0.field, field, "{error}");
    }
}

#[test]
fn malformed_bodies_are_rejected() {
    let build = |body: &Body| Frame::new(EC_RELOCATE).keyed("k").build(&body.0, &[]);
    let prefix = |moves: u32| {
        Body::default()
            .str16("k")
            .u64(5)
            .u64(2)
            .str16("e")
            .u64(5)
            .u64(8)
            .u32(moves)
    };
    let one = |body: Body, stripe: u32, from: &str, to: &str| {
        body.u32(stripe)
            .u8(0)
            .str8(from)
            .raw(&[0; 16])
            .str8(to)
            .raw(&[0; 16])
    };

    // No move, too many, or more than the bytes can hold.
    assert_eq!(malformed(&build(&prefix(0))).problem, Problem::Empty);
    let too_many = u32::try_from(MAX_MOVES + 1).unwrap();
    assert!(matches!(
        malformed(&build(&prefix(too_many))).problem,
        Problem::TooLong { .. }
    ));
    assert_eq!(malformed(&build(&prefix(1000))).problem, Problem::Truncated);

    let good = one(prefix(1), 0, "a", "b");
    assert!(LogRecord::decode(&build(&good)).is_ok());
    let invalid = |body: Body, field: &str| {
        let error = malformed(&build(&body));
        assert_eq!(error.field, field, "{error}");
    };
    invalid(one(prefix(1), 0, "a", "a"), "ec_relocate.to");
    invalid(one(prefix(1), 0, "A", "b"), "ec_relocate.from");
    invalid(one(prefix(1), 0, "a", "B"), "ec_relocate.to");
    invalid(
        one(one(prefix(2), 1, "a", "b"), 0, "a", "b"),
        "ec_relocate.moves",
    );
    // A version at the record's own position.
    let late = Body::default()
        .str16("k")
        .u64(5)
        .u64(9)
        .str16("e")
        .u64(5)
        .u64(8)
        .u32(1);
    invalid(one(late, 0, "a", "b"), "ec_relocate.version");
    invalid(good.u8(0), "body");
}

proptest! {
    #[test]
    fn every_relocation_round_trips(relocate in support::ec_relocate(position(5, 9))) {
        let record = record(RecordBody::EcRelocate(relocate));
        let bytes = record.to_bytes().unwrap();
        let (decoded, len) = LogRecord::decode(&bytes).unwrap();
        prop_assert_eq!(len, bytes.len());
        prop_assert_eq!(decoded, record);
    }
}
