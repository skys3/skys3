//! The `EC_RELEASE` body, byte for byte, and every way one is rejected.

mod support;

use proptest::prelude::*;
use skys3_log::record::{
    DecodeError, EcRelease, FieldError, LogRecord, MAX_RELEASED, Problem, RecordBody, RecordKind,
    ShardRef,
};
use skys3_types::{
    BucketId, Epoch, EpochSeq, FragmentId, FragmentLocation, KeyHash, NodeId, Seq, ShardId,
};
use support::{Body, Frame};

const EC_RELEASE: u16 = 16;

fn shard() -> ShardRef {
    ShardRef::new(BucketId::new("b1").unwrap(), ShardId::new(3))
}

fn record(body: RecordBody) -> LogRecord {
    LogRecord {
        shard: shard(),
        position: EpochSeq::new(Epoch::new(5), Seq::new(9)),
        body,
    }
}

fn location(node: &str, id: u128) -> FragmentLocation {
    FragmentLocation {
        node: NodeId::new(node).unwrap(),
        fragment: FragmentId::new(id),
    }
}

fn release() -> EcRelease {
    EcRelease {
        key: "k".into(),
        fragments: vec![
            location("n1", 7),
            location("n1", 1 << 100),
            location("n2", 3),
        ],
    }
}

fn malformed(bytes: &[u8]) -> FieldError {
    match LogRecord::decode(bytes).unwrap_err() {
        DecodeError::Malformed(error) => error,
        other => panic!("expected a malformed record, got {other:?}"),
    }
}

#[test]
fn ec_release_records_follow_the_documented_layout() {
    assert!(RecordKind::EcRelease.is_defined());
    assert!(RecordKind::EcRelease.has_key());
    assert_eq!(RecordKind::EcRelease.code(), EC_RELEASE);
    let body = Body::default()
        .str16("k")
        .u32(3)
        .str8("n1")
        .raw(&7u128.to_le_bytes())
        .str8("n1")
        .raw(&(1u128 << 100).to_le_bytes())
        .str8("n2")
        .raw(&3u128.to_le_bytes());
    let expected = Frame::new(EC_RELEASE).keyed("k").build(&body.0, &[]);
    let bytes = record(RecordBody::EcRelease(release())).to_bytes().unwrap();
    assert_eq!(bytes, expected);
    let (decoded, len) = LogRecord::decode(&bytes).unwrap();
    assert_eq!(len, bytes.len());
    assert_eq!(decoded.key(), Some("k"));
    assert_eq!(decoded.key_hash(), Some(KeyHash::of(&shard().bucket, b"k")));
    assert_eq!(decoded.body, RecordBody::EcRelease(release()));
}

#[test]
fn the_encoder_rejects_what_the_decoder_would() {
    let cases: Vec<(EcRelease, &str)> = vec![
        (
            EcRelease {
                key: String::new(),
                ..release()
            },
            "ec_release.key",
        ),
        (
            EcRelease {
                fragments: Vec::new(),
                ..release()
            },
            "ec_release.fragments",
        ),
        (
            EcRelease {
                fragments: release().fragments.into_iter().rev().collect(),
                ..release()
            },
            "ec_release.fragments",
        ),
        (
            EcRelease {
                fragments: vec![location("n1", 7), location("n1", 7)],
                ..release()
            },
            "ec_release.fragments",
        ),
    ];
    for (release, field) in cases {
        let record = record(RecordBody::EcRelease(release));
        let error = record.to_bytes().unwrap_err();
        assert_eq!(record.check().unwrap_err(), error);
        assert_eq!(error.0.field, field, "{error}");
    }
}

#[test]
fn malformed_bodies_are_rejected() {
    let build = |body: &Body| Frame::new(EC_RELEASE).keyed("k").build(&body.0, &[]);
    let prefix = |count: u32| Body::default().str16("k").u32(count);
    let one = |body: Body, node: &str, id: u128| body.str8(node).raw(&id.to_le_bytes());

    // No fragment, too many, or more than the bytes can hold.
    assert_eq!(malformed(&build(&prefix(0))).problem, Problem::Empty);
    let too_many = u32::try_from(MAX_RELEASED + 1).unwrap();
    assert!(matches!(
        malformed(&build(&prefix(too_many))).problem,
        Problem::TooLong { .. }
    ));
    assert_eq!(malformed(&build(&prefix(1000))).problem, Problem::Truncated);

    let good = one(prefix(1), "a", 1);
    assert!(LogRecord::decode(&build(&good)).is_ok());
    let invalid = |body: Body, field: &str| {
        let error = malformed(&build(&body));
        assert_eq!(error.field, field, "{error}");
    };
    invalid(one(prefix(1), "A", 1), "ec_release.node");
    invalid(one(one(prefix(2), "b", 1), "a", 1), "ec_release.fragments");
    invalid(one(one(prefix(2), "a", 2), "a", 1), "ec_release.fragments");
    invalid(one(one(prefix(2), "a", 1), "a", 1), "ec_release.fragments");
    invalid(Body::default().str16("").u32(1), "ec_release.key");
    invalid(good.u8(0), "body");
}

proptest! {
    #[test]
    fn every_release_round_trips(release in support::ec_release()) {
        let record = record(RecordBody::EcRelease(release));
        let bytes = record.to_bytes().unwrap();
        let (decoded, len) = LogRecord::decode(&bytes).unwrap();
        prop_assert_eq!(len, bytes.len());
        prop_assert_eq!(decoded, record);
    }
}
