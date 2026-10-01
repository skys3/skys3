//! The record layout, byte for byte, and every way a record is rejected.
//!
//! Records are persistent. If a golden test here fails, the change broke
//! the on-disk format: fix the code, or bump the format version, never the
//! vectors.

mod support;

use std::collections::BTreeMap;

use bytes::Bytes;
use skys3_log::record::{
    ChecksumAlgorithm, DecodeError, Delete, EncodeError, ErrorClass, Extent, ExtentRef,
    FORMAT_VERSION, FieldError, LogRecord, MAGIC, MAX_HEADER_LEN, MAX_PAYLOAD_LEN, MAX_TAGS,
    Problem, Put, PutData, RecordBody, RecordHeader, RecordKind, ShardRef,
};
use skys3_types::{
    BucketId, ETag, Epoch, EpochSeq, KeyHash, NodeId, ProposalId, Seq, ShardConfig, ShardId,
};
use support::{Body, Frame, reseal};

const PUT: u16 = 1;
const DELETE: u16 = 2;
const EXTENT: u16 = 3;
const CONFIG: u16 = 18;
const TRUNCATE: u16 = 17;

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

fn decode_err(bytes: &[u8]) -> DecodeError {
    LogRecord::decode(bytes).unwrap_err()
}

fn malformed(bytes: &[u8]) -> FieldError {
    match decode_err(bytes) {
        DecodeError::Malformed(error) => error,
        other => panic!("expected a malformed record, got {other:?}"),
    }
}

fn encode_err(body: RecordBody) -> FieldError {
    record(body).to_bytes().unwrap_err().0
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn inline_put(key: &str, data: &'static [u8]) -> Put {
    Put {
        key: key.into(),
        size: data.len() as u64,
        last_modified_ms: 1_700_000_000_000,
        etag: ETag::new("d41d8cd98f00b204e9800998ecf8427e").unwrap(),
        inherited_identity: None,
        metadata: BTreeMap::new(),
        tags: BTreeMap::new(),
        checksums: BTreeMap::new(),
        copy_source: None,
        data: PutData::Inline(Bytes::from_static(data)),
    }
}

/// The body of a minimal inline `PUT` of `k`, up to the data tag.
fn put_prefix() -> Body {
    Body::default()
        .str16("k")
        .u64(1)
        .u64(0)
        .str16("e")
        .u8(0) // no inherited identity
        .u16(0) // no metadata
        .u8(0) // no tags
        .u8(0) // no checksums
        .u8(0) // no copy source
}

// The layout.

#[test]
fn crc32c_is_the_castagnoli_crc() {
    // The standard check value of CRC-32C (iSCSI, RFC 3720).
    assert_eq!(crc32c::crc32c(b"123456789"), 0xe306_9283);
}

#[test]
fn delete_record_golden_bytes() {
    let record = LogRecord {
        shard: ShardRef::new(BucketId::new("b-7f3a").unwrap(), ShardId::new(7)),
        position: position(42, 1001),
        body: RecordBody::Delete(Delete {
            key: "photos/cat.jpg".into(),
        }),
    };
    let bytes = record.to_bytes().unwrap();
    assert_eq!(
        hex(&bytes),
        concat!(
            "534b594c",                                                         // magic "SKYL"
            "df5f015d", // CRC32C, checked with an independent bitwise implementation
            "0100",     // format version 1
            "0200",     // kind DELETE
            "60000000", // header_len 96
            "00000000", // payload_len 0
            "07",       // shard 7
            "06",       // bucket ID length
            "0000",     // reserved
            "2a00000000000000", // epoch 42
            "e903000000000000", // seq 1001
            "5730ceb8173a7db0", // key hash 0xb07d3a17b8ce3057
            "622d376633610000000000000000000000000000000000000000000000000000", // "b-7f3a"
            "0e00",     // key length 14
            "70686f746f732f6361742e6a7067", // "photos/cat.jpg"
        )
    );
}

#[test]
fn encoder_follows_the_documented_layout() {
    let put = inline_put("k", b"hi");
    let body = Body::default()
        .str16("k")
        .u64(2)
        .u64(1_700_000_000_000)
        .str16("d41d8cd98f00b204e9800998ecf8427e")
        .u8(0) // no inherited identity
        .u16(0) // no metadata
        .u8(0) // no tags
        .u8(0) // no checksums
        .u8(0) // no copy source
        .u8(0); // inline data
    let expected = Frame::new(PUT).keyed("k").build(&body.0, b"hi");
    assert_eq!(record(RecordBody::Put(put)).to_bytes().unwrap(), expected);

    let expected = Frame::new(TRUNCATE).build(&[], &[]);
    assert_eq!(record(RecordBody::Truncate).to_bytes().unwrap(), expected);
    assert_eq!(expected.len(), RecordHeader::LEN);
    assert_eq!(&expected[..4], &MAGIC);
    assert_eq!(FORMAT_VERSION, 1);
}

#[test]
fn record_kinds_have_fixed_codes_and_names() {
    let codes: Vec<u16> = RecordKind::ALL.iter().map(|k| k.code()).collect();
    assert_eq!(codes, (1..=18).collect::<Vec<_>>());
    for kind in RecordKind::ALL {
        assert_eq!(RecordKind::from_code(kind.code()), Some(kind));
        assert_eq!(kind.to_string(), kind.name());
    }
    assert_eq!(RecordKind::from_code(0), None);
    assert_eq!(RecordKind::from_code(19), None);
    let names: Vec<&str> = RecordKind::ALL.iter().map(|k| k.name()).collect();
    assert_eq!(
        names,
        [
            "PUT",
            "DELETE",
            "EXTENT",
            "MPU_CREATE",
            "MPU_PART",
            "MPU_COMPLETE",
            "MPU_ABORT",
            "UPLOAD_BEGIN",
            "FLUSHED",
            "PART_FLUSHED",
            "TAGS",
            "IMPORT",
            "ADOPT",
            "EC_PUBLISH",
            "EC_RELOCATE",
            "EC_RELEASE",
            "TRUNCATE",
            "CONFIG",
        ]
    );
    let defined: Vec<&str> = RecordKind::ALL
        .iter()
        .filter(|k| k.is_defined())
        .map(|k| k.name())
        .collect();
    assert_eq!(
        defined,
        [
            "PUT", "DELETE", "EXTENT", "FLUSHED", "TAGS", "IMPORT", "ADOPT", "TRUNCATE", "CONFIG"
        ]
    );
    assert!(!RecordKind::Config.has_key() && !RecordKind::Truncate.has_key());
    assert!(RecordKind::MpuPart.has_key());
}

#[test]
fn checksum_algorithms_have_fixed_codes() {
    for algorithm in ChecksumAlgorithm::ALL {
        assert_eq!(
            ChecksumAlgorithm::from_code(algorithm.code()),
            Some(algorithm)
        );
    }
    assert_eq!(ChecksumAlgorithm::from_code(0), None);
    let lens: Vec<usize> = ChecksumAlgorithm::ALL
        .iter()
        .map(|a| a.digest_len())
        .collect();
    assert_eq!(lens, [4, 4, 8, 20, 32, 16]);
}

#[test]
fn shard_refs_display_as_register_paths() {
    assert_eq!(shard().to_string(), "b1/3");
}

// Framing errors.

#[test]
fn framing_is_checked_before_the_rest() {
    let good = Frame::new(TRUNCATE).build(&[], &[]);

    assert_eq!(decode_err(b"SKX"), DecodeError::BadMagic);
    assert_eq!(decode_err(&[0; 80]), DecodeError::BadMagic);
    assert_eq!(
        decode_err(b"SK"),
        DecodeError::Incomplete {
            needed: 80,
            available: 2
        }
    );

    let mut bytes = good.clone();
    bytes[8] = 2;
    assert_eq!(decode_err(&bytes), DecodeError::UnsupportedVersion(2));

    for (at, value, field, min, max) in [
        (12, 79, "header_len", 80, MAX_HEADER_LEN),
        (12, MAX_HEADER_LEN + 1, "header_len", 80, MAX_HEADER_LEN),
        (16, MAX_PAYLOAD_LEN + 1, "payload_len", 0, MAX_PAYLOAD_LEN),
    ] {
        let mut bytes = good.clone();
        bytes[at..at + 4].copy_from_slice(&value.to_le_bytes());
        assert_eq!(
            decode_err(&bytes),
            DecodeError::FrameLength {
                field,
                len: value,
                min,
                max
            }
        );
    }

    let mut bytes = good.clone();
    bytes[16] = 1; // one payload byte that the buffer does not hold
    assert_eq!(
        decode_err(&bytes),
        DecodeError::Incomplete {
            needed: 81,
            available: 80
        }
    );
    assert_eq!(RecordHeader::peek_len(&bytes), Ok(81));

    let mut bytes = good.clone();
    bytes[30] ^= 1;
    let DecodeError::ChecksumMismatch { stored, computed } = decode_err(&bytes) else {
        panic!("expected a checksum mismatch");
    };
    assert_ne!(stored, computed);
}

#[test]
fn unknown_and_reserved_kinds_are_rejected() {
    for code in [0, 19, u16::MAX] {
        let bytes = Frame::new(code).build(&[], &[]);
        assert_eq!(decode_err(&bytes), DecodeError::UnknownKind(code));
    }
    for kind in RecordKind::ALL.into_iter().filter(|k| !k.is_defined()) {
        // Whatever the body holds, a reserved kind is never parsed.
        let bytes = Frame::new(kind.code()).keyed("k").build(b"anything", b"x");
        let error = decode_err(&bytes);
        assert_eq!(error, DecodeError::UnsupportedKind(kind));
        assert_eq!(error.class(), ErrorClass::Unsupported);
        assert!(error.to_string().contains("not yet supported"), "{error}");
    }
}

#[test]
fn fixed_header_fields_are_validated() {
    let good = Frame::new(TRUNCATE).build(&[], &[]);
    let field = |mut bytes: Vec<u8>, at: usize, value: u8| {
        bytes[at] = value;
        reseal(&mut bytes);
        malformed(&bytes)
    };
    assert_eq!(
        field(good.clone(), 22, 1),
        FieldError {
            field: "header.reserved",
            problem: Problem::NonZeroReserved
        }
    );
    assert_eq!(
        field(good.clone(), 21, 26).problem,
        Problem::TooLong { len: 26, max: 25 }
    );
    // With a shorter length, the rest of the ID counts as padding.
    assert_eq!(field(good.clone(), 21, 1).problem, Problem::NonZeroReserved);
    let mut empty = good.clone();
    empty[48..50].fill(0);
    assert_eq!(field(empty.clone(), 21, 0).field, "header.bucket_id");
    assert!(matches!(field(empty, 21, 0).problem, Problem::Invalid(_)));
    assert_eq!(field(good.clone(), 79, 1).problem, Problem::NonZeroReserved);
    assert!(matches!(
        field(good.clone(), 48, b'B').problem,
        Problem::Invalid(_)
    ));
    assert_eq!(field(good.clone(), 48, 0xff).problem, Problem::NotUtf8);
    assert_eq!(
        field(good.clone(), 40, 1),
        FieldError {
            field: "header.key_hash",
            problem: Problem::Inconsistent("a record without a key has a key hash")
        }
    );

    // A keyed record must carry its key's hash.
    let body = Body::default().str16("k");
    let bytes = Frame::new(DELETE).keyed("other").build(&body.0, &[]);
    assert_eq!(malformed(&bytes).field, "header.key_hash");
    let bytes = Frame::new(DELETE).keyed("k").build(&body.0, &[]);
    assert!(LogRecord::decode(&bytes).is_ok());
}

// Kind-specific headers.

#[test]
fn bodies_use_exactly_their_bytes() {
    let delete = |body: Body, payload: &[u8]| {
        malformed(&Frame::new(DELETE).keyed("k").build(&body.0, payload))
    };
    assert_eq!(
        delete(Body::default().str16("k").u8(0), &[]),
        FieldError {
            field: "body",
            problem: Problem::TrailingBytes(1)
        }
    );
    assert_eq!(
        delete(Body::default().u16(5).raw(b"k"), &[]).problem,
        Problem::Truncated
    );
    assert_eq!(
        delete(Body::default().u8(1), &[]).problem,
        Problem::Truncated
    );
    assert_eq!(
        delete(Body::default().str16(""), &[]).problem,
        Problem::Empty
    );
    assert_eq!(
        delete(Body::default().u16(1025).raw(&[b'a'; 1025]), &[]).problem,
        Problem::TooLong {
            len: 1025,
            max: 1024
        }
    );
    assert_eq!(
        delete(Body::default().u16(1).u8(0xff), &[]).problem,
        Problem::NotUtf8
    );
    assert_eq!(
        delete(Body::default().str16("k"), b"x"),
        FieldError {
            field: "payload",
            problem: Problem::Inconsistent("the payload length does not match the body")
        }
    );
    let truncate = Frame::new(TRUNCATE).build(&[], b"x");
    assert_eq!(malformed(&truncate).field, "payload");
    let truncate = Frame::new(TRUNCATE).build(&[0], &[]);
    assert_eq!(malformed(&truncate).problem, Problem::TrailingBytes(1));
}

#[test]
fn put_fields_are_validated() {
    let put = |body: Body, payload: &[u8]| {
        LogRecord::decode(&Frame::new(PUT).keyed("k").build(&body.0, payload))
    };
    let err = |body: Body, payload: &[u8]| match put(body, payload).unwrap_err() {
        DecodeError::Malformed(error) => error,
        other => panic!("{other:?}"),
    };
    assert!(put(put_prefix().u8(0), b"x").is_ok());
    assert_eq!(
        err(put_prefix().u8(0), b"xy"),
        FieldError {
            field: "put.data",
            problem: Problem::Inconsistent("the data length differs from the object size")
        }
    );
    assert_eq!(
        err(put_prefix().u8(2), b"x").problem,
        Problem::InvalidTag(2)
    );
    assert_eq!(err(put_prefix().u8(1).u32(0), &[]).problem, Problem::Empty);
    // A count the bytes cannot back is rejected before anything is
    // reserved for it.
    assert_eq!(
        err(put_prefix().u8(1).u32(81_920), &[]),
        FieldError {
            field: "put.extents",
            problem: Problem::Truncated
        }
    );
    assert_eq!(
        err(put_prefix().u8(1).u32(81_921), &[]).problem,
        Problem::TooLong {
            len: 81_921,
            max: 81_920
        }
    );
    let extent = |body: Body, epoch: u64, seq: u64, len: u32| body.u64(epoch).u64(seq).u32(len);
    assert!(put(extent(put_prefix().u8(1).u32(1), 5, 8, 1), &[]).is_ok());
    assert!(matches!(
        err(extent(put_prefix().u8(1).u32(1), 5, 8, 0), &[]).problem,
        Problem::Invalid(_)
    ));
    assert!(matches!(
        err(
            extent(put_prefix().u8(1).u32(1), 5, 8, MAX_PAYLOAD_LEN + 1),
            &[]
        )
        .problem,
        Problem::Invalid(_)
    ));
    assert_eq!(
        err(extent(put_prefix().u8(1).u32(1), 5, 9, 1), &[]).problem,
        Problem::Inconsistent("names a position at or after the record's own")
    );
    assert_eq!(
        err(extent(put_prefix().u8(1).u32(1), 4, 0, 2), &[]).field,
        "put.extents"
    );
    assert_eq!(
        err(put_prefix().u8(1).u32(1), &[]).problem,
        Problem::Truncated
    );
}

#[test]
fn put_maps_must_be_canonical() {
    let fields = |metadata: Body, tags: Body, checksums: Body| {
        Body::default()
            .str16("k")
            .u64(0)
            .u64(0)
            .str16("e")
            .u8(0)
            .raw(&metadata.0)
            .raw(&tags.0)
            .raw(&checksums.0)
            .u8(0)
            .u8(0)
    };
    let err = |body: Body| malformed(&Frame::new(PUT).keyed("k").build(&body.0, &[]));
    let none8 = || Body::default().u8(0);
    let none16 = || Body::default().u16(0);

    let unsorted = Body::default()
        .u16(2)
        .str16("b")
        .str16("")
        .str16("a")
        .str16("");
    assert_eq!(
        err(fields(unsorted, none8(), none8())),
        FieldError {
            field: "put.metadata",
            problem: Problem::Unsorted
        }
    );
    let repeated = Body::default()
        .u16(2)
        .str16("a")
        .str16("")
        .str16("a")
        .str16("");
    assert_eq!(
        err(fields(repeated, none8(), none8())).problem,
        Problem::Unsorted
    );
    let upper = Body::default().u16(1).str16("Content-Type").str16("x");
    assert!(matches!(
        err(fields(upper, none8(), none8())).problem,
        Problem::Invalid(_)
    ));
    let big = "v".repeat(5000);
    let too_big = Body::default()
        .u16(2)
        .str16("a")
        .str16(&big)
        .str16("b")
        .str16(&big);
    assert_eq!(
        err(fields(too_big, none8(), none8())).problem,
        Problem::TooLong {
            len: 10_002,
            max: 8192
        }
    );

    let tags = Body::default()
        .u8(2)
        .str16("y")
        .str16("1")
        .str16("x")
        .str16("2");
    assert_eq!(err(fields(none16(), tags, none8())).field, "put.tags");
    let too_many = u8::try_from(MAX_TAGS + 1).unwrap();
    assert_eq!(
        err(fields(none16(), Body::default().u8(too_many), none8())).problem,
        Problem::TooLong { len: 51, max: 50 }
    );
    let empty_key = Body::default().u8(1).str16("").str16("v");
    assert_eq!(
        err(fields(none16(), empty_key, none8())).problem,
        Problem::Empty
    );

    let crc32 = [1, 2, 3, 4];
    let checksums = Body::default().u8(2).u8(2).raw(&crc32).u8(1).raw(&crc32);
    assert_eq!(
        err(fields(none16(), none8(), checksums)).problem,
        Problem::Unsorted
    );
    let unknown = Body::default().u8(1).u8(7).raw(&crc32);
    assert_eq!(
        err(fields(none16(), none8(), unknown)).problem,
        Problem::InvalidTag(7)
    );
    let short = Body::default().u8(1).u8(5).raw(&crc32);
    assert_eq!(
        err(fields(none16(), none8(), short)).problem,
        Problem::Truncated
    );
}

#[test]
fn put_options_and_references_are_validated() {
    let err = |body: Body| malformed(&Frame::new(PUT).keyed("k").build(&body.0, &[]));
    let head = || Body::default().str16("k").u64(0).u64(0).str16("e");
    let tail = |body: Body| body.u16(0).u8(0).u8(0);

    assert_eq!(
        err(tail(head().u8(2))),
        FieldError {
            field: "put.inherited_identity",
            problem: Problem::InvalidTag(2)
        }
    );
    assert_eq!(
        err(tail(head().u8(1).u64(5).u64(10))).problem,
        Problem::Inconsistent("names a position at or after the record's own")
    );
    let identity = tail(head().u8(1).u64(5).u64(8)).u8(0).u8(0);
    assert!(LogRecord::decode(&Frame::new(PUT).keyed("k").build(&identity.0, &[])).is_ok());

    let source = |bucket: &str, etag: &str| {
        tail(head().u8(0))
            .u8(1)
            .str8(bucket)
            .str16("src")
            .u64(1)
            .str16(etag)
            .u8(0)
            .u8(0)
    };
    assert!(
        LogRecord::decode(&Frame::new(PUT).keyed("k").build(&source("b2", "e").0, &[])).is_ok()
    );
    assert_eq!(err(source("B2", "e")).field, "put.copy_source.bucket");
    assert_eq!(err(source("b2", "a b")).field, "put.copy_source.etag");
    assert!(matches!(
        err(source("b2", "a b")).problem,
        Problem::Invalid(_)
    ));
    assert_eq!(err(head().u8(0)).problem, Problem::Truncated);

    let bad_etag = Body::default().str16("k").u64(0).u64(0).str16("\"");
    assert_eq!(err(bad_etag).field, "put.etag");
}

#[test]
fn extent_records_carry_their_data() {
    let body = |offset: u64| Body::default().str16("k").u64(offset);
    let bytes = Frame::new(EXTENT).keyed("k").build(&body(0).0, b"data");
    let (record, _) = LogRecord::decode(&bytes).unwrap();
    assert_eq!(record.body.payload(), b"data");
    assert_eq!(record.key(), Some("k"));

    let empty = Frame::new(EXTENT).keyed("k").build(&body(0).0, &[]);
    assert_eq!(
        malformed(&empty),
        FieldError {
            field: "extent.data",
            problem: Problem::Empty
        }
    );
    let past_end = Frame::new(EXTENT).keyed("k").build(&body(u64::MAX).0, b"x");
    assert_eq!(malformed(&past_end).field, "extent.offset");
}

fn config_body(primary: &str, members: &[&str], proposal: &str) -> Body {
    let mut body = Body::default()
        .str8(primary)
        .u8(u8::try_from(members.len()).unwrap());
    for member in members {
        body = body.str8(member);
    }
    body.u8(0).u8(1).u8(1).str8(proposal)
}

#[test]
fn config_records_hold_a_valid_configuration() {
    let build = |body: Body| Frame::new(CONFIG).build(&body.0, &[]);
    let (record, _) = LogRecord::decode(&build(config_body("n1", &["n1"], "p1"))).unwrap();
    let RecordBody::Config(config) = &record.body else {
        panic!("expected a configuration");
    };
    assert_eq!(config.bucket_id.as_str(), "b1");
    assert_eq!(config.shard, ShardId::new(3));
    assert_eq!(config.epoch, Epoch::new(5));
    assert_eq!(record.key_hash(), None);

    assert_eq!(
        malformed(&build(config_body("n1", &[], "p1"))),
        FieldError {
            field: "config.members",
            problem: Problem::Empty
        }
    );
    assert_eq!(
        malformed(&build(config_body("N1", &["n1"], "p1"))).field,
        "config.primary"
    );
    let not_member = malformed(&build(config_body("n2", &["n1"], "p1")));
    assert_eq!(not_member.field, "config");
    assert!(
        not_member
            .to_string()
            .contains("primary n2 is not a member"),
        "{not_member}"
    );
    assert_eq!(
        malformed(&build(config_body("n1", &["n1"], "p 1"))).field,
        "config.proposal_id"
    );
}

#[test]
fn optional_text_fields_are_bounded() {
    // FLUSHED with an empty remote version ID.
    let body = Body::default().str16("k").u64(1).u8(0).u8(1).str16("");
    let bytes = Frame::new(9).keyed("k").build(&body.0, &[]);
    assert_eq!(
        malformed(&bytes),
        FieldError {
            field: "flushed.remote_version_id",
            problem: Problem::Empty
        }
    );
    // IMPORT with a storage class over the limit.
    let class = "S".repeat(65);
    let body = Body::default()
        .str16("k")
        .u64(1)
        .u64(2)
        .str16("e")
        .u8(1)
        .str16(&class);
    let bytes = Frame::new(12).keyed("k").build(&body.0, &[]);
    assert_eq!(
        malformed(&bytes).problem,
        Problem::TooLong { len: 65, max: 64 }
    );
}

// Encoding errors.

#[test]
fn the_encoder_rejects_what_the_decoder_would() {
    let mut put = inline_put("", b"");
    assert_eq!(
        encode_err(RecordBody::Put(put.clone())),
        FieldError {
            field: "put.key",
            problem: Problem::Empty
        }
    );
    put.key = "k".into();
    put.size = 1;
    assert_eq!(encode_err(RecordBody::Put(put.clone())).field, "put.data");
    put.data = PutData::Extents(Vec::new());
    assert_eq!(
        encode_err(RecordBody::Put(put.clone())).problem,
        Problem::Empty
    );
    put.data = PutData::Extents(vec![ExtentRef {
        position: position(5, 9),
        len: 1,
    }]);
    assert_eq!(
        encode_err(RecordBody::Put(put.clone())).field,
        "put.extents"
    );
    put.data = PutData::Extents(vec![ExtentRef {
        position: position(5, 8),
        len: 2,
    }]);
    assert_eq!(
        encode_err(RecordBody::Put(put.clone())).field,
        "put.extents"
    );
    put.size = 2;
    assert!(record(RecordBody::Put(put.clone())).to_bytes().is_ok());

    let mut bad = put.clone();
    bad.inherited_identity = Some(position(6, 0));
    assert_eq!(
        encode_err(RecordBody::Put(bad)).field,
        "put.inherited_identity"
    );
    let mut bad = put.clone();
    bad.metadata.insert("X-Upper".into(), String::new());
    assert!(matches!(
        encode_err(RecordBody::Put(bad)).problem,
        Problem::Invalid(_)
    ));
    let mut bad = put.clone();
    bad.checksums.insert(ChecksumAlgorithm::Sha256, vec![0; 4]);
    assert_eq!(encode_err(RecordBody::Put(bad)).field, "put.checksums");
    let mut bad = put.clone();
    bad.tags = (0..=MAX_TAGS)
        .map(|i| (i.to_string(), String::new()))
        .collect();
    assert_eq!(encode_err(RecordBody::Put(bad)).field, "put.tags");

    let extent = Extent {
        key: "k".into(),
        offset: 0,
        data: Bytes::new(),
    };
    assert_eq!(encode_err(RecordBody::Extent(extent)).field, "extent.data");
    let huge = Extent {
        key: "k".into(),
        offset: 0,
        data: vec![0; MAX_PAYLOAD_LEN as usize + 1].into(),
    };
    assert_eq!(
        encode_err(RecordBody::Extent(huge)),
        FieldError {
            field: "payload_len",
            problem: Problem::TooLong {
                len: u64::from(MAX_PAYLOAD_LEN) + 1,
                max: u64::from(MAX_PAYLOAD_LEN)
            }
        }
    );
}

fn config() -> ShardConfig {
    ShardConfig {
        bucket_id: BucketId::new("b1").unwrap(),
        shard: ShardId::new(3),
        epoch: Epoch::new(5),
        primary: NodeId::new("n1").unwrap(),
        members: vec![NodeId::new("n1").unwrap()],
        learners: vec![],
        min_write_replicas: 1,
        replicas: 1,
        proposal_id: ProposalId::new("p1").unwrap(),
    }
}

#[test]
fn config_records_must_match_their_header() {
    let bytes = record(RecordBody::Config(config())).to_bytes().unwrap();
    assert_eq!(
        bytes,
        Frame::new(CONFIG).build(&config_body("n1", &["n1"], "p1").0, &[])
    );

    let mut other = config();
    other.shard = ShardId::new(4);
    assert_eq!(encode_err(RecordBody::Config(other)).field, "config.shard");
    let mut other = config();
    other.bucket_id = BucketId::new("b2").unwrap();
    assert_eq!(encode_err(RecordBody::Config(other)).field, "config.shard");
    let mut other = config();
    other.epoch = Epoch::new(6);
    assert_eq!(encode_err(RecordBody::Config(other)).field, "config.epoch");
    let mut other = config();
    other.primary = NodeId::new("n2").unwrap();
    assert_eq!(encode_err(RecordBody::Config(other)).field, "config");
}

#[test]
fn a_failed_encode_leaves_the_buffer_unchanged() {
    let mut out = b"earlier records".to_vec();
    let error = record(RecordBody::Delete(Delete { key: String::new() }))
        .encode(&mut out)
        .unwrap_err();
    assert_eq!(out, b"earlier records");
    assert_eq!(
        error,
        EncodeError(FieldError {
            field: "delete.key",
            problem: Problem::Empty
        })
    );
    assert_eq!(
        error.to_string(),
        "cannot encode log record: delete.key: is empty"
    );
}

// Error reporting.

#[test]
fn errors_are_classified_and_described() {
    let cases = [
        (
            DecodeError::Incomplete {
                needed: 80,
                available: 3,
            },
            ErrorClass::Incomplete,
            "the record needs 80 bytes; the buffer holds 3",
        ),
        (
            DecodeError::BadMagic,
            ErrorClass::Corrupt,
            "the bytes do not start with the log record magic",
        ),
        (
            DecodeError::FrameLength {
                field: "payload_len",
                len: 9,
                min: 0,
                max: 8,
            },
            ErrorClass::Corrupt,
            "payload_len is 9; it must be from 0 to 8",
        ),
        (
            DecodeError::ChecksumMismatch {
                stored: 1,
                computed: 2,
            },
            ErrorClass::Corrupt,
            "CRC32C mismatch: the record stores 0x00000001, its bytes give 0x00000002",
        ),
        (
            DecodeError::UnsupportedVersion(2),
            ErrorClass::Unsupported,
            "log format version 2 is not supported; this build reads version 1",
        ),
        (
            DecodeError::UnknownKind(99),
            ErrorClass::Unsupported,
            "unknown log record kind 99",
        ),
        (
            DecodeError::UnsupportedKind(RecordKind::MpuCreate),
            ErrorClass::Unsupported,
            "log record kind MPU_CREATE is reserved but not yet supported",
        ),
        (
            DecodeError::Malformed(FieldError {
                field: "put.key",
                problem: Problem::NotUtf8,
            }),
            ErrorClass::Invalid,
            "malformed log record: put.key: is not UTF-8",
        ),
    ];
    for (error, class, text) in cases {
        assert_eq!(error.class(), class, "{error:?}");
        assert_eq!(error.to_string(), text);
    }

    let problems = [
        (Problem::Truncated, "runs past the end of the record header"),
        (
            Problem::TooLong { len: 3, max: 2 },
            "is 3 long; the limit is 2",
        ),
        (Problem::Empty, "is empty"),
        (Problem::InvalidTag(9), "has invalid tag byte 9"),
        (Problem::NotUtf8, "is not UTF-8"),
        (
            Problem::Unsorted,
            "entries are not in strictly increasing order",
        ),
        (Problem::NonZeroReserved, "reserved bytes are not zero"),
        (
            Problem::TrailingBytes(4),
            "is followed by 4 unexpected bytes",
        ),
        (Problem::Invalid("bad".into()), "is invalid: bad"),
        (Problem::Inconsistent("odd"), "is inconsistent: odd"),
    ];
    for (problem, text) in problems {
        assert_eq!(problem.to_string(), text);
    }
}

#[test]
fn records_expose_their_key_and_hash() {
    let record = record(RecordBody::Delete(Delete { key: "k".into() }));
    assert_eq!(record.kind(), RecordKind::Delete);
    assert_eq!(record.key(), Some("k"));
    assert_eq!(
        record.key_hash(),
        Some(KeyHash::of(&BucketId::new("b1").unwrap(), b"k"))
    );
    assert!(record.body.payload().is_empty());
}
