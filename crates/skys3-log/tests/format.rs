//! The record layout, byte for byte, and every way a record is rejected.
//!
//! Records are persistent. If a golden test here fails, the change broke
//! the on-disk format: fix the code, or bump the format version, never the
//! vectors.

mod support;

use std::collections::BTreeMap;

use bytes::Bytes;
use skys3_log::record::{
    Checksum, ChecksumAlgorithm, ChecksumType, CompletedPart, DecodeError, Delete, EncodeError,
    ErrorClass, Extent, ExtentRef, FORMAT_VERSION, FieldError, LogRecord, MAGIC, MAX_HEADER_LEN,
    MAX_PAYLOAD_LEN, MAX_TAGS, MIN_FORMAT_VERSION, MpuAbort, MpuComplete, MpuCreate, MpuPart,
    PartFlushed, Problem, Put, PutData, RecordBody, RecordHeader, RecordKind, RemoteStep, ShardRef,
    UploadBegin, UploadChecksum,
};
use skys3_types::{
    BucketId, ETag, Epoch, EpochSeq, KeyHash, NodeId, ProposalId, Seq, ShardConfig, ShardId,
};
use support::{Body, Frame, reseal};

const PUT: u16 = 1;
const DELETE: u16 = 2;
const EXTENT: u16 = 3;
const MPU_CREATE: u16 = 4;
const MPU_PART: u16 = 5;
const MPU_COMPLETE: u16 = 6;
const MPU_ABORT: u16 = 7;
const UPLOAD_BEGIN: u16 = 8;
const PART_FLUSHED: u16 = 10;
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

/// Returns why `body` does not encode, checking that `check` refuses it
/// for the same reason.
fn encode_err(body: RecordBody) -> FieldError {
    let record = record(body);
    let error = record.to_bytes().unwrap_err();
    assert_eq!(record.check().unwrap_err(), error);
    error.0
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

/// A `DELETE` record in format version 1, before checksums had part counts.
const DELETE_V1: &str = concat!(
    "534b594c",                                                         // magic "SKYL"
    "df5f015d",         // CRC32C, checked with an independent bitwise implementation
    "0100",             // format version 1
    "0200",             // kind DELETE
    "60000000",         // header_len 96
    "00000000",         // payload_len 0
    "07",               // shard 7
    "06",               // bucket ID length
    "0000",             // reserved
    "2a00000000000000", // epoch 42
    "e903000000000000", // seq 1001
    "5730ceb8173a7db0", // key hash 0xb07d3a17b8ce3057
    "622d376633610000000000000000000000000000000000000000000000000000", // "b-7f3a"
    "0e00",             // key length 14
    "70686f746f732f6361742e6a7067", // "photos/cat.jpg"
);

fn unhex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}

#[test]
fn version_1_records_still_decode() {
    let delete = LogRecord {
        shard: ShardRef::new(BucketId::new("b-7f3a").unwrap(), ShardId::new(7)),
        position: position(42, 1001),
        body: RecordBody::Delete(Delete {
            key: "photos/cat.jpg".into(),
        }),
    };
    let v1 = unhex(DELETE_V1);
    assert_eq!(LogRecord::decode(&v1).unwrap(), (delete, v1.len()));
    assert_eq!(RecordHeader::decode(&v1).unwrap().version, 1);

    // A version 1 `PUT` with checksums: a digest each, no part counts.
    let crc32 = [1, 2, 3, 4];
    let md5 = [9; 16];
    let body = |part_counts: bool| {
        let mut checksums = Body::default().u8(2).u8(1).raw(&crc32);
        if part_counts {
            checksums = checksums.u16(0);
        }
        checksums = checksums.u8(6).raw(&md5);
        if part_counts {
            checksums = checksums.u16(0);
        }
        Body::default()
            .str16("k")
            .u64(2)
            .u64(1_700_000_000_000)
            .str16("d41d8cd98f00b204e9800998ecf8427e")
            .u8(0) // no inherited identity
            .u16(0) // no metadata
            .u8(0) // no tags
            .raw(&checksums.0)
            .u8(0) // no copy source
            .u8(0) // inline data
    };
    let v1 = Frame {
        version: 1,
        ..Frame::new(PUT).keyed("k")
    }
    .build(&body(false).0, b"hi");
    let (decoded, _) = LogRecord::decode(&v1).unwrap();
    let mut put = inline_put("k", b"hi");
    put.checksums = BTreeMap::from([
        (
            ChecksumAlgorithm::Crc32,
            Checksum::full_object(ChecksumAlgorithm::Crc32, &crc32).unwrap(),
        ),
        (
            ChecksumAlgorithm::Md5,
            Checksum::full_object(ChecksumAlgorithm::Md5, &md5).unwrap(),
        ),
    ]);
    assert_eq!(decoded, record(RecordBody::Put(put)));
    // It re-encodes in the current version, with part counts.
    let v2 = Frame::new(PUT).keyed("k").build(&body(true).0, b"hi");
    assert_eq!(decoded.to_bytes().unwrap(), v2);
    assert_eq!(LogRecord::decode(&v2).unwrap().0, decoded);
    // Version 2 bytes read as version 1 leave the part counts unread.
    let mut misread = v2.clone();
    misread[8] = 1;
    reseal(&mut misread);
    assert!(LogRecord::decode(&misread).is_err());
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
            "2a0d3f54", // CRC32C, checked with an independent bitwise implementation
            "0200",     // format version 2
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
    assert_eq!(FORMAT_VERSION, 2);
    assert_eq!(MIN_FORMAT_VERSION, 1);
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
            "TRUNCATE",
            "CONFIG"
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

    for version in [0, 3, u16::MAX] {
        let mut bytes = good.clone();
        bytes[8..10].copy_from_slice(&version.to_le_bytes());
        assert_eq!(decode_err(&bytes), DecodeError::UnsupportedVersion(version));
    }

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
    let checksums = Body::default()
        .u8(2)
        .u8(2)
        .raw(&crc32)
        .u16(0)
        .u8(1)
        .raw(&crc32)
        .u16(0);
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
    // A part count makes a composite checksum, which CRC64NVME cannot be,
    // and is at most 10,000.
    let crc64 = [0; 8];
    let composite = Body::default().u8(1).u8(3).raw(&crc64).u16(2);
    assert!(matches!(
        err(fields(none16(), none8(), composite)).problem,
        Problem::Invalid(_)
    ));
    let too_many = Body::default().u8(1).u8(1).raw(&crc32).u16(10_001);
    assert!(matches!(
        err(fields(none16(), none8(), too_many)).problem,
        Problem::Invalid(_)
    ));
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
    bad.checksums.insert(
        ChecksumAlgorithm::Sha256,
        Checksum::full_object(ChecksumAlgorithm::Crc32, &[0; 4]).unwrap(),
    );
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
            DecodeError::UnsupportedVersion(3),
            ErrorClass::Unsupported,
            "log format version 3 is not supported; this build reads versions 1 to 2",
        ),
        (
            DecodeError::UnknownKind(99),
            ErrorClass::Unsupported,
            "unknown log record kind 99",
        ),
        (
            DecodeError::UnsupportedKind(RecordKind::EcRelease),
            ErrorClass::Unsupported,
            "log record kind EC_RELEASE is reserved but not yet supported",
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

fn mpu_part(upload: EpochSeq, data: PutData) -> MpuPart {
    let size = match &data {
        PutData::Inline(bytes) => bytes.len() as u64,
        PutData::Extents(extents) => extents.iter().map(|e| u64::from(e.len)).sum(),
    };
    MpuPart {
        key: "k".into(),
        upload,
        part_number: 2,
        size,
        last_modified_ms: 7,
        etag: ETag::new("d41d8cd98f00b204e9800998ecf8427e").unwrap(),
        checksums: BTreeMap::new(),
        data,
    }
}

fn mpu_complete(parts: &[(u16, EpochSeq)]) -> MpuComplete {
    MpuComplete {
        key: "k".into(),
        upload: position(5, 1),
        last_modified_ms: 8,
        size: 3,
        etag: ETag::new("aa-2").unwrap(),
        checksums: BTreeMap::new(),
        parts: parts
            .iter()
            .map(|&(number, position)| CompletedPart { number, position })
            .collect(),
    }
}

#[test]
fn multipart_records_follow_the_documented_layout() {
    let create = MpuCreate {
        key: "k".into(),
        initiated_ms: 1_700_000_000_000,
        metadata: BTreeMap::from([("content-type".into(), "a/b".into())]),
        tags: BTreeMap::new(),
        checksum: UploadChecksum::of(ChecksumAlgorithm::Sha256),
    };
    let body = Body::default()
        .str16("k")
        .u64(1_700_000_000_000)
        .u16(1)
        .str16("content-type")
        .str16("a/b")
        .u8(0) // no tags
        .u8(1) // a checksum:
        .u8(ChecksumAlgorithm::Sha256.code())
        .u8(1); // COMPOSITE
    let expected = Frame::new(MPU_CREATE).keyed("k").build(&body.0, &[]);
    assert_eq!(
        record(RecordBody::MpuCreate(create)).to_bytes().unwrap(),
        expected
    );

    let part = mpu_part(position(5, 1), PutData::Inline(Bytes::from_static(b"abc")));
    let body = Body::default()
        .str16("k")
        .u64(5)
        .u64(1) // the upload's position
        .u16(2) // part number
        .u64(3)
        .u64(7)
        .str16("d41d8cd98f00b204e9800998ecf8427e")
        .u8(0) // no checksums
        .u8(0); // inline data
    let expected = Frame::new(MPU_PART).keyed("k").build(&body.0, b"abc");
    let bytes = record(RecordBody::MpuPart(part)).to_bytes().unwrap();
    assert_eq!(bytes, expected);
    assert_eq!(LogRecord::decode(&bytes).unwrap().0.body.payload(), b"abc");

    let complete = mpu_complete(&[(1, position(5, 2)), (3, position(5, 4))]);
    let body = Body::default()
        .str16("k")
        .u64(5)
        .u64(1)
        .u64(8)
        .u64(3)
        .str16("aa-2")
        .u8(0) // no checksums
        .u16(2)
        .u16(1)
        .u64(5)
        .u64(2)
        .u16(3)
        .u64(5)
        .u64(4);
    let expected = Frame::new(MPU_COMPLETE).keyed("k").build(&body.0, &[]);
    assert_eq!(
        record(RecordBody::MpuComplete(complete))
            .to_bytes()
            .unwrap(),
        expected
    );

    let abort = MpuAbort {
        key: "k".into(),
        upload: position(5, 1),
    };
    let body = Body::default().str16("k").u64(5).u64(1);
    let expected = Frame::new(MPU_ABORT).keyed("k").build(&body.0, &[]);
    assert_eq!(
        record(RecordBody::MpuAbort(abort)).to_bytes().unwrap(),
        expected
    );
}

#[test]
fn upload_begin_records_follow_the_documented_layout() {
    // The key, and nothing else: the record's position is the identity.
    let begin = RecordBody::UploadBegin(UploadBegin { key: "k".into() });
    let body = Body::default().str16("k");
    let expected = Frame::new(UPLOAD_BEGIN).keyed("k").build(&body.0, &[]);
    let bytes = record(begin.clone()).to_bytes().unwrap();
    assert_eq!(bytes, expected);
    let (decoded, len) = LogRecord::decode(&bytes).unwrap();
    assert_eq!(decoded.key_hash(), Some(KeyHash::of(&shard().bucket, b"k")));
    assert_eq!((decoded.body, len), (begin, bytes.len()));

    // An empty or overlong key, a payload, or trailing bytes are refused.
    assert_eq!(
        encode_err(RecordBody::UploadBegin(UploadBegin { key: String::new() })).field,
        "upload_begin.key"
    );
    let long = "k".repeat(1025);
    assert_eq!(
        encode_err(RecordBody::UploadBegin(UploadBegin { key: long })).field,
        "upload_begin.key"
    );
    let with_payload = Frame::new(UPLOAD_BEGIN).keyed("k").build(&body.0, b"x");
    assert_eq!(malformed(&with_payload).field, "payload");
    let trailing = Body::default().str16("k").u8(0);
    let trailing = Frame::new(UPLOAD_BEGIN).keyed("k").build(&trailing.0, &[]);
    assert_eq!(malformed(&trailing).field, "body");
}

#[test]
fn part_flushed_records_follow_the_documented_layout() {
    let etag = ETag::new("5d41402abc4b2a76b9719d911017c592").unwrap();
    let steps = [
        (RemoteStep::Opened, Body::default().u8(0)),
        (
            RemoteStep::Part {
                number: 3,
                position: position(5, 7),
                remote_etag: etag.clone(),
            },
            Body::default()
                .u8(1)
                .u16(3)
                .u64(5)
                .u64(7)
                .str16(etag.as_str()),
        ),
        (RemoteStep::Ended, Body::default().u8(2)),
    ];
    for (step, tail) in steps {
        let flushed = RecordBody::PartFlushed(PartFlushed {
            key: "k".into(),
            upload: position(5, 2),
            remote_upload_id: "up-1".into(),
            step,
        });
        let body = Body::default()
            .str16("k")
            .u64(5)
            .u64(2)
            .str16("up-1")
            .raw(&tail.0);
        let expected = Frame::new(PART_FLUSHED).keyed("k").build(&body.0, &[]);
        let bytes = record(flushed.clone()).to_bytes().unwrap();
        assert_eq!(hex(&bytes), hex(&expected));
        let (decoded, len) = LogRecord::decode(&bytes).unwrap();
        assert_eq!((decoded.body, len), (flushed, bytes.len()));
    }

    // The upload precedes the record, the part follows the upload and
    // precedes the record, and the part number and upload ID are bounded.
    let part = |upload, number, at| {
        RecordBody::PartFlushed(PartFlushed {
            key: "k".into(),
            upload,
            remote_upload_id: "up-1".into(),
            step: RemoteStep::Part {
                number,
                position: at,
                remote_etag: etag.clone(),
            },
        })
    };
    let cases = [
        (
            part(position(5, 9), 1, position(5, 8)),
            "part_flushed.upload",
        ),
        (
            part(position(5, 2), 0, position(5, 3)),
            "part_flushed.number",
        ),
        (
            part(position(5, 2), 10_001, position(5, 3)),
            "part_flushed.number",
        ),
        (
            part(position(5, 2), 1, position(5, 2)),
            "part_flushed.position",
        ),
        (
            part(position(5, 2), 1, position(5, 9)),
            "part_flushed.position",
        ),
    ];
    for (body, field) in cases {
        assert_eq!(encode_err(body).field, field);
    }
    for id in [String::new(), "u".repeat(1025)] {
        let body = RecordBody::PartFlushed(PartFlushed {
            key: "k".into(),
            upload: position(5, 2),
            remote_upload_id: id,
            step: RemoteStep::Ended,
        });
        assert_eq!(encode_err(body).field, "part_flushed.remote_upload_id");
    }
    let head = Body::default().str16("k").u64(5).u64(2).str16("up-1");
    let unknown = Frame::new(PART_FLUSHED)
        .keyed("k")
        .build(&head.raw(&[3]).0, &[]);
    assert_eq!(
        malformed(&unknown),
        FieldError {
            field: "part_flushed.step",
            problem: Problem::InvalidTag(3)
        }
    );
    // A part named at the record's own position is refused on decoding too.
    let late = Body::default()
        .str16("k")
        .u64(5)
        .u64(2)
        .str16("up-1")
        .u8(1)
        .u16(1)
        .u64(5)
        .u64(9)
        .str16(etag.as_str());
    let late = Frame::new(PART_FLUSHED).keyed("k").build(&late.0, &[]);
    assert_eq!(malformed(&late).field, "part_flushed.position");
}

#[test]
fn multipart_records_are_validated() {
    // A part, completion, or abort names an upload opened before it.
    for upload in [position(5, 9), position(6, 0)] {
        let part = mpu_part(upload, PutData::Inline(Bytes::new()));
        assert_eq!(
            encode_err(RecordBody::MpuPart(part)).field,
            "mpu_part.upload"
        );
        let abort = MpuAbort {
            key: "k".into(),
            upload,
        };
        assert_eq!(
            encode_err(RecordBody::MpuAbort(abort)).field,
            "mpu_abort.upload"
        );
    }
    for number in [0, 10_001] {
        let mut part = mpu_part(position(1, 1), PutData::Inline(Bytes::new()));
        part.part_number = number;
        assert_eq!(
            encode_err(RecordBody::MpuPart(part)).field,
            "mpu_part.part_number"
        );
    }
    let mut part = mpu_part(position(1, 1), PutData::Inline(Bytes::from_static(b"x")));
    part.size = 2;
    assert_eq!(encode_err(RecordBody::MpuPart(part)).field, "mpu_part.data");
    let extent = ExtentRef {
        position: position(5, 9),
        len: 1,
    };
    let part = mpu_part(position(1, 1), PutData::Extents(vec![extent]));
    assert_eq!(
        encode_err(RecordBody::MpuPart(part)).field,
        "mpu_part.extents"
    );

    // Completed parts are in increasing order, between the upload and the
    // completion.
    let parts_err =
        |parts: &[(u16, EpochSeq)]| encode_err(RecordBody::MpuComplete(mpu_complete(parts)));
    assert_eq!(
        parts_err(&[(2, position(5, 2)), (1, position(5, 3))]).problem,
        Problem::Unsorted
    );
    assert_eq!(
        parts_err(&[(1, position(5, 2)), (1, position(5, 3))]).problem,
        Problem::Unsorted
    );
    for outside in [position(5, 1), position(5, 9), position(4, 7)] {
        assert_eq!(parts_err(&[(1, outside)]).field, "mpu_complete.parts");
    }
    assert_eq!(
        parts_err(&[(0, position(5, 2))]).field,
        "mpu_complete.parts"
    );
    assert_eq!(parts_err(&[]).problem, Problem::Empty);

    // Only the multipart checksums S3 defines.
    for (algorithm, checksum_type) in [
        (ChecksumAlgorithm::Sha256, ChecksumType::FullObject),
        (ChecksumAlgorithm::Crc64Nvme, ChecksumType::Composite),
        (ChecksumAlgorithm::Md5, ChecksumType::FullObject),
    ] {
        let create = MpuCreate {
            key: "k".into(),
            initiated_ms: 0,
            metadata: BTreeMap::new(),
            tags: BTreeMap::new(),
            checksum: Some(UploadChecksum {
                algorithm,
                checksum_type,
            }),
        };
        assert_eq!(
            encode_err(RecordBody::MpuCreate(create)).field,
            "mpu_create.checksum"
        );
        let body = Body::default()
            .str16("k")
            .u64(0)
            .u16(0)
            .u8(0)
            .u8(1)
            .u8(algorithm.code())
            .u8(u8::from(checksum_type == ChecksumType::Composite));
        let bytes = Frame::new(MPU_CREATE).keyed("k").build(&body.0, &[]);
        assert_eq!(malformed(&bytes).field, "mpu_create.checksum");
    }
    for tail in [[9, 0], [ChecksumAlgorithm::Crc32.code(), 2]] {
        let body = Body::default()
            .str16("k")
            .u64(0)
            .u16(0)
            .u8(0)
            .u8(1)
            .raw(&tail);
        let bytes = Frame::new(MPU_CREATE).keyed("k").build(&body.0, &[]);
        assert!(matches!(malformed(&bytes).problem, Problem::InvalidTag(_)));
    }
    assert_eq!(
        UploadChecksum::of(ChecksumAlgorithm::Crc64Nvme)
            .unwrap()
            .checksum_type,
        ChecksumType::FullObject
    );
    assert_eq!(UploadChecksum::of(ChecksumAlgorithm::Md5), None);

    // A part count past what the header holds is caught before allocating.
    let body = Body::default()
        .str16("k")
        .u64(5)
        .u64(1)
        .u64(0)
        .u64(0)
        .str16("e")
        .u8(0)
        .u16(10_000);
    let bytes = Frame::new(MPU_COMPLETE).keyed("k").build(&body.0, &[]);
    assert_eq!(malformed(&bytes).problem, Problem::Truncated);
}
