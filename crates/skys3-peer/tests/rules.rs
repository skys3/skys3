//! The protocol's rules and limits, and which messages a session allows.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use skys3_log::record::{IDENTITY_METADATA, IDENTITY_METADATA_RESERVED, MAX_METADATA_LEN};
use skys3_peer::{
    APPLY_BY_VERSION, Abort, AbortReason, Applied, ApplyError, Batch, Begin, ByteRanges,
    Capabilities, Commit, Data, Hello, MAX_BATCH_ITEMS, MAX_HEADER_LEN, MAX_OBJECT_BYTES,
    MAX_PAYLOAD_LEN, MAX_PIECE_BYTES, MAX_REASON_LEN, MAX_REPORTED_PIECES, MAX_REPORTED_RANGES,
    Message, MessageError, Outcome, PROTOCOL_VERSION, Precondition, ProtocolError, Put, PutData,
    SUPPORTED_VERSIONS, Side, StagedPart, StagedRanges, VersionRange, Write, negotiate,
};
use skys3_types::checksum::{Checksum, ChecksumAlgorithm};
use skys3_types::{BucketName, ClusterId, ETag, WriteIdentity};

fn identity() -> WriteIdentity {
    "prod-us/b-7f3a/5/42.1001".parse().unwrap()
}

fn bucket() -> BucketName {
    BucketName::new("archive").unwrap()
}

fn put(data: PutData, size: u64) -> Put {
    Put {
        size,
        etag: ETag::new("9b2cf535f27731c974343645a3985328").unwrap(),
        last_modified_ms: 1_700_000_000_000,
        metadata: BTreeMap::new(),
        tags: BTreeMap::new(),
        checksums: BTreeMap::new(),
        data,
    }
}

/// A commit of `key`, with a write identity no other commit has.
fn commit(key: &str, write: Write) -> Commit {
    static NEXT_SEQ: AtomicU64 = AtomicU64::new(1);
    let seq = NEXT_SEQ.fetch_add(1, Ordering::Relaxed);
    Commit {
        identity: format!("prod-us/b-7f3a/5/42.{seq}").parse().unwrap(),
        bucket: bucket(),
        key: key.to_owned(),
        precondition: Precondition::Absent,
        write,
        apply_by_ms: Some(1_800_000_000_000),
    }
}

fn inline(key: &str, bytes: &'static [u8]) -> Commit {
    commit(
        key,
        Write::Put(put(
            PutData::Inline(Bytes::from_static(bytes)),
            bytes.len() as u64,
        )),
    )
}

fn part(number: u16, piece: u64, size: u64) -> StagedPart {
    StagedPart {
        number,
        piece,
        size,
        md5: [7; 16],
    }
}

/// Asserts that encoding `message` refuses `field`.
#[track_caller]
fn refused(message: Message, field: &str) {
    match message.encode() {
        Err(MessageError::Invalid { field: f, problem }) => {
            assert_eq!(f, field, "{problem}");
            assert!(
                MessageError::Invalid { field: f, problem }
                    .to_string()
                    .starts_with(&format!("invalid {field}: "))
            );
        }
        other => panic!("expected an invalid {field}, got {other:?}"),
    }
}

#[track_caller]
fn accepted(message: Message) {
    let bytes = message.encode().unwrap();
    assert_eq!(
        Message::decode(&bytes).unwrap(),
        Some((message, bytes.len()))
    );
}

#[test]
fn keys_and_reasons_are_bounded() {
    let begin = |key: String| {
        Message::Begin(Begin {
            identity: identity(),
            bucket: bucket(),
            key,
        })
    };
    refused(begin(String::new()), "begin.key");
    refused(begin("k".repeat(1025)), "begin.key");
    accepted(begin("k".repeat(1024)));
    refused(Message::Commit(commit("", Write::Delete)), "commit.key");

    let abort = |detail: String| {
        Message::Abort(Abort {
            identity: identity(),
            reason: AbortReason::Expired,
            detail,
        })
    };
    refused(abort("x".repeat(MAX_REASON_LEN + 1)), "abort.detail");
    accepted(abort("x".repeat(MAX_REASON_LEN)));
    let failed = |reason: String| {
        Message::Applied(Applied {
            identity: identity(),
            outcome: Outcome::Failed {
                error: ApplyError::Refused,
                reason,
            },
        })
    };
    refused(failed("x".repeat(MAX_REASON_LEN + 1)), "applied.reason");
    accepted(failed("not a receiving bucket".to_owned()));
}

#[test]
fn data_stays_within_a_piece_and_the_payload_limit() {
    let data = |offset: u64, len: usize| {
        Message::Data(Data {
            piece: 3,
            offset,
            bytes: Bytes::from(vec![1; len]),
        })
    };
    refused(data(0, 0), "data.bytes");
    refused(data(MAX_PIECE_BYTES - 1, 2), "data.offset");
    refused(data(u64::MAX, 1), "data.offset");
    accepted(data(MAX_PIECE_BYTES - 1, 1));
    assert!(matches!(
        data(0, MAX_PAYLOAD_LEN as usize + 1).encode(),
        Err(MessageError::PayloadTooLong(_))
    ));
    accepted(data(0, MAX_PAYLOAD_LEN as usize));
}

#[test]
fn reported_ranges_are_bounded() {
    let report = |pieces: BTreeMap<u64, ByteRanges>| {
        Message::Durable(StagedRanges {
            identity: identity(),
            pieces,
        })
    };
    let many_pieces = (0..=MAX_REPORTED_PIECES as u64)
        .map(|piece| (piece, ByteRanges::new()))
        .collect();
    refused(report(many_pieces), "ranges.pieces");
    let many_ranges: ByteRanges = (0..=MAX_REPORTED_RANGES as u64)
        .map(|i| 2 * i..2 * i + 1)
        .collect();
    refused(report(BTreeMap::from([(1, many_ranges)])), "ranges.ranges");
    let past_the_piece: ByteRanges = std::iter::once(0..MAX_PIECE_BYTES + 1).collect();
    refused(
        report(BTreeMap::from([(1, past_the_piece)])),
        "ranges.ranges",
    );
    let whole: ByteRanges = std::iter::once(0..MAX_PIECE_BYTES).collect();
    accepted(report(BTreeMap::from([(1, whole), (2, ByteRanges::new())])));
}

#[test]
fn puts_follow_the_rules_of_log_records() {
    let staged = |put: Put| Message::Commit(commit("k", Write::Put(put)));
    refused(
        staged(put(PutData::Staged { piece: 1 }, MAX_PIECE_BYTES + 1)),
        "put.size",
    );
    refused(
        staged(put(
            PutData::Multipart(vec![part(1, 1, 1)]),
            MAX_OBJECT_BYTES + 1,
        )),
        "put.size",
    );
    accepted(staged(put(PutData::Staged { piece: 1 }, MAX_PIECE_BYTES)));

    let mut with_metadata = put(PutData::Staged { piece: 1 }, 5);
    with_metadata
        .metadata
        .insert("Content-Type".to_owned(), "text/plain".to_owned());
    refused(staged(with_metadata.clone()), "put.metadata");
    with_metadata.metadata.clear();
    with_metadata
        .metadata
        .insert("x-amz-meta-big".to_owned(), "v".repeat(8 * 1024));
    refused(staged(with_metadata.clone()), "put.metadata");
    with_metadata.metadata.clear();
    with_metadata
        .metadata
        .insert("content-type".to_owned(), "text/plain".to_owned());
    accepted(staged(with_metadata));

    // The metadata leaves room for the write identity the destination
    // stores with it, so a valid COMMIT always fits in a `PUT` record. An
    // identity entry already there does not count: it is replaced.
    let limit = MAX_METADATA_LEN - IDENTITY_METADATA_RESERVED;
    let name = "x-amz-meta-big";
    let mut full = put(PutData::Staged { piece: 1 }, 5);
    full.metadata
        .insert(name.to_owned(), "v".repeat(limit - name.len()));
    full.metadata
        .insert(IDENTITY_METADATA.to_owned(), "c/b/0/1.2".to_owned());
    accepted(staged(full.clone()));
    full.metadata
        .insert(name.to_owned(), "v".repeat(limit - name.len() + 1));
    refused(staged(full), "put.metadata");

    let mut with_tags = put(PutData::Staged { piece: 1 }, 5);
    with_tags.tags.insert(String::new(), "v".to_owned());
    refused(staged(with_tags.clone()), "put.tags");
    with_tags.tags.clear();
    for i in 0..51 {
        with_tags.tags.insert(format!("t{i}"), String::new());
    }
    refused(staged(with_tags.clone()), "put.tags");
    with_tags.tags.pop_first();
    accepted(staged(with_tags));

    let mut with_checksums = put(PutData::Staged { piece: 1 }, 5);
    with_checksums.checksums.insert(
        ChecksumAlgorithm::Sha256,
        Checksum::full_object(ChecksumAlgorithm::Crc32, &[0; 4]).unwrap(),
    );
    refused(staged(with_checksums.clone()), "put.checksums");
    with_checksums.checksums.insert(
        ChecksumAlgorithm::Sha256,
        Checksum::composite(ChecksumAlgorithm::Sha256, &[0; 32], 3).unwrap(),
    );
    accepted(staged(with_checksums));
}

#[test]
fn multipart_parts_are_ordered_distinct_and_add_up() {
    let multipart = |parts: Vec<StagedPart>, size: u64| {
        Message::Commit(commit(
            "k",
            Write::Put(put(PutData::Multipart(parts), size)),
        ))
    };
    refused(multipart(vec![], 0), "put.parts");
    refused(multipart(vec![part(0, 1, 5)], 5), "put.parts");
    refused(
        multipart(vec![part(2, 1, 5), part(1, 2, 5)], 10),
        "put.parts",
    );
    refused(
        multipart(vec![part(1, 1, 5), part(1, 2, 5)], 10),
        "put.parts",
    );
    refused(multipart(vec![part(10_001, 1, 5)], 5), "put.parts");
    refused(
        multipart(vec![part(1, 1, 5), part(2, 1, 5)], 10),
        "put.parts",
    );
    refused(
        multipart(vec![part(1, 1, MAX_PIECE_BYTES + 1)], MAX_PIECE_BYTES + 1),
        "put.parts",
    );
    refused(
        multipart(vec![part(1, 1, 5), part(3, 2, 5)], 11),
        "put.parts",
    );
    let too_many = (1..=10_001).map(|n| part(n, n.into(), 0)).collect();
    refused(multipart(too_many, 0), "put.parts");
    accepted(multipart(
        vec![part(1, 9, 5), part(3, 2, 0), part(10_000, 4, 6)],
        11,
    ));
}

#[test]
fn inline_bytes_travel_only_in_batches() {
    refused(Message::Commit(inline("k", b"abc")), "commit.put.data");
    let staged = commit("k", Write::Put(put(PutData::Staged { piece: 1 }, 3)));
    refused(
        Message::Batch(Batch {
            items: vec![staged],
        }),
        "commit.put.data",
    );
    let mut wrong_size = inline("k", b"abc");
    if let Write::Put(put) = &mut wrong_size.write {
        put.size = 4;
    }
    refused(
        Message::Batch(Batch {
            items: vec![wrong_size],
        }),
        "put.size",
    );
}

#[test]
fn batches_hold_distinct_keys_and_identities_within_the_limits() {
    refused(Message::Batch(Batch { items: vec![] }), "batch.items");
    let too_many = (0..=MAX_BATCH_ITEMS)
        .map(|i| commit(&i.to_string(), Write::Delete))
        .collect();
    refused(Message::Batch(Batch { items: too_many }), "batch.items");
    refused(
        Message::Batch(Batch {
            items: vec![inline("k", b"a"), commit("k", Write::Delete)],
        }),
        "batch.items",
    );
    let mut other_bucket = commit("k", Write::Delete);
    other_bucket.bucket = BucketName::new("other").unwrap();
    accepted(Message::Batch(Batch {
        items: vec![inline("k", b"a"), other_bucket.clone(), inline("e", b"")],
    }));
    // Each item's APPLIED names it by its write identity alone, so two items
    // of different keys may not share one.
    let mut same_identity = inline("k", b"a");
    same_identity.identity = other_bucket.identity.clone();
    match Message::Batch(Batch {
        items: vec![same_identity, other_bucket],
    })
    .encode()
    {
        Err(MessageError::Invalid { field, problem }) => {
            assert_eq!(field, "batch.items");
            assert!(problem.contains("write identity"), "{problem}");
        }
        other => panic!("expected a duplicate identity to be refused, got {other:?}"),
    }

    let half = Bytes::from(vec![0; MAX_PAYLOAD_LEN as usize / 2 + 1]);
    let big = |key: &str| {
        commit(
            key,
            Write::Put(put(PutData::Inline(half.clone()), half.len() as u64)),
        )
    };
    assert!(matches!(
        Message::Batch(Batch {
            items: vec![big("a"), big("b")],
        })
        .encode(),
        Err(MessageError::PayloadTooLong(_))
    ));
}

#[test]
fn headers_past_the_limit_are_not_encoded() {
    // 1,024 items of 1 KiB keys and metadata exceed the header limit.
    let items = (0..MAX_BATCH_ITEMS)
        .map(|i| {
            let mut item = inline(&format!("{i:04}{}", "k".repeat(1000)), b"");
            if let Write::Put(put) = &mut item.write {
                put.metadata
                    .insert("x-amz-meta-m".to_owned(), "v".repeat(1000));
            }
            item
        })
        .collect();
    let error = Message::Batch(Batch { items }).encode().unwrap_err();
    assert!(matches!(error, MessageError::HeaderTooLong(len) if len > MAX_HEADER_LEN.into()));
    assert!(error.to_string().contains("exceeds the limit"), "{error}");
}

#[test]
fn versions_and_capabilities() {
    assert_eq!(VersionRange::new(0, 1), None);
    assert_eq!(VersionRange::new(3, 2), None);
    let range = VersionRange::new(2, 4).unwrap();
    assert_eq!((range.min(), range.max()), (2, 4));
    assert!(range.contains(3) && !range.contains(1) && !range.contains(5));
    assert_eq!(range.to_string(), "2..=4");
    assert!(SUPPORTED_VERSIONS.contains(PROTOCOL_VERSION));

    let unknown = Capabilities::from_bits(1 << 40);
    let both = Capabilities::BATCH.union(unknown);
    assert_eq!(both.bits(), 1 | 1 << 40);
    assert!(both.contains(Capabilities::BATCH));
    assert!(!Capabilities::NONE.contains(Capabilities::BATCH));
    assert_eq!(both.intersection(unknown), unknown);
    assert_eq!(Capabilities::default(), Capabilities::NONE);
}

fn hello(cluster: &str, min: u16, max: u16, capabilities: Capabilities) -> Hello {
    Hello {
        cluster: ClusterId::new(cluster).unwrap(),
        versions: VersionRange::new(min, max).unwrap(),
        capabilities,
    }
}

#[test]
fn negotiation_fails_without_a_common_version() {
    let error = negotiate(
        &hello("a", 1, 2, Capabilities::NONE),
        &hello("b", 3, 4, Capabilities::NONE),
    )
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "no common protocol version: this end speaks 1..=2, the peer 3..=4"
    );
    // Unknown capabilities are never agreed on.
    let unknown = Capabilities::from_bits(1 << 63);
    let session = negotiate(
        &hello("a", 1, 1, Capabilities::BATCH.union(unknown)),
        &hello("b", 1, 1, Capabilities::BATCH.union(unknown)),
    )
    .unwrap();
    assert_eq!(session.capabilities, Capabilities::BATCH);
    assert_eq!(session.version, 1);
}

#[test]
fn sessions_allow_each_side_its_own_messages() {
    let with_batch = negotiate(
        &hello("a", 2, 2, Capabilities::BATCH),
        &hello("b", 2, 2, Capabilities::BATCH),
    )
    .unwrap();
    let without_batch = negotiate(
        &hello("a", 2, 2, Capabilities::BATCH),
        &hello("b", 2, 2, Capabilities::NONE),
    )
    .unwrap();
    let data = Message::Data(Data {
        piece: 0,
        offset: 0,
        bytes: Bytes::from_static(b"x"),
    });
    let applied = Message::Applied(Applied {
        identity: identity(),
        outcome: Outcome::Committed { etag: None },
    });
    let abort = Message::Abort(Abort {
        identity: identity(),
        reason: AbortReason::Cancelled,
        detail: String::new(),
    });
    let batch = Message::Batch(Batch {
        items: vec![commit("k", Write::Delete)],
    });
    let hello = Message::Hello(hello("a", 1, 1, Capabilities::NONE));

    assert_eq!(with_batch.check(&data, Side::Source), Ok(()));
    assert_eq!(with_batch.check(&applied, Side::Destination), Ok(()));
    assert_eq!(with_batch.check(&abort, Side::Source), Ok(()));
    assert_eq!(with_batch.check(&abort, Side::Destination), Ok(()));
    assert_eq!(with_batch.check(&batch, Side::Source), Ok(()));

    let error = with_batch.check(&data, Side::Destination).unwrap_err();
    assert_eq!(
        error,
        ProtocolError::WrongSide {
            side: Side::Destination,
            message: "DATA"
        }
    );
    assert_eq!(error.to_string(), "a Destination does not send DATA");
    assert!(matches!(
        with_batch.check(&applied, Side::Source),
        Err(ProtocolError::WrongSide { .. })
    ));
    assert_eq!(
        with_batch.check(&hello, Side::Source),
        Err(ProtocolError::RepeatedHello)
    );
    let error = without_batch.check(&batch, Side::Source).unwrap_err();
    assert_eq!(error, ProtocolError::NotNegotiated { message: "BATCH" });
    assert!(error.to_string().contains("BATCH"));

    // From version 2, every COMMIT and BATCH item carries an apply-by
    // time (§7.8), sent or received.
    assert_eq!(APPLY_BY_VERSION, 2);
    let mut unbounded = commit("k", Write::Delete);
    unbounded.apply_by_ms = None;
    let lone = Message::Commit(unbounded.clone());
    assert_eq!(
        with_batch.check(&Message::Commit(commit("k", Write::Delete)), Side::Source),
        Ok(())
    );
    for side in [Side::Source, Side::Destination] {
        let error = with_batch.check(&lone, side);
        let expected = match side {
            Side::Source => Err(ProtocolError::NoApplyBy { message: "COMMIT" }),
            Side::Destination => Err(ProtocolError::WrongSide {
                side,
                message: "COMMIT",
            }),
        };
        assert_eq!(error, expected);
    }
    let mixed = Message::Batch(Batch {
        items: vec![commit("a", Write::Delete), unbounded],
    });
    let error = with_batch.check(&mixed, Side::Source).unwrap_err();
    assert_eq!(error, ProtocolError::NoApplyBy { message: "BATCH" });
    assert_eq!(error.to_string(), "BATCH carries no apply-by time");
}

#[test]
fn every_message_has_its_design_name() {
    let ranges = StagedRanges {
        identity: identity(),
        pieces: BTreeMap::new(),
    };
    let names: Vec<_> = [
        Message::Hello(hello("a", 1, 1, Capabilities::NONE)),
        Message::Begin(Begin {
            identity: identity(),
            bucket: bucket(),
            key: "k".to_owned(),
        }),
        Message::Data(Data {
            piece: 0,
            offset: 0,
            bytes: Bytes::from_static(b"x"),
        }),
        Message::Durable(ranges.clone()),
        Message::Resume(ranges),
        Message::Commit(commit("k", Write::Delete)),
        Message::Applied(Applied {
            identity: identity(),
            outcome: Outcome::PreconditionFailed { current: None },
        }),
        Message::Batch(Batch {
            items: vec![commit("k", Write::Delete)],
        }),
        Message::Abort(Abort {
            identity: identity(),
            reason: AbortReason::QuotaExceeded,
            detail: String::new(),
        }),
    ]
    .iter()
    .map(Message::name)
    .collect();
    assert_eq!(
        names,
        [
            "HELLO", "BEGIN", "DATA", "DURABLE", "RESUME", "COMMIT", "APPLIED", "BATCH", "ABORT"
        ]
    );
}
