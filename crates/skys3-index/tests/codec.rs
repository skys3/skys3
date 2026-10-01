//! Property tests for the index's key and value encodings.

use std::collections::BTreeMap;

use proptest::collection::{btree_map, vec};
use proptest::option;
use proptest::prelude::*;
use skys3_index::codec::{self, MIN_VALUE_FORMAT, VALUE_FORMAT};
use skys3_index::{
    ControlEntry, Entry, EntryState, ObjectPart, ObjectVersion, Part, Payload, Upload,
};
use skys3_log::record::{
    Checksum, ChecksumAlgorithm, ChecksumType, Checksums, CopySource, ExtentRef, ShardRef,
    UploadChecksum,
};
use skys3_log::{RecordLocation, SegmentId, SegmentSummary};
use skys3_types::{
    BucketId, ETag, Epoch, EpochSeq, Generation, Label, Seq, ShardId, VersionIdentity,
};

fn position() -> impl Strategy<Value = EpochSeq> {
    (any::<u64>(), any::<u64>()).prop_map(|(e, s)| EpochSeq::new(Epoch::new(e), Seq::new(s)))
}

fn etag() -> impl Strategy<Value = ETag> {
    "[0-9a-f]{1,40}(-[0-9]{1,4})?".prop_map(|s| ETag::new(s).unwrap())
}

fn bucket() -> impl Strategy<Value = BucketId> {
    "[a-z0-9]([a-z0-9-]{0,23}[a-z0-9])?".prop_map(|s| BucketId::new(s).unwrap())
}

fn shard() -> impl Strategy<Value = ShardRef> {
    (bucket(), any::<u8>()).prop_map(|(b, n)| ShardRef::new(b, ShardId::new(n)))
}

fn text(max: usize) -> impl Strategy<Value = String> {
    proptest::string::string_regex(&format!(".{{0,{max}}}")).unwrap()
}

fn checksums() -> impl Strategy<Value = Checksums> {
    proptest::sample::subsequence(ChecksumAlgorithm::ALL.to_vec(), 0..=6).prop_flat_map(|algs| {
        algs.into_iter()
            .map(|a| {
                (vec(any::<u8>(), a.digest_len()), 0..=10_000_u32).prop_map(move |(d, parts)| {
                    let checksum = if parts == 0 || !a.supports_composite() {
                        Checksum::full_object(a, &d)
                    } else {
                        Checksum::composite(a, &d, parts)
                    };
                    (a, checksum.unwrap())
                })
            })
            .collect::<Vec<_>>()
            .prop_map(|pairs| pairs.into_iter().collect())
    })
}

fn payload() -> impl Strategy<Value = Payload> {
    prop_oneof![
        Just(Payload::None),
        position().prop_map(Payload::Inline),
        vec(
            (position(), 1..=skys3_log::record::MAX_PAYLOAD_LEN)
                .prop_map(|(position, len)| ExtentRef { position, len }),
            0..8
        )
        .prop_map(Payload::Extents),
        (position(), btree_map(1..=10_000_u16, any::<u64>(), 1..8)).prop_map(|(upload, parts)| {
            Payload::Parts {
                upload,
                parts: parts
                    .into_iter()
                    .map(|(number, size)| ObjectPart { number, size })
                    .collect(),
            }
        }),
    ]
}

fn copy_source() -> impl Strategy<Value = CopySource> {
    (bucket(), text(40), any::<u64>(), etag(), option::of(etag())).prop_map(
        |(bucket, key, seq, etag, remote_etag)| CopySource {
            bucket,
            key,
            version: VersionIdentity::new(Seq::new(seq), etag),
            remote_etag,
        },
    )
}

fn object() -> impl Strategy<Value = ObjectVersion> {
    (
        (any::<u64>(), any::<u64>(), etag(), option::of(position())),
        (
            btree_map("[a-z-]{1,12}", text(20), 0..4),
            btree_map(text(10), text(10), 0..4),
            checksums(),
        ),
        (option::of(text(12)), option::of(copy_source()), payload()),
    )
        .prop_map(
            |(
                (size, last_modified_ms, local_etag, write_identity),
                (metadata, tags, checksums),
                (storage_class, copy_source, payload),
            )| ObjectVersion {
                size,
                last_modified_ms,
                local_etag,
                write_identity,
                metadata,
                tags,
                checksums,
                storage_class,
                copy_source,
                payload,
            },
        )
}

fn entry() -> impl Strategy<Value = Entry> {
    (
        position(),
        proptest::sample::select(EntryState::ALL.to_vec()),
        option::of(object()),
        option::of(etag()),
        option::of(text(30)),
    )
        .prop_map(
            |(version, state, object, remote_etag, remote_version_id)| Entry {
                version,
                state,
                object,
                remote_etag,
                remote_version_id,
            },
        )
}

fn summary() -> impl Strategy<Value = SegmentSummary> {
    (any::<u64>(), btree_map(shard(), position(), 0..6)).prop_map(|(end, positions)| {
        SegmentSummary {
            start: 0,
            end,
            positions,
        }
    })
}

proptest! {
    #[test]
    fn entries_round_trip(entry in entry()) {
        let bytes = codec::encode_entry(&entry).unwrap();
        prop_assert_eq!(bytes[0], VALUE_FORMAT);
        prop_assert_eq!(codec::decode_entry(&bytes).unwrap(), entry);
        // Any proper prefix is truncated.
        prop_assert!(codec::decode_entry(&bytes[..bytes.len() - 1]).is_err());
        let mut longer = bytes.clone();
        longer.push(0);
        prop_assert!(codec::decode_entry(&longer).is_err());
    }

    #[test]
    fn keys_round_trip(shard in shard(), key in "[^\u{0}]{1,40}", position in position()) {
        let entry_key = codec::entry_key(&shard, &key);
        prop_assert!(entry_key.starts_with(&codec::shard_key(&shard)));
        prop_assert_eq!(codec::decode_entry_key(&entry_key).unwrap(), (shard.clone(), key));
        prop_assert_eq!(codec::decode_shard_key(&codec::shard_key(&shard)).unwrap(), shard.clone());
        let location_key = codec::location_key(&shard, position);
        prop_assert_eq!(codec::decode_location_key(&location_key).unwrap(), (shard, position));
    }

    #[test]
    fn location_keys_sort_by_position(shard in shard(), a in position(), b in position()) {
        let (ka, kb) = (codec::location_key(&shard, a), codec::location_key(&shard, b));
        prop_assert_eq!(ka.cmp(&kb), a.cmp(&b));
    }

    #[test]
    fn values_round_trip(
        summary in summary(),
        location in (any::<u64>(), any::<u64>(), any::<u32>()),
        applied in position(),
        generation in any::<u64>(),
        version in text(20),
        value in vec(any::<u8>(), 0..64),
        segment in any::<u64>(),
    ) {
        let bytes = codec::encode_summary(&summary).unwrap();
        prop_assert_eq!(codec::decode_summary(&bytes).unwrap(), summary);

        let location = RecordLocation {
            segment: SegmentId::new(location.0),
            offset: location.1,
            len: location.2,
        };
        prop_assert_eq!(codec::decode_location(&codec::encode_location(&location)).unwrap(), location);
        prop_assert_eq!(codec::decode_applied(&codec::encode_applied(applied)).unwrap(), applied);

        let control = ControlEntry { generation: Generation::new(generation), version, value };
        let bytes = codec::encode_control(&control).unwrap();
        prop_assert_eq!(codec::decode_control(&bytes).unwrap(), control);

        let disk = Label::new("disk-1").unwrap();
        let key = codec::coverage_key(&disk, SegmentId::new(segment));
        prop_assert!(key.starts_with(&codec::coverage_prefix(&disk)));
        prop_assert_eq!(codec::decode_coverage_key(&key).unwrap(), (disk, SegmentId::new(segment)));
    }

    /// Decoders never panic, and whatever decodes re-encodes to the same
    /// bytes.
    #[test]
    fn arbitrary_bytes_decode_canonically(mut bytes in vec(any::<u8>(), 0..200), format in any::<bool>()) {
        if format && !bytes.is_empty() {
            bytes[0] = VALUE_FORMAT;
        }
        if let Ok(entry) = codec::decode_entry(&bytes) {
            prop_assert_eq!(codec::encode_entry(&entry).unwrap(), bytes.clone());
        }
        if let Ok(summary) = codec::decode_summary(&bytes) {
            prop_assert_eq!(codec::encode_summary(&summary).unwrap(), bytes.clone());
        }
        if let Ok(control) = codec::decode_control(&bytes) {
            prop_assert_eq!(codec::encode_control(&control).unwrap(), bytes.clone());
        }
        if let Ok(location) = codec::decode_location(&bytes) {
            prop_assert_eq!(codec::encode_location(&location), bytes.clone());
        }
        if let Ok((shard, key)) = codec::decode_entry_key(&bytes) {
            prop_assert_eq!(codec::entry_key(&shard, &key), bytes.clone());
        }
        if let Ok((disk, segment)) = codec::decode_coverage_key(&bytes) {
            prop_assert_eq!(codec::coverage_key(&disk, segment), bytes.clone());
        }
    }
}

fn sample_entry() -> Entry {
    Entry {
        version: EpochSeq::new(Epoch::new(1), Seq::new(2)),
        state: EntryState::Dirty,
        object: Some(ObjectVersion {
            size: 3,
            last_modified_ms: 4,
            local_etag: ETag::new("123").unwrap(),
            write_identity: None,
            metadata: BTreeMap::from([("a".into(), "1".into()), ("b".into(), "2".into())]),
            tags: BTreeMap::new(),
            checksums: BTreeMap::from([(
                ChecksumAlgorithm::Crc32,
                Checksum::composite(ChecksumAlgorithm::Crc32, &[1, 2, 3, 4], 2).unwrap(),
            )]),
            storage_class: None,
            copy_source: None,
            payload: Payload::Extents(vec![ExtentRef {
                position: EpochSeq::new(Epoch::new(1), Seq::new(1)),
                len: 3,
            }]),
        }),
        remote_etag: None,
        remote_version_id: None,
    }
}

/// Returns `bytes` with the byte at `at` replaced.
fn with(bytes: &[u8], at: usize, value: u8) -> Vec<u8> {
    let mut bytes = bytes.to_vec();
    bytes[at] = value;
    bytes
}

/// An entry value in format 1, before checksums had part counts.
const ENTRY_V1: &str = concat!(
    "01",                                 // format 1
    "0100000000000000",                   // version: epoch 1
    "0200000000000000",                   // seq 2
    "01",                                 // state DIRTY
    "00",                                 // no remote ETag
    "00",                                 // no remote version ID
    "01",                                 // an object
    "0300000000000000",                   // size 3
    "0400000000000000",                   // last modified 4
    "0300313233",                         // local ETag "123"
    "00",                                 // no write identity
    "0000",                               // no metadata
    "00",                                 // no tags
    "02",                                 // two checksums
    "0101020304",                         // CRC32 01020304, no part count
    "06000102030405060708090a0b0c0d0e0f", // MD5 00..0f
    "00",                                 // no storage class
    "00",                                 // no copy source
    "00",                                 // no payload
);

fn unhex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}

#[test]
fn format_1_entries_still_decode() {
    let v1 = unhex(ENTRY_V1);
    let entry = codec::decode_entry(&v1).unwrap();
    let md5: Vec<u8> = (0..16).collect();
    let expected = Entry {
        version: EpochSeq::new(Epoch::new(1), Seq::new(2)),
        state: EntryState::Dirty,
        object: Some(ObjectVersion {
            size: 3,
            last_modified_ms: 4,
            local_etag: ETag::new("123").unwrap(),
            write_identity: None,
            metadata: BTreeMap::new(),
            tags: BTreeMap::new(),
            checksums: BTreeMap::from([
                (
                    ChecksumAlgorithm::Crc32,
                    Checksum::full_object(ChecksumAlgorithm::Crc32, &[1, 2, 3, 4]).unwrap(),
                ),
                (
                    ChecksumAlgorithm::Md5,
                    Checksum::full_object(ChecksumAlgorithm::Md5, &md5).unwrap(),
                ),
            ]),
            storage_class: None,
            copy_source: None,
            payload: Payload::None,
        }),
        remote_etag: None,
        remote_version_id: None,
    };
    assert_eq!(entry, expected);
    // It re-encodes in the current format: a part count, zero, after each
    // digest.
    let v2 = codec::encode_entry(&entry).unwrap();
    let mut expected_v2 = ENTRY_V1.replacen("01", "03", 1);
    expected_v2 = expected_v2.replace("0101020304", "01010203040000");
    expected_v2 = expected_v2.replace("0c0d0e0f", "0c0d0e0f0000");
    assert_eq!(v2, unhex(&expected_v2));
    assert_eq!(codec::decode_entry(&v2).unwrap(), entry);
    // Current bytes read as format 1 leave the part counts unread.
    assert!(codec::decode_entry(&with(&v2, 0, 1)).is_err());
    // Other values read the same in both formats.
    let applied = codec::encode_applied(EpochSeq::new(Epoch::new(3), Seq::new(4)));
    assert_eq!(applied[0], VALUE_FORMAT);
    assert_eq!(
        codec::decode_applied(&with(&applied, 0, MIN_VALUE_FORMAT)).unwrap(),
        EpochSeq::new(Epoch::new(3), Seq::new(4))
    );
}

#[test]
fn rejects_broken_entries() {
    let bytes = codec::encode_entry(&sample_entry()).unwrap();
    let decode = |bytes: &[u8]| codec::decode_entry(bytes).unwrap_err().field();
    assert_eq!(decode(&[]), "entry");
    for format in [0, 4, u8::MAX] {
        assert_eq!(decode(&with(&bytes, 0, format)), "entry", "format {format}");
    }
    // The state code follows the format byte and the version.
    assert_eq!(decode(&with(&bytes, 17, 0)), "entry.state");
    assert_eq!(decode(&with(&bytes, 18, 2)), "entry.remote_etag");
    // Metadata out of order: swap the two names, "a" and "b".
    let a = bytes.iter().position(|&b| b == b'a').unwrap();
    let b = bytes.iter().position(|&b| b == b'b').unwrap();
    let swapped = with(&with(&bytes, a, b'b'), b, b'a');
    assert_eq!(decode(&swapped), "object.metadata");
    // An extent of zero bytes, at the very end.
    let zero = with(&bytes, bytes.len() - 4, 0);
    assert_eq!(decode(&zero), "object.extents");
    // An invalid payload tag: the byte before the extent count.
    assert_eq!(decode(&with(&bytes, bytes.len() - 25, 9)), "object.payload");
    // A checksum algorithm that does not exist: find the CRC32 code.
    let crc = bytes
        .windows(7)
        .position(|w| w == [1, 1, 2, 3, 4, 2, 0])
        .unwrap();
    assert_eq!(decode(&with(&bytes, crc, 99)), "object.checksums");
    // A composite checksum of more than 10,000 parts (0x2711 = 10,001).
    let mut part_count = bytes.clone();
    part_count[crc + 5] = 0x11;
    part_count[crc + 6] = 0x27;
    assert_eq!(decode(&part_count), "object.checksums");
}

#[test]
fn rejects_values_it_cannot_encode() {
    let mut entry = sample_entry();
    let object = entry.object.as_mut().unwrap();
    object.checksums.insert(
        ChecksumAlgorithm::Sha1,
        Checksum::full_object(ChecksumAlgorithm::Crc32, &[0; 4]).unwrap(),
    );
    assert_eq!(
        codec::encode_entry(&entry).unwrap_err().field(),
        "object.checksums"
    );

    let mut entry = sample_entry();
    entry.object.as_mut().unwrap().payload = Payload::Extents(vec![ExtentRef {
        position: EpochSeq::new(Epoch::new(1), Seq::new(1)),
        len: 0,
    }]);
    assert_eq!(
        codec::encode_entry(&entry).unwrap_err().field(),
        "object.extents"
    );

    let mut entry = sample_entry();
    entry.object.as_mut().unwrap().metadata = BTreeMap::from([("x".into(), "y".repeat(9000))]);
    assert_eq!(
        codec::encode_entry(&entry).unwrap_err().field(),
        "object.metadata"
    );

    let summary = SegmentSummary::starting_at(5);
    assert_eq!(
        codec::encode_summary(&summary).unwrap_err().field(),
        "coverage"
    );

    let control = ControlEntry {
        generation: Generation::new(1),
        version: "v".repeat(2000),
        value: Vec::new(),
    };
    assert_eq!(
        codec::encode_control(&control).unwrap_err().field(),
        "control.version"
    );
    let error = codec::encode_control(&control).unwrap_err();
    assert!(
        error.to_string().starts_with("control.version: "),
        "{error}"
    );
}

#[test]
fn rejects_broken_keys_and_values() {
    // A shard with an invalid bucket ID, an empty object key, and bytes
    // that are not UTF-8.
    assert!(codec::decode_shard_key(&[2, b'A', b'B', 0]).is_err());
    assert!(codec::decode_shard_key(&[1, b'a', 0, 0]).is_err());
    assert!(codec::decode_entry_key(&[1, b'a', 0]).is_err());
    assert!(codec::decode_entry_key(&[1, b'a', 0, 0xff]).is_err());
    assert!(codec::decode_coverage_key(&[1, b'-', 0, 0, 0, 0, 0, 0, 0, 0]).is_err());
    // A summary whose count promises more shards than its bytes hold.
    let mut summary = vec![VALUE_FORMAT];
    summary.extend_from_slice(&0u64.to_le_bytes());
    summary.extend_from_slice(&1000u32.to_le_bytes());
    assert_eq!(
        codec::decode_summary(&summary).unwrap_err().field(),
        "coverage.positions"
    );
    // A control value longer than its bytes.
    let mut control = codec::encode_control(&ControlEntry {
        generation: Generation::new(1),
        version: String::new(),
        value: vec![1],
    })
    .unwrap();
    control.pop();
    assert_eq!(
        codec::decode_control(&control).unwrap_err().field(),
        "control.value"
    );
}

fn upload_checksum() -> impl Strategy<Value = Option<UploadChecksum>> {
    option::of(
        (
            proptest::sample::select(ChecksumAlgorithm::FLEXIBLE.to_vec()),
            any::<bool>(),
        )
            .prop_map(|(algorithm, composite)| UploadChecksum {
                algorithm,
                checksum_type: if composite {
                    ChecksumType::Composite
                } else {
                    ChecksumType::FullObject
                },
            })
            .prop_filter("defined by S3", |checksum| checksum.is_valid()),
    )
}

fn upload() -> impl Strategy<Value = Upload> {
    (
        any::<u64>(),
        btree_map("[a-z-]{1,12}", text(20), 0..4),
        btree_map(
            text(10).prop_filter("nonempty", |t| !t.is_empty()),
            text(10),
            0..4,
        ),
        upload_checksum(),
    )
        .prop_map(|(initiated_ms, metadata, tags, checksum)| Upload {
            initiated_ms,
            metadata,
            tags,
            checksum,
        })
}

fn part() -> impl Strategy<Value = Part> {
    (
        (position(), any::<u64>(), any::<u64>(), etag(), checksums()),
        prop_oneof![
            position().prop_map(Payload::Inline),
            vec(
                (position(), 1..=skys3_log::record::MAX_PAYLOAD_LEN)
                    .prop_map(|(position, len)| ExtentRef { position, len }),
                0..8
            )
            .prop_map(Payload::Extents),
        ],
    )
        .prop_map(
            |((position, size, last_modified_ms, etag, checksums), payload)| Part {
                position,
                size,
                last_modified_ms,
                etag,
                checksums,
                payload,
            },
        )
}

proptest! {
    #[test]
    fn uploads_and_parts_round_trip(upload in upload(), part in part()) {
        let bytes = codec::encode_upload(&upload).unwrap();
        prop_assert_eq!(bytes[0], VALUE_FORMAT);
        prop_assert_eq!(codec::decode_upload(&bytes).unwrap(), upload);
        prop_assert!(codec::decode_upload(&bytes[..bytes.len() - 1]).is_err());
        // Formats before 3 have no uploads.
        prop_assert!(codec::decode_upload(&with(&bytes, 0, 2)).is_err());

        let bytes = codec::encode_part(&part).unwrap();
        prop_assert_eq!(codec::decode_part(&bytes).unwrap(), part);
        prop_assert!(codec::decode_part(&bytes[..bytes.len() - 1]).is_err());
        prop_assert!(codec::decode_part(&with(&bytes, 0, 2)).is_err());
    }

    #[test]
    fn upload_and_part_keys_round_trip_and_sort(
        shard in shard(),
        keys in (".{1,20}", ".{1,20}"),
        uploads in (position(), position()),
        numbers in (any::<u16>(), any::<u16>()),
    ) {
        let (a, b) = (
            codec::upload_key(&shard, &keys.0, uploads.0),
            codec::upload_key(&shard, &keys.1, uploads.1),
        );
        prop_assert_eq!(
            codec::decode_upload_key(&a).unwrap(),
            (shard.clone(), keys.0.clone(), uploads.0)
        );
        // Keys without a zero byte sort by key, then by upload.
        if !keys.0.contains('\0') && !keys.1.contains('\0') {
            prop_assert_eq!(a.cmp(&b), (&keys.0, uploads.0).cmp(&(&keys.1, uploads.1)));
        }
        let (pa, pb) = (
            codec::part_key(&shard, uploads.0, numbers.0),
            codec::part_key(&shard, uploads.0, numbers.1),
        );
        prop_assert!(pa.starts_with(&codec::upload_parts_prefix(&shard, uploads.0)));
        prop_assert_eq!(
            codec::decode_part_key(&pa).unwrap(),
            (shard.clone(), uploads.0, numbers.0)
        );
        prop_assert_eq!(pa.cmp(&pb), numbers.0.cmp(&numbers.1));
    }

    #[test]
    fn arbitrary_upload_bytes_decode_canonically(mut bytes in vec(any::<u8>(), 0..120)) {
        if !bytes.is_empty() {
            bytes[0] = VALUE_FORMAT;
        }
        if let Ok(upload) = codec::decode_upload(&bytes) {
            prop_assert_eq!(codec::encode_upload(&upload).unwrap(), bytes.clone());
        }
        if let Ok(part) = codec::decode_part(&bytes) {
            prop_assert_eq!(codec::encode_part(&part).unwrap(), bytes.clone());
        }
        if let Ok((shard, key, upload)) = codec::decode_upload_key(&bytes) {
            prop_assert_eq!(codec::upload_key(&shard, &key, upload), bytes.clone());
        }
        if let Ok((shard, upload, number)) = codec::decode_part_key(&bytes) {
            prop_assert_eq!(codec::part_key(&shard, upload, number), bytes.clone());
        }
    }
}

#[test]
fn multipart_values_are_validated() {
    // A multipart payload exists from format 3 on.
    let mut entry = sample_entry();
    let object = entry.object.as_mut().unwrap();
    object.payload = Payload::Parts {
        upload: EpochSeq::new(Epoch::new(1), Seq::new(1)),
        parts: vec![
            ObjectPart { number: 1, size: 5 },
            ObjectPart { number: 3, size: 1 },
        ],
    };
    let bytes = codec::encode_entry(&entry).unwrap();
    assert_eq!(codec::decode_entry(&bytes).unwrap(), entry);
    assert_eq!(
        codec::decode_entry(&with(&bytes, 0, 2))
            .unwrap_err()
            .field(),
        "object.payload"
    );
    // Parts in increasing order, numbered from 1 to 10,000.
    for parts in [
        vec![(2, 1), (1, 1)],
        vec![(1, 1), (1, 1)],
        vec![(0, 1)],
        vec![(10_001, 1)],
    ] {
        let object = entry.object.as_mut().unwrap();
        object.payload = Payload::Parts {
            upload: EpochSeq::new(Epoch::new(1), Seq::new(1)),
            parts: parts
                .into_iter()
                .map(|(number, size)| ObjectPart { number, size })
                .collect(),
        };
        assert_eq!(
            codec::encode_entry(&entry).unwrap_err().field(),
            "object.parts"
        );
    }
    // A part's bytes are inline or in extents.
    let part = Part {
        position: EpochSeq::new(Epoch::new(1), Seq::new(2)),
        size: 0,
        last_modified_ms: 0,
        etag: ETag::new("e").unwrap(),
        checksums: BTreeMap::new(),
        payload: Payload::None,
    };
    assert_eq!(
        codec::encode_part(&part).unwrap_err().field(),
        "part.payload"
    );
    let mut bytes = codec::encode_part(&Part {
        payload: Payload::Inline(part.position),
        ..part.clone()
    })
    .unwrap();
    let tag = bytes.len() - 17;
    bytes.truncate(tag + 1);
    bytes[tag] = 0;
    assert_eq!(
        codec::decode_part(&bytes).unwrap_err().field(),
        "part.payload"
    );
    // An upload's checksum is one S3 defines for multipart uploads.
    let upload = Upload {
        initiated_ms: 0,
        metadata: BTreeMap::new(),
        tags: BTreeMap::new(),
        checksum: Some(UploadChecksum {
            algorithm: ChecksumAlgorithm::Sha1,
            checksum_type: ChecksumType::FullObject,
        }),
    };
    assert_eq!(
        codec::encode_upload(&upload).unwrap_err().field(),
        "upload.checksum"
    );
    let valid = codec::encode_upload(&Upload {
        checksum: UploadChecksum::of(ChecksumAlgorithm::Sha1),
        ..upload
    })
    .unwrap();
    let last = valid.len() - 1;
    for (at, value) in [(last, 0), (last, 7), (last - 1, 99)] {
        assert_eq!(
            codec::decode_upload(&with(&valid, at, value))
                .unwrap_err()
                .field(),
            "upload.checksum"
        );
    }
    // Upload keys need their separator and position.
    let key = codec::upload_key(
        &ShardRef::new(BucketId::new("b").unwrap(), ShardId::new(0)),
        "k",
        EpochSeq::new(Epoch::new(1), Seq::new(2)),
    );
    assert!(codec::decode_upload_key(&key[..key.len() - 1]).is_err());
    assert!(codec::decode_upload_key(&with(&key, key.len() - 17, 1)).is_err());
    let no_key = [&key[..3], &key[key.len() - 17..]].concat();
    assert!(codec::decode_upload_key(&no_key).is_err());
}
