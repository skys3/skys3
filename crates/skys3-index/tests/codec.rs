//! Property tests for the index's key and value encodings.

use std::collections::BTreeMap;

use proptest::collection::{btree_map, vec};
use proptest::option;
use proptest::prelude::*;
use skys3_index::codec::{self, MIN_VALUE_FORMAT, VALUE_FORMAT};
use skys3_index::{ControlEntry, Entry, EntryState, ObjectVersion, Payload};
use skys3_log::record::{Checksum, ChecksumAlgorithm, Checksums, CopySource, ExtentRef, ShardRef};
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
    // It re-encodes in format 2: a part count, zero, after each digest.
    let v2 = codec::encode_entry(&entry).unwrap();
    let mut expected_v2 = ENTRY_V1.replacen("01", "02", 1);
    expected_v2 = expected_v2.replace("0101020304", "01010203040000");
    expected_v2 = expected_v2.replace("0c0d0e0f", "0c0d0e0f0000");
    assert_eq!(v2, unhex(&expected_v2));
    assert_eq!(codec::decode_entry(&v2).unwrap(), entry);
    // Format 2 bytes read as format 1 leave the part counts unread.
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
    for format in [0, 3, u8::MAX] {
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
