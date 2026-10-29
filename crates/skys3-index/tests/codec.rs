//! Property tests for the index's key and value encodings.

use std::collections::BTreeMap;

use proptest::collection::{btree_map, vec};
use proptest::option;
use proptest::prelude::*;
use skys3_index::codec::{self, VALUE_FORMAT};
use skys3_index::{ControlEntry, Entry, EntryState, ObjectVersion, Payload};
use skys3_log::record::{ChecksumAlgorithm, CopySource, ExtentRef, ShardRef};
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

fn checksums() -> impl Strategy<Value = BTreeMap<ChecksumAlgorithm, Vec<u8>>> {
    proptest::sample::subsequence(ChecksumAlgorithm::ALL.to_vec(), 0..=6).prop_flat_map(|algs| {
        algs.into_iter()
            .map(|a| vec(any::<u8>(), a.digest_len()).prop_map(move |d| (a, d)))
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
            checksums: BTreeMap::from([(ChecksumAlgorithm::Crc32, vec![1, 2, 3, 4])]),
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

#[test]
fn rejects_broken_entries() {
    let bytes = codec::encode_entry(&sample_entry()).unwrap();
    let decode = |bytes: &[u8]| codec::decode_entry(bytes).unwrap_err().field();
    assert_eq!(decode(&[]), "entry");
    assert_eq!(decode(&with(&bytes, 0, 2)), "entry");
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
    let crc = bytes.windows(5).position(|w| w == [1, 1, 2, 3, 4]).unwrap();
    assert_eq!(decode(&with(&bytes, crc, 99)), "object.checksums");
}

#[test]
fn rejects_values_it_cannot_encode() {
    let mut entry = sample_entry();
    let object = entry.object.as_mut().unwrap();
    object.checksums.insert(ChecksumAlgorithm::Sha1, vec![0; 3]);
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
