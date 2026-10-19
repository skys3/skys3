//! The fragment record format: round trips, canonical encoding, the
//! classification of decoding errors, the checks on headers, and a golden
//! record that freezes the format.

mod support;

use std::collections::BTreeMap;

use bytes::Bytes;
use proptest::prelude::*;
use skys3_ec::fragment::{
    BLOCK_LEN, FIXED_LEN, FragmentDecodeError, FragmentHeader, FragmentRecord, MAGIC, ObjectMeta,
    PartSize, StripeInfo,
};
use skys3_ec::{AttemptId, CodecId, FragmentId, Geometry, current_codec};
use skys3_log::record::{ErrorClass, ShardRef};
use skys3_types::checksum::{Checksum, ChecksumAlgorithm};
use skys3_types::{BucketId, ETag, Epoch, ShardId};
use support::{header, multipart, position, sample};

fn record(header: FragmentHeader, payload: Vec<u8>) -> FragmentRecord {
    FragmentRecord {
        id: FragmentId::new(0x0100_0000_0000_0002_0000_0000_0000_0040),
        header,
        payload: payload.into(),
    }
}

/// Rewrites the header CRC of `bytes` after a deliberate change.
fn reseal(bytes: &mut [u8]) {
    let header_len = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
    let crc = crc32c::crc32c(&bytes[8..header_len]);
    bytes[4..8].copy_from_slice(&crc.to_le_bytes());
}

/// A deliberate change to an encoded record.
type ByteChange = Box<dyn Fn(&mut Vec<u8>)>;

/// A deliberate change to a header.
type HeaderChange = Box<dyn Fn(&mut FragmentHeader)>;

fn sample_record() -> FragmentRecord {
    record(header("photos/cat.jpg", 128, 5), sample(128, 3))
}

#[test]
fn records_round_trip_canonically() {
    let original = sample_record();
    let bytes = original.to_bytes().unwrap();
    assert_eq!(&bytes[..4], &MAGIC);
    let (decoded, len) = FragmentRecord::decode(&bytes).unwrap();
    assert_eq!(decoded, original);
    assert_eq!(len, bytes.len());
    assert_eq!(decoded.to_bytes().unwrap(), bytes);
    // Trailing bytes belong to the next record.
    let mut two = bytes.to_vec();
    two.extend_from_slice(&bytes);
    assert_eq!(FragmentRecord::decode(&two).unwrap().1, bytes.len());
}

/// Freezes format version 1. A change here is a format change: it needs a
/// new version, and readers of the old one (§10.1).
#[test]
fn the_format_is_frozen() {
    let mut header = header("k", 64, 1);
    header.object.metadata = BTreeMap::new();
    header.object.tags = BTreeMap::new();
    header.object.checksums = BTreeMap::new();
    let bytes = record(header, vec![0x5a; 64]).to_bytes().unwrap();
    // The fixed header, field by field.
    assert_eq!(&bytes[0..4], b"SKYF");
    assert_eq!(&bytes[8..12], [1, 0, 0, 0], "version and reserved");
    assert_eq!(&bytes[12..16], (bytes.len() as u32 - 64).to_le_bytes());
    assert_eq!(&bytes[16..24], 64u64.to_le_bytes());
    assert_eq!(
        &bytes[24..40],
        0x0100_0000_0000_0002_0000_0000_0000_0040u128.to_le_bytes()
    );
    // The whole record.
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(hex, GOLDEN);
}

/// The record of [`the_format_is_frozen`], in hexadecimal.
const GOLDEN: &str = concat!(
    "534b5946a263092601000000bc00000040000000000000004000000000000000",
    "020000000000000106622d376633610301006b04000000000000001200000000",
    "0000000400000000000000010000000000000000000000010000000000000000",
    "00000000010000000000000402010001000100000000000000a8da769b010000",
    "2000396232636635333566323737333163393734333433363435613339383533",
    "32380400000000000000110000000000000000000000000060acc3815a5a5a5a",
    "5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a",
    "5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a",
);

#[test]
fn every_prefix_is_incomplete() {
    let bytes = sample_record().to_bytes().unwrap();
    for len in 0..bytes.len() {
        let error = FragmentRecord::decode(&bytes[..len]).unwrap_err();
        assert_eq!(error.class(), ErrorClass::Incomplete, "{len}: {error}");
    }
}

#[test]
fn every_flipped_bit_is_caught() {
    let bytes = sample_record().to_bytes().unwrap();
    for at in 0..bytes.len() {
        for bit in [0, 3, 7] {
            let mut flipped = bytes.to_vec();
            flipped[at] ^= 1 << bit;
            let error = FragmentRecord::decode(&flipped).unwrap_err();
            let class = error.class();
            // A changed version is a newer format, and a longer header
            // needs more bytes; everything else fails a check.
            let expected = match at {
                8 | 9 => ErrorClass::Unsupported,
                _ => ErrorClass::Corrupt,
            };
            assert!(
                class == expected || class == ErrorClass::Incomplete,
                "byte {at} bit {bit}: {error}"
            );
        }
    }
}

#[test]
fn the_framing_is_checked_before_the_crc() {
    let bytes = sample_record().to_bytes().unwrap().to_vec();
    let mut bad = bytes.clone();
    bad[0] = b'X';
    assert_eq!(
        FragmentRecord::decode(&bad),
        Err(FragmentDecodeError::BadMagic)
    );
    let mut bad = bytes.clone();
    bad[16..24].copy_from_slice(&0u64.to_le_bytes());
    let error = FragmentRecord::decode(&bad).unwrap_err();
    assert!(matches!(
        error,
        FragmentDecodeError::FrameLength {
            field: "payload_len",
            ..
        }
    ));
    let mut bad = bytes.clone();
    bad[12..16].copy_from_slice(&39u32.to_le_bytes());
    let error = FragmentRecord::decode(&bad).unwrap_err();
    assert!(matches!(
        error,
        FragmentDecodeError::FrameLength {
            field: "header_len",
            ..
        }
    ));
    assert_eq!(error.class(), ErrorClass::Corrupt);
    let mut bad = bytes.clone();
    bad[5] ^= 1;
    assert!(matches!(
        FragmentRecord::decode(&bad),
        Err(FragmentDecodeError::ChecksumMismatch { .. })
    ));
    let mut bad = bytes;
    let last = bad.len() - 1;
    bad[last] ^= 1;
    let error = FragmentRecord::decode(&bad).unwrap_err();
    assert!(
        matches!(error, FragmentDecodeError::BlockMismatch { block: 0, .. }),
        "{error}"
    );
    assert!(error.to_string().contains("block 0"));
}

#[test]
fn malformed_headers_under_a_valid_crc_are_invalid() {
    let original = sample_record();
    let bytes = original.to_bytes().unwrap().to_vec();
    let header_len = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
    // Offsets of fields in the sample's header.
    let shard_len = 1 + 6 + 1;
    let key = FIXED_LEN + shard_len;
    let stripe = key + 2 + 14 + 16 + 16;
    let index = stripe + 24 + 2 + 2;
    let cases: Vec<(&str, ByteChange)> = vec![
        ("reserved", Box::new(|b| b[11] = 1)),
        ("shard.bucket", Box::new(|b| b[FIXED_LEN + 1] = b'B')),
        ("key", Box::new(move |b| b[key + 2] = 0xff)),
        ("stripe.number", Box::new(move |b| b[stripe] = 1)),
        ("stripe.geometry", Box::new(move |b| b[stripe + 24] = 0)),
        ("index", Box::new(move |b| b[index] = 6)),
        // A payload length that is not the stripe's fragment length.
        ("payload", Box::new(move |b| b[stripe + 17] = 1)),
        // Bytes left over after the block checksums.
        (
            "blocks",
            Box::new(move |b| {
                b[12..16].copy_from_slice(&((header_len + 4) as u32).to_le_bytes());
                b.splice(header_len..header_len, [0; 4]);
            }),
        ),
    ];
    for (field, change) in cases {
        let mut bad = bytes.clone();
        change(&mut bad);
        reseal(&mut bad);
        let error = FragmentRecord::decode(&bad).unwrap_err();
        assert_eq!(error.class(), ErrorClass::Invalid, "{field}: {error}");
        assert!(error.to_string().contains(field), "{field}: {error}");
    }
}

#[test]
fn the_encoder_refuses_what_the_decoder_would() {
    let base = header("k", 64, 0);
    let changes: Vec<(&str, HeaderChange)> = vec![
        ("key", Box::new(|h| h.key.clear())),
        ("key", Box::new(|h| h.key = "k".repeat(1025))),
        ("stripe.count", Box::new(|h| h.stripe.count = 0)),
        ("stripe.number", Box::new(|h| h.stripe.number = 1)),
        ("stripe.data_len", Box::new(|h| h.stripe.data_len = 0)),
        (
            "stripe.codec",
            Box::new(|h| h.stripe.codec = CodecId::new(0)),
        ),
        ("stripe.offset", Box::new(|h| h.stripe.offset = 1)),
        ("stripe.offset", Box::new(|h| h.stripe.offset = u64::MAX)),
        ("index", Box::new(|h| h.index = 6)),
        (
            "object.parts",
            Box::new(|h| {
                h.object = multipart(64, 4);
                h.object.parts[3].size = 65;
            }),
        ),
        (
            "object.parts",
            Box::new(|h| {
                h.object = multipart(64, 4);
                h.object.parts[1].number = 1;
            }),
        ),
        (
            "object.parts",
            Box::new(|h| {
                h.object = multipart(64, 4);
                h.object.parts[0].number = 0;
            }),
        ),
        (
            "object.metadata",
            Box::new(|h| {
                h.object.metadata.insert("Content-Type".into(), "x".into());
            }),
        ),
        (
            "object.metadata",
            Box::new(|h| {
                h.object
                    .metadata
                    .insert("x-amz-meta-big".into(), "v".repeat(8192));
            }),
        ),
        (
            "object.tags",
            Box::new(|h| {
                h.object.tags = (0..51).map(|n| (n.to_string(), String::new())).collect();
            }),
        ),
        (
            "object.checksums",
            Box::new(|h| {
                let crc = Checksum::full_object(ChecksumAlgorithm::Crc32, &[0; 4]).unwrap();
                h.object.checksums.insert(ChecksumAlgorithm::Sha256, crc);
            }),
        ),
    ];
    for (field, change) in changes {
        let mut header = base.clone();
        change(&mut header);
        let error = record(header, vec![0; 64]).to_bytes().unwrap_err();
        assert!(error.to_string().contains(field), "{field}: {error}");
    }
    // A payload that is not the stripe's fragment length, or none.
    for payload in [vec![0; 128], Vec::new()] {
        let error = record(base.clone(), payload).to_bytes().unwrap_err();
        assert!(error.to_string().contains("payload"), "{error}");
    }
}

#[test]
fn a_codec_this_build_does_not_know_allows_any_length() {
    let mut header = header("k", 64, 0);
    header.stripe.codec = CodecId::new(9);
    let record = record(header, vec![1; 1000]);
    let bytes = record.to_bytes().unwrap();
    assert_eq!(FragmentRecord::decode(&bytes).unwrap().0, record);
}

fn arb_text(max: usize) -> impl Strategy<Value = String> {
    proptest::collection::vec(proptest::char::range('a', 'z'), 1..=max)
        .prop_map(|chars| chars.into_iter().collect())
}

prop_compose! {
    fn arb_object()(
        etag in "[0-9a-f]{32}(-[1-9])?",
        metadata in proptest::collection::btree_map("x-amz-meta-[a-z]{1,8}", "[ -~]{0,20}", 0..4),
        tags in proptest::collection::btree_map(arb_text(10), "[ -~]{0,20}", 0..4),
        crc in proptest::option::of(any::<[u8; 4]>()),
        composite in 0u32..3,
        last_modified_ms in any::<u64>(),
        identity in (any::<u64>(), any::<u64>()),
    ) -> ObjectMeta {
        let mut checksums = BTreeMap::new();
        if let Some(crc) = crc {
            let checksum = match composite {
                0 => Checksum::full_object(ChecksumAlgorithm::Crc32c, &crc),
                parts => Checksum::composite(ChecksumAlgorithm::Crc32c, &crc, parts),
            };
            checksums.insert(ChecksumAlgorithm::Crc32c, checksum.unwrap());
        }
        ObjectMeta {
            size: 0,
            last_modified_ms,
            etag: ETag::new(etag).unwrap(),
            identity: position(identity.0, identity.1),
            metadata,
            tags,
            checksums,
            parts: Vec::new(),
        }
    }
}

prop_compose! {
    fn arb_record()(
        k in 1usize..12,
        m in 1usize..5,
        index_seed in any::<u8>(),
        data_len in 1u64..20_000,
        before in 0u64..1_000_000,
        after in 0u64..1_000_000,
        number_seed in any::<u32>(),
        count in 1u32..1000,
        parts in 0u16..4,
        bucket in "[a-z0-9]{1,25}",
        shard in any::<u8>(),
        key in "[ -~]{1,40}",
        attempt in (any::<u64>(), any::<u64>()),
        version in (any::<u64>(), any::<u64>()),
        object in arb_object(),
        seed in any::<u64>(),
    ) -> FragmentRecord {
        let geometry = Geometry::new(k, m).unwrap();
        let size = before + data_len + after;
        let mut object = ObjectMeta { size, ..object };
        if parts > 0 {
            // Every part gets an equal share; the last takes the rest.
            let share = size / u64::from(parts);
            object.parts = (1..=parts)
                .map(|number| PartSize { number, size: share })
                .collect();
            object.parts.last_mut().unwrap().size = size - share * u64::from(parts - 1);
        }
        let header = FragmentHeader {
            shard: ShardRef::new(BucketId::new(bucket).unwrap(), ShardId::new(shard)),
            key,
            version: position(version.0, version.1),
            attempt: AttemptId::new(Epoch::new(attempt.0), attempt.1),
            stripe: StripeInfo {
                number: number_seed % count,
                count,
                offset: before,
                data_len,
                geometry,
                codec: CodecId::REED_SOLOMON_V1,
            },
            index: (usize::from(index_seed) % geometry.total_fragments()) as u8,
            object,
        };
        let len = current_codec().fragment_len(geometry, data_len).unwrap();
        FragmentRecord {
            id: FragmentId::new(u128::from(seed) << 32 | u128::from(seed >> 7)),
            header,
            payload: Bytes::from(sample(len as usize, seed)),
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn any_valid_record_round_trips(record in arb_record()) {
        let bytes = record.to_bytes().unwrap();
        let (decoded, len) = FragmentRecord::decode(&bytes).unwrap();
        prop_assert_eq!(len, bytes.len());
        prop_assert_eq!(decoded.to_bytes().unwrap(), bytes.clone());
        prop_assert_eq!(decoded, record.clone());
        let blocks = (record.payload.len() as u64).div_ceil(BLOCK_LEN);
        let header_len = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
        prop_assert!(u64::from(header_len) > FIXED_LEN as u64 + 4 * blocks);
    }

    #[test]
    fn arbitrary_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
        if let Ok((record, len)) = FragmentRecord::decode(&bytes) {
            prop_assert_eq!(&record.to_bytes().unwrap()[..], &bytes[..len]);
        }
    }

    #[test]
    fn damaged_records_never_decode_as_others(
        record in arb_record(),
        at_seed in any::<usize>(),
        byte in 1u8..=255,
    ) {
        let bytes = record.to_bytes().unwrap();
        let mut damaged = bytes.to_vec();
        let at = at_seed % damaged.len();
        damaged[at] ^= byte;
        prop_assert!(FragmentRecord::decode(&damaged).is_err());
    }
}
