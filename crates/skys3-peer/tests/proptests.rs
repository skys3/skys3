//! Round trips of every message through its frame, and the properties of
//! version and capability negotiation.

use std::collections::BTreeMap;

use bytes::Bytes;
use proptest::prelude::*;
use skys3_peer::{
    Abort, AbortReason, Applied, ApplyError, Batch, Begin, ByteRanges, Capabilities, Commit, Data,
    Hello, MAX_PIECE_BYTES, Message, Outcome, PREFIX_LEN, Precondition, Put, PutData, StagedPart,
    StagedRanges, VersionRange, Write, negotiate,
};
use skys3_types::checksum::{Checksum, ChecksumAlgorithm, Checksums};
use skys3_types::{
    BucketId, BucketName, ClusterId, ETag, Epoch, EpochSeq, Seq, ShardId, WriteIdentity,
};

fn clusters() -> impl Strategy<Value = ClusterId> {
    "[a-z][a-z0-9-]{0,22}[a-z0-9]".prop_map(|s| ClusterId::new(s).unwrap())
}

fn identities() -> BoxedStrategy<WriteIdentity> {
    (
        clusters(),
        "[a-z0-9]{1,25}",
        any::<u8>(),
        any::<u64>(),
        any::<u64>(),
    )
        .prop_map(|(cluster, bucket, shard, epoch, seq)| {
            WriteIdentity::new(
                cluster,
                BucketId::new(bucket).unwrap(),
                ShardId::new(shard),
                EpochSeq::new(Epoch::new(epoch), Seq::new(seq)),
            )
        })
        .boxed()
}

fn buckets() -> impl Strategy<Value = BucketName> {
    "[a-z0-9][a-z0-9-]{1,40}[a-z0-9]".prop_map(|s| BucketName::new(s).unwrap())
}

fn keys() -> impl Strategy<Value = String> {
    ".{1,40}"
}

fn text() -> impl Strategy<Value = String> {
    ".{0,30}"
}

fn bytes(max: usize) -> impl Strategy<Value = Bytes> {
    prop::collection::vec(any::<u8>(), 0..max).prop_map(Bytes::from)
}

fn etags() -> impl Strategy<Value = ETag> {
    "[0-9a-f]{32}(-[1-9][0-9]{0,3})?".prop_map(|s| ETag::new(s).unwrap())
}

fn checksums() -> impl Strategy<Value = Checksums> {
    prop::collection::vec(
        (
            prop::sample::select(ChecksumAlgorithm::ALL.to_vec()),
            any::<[u8; 32]>(),
            0u32..=10_000,
        ),
        0..4,
    )
    .prop_map(|entries| {
        entries
            .into_iter()
            .map(|(algorithm, digest, parts)| {
                let digest = &digest[..algorithm.digest_len()];
                let checksum = if parts > 0 && algorithm.supports_composite() {
                    Checksum::composite(algorithm, digest, parts).unwrap()
                } else {
                    Checksum::full_object(algorithm, digest).unwrap()
                };
                (algorithm, checksum)
            })
            .collect()
    })
}

fn parts() -> impl Strategy<Value = Vec<StagedPart>> {
    (
        any::<u64>(),
        prop::collection::btree_map(
            1u16..=10_000,
            (0..=MAX_PIECE_BYTES, any::<[u8; 16]>()),
            1..8,
        ),
    )
        .prop_map(|(first_piece, parts)| {
            parts
                .into_iter()
                .zip(0u64..)
                .map(|((number, (size, md5)), index)| StagedPart {
                    number,
                    piece: first_piece.wrapping_add(index),
                    size,
                    md5,
                })
                .collect()
        })
}

/// A version's bytes: staged in one piece or by parts, or inline.
fn put_data(inline: bool) -> BoxedStrategy<PutData> {
    if inline {
        bytes(256).prop_map(PutData::Inline).boxed()
    } else {
        prop_oneof![
            any::<u64>().prop_map(|piece| PutData::Staged { piece }),
            parts().prop_map(PutData::Multipart),
        ]
        .boxed()
    }
}

fn puts(inline: bool) -> impl Strategy<Value = Put> {
    (
        0..=MAX_PIECE_BYTES,
        etags(),
        any::<u64>(),
        prop::collection::btree_map("[a-z][a-z0-9-]{0,15}", text(), 0..5),
        prop::collection::btree_map(".{1,10}", text(), 0..5),
        checksums(),
        put_data(inline),
    )
        .prop_map(
            |(size, etag, last_modified_ms, metadata, tags, checksums, data)| {
                let size = match &data {
                    PutData::Staged { .. } => size,
                    PutData::Multipart(parts) => parts.iter().map(|p| p.size).sum(),
                    PutData::Inline(bytes) => bytes.len() as u64,
                };
                Put {
                    size,
                    etag,
                    last_modified_ms,
                    metadata,
                    tags,
                    checksums,
                    data,
                }
            },
        )
}

fn commits(inline: bool) -> impl Strategy<Value = Commit> {
    let precondition = prop_oneof![
        Just(Precondition::Absent),
        Just(Precondition::Unconditional),
        identities().prop_map(Precondition::Matches),
    ];
    let write = prop_oneof![
        1 => Just(Write::Delete),
        4 => puts(inline).prop_map(Write::Put),
    ];
    (identities(), buckets(), keys(), precondition, write).prop_map(
        |(identity, bucket, key, precondition, write)| Commit {
            identity,
            bucket,
            key,
            precondition,
            write,
        },
    )
}

fn staged_ranges() -> impl Strategy<Value = StagedRanges> {
    let piece_ranges =
        prop::collection::vec((0..MAX_PIECE_BYTES, 1u64..1 << 20), 0..6).prop_map(|ranges| {
            ranges
                .into_iter()
                .map(|(start, len)| start..(start + len).min(MAX_PIECE_BYTES))
                .collect::<ByteRanges>()
        });
    (
        identities(),
        prop::collection::btree_map(any::<u64>(), piece_ranges, 0..6),
    )
        .prop_map(|(identity, pieces)| StagedRanges { identity, pieces })
}

fn messages() -> impl Strategy<Value = Message> {
    let hello = (clusters(), 1u16.., any::<u16>(), any::<u64>()).prop_map(
        |(cluster, min, extra, capabilities)| {
            Message::Hello(Hello {
                cluster,
                versions: VersionRange::new(min, min.saturating_add(extra)).unwrap(),
                capabilities: Capabilities::from_bits(capabilities),
            })
        },
    );
    let begin = (identities(), buckets(), keys()).prop_map(|(identity, bucket, key)| {
        Message::Begin(Begin {
            identity,
            bucket,
            key,
        })
    });
    let data = (any::<u64>(), 0..MAX_PIECE_BYTES - 1024, bytes(1024)).prop_map(
        |(piece, offset, bytes)| {
            let bytes = if bytes.is_empty() {
                Bytes::from_static(b"x")
            } else {
                bytes
            };
            Message::Data(Data {
                piece,
                offset,
                bytes,
            })
        },
    );
    let outcome = prop_oneof![
        prop::option::of(etags()).prop_map(|etag| Outcome::Committed { etag }),
        prop::option::of(identities()).prop_map(|current| Outcome::PreconditionFailed { current }),
        (
            prop::sample::select(vec![
                ApplyError::Incomplete,
                ApplyError::ChecksumMismatch,
                ApplyError::Refused,
                ApplyError::Unavailable,
            ]),
            text(),
        )
            .prop_map(|(error, reason)| Outcome::Failed { error, reason }),
    ];
    let applied = (identities(), outcome)
        .prop_map(|(identity, outcome)| Message::Applied(Applied { identity, outcome }));
    let batch = prop::collection::vec(commits(true), 1..6).prop_map(|items| {
        // Keys and write identities are distinct within a batch: the
        // index makes each key and each identity's seq unique.
        let items = items
            .into_iter()
            .zip(0u64..)
            .map(|(mut item, index)| {
                item.key = format!("{index}/{}", item.key);
                item.identity.position.seq = Seq::new(index);
                item
            })
            .collect();
        Message::Batch(Batch { items })
    });
    let abort = (
        identities(),
        prop::sample::select(vec![
            AbortReason::Cancelled,
            AbortReason::Expired,
            AbortReason::QuotaExceeded,
            AbortReason::Refused,
        ]),
        text(),
    )
        .prop_map(|(identity, reason, detail)| {
            Message::Abort(Abort {
                identity,
                reason,
                detail,
            })
        });
    prop_oneof![
        hello,
        begin,
        data,
        staged_ranges().prop_map(Message::Durable),
        staged_ranges().prop_map(Message::Resume),
        commits(false).prop_map(Message::Commit),
        applied,
        batch,
        abort,
    ]
}

fn hellos() -> impl Strategy<Value = Hello> {
    (clusters(), 1u16..20, 0u16..20, any::<u64>()).prop_map(|(cluster, min, extra, bits)| Hello {
        cluster,
        versions: VersionRange::new(min, min + extra).unwrap(),
        capabilities: Capabilities::from_bits(bits),
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn messages_round_trip(message in messages()) {
        let bytes = message.encode().unwrap();
        prop_assert_eq!(
            Message::decode(&bytes).unwrap(),
            Some((message.clone(), bytes.len()))
        );
        // Through a reader that splits the frame at its prefix.
        let prefix = bytes.first_chunk::<PREFIX_LEN>().unwrap();
        let (header_len, payload_len) = skys3_peer::parse_prefix(prefix).unwrap();
        prop_assert_eq!(PREFIX_LEN + header_len + payload_len, bytes.len());
        let header = &bytes[PREFIX_LEN..PREFIX_LEN + header_len];
        let payload = Bytes::copy_from_slice(&bytes[PREFIX_LEN + header_len..]);
        prop_assert_eq!(Message::decode_parts(header, payload).unwrap(), message);
    }

    #[test]
    fn every_prefix_of_a_frame_is_incomplete(
        message in messages(),
        cut in any::<prop::sample::Index>(),
    ) {
        let bytes = message.encode().unwrap();
        let cut = cut.index(bytes.len());
        prop_assert_eq!(Message::decode(&bytes[..cut]).unwrap(), None);
    }

    #[test]
    fn frames_decode_one_at_a_time(first in messages(), second in messages()) {
        let mut bytes = first.encode().unwrap();
        let first_len = bytes.len();
        bytes.extend(second.encode().unwrap());
        prop_assert_eq!(Message::decode(&bytes).unwrap(), Some((first, first_len)));
        let (decoded, len) = Message::decode(&bytes[first_len..]).unwrap().unwrap();
        prop_assert_eq!(decoded, second);
        prop_assert_eq!(first_len + len, bytes.len());
    }

    #[test]
    fn batches_with_a_repeated_identity_are_refused(
        message in messages(),
        from in any::<prop::sample::Index>(),
        to in any::<prop::sample::Index>(),
    ) {
        let Message::Batch(mut batch) = message else {
            return Ok(());
        };
        let (from, to) = (from.index(batch.items.len()), to.index(batch.items.len()));
        prop_assume!(from != to);
        batch.items[to].identity = batch.items[from].identity.clone();
        let refused = matches!(
            Message::Batch(batch).encode(),
            Err(skys3_peer::MessageError::Invalid { field: "batch.items", .. })
        );
        prop_assert!(refused);
    }

    #[test]
    fn corrupt_frames_are_refused_or_decode_to_valid_messages(
        message in messages(),
        flips in prop::collection::vec((any::<prop::sample::Index>(), 1u8..), 1..4),
    ) {
        let mut bytes = message.encode().unwrap();
        for (index, mask) in flips {
            let index = index.index(bytes.len());
            bytes[index] ^= mask;
        }
        // Whatever decodes is valid, and re-encodes to itself.
        if let Ok(Some((decoded, _))) = Message::decode(&bytes) {
            let again = decoded.encode().unwrap();
            prop_assert_eq!(Message::decode(&again).unwrap(), Some((decoded, again.len())));
        }
    }

    #[test]
    fn arbitrary_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..512)) {
        if let Ok(Some((decoded, len))) = Message::decode(&bytes) {
            prop_assert!(len <= bytes.len());
            decoded.validate().unwrap();
        }
    }

    #[test]
    fn negotiation_is_symmetric_and_picks_the_highest_common_version(
        a in hellos(),
        b in hellos(),
    ) {
        let ab = negotiate(&a, &b);
        let ba = negotiate(&b, &a);
        let common: Vec<u16> = (1..=40)
            .filter(|&v| a.versions.contains(v) && b.versions.contains(v))
            .collect();
        match (ab, ba) {
            (Ok(ab), Ok(ba)) => {
                prop_assert_eq!(Some(&ab.version), common.last());
                prop_assert_eq!(ab.version, ba.version);
                prop_assert_eq!(ab.capabilities, ba.capabilities);
                prop_assert!(Capabilities::KNOWN.contains(ab.capabilities));
                prop_assert!(a.capabilities.contains(ab.capabilities));
                prop_assert!(b.capabilities.contains(ab.capabilities));
                prop_assert_eq!(&ab.peer, &b.cluster);
                prop_assert_eq!(&ba.peer, &a.cluster);
            }
            (Err(ab), Err(ba)) => {
                prop_assert!(common.is_empty());
                prop_assert_eq!((ab.local, ab.remote), (ba.remote, ba.local));
            }
            (ab, ba) => prop_assert!(false, "asymmetric: {ab:?} and {ba:?}"),
        }
    }
}

#[test]
fn staged_ranges_of_large_maps_round_trip() {
    let identity: WriteIdentity = "c/b/0/1.2".parse().unwrap();
    let pieces: BTreeMap<u64, ByteRanges> = (0..2000)
        .map(|piece| {
            (
                piece,
                [0..piece + 1, piece + 2..MAX_PIECE_BYTES]
                    .into_iter()
                    .collect(),
            )
        })
        .collect();
    let message = Message::Resume(StagedRanges { identity, pieces });
    let bytes = message.encode().unwrap();
    assert_eq!(
        Message::decode(&bytes).unwrap(),
        Some((message, bytes.len()))
    );
}
