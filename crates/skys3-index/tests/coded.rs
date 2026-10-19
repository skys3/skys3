//! Coded versions in the index (§8.4): their layouts, the per-shard index
//! from node to fragments, what names their replicated bytes, and attempt
//! numbers, and the index of released fragments (§8.7).

use std::collections::BTreeMap;

use skys3_index::codec::{self, FragmentKey};
use skys3_index::{
    Coded, Entry, EntryState, Holder, Index, IndexConfig, ObjectVersion, Payload, Released,
    ShardTable,
};
use skys3_io::SimDisk;
use skys3_log::record::{ExtentRef, ShardRef};
use skys3_types::{
    AttemptId, BucketId, CodecId, CodedStripe, ETag, Epoch, EpochSeq, FragmentId, FragmentLocation,
    Geometry, NodeId, Seq, ShardId,
};

fn shard(number: u8) -> ShardRef {
    ShardRef::new(BucketId::new("b1").unwrap(), ShardId::new(number))
}

fn position(seq: u64) -> EpochSeq {
    EpochSeq::new(Epoch::new(1), Seq::new(seq))
}

fn node(n: usize) -> NodeId {
    NodeId::new(format!("n{n}")).unwrap()
}

/// Two stripes of 2+1 over nodes 0 to 3, fragment IDs from `first`.
fn coded(first: u128) -> Coded {
    let stripe = |number: u32, offset, len, nodes: [usize; 3]| {
        let fragments = nodes
            .iter()
            .zip(0..)
            .map(|(&n, i)| FragmentLocation {
                node: node(n),
                fragment: FragmentId::new(first + u128::from(number) * 10 + i),
            })
            .collect();
        let geometry = Geometry::new(2, 1).unwrap();
        CodedStripe::new(number, offset, len, geometry, CodecId::CURRENT, fragments).unwrap()
    };
    Coded {
        publish: position(9),
        version: position(3),
        attempt: AttemptId::new(Epoch::new(1), 4),
        stripes: vec![stripe(0, 0, 60, [0, 1, 2]), stripe(1, 60, 40, [1, 2, 3])],
    }
}

fn entry(coded: Option<Coded>) -> Entry {
    Entry {
        version: position(3),
        state: EntryState::Dirty,
        object: Some(ObjectVersion {
            size: 100,
            last_modified_ms: 1,
            local_etag: ETag::new("e").unwrap(),
            write_identity: None,
            metadata: BTreeMap::new(),
            tags: BTreeMap::new(),
            checksums: BTreeMap::new(),
            storage_class: None,
            copy_source: None,
            payload: Payload::Extents(vec![
                ExtentRef {
                    position: position(1),
                    len: 50,
                },
                ExtentRef {
                    position: position(2),
                    len: 50,
                },
            ]),
            coded,
        }),
        remote_etag: None,
        remote_version_id: None,
    }
}

fn open(disk: &SimDisk) -> Index {
    Index::open_sim(&disk.mount(), "index.redb", &IndexConfig::default()).unwrap()
}

#[test]
fn coded_entries_round_trip_and_must_cover_their_object() {
    let coded_entry = entry(Some(coded(100)));
    let bytes = codec::encode_entry(&coded_entry).unwrap();
    assert_eq!(codec::decode_entry(&bytes).unwrap(), coded_entry);

    // Stripes that do not cover the object, or out of order, are refused.
    let mut short = coded_entry.clone();
    short.object.as_mut().unwrap().size = 99;
    assert_eq!(
        codec::encode_entry(&short).unwrap_err().field(),
        "object.coded"
    );
    let mut reversed = coded_entry.clone();
    reversed
        .object
        .as_mut()
        .unwrap()
        .coded
        .as_mut()
        .unwrap()
        .stripes
        .reverse();
    assert_eq!(
        codec::encode_entry(&reversed).unwrap_err().field(),
        "object.coded"
    );
    // A decoder sees the same: the size is a fixed 8 bytes after the
    // object's presence byte.
    let at = 1 + 16 + 1 + 1 + 1 + 1;
    let mut bad = bytes.clone();
    bad[at] = 99;
    assert_eq!(
        codec::decode_entry(&bad).unwrap_err().field(),
        "object.coded"
    );
    // Every truncation fails cleanly.
    for len in 0..bytes.len() {
        assert!(codec::decode_entry(&bytes[..len]).is_err(), "{len} bytes");
    }
}

/// Encodes `at` as a value does: epoch, then seq, little-endian.
fn encoded(at: EpochSeq) -> Vec<u8> {
    [at.epoch.get().to_le_bytes(), at.seq.get().to_le_bytes()].concat()
}

#[test]
fn a_layout_records_the_version_its_fragments_were_written_for() {
    // The layout follows the payload: its presence byte, the publish, and
    // then the version.
    let uncoded = codec::encode_entry(&entry(None)).unwrap();
    let start = uncoded.len();
    let coded_entry = entry(Some(coded(100)));
    let bytes = codec::encode_entry(&coded_entry).unwrap();
    assert_eq!(bytes[..start - 1], uncoded[..start - 1]);
    assert_eq!(bytes[start - 1], 1);
    assert_eq!(bytes[start..start + 16], encoded(position(9)));
    assert_eq!(bytes[start + 16..start + 32], encoded(position(3)));

    // A retag moves the entry's version past the layout's, which stays.
    let mut retagged = coded_entry.clone();
    retagged.version = position(12);
    let bytes = codec::encode_entry(&retagged).unwrap();
    let decoded = codec::decode_entry(&bytes).unwrap();
    assert_eq!(decoded, retagged);
    assert_eq!(decoded.object.unwrap().coded.unwrap().version, position(3));

    // The version must precede the publish, in either direction.
    for version in [position(9), position(10)] {
        let mut late = coded_entry.clone();
        late.object
            .as_mut()
            .unwrap()
            .coded
            .as_mut()
            .unwrap()
            .version = version;
        assert_eq!(
            codec::encode_entry(&late).unwrap_err().field(),
            "object.coded"
        );
        let mut bad = codec::encode_entry(&coded_entry).unwrap();
        bad[start + 16..start + 32].copy_from_slice(&encoded(version));
        assert_eq!(
            codec::decode_entry(&bad).unwrap_err().field(),
            "object.coded"
        );
    }
}

#[test]
fn fragment_rows_round_trip_and_sort_by_node() {
    let key = codec::fragment_key(&shard(2), &node(7), "a/b", 3, 1);
    let decoded = codec::decode_fragment_key(&key).unwrap();
    assert_eq!(
        decoded,
        FragmentKey {
            shard: shard(2),
            node: node(7),
            key: "a/b".into(),
            stripe: 3,
            index: 1,
        }
    );
    assert!(key.starts_with(&codec::fragment_node_prefix(&shard(2), &node(7))));
    let value = codec::encode_fragment(FragmentId::new(42));
    assert_eq!(value[0], codec::VALUE_FORMAT);
    assert_eq!(codec::decode_fragment(&value).unwrap(), FragmentId::new(42));

    // Malformed keys and values are refused.
    for bad in [
        Vec::new(),
        key[..key.len() - 5].to_vec(),
        [&key[..key.len() - 8], &[0xff], &key[key.len() - 5..]].concat(),
    ] {
        assert!(codec::decode_fragment_key(&bad).is_err(), "{bad:?}");
    }
    let mut no_node = codec::fragment_node_prefix(&shard(2), &node(7));
    no_node[3] = 0;
    assert!(codec::decode_fragment_key(&no_node).is_err());
    assert!(codec::decode_fragment(&value[..10]).is_err());
    assert!(codec::decode_fragment(&[[3].as_slice(), &value[1..]].concat()).is_err());
    assert!(codec::decode_fragment(&[value.as_slice(), &[0]].concat()).is_err());
}

#[test]
fn the_node_index_follows_coded_versions() {
    let disk = SimDisk::new(1);
    let index = open(&disk);
    let (one, two) = (shard(1), shard(2));
    index
        .update_durable(|writer| {
            writer.put_entry(&one, "k", &entry(Some(coded(100))))?;
            writer.put_fragments(&one, "k", &coded(100))?;
            writer.put_fragments(&two, "k", &coded(200))
        })
        .unwrap();
    let reader = index.read().unwrap();
    let on = |shard: &ShardRef, n: usize| {
        reader
            .fragments_on(shard, &node(n))
            .unwrap()
            .into_iter()
            .map(|(key, id)| (key.key, key.stripe, key.index, id.get()))
            .collect::<Vec<_>>()
    };
    assert_eq!(on(&one, 0), [("k".to_owned(), 0, 0, 100)]);
    assert_eq!(
        on(&one, 2),
        [("k".to_owned(), 0, 2, 102), ("k".to_owned(), 1, 1, 111)]
    );
    assert_eq!(on(&two, 3), [("k".to_owned(), 1, 2, 212)]);
    assert!(on(&one, 9).is_empty());
    assert_eq!(reader.dump().unwrap().fragments.len(), 12);

    // A coded entry names its replicated bytes as coded.
    let holders = reader.holders(&one, "k").unwrap();
    assert_eq!(holders.len(), 2);
    assert!(holders.values().all(|h| *h
        == Holder::Coded {
            publish: position(9)
        }));

    // A snapshot carries the rows, which a learner checks.
    let rows = reader
        .shard_rows(ShardTable::Fragments, &one, None, usize::MAX)
        .unwrap();
    assert_eq!(rows.len(), 6);
    assert!(ShardTable::from_code(ShardTable::Fragments.code()) == Some(ShardTable::Fragments));
    let learner = open(&SimDisk::new(2));
    learner
        .install_rows(&one, ShardTable::Fragments, &rows)
        .unwrap();
    assert_eq!(
        learner
            .read()
            .unwrap()
            .fragments_on(&one, &node(1))
            .unwrap()
            .len(),
        2
    );
    assert!(
        learner
            .install_rows(&two, ShardTable::Fragments, &rows)
            .is_err()
    );
    drop(reader);

    // Removing a layout, or the shard, removes its rows.
    index
        .update_durable(|writer| writer.remove_fragments(&one, "k", &coded(100)))
        .unwrap();
    assert!(
        index
            .read()
            .unwrap()
            .fragments_on(&one, &node(1))
            .unwrap()
            .is_empty()
    );
    index.remove_shard(&two).unwrap();
    assert!(index.read().unwrap().dump().unwrap().fragments.is_empty());
}

#[test]
fn attempt_numbers_are_never_reserved_twice() {
    let disk = SimDisk::new(3);
    let index = open(&disk);
    assert_eq!(index.reserve_attempts(&shard(1), 16).unwrap(), 0..16);
    assert_eq!(index.reserve_attempts(&shard(1), 16).unwrap(), 16..32);
    assert_eq!(index.reserve_attempts(&shard(2), 4).unwrap(), 0..4);
    // Each reservation is durable at once: a power loss keeps it.
    drop(index);
    disk.crash();
    let index = open(&disk);
    assert_eq!(index.reserve_attempts(&shard(1), 1).unwrap(), 32..33);
    assert!(index.reserve_attempts(&shard(1), u64::MAX).is_err());
    assert_eq!(index.reserve_attempts(&shard(1), 1).unwrap(), 33..34);
}

#[test]
fn released_rows_round_trip_and_sort_by_node_and_id() {
    let id = FragmentId::new(1 << 70 | 5);
    let key = codec::released_key(&shard(2), &node(7), id);
    assert!(key.starts_with(&codec::fragment_node_prefix(&shard(2), &node(7))));
    let location = FragmentLocation {
        node: node(7),
        fragment: id,
    };
    assert_eq!(
        codec::decode_released_key(&key).unwrap(),
        (shard(2), location)
    );
    // Big-endian IDs sort numerically.
    assert!(
        codec::released_key(&shard(2), &node(7), FragmentId::new(255))
            < codec::released_key(&shard(2), &node(7), FragmentId::new(256))
    );
    let released = Released {
        key: "a/b".into(),
        version: position(3),
    };
    let value = codec::encode_released(&released).unwrap();
    assert_eq!(value[0], codec::VALUE_FORMAT);
    assert_eq!(codec::decode_released(&value).unwrap(), released);

    // Malformed keys and values are refused.
    for bad in [
        Vec::new(),
        key[..key.len() - 1].to_vec(),
        [key.as_slice(), &[0]].concat(),
    ] {
        assert!(codec::decode_released_key(&bad).is_err(), "{bad:?}");
    }
    let empty = Released {
        key: String::new(),
        version: position(3),
    };
    assert!(codec::encode_released(&empty).is_err());
    let mut no_key = vec![codec::VALUE_FORMAT, 0, 0];
    no_key.extend_from_slice(&[0; 16]);
    assert!(codec::decode_released(&no_key).is_err());
    assert!(codec::decode_released(&value[..10]).is_err());
    assert!(codec::decode_released(&[[3].as_slice(), &value[1..]].concat()).is_err());
    assert!(codec::decode_released(&[value.as_slice(), &[0]].concat()).is_err());
}

#[test]
fn released_fragments_wait_until_an_ec_release_removes_them() {
    let disk = SimDisk::new(4);
    let index = open(&disk);
    let (one, two) = (shard(1), shard(2));
    let moved = FragmentLocation {
        node: node(9),
        fragment: FragmentId::new(7),
    };
    index
        .update_durable(|writer| {
            writer.release_fragments(&one, "k", &coded(100))?;
            writer.release_fragment(&one, "other", position(5), &moved)?;
            writer.release_fragments(&two, "k", &coded(200))
        })
        .unwrap();
    let reader = index.read().unwrap();
    let all = reader.released(&one, None, usize::MAX).unwrap();
    assert_eq!(all.len(), 7);
    // In node and then ID order, each with the key and version it names.
    let order: Vec<(String, u128)> = all
        .iter()
        .map(|(location, _)| (location.node.to_string(), location.fragment.get()))
        .collect();
    let mut sorted = order.clone();
    sorted.sort();
    assert_eq!(order, sorted);
    let named = |key: &str, seq| Released {
        key: key.into(),
        version: position(seq),
    };
    assert_eq!(all[0].1, named("k", 3));
    assert_eq!(all[6], (moved.clone(), named("other", 5)));
    // Pages follow on from where the last ended.
    let first = reader.released(&one, None, 3).unwrap();
    let rest = reader.released(&one, Some(&first[2].0), 10).unwrap();
    assert_eq!([first, rest].concat(), all);
    assert_eq!(reader.dump().unwrap().released.len(), 13);

    // A snapshot carries them, and a learner checks their shard.
    let rows = reader
        .shard_rows(ShardTable::Released, &one, None, usize::MAX)
        .unwrap();
    assert_eq!(rows.len(), 7);
    assert_eq!(
        ShardTable::from_code(ShardTable::Released.code()),
        Some(ShardTable::Released)
    );
    let learner = open(&SimDisk::new(5));
    learner
        .install_rows(&one, ShardTable::Released, &rows)
        .unwrap();
    assert_eq!(
        learner.read().unwrap().released(&one, None, 100).unwrap(),
        all
    );
    assert!(
        learner
            .install_rows(&two, ShardTable::Released, &rows)
            .is_err()
    );
    drop(reader);

    // An EC_RELEASE removes only what the index holds for its key.
    let locations: Vec<FragmentLocation> =
        all.iter().map(|(location, _)| location.clone()).collect();
    let removed = index
        .update_durable(|writer| writer.unrelease(&one, "k", &locations))
        .unwrap();
    assert_eq!(removed, 6);
    let left = index.read().unwrap().released(&one, None, 100).unwrap();
    assert_eq!(left, vec![(moved, named("other", 5))]);
    let again = index
        .update_durable(|writer| writer.unrelease(&one, "k", &locations))
        .unwrap();
    assert_eq!(again, 0);
    index.remove_shard(&two).unwrap();
    assert_eq!(index.read().unwrap().dump().unwrap().released.len(), 1);
}
