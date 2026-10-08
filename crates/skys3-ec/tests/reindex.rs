//! Re-indexing a lost shard's coded objects (plan M5-11): how the latest
//! snapshot and the fragment headers found combine, key by key, and how a
//! restored layout places its fragments.

mod support;

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use skys3_ec::fragment::{FragmentHeader, ObjectMeta, PartSize, StripeInfo};
use skys3_ec::reindex::{
    HeaderSource, Placement, ReindexError, Reindexed, SnapshotState, UNKNOWN_FRAGMENT, reindex,
};
use skys3_ec::{
    AttemptId, CodecId, CodedStripe, FoundFragment, FragmentId, FragmentLocation, FragmentServer,
    FragmentStore, Geometry, RebuildError,
};
use skys3_index::{Coded, Entry, EntryState, ObjectPart, ObjectVersion, Payload};
use skys3_io::SimDisk;
use skys3_log::record::{ShardRef, TagSet};
use skys3_types::{BucketId, ETag, Epoch, EpochSeq, NodeId, Seq, ShardId};
use support::{runtime, small_config};

/// Stripes of 2+1, 100 bytes each.
const GEOMETRY: Geometry = Geometry::RS_3_2;
const STRIPE: u64 = 300;

fn shard() -> ShardRef {
    ShardRef::new(BucketId::new("b-lost").unwrap(), ShardId::new(1))
}

fn at(seq: u64) -> EpochSeq {
    EpochSeq::new(Epoch::new(2), Seq::new(seq))
}

fn node(n: usize) -> NodeId {
    format!("n{n}").parse().unwrap()
}

fn nodes() -> Vec<NodeId> {
    (0..8).map(node).collect()
}

fn attempt(number: u64) -> AttemptId {
    AttemptId::new(Epoch::new(2), number)
}

fn tags(value: &str) -> TagSet {
    BTreeMap::from([("v".to_owned(), value.to_owned())])
}

fn etag(n: u64) -> ETag {
    ETag::new(format!("{n:032x}")).unwrap()
}

/// An object version: a key, the version's position, and its metadata.
#[derive(Clone)]
struct Version {
    key: &'static str,
    version: EpochSeq,
    object: ObjectMeta,
}

impl Version {
    /// A one-stripe object of `key` at `seq`, with ETag `n`.
    fn new(key: &'static str, seq: u64, n: u64) -> Self {
        Self {
            key,
            version: at(seq),
            object: ObjectMeta {
                size: STRIPE,
                last_modified_ms: 1_700_000_000_000 + n,
                etag: etag(n),
                identity: at(seq),
                metadata: BTreeMap::new(),
                tags: tags("first"),
                checksums: BTreeMap::new(),
                parts: Vec::new(),
            },
        }
    }

    /// The same version as an attempt that read it retagged at `seq` with
    /// `value` holds it.
    fn retagged(&self, seq: u64, value: &str) -> Self {
        let mut retagged = self.clone();
        retagged.object.identity = at(seq);
        retagged.object.tags = tags(value);
        retagged
    }

    /// Fragment `index`, written by `attempt` and found on node `n`.
    fn fragment(&self, attempt_number: u64, index: u8, n: usize) -> FoundFragment {
        FoundFragment {
            location: FragmentLocation {
                node: node(n),
                fragment: FragmentId::new(u128::from(attempt_number) << 8 | u128::from(index)),
            },
            header: FragmentHeader {
                shard: shard(),
                key: self.key.to_owned(),
                version: self.version,
                attempt: attempt(attempt_number),
                stripe: StripeInfo {
                    number: 0,
                    count: 1,
                    offset: 0,
                    data_len: STRIPE,
                    geometry: GEOMETRY,
                    codec: CodecId::REED_SOLOMON_V1,
                },
                index,
                object: self.object.clone(),
            },
        }
    }

    /// Every fragment, written by `attempt` on nodes `n`, `n + 1`, ...
    fn fragments(&self, attempt_number: u64, first_node: usize) -> Vec<FoundFragment> {
        (0..5)
            .map(|index| self.fragment(attempt_number, index, first_node + usize::from(index)))
            .collect()
    }

    /// The entry a shard's index holds of the version, replicated.
    fn entry(&self) -> Entry {
        Entry {
            version: self.version.max(self.object.identity),
            state: EntryState::Dirty,
            object: Some(ObjectVersion {
                size: self.object.size,
                last_modified_ms: self.object.last_modified_ms,
                local_etag: self.object.etag.clone(),
                write_identity: None,
                metadata: BTreeMap::new(),
                tags: self.object.tags.clone(),
                checksums: BTreeMap::new(),
                storage_class: Some("STANDARD".to_owned()),
                copy_source: None,
                payload: Payload::Inline(self.version),
                coded: None,
            }),
            remote_etag: None,
            remote_version_id: None,
        }
    }

    /// The entry, coded by `attempt` at `publish`, its fragments on nodes
    /// `first_node`, ...
    fn coded_entry(&self, attempt_number: u64, publish: u64, first_node: usize) -> Entry {
        let mut entry = self.entry();
        let stripe = CodedStripe::new(
            0,
            0,
            STRIPE,
            GEOMETRY,
            CodecId::REED_SOLOMON_V1,
            self.fragments(attempt_number, first_node)
                .into_iter()
                .map(|found| found.location)
                .collect(),
        )
        .unwrap();
        if let Some(object) = &mut entry.object {
            object.coded = Some(Coded {
                publish: at(publish),
                version: self.version,
                attempt: attempt(attempt_number),
                stripes: vec![stripe],
            });
        }
        entry
    }
}

fn snapshot(position: u64, entries: &[(&str, Entry)]) -> SnapshotState {
    SnapshotState {
        position: at(position),
        entries: entries
            .iter()
            .map(|(key, entry)| ((*key).to_owned(), entry.clone()))
            .collect(),
    }
}

fn run(snapshot: Option<&SnapshotState>, found: Vec<FoundFragment>, lost: &[usize]) -> Reindexed {
    let lost: BTreeSet<NodeId> = lost.iter().copied().map(node).collect();
    let nodes = nodes();
    reindex(
        &shard(),
        snapshot,
        found,
        Placement {
            nodes: &nodes,
            lost: &lost,
        },
    )
}

fn coded(entry: &Entry) -> &Coded {
    entry.object.as_ref().unwrap().coded.as_ref().unwrap()
}

fn locations(entry: &Entry) -> Vec<FragmentLocation> {
    coded(entry).stripes[0].fragments().to_vec()
}

#[test]
fn a_version_written_after_the_snapshot_is_restored_from_its_headers_alone() {
    let old = Version::new("photo", 10, 1);
    let new = Version::new("photo", 30, 2);
    let state = snapshot(20, &[("photo", old.coded_entry(1, 11, 0))]);
    let mut found = old.fragments(1, 0);
    found.extend(new.fragments(3, 2));
    let reindexed = run(Some(&state), found, &[]);

    let restored = &reindexed.restored["photo"];
    assert!(restored.after_snapshot);
    assert_eq!(restored.missing, 0);
    let entry = &restored.entry;
    assert_eq!(entry.version, at(30));
    assert_eq!(entry.state, EntryState::Dirty);
    let object = entry.object.as_ref().unwrap();
    assert_eq!(object.local_etag, etag(2));
    assert_eq!(object.payload, Payload::None);
    assert_eq!(object.storage_class, None);
    let coded = coded(entry);
    assert_eq!((coded.version, coded.attempt), (at(30), attempt(3)));
    // Its `EC_PUBLISH` is lost: the layout is published at the restored
    // index's position, after everything named.
    assert_eq!(reindexed.position, at(30));
    assert_eq!(reindexed.applied(), at(31));
    assert_eq!(coded.publish, at(31));
    assert_eq!(
        locations(entry),
        new.fragments(3, 2)
            .into_iter()
            .map(|f| f.location)
            .collect::<Vec<_>>()
    );
    // The snapshot's version is superseded.
    assert_eq!(reindexed.superseded["photo"], vec![at(10)]);
    assert!(reindexed.unrecoverable.is_empty());
}

#[test]
fn the_snapshot_s_version_takes_the_latest_attempt_s_fragments() {
    let version = Version::new("photo", 10, 1);
    let state = snapshot(20, &[("photo", version.coded_entry(1, 11, 0))]);
    // A repair after the snapshot moved fragment 1 from n1 to n7; n1 still
    // holds the old copy, and fragment 4, on n4, is not found.
    let mut found = version.fragments(1, 0);
    found.retain(|f| f.header.index != 4);
    found.push(version.fragment(9, 1, 7));
    let reindexed = run(Some(&state), found, &[]);

    let restored = &reindexed.restored["photo"];
    assert!(!restored.after_snapshot);
    assert_eq!(restored.missing, 1, "no header of fragment 4 was found");
    let entry = &restored.entry;
    // It stays where the snapshot's layout put it, for repair to rebuild.
    assert_eq!(locations(entry)[4], version.fragment(1, 4, 4).location);
    // The snapshot's entry, with the layout's fragments where the latest
    // attempt put them, and its `EC_PUBLISH` and attempt.
    assert_eq!(entry.version, at(10));
    let object = entry.object.as_ref().unwrap();
    assert_eq!(object.storage_class.as_deref(), Some("STANDARD"));
    assert_eq!(object.payload, Payload::Inline(at(10)));
    let coded = coded(entry);
    assert_eq!((coded.publish, coded.attempt), (at(11), attempt(1)));
    let nodes: Vec<NodeId> = locations(entry).into_iter().map(|l| l.node).collect();
    assert_eq!(nodes, vec![node(0), node(7), node(2), node(3), node(4)]);
}

#[test]
fn a_version_coded_after_the_snapshot_keeps_the_snapshot_s_entry() {
    let version = Version::new("photo", 10, 1);
    let state = snapshot(20, &[("photo", version.entry())]);
    let reindexed = run(Some(&state), version.fragments(4, 1), &[]);

    let restored = &reindexed.restored["photo"];
    assert!(restored.after_snapshot, "coded after the snapshot");
    let entry = &restored.entry;
    assert_eq!(entry.version, at(10));
    assert_eq!(
        entry.object.as_ref().unwrap().payload,
        Payload::Inline(at(10))
    );
    assert_eq!(coded(entry).publish, reindexed.applied());
}

#[test]
fn versions_the_snapshot_superseded_are_never_restored() {
    // Deleted before the snapshot: no entry, and headers before it.
    let deleted = Version::new("deleted", 5, 1);
    // Overwritten before the snapshot by a replicated version.
    let overwritten = Version::new("overwritten", 6, 2);
    let newer = Version::new("overwritten", 12, 3);
    let state = snapshot(20, &[("overwritten", newer.entry())]);
    let mut found = deleted.fragments(1, 0);
    found.extend(overwritten.fragments(2, 1));
    let reindexed = run(Some(&state), found, &[]);

    assert!(reindexed.restored.is_empty(), "{:?}", reindexed.restored);
    assert!(reindexed.unrecoverable.is_empty());
    assert_eq!(reindexed.superseded["deleted"], vec![at(5)]);
    assert_eq!(reindexed.superseded["overwritten"], vec![at(6)]);
}

#[test]
fn without_a_snapshot_every_key_s_newest_version_is_restored() {
    let old = Version::new("photo", 5, 1);
    let new = Version::new("photo", 9, 2);
    let mut found = old.fragments(1, 0);
    found.extend(new.fragments(2, 3));
    let reindexed = run(None, found, &[]);
    let entry = &reindexed.restored["photo"].entry;
    assert_eq!(entry.object.as_ref().unwrap().local_etag, etag(2));
    assert_eq!(reindexed.superseded["photo"], vec![at(5)]);
}

#[test]
fn a_newest_version_that_cannot_be_rebuilt_is_unrecoverable_and_not_replaced() {
    let old = Version::new("photo", 10, 1);
    let new = Version::new("photo", 30, 2);
    let state = snapshot(20, &[("photo", old.coded_entry(1, 11, 0))]);
    let mut found = old.fragments(1, 0);
    // Two of the newest version's five fragments: `k` is 3.
    found.push(new.fragment(3, 0, 5));
    found.push(new.fragment(3, 4, 6));
    let reindexed = run(Some(&state), found, &[1, 2, 3]);

    assert!(reindexed.restored.is_empty());
    let lost = &reindexed.unrecoverable["photo"];
    assert_eq!(
        (lost.version, lost.identity, &lost.etag),
        (at(30), at(30), &etag(2))
    );
    assert_eq!(lost.size, STRIPE);
    assert_eq!(
        lost.error,
        ReindexError::Rebuild(RebuildError::NotEnoughFragments {
            stripe: 0,
            needed: 3,
            available: 2
        })
    );
}

#[test]
fn a_coded_entry_whose_fragments_are_gone_is_unrecoverable() {
    let version = Version::new("photo", 10, 1);
    let state = snapshot(20, &[("photo", version.coded_entry(1, 11, 0))]);
    let reindexed = run(Some(&state), Vec::new(), &[]);
    let lost = &reindexed.unrecoverable["photo"];
    assert_eq!(lost.error, ReindexError::NoFragments);
    assert_eq!((lost.version, &lost.etag), (at(10), &etag(1)));

    // So is one whose fragments are too few, and a replicated one whose
    // encoding after the snapshot left too few.
    let replicated = Version::new("other", 12, 2);
    let state = snapshot(
        20,
        &[
            ("photo", version.coded_entry(1, 11, 0)),
            ("other", replicated.entry()),
        ],
    );
    let found = vec![version.fragment(1, 0, 0), replicated.fragment(2, 1, 1)];
    let reindexed = run(Some(&state), found, &[]);
    for key in ["photo", "other"] {
        assert!(matches!(
            reindexed.unrecoverable[key].error,
            ReindexError::Rebuild(RebuildError::NotEnoughFragments { .. })
        ));
    }

    // A replicated entry with no headers at all is not re-indexing's: the
    // lost-key report lists it.
    let state = snapshot(20, &[("other", replicated.entry())]);
    let reindexed = run(Some(&state), Vec::new(), &[]);
    assert!(reindexed.restored.is_empty() && reindexed.unrecoverable.is_empty());
}

#[test]
fn tags_come_from_the_later_of_the_snapshot_and_the_latest_attempt() {
    let version = Version::new("photo", 10, 1);
    // Retagged at 15, before the snapshot, after the encoding.
    let state = snapshot(
        20,
        &[(
            "photo",
            version.retagged(15, "snapshot").coded_entry(1, 11, 0),
        )],
    );
    let reindexed = run(Some(&state), version.fragments(1, 0), &[]);
    let entry = &reindexed.restored["photo"].entry;
    assert_eq!(entry.version, at(15));
    assert_eq!(entry.object.as_ref().unwrap().tags, tags("snapshot"));

    // Retagged at 25, after the snapshot, and a repair read the retag.
    let mut found = version.fragments(1, 0);
    found.push(version.retagged(25, "repair").fragment(5, 2, 6));
    let reindexed = run(Some(&state), found, &[]);
    let entry = &reindexed.restored["photo"].entry;
    assert_eq!(entry.version, at(25));
    let object = entry.object.as_ref().unwrap();
    assert_eq!(object.tags, tags("repair"));
    assert_eq!(object.write_identity, None, "a retag names itself");
    // The rest of the entry is still the snapshot's.
    assert_eq!(object.storage_class.as_deref(), Some("STANDARD"));
    assert_eq!(coded(entry).publish, at(11));
    assert_eq!(reindexed.position, at(25));
}

#[test]
fn an_inherited_identity_and_parts_are_restored_from_headers() {
    let mut version = Version::new("multipart", 30, 1);
    version.object.identity = at(22);
    version.object.parts = vec![
        PartSize {
            number: 1,
            size: 200,
        },
        PartSize {
            number: 2,
            size: 100,
        },
    ];
    let reindexed = run(None, version.fragments(1, 0), &[]);
    let entry = &reindexed.restored["multipart"].entry;
    assert_eq!(entry.version, at(30));
    let object = entry.object.as_ref().unwrap();
    assert_eq!(object.write_identity, Some(at(22)));
    assert_eq!(
        object.payload,
        Payload::Parts {
            upload: at(22),
            parts: vec![
                ObjectPart {
                    number: 1,
                    size: 200
                },
                ObjectPart {
                    number: 2,
                    size: 100
                },
            ],
        }
    );
}

#[test]
fn fragments_are_placed_one_per_node_from_every_copy_found() {
    let version = Version::new("photo", 10, 1);
    // The encoding put fragment 1 on n1 and fragment 2 on n2. A repair
    // rebuilt fragment 2 on n1 after n1's copy of fragment 1 was found
    // missing, then n1 came back with it: the latest copies of 1 and 2
    // share n1, and the older copy of 2 on n2 must take its place.
    let mut found = version.fragments(1, 0);
    found.retain(|f| f.header.index != 4);
    found.push(version.fragment(7, 2, 1));
    let reindexed = run(None, found, &[]);
    let restored = &reindexed.restored["photo"];
    let nodes: Vec<NodeId> = locations(&restored.entry)
        .into_iter()
        .map(|l| l.node)
        .collect();
    assert_eq!(&nodes[..4], &[node(0), node(1), node(2), node(3)]);
    assert_eq!(restored.missing, 1);
    // Fragment 4, with no header and no snapshot, is placed where reads
    // and repair find it missing.
    let unknown = &locations(&restored.entry)[4];
    assert_eq!(unknown.fragment, UNKNOWN_FRAGMENT);
    assert_eq!(unknown.node, node(4));
}

#[test]
fn a_fragment_without_a_header_goes_to_a_lost_node_first() {
    let version = Version::new("photo", 10, 1);
    let mut found = version.fragments(1, 2);
    found.retain(|f| f.header.index < 3);
    let reindexed = run(None, found, &[0, 3, 6]);
    let restored = &reindexed.restored["photo"];
    assert_eq!(restored.missing, 2);
    let placed: Vec<(NodeId, FragmentId)> = locations(&restored.entry)[3..]
        .iter()
        .map(|l| (l.node.clone(), l.fragment))
        .collect();
    // n3 is lost but holds nothing of the stripe; n0 is lost too.
    assert_eq!(
        placed,
        vec![(node(0), UNKNOWN_FRAGMENT), (node(6), UNKNOWN_FRAGMENT)]
    );
}

#[test]
fn a_stripe_that_cannot_be_laid_out_is_unrecoverable() {
    let version = Version::new("photo", 10, 1);
    // Three fragments, all on one node: `k` copies, but on one node.
    let found = vec![
        version.fragment(1, 0, 3),
        version.fragment(1, 1, 3),
        version.fragment(1, 2, 3),
    ];
    let reindexed = run(None, found, &[]);
    assert!(matches!(
        reindexed.unrecoverable["photo"].error,
        ReindexError::Layout { stripe: 0, .. }
    ));
}

#[test]
fn positions_and_epochs_cover_everything_the_snapshot_and_headers_name() {
    let version = Version::new("photo", 10, 1);
    let state = snapshot(20, &[("photo", version.coded_entry(1, 40, 0))]);
    let mut late = version.fragment(1, 0, 6);
    late.header.attempt = AttemptId::new(Epoch::new(5), 3);
    let mut found = version.fragments(1, 0);
    found.push(late);
    // Headers of other shards are ignored.
    let mut other = Version::new("photo", 90, 9).fragments(1, 0);
    for fragment in &mut other {
        fragment.header.shard = ShardRef::new(BucketId::new("b-other").unwrap(), ShardId::new(1));
    }
    found.extend(other);
    let reindexed = run(Some(&state), found, &[]);
    assert_eq!(reindexed.position, at(40));
    assert_eq!(reindexed.epoch, Epoch::new(5));
    assert_eq!(reindexed.restored.len(), 1);
    assert_eq!(reindexed.restored["photo"].entry.version, at(10));
}

#[test]
fn fragment_servers_list_the_headers_of_a_shard() {
    runtime().block_on(async {
        let (store, _) = FragmentStore::open(SimDisk::new(3).mount(), small_config())
            .await
            .unwrap();
        let version = Version::new("photo", 10, 1);
        let mut ids = Vec::new();
        for index in [0u8, 3] {
            let header = version.fragment(1, index, 0).header;
            ids.push(
                store
                    .write(&header, Bytes::from(vec![index; 128]))
                    .await
                    .unwrap(),
            );
        }
        let mut other = version.fragment(1, 1, 0).header;
        other.shard = ShardRef::new(BucketId::new("b-other").unwrap(), ShardId::new(1));
        store
            .write(&other, Bytes::from(vec![1; 128]))
            .await
            .unwrap();

        let servers = BTreeMap::from([(node(2), FragmentServer::new(vec![store]))]);
        let found = servers.headers(&node(2), &shard()).await.unwrap();
        let listed: Vec<(FragmentLocation, u8)> = found
            .into_iter()
            .map(|f| (f.location, f.header.index))
            .collect();
        let expected: Vec<(FragmentLocation, u8)> = ids
            .into_iter()
            .zip([0, 3])
            .map(|(fragment, index)| {
                (
                    FragmentLocation {
                        node: node(2),
                        fragment,
                    },
                    index,
                )
            })
            .collect();
        assert_eq!(listed, expected);

        let error = servers.headers(&node(5), &shard()).await.unwrap_err();
        assert_eq!(error.node, node(5));
        assert!(error.to_string().contains("n5"), "{error}");
    });
}
