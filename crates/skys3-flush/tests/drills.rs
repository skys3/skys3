//! The restore drill of a shard whose members are all lost (plan M5-11):
//! which nodes it asks for fragment headers, how a durable home overrules
//! what the headers restore, what the report says, and the index it
//! restores.

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use skys3_ec::fragment::{FragmentHeader, ObjectMeta, StripeInfo};
use skys3_ec::reindex::{HeaderError, HeaderSource, ReindexError, UNKNOWN_FRAGMENT};
use skys3_ec::{
    AttemptId, CodecId, CodedStripe, FoundFragment, FragmentId, FragmentLocation, Geometry,
};
use skys3_flush::snapshot::{
    ChainId, Contents, Drill, DrillRequest, DurableHome, Snapshot, drill, object_key, shard_dir,
};
use skys3_index::{
    Coded, Entry, EntryState, Index, IndexConfig, ObjectVersion, Payload, ShardTable, codec,
};
use skys3_io::SimDisk;
use skys3_log::ShardRef;
use skys3_remote::{ObjectStore, PutObject, UserMetadata};
use skys3_sim::SimS3;
use skys3_types::{ETag, Epoch, EpochSeq, NodeId, ProposalId, Seq, ShardConfig, WriteIdentity};
use support::{cluster, remote, runtime, shard_ref};

const SNAPSHOTS: &str = "snaps/";
const HOME: &str = "backup/";
const SIZE: u64 = 300;
const TAKEN_MS: u64 = 1_800_000_000_000;

fn at(seq: u64) -> EpochSeq {
    EpochSeq::new(Epoch::new(1), Seq::new(seq))
}

fn node(n: usize) -> NodeId {
    format!("n{n}").parse().unwrap()
}

fn etag(n: u64) -> ETag {
    ETag::new(format!("{n:032x}")).unwrap()
}

/// The header of fragment `index` of `key`'s version at `seq` with ETag
/// `n`, a 3+2 stripe, on node `holder`.
fn fragment(key: &str, seq: u64, n: u64, index: u8, holder: usize) -> FoundFragment {
    FoundFragment {
        location: FragmentLocation {
            node: node(holder),
            fragment: FragmentId::new(u128::from(seq) << 8 | u128::from(index)),
        },
        header: FragmentHeader {
            shard: shard_ref(),
            key: key.to_owned(),
            version: at(seq),
            attempt: AttemptId::new(Epoch::new(1), seq),
            stripe: StripeInfo {
                number: 0,
                count: 1,
                offset: 0,
                data_len: SIZE,
                geometry: Geometry::RS_3_2,
                codec: CodecId::REED_SOLOMON_V1,
            },
            index,
            object: ObjectMeta {
                size: SIZE,
                last_modified_ms: TAKEN_MS - 1000,
                etag: etag(n),
                identity: at(seq),
                metadata: BTreeMap::new(),
                tags: BTreeMap::new(),
                checksums: BTreeMap::new(),
                parts: Vec::new(),
            },
        },
    }
}

/// Every fragment of the version, on nodes `first`, `first + 1`, ...
fn fragments(key: &str, seq: u64, n: u64, first: usize) -> Vec<FoundFragment> {
    (0..5)
        .map(|index| fragment(key, seq, n, index, first + usize::from(index)))
        .collect()
}

/// A dirty entry of the version at `seq` with ETag `n`, coded on nodes
/// `first`, ... if `coded_on` names them.
fn entry(seq: u64, n: u64, coded_on: Option<usize>) -> Entry {
    let coded = coded_on.map(|first| Coded {
        publish: at(seq + 1),
        version: at(seq),
        attempt: AttemptId::new(Epoch::new(1), seq),
        stripes: vec![
            CodedStripe::new(
                0,
                0,
                SIZE,
                Geometry::RS_3_2,
                CodecId::REED_SOLOMON_V1,
                fragments("", seq, n, first)
                    .into_iter()
                    .map(|f| f.location)
                    .collect(),
            )
            .unwrap(),
        ],
    });
    Entry {
        version: at(seq),
        state: EntryState::Dirty,
        object: Some(ObjectVersion {
            size: SIZE,
            last_modified_ms: TAKEN_MS - 1000,
            local_etag: etag(n),
            write_identity: None,
            metadata: BTreeMap::new(),
            tags: BTreeMap::new(),
            checksums: BTreeMap::new(),
            storage_class: None,
            copy_source: None,
            payload: Payload::Inline(at(seq)),
            coded,
        }),
        remote_etag: None,
        remote_version_id: None,
    }
}

/// Writes a base snapshot of `entries` taken at position `position`.
async fn snapshot(store: &SimS3, position: u64, entries: &[(&str, Entry)]) {
    let shard = shard_ref();
    let chain = ChainId {
        epoch: Epoch::new(1),
        base: at(position),
        taken_ms: TAKEN_MS,
    };
    let rows = entries
        .iter()
        .map(|(key, entry)| {
            (
                ShardTable::Namespace,
                (
                    codec::entry_key(&shard, key),
                    codec::encode_entry(entry).unwrap(),
                ),
            )
        })
        .collect();
    let snapshot = Snapshot {
        shard: shard.clone(),
        contents: Contents::Full,
        chain,
        number: 0,
        position: at(position),
        taken_ms: TAKEN_MS,
        rows,
        removed: Vec::new(),
    };
    let key = object_key(&shard_dir(SNAPSHOTS, &shard), &chain, 0);
    store
        .put_object(PutObject::new(key, snapshot.encode()))
        .await
        .unwrap();
}

/// Writes `key` to the home, as written by this shard at `seq`.
async fn at_home(store: &SimS3, key: &str, seq: u64) -> ETag {
    let mut metadata = UserMetadata::new();
    metadata.set_write_identity(&WriteIdentity {
        cluster: cluster(),
        bucket: shard_ref().bucket,
        shard: shard_ref().shard,
        position: at(seq),
    });
    let put =
        PutObject::new(format!("{HOME}{key}"), vec![7u8; SIZE as usize]).with_metadata(metadata);
    store.put_object(put).await.unwrap().etag
}

/// Fragment headers by node, a node that does not answer, and the nodes
/// asked.
#[derive(Default)]
struct Headers {
    held: BTreeMap<NodeId, Vec<FoundFragment>>,
    silent: BTreeSet<NodeId>,
    asked: Mutex<Vec<NodeId>>,
}

impl Headers {
    fn new(found: Vec<FoundFragment>) -> Self {
        let mut held = BTreeMap::<NodeId, Vec<FoundFragment>>::new();
        for fragment in found {
            held.entry(fragment.location.node.clone())
                .or_default()
                .push(fragment);
        }
        Self {
            held,
            ..Self::default()
        }
    }
}

impl HeaderSource for Headers {
    async fn headers(
        &self,
        node: &NodeId,
        shard: &ShardRef,
    ) -> Result<Vec<FoundFragment>, HeaderError> {
        self.asked.lock().unwrap().push(node.clone());
        if self.silent.contains(node) {
            return Err(HeaderError {
                node: node.clone(),
                reason: "silent".to_owned(),
            });
        }
        let held = self.held.get(node).cloned().unwrap_or_default();
        Ok(held
            .into_iter()
            .filter(|f| f.header.shard == *shard)
            .collect())
    }
}

async fn run(store: &SimS3, headers: &Headers, home: bool, lost: &[usize]) -> Drill {
    let nodes: Vec<NodeId> = (0..8).map(node).collect();
    let lost: BTreeSet<NodeId> = lost.iter().copied().map(node).collect();
    let cluster = cluster();
    let request = DrillRequest {
        shard: &shard_ref(),
        snapshots: store,
        prefix: SNAPSHOTS,
        home: home.then_some(DurableHome {
            store,
            prefix: HOME,
            cluster: &cluster,
        }),
        nodes: &nodes,
        lost: &lost,
        until_ms: TAKEN_MS + 5000,
    };
    drill(request, headers).await.unwrap()
}

fn lost_keys(drill: &Drill) -> Vec<&str> {
    drill.report.lost.iter().map(|l| l.key.as_str()).collect()
}

#[test]
fn a_drill_restores_coded_objects_from_the_snapshot_and_headers() {
    runtime().block_on(async {
        let store = remote(1, false);
        snapshot(
            &store,
            20,
            &[
                ("coded", entry(10, 1, Some(1))),
                ("replicated", entry(11, 2, None)),
                ("gone", entry(12, 3, Some(1))),
            ],
        )
        .await;
        let mut found = fragments("coded", 10, 1, 1);
        // Written after the snapshot, and coded; n4 holds two copies of
        // it but n3 does not answer.
        found.extend(fragments("fresh", 25, 4, 2));
        found.push(fragment("other", 5, 9, 0, 0));
        let mut headers = Headers::new(found);
        headers.silent.insert(node(3));
        let drill = run(&store, &headers, false, &[0]).await;

        // Every node is asked but the lost one.
        let asked = headers.asked.lock().unwrap().clone();
        assert_eq!(asked, (1..8).map(node).collect::<Vec<_>>());
        assert_eq!(drill.unreachable.len(), 1);
        assert_eq!(drill.unreachable[0].node, node(3));

        let restored: Vec<(&str, bool, usize)> = drill
            .restored
            .iter()
            .map(|r| (r.key.as_str(), r.after_snapshot, r.missing))
            .collect();
        // n3 holds a fragment of each.
        assert_eq!(restored, vec![("coded", false, 1), ("fresh", true, 1)]);
        assert_eq!(drill.report.coded, vec!["coded", "fresh"]);
        // A replicated object is lost; a coded one with no fragment found
        // is lost as coded.
        assert_eq!(lost_keys(&drill), vec!["gone", "replicated"]);
        assert_eq!(drill.report.lost[0].coded, Some(ReindexError::NoFragments));
        assert_eq!(drill.report.lost[1].coded, None);
        assert_eq!(drill.report.window.from_ms, Some(TAKEN_MS));

        // The restored index: after everything named, in a later epoch.
        let index = &drill.index;
        assert_eq!(index.applied, at(26));
        assert_eq!(index.epoch, Epoch::new(2));
        let fresh = &index.entries["fresh"];
        let layout = &fresh
            .object
            .as_ref()
            .unwrap()
            .coded
            .as_ref()
            .unwrap()
            .stripes[0];
        assert_eq!(layout.fragments()[1].node, node(0), "on the lost node");
        assert_eq!(layout.fragments()[1].fragment, UNKNOWN_FRAGMENT);
        install(&drill);
    });
}

/// Installs the drill's index as a learner installs a snapshot and reads
/// it back.
fn install(drill: &Drill) {
    let restored = &drill.index;
    let shard = shard_ref();
    let disk = SimDisk::new(9);
    let index = Index::open_sim(&disk.mount(), "index.redb", &IndexConfig::default()).unwrap();
    index
        .begin_install(&shard, EpochSeq::new(restored.epoch, Seq::MAX))
        .unwrap();
    let rows = restored.rows().unwrap();
    for (table, rows) in &rows {
        index.install_rows(&shard, *table, rows).unwrap();
    }
    let config = ShardConfig {
        bucket_id: shard.bucket.clone(),
        shard: shard.shard,
        epoch: restored.epoch,
        primary: node(8),
        members: vec![node(8)],
        learners: Vec::new(),
        min_write_replicas: 1,
        replicas: 1,
        proposal_id: ProposalId::new("p-restored").unwrap(),
    };
    index.finish_install(&config, restored.applied).unwrap();
    let reader = index.read().unwrap();
    for (key, entry) in &restored.entries {
        assert_eq!(reader.entry(&shard, key).unwrap().as_ref(), Some(entry));
    }
    // Repair finds the stripes from the index from node to fragments.
    let fragment_rows: usize = rows
        .iter()
        .filter(|(table, _)| *table == ShardTable::Fragments)
        .map(|(_, rows)| rows.len())
        .sum();
    assert_eq!(fragment_rows, 5 * restored.entries.len());
    let listed = reader.fragment_nodes(&shard).unwrap();
    assert!(!listed.is_empty());
}

#[test]
fn a_durable_home_overrules_what_headers_restore() {
    runtime().block_on(async {
        let store = remote(2, false);
        snapshot(
            &store,
            20,
            &[
                ("held", entry(10, 1, Some(1))),
                ("unrecoverable-held", entry(11, 2, Some(1))),
                ("unrecoverable", entry(12, 3, Some(1))),
            ],
        )
        .await;
        let mut found = fragments("held", 10, 1, 1);
        found.extend(fragments("deleted-after", 22, 4, 2));
        found.extend(fragments("overwritten-after", 23, 5, 2));
        found.extend(fragments("kept", 24, 6, 3));
        let headers = Headers::new(found);
        // The home holds the version of `held` and `kept`, a later write of
        // `overwritten-after`, nothing of `deleted-after`, and the version
        // of `unrecoverable-held`.
        let home_etag = at_home(&store, "kept", 24).await;
        at_home(&store, "held", 10).await;
        at_home(&store, "overwritten-after", 30).await;
        at_home(&store, "unrecoverable-held", 11).await;
        let drill = run(&store, &headers, true, &[0]).await;

        let restored: Vec<&str> = drill.restored.iter().map(|r| r.key.as_str()).collect();
        assert_eq!(restored, vec!["held", "kept"]);
        assert_eq!(lost_keys(&drill), vec!["unrecoverable"]);
        // A version restored from headers that the home holds is clean.
        let kept = &drill.index.entries["kept"];
        assert_eq!(kept.state, EntryState::Clean);
        assert_eq!(kept.remote_etag, Some(home_etag));
        // So is one the snapshot held dirty: the flush landed after it.
        assert_eq!(drill.index.entries["held"].state, EntryState::Clean);
    });
}

#[test]
fn a_drill_without_a_snapshot_restores_from_headers_alone() {
    runtime().block_on(async {
        let store = remote(3, false);
        let headers = Headers::new(fragments("fresh", 25, 4, 1));
        let drill = run(&store, &headers, false, &[]).await;
        assert_eq!(drill.report.snapshot, None);
        assert_eq!(drill.report.window.from_ms, None);
        assert!(drill.report.lost.is_empty());
        assert_eq!(drill.report.coded, vec!["fresh"]);
        assert!(drill.restored[0].after_snapshot);
        install(&drill);
    });
}
