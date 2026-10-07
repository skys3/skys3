//! A stripe's layout and codec, and its object's metadata, rebuilt from
//! fragment headers alone (plan M5-02): what an `EC_PUBLISH` record holds,
//! for a shard whose index was lost.

mod support;

use std::collections::BTreeMap;

use bytes::Bytes;
use skys3_ec::fragment::{FragmentHeader, StripeInfo};
use skys3_ec::{
    AttemptId, CodecId, FoundFragment, FragmentId, FragmentLocation, FragmentStore, Geometry,
    ObjectLayout, ObjectVersion, RebuildError, StripeLayout, codec, current_codec, rebuild_layouts,
};
use skys3_io::{SimDisk, SimMount};
use skys3_types::{Epoch, NodeId};
use support::{object, position, runtime, sample, shard, small_config};

const GEOMETRY: Geometry = Geometry::RS_4_2;

/// The object's stripes: offsets and data lengths.
const STRIPES: [(u64, u64); 3] = [(0, 10_000), (10_000, 10_000), (20_000, 3_333)];

const SIZE: u64 = 23_333;

fn node(n: usize) -> NodeId {
    NodeId::new(format!("n{n}")).unwrap()
}

fn attempt(epoch: u64, number: u64) -> AttemptId {
    AttemptId::new(Epoch::new(epoch), number)
}

/// The header of fragment `index` of stripe `number` of the test object,
/// written by `attempt`.
fn header(number: u32, index: u8, attempt: AttemptId) -> FragmentHeader {
    let (offset, data_len) = STRIPES[number as usize];
    FragmentHeader {
        shard: shard(),
        key: "videos/launch.mp4".to_owned(),
        version: position(4, 18),
        attempt,
        stripe: StripeInfo {
            number,
            count: STRIPES.len() as u32,
            offset,
            data_len,
            geometry: GEOMETRY,
            codec: CodecId::REED_SOLOMON_V1,
        },
        index,
        object: object(SIZE),
    }
}

/// Six nodes, each with a fragment store on its own disk.
struct Cluster {
    stores: BTreeMap<NodeId, FragmentStore<SimMount>>,
}

impl Cluster {
    async fn new() -> Self {
        let mut stores = BTreeMap::new();
        for n in 0..GEOMETRY.total_fragments() {
            let disk = SimDisk::new(n as u64);
            let (store, _) = FragmentStore::open(disk.mount(), small_config())
                .await
                .unwrap();
            stores.insert(node(n), store);
        }
        Self { stores }
    }

    /// Encodes `data` stripe by stripe and writes fragment `i` of stripe
    /// `s` to node `(s + i) mod 6`, as placement might. Returns the layout
    /// an `EC_PUBLISH` would record.
    async fn encode(&self, data: &[u8]) -> Vec<StripeLayout> {
        let mut layouts = Vec::new();
        for (number, &(offset, data_len)) in STRIPES.iter().enumerate() {
            let stripe = &data[offset as usize..(offset + data_len) as usize];
            let fragments = current_codec().encode(GEOMETRY, stripe).unwrap();
            let mut locations = Vec::new();
            for (index, fragment) in fragments.into_iter().enumerate() {
                let node = node((number + index) % GEOMETRY.total_fragments());
                let header = header(number as u32, index as u8, attempt(4, 1));
                let id = self.stores[&node]
                    .write(&header, Bytes::from(fragment))
                    .await
                    .unwrap();
                locations.push(Some(FragmentLocation { node, fragment: id }));
            }
            layouts.push(StripeLayout {
                number: number as u32,
                offset,
                data_len,
                geometry: GEOMETRY,
                codec: CodecId::REED_SOLOMON_V1,
                fragments: locations,
            });
        }
        layouts
    }

    /// Every fragment header on every node.
    async fn headers(&self) -> Vec<FoundFragment> {
        let mut found = Vec::new();
        for (node, store) in &self.stores {
            for id in store.ids() {
                found.push(FoundFragment {
                    location: FragmentLocation {
                        node: node.clone(),
                        fragment: id,
                    },
                    header: store.header(id).await.unwrap(),
                });
            }
        }
        found
    }

    /// Reads the object back through `layout` alone: each stripe's
    /// fragments from their nodes, decoded by the stripe's codec.
    async fn read(&self, layout: &ObjectLayout) -> Vec<u8> {
        let mut data = Vec::new();
        for stripe in &layout.stripes {
            let mut fragments: Vec<Option<Vec<u8>>> = Vec::new();
            for location in &stripe.fragments {
                let fragment = match location {
                    Some(location) => {
                        let store = &self.stores[&location.node];
                        let len = store.len(location.fragment).unwrap();
                        let read = store.read(location.fragment, 0..len).await.unwrap();
                        Some(read.data.to_vec())
                    }
                    None => None,
                };
                fragments.push(fragment);
            }
            let slots: Vec<Option<&[u8]>> = fragments.iter().map(Option::as_deref).collect();
            let codec = codec(stripe.codec).unwrap();
            data.extend(
                codec
                    .decode(stripe.geometry, stripe.data_len, &slots)
                    .unwrap(),
            );
        }
        data
    }
}

fn the_version() -> ObjectVersion {
    ObjectVersion {
        shard: shard(),
        key: "videos/launch.mp4".to_owned(),
        version: position(4, 18),
    }
}

/// The one object `found` names, rebuilt.
fn rebuild_one(found: Vec<FoundFragment>) -> Result<ObjectLayout, RebuildError> {
    let mut layouts = rebuild_layouts(found);
    assert_eq!(layouts.len(), 1);
    layouts.remove(&the_version()).unwrap()
}

#[test]
fn headers_alone_rebuild_every_stripe_with_up_to_m_fragments_missing() {
    runtime().block_on(async {
        let cluster = Cluster::new().await;
        let data = sample(SIZE as usize, 1);
        let published = cluster.encode(&data).await;
        let found = cluster.headers().await;
        assert_eq!(found.len(), STRIPES.len() * GEOMETRY.total_fragments());

        let total = GEOMETRY.total_fragments();
        let mut patterns = 0;
        for lost in 0u32..1 << total {
            if lost.count_ones() as usize > GEOMETRY.parity_fragments() {
                continue;
            }
            // The same fragment indexes are lost in every stripe.
            let kept = found
                .iter()
                .filter(|f| lost & 1 << f.header.index == 0)
                .cloned();
            let layout = rebuild_one(kept.collect()).unwrap();
            assert_eq!(layout.version, the_version());
            assert_eq!(layout.object, object(SIZE));
            let mut expected = published.clone();
            for stripe in &mut expected {
                for (index, slot) in stripe.fragments.iter_mut().enumerate() {
                    if lost & 1 << index != 0 {
                        *slot = None;
                    }
                }
            }
            assert_eq!(layout.stripes, expected, "lost {lost:#b}");
            assert_eq!(cluster.read(&layout).await, data, "lost {lost:#b}");
            patterns += 1;
        }
        assert_eq!(patterns, 22);

        // Different losses in different stripes, up to `m` each.
        let kept = found.iter().filter(|f| {
            let (stripe, index) = (f.header.stripe.number, f.header.index);
            !matches!((stripe, index), (0, 0 | 1) | (1, 3) | (2, 4 | 5))
        });
        let layout = rebuild_one(kept.cloned().collect()).unwrap();
        assert_eq!(
            layout
                .stripes
                .iter()
                .map(StripeLayout::located)
                .collect::<Vec<_>>(),
            [4, 5, 4]
        );
        assert_eq!(cluster.read(&layout).await, data);
    });
}

#[test]
fn too_few_fragments_are_reported_per_stripe() {
    let all: Vec<FoundFragment> = (0..STRIPES.len() as u32)
        .flat_map(|stripe| (0..6).map(move |index| found(stripe, index, attempt(4, 1), 0)))
        .collect();
    // Three fragments of stripe 1 lost.
    let kept = all
        .iter()
        .filter(|f| !(f.header.stripe.number == 1 && f.header.index < 3))
        .cloned();
    assert_eq!(
        rebuild_one(kept.collect()),
        Err(RebuildError::NotEnoughFragments {
            stripe: 1,
            needed: 4,
            available: 3,
        })
    );
    // Stripe 2 lost whole.
    let kept = all.iter().filter(|f| f.header.stripe.number != 2).cloned();
    assert_eq!(
        rebuild_one(kept.collect()),
        Err(RebuildError::MissingStripe { stripe: 2 })
    );
    let error = RebuildError::MissingStripe { stripe: 2 };
    assert_eq!(error.to_string(), "no fragment of stripe 2 was found");
}

/// A synthetic header found on node `node` with fragment ID `id`.
fn found(stripe: u32, index: u8, attempt: AttemptId, node: usize) -> FoundFragment {
    let id = u128::from(stripe) << 64 | u128::from(index) << 32 | u128::from(attempt.number);
    FoundFragment {
        location: FragmentLocation {
            node: self::node(node),
            fragment: FragmentId::new(id),
        },
        header: header(stripe, index, attempt),
    }
}

fn complete(attempt: AttemptId) -> Vec<FoundFragment> {
    (0..STRIPES.len() as u32)
        .flat_map(|stripe| (0..6).map(move |index| found(stripe, index, attempt, index.into())))
        .collect()
}

#[test]
fn later_attempts_win_where_copies_overlap() {
    let mut all = complete(attempt(4, 1));
    // A repair rebuilt fragment 2 of stripe 0 on node 9, after a tag change.
    let mut repaired = found(0, 2, attempt(5, 1), 9);
    repaired.header.object.tags = BTreeMap::from([("team".into(), "video".into())]);
    all.push(repaired.clone());
    let layout = rebuild_one(all).unwrap();
    assert_eq!(layout.stripes[0].fragments[2], Some(repaired.location));
    assert_eq!(layout.object.tags, repaired.header.object.tags);
    assert_eq!(
        layout.stripes[1].fragments[2].as_ref().unwrap().node,
        node(2)
    );
}

#[test]
fn an_incomplete_newer_attempt_falls_back_to_a_complete_older_one() {
    let mut all = complete(attempt(4, 1));
    // An abandoned encoding cut the object into two stripes and wrote only
    // three fragments of its first.
    for index in 0..3 {
        let mut abandoned = found(0, index, attempt(6, 2), 7);
        abandoned.header.stripe.count = 2;
        abandoned.header.stripe.data_len = 20_000;
        all.push(abandoned);
    }
    let layout = rebuild_one(all.clone()).unwrap();
    assert_eq!(layout.stripes.len(), 3);
    assert!(layout.stripes.iter().all(|s| s.located() == 6));
    // Without the older attempt, the newer one's error is reported.
    let newer: Vec<_> = all
        .into_iter()
        .filter(|f| f.header.attempt == attempt(6, 2))
        .collect();
    assert_eq!(
        rebuild_one(newer),
        Err(RebuildError::NotEnoughFragments {
            stripe: 0,
            needed: 4,
            available: 3,
        })
    );
}

#[test]
fn headers_that_disagree_or_leave_gaps_are_refused() {
    let mut all = complete(attempt(4, 1));
    let odd = all.len() - 1;
    all[odd].header.object.last_modified_ms += 1;
    let other = all[odd].location.clone();
    match rebuild_one(all) {
        Err(RebuildError::InconsistentObject { other: found, .. }) => assert_eq!(found, other),
        unexpected => panic!("{unexpected:?}"),
    }

    // Stripe 1 starts one byte late.
    let mut all = complete(attempt(4, 1));
    for fragment in all.iter_mut().filter(|f| f.header.stripe.number == 1) {
        fragment.header.stripe.offset += 1;
    }
    assert_eq!(rebuild_one(all), Err(RebuildError::Gap { stripe: 1 }));

    // The stripes end before the object does.
    let mut all = complete(attempt(4, 1));
    for fragment in &mut all {
        fragment.header.object.size += 1;
    }
    assert_eq!(rebuild_one(all), Err(RebuildError::Gap { stripe: 3 }));
}

#[test]
fn each_object_version_is_rebuilt_on_its_own() {
    let mut all = complete(attempt(4, 1));
    let mut other = complete(attempt(4, 2));
    for fragment in &mut other {
        fragment.header.version = position(4, 30);
        fragment.location.fragment = FragmentId::new(fragment.location.fragment.get() + 1);
    }
    // The other version lost a stripe.
    other.retain(|f| f.header.stripe.number != 0);
    all.extend(other);
    let layouts = rebuild_layouts(all);
    assert_eq!(layouts.len(), 2);
    assert!(layouts[&the_version()].is_ok());
    let other = ObjectVersion {
        version: position(4, 30),
        ..the_version()
    };
    assert_eq!(
        layouts[&other],
        Err(RebuildError::MissingStripe { stripe: 0 })
    );
}
