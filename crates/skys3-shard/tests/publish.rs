//! `EC_PUBLISH` (§8.4): it codes the key's current version only, keeps the
//! version, and enters the stripes' fragments in the shard's index from
//! node to fragments; a later write supersedes it.

mod support;

use skys3_index::{Holder, Index};
use skys3_log::RecordBody;
use skys3_log::record::EcPublish;
use skys3_shard::{Effect, Outcome, Rejection, SeededBug};
use skys3_types::{
    AttemptId, CodecId, CodedStripe, Epoch, FragmentId, FragmentLocation, Geometry, NodeId,
};
use support::{apply, at, delete, entry, etag, new_index, put, record, shard, tags};

/// Applies the body at `seq` and returns its outcome.
fn step(index: &Index, seq: u64, body: RecordBody) -> Outcome {
    apply(index, &record(at(seq), body)).expect("a new position is applied")
}

fn node(n: usize) -> NodeId {
    NodeId::new(format!("n{n}")).unwrap()
}

/// An `EC_PUBLISH` of `key`'s version at `version` with `tag`'s ETag and
/// `len` bytes, in one 2+1 stripe on nodes 1 to 3.
fn publish(key: &str, version: u64, tag: u64, len: u64) -> RecordBody {
    let fragments = (1..=3)
        .map(|n| FragmentLocation {
            node: node(n),
            fragment: FragmentId::new(n as u128),
        })
        .collect();
    let geometry = Geometry::new(2, 1).unwrap();
    let stripe = CodedStripe::new(0, 0, len, geometry, CodecId::CURRENT, fragments).unwrap();
    RecordBody::EcPublish(EcPublish {
        key: key.into(),
        version: at(version),
        etag: etag(tag),
        attempt: AttemptId::new(Epoch::new(1), 7),
        size: len,
        stripes: vec![stripe],
    })
}

fn on(index: &Index, n: usize) -> usize {
    index
        .read()
        .unwrap()
        .fragments_on(&shard(0), &node(n))
        .unwrap()
        .len()
}

const PUBLISHED: Outcome = Outcome::Applied(Effect::Published);

#[test]
fn a_publish_codes_the_current_version_and_keeps_it() {
    let (_, index) = new_index();
    step(&index, 1, put("k", 10, 1));
    let before = entry(&index, "k").unwrap();
    assert_eq!(step(&index, 2, publish("k", 1, 1, 10)), PUBLISHED);
    let after = entry(&index, "k").unwrap();
    assert_eq!((after.version, after.state), (before.version, before.state));
    let object = after.object.unwrap();
    assert_eq!(object.payload, before.object.unwrap().payload);
    let coded = object.coded.unwrap();
    assert_eq!(coded.publish, at(2));
    assert_eq!(coded.attempt, AttemptId::new(Epoch::new(1), 7));
    assert_eq!(coded.stripes.len(), 1);
    assert_eq!(on(&index, 2), 1);
    let holders = index.read().unwrap().holders(&shard(0), "k").unwrap();
    assert_eq!(
        holders.get(&at(1)),
        Some(&Holder::Coded { publish: at(2) })
    );

    // A second publish of the version is refused; tags keep the layout.
    assert_eq!(
        step(&index, 3, publish("k", 1, 1, 10)),
        Outcome::Rejected(Rejection::AlreadyCoded)
    );
    step(&index, 4, tags("k", "v"));
    let tagged = entry(&index, "k").unwrap();
    assert_eq!(tagged.version, at(4));
    assert!(tagged.object.unwrap().coded.is_some());
    assert_eq!(on(&index, 1), 1);

    // An overwrite replaces the coded version and its fragment rows.
    step(&index, 5, put("k", 10, 5));
    assert!(entry(&index, "k").unwrap().object.unwrap().coded.is_none());
    assert_eq!(on(&index, 1), 0);
}

#[test]
fn a_publish_of_a_superseded_version_is_dropped() {
    let (_, index) = new_index();
    assert_eq!(
        step(&index, 1, publish("k", 0, 1, 10)),
        Outcome::Rejected(Rejection::NoEntry)
    );
    step(&index, 2, put("k", 10, 2));
    step(&index, 3, put("k", 10, 3));
    let superseded = Outcome::Rejected(Rejection::Superseded);
    // An older version, an ETag or size that differs.
    assert_eq!(step(&index, 4, publish("k", 2, 2, 10)), superseded);
    assert_eq!(step(&index, 5, publish("k", 3, 2, 10)), superseded);
    assert_eq!(step(&index, 6, publish("k", 3, 3, 9)), superseded);
    // A tag change is a new version too.
    step(&index, 7, tags("k", "v"));
    assert_eq!(step(&index, 8, publish("k", 3, 3, 10)), superseded);
    step(&index, 9, delete("k"));
    assert_eq!(
        step(&index, 10, publish("k", 7, 3, 10)),
        Outcome::Rejected(Rejection::Deleted)
    );
    assert_eq!(on(&index, 1), 0);
    assert_eq!(
        Rejection::Superseded.to_string(),
        "the published version is superseded"
    );
    assert_eq!(
        Rejection::AlreadyCoded.to_string(),
        "the version is coded already"
    );
}

#[test]
fn a_seeded_bug_applies_a_superseded_publish() {
    let (_, index) = new_index();
    step(&index, 1, put("k", 10, 1));
    step(&index, 2, put("k", 10, 2));
    skys3_shard::seed_bug(Some(SeededBug::PublishSuperseded));
    let outcome = step(&index, 3, publish("k", 1, 1, 10));
    skys3_shard::seed_bug(None);
    assert_eq!(outcome, PUBLISHED);
    assert_eq!(entry(&index, "k").unwrap().version, at(2));
}

#[test]
fn a_delete_of_a_coded_version_removes_its_rows() {
    let (_, index) = new_index();
    step(&index, 1, put("k", 10, 1));
    assert_eq!(step(&index, 2, publish("k", 1, 1, 10)), PUBLISHED);
    assert_eq!(on(&index, 3), 1);
    step(&index, 3, delete("k"));
    assert_eq!(on(&index, 3), 0);
}
