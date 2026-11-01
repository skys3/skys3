//! `EC_RELOCATE` (§8.6): it moves fragments of the key's current coded
//! version to new locations, all or none, and the shard's index from node
//! to fragments follows; a record whose version or fragments moved on is
//! dropped.

mod support;

use skys3_index::Index;
use skys3_log::RecordBody;
use skys3_log::record::{EcPublish, EcRelocate, FragmentMove};
use skys3_shard::{Effect, Outcome, Rejection};
use skys3_types::{
    AttemptId, CodecId, CodedStripe, Epoch, FragmentId, FragmentLocation, Geometry, NodeId,
};
use support::{apply, at, delete, entry, etag, new_index, put, record, shard, tags};

fn step(index: &Index, seq: u64, body: RecordBody) -> Outcome {
    apply(index, &record(at(seq), body)).expect("a new position is applied")
}

fn location(node: usize, id: u128) -> FragmentLocation {
    FragmentLocation {
        node: NodeId::new(format!("n{node}")).unwrap(),
        fragment: FragmentId::new(id),
    }
}

/// An `EC_PUBLISH` of `key`'s version at `version`: two 2+1 stripes of 10
/// bytes, stripe `s` with fragment `i` on node `s + i + 1` under the ID
/// `10 s + i`.
fn publish(key: &str, version: u64) -> RecordBody {
    let geometry = Geometry::new(2, 1).unwrap();
    let stripes = (0..2u32)
        .map(|s| {
            let fragments = (0..3)
                .map(|i| location(s as usize + i + 1, u128::from(10 * s) + i as u128))
                .collect();
            CodedStripe::new(
                s,
                u64::from(s) * 10,
                10,
                geometry,
                CodecId::CURRENT,
                fragments,
            )
            .unwrap()
        })
        .collect();
    RecordBody::EcPublish(EcPublish {
        key: key.into(),
        version: at(version),
        etag: etag(version),
        attempt: AttemptId::new(Epoch::new(1), 1),
        size: 20,
        stripes,
    })
}

/// An `EC_RELOCATE` of `key`'s version at `version` with `moves`.
fn relocate(key: &str, version: u64, moves: Vec<FragmentMove>) -> RecordBody {
    RecordBody::EcRelocate(EcRelocate {
        key: key.into(),
        version: at(version),
        etag: etag(version),
        attempt: AttemptId::new(Epoch::new(1), 2),
        moves,
    })
}

/// Fragment `index` of stripe `stripe`, from `from` to `to`.
fn moved(stripe: u32, index: u8, from: FragmentLocation, to: FragmentLocation) -> FragmentMove {
    FragmentMove {
        stripe,
        index,
        from,
        to,
    }
}

/// The fragments node `n` holds, by key, stripe, index, and ID.
fn on(index: &Index, n: usize) -> Vec<(String, u32, u8, u128)> {
    index
        .read()
        .unwrap()
        .fragments_on(&shard(0), &NodeId::new(format!("n{n}")).unwrap())
        .unwrap()
        .into_iter()
        .map(|(row, id)| (row.key, row.stripe, row.index, id.get()))
        .collect()
}

fn layout(index: &Index, key: &str) -> Vec<CodedStripe> {
    entry(index, key)
        .unwrap()
        .object
        .unwrap()
        .coded
        .unwrap()
        .stripes
}

const RELOCATED: Outcome = Outcome::Applied(Effect::Relocated);

#[test]
fn a_relocation_moves_fragments_of_the_current_layout() {
    let (_, index) = new_index();
    step(&index, 1, put("k", 20, 1));
    step(&index, 2, publish("k", 1));
    let before = entry(&index, "k").unwrap();
    // Node 2 holds fragment 1 of stripe 0 and fragment 0 of stripe 1.
    assert_eq!(
        on(&index, 2),
        [("k".into(), 0, 1, 1), ("k".into(), 1, 0, 10)]
    );
    let moves = vec![
        moved(0, 1, location(2, 1), location(7, 100)),
        moved(1, 0, location(2, 10), location(8, 101)),
    ];
    assert_eq!(step(&index, 3, relocate("k", 1, moves)), RELOCATED);

    let after = entry(&index, "k").unwrap();
    assert_eq!((after.version, after.state), (before.version, before.state));
    let coded = after.object.unwrap().coded.unwrap();
    assert_eq!(coded.publish, at(2), "the publish position stays");
    assert_eq!(coded.stripes[0].fragments()[1], location(7, 100));
    assert_eq!(coded.stripes[1].fragments()[0], location(8, 101));
    assert_eq!(coded.stripes[0].fragments()[0], location(1, 0));
    assert!(on(&index, 2).is_empty());
    assert_eq!(on(&index, 7), [("k".into(), 0, 1, 100)]);
    assert_eq!(on(&index, 8), [("k".into(), 1, 0, 101)]);
    let nodes = index.read().unwrap().fragment_nodes(&shard(0)).unwrap();
    let names: Vec<&str> = nodes.iter().map(NodeId::as_str).collect();
    assert_eq!(names, ["n1", "n3", "n4", "n7", "n8"]);

    // Tags keep the layout, relocated; an overwrite drops its rows.
    step(&index, 4, tags("k", "v"));
    assert_eq!(layout(&index, "k")[0].fragments()[1], location(7, 100));
    step(&index, 5, put("k", 20, 5));
    assert!(on(&index, 7).is_empty());
    assert!(
        index
            .read()
            .unwrap()
            .fragment_nodes(&shard(0))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn a_relocation_that_no_longer_fits_is_dropped_whole() {
    let (_, index) = new_index();
    let one = |from, to| vec![moved(0, 0, from, to)];
    assert_eq!(
        step(
            &index,
            1,
            relocate("k", 0, one(location(1, 0), location(9, 9)))
        ),
        Outcome::Rejected(Rejection::NoEntry)
    );
    step(&index, 2, put("k", 20, 2));
    assert_eq!(
        step(
            &index,
            3,
            relocate("k", 2, one(location(1, 0), location(9, 9)))
        ),
        Outcome::Rejected(Rejection::NotCoded)
    );
    step(&index, 4, publish("k", 2));
    let before = layout(&index, "k");

    // The fragment is no longer where the record moves it from: the second
    // move fails, so the first is not applied either.
    let stale = vec![
        moved(0, 0, location(1, 0), location(9, 9)),
        moved(1, 2, location(1, 12), location(9, 8)),
    ];
    assert_eq!(
        step(&index, 5, relocate("k", 2, stale)),
        Outcome::Rejected(Rejection::Moved {
            stripe: 1,
            index: 2
        })
    );
    // A stripe or index the layout does not have, or two fragments of a
    // stripe on one node.
    for (seq, bad) in [
        (6, moved(2, 0, location(1, 0), location(9, 9))),
        (7, moved(0, 3, location(1, 0), location(9, 9))),
        (8, moved(0, 0, location(1, 0), location(2, 9))),
    ] {
        assert!(matches!(
            step(&index, seq, relocate("k", 2, vec![bad])),
            Outcome::Rejected(Rejection::Moved { .. })
        ));
    }
    assert_eq!(layout(&index, "k"), before);
    assert_eq!(on(&index, 1), [("k".into(), 0, 0, 0)]);
    assert!(on(&index, 9).is_empty());

    // Another version, or another ETag.
    let superseded = Outcome::Rejected(Rejection::Superseded);
    assert_eq!(
        step(
            &index,
            9,
            relocate("k", 1, one(location(1, 0), location(9, 9)))
        ),
        superseded
    );
    let RecordBody::EcRelocate(mut other) = relocate("k", 2, one(location(1, 0), location(9, 9)))
    else {
        unreachable!()
    };
    other.etag = etag(3);
    assert_eq!(step(&index, 10, RecordBody::EcRelocate(other)), superseded);

    // Applied once, a relocation is stale the second time.
    let fresh = || relocate("k", 2, one(location(1, 0), location(9, 9)));
    assert_eq!(step(&index, 11, fresh()), RELOCATED);
    assert_eq!(
        step(&index, 12, fresh()),
        Outcome::Rejected(Rejection::Moved {
            stripe: 0,
            index: 0
        })
    );
    step(&index, 13, delete("k"));
    assert_eq!(
        step(&index, 14, fresh()),
        Outcome::Rejected(Rejection::Deleted)
    );
    assert_eq!(Rejection::NotCoded.to_string(), "the version is not coded");
}

#[test]
fn fragments_of_a_stripe_may_trade_nodes() {
    let (_, index) = new_index();
    step(&index, 1, put("k", 20, 1));
    step(&index, 2, publish("k", 1));
    // Stripe 0 has fragment 0 on n1 and fragment 1 on n2: each moves to
    // the other's node, which leaves the stripe on distinct nodes only
    // once both moved.
    let swap = vec![
        moved(0, 0, location(1, 0), location(2, 200)),
        moved(0, 1, location(2, 1), location(1, 201)),
    ];
    assert_eq!(step(&index, 3, relocate("k", 1, swap)), RELOCATED);
    let stripe = &layout(&index, "k")[0];
    assert_eq!(stripe.fragments()[0], location(2, 200));
    assert_eq!(stripe.fragments()[1], location(1, 201));
    assert_eq!(on(&index, 1), [("k".into(), 0, 1, 201)]);
}

#[test]
fn a_relocation_names_the_entry_s_version_which_a_retag_moves() {
    let (_, index) = new_index();
    step(&index, 1, put("k", 20, 1));
    step(&index, 2, publish("k", 1));
    step(&index, 3, tags("k", "v"));
    let one = || vec![moved(0, 0, location(1, 0), location(9, 9))];
    // The coded version's position no longer names the entry: a repair
    // that read the entry before the retag is fenced out.
    assert_eq!(
        step(&index, 4, relocate("k", 1, one())),
        Outcome::Rejected(Rejection::Superseded)
    );
    // The retag's position, with the version's ETag, does.
    let RecordBody::EcRelocate(mut retagged) = relocate("k", 3, one()) else {
        unreachable!()
    };
    retagged.etag = etag(1);
    assert_eq!(step(&index, 5, RecordBody::EcRelocate(retagged)), RELOCATED);
    let coded = entry(&index, "k").unwrap().object.unwrap().coded.unwrap();
    assert_eq!((coded.version, coded.publish), (at(1), at(2)));
    assert_eq!(coded.stripes[0].fragments()[0], location(9, 9));
}
