//! Every object-state transition (§4.2) that `PUT`, `DELETE`, `TAGS`,
//! `FLUSHED`, `ADOPT`, and `IMPORT` records drive, and the rejection of
//! every transition they may not make.

mod support;

use skys3_index::{EntryState, Index, Payload};
use skys3_log::record::{Extent, ExtentRef};
use skys3_log::{RecordBody, RecordKind};
use skys3_shard::{Effect, Outcome, Rejection};
use skys3_types::Seq;
use support::{
    adopt, apply, at, delete, dump, entry, etag, flushed, import, location, new_index, put,
    put_extents, record, set_state, shard, streamed, tags, upload_begin,
};

/// Applies the body at `seq` and returns its outcome.
fn step(index: &Index, seq: u64, body: RecordBody) -> Outcome {
    apply(index, &record(at(seq), body)).expect("a new position is applied")
}

/// Applies the body at `seq`, expects it rejected for `rejection`, and
/// checks that only the applied position changed.
fn rejected(index: &Index, seq: u64, body: RecordBody, rejection: Rejection) {
    let before = dump(index);
    assert_eq!(step(index, seq, body), Outcome::Rejected(rejection));
    let after = dump(index);
    assert_eq!(
        after.entries, before.entries,
        "a rejection keeps every entry"
    );
    assert_eq!(after.locations, before.locations);
    assert_eq!(after.applied.get(&shard(0)), Some(&at(seq)));
}

/// An index where `k` was put at seq 1 and flushed at seq 2: clean.
fn clean() -> Index {
    let (_, index) = new_index();
    step(&index, 1, put("k", 10, 1));
    assert_eq!(
        step(&index, 2, flushed("k", 1, false)),
        Outcome::Applied(Effect::Cleaned)
    );
    index
}

/// An index where `k` is an imported stub at seq 1: evicted.
fn evicted() -> Index {
    let (_, index) = new_index();
    assert_eq!(
        step(&index, 1, import("k", 5)),
        Outcome::Applied(Effect::Imported)
    );
    index
}

fn state(index: &Index) -> EntryState {
    entry(index, "k").unwrap().state
}

const DIRTY: Outcome = Outcome::Applied(Effect::Stored {
    state: EntryState::Dirty,
});
const TOMBSTONED: Outcome = Outcome::Applied(Effect::Tombstoned {
    state: EntryState::Dirty,
});

#[test]
fn a_put_of_a_new_key_is_dirty_with_inline_payload() {
    let (_, index) = new_index();
    assert_eq!(step(&index, 1, put("k", 10, 1)), DIRTY);
    let entry = entry(&index, "k").unwrap();
    assert_eq!(entry.version, at(1));
    assert_eq!(entry.state, EntryState::Dirty);
    assert_eq!(entry.remote_etag, None, "the remote state is unknown");
    let object = entry.object.unwrap();
    assert_eq!((object.size, object.local_etag), (10, etag(1)));
    assert_eq!(object.payload, Payload::Inline(at(1)));
    assert_eq!(object.write_identity, None);
    let located = index.read().unwrap().location(&shard(0), at(1)).unwrap();
    assert_eq!(located, Some(location(at(1))));
}

#[test]
fn a_put_references_extents_applied_before_it() {
    let (_, index) = new_index();
    for (seq, offset) in [(1, 0), (2, 1000)] {
        let body = RecordBody::Extent(Extent {
            key: "big".into(),
            offset,
            data: support::fill(1000),
        });
        assert_eq!(step(&index, seq, body), Outcome::Applied(Effect::Located));
    }
    let extents = vec![
        ExtentRef {
            position: at(1),
            len: 1000,
        },
        ExtentRef {
            position: at(2),
            len: 1000,
        },
    ];
    assert_eq!(
        step(&index, 3, put_extents("big", extents.clone(), 3)),
        DIRTY
    );
    let object = entry(&index, "big").unwrap().object.unwrap();
    assert_eq!(object.payload, Payload::Extents(extents));
    assert_eq!(object.size, 2000);
    let reader = index.read().unwrap();
    for seq in 1..=2 {
        assert_eq!(
            reader.location(&shard(0), at(seq)).unwrap(),
            Some(location(at(seq)))
        );
    }
    assert_eq!(
        reader.location(&shard(0), at(3)).unwrap(),
        None,
        "no inline payload"
    );
}

#[test]
fn a_delete_of_a_new_key_leaves_a_tombstone() {
    let (_, index) = new_index();
    assert_eq!(step(&index, 1, delete("k")), TOMBSTONED);
    let entry = entry(&index, "k").unwrap();
    assert_eq!((entry.version, entry.state), (at(1), EntryState::Dirty));
    assert!(entry.object.is_none());
}

#[test]
fn dirty_entries_take_overwrites_and_deletes() {
    let (_, index) = new_index();
    step(&index, 1, put("k", 10, 1));
    assert_eq!(step(&index, 2, put("k", 20, 2)), DIRTY);
    assert_eq!(entry(&index, "k").unwrap().object.unwrap().size, 20);
    assert_eq!(step(&index, 3, delete("k")), TOMBSTONED);
    assert_eq!(step(&index, 4, delete("k")), TOMBSTONED);
    assert_eq!(entry(&index, "k").unwrap().version, at(4));
    assert_eq!(step(&index, 5, put("k", 30, 5)), DIRTY);
}

#[test]
fn flushing_entries_become_dirty_on_a_write() {
    let (_, index) = new_index();
    step(&index, 1, put("k", 10, 1));
    set_state(&index, at(2), "k", EntryState::Flushing);
    assert_eq!(step(&index, 3, put("k", 20, 3)), DIRTY);
    set_state(&index, at(4), "k", EntryState::Flushing);
    assert_eq!(step(&index, 5, delete("k")), TOMBSTONED);
}

#[test]
fn flushed_cleans_a_dirty_or_flushing_entry_at_its_seq() {
    let index = clean();
    let entry = entry(&index, "k").unwrap();
    assert_eq!(entry.state, EntryState::Clean);
    assert_eq!(entry.version, at(1), "flushing makes no new version");
    assert_eq!(entry.remote_etag, Some(etag(1001)));
    assert_eq!(entry.remote_version_id.as_deref(), Some("v1"));

    let (_, index) = new_index();
    step(&index, 1, put("k", 10, 1));
    set_state(&index, at(2), "k", EntryState::Flushing);
    assert_eq!(
        step(&index, 3, flushed("k", 1, false)),
        Outcome::Applied(Effect::Cleaned)
    );
    assert_eq!(state(&index), EntryState::Clean);
}

#[test]
fn flushed_of_a_tombstone_removes_the_entry() {
    let index = clean();
    assert_eq!(step(&index, 3, delete("k")), TOMBSTONED);
    let tombstone = entry(&index, "k").unwrap();
    assert_eq!(
        tombstone.remote_etag,
        Some(etag(1001)),
        "the remote still holds it"
    );
    assert_eq!(
        step(&index, 4, flushed("k", 3, true)),
        Outcome::Applied(Effect::Removed)
    );
    assert_eq!(entry(&index, "k"), None);
}

#[test]
fn a_stale_flushed_records_the_remote_and_keeps_the_newer_version_dirty() {
    let (_, index) = new_index();
    step(&index, 1, put("k", 10, 1));
    set_state(&index, at(2), "k", EntryState::Flushing);
    // A newer version commits while seq 1 is being flushed.
    step(&index, 3, put("k", 20, 3));
    assert_eq!(
        step(&index, 4, flushed("k", 1, false)),
        Outcome::Applied(Effect::RemoteRecorded)
    );
    let entry = entry(&index, "k").unwrap();
    assert_eq!((entry.version, entry.state), (at(3), EntryState::Dirty));
    assert_eq!(
        entry.remote_etag,
        Some(etag(1001)),
        "the next flush conditions on it"
    );
    // The newer version's own flush then cleans it.
    assert_eq!(
        step(&index, 5, flushed("k", 3, false)),
        Outcome::Applied(Effect::Cleaned)
    );
}

#[test]
fn conflicted_entries_stay_in_conflict_until_resolved() {
    let (_, index) = new_index();
    step(&index, 1, put("k", 10, 1));
    set_state(&index, at(2), "k", EntryState::Conflict);
    let conflict = Outcome::Applied(Effect::Stored {
        state: EntryState::Conflict,
    });
    assert_eq!(step(&index, 3, put("k", 20, 3)), conflict);
    assert_eq!(step(&index, 4, tags("k", "x")), conflict);
    assert_eq!(
        step(&index, 5, delete("k")),
        Outcome::Applied(Effect::Tombstoned {
            state: EntryState::Conflict
        })
    );
    assert_eq!(step(&index, 6, put("k", 30, 6)), conflict);
    // An unconditional flush under the `overwrite` policy resolves it.
    assert_eq!(
        step(&index, 7, flushed("k", 6, false)),
        Outcome::Applied(Effect::Cleaned)
    );
}

#[test]
fn clean_and_evicted_entries_become_dirty_on_a_write() {
    for (setup, seq) in [(clean as fn() -> Index, 3), (evicted, 2)] {
        let index = setup();
        let remote = entry(&index, "k").unwrap().remote_etag;
        assert_eq!(step(&index, seq, put("k", 20, seq)), DIRTY);
        assert_eq!(
            entry(&index, "k").unwrap().remote_etag,
            remote,
            "kept for the flush"
        );

        let index = setup();
        assert_eq!(step(&index, seq, delete("k")), TOMBSTONED);
        let entry = entry(&index, "k").unwrap();
        assert_eq!(entry.state, EntryState::Dirty);
        assert_eq!(entry.remote_etag, remote);
    }
}

#[test]
fn tags_make_a_new_dirty_version_of_the_same_bytes() {
    let index = clean();
    let before = entry(&index, "k").unwrap().object.unwrap();
    assert_eq!(step(&index, 3, tags("k", "blue")), DIRTY);
    let entry = entry(&index, "k").unwrap();
    assert_eq!((entry.version, entry.state), (at(3), EntryState::Dirty));
    let object = entry.object.unwrap();
    assert_eq!(object.tags.get("t").map(String::as_str), Some("blue"));
    assert_eq!(object.payload, before.payload);
    assert_eq!(object.last_modified_ms, before.last_modified_ms);
    assert_eq!(object.local_etag, before.local_etag);
    // A flush of the older version no longer cleans it.
    assert_eq!(
        step(&index, 4, flushed("k", 1, false)),
        Outcome::Applied(Effect::RemoteRecorded)
    );
    assert_eq!(state(&index), EntryState::Dirty);
}

#[test]
fn tags_need_a_live_object() {
    let (_, index) = new_index();
    rejected(&index, 1, tags("k", "x"), Rejection::NoEntry);
    step(&index, 2, delete("k"));
    rejected(&index, 3, tags("k", "x"), Rejection::Deleted);
}

#[test]
fn flushed_is_rejected_unless_it_names_the_entry() {
    let (_, index) = new_index();
    rejected(&index, 1, flushed("k", 1, false), Rejection::NoEntry);
    step(&index, 2, put("k", 10, 2));
    rejected(
        &index,
        3,
        flushed("k", 3, false),
        Rejection::UnknownVersion {
            flushed: Seq::new(3),
            current: Seq::new(2),
        },
    );
    rejected(
        &index,
        4,
        flushed("k", 2, true),
        Rejection::RemoteEtagMismatch,
    );
    step(&index, 5, flushed("k", 2, false));
    rejected(&index, 6, flushed("k", 2, false), Rejection::AlreadyClean);
    rejected(&index, 7, flushed("k", 1, false), Rejection::Stale);

    step(&index, 8, delete("k"));
    rejected(
        &index,
        9,
        flushed("k", 8, false),
        Rejection::RemoteEtagMismatch,
    );

    let index = evicted();
    rejected(&index, 2, flushed("k", 1, false), Rejection::AlreadyClean);
}

#[test]
fn import_creates_a_stub_only_for_a_key_with_no_entry() {
    let index = evicted();
    let entry = entry(&index, "k").unwrap();
    assert_eq!((entry.version, entry.state), (at(1), EntryState::Evicted));
    assert_eq!(entry.remote_etag, Some(etag(5)));
    let object = entry.object.unwrap();
    assert_eq!((object.size, object.local_etag), (42, etag(5)));
    assert_eq!(object.payload, Payload::None);
    assert_eq!(object.storage_class.as_deref(), Some("STANDARD"));
    assert!(object.metadata.is_empty(), "metadata is loaded lazily");

    // A stub, or an entry whose remote ETag is known, blocks an import.
    rejected(&index, 2, import("k", 6), Rejection::HasEntry);
    let index = clean();
    rejected(&index, 3, import("k", 6), Rejection::HasEntry);
    step(&index, 4, delete("k"));
    rejected(&index, 5, import("k", 6), Rejection::HasEntry);
}

#[test]
fn import_records_the_remote_etag_of_a_key_written_before_it() {
    // A write and a delete of keys the import had not reached: their
    // remote state is unknown until the import tells it.
    let (_, index) = new_index();
    step(&index, 1, put("k", 10, 1));
    step(&index, 2, delete("d"));
    for (seq, key) in [(3, "k"), (4, "d")] {
        assert_eq!(
            step(&index, seq, import(key, 6)),
            Outcome::Applied(Effect::RemoteRecorded)
        );
    }
    let written = entry(&index, "k").unwrap();
    assert_eq!(
        (written.version, written.state, written.remote_etag),
        (at(1), EntryState::Dirty, Some(etag(6))),
        "the local version stays, to replace the remote's"
    );
    assert_eq!(written.object.unwrap().size, 10);
    let deleted = entry(&index, "d").unwrap();
    assert_eq!((deleted.object, deleted.remote_etag), (None, Some(etag(6))));
    // Once known, the remote ETag is not replaced.
    rejected(&index, 5, import("k", 7), Rejection::HasEntry);
    rejected(&index, 6, import("d", 7), Rejection::HasEntry);
}

#[test]
fn a_deleted_key_is_imported_again_only_after_its_tombstone_is_flushed() {
    let (_, index) = new_index();
    step(&index, 1, delete("k"));
    assert_eq!(
        step(&index, 2, import("k", 6)),
        Outcome::Applied(Effect::RemoteRecorded),
        "the tombstone hides the remote object"
    );
    assert!(entry(&index, "k").unwrap().object.is_none());
    step(&index, 3, flushed("k", 1, true));
    assert_eq!(
        step(&index, 4, import("k", 6)),
        Outcome::Applied(Effect::Imported)
    );
}

#[test]
fn adopt_replaces_a_clean_or_evicted_entry_at_the_named_seq() {
    for (setup, seq, expected) in [(clean as fn() -> Index, 3, 1), (evicted, 2, 1)] {
        let index = setup();
        assert_eq!(
            step(&index, seq, adopt("k", expected, 77)),
            Outcome::Applied(Effect::Adopted)
        );
        let entry = entry(&index, "k").unwrap();
        assert_eq!(entry.version, at(seq), "a new version identity");
        assert_eq!(entry.state, EntryState::Evicted);
        assert_eq!(entry.remote_etag, Some(etag(77)));
        assert_eq!(entry.remote_version_id.as_deref(), Some("remote"));
        let object = entry.object.unwrap();
        assert_eq!((object.size, object.local_etag), (7, etag(77)));
        assert_eq!(object.payload, Payload::None);
        assert!(object.tags.is_empty());
        assert_eq!(
            object.metadata.get("content-type").map(String::as_str),
            Some("image/png")
        );
    }
}

#[test]
fn adopt_is_rejected_once_the_entry_changed() {
    let (_, index) = new_index();
    rejected(&index, 1, adopt("k", 1, 7), Rejection::NoEntry);

    let index = clean();
    rejected(
        &index,
        3,
        adopt("k", 2, 7),
        Rejection::VersionChanged {
            expected: Seq::new(2),
            current: Seq::new(1),
        },
    );
    // A local write committed after the read plan.
    step(&index, 4, put("k", 20, 4));
    rejected(
        &index,
        5,
        adopt("k", 1, 7),
        Rejection::NotClean(EntryState::Dirty),
    );
    rejected(
        &index,
        6,
        adopt("k", 4, 7),
        Rejection::NotClean(EntryState::Dirty),
    );
    step(&index, 7, delete("k"));
    rejected(
        &index,
        8,
        adopt("k", 7, 7),
        Rejection::NotClean(EntryState::Dirty),
    );

    for state in [EntryState::Flushing, EntryState::Conflict] {
        let (_, index) = new_index();
        step(&index, 1, put("k", 10, 1));
        set_state(&index, at(2), "k", state);
        rejected(&index, 3, adopt("k", 1, 7), Rejection::NotClean(state));
    }
}

#[test]
fn a_streamed_put_inherits_the_identity_of_its_upload_begin() {
    let index = clean();
    let before = dump(&index);
    // The record changes nothing but the applied position.
    assert_eq!(
        step(&index, 3, upload_begin("k")),
        Outcome::Applied(Effect::UploadBegun)
    );
    let after = dump(&index);
    assert_eq!(
        (after.entries, after.locations),
        (before.entries, before.locations)
    );
    assert_eq!(after.applied.get(&shard(0)), Some(&at(3)));

    // A write of the key between the two takes its own identity, and the
    // PUT that completes the upload inherits the begin's.
    assert_eq!(step(&index, 4, put("k", 20, 4)), DIRTY);
    assert_eq!(
        entry(&index, "k").unwrap().object.unwrap().write_identity,
        None
    );
    assert_eq!(step(&index, 5, streamed(put("k", 30, 5), at(3))), DIRTY);
    let entry = entry(&index, "k").unwrap();
    assert_eq!(entry.version, at(5));
    let object = entry.object.unwrap();
    assert_eq!((object.size, object.write_identity), (30, Some(at(3))));
    // The remote state the next flush conditions on is kept.
    assert_eq!(entry.remote_etag, Some(etag(1001)));

    // A tag change is a write of its own: the identity names it.
    assert_eq!(step(&index, 6, tags("k", "a")), DIRTY);
    let object = support::entry(&index, "k").unwrap().object.unwrap();
    assert_eq!(object.write_identity, None);
}

#[test]
fn a_config_or_truncate_changes_no_entry() {
    let index = clean();
    let before = dump(&index);
    assert_eq!(
        step(&index, 3, RecordBody::Truncate),
        Outcome::Applied(Effect::Unchanged)
    );
    let config = RecordBody::Config(support::config(&shard(0), 2));
    let position = skys3_types::EpochSeq::new(skys3_types::Epoch::new(2), Seq::new(3));
    assert_eq!(
        apply(&index, &support::record(position, config)),
        Some(Outcome::Applied(Effect::Unchanged))
    );
    assert_eq!(dump(&index).entries, before.entries);
}

#[test]
fn a_record_at_or_before_the_applied_position_is_skipped() {
    let index = clean();
    let before = dump(&index);
    assert_eq!(apply(&index, &record(at(2), put("k", 99, 9))), None);
    assert_eq!(dump(&index), before);
}

#[test]
fn outcomes_describe_themselves() {
    assert!(DIRTY.is_applied());
    assert!(!Outcome::Rejected(Rejection::Stale).is_applied());
    assert_eq!(
        Outcome::Rejected(Rejection::NotClean(EntryState::Dirty)).to_string(),
        "rejected: the entry is Dirty, not clean"
    );
    assert_eq!(
        Outcome::Applied(Effect::Cleaned).to_string(),
        "applied: Cleaned"
    );
    assert_eq!(
        Rejection::Unsupported(RecordKind::EcPublish).to_string(),
        "EcPublish records are not applied by this build"
    );
}
