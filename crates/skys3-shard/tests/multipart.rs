//! What the multipart records do to the index: uploads, parts, completion
//! with the upload's write identity, and the release of bytes no upload or
//! object can reach (§7.2, §7.4, §10.3).

mod support;

use skys3_index::{EntryState, Index, ObjectPart, Payload};
use skys3_log::RecordBody;
use skys3_log::record::ExtentRef;
use skys3_shard::{Effect, Outcome, Rejection};
use skys3_types::EpochSeq;
use support::{
    adopt, apply, at, delete, dump, entry, extent, flushed, location, mpu_abort, mpu_complete,
    mpu_create, mpu_part, new_index, put, record, shard,
};

fn step(index: &Index, seq: u64, body: RecordBody) -> Outcome {
    apply(index, &record(at(seq), body)).expect("a new position is applied")
}

fn located(index: &Index, position: EpochSeq) -> bool {
    let reader = index.read().unwrap();
    reader.location(&shard(0), position).unwrap().is_some()
}

const DIRTY: Outcome = Outcome::Applied(Effect::Stored {
    state: EntryState::Dirty,
});

/// An upload of `k` opened at 1, with part 1 inline at 2, part 2 in the
/// extents at 3 and 4 and stored at 5, and part 3 inline at 6.
fn open_upload() -> Index {
    let (_, index) = new_index();
    assert_eq!(
        step(&index, 1, mpu_create("k")),
        Outcome::Applied(Effect::UploadCreated)
    );
    let stored = Outcome::Applied(Effect::PartStored);
    assert_eq!(step(&index, 2, mpu_part("k", at(1), 1, 10, vec![])), stored);
    for (seq, offset) in [(3, 0), (4, 100)] {
        let body = RecordBody::Extent(extent("k", offset, 100));
        assert_eq!(step(&index, seq, body), Outcome::Applied(Effect::Located));
    }
    let extents = vec![
        ExtentRef {
            position: at(3),
            len: 100,
        },
        ExtentRef {
            position: at(4),
            len: 100,
        },
    ];
    assert_eq!(step(&index, 5, mpu_part("k", at(1), 2, 0, extents)), stored);
    assert_eq!(step(&index, 6, mpu_part("k", at(1), 3, 5, vec![])), stored);
    index
}

#[test]
fn an_upload_holds_its_parts_until_it_completes() {
    let index = open_upload();
    let reader = index.read().unwrap();
    let upload = reader.upload(&shard(0), "k", at(1)).unwrap().unwrap();
    assert_eq!(upload.metadata["content-type"], "video/mp4");
    let parts = reader.parts(&shard(0), at(1), 0, 10).unwrap();
    let numbers: Vec<_> = parts
        .iter()
        .map(|(n, p)| (*n, p.position, p.size))
        .collect();
    assert_eq!(numbers, [(1, at(2), 10), (2, at(5), 200), (3, at(6), 5)]);
    assert_eq!(parts[0].1.payload, Payload::Inline(at(2)));
    // Inline parts are located like inline PUTs; an upload is no object.
    assert!(located(&index, at(2)) && located(&index, at(6)));
    assert_eq!(entry(&index, "k"), None);
}

#[test]
fn completing_an_upload_stores_an_object_with_its_identity_and_boundaries() {
    let index = open_upload();
    // Part 2 is left out.
    let body = mpu_complete("k", at(1), &[(1, at(2)), (3, at(6))], 15);
    assert_eq!(step(&index, 7, body), DIRTY);
    let entry = entry(&index, "k").unwrap();
    assert_eq!(entry.version, at(7));
    let object = entry.object.unwrap();
    assert_eq!(
        object.write_identity,
        Some(at(1)),
        "the MPU_CREATE identity"
    );
    assert_eq!(object.size, 15);
    assert_eq!(object.local_etag.as_str(), format!("{:032x}-2", 1));
    assert_eq!(object.metadata["content-type"], "video/mp4");
    assert_eq!(
        object.payload,
        Payload::Parts {
            upload: at(1),
            parts: vec![
                ObjectPart {
                    number: 1,
                    size: 10
                },
                ObjectPart { number: 3, size: 5 },
            ],
        }
    );
    // The upload is gone; the object's parts stay, and the part left out
    // is released.
    let after = dump(&index);
    assert!(after.uploads.is_empty());
    let kept: Vec<_> = after.parts.keys().map(|(_, _, n)| *n).collect();
    assert_eq!(kept, [1, 3]);
    assert!(!located(&index, at(3)) && !located(&index, at(4)));
    assert!(located(&index, at(2)) && located(&index, at(6)));

    // It flushes like any version, and a later write drops its parts.
    assert_eq!(
        step(&index, 8, flushed("k", 7, false)),
        Outcome::Applied(Effect::Cleaned)
    );
    assert_eq!(step(&index, 9, put("k", 3, 9)), DIRTY);
    assert!(dump(&index).parts.is_empty());
}

#[test]
fn a_completion_needs_the_open_upload_and_the_parts_it_read() {
    let index = open_upload();
    let before = dump(&index);
    for (seq, body, rejection) in [
        (
            7,
            mpu_complete("k", at(1), &[(1, at(2)), (2, at(4))], 1),
            Rejection::PartChanged { number: 2 },
        ),
        (
            8,
            mpu_complete("k", at(1), &[(1, at(2)), (4, at(6))], 1),
            Rejection::PartChanged { number: 4 },
        ),
        (
            9,
            mpu_complete("k", at(2), &[(1, at(3))], 1),
            Rejection::NoSuchUpload,
        ),
        (
            10,
            mpu_complete("other", at(1), &[(1, at(2))], 1),
            Rejection::NoSuchUpload,
        ),
    ] {
        assert_eq!(step(&index, seq, body), Outcome::Rejected(rejection));
    }
    let after = dump(&index);
    assert_eq!(after.uploads, before.uploads);
    assert_eq!(after.parts, before.parts);
    assert_eq!(after.locations, before.locations);

    // Once completed, the upload takes no more parts and no second
    // completion or abort.
    assert_eq!(
        step(&index, 11, mpu_complete("k", at(1), &[(1, at(2))], 10)),
        DIRTY
    );
    let gone = Outcome::Rejected(Rejection::NoSuchUpload);
    assert_eq!(
        step(&index, 12, mpu_complete("k", at(1), &[(1, at(2))], 10)),
        gone
    );
    assert_eq!(step(&index, 13, mpu_abort("k", at(1))), gone);
    assert_eq!(step(&index, 14, mpu_part("k", at(1), 1, 4, vec![])), gone);
    assert!(
        !located(&index, at(14)),
        "a refused part's bytes are released"
    );
}

#[test]
fn aborting_an_upload_releases_every_part() {
    let index = open_upload();
    assert_eq!(
        step(&index, 7, mpu_abort("k", at(1))),
        Outcome::Applied(Effect::Aborted { parts: 3 })
    );
    let after = dump(&index);
    assert!(after.uploads.is_empty() && after.parts.is_empty());
    for seq in [2, 3, 4, 6] {
        assert!(!located(&index, at(seq)), "position {seq} is released");
    }
    assert_eq!(entry(&index, "k"), None);
}

#[test]
fn a_replaced_part_releases_the_bytes_it_replaces() {
    let index = open_upload();
    assert_eq!(
        step(&index, 7, mpu_part("k", at(1), 2, 7, vec![])),
        Outcome::Applied(Effect::PartStored)
    );
    assert!(!located(&index, at(3)) && !located(&index, at(4)));
    let part = index
        .read()
        .unwrap()
        .parts(&shard(0), at(1), 1, 1)
        .unwrap()
        .remove(0);
    assert_eq!((part.0, part.1.position, part.1.size), (2, at(7), 7));
    assert_eq!(
        index.read().unwrap().location(&shard(0), at(7)).unwrap(),
        Some(location(at(7)))
    );
}

#[test]
fn uploads_of_a_key_are_independent_and_survive_its_writes() {
    let index = open_upload();
    assert_eq!(
        step(&index, 7, mpu_create("k")),
        Outcome::Applied(Effect::UploadCreated)
    );
    assert_eq!(step(&index, 8, put("k", 1, 8)), DIRTY);
    assert_eq!(
        step(&index, 9, delete("k")),
        Outcome::Applied(Effect::Tombstoned {
            state: EntryState::Dirty
        })
    );
    assert_eq!(dump(&index).uploads.len(), 2);
    // The second upload's part 1 is not the first upload's.
    assert_eq!(
        step(&index, 10, mpu_complete("k", at(7), &[(1, at(2))], 10)),
        Outcome::Rejected(Rejection::PartChanged { number: 1 })
    );
    assert_eq!(
        step(&index, 11, mpu_part("k", at(7), 1, 4, vec![])),
        Outcome::Applied(Effect::PartStored)
    );
    assert_eq!(
        step(&index, 12, mpu_complete("k", at(7), &[(1, at(11))], 4)),
        DIRTY
    );
    let object = entry(&index, "k").unwrap().object.unwrap();
    assert_eq!(object.write_identity, Some(at(7)));
    // The first upload is still open, with its parts.
    let after = dump(&index);
    assert_eq!(after.uploads.len(), 1);
    assert_eq!(after.parts.len(), 4);
}

#[test]
fn adopting_a_remote_version_drops_the_object_parts() {
    let index = open_upload();
    assert_eq!(
        step(&index, 7, mpu_complete("k", at(1), &[(1, at(2))], 10)),
        DIRTY
    );
    assert_eq!(
        step(&index, 8, flushed("k", 7, false)),
        Outcome::Applied(Effect::Cleaned)
    );
    assert_eq!(
        step(&index, 9, adopt("k", 7, 9)),
        Outcome::Applied(Effect::Adopted)
    );
    assert!(dump(&index).parts.is_empty());
}

#[test]
fn multipart_outcomes_describe_themselves() {
    assert_eq!(
        Outcome::Rejected(Rejection::PartChanged { number: 4 }).to_string(),
        "rejected: part 4 is not the part the completion names"
    );
    assert_eq!(
        Rejection::NoSuchUpload.to_string(),
        "the upload is not open"
    );
}
