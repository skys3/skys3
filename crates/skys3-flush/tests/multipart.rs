//! Tests of the flush of completed multipart objects (§7.3, §7.4): remote
//! multipart uploads with the client's part boundaries, the 412 recovery
//! by the `MPU_CREATE` write identity, and the abort of uploads that do not
//! complete.

mod support;

use std::collections::BTreeMap;
use std::time::Duration;

use skys3_flush::{Phase, ShardFlusher};
use skys3_index::EntryState;
use skys3_remote::{ObjectStore, PutObject, UserMetadata};
use skys3_sim::SimS3;
use skys3_sim::s3::{Fault, Operation};
use support::{Multipart, Node, Patience, identity, remote, runtime, target, target_with, writes};

/// The remote object at `key`: its body and write identity.
fn remote_object(store: &SimS3, key: &str) -> Option<(String, Option<String>)> {
    store.object(key).map(|object| {
        let body = String::from_utf8(object.body.to_vec()).unwrap();
        let wid = object.info.metadata.write_identity().map(str::to_owned);
        (body, wid)
    })
}

/// Writes `body` to `key` at the remote, as another writer would.
async fn out_of_band(store: &SimS3, key: &str, body: &str) {
    let mut metadata = UserMetadata::new();
    metadata.insert("writer", "someone-else").unwrap();
    store
        .put_object(PutObject::new(key, body.to_owned()).with_metadata(metadata))
        .await
        .unwrap();
}

/// Waits until `done` holds.
async fn wait_until(what: &str, done: impl Fn() -> bool) {
    let patience = Patience::new();
    while !done() {
        assert!(!patience.is_exhausted(), "{what} never happened");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Checks that the remote holds `object` at `key` with `body`, its local
/// ETag, and the write identity of its `MPU_CREATE`, and that the index
/// records it as flushed.
async fn assert_flushed(node: &Node, store: &SimS3, key: &str, object: &Multipart, body: &str) {
    let remote = store.object(key).unwrap();
    assert_eq!(remote.info.etag, object.etag, "{key}");
    assert_eq!(remote.body, body.as_bytes(), "{key}");
    assert_eq!(
        remote.info.metadata.write_identity(),
        Some(identity(object.upload).as_str()),
        "{key}"
    );
    let entry = node.entry(key).await.unwrap();
    assert_eq!(entry.version.seq.get(), object.complete);
    assert_eq!(entry.state, EntryState::Clean);
    assert_eq!(entry.remote_etag.as_ref(), Some(&object.etag));
    assert_eq!(entry.object.unwrap().local_etag, object.etag);
}

fn assert_quiet(flusher: &ShardFlusher, store: &SimS3) {
    let status = flusher.status();
    assert!(status.conflicts.is_empty(), "{status:?}");
    assert_eq!(status.last_error, None);
    assert_eq!((status.dirty, status.dirty_bytes), (0, 0));
    assert!(store.uploads().is_empty(), "{:?}", store.uploads());
}

#[test]
fn multipart_objects_flush_with_their_part_boundaries() {
    runtime().block_on(async {
        let node = Node::open(30).await;
        let store = remote(30, false);
        node.put("over", "v1").await;
        let target = target(&store);
        let flusher = node.flusher(&target);
        node.settle(&flusher).await;

        // One over a flushed key (`If-Match`), one over none
        // (`If-None-Match: *`).
        let over = node.multipart("over", &["aaaa", "bb"]).await;
        let fresh = node.multipart("fresh", &["c", "dd", "eee"]).await;
        node.settle(&flusher).await;
        assert_flushed(&node, &store, "over", &over, "aaaabb").await;
        assert_flushed(&node, &store, "fresh", &fresh, "cddeee").await;
        let object = store.object("fresh").unwrap();
        assert_eq!(object.info.content_type.as_deref(), Some("video/mp4"));
        assert_eq!(object.info.metadata.get("owner"), Some("team-b"));
        let video = BTreeMap::from([("kind".to_owned(), "video".to_owned())]);
        assert_eq!(store.tags("fresh"), Some(video));
        // The PUT, then a create, the parts, and a complete per object:
        // nothing to recover from, nothing to abort.
        assert_eq!(store.stats().requests, 1 + 4 + 5);
        assert_quiet(&flusher, &store);

        // A tag change keeps the parts: they are uploaded again with the
        // new tags and the identity of the `TAGS` record, and the ETag
        // stays.
        let tagged = node.tag("over", &[("kind", "clip")]).await;
        node.settle(&flusher).await;
        let object = store.object("over").unwrap();
        assert_eq!(object.info.etag, over.etag);
        assert_eq!(
            object.info.metadata.write_identity(),
            Some(identity(tagged).as_str())
        );
        let clip = BTreeMap::from([("kind".to_owned(), "clip".to_owned())]);
        assert_eq!(store.tags("over"), Some(clip));

        // An overwrite and a delete flush as usual.
        let put = node.put("over", "v2").await;
        node.delete("fresh").await;
        node.settle(&flusher).await;
        assert_eq!(
            remote_object(&store, "over"),
            Some(("v2".to_owned(), Some(identity(put))))
        );
        assert_eq!(remote_object(&store, "fresh"), None);
        assert!(node.entry("fresh").await.is_none());
        assert_quiet(&flusher, &store);
        assert_eq!(target.orphaned_uploads(), 0);
    });
}

#[test]
fn a_lost_complete_answer_is_recognized_by_the_upload_identity() {
    runtime().block_on(async {
        let node = Node::open(31).await;
        let store = remote(31, false);
        let target = target(&store);
        let first = node.multipart("k", &["ab", "c"]).await;
        store.inject(Operation::CompleteMultipartUpload, Fault::LostResponse);
        let flusher = node.flusher(&target);
        node.settle(&flusher).await;
        // The abort found the upload gone, and the HEAD found the
        // identity of its `MPU_CREATE`: done, without a retry.
        assert_eq!(store.stats().lost_responses, 1);
        assert_flushed(&node, &store, "k", &first, "abc").await;
        assert!(flusher.status().last_error.is_none());
        assert_quiet(&flusher, &store);

        // If the abort fails too, the attempt is retried. The retry aborts
        // the upload left over (it is gone), uploads the parts again, and
        // completes conditioned on the first object's ETag: that fails,
        // and the HEAD finds the second object's `MPU_CREATE` identity, so
        // its new upload is aborted and the flush is done. The flusher
        // follows applied writes, so it may start before the write below
        // returns: the faults and the count come first.
        store.inject(Operation::CompleteMultipartUpload, Fault::LostResponse);
        store.inject(Operation::AbortMultipartUpload, Fault::InternalError);
        let before = store.stats().requests;
        let second = node.multipart("k", &["xy", "z"]).await;
        node.settle(&flusher).await;
        assert_flushed(&node, &store, "k", &second, "xyz").await;
        // Create, 2 parts, complete, abort; abort; create, 2 parts,
        // complete, HEAD, abort.
        assert_eq!(store.stats().requests - before, 5 + 1 + 6);
        assert_quiet(&flusher, &store);
        assert_eq!(target.orphaned_uploads(), 0);
    });
}

#[test]
fn uploads_that_fail_are_aborted() {
    runtime().block_on(async {
        let node = Node::open(32).await;
        let store = remote(32, false);
        let target = target(&store);
        let first = node.multipart("a", &["ab", "c"]).await;
        store.inject(Operation::UploadPart, Fault::InternalError);
        let flusher = node.flusher(&target);
        wait_until("a backoff", || {
            matches!(flusher.phase("a"), Some(Phase::Backoff(_)))
        })
        .await;
        assert!(store.uploads().is_empty());
        // The failure is the key's error until the key is flushed.
        let error = flusher.status().last_error.unwrap();
        assert!(error.starts_with("a: "), "{error}");
        node.settle(&flusher).await;
        assert_flushed(&node, &store, "a", &first, "abc").await;
        assert_eq!(flusher.status().last_error, None);

        // An abort that fails leaves the upload to the target, and the
        // next multipart flush aborts it first. The faults are queued
        // before the write, which the running flusher may start on before
        // it returns.
        store.inject(Operation::UploadPart, Fault::InternalError);
        store.inject(Operation::AbortMultipartUpload, Fault::SlowDown);
        let second = node.multipart("b", &["de", "f"]).await;
        wait_until("a backoff", || {
            matches!(flusher.phase("b"), Some(Phase::Backoff(_)))
        })
        .await;
        assert_eq!(target.orphaned_uploads(), 1);
        assert_eq!(store.uploads().len(), 1);
        node.settle(&flusher).await;
        assert_flushed(&node, &store, "b", &second, "def").await;
        assert_eq!(target.orphaned_uploads(), 0);
        assert_quiet(&flusher, &store);
    });
}

#[test]
fn an_upload_left_by_a_stopped_flusher_is_aborted_later() {
    runtime().block_on(async {
        let node = Node::open(33).await;
        let store = remote(33, false);
        let target = target(&store);
        let object = node.multipart("k", &["ab", "c"]).await;
        store.inject(Operation::UploadPart, Fault::Delay(Duration::from_secs(10)));
        let flusher = node.flusher(&target);
        wait_until("an open upload", || store.uploads().len() == 1).await;
        // Its flush task is cancelled after the flusher's, and hands the
        // upload to the target.
        flusher.stop().await;
        wait_until("an orphaned upload", || target.orphaned_uploads() == 1).await;

        let flusher = node.flusher(&target);
        node.settle(&flusher).await;
        assert_flushed(&node, &store, "k", &object, "abc").await;
        assert_eq!(target.orphaned_uploads(), 0);
        assert_quiet(&flusher, &store);
    });
}

#[test]
fn an_earlier_flush_whose_record_was_lost_is_superseded() {
    runtime().block_on(async {
        let node = Node::open(34).await;
        let store = remote(34, false);
        // The remote holds a write of this shard the index does not know
        // about, as after a crash that lost a `FLUSHED`.
        let earlier = node.put("other", "x").await;
        let mut metadata = UserMetadata::new();
        metadata.insert("skys3-wid", identity(earlier)).unwrap();
        store
            .put_object(PutObject::new("k", "old").with_metadata(metadata))
            .await
            .unwrap();
        let object = node.multipart("k", &["ab", "c"]).await;
        let flusher = node.flusher(&target(&store));
        node.settle(&flusher).await;
        assert_flushed(&node, &store, "k", &object, "abc").await;
        // `If-None-Match: *` failed, the HEAD found the earlier write, and
        // the same upload was completed again over it: after the planted
        // object and the PUT of `other`, one create, two parts, two
        // completes, and a HEAD.
        assert_eq!(store.stats().requests, 2 + 6);
        assert_quiet(&flusher, &store);
    });
}

#[test]
fn out_of_band_writes_hold_multipart_objects_in_conflict() {
    runtime().block_on(async {
        let node = Node::open(35).await;
        let store = remote(35, false);
        out_of_band(&store, "k", "theirs").await;
        let object = node.multipart("k", &["ab", "c"]).await;
        let flusher = node.flusher(&target(&store));
        node.settle(&flusher).await;
        let Some(Phase::Conflict(conflict)) = flusher.phase("k") else {
            panic!("k is not in conflict: {:?}", flusher.phase("k"));
        };
        assert_eq!(conflict.seq.get(), object.complete);
        assert_eq!(
            remote_object(&store, "k"),
            Some(("theirs".to_owned(), None))
        );
        assert_eq!(node.entry("k").await.unwrap().state, EntryState::Dirty);
        // The upload that could not complete was aborted.
        assert!(store.uploads().is_empty());
    });
}

#[test]
fn unprotected_completions_are_sent_unconditionally() {
    runtime().block_on(async {
        let node = Node::open(36).await;
        let store = remote(36, false);
        // Like R2: conditional PUTs, unconditional completions and deletes.
        let target = target_with(&store, writes(true, false, false));
        node.put("k", "ours").await;
        let flusher = node.flusher(&target);
        node.settle(&flusher).await;
        out_of_band(&store, "k", "theirs").await;
        let object = node.multipart("k", &["ab", "c"]).await;
        node.settle(&flusher).await;
        // The completion carried no `If-Match`, so it replaced the other
        // writer's object: what the bucket status warns about.
        assert_flushed(&node, &store, "k", &object, "abc").await;
        assert_quiet(&flusher, &store);
    });
}
