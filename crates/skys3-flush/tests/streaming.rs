//! Tests of streaming multipart flush (§7.3): the remote upload opens with
//! the local one, parts stream while the client uploads, and the remote
//! upload completes only after the local completion commits, with exactly
//! the parts it kept, so the remote ETag is the local one. Aborted and
//! abandoned remote uploads are aborted, and a restarted flusher resumes
//! from the `PART_FLUSHED` records.

mod support;

use std::time::Duration;

use skys3_flush::{Counters, ShardFlusher, ShardStatus};
use skys3_index::EntryState;
use skys3_remote::{AbortMultipartUpload, ListParts, ObjectStore};
use skys3_sim::SimS3;
use skys3_sim::s3::{Fault, Operation};
use skys3_types::EpochSeq;
use support::{Multipart, Node, Patience, identity, remote, runtime, streaming_target, writes};

/// Waits until `done` holds.
async fn wait_until(what: &str, done: impl AsyncFn() -> bool) {
    let patience = Patience::new();
    while !done().await {
        assert!(!patience.is_exhausted(), "{what} never happened");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// The parts the store's only open upload holds: number and size.
async fn remote_parts(store: &SimS3, key: &str) -> Vec<(u32, u64)> {
    let uploads = store.uploads();
    let [(id, _)] = uploads.as_slice() else {
        return Vec::new();
    };
    let listed = store.list_parts(ListParts::new(key, id.clone())).await;
    listed
        .map(|output| {
            output
                .parts
                .iter()
                .map(|p| (p.part_number, p.size))
                .collect()
        })
        .unwrap_or_default()
}

/// Whether the index records a remote upload for the upload at `upload`.
async fn recorded(node: &Node, upload: EpochSeq) -> bool {
    node.shard.remote_upload(upload).await.unwrap().is_some()
}

/// Checks that the remote holds `object` at `key` with `body`, its local
/// ETag, and its `MPU_CREATE` identity, and that the index has it clean.
async fn assert_flushed(node: &Node, store: &SimS3, key: &str, object: &Multipart, body: &str) {
    let remote = store.object(key).unwrap();
    assert_eq!(remote.info.etag, object.etag, "{key}");
    assert_eq!(remote.body, body.as_bytes(), "{key}");
    assert_eq!(
        remote.info.metadata.write_identity(),
        Some(identity(object.upload).as_str())
    );
    let entry = node.entry(key).await.unwrap();
    assert_eq!(entry.version.seq.get(), object.complete);
    assert_eq!(entry.state, EntryState::Clean);
    assert_eq!(entry.remote_etag.as_ref(), Some(&object.etag));
}

/// The streaming-overlap histogram of `counters`, as the metrics endpoint
/// shows it.
fn overlap(counters: &Counters) -> String {
    let mut registry = prometheus_client::registry::Registry::default();
    registry.register("overlap", "", counters.streaming_overlap.clone());
    let mut text = String::new();
    prometheus_client::encoding::text::encode(&mut text, &registry).unwrap();
    text
}

/// Waits until no stream is left: every remote upload completed or
/// aborted, and the end of each recorded.
async fn wait_for_no_streams(node: &Node, flusher: &ShardFlusher, store: &SimS3) {
    wait_until("the streams' end", async || {
        flusher.status().streams == 0
            && store.uploads().is_empty()
            && node.shard.remote_uploads().await.unwrap().is_empty()
    })
    .await;
}

#[test]
fn parts_stream_while_the_upload_is_open_and_complete_after_its_commit() {
    runtime().block_on(async {
        let node = Node::open(60).await;
        let store = remote(60, false);
        let target = streaming_target(&store, writes(true, true, true));
        let flusher = node.flusher(&target);

        // The remote upload opens with the local one, and its ID is in the
        // log before any part is sent.
        let upload = node.create("big").await;
        wait_until("the remote upload", async || {
            store.uploads().len() == 1 && recorded(&node, upload).await
        })
        .await;
        let first = node.part("big", upload, 1, "first part; ").await;
        let second = node.part("big", upload, 2, "second").await;
        wait_until("the streamed parts", async || {
            remote_parts(&store, "big").await == [(1, 12), (2, 6)]
        })
        .await;
        // Nothing is visible at the remote before the local completion.
        assert!(store.object("big").is_none());
        assert_eq!(flusher.status().streams, 1);
        let before = store.stats().requests;

        let parts = [(1, first, "first part; "), (2, second, "second")];
        let object = node.finish("big", upload, &parts).await;
        node.settle(&flusher).await;
        assert_flushed(&node, &store, "big", &object, "first part; second").await;
        let video = [("kind".to_owned(), "video".to_owned())].into();
        assert_eq!(store.tags("big"), Some(video));
        assert_eq!(
            store.object("big").unwrap().info.content_type.as_deref(),
            Some("video/mp4")
        );
        wait_for_no_streams(&node, &flusher, &store).await;
        // The completion only completes: every part was at the remote.
        assert_eq!(store.stats().requests - before, 1);
        // Every byte was at the remote when the client completed.
        let overlap = overlap(target.counters());
        assert!(overlap.contains("overlap_sum 1.0"), "{overlap}");
        assert!(overlap.contains("overlap_count 1\n"), "{overlap}");
        let status = flusher.status();
        assert_eq!(status.last_error, None, "{status:?}");
    });
}

#[test]
fn a_completion_lists_exactly_the_parts_it_kept() {
    runtime().block_on(async {
        let node = Node::open(61).await;
        let store = remote(61, false);
        let target = streaming_target(&store, writes(true, true, true));
        let flusher = node.flusher(&target);
        let upload = node.create("k").await;
        let first = node.part("k", upload, 1, "one; ").await;
        node.part("k", upload, 2, "two, first try; ").await;
        node.part("k", upload, 3, "three, left out").await;
        wait_until("the streamed parts", async || {
            remote_parts(&store, "k").await.len() == 3
        })
        .await;
        // The client uploads part 2 again and leaves part 3 out: the
        // remote part 2 is replaced, and part 3 is never listed.
        let second = node.part("k", upload, 2, "two").await;
        let parts = [(1, first, "one; "), (2, second, "two")];
        let object = node.finish("k", upload, &parts).await;
        node.settle(&flusher).await;
        assert_flushed(&node, &store, "k", &object, "one; two").await;
        wait_for_no_streams(&node, &flusher, &store).await;
    });
}

#[test]
fn a_part_uploaded_again_while_it_is_sent_follows_it() {
    runtime().block_on(async {
        let node = Node::open(62).await;
        let store = remote(62, false);
        let target = streaming_target(&store, writes(true, true, true));
        let flusher = node.flusher(&target);
        let upload = node.create("k").await;
        wait_until("the remote upload", async || recorded(&node, upload).await).await;
        // The first send of part 1 is slow; the second waits for it, so the
        // remote holds the later bytes.
        store.inject(Operation::UploadPart, Fault::Delay(Duration::from_secs(5)));
        node.part("k", upload, 1, "slow and old").await;
        let again = node.part("k", upload, 1, "new").await;
        wait_until("the second send", async || {
            remote_parts(&store, "k").await == [(1, 3)]
        })
        .await;
        let object = node.finish("k", upload, &[(1, again, "new")]).await;
        node.settle(&flusher).await;
        assert_flushed(&node, &store, "k", &object, "new").await;
        wait_for_no_streams(&node, &flusher, &store).await;
    });
}

#[test]
fn aborted_and_abandoned_uploads_are_aborted_at_the_remote() {
    runtime().block_on(async {
        let node = Node::open(63).await;
        let store = remote(63, false);
        let target = streaming_target(&store, writes(true, true, true));
        let flusher = node.flusher(&target);

        // A local abort aborts the remote upload, and records its end.
        let upload = node.create("gone").await;
        node.part("gone", upload, 1, "bytes").await;
        wait_until("the streamed part", async || {
            remote_parts(&store, "gone").await.len() == 1
        })
        .await;
        node.abort("gone", upload).await;
        wait_for_no_streams(&node, &flusher, &store).await;
        assert!(store.object("gone").is_none());

        // A completed upload whose key is overwritten before its flush is
        // never completed: the newer version flushes, and the remote
        // upload is aborted. Its first completion attempt fails, so the
        // overwrite comes first.
        store.inject(Operation::CompleteMultipartUpload, Fault::InternalError);
        let upload = node.create("over").await;
        let part = node.part("over", upload, 1, "parts").await;
        node.finish("over", upload, &[(1, part, "parts")]).await;
        let put = node.put("over", "a single put").await;
        node.settle(&flusher).await;
        let object = store.object("over").unwrap();
        assert_eq!(object.body, "a single put".as_bytes());
        assert_eq!(
            object.info.metadata.write_identity(),
            Some(identity(put).as_str())
        );
        wait_for_no_streams(&node, &flusher, &store).await;
    });
}

#[test]
fn a_retagged_object_is_sent_after_commit() {
    runtime().block_on(async {
        let node = Node::open(64).await;
        let store = remote(64, false);
        let target = streaming_target(&store, writes(true, true, true));
        let flusher = node.flusher(&target);
        store.inject(Operation::CompleteMultipartUpload, Fault::InternalError);
        let upload = node.create("k").await;
        let part = node.part("k", upload, 1, "tagged").await;
        let object = node.finish("k", upload, &[(1, part, "tagged")]).await;
        // The stream carries the `MPU_CREATE` identity and tags; the `TAGS`
        // version is sent as a new upload, and the stream is aborted.
        let tagged = node.tag("k", &[("kind", "clip")]).await;
        node.settle(&flusher).await;
        let remote = store.object("k").unwrap();
        assert_eq!(remote.info.etag, object.etag);
        assert_eq!(
            remote.info.metadata.write_identity(),
            Some(identity(tagged).as_str())
        );
        let clip = [("kind".to_owned(), "clip".to_owned())].into();
        assert_eq!(store.tags("k"), Some(clip));
        wait_for_no_streams(&node, &flusher, &store).await;
    });
}

#[test]
fn a_restarted_flusher_resumes_the_streams_the_log_records() {
    runtime().block_on(async {
        let node = Node::open(65).await;
        let store = remote(65, false);
        let target = streaming_target(&store, writes(true, true, true));
        let flusher = node.flusher(&target);
        let upload = node.create("k").await;
        let first = node.part("k", upload, 1, "first; ").await;
        wait_until("the streamed part", async || {
            remote_parts(&store, "k").await.len() == 1
        })
        .await;
        flusher.stop().await;
        let before = store.stats().requests;

        // The new flusher reads the remote upload and its part from the
        // index: it opens nothing, and sends only the part it lacks.
        let second = node.part("k", upload, 2, "second").await;
        let flusher = node.flusher(&target);
        let parts = [(1, first, "first; "), (2, second, "second")];
        let object = node.finish("k", upload, &parts).await;
        node.settle(&flusher).await;
        assert_flushed(&node, &store, "k", &object, "first; second").await;
        wait_for_no_streams(&node, &flusher, &store).await;
        assert_eq!(store.stats().requests - before, 2);

        // An upload opened while no flusher ran gets its stream when one
        // starts.
        flusher.stop().await;
        let upload = node.create("late").await;
        let part = node.part("late", upload, 1, "late part").await;
        let flusher = node.flusher(&target);
        wait_until("the streamed part", async || {
            remote_parts(&store, "late").await.len() == 1
        })
        .await;
        let object = node.finish("late", upload, &[(1, part, "late part")]).await;
        node.settle(&flusher).await;
        assert_flushed(&node, &store, "late", &object, "late part").await;
        wait_for_no_streams(&node, &flusher, &store).await;
    });
}

#[test]
fn a_remote_upload_that_vanished_is_replaced_after_commit() {
    runtime().block_on(async {
        let node = Node::open(66).await;
        let store = remote(66, false);
        let target = streaming_target(&store, writes(true, true, true));
        let flusher = node.flusher(&target);
        let upload = node.create("k").await;
        let part = node.part("k", upload, 1, "bytes").await;
        wait_until("the streamed part", async || {
            remote_parts(&store, "k").await.len() == 1
        })
        .await;
        // The bucket's lifecycle rule aborts the remote upload.
        let (id, key) = store.uploads().remove(0);
        let abort = AbortMultipartUpload { key, upload_id: id };
        store.abort_multipart_upload(abort).await.unwrap();
        let object = node.finish("k", upload, &[(1, part, "bytes")]).await;
        node.settle(&flusher).await;
        assert_flushed(&node, &store, "k", &object, "bytes").await;
        wait_for_no_streams(&node, &flusher, &store).await;
    });
}

#[test]
fn a_lost_complete_answer_is_recognized_by_the_upload_identity() {
    runtime().block_on(async {
        let node = Node::open(67).await;
        let store = remote(67, false);
        let target = streaming_target(&store, writes(true, true, true));
        let flusher = node.flusher(&target);
        let upload = node.create("k").await;
        let part = node.part("k", upload, 1, "bytes").await;
        wait_until("the streamed part", async || {
            remote_parts(&store, "k").await.len() == 1
        })
        .await;
        store.inject(Operation::CompleteMultipartUpload, Fault::LostResponse);
        let object = node.finish("k", upload, &[(1, part, "bytes")]).await;
        node.settle(&flusher).await;
        assert_flushed(&node, &store, "k", &object, "bytes").await;
        wait_for_no_streams(&node, &flusher, &store).await;
        let status: ShardStatus = flusher.status();
        assert!(status.conflicts.is_empty(), "{status:?}");
    });
}
