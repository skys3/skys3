//! Tests of upload takeover (§7.3): a flusher that resumes a streamed
//! upload from the index, after a restart or as a new primary's, lists the
//! remote upload before it sends a part or completes. A part counts as sent
//! only if the remote holds it with the recorded ETag or with the local
//! part's MD5; every other part is sent again, so the completed object is
//! never partial or wrong. A part the remote holds without its record (the
//! old primary's send whose `PART_FLUSHED` was lost) is not sent again.
//!
//! The old primary's sends are played by the tests, straight to the
//! remote: a send that landed without its record, a late send that
//! replaced a recorded part, and a Complete whose `FLUSHED` was lost.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use skys3_flush::{FlushSettings, ShardFlusher, Target};
use skys3_remote::{
    AbortMultipartUpload, CompleteMultipartUpload, CompletedPart, CopyObject,
    CreateMultipartUpload, DeleteObject, DeleteOutput, GetObject, GetOutput, HeadObject,
    ListObjectsV2, ListObjectsV2Output, ListParts, ListPartsOutput, ObjectInfo, ObjectStore,
    PutObject, S3Result, UploadId, UploadPart, WriteOutput, WritePrecondition,
};
use skys3_sim::SimS3;
use skys3_sim::s3::{Fault, Operation};
use skys3_types::{ETag, EpochSeq};
use support::{
    Multipart, Node, Patience, body_target, cluster, identity, md5_etag, remote, runtime, settings,
    streamed_etag, streaming_target, writes,
};

/// The part size of the streamed PUTs of these tests.
const PART: u64 = 8;

/// Waits until `done` holds.
async fn wait_until(what: &str, done: impl AsyncFn() -> bool) {
    let patience = Patience::new();
    while !done().await {
        assert!(!patience.is_exhausted(), "{what} never happened");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// The remote upload the index records for the upload at `upload`, and how
/// many of its parts.
async fn recorded(node: &Node, upload: EpochSeq) -> Option<(UploadId, usize)> {
    let (remote, parts) = node.shard.remote_upload(upload).await.unwrap()?;
    Some((UploadId(remote.id), parts.len()))
}

/// Waits until the index records `parts` parts of the remote upload of the
/// upload at `upload`, and returns that upload's ID.
async fn wait_for_records(node: &Node, upload: EpochSeq, parts: usize) -> UploadId {
    wait_until("the records", async || {
        recorded(node, upload)
            .await
            .is_some_and(|(_, n)| n == parts)
    })
    .await;
    recorded(node, upload).await.unwrap().0
}

/// Sends `body` as part `number` of the remote upload `id` of `key`, as
/// the old primary did.
async fn send(store: &SimS3, key: &str, id: &UploadId, number: u32, body: &str) {
    let part = UploadPart::new(
        key,
        id.clone(),
        number,
        Bytes::copy_from_slice(body.as_bytes()),
    );
    store.upload_part(part).await.unwrap();
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

/// Checks that the remote holds `object` at `key` with `body`, its local
/// ETag, and its `MPU_CREATE` identity, and that the index has it clean
/// with that ETag.
async fn assert_flushed(node: &Node, store: &SimS3, key: &str, object: &Multipart, body: &str) {
    let remote = store.object(key).unwrap();
    assert_eq!(remote.body, body.as_bytes(), "{key}");
    assert_eq!(remote.info.etag, object.etag, "{key}");
    assert_eq!(
        remote.info.metadata.write_identity(),
        Some(identity(object.upload).as_str())
    );
    let entry = node.entry(key).await.unwrap();
    assert_eq!(entry.version.seq.get(), object.complete);
    assert_eq!(entry.remote_etag.as_ref(), Some(&object.etag));
}

#[test]
fn a_part_the_remote_holds_without_its_record_is_not_sent_again() {
    runtime().block_on(async {
        let node = Node::open_inline(120).await;
        let store = remote(120, false);
        let target = streaming_target(&store, writes(true, true, true));
        let flusher = node.flusher(&target);
        let upload = node.create("k").await;
        let first = node.part("k", upload, 1, "first; ").await;
        let id = wait_for_records(&node, upload, 1).await;

        // The old primary sent part 2 and crashed before its `PART_FLUSHED`
        // committed.
        flusher.stop().await;
        let second = node.part("k", upload, 2, "second").await;
        send(&store, "k", &id, 2, "second").await;
        let node = node.crash().await;

        // The new primary lists the remote upload (once more after a
        // failed `ListParts`), takes part 2 as sent and records it, and
        // only completes.
        store.inject(Operation::ListParts, Fault::InternalError);
        let before = store.stats().requests;
        let flusher = node.flusher(&target);
        wait_for_records(&node, upload, 2).await;
        let parts = [(1, first, "first; "), (2, second, "second")];
        let object = node.finish("k", upload, &parts).await;
        node.settle(&flusher).await;
        assert_flushed(&node, &store, "k", &object, "first; second").await;
        wait_for_no_streams(&node, &flusher, &store).await;
        // Two `ListParts` and the Complete.
        assert_eq!(store.stats().requests - before, 3);
    });
}

#[test]
fn a_recorded_part_the_remote_no_longer_holds_is_sent_again() {
    runtime().block_on(async {
        let node = Node::open_inline(121).await;
        let store = remote(121, false);
        let target = streaming_target(&store, writes(true, true, true));
        let flusher = node.flusher(&target);
        let upload = node.create("k").await;
        let first = node.part("k", upload, 1, "first; ").await;
        let id = wait_for_records(&node, upload, 1).await;
        let second = node.part("k", upload, 2, "second").await;
        wait_for_records(&node, upload, 2).await;

        // A send of an earlier body of part 2, which the old primary's
        // flusher took for failed, lands after the later one: the record
        // of part 2 names bytes the remote no longer holds.
        flusher.stop().await;
        send(&store, "k", &id, 2, "earlier").await;
        let parts = [(1, first, "first; "), (2, second, "second")];
        let object = node.finish("k", upload, &parts).await;
        let node = node.crash().await;

        // The new primary's completion lists the remote upload and sends
        // part 2 again before it completes.
        let before = store.stats().requests;
        let flusher = node.flusher(&target);
        node.settle(&flusher).await;
        assert_flushed(&node, &store, "k", &object, "first; second").await;
        wait_for_no_streams(&node, &flusher, &store).await;
        assert_eq!(store.stats().requests - before, 3);
    });
}

#[test]
fn a_completion_refused_for_a_part_lists_the_remote_upload_again() {
    runtime().block_on(async {
        let node = Node::open_inline(122).await;
        let store = remote(122, false);
        let target = streaming_target(&store, writes(true, true, true));
        let flusher = node.flusher(&target);
        let upload = node.create("k").await;
        let first = node.part("k", upload, 1, "first; ").await;
        let second = node.part("k", upload, 2, "second").await;
        let id = wait_for_records(&node, upload, 2).await;

        // The deposed primary's late send lands after this flusher sent
        // part 1: the remote refuses the Complete that lists it, and the
        // flusher lists the remote upload and sends part 1 again.
        send(&store, "k", &id, 1, "late").await;
        let before = store.stats().requests;
        let parts = [(1, first, "first; "), (2, second, "second")];
        let object = node.finish("k", upload, &parts).await;
        node.settle(&flusher).await;
        assert_flushed(&node, &store, "k", &object, "first; second").await;
        wait_for_no_streams(&node, &flusher, &store).await;
        // The refused Complete, `ListParts`, part 1, and the Complete.
        assert_eq!(store.stats().requests - before, 4);
    });
}

#[test]
fn a_complete_the_old_primary_sent_is_recognized_by_its_identity() {
    runtime().block_on(async {
        let node = Node::open_inline(123).await;
        let store = remote(123, true);
        let target = streaming_target(&store, writes(true, true, true));
        let flusher = node.flusher(&target);
        let upload = node.create("k").await;
        let first = node.part("k", upload, 1, "only part").await;
        let id = wait_for_records(&node, upload, 1).await;

        // The old primary completed the remote upload and crashed before
        // its `FLUSHED` committed.
        flusher.stop().await;
        let object = node.finish("k", upload, &[(1, first, "only part")]).await;
        let complete = CompleteMultipartUpload {
            key: "k".to_owned(),
            upload_id: id,
            parts: vec![CompletedPart {
                part_number: 1,
                etag: md5_etag(b"only part"),
            }],
            precondition: WritePrecondition::None,
            apply_by_ms: None,
        };
        store.complete_multipart_upload(complete).await.unwrap();
        let node = node.crash().await;

        // The new primary finds the remote upload gone, and the object
        // with the upload's identity: it sends nothing more.
        let before = store.stats().requests;
        let flusher = node.flusher(&target);
        node.settle(&flusher).await;
        assert_flushed(&node, &store, "k", &object, "only part").await;
        wait_for_no_streams(&node, &flusher, &store).await;
        assert_eq!(store.versions().len(), 1);
        // `ListParts`, the HEAD, and the abort that records the end.
        assert_eq!(store.stats().requests - before, 3);
    });
}

#[test]
fn an_open_upload_whose_remote_upload_is_gone_is_sent_after_commit() {
    runtime().block_on(async {
        let node = Node::open_inline(124).await;
        let store = remote(124, false);
        let target = streaming_target(&store, writes(true, true, true));
        let flusher = node.flusher(&target);
        let upload = node.create("k").await;
        let first = node.part("k", upload, 1, "first; ").await;
        let id = wait_for_records(&node, upload, 1).await;

        // The remote bucket's lifecycle rule aborted the upload while no
        // primary served.
        flusher.stop().await;
        let abort = AbortMultipartUpload {
            key: "k".to_owned(),
            upload_id: id,
        };
        store.abort_multipart_upload(abort).await.unwrap();
        let flusher = node.flusher(&target);
        wait_until("the stream's end", async || {
            node.shard.remote_uploads().await.unwrap().is_empty()
        })
        .await;

        let second = node.part("k", upload, 2, "second").await;
        let parts = [(1, first, "first; "), (2, second, "second")];
        let object = node.finish("k", upload, &parts).await;
        node.settle(&flusher).await;
        assert_flushed(&node, &store, "k", &object, "first; second").await;
        wait_for_no_streams(&node, &flusher, &store).await;
    });
}

#[test]
fn a_resumed_body_sends_again_only_the_parts_the_remote_lacks() {
    runtime().block_on(async {
        let node = Node::open_inline(125).await;
        let store = remote(125, false);
        let timeout = Duration::from_secs(5);
        let target = body_target(&store, writes(true, true, true), PART, timeout);
        let flusher = node.flusher(&target);
        let body = "0123456789abcdefghijklmn";
        let begun = node.begin("big").await;
        let extents = node.extents("big", body, 4).await;
        node.announce("big", begun, &extents[..4]);
        wait_until("two parts", async || {
            let id = recorded(&node, support::at(begun)).await;
            let Some((id, _)) = id else { return false };
            let listed = store.list_parts(ListParts::new("big", id)).await;
            listed.is_ok_and(|listed| listed.parts.len() == 2)
        })
        .await;
        let (id, _) = recorded(&node, support::at(begun)).await.unwrap();

        // A late send of the old primary replaced part 2 with other bytes;
        // the `PUT` commits while no primary serves.
        flusher.stop().await;
        send(&store, "big", &id, 2, "XXXXXXXX").await;
        node.complete_extents("big", body, begun, &extents).await;
        let node = node.crash().await;

        // The completion lists the remote upload, keeps part 1, whose ETag
        // is the MD5 of the body's first bytes, and sends parts 2 and 3.
        let before = store.stats().requests;
        let flusher = node.flusher(&target);
        node.settle(&flusher).await;
        let remote = store.object("big").unwrap();
        assert_eq!(remote.body, body.as_bytes());
        assert_eq!(remote.info.etag, streamed_etag(body, PART as usize));
        assert_eq!(
            remote.info.metadata.write_identity(),
            Some(identity(begun).as_str())
        );
        wait_for_no_streams(&node, &flusher, &store).await;
        assert_eq!(store.stats().requests - before, 4);
    });
}

/// A store that answers `ListParts` a part at a time, or, once `stuck`,
/// with pages that do not advance.
#[derive(Debug)]
struct Paged {
    inner: SimS3,
    stuck: AtomicBool,
}

impl ObjectStore for Paged {
    async fn put_object(&self, request: PutObject) -> S3Result<WriteOutput> {
        self.inner.put_object(request).await
    }

    async fn get_object(&self, request: GetObject) -> S3Result<GetOutput> {
        self.inner.get_object(request).await
    }

    async fn head_object(&self, request: HeadObject) -> S3Result<ObjectInfo> {
        self.inner.head_object(request).await
    }

    async fn delete_object(&self, request: DeleteObject) -> S3Result<DeleteOutput> {
        self.inner.delete_object(request).await
    }

    async fn list_objects_v2(&self, request: ListObjectsV2) -> S3Result<ListObjectsV2Output> {
        self.inner.list_objects_v2(request).await
    }

    async fn copy_object(&self, request: CopyObject) -> S3Result<WriteOutput> {
        self.inner.copy_object(request).await
    }

    async fn create_multipart_upload(&self, request: CreateMultipartUpload) -> S3Result<UploadId> {
        self.inner.create_multipart_upload(request).await
    }

    async fn upload_part(&self, request: UploadPart) -> S3Result<ETag> {
        self.inner.upload_part(request).await
    }

    async fn complete_multipart_upload(
        &self,
        request: CompleteMultipartUpload,
    ) -> S3Result<WriteOutput> {
        self.inner.complete_multipart_upload(request).await
    }

    async fn abort_multipart_upload(&self, request: AbortMultipartUpload) -> S3Result<()> {
        self.inner.abort_multipart_upload(request).await
    }

    async fn list_parts(&self, request: ListParts) -> S3Result<ListPartsOutput> {
        let stuck = self.stuck.load(Ordering::SeqCst);
        let marker = request.part_number_marker;
        let mut page = self
            .inner
            .list_parts(ListParts {
                max_parts: 1,
                ..request
            })
            .await?;
        if stuck && page.is_truncated {
            page.next_part_number_marker = marker;
        }
        Ok(page)
    }
}

#[test]
fn every_page_of_the_listing_counts_and_pages_must_advance() {
    runtime().block_on(async {
        let node = Node::open_inline(126).await;
        let store = Arc::new(Paged {
            inner: remote(126, false),
            stuck: AtomicBool::new(false),
        });
        let settings = FlushSettings {
            streaming: true,
            ..settings()
        };
        let target = Arc::new(Target::new(
            Arc::clone(&store),
            "",
            writes(true, true, true),
            cluster(),
            settings,
        ));
        let flusher = ShardFlusher::spawn(node.shard.clone(), Arc::clone(&target));
        let upload = node.create("k").await;
        let bodies = ["one; ", "two; ", "three"];
        let mut parts = Vec::new();
        for (number, body) in (1..).zip(bodies) {
            parts.push((number, node.part("k", upload, number, body).await, body));
        }
        wait_for_records(&node, upload, 3).await;
        flusher.stop().await;
        let object = node.finish("k", upload, &parts).await;

        // Pages that do not advance fail the completion, which is retried.
        store.stuck.store(true, Ordering::SeqCst);
        let flusher = ShardFlusher::spawn(node.shard.clone(), Arc::clone(&target));
        wait_until("the failed listing", async || {
            flusher
                .status()
                .last_error
                .is_some_and(|error| error.contains("do not advance"))
        })
        .await;

        // Three pages of one part each confirm every recorded part: the
        // completion only completes.
        store.stuck.store(false, Ordering::SeqCst);
        let before = store.inner.stats().requests;
        node.settle(&flusher).await;
        assert_flushed(&node, &store.inner, "k", &object, "one; two; three").await;
        assert_eq!(store.inner.stats().requests - before, 4);
    });
}
