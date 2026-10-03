//! Tests of streaming flush for large single PUTs (§7.3, §7.4): the body
//! streams to a remote multipart upload in parts of `flush_part_bytes`
//! while it arrives, the remote upload completes only after the `PUT`
//! commits, and the entry records the remote multipart ETag as its
//! `remote_etag` beside the MD5 `local_etag`, which later flushes are
//! conditioned on. A body whose `PUT` never commits has its remote upload
//! aborted, also after a crash.

mod support;

use std::time::Duration;

use skys3_flush::ShardFlusher;
use skys3_index::EntryState;
use skys3_remote::{ListParts, ObjectStore};
use skys3_sim::SimS3;
use support::{
    Node, Patience, at, body_target, identity, md5_etag, multipart_etag, remote, runtime,
    streamed_etag, target_with, writes,
};

/// The part size of these tests.
const PART: u64 = 8;

/// How long a body may go unannounced before its `PUT` is given up on.
const TIMEOUT: Duration = Duration::from_secs(5);

/// Waits until `done` holds.
async fn wait_until(what: &str, done: impl AsyncFn() -> bool) {
    let patience = Patience::new();
    while !done().await {
        assert!(!patience.is_exhausted(), "{what} never happened");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// The sizes of the parts the store's only open upload holds.
async fn remote_parts(store: &SimS3, key: &str) -> Vec<u64> {
    let uploads = store.uploads();
    let [(id, _)] = uploads.as_slice() else {
        return Vec::new();
    };
    let listed = store.list_parts(ListParts::new(key, id.clone())).await;
    listed
        .map(|output| output.parts.iter().map(|part| part.size).collect())
        .unwrap_or_default()
}

/// Whether the index records a remote upload for the body begun at
/// `begun`.
async fn recorded(node: &Node, begun: u64) -> bool {
    node.shard.remote_upload(at(begun)).await.unwrap().is_some()
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

/// Checks that the remote holds `body` at `key` as the streamed PUT begun
/// at `begun` sends it, in parts of [`PART`], and that the index has it
/// clean with both ETags.
async fn assert_streamed(node: &Node, store: &SimS3, key: &str, body: &str, begun: u64) {
    let remote_etag = streamed_etag(body, PART as usize);
    let remote = store.object(key).unwrap();
    assert_eq!(remote.body, body.as_bytes(), "{key}");
    assert_eq!(remote.info.etag, remote_etag, "{key}");
    assert_eq!(
        remote.info.metadata.write_identity(),
        Some(identity(begun).as_str())
    );
    let entry = node.entry(key).await.unwrap();
    assert_eq!(entry.state, EntryState::Clean);
    assert_eq!(entry.remote_etag, Some(remote_etag));
    let object = entry.object.unwrap();
    assert_eq!(object.local_etag, md5_etag(body.as_bytes()));
    assert_eq!(object.write_identity, Some(at(begun)));
}

#[test]
fn a_large_put_streams_while_it_arrives_and_completes_after_its_commit() {
    runtime().block_on(async {
        let node = Node::open_inline(80).await;
        let store = remote(80, false);
        let target = body_target(&store, writes(true, true, true), PART, TIMEOUT);
        let flusher = node.flusher(&target);

        let body = "0123456789abcdefghij";
        let begun = node.begin("big").await;
        let extents = node.extents("big", body, 6).await;
        // The first two extents hold the first part: it is sent, and the
        // remote upload's ID is in the log.
        node.announce("big", begun, &extents[..2]);
        wait_until("the first part", async || {
            remote_parts(&store, "big").await == [PART] && recorded(&node, begun).await
        })
        .await;
        node.announce("big", begun, &extents[2..]);
        wait_until("the second part", async || {
            remote_parts(&store, "big").await == [PART, PART]
        })
        .await;
        // Nothing is visible at the remote before the `PUT` commits.
        assert!(store.object("big").is_none());
        assert_eq!(flusher.status().streams, 1);

        let before = store.stats().requests;
        node.complete_extents("big", body, begun, &extents).await;
        node.settle(&flusher).await;
        assert_streamed(&node, &store, "big", body, begun).await;
        // The flush sent only the last part and the completion.
        assert_eq!(store.stats().requests - before, 2);
        wait_for_no_streams(&node, &flusher, &store).await;
        let tags = [("kind".to_owned(), "test".to_owned())].into();
        assert_eq!(store.tags("big"), Some(tags));

        // The next version is conditioned on the remote multipart ETag,
        // not the local MD5 one: its `PutObject` succeeds at once, where an
        // `If-Match` on the local ETag would fail and need a HEAD and
        // another try.
        let before = store.stats().requests;
        let seq = node.put("big", "small").await;
        node.settle(&flusher).await;
        assert_eq!(store.stats().requests - before, 1);
        let remote = store.object("big").unwrap();
        assert_eq!(remote.body, "small".as_bytes());
        assert_eq!(
            remote.info.metadata.write_identity(),
            Some(identity(seq).as_str())
        );
        let entry = node.entry("big").await.unwrap();
        assert_eq!(entry.remote_etag, Some(md5_etag(b"small")));
    });
}

#[test]
fn a_part_sent_from_other_extents_than_the_puts_is_sent_again() {
    runtime().block_on(async {
        let node = Node::open_inline(81).await;
        let store = remote(81, false);
        let target = body_target(&store, writes(true, true, true), PART, TIMEOUT);
        let flusher = node.flusher(&target);

        let begun = node.begin("big").await;
        // Extents announced for the body that its `PUT` does not name end
        // up in no remote object.
        let stale = node.extents("big", "stale bytes, not the body", 6).await;
        node.announce("big", begun, &stale);
        wait_until("the stale parts", async || {
            remote_parts(&store, "big").await.len() == 3
        })
        .await;

        let body = "the body that the PUT names";
        let extents = node.extents("big", body, 5).await;
        node.complete_extents("big", body, begun, &extents).await;
        node.settle(&flusher).await;
        assert_streamed(&node, &store, "big", body, begun).await;
        wait_for_no_streams(&node, &flusher, &store).await;
    });
}

#[test]
fn a_body_whose_put_never_commits_has_its_remote_upload_aborted() {
    runtime().block_on(async {
        let node = Node::open_inline(82).await;
        let store = remote(82, false);
        let target = body_target(&store, writes(true, true, true), PART, TIMEOUT);
        let flusher = node.flusher(&target);

        let begun = node.begin("big").await;
        let extents = node.extents("big", "0123456789abcdef", 8).await;
        node.announce("big", begun, &extents);
        wait_until("the parts", async || {
            remote_parts(&store, "big").await.len() == 2 && recorded(&node, begun).await
        })
        .await;
        // The body breaks off: no `PUT` commits, and once the timeout
        // passes the remote upload is aborted and its end recorded.
        wait_for_no_streams(&node, &flusher, &store).await;
        assert!(store.object("big").is_none());
        assert!(store.keys().is_empty());
    });
}

#[test]
fn a_crash_before_the_put_commits_leaves_the_remote_upload_to_be_aborted() {
    runtime().block_on(async {
        let node = Node::open_inline(83).await;
        let store = remote(83, false);
        let target = body_target(&store, writes(true, true, true), PART, TIMEOUT);
        let flusher = node.flusher(&target);

        let begun = node.begin("big").await;
        let extents = node.extents("big", "0123456789abcdef", 8).await;
        node.announce("big", begun, &extents);
        wait_until("the parts", async || {
            remote_parts(&store, "big").await.len() == 2 && recorded(&node, begun).await
        })
        .await;
        // The node crashes before the body's `PUT`, and its gateway's
        // client gives up. The remote upload's ID survives in the log.
        flusher.stop().await;
        let node = node.crash().await;
        assert!(recorded(&node, begun).await);
        assert_eq!(store.uploads().len(), 1);

        // The flusher that starts finds it, waits for a `PUT` that never
        // comes, and aborts it.
        let flusher = node.flusher(&target);
        wait_until("the resumed stream", async || flusher.status().streams == 1).await;
        wait_for_no_streams(&node, &flusher, &store).await;
        assert!(store.keys().is_empty());
    });
}

#[test]
fn a_put_that_commits_after_a_restart_completes_the_resumed_upload() {
    runtime().block_on(async {
        let node = Node::open_inline(84).await;
        let store = remote(84, false);
        let target = body_target(&store, writes(true, true, true), PART, TIMEOUT);
        let flusher = node.flusher(&target);

        let body = "0123456789abcdefghij";
        let begun = node.begin("big").await;
        let extents = node.extents("big", body, 4).await;
        node.announce("big", begun, &extents[..2]);
        wait_until("the first part", async || {
            remote_parts(&store, "big").await == [PART] && recorded(&node, begun).await
        })
        .await;

        // A new flusher resumes the remote upload from the index, but does
        // not know which extents its part came from: the completion sends
        // every part again, to the same remote upload.
        flusher.stop().await;
        let flusher = node.flusher(&target);
        wait_until("the resumed stream", async || flusher.status().streams == 1).await;
        let uploads = store.uploads();
        node.complete_extents("big", body, begun, &extents).await;
        node.settle(&flusher).await;
        assert_streamed(&node, &store, "big", body, begun).await;
        wait_for_no_streams(&node, &flusher, &store).await;
        assert_eq!(uploads.len(), 1);
    });
}

#[test]
fn a_streamed_put_stored_inline_completes_with_one_part() {
    runtime().block_on(async {
        let node = Node::open_inline(85).await;
        let store = remote(85, false);
        let target = body_target(&store, writes(true, true, true), PART, TIMEOUT);
        let flusher = node.flusher(&target);

        // A body announced before any extent opens the remote upload; its
        // `PUT` holds its bytes inline.
        let begun = node.begin("small").await;
        node.announce("small", begun, &[]);
        wait_until("the remote upload", async || recorded(&node, begun).await).await;
        node.complete("small", "tiny", begun).await;
        node.settle(&flusher).await;
        let remote = store.object("small").unwrap();
        assert_eq!(remote.info.etag, multipart_etag(&["tiny"]));
        let entry = node.entry("small").await.unwrap();
        assert_eq!(entry.remote_etag, Some(multipart_etag(&["tiny"])));
        assert_eq!(entry.object.unwrap().local_etag, md5_etag(b"tiny"));
        wait_for_no_streams(&node, &flusher, &store).await;
    });
}

#[test]
fn a_retagged_streamed_put_is_sent_whole_and_its_upload_aborted() {
    runtime().block_on(async {
        let node = Node::open_inline(86).await;
        let store = remote(86, false);
        let target = body_target(&store, writes(true, true, true), PART, TIMEOUT);
        let flusher = node.flusher(&target);

        let body = "0123456789abcdef";
        let begun = node.begin("big").await;
        let extents = node.extents("big", body, 8).await;
        node.announce("big", begun, &extents);
        wait_until("the parts", async || remote_parts(&store, "big").await.len() == 2).await;
        node.complete_extents("big", body, begun, &extents).await;
        // A `TAGS` before the flush gives the version an identity of its
        // own, which the stream does not carry.
        let tagged = node.tag("big", &[("new", "tag")]).await;
        node.settle(&flusher).await;
        let remote = store.object("big").unwrap();
        assert_eq!(remote.info.etag, md5_etag(body.as_bytes()));
        assert_eq!(
            remote.info.metadata.write_identity(),
            Some(identity(tagged).as_str())
        );
        wait_for_no_streams(&node, &flusher, &store).await;
    });
}

#[test]
fn without_streaming_a_large_put_is_sent_after_its_commit() {
    runtime().block_on(async {
        let node = Node::open_inline(87).await;
        let store = remote(87, false);
        let target = target_with(&store, writes(true, true, true));
        let flusher = node.flusher(&target);

        let body = "0123456789abcdef";
        let begun = node.begin("big").await;
        let extents = node.extents("big", body, 8).await;
        node.announce("big", begun, &extents);
        node.complete_extents("big", body, begun, &extents).await;
        node.settle(&flusher).await;
        let remote = store.object("big").unwrap();
        assert_eq!(remote.info.etag, md5_etag(body.as_bytes()));
        assert_eq!(
            remote.info.metadata.write_identity(),
            Some(identity(begun).as_str())
        );
        assert_eq!(store.stats().requests, 1);
        assert_eq!(flusher.status().streams, 0);
    });
}
