//! Streaming multipart flush (§7.3) when the flusher's own reads fail. The
//! failpoint is process-wide, so these tests have a binary of their own.

mod support;

use std::time::Duration;

use skys3_flush::test_hooks;
use skys3_remote::{ListParts, ObjectStore};
use skys3_sim::SimS3;
use support::{Node, Patience, remote, runtime, streaming_target, writes};

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

/// A failed read of the local upload's parts while the remote upload is
/// opened leaves one remote upload, which the stream keeps: later parts
/// stream to it, and the completion completes it.
#[test]
fn a_failed_read_while_opening_keeps_the_remote_upload() {
    runtime().block_on(async {
        let node = Node::open(61).await;
        let store = remote(61, false);
        let target = streaming_target(&store, writes(true, true, true));
        let flusher = node.flusher(&target);

        test_hooks::fail_parts_reads(1);
        let upload = node.create("big").await;
        let first = node.part("big", upload, 1, "first part; ").await;
        let second = node.part("big", upload, 2, "second").await;
        wait_until("the streamed parts", async || {
            remote_parts(&store, "big").await == [(1, 12), (2, 6)]
        })
        .await;
        assert_eq!(store.uploads().len(), 1);
        assert!(node.shard.remote_upload(upload).await.unwrap().is_some());
        let before = store.stats().requests;

        let parts = [(1, first, "first part; "), (2, second, "second")];
        let object = node.finish("big", upload, &parts).await;
        node.settle(&flusher).await;
        let remote = store.object("big").unwrap();
        assert_eq!(remote.info.etag, object.etag);
        assert_eq!(&remote.body[..], b"first part; second");
        // The completion only completes the one remote upload.
        assert_eq!(store.stats().requests - before, 1);
        wait_until("the stream's end", async || {
            flusher.status().streams == 0
                && store.uploads().is_empty()
                && node.shard.remote_upload(upload).await.unwrap().is_none()
        })
        .await;
    });
}
