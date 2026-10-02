//! The write identity of streamed single PUTs (§7.2): the `PUT` inherits
//! the identity of the `UPLOAD_BEGIN` committed when its body started to
//! stream, and the remote create, a replayed write, and the HEAD after a
//! failed precondition all compare that one identity. Each test also
//! counts the remote requests: streaming adds none.

mod support;

use skys3_index::EntryState;
use skys3_remote::{ObjectStore, PutObject, UserMetadata};
use skys3_sim::SimS3;
use skys3_sim::s3::{Fault, Operation};
use support::{Node, identity, md5_etag, remote, runtime, target};

/// The remote object at `key`: its body and write identity.
fn remote_object(store: &SimS3, key: &str) -> Option<(String, Option<String>)> {
    store.object(key).map(|object| {
        let body = String::from_utf8(object.body.to_vec()).unwrap();
        let wid = object.info.metadata.write_identity().map(str::to_owned);
        (body, wid)
    })
}

/// Puts `body` at `key` of the remote with the write identity of `seq`,
/// as a flush whose `FLUSHED` was lost left it.
async fn flushed_earlier(store: &SimS3, key: &str, body: &str, seq: u64) {
    let mut metadata = UserMetadata::new();
    metadata.insert("skys3-wid", identity(seq)).unwrap();
    let request = PutObject::new(key, body.to_owned()).with_metadata(metadata);
    store.put_object(request).await.unwrap();
}

#[test]
fn the_remote_create_carries_the_inherited_identity() {
    runtime().block_on(async {
        let node = Node::open(21).await;
        let store = remote(21, false);
        // A streamed PUT that failed: its UPLOAD_BEGIN names no write.
        let failed = node.begin("k").await;
        let plain = node.put("k", "plain").await;
        let begun = node.begin("k").await;
        let seq = node.complete("k", "streamed", begun).await;
        assert!(failed < plain && plain < begun && begun < seq);
        let flusher = node.flusher(&target(&store));
        node.settle(&flusher).await;

        // One PutObject, as for any single PUT, named by the UPLOAD_BEGIN:
        // the failed upload's identity never reached the remote.
        assert_eq!(store.stats().requests, 1);
        assert_eq!(
            remote_object(&store, "k"),
            Some(("streamed".to_owned(), Some(identity(begun))))
        );
        let entry = node.entry("k").await.unwrap();
        assert_eq!(entry.version.seq.get(), seq);
        assert_eq!(entry.state, EntryState::Clean);
        assert_eq!(entry.remote_etag, Some(md5_etag(b"streamed")));
    });
}

#[test]
fn a_replayed_write_is_recognized_by_the_inherited_identity() {
    runtime().block_on(async {
        let node = Node::open(22).await;
        let store = remote(22, false);
        let begun = node.begin("k").await;
        let seq = node.complete("k", "streamed", begun).await;
        // The remote applies the PutObject but its answer is lost: the
        // retry's `If-None-Match: *` fails, and the HEAD finds the
        // identity the create carried.
        store.inject(Operation::PutObject, Fault::LostResponse);
        let flusher = node.flusher(&target(&store));
        node.settle(&flusher).await;

        assert_eq!(store.stats().lost_responses, 1);
        assert!(flusher.status().conflicts.is_empty());
        assert_eq!(
            remote_object(&store, "k"),
            Some(("streamed".to_owned(), Some(identity(begun))))
        );
        let entry = node.entry("k").await.unwrap();
        assert_eq!(
            (entry.version.seq.get(), entry.state),
            (seq, EntryState::Clean)
        );
        // The lost PutObject, the retry that failed its precondition, and
        // the HEAD: the same three requests as for any single PUT.
        assert_eq!(store.stats().requests, 3);
    });
}

#[test]
fn the_head_after_a_failed_precondition_compares_versions_not_identities() {
    runtime().block_on(async {
        let node = Node::open(23).await;
        let store = remote(23, false);
        // A write of the key commits while a streamed PUT of it streams:
        // its seq lies between the UPLOAD_BEGIN and the completing PUT.
        let begun = node.begin("k").await;
        let between = node.put("k", "between").await;
        let seq = node.complete("k", "streamed", begun).await;
        // The write in between reached the remote, but its `FLUSHED` was
        // lost. Its identity is newer than the streamed PUT's, but its
        // version is older, so the streamed PUT supersedes it.
        flushed_earlier(&store, "k", "between", between).await;
        let flusher = node.flusher(&target(&store));
        node.settle(&flusher).await;

        assert!(flusher.status().conflicts.is_empty());
        assert_eq!(
            remote_object(&store, "k"),
            Some(("streamed".to_owned(), Some(identity(begun))))
        );
        let entry = node.entry("k").await.unwrap();
        assert_eq!(
            (entry.version.seq.get(), entry.state),
            (seq, EntryState::Clean)
        );
        // The earlier PUT, then: `If-None-Match: *`, refused; the HEAD;
        // and `If-Match` on the superseded object's ETag.
        assert_eq!(store.stats().requests, 4);

        // A version after a streamed PUT supersedes it the same way when
        // the streamed PUT's `FLUSHED` was lost: its inherited identity
        // predates the version.
        flusher.stop().await;
        let begun = node.begin("j").await;
        node.complete("j", "streamed", begun).await;
        flushed_earlier(&store, "j", "streamed", begun).await;
        let later = node.put("j", "later").await;
        let flusher = node.flusher(&target(&store));
        node.settle(&flusher).await;
        assert!(flusher.status().conflicts.is_empty());
        assert_eq!(
            remote_object(&store, "j"),
            Some(("later".to_owned(), Some(identity(later))))
        );
    });
}
