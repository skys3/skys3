//! Tests of the shard flusher against the simulated store: conditional
//! requests, the 412 recovery rule, retries, conflicts, and the §4.2
//! transitions Dirty → Flushing, Flushing → Dirty, and Flushing →
//! Conflict.

mod support;

use std::collections::BTreeMap;
use std::time::Duration;

use skys3_flush::{Phase, ShardFlusher};
use skys3_index::EntryState;
use skys3_log::RecordBody;
use skys3_remote::{ObjectStore, PutObject, UserMetadata};
use skys3_sim::SimS3;
use skys3_sim::s3::{Fault, Operation};
use support::{Node, Patience, identity, md5_etag, remote, runtime, target, target_with, writes};

/// Waits until `key`'s phase satisfies `check`.
async fn wait_for(flusher: &ShardFlusher, key: &str, check: impl Fn(Option<&Phase>) -> bool) {
    let patience = Patience::new();
    while !patience.is_exhausted() {
        if check(flusher.phase(key).as_ref()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!("{key} never reached the phase: {:?}", flusher.phase(key));
}

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

#[test]
fn flushes_puts_tags_and_deletes_with_their_identity() {
    runtime().block_on(async {
        let node = Node::open(1).await;
        let store = remote(1, false);
        let a = node.put("a", "alpha").await;
        let b = node.put("dir/b", "beta").await;
        let flusher = node.flusher(&target(&store));
        node.settle(&flusher).await;

        assert_eq!(
            remote_object(&store, "a"),
            Some(("alpha".to_owned(), Some(identity(a))))
        );
        assert_eq!(
            remote_object(&store, "dir/b"),
            Some(("beta".to_owned(), Some(identity(b))))
        );
        let object = store.object("a").unwrap();
        assert_eq!(object.info.etag, md5_etag(b"alpha"));
        assert_eq!(object.info.content_type.as_deref(), Some("text/plain"));
        assert_eq!(object.info.metadata.get("owner"), Some("team-a"));
        assert_eq!(
            store.tags("a"),
            Some(BTreeMap::from([("kind".to_owned(), "test".to_owned())]))
        );
        let entry = node.entry("a").await.unwrap();
        assert_eq!(entry.state, EntryState::Clean);
        assert_eq!(entry.remote_etag, Some(md5_etag(b"alpha")));

        // A tag-only change re-PUTs the bytes with the new tags and the
        // identity of its TAGS record.
        let tagged = node.tag("a", &[("kind", "changed")]).await;
        node.settle(&flusher).await;
        assert_eq!(
            remote_object(&store, "a"),
            Some(("alpha".to_owned(), Some(identity(tagged))))
        );
        assert_eq!(
            store.tags("a"),
            Some(BTreeMap::from([("kind".to_owned(), "changed".to_owned())]))
        );

        // A flushed delete removes the key and its entry.
        node.delete("a").await;
        node.settle(&flusher).await;
        assert_eq!(remote_object(&store, "a"), None);
        assert!(node.entry("a").await.is_none());
        assert!(flusher.phase("a").is_none());
        let status = flusher.status();
        assert_eq!((status.dirty, status.dirty_bytes), (0, 0));
        assert!(status.conflicts.is_empty() && status.last_error.is_none());
    });
}

#[test]
fn only_the_latest_version_is_flushed() {
    runtime().block_on(async {
        let node = Node::open(2).await;
        let store = remote(2, false);
        for i in 0..20 {
            node.put("k", &format!("v{i}")).await;
        }
        let last = node.put("k", "final").await;
        let flusher = node.flusher(&target(&store));
        node.settle(&flusher).await;
        assert_eq!(
            remote_object(&store, "k"),
            Some(("final".to_owned(), Some(identity(last))))
        );
        // One PUT for the coalesced versions.
        assert_eq!(store.stats().requests, 1);
    });
}

#[test]
fn a_version_committed_during_a_flush_is_flushed_next() {
    runtime().block_on(async {
        let node = Node::open(3).await;
        let store = remote(3, false);
        node.put("k", "one").await;
        store.inject(Operation::PutObject, Fault::Delay(Duration::from_secs(5)));
        let flusher = node.flusher(&target(&store));
        // Dirty → Flushing.
        wait_for(&flusher, "k", |phase| phase == Some(&Phase::Flushing)).await;
        let two = node.put("k", "two").await;
        node.settle(&flusher).await;
        assert_eq!(
            remote_object(&store, "k"),
            Some(("two".to_owned(), Some(identity(two))))
        );
        // The second PUT was conditioned on the first one's ETag, so it
        // found no conflict.
        assert!(flusher.status().conflicts.is_empty());
    });
}

#[test]
fn retryable_errors_back_off_and_retry() {
    runtime().block_on(async {
        let node = Node::open(4).await;
        let store = remote(4, false);
        let seq = node.put("k", "body").await;
        for fault in [Fault::SlowDown, Fault::InternalError, Fault::LostRequest] {
            store.inject(Operation::PutObject, fault);
        }
        let flusher = node.flusher(&target(&store));
        // Flushing → Dirty on a retryable error, until it succeeds.
        wait_for(&flusher, "k", |phase| {
            matches!(phase, Some(Phase::Backoff(_)))
        })
        .await;
        assert!(flusher.status().last_error.is_some());
        node.settle(&flusher).await;
        assert_eq!(
            remote_object(&store, "k"),
            Some(("body".to_owned(), Some(identity(seq))))
        );
        let stats = store.stats();
        assert_eq!(
            (stats.slow_downs, stats.internal_errors, stats.lost_requests),
            (1, 1, 1)
        );
    });
}

#[test]
fn a_lost_response_is_recognized_by_the_write_identity() {
    runtime().block_on(async {
        let node = Node::open(5).await;
        let store = remote(5, false);
        let seq = node.put("k", "body").await;
        store.inject(Operation::PutObject, Fault::LostResponse);
        let flusher = node.flusher(&target(&store));
        node.settle(&flusher).await;
        // The retry's `If-None-Match: *` failed, and the HEAD found this
        // version's identity: the flush is done, with no conflict.
        assert_eq!(store.stats().lost_responses, 1);
        assert!(flusher.status().conflicts.is_empty());
        assert_eq!(
            remote_object(&store, "k"),
            Some(("body".to_owned(), Some(identity(seq))))
        );
        assert_eq!(node.entry("k").await.unwrap().state, EntryState::Clean);

        // A lost delete response: the retry's HEAD finds nothing.
        node.delete("k").await;
        store.inject(Operation::DeleteObject, Fault::LostResponse);
        node.settle(&flusher).await;
        assert_eq!(remote_object(&store, "k"), None);
        assert!(node.entry("k").await.is_none());
    });
}

#[test]
fn an_earlier_flush_whose_record_was_lost_is_superseded() {
    runtime().block_on(async {
        let node = Node::open(6).await;
        let store = remote(6, false);
        node.put("k", "one").await;
        let flusher = node.flusher(&target(&store));
        node.settle(&flusher).await;
        flusher.stop().await;
        // The remote holds a version of the key the index does not know
        // about, as after a crash that lost a `FLUSHED`: put it there with
        // an identity of this shard that predates the next version.
        let mut metadata = UserMetadata::new();
        metadata.insert("skys3-wid", identity(2)).unwrap();
        store
            .put_object(PutObject::new("fresh", "old").with_metadata(metadata))
            .await
            .unwrap();
        let fresh = node.put("fresh", "new").await;
        let flusher = node.flusher(&target(&store));
        node.settle(&flusher).await;
        assert_eq!(
            remote_object(&store, "fresh"),
            Some(("new".to_owned(), Some(identity(fresh))))
        );
        assert!(flusher.status().conflicts.is_empty());
    });
}

#[test]
fn out_of_band_writes_become_conflicts() {
    runtime().block_on(async {
        let node = Node::open(7).await;
        let store = remote(7, false);
        // Another writer creates a key SkyS3 believes absent.
        out_of_band(&store, "new", "theirs").await;
        let seq = node.put("new", "ours").await;
        // And overwrites a key SkyS3 flushed.
        node.put("old", "ours").await;
        let flusher = node.flusher(&target(&store));
        node.settle(&flusher).await;
        out_of_band(&store, "old", "theirs too").await;
        node.put("old", "ours again").await;
        node.settle(&flusher).await;

        // Flushing → Conflict for both; neither remote write is overwritten.
        for (key, body) in [("new", "theirs"), ("old", "theirs too")] {
            assert!(matches!(flusher.phase(key), Some(Phase::Conflict(_))));
            assert_eq!(remote_object(&store, key), Some((body.to_owned(), None)));
            assert_eq!(node.entry(key).await.unwrap().state, EntryState::Dirty);
        }
        let status = flusher.status();
        assert_eq!(status.conflicts.len(), 2);
        assert_eq!(status.conflicts[0].key, "new");
        assert_eq!(status.conflicts[0].seq.get(), seq);
        assert_eq!(status.conflicts[0].remote_identity, None);
        assert!(status.oldest_dirty.is_some());
        assert_eq!(status.oldest_pending, None);

        // A conflicted key stays held after another local write.
        node.put("new", "ours, later").await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(matches!(flusher.phase("new"), Some(Phase::Conflict(_))));
        assert_eq!(remote_object(&store, "new").unwrap().0, "theirs");

        // Deletes are held too.
        out_of_band(&store, "gone", "theirs").await;
        node.put("gone", "ours").await;
        node.delete("gone").await;
        node.settle(&flusher).await;
        assert!(matches!(flusher.phase("gone"), Some(Phase::Conflict(_))));
        assert_eq!(remote_object(&store, "gone").unwrap().0, "theirs");
    });
}

#[test]
fn a_restarted_flusher_finds_conflicts_again() {
    runtime().block_on(async {
        let node = Node::open(8).await;
        let store = remote(8, false);
        out_of_band(&store, "k", "theirs").await;
        node.put("k", "ours").await;
        let flusher = node.flusher(&target(&store));
        node.settle(&flusher).await;
        assert_eq!(flusher.status().conflicts.len(), 1);
        flusher.stop().await;
        let flusher = node.flusher(&target(&store));
        node.settle(&flusher).await;
        assert_eq!(flusher.status().conflicts.len(), 1);
        assert_eq!(remote_object(&store, "k").unwrap().0, "theirs");
    });
}

#[test]
fn unprotected_operations_are_sent_unconditionally() {
    runtime().block_on(async {
        let node = Node::open(9).await;
        let store = remote(9, false);
        node.put("k", "ours").await;
        // Like R2: conditional PUTs, unconditional deletes.
        let target = target_with(&store, writes(true, false, false));
        let flusher = node.flusher(&target);
        node.settle(&flusher).await;
        out_of_band(&store, "k", "theirs").await;
        node.delete("k").await;
        node.settle(&flusher).await;
        // The delete was sent without If-Match, so it removed the other
        // writer's object: what the bucket status warns about.
        assert_eq!(remote_object(&store, "k"), None);
        assert!(flusher.status().conflicts.is_empty());
        assert_eq!(target.writes().unprotected().len(), 2);
    });
}

#[test]
fn conflicting_requests_are_retried() {
    runtime().block_on(async {
        let node = Node::open(10).await;
        let store = remote(10, false);
        let seq = node.put("k", "ours").await;
        store.inject(Operation::PutObject, Fault::Conflict);
        let flusher = node.flusher(&target(&store));
        node.settle(&flusher).await;
        assert_eq!(store.stats().conflicts, 1);
        assert_eq!(
            remote_object(&store, "k"),
            Some(("ours".to_owned(), Some(identity(seq))))
        );
    });
}

#[test]
fn a_flusher_stops_with_its_shard() {
    runtime().block_on(async {
        let node = Node::open(11).await;
        let store = remote(11, false);
        let flusher = node.flusher(&target(&store));
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!flusher.is_stopped());
        node.shard.close().await.unwrap();
        let patience = Patience::new();
        while !patience.is_exhausted() {
            if flusher.is_stopped() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(flusher.status().stopped);
    });
}

#[test]
fn a_version_that_cannot_be_sent_is_retried_and_reported() {
    runtime().block_on(async {
        let node = Node::open(12).await;
        let store = remote(12, false);
        let RecordBody::Put(mut put) = support::put("k", "body") else {
            unreachable!()
        };
        // A value S3 cannot carry in a header unchanged.
        put.metadata
            .insert("x-amz-meta-note".to_owned(), "caf\u{e9}".to_owned());
        node.shard.commit(RecordBody::Put(put)).await.unwrap();
        let flusher = node.flusher(&target(&store));
        wait_for(&flusher, "k", |phase| {
            matches!(phase, Some(Phase::Backoff(_)))
        })
        .await;
        let status = flusher.status();
        assert!(status.last_error.unwrap().contains("note"));
        assert_eq!((status.dirty, status.dirty_bytes), (1, 4));
        assert!(status.oldest_pending.is_some());
        assert_eq!(store.stats().requests, 0);

        // A later version that can be sent replaces it.
        let seq = node.put("k", "fixed").await;
        node.settle(&flusher).await;
        assert_eq!(
            remote_object(&store, "k"),
            Some(("fixed".to_owned(), Some(identity(seq))))
        );
    });
}

#[test]
fn multipart_objects_wait_without_holding_up_other_keys() {
    runtime().block_on(async {
        let node = Node::open(13).await;
        let store = remote(13, false);
        let first = node.put("over", "v1").await;
        node.put("gone", "v1").await;
        let flusher = node.flusher(&target(&store));
        node.settle(&flusher).await;

        // Completed multipart objects over flushed keys and a new one.
        node.multipart("over", &["aaaa", "bb"]).await;
        node.multipart("gone", &["c"]).await;
        node.multipart("fresh", &["ddd"]).await;
        let plain = node.put("plain", "p").await;
        node.settle(&flusher).await;

        // They wait, dirty, apart from the line and from conflicts; the
        // remote keeps what it had, and other keys flush.
        let status = flusher.status();
        assert_eq!(status.awaiting_multipart, ["fresh", "gone", "over"]);
        assert!(status.conflicts.is_empty() && status.last_error.is_none());
        assert_eq!((status.dirty, status.dirty_bytes), (0, 10));
        assert!(status.oldest_dirty.is_some());
        assert_eq!(status.oldest_pending, None);
        for key in ["over", "gone", "fresh"] {
            assert_eq!(flusher.phase(key), Some(Phase::AwaitsMultipart));
            assert_eq!(node.entry(key).await.unwrap().state, EntryState::Dirty);
        }
        assert_eq!(
            remote_object(&store, "over"),
            Some(("v1".to_owned(), Some(identity(first))))
        );
        assert_eq!(remote_object(&store, "fresh"), None);
        assert_eq!(
            remote_object(&store, "plain"),
            Some(("p".to_owned(), Some(identity(plain))))
        );

        // A tag change keeps the parts, so the key still waits; so it does
        // for a restarted flusher.
        node.tag("over", &[("kind", "video")]).await;
        flusher.stop().await;
        let flusher = node.flusher(&target(&store));
        node.settle(&flusher).await;
        assert_eq!(flusher.status().awaiting_multipart.len(), 3);
        assert_eq!(remote_object(&store, "over").unwrap().0, "v1");

        // An overwrite and tombstones flush as usual.
        let over = node.put("over", "v2").await;
        node.delete("gone").await;
        node.delete("fresh").await;
        node.settle(&flusher).await;
        assert_eq!(
            remote_object(&store, "over"),
            Some(("v2".to_owned(), Some(identity(over))))
        );
        assert_eq!(remote_object(&store, "gone"), None);
        assert_eq!(remote_object(&store, "fresh"), None);
        assert_eq!(node.entry("over").await.unwrap().state, EntryState::Clean);
        assert!(node.entry("gone").await.is_none() && node.entry("fresh").await.is_none());
        let status = flusher.status();
        assert!(status.awaiting_multipart.is_empty() && status.conflicts.is_empty());
        assert_eq!((status.dirty, status.dirty_bytes), (0, 0));
    });
}
