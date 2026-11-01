//! Copies flushed as remote server-side copies (plan M4-07, design §7.2,
//! §11): a copy of a clean source in the same remote bucket is sent as a
//! `CopyObject` with its own metadata, tags, and write identity, the
//! source's `remote_etag` as `x-amz-copy-source-if-match`, and the
//! destination precondition. A lost answer is recognized by the copy's
//! identity after a `412`; a source changed at the remote, a target
//! without support, and a target that refuses the copy all get a regular
//! upload, and none of them is a conflict.

mod support;

use std::collections::BTreeMap;
use std::sync::Arc;

use skys3_config::ConflictPolicy;
use skys3_flush::{CopySources, ShardFlusher, Target};
use skys3_index::EntryState;
use skys3_log::RecordBody;
use skys3_log::record::CopySource;
use skys3_remote::probe::{ConditionalProbe, ConditionalWrites};
use skys3_remote::{DeleteObject, ObjectStore, PutObject, UserMetadata};
use skys3_sim::SimS3;
use skys3_sim::s3::{ConditionalSupport, CopyDirectives, Fault, Operation, SimS3Config};
use skys3_types::{BucketId, ETag, Seq, VersionIdentity};
use support::{
    Node, cluster, identity, md5_etag, put, remote, runtime, settings, shard_ref, writes,
};

/// The bucket the test shard belongs to, whose objects are at the root of
/// the remote bucket.
fn own() -> BucketId {
    shard_ref().bucket
}

/// Another bucket flushed to the same remote bucket, under `other/`.
fn other() -> BucketId {
    BucketId::new("b-other").unwrap()
}

/// A target on `store` honoring `writes`, whose copies find sources of
/// the test bucket and of [`other`] there, under `policy`.
fn target(store: &SimS3, writes: ConditionalWrites, policy: ConflictPolicy) -> Arc<Target<SimS3>> {
    let sources = CopySources::new().with(own(), "").with(other(), "other/");
    let target = Target::new(Arc::new(store.clone()), "", writes, cluster(), settings())
        .with_copy_sources(sources)
        .with_conflict_policy(policy);
    Arc::new(target)
}

/// A target honoring everything, holding conflicts.
fn full(store: &SimS3) -> Arc<Target<SimS3>> {
    target(store, writes(true, true, true), ConflictPolicy::Hold)
}

/// The tags and user metadata every copy gets, which differ from the
/// source's.
fn copy_tags() -> BTreeMap<String, String> {
    BTreeMap::from([("copied".to_owned(), "yes".to_owned())])
}

/// Commits a copy of `body`, whose source is `source` of `bucket` at
/// `seq` with `remote_etag`, to `key`, with its own metadata and tags, and
/// returns the copy's `seq`.
async fn copy(
    node: &Node,
    key: &str,
    body: &str,
    bucket: BucketId,
    (source, seq): (&str, u64),
    remote_etag: Option<ETag>,
) -> u64 {
    let RecordBody::Put(mut record) = put(key, body) else {
        unreachable!("put makes a PUT")
    };
    record.metadata = BTreeMap::from([
        ("content-type".to_owned(), "image/png".to_owned()),
        ("content-language".to_owned(), "en".to_owned()),
        ("x-amz-meta-owner".to_owned(), "team-copy".to_owned()),
    ]);
    record.tags = copy_tags();
    record.copy_source = Some(CopySource {
        bucket,
        key: source.to_owned(),
        version: VersionIdentity::new(Seq::new(seq), md5_etag(body.as_bytes())),
        remote_etag,
    });
    let committed = node.shard.commit(RecordBody::Put(record)).await.unwrap();
    assert!(committed.outcome.is_applied(), "{:?}", committed.outcome);
    committed.position.seq.get()
}

/// Puts `body` at `key` and flushes it with `flusher`, and returns its
/// `seq` and remote ETag: a clean source.
async fn clean(node: &Node, flusher: &ShardFlusher, key: &str, body: &str) -> (u64, ETag) {
    let seq = node.put(key, body).await;
    node.settle(flusher).await;
    let entry = node.entry(key).await.unwrap();
    assert_eq!(entry.state, EntryState::Clean);
    (seq, entry.remote_etag.unwrap())
}

/// Checks that the remote holds the copy committed at `seq` at `key`: its
/// bytes, metadata, tags, and write identity, and that the entry is clean
/// with the remote's ETag.
async fn assert_copied(node: &Node, store: &SimS3, key: &str, body: &str, seq: u64) {
    let object = store.object(key).unwrap();
    assert_eq!(object.body, body.as_bytes());
    assert_eq!(
        object.info.metadata.write_identity(),
        Some(identity(seq).as_str())
    );
    assert_eq!(object.info.metadata.get("owner"), Some("team-copy"));
    assert_eq!(object.info.content_type.as_deref(), Some("image/png"));
    assert_eq!(store.tags(key), Some(copy_tags()));
    let entry = node.entry(key).await.unwrap();
    assert_eq!(entry.state, EntryState::Clean);
    assert_eq!(entry.remote_etag, Some(object.info.etag));
}

/// The keys the store applied `operation` to.
fn applied(store: &SimS3, operation: Operation) -> Vec<String> {
    store.applied(operation)
}

#[test]
fn copies_of_clean_sources_are_copied_at_the_remote() {
    runtime().block_on(async {
        let node = Node::open(1).await;
        let store = remote(1, false);
        let target = full(&store);
        let flusher = node.flusher(&target);
        let (src, etag) = clean(&node, &flusher, "src", "the bytes").await;

        // A copy to a new key, and one over a key the remote holds.
        let (_, _) = clean(&node, &flusher, "dst-2", "old").await;
        let first = copy(
            &node,
            "dst-1",
            "the bytes",
            own(),
            ("src", src),
            Some(etag.clone()),
        )
        .await;
        let second = copy(&node, "dst-2", "the bytes", own(), ("src", src), Some(etag)).await;
        node.settle(&flusher).await;
        assert_copied(&node, &store, "dst-1", "the bytes", first).await;
        assert_copied(&node, &store, "dst-2", "the bytes", second).await;
        assert_eq!(applied(&store, Operation::CopyObject), ["dst-1", "dst-2"]);
        assert_eq!(applied(&store, Operation::PutObject), ["src", "dst-2"]);
        assert_eq!(target.counters().copies.get(), 2);
        assert_eq!(target.counters().copy_fallbacks.get(), 0);

        // A copy from another bucket in the same remote bucket.
        let written = store
            .put_object(PutObject::new("other/src", "other bytes"))
            .await
            .unwrap();
        let third = copy(
            &node,
            "dst-3",
            "other bytes",
            other(),
            ("src", 1),
            Some(written.etag),
        )
        .await;
        node.settle(&flusher).await;
        assert_copied(&node, &store, "dst-3", "other bytes", third).await;
        assert_eq!(target.counters().copies.get(), 3);

        // Without a known remote source, or from a bucket elsewhere, the
        // copy is uploaded.
        let unclean = copy(&node, "dst-4", "the bytes", own(), ("src", src), None).await;
        let elsewhere = BucketId::new("b-elsewhere").unwrap();
        let far = copy(
            &node,
            "dst-5",
            "far",
            elsewhere,
            ("src", 1),
            Some(md5_etag(b"far")),
        )
        .await;
        node.settle(&flusher).await;
        assert_copied(&node, &store, "dst-4", "the bytes", unclean).await;
        assert_copied(&node, &store, "dst-5", "far", far).await;
        assert_eq!(applied(&store, Operation::CopyObject).len(), 3);
        assert!(flusher.status().conflicts.is_empty());
    });
}

#[test]
fn a_lost_answer_to_a_copy_is_recognized_by_its_identity() {
    runtime().block_on(async {
        for (source_changes, over_an_object) in
            [(false, false), (true, false), (false, true), (true, true)]
        {
            let node = Node::open(2).await;
            let store = remote(2, false);
            let target = full(&store);
            let flusher = node.flusher(&target);
            let (src, etag) = clean(&node, &flusher, "src", "the bytes").await;
            if over_an_object {
                clean(&node, &flusher, "dst", "old").await;
            }
            // The copy lands, and its answer is lost. Then, if
            // `source_changes`, the source changes at the remote, so that
            // the retry fails both preconditions.
            store.inject(Operation::CopyObject, Fault::LostResponse);
            let seq = copy(&node, "dst", "the bytes", own(), ("src", src), Some(etag)).await;
            if source_changes {
                let mut waited = 0;
                while applied(&store, Operation::CopyObject).is_empty() {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    waited += 1;
                    assert!(waited < 60_000, "the copy never landed");
                }
                store
                    .put_object(PutObject::new("src", "changed"))
                    .await
                    .unwrap();
            }
            node.settle(&flusher).await;
            let case = format!("source changes: {source_changes}, over: {over_an_object}");
            assert_copied(&node, &store, "dst", "the bytes", seq).await;
            assert!(flusher.status().conflicts.is_empty(), "{case}");
            // One copy landed, the retry was refused, and nothing was
            // uploaded for it.
            assert_eq!(applied(&store, Operation::CopyObject), ["dst"], "{case}");
            assert_eq!(store.unanswered(Operation::CopyObject), 1, "{case}");
            assert!(
                !applied(&store, Operation::PutObject)
                    .iter()
                    .skip(usize::from(over_an_object) + 1)
                    .any(|key| key == "dst"),
                "{case}"
            );
        }
    });
}

#[test]
fn a_copy_whose_source_changed_at_the_remote_is_uploaded() {
    runtime().block_on(async {
        // The source overwritten or deleted at the remote, before a copy
        // to a new key or over an object the remote holds.
        for (deleted, over_an_object) in
            [(false, false), (true, false), (false, true), (true, true)]
        {
            let node = Node::open(3).await;
            let store = remote(3, false);
            let target = full(&store);
            let flusher = node.flusher(&target);
            let (src, etag) = clean(&node, &flusher, "src", "the bytes").await;
            if over_an_object {
                clean(&node, &flusher, "dst", "old").await;
            }
            flusher.stop().await;
            let seq = copy(&node, "dst", "the bytes", own(), ("src", src), Some(etag)).await;
            if deleted {
                store.delete_object(DeleteObject::new("src")).await.unwrap();
            } else {
                store
                    .put_object(PutObject::new("src", "changed"))
                    .await
                    .unwrap();
            }
            let flusher = node.flusher(&target);
            node.settle(&flusher).await;
            let case = format!("deleted: {deleted}, over: {over_an_object}");
            assert_copied(&node, &store, "dst", "the bytes", seq).await;
            assert!(flusher.status().conflicts.is_empty(), "{case}");
            assert!(applied(&store, Operation::CopyObject).is_empty(), "{case}");
            let puts = applied(&store, Operation::PutObject);
            assert_eq!(puts.last().map(String::as_str), Some("dst"), "{case}");
            assert_eq!(target.counters().copy_fallbacks.get(), 1, "{case}");
            assert_eq!(target.counters().copies.get(), 0, "{case}");
        }
    });
}

#[test]
fn targets_without_support_get_uploads() {
    runtime().block_on(async {
        // What the probe finds on a store without object tags, and one
        // that rejects the directives: copies are uploaded.
        for directives in [
            CopyDirectives::WITHOUT_TAGS,
            CopyDirectives {
                metadata: ConditionalSupport::Rejected,
                tagging: ConditionalSupport::Honored,
            },
        ] {
            let store = SimS3::new(
                4,
                SimS3Config {
                    copy_directives: directives,
                    min_part_size: 1,
                    ..SimS3Config::default()
                },
            );
            let found = ConditionalProbe::new("", 4).run(&store).await.unwrap();
            assert!(!found.copy_object.is_usable(), "{directives:?}");
            let before = applied(&store, Operation::CopyObject).len();
            let node = Node::open(4).await;
            let target = target(&store, found, ConflictPolicy::Hold);
            let flusher = node.flusher(&target);
            let (src, etag) = clean(&node, &flusher, "src", "the bytes").await;
            let seq = copy(&node, "dst", "the bytes", own(), ("src", src), Some(etag)).await;
            node.settle(&flusher).await;
            assert_copied(&node, &store, "dst", "the bytes", seq).await;
            assert_eq!(applied(&store, Operation::CopyObject).len(), before);
            assert_eq!(target.counters().copy_fallbacks.get(), 0);
        }

        // A target that refuses the copy although the probe found it
        // supported (its support changed since): the copy is uploaded.
        let store = SimS3::new(
            5,
            SimS3Config {
                copy_directives: CopyDirectives {
                    metadata: ConditionalSupport::Rejected,
                    tagging: ConditionalSupport::Rejected,
                },
                ..SimS3Config::default()
            },
        );
        let node = Node::open(5).await;
        let target = full(&store);
        let flusher = node.flusher(&target);
        let (src, etag) = clean(&node, &flusher, "src", "the bytes").await;
        let seq = copy(&node, "dst", "the bytes", own(), ("src", src), Some(etag)).await;
        node.settle(&flusher).await;
        assert_copied(&node, &store, "dst", "the bytes", seq).await;
        assert!(applied(&store, Operation::CopyObject).is_empty());
        assert_eq!(target.counters().copy_fallbacks.get(), 1);
        assert!(flusher.status().conflicts.is_empty());
    });
}

#[test]
fn copies_follow_the_conflict_policy_and_races() {
    runtime().block_on(async {
        // Under `overwrite`, a copy that meets another writer's object is
        // sent again without a destination precondition, and still with
        // the source's.
        let node = Node::open(6).await;
        let store = remote(6, false);
        let target = target(&store, writes(true, true, true), ConflictPolicy::Overwrite);
        let flusher = node.flusher(&target);
        let (src, etag) = clean(&node, &flusher, "src", "the bytes").await;
        flusher.stop().await;
        let seq = copy(
            &node,
            "dst",
            "the bytes",
            own(),
            ("src", src),
            Some(etag.clone()),
        )
        .await;
        let mut foreign = UserMetadata::new();
        foreign.insert("writer", "someone-else").unwrap();
        store
            .put_object(PutObject::new("dst", "foreign").with_metadata(foreign))
            .await
            .unwrap();
        let flusher = node.flusher(&target);
        node.settle(&flusher).await;
        assert_copied(&node, &store, "dst", "the bytes", seq).await;
        assert_eq!(applied(&store, Operation::CopyObject), ["dst"]);
        assert_eq!(target.counters().overwritten.get(), 1);

        // Under `overwrite` too, a changed source makes it an upload.
        flusher.stop().await;
        let again = copy(&node, "dst", "the bytes", own(), ("src", src), Some(etag)).await;
        store
            .put_object(PutObject::new("src", "changed"))
            .await
            .unwrap();
        store
            .put_object(PutObject::new("dst", "foreign again"))
            .await
            .unwrap();
        let flusher = node.flusher(&target);
        node.settle(&flusher).await;
        assert_copied(&node, &store, "dst", "the bytes", again).await;
        assert_eq!(target.counters().copy_fallbacks.get(), 1);

        // Under `hold`, a foreign object at the destination holds the copy.
        let node = Node::open(7).await;
        let store = remote(7, false);
        let target = full(&store);
        let flusher = node.flusher(&target);
        let (src, etag) = clean(&node, &flusher, "src", "the bytes").await;
        flusher.stop().await;
        copy(
            &node,
            "dst",
            "the bytes",
            own(),
            ("src", src),
            Some(etag.clone()),
        )
        .await;
        store
            .put_object(PutObject::new("dst", "foreign"))
            .await
            .unwrap();
        let flusher = node.flusher(&target);
        node.settle(&flusher).await;
        let status = flusher.status();
        assert_eq!(status.conflicts.len(), 1);
        assert_eq!(store.object("dst").unwrap().body, "foreign");

        // A copy that races another write (409) is sent again, as a copy.
        let node = Node::open(8).await;
        let store = remote(8, false);
        let target = full(&store);
        let flusher = node.flusher(&target);
        let (src, etag) = clean(&node, &flusher, "src", "the bytes").await;
        store.inject(Operation::CopyObject, Fault::Conflict);
        let seq = copy(&node, "dst", "the bytes", own(), ("src", src), Some(etag)).await;
        node.settle(&flusher).await;
        assert_copied(&node, &store, "dst", "the bytes", seq).await;
        assert_eq!(applied(&store, Operation::CopyObject), ["dst"]);
        assert_eq!(target.counters().copy_fallbacks.get(), 0);
    });
}
