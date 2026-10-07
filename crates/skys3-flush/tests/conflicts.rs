//! Tests of the conflict policies (§7.2) against the simulated store,
//! which takes out-of-band writes: `hold`, `overwrite`, and
//! `discard_local`, each on protected and unprotected operations, an
//! operator's resolution of a held conflict, which returns the key to
//! dirty (§4.2 Conflict → Dirty), and write-through waits.

mod support;

use std::sync::Arc;
use std::time::Duration;

use skys3_config::ConflictPolicy;
use skys3_flush::{Phase, ShardFlusher, Target, Unresolved};
use skys3_index::{EntryState, Payload};
use skys3_remote::probe::ConditionalWrites;
use skys3_remote::{ObjectStore, PutObject, UserMetadata};
use skys3_shard::FlushState;
use skys3_sim::SimS3;
use skys3_sim::s3::{ConditionalSupport, Conditionals, SimS3Config, SimS3Faults};
use support::{Node, at, cluster, identity, md5_etag, runtime, settings, writes};

/// A target on `store` honoring `writes` that applies `policy`.
fn target(store: &SimS3, writes: ConditionalWrites, policy: ConflictPolicy) -> Arc<Target<SimS3>> {
    let target = Target::new(Arc::new(store.clone()), "", writes, cluster(), settings());
    Arc::new(target.with_conflict_policy(policy))
}

/// Every precondition honored.
fn protected() -> ConditionalWrites {
    writes(true, true, true)
}

/// No precondition honored: every write is sent unconditionally.
fn unprotected() -> ConditionalWrites {
    writes(false, false, false)
}

/// A store that behaves like AWS S3, or, if `honors` is false, one that
/// ignores every precondition, as the probe would have found.
fn store(seed: u64, honors: bool) -> SimS3 {
    let support = if honors {
        ConditionalSupport::Honored
    } else {
        ConditionalSupport::Ignored
    };
    SimS3::new(
        seed,
        SimS3Config {
            conditionals: Conditionals::all(support),
            min_part_size: 1,
            ..SimS3Config::default()
        },
    )
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

/// The remote object at `key`: its body and write identity.
fn remote_object(store: &SimS3, key: &str) -> Option<(String, Option<String>)> {
    store.object(key).map(|object| {
        let body = String::from_utf8(object.body.to_vec()).unwrap();
        let wid = object.info.metadata.write_identity().map(str::to_owned);
        (body, wid)
    })
}

/// The remote object at `key`, as a pair of its body and the identity of
/// the write at `seq`.
fn ours(body: &str, seq: u64) -> Option<(String, Option<String>)> {
    Some((body.to_owned(), Some(identity(seq))))
}

/// The remote object at `key` written out of band with `body`.
fn theirs(body: &str) -> Option<(String, Option<String>)> {
    Some((body.to_owned(), None))
}

/// Puts `key` in conflict on a fresh node: a local write of a key that
/// another writer wrote at the remote first. Returns the local write's
/// `seq`.
async fn conflicting_put(node: &Node, store: &SimS3, key: &str) -> u64 {
    out_of_band(store, key, &format!("{key} theirs")).await;
    node.put(key, &format!("{key} ours")).await
}

/// Waits until the flusher holds `key` in conflict.
async fn held(flusher: &ShardFlusher, key: &str) {
    let patience = support::Patience::new();
    while !matches!(flusher.phase(key), Some(Phase::Conflict(_))) {
        assert!(!patience.is_exhausted(), "{key} is not held");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[test]
fn hold_keeps_out_of_band_writes_until_an_operator_resolves_them() {
    runtime().block_on(async {
        let node = Node::open(1).await;
        let store = store(1, true);
        let flusher = node.flusher(&target(&store, protected(), ConflictPolicy::Hold));
        let seq = conflicting_put(&node, &store, "k").await;
        held(&flusher, "k").await;
        node.settle(&flusher).await;
        assert_eq!(remote_object(&store, "k"), theirs("k theirs"));
        let status = flusher.status();
        assert_eq!(status.conflicts.len(), 1);
        assert_eq!(status.conflicts[0].seq.get(), seq);
        assert_eq!(status.conflicts[0].remote_etag, Some(md5_etag(b"k theirs")));
        assert_eq!(node.entry("k").await.unwrap().state, EntryState::Dirty);
        // Only a held key can be resolved.
        assert_eq!(
            flusher.resolve("other", ConflictPolicy::Overwrite).await,
            Err(Unresolved::NotHeld)
        );
    });
}

#[test]
fn overwrite_replaces_out_of_band_writes_on_protected_operations() {
    runtime().block_on(async {
        let node = Node::open(2).await;
        let store = store(2, true);
        let target = target(&store, protected(), ConflictPolicy::Overwrite);
        let flusher = node.flusher(&target);
        // A key SkyS3 believes absent, a key it flushed, a delete, and a
        // multipart object, each written out of band first.
        let new = conflicting_put(&node, &store, "new").await;
        node.put("old", "old v1").await;
        node.settle(&flusher).await;
        out_of_band(&store, "old", "old theirs").await;
        let old = node.put("old", "old v2").await;
        node.put("gone", "gone v1").await;
        node.settle(&flusher).await;
        out_of_band(&store, "gone", "gone theirs").await;
        node.delete("gone").await;
        out_of_band(&store, "parts", "parts theirs").await;
        let parts = node.multipart("parts", &["first part; ", "second"]).await;
        node.settle(&flusher).await;

        assert_eq!(remote_object(&store, "new"), ours("new ours", new));
        assert_eq!(remote_object(&store, "old"), ours("old v2", old));
        assert_eq!(remote_object(&store, "gone"), None);
        assert_eq!(
            remote_object(&store, "parts"),
            ours("first part; second", parts.upload)
        );
        assert_eq!(store.object("parts").unwrap().info.etag, parts.etag);
        assert!(node.unclean().await.is_empty());
        assert!(flusher.status().conflicts.is_empty());
        let counters = target.counters();
        assert_eq!(counters.conflicts.get(), 4);
        assert_eq!(counters.overwritten.get(), 4);
        assert_eq!(counters.discarded.get(), 0);

        // The keys are owned again: the next flushes are conditional and
        // find no conflict.
        let again = node.put("old", "old v3").await;
        node.settle(&flusher).await;
        assert_eq!(remote_object(&store, "old"), ours("old v3", again));
        assert_eq!(counters.conflicts.get(), 4);
    });
}

#[test]
fn discard_local_adopts_out_of_band_writes_on_protected_operations() {
    runtime().block_on(async {
        let node = Node::open(3).await;
        let store = store(3, true);
        let target = target(&store, protected(), ConflictPolicy::DiscardLocal);
        let flusher = node.flusher(&target);
        let seq = conflicting_put(&node, &store, "k").await;
        node.put("gone", "gone v1").await;
        node.settle(&flusher).await;
        out_of_band(&store, "gone", "gone theirs").await;
        let deleted = node.delete("gone").await;
        node.settle(&flusher).await;

        // The remote keeps the other writer's objects, and the local
        // versions are dropped for them: Conflict → Dirty, then an `ADOPT`
        // makes each an evicted stub of the remote's write.
        for (key, body, dropped) in [("k", "k theirs", seq), ("gone", "gone theirs", deleted)] {
            assert_eq!(remote_object(&store, key), theirs(body));
            let entry = node.entry(key).await.unwrap();
            assert_eq!(entry.state, EntryState::Evicted, "{key}");
            assert!(entry.version.seq.get() > dropped, "{key}");
            assert_eq!(entry.remote_etag, Some(md5_etag(body.as_bytes())));
            let object = entry.object.unwrap();
            assert_eq!(object.local_etag, md5_etag(body.as_bytes()));
            assert_eq!(object.payload, Payload::None);
            assert_eq!(object.write_identity, None);
            assert_eq!(
                object.metadata.get("x-amz-meta-writer").map(String::as_str),
                Some("someone-else")
            );
        }
        assert!(flusher.status().conflicts.is_empty());
        assert_eq!(target.counters().discarded.get(), 2);
        assert_eq!(target.counters().overwritten.get(), 0);

        // A write after the discard builds on the adopted object: its
        // flush replaces it with `If-Match`, with no conflict.
        let later = node.put("k", "k later").await;
        node.settle(&flusher).await;
        assert_eq!(remote_object(&store, "k"), ours("k later", later));
        assert_eq!(target.counters().conflicts.get(), 2);
    });
}

#[test]
fn discard_local_drops_versions_committed_while_the_conflict_was_found() {
    runtime().block_on(async {
        let node = Node::open(4).await;
        let store = store(4, true);
        // Every request takes a while, so that a write commits while the
        // conflict is found and resolved.
        store.set_faults(SimS3Faults {
            min_delay: Duration::from_millis(20),
            max_delay: Duration::from_millis(20),
            ..SimS3Faults::NONE
        });
        let target = target(&store, protected(), ConflictPolicy::DiscardLocal);
        let flusher = node.flusher(&target);
        conflicting_put(&node, &store, "k").await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        node.put("k", "k meanwhile").await;
        node.settle(&flusher).await;
        assert_eq!(remote_object(&store, "k"), theirs("k theirs"));
        let entry = node.entry("k").await.unwrap();
        assert_eq!(entry.state, EntryState::Evicted);
        assert_eq!(entry.object.unwrap().local_etag, md5_etag(b"k theirs"));
    });
}

#[test]
fn a_discard_finds_the_out_of_band_write_gone_and_flushes() {
    runtime().block_on(async {
        let node = Node::open(5).await;
        let store = store(5, true);
        let flusher = node.flusher(&target(&store, protected(), ConflictPolicy::Hold));
        let seq = conflicting_put(&node, &store, "k").await;
        held(&flusher, "k").await;
        // The other writer's object is gone by the time the operator
        // resolves: there is nothing to adopt, and nothing to overwrite.
        store
            .delete_object(skys3_remote::DeleteObject::new("k"))
            .await
            .unwrap();
        flusher
            .resolve("k", ConflictPolicy::DiscardLocal)
            .await
            .unwrap();
        node.settle(&flusher).await;
        assert_eq!(remote_object(&store, "k"), ours("k ours", seq));
        assert_eq!(node.entry("k").await.unwrap().state, EntryState::Clean);
    });
}

#[test]
fn resolving_a_held_conflict_returns_the_key_to_dirty_under_each_policy() {
    runtime().block_on(async {
        let node = Node::open(6).await;
        let store = store(6, true);
        let target = target(&store, protected(), ConflictPolicy::Hold);
        let flusher = node.flusher(&target);
        let mut seqs = Vec::new();
        for key in ["a", "b", "c"] {
            seqs.push(conflicting_put(&node, &store, key).await);
            held(&flusher, key).await;
        }
        node.settle(&flusher).await;
        assert_eq!(flusher.status().conflicts.len(), 3);

        // Conflict → Dirty: the key is back in line, no longer held, or
        // flushed already.
        flusher
            .resolve("a", ConflictPolicy::Overwrite)
            .await
            .unwrap();
        assert!(!matches!(flusher.phase("a"), Some(Phase::Conflict(_))));
        flusher
            .resolve("b", ConflictPolicy::DiscardLocal)
            .await
            .unwrap();
        // `hold` retries the conditional flush, which finds the other
        // writer's object again.
        flusher.resolve("c", ConflictPolicy::Hold).await.unwrap();
        node.settle(&flusher).await;

        assert_eq!(remote_object(&store, "a"), ours("a ours", seqs[0]));
        assert_eq!(node.entry("a").await.unwrap().state, EntryState::Clean);
        assert_eq!(remote_object(&store, "b"), theirs("b theirs"));
        assert_eq!(node.entry("b").await.unwrap().state, EntryState::Evicted);
        assert!(matches!(flusher.phase("c"), Some(Phase::Conflict(_))));
        assert_eq!(remote_object(&store, "c"), theirs("c theirs"));
        assert_eq!(target.counters().conflicts.get(), 4);
        assert_eq!(target.counters().overwritten.get(), 1);
        assert_eq!(target.counters().discarded.get(), 1);
        // A resolved key is held no more.
        assert_eq!(
            flusher.resolve("a", ConflictPolicy::Overwrite).await,
            Err(Unresolved::NotHeld)
        );
    });
}

#[test]
fn a_resolution_lasts_only_as_long_as_its_flusher() {
    runtime().block_on(async {
        let node = Node::open(7).await;
        let store = store(7, true);
        let flusher = node.flusher(&target(&store, protected(), ConflictPolicy::Hold));
        conflicting_put(&node, &store, "k").await;
        held(&flusher, "k").await;
        // The remote is down, so the resolution cannot be carried out
        // before the flusher stops, as on a primary change.
        store.set_faults(SimS3Faults::OUTAGE);
        flusher
            .resolve("k", ConflictPolicy::Overwrite)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        flusher.stop().await;
        store.set_faults(SimS3Faults::NONE);

        // The next flusher finds the conflict again and holds it.
        let flusher = node.flusher(&target(&store, protected(), ConflictPolicy::Hold));
        held(&flusher, "k").await;
        assert_eq!(remote_object(&store, "k"), theirs("k theirs"));
        assert_eq!(
            flusher.resolve("k", ConflictPolicy::Overwrite).await,
            Ok(())
        );
        node.settle(&flusher).await;
        assert_eq!(remote_object(&store, "k").unwrap().0, "k ours");
        assert_eq!(
            flusher.resolve("k", ConflictPolicy::Overwrite).await,
            Err(Unresolved::NotHeld)
        );
    });
}

/// On a target that honors no precondition, a write is sent
/// unconditionally and replaces an out-of-band write silently under every
/// policy: no conflict is found (§7.2). A delete whose remote ETag is not
/// known HEADs the key first, though, and finds the other writer's object:
/// that conflict is decided by the policy.
#[test]
fn unprotected_operations_overwrite_silently_under_every_policy() {
    for (seed, policy) in [
        (10, ConflictPolicy::Hold),
        (11, ConflictPolicy::Overwrite),
        (12, ConflictPolicy::DiscardLocal),
    ] {
        runtime().block_on(async {
            let node = Node::open(seed).await;
            let store = store(seed, false);
            let target = target(&store, unprotected(), policy);
            let flusher = node.flusher(&target);
            node.put("k", "k v1").await;
            node.settle(&flusher).await;
            out_of_band(&store, "k", "k theirs").await;
            let seq = node.put("k", "k v2").await;
            let created = conflicting_put(&node, &store, "new").await;
            node.settle(&flusher).await;
            assert_eq!(remote_object(&store, "k"), ours("k v2", seq), "{policy:?}");
            assert_eq!(
                remote_object(&store, "new"),
                ours("new ours", created),
                "{policy:?}"
            );
            assert_eq!(target.counters().conflicts.get(), 0, "{policy:?}");

            // A delete written locally before anything was flushed: no
            // remote ETag is known, so a HEAD finds the foreign object.
            out_of_band(&store, "gone", "gone theirs").await;
            node.delete("gone").await;
            node.settle(&flusher).await;
            assert_eq!(target.counters().conflicts.get(), 1, "{policy:?}");
            let remote = remote_object(&store, "gone");
            let entry = node.entry("gone").await;
            match policy {
                ConflictPolicy::Hold => {
                    assert!(matches!(flusher.phase("gone"), Some(Phase::Conflict(_))));
                    assert_eq!(remote, theirs("gone theirs"));
                }
                ConflictPolicy::Overwrite => {
                    assert_eq!(remote, None);
                    assert!(entry.is_none());
                }
                ConflictPolicy::DiscardLocal => {
                    assert_eq!(remote, theirs("gone theirs"));
                    assert_eq!(entry.unwrap().state, EntryState::Evicted);
                }
            }
        });
    }
}

#[test]
fn write_through_waits_follow_the_policy() {
    for (seed, policy, answer) in [
        (20, ConflictPolicy::Hold, FlushState::Conflict),
        (21, ConflictPolicy::Overwrite, FlushState::Flushed),
        (22, ConflictPolicy::DiscardLocal, FlushState::Conflict),
    ] {
        runtime().block_on(async {
            let node = Node::open(seed).await;
            let store = store(seed, true);
            let flusher = node.flusher(&target(&store, protected(), policy));
            let seq = conflicting_put(&node, &store, "k").await;
            let mut wait = node.shard.await_flush("k", at(seq)).unwrap();
            assert_eq!(wait.answered().await, Some(answer), "{policy:?}");
            node.settle(&flusher).await;
        });
    }
}

/// A write that waits for a version of a held key learns of the conflict
/// at once; once an operator resolves it with `overwrite`, a wait for the
/// version is answered when the remote holds it.
#[test]
fn a_resolution_answers_write_through_waits() {
    runtime().block_on(async {
        let node = Node::open(23).await;
        let store = store(23, true);
        let flusher = node.flusher(&target(&store, protected(), ConflictPolicy::Hold));
        conflicting_put(&node, &store, "k").await;
        held(&flusher, "k").await;
        let newer = node.put("k", "k newer").await;
        let mut wait = node.shard.await_flush("k", at(newer)).unwrap();
        assert_eq!(wait.answered().await, Some(FlushState::Conflict));
        flusher
            .resolve("k", ConflictPolicy::Overwrite)
            .await
            .unwrap();
        let mut wait = node.shard.await_flush("k", at(newer)).unwrap();
        assert_eq!(wait.answered().await, Some(FlushState::Flushed));
        assert_eq!(remote_object(&store, "k"), ours("k newer", newer));
    });
}
