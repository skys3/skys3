//! Flushes to a SkyS3 peer over the native protocol (§7.8, plan M6-06),
//! against a destination in memory that applies them with the real
//! staging and commits: a key is recorded flushed only once the
//! destination's `APPLIED` says it holds the key's exact write identity,
//! across lost answers and dropped links, and write-through waits are
//! answered only then.
//!
//! These tests run in real time: the destination's index works on real
//! threads, which a paused clock would race.

mod support;

use std::time::Duration;

use skys3_flush::FlushSettings;
use skys3_index::EntryState;
use skys3_log::record::IDENTITY_METADATA;
use skys3_peer::{Message, Precondition, StagedRanges};
use skys3_shard::FlushState;
use support::peer::{Destination, committed, preconditions};
use support::{Node, Patience, at, identity, md5_etag, settings};

/// The `DATA` frame size of the tests: small, so objects of a few hundred
/// bytes are staged in several frames.
const FRAME: u64 = 64;

fn wid(seq: u64) -> skys3_types::WriteIdentity {
    identity(seq).parse().unwrap()
}

/// Waits until `done` holds, in real time.
async fn until(what: &str, mut done: impl FnMut() -> bool) {
    let patience = Patience::new();
    while !done() {
        assert!(!patience.is_exhausted(), "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn body(len: usize) -> String {
    (0..len)
        .map(|n| char::from(b'a' + u8::try_from(n % 26).unwrap()))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn small_objects_and_deletes_flush_in_batches_with_their_identities() {
    let destination = Destination::start(true).await;
    let node = Node::open(1).await;
    let mut seqs = Vec::new();
    for n in 0..6 {
        seqs.push(node.put(&format!("k{n}"), &format!("small {n}")).await);
    }
    let flusher = node.flusher(&destination.default_target(FRAME));
    node.settle(&flusher).await;
    for (n, seq) in seqs.iter().enumerate() {
        let key = format!("k{n}");
        let entry = destination.entry(&key).await.unwrap();
        let object = entry.object.unwrap();
        assert_eq!(object.metadata[IDENTITY_METADATA], identity(*seq));
        assert_eq!(object.local_etag, md5_etag(format!("small {n}").as_bytes()));
        assert_eq!(object.metadata["x-amz-meta-owner"], "team-a");
        assert_eq!(object.tags["kind"], "test");
        assert_eq!(
            destination.bytes(&key).await.unwrap(),
            format!("small {n}").as_bytes()
        );
        // The source recorded the identity the destination's version
        // carries, which its next version is conditioned on.
        let local = node.entry(&key).await.unwrap();
        assert_eq!(local.state, EntryState::Clean);
        assert_eq!(local.remote_version_id, Some(identity(*seq)));
    }
    {
        let seen = destination.seen();
        assert!(seen.count(|m| matches!(m, Message::Batch(_))) >= 1);
        assert_eq!(seen.count(|m| matches!(m, Message::Begin(_))), 0);
    }

    let deleted = node.delete("k1").await;
    node.settle(&flusher).await;
    let entry = destination.entry("k1").await;
    assert!(entry.and_then(|entry| entry.object).is_none());
    assert!(node.entry("k1").await.is_none(), "the tombstone is gone");
    let found = preconditions(&destination.seen());
    assert!(found.contains(&(identity(deleted), Precondition::Matches(wid(seqs[1])))));
    flusher.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrites_are_conditioned_on_the_destinations_identity_across_restarts() {
    let destination = Destination::start(true).await;
    let node = Node::open(2).await;
    let target = destination.default_target(FRAME);
    let first = node.put("k", "first").await;
    let flusher = node.flusher(&target);
    node.settle(&flusher).await;
    flusher.stop().await;

    // A new flusher knows only what the entry recorded.
    let second = node.put("k", "second").await;
    let flusher = node.flusher(&target);
    node.settle(&flusher).await;
    let third = node.put("k", &body(300)).await;
    node.settle(&flusher).await;
    flusher.stop().await;

    let found = preconditions(&destination.seen());
    assert_eq!(
        found,
        [
            (identity(first), Precondition::Absent),
            (identity(second), Precondition::Matches(wid(first))),
            (identity(third), Precondition::Matches(wid(second))),
        ]
    );
    assert_eq!(destination.bytes("k").await.unwrap(), body(300).as_bytes());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_link_resends_only_what_the_destination_lacks() {
    let destination = Destination::start(true).await;
    let node = Node::open(3).await;
    let text = body(1000);
    let seq = node.put("big", &text).await;
    destination.faults().cut_after_data = Some(5);
    let flusher = node.flusher(&destination.default_target(FRAME));
    node.settle(&flusher).await;
    flusher.stop().await;
    assert_eq!(destination.bytes("big").await.unwrap(), text.as_bytes());

    let seen = destination.seen();
    let begins = seen.count(|m| matches!(m, Message::Begin(_)));
    assert!(begins >= 2, "the stream was cut and begun again");
    // After the last `BEGIN`, exactly what its `RESUME` lacked was sent.
    let resume = seen
        .sent
        .iter()
        .rev()
        .find_map(|m| match m {
            Message::Resume(StagedRanges {
                identity: i,
                pieces,
            }) if *i == wid(seq) => Some(pieces.get(&0).cloned().unwrap_or_default()),
            _ => None,
        })
        .unwrap();
    let last_begin = seen
        .received
        .iter()
        .rposition(|m| matches!(m, Message::Begin(_)))
        .unwrap();
    let mut resent: Vec<(u64, u64)> = seen.received[last_begin..]
        .iter()
        .filter_map(|m| match m {
            Message::Data(data) => Some((data.offset, data.offset + data.bytes.len() as u64)),
            _ => None,
        })
        .collect();
    resent.sort_unstable();
    let mut covered = resume.clone();
    for (start, end) in &resent {
        assert!(
            resume
                .missing(1000)
                .iter()
                .any(|gap| gap.start <= *start && *end <= gap.end),
            "{start}..{end} was durable already: {resume:?}"
        );
        covered.insert(*start..*end);
    }
    assert!(covered.covers(1000));
    assert!(!resume.is_empty(), "the first stream staged something");
    assert_eq!(committed(&seen), [identity(seq)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_key_stays_dirty_until_applied_and_a_replay_writes_nothing() {
    let destination = Destination::start(true).await;
    let node = Node::open(4).await;
    destination.faults().lose_applied = 1;
    let seq = node.put("k", "value").await;
    let settings = FlushSettings {
        min_backoff: Duration::from_millis(400),
        ..settings()
    };
    let flusher = node.flusher(&destination.target(settings, FRAME));
    until("the lost APPLIED", || {
        committed(&destination.seen()).len() == 1
    })
    .await;
    let version = destination.entry("k").await.unwrap().version;
    // The destination applied it, but the answer was lost: still dirty.
    assert_eq!(node.entry("k").await.unwrap().state, EntryState::Dirty);
    assert_eq!(flusher.status().dirty, 1);

    node.settle(&flusher).await;
    flusher.stop().await;
    assert_eq!(
        committed(&destination.seen()),
        [identity(seq), identity(seq)]
    );
    // The replay was answered from the version, which nothing replaced.
    assert_eq!(destination.entry("k").await.unwrap().version, version);
    assert_eq!(node.entry("k").await.unwrap().state, EntryState::Clean);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_through_waits_are_answered_only_after_applied() {
    let destination = Destination::start(true).await;
    let node = Node::open(5).await;
    destination.faults().lose_applied = 1;
    let settings = FlushSettings {
        min_backoff: Duration::from_millis(400),
        ..settings()
    };
    let flusher = node.flusher(&destination.target(settings, FRAME));
    let seq = node.put("k", "value").await;
    let mut wait = node.shard.await_flush("k", at(seq)).unwrap();
    until("the lost APPLIED", || {
        committed(&destination.seen()).len() == 1
    })
    .await;
    let early = tokio::time::timeout(Duration::from_millis(100), wait.answered()).await;
    assert!(
        early.is_err(),
        "answered before an APPLIED arrived: {early:?}"
    );
    let answer = tokio::time::timeout(Duration::from_secs(10), wait.answered()).await;
    assert_eq!(answer.unwrap(), Some(FlushState::Flushed));
    assert_eq!(committed(&destination.seen()).len(), 2);
    flusher.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_foreign_version_is_a_conflict_that_overwrite_resolves() {
    let destination = Destination::start(true).await;
    let node = Node::open(6).await;
    // The destination's own clients wrote the key (`peer_local_writes`).
    destination
        .shards
        .put(
            &destination.bucket,
            "k",
            skys3_gateway::stub::EntryState::Dirty,
        )
        .await
        .unwrap();
    let mut wait_on = None;
    let flusher = node.flusher(&destination.default_target(FRAME));
    let seq = node.put("k", "mine").await;
    if let Ok(wait) = node.shard.await_flush("k", at(seq)) {
        wait_on = Some(wait);
    }
    let patience = Patience::new();
    let conflict = loop {
        if let Some(conflict) = flusher.status().conflicts.pop() {
            break conflict;
        }
        assert!(!patience.is_exhausted(), "no conflict");
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    assert_eq!(conflict.key, "k");
    let foreign = conflict.remote_identity.unwrap();
    assert!(foreign.starts_with("dest/b-archive/0/"), "{foreign}");
    if let Some(mut wait) = wait_on {
        let answer = tokio::time::timeout(Duration::from_secs(10), wait.answered()).await;
        assert_eq!(answer.unwrap(), Some(FlushState::Conflict));
    }
    assert_eq!(destination.bytes("k").await.unwrap().len(), 0);

    flusher
        .resolve("k", skys3_config::ConflictPolicy::Overwrite)
        .await
        .unwrap();
    node.settle(&flusher).await;
    flusher.stop().await;
    assert_eq!(destination.bytes("k").await.unwrap(), "mine".as_bytes());
    let found = preconditions(&destination.seen());
    assert_eq!(
        found.last().unwrap(),
        &(identity(seq), Precondition::Unconditional)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_streamed_body_is_staged_before_its_put_commits() {
    let destination = Destination::start(true).await;
    let node = Node::open(7).await;
    let settings = FlushSettings {
        streaming: true,
        ..settings()
    };
    let flusher = node.flusher(&destination.target(settings, FRAME));
    let text = body(600);
    let begun = node.begin("big").await;
    let extents = node.extents("big", &text, 200).await;
    node.announce("big", begun, &extents[..2]);
    until("two extents staged", || {
        destination
            .staged(&identity(begun))
            .is_some_and(|staged| staged.extents(0, 400).is_some())
    })
    .await;
    node.announce("big", begun, &extents[2..]);
    until("the body staged", || {
        destination
            .staged(&identity(begun))
            .is_some_and(|staged| staged.extents(0, 600).is_some())
    })
    .await;
    let frames = destination.seen().count(|m| matches!(m, Message::Data(_)));
    assert_eq!(frames, 12, "each extent once, in frames of {FRAME}");

    let seq = node.complete_extents("big", &text, begun, &extents).await;
    node.settle(&flusher).await;
    flusher.stop().await;
    {
        let seen = destination.seen();
        assert_eq!(seen.count(|m| matches!(m, Message::Data(_))), frames);
        assert_eq!(committed(&seen), [identity(begun)]);
    }
    assert_eq!(destination.bytes("big").await.unwrap(), text.as_bytes());
    let entry = node.entry("big").await.unwrap();
    assert_eq!(entry.version, at(seq));
    assert_eq!(entry.remote_version_id, Some(identity(begun)));
    assert_eq!(entry.remote_etag, Some(md5_etag(text.as_bytes())));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_body_whose_put_never_commits_is_aborted() {
    let destination = Destination::start(true).await;
    let node = Node::open(8).await;
    let settings = FlushSettings {
        streaming: true,
        body_timeout: Duration::from_millis(300),
        ..settings()
    };
    let flusher = node.flusher(&destination.target(settings, FRAME));
    let begun = node.begin("big").await;
    let extents = node.extents("big", &body(300), 100).await;
    node.announce("big", begun, &extents);
    until("the body's ABORT", || {
        destination
            .seen()
            .count(|m| matches!(m, Message::Abort(abort) if abort.identity == wid(begun)))
            == 1
    })
    .await;
    until("the staging discarded", || {
        destination.staged(&identity(begun)).is_none()
    })
    .await;
    assert_eq!(flusher.status().streams, 0);
    flusher.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_batches_small_objects_and_deletes_go_alone() {
    let destination = Destination::start(false).await;
    let node = Node::open(9).await;
    node.put("a", "small").await;
    node.put("b", "small").await;
    let flusher = node.flusher(&destination.default_target(FRAME));
    node.settle(&flusher).await;
    node.delete("a").await;
    node.settle(&flusher).await;
    flusher.stop().await;
    {
        let seen = destination.seen();
        assert_eq!(seen.count(|m| matches!(m, Message::Batch(_))), 0);
        assert_eq!(seen.count(|m| matches!(m, Message::Begin(_))), 2);
        assert_eq!(seen.count(|m| matches!(m, Message::Commit(_))), 3);
    }
    assert_eq!(destination.bytes("b").await.unwrap(), "small".as_bytes());
    assert!(
        destination
            .entry("a")
            .await
            .and_then(|e| e.object)
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_multipart_object_keeps_its_etag_as_one_piece() {
    let destination = Destination::start(true).await;
    let node = Node::open(10).await;
    let parts = [body(150), body(40)];
    let multipart = node
        .multipart("mp", &[parts[0].as_str(), parts[1].as_str()])
        .await;
    let flusher = node.flusher(&destination.default_target(FRAME));
    node.settle(&flusher).await;
    flusher.stop().await;
    let object = destination.entry("mp").await.unwrap().object.unwrap();
    assert_eq!(object.local_etag, multipart.etag);
    assert_eq!(object.metadata["x-amz-meta-owner"], "team-b");
    assert_eq!(
        object.metadata[IDENTITY_METADATA],
        identity(multipart.upload)
    );
    let joined = format!("{}{}", parts[0], parts[1]);
    assert_eq!(destination.bytes("mp").await.unwrap(), joined.as_bytes());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unreachable_destination_keeps_keys_dirty() {
    let destination = Destination::start(true).await;
    let node = Node::open(11).await;
    destination.faults().down = true;
    let seq = node.put("k", "value").await;
    let flusher = node.flusher(&destination.default_target(FRAME));
    let patience = Patience::new();
    while flusher.status().last_error.is_none() {
        assert!(!patience.is_exhausted());
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let error = flusher.status().last_error.unwrap();
    assert!(error.contains("unreachable"), "{error}");
    assert_eq!(node.entry("k").await.unwrap().state, EntryState::Dirty);
    destination.faults().down = false;
    node.settle(&flusher).await;
    flusher.stop().await;
    assert_eq!(committed(&destination.seen()), [identity(seq)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delete_of_a_key_the_destination_never_had_settles() {
    let destination = Destination::start(true).await;
    let node = Node::open(12).await;
    node.put("k", "value").await;
    let seq = node.delete("k").await;
    let flusher = node.flusher(&destination.default_target(FRAME));
    node.settle(&flusher).await;
    flusher.stop().await;
    assert!(node.entry("k").await.is_none());
    assert!(destination.entry("k").await.is_none());
    assert_eq!(committed(&destination.seen()), [identity(seq)]);
}
