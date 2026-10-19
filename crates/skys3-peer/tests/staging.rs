//! Staging over loopback QUIC: a destination that relays frames to a sink
//! standing in for the shard primaries, answers `BEGIN` with `RESUME`,
//! reports durable ranges, and resumes after a reconnect.

// The tests spell out sets of one range on purpose.
#![allow(clippy::single_range_in_vec_init)]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use common::*;
use skys3_log::record::ExtentRef;
use skys3_peer::{
    AbortReason, Applied, ApplyError, Batch, BatchBuilder, Begin, ByteRanges, Commit, CommitSink,
    Data, ExtentSink, MAX_REASON_LEN, Message, MessageStream, NoCommits, Outcome, PeerEndpoint,
    PeerTrust, Precondition, Put, PutData, SinkError, StagedObject, Staging, StagingLimits,
    StagingService, Write, send_batch,
};
use skys3_types::{BucketName, ETag, Epoch, EpochSeq, Seq, WriteIdentity};

const US: &str = "prod-us";
const EU: &str = "prod-eu";
const SOURCE_BUCKET: &str = "b-src";
const DESTINATION_BUCKET: &str = "archive";
const FRAME: u64 = 64 << 10;

/// A key whose appends the sink refuses, as for a bucket being deleted.
const REFUSED_KEY: &str = "refused";

fn identity(seq: u64) -> WriteIdentity {
    format!("{US}/{SOURCE_BUCKET}/5/42.{seq}").parse().unwrap()
}

fn begin(identity: &WriteIdentity, key: &str) -> Message {
    Message::Begin(Begin {
        identity: identity.clone(),
        bucket: BucketName::new(DESTINATION_BUCKET).unwrap(),
        key: key.to_owned(),
    })
}

/// The bytes of a piece, different at every offset of a frame.
fn body(len: u64) -> Bytes {
    (0..len).map(|at| (at % 253) as u8).collect()
}

fn frame(body: &Bytes, piece: u64, range: Range<u64>) -> Message {
    Message::Data(Data {
        piece,
        offset: range.start,
        bytes: body.slice(range.start as usize..range.end as usize),
    })
}

/// An append the sink took.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Appended {
    key: String,
    offset: u64,
    bytes: Bytes,
    position: EpochSeq,
}

/// Stands in for the shard primaries: stores each extent at the next
/// position, fails the first append at each offset in `fail_once`, and
/// refuses every append of [`REFUSED_KEY`].
#[derive(Debug, Clone, Default)]
struct TestSink {
    state: Arc<Mutex<SinkState>>,
}

#[derive(Debug, Default)]
struct SinkState {
    fail_once: BTreeSet<u64>,
    appended: Vec<Appended>,
    attempts: usize,
}

impl TestSink {
    fn failing_once(offsets: impl IntoIterator<Item = u64>) -> Self {
        let sink = Self::default();
        sink.state.lock().unwrap().fail_once = offsets.into_iter().collect();
        sink
    }

    fn appended(&self) -> Vec<Appended> {
        self.state.lock().unwrap().appended.clone()
    }

    fn attempts(&self) -> usize {
        self.state.lock().unwrap().attempts
    }
}

impl ExtentSink for TestSink {
    async fn append(
        &self,
        bucket: &BucketName,
        key: &str,
        offset: u64,
        data: Bytes,
    ) -> Result<ExtentRef, SinkError> {
        assert_eq!(bucket.as_str(), DESTINATION_BUCKET);
        // Let other appends and messages interleave.
        tokio::task::yield_now().await;
        let mut state = self.state.lock().unwrap();
        state.attempts += 1;
        if key == REFUSED_KEY {
            return Err(SinkError::Refused("the bucket is being deleted".into()));
        }
        if state.fail_once.remove(&offset) {
            return Err(SinkError::Unavailable("not acknowledged in time".into()));
        }
        let position = EpochSeq::new(Epoch::new(1), Seq::new(state.appended.len() as u64 + 1));
        let len = u32::try_from(data.len()).unwrap();
        state.appended.push(Appended {
            key: key.to_owned(),
            offset,
            bytes: data,
            position,
        });
        Ok(ExtentRef { position, len })
    }
}

/// A source in `prod-us` and a destination in `prod-eu` that serves
/// staging with `sink`.
struct Setup {
    source: PeerEndpoint,
    destination: PeerEndpoint,
    staging: Arc<Staging>,
}

fn setup(sink: TestSink, limits: StagingLimits) -> Setup {
    setup_with(sink, limits, NoCommits)
}

/// [`setup`], applying `COMMIT`s through `commits`.
fn setup_with(sink: TestSink, limits: StagingLimits, commits: impl CommitSink) -> Setup {
    let us = Cluster::new(US);
    let eu = Cluster::new(EU);
    let mut destination_trust = PeerTrust::new();
    us.trusted(
        &mut destination_trust,
        &[pair(SOURCE_BUCKET, DESTINATION_BUCKET)],
    );
    let mut source_trust = PeerTrust::new();
    eu.trusted(&mut source_trust, &[]);
    let destination = eu.endpoint("eu-1", destination_trust);
    let staging = Arc::new(Staging::new(limits));
    let service = StagingService::new(Arc::clone(&staging), sink).with_commits(commits);
    assert!(format!("{service:?}").contains("StagingService"));
    let accepting = destination.clone();
    tokio::spawn(async move {
        while let Some(incoming) = accepting.accept().await {
            let service = service.clone();
            tokio::spawn(async move {
                if let Ok(connection) = incoming.establish().await {
                    service.serve_connection(connection).await;
                }
            });
        }
    });
    Setup {
        source: us.endpoint("us-1", source_trust),
        destination,
        staging,
    }
}

const LIMITS: StagingLimits = StagingLimits {
    quota_bytes: 1 << 30,
    ttl: Duration::from_secs(3600),
};

impl Setup {
    /// A new connection and a stream on it.
    async fn stream(&self) -> (skys3_peer::PeerConnection, MessageStream) {
        let to = destination(EU, &self.destination);
        let connection = self.source.connect(&to).bounded().await.unwrap();
        let stream = connection.open_stream().bounded().await.unwrap();
        (connection, stream)
    }
}

/// Receives messages until a `DURABLE` reports `want` for `piece`, and
/// returns every message before it that is not a `DURABLE`.
async fn durable_until(stream: &mut MessageStream, piece: u64, want: &[Range<u64>]) {
    loop {
        match stream.recv().bounded().await.unwrap() {
            Some(Message::Durable(durable)) => {
                if durable
                    .pieces
                    .get(&piece)
                    .is_some_and(|ranges| ranges.as_slice() == want)
                {
                    return;
                }
            }
            other => panic!("expected DURABLE, got {other:?}"),
        }
    }
}

async fn resume(stream: &mut MessageStream) -> BTreeMap<u64, ByteRanges> {
    match stream.recv().bounded().await.unwrap() {
        Some(Message::Resume(resume)) => resume.pieces,
        other => panic!("expected RESUME, got {other:?}"),
    }
}

#[tokio::test]
async fn a_reconnect_resends_only_the_ranges_not_yet_durable() {
    // Frames 5 and 6 are not acknowledged the first time.
    let sink = TestSink::failing_once([5 * FRAME, 6 * FRAME]);
    let setup = setup(sink.clone(), LIMITS);
    let id = identity(1);
    let len = 8 * FRAME;
    let body = body(len);

    // The first connection streams every frame without waiting for any.
    let (first, mut stream) = setup.stream().await;
    stream
        .send(&begin(&id, "photos/cat.jpg"))
        .bounded()
        .await
        .unwrap();
    assert!(resume(&mut stream).await.is_empty());
    let (mut sender, mut receiver) = stream.split();
    let sending = tokio::spawn(async move {
        let body = self::body(len);
        for at in (0..len).step_by(FRAME as usize) {
            sender.send(&frame(&body, 1, at..at + FRAME)).await.unwrap();
        }
        sender
    });
    // DURABLEs arrive while the frames are sent.
    let durable = [0..5 * FRAME, 7 * FRAME..len];
    async {
        loop {
            match receiver.recv().await.unwrap() {
                Some(Message::Durable(report)) if report.pieces[&1].as_slice() == durable => break,
                Some(Message::Durable(_)) => {}
                other => panic!("expected DURABLE, got {other:?}"),
            }
        }
    }
    .bounded()
    .await;
    let _sender = sending.bounded().await.unwrap();
    assert_eq!(sink.attempts(), 8);
    // The link drops.
    first.close();

    // After the reconnect, the RESUME holds what is durable, and the
    // source resends only the rest.
    let (second, mut stream) = setup.stream().await;
    stream
        .send(&begin(&id, "photos/cat.jpg"))
        .bounded()
        .await
        .unwrap();
    let held = resume(&mut stream).await;
    assert_eq!(held[&1].as_slice(), durable);
    let missing = held[&1].missing(len);
    assert_eq!(missing, [5 * FRAME..7 * FRAME]);
    for gap in missing {
        for at in gap.step_by(FRAME as usize) {
            stream
                .send(&frame(&body, 1, at..at + FRAME))
                .bounded()
                .await
                .unwrap();
        }
    }
    durable_until(&mut stream, 1, &[0..len]).await;
    // Eight frames, two of them twice: none that was durable was sent
    // again.
    assert_eq!(sink.attempts(), 10);
    let appended = sink.appended();
    let offsets: BTreeSet<u64> = appended.iter().map(|a| a.offset).collect();
    assert_eq!(appended.len(), 8);
    assert_eq!(offsets.len(), 8);
    assert!(appended.iter().all(|a| a.key == "photos/cat.jpg"));

    // What a COMMIT will publish: the piece's extents, in order.
    let staged = setup.staging.staged(&id).unwrap();
    let extents = staged.extents(1, len).unwrap();
    let assembled: Vec<u8> = extents
        .iter()
        .flat_map(|extent| {
            let appended = appended.iter().find(|a| a.position == extent.position);
            appended.unwrap().bytes.to_vec()
        })
        .collect();
    assert_eq!(assembled, body.to_vec());

    // The destination finishes the stream once the source has.
    stream.finish().unwrap();
    assert_eq!(stream.recv().bounded().await.unwrap(), None);
    second.close();
}

#[tokio::test]
async fn staging_that_cannot_continue_is_aborted() {
    let sink = TestSink::default();
    let setup = setup(
        sink.clone(),
        StagingLimits {
            quota_bytes: 2 * FRAME,
            ttl: LIMITS.ttl,
        },
    );
    let body = body(4 * FRAME);
    let (connection, mut stream) = setup.stream().await;

    // A frame that takes the source past its quota discards the staging,
    // and the stream's later frames go nowhere.
    let id = identity(1);
    stream.send(&begin(&id, "k")).bounded().await.unwrap();
    assert!(resume(&mut stream).await.is_empty());
    stream
        .send(&frame(&body, 1, 0..FRAME))
        .bounded()
        .await
        .unwrap();
    durable_until(&mut stream, 1, &[0..FRAME]).await;
    stream
        .send(&frame(&body, 1, FRAME..3 * FRAME))
        .bounded()
        .await
        .unwrap();
    let Some(Message::Abort(abort)) = stream.recv().bounded().await.unwrap() else {
        panic!("expected ABORT");
    };
    assert_eq!(
        (abort.identity, abort.reason),
        (id.clone(), AbortReason::QuotaExceeded)
    );
    stream
        .send(&frame(&body, 1, 3 * FRAME..4 * FRAME))
        .bounded()
        .await
        .unwrap();
    assert!(setup.staging.is_empty());
    assert_eq!(setup.staging.used(&id.cluster), 0);

    // A BEGIN of a staged identity for another key is refused, and the
    // staging stays.
    let other = identity(2);
    stream.send(&begin(&other, "k")).bounded().await.unwrap();
    assert!(resume(&mut stream).await.is_empty());
    stream
        .send(&begin(&other, "elsewhere"))
        .bounded()
        .await
        .unwrap();
    let Some(Message::Abort(abort)) = stream.recv().bounded().await.unwrap() else {
        panic!("expected ABORT");
    };
    assert_eq!(abort.reason, AbortReason::Refused);
    assert!(setup.staging.staged(&other).is_some());
    // The source's ABORT discards it.
    stream
        .send(&Message::Abort(skys3_peer::Abort {
            identity: other.clone(),
            reason: AbortReason::Cancelled,
            detail: String::new(),
        }))
        .bounded()
        .await
        .unwrap();

    // A refused append discards the staging and aborts it.
    let refused = identity(3);
    stream
        .send(&begin(&refused, REFUSED_KEY))
        .bounded()
        .await
        .unwrap();
    assert!(resume(&mut stream).await.is_empty());
    stream
        .send(&frame(&body, 1, 0..FRAME))
        .bounded()
        .await
        .unwrap();
    let Some(Message::Abort(abort)) = stream.recv().bounded().await.unwrap() else {
        panic!("expected ABORT");
    };
    assert_eq!(
        (abort.identity, abort.reason),
        (refused.clone(), AbortReason::Refused)
    );
    assert!(abort.detail.contains("being deleted"), "{}", abort.detail);
    // Its later frames are dropped without an answer.
    stream
        .send(&frame(&body, 1, FRAME..2 * FRAME))
        .bounded()
        .await
        .unwrap();

    // Without a commit sink, COMMIT and BATCH are answered `unavailable`.
    let commit = Commit {
        identity: identity(4),
        bucket: BucketName::new(DESTINATION_BUCKET).unwrap(),
        key: "k".into(),
        precondition: Precondition::Absent,
        write: Write::Delete,
        apply_by_ms: Some(1_800_000_000_000),
    };
    stream
        .send(&Message::Commit(commit.clone()))
        .bounded()
        .await
        .unwrap();
    let inline = Commit {
        identity: identity(5),
        write: Write::Put(Put {
            size: 5,
            etag: ETag::new("5d41402abc4b2a76b9719d911017c592").unwrap(),
            last_modified_ms: 0,
            metadata: BTreeMap::new(),
            tags: BTreeMap::new(),
            checksums: BTreeMap::new(),
            data: PutData::Inline(Bytes::from_static(b"hello")),
        }),
        key: "small".into(),
        ..commit
    };
    stream
        .send(&Message::Batch(Batch {
            items: vec![inline],
        }))
        .bounded()
        .await
        .unwrap();
    for seq in [4, 5] {
        let Some(Message::Applied(Applied { identity, outcome })) =
            stream.recv().bounded().await.unwrap()
        else {
            panic!("expected APPLIED");
        };
        assert_eq!(identity, self::identity(seq));
        assert!(matches!(
            outcome,
            Outcome::Failed {
                error: ApplyError::Unavailable,
                ..
            }
        ));
    }
    stream.finish().unwrap();
    assert_eq!(stream.recv().bounded().await.unwrap(), None);
    assert!(setup.staging.is_empty());
    // Only the frames within the quota reached the sink.
    assert_eq!(sink.appended().len(), 1);
    connection.close();
}

/// A commit's identity and what it found staged.
type Seen = (WriteIdentity, Option<StagedObject>);

/// Stands in for the shard primaries' side of `COMMIT` and `BATCH`:
/// records what each commit found staged and the items of each batch, and
/// answers with the outcome set for each identity, `committed` by default.
#[derive(Debug, Clone, Default)]
struct TestCommits {
    outcomes: Arc<Mutex<BTreeMap<WriteIdentity, Outcome>>>,
    seen: Arc<Mutex<Vec<Seen>>>,
    batches: Arc<Mutex<Vec<Vec<WriteIdentity>>>>,
    /// How many items of a batch it answers, if not all.
    answers: Arc<Mutex<Option<usize>>>,
}

impl TestCommits {
    fn answer(&self, identity: &WriteIdentity, outcome: Outcome) {
        self.outcomes
            .lock()
            .unwrap()
            .insert(identity.clone(), outcome);
    }

    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }
}

impl CommitSink for TestCommits {
    async fn apply(&self, commit: &Commit, staged: Option<&StagedObject>) -> Outcome {
        self.seen
            .lock()
            .unwrap()
            .push((commit.identity.clone(), staged.cloned()));
        self.outcomes
            .lock()
            .unwrap()
            .get(&commit.identity)
            .cloned()
            .unwrap_or(Outcome::Committed { etag: None })
    }

    async fn apply_batch(&self, items: &[Commit]) -> Vec<Outcome> {
        let identities = items.iter().map(|item| item.identity.clone()).collect();
        self.batches.lock().unwrap().push(identities);
        let outcomes = self.outcomes.lock().unwrap();
        let answers = self.answers.lock().unwrap().unwrap_or(items.len());
        items
            .iter()
            .take(answers)
            .map(|item| {
                outcomes
                    .get(&item.identity)
                    .cloned()
                    .unwrap_or(Outcome::Committed { etag: None })
            })
            .collect()
    }
}

/// A batch item of `key`: a small object, or a delete if `body` is `None`.
fn batch_item(identity: &WriteIdentity, key: &str, body: Option<&'static [u8]>) -> Commit {
    let write = body.map_or(Write::Delete, |body| {
        Write::Put(Put {
            size: body.len() as u64,
            etag: ETag::new("5d41402abc4b2a76b9719d911017c592").unwrap(),
            last_modified_ms: 0,
            metadata: BTreeMap::new(),
            tags: BTreeMap::new(),
            checksums: BTreeMap::new(),
            data: PutData::Inline(Bytes::from_static(body)),
        })
    });
    Commit {
        identity: identity.clone(),
        bucket: BucketName::new(DESTINATION_BUCKET).unwrap(),
        key: key.to_owned(),
        precondition: Precondition::Absent,
        write,
        apply_by_ms: Some(1_800_000_000_000),
    }
}

#[tokio::test]
async fn a_batch_is_answered_item_by_item_in_one_round_trip() {
    let commits = TestCommits::default();
    let setup = setup_with(TestSink::default(), LIMITS, commits.clone());
    let failed = Outcome::PreconditionFailed {
        current: Some(identity(9)),
    };
    commits.answer(&identity(2), failed.clone());
    let long = Outcome::Failed {
        error: ApplyError::Unavailable,
        reason: "é".repeat(MAX_REASON_LEN),
    };
    commits.answer(&identity(3), long);

    // An item of a bucket pair the source may not write is refused before
    // the sink sees the batch; the others reach it as one batch.
    let mut builder = BatchBuilder::new();
    let items = [
        batch_item(&identity(1), "a", Some(b"first")),
        batch_item(&identity(2), "b", Some(b"second")),
        batch_item(&identity(3), "c", None),
        batch_item(&"prod-us/b-other/5/42.4".parse().unwrap(), "d", None),
    ];
    for item in &items {
        builder.push(item).unwrap();
    }
    let batch = builder.take().unwrap();
    let (connection, mut stream) = setup.stream().await;
    let outcomes = send_batch(&mut stream, &batch).bounded().await.unwrap();
    assert_eq!(outcomes.len(), 4);
    assert_eq!(outcomes[0], Some(Outcome::Committed { etag: None }));
    assert_eq!(outcomes[1], Some(failed));
    let Some(Outcome::Failed { error, reason }) = &outcomes[2] else {
        panic!("expected a failure, got {:?}", outcomes[2]);
    };
    assert_eq!(*error, ApplyError::Unavailable);
    assert!(reason.len() <= MAX_REASON_LEN, "the reason is cut to fit");
    assert!(matches!(
        outcomes[3],
        Some(Outcome::Failed {
            error: ApplyError::Refused,
            ..
        })
    ));
    let batches = commits.batches.lock().unwrap().clone();
    assert_eq!(batches, [vec![identity(1), identity(2), identity(3)]]);
    // A batch stages nothing.
    assert!(setup.staging.is_empty());
    assert!(commits.seen().is_empty());

    // A sink that answers only some items leaves the rest `unavailable`,
    // for the source to send again.
    *commits.answers.lock().unwrap() = Some(1);
    let batch = Batch {
        items: items[..2].to_vec(),
    };
    let mut stream = connection.open_stream().bounded().await.unwrap();
    let outcomes = send_batch(&mut stream, &batch).bounded().await.unwrap();
    assert_eq!(outcomes[0], Some(Outcome::Committed { etag: None }));
    assert!(matches!(
        outcomes[1],
        Some(Outcome::Failed {
            error: ApplyError::Unavailable,
            ..
        })
    ));
    connection.close();
}

fn staged_commit(identity: &WriteIdentity, key: &str, size: u64) -> Message {
    Message::Commit(Commit {
        identity: identity.clone(),
        bucket: BucketName::new(DESTINATION_BUCKET).unwrap(),
        key: key.to_owned(),
        precondition: Precondition::Absent,
        write: Write::Put(Put {
            size,
            etag: ETag::new("5d41402abc4b2a76b9719d911017c592").unwrap(),
            last_modified_ms: 0,
            metadata: BTreeMap::new(),
            tags: BTreeMap::new(),
            checksums: BTreeMap::new(),
            data: PutData::Staged { piece: 1 },
        }),
        apply_by_ms: Some(1_800_000_000_000),
    })
}

/// The `APPLIED` that answers the next message other than a `DURABLE`.
async fn applied(stream: &mut MessageStream) -> Applied {
    loop {
        match stream.recv().bounded().await.unwrap() {
            Some(Message::Durable(_)) => {}
            Some(Message::Applied(applied)) => return applied,
            other => panic!("expected APPLIED, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn a_commit_publishes_what_is_staged_and_consumes_it() {
    let commits = TestCommits::default();
    let setup = setup_with(TestSink::default(), LIMITS, commits.clone());
    let (connection, mut stream) = setup.stream().await;
    let len = 4 * FRAME;
    let body = body(len);

    // The COMMIT follows the frames at once, and still finds every one of
    // them staged: the stream's appends settle first.
    let id = identity(1);
    stream.send(&begin(&id, "k")).bounded().await.unwrap();
    assert!(resume(&mut stream).await.is_empty());
    for at in (0..len).step_by(FRAME as usize) {
        stream
            .send(&frame(&body, 1, at..at + FRAME))
            .bounded()
            .await
            .unwrap();
    }
    stream
        .send(&staged_commit(&id, "k", len))
        .bounded()
        .await
        .unwrap();
    let answer = applied(&mut stream).await;
    assert_eq!(answer.identity, id);
    assert_eq!(answer.outcome, Outcome::Committed { etag: None });
    let (seen, staged) = commits.seen().pop().unwrap();
    assert_eq!(seen, id);
    let staged = staged.unwrap();
    assert_eq!(staged.extents(1, len).unwrap().len(), 4);
    // Committed: the staging is consumed, the stream's later DATA for it is
    // dropped, and a replay finds nothing staged.
    assert!(setup.staging.staged(&id).is_none());
    stream
        .send(&frame(&body, 1, 0..FRAME))
        .bounded()
        .await
        .unwrap();
    stream
        .send(&staged_commit(&id, "k", len))
        .bounded()
        .await
        .unwrap();
    let answer = applied(&mut stream).await;
    assert_eq!(answer.outcome, Outcome::Committed { etag: None });
    assert_eq!(commits.seen().pop().unwrap().1, None);

    // A result that leaves the source something to do keeps the staging;
    // one that makes it stage again discards it.
    let id = identity(2);
    stream.send(&begin(&id, "k")).bounded().await.unwrap();
    assert!(resume(&mut stream).await.is_empty());
    stream
        .send(&frame(&body, 1, 0..FRAME))
        .bounded()
        .await
        .unwrap();
    let failed = |error, reason: &str| Outcome::Failed {
        error,
        reason: reason.to_owned(),
    };
    let long = "é".repeat(MAX_REASON_LEN);
    for (outcome, kept) in [
        (
            Outcome::PreconditionFailed {
                current: Some(identity(1)),
            },
            true,
        ),
        (failed(ApplyError::Incomplete, "a gap"), true),
        (failed(ApplyError::Unavailable, &long), true),
        (failed(ApplyError::ChecksumMismatch, "bytes differ"), false),
    ] {
        commits.answer(&id, outcome.clone());
        stream
            .send(&staged_commit(&id, "k", len))
            .bounded()
            .await
            .unwrap();
        let answer = applied(&mut stream).await;
        match (&answer.outcome, &outcome) {
            (
                Outcome::Failed { error, reason },
                Outcome::Failed {
                    error: wanted,
                    reason: full,
                },
            ) => {
                assert_eq!(error, wanted);
                assert!(reason.len() <= MAX_REASON_LEN && full.starts_with(reason.as_str()));
            }
            _ => assert_eq!(answer.outcome, outcome),
        }
        assert_eq!(setup.staging.staged(&id).is_some(), kept, "{outcome:?}");
    }

    // A COMMIT for another key than its staging's is refused without
    // applying it, and the staging is discarded.
    let id = identity(3);
    stream.send(&begin(&id, "a")).bounded().await.unwrap();
    assert!(resume(&mut stream).await.is_empty());
    let calls = commits.seen().len();
    stream
        .send(&staged_commit(&id, "b", len))
        .bounded()
        .await
        .unwrap();
    let answer = applied(&mut stream).await;
    assert!(matches!(
        answer.outcome,
        Outcome::Failed {
            error: ApplyError::Refused,
            ..
        }
    ));
    assert_eq!(commits.seen().len(), calls);
    assert!(setup.staging.is_empty());

    stream.finish().unwrap();
    assert_eq!(stream.recv().bounded().await.unwrap(), None);
    connection.close();
}
