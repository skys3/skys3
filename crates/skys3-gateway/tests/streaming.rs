//! Streamed single PUTs (§7.2, §7.3): a body of a `write_back` bucket that
//! reaches `streaming_flush_min_bytes` commits an `UPLOAD_BEGIN`, and its
//! `PUT` inherits that record's write identity; a body that fails leaves
//! the identity naming no write; and every body streamed as extents must
//! arrive within its deadline (§10.3).

mod common;

use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use common::signing::request as with_body;
use common::{Answer, Setup, config, setup_with};
use http::{Method, Request};
use s3s::Body;
use skys3_gateway::{GatewayConfig, ShardRef, Shards};
use skys3_log::{LogRecord, RecordBody};
use skys3_types::{EpochSeq, Seq};

/// Bodies over 1,024 bytes go in extents of 1,000, and PUTs to
/// `write_back` buckets stream from 3,000 bytes.
fn streaming() -> GatewayConfig {
    let mut config = config("");
    config.inline_max_bytes = 1024;
    config.extent_bytes = 1000;
    config.streaming_flush_min_bytes = Some(3000);
    config
}

/// `len` bytes of lowercase letters.
fn fill(len: usize) -> Bytes {
    (0..len).map(|i| b'a' + (i % 26) as u8).collect()
}

async fn put(setup: &Setup, bucket: &str, key: &str, body: Bytes) -> Answer {
    let uri = format!("/{bucket}/{key}");
    setup.send(with_body(Method::PUT, &uri, &[], body)).await
}

/// The write identity the current version of `key` inherited, if any.
async fn identity(setup: &Setup, shard: &ShardRef, key: &str) -> Option<EpochSeq> {
    let entry = setup.shards.entry(shard, key).await.unwrap().unwrap();
    entry.object.unwrap().write_identity
}

/// The positions of the `UPLOAD_BEGIN` records of `shard`, with their keys.
async fn begins(setup: &Setup, shard: &ShardRef) -> Vec<(EpochSeq, String)> {
    let replica = setup.shards.local().set().get(&shard.into()).await.unwrap();
    let last = replica.last_sequenced();
    let records = replica.read_tail(Seq::new(0), last).await.unwrap();
    records
        .iter()
        .filter_map(|(_, bytes)| match LogRecord::decode(bytes).unwrap().0 {
            LogRecord {
                position,
                body: RecordBody::UploadBegin(begin),
                ..
            } => Some((position, begin.key)),
            _ => None,
        })
        .collect()
}

/// A body whose stream fails once it has sent 5,000 bytes.
struct Failing {
    sent: bool,
}

impl http_body::Body for Failing {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, std::io::Error>>> {
        if self.sent {
            return Poll::Ready(Some(Err(std::io::Error::other("reset"))));
        }
        self.sent = true;
        Poll::Ready(Some(Ok(http_body::Frame::data(fill(5000)))))
    }
}

/// A body that sends its frames `pause` apart.
struct Paced {
    frames: VecDeque<Bytes>,
    pause: Duration,
    sleep: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl http_body::Body for Paced {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, std::io::Error>>> {
        if let Some(sleep) = &mut self.sleep {
            if sleep.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
            self.sleep = None;
        }
        let Some(frame) = self.frames.pop_front() else {
            return Poll::Ready(None);
        };
        self.sleep = Some(Box::pin(tokio::time::sleep(self.pause)));
        Poll::Ready(Some(Ok(http_body::Frame::data(frame))))
    }
}

/// A body that sends one frame and then nothing more, without ending.
struct Stalled {
    first: Option<Bytes>,
}

impl http_body::Body for Stalled {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, std::io::Error>>> {
        match self.first.take() {
            Some(first) => Poll::Ready(Some(Ok(http_body::Frame::data(first)))),
            None => Poll::Pending,
        }
    }
}

#[tokio::test]
async fn streamed_puts_inherit_the_identity_of_their_upload_begin() {
    let setup = setup_with(streaming()).await;
    let bucket = setup.create_write_back("remote").await;
    let shard = ShardRef::for_key(&bucket, "big");

    // A body that reaches the threshold commits one UPLOAD_BEGIN, and its
    // PUT inherits that record's identity.
    let data = fill(10_500);
    put(&setup, "remote", "big", data.clone())
        .await
        .assert(200, None);
    let begin = identity(&setup, &shard, "big").await.unwrap();
    assert_eq!(begins(&setup, &shard).await, [(begin, "big".to_owned())]);
    let entry = setup.shards.entry(&shard, "big").await.unwrap().unwrap();
    assert!(begin < entry.version, "{begin} precedes {}", entry.version);
    let got = setup.call(Method::GET, "/remote/big", &[], "").await;
    got.assert(200, None);
    assert_eq!(got.body.as_bytes(), &data[..]);

    // The threshold counts bytes, inline or not; a shorter body names its
    // own record.
    for (key, len, streamed) in [("at", 3000, true), ("below", 2999, false)] {
        put(&setup, "remote", key, fill(len))
            .await
            .assert(200, None);
        let shard = ShardRef::for_key(&bucket, key);
        let inherited = identity(&setup, &shard, key).await;
        assert_eq!(inherited.is_some(), streamed, "{key}");
    }

    // A body that fails after its UPLOAD_BEGIN publishes nothing: the
    // record names no version, and the key keeps its last one.
    let request = Request::put("/remote/big")
        .body(Body::http_body(Failing { sent: false }))
        .unwrap();
    setup
        .send(request)
        .await
        .assert(400, Some("IncompleteBody"));
    let all = begins(&setup, &shard).await;
    assert_eq!(all.len(), 2, "{all:?}");
    assert_eq!(identity(&setup, &shard, "big").await, Some(begin));
    // The next PUT of the key gets an identity of its own.
    put(&setup, "remote", "big", data.clone())
        .await
        .assert(200, None);
    let next = identity(&setup, &shard, "big").await.unwrap();
    assert!(
        next > all[1].0,
        "{next} follows the failed upload's {:?}",
        all[1]
    );
    assert_eq!(begins(&setup, &shard).await.last().unwrap().0, next);
}

#[tokio::test]
async fn only_write_back_buckets_stream_and_the_threshold_can_be_off() {
    let setup = setup_with(streaming()).await;
    let bucket = setup.create_local("photos").await;
    put(&setup, "photos", "big", fill(10_500))
        .await
        .assert(200, None);
    let shard = ShardRef::for_key(&bucket, "big");
    assert_eq!(identity(&setup, &shard, "big").await, None);
    assert!(begins(&setup, &shard).await.is_empty());

    let mut off = streaming();
    off.streaming_flush_min_bytes = None;
    let setup = setup_with(off).await;
    let bucket = setup.create_write_back("remote").await;
    put(&setup, "remote", "big", fill(10_500))
        .await
        .assert(200, None);
    let shard = ShardRef::for_key(&bucket, "big");
    assert_eq!(identity(&setup, &shard, "big").await, None);
    assert!(begins(&setup, &shard).await.is_empty());
}

#[tokio::test]
async fn a_body_that_streams_past_its_deadline_is_refused() {
    let mut config = streaming();
    config.max_body_duration = Duration::from_millis(50);
    let setup = setup_with(config).await;
    let bucket = setup.create_write_back("remote").await;
    let shard = ShardRef::for_key(&bucket, "slow");
    let body = |pause| Paced {
        frames: (0..4).map(|_| fill(1500)).collect(),
        pause,
        sleep: None,
    };
    // Its first extent starts the clock, and a later one finds it expired.
    let request = Request::put("/remote/slow")
        .body(Body::http_body(body(Duration::from_millis(100))))
        .unwrap();
    setup
        .send(request)
        .await
        .assert(400, Some("RequestTimeout"));
    assert_eq!(setup.shards.entry(&shard, "slow").await.unwrap(), None);
    // The same body sent in time is stored.
    let request = Request::put("/remote/slow")
        .body(Body::http_body(body(Duration::ZERO)))
        .unwrap();
    setup.send(request).await.assert(200, None);
    let got = setup.call(Method::GET, "/remote/slow", &[], "").await;
    assert_eq!(got.body.len(), 6000);
}

#[tokio::test]
async fn a_body_that_stalls_after_its_first_extent_is_refused_at_its_deadline() {
    let mut config = streaming();
    config.max_body_duration = Duration::from_millis(50);
    let setup = setup_with(config).await;
    let bucket = setup.create_write_back("remote").await;
    let shard = ShardRef::for_key(&bucket, "stalled");
    // 1,500 bytes send one extent, which starts the clock, and the client
    // then sends nothing: the deadline ends the wait for its next bytes.
    let request = Request::put("/remote/stalled")
        .body(Body::http_body(Stalled {
            first: Some(fill(1500)),
        }))
        .unwrap();
    let answer = tokio::time::timeout(Duration::from_secs(10), setup.send(request))
        .await
        .expect("a stalled body is answered once its deadline passes");
    answer.assert(400, Some("RequestTimeout"));
    assert_eq!(setup.shards.entry(&shard, "stalled").await.unwrap(), None);
}
