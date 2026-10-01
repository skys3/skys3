//! GetObject of evicted versions of `write_back` buckets: read through
//! [`Fills`], resolved again after a fill found the key changed (§9.2).

mod common;

use std::collections::VecDeque;
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use common::{Setup, config, setup_with};
use http::Method;
use skys3_gateway::{FillBody, FillError, Fills, ShardRef, Shards};
use skys3_index::EntryState;
use skys3_types::{BucketDocument, EpochSeq};
use tokio::sync::mpsc;

/// What one read through the fake asked for.
type Call = (ShardRef, String, EpochSeq, Range<u64>);

/// Fills that answer from a script: bytes of a whole object, of which the
/// requested range is streamed, or an error.
#[derive(Debug, Default)]
struct ScriptedFills {
    answers: Mutex<VecDeque<Result<&'static [u8], FillError>>>,
    calls: Mutex<Vec<Call>>,
}

impl ScriptedFills {
    fn new(answers: impl IntoIterator<Item = Result<&'static [u8], FillError>>) -> Arc<Self> {
        Arc::new(Self {
            answers: Mutex::new(answers.into_iter().collect()),
            calls: Mutex::default(),
        })
    }

    fn calls(&self) -> Vec<Call> {
        self.calls.lock().unwrap().clone()
    }
}

impl Fills for ScriptedFills {
    fn read(
        &self,
        shard: &ShardRef,
        key: &str,
        version: EpochSeq,
        range: Range<u64>,
    ) -> Pin<Box<dyn Future<Output = Result<FillBody, FillError>> + Send + '_>> {
        let call = (shard.clone(), key.to_owned(), version, range.clone());
        self.calls.lock().unwrap().push(call);
        let answer = self.answers.lock().unwrap().pop_front();
        Box::pin(async move {
            let object = answer.expect("a scripted answer")?;
            let (sender, receiver) = mpsc::channel(2);
            let slice = &object[range.start as usize..range.end as usize];
            // Two chunks, as a fill streams extents.
            let middle = slice.len() / 2;
            for chunk in [&slice[..middle], &slice[middle..]] {
                sender.try_send(Ok(Bytes::from_static(chunk))).unwrap();
            }
            Ok(receiver)
        })
    }
}

/// A gateway with `fills`, a `write_back` bucket, and `key` written,
/// flushed, and evicted. Returns the bucket and the version.
async fn evicted(
    fills: Option<Arc<ScriptedFills>>,
    key: &str,
) -> (Setup, BucketDocument, EpochSeq) {
    let mut config = config("");
    config.fills = fills.map(|fills| fills as Arc<dyn Fills>);
    let setup = setup_with(config).await;
    let bucket = setup.create_write_back("remote").await;
    setup
        .call(Method::PUT, &format!("/remote/{key}"), &[], "hello, world")
        .await
        .assert(200, None);
    setup.shards.flush(&bucket.bucket_id).await;
    let shard = ShardRef::for_key(&bucket, key);
    let version = setup
        .shards
        .entry(&shard, key)
        .await
        .unwrap()
        .unwrap()
        .version;
    let local = setup
        .shards
        .local()
        .set()
        .get(&(&shard).into())
        .await
        .unwrap();
    local.evict(key, version).await.unwrap().unwrap();
    let entry = setup.shards.entry(&shard, key).await.unwrap().unwrap();
    assert_eq!(entry.state, EntryState::Evicted);
    (setup, bucket, version)
}

#[tokio::test]
async fn an_evicted_object_is_read_through_a_fill() {
    let fills = ScriptedFills::new([Ok(&b"hello, world"[..]), Ok(&b"hello, world"[..])]);
    let (setup, bucket, version) = evicted(Some(Arc::clone(&fills)), "k").await;
    let whole = setup.call(Method::GET, "/remote/k", &[], "").await;
    whole.assert(200, None);
    assert_eq!(whole.body, "hello, world");
    assert_eq!(whole.headers["content-length"], "12");

    let part = setup
        .call(Method::GET, "/remote/k", &[("range", "bytes=7-11")], "")
        .await;
    part.assert(206, None);
    assert_eq!(part.body, "world");
    let shard = ShardRef::for_key(&bucket, "k");
    assert_eq!(
        fills.calls(),
        [
            (shard.clone(), "k".to_owned(), version, 0..12),
            (shard, "k".to_owned(), version, 7..12),
        ]
    );

    // HEAD and a GET that its conditions answer need no bytes.
    setup
        .call(Method::HEAD, "/remote/k", &[], "")
        .await
        .assert(200, None);
    let etag = whole.headers["etag"].to_str().unwrap();
    setup
        .call(Method::GET, "/remote/k", &[("if-none-match", etag)], "")
        .await
        .assert(304, None);
    assert_eq!(fills.calls().len(), 2);
}

#[tokio::test]
async fn a_changed_key_is_resolved_again_a_few_times() {
    let fills = ScriptedFills::new([Err(FillError::Changed), Ok(&b"hello, world"[..])]);
    let (setup, _, version) = evicted(Some(Arc::clone(&fills)), "k").await;
    let answer = setup.call(Method::GET, "/remote/k", &[], "").await;
    answer.assert(200, None);
    assert_eq!(answer.body, "hello, world");
    let versions: Vec<_> = fills.calls().into_iter().map(|call| call.2).collect();
    assert_eq!(versions, [version, version]);

    let fills = ScriptedFills::new([
        Err(FillError::Changed),
        Err(FillError::Changed),
        Err(FillError::Changed),
    ]);
    let (setup, ..) = evicted(Some(Arc::clone(&fills)), "k").await;
    let answer = setup.call(Method::GET, "/remote/k", &[], "").await;
    answer.assert(503, Some("ServiceUnavailable"));
    assert_eq!(fills.calls().len(), 3);
}

#[tokio::test]
async fn a_failed_fill_answers_503() {
    let reason = "the object is gone";
    let fills = ScriptedFills::new([Err(FillError::Unavailable(reason.to_owned()))]);
    let (setup, ..) = evicted(Some(fills), "k").await;
    let answer = setup.call(Method::GET, "/remote/k", &[], "").await;
    answer.assert(503, Some("ServiceUnavailable"));
    assert!(answer.body.contains(reason), "{}", answer.body);

    // Without fills, the bytes are not cached here.
    let (setup, ..) = evicted(None, "k").await;
    let answer = setup.call(Method::GET, "/remote/k", &[], "").await;
    answer.assert(503, Some("ServiceUnavailable"));
    assert!(answer.body.contains("not cached"), "{}", answer.body);
}
