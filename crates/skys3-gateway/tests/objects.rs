//! Object operations through the gateway's pipeline, over real shards on a
//! simulated disk: PutObject, GetObject with ranges, HeadObject,
//! DeleteObject, conditional requests, metadata, checksums, and durability.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use common::signing::{NOW, request as with_body};
use common::{Answer, Setup, config, gateway_with, setup_with};
use http::{Method, Request};
use s3s::Body;
use s3s::dto::{Timestamp, TimestampFormat};
use skys3_gateway::stub::MemoryShards;
use skys3_gateway::{
    Gateway, GatewayConfig, IdSource, MAX_USER_METADATA_BYTES, Precondition, ShardRef, Shards,
    SigV4Authenticator, StaticCredentials, TrustAll,
};
use skys3_index::Payload;
use skys3_io::ManualWallClock;
use skys3_log::RecordBody;
use skys3_log::record::{Import, Put, PutData};
use skys3_types::{BucketDocument, ETag};

/// Small bodies go inline, and longer ones in extents of 1,000 bytes.
fn small_extents() -> GatewayConfig {
    let mut config = config("");
    config.inline_max_bytes = 1024;
    config.extent_bytes = 1000;
    config
}

async fn local(config: GatewayConfig) -> (Setup, BucketDocument) {
    let setup = setup_with(config).await;
    let bucket = setup.create_local("photos").await;
    (setup, bucket)
}

/// `len` bytes of lowercase letters.
fn fill(len: usize) -> Bytes {
    (0..len).map(|i| b'a' + (i % 26) as u8).collect()
}

fn md5_etag(data: &[u8]) -> String {
    use md5::Digest as _;
    let digest = md5::Md5::digest(data);
    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    format!("\"{hex}\"")
}

impl Setup {
    async fn put(&self, key: &str, headers: &[(&str, &str)], body: Bytes) -> Answer {
        let uri = format!("/photos/{key}");
        self.send(with_body(Method::PUT, &uri, headers, body)).await
    }

    async fn get(&self, key: &str, headers: &[(&str, &str)]) -> Answer {
        let uri = format!("/photos/{key}");
        self.call(Method::GET, &uri, headers, "").await
    }

    async fn head(&self, key: &str, headers: &[(&str, &str)]) -> Answer {
        let uri = format!("/photos/{key}");
        self.call(Method::HEAD, &uri, headers, "").await
    }

    async fn delete(&self, key: &str, headers: &[(&str, &str)]) -> Answer {
        let uri = format!("/photos/{key}");
        self.call(Method::DELETE, &uri, headers, "").await
    }
}

impl Answer {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(|value| value.to_str().unwrap())
    }
}

#[tokio::test]
async fn objects_are_written_read_and_deleted() {
    let (setup, _) = local(config("")).await;
    let put = setup
        .put(
            "cat.txt",
            &[
                ("content-type", "text/plain"),
                ("cache-control", "max-age=60"),
                ("content-disposition", "inline"),
                ("content-encoding", "identity"),
                ("content-language", "en"),
                ("expires", "Thu, 01 Dec 2033 16:00:00 GMT"),
                ("x-amz-meta-Color", "tabby"),
            ],
            Bytes::from_static(b"meow"),
        )
        .await;
    put.assert(200, None);
    let etag = md5_etag(b"meow");
    assert_eq!(put.header("etag"), Some(etag.as_str()));
    // S3 stores a CRC64NVME checksum with a body uploaded without one.
    assert!(put.header("x-amz-checksum-crc64nvme").is_some(), "{put:?}");
    assert_eq!(put.header("x-amz-checksum-type"), Some("FULL_OBJECT"));

    let got = setup.get("cat.txt", &[]).await;
    got.assert(200, None);
    assert_eq!(got.body, "meow");
    for (name, value) in [
        ("etag", etag.as_str()),
        ("content-length", "4"),
        ("content-type", "text/plain"),
        ("cache-control", "max-age=60"),
        ("content-disposition", "inline"),
        ("content-encoding", "identity"),
        ("content-language", "en"),
        ("expires", "Thu, 01 Dec 2033 16:00:00 GMT"),
        ("x-amz-meta-color", "tabby"),
        ("accept-ranges", "bytes"),
    ] {
        assert_eq!(got.header(name), Some(value), "{name}: {got:?}");
    }
    let modified = got.header("last-modified").unwrap();
    let modified = Timestamp::parse(TimestampFormat::HttpDate, modified).unwrap();
    assert!(modified <= Timestamp::from(SystemTime::now()));
    // Checksums are returned only when asked for.
    assert!(got.header("x-amz-checksum-crc64nvme").is_none());

    let head = setup.head("cat.txt", &[]).await;
    head.assert(200, None);
    assert_eq!(head.body, "");
    assert_eq!(head.header("content-length"), Some("4"));
    assert_eq!(head.header("etag"), Some(etag.as_str()));
    assert_eq!(head.header("x-amz-meta-color"), Some("tabby"));

    // An overwrite replaces the object, metadata and all.
    setup
        .put("cat.txt", &[], Bytes::from_static(b"purr"))
        .await
        .assert(200, None);
    let got = setup.get("cat.txt", &[]).await;
    assert_eq!(got.body, "purr");
    assert_eq!(got.header("content-type"), Some("binary/octet-stream"));
    assert!(got.header("x-amz-meta-color").is_none());

    setup.delete("cat.txt", &[]).await.assert(204, None);
    setup
        .get("cat.txt", &[])
        .await
        .assert(404, Some("NoSuchKey"));
    assert_eq!(setup.head("cat.txt", &[]).await.status, 404);
    // Deleting a key with no object succeeds, as in S3.
    setup.delete("cat.txt", &[]).await.assert(204, None);
    setup.delete("never.txt", &[]).await.assert(204, None);

    // Objects of a bucket that does not exist.
    let missing = with_body(Method::PUT, "/nope/k", &[], Bytes::from_static(b"x"));
    setup.send(missing).await.assert(404, Some("NoSuchBucket"));
    let missing = setup.call(Method::GET, "/nope/k", &[], "").await;
    missing.assert(404, Some("NoSuchBucket"));
    let missing = setup.call(Method::DELETE, "/nope/k", &[], "").await;
    missing.assert(404, Some("NoSuchBucket"));
    assert_eq!(
        setup.call(Method::HEAD, "/nope/k", &[], "").await.status,
        404
    );
}

#[tokio::test]
async fn empty_objects_are_stored() {
    let (setup, _) = local(config("")).await;
    setup
        .put("empty", &[], Bytes::new())
        .await
        .assert(200, None);
    let got = setup.get("empty", &[]).await;
    got.assert(200, None);
    assert_eq!(got.body, "");
    assert_eq!(got.header("content-length"), Some("0"));
    assert_eq!(got.header("etag"), Some(md5_etag(b"").as_str()));
    // No byte range of an empty object is satisfiable.
    let ranged = setup.get("empty", &[("range", "bytes=0-")]).await;
    ranged.assert(416, Some("InvalidRange"));
}

#[tokio::test]
async fn large_bodies_are_streamed_as_extents() {
    let (setup, bucket) = local(small_extents()).await;
    let data = fill(10_500);
    setup.put("big", &[], data.clone()).await.assert(200, None);
    let shard = ShardRef::for_key(&bucket, "big");
    let entry = setup.shards.entry(&shard, "big").await.unwrap().unwrap();
    let object = entry.object.unwrap();
    let Payload::Extents(extents) = &object.payload else {
        panic!("expected extents: {:?}", object.payload);
    };
    let lengths: Vec<u32> = extents.iter().map(|extent| extent.len).collect();
    assert_eq!(lengths, [vec![1000; 10], vec![500]].concat());
    assert!(extents.iter().all(|extent| extent.position < entry.version));

    let got = setup.get("big", &[]).await;
    got.assert(200, None);
    assert_eq!(got.body.as_bytes(), &data[..]);
    assert_eq!(got.header("etag"), Some(md5_etag(&data).as_str()));

    // Ranges within one extent, across extents, and to the end.
    for (range, bytes) in [
        ("bytes=0-0", 0..1),
        ("bytes=999-1000", 999..1001),
        ("bytes=1500-4499", 1500..4500),
        ("bytes=10000-", 10_000..10_500),
        ("bytes=-501", 9999..10_500),
        ("bytes=10499-99999", 10_499..10_500),
    ] {
        let got = setup.get("big", &[("range", range)]).await;
        got.assert(206, None);
        assert_eq!(got.body.as_bytes(), &data[bytes.clone()], "{range}");
        let content_range = format!("bytes {}-{}/10500", bytes.start, bytes.end - 1);
        assert_eq!(got.header("content-range"), Some(content_range.as_str()));
        let length = bytes.len().to_string();
        assert_eq!(got.header("content-length"), Some(length.as_str()));
    }

    // A body just over the inline limit is one extent.
    setup.put("edge", &[], fill(1025)).await.assert(200, None);
    let shard = ShardRef::for_key(&bucket, "edge");
    let entry = setup.shards.entry(&shard, "edge").await.unwrap().unwrap();
    let payload = entry.object.unwrap().payload;
    assert!(
        matches!(&payload, Payload::Extents(e) if e.len() == 2),
        "{payload:?}"
    );
    let inline = fill(1024);
    setup.put("inline", &[], inline).await.assert(200, None);
    let shard = ShardRef::for_key(&bucket, "inline");
    let entry = setup.shards.entry(&shard, "inline").await.unwrap().unwrap();
    assert!(matches!(entry.object.unwrap().payload, Payload::Inline(_)));
}

#[tokio::test]
async fn ranges_follow_s3() {
    let (setup, _) = local(config("")).await;
    setup
        .put("k", &[], Bytes::from_static(b"0123456789"))
        .await
        .assert(200, None);
    for (range, status, body) in [
        ("bytes=2-5", 206, "2345"),
        ("bytes=7-", 206, "789"),
        ("bytes=-3", 206, "789"),
        ("bytes=-30", 206, "0123456789"),
        ("bytes=8-100", 206, "89"),
        // Ranges S3 ignores: several ranges, a reversed one, and nonsense.
        ("bytes=0-1,4-5", 200, "0123456789"),
        ("bytes=5-3", 200, "0123456789"),
        ("chars=0-1", 200, "0123456789"),
    ] {
        let got = setup.get("k", &[("range", range)]).await;
        assert_eq!(
            (got.status.as_u16(), got.body.as_str()),
            (status, body),
            "{range}"
        );
    }
    for range in ["bytes=10-", "bytes=10-20", "bytes=-0"] {
        let got = setup.get("k", &[("range", range)]).await;
        got.assert(416, Some("InvalidRange"));
    }
    let head = setup.head("k", &[("range", "bytes=2-5")]).await;
    assert_eq!(head.status, 206);
    assert_eq!(head.header("content-length"), Some("4"));
    assert_eq!(head.header("content-range"), Some("bytes 2-5/10"));
    assert_eq!(setup.head("k", &[("range", "bytes=10-")]).await.status, 416);

    // A single-PUT object has one part.
    let part = setup
        .call(Method::GET, "/photos/k?partNumber=1", &[], "")
        .await;
    part.assert(200, None);
    assert_eq!(part.body, "0123456789");
    let part = setup
        .call(Method::GET, "/photos/k?partNumber=2", &[], "")
        .await;
    part.assert(416, Some("InvalidPartNumber"));
    let both = setup
        .call(
            Method::GET,
            "/photos/k?partNumber=1",
            &[("range", "bytes=0-1")],
            "",
        )
        .await;
    both.assert(400, Some("InvalidRequest"));
}

/// An HTTP date `offset` seconds from now.
fn http_date(offset: i64) -> String {
    let now = SystemTime::now();
    let time = if offset >= 0 {
        now + Duration::from_secs(offset.unsigned_abs())
    } else {
        now - Duration::from_secs(offset.unsigned_abs())
    };
    let mut text = Vec::new();
    Timestamp::from(time)
        .format(TimestampFormat::HttpDate, &mut text)
        .unwrap();
    String::from_utf8(text).unwrap()
}

#[tokio::test]
async fn conditional_reads_follow_rfc_9110() {
    let (setup, _) = local(config("")).await;
    setup
        .put("k", &[], Bytes::from_static(b"data"))
        .await
        .assert(200, None);
    let etag = md5_etag(b"data");
    let other = md5_etag(b"other");
    let (past, future) = (http_date(-3600), http_date(3600));
    let weak = format!("W/{etag}");
    let cases: &[(&[(&str, &str)], u16)] = &[
        (&[("if-match", &etag)], 200),
        (&[("if-match", "*")], 200),
        (&[("if-match", &other)], 412),
        // If-Match compares strongly.
        (&[("if-match", &weak)], 412),
        (&[("if-none-match", &etag)], 304),
        (&[("if-none-match", &weak)], 304),
        (&[("if-none-match", "*")], 304),
        (&[("if-none-match", &other)], 200),
        (&[("if-modified-since", &past)], 200),
        (&[("if-modified-since", &future)], 304),
        (&[("if-unmodified-since", &past)], 412),
        (&[("if-unmodified-since", &future)], 200),
        // If-Match takes precedence over If-Unmodified-Since, and
        // If-None-Match over If-Modified-Since.
        (&[("if-match", &etag), ("if-unmodified-since", &past)], 200),
        (
            &[("if-none-match", &other), ("if-modified-since", &future)],
            200,
        ),
        (
            &[("if-none-match", &etag), ("if-modified-since", &past)],
            304,
        ),
        // A failed precondition answers before Not Modified.
        (&[("if-match", &other), ("if-none-match", &etag)], 412),
    ];
    for (headers, status) in cases {
        let got = setup.get("k", headers).await;
        assert_eq!(got.status.as_u16(), *status, "GET {headers:?}: {got:?}");
        match status {
            200 => assert_eq!(got.body, "data"),
            412 => assert_eq!(got.code(), Some("PreconditionFailed")),
            _ => {
                assert_eq!(got.body, "");
                assert_eq!(got.header("etag"), Some(etag.as_str()));
                assert!(got.header("last-modified").is_some());
            }
        }
        let head = setup.head("k", headers).await;
        assert_eq!(head.status.as_u16(), *status, "HEAD {headers:?}: {head:?}");
    }
    // A key with no object is missing, whatever the conditions.
    let missing = setup.get("missing", &[("if-none-match", "*")]).await;
    missing.assert(404, Some("NoSuchKey"));
}

#[tokio::test]
async fn conditional_writes_follow_s3() {
    let (setup, _) = local(config("")).await;
    let body = || Bytes::from_static(b"v1");
    let absent = [("if-none-match", "*")];
    setup.put("k", &absent, body()).await.assert(200, None);
    setup
        .put("k", &absent, body())
        .await
        .assert(412, Some("PreconditionFailed"));
    let etag = md5_etag(b"v1");
    let matching = [("if-match", etag.as_str())];
    setup
        .put("k", &matching, Bytes::from_static(b"v2"))
        .await
        .assert(200, None);
    // The ETag is now v2's.
    setup
        .put("k", &matching, body())
        .await
        .assert(412, Some("PreconditionFailed"));
    setup
        .put("k", &[("if-match", "*")], Bytes::from_static(b"v3"))
        .await
        .assert(200, None);
    assert_eq!(setup.get("k", &[]).await.body, "v3");
    // If-Match on a key with no object is answered as S3 does.
    setup
        .put("new", &matching, body())
        .await
        .assert(404, Some("NoSuchKey"));
    setup
        .put("new", &[("if-match", "*")], body())
        .await
        .assert(404, Some("NoSuchKey"));
    // S3 accepts only `*` in If-None-Match on a write.
    setup
        .put("k", &[("if-none-match", etag.as_str())], body())
        .await
        .assert(501, Some("NotImplemented"));
    setup
        .put("k", &[("if-match", "*"), ("if-none-match", "*")], body())
        .await
        .assert(501, Some("NotImplemented"));

    // A deleted key has no object, so it may be created again.
    setup.delete("k", &[]).await.assert(204, None);
    setup.put("k", &absent, body()).await.assert(200, None);

    // Conditional deletes.
    let wrong = md5_etag(b"wrong");
    setup
        .delete("k", &[("if-match", wrong.as_str())])
        .await
        .assert(412, Some("PreconditionFailed"));
    setup
        .delete("k", &[("if-match", etag.as_str())])
        .await
        .assert(204, None);
    setup
        .delete("k", &[("if-match", "*")])
        .await
        .assert(404, Some("NoSuchKey"));
    setup
        .delete("k", &[("x-amz-if-match-size", "2")])
        .await
        .assert(501, Some("NotImplemented"));
}

/// Concurrent creations of one key with `If-None-Match: *`: exactly one
/// wins, also when they race on worker threads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conditional_writes_are_linearizable() {
    let (setup, _) = local(small_extents()).await;
    let setup = Arc::new(setup);
    for round in 0..8 {
        let key = format!("race-{round}");
        let tasks: Vec<_> = (0..12)
            .map(|n| {
                let (setup, key) = (Arc::clone(&setup), key.clone());
                // Some bodies are inline, some in extents.
                let body = fill(if n % 2 == 0 { 10 + n } else { 2000 + n });
                tokio::spawn(async move {
                    let answer = setup
                        .put(&key, &[("if-none-match", "*")], body.clone())
                        .await;
                    (answer.status.as_u16(), body)
                })
            })
            .collect();
        let mut winners = Vec::new();
        for task in tasks {
            let (status, body) = task.await.unwrap();
            match status {
                200 => winners.push(body),
                412 => {}
                other => panic!("unexpected status {other}"),
            }
        }
        assert_eq!(winners.len(), 1, "round {round}");
        let stored = setup.get(&key, &[]).await;
        assert_eq!(stored.body.as_bytes(), &winners[0][..]);
    }

    // Unconditional writes interleave with compare-and-swaps: no update is
    // lost, so the counter counts every successful swap.
    setup
        .put("counter", &[], Bytes::from("0"))
        .await
        .assert(200, None);
    let tasks: Vec<_> = (0..8)
        .map(|_| {
            let setup = Arc::clone(&setup);
            tokio::spawn(async move {
                let mut swaps = 0;
                for _ in 0..5 {
                    loop {
                        let got = setup.get("counter", &[]).await;
                        let etag = got.header("etag").unwrap().to_owned();
                        let next = (got.body.parse::<u64>().unwrap() + 1).to_string();
                        let put = setup
                            .put("counter", &[("if-match", &etag)], Bytes::from(next))
                            .await;
                        if put.status == 200 {
                            swaps += 1;
                            break;
                        }
                        assert_eq!(put.status, 412);
                    }
                }
                swaps
            })
        })
        .collect();
    let mut swaps = 0;
    for task in tasks {
        swaps += task.await.unwrap();
    }
    assert_eq!(swaps, 40);
    assert_eq!(setup.get("counter", &[]).await.body, "40");
}

#[tokio::test]
async fn user_metadata_is_bounded_and_the_write_identity_reserved() {
    let (setup, bucket) = local(config("")).await;
    // Names and values count, without the `x-amz-meta-` prefix.
    let name = "x-amz-meta-m";
    let fits = "v".repeat(MAX_USER_METADATA_BYTES - 1);
    setup
        .put("k", &[(name, &fits)], Bytes::new())
        .await
        .assert(200, None);
    let over = "v".repeat(MAX_USER_METADATA_BYTES);
    setup
        .put("k", &[(name, &over)], Bytes::new())
        .await
        .assert(400, Some("MetadataTooLarge"));
    assert_eq!(MAX_USER_METADATA_BYTES, 2048 - 105);
    setup
        .put("k", &[("x-amz-meta-skys3-wid", "c/b/0/1.2")], Bytes::new())
        .await
        .assert(400, Some("InvalidArgument"));

    // An object that carries a write identity, as one adopted from the
    // remote can, never shows it to clients.
    let shard = ShardRef::for_key(&bucket, "adopted");
    let put = Put {
        key: "adopted".into(),
        size: 2,
        last_modified_ms: 1_700_000_000_000,
        etag: ETag::new("e").unwrap(),
        inherited_identity: None,
        metadata: BTreeMap::from([
            ("x-amz-meta-skys3-wid".into(), "c/b/0/1.2".into()),
            ("x-amz-meta-kept".into(), "yes".into()),
        ]),
        tags: BTreeMap::new(),
        checksums: BTreeMap::new(),
        copy_source: None,
        data: PutData::Inline(Bytes::from_static(b"hi")),
    };
    setup
        .shards
        .write(&shard, RecordBody::Put(put), Precondition::None)
        .await
        .unwrap()
        .unwrap();
    for answer in [
        setup.get("adopted", &[]).await,
        setup.head("adopted", &[]).await,
    ] {
        answer.assert(200, None);
        assert!(
            answer.header("x-amz-meta-skys3-wid").is_none(),
            "{answer:?}"
        );
        assert_eq!(answer.header("x-amz-meta-kept"), Some("yes"));
    }

    // Appending is not supported.
    setup
        .put("k", &[("x-amz-write-offset-bytes", "0")], Bytes::new())
        .await
        .assert(501, Some("NotImplemented"));
}

#[tokio::test]
async fn checksums_are_stored_and_returned() {
    let (setup, _) = local(config("")).await;
    // SHA-256 of "hello".
    let sha256 = "LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ=";
    let put = setup
        .put(
            "k",
            &[("x-amz-checksum-sha256", sha256)],
            Bytes::from_static(b"hello"),
        )
        .await;
    put.assert(200, None);
    assert_eq!(put.header("x-amz-checksum-sha256"), Some(sha256));
    let enabled = [("x-amz-checksum-mode", "ENABLED")];
    for answer in [
        setup.get("k", &enabled).await,
        setup.head("k", &enabled).await,
    ] {
        assert_eq!(answer.header("x-amz-checksum-sha256"), Some(sha256));
        assert_eq!(answer.header("x-amz-checksum-type"), Some("FULL_OBJECT"));
    }
    // Not for a range, nor without the mode.
    let ranged = setup
        .get(
            "k",
            &[("x-amz-checksum-mode", "ENABLED"), ("range", "bytes=0-1")],
        )
        .await;
    assert!(ranged.header("x-amz-checksum-sha256").is_none());
    assert!(
        setup
            .get("k", &[])
            .await
            .header("x-amz-checksum-sha256")
            .is_none()
    );

    // A body that fails its checks leaves no object behind.
    let wrong = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    setup
        .put(
            "bad",
            &[("x-amz-checksum-sha256", wrong)],
            Bytes::from_static(b"hello"),
        )
        .await
        .assert(400, Some("BadDigest"));
    setup
        .put(
            "bad",
            &[("content-md5", "AAAAAAAAAAAAAAAAAAAAAA==")],
            Bytes::new(),
        )
        .await
        .assert(400, Some("BadDigest"));
    assert_eq!(setup.head("bad", &[]).await.status, 404);
}

/// A body whose stream fails after some bytes.
struct Failing {
    sent: bool,
}

impl http_body::Body for Failing {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Bytes>, std::io::Error>>> {
        if self.sent {
            return std::task::Poll::Ready(Some(Err(std::io::Error::other("reset"))));
        }
        self.sent = true;
        std::task::Poll::Ready(Some(Ok(http_body::Frame::data(fill(5000)))))
    }
}

#[tokio::test]
async fn a_body_that_fails_commits_nothing() {
    let (setup, _) = local(small_extents()).await;
    setup
        .put("k", &[], Bytes::from_static(b"old"))
        .await
        .assert(200, None);
    let request = Request::put("/photos/k")
        .body(Body::http_body(Failing { sent: false }))
        .unwrap();
    setup
        .send(request)
        .await
        .assert(400, Some("IncompleteBody"));
    // Its extents were written, but no PUT references them.
    assert_eq!(setup.get("k", &[]).await.body, "old");

    let request = Request::put("/photos/k")
        .header("content-length", "6000000000")
        .body(Body::empty())
        .unwrap();
    setup
        .send(request)
        .await
        .assert(400, Some("EntityTooLarge"));
}

#[tokio::test]
async fn deletes_leave_tombstones_only_where_a_flush_needs_them() {
    let setup = setup_with(config("")).await;
    let local = setup.create_local("photos").await;
    let remote = setup.create_write_back("remote").await;
    for name in ["photos", "remote"] {
        let uri = format!("/{name}/k");
        let put = with_body(Method::PUT, &uri, &[], Bytes::from_static(b"x"));
        setup.send(put).await.assert(200, None);
        setup
            .call(Method::DELETE, &uri, &[], "")
            .await
            .assert(204, None);
    }
    // A write-back bucket keeps the tombstone until the delete is flushed.
    let shard = ShardRef::for_key(&remote, "k");
    let entry = setup.shards.entry(&shard, "k").await.unwrap().unwrap();
    assert!(entry.object.is_none());
    // A local bucket's is removed, shortly after the answer.
    let shard = ShardRef::for_key(&local, "k");
    for _ in 0..100 {
        if setup.shards.entry(&shard, "k").await.unwrap().is_none() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("the local tombstone was not removed");
}

#[tokio::test]
async fn stubs_without_local_bytes_are_not_served_yet() {
    let (setup, bucket) = local(config("")).await;
    let shard = ShardRef::for_key(&bucket, "imported");
    let import = RecordBody::Import(Import {
        key: "imported".into(),
        size: 3,
        last_modified_ms: 1_600_000_000_000,
        etag: ETag::new("abc").unwrap(),
        storage_class: None,
    });
    setup
        .shards
        .write(&shard, import, Precondition::None)
        .await
        .unwrap()
        .unwrap();
    let head = setup.head("imported", &[]).await;
    head.assert(200, None);
    assert_eq!(head.header("etag"), Some("\"abc\""));
    let got = setup.get("imported", &[]).await;
    got.assert(503, Some("ServiceUnavailable"));
}

#[tokio::test]
async fn the_stored_storage_class_is_returned_unless_standard() {
    let (setup, bucket) = local(config("")).await;
    for (key, class) in [
        ("glacier", Some("GLACIER")),
        ("standard", Some("STANDARD")),
        ("none", None),
    ] {
        let import = RecordBody::Import(Import {
            key: key.into(),
            size: 3,
            last_modified_ms: 1_600_000_000_000,
            etag: ETag::new("abc").unwrap(),
            storage_class: class.map(str::to_owned),
        });
        let shard = ShardRef::for_key(&bucket, key);
        setup
            .shards
            .write(&shard, import, Precondition::None)
            .await
            .unwrap()
            .unwrap();
    }
    let head = setup.head("glacier", &[]).await;
    head.assert(200, None);
    assert_eq!(head.header("x-amz-storage-class"), Some("GLACIER"));
    for key in ["standard", "none"] {
        let head = setup.head(key, &[]).await;
        head.assert(200, None);
        assert!(head.header("x-amz-storage-class").is_none(), "{head:?}");
    }
    // An object written through the gateway is STANDARD.
    setup
        .put("cat", &[], Bytes::from_static(b"meow"))
        .await
        .assert(200, None);
    let got = setup.get("cat", &[]).await;
    got.assert(200, None);
    assert!(got.header("x-amz-storage-class").is_none(), "{got:?}");
}

#[tokio::test]
async fn response_header_overrides_replace_the_stored_headers() {
    let (setup, _) = local(config("")).await;
    setup
        .put(
            "cat.txt",
            &[
                ("content-type", "text/plain"),
                ("cache-control", "max-age=60"),
            ],
            Bytes::from_static(b"meow"),
        )
        .await
        .assert(200, None);
    let query = "response-content-type=image%2Fpng&response-content-language=fr\
                 &response-expires=Thu%2C%2001%20Dec%202033%2016%3A00%3A00%20GMT\
                 &response-cache-control=no-store\
                 &response-content-disposition=attachment%3B%20filename%3D%22c.txt%22\
                 &response-content-encoding=gzip";
    let uri = format!("/photos/cat.txt?{query}");
    let expected = [
        ("content-type", "image/png"),
        ("content-language", "fr"),
        ("expires", "Thu, 01 Dec 2033 16:00:00 GMT"),
        ("cache-control", "no-store"),
        ("content-disposition", "attachment; filename=\"c.txt\""),
        ("content-encoding", "gzip"),
    ];
    for method in [Method::GET, Method::HEAD] {
        let answer = setup.call(method.clone(), &uri, &[], "").await;
        answer.assert(200, None);
        for (name, value) in expected {
            assert_eq!(
                answer.header(name),
                Some(value),
                "{method} {name}: {answer:?}"
            );
        }
        assert_eq!(answer.header("content-length"), Some("4"));
    }
    // One override leaves the other stored headers as they are.
    let uri = "/photos/cat.txt?response-content-type=text%2Fhtml";
    for method in [Method::GET, Method::HEAD] {
        let answer = setup.call(method.clone(), uri, &[], "").await;
        answer.assert(200, None);
        assert_eq!(answer.header("content-type"), Some("text/html"), "{method}");
        assert_eq!(
            answer.header("cache-control"),
            Some("max-age=60"),
            "{method}"
        );
    }
    // A value that is not a valid header value is refused.
    let uri = "/photos/cat.txt?response-cache-control=a%0Ab";
    for method in [Method::GET, Method::HEAD] {
        let answer = setup.call(method.clone(), uri, &[], "").await;
        assert_eq!(answer.status, 400, "{method}: {answer:?}");
    }
    // Overrides do not turn a missing object into anything else.
    let uri = "/photos/missing?response-content-type=text%2Fhtml";
    let answer = setup.call(Method::GET, uri, &[], "").await;
    answer.assert(404, Some("NoSuchKey"));
}

#[tokio::test]
async fn anonymous_requests_cannot_override_response_headers() {
    let allow_all = r#"{"Version": "2012-10-17", "Statement": {"Effect": "Allow", "Action": "*", "Resource": "*"}}"#;
    let config = config(&format!(
        "[buckets.defaults]\nmode = \"local\"\n[identity]\nanonymous_access = true\nanonymous_policy = '{allow_all}'\n"
    ));
    let clock = Arc::new(ManualWallClock::new(Duration::from_secs(NOW)));
    let auth = SigV4Authenticator::new(StaticCredentials::new(), clock);
    let gateway = gateway_with(config, auth).await;
    let send = async |method, uri: &str, body: &'static [u8]| {
        let request = with_body(method, uri, &[], Bytes::from_static(body));
        common::answer(gateway.handle(request).await).await
    };
    send(Method::PUT, "/photos", b"").await.assert(200, None);
    send(Method::PUT, "/photos/cat.txt", b"meow")
        .await
        .assert(200, None);
    let got = send(Method::GET, "/photos/cat.txt", b"").await;
    got.assert(200, None);
    assert_eq!(got.body, "meow");
    let uri = "/photos/cat.txt?response-content-type=text%2Fhtml";
    send(Method::GET, uri, b"")
        .await
        .assert(400, Some("InvalidRequest"));
    let head = send(Method::HEAD, uri, b"").await;
    assert_eq!(head.status, 400, "{head:?}");
}

#[tokio::test]
async fn unavailable_shards_answer_503() {
    let (setup, _) = local(small_extents()).await;
    setup.shards.set_unavailable(true);
    let put = setup.put("k", &[], Bytes::from_static(b"x")).await;
    put.assert(503, Some("ServiceUnavailable"));
    // A streamed body fails on its first extent.
    let put = setup.put("k", &[], fill(5000)).await;
    put.assert(503, Some("ServiceUnavailable"));
    setup
        .get("k", &[])
        .await
        .assert(503, Some("ServiceUnavailable"));
    setup
        .delete("k", &[])
        .await
        .assert(503, Some("ServiceUnavailable"));
}

/// A gateway over the control store of `setup` and shards on `disk` after
/// a power loss, with the shards of `bucket` open, as a restarted node
/// has them.
async fn restart(setup: &Setup, bucket: &BucketDocument) -> Setup {
    let shards = MemoryShards::open(setup.shards.disk().clone())
        .await
        .unwrap();
    for shard in ShardRef::all(bucket) {
        shards.open(&shard, bucket).await.unwrap();
    }
    let gateway = Gateway::new(
        small_extents(),
        setup.store.clone(),
        shards.clone(),
        IdSource::seeded(9),
        TrustAll,
    )
    .await
    .unwrap();
    Setup {
        gateway,
        store: setup.store.clone(),
        memory: setup.memory.clone(),
        shards,
    }
}

/// A PUT is answered only once its record is durable: every acknowledged
/// object survives a power loss, and a PUT whose sync fails is not
/// acknowledged.
#[tokio::test]
async fn acknowledged_writes_survive_a_power_loss() {
    let (setup, bucket) = local(small_extents()).await;
    let mut acknowledged = BTreeMap::new();
    for n in 0..12 {
        let key = format!("k{n}");
        let data = fill(if n % 3 == 0 { 3500 + n } else { 40 + n });
        setup.put(&key, &[], data.clone()).await.assert(200, None);
        acknowledged.insert(key, data);
    }
    setup.delete("k1", &[]).await.assert(204, None);
    acknowledged.remove("k1");

    // The next sync fails: the disk goes out of service, and the PUT whose
    // record it covered is not acknowledged.
    setup.shards.disk().fail_next_syncs(1);
    let failed = setup.put("lost", &[], fill(100)).await;
    failed.assert(503, Some("ServiceUnavailable"));
    let after = setup.put("after", &[], fill(100)).await;
    after.assert(503, Some("ServiceUnavailable"));

    setup.shards.disk().crash();
    let restarted = restart(&setup, &bucket).await;
    drop(setup);
    for (key, data) in &acknowledged {
        let got = restarted.get(key, &[]).await;
        got.assert(200, None);
        assert_eq!(got.body.as_bytes(), &data[..], "{key}");
    }
    restarted
        .get("k1", &[])
        .await
        .assert(404, Some("NoSuchKey"));
    restarted
        .get("after", &[])
        .await
        .assert(404, Some("NoSuchKey"));
    // The restarted node writes again.
    restarted
        .put("after", &[], fill(2000))
        .await
        .assert(200, None);
    assert_eq!(
        restarted.get("after", &[]).await.body.as_bytes(),
        &fill(2000)[..]
    );
}
