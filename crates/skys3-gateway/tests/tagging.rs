//! Object tagging through the gateway's pipeline, over real shards on a
//! simulated disk: tags given with a PUT, GetObjectTagging,
//! PutObjectTagging and DeleteObjectTagging as `TAGS` records,
//! `x-amz-tagging-count`, and S3's limits.

mod common;

use std::collections::BTreeMap;

use bytes::Bytes;
use common::signing::request as with_body;
use common::{Answer, Setup, config, setup_with};
use http::Method;
use skys3_gateway::stub::MemoryShards;
use skys3_gateway::{MAX_OBJECT_TAGS, ShardRef, Shards};
use skys3_index::{Entry, EntryState};
use skys3_types::BucketDocument;

async fn local() -> (Setup, BucketDocument) {
    let setup = setup_with(config("")).await;
    let bucket = setup.create_local("photos").await;
    (setup, bucket)
}

impl Setup {
    async fn put(&self, key: &str, headers: &[(&str, &str)]) -> Answer {
        let uri = format!("/photos/{key}");
        let body = Bytes::from_static(b"meow");
        self.send(with_body(Method::PUT, &uri, headers, body)).await
    }

    async fn tagging(
        &self,
        method: Method,
        key: &str,
        headers: &[(&str, &str)],
        body: &str,
    ) -> Answer {
        self.call(method, &format!("/photos/{key}?tagging"), headers, body)
            .await
    }

    async fn entry(&self, bucket: &BucketDocument, key: &str) -> Entry {
        let shard = ShardRef::for_key(bucket, key);
        self.shards.entry(&shard, key).await.unwrap().unwrap()
    }
}

impl Answer {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(|value| value.to_str().unwrap())
    }
}

/// A PutObjectTagging body.
fn tagging_xml(tags: &[(&str, &str)]) -> String {
    let tags: String = tags
        .iter()
        .map(|(key, value)| format!("<Tag><Key>{key}</Key><Value>{value}</Value></Tag>"))
        .collect();
    format!("<Tagging><TagSet>{tags}</TagSet></Tagging>")
}

fn tags(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect()
}

#[tokio::test]
async fn tags_are_stored_read_replaced_and_deleted() {
    let (setup, bucket) = local().await;
    setup
        .put(
            "cat",
            &[("x-amz-tagging", "size=small&kind=cat%20%2B%20dog&empty=")],
        )
        .await
        .assert(200, None);
    let entry = setup.entry(&bucket, "cat").await;
    let put_version = entry.version;
    assert_eq!(
        entry.object.as_ref().unwrap().tags,
        tags(&[("empty", ""), ("kind", "cat + dog"), ("size", "small")]),
        "tags given with the PUT are in its record"
    );

    let got = setup.tagging(Method::GET, "cat", &[], "").await;
    got.assert(200, None);
    assert!(
        got.body.contains(
            "<TagSet><Tag><Key>empty</Key><Value></Value></Tag>\
             <Tag><Key>kind</Key><Value>cat + dog</Value></Tag>\
             <Tag><Key>size</Key><Value>small</Value></Tag></TagSet>"
        ),
        "{got:?}"
    );
    let head = setup.call(Method::HEAD, "/photos/cat", &[], "").await;
    assert_eq!(head.header("x-amz-tagging-count"), Some("3"));
    let etag = head.header("etag").unwrap().to_owned();
    let modified = head.header("last-modified").unwrap().to_owned();

    // PutObjectTagging replaces the whole set, as a new version with the
    // same bytes, ETag, and Last-Modified.
    let body = tagging_xml(&[("color", "grey"), ("ünïcödé", "välüé:/@=+-._ ")]);
    setup
        .tagging(Method::PUT, "cat", &[], &body)
        .await
        .assert(200, None);
    let entry = setup.entry(&bucket, "cat").await;
    assert!(entry.version > put_version);
    assert_eq!(entry.state, EntryState::Dirty);
    let object = entry.object.unwrap();
    assert_eq!(
        object.tags,
        tags(&[("color", "grey"), ("ünïcödé", "välüé:/@=+-._ ")])
    );
    assert_eq!(object.write_identity, None, "the identity names the TAGS");
    let got = setup.call(Method::GET, "/photos/cat", &[], "").await;
    assert_eq!(got.body, "meow");
    assert_eq!(got.header("etag"), Some(etag.as_str()));
    assert_eq!(got.header("last-modified"), Some(modified.as_str()));
    assert_eq!(got.header("x-amz-tagging-count"), Some("2"));

    let deleted = setup.tagging(Method::DELETE, "cat", &[], "").await;
    deleted.assert(204, None);
    let got = setup.tagging(Method::GET, "cat", &[], "").await;
    got.assert(200, None);
    assert!(
        got.body.contains("<TagSet></TagSet>") || got.body.contains("<TagSet/>"),
        "{got:?}"
    );
    let head = setup.call(Method::HEAD, "/photos/cat", &[], "").await;
    assert_eq!(head.header("x-amz-tagging-count"), None);

    // An overwrite without tags has none.
    setup
        .tagging(Method::PUT, "cat", &[], &tagging_xml(&[("a", "b")]))
        .await
        .assert(200, None);
    setup.put("cat", &[]).await.assert(200, None);
    assert!(
        setup
            .entry(&bucket, "cat")
            .await
            .object
            .unwrap()
            .tags
            .is_empty()
    );
}

#[tokio::test]
async fn tagging_needs_an_object() {
    let (setup, _) = local().await;
    let body = tagging_xml(&[("a", "b")]);
    for method in [Method::GET, Method::PUT, Method::DELETE] {
        setup
            .tagging(method.clone(), "absent", &[], &body)
            .await
            .assert(404, Some("NoSuchKey"));
    }
    setup.put("gone", &[]).await.assert(200, None);
    setup
        .call(Method::DELETE, "/photos/gone", &[], "")
        .await
        .assert(204, None);
    setup
        .tagging(Method::PUT, "gone", &[], &body)
        .await
        .assert(404, Some("NoSuchKey"));
    setup
        .call(Method::GET, "/missing/key?tagging", &[], "")
        .await
        .assert(404, Some("NoSuchBucket"));
}

#[tokio::test]
async fn tags_follow_s3s_limits() {
    let (setup, _) = local().await;
    setup.put("cat", &[]).await.assert(200, None);
    let long_key = "k".repeat(129);
    let long_value = "v".repeat(257);
    let widest_key = "ü".repeat(128);
    let widest_value = "語".repeat(256);
    let eleven: Vec<(String, String)> = (0..=MAX_OBJECT_TAGS)
        .map(|n| (format!("k{n}"), String::new()))
        .collect();
    let eleven: Vec<(&str, &str)> = eleven
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let refused: &[&[(&str, &str)]] = &[
        &eleven,
        &[(&long_key, "v")],
        &[("k", &long_value)],
        &[("bad<", "v")],
        &[("k", "semi;colon")],
        &[("aws:reserved", "v")],
        &[("dup", "1"), ("dup", "2")],
    ];
    for tags in refused {
        let body = tagging_xml(tags).replace("bad<", "bad&lt;");
        setup
            .tagging(Method::PUT, "cat", &[], &body)
            .await
            .assert(400, Some("InvalidTag"));
    }
    setup
        .tagging(Method::PUT, "cat", &[], &tagging_xml(&eleven[1..]))
        .await
        .assert(200, None);
    setup
        .tagging(
            Method::PUT,
            "cat",
            &[],
            &tagging_xml(&[(&widest_key, &widest_value)]),
        )
        .await
        .assert(200, None);
    setup
        .tagging(
            Method::PUT,
            "cat",
            &[],
            "<Tagging><TagSet><Tag><Key>k</Key></Tag></TagSet></Tagging>",
        )
        .await
        .assert(400, Some("MalformedXML"));

    // The same limits hold for x-amz-tagging, whose own syntax errors are
    // InvalidArgument.
    let header = |tags: &str| [("x-amz-tagging", tags.to_owned())];
    for (tags, status, code) in [
        (format!("{long_key}=v"), 400, "InvalidTag"),
        ("aws:x=1".to_owned(), 400, "InvalidTag"),
        ("a=1&a=2".to_owned(), 400, "InvalidArgument"),
        (
            (0..=MAX_OBJECT_TAGS)
                .map(|n| format!("k{n}=v"))
                .collect::<Vec<_>>()
                .join("&"),
            400,
            "InvalidTag",
        ),
    ] {
        let headers = header(&tags);
        let headers: Vec<(&str, &str)> = headers.iter().map(|(n, v)| (*n, v.as_str())).collect();
        setup.put("new", &headers).await.assert(status, Some(code));
    }
    setup
        .call(Method::HEAD, "/photos/new", &[], "")
        .await
        .assert(404, Some("NoSuchKey"));
}

#[tokio::test]
async fn tagging_bodies_are_checked_against_their_digest() {
    let (setup, bucket) = local().await;
    setup.put("cat", &[]).await.assert(200, None);
    let body = tagging_xml(&[("a", "b")]);
    let md5 = {
        use base64::Engine as _;
        use md5::Digest as _;
        base64::engine::general_purpose::STANDARD.encode(md5::Md5::digest(body.as_bytes()))
    };
    setup
        .tagging(
            Method::PUT,
            "cat",
            &[("content-md5", "1B2M2Y8AsgTpgAmY7PhCfg==")],
            &body,
        )
        .await
        .assert(400, Some("BadDigest"));
    setup
        .tagging(
            Method::PUT,
            "cat",
            &[("x-amz-checksum-crc32", "AAAAAA==")],
            &body,
        )
        .await
        .assert(400, Some("BadDigest"));
    assert!(
        setup
            .entry(&bucket, "cat")
            .await
            .object
            .unwrap()
            .tags
            .is_empty()
    );
    setup
        .tagging(Method::PUT, "cat", &[("content-md5", &md5)], &body)
        .await
        .assert(200, None);
}

#[tokio::test]
async fn tags_survive_a_crash() {
    let (setup, bucket) = local().await;
    setup
        .put("cat", &[("x-amz-tagging", "a=1")])
        .await
        .assert(200, None);
    setup
        .tagging(Method::PUT, "cat", &[], &tagging_xml(&[("b", "2")]))
        .await
        .assert(200, None);
    let before = setup.entry(&bucket, "cat").await;
    setup.shards.disk().crash();
    let shards = MemoryShards::open(setup.shards.disk().clone())
        .await
        .unwrap();
    let shard = ShardRef::for_key(&bucket, "cat");
    shards.open(&shard, &bucket).await.unwrap();
    let after = shards.entry(&shard, "cat").await.unwrap().unwrap();
    assert_eq!(after, before);
    assert_eq!(after.object.unwrap().tags, tags(&[("b", "2")]));
}
