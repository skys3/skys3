//! DeleteObjects through the gateway's pipeline, over real shards on a
//! simulated disk: per-key results, quiet mode, per-key conditions, the
//! integrity header S3 requires, and the 1,000-key limit.

mod common;

use std::time::Duration;

use bytes::Bytes;
use common::signing::request as with_body;
use common::{Answer, Setup, config, setup_with};
use http::Method;
use skys3_gateway::checksum::digest;
use skys3_gateway::{MAX_DELETE_KEYS, ShardRef, Shards};
use skys3_types::BucketDocument;
use skys3_types::checksum::ChecksumAlgorithm;

async fn local() -> (Setup, BucketDocument) {
    let setup = setup_with(config("[buckets.defaults]\nshards_per_bucket = 4\n")).await;
    let bucket = setup.create_local("photos").await;
    (setup, bucket)
}

fn md5(body: &str) -> String {
    use base64::Engine as _;
    use md5::Digest as _;
    base64::engine::general_purpose::STANDARD.encode(md5::Md5::digest(body.as_bytes()))
}

/// A DeleteObjects body for `objects`, each the inside of an `<Object>`.
fn delete_xml(quiet: bool, objects: &[String]) -> String {
    let objects: String = objects
        .iter()
        .map(|object| format!("<Object>{object}</Object>"))
        .collect();
    let quiet = if quiet { "<Quiet>true</Quiet>" } else { "" };
    format!("<Delete>{quiet}{objects}</Delete>")
}

fn key(key: &str) -> String {
    format!("<Key>{key}</Key>")
}

impl Setup {
    async fn put(&self, key: &str) -> Answer {
        let uri = format!("/photos/{key}");
        let body = Bytes::from_static(b"meow");
        self.send(with_body(Method::PUT, &uri, &[], body)).await
    }

    async fn delete_objects(&self, body: &str) -> Answer {
        let md5 = md5(body);
        self.call(
            Method::POST,
            "/photos?delete",
            &[("content-md5", &md5)],
            body,
        )
        .await
    }

    async fn exists(&self, key: &str) -> bool {
        let uri = format!("/photos/{key}");
        self.call(Method::HEAD, &uri, &[], "").await.status == 200
    }
}

#[tokio::test]
async fn keys_are_deleted_and_reported() {
    let (setup, bucket) = local().await;
    for name in ["a", "b", "c/d", "ünï"] {
        setup.put(name).await.assert(200, None);
    }
    let body = delete_xml(
        false,
        &[key("a"), key("c/d"), key("ünï"), key("absent"), key("a")],
    );
    let answer = setup.delete_objects(&body).await;
    answer.assert(200, None);
    for name in ["a", "c/d", "ünï", "absent"] {
        assert!(
            answer
                .body
                .contains(&format!("<Deleted><Key>{name}</Key></Deleted>")),
            "{name}: {answer:?}"
        );
    }
    assert!(!answer.body.contains("<Error>"), "{answer:?}");
    for name in ["a", "c/d", "ünï"] {
        assert!(!setup.exists(name).await, "{name}");
    }
    assert!(setup.exists("b").await);

    // In a local bucket the tombstones go away once their FLUSHED applies.
    let shard = ShardRef::for_key(&bucket, "a");
    let mut gone = false;
    for _ in 0..100 {
        if setup.shards.entry(&shard, "a").await.unwrap().is_none() {
            gone = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(gone, "the tombstone of a was removed");
}

/// The answer echoes keys and version IDs in XML, which cannot carry
/// control characters: such a request deletes nothing (M7-03).
#[tokio::test]
async fn keys_with_control_characters_are_refused_before_any_delete() {
    let (setup, _) = local().await;
    setup.put("a").await.assert(200, None);
    for (odd, code) in [
        (key("bell\u{7}"), "InvalidArgument"),
        (
            format!("{}<VersionId>\u{1}</VersionId>", key("b")),
            "InvalidArgument",
        ),
        // The XML parser refuses a reference to a control character.
        (key("bell&#x7;"), "MalformedXML"),
    ] {
        let body = delete_xml(false, &[key("a"), odd.clone()]);
        let answer = setup.delete_objects(&body).await;
        assert_eq!(
            (answer.status.as_u16(), answer.code()),
            (400, Some(code)),
            "{odd}"
        );
        assert!(setup.exists("a").await, "{odd}");
    }
    let whitespace = delete_xml(false, &[key("a"), key("tab\there")]);
    setup.delete_objects(&whitespace).await.assert(200, None);
    assert!(!setup.exists("a").await);
}

#[tokio::test]
async fn quiet_mode_lists_only_errors() {
    let (setup, _) = local().await;
    setup.put("a").await.assert(200, None);
    setup.put("b").await.assert(200, None);
    let body = delete_xml(
        true,
        &[
            key("a"),
            format!("{}<VersionId>3HL4kqtJlcpXroDTDmJ</VersionId>", key("b")),
        ],
    );
    let answer = setup.delete_objects(&body).await;
    assert_eq!(answer.status, 200, "{answer:?}");
    assert!(!answer.body.contains("<Deleted>"), "{answer:?}");
    assert!(
        answer.body.contains(
            "<Error><Code>InvalidArgument</Code><Key>b</Key>\
             <Message>Invalid version id specified</Message>\
             <VersionId>3HL4kqtJlcpXroDTDmJ</VersionId></Error>"
        ),
        "{answer:?}"
    );
    assert!(!setup.exists("a").await);
    assert!(setup.exists("b").await, "a refused key is kept");

    let answer = setup.delete_objects(&delete_xml(true, &[key("b")])).await;
    answer.assert(200, None);
    assert!(!answer.body.contains("<Deleted>") && !answer.body.contains("<Error>"));
}

#[tokio::test]
async fn keys_carry_their_own_conditions() {
    let (setup, _) = local().await;
    let etag = setup.put("a").await.headers["etag"]
        .to_str()
        .unwrap()
        .to_owned();
    setup.put("b").await.assert(200, None);
    setup.put("c").await.assert(200, None);
    let body = delete_xml(
        false,
        &[
            format!("{}<ETag>{etag}</ETag>", key("a")),
            format!("{}<ETag>\"0123\"</ETag>", key("b")),
            format!("{}<ETag>{etag}</ETag>", key("absent")),
            format!("{}<Size>4</Size>", key("c")),
            format!("{}<VersionId>null</VersionId>", key("c")),
        ],
    );
    let answer = setup.delete_objects(&body).await;
    assert_eq!(answer.status, 200, "{answer:?}");
    for expected in [
        "<Deleted><Key>a</Key></Deleted>".to_owned(),
        "<Error><Code>PreconditionFailed</Code><Key>b</Key>".to_owned(),
        "<Error><Code>NoSuchKey</Code><Key>absent</Key>".to_owned(),
        "<Error><Code>NotImplemented</Code><Key>c</Key>".to_owned(),
        "<Deleted><Key>c</Key><VersionId>null</VersionId></Deleted>".to_owned(),
    ] {
        assert!(answer.body.contains(&expected), "{expected}: {answer:?}");
    }
    assert!(!setup.exists("a").await);
    assert!(setup.exists("b").await);
    assert!(!setup.exists("c").await);
}

#[tokio::test]
async fn requests_are_checked_as_s3_checks_them() {
    let (setup, _) = local().await;
    setup.put("a").await.assert(200, None);
    let body = delete_xml(false, &[key("a")]);
    let call = async |headers: &[(&str, &str)], body: &str| {
        setup
            .call(Method::POST, "/photos?delete", headers, body)
            .await
    };
    call(&[], &body).await.assert(400, Some("InvalidRequest"));
    call(&[("content-md5", &md5("other"))], &body)
        .await
        .assert(400, Some("BadDigest"));
    call(&[("content-md5", "not base64")], &body)
        .await
        .assert(400, Some("InvalidDigest"));
    assert!(setup.exists("a").await);

    // A flexible checksum serves as well as Content-MD5.
    let crc32 = {
        use base64::Engine as _;
        let digest = digest(ChecksumAlgorithm::Crc32, body.as_bytes());
        base64::engine::general_purpose::STANDARD.encode(digest)
    };
    call(&[("x-amz-checksum-crc32", &crc32)], &body)
        .await
        .assert(200, None);
    assert!(!setup.exists("a").await);

    let empty_key = delete_xml(false, &[key("")]);
    setup
        .delete_objects(&empty_key)
        .await
        .assert(400, Some("UserKeyMustBeSpecified"));
    let long_key = delete_xml(false, &[key(&"k".repeat(1025))]);
    let answer = setup.delete_objects(&long_key).await;
    assert!(
        answer.body.contains("<Code>KeyTooLongError</Code>"),
        "{answer:?}"
    );
    setup
        .delete_objects("<Delete></Delete>")
        .await
        .assert(400, Some("MalformedXML"));
    setup
        .call(
            Method::POST,
            "/missing?delete",
            &[("content-md5", &md5(&body))],
            &body,
        )
        .await
        .assert(404, Some("NoSuchBucket"));
}

#[tokio::test]
async fn up_to_a_thousand_keys_are_deleted_at_once() {
    let (setup, bucket) = local().await;
    let names: Vec<String> = (0..MAX_DELETE_KEYS).map(|n| format!("k{n:04}")).collect();
    for name in names.iter().step_by(97) {
        setup.put(name).await.assert(200, None);
    }
    let keys: Vec<String> = names.iter().map(|name| key(name)).collect();
    let answer = setup.delete_objects(&delete_xml(false, &keys)).await;
    answer.assert(200, None);
    assert_eq!(answer.body.matches("<Deleted>").count(), MAX_DELETE_KEYS);
    let shards: std::collections::BTreeSet<_> = names
        .iter()
        .map(|name| ShardRef::for_key(&bucket, name))
        .collect();
    assert_eq!(shards.len(), 4, "the keys span every shard");
    for name in names.iter().step_by(97) {
        assert!(!setup.exists(name).await, "{name}");
    }

    let mut keys = keys;
    keys.push(key("one-too-many"));
    setup
        .delete_objects(&delete_xml(false, &keys))
        .await
        .assert(400, Some("MalformedXML"));
}
