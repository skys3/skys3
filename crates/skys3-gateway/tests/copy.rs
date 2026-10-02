//! CopyObject through the gateway's pipeline, over real shards on a
//! simulated disk: copies within a shard, across shards, and across
//! buckets, with both metadata and tagging directives, the source reference
//! the copy's `PUT` records, and the copy-source conditions.

mod common;

use std::collections::BTreeMap;

use bytes::Bytes;
use common::signing::request as with_body;
use common::{Answer, Setup, config, setup_with};
use http::Method;
use skys3_gateway::{GatewayConfig, Precondition, ShardRef, Shards};
use skys3_index::{Entry, EntryState, Payload};
use skys3_log::RecordBody;
use skys3_log::record::{CopySource, Import, Put, PutData};
use skys3_types::{BucketDocument, ETag, VersionIdentity};

/// Four shards per bucket; bodies over 1 KiB go in extents of 1,000 bytes.
fn small_extents() -> GatewayConfig {
    let mut config = config("[buckets.defaults]\nshards_per_bucket = 4\n");
    config.inline_max_bytes = 1024;
    config.extent_bytes = 1000;
    config
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

/// A key other than `key` whose shard in `bucket` is (or is not) `key`'s.
fn key_near(bucket: &BucketDocument, key: &str, same_shard: bool) -> String {
    let shard = ShardRef::for_key(bucket, key);
    (0..)
        .map(|n| format!("copy-{n}"))
        .find(|other| (ShardRef::for_key(bucket, other) == shard) == same_shard)
        .unwrap()
}

impl Setup {
    async fn put(&self, uri: &str, headers: &[(&str, &str)], body: Bytes) -> Answer {
        self.send(with_body(Method::PUT, uri, headers, body)).await
    }

    async fn copy(&self, to: &str, from: &str, headers: &[(&str, &str)]) -> Answer {
        let mut headers = headers.to_vec();
        headers.push(("x-amz-copy-source", from));
        self.call(Method::PUT, to, &headers, "").await
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

    /// The text of the first `<name>` element of the body.
    fn element(&self, name: &str) -> Option<&str> {
        let open = format!("<{name}>");
        let start = self.body.find(&open)? + open.len();
        let end = self.body[start..].find(&format!("</{name}>"))? + start;
        Some(&self.body[start..end])
    }
}

/// Request headers.
type Headers<'a> = &'a [(&'a str, &'a str)];

const SOURCE_HEADERS: Headers = &[
    ("content-type", "text/plain"),
    ("cache-control", "max-age=60"),
    ("x-amz-meta-color", "tabby"),
    ("x-amz-tagging", "kind=cat&size=small"),
];

/// The source's metadata, as stored.
fn source_metadata() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("cache-control".to_owned(), "max-age=60".to_owned()),
        ("content-type".to_owned(), "text/plain".to_owned()),
        ("x-amz-meta-color".to_owned(), "tabby".to_owned()),
    ])
}

fn source_tags() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("kind".to_owned(), "cat".to_owned()),
        ("size".to_owned(), "small".to_owned()),
    ])
}

#[tokio::test]
async fn copies_within_a_shard_keep_the_source_and_record_it() {
    let setup = setup_with(small_extents()).await;
    let photos = setup.create_local("photos").await;
    setup
        .put(
            "/photos/cat.txt",
            SOURCE_HEADERS,
            Bytes::from_static(b"meow"),
        )
        .await
        .assert(200, None);
    let source = setup.entry(&photos, "cat.txt").await;
    let target = key_near(&photos, "cat.txt", true);

    let copied = setup
        .copy(&format!("/photos/{target}"), "photos/cat.txt", &[])
        .await;
    copied.assert(200, None);
    let etag = md5_etag(b"meow");
    assert_eq!(copied.element("ETag"), Some(etag.as_str()));
    assert!(copied.element("LastModified").is_some(), "{copied:?}");

    let got = setup
        .call(Method::GET, &format!("/photos/{target}"), &[], "")
        .await;
    got.assert(200, None);
    assert_eq!(got.body, "meow");
    assert_eq!(got.header("etag"), Some(etag.as_str()));
    assert_eq!(got.header("content-type"), Some("text/plain"));
    assert_eq!(got.header("cache-control"), Some("max-age=60"));
    assert_eq!(got.header("x-amz-meta-color"), Some("tabby"));
    assert_eq!(got.header("x-amz-tagging-count"), Some("2"));

    let entry = setup.entry(&photos, &target).await;
    let object = entry.object.unwrap();
    assert_eq!(object.metadata, source_metadata());
    assert_eq!(object.tags, source_tags());
    let source_object = source.object.unwrap();
    assert_eq!(object.checksums, source_object.checksums);
    assert_eq!(object.write_identity, None, "the identity names the copy");
    assert_eq!(
        object.copy_source,
        Some(CopySource {
            bucket: photos.bucket_id.clone(),
            key: "cat.txt".to_owned(),
            version: VersionIdentity::new(source.version.seq, source_object.local_etag),
            remote_etag: None,
        }),
        "a dirty source has no remote ETag to copy from"
    );
    assert_eq!(entry.state, EntryState::Dirty);
    assert!(matches!(object.payload, Payload::Inline(position) if position == entry.version));

    // The copy is independent of its source.
    setup
        .put("/photos/cat.txt", &[], Bytes::from_static(b"purr"))
        .await
        .assert(200, None);
    let got = setup
        .call(Method::GET, &format!("/photos/{target}"), &[], "")
        .await;
    assert_eq!(got.body, "meow");
}

#[tokio::test]
async fn copies_across_shards_and_buckets_follow_both_directives() {
    let setup = setup_with(small_extents()).await;
    let photos = setup.create_local("photos").await;
    let archive = setup.create_local("archive").await;
    let data = fill(3500);
    setup
        .put("/photos/big.bin", SOURCE_HEADERS, data.clone())
        .await
        .assert(200, None);
    let other_shard = key_near(&photos, "big.bin", false);
    let targets = [
        (&photos, other_shard.as_str()),
        (&archive, "big.bin"),
        (&archive, "nested/big-copy.bin"),
    ];
    for (bucket, key) in targets {
        let uri = format!("/{}/{key}", bucket.name);
        // COPY, the default: the source's metadata and tags; the request's
        // are ignored.
        setup
            .copy(
                &uri,
                "/photos/big.bin",
                &[
                    ("x-amz-metadata-directive", "COPY"),
                    ("content-type", "image/png"),
                    ("x-amz-tagging", "ignored=yes"),
                ],
            )
            .await
            .assert(200, None);
        let entry = setup.entry(bucket, key).await;
        let object = entry.object.unwrap();
        assert_eq!(object.metadata, source_metadata(), "{key}");
        assert_eq!(object.tags, source_tags(), "{key}");
        let Payload::Extents(extents) = &object.payload else {
            panic!(
                "a 3,500-byte copy is stored in extents: {:?}",
                object.payload
            );
        };
        assert_eq!(extents.len(), 4);
        assert_eq!(object.copy_source.unwrap().bucket, photos.bucket_id);
        let got = setup.call(Method::GET, &uri, &[], "").await;
        assert_eq!(got.body.as_bytes(), &data[..], "{key}");
        assert_eq!(got.header("etag"), Some(md5_etag(&data).as_str()));

        // REPLACE: the request's metadata and tags.
        setup
            .copy(
                &uri,
                "photos/big.bin",
                &[
                    ("x-amz-metadata-directive", "REPLACE"),
                    ("x-amz-tagging-directive", "REPLACE"),
                    ("content-type", "image/png"),
                    ("x-amz-meta-shade", "grey"),
                    ("x-amz-tagging", "new=tag"),
                ],
            )
            .await
            .assert(200, None);
        let object = setup.entry(bucket, key).await.object.unwrap();
        assert_eq!(
            object.metadata,
            BTreeMap::from([
                ("content-type".to_owned(), "image/png".to_owned()),
                ("x-amz-meta-shade".to_owned(), "grey".to_owned()),
            ])
        );
        assert_eq!(
            object.tags,
            BTreeMap::from([("new".to_owned(), "tag".to_owned())])
        );
        let head = setup.call(Method::HEAD, &uri, &[], "").await;
        assert_eq!(head.header("content-type"), Some("image/png"));
        assert_eq!(head.header("x-amz-meta-color"), None);
        assert_eq!(head.header("x-amz-tagging-count"), Some("1"));

        // REPLACE of tags without x-amz-tagging leaves none.
        setup
            .copy(
                &uri,
                "photos/big.bin",
                &[("x-amz-tagging-directive", "REPLACE")],
            )
            .await
            .assert(200, None);
        let object = setup.entry(bucket, key).await.object.unwrap();
        assert!(object.tags.is_empty());
        assert_eq!(object.metadata, source_metadata());
    }
}

#[tokio::test]
async fn a_clean_sources_remote_etag_is_recorded() {
    let setup = setup_with(small_extents()).await;
    let remote = setup.create_write_back("remote").await;
    setup
        .put("/remote/clean", &[], Bytes::from_static(b"flushed"))
        .await
        .assert(200, None);
    setup.shards.flush(&remote.bucket_id).await;
    let source = setup.entry(&remote, "clean").await;
    assert_eq!(source.state, EntryState::Clean);

    setup
        .copy("/remote/copy", "remote/clean", &[])
        .await
        .assert(200, None);
    let copy = setup.entry(&remote, "copy").await.object.unwrap();
    let recorded = copy.copy_source.unwrap();
    assert_eq!(recorded.remote_etag, source.remote_etag);
    assert!(recorded.remote_etag.is_some());
    assert_eq!(recorded.version.seq, source.version.seq);
}

#[tokio::test]
async fn copy_source_conditions_answer_412() {
    let setup = setup_with(small_extents()).await;
    setup.create_local("photos").await;
    setup
        .put("/photos/cat", &[], Bytes::from_static(b"meow"))
        .await
        .assert(200, None);
    let etag = md5_etag(b"meow");
    let past = "Wed, 01 Jan 2020 00:00:00 GMT";
    let future = "Fri, 01 Jan 2100 00:00:00 GMT";
    let cases: &[(Headers, Option<&str>)] = &[
        (&[("x-amz-copy-source-if-match", &etag)], None),
        (
            &[("x-amz-copy-source-if-match", "\"0\"")],
            Some("PreconditionFailed"),
        ),
        (
            &[("x-amz-copy-source-if-none-match", &etag)],
            Some("PreconditionFailed"),
        ),
        (&[("x-amz-copy-source-if-none-match", "\"0\"")], None),
        (
            &[("x-amz-copy-source-if-modified-since", future)],
            Some("PreconditionFailed"),
        ),
        (&[("x-amz-copy-source-if-modified-since", past)], None),
        (
            &[("x-amz-copy-source-if-unmodified-since", past)],
            Some("PreconditionFailed"),
        ),
        (&[("x-amz-copy-source-if-unmodified-since", future)], None),
        // If-Match that holds wins over If-Unmodified-Since that does not,
        // as in S3.
        (
            &[
                ("x-amz-copy-source-if-match", &etag),
                ("x-amz-copy-source-if-unmodified-since", past),
            ],
            None,
        ),
    ];
    for (n, (headers, code)) in cases.iter().enumerate() {
        let answer = setup
            .copy(&format!("/photos/copy-{n}"), "photos/cat", headers)
            .await;
        match code {
            None => answer.assert(200, None),
            Some(code) => answer.assert(412, Some(code)),
        }
        let copied = setup
            .call(Method::HEAD, &format!("/photos/copy-{n}"), &[], "")
            .await;
        assert_eq!(copied.status == 200, code.is_none(), "{headers:?}");
    }

    // The destination's own preconditions, as on PutObject.
    setup
        .copy("/photos/copy-0", "photos/cat", &[("if-none-match", "*")])
        .await
        .assert(412, Some("PreconditionFailed"));
    setup
        .copy("/photos/absent", "photos/cat", &[("if-match", &etag)])
        .await
        .assert(404, Some("NoSuchKey"));
    setup
        .copy("/photos/copy-0", "photos/cat", &[("if-match", &etag)])
        .await
        .assert(200, None);
    setup
        .copy("/photos/fresh", "photos/cat", &[("if-none-match", "*")])
        .await
        .assert(200, None);
}

#[tokio::test]
async fn copies_onto_themselves_must_change_something() {
    let setup = setup_with(small_extents()).await;
    let photos = setup.create_local("photos").await;
    setup
        .put("/photos/cat", SOURCE_HEADERS, Bytes::from_static(b"meow"))
        .await
        .assert(200, None);
    // A storage class or website redirect is not stored (PutObject accepts
    // and ignores them), so setting one would change nothing either.
    for headers in [
        &[][..],
        &[("x-amz-tagging-directive", "REPLACE")][..],
        &[("x-amz-storage-class", "STANDARD_IA")][..],
        &[("x-amz-website-redirect-location", "/dog")][..],
    ] {
        setup
            .copy("/photos/cat", "photos/cat", headers)
            .await
            .assert(400, Some("InvalidRequest"));
    }
    let before = setup.entry(&photos, "cat").await;
    setup
        .copy(
            "/photos/cat",
            "photos/cat",
            &[
                ("x-amz-metadata-directive", "REPLACE"),
                ("content-type", "text/x-cat"),
            ],
        )
        .await
        .assert(200, None);
    let after = setup.entry(&photos, "cat").await;
    assert!(after.version > before.version);
    let object = after.object.unwrap();
    assert_eq!(object.local_etag, before.object.unwrap().local_etag);
    assert_eq!(object.tags, source_tags(), "the tags are copied");
    let got = setup.call(Method::GET, "/photos/cat", &[], "").await;
    assert_eq!(got.body, "meow");
    assert_eq!(got.header("content-type"), Some("text/x-cat"));
    assert_eq!(got.header("x-amz-meta-color"), None);
}

#[tokio::test]
async fn copies_are_refused_as_s3_refuses_them() {
    let setup = setup_with(small_extents()).await;
    let photos = setup.create_local("photos").await;
    setup
        .put("/photos/cat", &[], Bytes::from_static(b"meow"))
        .await
        .assert(200, None);
    let refusals: &[(&str, Headers, u16, &str)] = &[
        ("photos/absent", &[], 404, "NoSuchKey"),
        ("missing/cat", &[], 404, "NoSuchBucket"),
        (
            "photos/cat?versionId=3HL4kqtJlcpXroD",
            &[],
            400,
            "InvalidArgument",
        ),
        (
            "photos/cat",
            &[("x-amz-metadata-directive", "MERGE")],
            400,
            "InvalidArgument",
        ),
        (
            "photos/cat",
            &[("x-amz-tagging-directive", "MERGE")],
            400,
            "InvalidArgument",
        ),
        (
            "photos/cat",
            &[
                ("x-amz-tagging-directive", "REPLACE"),
                ("x-amz-tagging", "aws:reserved=1"),
            ],
            400,
            "InvalidTag",
        ),
        (
            "photos/cat",
            &[
                ("x-amz-metadata-directive", "REPLACE"),
                ("x-amz-meta-skys3-wid", "forged"),
            ],
            400,
            "InvalidArgument",
        ),
        (
            "photos/cat",
            &[("x-amz-checksum-algorithm", "SHA512")],
            501,
            "NotImplemented",
        ),
    ];
    for (source, headers, status, code) in refusals {
        setup
            .copy("/photos/copy", source, headers)
            .await
            .assert(*status, Some(code));
    }
    setup
        .copy("/photos/copy", "photos/cat?versionId=null", &[])
        .await
        .assert(200, None);

    // A deleted source is no source.
    setup
        .call(Method::DELETE, "/photos/cat", &[], "")
        .await
        .assert(204, None);
    setup
        .copy("/photos/copy", "photos/cat", &[])
        .await
        .assert(404, Some("NoSuchKey"));

    // An imported stub has no bytes here until read-through fill.
    let shard = ShardRef::for_key(&photos, "stub");
    let import = RecordBody::Import(Import {
        key: "stub".to_owned(),
        size: 4,
        last_modified_ms: 0,
        etag: ETag::new("0123").unwrap(),
        storage_class: None,
    });
    setup
        .shards
        .write(&shard, import, Precondition::None)
        .await
        .unwrap()
        .unwrap();
    setup
        .copy("/photos/copy", "photos/stub", &[])
        .await
        .assert(503, Some("ServiceUnavailable"));
}

#[tokio::test]
async fn copies_compute_a_requested_checksum() {
    let setup = setup_with(small_extents()).await;
    let photos = setup.create_local("photos").await;
    let data = fill(2500);
    let put = setup.put("/photos/big", &[], data.clone()).await;
    put.assert(200, None);
    let crc64 = put.header("x-amz-checksum-crc64nvme").unwrap().to_owned();

    let sha256 = {
        use aws_lc_rs::digest;
        use base64::Engine as _;
        let digest = digest::digest(&digest::SHA256, &data);
        base64::engine::general_purpose::STANDARD.encode(digest.as_ref())
    };
    let copied = setup
        .copy(
            "/photos/sha",
            "photos/big",
            &[("x-amz-checksum-algorithm", "SHA256")],
        )
        .await;
    copied.assert(200, None);
    assert_eq!(copied.element("ChecksumSHA256"), Some(sha256.as_str()));
    let head = setup
        .call(
            Method::HEAD,
            "/photos/sha",
            &[("x-amz-checksum-mode", "ENABLED")],
            "",
        )
        .await;
    assert_eq!(head.header("x-amz-checksum-sha256"), Some(sha256.as_str()));
    assert_eq!(head.header("x-amz-checksum-crc64nvme"), None);
    let object = setup.entry(&photos, "sha").await.object.unwrap();
    assert_eq!(object.checksums.len(), 1);

    // The source's own algorithm is kept, not recomputed.
    let copied = setup
        .copy(
            "/photos/crc",
            "photos/big",
            &[("x-amz-checksum-algorithm", "CRC64NVME")],
        )
        .await;
    copied.assert(200, None);
    assert_eq!(copied.element("ChecksumCRC64NVME"), Some(crc64.as_str()));
}

#[tokio::test]
async fn copies_never_carry_the_sources_write_identity() {
    let setup = setup_with(small_extents()).await;
    let photos = setup.create_local("photos").await;
    // An object adopted from the remote can carry the identity of the write
    // that put it there.
    let shard = ShardRef::for_key(&photos, "adopted");
    let put = RecordBody::Put(Put {
        key: "adopted".to_owned(),
        size: 2,
        last_modified_ms: 0,
        etag: ETag::new("5f4dcc3b5aa765d61d8327deb882cf99").unwrap(),
        inherited_identity: None,
        metadata: BTreeMap::from([
            ("x-amz-meta-skys3-wid".to_owned(), "c/b-1/0/1.2".to_owned()),
            ("x-amz-meta-color".to_owned(), "tabby".to_owned()),
        ]),
        tags: BTreeMap::new(),
        checksums: BTreeMap::new(),
        copy_source: None,
        data: PutData::Inline(Bytes::from_static(b"hi")),
    });
    setup
        .shards
        .write(&shard, put, Precondition::None)
        .await
        .unwrap()
        .unwrap();
    setup
        .copy("/photos/copy", "photos/adopted", &[])
        .await
        .assert(200, None);
    let object = setup.entry(&photos, "copy").await.object.unwrap();
    assert_eq!(
        object.metadata,
        BTreeMap::from([("x-amz-meta-color".to_owned(), "tabby".to_owned())])
    );
}
