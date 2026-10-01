//! Multipart uploads through the gateway's pipeline, over real shards on a
//! simulated disk: the upload's life cycle, ETags and checksums against
//! known answers, reads by part, listing, released bytes, and durability.

mod common;

use std::collections::BTreeSet;

use bytes::Bytes;
use common::signing::request as with_body;
use common::{Answer, Setup, config, setup_with};
use http::Method;
use skys3_gateway::checksum::digest;
use skys3_gateway::stub::MemoryShards;
use skys3_gateway::{
    Gateway, GatewayConfig, IdSource, ShardRef, Shards, TrustAll, parse_upload_id,
};
use skys3_index::Payload;
use skys3_types::checksum::{ChecksumAlgorithm, encode_digest};
use skys3_types::{BucketDocument, EpochSeq};

const MIB: usize = 1 << 20;
const PART: usize = 5 * MIB;

/// Bodies up to 64 KiB go inline, longer ones in 1 MiB extents.
fn mib_extents() -> GatewayConfig {
    let mut config = config("");
    config.inline_max_bytes = 64 * 1024;
    config.extent_bytes = MIB as u64;
    config
}

async fn local() -> (Setup, BucketDocument) {
    let setup = setup_with(mib_extents()).await;
    let bucket = setup.create_local("photos").await;
    (setup, bucket)
}

/// `n` bytes of `fill`, as the `s3-tests` suite makes its parts.
fn filled(n: usize, fill: u8) -> Bytes {
    vec![fill; n].into()
}

fn md5_hex(data: &[u8]) -> String {
    digest(ChecksumAlgorithm::Md5, data)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// The text of the first `<tag>` element of an XML body.
fn element<'a>(body: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let start = body.find(&open)? + open.len();
    let end = body[start..].find(&format!("</{tag}>"))? + start;
    Some(&body[start..end])
}

/// Every `<tag>` element of an XML body, in order.
fn elements<'a>(body: &'a str, tag: &str) -> Vec<&'a str> {
    let mut found = Vec::new();
    let mut rest = body;
    while let Some(text) = element(rest, tag) {
        found.push(text);
        let close = format!("</{tag}>");
        rest = &rest[rest.find(&close).unwrap() + close.len()..];
    }
    found
}

fn completion(parts: &[(u16, &str)]) -> String {
    let parts: String = parts
        .iter()
        .map(|(number, etag)| {
            format!("<Part><PartNumber>{number}</PartNumber><ETag>{etag}</ETag></Part>")
        })
        .collect();
    format!("<CompleteMultipartUpload>{parts}</CompleteMultipartUpload>")
}

impl Answer {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(|value| value.to_str().unwrap())
    }
}

impl Setup {
    async fn create_upload(&self, key: &str, headers: &[(&str, &str)]) -> String {
        let answer = self
            .call(Method::POST, &format!("/photos/{key}?uploads"), headers, "")
            .await;
        answer.assert(200, None);
        element(&answer.body, "UploadId").unwrap().to_owned()
    }

    async fn upload_part(
        &self,
        key: &str,
        id: &str,
        number: u16,
        headers: &[(&str, &str)],
        body: Bytes,
    ) -> Answer {
        let uri = format!("/photos/{key}?partNumber={number}&uploadId={id}");
        self.send(with_body(Method::PUT, &uri, headers, body)).await
    }

    /// Uploads a part that must succeed, and returns its quoted ETag.
    async fn part(&self, key: &str, id: &str, number: u16, body: Bytes) -> String {
        let answer = self.upload_part(key, id, number, &[], body).await;
        answer.assert(200, None);
        answer.header("etag").unwrap().to_owned()
    }

    async fn complete(
        &self,
        key: &str,
        id: &str,
        headers: &[(&str, &str)],
        parts: &[(u16, &str)],
    ) -> Answer {
        let uri = format!("/photos/{key}?uploadId={id}");
        self.call(Method::POST, &uri, headers, &completion(parts))
            .await
    }

    async fn abort(&self, key: &str, id: &str) -> Answer {
        let uri = format!("/photos/{key}?uploadId={id}");
        self.call(Method::DELETE, &uri, &[], "").await
    }

    async fn list_parts(&self, key: &str, id: &str, query: &str) -> Answer {
        let uri = format!("/photos/{key}?uploadId={id}{query}");
        self.call(Method::GET, &uri, &[], "").await
    }

    async fn list_uploads(&self, query: &str) -> Answer {
        let answer = self
            .call(Method::GET, &format!("/photos?uploads{query}"), &[], "")
            .await;
        answer.assert(200, None);
        answer
    }

    async fn get(&self, key: &str, headers: &[(&str, &str)], query: &str) -> Answer {
        let uri = format!("/photos/{key}{query}");
        self.call(Method::GET, &uri, headers, "").await
    }

    /// The positions whose records the node locates in `key`'s shard.
    async fn located(&self, bucket: &BucketDocument, key: &str) -> BTreeSet<EpochSeq> {
        let shard = ShardRef::for_key(bucket, key);
        let local = self.shards.local().set().get(&(&shard).into()).await;
        let dump = local.unwrap().index().read().unwrap().dump().unwrap();
        dump.locations.into_keys().map(|(_, p)| p).collect()
    }
}

/// The `s3-tests` suite's three 5 MiB parts of `A`, `B`, and `C`, uploaded
/// with SHA256 checksums: the ETag and the composite checksum are the ones
/// S3 gives the same parts, and the object reads back whole, by part, and
/// by range.
#[tokio::test]
async fn an_upload_completes_with_the_etag_and_checksum_s3_gives() {
    let (setup, bucket) = local().await;
    let id = setup
        .create_upload(
            "movie.mp4",
            &[
                ("x-amz-checksum-algorithm", "SHA256"),
                ("content-type", "video/mp4"),
                ("x-amz-meta-camera", "x100"),
            ],
        )
        .await;
    let parts = [filled(PART, b'A'), filled(PART, b'B'), filled(PART, b'C')];
    let expected_part_checksums = [
        "275VF5loJr1YYawit0XSHREhkFXYkkPKGuoK0x9VKxI=",
        "mrHwOfjTL5Zwfj74F05HOQGLdUb7E5szdCbxgUSq6NM=",
        "Vw7oB/nKQ5xWb3hNgbyfkvDiivl+U+/Dft48nfJfDow=",
    ];
    let mut etags = Vec::new();
    for (number, (part, checksum)) in (1..).zip(parts.iter().zip(expected_part_checksums)) {
        let answer = setup
            .upload_part(
                "movie.mp4",
                &id,
                number,
                &[("x-amz-checksum-sha256", checksum)],
                part.clone(),
            )
            .await;
        answer.assert(200, None);
        assert_eq!(answer.header("x-amz-checksum-sha256"), Some(checksum));
        let etag = answer.header("etag").unwrap().to_owned();
        assert_eq!(etag, format!("\"{}\"", md5_hex(part)));
        etags.push(etag);
    }

    let listed = setup.list_parts("movie.mp4", &id, "").await;
    listed.assert(200, None);
    assert_eq!(elements(&listed.body, "PartNumber"), ["1", "2", "3"]);
    assert_eq!(
        elements(&listed.body, "ChecksumSHA256"),
        expected_part_checksums
    );
    assert_eq!(element(&listed.body, "ChecksumAlgorithm"), Some("SHA256"));
    assert_eq!(element(&listed.body, "IsTruncated"), Some("false"));

    let complete = setup
        .complete(
            "movie.mp4",
            &id,
            &[(
                "x-amz-checksum-sha256",
                "uWBwpe1dxI4Vw8Gf0X9ynOdw/SS6VBzfWm9giiv1sf4=-3",
            )],
            &[(1, &etags[0]), (2, &etags[1]), (3, &etags[2])],
        )
        .await;
    complete.assert(200, None);
    let s3_etag = "b2add96cc9702bbf4efb0ccdfc6b7747-3";
    let quoted = format!("\"{s3_etag}\"");
    assert_eq!(element(&complete.body, "ETag"), Some(quoted.as_str()));
    assert_eq!(
        element(&complete.body, "ChecksumSHA256"),
        Some("uWBwpe1dxI4Vw8Gf0X9ynOdw/SS6VBzfWm9giiv1sf4=-3")
    );
    assert_eq!(element(&complete.body, "ChecksumType"), Some("COMPOSITE"));
    assert_eq!(
        element(&complete.body, "Location"),
        Some("/photos/movie.mp4")
    );

    // The object carries the identity of the upload's MPU_CREATE (§7.2),
    // and the parts' boundaries.
    let shard = ShardRef::for_key(&bucket, "movie.mp4");
    let entry = setup.shards.entry(&shard, "movie.mp4").await.unwrap();
    let object = entry.unwrap().object.unwrap();
    assert_eq!(object.write_identity, parse_upload_id(&id));
    let Payload::Parts {
        upload,
        parts: kept,
    } = &object.payload
    else {
        panic!("{:?}", object.payload);
    };
    assert_eq!(Some(*upload), parse_upload_id(&id));
    assert_eq!(kept.len(), 3);
    assert!(kept.iter().all(|part| part.size == PART as u64));

    let whole = setup
        .get("movie.mp4", &[("x-amz-checksum-mode", "ENABLED")], "")
        .await;
    whole.assert(200, None);
    assert_eq!(whole.header("etag"), Some(&*format!("\"{s3_etag}\"")));
    assert_eq!(whole.header("content-type"), Some("video/mp4"));
    assert_eq!(whole.header("x-amz-meta-camera"), Some("x100"));
    assert_eq!(
        whole.header("x-amz-checksum-sha256"),
        Some("uWBwpe1dxI4Vw8Gf0X9ynOdw/SS6VBzfWm9giiv1sf4=-3")
    );
    assert_eq!(whole.header("x-amz-checksum-type"), Some("COMPOSITE"));
    assert!(whole.header("x-amz-mp-parts-count").is_none());
    assert_eq!(whole.body.as_bytes(), &parts.concat()[..]);

    // Part 2 by number, with HEAD and GET.
    for method in [Method::HEAD, Method::GET] {
        let uri = "/photos/movie.mp4?partNumber=2";
        let answer = setup.call(method.clone(), uri, &[], "").await;
        answer.assert(206, None);
        let range = format!("bytes {PART}-{}/{}", 2 * PART - 1, 3 * PART);
        assert_eq!(answer.header("content-range"), Some(range.as_str()));
        assert_eq!(answer.header("content-length"), Some(&*PART.to_string()));
        assert_eq!(answer.header("x-amz-mp-parts-count"), Some("3"));
        if method == Method::GET {
            assert_eq!(answer.body.as_bytes(), &parts[1][..]);
        }
    }
    for number in [0, 4] {
        let answer = setup
            .get("movie.mp4", &[], &format!("?partNumber={number}"))
            .await;
        assert_eq!(answer.status.as_u16(), if number == 0 { 400 } else { 416 });
    }
    // A range across the parts' boundaries.
    let range = format!("bytes={}-{}", PART - 3, 2 * PART + 2);
    let answer = setup.get("movie.mp4", &[("range", &range)], "").await;
    answer.assert(206, None);
    let all = parts.concat();
    assert_eq!(answer.body.as_bytes(), &all[PART - 3..=2 * PART + 2]);

    // The upload is gone once completed.
    setup
        .list_parts("movie.mp4", &id, "")
        .await
        .assert(404, Some("NoSuchUpload"));
    setup
        .complete("movie.mp4", &id, &[], &[(1, &etags[0])])
        .await
        .assert(404, Some("NoSuchUpload"));
}

/// Without a checksum algorithm, parts get CRC64NVME checksums and the
/// object a combined FULL_OBJECT one, as S3 gives them; parts may be
/// inline, and the last may be small. Parts left out are released.
#[tokio::test]
async fn an_upload_without_an_algorithm_gets_crc64nvme_and_keeps_listed_parts() {
    let (setup, bucket) = local().await;
    let id = setup.create_upload("k", &[]).await;
    let first = filled(PART + 17, b'x');
    let skipped = filled(PART, b'y');
    let last = filled(100, b'z');
    let e1 = setup.part("k", &id, 1, first.clone()).await;
    let before_skipped = setup.located(&bucket, "k").await;
    let e2 = setup.part("k", &id, 2, skipped).await;
    let skipped_positions: BTreeSet<_> = setup
        .located(&bucket, "k")
        .await
        .difference(&before_skipped)
        .copied()
        .collect();
    assert_eq!(skipped_positions.len(), 5, "five 1 MiB extents");
    // A part supplying its own checksum is checked, and gets both.
    let crc32 = encode_digest(&digest(ChecksumAlgorithm::Crc32, &last));
    let answer = setup
        .upload_part(
            "k",
            &id,
            5,
            &[("x-amz-checksum-crc32", &crc32)],
            last.clone(),
        )
        .await;
    answer.assert(200, None);
    let e5 = answer.header("etag").unwrap().to_owned();
    assert_eq!(answer.header("x-amz-checksum-crc32"), Some(crc32.as_str()));
    assert!(answer.header("x-amz-checksum-crc64nvme").is_some());
    let _ = e2;

    let complete = setup.complete("k", &id, &[], &[(1, &e1), (5, &e5)]).await;
    complete.assert(200, None);
    let body = [first, last].concat();
    let mut md5s = Vec::new();
    for part in [&body[..PART + 17], &body[PART + 17..]] {
        md5s.extend(digest(ChecksumAlgorithm::Md5, part));
    }
    let etag = format!("\"{}-2\"", md5_hex(&md5s));
    let crc = encode_digest(&digest(ChecksumAlgorithm::Crc64Nvme, &body));
    let got = setup
        .get("k", &[("x-amz-checksum-mode", "ENABLED")], "")
        .await;
    got.assert(200, None);
    assert_eq!(got.header("etag"), Some(etag.as_str()));
    assert_eq!(got.header("x-amz-checksum-crc64nvme"), Some(crc.as_str()));
    assert_eq!(got.header("x-amz-checksum-type"), Some("FULL_OBJECT"));
    assert_eq!(got.body.as_bytes(), &body[..]);
    // Part 2 of the object is the one uploaded as part 5, inline.
    let second = setup.get("k", &[], "?partNumber=2").await;
    second.assert(206, None);
    assert_eq!(second.body.as_bytes(), &body[PART + 17..]);

    // The part left out is released for compaction.
    let located = setup.located(&bucket, "k").await;
    assert!(located.is_disjoint(&skipped_positions), "{located:?}");
}

#[tokio::test]
async fn completions_check_their_parts() {
    let (setup, _) = local().await;
    let id = setup.create_upload("k", &[]).await;
    let small = setup.part("k", &id, 1, filled(10, b'a')).await;
    let big = setup.part("k", &id, 2, filled(PART, b'b')).await;
    let last = setup.part("k", &id, 3, filled(10, b'c')).await;
    for (parts, status, code) in [
        (
            vec![(1, small.as_str()), (3, last.as_str())],
            400,
            "EntityTooSmall",
        ),
        (
            vec![(2, big.as_str()), (1, small.as_str())],
            400,
            "InvalidPartOrder",
        ),
        (
            vec![(2, big.as_str()), (2, big.as_str())],
            400,
            "InvalidPartOrder",
        ),
        (vec![(2, small.as_str())], 400, "InvalidPart"),
        (vec![(4, small.as_str())], 400, "InvalidPart"),
        (vec![], 400, "InvalidRequest"),
    ] {
        setup
            .complete("k", &id, &[], &parts)
            .await
            .assert(status, Some(code));
    }
    // A whole-object checksum that does not match.
    let parts = [(2, big.as_str()), (3, last.as_str())];
    for (header, value, code) in [
        ("x-amz-checksum-crc64nvme", "AAAAAAAAAAA=", "BadDigest"),
        ("x-amz-checksum-type", "COMPOSITE", "InvalidRequest"),
        ("x-amz-mp-object-size", "7", "InvalidRequest"),
    ] {
        setup
            .complete("k", &id, &[(header, value)], &parts)
            .await
            .assert(400, Some(code));
    }

    // A part uploaded again replaces the earlier one: completing with the
    // old ETag fails, and with the new one succeeds.
    let again = setup.part("k", &id, 3, filled(11, b'd')).await;
    setup
        .complete("k", &id, &[], &parts)
        .await
        .assert(400, Some("InvalidPart"));
    // Conditional completion, as for PutObject.
    setup
        .call(Method::PUT, "/photos/k", &[], "existing")
        .await
        .assert(200, None);
    let parts = [(2, big.as_str()), (3, again.as_str())];
    setup
        .complete("k", &id, &[("if-none-match", "*")], &parts)
        .await
        .assert(412, Some("PreconditionFailed"));
    let complete = setup.complete("k", &id, &[("if-match", "*")], &parts).await;
    complete.assert(200, None);
    let got = setup.get("k", &[], "").await;
    assert_eq!(got.body.len(), PART + 11);
}

#[tokio::test]
async fn requests_for_unknown_or_invalid_uploads_are_refused() {
    let (setup, _) = local().await;
    let id = setup.create_upload("k", &[]).await;
    let unknown = "0000000000000001000000000000ffff";
    for upload in [unknown, "not-an-upload-id"] {
        setup
            .upload_part("k", upload, 1, &[], filled(1, b'a'))
            .await
            .assert(404, Some("NoSuchUpload"));
        setup
            .complete("k", upload, &[], &[(1, "\"e\"")])
            .await
            .assert(404, Some("NoSuchUpload"));
        setup
            .abort("k", upload)
            .await
            .assert(404, Some("NoSuchUpload"));
        setup
            .list_parts("k", upload, "")
            .await
            .assert(404, Some("NoSuchUpload"));
    }
    // An upload belongs to its key.
    setup
        .upload_part("other", &id, 1, &[], filled(1, b'a'))
        .await
        .assert(404, Some("NoSuchUpload"));
    // A part whose checksum is not the upload's algorithm.
    let sha = setup
        .create_upload("k", &[("x-amz-checksum-algorithm", "SHA256")])
        .await;
    let crc32 = encode_digest(&digest(ChecksumAlgorithm::Crc32, b"a"));
    setup
        .upload_part(
            "k",
            &sha,
            1,
            &[("x-amz-checksum-crc32", &crc32)],
            filled(1, b'a'),
        )
        .await
        .assert(400, Some("InvalidRequest"));
    // A part whose checksum does not match its bytes.
    setup
        .upload_part(
            "k",
            &id,
            1,
            &[("x-amz-checksum-crc32", &crc32)],
            filled(1, b'b'),
        )
        .await
        .assert(400, Some("BadDigest"));
    // Creation options SkyS3 refuses.
    for (headers, status, code) in [
        (vec![("x-amz-tagging", "a=b")], 501, "NotImplemented"),
        (
            vec![("x-amz-checksum-type", "COMPOSITE")],
            400,
            "InvalidRequest",
        ),
        (
            vec![("x-amz-checksum-algorithm", "SHA512")],
            501,
            "NotImplemented",
        ),
        (
            vec![
                ("x-amz-checksum-algorithm", "SHA1"),
                ("x-amz-checksum-type", "FULL_OBJECT"),
            ],
            400,
            "InvalidRequest",
        ),
        (
            vec![("x-amz-meta-skys3-wid", "forged")],
            400,
            "InvalidArgument",
        ),
    ] {
        setup
            .call(Method::POST, "/photos/k?uploads", &headers, "")
            .await
            .assert(status, Some(code));
    }
    setup
        .call(Method::POST, "/missing/k?uploads", &[], "")
        .await
        .assert(404, Some("NoSuchBucket"));
    setup
        .call(
            Method::DELETE,
            &format!("/photos/k?uploadId={id}"),
            &[(
                "x-amz-if-match-initiated-time",
                "Thu, 01 Dec 2033 16:00:00 GMT",
            )],
            "",
        )
        .await
        .assert(501, Some("NotImplemented"));
}

/// Aborting an upload releases its parts' bytes: compaction may drop their
/// records (§10.3).
#[tokio::test]
async fn aborted_uploads_release_their_extents() {
    let (setup, bucket) = local().await;
    let before = setup.located(&bucket, "k").await;
    let id = setup.create_upload("k", &[]).await;
    setup.part("k", &id, 1, filled(3 * MIB, b'a')).await;
    setup.part("k", &id, 2, filled(10, b'b')).await;
    let during = setup.located(&bucket, "k").await;
    assert_eq!(
        during.difference(&before).count(),
        4,
        "3 extents and 1 inline part"
    );
    setup.abort("k", &id).await.assert(204, None);
    assert_eq!(setup.located(&bucket, "k").await, before);
    setup
        .list_parts("k", &id, "")
        .await
        .assert(404, Some("NoSuchUpload"));
    setup.get("k", &[], "").await.assert(404, Some("NoSuchKey"));
}

#[tokio::test]
async fn parts_are_listed_in_pages() {
    let (setup, _) = local().await;
    let id = setup.create_upload("k", &[]).await;
    for number in [1, 2, 5, 9] {
        setup.part("k", &id, number, filled(5, b'p')).await;
    }
    let page = setup.list_parts("k", &id, "&max-parts=2").await;
    page.assert(200, None);
    assert_eq!(elements(&page.body, "PartNumber"), ["1", "2"]);
    assert_eq!(element(&page.body, "IsTruncated"), Some("true"));
    assert_eq!(element(&page.body, "NextPartNumberMarker"), Some("2"));
    assert_eq!(elements(&page.body, "Size"), ["5", "5"]);
    let page = setup
        .list_parts("k", &id, "&max-parts=2&part-number-marker=2")
        .await;
    assert_eq!(elements(&page.body, "PartNumber"), ["5", "9"]);
    assert_eq!(element(&page.body, "IsTruncated"), Some("false"));
    setup
        .list_parts("k", &id, "&max-parts=-1")
        .await
        .assert(400, Some("InvalidArgument"));
}

#[tokio::test]
async fn uploads_are_listed_across_shards() {
    let (setup, _) = local().await;
    let mut ids = Vec::new();
    for key in ["a/1", "a/2", "b", "c/x/1", "c/y", "a/1"] {
        ids.push((key, setup.create_upload(key, &[]).await));
    }
    let all = setup.list_uploads("").await;
    let keys = elements(&all.body, "Key");
    assert_eq!(keys, ["a/1", "a/1", "a/2", "b", "c/x/1", "c/y"]);
    // Two uploads of one key are listed by age.
    let a1: Vec<_> = elements(&all.body, "UploadId")[..2].to_vec();
    assert_eq!(a1, [ids[0].1.as_str(), ids[5].1.as_str()]);
    assert_eq!(element(&all.body, "IsTruncated"), Some("false"));

    let page = setup.list_uploads("&max-uploads=2").await;
    assert_eq!(elements(&page.body, "Key"), ["a/1", "a/1"]);
    assert_eq!(element(&page.body, "IsTruncated"), Some("true"));
    assert_eq!(element(&page.body, "NextKeyMarker"), Some("a/1"));
    let marker = element(&page.body, "NextUploadIdMarker").unwrap();
    assert_eq!(marker, ids[5].1);
    let rest = setup
        .list_uploads(&format!("&key-marker=a/1&upload-id-marker={marker}"))
        .await;
    assert_eq!(elements(&rest.body, "Key"), ["a/2", "b", "c/x/1", "c/y"]);
    let rest = setup.list_uploads("&key-marker=a/1").await;
    assert_eq!(elements(&rest.body, "Key"), ["a/2", "b", "c/x/1", "c/y"]);

    let rolled = setup.list_uploads("&delimiter=/").await;
    assert_eq!(elements(&rolled.body, "Key"), ["b"]);
    assert_eq!(elements(&rolled.body, "Prefix"), ["a/", "c/"]);
    let page = setup.list_uploads("&delimiter=/&max-uploads=1").await;
    assert_eq!(elements(&page.body, "Prefix"), ["a/"]);
    assert_eq!(element(&page.body, "NextKeyMarker"), Some("a/"));
    let page = setup
        .list_uploads("&delimiter=/&max-uploads=1&key-marker=a/")
        .await;
    assert_eq!(elements(&page.body, "Key"), ["b"]);
    let nested = setup.list_uploads("&prefix=c/&delimiter=/").await;
    assert_eq!(elements(&nested.body, "Key"), ["c/y"]);
    assert_eq!(elements(&nested.body, "Prefix"), ["c/x/", "c/"]);

    setup
        .call(
            Method::GET,
            "/photos?uploads&key-marker=a&upload-id-marker=nope",
            &[],
            "",
        )
        .await
        .assert(400, Some("InvalidArgument"));
    // Completed and aborted uploads are no longer listed.
    let e = setup.part("b", &ids[2].1, 1, filled(1, b'b')).await;
    setup
        .complete("b", &ids[2].1, &[], &[(1, &e)])
        .await
        .assert(200, None);
    setup.abort("c/y", &ids[4].1).await.assert(204, None);
    let left = setup.list_uploads("").await;
    assert_eq!(elements(&left.body, "Key"), ["a/1", "a/1", "a/2", "c/x/1"]);
}

/// Uploads, their parts, and completed objects survive a power loss, and
/// an upload left open completes after the restart.
#[tokio::test]
async fn uploads_survive_a_power_loss() {
    let (setup, bucket) = local().await;
    let done = setup.create_upload("done", &[]).await;
    let e1 = setup.part("done", &done, 1, filled(PART, b'a')).await;
    let e2 = setup.part("done", &done, 2, filled(7, b'b')).await;
    let complete = setup
        .complete("done", &done, &[], &[(1, &e1), (2, &e2)])
        .await;
    complete.assert(200, None);
    let etag = element(&complete.body, "ETag").unwrap().to_owned();
    let open = setup.create_upload("open", &[]).await;
    let o1 = setup.part("open", &open, 1, filled(PART, b'c')).await;
    let o2 = setup.part("open", &open, 2, filled(3, b'd')).await;
    let gone = setup.create_upload("gone", &[]).await;
    setup.part("gone", &gone, 1, filled(9, b'e')).await;
    setup.abort("gone", &gone).await.assert(204, None);

    setup.shards.disk().crash();
    let restarted = restart(&setup, &bucket).await;
    drop(setup);

    let got = restarted.get("done", &[], "").await;
    got.assert(200, None);
    assert_eq!(got.header("etag"), Some(etag.as_str()));
    assert_eq!(got.body.len(), PART + 7);
    let listed = restarted.list_uploads("").await;
    assert_eq!(elements(&listed.body, "Key"), ["open"]);
    let parts = restarted.list_parts("open", &open, "").await;
    assert_eq!(elements(&parts.body, "PartNumber"), ["1", "2"]);
    restarted
        .complete("open", &open, &[], &[(1, &o1), (2, &o2)])
        .await
        .assert(200, None);
    let got = restarted.get("open", &[], "?partNumber=2").await;
    got.assert(206, None);
    assert_eq!(got.body, "ddd");
    restarted
        .list_parts("gone", &gone, "")
        .await
        .assert(404, Some("NoSuchUpload"));
}

/// A gateway over the control store of `setup` and shards on its disk
/// after a power loss, with the shards of `bucket` open.
async fn restart(setup: &Setup, bucket: &BucketDocument) -> Setup {
    let shards = MemoryShards::open(setup.shards.disk().clone())
        .await
        .unwrap();
    for shard in ShardRef::all(bucket) {
        shards.open(&shard, bucket).await.unwrap();
    }
    let gateway = Gateway::new(
        mib_extents(),
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
