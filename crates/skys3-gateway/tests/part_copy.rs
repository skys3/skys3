//! UploadPartCopy through the gateway's pipeline, over real shards on a
//! simulated disk: parts copied whole and by range, from single-PUT and
//! multipart sources, in the same bucket and across buckets, from a holder
//! on another node and through a fill; the ETags and checksums of the
//! completed objects against values S3 computed; source conditions; and
//! refusals.
//!
//! **Where the known values come from.** The ETag
//! `b2add96cc9702bbf4efb0ccdfc6b7747-3` and the checksums of the three
//! 5 MiB parts of `A`, `B`, and `C` and of the object they make are those
//! ceph/s3-tests asserts as S3's answers, at the commit the SDK matrix pins
//! (`tests/sdk/s3-tests/Dockerfile`): `test_multipart_reupload_checksum_and_etag`
//! ("known MD5/etag and sha256 checksum values") and
//! `test_multipart_use_cksum_helper_{sha256,crc64nvme,crc32,crc32c,sha1}`.
//! s3-tests runs against AWS S3, and none of these tests is marked as
//! failing there. Here the same bytes reach the parts by UploadPartCopy
//! from ranges of one object, so the object must come out the same.

mod common;

use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use common::signing::request as with_body;
use common::{Answer, Setup, config, setup_over, setup_with};
use http::Method;
use skys3_gateway::checksum::digest;
use skys3_gateway::stub::MemoryShards;
use skys3_gateway::{FillBody, FillError, Fills, GatewayConfig, ShardRef, Shards};
use skys3_index::EntryState;
use skys3_types::checksum::ChecksumAlgorithm;
use skys3_types::{BucketDocument, EpochSeq};
use tokio::sync::mpsc;

const MIB: usize = 1 << 20;
const PART: usize = 5 * MIB;

/// S3's ETag of the three parts of [`abc`].
const ABC_ETAG: &str = "\"b2add96cc9702bbf4efb0ccdfc6b7747-3\"";

/// Bodies up to 64 KiB go inline, longer ones in 1 MiB extents.
fn mib_extents() -> GatewayConfig {
    let mut config = config("");
    config.inline_max_bytes = 64 * 1024;
    config.extent_bytes = MIB as u64;
    config
}

/// A gateway with the `local` buckets `photos` and `media`.
async fn local() -> (Setup, BucketDocument) {
    let setup = setup_with(mib_extents()).await;
    let photos = setup.create_local("photos").await;
    setup.create_local("media").await;
    (setup, photos)
}

/// 5 MiB of `A`, then of `B`, then of `C`: the parts of the s3-tests
/// upload whose ETag and checksums S3 computed.
fn abc() -> Bytes {
    b"ABC"
        .iter()
        .flat_map(|&letter| vec![letter; PART])
        .collect()
}

/// `len` bytes of lowercase letters, which differ every byte.
fn letters(len: usize) -> Bytes {
    (0..len).map(|i| b'a' + (i % 26) as u8).collect()
}

fn md5_hex(data: &[u8]) -> String {
    digest(ChecksumAlgorithm::Md5, data)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// The quoted ETag of an object or part stored as `data`: its MD5.
fn etag_of(data: &[u8]) -> String {
    format!("\"{}\"", md5_hex(data))
}

/// The quoted multipart ETag of an object of `parts`: the MD5 of their MD5s,
/// and their count.
fn multipart_etag(parts: &[&[u8]]) -> String {
    let digests: Vec<u8> = parts
        .iter()
        .flat_map(|part| digest(ChecksumAlgorithm::Md5, part))
        .collect();
    format!("\"{}-{}\"", md5_hex(&digests), parts.len())
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

/// A part in a CompleteMultipartUpload body: its number, ETag, and
/// checksum element, if listed.
type Listed<'a> = (u16, &'a str, Option<(&'a str, &'a str)>);

fn completion(parts: &[Listed<'_>]) -> String {
    let parts: String = parts
        .iter()
        .map(|(number, etag, checksum)| {
            let checksum = checksum
                .map(|(name, value)| format!("<{name}>{value}</{name}>"))
                .unwrap_or_default();
            format!("<Part><PartNumber>{number}</PartNumber><ETag>{etag}</ETag>{checksum}</Part>")
        })
        .collect();
    format!("<CompleteMultipartUpload>{parts}</CompleteMultipartUpload>")
}

impl Answer {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(|value| value.to_str().unwrap())
    }

    /// The text of the first `<tag>` element of the body.
    fn element(&self, tag: &str) -> Option<&str> {
        element(&self.body, tag)
    }
}

impl Setup {
    async fn put(&self, uri: &str, headers: &[(&str, &str)], body: Bytes) -> Answer {
        self.send(with_body(Method::PUT, uri, headers, body)).await
    }

    /// Creates an upload of `object`, a `/bucket/key` path, and returns
    /// its ID.
    async fn create_upload(&self, object: &str, headers: &[(&str, &str)]) -> String {
        let answer = self
            .call(Method::POST, &format!("{object}?uploads"), headers, "")
            .await;
        answer.assert(200, None);
        answer.element("UploadId").unwrap().to_owned()
    }

    /// Copies `source`, a `bucket/key` copy source, as part `number` of the
    /// upload `id` of `object`.
    async fn copy_part(
        &self,
        object: &str,
        id: &str,
        number: u16,
        source: &str,
        headers: &[(&str, &str)],
    ) -> Answer {
        let uri = format!("{object}?partNumber={number}&uploadId={id}");
        let mut headers = headers.to_vec();
        headers.push(("x-amz-copy-source", source));
        self.call(Method::PUT, &uri, &headers, "").await
    }

    /// Copies a part that must succeed, and returns its quoted ETag.
    async fn copied(
        &self,
        object: &str,
        id: &str,
        number: u16,
        source: &str,
        range: Option<&str>,
    ) -> String {
        let headers: Vec<_> = range
            .map(|range| ("x-amz-copy-source-range", range))
            .into_iter()
            .collect();
        let answer = self.copy_part(object, id, number, source, &headers).await;
        answer.assert(200, None);
        answer.element("ETag").unwrap().to_owned()
    }

    async fn complete(&self, object: &str, id: &str, parts: &[Listed<'_>]) -> Answer {
        let uri = format!("{object}?uploadId={id}");
        self.call(Method::POST, &uri, &[], &completion(parts)).await
    }
}

/// One upload of the s3-tests object, by its checksum: how it is created,
/// the checksum element of each part, S3's checksums of the three parts,
/// and S3's checksum of the object, with its type.
struct Known {
    create: &'static [(&'static str, &'static str)],
    element: &'static str,
    parts: [&'static str; 3],
    object: &'static str,
    checksum_type: &'static str,
}

const KNOWN: [Known; 6] = [
    Known {
        create: &[("x-amz-checksum-algorithm", "SHA256")],
        element: "ChecksumSHA256",
        parts: [
            "275VF5loJr1YYawit0XSHREhkFXYkkPKGuoK0x9VKxI=",
            "mrHwOfjTL5Zwfj74F05HOQGLdUb7E5szdCbxgUSq6NM=",
            "Vw7oB/nKQ5xWb3hNgbyfkvDiivl+U+/Dft48nfJfDow=",
        ],
        object: "uWBwpe1dxI4Vw8Gf0X9ynOdw/SS6VBzfWm9giiv1sf4=-3",
        checksum_type: "COMPOSITE",
    },
    Known {
        create: &[("x-amz-checksum-algorithm", "SHA1")],
        element: "ChecksumSHA1",
        parts: [
            "iIaTCGbm+vdVjNqIMF2S0T7ibMk=",
            "LS/TJ32bAVKEwRu+sE3X7awh/lk=",
            "6DDwovUaHwrKNXDMzOGbuvj9kxI=",
        ],
        object: "sizjvY4eud3MrcHdZM3cQ/ol39o=-3",
        checksum_type: "COMPOSITE",
    },
    Known {
        create: &[("x-amz-checksum-algorithm", "CRC64NVME")],
        element: "ChecksumCRC64NVME",
        parts: ["L/E4WYn8v98=", "xW1l19VobYM=", "cK5MnNaWrW4="],
        object: "i+6LR0y3eFo=",
        checksum_type: "FULL_OBJECT",
    },
    // An upload created without an algorithm computes CRC64NVME, as S3
    // does.
    Known {
        create: &[],
        element: "ChecksumCRC64NVME",
        parts: ["L/E4WYn8v98=", "xW1l19VobYM=", "cK5MnNaWrW4="],
        object: "i+6LR0y3eFo=",
        checksum_type: "FULL_OBJECT",
    },
    Known {
        create: &[
            ("x-amz-checksum-algorithm", "CRC32"),
            ("x-amz-checksum-type", "FULL_OBJECT"),
        ],
        element: "ChecksumCRC32",
        parts: ["JRTCyQ==", "QoZTGg==", "YAgjqw=="],
        object: "WgDhBQ==",
        checksum_type: "FULL_OBJECT",
    },
    Known {
        create: &[
            ("x-amz-checksum-algorithm", "CRC32C"),
            ("x-amz-checksum-type", "FULL_OBJECT"),
        ],
        element: "ChecksumCRC32C",
        parts: ["MDaLrw==", "TH4EZg==", "Z7mBIQ=="],
        object: "xU+Krw==",
        checksum_type: "FULL_OBJECT",
    },
];

/// Three ranged copies of one 15 MiB object, stored by a single PUT in
/// another bucket, make the object s3-tests uploads as three parts: each
/// part has the ETag and checksum S3 gives that part, and the completed
/// object S3's multipart ETag and checksum, for every checksum algorithm.
#[tokio::test]
async fn ranged_copies_complete_with_the_etag_and_checksums_s3_computes() {
    let (setup, _) = local().await;
    let source = abc();
    setup
        .put("/media/abc", &[], source.clone())
        .await
        .assert(200, None);
    let ranges = [
        format!("bytes=0-{}", PART - 1),
        format!("bytes={PART}-{}", 2 * PART - 1),
        format!("bytes={}-{}", 2 * PART, 3 * PART - 1),
    ];
    for (n, known) in KNOWN.iter().enumerate() {
        let object = format!("/photos/copy-{n}");
        let id = setup.create_upload(&object, known.create).await;
        let mut etags = Vec::new();
        for (number, (range, checksum)) in (1..).zip(ranges.iter().zip(known.parts)) {
            let answer = setup
                .copy_part(
                    &object,
                    &id,
                    number,
                    "media/abc",
                    &[("x-amz-copy-source-range", range)],
                )
                .await;
            answer.assert(200, None);
            let start = usize::from(number - 1) * PART;
            let etag = answer.element("ETag").unwrap().to_owned();
            assert_eq!(etag, etag_of(&source[start..start + PART]), "{object}");
            assert_eq!(answer.element(known.element), Some(checksum), "{object}");
            assert!(answer.element("LastModified").is_some(), "{answer:?}");
            etags.push(etag);
        }

        let listed = setup
            .call(Method::GET, &format!("{object}?uploadId={id}"), &[], "")
            .await;
        listed.assert(200, None);
        assert_eq!(elements(&listed.body, "ETag"), etags);
        assert_eq!(elements(&listed.body, known.element), known.parts);
        let size = PART.to_string();
        assert_eq!(elements(&listed.body, "Size"), [&*size, &*size, &*size]);

        let parts: Vec<Listed<'_>> = (1..)
            .zip(&etags)
            .zip(known.parts)
            .map(|((number, etag), checksum)| {
                (number, etag.as_str(), Some((known.element, checksum)))
            })
            .collect();
        let complete = setup.complete(&object, &id, &parts).await;
        complete.assert(200, None);
        assert_eq!(complete.element("ETag"), Some(ABC_ETAG), "{object}");
        assert_eq!(complete.element(known.element), Some(known.object));
        assert_eq!(complete.element("ChecksumType"), Some(known.checksum_type));

        let got = setup
            .call(
                Method::GET,
                &object,
                &[("x-amz-checksum-mode", "ENABLED")],
                "",
            )
            .await;
        got.assert(200, None);
        assert_eq!(got.header("etag"), Some(ABC_ETAG));
        let header = format!("x-amz-{}", known.element.to_ascii_lowercase())
            .replace("checksum", "checksum-");
        assert_eq!(got.header(&header), Some(known.object), "{header}");
        assert_eq!(got.body.as_bytes(), &source[..]);
    }
}

/// A copy of a multipart source, whole or by a range across its parts'
/// boundaries, is a part like any other: its ETag is the MD5 of the bytes
/// copied, whatever the source's ETag, and it stays once the source is
/// gone.
#[tokio::test]
async fn copies_of_a_multipart_source_take_the_md5_of_the_bytes_copied() {
    let (setup, _) = local().await;
    let source = abc();
    let id = setup.create_upload("/media/abc", &[]).await;
    let mut etags = Vec::new();
    for (number, chunk) in (1..).zip(source.chunks(PART)) {
        let uri = format!("/media/abc?partNumber={number}&uploadId={id}");
        let answer = setup.put(&uri, &[], Bytes::copy_from_slice(chunk)).await;
        answer.assert(200, None);
        etags.push(answer.header("etag").unwrap().to_owned());
    }
    let parts: Vec<Listed<'_>> = (1..).zip(&etags).map(|(n, e)| (n, &**e, None)).collect();
    setup
        .complete("/media/abc", &id, &parts)
        .await
        .assert(200, None);

    let id = setup.create_upload("/photos/whole", &[]).await;
    let whole = setup
        .copied("/photos/whole", &id, 1, "media/abc", None)
        .await;
    assert_eq!(whole, etag_of(&source));
    let across = PART - 10..2 * PART + 10;
    let range = format!("bytes={}-{}", across.start, across.end - 1);
    let part = setup
        .copied("/photos/whole", &id, 2, "media/abc", Some(&range))
        .await;
    assert_eq!(part, etag_of(&source[across.clone()]));

    // The source goes before the copy completes.
    setup
        .call(Method::DELETE, "/media/abc", &[], "")
        .await
        .assert(204, None);
    let complete = setup
        .complete("/photos/whole", &id, &[(1, &whole, None), (2, &part, None)])
        .await;
    complete.assert(200, None);
    let expected = multipart_etag(&[&source, &source[across.clone()]]);
    assert_eq!(complete.element("ETag"), Some(expected.as_str()));
    let got = setup.call(Method::GET, "/photos/whole", &[], "").await;
    got.assert(200, None);
    assert_eq!(got.header("etag"), Some(expected.as_str()));
    assert_eq!(&got.body.as_bytes()[..source.len()], &source[..]);
    assert_eq!(&got.body.as_bytes()[source.len()..], &source[across]);
    let second = setup
        .call(Method::GET, "/photos/whole?partNumber=2", &[], "")
        .await;
    second.assert(206, None);
    assert_eq!(second.header("x-amz-mp-parts-count"), Some("2"));
    assert_eq!(second.header("content-length"), Some("5242900"));
}

/// The copy-source conditions are a GET's, against the source, except that
/// every failure answers `412`. The pairs are the ones AWS documents for
/// UploadPartCopy.
#[tokio::test]
async fn source_conditions_are_evaluated_against_the_source() {
    let (setup, _) = local().await;
    let data = letters(3000);
    let put = setup.put("/media/source", &[], data.clone()).await;
    put.assert(200, None);
    let etag = put.header("etag").unwrap().to_owned();
    let id = setup.create_upload("/photos/k", &[]).await;
    let past = "Sat, 01 Jan 2000 00:00:00 GMT";
    let future = "Fri, 01 Jan 2100 00:00:00 GMT";
    let cases: [(&[(&str, &str)], u16); 11] = [
        (&[("x-amz-copy-source-if-match", &etag)], 200),
        (&[("x-amz-copy-source-if-match", "\"0123\"")], 412),
        (&[("x-amz-copy-source-if-none-match", &etag)], 412),
        (&[("x-amz-copy-source-if-none-match", "\"0123\"")], 200),
        (&[("x-amz-copy-source-if-modified-since", past)], 200),
        (&[("x-amz-copy-source-if-modified-since", future)], 412),
        (&[("x-amz-copy-source-if-unmodified-since", past)], 412),
        (&[("x-amz-copy-source-if-unmodified-since", future)], 200),
        // If-Match holds and If-Unmodified-Since does not: copied.
        (
            &[
                ("x-amz-copy-source-if-match", &etag),
                ("x-amz-copy-source-if-unmodified-since", past),
            ],
            200,
        ),
        // If-None-Match fails and If-Modified-Since holds: refused.
        (
            &[
                ("x-amz-copy-source-if-none-match", &etag),
                ("x-amz-copy-source-if-modified-since", past),
            ],
            412,
        ),
        // If-None-Match holds, so If-Modified-Since is not evaluated.
        (
            &[
                ("x-amz-copy-source-if-none-match", "\"0123\""),
                ("x-amz-copy-source-if-modified-since", future),
            ],
            200,
        ),
    ];
    for (headers, status) in cases {
        let answer = setup
            .copy_part("/photos/k", &id, 1, "media/source", headers)
            .await;
        let code = (status == 412).then_some("PreconditionFailed");
        answer.assert(status, code);
        if status == 200 {
            assert_eq!(answer.element("ETag"), Some(etag.as_str()));
        }
    }
    // Conditions come before the range.
    let answer = setup
        .copy_part(
            "/photos/k",
            &id,
            1,
            "media/source",
            &[
                ("x-amz-copy-source-if-match", "\"0123\""),
                ("x-amz-copy-source-range", "bytes=0-5000"),
            ],
        )
        .await;
    answer.assert(412, Some("PreconditionFailed"));
}

/// A range has one form, `bytes=first-last`, and must end within the
/// source; the forms are those of s3-tests' improper and invalid range
/// tests.
#[tokio::test]
async fn ranges_must_be_well_formed_and_within_the_source() {
    let (setup, _) = local().await;
    setup
        .put("/media/five", &[], Bytes::from_static(b"abcde"))
        .await
        .assert(200, None);
    setup
        .put("/media/empty", &[], Bytes::new())
        .await
        .assert(200, None);
    let id = setup.create_upload("/photos/k", &[]).await;
    for range in [
        "0-2",
        "bytes=0",
        "bytes=hello-world",
        "bytes=0-bar",
        "bytes=hello-",
        "bytes=0-2,3-5",
        "bytes=-2",
        "bytes=2-",
        "bytes=3-1",
    ] {
        let headers = [("x-amz-copy-source-range", range)];
        let answer = setup
            .copy_part("/photos/k", &id, 1, "media/five", &headers)
            .await;
        answer.assert(400, Some("InvalidArgument"));
    }
    for (source, range) in [
        ("media/five", "bytes=0-21"),
        ("media/five", "bytes=5-5"),
        ("media/empty", "bytes=0-0"),
    ] {
        let headers = [("x-amz-copy-source-range", range)];
        let answer = setup.copy_part("/photos/k", &id, 1, source, &headers).await;
        answer.assert(416, Some("InvalidRange"));
    }

    // A valid range of a source of 5 MiB or less is not supported, as in
    // S3; the whole source is.
    let headers = [("x-amz-copy-source-range", "bytes=4-4")];
    let answer = setup
        .copy_part("/photos/k", &id, 1, "media/five", &headers)
        .await;
    answer.assert(400, Some("InvalidRequest"));
    let whole = setup.copied("/photos/k", &id, 1, "media/five", None).await;
    assert_eq!(whole, etag_of(b"abcde"));

    // An empty source copied whole, and a range of one byte.
    let empty = setup.copied("/photos/k", &id, 2, "media/empty", None).await;
    assert_eq!(empty, "\"d41d8cd98f00b204e9800998ecf8427e\"");
    let complete = setup
        .complete("/photos/k", &id, &[(1, &whole, None), (2, &empty, None)])
        .await;
    complete.assert(400, Some("EntityTooSmall"));
    let complete = setup.complete("/photos/k", &id, &[(1, &whole, None)]).await;
    complete.assert(200, None);
    let got = setup.call(Method::GET, "/photos/k", &[], "").await;
    assert_eq!(got.body, "abcde");
    assert_eq!(got.header("etag"), Some(&*multipart_etag(&[b"abcde"])));
}

/// S3 copies a range only of a source larger than 5 MiB: a source of
/// exactly 5 MiB is refused, one byte more is accepted, and the last byte
/// of it is a part of its own.
#[tokio::test]
async fn ranged_copies_need_a_source_larger_than_five_mib() {
    let (setup, _) = local().await;
    let limit = letters(PART);
    let over = letters(PART + 1);
    setup
        .put("/media/limit", &[], limit.clone())
        .await
        .assert(200, None);
    setup
        .put("/media/over", &[], over.clone())
        .await
        .assert(200, None);
    let id = setup.create_upload("/photos/k", &[]).await;
    let first = [("x-amz-copy-source-range", "bytes=0-0")];
    let answer = setup
        .copy_part("/photos/k", &id, 1, "media/limit", &first)
        .await;
    answer.assert(400, Some("InvalidRequest"));
    let whole = setup.copied("/photos/k", &id, 1, "media/limit", None).await;
    assert_eq!(whole, etag_of(&limit));

    let range = format!("bytes={PART}-{PART}");
    let last = setup
        .copied("/photos/k", &id, 2, "media/over", Some(&range))
        .await;
    assert_eq!(last, etag_of(&over[PART..]));
    let complete = setup
        .complete("/photos/k", &id, &[(1, &whole, None), (2, &last, None)])
        .await;
    complete.assert(200, None);
    let got = setup.call(Method::GET, "/photos/k", &[], "").await;
    assert_eq!(got.body.as_bytes(), [&limit[..], &over[PART..]].concat());
}

/// Requests that name no upload, source, or valid part are refused as S3
/// refuses them, and store nothing.
#[tokio::test]
async fn missing_uploads_sources_and_bad_requests_are_refused() {
    let (setup, _) = local().await;
    setup
        .put("/media/source", &[], letters(100))
        .await
        .assert(200, None);
    setup
        .put("/media/deleted", &[], letters(100))
        .await
        .assert(200, None);
    setup
        .call(Method::DELETE, "/media/deleted", &[], "")
        .await
        .assert(204, None);
    let id = setup.create_upload("/photos/k", &[]).await;
    let cases: [(&str, u16, &str, &str); 9] = [
        (&id, 1, "media/missing", "NoSuchKey"),
        (&id, 1, "media/deleted", "NoSuchKey"),
        (&id, 1, "nowhere/source", "NoSuchBucket"),
        (&id, 0, "media/source", "InvalidArgument"),
        (&id, 10_001, "media/source", "InvalidArgument"),
        (
            &id,
            1,
            "media/source?versionId=3HL4kqtJl",
            "InvalidArgument",
        ),
        (
            "00000000000000010000000000000099",
            1,
            "media/source",
            "NoSuchUpload",
        ),
        ("not-an-upload", 1, "media/source", "NoSuchUpload"),
        ("not-an-upload", 1, "media/missing", "NoSuchUpload"),
    ];
    for (upload, number, source, code) in cases {
        let answer = setup
            .copy_part("/photos/k", upload, number, source, &[])
            .await;
        let status = match code {
            "NoSuchKey" | "NoSuchBucket" | "NoSuchUpload" => 404,
            _ => 400,
        };
        answer.assert(status, Some(code));
    }
    // Into a bucket that does not exist.
    let answer = setup
        .copy_part("/nowhere/k", &id, 1, "media/source", &[])
        .await;
    answer.assert(404, Some("NoSuchBucket"));
    let listed = setup
        .call(Method::GET, &format!("/photos/k?uploadId={id}"), &[], "")
        .await;
    listed.assert(200, None);
    assert!(
        elements(&listed.body, "PartNumber").is_empty(),
        "{listed:?}"
    );
}

/// A part copied again, or uploaded over, is replaced like any part, and
/// a copy may come from the upload's own key.
#[tokio::test]
async fn copied_parts_are_replaced_like_uploaded_ones() {
    let (setup, _) = local().await;
    let first = letters(PART + 7);
    setup
        .put("/photos/k", &[], first.clone())
        .await
        .assert(200, None);
    let id = setup.create_upload("/photos/k", &[]).await;
    // From the key the upload will replace, and from another bucket.
    let own = setup.copied("/photos/k", &id, 1, "photos/k", None).await;
    assert_eq!(own, etag_of(&first));
    setup
        .put("/media/other", &[], Bytes::from_static(b"other"))
        .await
        .assert(200, None);
    let replaced = setup.copied("/photos/k", &id, 2, "media/other", None).await;
    assert_eq!(replaced, etag_of(b"other"));
    let uploaded = setup
        .put(
            &format!("/photos/k?partNumber=2&uploadId={id}"),
            &[],
            Bytes::from_static(b"uploaded"),
        )
        .await;
    uploaded.assert(200, None);
    let uploaded = uploaded.header("etag").unwrap().to_owned();
    // The replaced copy no longer completes.
    let stale = setup
        .complete("/photos/k", &id, &[(1, &own, None), (2, &replaced, None)])
        .await;
    stale.assert(400, Some("InvalidPart"));
    let complete = setup
        .complete("/photos/k", &id, &[(1, &own, None), (2, &uploaded, None)])
        .await;
    complete.assert(200, None);
    let got = setup.call(Method::GET, "/photos/k", &[], "").await;
    assert_eq!(got.body.len(), first.len() + b"uploaded".len());
    assert!(got.body.ends_with("uploaded"));
    assert_eq!(&got.body.as_bytes()[..first.len()], &first[..]);
}

/// A gateway on another node than the shards copies from the holder the
/// source's read plan names, as a GET reads.
#[tokio::test]
async fn a_source_on_another_node_is_read_from_its_holder() {
    let shards = MemoryShards::new()
        .await
        .seen_from("node-2".parse().unwrap());
    let setup = setup_over(mib_extents(), shards).await;
    setup.create_local("photos").await;
    let source = letters(PART + 3 * MIB);
    setup
        .put("/photos/source", &[], source.clone())
        .await
        .assert(200, None);
    let id = setup.create_upload("/photos/copy", &[]).await;
    let range = format!("bytes=1000-{}", PART - 1);
    let etag = setup
        .copied("/photos/copy", &id, 1, "photos/source", Some(&range))
        .await;
    assert_eq!(etag, etag_of(&source[1000..PART]));
    setup
        .complete("/photos/copy", &id, &[(1, &etag, None)])
        .await
        .assert(200, None);
    let got = setup.call(Method::GET, "/photos/copy", &[], "").await;
    assert_eq!(got.body.as_bytes(), &source[1000..PART]);
}

/// Fills that serve a fixed object, and record the ranges asked for.
#[derive(Debug, Default)]
struct FixedFills {
    object: &'static [u8],
    ranges: Mutex<Vec<Range<u64>>>,
}

impl Fills for FixedFills {
    fn read(
        &self,
        _shard: &ShardRef,
        _key: &str,
        _version: EpochSeq,
        range: Range<u64>,
    ) -> Pin<Box<dyn Future<Output = Result<FillBody, FillError>> + Send + '_>> {
        self.ranges.lock().unwrap().push(range.clone());
        let slice = &self.object[range.start as usize..range.end as usize];
        Box::pin(async move {
            let (sender, receiver) = mpsc::channel(1);
            sender.try_send(Ok(Bytes::from_static(slice))).unwrap();
            Ok(receiver)
        })
    }
}

/// The source of a copy may be an evicted version of a `write_back`
/// bucket: the copy reads the range it needs through a fill (§9.2).
#[tokio::test]
async fn an_evicted_source_is_read_through_a_fill() {
    // Over 5 MiB, so that a range of it may be copied.
    let mut data = b"hello, evicted world".to_vec();
    data.resize(PART + 20, b'.');
    let object: &'static [u8] = Box::leak(data.into_boxed_slice());
    let fills = Arc::new(FixedFills {
        object,
        ..FixedFills::default()
    });
    let mut config = mib_extents();
    config.fills = Some(Arc::clone(&fills) as Arc<dyn Fills>);
    let setup = setup_with(config).await;
    let remote = setup.create_write_back("remote").await;
    setup.create_local("photos").await;
    setup
        .put("/remote/k", &[], Bytes::from_static(object))
        .await
        .assert(200, None);
    setup.shards.flush(&remote.bucket_id).await;
    let shard = ShardRef::for_key(&remote, "k");
    let version = setup
        .shards
        .entry(&shard, "k")
        .await
        .unwrap()
        .unwrap()
        .version;
    let local = setup.shards.local().set().get(&(&shard).into()).await;
    local.unwrap().evict("k", version).await.unwrap().unwrap();
    let entry = setup.shards.entry(&shard, "k").await.unwrap().unwrap();
    assert_eq!(entry.state, EntryState::Evicted);

    let id = setup.create_upload("/photos/k", &[]).await;
    let etag = setup
        .copied("/photos/k", &id, 1, "remote/k", Some("bytes=7-13"))
        .await;
    assert_eq!(etag, etag_of(b"evicted"));
    let ranges = fills.ranges.lock().unwrap().clone();
    assert_eq!(ranges.len(), 1, "{ranges:?}");
    assert_eq!(ranges[0], 7..14);
    setup
        .complete("/photos/k", &id, &[(1, &etag, None)])
        .await
        .assert(200, None);
    let got = setup.call(Method::GET, "/photos/k", &[], "").await;
    assert_eq!(got.body, "evicted");
}
