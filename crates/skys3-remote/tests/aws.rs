//! The AWS SDK client against a local mock S3 server: request
//! construction (addressing, conditional headers, metadata, signing),
//! response parsing, and error mapping. No request leaves the host.

use std::collections::{BTreeMap, VecDeque};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http::{HeaderMap, Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use skys3_remote::aws::{
    AwsS3, Credentials, ProvideCredentials, SharedCredentialsProvider, default_credentials,
    web_identity_credentials,
};
use skys3_remote::probe::{ConditionalProbe, CopySupport};
use skys3_remote::{
    AbortMultipartUpload, ByteRange, CompleteMultipartUpload, CompletedPart, CopyObject,
    CreateMultipartUpload, DeleteObject, GetObject, HeadObject, ListObjectsV2, ListParts,
    MetadataDirective, ObjectStore, PutObject, S3ErrorKind, TaggingDirective, UploadId, UploadPart,
    UserMetadata, VersionId, WritePrecondition,
};
use skys3_types::{ETag, RemoteTarget};
use tokio::net::TcpListener;

/// A request the mock server received.
#[derive(Debug)]
struct Received {
    method: Method,
    /// The path and query.
    target: String,
    headers: HeaderMap,
    body: Bytes,
}

impl Received {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(|v| v.to_str().unwrap())
    }

    /// The path, without the query.
    fn path(&self) -> &str {
        self.target.split('?').next().unwrap()
    }

    /// The query parameters, sorted, without the SDK's `x-id`, which names
    /// the operation.
    fn query(&self) -> Vec<&str> {
        let mut query: Vec<&str> = match self.target.split_once('?') {
            Some((_, query)) => query
                .split('&')
                .filter(|p| !p.starts_with("x-id="))
                .collect(),
            None => Vec::new(),
        };
        query.sort_unstable();
        query
    }
}

/// A scripted answer.
enum Reply {
    Respond {
        status: u16,
        headers: Vec<(&'static str, String)>,
        body: String,
    },
    /// Never answer.
    Hang,
}

fn ok(headers: &[(&'static str, &str)], body: &str) -> Reply {
    status(200, headers, body)
}

fn status(status: u16, headers: &[(&'static str, &str)], body: &str) -> Reply {
    Reply::Respond {
        status,
        headers: headers.iter().map(|&(k, v)| (k, v.to_owned())).collect(),
        body: body.to_owned(),
    }
}

fn error_xml(status: u16, code: &str, message: &str) -> Reply {
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <Error><Code>{code}</Code><Message>{message}</Message><RequestId>r1</RequestId></Error>"
    );
    self::status(status, &[("content-type", "application/xml")], &body)
}

#[derive(Default)]
struct State {
    received: Vec<Received>,
    replies: VecDeque<Reply>,
}

/// A mock S3 server on a local port, answering with scripted replies.
struct Mock {
    addr: SocketAddr,
    state: Arc<Mutex<State>>,
}

impl Mock {
    async fn start() -> Mock {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(State::default()));
        let shared = Arc::clone(&state);
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let state = Arc::clone(&shared);
                tokio::spawn(async move {
                    let service = service_fn(move |request| answer(Arc::clone(&state), request));
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        Mock { addr, state }
    }

    fn reply(&self, reply: Reply) {
        self.state.lock().unwrap().replies.push_back(reply);
    }

    fn received(&self) -> Vec<Received> {
        std::mem::take(&mut self.state.lock().unwrap().received)
    }

    fn only_request(&self) -> Received {
        let mut received = self.received();
        assert_eq!(received.len(), 1, "{received:#?}");
        received.pop().unwrap()
    }

    fn target(&self) -> RemoteTarget {
        RemoteTarget {
            endpoint: format!("http://{}", self.addr),
            bucket: "bucket".to_owned(),
            prefix: None,
        }
    }

    fn client(&self) -> AwsS3 {
        AwsS3::new(&self.target(), "us-east-1", credentials())
    }
}

async fn answer(
    state: Arc<Mutex<State>>,
    request: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let (parts, body) = request.into_parts();
    let body = body.collect().await.unwrap().to_bytes();
    let reply = {
        let mut state = state.lock().unwrap();
        state.received.push(Received {
            method: parts.method,
            target: parts.uri.path_and_query().unwrap().to_string(),
            headers: parts.headers,
            body,
        });
        state.replies.pop_front().expect("a scripted reply")
    };
    match reply {
        Reply::Respond {
            status,
            headers,
            body,
        } => {
            let mut response = Response::builder().status(StatusCode::from_u16(status).unwrap());
            for (name, value) in headers {
                response = response.header(name, value);
            }
            Ok(response.body(Full::new(Bytes::from(body))).unwrap())
        }
        Reply::Hang => std::future::pending().await,
    }
}

fn credentials() -> SharedCredentialsProvider {
    SharedCredentialsProvider::new(Credentials::new("AKIDTEST", "secret", None, None, "test"))
}

fn etag(value: &str) -> ETag {
    ETag::new(value).unwrap()
}

/// The hex SHA-256 of `hello`, the payload hash SigV4 signs.
const HELLO_SHA256: &str = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";

#[tokio::test]
async fn put_object_sends_a_signed_conditional_put() {
    let mock = Mock::start().await;
    let store = mock.client();
    assert!(store.is_path_style());

    mock.reply(ok(&[("etag", "\"e1\""), ("x-amz-version-id", "v1")], ""));
    let mut metadata = UserMetadata::new();
    metadata.insert("skys3-wid", "c/b/1/2.3").unwrap();
    let request = PutObject::new("dir/a b+c.txt", "hello")
        .with_metadata(metadata)
        .with_content_type("text/plain")
        .with_precondition(WritePrecondition::IfAbsent);
    let output = store.put_object(request).await.unwrap();
    assert_eq!(output.etag, etag("e1"));
    assert_eq!(output.version_id, Some(VersionId("v1".to_owned())));

    let put = mock.only_request();
    assert_eq!(put.method, Method::PUT);
    assert_eq!(put.path(), "/bucket/dir/a%20b%2Bc.txt");
    assert_eq!(put.query(), Vec::<&str>::new());
    assert_eq!(put.body, "hello");
    assert_eq!(put.header("if-none-match"), Some("*"));
    assert_eq!(put.header("if-match"), None);
    assert_eq!(put.header("x-amz-meta-skys3-wid"), Some("c/b/1/2.3"));
    assert_eq!(put.header("content-type"), Some("text/plain"));
    assert_eq!(put.header("content-length"), Some("5"));
    assert_eq!(put.header("x-amz-content-sha256"), Some(HELLO_SHA256));
    let authorization = put.header("authorization").unwrap();
    assert!(
        authorization.starts_with("AWS4-HMAC-SHA256 Credential=AKIDTEST/")
            && authorization.contains("/us-east-1/s3/aws4_request")
            && authorization.contains("if-none-match"),
        "{authorization}"
    );
    // Flexible checksums are sent only where S3 requires them.
    assert!(
        put.headers
            .keys()
            .all(|name| !name.as_str().starts_with("x-amz-checksum")),
        "{:?}",
        put.headers
    );
}

#[tokio::test]
async fn put_object_sends_headers_tags_and_content_md5() {
    let mock = Mock::start().await;
    let store = mock.client();
    mock.reply(ok(&[("etag", "\"5d41402abc4b2a76b9719d911017c592\"")], ""));
    let headers = BTreeMap::from([
        ("cache-control".to_owned(), "no-cache".to_owned()),
        ("content-disposition".to_owned(), "inline".to_owned()),
        ("content-encoding".to_owned(), "identity".to_owned()),
        ("content-language".to_owned(), "en".to_owned()),
        (
            "expires".to_owned(),
            "Wed, 21 Oct 2015 07:28:00 GMT".to_owned(),
        ),
    ]);
    let tags = BTreeMap::from([
        ("team".to_owned(), "a b".to_owned()),
        ("x&y".to_owned(), "1=2".to_owned()),
    ]);
    let request = PutObject::new("k", "hello")
        .with_headers(headers)
        .with_tags(tags)
        .with_content_md5("XUFAKrxLKna5cZ2REBfFkg==");
    store.put_object(request).await.unwrap();
    let put = mock.only_request();
    assert_eq!(put.header("cache-control"), Some("no-cache"));
    assert_eq!(put.header("content-disposition"), Some("inline"));
    assert_eq!(put.header("content-encoding"), Some("identity"));
    assert_eq!(put.header("content-language"), Some("en"));
    assert_eq!(put.header("expires"), Some("Wed, 21 Oct 2015 07:28:00 GMT"));
    assert_eq!(put.header("x-amz-tagging"), Some("team=a%20b&x%26y=1%3D2"));
    assert_eq!(put.header("content-md5"), Some("XUFAKrxLKna5cZ2REBfFkg=="));
}

#[tokio::test]
async fn writes_carry_if_match() {
    let mock = Mock::start().await;
    let store = mock.client();
    let current = etag("0123abcd");

    mock.reply(ok(&[("etag", "\"e2\"")], ""));
    let request =
        PutObject::new("k", "x").with_precondition(WritePrecondition::IfMatch(current.clone()));
    let output = store.put_object(request).await.unwrap();
    assert_eq!(output.version_id, None);
    let put = mock.only_request();
    assert_eq!(put.header("if-match"), Some("\"0123abcd\""));
    assert_eq!(put.header("if-none-match"), None);
    assert!(
        put.headers
            .keys()
            .all(|name| !name.as_str().starts_with("x-amz-meta-"))
    );

    mock.reply(status(
        204,
        &[("x-amz-delete-marker", "true"), ("x-amz-version-id", "m1")],
        "",
    ));
    let deleted = store
        .delete_object(DeleteObject::new("k").with_if_match(current.clone()))
        .await
        .unwrap();
    assert!(deleted.delete_marker);
    assert_eq!(deleted.version_id, Some(VersionId("m1".to_owned())));
    let delete = mock.only_request();
    assert_eq!(delete.method, Method::DELETE);
    assert_eq!(delete.path(), "/bucket/k");
    assert_eq!(delete.header("if-match"), Some("\"0123abcd\""));

    mock.reply(status(204, &[], ""));
    let deleted = store
        .delete_object(DeleteObject::new("k").with_version_id(VersionId("v 1".to_owned())))
        .await
        .unwrap();
    assert!(!deleted.delete_marker);
    let delete = mock.only_request();
    assert_eq!(delete.query(), ["versionId=v%201"]);
    assert_eq!(delete.header("if-match"), None);
}

#[tokio::test]
async fn writes_to_a_peer_carry_their_signed_apply_by_time() {
    let mock = Mock::start().await;
    let store = mock.client();
    let by = Some(1_800_000_000_000);
    let signed = |request: &Received| {
        assert_eq!(request.header("x-skys3-apply-by"), Some("1800000000000"));
        let authorization = request.header("authorization").unwrap();
        assert!(
            authorization.contains("x-skys3-apply-by"),
            "{authorization}"
        );
    };

    mock.reply(ok(&[("etag", "\"e1\"")], ""));
    let put = PutObject {
        apply_by_ms: by,
        ..PutObject::new("k", "x")
    };
    store.put_object(put).await.unwrap();
    signed(&mock.only_request());

    mock.reply(status(204, &[], ""));
    let delete = DeleteObject {
        apply_by_ms: by,
        ..DeleteObject::new("k")
    };
    store.delete_object(delete).await.unwrap();
    signed(&mock.only_request());

    mock.reply(ok(
        &[],
        "<CopyObjectResult><ETag>\"c1\"</ETag></CopyObjectResult>",
    ));
    let copy = CopyObject {
        apply_by_ms: by,
        ..CopyObject::new("src", "dst")
    };
    store.copy_object(copy).await.unwrap();
    signed(&mock.only_request());

    mock.reply(ok(
        &[],
        "<CompleteMultipartUploadResult><ETag>\"abc-1\"</ETag>\
         </CompleteMultipartUploadResult>",
    ));
    let complete = CompleteMultipartUpload {
        key: "k".to_owned(),
        upload_id: UploadId("u".to_owned()),
        parts: vec![CompletedPart {
            part_number: 1,
            etag: etag("p1"),
        }],
        precondition: WritePrecondition::None,
        apply_by_ms: by,
    };
    store.complete_multipart_upload(complete).await.unwrap();
    signed(&mock.only_request());

    // Without one, nothing is sent.
    mock.reply(ok(&[("etag", "\"e2\"")], ""));
    store.put_object(PutObject::new("k", "y")).await.unwrap();
    assert_eq!(mock.only_request().header("x-skys3-apply-by"), None);
}

#[tokio::test]
async fn copies_replace_metadata_and_check_the_source() {
    let mock = Mock::start().await;
    let store = mock.client();
    let mut metadata = UserMetadata::new();
    metadata.insert("skys3-wid", "c/b/1/2.4").unwrap();

    mock.reply(ok(
        &[("x-amz-version-id", "v9")],
        "<CopyObjectResult><ETag>\"c1\"</ETag>\
         <LastModified>2026-01-01T00:00:00.000Z</LastModified></CopyObjectResult>",
    ));
    let request = CopyObject::new("src dir/a+b", "dst")
        .with_source_version_id(VersionId("sv1".to_owned()))
        .with_source_if_match(etag("s1"))
        .with_metadata_directive(MetadataDirective::Replace {
            metadata,
            content_type: Some("application/octet-stream".to_owned()),
            headers: BTreeMap::from([
                ("cache-control".to_owned(), "no-cache".to_owned()),
                ("content-language".to_owned(), "en".to_owned()),
            ]),
        })
        .with_tagging_directive(TaggingDirective::Replace(BTreeMap::from([
            ("a b".to_owned(), "c&d".to_owned()),
            ("team".to_owned(), "x".to_owned()),
        ])))
        .with_precondition(WritePrecondition::IfAbsent);
    let output = store.copy_object(request).await.unwrap();
    assert_eq!(output.etag, etag("c1"));
    assert_eq!(output.version_id, Some(VersionId("v9".to_owned())));

    let copy = mock.only_request();
    assert_eq!(copy.method, Method::PUT);
    assert_eq!(copy.path(), "/bucket/dst");
    assert_eq!(
        copy.header("x-amz-copy-source"),
        Some("bucket/src%20dir/a%2Bb?versionId=sv1")
    );
    assert_eq!(copy.header("x-amz-copy-source-if-match"), Some("\"s1\""));
    assert_eq!(copy.header("x-amz-metadata-directive"), Some("REPLACE"));
    assert_eq!(copy.header("x-amz-meta-skys3-wid"), Some("c/b/1/2.4"));
    assert_eq!(
        copy.header("content-type"),
        Some("application/octet-stream")
    );
    assert_eq!(copy.header("cache-control"), Some("no-cache"));
    assert_eq!(copy.header("content-language"), Some("en"));
    assert_eq!(copy.header("x-amz-tagging-directive"), Some("REPLACE"));
    assert_eq!(copy.header("x-amz-tagging"), Some("a%20b=c%26d&team=x"));
    assert_eq!(copy.header("if-none-match"), Some("*"));

    mock.reply(ok(
        &[],
        "<CopyObjectResult><ETag>\"c2\"</ETag></CopyObjectResult>",
    ));
    let request =
        CopyObject::new("a", "b").with_precondition(WritePrecondition::IfMatch(etag("d1")));
    store.copy_object(request).await.unwrap();
    let copy = mock.only_request();
    assert_eq!(copy.header("x-amz-copy-source"), Some("bucket/a"));
    assert_eq!(copy.header("x-amz-metadata-directive"), Some("COPY"));
    assert_eq!(copy.header("x-amz-tagging-directive"), Some("COPY"));
    assert_eq!(copy.header("x-amz-tagging"), None);
    assert_eq!(copy.header("if-match"), Some("\"d1\""));
    assert_eq!(copy.header("x-amz-copy-source-if-match"), None);

    // A 200 without a result is malformed: the copy may have been applied.
    mock.reply(ok(&[], ""));
    let error = store
        .copy_object(CopyObject::new("a", "b"))
        .await
        .unwrap_err();
    assert_eq!(error.status(), Some(200));
    assert!(error.may_have_applied());
}

#[tokio::test]
async fn reads_send_ranges_and_conditions() {
    let mock = Mock::start().await;
    let store = mock.client();

    mock.reply(status(
        206,
        &[
            ("etag", "\"g1\""),
            ("content-range", "bytes 2-4/10"),
            ("content-length", "3"),
            ("content-type", "text/plain"),
            ("x-amz-version-id", "v2"),
            ("x-amz-meta-owner", "team"),
        ],
        "llo",
    ));
    let request = GetObject::new("k")
        .with_version_id(VersionId("v2".to_owned()))
        .with_range(ByteRange::inclusive(2, 4).unwrap())
        .with_if_match(etag("g1"))
        .with_if_none_match(etag("g0"));
    let output = store.get_object(request).await.unwrap();
    assert_eq!(output.body, "llo");
    assert_eq!(output.range, Some(2..5));
    assert_eq!(output.info.size, 10);
    assert_eq!(output.info.etag, etag("g1"));
    assert_eq!(output.info.metadata.get("owner"), Some("team"));
    assert_eq!(output.info.content_type.as_deref(), Some("text/plain"));
    assert_eq!(output.info.tag_count, 0);
    let get = mock.only_request();
    assert_eq!(get.method, Method::GET);
    assert_eq!(get.path(), "/bucket/k");
    assert_eq!(get.query(), ["versionId=v2"]);
    assert_eq!(get.header("range"), Some("bytes=2-4"));
    assert_eq!(get.header("if-match"), Some("\"g1\""));
    assert_eq!(get.header("if-none-match"), Some("\"g0\""));

    mock.reply(ok(&[("etag", "\"g1\""), ("content-length", "5")], "hello"));
    let output = store.get_object(GetObject::new("k")).await.unwrap();
    assert_eq!(output.range, None);
    assert_eq!(output.info.size, 5);
    assert_eq!(output.info.version_id, None);
    mock.received();

    mock.reply(ok(
        &[
            ("etag", "\"h1\""),
            ("content-length", "42"),
            ("x-amz-meta-skys3-wid", "c/b/1/2.3"),
            ("x-amz-tagging-count", "2"),
        ],
        "",
    ));
    let info = store
        .head_object(HeadObject::new("k").with_if_none_match(etag("h0")))
        .await
        .unwrap();
    assert_eq!(info.size, 42);
    assert_eq!(info.tag_count, 2);
    assert_eq!(info.metadata.write_identity(), Some("c/b/1/2.3"));
    let head = mock.only_request();
    assert_eq!(head.method, Method::HEAD);
    assert_eq!(head.header("if-none-match"), Some("\"h0\""));

    mock.reply(status(
        206,
        &[("etag", "\"g1\""), ("content-range", "bytes 9/10")],
        "",
    ));
    let error = store.get_object(GetObject::new("k")).await.unwrap_err();
    assert!(error.message().contains("Content-Range"), "{error}");
}

#[tokio::test]
async fn listings_parse_pages() {
    let mock = Mock::start().await;
    let store = mock.client();

    mock.reply(ok(
        &[],
        "<ListBucketResult><Name>bucket</Name><Prefix>p/</Prefix><KeyCount>2</KeyCount>\
         <MaxKeys>2</MaxKeys><Delimiter>/</Delimiter><IsTruncated>true</IsTruncated>\
         <NextContinuationToken>tok2</NextContinuationToken>\
         <Contents><Key>p/a</Key><ETag>\"e1\"</ETag><Size>3</Size></Contents>\
         <CommonPrefixes><Prefix>p/d/</Prefix></CommonPrefixes></ListBucketResult>",
    ));
    let request = ListObjectsV2::new("p/")
        .with_delimiter("/")
        .with_max_keys(5000)
        .with_continuation_token("tok1")
        .with_start_after("p/0");
    let page = store.list_objects_v2(request).await.unwrap();
    assert_eq!(page.objects.len(), 1);
    assert_eq!(page.objects[0].key, "p/a");
    assert_eq!(page.objects[0].etag, etag("e1"));
    assert_eq!(page.objects[0].size, 3);
    assert_eq!(page.common_prefixes, ["p/d/"]);
    assert!(page.is_truncated);
    assert_eq!(page.next_continuation_token.as_deref(), Some("tok2"));
    let list = mock.only_request();
    assert_eq!(list.method, Method::GET);
    assert_eq!(list.path(), "/bucket/");
    assert_eq!(
        list.query(),
        [
            "continuation-token=tok1",
            "delimiter=%2F",
            "list-type=2",
            "max-keys=1000",
            "prefix=p%2F",
            "start-after=p%2F0",
        ]
    );

    mock.reply(ok(
        &[],
        "<ListBucketResult><IsTruncated>true</IsTruncated></ListBucketResult>",
    ));
    let error = store
        .list_objects_v2(ListObjectsV2::new(""))
        .await
        .unwrap_err();
    assert!(error.message().contains("continuation token"), "{error}");
}

#[tokio::test]
async fn multipart_uploads_round_trip() {
    let mock = Mock::start().await;
    let store = mock.client();
    let upload = UploadId("up/1".to_owned());

    mock.reply(ok(
        &[],
        "<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>k</Key>\
         <UploadId>up/1</UploadId></InitiateMultipartUploadResult>",
    ));
    let mut metadata = UserMetadata::new();
    metadata.insert("skys3-wid", "c/b/1/7.1").unwrap();
    let create = CreateMultipartUpload::new("k")
        .with_metadata(metadata)
        .with_headers(BTreeMap::from([
            ("cache-control".to_owned(), "no-cache".to_owned()),
            (
                "expires".to_owned(),
                "Thu, 01 Dec 1994 16:00:00 GMT".to_owned(),
            ),
        ]))
        .with_tags(BTreeMap::from([("team".to_owned(), "a b".to_owned())]));
    let created = store.create_multipart_upload(create).await.unwrap();
    assert_eq!(created, upload);
    let create = mock.only_request();
    assert_eq!(create.method, Method::POST);
    assert_eq!(create.path(), "/bucket/k");
    assert_eq!(create.query(), ["uploads"]);
    assert_eq!(create.header("x-amz-meta-skys3-wid"), Some("c/b/1/7.1"));
    assert_eq!(create.header("cache-control"), Some("no-cache"));
    assert_eq!(
        create.header("expires"),
        Some("Thu, 01 Dec 1994 16:00:00 GMT")
    );
    assert_eq!(create.header("x-amz-tagging"), Some("team=a%20b"));

    mock.reply(ok(&[("etag", "\"p1\"")], ""));
    let part = UploadPart::new("k", upload.clone(), 1, Bytes::from_static(b"hello"))
        .with_content_md5("XUFAKrxLKna5cZ2REBfFkg==");
    assert_eq!(store.upload_part(part).await.unwrap(), etag("p1"));
    let put = mock.only_request();
    assert_eq!(put.method, Method::PUT);
    assert_eq!(put.query(), ["partNumber=1", "uploadId=up%2F1"]);
    assert_eq!(put.header("x-amz-content-sha256"), Some(HELLO_SHA256));
    assert_eq!(put.header("content-md5"), Some("XUFAKrxLKna5cZ2REBfFkg=="));

    mock.reply(ok(
        &[("x-amz-version-id", "v3")],
        "<CompleteMultipartUploadResult><Bucket>bucket</Bucket><Key>k</Key>\
         <ETag>\"abc-2\"</ETag></CompleteMultipartUploadResult>",
    ));
    let complete = CompleteMultipartUpload {
        key: "k".to_owned(),
        upload_id: upload.clone(),
        parts: vec![
            CompletedPart {
                part_number: 1,
                etag: etag("p1"),
            },
            CompletedPart {
                part_number: 2,
                etag: etag("p2"),
            },
        ],
        precondition: WritePrecondition::IfMatch(etag("old")),
        apply_by_ms: None,
    };
    let output = store.complete_multipart_upload(complete).await.unwrap();
    assert_eq!(output.etag, etag("abc-2"));
    assert_eq!(output.version_id, Some(VersionId("v3".to_owned())));
    let post = mock.only_request();
    assert_eq!(post.method, Method::POST);
    assert_eq!(post.query(), ["uploadId=up%2F1"]);
    assert_eq!(post.header("if-match"), Some("\"old\""));
    let body = std::str::from_utf8(&post.body).unwrap();
    assert!(
        body.contains("<Part><ETag>&quot;p1&quot;</ETag><PartNumber>1</PartNumber></Part>")
            || body.contains("<Part><ETag>\"p1\"</ETag><PartNumber>1</PartNumber></Part>"),
        "{body}"
    );
    assert!(body.contains("<PartNumber>2</PartNumber>"), "{body}");

    mock.reply(ok(
        &[],
        "<ListPartsResult><Bucket>bucket</Bucket><Key>k</Key><UploadId>up/1</UploadId>\
         <PartNumberMarker>0</PartNumberMarker><NextPartNumberMarker>1</NextPartNumberMarker>\
         <MaxParts>1</MaxParts><IsTruncated>true</IsTruncated>\
         <Part><PartNumber>1</PartNumber><ETag>\"p1\"</ETag><Size>5</Size></Part>\
         </ListPartsResult>",
    ));
    let mut list = ListParts::new("k", upload.clone());
    list.part_number_marker = Some(0);
    list.max_parts = 1;
    let page = store.list_parts(list).await.unwrap();
    assert_eq!(page.parts.len(), 1);
    assert_eq!(page.parts[0].part_number, 1);
    assert_eq!(page.parts[0].size, 5);
    assert!(page.is_truncated);
    assert_eq!(page.next_part_number_marker, Some(1));
    let get = mock.only_request();
    assert_eq!(
        get.query(),
        ["max-parts=1", "part-number-marker=0", "uploadId=up%2F1"]
    );

    mock.reply(ok(
        &[],
        "<ListPartsResult><IsTruncated>true</IsTruncated>\
         <NextPartNumberMarker>x</NextPartNumberMarker></ListPartsResult>",
    ));
    let error = store
        .list_parts(ListParts::new("k", upload.clone()))
        .await
        .unwrap_err();
    assert!(error.message().contains("NextPartNumberMarker"), "{error}");
    mock.received();

    mock.reply(status(204, &[], ""));
    let abort = AbortMultipartUpload {
        key: "k".to_owned(),
        upload_id: upload,
    };
    store.abort_multipart_upload(abort).await.unwrap();
    let delete = mock.only_request();
    assert_eq!(delete.method, Method::DELETE);
    assert_eq!(delete.query(), ["uploadId=up%2F1"]);
}

#[tokio::test]
async fn errors_map_to_kinds_without_retries() {
    let mock = Mock::start().await;
    let store = mock.client();
    let put = || PutObject::new("k", "x").with_precondition(WritePrecondition::IfAbsent);

    let cases = [
        (
            error_xml(412, "PreconditionFailed", "did not hold"),
            S3ErrorKind::PreconditionFailed,
        ),
        (
            error_xml(409, "ConditionalRequestConflict", "race"),
            S3ErrorKind::ConditionalRequestConflict,
        ),
        (
            error_xml(501, "NotImplemented", "no"),
            S3ErrorKind::NotImplemented,
        ),
        (error_xml(503, "SlowDown", "slow"), S3ErrorKind::SlowDown),
        (
            error_xml(500, "InternalError", "oops"),
            S3ErrorKind::InternalError,
        ),
        (error_xml(404, "NoSuchKey", "gone"), S3ErrorKind::NoSuchKey),
        (error_xml(403, "AccessDenied", "denied"), S3ErrorKind::Other),
        (
            status(
                502,
                &[("content-type", "text/html")],
                "<html>bad gateway</html>",
            ),
            S3ErrorKind::InternalError,
        ),
    ];
    for (reply, kind) in cases {
        mock.reply(reply);
        let error = store.put_object(put()).await.unwrap_err();
        assert_eq!(error.kind(), kind, "{error}");
        mock.only_request();
    }

    mock.reply(error_xml(403, "AccessDenied", "denied"));
    let error = store.put_object(put()).await.unwrap_err();
    assert_eq!(error.to_string(), "403 AccessDenied: PutObject: denied");
    assert!(!error.may_have_applied() && !error.is_transient());
    mock.received();

    // HEAD errors have no body: the status decides.
    for (code, kind) in [
        (404, S3ErrorKind::NoSuchKey),
        (412, S3ErrorKind::PreconditionFailed),
        (304, S3ErrorKind::NotModified),
    ] {
        mock.reply(status(code, &[], ""));
        let error = store.head_object(HeadObject::new("k")).await.unwrap_err();
        assert_eq!(error.kind(), kind, "{error}");
        mock.only_request();
    }

    mock.reply(status(304, &[("etag", "\"g1\"")], ""));
    let request = GetObject::new("k").with_if_none_match(etag("g1"));
    let error = store.get_object(request).await.unwrap_err();
    assert_eq!(error.kind(), S3ErrorKind::NotModified);
    mock.only_request();

    // A success without an ETag was applied, but cannot be used.
    mock.reply(ok(&[], ""));
    let error = store.put_object(put()).await.unwrap_err();
    assert_eq!(error.kind(), S3ErrorKind::Other);
    assert!(error.may_have_applied());
}

#[tokio::test]
async fn lost_responses_are_timeouts() {
    let mock = Mock::start().await;
    let store = AwsS3::builder(&mock.target(), "us-east-1", credentials())
        .attempt_timeout(Duration::from_millis(200))
        .build();
    mock.reply(Reply::Hang);
    let error = store
        .put_object(PutObject::new("k", "x"))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), S3ErrorKind::Timeout, "{error}");
    assert!(error.is_transient() && error.may_have_applied());

    // Nothing listens on the port: the connection fails.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = RemoteTarget {
        endpoint: format!("http://{}", listener.local_addr().unwrap()),
        bucket: "bucket".to_owned(),
        prefix: None,
    };
    drop(listener);
    let store = AwsS3::builder(&target, "us-east-1", credentials())
        .connect_timeout(Duration::from_secs(2))
        .build();
    let error = store.head_object(HeadObject::new("k")).await.unwrap_err();
    assert_eq!(error.kind(), S3ErrorKind::Timeout, "{error}");
}

#[tokio::test]
async fn the_probe_classifies_a_provider_that_answers_501() {
    // A provider like R2 on CompleteMultipartUpload and DeleteObject, but
    // rejecting the headers instead of ignoring them.
    let mock = Mock::start().await;
    let store = mock.client();
    let not_implemented = || error_xml(501, "NotImplemented", "header not implemented");
    let upload = "<InitiateMultipartUploadResult><UploadId>u</UploadId>\
                  </InitiateMultipartUploadResult>";

    // PutObject: If-None-Match on a missing key, on an existing one, then
    // If-Match with a different ETag and with the current one.
    mock.reply(ok(&[("etag", "\"e1\"")], ""));
    mock.reply(error_xml(412, "PreconditionFailed", "exists"));
    mock.reply(error_xml(412, "PreconditionFailed", "differs"));
    mock.reply(ok(&[("etag", "\"e2\"")], ""));
    // CompleteMultipartUpload: If-None-Match is rejected, so the upload is
    // aborted and the key created with a plain PUT; then If-Match.
    for reply in [
        ok(&[], upload),
        ok(&[("etag", "\"p1\"")], ""),
        not_implemented(),
        status(204, &[], ""),
        ok(&[("etag", "\"e3\"")], ""),
        ok(&[], upload),
        ok(&[("etag", "\"p1\"")], ""),
        not_implemented(),
        status(204, &[], ""),
    ] {
        mock.reply(reply);
    }
    // DeleteObject: the key is created, and If-Match is rejected.
    mock.reply(ok(&[("etag", "\"e4\"")], ""));
    mock.reply(not_implemented());
    // CopyObject: the source is written, and every copy that replaces a
    // directive or carries a precondition is rejected, so the destination
    // is created with a plain PUT to probe If-Match.
    mock.reply(ok(&[("etag", "\"e5\"")], ""));
    for _ in 0..4 {
        mock.reply(not_implemented());
    }
    mock.reply(ok(&[("etag", "\"e6\"")], ""));
    mock.reply(not_implemented());
    // Cleanup: one delete per key.
    for _ in 0..5 {
        mock.reply(status(204, &[], ""));
    }

    let probe = ConditionalProbe::new("", 1);
    let writes = probe.run(&store).await.unwrap();
    assert!(writes.put_object.is_protected());
    assert_eq!(writes.unprotected().len(), 2);
    assert_eq!(writes.copy_object, CopySupport::NONE);
    let received = mock.received();
    assert_eq!(received.len(), 27);
    let copy = &received[16];
    assert_eq!(copy.header("x-amz-metadata-directive"), Some("REPLACE"));
    assert_eq!(copy.header("x-amz-meta-skys3-probe"), Some("replaced"));
    let copy = &received[17];
    assert_eq!(copy.header("x-amz-tagging-directive"), Some("REPLACE"));
    assert_eq!(
        copy.header("x-amz-tagging"),
        Some("skys3-probe-0=probe&skys3-probe-1=probe")
    );
    let deletes: Vec<&str> = received[22..].iter().map(Received::path).collect();
    assert_eq!(
        deletes,
        [
            "/bucket/.skys3-probe/0000000000000001/put-object",
            "/bucket/.skys3-probe/0000000000000001/complete-multipart-upload",
            "/bucket/.skys3-probe/0000000000000001/delete-object",
            "/bucket/.skys3-probe/0000000000000001/copy-source",
            "/bucket/.skys3-probe/0000000000000001/copy-destination",
        ]
    );
}

#[tokio::test]
async fn credential_providers_load_lazily() {
    // Building the default chain resolves nothing, so it needs no network.
    let chain = default_credentials("us-east-1").await;
    let store = AwsS3::new(&Mock::start().await.target(), "us-east-1", chain);
    assert_eq!(store.bucket(), "bucket");

    // The web-identity provider reads its token file before it calls STS.
    let provider = web_identity_credentials(
        "/nonexistent/skys3/token",
        "arn:aws:iam::123456789012:role/skys3",
        "skys3-node",
        "us-east-1",
    );
    let error = provider.provide_credentials().await.unwrap_err();
    let io = std::error::Error::source(&error).and_then(|e| e.downcast_ref::<std::io::Error>());
    assert_eq!(
        io.map(std::io::Error::kind),
        Some(std::io::ErrorKind::NotFound),
        "{error:?}"
    );
}
