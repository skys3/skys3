//! The S3 endpoint as a peer cluster sees it (design §7.8): the peer
//! descriptor of a receiving bucket, and a peer's flushes over S3 REST,
//! which may write the bucket and carry the source's write identity.

mod common;

use std::sync::Arc;

use bytes::Bytes;
use common::{answer, config, gateway_with, request};
use http::{Method, Request};
use s3s::{Body, S3Error};
use skys3_gateway::sigv4::{AuthMethod, Authenticated};
use skys3_gateway::{
    Authenticator, DESCRIPTOR_KEY, Gateway, GatewayConfig, MODE_HEADER, PeerDescriptors,
    Permissions, Principal,
};
use skys3_types::{BucketName, ClusterId};

/// The header the test authenticator reads the access key from.
const KEY_HEADER: &str = "x-test-access-key";
/// The access key of `prod-us`'s flushers.
const PEER_KEY: &str = "AKIAPRODUS";
/// An identity `prod-us` may carry into `archive`.
const IDENTITY: &str = "prod-us/b-src/3/2.17";

/// Takes every request as signed by whatever access key its test header
/// names, with every permission.
#[derive(Debug, Clone, Copy)]
struct ByKey;

impl Authenticator for ByKey {
    async fn authenticate(&self, mut request: Request<Body>) -> Result<Request<Body>, S3Error> {
        let key = request
            .headers()
            .get(KEY_HEADER)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("AKIACLIENT")
            .to_owned();
        request.extensions_mut().insert(Authenticated {
            principal: Principal::new(key.clone(), Permissions::allow_all()),
            access_key_id: key,
            method: AuthMethod::Header,
        });
        Ok(request)
    }
}

/// Signs nothing: answers a fixed descriptor for the bucket it expects.
#[derive(Debug)]
struct Fixed;

impl PeerDescriptors for Fixed {
    fn descriptor(&self, bucket: &BucketName, source: &ClusterId) -> Option<Bytes> {
        (bucket.as_str() == "archive" && source.as_str() == "prod-us")
            .then(|| Bytes::from_static(b"descriptor of archive"))
    }
}

/// `archive` receives from `prod-us`, whose flushers sign with
/// [`PEER_KEY`] and may write it from `b-src`; `photos` is an ordinary
/// bucket.
fn receiving(descriptors: bool) -> GatewayConfig {
    let mut config = config(&format!(
        "[transport]\ntls_cert_file = \"/n.crt\"\ntls_key_file = \"/n.key\"\n\
         tls_ca_file = \"/ca.crt\"\n\
         [buckets.archive]\nmode = \"local\"\npeer_source = \"prod-us\"\n\
         [peering.peers.prod-us]\nca_file = \"/us.crt\"\n\
         buckets = [{{ source = \"b-src\", destination = \"archive\" }}]\n\
         s3_access_key_ids = [\"{PEER_KEY}\"]\n"
    ));
    if descriptors {
        config.descriptors = Some(Arc::new(Fixed));
    }
    config
}

async fn call(
    gateway: &Gateway<ByKey>,
    method: Method,
    uri: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> common::Answer {
    answer(gateway.handle(request(method, uri, headers, body)).await).await
}

async fn start(descriptors: bool) -> Gateway<ByKey> {
    let gateway = gateway_with(receiving(descriptors), ByKey).await;
    for name in ["archive", "photos"] {
        call(
            &gateway,
            Method::PUT,
            &format!("/{name}"),
            &[(MODE_HEADER, "local")],
            "",
        )
        .await
        .assert(200, None);
    }
    gateway
}

#[tokio::test]
async fn a_receiving_bucket_serves_its_descriptor() {
    let gateway = start(true).await;
    let path = format!("/archive/{DESCRIPTOR_KEY}");
    let got = call(&gateway, Method::GET, &path, &[], "").await;
    got.assert(200, None);
    assert_eq!(got.body, "descriptor of archive");
    assert_eq!(got.headers["content-type"], "application/x-protobuf");
    // The MD5 of its bytes.
    assert_eq!(got.headers["etag"], "\"81e1dc64b0752d6777859c7c0a716f89\"");
    let head = call(&gateway, Method::HEAD, &path, &[], "").await;
    head.assert(200, None);
    assert_eq!(head.headers["content-length"], "21");
    // Another bucket has none, and is read as usual.
    call(
        &gateway,
        Method::GET,
        &format!("/photos/{DESCRIPTOR_KEY}"),
        &[],
        "",
    )
    .await
    .assert(404, Some("NoSuchKey"));
    // A node that signs none serves none.
    let gateway = start(false).await;
    call(&gateway, Method::GET, &path, &[], "")
        .await
        .assert(404, Some("NoSuchKey"));
}

#[tokio::test]
async fn a_peer_flushes_over_s3_with_its_write_identity() {
    let gateway = start(false).await;
    let peer = [(KEY_HEADER, PEER_KEY), ("x-amz-meta-skys3-wid", IDENTITY)];
    call(&gateway, Method::PUT, "/archive/k", &peer, "from the peer")
        .await
        .assert(200, None);
    // The peer sees the identity the version carries; clients do not.
    let head = call(
        &gateway,
        Method::HEAD,
        "/archive/k",
        &[(KEY_HEADER, PEER_KEY)],
        "",
    )
    .await;
    head.assert(200, None);
    assert_eq!(head.headers["x-amz-meta-skys3-wid"], IDENTITY);
    let head = call(&gateway, Method::HEAD, "/archive/k", &[], "").await;
    head.assert(200, None);
    assert!(!head.headers.contains_key("x-amz-meta-skys3-wid"));

    // Conditional writes and deletes, as the flusher sends them.
    let etag = head.headers["etag"].to_str().unwrap().to_owned();
    let replace = [
        (KEY_HEADER, PEER_KEY),
        ("x-amz-meta-skys3-wid", "prod-us/b-src/3/2.18"),
        ("if-match", etag.as_str()),
    ];
    call(&gateway, Method::PUT, "/archive/k", &replace, "newer")
        .await
        .assert(200, None);
    call(&gateway, Method::PUT, "/archive/k", &replace, "stale")
        .await
        .assert(412, Some("PreconditionFailed"));
    call(
        &gateway,
        Method::DELETE,
        "/archive/k",
        &[(KEY_HEADER, PEER_KEY)],
        "",
    )
    .await
    .assert(204, None);

    // A multipart upload carries the identity to the completed object.
    let create = call(&gateway, Method::POST, "/archive/m?uploads", &peer, "").await;
    create.assert(200, None);
    let upload = between(&create.body, "<UploadId>", "</UploadId>");
    let part = call(
        &gateway,
        Method::PUT,
        &format!("/archive/m?partNumber=1&uploadId={upload}"),
        &[(KEY_HEADER, PEER_KEY)],
        "part",
    )
    .await;
    part.assert(200, None);
    let complete = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part>\
         </CompleteMultipartUpload>",
        part.headers["etag"].to_str().unwrap()
    );
    call(
        &gateway,
        Method::POST,
        &format!("/archive/m?uploadId={upload}"),
        &[(KEY_HEADER, PEER_KEY)],
        &complete,
    )
    .await
    .assert(200, None);
    let head = call(
        &gateway,
        Method::HEAD,
        "/archive/m",
        &[(KEY_HEADER, PEER_KEY)],
        "",
    )
    .await;
    assert_eq!(head.headers["x-amz-meta-skys3-wid"], IDENTITY);
}

#[tokio::test]
async fn only_the_peer_writes_and_only_its_own_identities() {
    let gateway = start(false).await;
    // The bucket stays read-only to this cluster's clients.
    call(&gateway, Method::PUT, "/archive/k", &[], "local")
        .await
        .assert(403, Some("AccessDenied"));
    // An identity of another cluster, of an unpaired source bucket, or
    // into another bucket, is refused, as is one a client sends.
    for (uri, identity) in [
        ("/archive/k", "prod-ap/b-src/3/2.17"),
        ("/archive/k", "prod-us/b-other/3/2.17"),
        ("/photos/k", IDENTITY),
    ] {
        call(
            &gateway,
            Method::PUT,
            uri,
            &[(KEY_HEADER, PEER_KEY), ("x-amz-meta-skys3-wid", identity)],
            "body",
        )
        .await
        .assert(400, Some("InvalidArgument"));
    }
    call(
        &gateway,
        Method::PUT,
        "/photos/k",
        &[("x-amz-meta-skys3-wid", IDENTITY)],
        "body",
    )
    .await
    .assert(400, Some("InvalidArgument"));
    // The peer may write other buckets as any client, without an
    // identity.
    call(
        &gateway,
        Method::PUT,
        "/photos/k",
        &[(KEY_HEADER, PEER_KEY)],
        "body",
    )
    .await
    .assert(200, None);
}

/// The text between `open` and `close` in `xml`.
fn between<'a>(xml: &'a str, open: &str, close: &str) -> &'a str {
    let start = xml.find(open).unwrap() + open.len();
    let end = start + xml[start..].find(close).unwrap();
    &xml[start..end]
}

#[tokio::test]
async fn no_write_reaches_the_descriptor_key() {
    let gateway = start(true).await;
    let peer = [(KEY_HEADER, PEER_KEY)];
    let with_identity = [(KEY_HEADER, PEER_KEY), ("x-amz-meta-skys3-wid", IDENTITY)];
    // A client in an ordinary bucket, and the peer in the bucket that
    // receives from it, both with an object to copy and an upload of
    // another key whose parts could be copied.
    for (bucket, headers) in [("photos", &[][..]), ("archive", &peer[..])] {
        let reserved = format!("/{bucket}/{DESCRIPTOR_KEY}");
        call(
            &gateway,
            Method::PUT,
            &format!("/{bucket}/src"),
            headers,
            "body",
        )
        .await
        .assert(200, None);
        let copy_source = format!("/{bucket}/src");
        let copy: Vec<(&str, &str)> = headers
            .iter()
            .copied()
            .chain([("x-amz-copy-source", copy_source.as_str())])
            .collect();
        let refusals = [
            (Method::PUT, reserved.clone(), headers, "body"),
            (Method::PUT, reserved.clone(), &copy[..], ""),
            (Method::POST, format!("{reserved}?uploads"), headers, ""),
            (
                Method::PUT,
                format!("{reserved}?partNumber=1&uploadId=u"),
                headers,
                "part",
            ),
            (
                Method::POST,
                format!("{reserved}?uploadId=u"),
                headers,
                "<CompleteMultipartUpload/>",
            ),
            (
                Method::PUT,
                format!("{reserved}?partNumber=1&uploadId=u"),
                &copy[..],
                "",
            ),
            (
                Method::PUT,
                format!("{reserved}?tagging"),
                headers,
                "<Tagging><TagSet></TagSet></Tagging>",
            ),
            (Method::DELETE, format!("{reserved}?tagging"), headers, ""),
            (Method::DELETE, reserved.clone(), headers, ""),
        ];
        for (method, uri, headers, body) in refusals {
            let got = call(&gateway, method.clone(), &uri, headers, body).await;
            assert_eq!(
                (
                    got.status.as_u16(),
                    got.code(),
                    got.body.contains("reserved")
                ),
                (400, Some("InvalidArgument"), true),
                "{method} {uri}: {got:?}"
            );
        }
        // A multi-object delete refuses that key alone.
        let delete = format!(
            "<Delete><Object><Key>{DESCRIPTOR_KEY}</Key></Object>\
             <Object><Key>src</Key></Object></Delete>"
        );
        let digest = md5(&delete);
        let headers: Vec<(&str, &str)> = headers
            .iter()
            .copied()
            .chain([("content-md5", digest.as_str())])
            .collect();
        let got = call(
            &gateway,
            Method::POST,
            &format!("/{bucket}?delete"),
            &headers,
            &delete,
        )
        .await;
        assert_eq!(got.status.as_u16(), 200, "{got:?}");
        assert!(
            got.body.contains(&format!(
                "<Error><Code>InvalidArgument</Code><Key>{DESCRIPTOR_KEY}</Key>"
            )),
            "{got:?}"
        );
        assert!(got.body.contains("<Deleted><Key>src</Key>"), "{got:?}");
    }
    // Nor with the peer's write identity.
    call(
        &gateway,
        Method::PUT,
        &format!("/archive/{DESCRIPTOR_KEY}"),
        &with_identity,
        "body",
    )
    .await
    .assert(400, Some("InvalidArgument"));
    // The descriptor still answers, and the ordinary bucket has nothing.
    let got = call(
        &gateway,
        Method::GET,
        &format!("/archive/{DESCRIPTOR_KEY}"),
        &[],
        "",
    )
    .await;
    assert_eq!(got.body, "descriptor of archive");
    call(
        &gateway,
        Method::GET,
        &format!("/photos/{DESCRIPTOR_KEY}"),
        &[],
        "",
    )
    .await
    .assert(404, Some("NoSuchKey"));
}

fn md5(body: &str) -> String {
    use base64::Engine as _;
    use md5::Digest as _;
    base64::engine::general_purpose::STANDARD.encode(md5::Md5::digest(body.as_bytes()))
}
