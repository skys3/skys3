//! Admission control through the gateway's pipeline (design §7.6, §13):
//! writes that add data get `503 SlowDown` while admission refuses them,
//! before their body is stored, and deletes are always admitted.

mod common;

use std::sync::{Arc, Mutex};

use common::{config, setup_with};
use http::Method;
use skys3_gateway::{Admission, Refusal, ShardRef};
use skys3_types::BucketDocument;

/// An admission that refuses with whatever refusal the test sets, and
/// records what it was asked.
#[derive(Debug, Default)]
struct Switch {
    refusal: Mutex<Option<Refusal>>,
    asked: Mutex<Vec<String>>,
}

impl Switch {
    fn set(&self, refusal: Option<Refusal>) {
        *self.refusal.lock().unwrap() = refusal;
    }
}

impl Admission for Switch {
    fn admit(&self, bucket: &BucketDocument, shard: &ShardRef) -> Result<(), Refusal> {
        assert_eq!(shard.bucket, bucket.bucket_id);
        self.asked.lock().unwrap().push(bucket.name.to_string());
        self.refusal.lock().unwrap().map_or(Ok(()), Err)
    }
}

fn element<'a>(body: &'a str, tag: &str) -> &'a str {
    let open = format!("<{tag}>");
    let start = body.find(&open).unwrap() + open.len();
    let end = body[start..].find(&format!("</{tag}>")).unwrap() + start;
    &body[start..end]
}

#[tokio::test]
async fn refused_writes_get_slow_down_and_deletes_pass() {
    let switch = Arc::new(Switch::default());
    let mut config = config("");
    config.admission = switch.clone();
    let setup = setup_with(config).await;
    setup.create_local("photos").await;

    // Admitted while nothing refuses.
    let put = |key: &str| (Method::PUT, format!("/photos/{key}"));
    let (method, uri) = put("a");
    setup.call(method, &uri, &[], "one").await.assert(200, None);
    let answer = setup
        .call(Method::POST, "/photos/big?uploads", &[], "")
        .await;
    answer.assert(200, None);
    let upload = element(&answer.body, "UploadId").to_owned();
    let uri = format!("/photos/big?partNumber=1&uploadId={upload}");
    let answer = setup.call(Method::PUT, &uri, &[], "part").await;
    answer.assert(200, None);
    let etag = answer.headers["etag"].to_str().unwrap().to_owned();
    let completion = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part>\
         </CompleteMultipartUpload>"
    );
    let tagging = "<Tagging><TagSet><Tag><Key>k</Key><Value>v</Value></Tag></TagSet></Tagging>";

    for refusal in [
        Refusal::BucketBudget,
        Refusal::ClusterBudget,
        Refusal::DiskSpace,
    ] {
        switch.set(Some(refusal));
        let copy = [("x-amz-copy-source", "/photos/a")];
        for (method, uri, headers, body) in [
            (Method::PUT, "/photos/b".to_owned(), &[][..], "two"),
            (Method::PUT, "/photos/c".to_owned(), &copy[..], ""),
            (Method::POST, "/photos/d?uploads".to_owned(), &[][..], ""),
            (
                Method::PUT,
                format!("/photos/big?partNumber=2&uploadId={upload}"),
                &[][..],
                "part",
            ),
            (
                Method::POST,
                format!("/photos/big?uploadId={upload}"),
                &[][..],
                completion.as_str(),
            ),
            (
                Method::PUT,
                "/photos/a?tagging".to_owned(),
                &[][..],
                tagging,
            ),
            (Method::DELETE, "/photos/a?tagging".to_owned(), &[][..], ""),
        ] {
            let answer = setup.call(method, &uri, headers, body).await;
            answer.assert(503, Some("SlowDown"));
            assert!(answer.body.contains(&refusal.to_string()), "{answer:?}");
        }
    }
    // Nothing refused was stored.
    let (method, uri) = (Method::GET, "/photos/b");
    setup
        .call(method, uri, &[], "")
        .await
        .assert(404, Some("NoSuchKey"));

    // Deletes and aborts are admitted, and are not even asked about.
    let asked = switch.asked.lock().unwrap().len();
    setup
        .call(Method::DELETE, "/photos/a", &[], "")
        .await
        .assert(204, None);
    let delete = "<Delete><Object><Key>a</Key></Object></Delete>";
    let md5 = "content-md5";
    let digest = base64_md5(delete);
    setup
        .call(Method::POST, "/photos?delete", &[(md5, &digest)], delete)
        .await
        .assert(200, None);
    let uri = format!("/photos/big?uploadId={upload}");
    setup
        .call(Method::DELETE, &uri, &[], "")
        .await
        .assert(204, None);
    assert_eq!(switch.asked.lock().unwrap().len(), asked);

    // Writes resume once admission lets them through.
    switch.set(None);
    let (method, uri) = put("b");
    setup.call(method, &uri, &[], "two").await.assert(200, None);
    assert!(switch.asked.lock().unwrap().iter().all(|b| b == "photos"));
}

/// The base64 MD5 that DeleteObjects requires of its body.
fn base64_md5(body: &str) -> String {
    use base64::Engine;
    use md5::Digest;
    base64::engine::general_purpose::STANDARD.encode(md5::Md5::digest(body.as_bytes()))
}
