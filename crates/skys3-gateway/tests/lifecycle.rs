//! Bucket lifecycle configurations (design §8.7, §11): Put, Get, and
//! Delete through the S3 API, stored in the bucket register, with the
//! answers S3 gives for what SkyS3 refuses.

mod common;

use common::{Answer, Setup, setup};
use http::Method;
use skys3_control::faults::Fault;
use skys3_control::{ControlStore, Expected, PutOutcome, TypedKey, read};
use skys3_types::lifecycle::{DAY_MS, Expiration, LifecycleRule, RuleFilter};
use skys3_types::{BucketName, RegisterDocument};

fn base64_md5(body: &str) -> String {
    use base64::Engine;
    use md5::Digest;
    base64::engine::general_purpose::STANDARD.encode(md5::Md5::digest(body.as_bytes()))
}

fn configuration(rules: &str) -> String {
    format!(
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{rules}\
         </LifecycleConfiguration>"
    )
}

async fn put(setup: &Setup, bucket: &str, rules: &str) -> Answer {
    let body = configuration(rules);
    let md5 = base64_md5(&body);
    setup
        .call(
            Method::PUT,
            &format!("/{bucket}?lifecycle"),
            &[("content-md5", &md5)],
            &body,
        )
        .await
}

async fn get(setup: &Setup, bucket: &str) -> Answer {
    setup
        .call(Method::GET, &format!("/{bucket}?lifecycle"), &[], "")
        .await
}

async fn stored_rules(setup: &Setup, bucket: &str) -> Option<Vec<LifecycleRule>> {
    setup
        .register(bucket)
        .await
        .unwrap()
        .lifecycle
        .map(|config| config.rules)
}

#[tokio::test]
async fn a_configuration_is_stored_returned_and_removed() {
    let setup = setup("").await;
    setup.create_local("logs").await;
    get(&setup, "logs")
        .await
        .assert(404, Some("NoSuchLifecycleConfiguration"));

    let before = setup.generation().await;
    let rules = "\
        <Rule><ID>tmp</ID><Status>Enabled</Status><Filter><Prefix>tmp/</Prefix></Filter>\
          <Expiration><Days>3</Days></Expiration></Rule>\
        <Rule><ID>tagged</ID><Status>Disabled</Status>\
          <Filter><Tag><Key>class</Key><Value>scratch</Value></Tag></Filter>\
          <Expiration><Date>2030-01-01T00:00:00Z</Date></Expiration></Rule>\
        <Rule><ID>both</ID><Status>Enabled</Status><Filter><And><Prefix>big/</Prefix>\
          <Tag><Key>a</Key><Value>1</Value></Tag><ObjectSizeGreaterThan>100</ObjectSizeGreaterThan>\
          <ObjectSizeLessThan>200</ObjectSizeLessThan></And></Filter>\
          <Expiration><Days>1</Days></Expiration></Rule>\
        <Rule><ID>old</ID><Prefix>old/</Prefix><Status>Enabled</Status>\
          <AbortIncompleteMultipartUpload><DaysAfterInitiation>7</DaysAfterInitiation>\
          </AbortIncompleteMultipartUpload></Rule>\
        <Rule><ID>small</ID><Status>Enabled</Status>\
          <Filter><ObjectSizeLessThan>10</ObjectSizeLessThan></Filter>\
          <Expiration><Days>30</Days></Expiration></Rule>\
        <Rule><Status>Enabled</Status><Filter></Filter>\
          <AbortIncompleteMultipartUpload><DaysAfterInitiation>2</DaysAfterInitiation>\
          </AbortIncompleteMultipartUpload></Rule>";
    put(&setup, "logs", rules).await.assert(200, None);
    assert!(
        setup.generation().await > before,
        "the change was not announced"
    );

    let stored = stored_rules(&setup, "logs").await.unwrap();
    assert_eq!(stored.len(), 6);
    assert_eq!(stored[0].filter.prefix, "tmp/");
    assert_eq!(stored[0].expiration, Some(Expiration::Days(3)));
    assert!(!stored[1].enabled);
    assert_eq!(stored[1].filter.tags["class"], "scratch");
    let date = 21_915 * DAY_MS; // 2030-01-01
    assert_eq!(stored[1].expiration, Some(Expiration::DateMs(date)));
    let both = &stored[2].filter;
    assert_eq!(
        (
            both.prefix.as_str(),
            both.tags.len(),
            both.size_greater_than,
            both.size_less_than
        ),
        ("big/", 1, Some(100), Some(200))
    );
    assert!(stored[3].filter.legacy_prefix);
    assert_eq!(stored[3].abort_upload_days, Some(7));
    assert_eq!(stored[4].filter.size_less_than, Some(10));
    // A rule without an ID gets one; its empty filter matches every key.
    assert_eq!(stored[5].id.len(), 16);
    assert_eq!(stored[5].filter, RuleFilter::default());

    let got = get(&setup, "logs").await;
    got.assert(200, None);
    for expected in [
        "<ID>tmp</ID>",
        "<Filter><Prefix>tmp/</Prefix></Filter>",
        "<Days>3</Days>",
        "<Status>Disabled</Status>",
        "<Filter><Tag><Key>class</Key><Value>scratch</Value></Tag></Filter>",
        "<Date>2030-01-01T00:00:00",
        "<And>",
        "<ObjectSizeGreaterThan>100</ObjectSizeGreaterThan>",
        "<Prefix>old/</Prefix>",
        "<DaysAfterInitiation>7</DaysAfterInitiation>",
        "<Filter><ObjectSizeLessThan>10</ObjectSizeLessThan></Filter>",
        "<Filter><Prefix></Prefix></Filter>",
    ] {
        assert!(got.body.contains(expected), "{expected} in {}", got.body);
    }
    // What Get returns is accepted by Put and stores the same rules.
    let returned = got
        .body
        .split_once("<LifecycleConfiguration")
        .and_then(|(_, rest)| rest.split_once('>'))
        .and_then(|(_, rest)| rest.rsplit_once("</LifecycleConfiguration>"))
        .map(|(rules, _)| rules.to_owned())
        .unwrap();
    put(&setup, "logs", &returned).await.assert(200, None);
    assert_eq!(stored_rules(&setup, "logs").await.unwrap(), stored);

    let delete = setup.call(Method::DELETE, "/logs?lifecycle", &[], "").await;
    delete.assert(204, None);
    assert_eq!(stored_rules(&setup, "logs").await, None);
    get(&setup, "logs")
        .await
        .assert(404, Some("NoSuchLifecycleConfiguration"));
    // Deleting none is no error, and writes nothing.
    let before = setup.generation().await;
    let again = setup.call(Method::DELETE, "/logs?lifecycle", &[], "").await;
    again.assert(204, None);
    assert_eq!(setup.generation().await, before);
}

#[tokio::test]
async fn configurations_are_refused_as_s3_refuses_them() {
    let setup = setup("").await;
    setup.create_local("logs").await;
    let rule = |filter: &str, action: &str| {
        format!("<Rule><ID>r</ID><Status>Enabled</Status>{filter}{action}</Rule>")
    };
    let all = "<Filter><Prefix></Prefix></Filter>";
    let expire = "<Expiration><Days>1</Days></Expiration>";
    let abort = "<AbortIncompleteMultipartUpload><DaysAfterInitiation>1\
                 </DaysAfterInitiation></AbortIncompleteMultipartUpload>";
    let cases: Vec<(String, u16, &str)> = vec![
        (String::new(), 400, "MalformedXML"),
        (rule(all, ""), 400, "InvalidRequest"),
        (rule("", expire), 400, "MalformedXML"),
        (
            rule(&format!("{all}<Prefix>a</Prefix>"), expire),
            400,
            "MalformedXML",
        ),
        (
            rule(
                "<Filter><Prefix>a</Prefix><ObjectSizeLessThan>5</ObjectSizeLessThan></Filter>",
                expire,
            ),
            400,
            "MalformedXML",
        ),
        (
            "<Rule><ID>r</ID><Status>Maybe</Status><Filter></Filter>\
             <Expiration><Days>1</Days></Expiration></Rule>"
                .to_owned(),
            400,
            "MalformedXML",
        ),
        (
            rule(all, "<Expiration><Days>0</Days></Expiration>"),
            400,
            "InvalidArgument",
        ),
        (
            rule(all, "<Expiration><Days>-2</Days></Expiration>"),
            400,
            "InvalidArgument",
        ),
        (rule(all, "<Expiration></Expiration>"), 400, "MalformedXML"),
        (
            rule(
                all,
                "<Expiration><Days>1</Days><Date>2030-01-01T00:00:00Z</Date></Expiration>",
            ),
            400,
            "MalformedXML",
        ),
        (
            rule(
                all,
                "<Expiration><Date>2030-01-01T10:00:00Z</Date></Expiration>",
            ),
            400,
            "InvalidArgument",
        ),
        (
            rule(
                all,
                "<Expiration><Date>1960-01-01T00:00:00Z</Date></Expiration>",
            ),
            400,
            "InvalidArgument",
        ),
        (
            rule(
                all,
                "<AbortIncompleteMultipartUpload><DaysAfterInitiation>0\
                       </DaysAfterInitiation></AbortIncompleteMultipartUpload>",
            ),
            400,
            "InvalidArgument",
        ),
        (
            rule(
                all,
                "<AbortIncompleteMultipartUpload></AbortIncompleteMultipartUpload>",
            ),
            400,
            "MalformedXML",
        ),
        (
            format!("{}{}", rule(all, expire), rule(all, abort)),
            400,
            "InvalidArgument",
        ),
        (
            rule(
                "<Filter><Tag><Key>k</Key><Value>v</Value></Tag></Filter>",
                abort,
            ),
            400,
            "InvalidRequest",
        ),
        (
            rule(
                "<Filter><ObjectSizeGreaterThan>5</ObjectSizeGreaterThan></Filter>",
                abort,
            ),
            400,
            "InvalidRequest",
        ),
        (
            rule(
                "<Filter><And><Tag><Key>k</Key><Value>v</Value></Tag>\
                 <Tag><Key>k</Key><Value>w</Value></Tag></And></Filter>",
                expire,
            ),
            400,
            "InvalidTag",
        ),
        (
            rule(
                "<Filter><And><ObjectSizeGreaterThan>9</ObjectSizeGreaterThan>\
                 <ObjectSizeLessThan>9</ObjectSizeLessThan></And></Filter>",
                expire,
            ),
            400,
            "InvalidArgument",
        ),
        (
            rule(
                "<Filter><ObjectSizeLessThan>-1</ObjectSizeLessThan></Filter>",
                expire,
            ),
            400,
            "InvalidArgument",
        ),
        (
            rule(
                &format!("<Filter><Prefix>{}</Prefix></Filter>", "p".repeat(1025)),
                expire,
            ),
            400,
            "InvalidArgument",
        ),
        (
            rule(
                all,
                "<Transition><Days>1</Days><StorageClass>GLACIER</StorageClass></Transition>",
            ),
            501,
            "NotImplemented",
        ),
        (
            rule(
                all,
                "<NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays>\
                 </NoncurrentVersionExpiration>",
            ),
            501,
            "NotImplemented",
        ),
        (
            rule(
                all,
                "<Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker>\
                 </Expiration>",
            ),
            501,
            "NotImplemented",
        ),
        // ExpiredObjectDeleteMarker false does nothing, so the rule has no
        // action left.
        (
            rule(
                all,
                "<Expiration><ExpiredObjectDeleteMarker>false</ExpiredObjectDeleteMarker>\
                 </Expiration>",
            ),
            400,
            "InvalidRequest",
        ),
    ];
    for (rules, status, code) in cases {
        let answer = put(&setup, "logs", &rules).await;
        assert_eq!(answer.status.as_u16(), status, "{rules}: {answer:?}");
        assert_eq!(answer.code(), Some(code), "{rules}: {answer:?}");
    }
    assert_eq!(stored_rules(&setup, "logs").await, None);

    // The body needs a digest.
    let body = configuration(&rule(all, expire));
    setup
        .call(Method::PUT, "/logs?lifecycle", &[], &body)
        .await
        .assert(400, Some("InvalidRequest"));
    // A thousand and one rules are too many.
    let many: String = (0..1001)
        .map(|n| format!("<Rule><ID>{n}</ID><Status>Enabled</Status>{all}{expire}</Rule>"))
        .collect();
    put(&setup, "logs", &many)
        .await
        .assert(400, Some("InvalidRequest"));
    // Missing buckets.
    put(&setup, "nothing", &rule(all, expire))
        .await
        .assert(404, Some("NoSuchBucket"));
    get(&setup, "nothing")
        .await
        .assert(404, Some("NoSuchBucket"));
    setup
        .call(Method::DELETE, "/nothing?lifecycle", &[], "")
        .await
        .assert(404, Some("NoSuchBucket"));
}

#[tokio::test]
async fn only_local_buckets_have_lifecycle_rules() {
    let setup = setup("").await;
    setup.create_write_back("cached").await;
    let rule = "<Rule><ID>r</ID><Status>Enabled</Status><Filter></Filter>\
                <Expiration><Days>1</Days></Expiration></Rule>";
    put(&setup, "cached", rule)
        .await
        .assert(501, Some("NotImplemented"));
    assert_eq!(stored_rules(&setup, "cached").await, None);
    get(&setup, "cached")
        .await
        .assert(404, Some("NoSuchLifecycleConfiguration"));
    setup
        .call(Method::DELETE, "/cached?lifecycle", &[], "")
        .await
        .assert(204, None);
}

#[tokio::test]
async fn a_change_that_loses_a_race_is_made_again() {
    let setup = setup("").await;
    setup.create_local("logs").await;
    let rule = |days: u32| {
        format!(
            "<Rule><ID>r</ID><Status>Enabled</Status><Filter></Filter>\
             <Expiration><Days>{days}</Days></Expiration></Rule>"
        )
    };
    // Another change of the register lands between each read and its swap.
    let rewrite = |memory: skys3_control::MemoryControlStore| {
        Fault::before(move || {
            let memory = memory.clone();
            async move {
                let key = TypedKey::bucket(&BucketName::new("logs").unwrap());
                let current = read(&memory, &key).await.unwrap().unwrap();
                let mut bucket = current.value;
                bucket.created_unix_ms += 1;
                let value = bucket.to_json().unwrap();
                let outcome = memory
                    .put_if(key.key(), Expected::Version(current.version), value.into())
                    .await
                    .unwrap();
                assert!(matches!(outcome, PutOutcome::Written(_)));
            }
        })
    };
    setup
        .store
        .script([Fault::Pass, rewrite(setup.memory.clone())]);
    put(&setup, "logs", &rule(2)).await.assert(200, None);
    let stored = stored_rules(&setup, "logs").await.unwrap();
    assert_eq!(stored[0].expiration, Some(Expiration::Days(2)));

    // A register that keeps changing ends in OperationAborted.
    let mut faults = Vec::new();
    for _ in 0..3 {
        faults.push(Fault::Pass);
        faults.push(rewrite(setup.memory.clone()));
    }
    setup.store.script(faults);
    put(&setup, "logs", &rule(5))
        .await
        .assert(409, Some("OperationAborted"));
    let stored = stored_rules(&setup, "logs").await.unwrap();
    assert_eq!(stored[0].expiration, Some(Expiration::Days(2)));

    // A store that does not answer: 503, and nothing changed.
    setup.store.script(vec![Fault::Unavailable; 3]);
    put(&setup, "logs", &rule(9))
        .await
        .assert(503, Some("ServiceUnavailable"));
    let stored = stored_rules(&setup, "logs").await.unwrap();
    assert_eq!(stored[0].expiration, Some(Expiration::Days(2)));
}
