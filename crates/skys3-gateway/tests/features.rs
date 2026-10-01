//! Every feature SkyS3 rejects explicitly (design §11) answers with its S3
//! error code, and request limits hold at the gateway.

mod common;

use common::{Setup, setup};
use http::Method;

const VERSIONING_ENABLED: &str = "<VersioningConfiguration \
     xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>Enabled</Status>\
     </VersioningConfiguration>";
const VERSIONING_SUSPENDED: &str =
    "<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>";
const ENCRYPTION: &str = "<ServerSideEncryptionConfiguration><Rule>\
     <ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm>\
     </ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>";
const OBJECT_LOCK: &str = "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled>\
     </ObjectLockConfiguration>";
const RETENTION: &str = "<Retention><Mode>GOVERNANCE</Mode>\
     <RetainUntilDate>2030-01-01T00:00:00Z</RetainUntilDate></Retention>";
const LEGAL_HOLD: &str = "<LegalHold><Status>ON</Status></LegalHold>";
const ACL_POLICY: &str = "<AccessControlPolicy><Owner><ID>o</ID></Owner><AccessControlList>\
     <Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \
     xsi:type=\"Group\"><URI>http://acs.amazonaws.com/groups/global/AllUsers</URI></Grantee>\
     <Permission>READ</Permission></Grant></AccessControlList></AccessControlPolicy>";

fn ownership(rule: &str) -> String {
    format!(
        "<OwnershipControls><Rule><ObjectOwnership>{rule}</ObjectOwnership></Rule>\
         </OwnershipControls>"
    )
}

async fn photos() -> Setup {
    let setup = setup("").await;
    setup.create_local("photos").await;
    setup
}

#[tokio::test]
async fn server_side_encryption_is_rejected() {
    let setup = photos().await;
    let put = |headers: &'static [(&'static str, &'static str)]| {
        setup.call(Method::PUT, "/photos/key", headers, "data")
    };
    put(&[("x-amz-server-side-encryption", "AES256")])
        .await
        .assert(501, Some("NotImplemented"));
    put(&[
        ("x-amz-server-side-encryption", "aws:kms"),
        ("x-amz-server-side-encryption-aws-kms-key-id", "alias/key"),
    ])
    .await
    .assert(501, Some("NotImplemented"));
    put(&[
        ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
        (
            "x-amz-server-side-encryption-customer-key",
            "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
        ),
        ("x-amz-server-side-encryption-customer-key-md5", "ZGlnZXN0"),
    ])
    .await
    .assert(501, Some("NotImplemented"));
    let get = setup
        .call(
            Method::GET,
            "/photos/key",
            &[("x-amz-server-side-encryption-customer-algorithm", "AES256")],
            "",
        )
        .await;
    get.assert(501, Some("NotImplemented"));
    let create = setup
        .call(
            Method::PUT,
            "/other",
            &[("x-amz-server-side-encryption", "AES256")],
            "",
        )
        .await;
    create.assert(501, Some("NotImplemented"));

    let answer = setup
        .call(Method::PUT, "/photos?encryption", &[], ENCRYPTION)
        .await;
    answer.assert(501, Some("NotImplemented"));
    let answer = setup.call(Method::GET, "/photos?encryption", &[], "").await;
    answer.assert(400, Some("ServerSideEncryptionConfigurationNotFoundError"));
}

#[tokio::test]
async fn object_lock_is_rejected() {
    let setup = photos().await;
    let create = setup
        .call(
            Method::PUT,
            "/locked",
            &[("x-amz-bucket-object-lock-enabled", "true")],
            "",
        )
        .await;
    create.assert(501, Some("NotImplemented"));
    assert!(setup.register("locked").await.is_none());
    let put = setup
        .call(
            Method::PUT,
            "/photos/key",
            &[
                ("x-amz-object-lock-mode", "COMPLIANCE"),
                (
                    "x-amz-object-lock-retain-until-date",
                    "2030-01-01T00:00:00Z",
                ),
            ],
            "data",
        )
        .await;
    put.assert(400, Some("InvalidRequest"));
    let hold = setup
        .call(
            Method::PUT,
            "/photos/key",
            &[("x-amz-object-lock-legal-hold", "ON")],
            "",
        )
        .await;
    hold.assert(400, Some("InvalidRequest"));

    let cases = [
        (
            Method::PUT,
            "/photos?object-lock",
            OBJECT_LOCK,
            501,
            "NotImplemented",
        ),
        (
            Method::GET,
            "/photos?object-lock",
            "",
            404,
            "ObjectLockConfigurationNotFoundError",
        ),
        (
            Method::PUT,
            "/photos/key?retention",
            RETENTION,
            400,
            "InvalidRequest",
        ),
        (
            Method::GET,
            "/photos/key?retention",
            "",
            400,
            "InvalidRequest",
        ),
        (
            Method::PUT,
            "/photos/key?legal-hold",
            LEGAL_HOLD,
            400,
            "InvalidRequest",
        ),
        (
            Method::GET,
            "/photos/key?legal-hold",
            "",
            400,
            "InvalidRequest",
        ),
    ];
    for (method, uri, body, status, code) in cases {
        let answer = setup.call(method.clone(), uri, &[], body).await;
        assert_eq!(
            (answer.status.as_u16(), answer.code()),
            (status, Some(code)),
            "{method} {uri}: {answer:?}"
        );
    }
}

#[tokio::test]
async fn local_versioning_is_rejected() {
    let setup = photos().await;
    for body in [VERSIONING_ENABLED, VERSIONING_SUSPENDED] {
        let answer = setup
            .call(Method::PUT, "/photos?versioning", &[], body)
            .await;
        answer.assert(501, Some("NotImplemented"));
    }
    let status = setup.call(Method::GET, "/photos?versioning", &[], "").await;
    status.assert(200, None);
    assert!(!status.body.contains("<Status>"), "{status:?}");
    let versions = setup.call(Method::GET, "/photos?versions", &[], "").await;
    versions.assert(501, Some("NotImplemented"));
    for uri in [
        "/photos/key?versionId=3HL4kqtJlcpXroDTDmJ",
        "/photos/key?versionId=",
    ] {
        let answer = setup.call(Method::GET, uri, &[], "").await;
        answer.assert(400, Some("InvalidArgument"));
    }
    let answer = setup
        .call(Method::DELETE, "/photos/key?versionId=abc", &[], "")
        .await;
    answer.assert(400, Some("InvalidArgument"));
}

#[tokio::test]
async fn acls_other_than_bucket_owner_enforced_are_rejected() {
    let setup = photos().await;
    let create = setup
        .call(Method::PUT, "/public", &[("x-amz-acl", "public-read")], "")
        .await;
    create.assert(400, Some("InvalidBucketAclWithObjectOwnership"));
    let create = setup
        .call(
            Method::PUT,
            "/public",
            &[("x-amz-grant-read", "id=someone")],
            "",
        )
        .await;
    create.assert(400, Some("InvalidBucketAclWithObjectOwnership"));
    let create = setup
        .call(
            Method::PUT,
            "/public",
            &[("x-amz-object-ownership", "ObjectWriter")],
            "",
        )
        .await;
    create.assert(501, Some("NotImplemented"));
    assert!(setup.register("public").await.is_none());
    // Owner-only settings are accepted.
    let create = setup
        .call(
            Method::PUT,
            "/private",
            &[
                ("x-skys3-bucket-mode", "local"),
                ("x-amz-acl", "private"),
                ("x-amz-object-ownership", "BucketOwnerEnforced"),
            ],
            "",
        )
        .await;
    create.assert(200, None);

    let acl = |headers: &'static [(&'static str, &'static str)], body: &'static str| {
        setup.call(Method::PUT, "/photos?acl", headers, body)
    };
    acl(&[("x-amz-acl", "private")], "").await.assert(200, None);
    acl(&[("x-amz-acl", "bucket-owner-full-control")], "")
        .await
        .assert(200, None);
    acl(&[("x-amz-acl", "public-read-write")], "")
        .await
        .assert(400, Some("AccessControlListNotSupported"));
    acl(&[("x-amz-grant-full-control", "id=someone")], "")
        .await
        .assert(400, Some("AccessControlListNotSupported"));
    acl(&[], ACL_POLICY)
        .await
        .assert(400, Some("AccessControlListNotSupported"));
    let object_acl = setup
        .call(Method::PUT, "/photos/key?acl", &[], ACL_POLICY)
        .await;
    object_acl.assert(400, Some("AccessControlListNotSupported"));
    let object_acl = setup
        .call(
            Method::PUT,
            "/photos/key?acl",
            &[("x-amz-acl", "authenticated-read")],
            "",
        )
        .await;
    object_acl.assert(400, Some("AccessControlListNotSupported"));
    let put = setup
        .call(
            Method::PUT,
            "/photos/key",
            &[("x-amz-acl", "public-read")],
            "data",
        )
        .await;
    put.assert(400, Some("AccessControlListNotSupported"));

    for (rule, status, code) in [
        ("BucketOwnerEnforced", 200, None),
        ("ObjectWriter", 501, Some("NotImplemented")),
        ("BucketOwnerPreferred", 501, Some("NotImplemented")),
    ] {
        let body = ownership(rule);
        let answer = setup
            .call(Method::PUT, "/photos?ownershipControls", &[], &body)
            .await;
        answer.assert(status, code);
    }
    let current = setup
        .call(Method::GET, "/photos?ownershipControls", &[], "")
        .await;
    current.assert(200, None);
    assert!(current.body.contains("BucketOwnerEnforced"), "{current:?}");
}

#[tokio::test]
async fn request_limits_hold() {
    let setup = photos().await;
    let key = "k".repeat(1025);
    let answer = setup
        .call(Method::GET, &format!("/photos/{key}"), &[], "")
        .await;
    answer.assert(400, Some("KeyTooLongError"));
    let answer = setup
        .call(
            Method::PUT,
            "/photos/key?partNumber=10001&uploadId=u",
            &[],
            "",
        )
        .await;
    answer.assert(400, Some("InvalidArgument"));
    let many: Vec<(String, &str)> = (0..101).map(|n| (format!("x-many-{n}"), "v")).collect();
    let many: Vec<(&str, &str)> = many.iter().map(|(n, v)| (n.as_str(), *v)).collect();
    let answer = setup.call(Method::GET, "/", &many, "").await;
    answer.assert(400, Some("RequestHeaderSectionTooLarge"));

    let deep = format!(
        "<Delete>{}{}</Delete>",
        "<Object>".repeat(40),
        "</Object>".repeat(40)
    );
    let answer = setup.call(Method::POST, "/photos?delete", &[], &deep).await;
    answer.assert(400, Some("MalformedXML"));
    // Found by fuzzing: the parser's error quoted a NUL byte, which no XML
    // error response can carry.
    let nul = "<//+++++\0\0\0> '\"+";
    let answer = setup.call(Method::PUT, "/photos?lifecycle", &[], nul).await;
    answer.assert(400, Some("MalformedXML"));
    let doctype = "<!DOCTYPE d [<!ENTITY a \"aaaa\">]><Delete>&a;</Delete>";
    let answer = setup
        .call(Method::POST, "/photos?delete", &[], doctype)
        .await;
    answer.assert(400, Some("MalformedXML"));
    // Bodies over the limit, whether declared or not.
    let big = format!("<Tagging>{}</Tagging>", " ".repeat(4 * 1024 * 1024));
    let answer = setup.call(Method::PUT, "/photos?tagging", &[], &big).await;
    answer.assert(400, Some("MaxMessageLengthExceeded"));
    let length = big.len().to_string();
    let answer = setup
        .call(
            Method::PUT,
            "/photos?tagging",
            &[("content-length", &length)],
            "",
        )
        .await;
    answer.assert(400, Some("MaxMessageLengthExceeded"));
}
