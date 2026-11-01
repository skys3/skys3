//! The S3 operations and rejected requests of the compatibility matrix
//! (`docs/skys3-s3-compatibility.md`, plan M7-01, design §11 and §16.2).
//!
//! [`OPERATIONS`] lists every operation `s3s` routes, which is every
//! operation of the S3 API and the browser-form `PostObject`, with whether
//! SkyS3 supports it. [`REJECTED`] lists every request header, parameter,
//! and signature form that asks for a feature SkyS3 does not support. Each
//! entry carries a request, and the tests send each one through the
//! gateway and check:
//!
//! - that `s3s` routes an operation's request to that operation, so the
//!   list names real operations, none twice;
//! - that the answer is the one the entry gives, so a feature that is not
//!   supported answers with its documented error and is never silently
//!   accepted, and a supported operation answers as it should;
//! - that the matrix's generated sections are what the lists render, so
//!   the published matrix cannot drift from what the gateway does.
//!
//! The list of operations is the one of the `s3s` version in
//! [`S3S_VERSION`]; a new version may route new operations, so the test
//! fails until the list is checked against it. Run the tests with
//! `SKYS3_UPDATE_DOCS=1` to rewrite the generated sections.

mod common;

use std::sync::{Arc, Mutex};

use common::signing::credentials;
use common::{Answer, Setup, config, gateway_with, request, setup};
use http::Method;
use skys3_gateway::SigV4Authenticator;
use skys3_io::ManualWallClock;

/// The `s3s` version whose routes [`OPERATIONS`] lists.
const S3S_VERSION: &str = "0.17.0";

/// The compatibility matrix, relative to the workspace root.
const MATRIX: &str = "docs/skys3-s3-compatibility.md";

/// How well SkyS3 supports an operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Support {
    /// As S3 does it, apart from rejected request headers ([`REJECTED`]).
    Supported,
    /// Supported, except what the note says.
    Partial,
    /// Answered with the error of its probe, whatever the request.
    NotSupported,
}

/// A request a test sends, and the answer it must get.
#[derive(Debug, Clone, Copy)]
struct Probe {
    method: &'static str,
    /// The request target; `{upload}` stands for the open upload's ID.
    uri: &'static str,
    headers: &'static [(&'static str, &'static str)],
    body: &'static str,
    /// Whether the request carries its body's `Content-MD5`.
    md5: bool,
    /// Whether it goes to a gateway that checks SigV4 signatures, rather
    /// than one that trusts every request.
    signed: bool,
    status: u16,
    code: Option<&'static str>,
}

const fn probe(method: &'static str, uri: &'static str, status: u16) -> Probe {
    Probe {
        method,
        uri,
        headers: &[],
        body: "",
        md5: false,
        signed: false,
        status,
        code: None,
    }
}

impl Probe {
    const fn code(mut self, code: &'static str) -> Self {
        self.code = Some(code);
        self
    }

    const fn headers(mut self, headers: &'static [(&'static str, &'static str)]) -> Self {
        self.headers = headers;
        self
    }

    const fn body(mut self, body: &'static str) -> Self {
        self.body = body;
        self
    }

    const fn md5(mut self) -> Self {
        self.md5 = true;
        self
    }

    const fn signed(mut self) -> Self {
        self.signed = true;
        self
    }

    /// The answer, as the matrix writes it.
    fn answer(&self) -> String {
        match self.code {
            Some(code) if self.status == 200 => {
                format!("`200`, with an `Error` entry `{code}` for the key")
            }
            Some(code) => format!("`{} {code}`", self.status),
            None => format!("`{}`", self.status),
        }
    }
}

const fn get(uri: &'static str, status: u16) -> Probe {
    probe("GET", uri, status)
}

const fn put(uri: &'static str, status: u16) -> Probe {
    probe("PUT", uri, status)
}

const fn post(uri: &'static str, status: u16) -> Probe {
    probe("POST", uri, status)
}

const fn delete(uri: &'static str, status: u16) -> Probe {
    probe("DELETE", uri, status)
}

const fn head(uri: &'static str, status: u16) -> Probe {
    probe("HEAD", uri, status)
}

/// A request for an operation that no IAM action maps to, which every
/// caller is denied (design §11).
const fn no_action(method: &'static str, uri: &'static str) -> Probe {
    probe(method, uri, 403).code("AccessDenied")
}

/// A request for an operation that `s3s` routes but SkyS3 does not
/// implement.
const fn unimplemented(method: &'static str, uri: &'static str) -> Probe {
    probe(method, uri, 501).code("NotImplemented")
}

/// One S3 operation.
#[derive(Debug, Clone, Copy)]
struct Operation {
    /// The operation's name in the S3 API, as `s3s` routes it.
    name: &'static str,
    /// The heading the matrix lists it under.
    area: &'static str,
    support: Support,
    /// What is missing for [`Support::Partial`], or anything else worth
    /// knowing.
    note: &'static str,
    probe: Probe,
}

const fn op(
    name: &'static str,
    area: &'static str,
    support: Support,
    note: &'static str,
    probe: Probe,
) -> Operation {
    Operation {
        name,
        area,
        support,
        note,
        probe,
    }
}

const OBJECTS: &str = "Objects";
const MULTIPART: &str = "Multipart uploads";
const LISTING: &str = "Listing";
const BUCKETS: &str = "Buckets";
const LIFECYCLE: &str = "Lifecycle";
const VERSIONING: &str = "Versioning";
const ENCRYPTION: &str = "Server-side encryption";
const OBJECT_LOCK: &str = "Object Lock";
const ACCESS: &str = "Access control";
const CONFIGURATION: &str = "Other bucket configuration";
const ANALYTICS: &str = "Analytics, inventory, and metrics";
const METADATA: &str = "S3 Metadata";
const OTHER: &str = "Other operations";

/// The areas in the order the matrix lists them.
const AREAS: &[&str] = &[
    OBJECTS,
    MULTIPART,
    LISTING,
    BUCKETS,
    LIFECYCLE,
    VERSIONING,
    ENCRYPTION,
    OBJECT_LOCK,
    ACCESS,
    CONFIGURATION,
    ANALYTICS,
    METADATA,
    OTHER,
];

use Support::{NotSupported, Partial, Supported};

const TAGS: &str = "<Tagging><TagSet><Tag><Key>a</Key><Value>b</Value></Tag></TagSet></Tagging>";
const DELETE: &str = "<Delete><Object><Key>gone</Key></Object></Delete>";
const COMPLETE: &str = "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>\
     <ETag>\"00000000000000000000000000000000\"</ETag></Part></CompleteMultipartUpload>";
const LIFECYCLE_RULES: &str = "<LifecycleConfiguration><Rule><ID>tmp</ID><Status>Enabled</Status>\
     <Filter><Prefix>tmp/</Prefix></Filter><Expiration><Days>3</Days></Expiration></Rule>\
     </LifecycleConfiguration>";
const VERSIONING_ENABLED: &str =
    "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>";
const ENCRYPTION_RULE: &str = "<ServerSideEncryptionConfiguration><Rule>\
     <ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm>\
     </ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>";
const OBJECT_LOCK_RULE: &str = "<ObjectLockConfiguration><ObjectLockEnabled>Enabled\
     </ObjectLockEnabled></ObjectLockConfiguration>";
const RETENTION: &str = "<Retention><Mode>GOVERNANCE</Mode>\
     <RetainUntilDate>2030-01-01T00:00:00Z</RetainUntilDate></Retention>";
const LEGAL_HOLD: &str = "<LegalHold><Status>ON</Status></LegalHold>";
const ACL: &str = "<AccessControlPolicy><Owner><ID>o</ID></Owner><AccessControlList>\
     <Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \
     xsi:type=\"Group\"><URI>http://acs.amazonaws.com/groups/global/AllUsers</URI></Grantee>\
     <Permission>READ</Permission></Grant></AccessControlList></AccessControlPolicy>";
const ENFORCED: &str = "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced\
     </ObjectOwnership></Rule></OwnershipControls>";
const OBJECT_WRITER: &str = "<OwnershipControls><Rule><ObjectOwnership>ObjectWriter\
     </ObjectOwnership></Rule></OwnershipControls>";
const POLICY: &str = r#"{"Version":"2012-10-17","Statement":[]}"#;
const PUBLIC_ACCESS_BLOCK: &str = "<PublicAccessBlockConfiguration><BlockPublicAcls>true\
     </BlockPublicAcls></PublicAccessBlockConfiguration>";
const CORS: &str = "<CORSConfiguration><CORSRule><AllowedMethod>GET</AllowedMethod>\
     <AllowedOrigin>*</AllowedOrigin></CORSRule></CORSConfiguration>";
const WEBSITE: &str = "<WebsiteConfiguration><IndexDocument><Suffix>index.html</Suffix>\
     </IndexDocument></WebsiteConfiguration>";
const LOGGING: &str = "<BucketLoggingStatus></BucketLoggingStatus>";
const NOTIFICATION: &str = "<NotificationConfiguration></NotificationConfiguration>";
const REPLICATION: &str = "<ReplicationConfiguration><Role>arn:aws:iam::1:role/r</Role><Rule>\
     <Status>Enabled</Status><Destination><Bucket>arn:aws:s3:::d</Bucket></Destination></Rule>\
     </ReplicationConfiguration>";
const REQUEST_PAYMENT: &str =
    "<RequestPaymentConfiguration><Payer>Requester</Payer></RequestPaymentConfiguration>";
const ACCELERATE: &str =
    "<AccelerateConfiguration><Status>Enabled</Status></AccelerateConfiguration>";
const ANALYTICS_RULE: &str =
    "<AnalyticsConfiguration><Id>a</Id><StorageClassAnalysis/></AnalyticsConfiguration>";
const TIERING: &str = "<IntelligentTieringConfiguration><Id>a</Id><Status>Enabled</Status>\
     <Tiering><Days>90</Days><AccessTier>ARCHIVE_ACCESS</AccessTier></Tiering>\
     </IntelligentTieringConfiguration>";
const INVENTORY: &str = "<InventoryConfiguration><Id>a</Id><IsEnabled>true</IsEnabled>\
     <Destination><S3BucketDestination><Bucket>arn:aws:s3:::d</Bucket><Format>CSV</Format>\
     </S3BucketDestination></Destination><IncludedObjectVersions>Current\
     </IncludedObjectVersions><Schedule><Frequency>Daily</Frequency></Schedule>\
     </InventoryConfiguration>";
const METRICS: &str = "<MetricsConfiguration><Id>a</Id></MetricsConfiguration>";
const RESTORE: &str = "<RestoreRequest><Days>1</Days></RestoreRequest>";
const SELECT: &str = "<SelectObjectContentRequest><Expression>SELECT * FROM S3Object\
     </Expression><ExpressionType>SQL</ExpressionType><InputSerialization><CSV/>\
     </InputSerialization><OutputSerialization><CSV/></OutputSerialization>\
     </SelectObjectContentRequest>";
const FORM: &str = "--b\r\nContent-Disposition: form-data; name=\"key\"\r\n\r\nform\r\n\
     --b\r\nContent-Disposition: form-data; name=\"file\"; filename=\"f\"\r\n\r\ndata\r\n--b--\r\n";

/// Every operation `s3s` routes, in the order the matrix lists them. The
/// tests send the probes in this order, against a `local` bucket `photos`
/// holding the object `key`: a later probe may rely on an earlier one.
const OPERATIONS: &[Operation] = &[
    op(
        "PutObject",
        OBJECTS,
        Supported,
        "Storage class and website redirect are accepted and ignored",
        put("/photos/key", 200).body("data"),
    ),
    op("GetObject", OBJECTS, Supported, "", get("/photos/key", 200)),
    op(
        "HeadObject",
        OBJECTS,
        Supported,
        "",
        head("/photos/key", 200),
    ),
    op(
        "CopyObject",
        OBJECTS,
        Partial,
        "A source in a `read_only` bucket answers `501 NotImplemented`",
        put("/photos/copy", 200).headers(&[("x-amz-copy-source", "photos/key")]),
    ),
    op(
        "DeleteObject",
        OBJECTS,
        Partial,
        "`x-amz-if-match-last-modified-time` and `x-amz-if-match-size` are not supported",
        delete("/photos/gone", 204),
    ),
    op(
        "DeleteObjects",
        OBJECTS,
        Partial,
        "A key's `LastModifiedTime` and `Size` conditions are not supported",
        post("/photos?delete", 200).body(DELETE).md5(),
    ),
    op(
        "PutObjectTagging",
        OBJECTS,
        Supported,
        "",
        put("/photos/key?tagging", 200).body(TAGS),
    ),
    op(
        "GetObjectTagging",
        OBJECTS,
        Partial,
        "Not for the objects of `read_only` buckets (`501 NotImplemented`)",
        get("/photos/key?tagging", 200),
    ),
    op(
        "DeleteObjectTagging",
        OBJECTS,
        Supported,
        "",
        delete("/photos/key?tagging", 204),
    ),
    op(
        "GetObjectAttributes",
        OBJECTS,
        NotSupported,
        "",
        unimplemented("GET", "/photos/key?attributes")
            .headers(&[("x-amz-object-attributes", "ETag")]),
    ),
    op(
        "CreateMultipartUpload",
        MULTIPART,
        Supported,
        "",
        post("/photos/upload?uploads", 200),
    ),
    op(
        "UploadPart",
        MULTIPART,
        Supported,
        "",
        put("/photos/upload?partNumber=1&uploadId={upload}", 200).body("part"),
    ),
    op(
        "UploadPartCopy",
        MULTIPART,
        Supported,
        "",
        put("/photos/upload?partNumber=2&uploadId={upload}", 200)
            .headers(&[("x-amz-copy-source", "photos/key")]),
    ),
    op(
        "ListParts",
        MULTIPART,
        Supported,
        "",
        get("/photos/upload?uploadId={upload}", 200),
    ),
    op(
        "ListMultipartUploads",
        MULTIPART,
        Supported,
        "",
        get("/photos?uploads", 200),
    ),
    op(
        "CompleteMultipartUpload",
        MULTIPART,
        Supported,
        "",
        post("/photos/upload?uploadId={upload}", 400)
            .code("InvalidPart")
            .body(COMPLETE),
    ),
    op(
        "AbortMultipartUpload",
        MULTIPART,
        Partial,
        "`x-amz-if-match-initiated-time` is not supported",
        delete("/photos/upload?uploadId={upload}", 204),
    ),
    op("ListObjects", LISTING, Supported, "", get("/photos", 200)),
    op(
        "ListObjectsV2",
        LISTING,
        Supported,
        "",
        get("/photos?list-type=2", 200),
    ),
    op(
        "ListObjectVersions",
        LISTING,
        NotSupported,
        "Local versioning APIs are not supported",
        get("/photos?versions", 501).code("NotImplemented"),
    ),
    op("ListBuckets", BUCKETS, Supported, "", get("/", 200)),
    op(
        "CreateBucket",
        BUCKETS,
        Supported,
        "The bucket's mode and target come from the `x-skys3-bucket-mode` and \
         `x-skys3-bucket-target` headers; `LocationConstraint` is accepted and ignored",
        put("/other", 200).headers(&[("x-skys3-bucket-mode", "local")]),
    ),
    op("HeadBucket", BUCKETS, Supported, "", head("/other", 200)),
    op(
        "GetBucketLocation",
        BUCKETS,
        Supported,
        "Every bucket answers as one in `us-east-1`",
        get("/other?location", 200),
    ),
    op(
        "DeleteBucket",
        BUCKETS,
        Supported,
        "Detaches the bucket (§11)",
        delete("/other", 204),
    ),
    op(
        "PutBucketLifecycleConfiguration",
        LIFECYCLE,
        Partial,
        "`local` buckets only; `Transition`, the noncurrent-version actions, and \
         `ExpiredObjectDeleteMarker` answer `501 NotImplemented`",
        put("/photos?lifecycle", 200).body(LIFECYCLE_RULES).md5(),
    ),
    op(
        "GetBucketLifecycleConfiguration",
        LIFECYCLE,
        Supported,
        "",
        get("/photos?lifecycle", 200),
    ),
    op(
        "DeleteBucketLifecycle",
        LIFECYCLE,
        Supported,
        "",
        delete("/photos?lifecycle", 204),
    ),
    op(
        "GetBucketVersioning",
        VERSIONING,
        Supported,
        "Reports no status: versioning is never enabled",
        get("/photos?versioning", 200),
    ),
    op(
        "PutBucketVersioning",
        VERSIONING,
        NotSupported,
        "",
        put("/photos?versioning", 501)
            .code("NotImplemented")
            .body(VERSIONING_ENABLED),
    ),
    op(
        "PutBucketEncryption",
        ENCRYPTION,
        NotSupported,
        "",
        put("/photos?encryption", 501)
            .code("NotImplemented")
            .body(ENCRYPTION_RULE),
    ),
    op(
        "GetBucketEncryption",
        ENCRYPTION,
        NotSupported,
        "As for a bucket without a configuration",
        get("/photos?encryption", 400).code("ServerSideEncryptionConfigurationNotFoundError"),
    ),
    op(
        "DeleteBucketEncryption",
        ENCRYPTION,
        NotSupported,
        "",
        unimplemented("DELETE", "/photos?encryption"),
    ),
    op(
        "UpdateObjectEncryption",
        ENCRYPTION,
        NotSupported,
        "",
        no_action("PUT", "/photos/key?encryption"),
    ),
    op(
        "PutObjectLockConfiguration",
        OBJECT_LOCK,
        NotSupported,
        "",
        put("/photos?object-lock", 501)
            .code("NotImplemented")
            .body(OBJECT_LOCK_RULE),
    ),
    op(
        "GetObjectLockConfiguration",
        OBJECT_LOCK,
        NotSupported,
        "As for a bucket without Object Lock",
        get("/photos?object-lock", 404).code("ObjectLockConfigurationNotFoundError"),
    ),
    op(
        "PutObjectRetention",
        OBJECT_LOCK,
        NotSupported,
        "As for a bucket without Object Lock",
        put("/photos/key?retention", 400)
            .code("InvalidRequest")
            .body(RETENTION),
    ),
    op(
        "GetObjectRetention",
        OBJECT_LOCK,
        NotSupported,
        "As for a bucket without Object Lock",
        get("/photos/key?retention", 400).code("InvalidRequest"),
    ),
    op(
        "PutObjectLegalHold",
        OBJECT_LOCK,
        NotSupported,
        "As for a bucket without Object Lock",
        put("/photos/key?legal-hold", 400)
            .code("InvalidRequest")
            .body(LEGAL_HOLD),
    ),
    op(
        "GetObjectLegalHold",
        OBJECT_LOCK,
        NotSupported,
        "As for a bucket without Object Lock",
        get("/photos/key?legal-hold", 400).code("InvalidRequest"),
    ),
    op(
        "PutBucketAcl",
        ACCESS,
        Partial,
        "Only the canned ACLs `private` and `bucket-owner-full-control`, which change nothing",
        put("/photos?acl", 200).headers(&[("x-amz-acl", "private")]),
    ),
    op(
        "GetBucketAcl",
        ACCESS,
        NotSupported,
        "",
        unimplemented("GET", "/photos?acl"),
    ),
    op(
        "PutObjectAcl",
        ACCESS,
        NotSupported,
        "An ACL body answers `400 AccessControlListNotSupported`",
        unimplemented("PUT", "/photos/key?acl").headers(&[("x-amz-acl", "private")]),
    ),
    op(
        "GetObjectAcl",
        ACCESS,
        NotSupported,
        "",
        unimplemented("GET", "/photos/key?acl"),
    ),
    op(
        "PutBucketOwnershipControls",
        ACCESS,
        Partial,
        "`BucketOwnerEnforced` only",
        put("/photos?ownershipControls", 200).body(ENFORCED),
    ),
    op(
        "GetBucketOwnershipControls",
        ACCESS,
        Supported,
        "Always `BucketOwnerEnforced`",
        get("/photos?ownershipControls", 200),
    ),
    op(
        "DeleteBucketOwnershipControls",
        ACCESS,
        NotSupported,
        "",
        unimplemented("DELETE", "/photos?ownershipControls"),
    ),
    op(
        "PutBucketPolicy",
        ACCESS,
        NotSupported,
        "Permissions come from identity policies (§11)",
        unimplemented("PUT", "/photos?policy").body(POLICY),
    ),
    op(
        "GetBucketPolicy",
        ACCESS,
        NotSupported,
        "",
        unimplemented("GET", "/photos?policy"),
    ),
    op(
        "DeleteBucketPolicy",
        ACCESS,
        NotSupported,
        "",
        unimplemented("DELETE", "/photos?policy"),
    ),
    op(
        "GetBucketPolicyStatus",
        ACCESS,
        NotSupported,
        "",
        unimplemented("GET", "/photos?policyStatus"),
    ),
    op(
        "PutPublicAccessBlock",
        ACCESS,
        NotSupported,
        "",
        unimplemented("PUT", "/photos?publicAccessBlock").body(PUBLIC_ACCESS_BLOCK),
    ),
    op(
        "GetPublicAccessBlock",
        ACCESS,
        NotSupported,
        "",
        unimplemented("GET", "/photos?publicAccessBlock"),
    ),
    op(
        "DeletePublicAccessBlock",
        ACCESS,
        NotSupported,
        "",
        unimplemented("DELETE", "/photos?publicAccessBlock"),
    ),
    op(
        "PutBucketAbac",
        ACCESS,
        NotSupported,
        "",
        no_action("PUT", "/photos?abac"),
    ),
    op(
        "GetBucketAbac",
        ACCESS,
        NotSupported,
        "",
        no_action("GET", "/photos?abac"),
    ),
    op(
        "PutBucketTagging",
        CONFIGURATION,
        NotSupported,
        "",
        unimplemented("PUT", "/photos?tagging").body(TAGS),
    ),
    op(
        "GetBucketTagging",
        CONFIGURATION,
        NotSupported,
        "",
        unimplemented("GET", "/photos?tagging"),
    ),
    op(
        "DeleteBucketTagging",
        CONFIGURATION,
        NotSupported,
        "",
        unimplemented("DELETE", "/photos?tagging"),
    ),
    op(
        "PutBucketCors",
        CONFIGURATION,
        NotSupported,
        "",
        unimplemented("PUT", "/photos?cors").body(CORS),
    ),
    op(
        "GetBucketCors",
        CONFIGURATION,
        NotSupported,
        "",
        unimplemented("GET", "/photos?cors"),
    ),
    op(
        "DeleteBucketCors",
        CONFIGURATION,
        NotSupported,
        "",
        unimplemented("DELETE", "/photos?cors"),
    ),
    op(
        "PutBucketWebsite",
        CONFIGURATION,
        NotSupported,
        "",
        unimplemented("PUT", "/photos?website").body(WEBSITE),
    ),
    op(
        "GetBucketWebsite",
        CONFIGURATION,
        NotSupported,
        "",
        unimplemented("GET", "/photos?website"),
    ),
    op(
        "DeleteBucketWebsite",
        CONFIGURATION,
        NotSupported,
        "",
        unimplemented("DELETE", "/photos?website"),
    ),
    op(
        "PutBucketLogging",
        CONFIGURATION,
        NotSupported,
        "",
        unimplemented("PUT", "/photos?logging").body(LOGGING),
    ),
    op(
        "GetBucketLogging",
        CONFIGURATION,
        NotSupported,
        "",
        unimplemented("GET", "/photos?logging"),
    ),
    op(
        "PutBucketNotificationConfiguration",
        CONFIGURATION,
        NotSupported,
        "",
        unimplemented("PUT", "/photos?notification").body(NOTIFICATION),
    ),
    op(
        "GetBucketNotificationConfiguration",
        CONFIGURATION,
        NotSupported,
        "",
        unimplemented("GET", "/photos?notification"),
    ),
    op(
        "PutBucketReplication",
        CONFIGURATION,
        NotSupported,
        "Replication between clusters is configured on the node (§7.8, §8.9)",
        unimplemented("PUT", "/photos?replication").body(REPLICATION),
    ),
    op(
        "GetBucketReplication",
        CONFIGURATION,
        NotSupported,
        "",
        unimplemented("GET", "/photos?replication"),
    ),
    op(
        "DeleteBucketReplication",
        CONFIGURATION,
        NotSupported,
        "",
        unimplemented("DELETE", "/photos?replication"),
    ),
    op(
        "PutBucketRequestPayment",
        CONFIGURATION,
        NotSupported,
        "",
        unimplemented("PUT", "/photos?requestPayment").body(REQUEST_PAYMENT),
    ),
    op(
        "GetBucketRequestPayment",
        CONFIGURATION,
        NotSupported,
        "",
        unimplemented("GET", "/photos?requestPayment"),
    ),
    op(
        "PutBucketAccelerateConfiguration",
        CONFIGURATION,
        NotSupported,
        "",
        unimplemented("PUT", "/photos?accelerate").body(ACCELERATE),
    ),
    op(
        "GetBucketAccelerateConfiguration",
        CONFIGURATION,
        NotSupported,
        "",
        unimplemented("GET", "/photos?accelerate"),
    ),
    op(
        "PutBucketAnalyticsConfiguration",
        ANALYTICS,
        NotSupported,
        "",
        unimplemented("PUT", "/photos?analytics&id=a").body(ANALYTICS_RULE),
    ),
    op(
        "GetBucketAnalyticsConfiguration",
        ANALYTICS,
        NotSupported,
        "",
        unimplemented("GET", "/photos?analytics&id=a"),
    ),
    op(
        "DeleteBucketAnalyticsConfiguration",
        ANALYTICS,
        NotSupported,
        "",
        unimplemented("DELETE", "/photos?analytics&id=a"),
    ),
    op(
        "ListBucketAnalyticsConfigurations",
        ANALYTICS,
        NotSupported,
        "",
        unimplemented("GET", "/photos?analytics"),
    ),
    op(
        "PutBucketIntelligentTieringConfiguration",
        ANALYTICS,
        NotSupported,
        "",
        unimplemented("PUT", "/photos?intelligent-tiering&id=a").body(TIERING),
    ),
    op(
        "GetBucketIntelligentTieringConfiguration",
        ANALYTICS,
        NotSupported,
        "",
        unimplemented("GET", "/photos?intelligent-tiering&id=a"),
    ),
    op(
        "DeleteBucketIntelligentTieringConfiguration",
        ANALYTICS,
        NotSupported,
        "",
        unimplemented("DELETE", "/photos?intelligent-tiering&id=a"),
    ),
    op(
        "ListBucketIntelligentTieringConfigurations",
        ANALYTICS,
        NotSupported,
        "",
        unimplemented("GET", "/photos?intelligent-tiering"),
    ),
    op(
        "PutBucketInventoryConfiguration",
        ANALYTICS,
        NotSupported,
        "",
        unimplemented("PUT", "/photos?inventory&id=a").body(INVENTORY),
    ),
    op(
        "GetBucketInventoryConfiguration",
        ANALYTICS,
        NotSupported,
        "",
        unimplemented("GET", "/photos?inventory&id=a"),
    ),
    op(
        "DeleteBucketInventoryConfiguration",
        ANALYTICS,
        NotSupported,
        "",
        unimplemented("DELETE", "/photos?inventory&id=a"),
    ),
    op(
        "ListBucketInventoryConfigurations",
        ANALYTICS,
        NotSupported,
        "",
        unimplemented("GET", "/photos?inventory"),
    ),
    op(
        "PutBucketMetricsConfiguration",
        ANALYTICS,
        NotSupported,
        "",
        unimplemented("PUT", "/photos?metrics&id=a").body(METRICS),
    ),
    op(
        "GetBucketMetricsConfiguration",
        ANALYTICS,
        NotSupported,
        "",
        unimplemented("GET", "/photos?metrics&id=a"),
    ),
    op(
        "DeleteBucketMetricsConfiguration",
        ANALYTICS,
        NotSupported,
        "",
        unimplemented("DELETE", "/photos?metrics&id=a"),
    ),
    op(
        "ListBucketMetricsConfigurations",
        ANALYTICS,
        NotSupported,
        "",
        unimplemented("GET", "/photos?metrics"),
    ),
    op(
        "CreateBucketMetadataConfiguration",
        METADATA,
        NotSupported,
        "",
        no_action("POST", "/photos?metadataConfiguration"),
    ),
    op(
        "GetBucketMetadataConfiguration",
        METADATA,
        NotSupported,
        "",
        no_action("GET", "/photos?metadataConfiguration"),
    ),
    op(
        "DeleteBucketMetadataConfiguration",
        METADATA,
        NotSupported,
        "",
        no_action("DELETE", "/photos?metadataConfiguration"),
    ),
    op(
        "CreateBucketMetadataTableConfiguration",
        METADATA,
        NotSupported,
        "",
        no_action("POST", "/photos?metadataTable"),
    ),
    op(
        "GetBucketMetadataTableConfiguration",
        METADATA,
        NotSupported,
        "",
        no_action("GET", "/photos?metadataTable"),
    ),
    op(
        "DeleteBucketMetadataTableConfiguration",
        METADATA,
        NotSupported,
        "",
        no_action("DELETE", "/photos?metadataTable"),
    ),
    op(
        "UpdateBucketMetadataAnnotationTableConfiguration",
        METADATA,
        NotSupported,
        "",
        no_action("PUT", "/photos?metadataAnnotationTable"),
    ),
    op(
        "UpdateBucketMetadataInventoryTableConfiguration",
        METADATA,
        NotSupported,
        "",
        no_action("PUT", "/photos?metadataInventoryTable"),
    ),
    op(
        "UpdateBucketMetadataJournalTableConfiguration",
        METADATA,
        NotSupported,
        "",
        no_action("PUT", "/photos?metadataJournalTable"),
    ),
    op(
        "PutObjectAnnotation",
        METADATA,
        NotSupported,
        "",
        no_action("PUT", "/photos/key?annotation"),
    ),
    op(
        "GetObjectAnnotation",
        METADATA,
        NotSupported,
        "",
        no_action("GET", "/photos/key?annotation&annotationName=a"),
    ),
    op(
        "DeleteObjectAnnotation",
        METADATA,
        NotSupported,
        "",
        no_action("DELETE", "/photos/key?annotation"),
    ),
    op(
        "ListObjectAnnotations",
        METADATA,
        NotSupported,
        "",
        no_action("GET", "/photos/key?annotation"),
    ),
    op(
        "SelectObjectContent",
        OTHER,
        NotSupported,
        "",
        unimplemented("POST", "/photos/key?select&select-type=2").body(SELECT),
    ),
    op(
        "RestoreObject",
        OTHER,
        NotSupported,
        "SkyS3 has no archive storage classes",
        unimplemented("POST", "/photos/key?restore").body(RESTORE),
    ),
    op(
        "GetObjectTorrent",
        OTHER,
        NotSupported,
        "",
        no_action("GET", "/photos/key?torrent"),
    ),
    op(
        "PostObject",
        OTHER,
        NotSupported,
        "Browser-based uploads with an HTML form",
        no_action("POST", "/photos")
            .headers(&[("content-type", "multipart/form-data; boundary=b")])
            .body(FORM),
    ),
    op(
        "WriteGetObjectResponse",
        OTHER,
        NotSupported,
        "S3 Object Lambda",
        no_action("POST", "/photos").headers(&[
            ("x-amz-request-route", "route"),
            ("x-amz-request-token", "token"),
        ]),
    ),
    op(
        "CreateSession",
        OTHER,
        NotSupported,
        "S3 Express One Zone",
        no_action("GET", "/photos?session"),
    ),
    op(
        "ListDirectoryBuckets",
        OTHER,
        NotSupported,
        "S3 Express One Zone",
        no_action("GET", "/?x-id=ListDirectoryBuckets"),
    ),
    op(
        "RenameObject",
        OTHER,
        NotSupported,
        "S3 Express One Zone",
        no_action("PUT", "/photos/renamed?renameObject")
            .headers(&[("x-amz-rename-source", "/photos/key")]),
    ),
];

/// A request that asks for a feature SkyS3 does not support.
#[derive(Debug, Clone, Copy)]
struct Rejection {
    /// What is rejected, as the matrix writes it.
    request: &'static str,
    probe: Probe,
}

const fn rejected(request: &'static str, probe: Probe) -> Rejection {
    Rejection { request, probe }
}

/// A base64 value, of no particular checksum, for checksum headers.
const DIGEST: &str =
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

/// Every request header, parameter, and signature form that asks for a
/// feature SkyS3 does not support (design §2.2 and §11), on the operation
/// it is most often sent with. Headers that ask for a rejected feature are
/// refused on every operation.
const REJECTED: &[Rejection] = &[
    rejected(
        "`x-amz-server-side-encryption: AES256` (SSE-S3)",
        put("/photos/sse", 501)
            .code("NotImplemented")
            .headers(&[("x-amz-server-side-encryption", "AES256")])
            .body("data"),
    ),
    rejected(
        "`x-amz-server-side-encryption: aws:kms` and `x-amz-server-side-encryption-aws-kms-key-id` \
         (SSE-KMS)",
        put("/photos/sse", 501)
            .code("NotImplemented")
            .headers(&[
                ("x-amz-server-side-encryption", "aws:kms"),
                ("x-amz-server-side-encryption-aws-kms-key-id", "alias/key"),
            ])
            .body("data"),
    ),
    rejected(
        "`x-amz-server-side-encryption-customer-*` (SSE-C)",
        get("/photos/key", 501).code("NotImplemented").headers(&[
            ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
            (
                "x-amz-server-side-encryption-customer-key",
                "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
            ),
            ("x-amz-server-side-encryption-customer-key-md5", "ZGlnZXN0"),
        ]),
    ),
    rejected(
        "`x-amz-copy-source-server-side-encryption-customer-*` (SSE-C copy source)",
        put("/photos/copy", 501).code("NotImplemented").headers(&[
            ("x-amz-copy-source", "photos/key"),
            (
                "x-amz-copy-source-server-side-encryption-customer-algorithm",
                "AES256",
            ),
        ]),
    ),
    rejected(
        "`x-amz-bucket-object-lock-enabled: true` on CreateBucket",
        put("/locked", 501)
            .code("NotImplemented")
            .headers(&[("x-amz-bucket-object-lock-enabled", "true")]),
    ),
    rejected(
        "`x-amz-object-lock-mode` and `x-amz-object-lock-retain-until-date`",
        put("/photos/locked", 400)
            .code("InvalidRequest")
            .headers(&[
                ("x-amz-object-lock-mode", "GOVERNANCE"),
                (
                    "x-amz-object-lock-retain-until-date",
                    "2030-01-01T00:00:00Z",
                ),
            ])
            .body("data"),
    ),
    rejected(
        "`x-amz-object-lock-legal-hold`",
        put("/photos/locked", 400)
            .code("InvalidRequest")
            .headers(&[("x-amz-object-lock-legal-hold", "ON")])
            .body("data"),
    ),
    rejected(
        "`versionId` other than `null`, on any operation",
        get("/photos/key?versionId=3HL4kqtJlcpXroDTDmJ", 400).code("InvalidArgument"),
    ),
    rejected(
        "`x-amz-acl` other than `private` or `bucket-owner-full-control`, on CreateBucket",
        put("/public", 400)
            .code("InvalidBucketAclWithObjectOwnership")
            .headers(&[("x-amz-acl", "public-read")]),
    ),
    rejected(
        "`x-amz-grant-*`, on CreateBucket",
        put("/public", 400)
            .code("InvalidBucketAclWithObjectOwnership")
            .headers(&[("x-amz-grant-read", "id=someone")]),
    ),
    rejected(
        "`x-amz-acl` other than `private` or `bucket-owner-full-control`, on other operations",
        put("/photos/public", 400)
            .code("AccessControlListNotSupported")
            .headers(&[("x-amz-acl", "public-read")])
            .body("data"),
    ),
    rejected(
        "`x-amz-grant-*`, on other operations",
        put("/photos?acl", 400)
            .code("AccessControlListNotSupported")
            .headers(&[("x-amz-grant-full-control", "id=someone")]),
    ),
    rejected(
        "An ACL document on PutBucketAcl or PutObjectAcl",
        put("/photos/key?acl", 400)
            .code("AccessControlListNotSupported")
            .body(ACL),
    ),
    rejected(
        "`x-amz-object-ownership` other than `BucketOwnerEnforced`, on CreateBucket",
        put("/writer", 501)
            .code("NotImplemented")
            .headers(&[("x-amz-object-ownership", "ObjectWriter")]),
    ),
    rejected(
        "Object ownership other than `BucketOwnerEnforced`, on PutBucketOwnershipControls",
        put("/photos?ownershipControls", 501)
            .code("NotImplemented")
            .body(OBJECT_WRITER),
    ),
    rejected(
        "`If-None-Match` with an ETag, on a write",
        put("/photos/key", 501)
            .code("NotImplemented")
            .headers(&[("if-none-match", "\"8d777f385d3dfec8815d20f7496026dc\"")])
            .body("data"),
    ),
    rejected(
        "`If-Match` and `If-None-Match` together, on a write",
        put("/photos/key", 501)
            .code("NotImplemented")
            .headers(&[("if-match", "*"), ("if-none-match", "*")])
            .body("data"),
    ),
    rejected(
        "`x-amz-if-match-last-modified-time`, on DeleteObject",
        delete("/photos/key", 501)
            .code("NotImplemented")
            .headers(&[(
                "x-amz-if-match-last-modified-time",
                "Wed, 01 Jan 2025 00:00:00 GMT",
            )]),
    ),
    rejected(
        "`x-amz-if-match-size`, on DeleteObject",
        delete("/photos/key", 501)
            .code("NotImplemented")
            .headers(&[("x-amz-if-match-size", "4")]),
    ),
    rejected(
        "A key's `LastModifiedTime` or `Size`, on DeleteObjects",
        post("/photos?delete", 200)
            .code("NotImplemented")
            .body("<Delete><Object><Key>key</Key><Size>4</Size></Object></Delete>")
            .md5(),
    ),
    rejected(
        "`x-amz-if-match-initiated-time`, on AbortMultipartUpload",
        delete(
            "/photos/upload?uploadId=00000000000000000000000000000001",
            501,
        )
        .code("NotImplemented")
        .headers(&[(
            "x-amz-if-match-initiated-time",
            "Wed, 01 Jan 2025 00:00:00 GMT",
        )]),
    ),
    rejected(
        "`x-amz-checksum-sha512`, `x-amz-checksum-md5`, and the XXHASH checksums",
        put("/photos/sum", 501)
            .code("NotImplemented")
            .headers(&[("x-amz-checksum-sha512", DIGEST)])
            .body("data"),
    ),
    rejected(
        "`x-amz-checksum-algorithm` other than CRC32, CRC32C, CRC64NVME, SHA1, or SHA256, on \
         CreateMultipartUpload",
        post("/photos/sum?uploads", 501)
            .code("NotImplemented")
            .headers(&[("x-amz-checksum-algorithm", "SHA512")]),
    ),
    rejected(
        "An access-point or outpost ARN in `x-amz-copy-source`",
        put("/photos/copy", 501).code("NotImplemented").headers(&[(
            "x-amz-copy-source",
            "arn:aws:s3:us-east-1:123456789012:accesspoint/my-ap/object/key",
        )]),
    ),
    rejected(
        "A lifecycle rule's `Transition`",
        put("/photos?lifecycle", 501)
            .code("NotImplemented")
            .body(
                "<LifecycleConfiguration><Rule><ID>t</ID><Status>Enabled</Status>\
                 <Filter><Prefix></Prefix></Filter><Transition><Days>30</Days>\
                 <StorageClass>GLACIER</StorageClass></Transition></Rule>\
                 </LifecycleConfiguration>",
            )
            .md5(),
    ),
    rejected(
        "A lifecycle rule's `NoncurrentVersionExpiration` or `NoncurrentVersionTransition`",
        put("/photos?lifecycle", 501)
            .code("NotImplemented")
            .body(
                "<LifecycleConfiguration><Rule><ID>n</ID><Status>Enabled</Status>\
                 <Filter><Prefix></Prefix></Filter><NoncurrentVersionExpiration>\
                 <NoncurrentDays>30</NoncurrentDays></NoncurrentVersionExpiration></Rule>\
                 </LifecycleConfiguration>",
            )
            .md5(),
    ),
    rejected(
        "A lifecycle rule's `ExpiredObjectDeleteMarker` set to `true`",
        put("/photos?lifecycle", 501)
            .code("NotImplemented")
            .body(
                "<LifecycleConfiguration><Rule><ID>m</ID><Status>Enabled</Status>\
                 <Filter><Prefix></Prefix></Filter><Expiration>\
                 <ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration>\
                 </Rule></LifecycleConfiguration>",
            )
            .md5(),
    ),
    rejected(
        "PutBucketLifecycleConfiguration on a `write_back` or `read_only` bucket",
        put("/remote?lifecycle", 501)
            .code("NotImplemented")
            .body(LIFECYCLE_RULES)
            .md5(),
    ),
    rejected(
        "A write to a `read_only` bucket: PutObject, CopyObject, DeleteObject, DeleteObjects, \
         multipart uploads, and object tags",
        put("/mirror/key", 403).code("AccessDenied").body("data"),
    ),
    rejected(
        "CopyObject from a `read_only` bucket",
        put("/photos/copy", 501)
            .code("NotImplemented")
            .headers(&[("x-amz-copy-source", "mirror/key")]),
    ),
    rejected(
        "GetObjectTagging in a `read_only` bucket",
        get("/mirror/key?tagging", 501).code("NotImplemented"),
    ),
    rejected(
        "Signature Version 2 (`Authorization: AWS ...`)",
        get("/photos/key", 400)
            .code("InvalidRequest")
            .headers(&[("authorization", "AWS AKIDTEST:c2lnbmF0dXJl")])
            .signed(),
    ),
    rejected(
        "A Signature Version 2 presigned URL (`AWSAccessKeyId` and `Signature`)",
        get(
            "/photos/key?AWSAccessKeyId=AKIDTEST&Expires=1790812800&Signature=c2ln",
            400,
        )
        .code("InvalidRequest")
        .signed(),
    ),
    rejected(
        "SigV4a (`AWS4-ECDSA-P256-SHA256`)",
        get("/photos/key", 400)
            .code("InvalidRequest")
            .headers(&[(
                "authorization",
                "AWS4-ECDSA-P256-SHA256 Credential=AKIDTEST/20260101/s3/aws4_request, \
                 SignedHeaders=host, Signature=00",
            )])
            .signed(),
    ),
    rejected(
        "A SigV4a `aws-chunked` body (`STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD`)",
        put("/photos/chunked", 400)
            .code("InvalidRequest")
            .headers(&[
                (
                    "authorization",
                    "AWS4-HMAC-SHA256 Credential=AKIDTEST/20261001/us-east-1/s3/aws4_request, \
                     SignedHeaders=host;x-amz-content-sha256;x-amz-date, \
                     Signature=0000000000000000000000000000000000000000000000000000000000000000",
                ),
                ("x-amz-date", "20261001T000000Z"),
                (
                    "x-amz-content-sha256",
                    "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD",
                ),
            ])
            .signed(),
    ),
    rejected(
        "An unsigned request, unless `anonymous_access` is on",
        get("/photos/key", 403).code("AccessDenied").signed(),
    ),
];

/// Records the operation `s3s` routes each request to, from the debug
/// event it emits once it has resolved the route.
#[derive(Clone, Default)]
struct Routes(Arc<Mutex<Vec<String>>>);

impl Routes {
    fn take(&self) -> Vec<String> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

impl tracing::Subscriber for Routes {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        struct Fields {
            message: String,
            op: Option<String>,
        }
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                match field.name() {
                    "message" => self.message = format!("{value:?}"),
                    "op" => self.op = Some(format!("{value:?}")),
                    _ => {}
                }
            }
        }
        let mut fields = Fields {
            message: String::new(),
            op: None,
        };
        event.record(&mut fields);
        if fields.message == "resolved route"
            && let Some(op) = fields.op
        {
            self.0.lock().unwrap().push(op);
        }
    }

    fn enter(&self, _: &tracing::span::Id) {}

    fn exit(&self, _: &tracing::span::Id) {}
}

/// The trusting gateway with the `local` bucket `photos` and its object
/// `key`, the `write_back` bucket `remote`, and the `read_only` bucket
/// `mirror`.
async fn photos() -> Setup {
    let setup = setup("").await;
    setup.create_local("photos").await;
    setup.create_write_back("remote").await;
    setup
        .create("mirror", "read_only", Some("https://origin.example/data/"))
        .await
        .assert(200, None);
    setup
        .call(Method::PUT, "/photos/key", &[], "data")
        .await
        .assert(200, None);
    setup
}

fn content_md5(body: &str) -> String {
    use base64::Engine as _;
    use md5::Digest as _;
    base64::engine::general_purpose::STANDARD.encode(md5::Md5::digest(body.as_bytes()))
}

/// Sends `probe` to `setup`'s gateway, or, if it is signed, to `signing`,
/// with `upload` for `{upload}`.
async fn send(
    setup: &Setup,
    signing: &skys3_gateway::Gateway<SigV4Authenticator<skys3_gateway::sigv4::MemoryCredentials>>,
    probe: &Probe,
    upload: &str,
) -> Answer {
    let uri = probe.uri.replace("{upload}", upload);
    let md5 = content_md5(probe.body);
    let mut headers = probe.headers.to_vec();
    if probe.md5 {
        headers.push(("content-md5", &md5));
    }
    let method = Method::from_bytes(probe.method.as_bytes()).unwrap();
    let request = request(method, &uri, &headers, probe.body);
    if probe.signed {
        common::answer(signing.handle(request).await).await
    } else {
        setup.send(request).await
    }
}

/// A gateway that checks SigV4 signatures, with the bucket `photos`.
async fn signing_gateway()
-> skys3_gateway::Gateway<SigV4Authenticator<skys3_gateway::sigv4::MemoryCredentials>> {
    let clock = Arc::new(ManualWallClock::new(std::time::Duration::from_secs(
        common::signing::NOW,
    )));
    gateway_with(config(""), SigV4Authenticator::new(credentials(), clock)).await
}

/// Records a failure if `answer` is not the one `probe` expects.
fn check(failures: &mut Vec<String>, what: &str, probe: &Probe, answer: &Answer) {
    if (answer.status.as_u16(), answer.code()) != (probe.status, probe.code) {
        failures.push(format!(
            "{what}: expected {} {:?}, answered {} {:?}: {}",
            probe.status,
            probe.code,
            answer.status.as_u16(),
            answer.code(),
            answer.body
        ));
    }
}

/// Sends every probe, in one test: the routes are read from `s3s`'s debug
/// events, which reach this thread's subscriber only if no other thread
/// sends requests through `s3s` meanwhile (a callsite first hit on a
/// thread without the subscriber would stay disabled).
#[tokio::test]
async fn every_request_answers_as_the_matrix_says() {
    let routes = Routes::default();
    let _guard = tracing::subscriber::set_default(routes.clone());
    tracing::callsite::rebuild_interest_cache();
    let setup = photos().await;
    let signing = signing_gateway().await;
    let mut failures = Vec::new();

    let mut upload = String::new();
    let mut seen = std::collections::BTreeSet::new();
    for operation in OPERATIONS {
        assert!(
            seen.insert(operation.name),
            "{} is listed twice",
            operation.name
        );
        assert!(AREAS.contains(&operation.area), "{}", operation.name);
        let probe = &operation.probe;
        match operation.support {
            NotSupported => assert!(
                probe.status >= 400 && probe.code.is_some(),
                "{}: an unsupported operation answers an error",
                operation.name
            ),
            Supported | Partial => assert!(
                !matches!(probe.code, Some("NotImplemented" | "AccessDenied")),
                "{}: a supported operation is implemented",
                operation.name
            ),
        }
        if operation.support == Partial {
            assert!(
                !operation.note.is_empty(),
                "{}: say what is missing",
                operation.name
            );
        }
        routes.take();
        let answer = send(&setup, &signing, probe, &upload).await;
        if operation.name == "CreateMultipartUpload" {
            upload = element(&answer.body, "UploadId").unwrap_or_default();
        }
        let routed = routes.take();
        if routed != [operation.name] {
            failures.push(format!("{}: routed to {routed:?}", operation.name));
        }
        check(&mut failures, operation.name, probe, &answer);
    }

    for rejection in REJECTED {
        let answer = send(&setup, &signing, &rejection.probe, "").await;
        check(&mut failures, rejection.request, &rejection.probe, &answer);
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    // Nothing a rejected request asked for was done.
    for name in ["locked", "public", "writer"] {
        assert!(setup.register(name).await.is_none(), "{name} was created");
    }
    for key in ["sse", "locked", "public", "sum"] {
        let answer = setup
            .call(Method::HEAD, &format!("/photos/{key}"), &[], "")
            .await;
        assert_eq!(answer.status, 404, "{key} was written");
    }
}

#[test]
fn the_operations_are_those_of_the_pinned_s3s() {
    let lock = std::fs::read_to_string(workspace().join("Cargo.lock")).unwrap();
    let version = lock
        .split("\n[[package]]\n")
        .find_map(|package| {
            package
                .strip_prefix("name = \"s3s\"\nversion = \"")?
                .split('"')
                .next()
        })
        .expect("s3s in Cargo.lock");
    assert_eq!(
        version, S3S_VERSION,
        "s3s changed: check OPERATIONS against the operations it routes, then S3S_VERSION"
    );
    // `s3s` routes the 112 operations of the S3 API and PostObject.
    assert_eq!(OPERATIONS.len(), 113);
}

#[test]
fn the_matrix_lists_every_operation_and_rejected_request() {
    let mut operations = String::new();
    for area in AREAS {
        operations.push_str(&format!(
            "\n### {area}\n\n| Operation | Status | Notes |\n|---|---|---|\n"
        ));
        for operation in OPERATIONS.iter().filter(|op| op.area == *area) {
            let (status, note) = match operation.support {
                Supported => ("Supported", operation.note.to_owned()),
                Partial => ("Partial", operation.note.to_owned()),
                NotSupported => {
                    let answer = operation.probe.answer();
                    let note = if operation.note.is_empty() {
                        answer
                    } else {
                        format!("{answer}. {}", operation.note)
                    };
                    ("Not supported", note)
                }
            };
            let note = if note.is_empty() {
                " ".to_owned()
            } else {
                format!(" {note} ")
            };
            operations.push_str(&format!("| {} | {status} |{note}|\n", operation.name));
        }
    }
    let count = |support| OPERATIONS.iter().filter(|op| op.support == support).count();
    let summary = format!(
        "{} operations: {} supported, {} partially supported, {} not supported.\n",
        OPERATIONS.len(),
        count(Supported),
        count(Partial),
        count(NotSupported),
    );
    check_section("operations", &format!("{summary}{operations}"));

    let mut rejected = String::from("| Request | Answer |\n|---|---|\n");
    for rejection in REJECTED {
        rejected.push_str(&format!(
            "| {} | {} |\n",
            rejection.request,
            rejection.probe.answer()
        ));
    }
    check_section("rejected", &rejected);
}

/// The workspace root.
fn workspace() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Checks that the generated `section` of the matrix holds `generated`,
/// or with `SKYS3_UPDATE_DOCS=1`, writes it there. A section is the text
/// between a line `<!-- BEGIN <section>: ... -->` and a line
/// `<!-- END <section> -->`.
fn check_section(section: &str, generated: &str) {
    let path = workspace().join(MATRIX);
    let text = std::fs::read_to_string(&path).unwrap();
    let begin = format!("<!-- BEGIN {section}:");
    let start = text
        .find(&begin)
        .and_then(|at| text[at..].find('\n').map(|end| at + end + 1))
        .unwrap_or_else(|| panic!("{MATRIX} has no `{begin}...` line"));
    let end = format!("<!-- END {section} -->");
    let stop = text[start..]
        .find(&end)
        .map(|at| start + at)
        .unwrap_or_else(|| panic!("{MATRIX} has no `{end}` line"));
    if text[start..stop] == *generated {
        return;
    }
    if std::env::var("SKYS3_UPDATE_DOCS").is_ok_and(|value| value == "1") {
        let updated = format!("{}{generated}{}", &text[..start], &text[stop..]);
        std::fs::write(&path, updated).unwrap();
        return;
    }
    panic!(
        "the `{section}` section of {MATRIX} is out of date; rerun with SKYS3_UPDATE_DOCS=1 \
         to rewrite it. It should read:\n{generated}"
    );
}

/// The text of the first `<name>` element of `body`.
fn element(body: &str, name: &str) -> Option<String> {
    let start = body.find(&format!("<{name}>"))? + name.len() + 2;
    let end = body[start..].find(&format!("</{name}>"))? + start;
    Some(body[start..end].to_owned())
}
