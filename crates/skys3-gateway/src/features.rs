//! S3 features SkyS3 rejects explicitly (design §11): server-side
//! encryption, Object Lock, local versioning, and ACLs other than
//! bucket-owner-enforced.
//!
//! [`reject_unsupported`] checks the headers and query parameters that ask
//! for one of them on any request, so object operations added later are
//! covered without code of their own. The operations that exist only for
//! these features answer in [`crate::api`]. Each answer is the one S3 gives
//! where it has one:
//!
//! | Request | Answer |
//! |---|---|
//! | `x-amz-server-side-encryption*` (SSE-S3, SSE-KMS, SSE-C), `x-amz-copy-source-server-side-encryption-customer-*` | `501 NotImplemented` |
//! | `x-amz-bucket-object-lock-enabled: true` | `501 NotImplemented` |
//! | `x-amz-object-lock-*` | `400 InvalidRequest`, as for a bucket without Object Lock |
//! | `x-amz-grant-*`, or an `x-amz-acl` other than `private` or `bucket-owner-full-control` | `400 InvalidBucketAclWithObjectOwnership` on CreateBucket, otherwise `400 AccessControlListNotSupported`, as for a bucket-owner-enforced bucket |
//! | `x-amz-object-ownership` other than `BucketOwnerEnforced` | `501 NotImplemented` |
//! | `versionId` other than `null` | `400 InvalidArgument`, as for an unversioned bucket |

use http::HeaderMap;
use http::request::Parts;
use s3s::{S3Error, s3_error};

use crate::limits::RequestShape;

/// The canned ACLs that grant nothing beyond the bucket owner's full
/// control, which a bucket-owner-enforced bucket accepts.
pub(crate) const OWNER_ONLY_ACLS: [&str; 2] = ["private", "bucket-owner-full-control"];

/// The one object ownership setting SkyS3 supports.
pub(crate) const BUCKET_OWNER_ENFORCED: &str = "BucketOwnerEnforced";

pub(crate) fn sse_not_implemented() -> S3Error {
    s3_error!(
        NotImplemented,
        "SkyS3 does not support server-side encryption (SSE-S3, SSE-KMS, or SSE-C); \
         encrypt objects before uploading them"
    )
}

pub(crate) fn object_lock_not_implemented() -> S3Error {
    s3_error!(NotImplemented, "SkyS3 does not support Object Lock")
}

pub(crate) fn missing_object_lock() -> S3Error {
    s3_error!(
        InvalidRequest,
        "Bucket is missing Object Lock Configuration"
    )
}

pub(crate) fn versioning_not_implemented() -> S3Error {
    s3_error!(
        NotImplemented,
        "SkyS3 does not support bucket versioning; versioning belongs to the remote target"
    )
}

pub(crate) fn acls_not_supported() -> S3Error {
    s3_error!(
        AccessControlListNotSupported,
        "The bucket does not allow ACLs"
    )
}

pub(crate) fn ownership_not_implemented() -> S3Error {
    s3_error!(
        NotImplemented,
        "SkyS3 buckets use the BucketOwnerEnforced object ownership setting; ACLs are disabled"
    )
}

/// Rejects a request that asks for a feature SkyS3 does not support.
///
/// # Errors
///
/// The S3 error the table in the module documentation gives.
pub(crate) fn reject_unsupported(parts: &Parts, shape: &RequestShape) -> Result<(), S3Error> {
    let headers = &parts.headers;
    for name in headers.keys().map(http::HeaderName::as_str) {
        if name.starts_with("x-amz-server-side-encryption")
            || name.starts_with("x-amz-copy-source-server-side-encryption")
        {
            return Err(sse_not_implemented());
        }
        if name.starts_with("x-amz-object-lock-") {
            return Err(missing_object_lock());
        }
    }
    if header_is(headers, "x-amz-bucket-object-lock-enabled", |v| {
        !v.eq_ignore_ascii_case("false")
    }) {
        return Err(object_lock_not_implemented());
    }
    let grants = headers
        .keys()
        .any(|name| name.as_str().starts_with("x-amz-grant-"));
    if grants || header_is(headers, "x-amz-acl", |v| !OWNER_ONLY_ACLS.contains(&v)) {
        return Err(if shape.is_create_bucket(parts) {
            s3_error!(
                InvalidBucketAclWithObjectOwnership,
                "Bucket cannot have ACLs set with ObjectOwnership's BucketOwnerEnforced setting"
            )
        } else {
            acls_not_supported()
        });
    }
    if header_is(headers, "x-amz-object-ownership", |v| {
        v != BUCKET_OWNER_ENFORCED
    }) {
        return Err(ownership_not_implemented());
    }
    if shape
        .query()
        .any(|(name, value)| name == "versionId" && value != "null")
    {
        return Err(s3_error!(InvalidArgument, "Invalid version id specified"));
    }
    Ok(())
}

/// Whether header `name` is present and its value (or, if it is not text,
/// any value) satisfies `test`. Every occurrence is checked.
fn header_is(headers: &HeaderMap, name: &str, test: impl Fn(&str) -> bool) -> bool {
    headers
        .get_all(name)
        .iter()
        .any(|value| value.to_str().map_or(true, |value| test(value.trim())))
}

#[cfg(test)]
mod tests {
    use http::{Method, Request};
    use s3s::S3ErrorCode;

    use super::*;
    use crate::limits::RequestLimits;

    fn check(method: Method, uri: &str, headers: &[(&str, &str)]) -> Result<(), S3Error> {
        let mut request = Request::builder().method(method).uri(uri);
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        let parts = request.body(()).unwrap().into_parts().0;
        let shape = RequestLimits::default().check_head(&parts).unwrap();
        reject_unsupported(&parts, &shape)
    }

    fn code(method: Method, uri: &str, headers: &[(&str, &str)]) -> S3ErrorCode {
        check(method, uri, headers).unwrap_err().code().clone()
    }

    #[test]
    fn server_side_encryption_is_not_implemented() {
        for header in [
            ("x-amz-server-side-encryption", "AES256"),
            ("x-amz-server-side-encryption", "aws:kms"),
            ("x-amz-server-side-encryption-aws-kms-key-id", "k"),
            ("x-amz-server-side-encryption-bucket-key-enabled", "true"),
            ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
            ("x-amz-server-side-encryption-customer-key", "a2V5"),
            (
                "x-amz-copy-source-server-side-encryption-customer-key",
                "a2V5",
            ),
        ] {
            assert_eq!(
                code(Method::PUT, "/b/k", &[header]),
                S3ErrorCode::NotImplemented,
                "{header:?}"
            );
        }
        assert_eq!(
            code(
                Method::PUT,
                "/b",
                &[("x-amz-server-side-encryption", "AES256")]
            ),
            S3ErrorCode::NotImplemented
        );
    }

    #[test]
    fn object_lock_is_rejected() {
        for header in [
            ("x-amz-object-lock-mode", "GOVERNANCE"),
            (
                "x-amz-object-lock-retain-until-date",
                "2030-01-01T00:00:00Z",
            ),
            ("x-amz-object-lock-legal-hold", "ON"),
        ] {
            assert_eq!(
                code(Method::PUT, "/b/k", &[header]),
                S3ErrorCode::InvalidRequest
            );
        }
        let lock = |v| {
            code(
                Method::PUT,
                "/b",
                &[("x-amz-bucket-object-lock-enabled", v)],
            )
        };
        assert_eq!(lock("true"), S3ErrorCode::NotImplemented);
        assert_eq!(lock("TRUE"), S3ErrorCode::NotImplemented);
        check(
            Method::PUT,
            "/b",
            &[("x-amz-bucket-object-lock-enabled", "false")],
        )
        .unwrap();
    }

    #[test]
    fn acls_other_than_owner_full_control_are_rejected() {
        for acl in OWNER_ONLY_ACLS {
            check(Method::PUT, "/b", &[("x-amz-acl", acl)]).unwrap();
            check(Method::PUT, "/b/k", &[("x-amz-acl", acl)]).unwrap();
        }
        let public = [("x-amz-acl", "public-read")];
        assert_eq!(
            code(Method::PUT, "/b", &public),
            S3ErrorCode::InvalidBucketAclWithObjectOwnership
        );
        assert_eq!(
            code(Method::PUT, "/b?acl", &public),
            S3ErrorCode::AccessControlListNotSupported
        );
        assert_eq!(
            code(Method::PUT, "/b/k", &public),
            S3ErrorCode::AccessControlListNotSupported
        );
        let grant = [(
            "x-amz-grant-read",
            "uri=\"http://acs.amazonaws.com/groups/global/AllUsers\"",
        )];
        assert_eq!(
            code(Method::PUT, "/b", &grant),
            S3ErrorCode::InvalidBucketAclWithObjectOwnership
        );
        assert_eq!(
            code(Method::PUT, "/b/k", &grant),
            S3ErrorCode::AccessControlListNotSupported
        );
        let ownership = |v| code(Method::PUT, "/b", &[("x-amz-object-ownership", v)]);
        assert_eq!(ownership("ObjectWriter"), S3ErrorCode::NotImplemented);
        assert_eq!(
            ownership("BucketOwnerPreferred"),
            S3ErrorCode::NotImplemented
        );
        check(
            Method::PUT,
            "/b",
            &[("x-amz-object-ownership", "BucketOwnerEnforced")],
        )
        .unwrap();
    }

    #[test]
    fn version_ids_other_than_null_are_rejected() {
        check(Method::GET, "/b/k?versionId=null", &[]).unwrap();
        assert_eq!(
            code(Method::GET, "/b/k?versionId=3HL4kqtJlcpXroDTDmJ", &[]),
            S3ErrorCode::InvalidArgument
        );
        assert_eq!(
            code(Method::DELETE, "/b/k?versionId=", &[]),
            S3ErrorCode::InvalidArgument
        );
    }

    #[test]
    fn non_text_header_values_count_as_requests_for_the_feature() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-acl", http::HeaderValue::from_bytes(b"\xff").unwrap());
        assert!(header_is(&headers, "x-amz-acl", |_| false));
        assert!(!header_is(&headers, "x-amz-object-ownership", |_| true));
    }
}
