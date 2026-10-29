//! The S3 operations, as `s3s` calls them.
//!
//! `s3s` parses each request into its operation's input and serializes the
//! output or error; [`Api`] implements the operations. Operations it does
//! not implement answer `501 NotImplemented`, the default of `s3s`'s
//! [`S3`] trait. The operations that exist only for features SkyS3 rejects
//! (`crate::features`) answer here, after checking that the bucket exists,
//! as S3 does.

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use http::HeaderMap;
use s3s::dto::{
    Bucket, CreateBucketInput, CreateBucketOutput, DeleteBucketInput, DeleteBucketOutput,
    GetBucketEncryptionInput, GetBucketEncryptionOutput, GetBucketLocationInput,
    GetBucketLocationOutput, GetBucketOwnershipControlsInput, GetBucketOwnershipControlsOutput,
    GetBucketVersioningInput, GetBucketVersioningOutput, GetObjectLegalHoldInput,
    GetObjectLegalHoldOutput, GetObjectLockConfigurationInput, GetObjectLockConfigurationOutput,
    GetObjectRetentionInput, GetObjectRetentionOutput, HeadBucketInput, HeadBucketOutput,
    ListBucketsInput, ListBucketsOutput, ListObjectVersionsInput, ListObjectVersionsOutput,
    ObjectOwnership, OwnershipControls, OwnershipControlsRule, PutBucketAclInput,
    PutBucketAclOutput, PutBucketEncryptionInput, PutBucketEncryptionOutput,
    PutBucketOwnershipControlsInput, PutBucketOwnershipControlsOutput, PutBucketVersioningInput,
    PutBucketVersioningOutput, PutObjectAclInput, PutObjectAclOutput, PutObjectLegalHoldInput,
    PutObjectLegalHoldOutput, PutObjectLockConfigurationInput, PutObjectLockConfigurationOutput,
    PutObjectRetentionInput, PutObjectRetentionOutput, Timestamp,
};
use s3s::{S3, S3Request, S3Response, S3Result, s3_error};
use skys3_control::ControlStore;
use skys3_types::{BucketDocument, BucketName};

use crate::buckets::{Buckets, MODE_HEADER, TARGET_HEADER};
use crate::features::{
    BUCKET_OWNER_ENFORCED, acls_not_supported, missing_object_lock, object_lock_not_implemented,
    ownership_not_implemented, sse_not_implemented, versioning_not_implemented,
};
use crate::shard::Shards;

/// The most buckets one ListBuckets page returns (the S3 limit).
const MAX_BUCKETS_PER_PAGE: usize = 10_000;

/// The S3 operations, over the bucket records and the shards.
pub(crate) struct Api<C, H> {
    buckets: Arc<Buckets<C, H>>,
}

impl<C, H> Api<C, H> {
    pub(crate) fn new(buckets: Arc<Buckets<C, H>>) -> Self {
        Self { buckets }
    }
}

impl<C: ControlStore, H: Shards> Api<C, H> {
    /// The bucket a request names, which must exist.
    fn bucket(&self, name: &str) -> S3Result<BucketDocument> {
        self.buckets.require(&bucket_name(name)?)
    }
}

fn bucket_name(name: &str) -> S3Result<BucketName> {
    BucketName::new(name).map_err(|error| s3_error!(InvalidBucketName, "{error}"))
}

/// A header's text value, or `None` if it is absent.
fn header<'a>(headers: &'a HeaderMap, name: &str) -> S3Result<Option<&'a str>> {
    headers
        .get(name)
        .map(|value| {
            value
                .to_str()
                .map_err(|_| s3_error!(InvalidArgument, "{name} must be visible ASCII"))
        })
        .transpose()
}

fn ok<T>(output: T) -> S3Result<S3Response<T>> {
    Ok(S3Response::new(output))
}

#[async_trait::async_trait]
impl<C: ControlStore, H: Shards> S3 for Api<C, H> {
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let name = bucket_name(&req.input.bucket)?;
        let mode = header(&req.headers, MODE_HEADER)?;
        let target = header(&req.headers, TARGET_HEADER)?;
        let bucket = self.buckets.create(name, mode, target).await?;
        ok(CreateBucketOutput {
            location: Some(format!("/{}", bucket.name)),
            ..CreateBucketOutput::default()
        })
    }

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        self.buckets
            .delete(&bucket_name(&req.input.bucket)?)
            .await?;
        ok(DeleteBucketOutput::default())
    }

    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        self.bucket(&req.input.bucket)?;
        ok(HeadBucketOutput::default())
    }

    async fn list_buckets(
        &self,
        req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let input = req.input;
        let limit = match input.max_buckets {
            None => MAX_BUCKETS_PER_PAGE,
            Some(n) if n >= 1 => {
                usize::try_from(n).map_or(MAX_BUCKETS_PER_PAGE, |n| n.min(MAX_BUCKETS_PER_PAGE))
            }
            Some(_) => return Err(s3_error!(InvalidArgument, "max-buckets must be at least 1")),
        };
        let prefix = input.prefix.as_deref().unwrap_or("");
        // The continuation token is the last name of the previous page.
        let after = input.continuation_token.as_deref().unwrap_or("");
        let mut matching = self.buckets.list().into_iter().filter(|bucket| {
            bucket.name.as_str().starts_with(prefix) && bucket.name.as_str() > after
        });
        let page: Vec<_> = matching.by_ref().take(limit).collect();
        let more = matching.next().is_some();
        let continuation_token = more
            .then(|| page.last().map(|bucket| bucket.name.to_string()))
            .flatten();
        let buckets = page
            .into_iter()
            .map(|bucket| Bucket {
                name: Some(bucket.name.into_string()),
                creation_date: Some(Timestamp::from(
                    UNIX_EPOCH + Duration::from_millis(bucket.created_unix_ms),
                )),
                ..Bucket::default()
            })
            .collect();
        ok(ListBucketsOutput {
            buckets: Some(buckets),
            continuation_token,
            prefix: input.prefix,
            ..ListBucketsOutput::default()
        })
    }

    async fn get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        // SkyS3 has no regions: every bucket answers as one in us-east-1.
        self.bucket(&req.input.bucket)?;
        ok(GetBucketLocationOutput::default())
    }

    async fn get_bucket_versioning(
        &self,
        req: S3Request<GetBucketVersioningInput>,
    ) -> S3Result<S3Response<GetBucketVersioningOutput>> {
        // Versioning has never been enabled, so the status is absent.
        self.bucket(&req.input.bucket)?;
        ok(GetBucketVersioningOutput::default())
    }

    async fn put_bucket_versioning(
        &self,
        req: S3Request<PutBucketVersioningInput>,
    ) -> S3Result<S3Response<PutBucketVersioningOutput>> {
        self.bucket(&req.input.bucket)?;
        Err(versioning_not_implemented())
    }

    async fn list_object_versions(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        self.bucket(&req.input.bucket)?;
        Err(versioning_not_implemented())
    }

    async fn put_bucket_encryption(
        &self,
        req: S3Request<PutBucketEncryptionInput>,
    ) -> S3Result<S3Response<PutBucketEncryptionOutput>> {
        self.bucket(&req.input.bucket)?;
        Err(sse_not_implemented())
    }

    async fn get_bucket_encryption(
        &self,
        req: S3Request<GetBucketEncryptionInput>,
    ) -> S3Result<S3Response<GetBucketEncryptionOutput>> {
        self.bucket(&req.input.bucket)?;
        Err(s3_error!(
            ServerSideEncryptionConfigurationNotFoundError,
            "The server side encryption configuration was not found"
        ))
    }

    async fn put_object_lock_configuration(
        &self,
        req: S3Request<PutObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<PutObjectLockConfigurationOutput>> {
        self.bucket(&req.input.bucket)?;
        Err(object_lock_not_implemented())
    }

    async fn get_object_lock_configuration(
        &self,
        req: S3Request<GetObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<GetObjectLockConfigurationOutput>> {
        self.bucket(&req.input.bucket)?;
        Err(s3_error!(
            ObjectLockConfigurationNotFoundError,
            "Object Lock configuration does not exist for this bucket"
        ))
    }

    async fn put_object_retention(
        &self,
        req: S3Request<PutObjectRetentionInput>,
    ) -> S3Result<S3Response<PutObjectRetentionOutput>> {
        self.bucket(&req.input.bucket)?;
        Err(missing_object_lock())
    }

    async fn get_object_retention(
        &self,
        req: S3Request<GetObjectRetentionInput>,
    ) -> S3Result<S3Response<GetObjectRetentionOutput>> {
        self.bucket(&req.input.bucket)?;
        Err(missing_object_lock())
    }

    async fn put_object_legal_hold(
        &self,
        req: S3Request<PutObjectLegalHoldInput>,
    ) -> S3Result<S3Response<PutObjectLegalHoldOutput>> {
        self.bucket(&req.input.bucket)?;
        Err(missing_object_lock())
    }

    async fn get_object_legal_hold(
        &self,
        req: S3Request<GetObjectLegalHoldInput>,
    ) -> S3Result<S3Response<GetObjectLegalHoldOutput>> {
        self.bucket(&req.input.bucket)?;
        Err(missing_object_lock())
    }

    async fn put_bucket_acl(
        &self,
        req: S3Request<PutBucketAclInput>,
    ) -> S3Result<S3Response<PutBucketAclOutput>> {
        // Canned ACLs and grant headers were checked with every request;
        // an owner-only canned ACL changes nothing.
        self.bucket(&req.input.bucket)?;
        if req.input.access_control_policy.is_some() {
            return Err(acls_not_supported());
        }
        ok(PutBucketAclOutput::default())
    }

    async fn put_object_acl(
        &self,
        req: S3Request<PutObjectAclInput>,
    ) -> S3Result<S3Response<PutObjectAclOutput>> {
        self.bucket(&req.input.bucket)?;
        if req.input.access_control_policy.is_some() {
            return Err(acls_not_supported());
        }
        Err(s3_error!(NotImplemented))
    }

    async fn put_bucket_ownership_controls(
        &self,
        req: S3Request<PutBucketOwnershipControlsInput>,
    ) -> S3Result<S3Response<PutBucketOwnershipControlsOutput>> {
        self.bucket(&req.input.bucket)?;
        let enforced = req
            .input
            .ownership_controls
            .rules
            .iter()
            .all(|rule| rule.object_ownership.as_str() == BUCKET_OWNER_ENFORCED);
        if !enforced {
            return Err(ownership_not_implemented());
        }
        ok(PutBucketOwnershipControlsOutput::default())
    }

    async fn get_bucket_ownership_controls(
        &self,
        req: S3Request<GetBucketOwnershipControlsInput>,
    ) -> S3Result<S3Response<GetBucketOwnershipControlsOutput>> {
        self.bucket(&req.input.bucket)?;
        ok(GetBucketOwnershipControlsOutput {
            ownership_controls: Some(OwnershipControls {
                rules: vec![OwnershipControlsRule {
                    object_ownership: ObjectOwnership::from_static(
                        ObjectOwnership::BUCKET_OWNER_ENFORCED,
                    ),
                }],
            }),
        })
    }
}
