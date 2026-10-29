//! The [`ObjectStore`] trait.

use std::fmt;
use std::future::Future;

use skys3_types::ETag;

use crate::S3Error;
use crate::model::{
    AbortMultipartUpload, CompleteMultipartUpload, CopyObject, CreateMultipartUpload, DeleteObject,
    DeleteOutput, GetObject, GetOutput, HeadObject, ListObjectsV2, ListObjectsV2Output, ListParts,
    ListPartsOutput, ObjectInfo, PutObject, UploadId, UploadPart, WriteOutput,
};

/// The result of an [`ObjectStore`] operation.
pub type S3Result<T> = Result<T, S3Error>;

/// One bucket of an S3-compatible object store.
///
/// SkyS3 reaches every S3 store through this trait: remote targets for
/// flush, import, and read-through fill (design §7, §9.1), and the S3
/// control-store backend (design §6.1). The AWS SDK client implements it for
/// real stores, and `skys3-sim` implements it with an in-memory store for
/// simulation, so the same flush and control-store code runs against both.
///
/// Each method is one S3 request. Implementations do not retry: retries,
/// backoff, and the handling of lost responses are the caller's, because
/// what is safe to retry depends on the caller's identities (design §6.1,
/// §7.2).
///
/// # Preconditions
///
/// A store may honor, ignore, or reject each write precondition (design
/// §7.2); the capability probe finds out which. When it honors one:
///
/// - A precondition that does not hold fails with `412 Precondition Failed`
///   ([`S3ErrorKind::PreconditionFailed`](crate::S3ErrorKind::PreconditionFailed)),
///   and nothing is written.
/// - `If-Match` on a key without a current object fails with `404 NoSuchKey`
///   on writes that create objects. The caller treats it like a 412: the
///   object it expected is gone.
/// - A conditional write that races another write to the same key may fail
///   with `409 ConditionalRequestConflict`; the caller re-reads and retries.
///
/// # Design
///
/// The methods return `impl Future + Send`, like the disk traits in
/// `skys3-io`, so callers are generic over the store and no call allocates
/// a boxed future. The trait is therefore not object-safe. A node talks to
/// one kind of store per role, so generics cost nothing; a type-erased
/// wrapper can be added if a caller ever needs to mix kinds.
///
/// Bodies are whole [`Bytes`](bytes::Bytes) values. Flush sends objects
/// part by part, so a request body is bounded by the part size; streaming
/// bodies can be added beside these methods when a caller needs them.
pub trait ObjectStore: fmt::Debug + Send + Sync + 'static {
    /// `PutObject`: stores an object and returns its ETag.
    ///
    /// # Errors
    ///
    /// The precondition errors above, `400 MetadataTooLarge`, `501
    /// NotImplemented` from a store that rejects the precondition, and
    /// transport and server errors.
    fn put_object(&self, request: PutObject) -> impl Future<Output = S3Result<WriteOutput>> + Send;

    /// `GetObject`: reads an object, or a range of it.
    ///
    /// # Errors
    ///
    /// `404 NoSuchKey`, `304 Not Modified` for a matching `If-None-Match`,
    /// `412` for a failed `If-Match`, `416 InvalidRange`, and transport and
    /// server errors.
    fn get_object(&self, request: GetObject) -> impl Future<Output = S3Result<GetOutput>> + Send;

    /// `HeadObject`: reads an object's attributes.
    ///
    /// # Errors
    ///
    /// As [`ObjectStore::get_object`], without `416`.
    fn head_object(&self, request: HeadObject)
    -> impl Future<Output = S3Result<ObjectInfo>> + Send;

    /// `DeleteObject`. Deleting a key without a current object succeeds,
    /// unless the request has `If-Match`.
    ///
    /// # Errors
    ///
    /// The precondition errors above, and transport and server errors.
    fn delete_object(
        &self,
        request: DeleteObject,
    ) -> impl Future<Output = S3Result<DeleteOutput>> + Send;

    /// `ListObjectsV2`: one page of the keys under a prefix.
    ///
    /// # Errors
    ///
    /// `400 InvalidArgument` for a continuation token the store did not
    /// issue, and transport and server errors.
    fn list_objects_v2(
        &self,
        request: ListObjectsV2,
    ) -> impl Future<Output = S3Result<ListObjectsV2Output>> + Send;

    /// `CopyObject` within the bucket.
    ///
    /// # Errors
    ///
    /// `404 NoSuchKey` for a missing source, `412` for a failed
    /// `x-amz-copy-source-if-match`, the destination precondition errors
    /// above, and transport and server errors.
    fn copy_object(
        &self,
        request: CopyObject,
    ) -> impl Future<Output = S3Result<WriteOutput>> + Send;

    /// `CreateMultipartUpload`: starts an upload and returns its ID.
    ///
    /// # Errors
    ///
    /// `400 MetadataTooLarge`, and transport and server errors.
    fn create_multipart_upload(
        &self,
        request: CreateMultipartUpload,
    ) -> impl Future<Output = S3Result<UploadId>> + Send;

    /// `UploadPart`: stores one part and returns its ETag.
    ///
    /// # Errors
    ///
    /// `404 NoSuchUpload`, `400 InvalidArgument` for a part number outside
    /// [`PART_NUMBERS`](crate::PART_NUMBERS), and transport and server
    /// errors.
    fn upload_part(&self, request: UploadPart) -> impl Future<Output = S3Result<ETag>> + Send;

    /// `CompleteMultipartUpload`: assembles the named parts into the object.
    ///
    /// # Errors
    ///
    /// `404 NoSuchUpload`, `400 InvalidPart`, `InvalidPartOrder`, or
    /// `EntityTooSmall`, the precondition errors above, and transport and
    /// server errors.
    fn complete_multipart_upload(
        &self,
        request: CompleteMultipartUpload,
    ) -> impl Future<Output = S3Result<WriteOutput>> + Send;

    /// `AbortMultipartUpload`: discards an upload and its parts.
    ///
    /// # Errors
    ///
    /// `404 NoSuchUpload`, and transport and server errors.
    fn abort_multipart_upload(
        &self,
        request: AbortMultipartUpload,
    ) -> impl Future<Output = S3Result<()>> + Send;

    /// `ListParts`: one page of an upload's parts, which a new primary
    /// reconciles with before it completes an upload it took over (design
    /// §7.3).
    ///
    /// # Errors
    ///
    /// `404 NoSuchUpload`, and transport and server errors.
    fn list_parts(
        &self,
        request: ListParts,
    ) -> impl Future<Output = S3Result<ListPartsOutput>> + Send;
}
