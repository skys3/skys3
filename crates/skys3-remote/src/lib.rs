#![forbid(unsafe_code)]
//! Remote targets for SkyS3: the interface to S3-compatible object stores.
//!
//! Section numbers (§) refer to the [SkyS3 design](https://github.com/skys3/skys3/blob/main/docs/skys3-design.md).
//!
//! - [`ObjectStore`]: one bucket of an S3-compatible store. The flusher,
//!   import, and read-through fill use it for remote targets (§7, §9.1), and
//!   the S3 control-store backend for its registers (§6.1). `skys3-sim`
//!   implements it with a simulated store.
//! - [`model`]: the requests and responses of its operations, including
//!   write preconditions ([`WritePrecondition`]) and typed byte ranges
//!   ([`ByteRange`]).
//! - [`UserMetadata`]: `x-amz-meta-*`, which carries the write identity
//!   (§7.2).
//! - [`S3Error`]: S3 error codes, and whether an error is transient or may
//!   hide an applied write.
//! - [`aws`]: the remote-target client, [`ObjectStore`] over the AWS SDK for
//!   AWS S3 and S3-compatible providers, with credentials from `aws-config`
//!   providers (§7, §11).
//! - [`probe`]: the capability probe run when a target is attached, which
//!   finds the write preconditions a store honors (§7.2). It is generic
//!   over [`ObjectStore`], so simulations probe the simulated store.
//!
//! ```
//! use skys3_remote::{PutObject, S3ErrorKind, WritePrecondition};
//!
//! // Create a register only if it does not exist yet (§6.1).
//! let request = PutObject::new("cluster/cluster.json", "{}")
//!     .with_precondition(WritePrecondition::IfAbsent);
//! assert!(request.precondition.is_some());
//! assert_eq!(S3ErrorKind::PreconditionFailed.status(), Some(412));
//! ```

pub mod aws;
mod error;
mod metadata;
pub mod model;
pub mod probe;
mod store;

pub use error::{S3Error, S3ErrorKind};
pub use metadata::{MetadataError, UserMetadata};
pub use model::{
    AbortMultipartUpload, ByteRange, CompleteMultipartUpload, CompletedPart, CopyObject,
    CreateMultipartUpload, DeleteObject, DeleteOutput, GetObject, GetOutput, HeadObject,
    ListObjectsV2, ListObjectsV2Output, ListParts, ListPartsOutput, ListedObject, ListedPart,
    MAX_KEY_LEN, MAX_LIST_KEYS, MAX_LIST_PARTS, MetadataDirective, ObjectInfo, PART_NUMBERS,
    PutObject, UploadId, UploadPart, VersionId, WriteOutput, WritePrecondition,
};
pub use store::{ObjectStore, S3Result};
