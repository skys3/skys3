#![forbid(unsafe_code)]
//! The SkyS3 S3 gateway: the S3 API over HTTP, request limits, key-to-shard
//! routing, and bucket and object operations (design §3, §4.1, §5.1, §9.2,
//! §11, §12).
//!
//! Section numbers (§) refer to the [SkyS3 design](https://github.com/skys3/skys3/blob/main/docs/skys3-design.md).
//!
//! - [`Gateway`]: the hyper service. Each request passes the
//!   [`RequestLimits`], an [`Authenticator`] such as the
//!   [`SigV4Authenticator`], the anonymous-access gate, the checks that
//!   reject features SkyS3 does not support (SSE, Object Lock, versioning,
//!   ACLs other than bucket-owner-enforced), and bounds on XML bodies,
//!   before `s3s` routes it, [`authz`] authorizes it, and `s3s` parses it
//!   and calls the S3 operation.
//!   [`GatewayListener`] serves it over HTTP/1.1. A `POST` to `/` goes to
//!   the gateway's [`StsService`], if it has one, instead.
//! - Bucket operations: CreateBucket with a mode and, for `write_back`, a
//!   target ([`MODE_HEADER`], [`TARGET_HEADER`]); DeleteBucket, which
//!   detaches; HeadBucket; ListBuckets; and GetBucketLocation. Bucket
//!   registers live in the control store, and the gateway answers reads
//!   from its local copy.
//! - Object operations: PutObject, GetObject (with a byte range),
//!   HeadObject, and DeleteObject, with conditional requests evaluated
//!   against the key's entry in its shard's index, and conditional PUTs
//!   ([`Precondition`]) checked when the shard sequences the write. A body
//!   up to `inline_max_bytes` is stored inline in its `PUT` record, a
//!   larger one as `EXTENT` records while it arrives, and a PUT is answered
//!   only once its record is durable and applied. User metadata is limited
//!   to [`MAX_USER_METADATA_BYTES`]. DeleteObjects deletes up to
//!   [`MAX_DELETE_KEYS`] keys, each authorized on its own. CopyObject copies
//!   the source's bytes into the destination's shard and commits a `PUT`
//!   that records its source. Object tags, at most [`MAX_OBJECT_TAGS`], are
//!   stored with the `PUT` or replaced by a `TAGS` record.
//! - Listings: ListObjectsV2 and ListObjects (V1), with prefixes,
//!   delimiters, `start-after` and markers, and `encoding-type=url`. Each
//!   shard returns a sorted page from its index, the gateway merges them,
//!   and a V2 page ends with a continuation token authenticated with the
//!   gateway's [`ListTokenKeys`].
//! - [`Shards`] and [`ShardRef`]: the shard interface the gateway calls,
//!   and routing of each key to its shard with the frozen hash. On a single
//!   node every shard is local: [`LocalShards`] serves the interface from
//!   the node's `skys3_shard::ShardSet`.
//!
//! - [`sigv4`]: SigV4 authentication with the `Authorization` header or a
//!   presigned URL, session tokens, and `aws-chunked` bodies with signed
//!   chunks and trailers. It finds credentials through a
//!   [`CredentialLookup`] and records the caller in an [`Authenticated`]
//!   request extension.
//! - [`StaticCredentials`]: the access keys of bootstrap and service
//!   accounts, from `[identity.static_credentials]`, with secrets held by
//!   `secrecy` and zeroed when dropped.
//! - [`authz`]: the IAM action each S3 operation needs, and the
//!   [`Principal`] and [`Permissions`] a request is authorized against,
//!   built from policies in the subset of `skys3_types::policy`.
//!
//! - [`checksum`]: checksum validation of request bodies, on a blocking
//!   pool, for CRC32, CRC32C, CRC64NVME, SHA1, SHA256, and `Content-MD5`
//!   (trailing checksums included), MD5 ETags, and multipart ETags and
//!   checksums. PutObject uses it.
//!
//! The gateway serves path-style requests, over HTTP or, with
//! [`GatewayListener::with_tls`], HTTPS. The `skys3` node binary serves it.
//!
//! With the `test-util` feature, `stub` provides [`LocalShards`] on a
//! simulated disk, `TrustAll` an authenticator that takes every request
//! as signed by a principal allowed everything, and
//! `sigv4::MemoryCredentials` a fixed set of keys.
//!
//! ```
//! use skys3_control::{MemoryControlStore, ProposalIds, RetryPolicy, bootstrap};
//! use skys3_gateway::stub::MemoryShards;
//! use skys3_gateway::{Gateway, GatewayConfig, IdSource, TrustAll};
//!
//! # tokio::runtime::Builder::new_current_thread().enable_time().build()?.block_on(async {
//! let config: skys3_config::Config = r#"
//!     [cluster]
//!     cluster_id = "dev"
//!     [control_store]
//!     etcd_endpoints = ["https://etcd-1.example.internal:2379"]
//! "#
//! .parse()?;
//! let config = GatewayConfig::new(&config);
//! let store = MemoryControlStore::new();
//! bootstrap(&store, &config.cluster_id, ProposalIds::seeded(1).next_id(), &RetryPolicy::default())
//!     .await?;
//! let shards = MemoryShards::new().await;
//! let gateway = Gateway::new(config, store, shards, IdSource::seeded(1), TrustAll).await?;
//! let request = http::Request::put("/photos")
//!     .header(skys3_gateway::MODE_HEADER, "local")
//!     .body(s3s::Body::empty())?;
//! assert_eq!(gateway.handle(request).await.status(), 200);
//!
//! let put = http::Request::put("/photos/cat.txt").body(s3s::Body::from("meow".to_owned()))?;
//! let response = gateway.handle(put).await;
//! assert_eq!(response.status(), 200);
//! assert_eq!(response.headers()["etag"], "\"4a4be40c96ac6314e91d93f38043a634\"");
//! let get = http::Request::get("/photos/cat.txt").body(s3s::Body::empty())?;
//! assert_eq!(gateway.handle(get).await.headers()["content-length"], "4");
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # })?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod api;
pub mod authz;
mod buckets;
pub mod checksum;
mod conditions;
mod credentials;
mod features;
#[cfg(feature = "test-util")]
#[doc(hidden)]
pub mod fuzzing;
mod limits;
mod listener;
mod listing;
mod local;
mod objects;
mod service;
mod shard;
pub mod sigv4;
#[cfg(any(test, feature = "test-util"))]
pub mod stub;

pub use authz::{Permissions, Principal};
pub use buckets::{GatewayConfig, IdSource, MODE_HEADER, TARGET_HEADER};
pub use conditions::{ConditionFailed, Precondition};
pub use credentials::{CredentialError, MAX_SECRET_BYTES, MIN_SECRET_BYTES, StaticCredentials};
pub use limits::{MAX_KEY_BYTES, MAX_PART_NUMBER, MAX_RANGE_HEADER_BYTES, RequestLimits};
pub use listener::GatewayListener;
pub use listing::{ListTokenKeys, MAX_KEYS, ShortTokenKey};
pub use local::LocalShards;
pub use objects::{
    MAX_DELETE_KEYS, MAX_OBJECT_BYTES, MAX_OBJECT_TAGS, MAX_TAG_KEY_CHARS, MAX_TAG_VALUE_CHARS,
    MAX_USER_METADATA_BYTES,
};
#[cfg(any(test, feature = "test-util"))]
pub use service::TrustAll;
pub use service::{Authenticator, Gateway, StsService};
pub use shard::{ShardError, ShardRef, ShardSummary, Shards};
pub use sigv4::{
    Authenticated, BodyError, CredentialLookup, LookupError, SecretAccessKey, SigV4Authenticator,
    SigningCredential, Trailers,
};
