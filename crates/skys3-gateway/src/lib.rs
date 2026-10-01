#![forbid(unsafe_code)]
//! The SkyS3 S3 gateway: the S3 API over HTTP, request limits, key-to-shard
//! routing, and bucket operations (design §3, §4.1, §11, §12).
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
//!   [`GatewayListener`] serves it over HTTP/1.1.
//! - Bucket operations: CreateBucket with a mode and, for `write_back`, a
//!   target ([`MODE_HEADER`], [`TARGET_HEADER`]); DeleteBucket, which
//!   detaches; HeadBucket; ListBuckets; and GetBucketLocation. Bucket
//!   registers live in the control store, and the gateway answers reads
//!   from its local copy.
//! - [`Shards`] and [`ShardRef`]: the shard interface the gateway calls,
//!   and routing of each key to its shard with the frozen hash. On a single
//!   node every shard is local.
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
//!   checksums. Object operations (plan M1-09) use it.
//!
//! The gateway serves path-style requests. The node binary does not serve
//! it yet (plan M1-13).
//!
//! With the `test-util` feature, `stub` provides an in-memory [`Shards`]
//! implementation, `TrustAll` an authenticator that takes every request
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
//! let gateway =
//!     Gateway::new(config, store, MemoryShards::new(), IdSource::seeded(1), TrustAll)
//!         .await?;
//! let request = http::Request::put("/photos")
//!     .header(skys3_gateway::MODE_HEADER, "local")
//!     .body(s3s::Body::empty())?;
//! assert_eq!(gateway.handle(request).await.status(), 200);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # })?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod api;
pub mod authz;
mod buckets;
pub mod checksum;
mod credentials;
mod features;
#[cfg(feature = "test-util")]
#[doc(hidden)]
pub mod fuzzing;
mod limits;
mod listener;
mod service;
mod shard;
pub mod sigv4;
#[cfg(any(test, feature = "test-util"))]
pub mod stub;

pub use authz::{Permissions, Principal};
pub use buckets::{GatewayConfig, IdSource, MODE_HEADER, TARGET_HEADER};
pub use credentials::{CredentialError, MAX_SECRET_BYTES, MIN_SECRET_BYTES, StaticCredentials};
pub use limits::{MAX_KEY_BYTES, MAX_PART_NUMBER, MAX_RANGE_HEADER_BYTES, RequestLimits};
pub use listener::GatewayListener;
#[cfg(any(test, feature = "test-util"))]
pub use service::TrustAll;
pub use service::{Authenticator, Gateway};
pub use shard::{ShardError, ShardRef, ShardSummary, Shards};
pub use sigv4::{
    Authenticated, BodyError, CredentialLookup, LookupError, SecretAccessKey, SigV4Authenticator,
    SigningCredential, Trailers,
};
