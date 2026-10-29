#![forbid(unsafe_code)]
//! Core types shared by every SkyS3 crate.
//!
//! Section numbers (§) refer to the [SkyS3 design](https://github.com/skys3/skys3/blob/main/docs/skys3-design.md).
//!
//! - **Identifiers** ([`ClusterId`], [`BucketId`], [`NodeId`], [`Label`],
//!   [`BucketName`], [`ProposalId`]): validated strings whose length limits
//!   and character sets are part of the design (§7.2). Constructors,
//!   [`FromStr`](std::str::FromStr), and deserialization all validate, so a
//!   value of one of these types is always valid. [`NodeAddress`] validates
//!   the `host:port` a node is reached at the same way.
//! - **Log positions** ([`Epoch`], [`Seq`], [`EpochSeq`], [`Generation`]):
//!   shard configuration epochs, per-shard sequence numbers, and the
//!   `(epoch, seq)` pairs that order a shard's log.
//! - **Sharding** ([`ShardId`], [`ShardCount`], [`KeyHash`],
//!   [`shard_for_key`]): the frozen key-to-shard function
//!   `hash(bucket_id, key) mod shards` (§4.1). The [`shard`] module
//!   specifies the hash.
//! - **Identities** ([`WriteIdentity`], [`VersionIdentity`], [`ETag`]): the
//!   identity every flushed object carries (§7.2) and the identity of a
//!   committed version (§9.2).
//! - **Limits** ([`limits`]): size limits that the log record format and
//!   configuration loading both enforce.
//! - **Register documents** ([`ShardConfig`], [`BucketDocument`],
//!   [`NodeRegistration`], [`CoordinatorLease`], [`ClusterDocument`]): the
//!   JSON values of the control-store registers (§6.1), behind the
//!   [`RegisterDocument`] trait.
//!
//! ```
//! use skys3_types::{BucketId, ClusterId, EpochSeq, ShardCount, WriteIdentity, shard_for_key};
//!
//! let bucket = BucketId::new("b-7f3a")?;
//! let shard = shard_for_key(&bucket, b"photos/cat.jpg", ShardCount::new(8)?);
//! let position: EpochSeq = "42.1001".parse()?;
//! let wid = WriteIdentity::new(ClusterId::new("skys3-prod-a")?, bucket, shard, position);
//! assert_eq!(wid.to_string(), "skys3-prod-a/b-7f3a/7/42.1001");
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod address;
mod id;
mod identity;
pub mod limits;
mod position;
mod register;
pub mod shard;

pub use address::{AddressError, DnsName, Host, NodeAddress};
pub use id::{BucketId, BucketName, ClusterId, IdError, Label, NodeId, ProposalId};
pub use identity::{ETag, ETagError, ParseWriteIdentityError, VersionIdentity, WriteIdentity};
pub use position::{Epoch, EpochSeq, Generation, ParseNumberError, Seq};
pub use register::{
    BucketDocument, BucketMode, ClusterDocument, CoordinatorLease, DiskInfo, InvalidRegister,
    NodeRegistration, RegisterDocument, RegisterError, RemoteTarget, ReplicationError,
    ReplicationSettings, ShardConfig,
};
pub use shard::{KeyHash, ShardCount, ShardCountError, ShardId, shard_for_key};
