#![forbid(unsafe_code)]
//! The node log: SkyS3's source of truth on each disk (§10.1, §10.4).
//!
//! Section numbers (§) refer to the [SkyS3 design](https://github.com/skys3/skys3/blob/main/docs/skys3-design.md).
//!
//! Each disk has append-only segment files shared by every shard replica on
//! it. This crate owns what goes into them:
//!
//! - [`record`]: the on-disk record format. A record is a fixed header
//!   (magic, format version, kind, shard, epoch, seq, key hash, lengths, and
//!   a CRC32C), a kind-specific header, and a payload. The module
//!   documentation specifies the layout byte for byte.
//!
//! Segments, group commit, and recovery build on the record format and
//! are added to this crate next.
//!
//! ```
//! use skys3_log::record::{DecodeError, ErrorClass, LogRecord, RecordBody, ShardRef};
//! use skys3_types::{BucketId, Epoch, EpochSeq, Seq, ShardId};
//!
//! let record = LogRecord {
//!     shard: ShardRef::new(BucketId::new("b-7f3a")?, ShardId::new(3)),
//!     position: EpochSeq::new(Epoch::new(43), Seq::new(100)),
//!     body: RecordBody::Truncate,
//! };
//! let bytes = record.to_bytes()?;
//!
//! // A torn write leaves a prefix of the record, which never decodes.
//! let torn = LogRecord::decode(&bytes[..bytes.len() - 1]).unwrap_err();
//! assert_eq!(torn.class(), ErrorClass::Incomplete);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

pub mod record;

pub use record::{DecodeError, EncodeError, LogRecord, RecordBody, RecordKind, ShardRef};
