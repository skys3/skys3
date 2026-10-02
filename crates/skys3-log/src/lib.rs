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
//! - [`segment`]: segment classes (hot and bulk), ids, file names, and the
//!   [`RecordLocation`] that addresses a record.
//! - [`SegmentLog`]: a disk's log. It recovers the disk's segments, appends
//!   records with group commit, acknowledges each only once `fdatasync`
//!   covers it, takes the disk out of service on the first I/O error, and
//!   reads records back by location.
//! - [`recovery`]: what recovery does with torn tails, damage, and records
//!   it cannot read.
//!
//! The record format:
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
//!
//! A log on a simulated disk, which survives a power loss once a record is
//! acknowledged:
//!
//! ```
//! use std::sync::Arc;
//!
//! use skys3_io::{MonotonicClock, SimDisk};
//! use skys3_log::record::Delete;
//! use skys3_log::{LogConfig, LogRecord, RecordBody, SegmentClass, SegmentLog, ShardRef};
//! use skys3_types::{BucketId, Epoch, EpochSeq, Seq, ShardId};
//!
//! # let runtime = tokio::runtime::Builder::new_current_thread().enable_time().build()?;
//! # runtime.block_on(async {
//! let disk = SimDisk::new(7);
//! let clock = Arc::new(MonotonicClock::new());
//! let (log, _) = SegmentLog::open(disk.mount(), LogConfig::default(), clock.clone()).await?;
//! let record = LogRecord {
//!     shard: ShardRef::new(BucketId::new("b-7f3a")?, ShardId::new(3)),
//!     position: EpochSeq::new(Epoch::new(43), Seq::new(101)),
//!     body: RecordBody::Delete(Delete { key: "photos/cat.jpg".into() }),
//! };
//! let location = log.append(&record).await?;
//! assert_eq!(log.segments()[0].class, SegmentClass::Hot);
//!
//! drop(log);
//! disk.crash();
//! let (log, report) = SegmentLog::open(disk.mount(), LogConfig::default(), clock).await?;
//! assert!(report.torn_tails.is_empty());
//! assert_eq!(log.read(location).await?, record);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # })?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod commit;
mod config;
mod log;
pub mod record;
pub mod recovery;
mod scan;
pub mod segment;

pub use config::LogConfig;
pub use log::{LAZY_MAX_DELAY, LogError, LogStats, MAX_RECORD_LEN, Queued, SegmentLog};
pub use record::{DecodeError, EncodeError, LogRecord, RecordBody, RecordKind, ShardRef};
pub use recovery::{RecoveryError, RecoveryReport, TornTail};
pub use scan::{ScanError, ScannedRecord, SegmentScanner};
pub use segment::{RecordLocation, SegmentClass, SegmentId, SegmentInfo, SegmentSummary};
