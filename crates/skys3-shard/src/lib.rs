#![forbid(unsafe_code)]
//! Shard replicas: the state machine every replica runs, and the runtime
//! that sequences and commits a shard's records on one node (§4.2, §5.1).
//!
//! Section numbers (§) refer to the [SkyS3 design](https://github.com/skys3/skys3/blob/main/docs/skys3-design.md).
//!
//! - [`StateMachine`]: the deterministic `apply(record)`. It moves entries
//!   through the object states of §4.2 and decides the conditional
//!   records: `IMPORT` only for a key with no entry (§9.1), `ADOPT` only
//!   for an entry still clean at the named `seq` (§9.2), and `FLUSHED`,
//!   which cleans an entry only at its current `seq` (§7.1). It depends on
//!   nothing but the record and the index, so every replica, and replay
//!   after a crash, reaches the same index. It plugs into the index as its
//!   [`Applier`](skys3_index::Applier).
//! - [`Shard`]: one shard replica with `replicas = 1`. It gives each record
//!   the shard's next position `(epoch, seq)`, commits it once the log has
//!   made it durable, and applies records in position order, whatever order
//!   the log acknowledges them in. Seals fence client writes while a bucket
//!   is deleted (§4.1).
//! - [`ShardSet`]: the shards open on a node, as the gateway opens, seals,
//!   and drops them.
//! - [`cache`]: the node-local transitions between clean and evicted, which
//!   no record makes: a read-through fill ([`Shard::fill`]) and eviction
//!   ([`Shard::evict`]).
//!
//! Replication (M2) runs the same state machine on every member; only the
//! commit rule changes.
//!
//! ```
//! use skys3_index::{Index, IndexConfig};
//! use skys3_io::SimDisk;
//! use skys3_log::record::Delete;
//! use skys3_log::{LogRecord, RecordBody, RecordLocation, SegmentId, ShardRef};
//! use skys3_shard::StateMachine;
//! use skys3_types::{BucketId, Epoch, EpochSeq, Seq, ShardId};
//!
//! let disk = SimDisk::new(7);
//! let index = Index::open_sim(&disk.mount(), "index.redb", &IndexConfig::default())?;
//! let record = LogRecord {
//!     shard: ShardRef::new(BucketId::new("b-7f3a")?, ShardId::new(3)),
//!     position: EpochSeq::new(Epoch::new(1), Seq::new(1)),
//!     body: RecordBody::Delete(Delete { key: "photos/cat.jpg".into() }),
//! };
//! let location = RecordLocation { segment: SegmentId::new(0), offset: 0, len: 100 };
//! index.apply(&StateMachine, &[(record.clone(), location)])?;
//!
//! // A delete of a key with no entry still leaves a tombstone (§9.1).
//! let entry = index.read()?.entry(&record.shard, "photos/cat.jpg")?.unwrap();
//! assert!(entry.object.is_none());
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

pub mod cache;
mod error;
mod machine;
mod multipart;
mod pipeline;
mod set;
mod shard;

pub use cache::CacheRefusal;
pub use error::ShardError;
pub use machine::{Effect, Outcome, Recorder, Rejection, StateMachine};
pub use set::ShardSet;
pub use shard::{Change, Committed, Shard, ShardSummary};
