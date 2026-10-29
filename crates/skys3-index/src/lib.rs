#![forbid(unsafe_code)]
//! The node index: what a node knows about the objects and control state it
//! holds, in one redb database per node (§10.2).
//!
//! Section numbers (§) refer to the [SkyS3 design](https://github.com/skys3/skys3/blob/main/docs/skys3-design.md).
//!
//! The log is the source of truth (§10.1). The index is what applying the
//! log produced, kept so that reads never scan the log:
//!
//! - **Tables** ([`IndexReader`], [`IndexWriter`], [`ControlWriter`]):
//!   each shard's namespace index of [`Entry`] values, keyed by shard and
//!   object key; the node-local location map from a record's position to
//!   its segment, offset, and length; each shard's applied position; and
//!   the node's copy of control-state registers, tagged with their
//!   configuration generation (§6.2), and each bucket's namespace import
//!   ranges and their checkpoints ([`ImportRanges`], §9.1). [`codec`] specifies every
//!   encoding.
//! - **Listing** ([`IndexReader::list`]): one shard's page of a
//!   listing, with prefix and delimiter handling (§9.4).
//! - **Applying** ([`Index::apply`], [`Applier`]): records are applied
//!   with non-durable commits. The state machine that decides what a
//!   record does to an entry plugs in as an [`Applier`] (M1-04).
//! - **Checkpoints** ([`Index::checkpoint`], [`Checkpointer`]): a durable
//!   commit every `index_checkpoint_interval`. After a crash the database
//!   reverts to its last checkpoint, and [`Checkpointer::replay`] applies
//!   the log records after each shard's checkpointed position, through
//!   the same [`Applier`], which reproduces the index exactly.
//! - **Snapshots** ([`IndexReader::shard_rows`], [`Index::begin_install`]):
//!   a primary sends a learner a shard's rows, and the learner installs
//!   them in place of what it held, at the position they were taken at
//!   (§6.7).
//! - **Releasing segments**: each checkpoint also stores what each log
//!   segment holds: the highest position of each shard's records in it,
//!   from the log's [`SegmentSummary`](skys3_log::SegmentSummary). A
//!   segment is released to the log ([`SegmentLog::release`](skys3_log::SegmentLog::release))
//!   once every record in it is behind the durable checkpoint, and replay
//!   skips what the checkpoint covers.
//!
//! The index runs on a real file ([`Index::open`]) or on a simulated
//! disk's block file ([`Index::open_sim`]), where a simulated crash
//! reverts it as a power loss would.
//!
//! ```
//! use skys3_index::{Index, IndexConfig};
//! use skys3_io::SimDisk;
//!
//! let disk = SimDisk::new(7);
//! let index = Index::open_sim(&disk.mount(), "index.redb", &IndexConfig::default())?;
//! assert!(index.read()?.applied_positions()?.is_empty());
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod checkpointer;
pub mod codec;
mod entry;
mod error;
mod import;
mod index;
mod listing;
mod tables;

pub use checkpointer::{Checkpointer, ReplayReport};
pub use entry::{
    ControlEntry, Entry, EntryState, ImportCheckpoint, ObjectPart, ObjectVersion, Part, Payload,
    Upload,
};
pub use error::IndexError;
pub use import::{ImportRange, ImportRanges, MAX_IMPORT_RANGES};
pub use index::{
    Applier, Checkpoint, FORMAT_VERSION, Index, IndexConfig, LogState, MIN_FORMAT_VERSION,
};
pub use listing::{ListItem, ListPage, ListQuery};
pub use tables::{ControlWriter, IndexDump, IndexReader, IndexWriter, ShardRow, ShardTable};
