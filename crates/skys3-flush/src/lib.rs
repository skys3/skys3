#![forbid(unsafe_code)]
//! Write-back flush: sending each `write_back` bucket's committed writes to
//! its remote target (§7.1, §7.2, §7.4).
//!
//! Section numbers (§) refer to the [SkyS3 design](https://github.com/skys3/skys3/blob/main/docs/skys3-design.md).
//!
//! - [`ShardFlusher`]: the flusher of one shard on its primary. It flushes
//!   each dirty key's latest committed version, oldest first, with at most
//!   one flush per key in flight and a fixed number of keys at once, and
//!   records each with a `FLUSHED` record that rides the next group commit.
//!   Its keys move through the §4.2 states Dirty, Flushing, and Conflict.
//! - **Conditional requests** (§7.2). A version is sent with `PutObject`,
//!   a tombstone with `DeleteObject`, conditioned on what the index or an
//!   earlier flush says the remote holds: `If-Match` on its ETag, or
//!   `If-None-Match: *` where the key is absent. Every object carries the
//!   write identity of the version. After a failed precondition the
//!   flusher HEADs the key: its own identity means an earlier attempt
//!   succeeded, an earlier write of the shard is superseded, and anything
//!   else is a conflict, held under the `hold` policy. Operations the
//!   capability probe found unprotected are sent unconditionally.
//! - **Multipart objects** (§7.4) are sent as a remote multipart upload
//!   with the client's part boundaries, so the remote ETag is the local
//!   one. The remote `CreateMultipartUpload` carries the write identity of
//!   the upload's `MPU_CREATE`, and the `CompleteMultipartUpload` carries
//!   the precondition. An upload that does not complete is aborted, and
//!   one whose abort fails is kept by the [`Target`] to abort later.
//! - **Tags** (`TAGS`) need no request of their own: a version made by
//!   `TAGS` is flushed like any other, the bytes uploaded again with the
//!   new tags and the `TAGS` record's write identity.
//! - [`Target`]: what the flushers of one target share: the store, the
//!   probe's findings, the in-flight byte budget, the settings
//!   ([`FlushSettings`]), and the remote uploads left to abort.
//! - [`FlushService`]: every flusher of a node, following its buckets and
//!   open shards, with each target's capability probe.
//! - [`FlushMetrics`]: `dirty_bytes`, `oldest_dirty_age`,
//!   `flush_lag_seconds`, and conflict counts, by bucket.
//! - [`DirtyBudget`] (§7.6): the flushers' dirty bytes counted against each
//!   bucket's and the cluster's `max_dirty_bytes`, which admission control
//!   checks before a write. Each node enforces a share of each budget in
//!   proportion to the shards whose primary it is ([`share`]).
//!
//! Streaming multipart flush (M4-02) opens the remote upload while the
//! client uploads, and adaptive concurrency (M4-10) replaces the fixed
//! [`FlushSettings::concurrency`].
//!
//! ```
//! use skys3_flush::FlushSettings;
//!
//! let settings = FlushSettings::default();
//! assert_eq!(settings.backoff(1), settings.min_backoff);
//! assert_eq!(settings.backoff(40), settings.max_backoff);
//! ```

mod attempt;
mod budget;
mod metrics;
mod multipart;
mod service;
mod shard;
mod target;

pub use attempt::Conflict;
pub use budget::{DirtyBudget, Exhausted, Usage, share};
pub use metrics::{Counters, FlushMetrics, Gauges};
pub use service::{BucketStatus, Connect, FlushService, ProbeStatus};
pub use shard::{ConflictStatus, Phase, ShardFlusher, ShardStatus};
pub use target::{FlushSettings, ImportDone, ImportProgress, Target};
