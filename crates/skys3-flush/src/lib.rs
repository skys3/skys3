#![forbid(unsafe_code)]
//! Write-back flush: sending each `write_back` bucket's committed writes to
//! its remote target (§7.1, §7.2, §7.4), and reading evicted versions back
//! from it (§9.2).
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
//!   else is a conflict. Operations the capability probe found
//!   unprotected are sent unconditionally.
//! - **Conflict policies** (§7.2). A conflict is held under `hold`, the
//!   default, until an operator resolves it under a chosen policy
//!   ([`FlushService::resolve`]); `overwrite` resolves it by sending the
//!   local version unconditionally, and `discard_local`, which a
//!   `write_back` bucket's own table must choose, by adopting the remote's
//!   write with an `ADOPT` that drops the local version. Either way the key
//!   returns to dirty, and its next flush carries out the resolution.
//! - **Multipart objects** (§7.4) are sent as a remote multipart upload
//!   with the client's part boundaries, so the remote ETag is the local
//!   one. The remote `CreateMultipartUpload` carries the write identity of
//!   the upload's `MPU_CREATE`, and the `CompleteMultipartUpload` carries
//!   the precondition.
//! - **Streaming** (§7.3). The remote upload is opened when the local one
//!   is, each part is sent once it commits while the client uploads more,
//!   and the remote upload is completed by the flush of the completed
//!   version, never before the local completion commits; every step is
//!   recorded with a `PART_FLUSHED`, so the shard log keeps the remote
//!   upload IDs and a restarted flusher, or a new primary's, resumes. A
//!   resumed stream is reconciled with a remote `ListParts` before it sends
//!   or completes: a part counts as sent only if the remote holds it with
//!   the recorded ETag or the local part's MD5. Aborted and abandoned
//!   remote uploads are aborted. A version that cannot use its stream,
//!   such as one retagged by `TAGS`, is sent after commit: a new remote
//!   upload filled from the local log. An upload of that kind that does
//!   not complete is aborted, and one whose abort fails is kept by the
//!   [`Target`] to abort later.
//! - **Streamed single PUTs** (§7.3). A single PUT that reaches
//!   `streaming_flush_min_bytes` streams the same way, as a remote
//!   multipart upload in parts of `flush_part_bytes` that carries its
//!   `UPLOAD_BEGIN`'s identity: the gateway receiving the body announces
//!   its extents to the primary ([`skys3_shard::Shard::announce`]), each
//!   part is sent once they cover it, and the flush of the `PUT` completes
//!   the upload. The entry records the multipart ETag the remote returns as
//!   its `remote_etag`, beside the MD5 `local_etag` clients see (§7.4). A
//!   body whose `PUT` does not commit within
//!   [`FlushSettings::body_timeout`] has its remote upload aborted.
//! - **Tags** (`TAGS`) need no request of their own: a version made by
//!   `TAGS` is flushed like any other, the bytes uploaded again with the
//!   new tags and the `TAGS` record's write identity.
//! - [`Target`]: what the flushers of one target share: the store, the
//!   probe's findings, the in-flight byte budget, the settings
//!   ([`FlushSettings`]), and the remote uploads left to abort.
//! - [`Filler`]: read-through fill (§9.2). It reads an evicted version from
//!   the target with `If-Match` and `versionId`, commits it as extents that
//!   become clean cache, serves ranges while it streams, coalesces
//!   concurrent reads of one version, and commits `ADOPT` when the remote
//!   changed out of band.
//! - [`FlushService`]: every flusher of a node, following its buckets and
//!   open shards, with each target's capability probe, each bucket's
//!   namespace import, and each bucket's [`Filler`].
//! - [`Origin`]: how a node reads a `read_only` bucket's origin (§9.5),
//!   which the flush service keeps beside the flushers: a
//!   [`RemoteReader`] and a [`Filler`], with the credentials of the
//!   bucket's `origin_profile`, and no flusher, probe, or import.
//! - **Namespace import** (§9.1, [`ImportState`]): a `write_back` bucket
//!   lists its remote prefix into `IMPORT` records, a page at a time, rate
//!   limited, with a durable checkpoint after each page that a restart
//!   resumes from. Until the import passes a key, its flushers keep its
//!   tombstone and HEAD before writing it, and [`RemoteReader`] serves
//!   client reads that miss locally from the remote.
//! - [`FlushMetrics`]: `dirty_bytes`, `oldest_dirty_age`,
//!   `flush_lag_seconds`, conflict counts, and fill counts, by bucket.
//! - [`DirtyBudget`] (§7.6): the flushers' dirty bytes counted against each
//!   bucket's and the cluster's `max_dirty_bytes`, which admission control
//!   checks before a write. Each node enforces a share of each budget in
//!   proportion to the shards whose primary it is ([`share`]).
//!
//! - [`snapshot`]: index snapshots of each shard to its bucket's
//!   `snapshot_target`, and the lost-key report of a shard whose members
//!   are all lost (§6.9, §8.9).
//!
//! - **Adaptive concurrency** (§7.7, [`ConcurrencyStatus`]): every request
//!   of a target's flushers goes through the target's window, which grows
//!   additively while latency stays near the target's base round trip and
//!   requests wait for it, and shrinks multiplicatively on rising latency
//!   and on throttles such as `503 SlowDown`, between
//!   `flush_min_concurrency_per_shard` and `flush_max_concurrency_per_shard`
//!   times the shards flushing to it; the bytes held for requests stay
//!   within `flush_max_inflight_bytes_per_target`.
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
mod concurrency;
mod conflict;
mod fill;
mod import;
mod metrics;
mod multipart;
mod origin;
mod service;
mod shard;
mod single;
pub mod snapshot;
mod stream;
mod target;

pub use attempt::Conflict;
pub use budget::{DirtyBudget, Exhausted, Usage, share};
pub use concurrency::{
    BASE_ROUNDS, ConcurrencyStatus, LATENCY_MIN_FACTOR, LATENCY_TARGET, LATENCY_TOLERANCE,
    THROTTLE_DECREASE,
};
pub use conflict::Unresolved;
pub use fill::{FILL_CHUNK_BYTES, FillBody, FillError, Filler};
pub use import::{
    DEFAULT_CONTENT_TYPE, IMPORT_PAGE_KEYS, ImportState, ImportStatus, RemoteObject, RemoteReader,
    loaded_metadata,
};
pub use metrics::{Counters, FlushMetrics, Gauges};
pub use origin::{DEFAULT_CREDENTIALS, Origin, OriginConnect, profile_credentials};
pub use service::{BucketStatus, Connect, FlushService, ProbeStatus};
pub use shard::{ConflictStatus, Phase, ShardFlusher, ShardStatus};
pub use target::{FlushSettings, ImportDone, ImportProgress, Target};

/// Failpoints and seeded bugs for tests (the `test-util` feature).
#[cfg(feature = "test-util")]
pub mod test_hooks {
    pub use crate::concurrency::{ConcurrencyBug, seed_concurrency_bug};
    pub use crate::conflict::{ConflictBug, seed_conflict_bug};
    pub use crate::snapshot::hooks::{SnapshotBug, seed_snapshot_bug};
    pub use crate::stream::test_hooks::*;
}
