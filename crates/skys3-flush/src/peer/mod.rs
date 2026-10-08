//! The native peer protocol as a target's transport (§7.8, plan M6-06):
//! flushing to another SkyS3 cluster with `COMMIT`s that the destination
//! evaluates in its own shard log, instead of S3 REST requests.
//!
//! - **Which targets.** A bucket whose `target_transport` is `native`,
//!   when the flush service can reach the peer
//!   ([`FlushService::with_peer_transport`](crate::FlushService::with_peer_transport)).
//!   Its target is ready at once: the destination honors every
//!   precondition, so nothing is probed.
//! - **Preconditions** (§7.2). A version is committed under `Matches` the
//!   destination's current write identity, `Absent` where the key is known
//!   to be absent, or `Unconditional` under an `overwrite` resolution. A
//!   failed precondition names the current identity, which takes the place
//!   of the 412 HEAD: the version's own identity means an earlier commit
//!   applied, an earlier write of the shard is superseded, and anything
//!   else is a conflict.
//! - **`FLUSHED` only after `APPLIED`.** A flush ends only with the
//!   destination's `APPLIED`; a lost answer, a dropped link, or an answer
//!   that does not come in time leaves the key dirty, and its next flush
//!   sends the same `COMMIT` again, which the destination answers from the
//!   version it stored. The `FLUSHED` record keeps the destination
//!   version's write identity as its `remote_version_id`, so a restarted
//!   flusher, or a new primary's, conditions the key's next version on it.
//!   Write-through waits are answered by the same flush, so only after
//!   `APPLIED` too.
//! - **Small objects** of up to one frame, and deletes, travel in `BATCH`es
//!   that the target's flushers share ([`Batcher`](batch::Batcher)).
//! - **Large objects** are staged: `BEGIN`, the `RESUME` that says what the
//!   destination holds durably, `DATA` frames for the rest, and the
//!   `COMMIT` on the same stream. A link that drops costs only the frames
//!   it lost: the next attempt's `RESUME` says what to send again.
//! - **Streamed single PUTs** (§7.3) are staged while the client uploads:
//!   the frames of each announced extent go out once it is durable locally
//!   ([`Bodies`]), and the flush of the `PUT` sends what is left and
//!   commits.
//!
//! Nothing new is persisted. Staging is keyed by the write identity, which
//! the log already holds (the `UPLOAD_BEGIN`, `MPU_CREATE`, or version
//! record), so a flusher that starts after a restart, or on a new primary,
//! finds what the destination staged by sending `BEGIN` for the version it
//! flushes. Staging that is never committed, such as a body whose `PUT`
//! never commits, expires at the destination after
//! `peer_staging_ttl_seconds`.

mod batch;
mod bodies;
mod commit;
mod link;
mod upload;

use std::sync::Arc;
use std::time::Duration;

use skys3_types::{BucketName, RemoteTarget};

pub(crate) use bodies::{Bodies, BodyStep};
pub use link::{BoxFuture, LinkError, PeerLink, PeerReceive, PeerSend, PeerStream};

use batch::Batcher;

/// The piece that holds a staged object's bytes. One write identity names
/// one write, whose bytes at an offset never change, so its staging needs
/// a single piece, whichever flusher, in whichever life, sends them.
pub(crate) const PIECE: u64 = 0;

/// How long a stream to a peer may make no progress before its flush
/// fails and is retried: the QUIC idle timeout (plan M6-02).
pub const DEFAULT_ANSWER_TIMEOUT: Duration = Duration::from_secs(30);

/// Builds the link to the SkyS3 cluster a `native` target names, or
/// `None` if the node cannot reach one there.
pub type PeerConnect = Box<dyn Fn(&RemoteTarget) -> Option<Arc<dyn PeerLink>> + Send + Sync>;

/// How a flush service reaches the SkyS3 peers its `native` targets name
/// (§7.8).
pub struct PeerTransport {
    pub(crate) connect: PeerConnect,
    pub(crate) frame_bytes: u64,
    pub(crate) timeout: Duration,
}

impl std::fmt::Debug for PeerTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerTransport")
            .field("frame_bytes", &self.frame_bytes)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl PeerTransport {
    /// Reaches each target's peer through `connect`, with `DATA` frames of
    /// `frame_bytes` (`peer_frame_bytes`), which is also the largest
    /// object a `BATCH` carries.
    #[must_use]
    pub fn new(connect: PeerConnect, frame_bytes: u64) -> Self {
        Self {
            connect,
            frame_bytes: frame_bytes.max(1),
            timeout: DEFAULT_ANSWER_TIMEOUT,
        }
    }

    /// Sets how long a stream may make no progress
    /// ([`DEFAULT_ANSWER_TIMEOUT`] by default).
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// The native transport of `target`, whose objects go to the bucket
    /// its URL names, if the peer can be reached.
    pub(crate) fn native(&self, target: &RemoteTarget) -> Option<Native> {
        let link = (self.connect)(target)?;
        let bucket = match target.bucket.parse::<BucketName>() {
            Ok(bucket) => bucket,
            Err(error) => {
                tracing::warn!(bucket = target.bucket, %error,
                    "a native target's bucket is not a valid bucket name");
                return None;
            }
        };
        Some(Native::new(link, bucket, self.frame_bytes, self.timeout))
    }
}

/// A target's native transport: the destination bucket, and how its
/// flushers reach it.
pub(crate) struct Native {
    pub(crate) link: Arc<dyn PeerLink>,
    /// The destination bucket.
    pub(crate) bucket: BucketName,
    /// `peer_frame_bytes`.
    pub(crate) frame_bytes: u64,
    /// How long a stream may make no progress.
    pub(crate) timeout: Duration,
    /// The batches every flusher of the target shares.
    pub(crate) batcher: Batcher,
}

impl Native {
    /// A transport over `link` to `bucket`. Starts the target's batcher on
    /// the current Tokio runtime.
    pub(crate) fn new(
        link: Arc<dyn PeerLink>,
        bucket: BucketName,
        frame_bytes: u64,
        timeout: Duration,
    ) -> Self {
        let batcher = Batcher::spawn(Arc::clone(&link), timeout);
        Self {
            link,
            bucket,
            frame_bytes: frame_bytes.max(1),
            timeout,
            batcher,
        }
    }
}

impl std::fmt::Debug for Native {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Native")
            .field("bucket", &self.bucket)
            .field("frame_bytes", &self.frame_bytes)
            .finish_non_exhaustive()
    }
}

/// Seeded bugs of the native transport, for the simulation's audits (the
/// `test-util` feature exports [`seed_peer_bug`](hooks::seed_peer_bug)).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum PeerBug {
    /// No bug.
    #[default]
    None,
    /// A flush counts as done once its `COMMIT` is sent, without waiting
    /// for `APPLIED`: the key is recorded `FLUSHED` before the destination
    /// applied it, or although it never does.
    FlushedOnCommit,
    /// Write-through writes waiting for a version are answered once its
    /// `COMMIT` is sent, before `APPLIED`; the `FLUSHED` record still waits
    /// for it.
    AnsweredOnCommit,
}

/// What a flusher records as flushed to a native target, as the
/// simulation's observer sees it ([`observe_flushed`](hooks::observe_flushed)).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FlushedRecord {
    /// The shard.
    pub shard: skys3_log::ShardRef,
    /// The key.
    pub key: String,
    /// The version recorded.
    pub version: skys3_types::EpochSeq,
    /// The write identity the destination's version carries, `None` for a
    /// delete.
    pub identity: Option<String>,
}

pub(crate) mod hooks {
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;

    use super::{FlushedRecord, PeerBug};

    /// What the simulation calls for each `FLUSHED` a flusher decides to
    /// record ([`observe_flushed`]).
    #[cfg(feature = "test-util")]
    pub type Observer = Box<dyn Fn(&FlushedRecord)>;

    /// The observer, shared with the calls in progress.
    type Shared = Rc<dyn Fn(&FlushedRecord)>;

    thread_local! {
        static BUG: Cell<PeerBug> = const { Cell::new(PeerBug::None) };
        static OBSERVER: RefCell<Option<Shared>> = const { RefCell::new(None) };
    }

    /// Seeds `bug` into the native transport of every flusher this thread
    /// runs. A deterministic simulation runs every node on its test's
    /// thread, so other tests run the real code.
    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    pub fn seed_peer_bug(bug: PeerBug) {
        BUG.with(|seeded| seeded.set(bug));
    }

    /// Calls `observer`, or with `None` nothing, for each `FLUSHED` record
    /// a flusher on this thread decides to commit for a native target,
    /// before it commits it.
    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    pub fn observe_flushed(observer: Option<Observer>) {
        OBSERVER.with(|slot| *slot.borrow_mut() = observer.map(Rc::from));
    }

    /// The bug seeded on this thread.
    pub(crate) fn peer_bug() -> PeerBug {
        BUG.with(Cell::get)
    }

    /// Tells the observer, if any, of `record`.
    pub(crate) fn flushed(record: &FlushedRecord) {
        let observer = OBSERVER.with(|slot| slot.borrow().clone());
        if let Some(observer) = observer {
            observer(record);
        }
    }
}
