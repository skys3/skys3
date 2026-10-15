//! The native peer protocol as a target's transport (§7.8, plan M6-06):
//! flushing to another SkyS3 cluster with `COMMIT`s that the destination
//! evaluates in its own shard log, instead of S3 REST requests.
//!
//! - **Which targets.** A bucket whose `target_transport` is `auto` or
//!   `native`, on a node whose flush service can reach peers
//!   ([`FlushService::with_peer_transport`](crate::FlushService::with_peer_transport)),
//!   once its target's discovery chose QUIC ([`discovery`]). Its target is
//!   ready at once: the destination honors every precondition, so nothing
//!   is probed.
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
pub(crate) mod discovery;
mod link;
mod upload;

use std::sync::Arc;
use std::time::Duration;

use skys3_io::WallClock;
use skys3_peer::{Commit, DescriptorVerifier, PeerDescriptor};
use skys3_types::BucketName;

pub(crate) use bodies::{Bodies, BodyStep};
pub use discovery::{Transport, TransportStatus};
pub use link::{BoxFuture, LinkError, PeerLink, PeerReceive, PeerSend, PeerStream};

use batch::Batcher;
use discovery::LinkAlarm;

/// The piece that holds a staged object's bytes. One write identity names
/// one write, whose bytes at an offset never change, so its staging needs
/// a single piece, whichever flusher, in whichever life, sends them.
pub(crate) const PIECE: u64 = 0;

/// How long a stream to a peer may make no progress before its flush
/// fails and is retried: the QUIC idle timeout (plan M6-02).
pub const DEFAULT_ANSWER_TIMEOUT: Duration = Duration::from_secs(30);

/// The least time between two handshakes a target falling back to S3 REST
/// tries (§7.8); it doubles after each one that fails.
pub const DEFAULT_REPROBE_MIN: Duration = Duration::from_secs(15);

/// The most time between two handshakes a target on S3 REST tries.
pub const DEFAULT_REPROBE_MAX: Duration = Duration::from_secs(300);

/// How far the wall clock of any node of two peered clusters may be from
/// true time, which the quarantine allows for twice: once on the source
/// that stamps a write's apply-by time, once on the destination that
/// checks it (§7.8).
pub const CLOCK_TOLERANCE: Duration = Duration::from_secs(30);

/// The most bytes a peer write whose apply-by time bounds it carries,
/// inline in a `BATCH` or as an S3 `PutObject`'s body: the commit window
/// must cover sending them (§7.8). Larger objects are staged, or uploaded
/// in parts, and the small `COMMIT` or `CompleteMultipartUpload` that
/// publishes them carries the apply-by time.
pub const MAX_BOUNDED_BYTES: u64 = 1 << 20;

/// How a target's peer writes are stamped with their apply-by time: the
/// source's wall clock as each is sent, plus the commit window (§7.8).
#[derive(Clone)]
pub(crate) struct Stamp {
    wall: Arc<dyn WallClock>,
    window: Duration,
}

impl std::fmt::Debug for Stamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Stamp")
            .field("window", &self.window)
            .finish_non_exhaustive()
    }
}

impl Stamp {
    pub(crate) fn new(wall: Arc<dyn WallClock>, window: Duration) -> Self {
        Self { wall, window }
    }

    /// The apply-by time of a write sent now.
    pub(crate) fn apply_by_ms(&self) -> u64 {
        u64::try_from((self.wall.now() + self.window).as_millis()).unwrap_or(u64::MAX)
    }

    /// `commit` as it is sent now, with its apply-by time.
    pub(crate) fn commit(&self, commit: &Commit) -> Commit {
        Commit {
            apply_by_ms: Some(self.apply_by_ms()),
            ..commit.clone()
        }
    }
}

/// Makes a link to the destination a verified descriptor names, once a
/// fresh QUIC handshake with it, `HELLO`s included, succeeded: the probe
/// that decides whether a target uses QUIC (§7.8). An error says why the
/// handshake failed.
pub type PeerConnect = Box<
    dyn Fn(&PeerDescriptor) -> BoxFuture<'static, Result<Arc<dyn PeerLink>, LinkError>>
        + Send
        + Sync,
>;

/// How a flush service reaches the SkyS3 peers its `auto` and `native`
/// targets name (§7.8).
pub struct PeerTransport {
    pub(crate) connect: PeerConnect,
    pub(crate) verifier: DescriptorVerifier,
    pub(crate) frame_bytes: u64,
    pub(crate) timeout: Duration,
    pub(crate) connect_timeout: Duration,
    pub(crate) reprobe: (Duration, Duration),
    pub(crate) quarantine: Option<Duration>,
    pub(crate) commit_window: Option<Duration>,
}

impl std::fmt::Debug for PeerTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerTransport")
            .field("frame_bytes", &self.frame_bytes)
            .field("timeout", &self.timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("reprobe", &self.reprobe)
            .field("commit_window", &self.commit_window())
            .field("quarantine", &self.quarantine())
            .finish_non_exhaustive()
    }
}

impl PeerTransport {
    /// Reaches peers through `connect`, once their descriptors pass
    /// `verifier`, with `DATA` frames of `frame_bytes`
    /// (`peer_frame_bytes`), which is also the largest object a `BATCH`
    /// carries. Handshakes must finish within 3 s (the default
    /// `peer_connect_timeout_ms`).
    #[must_use]
    pub fn new(connect: PeerConnect, verifier: DescriptorVerifier, frame_bytes: u64) -> Self {
        Self {
            connect,
            verifier,
            frame_bytes: frame_bytes.max(1),
            timeout: DEFAULT_ANSWER_TIMEOUT,
            connect_timeout: Duration::from_secs(3),
            reprobe: (DEFAULT_REPROBE_MIN, DEFAULT_REPROBE_MAX),
            quarantine: None,
            commit_window: None,
        }
    }

    /// Sets how long a stream may make no progress
    /// ([`DEFAULT_ANSWER_TIMEOUT`] by default).
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets how long a handshake may take before the target falls back to
    /// S3 REST (`peer_connect_timeout_ms`).
    #[must_use]
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Sets the least and the most time between two handshakes of a target
    /// on S3 REST ([`DEFAULT_REPROBE_MIN`] and [`DEFAULT_REPROBE_MAX`] by
    /// default).
    #[must_use]
    pub fn with_reprobe(mut self, min: Duration, max: Duration) -> Self {
        self.reprobe = (min, max.max(min));
        self
    }

    /// Sets how long a shard flusher that starts on S3 REST to a peer waits
    /// before its first S3 flush ([`PeerTransport::quarantine`]).
    #[must_use]
    pub fn with_quarantine(mut self, quarantine: Duration) -> Self {
        self.quarantine = Some(quarantine);
        self
    }

    /// Sets how long after it is sent a `COMMIT` may still be applied:
    /// its apply-by time is the source's wall clock as it is sent plus
    /// this window (the answer timeout by default).
    #[must_use]
    pub fn with_commit_window(mut self, window: Duration) -> Self {
        self.commit_window = Some(window);
        self
    }

    /// How long after it is sent a `COMMIT` may still be applied
    /// ([`PeerTransport::with_commit_window`]).
    #[must_use]
    pub fn commit_window(&self) -> Duration {
        self.commit_window.unwrap_or(self.timeout)
    }

    /// How long each shard flusher that starts on S3 REST to a peer, after
    /// a fallback, a restart, or a takeover, waits before its first S3
    /// flush: until no `COMMIT` sent over QUIC before it started, on this
    /// node or by an earlier primary, can still apply (§7.8). Each
    /// `COMMIT` carries an apply-by time, its sender's wall clock plus the
    /// commit window, and the destination's shard sequences it by then on
    /// its own wall clock, or never. With every clock within
    /// [`CLOCK_TOLERANCE`] of true time, the wait is the commit window
    /// plus twice that: 90 s by default.
    #[must_use]
    pub fn quarantine(&self) -> Duration {
        self.quarantine
            .unwrap_or(self.commit_window() + CLOCK_TOLERANCE.saturating_mul(2))
    }

    /// The wait before the next handshake after `failures` failed rounds.
    pub(crate) fn reprobe_after(&self, failures: u32) -> Duration {
        discovery::backoff(self.reprobe.0, self.reprobe.1, failures)
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
    /// Tells the target's discovery when a link fails, if it has one.
    alarm: Option<LinkAlarm>,
    /// How its `COMMIT`s are stamped with their apply-by time.
    stamp: Stamp,
}

impl Native {
    /// A transport over `link` to `bucket`, whose `COMMIT`s `stamp` gives
    /// an apply-by time. Starts the target's batcher on the current Tokio
    /// runtime.
    pub(crate) fn new(
        link: Arc<dyn PeerLink>,
        bucket: BucketName,
        frame_bytes: u64,
        timeout: Duration,
        stamp: Stamp,
    ) -> Self {
        let batcher = Batcher::spawn(Arc::clone(&link), timeout, stamp.clone());
        Self {
            link,
            bucket,
            frame_bytes: frame_bytes.max(1),
            timeout,
            batcher,
            alarm: None,
            stamp,
        }
    }

    /// `commit` as it is sent now, with its apply-by time.
    pub(crate) fn stamped(&self, commit: &Commit) -> Commit {
        self.stamp.commit(commit)
    }

    /// Tells `alarm` of every link that fails.
    pub(crate) fn with_alarm(mut self, alarm: LinkAlarm) -> Self {
        self.alarm = Some(alarm);
        self
    }

    /// A flush's link failed: the discovery, if any, checks the path.
    pub(crate) fn link_failed(&self) {
        if let Some(alarm) = &self.alarm {
            alarm.link_failed();
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
    /// A peer descriptor is used without checking its chain, signature, or
    /// times: a forged or expired descriptor sends the flushes wherever it
    /// points.
    UnverifiedDescriptor,
    /// A shard flusher that starts on S3 REST to a peer flushes at once,
    /// while `COMMIT`s sent over QUIC may still apply.
    NoQuarantine,
    /// A shard flusher that starts on QUIC commits at once, while S3
    /// writes sent to the peer before it started may still apply.
    NoReturnQuarantine,
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
    /// The write identity of the `COMMIT` whose `APPLIED` the record
    /// follows: the one the destination's version carries, or a delete's.
    pub identity: String,
    /// Whether the version is a delete.
    pub delete: bool,
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

#[cfg(test)]
mod tests {
    use skys3_io::ManualWallClock;
    use skys3_peer::{PeerTrust, Precondition, Write};

    use super::*;

    fn transport() -> PeerTransport {
        PeerTransport::new(
            Box::new(|_: &PeerDescriptor| Box::pin(async { Err(LinkError::new("no peers here")) })),
            DescriptorVerifier::new(Arc::new(PeerTrust::new())),
            1024,
        )
    }

    #[test]
    fn the_quarantine_outlasts_every_commit_window_and_clock_error() {
        let peers = transport();
        assert_eq!(peers.commit_window(), DEFAULT_ANSWER_TIMEOUT);
        assert_eq!(peers.quarantine(), Duration::from_secs(90));
        let peers = transport()
            .with_timeout(Duration::from_secs(5))
            .with_commit_window(Duration::from_secs(8));
        assert_eq!(peers.commit_window(), Duration::from_secs(8));
        assert_eq!(peers.quarantine(), Duration::from_secs(68));
        let peers = peers.with_quarantine(Duration::from_secs(3));
        assert_eq!(peers.quarantine(), Duration::from_secs(3));
    }

    #[test]
    fn commits_are_stamped_as_they_are_sent() {
        let wall = Arc::new(ManualWallClock::new(Duration::from_secs(1_000)));
        let stamp = Stamp::new(wall.clone(), Duration::from_secs(30));
        let commit = Commit {
            identity: "prod-us/b-src/3/2.17".parse().unwrap(),
            bucket: "archive".parse().unwrap(),
            key: "k".to_owned(),
            precondition: Precondition::Absent,
            write: Write::Delete,
            apply_by_ms: None,
        };
        let stamped = stamp.commit(&commit);
        assert_eq!(stamped.apply_by_ms, Some(1_030_000));
        assert_eq!(
            Commit {
                apply_by_ms: None,
                ..stamped
            },
            commit
        );
        wall.advance(Duration::from_secs(5));
        assert_eq!(stamp.commit(&commit).apply_by_ms, Some(1_035_000));
        assert_eq!(stamp.apply_by_ms(), 1_035_000);
    }
}
